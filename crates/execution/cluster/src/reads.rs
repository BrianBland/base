//! Read-only cursor routing over one sharded table: local MDBX for held keys, owners for the
//! rest, with the cursor position kept here rather than in MDBX.

use std::{collections::VecDeque, mem, sync::Arc, time::Instant};

use reth_db::{
    DatabaseError,
    common::{PairResult, ValueOnlyResult},
    cursor::{DbCursorRO, DbDupCursorRO},
    mdbx::{TransactionKind, cursor::Cursor},
    table::{Decode, Decompress, DupSort, Encode, IntoVec, Table, TableRow},
};

use crate::{KeyPlacement, ReadSnapshot, Shard, ShardedTable, SplitCursor};

/// Where a [`ShardedReads`] cursor is.
#[derive(Debug)]
pub enum CursorPosition {
    /// Not positioned yet: `next` starts at the first entry.
    Unset,
    /// At an encoded key read from local MDBX; the MDBX cursor is positioned there. For a
    /// duplicate-key table every operation on this key is left to MDBX.
    Local {
        /// Encoded key.
        key: Vec<u8>,
    },
    /// At a duplicate of a split contract read from local MDBX; the [`SplitCursor`] is positioned
    /// there.
    LocalSplit {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded value.
        value: Vec<u8>,
    },
    /// At an entry read from an owner, followed by the rest of its page.
    Remote {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded value.
        value: Vec<u8>,
        /// Entries of the page after this one, ascending.
        rest: VecDeque<(Vec<u8>, Vec<u8>)>,
        /// No entry of the shard (or duplicate of the key, or of the range of a split contract)
        /// follows the page.
        complete: bool,
    },
    /// A positioning call found nothing: `current` and `next` return nothing.
    Missed,
    /// `next` ran past the last entry; `current` still returns that entry.
    PastEnd {
        /// The position of the last entry.
        last: Box<Self>,
    },
}

/// An entry found by a [`ShardedReads`] cursor and the position it leaves the cursor at.
pub type FoundEntry<T> = (TableRow<T>, CursorPosition);

/// Routes the reads of one read-only cursor over a sharded table, following the spec's
/// [Read rule](crate#read-rule). The MDBX cursor of the same table is passed to every call;
/// duplicates of split contracts are read locally through this type's own [`SplitCursor`].
#[derive(Debug)]
pub struct ShardedReads<K: TransactionKind> {
    snapshot: Arc<ReadSnapshot>,
    table: ShardedTable,
    position: CursorPosition,
    split: Option<SplitCursor<K>>,
}

impl<K: TransactionKind> ShardedReads<K> {
    /// A fresh cursor over `table` reading at `snapshot`. `split` is required to read a split
    /// contract.
    pub const fn new(
        snapshot: Arc<ReadSnapshot>,
        table: ShardedTable,
        split: Option<SplitCursor<K>>,
    ) -> Self {
        Self { snapshot, table, position: CursorPosition::Unset, split }
    }

    /// The error returned by an operation that is not supported on a sharded table.
    pub fn unsupported(&self, operation: &str) -> DatabaseError {
        DatabaseError::Other(format!(
            "{operation} is not supported on the sharded table {:?} of a clustered database",
            self.table
        ))
    }

    fn reject_duplicate(&self, operation: &str) -> Result<(), DatabaseError> {
        if self.table.is_duplicate() { Err(self.unsupported(operation)) } else { Ok(()) }
    }

    fn encode<T: Table>(key: &T::Key) -> Vec<u8> {
        key.clone().encode().into_vec()
    }

    fn decode<T: Table>(key: &[u8], value: &[u8]) -> Result<TableRow<T>, DatabaseError> {
        Ok((T::Key::decode(key)?, T::Value::decompress(value)?))
    }

    fn owns(&self, shard: Shard) -> bool {
        self.snapshot.node().cluster().owns(shard)
    }

    /// `DbCursorRO::first`.
    pub fn first<T: Table>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        self.reject_duplicate("first")?;
        let found = self.find(inner, &self.table.first_key(), false)?;
        Ok(self.settle::<T>(found))
    }

    /// `DbCursorRO::seek`.
    pub fn seek<T: Table>(&mut self, inner: &mut Cursor<K, T>, key: T::Key) -> PairResult<T> {
        self.reject_duplicate("seek")?;
        let start = Self::encode::<T>(&key);
        self.snapshot.node().holds(self.table, &start, &[])?;
        let found = self.find(inner, &start, false)?;
        Ok(self.settle::<T>(found))
    }

    /// Moves to the found entry, or to [`CursorPosition::Missed`] when there is none.
    fn settle<T: Table>(&mut self, found: Option<FoundEntry<T>>) -> Option<TableRow<T>> {
        let (row, position) = match found {
            Some((row, position)) => (Some(row), position),
            None => (None, CursorPosition::Missed),
        };
        self.position = position;
        row
    }

    /// `DbCursorRO::next`.
    pub fn next<T: Table>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        self.reject_duplicate("next")?;
        if let CursorPosition::Remote { key, value, rest, .. } = &mut self.position
            && let Some((next_key, next_value)) = rest.pop_front()
        {
            let row = Self::decode::<T>(&next_key, &next_value)?;
            *key = next_key;
            *value = next_value;
            return Ok(Some(row));
        }
        let position = mem::replace(&mut self.position, CursorPosition::Missed);
        let found = match &position {
            CursorPosition::Unset => return self.first(inner),
            // `LocalSplit` is a duplicate position, and duplicate-key tables reject `next`.
            CursorPosition::Missed
            | CursorPosition::PastEnd { .. }
            | CursorPosition::LocalSplit { .. } => {
                self.position = position;
                return Ok(None);
            }
            CursorPosition::Local { key } => {
                let shard = Shard(key[0]);
                if self.owns(shard) {
                    match inner.next()? {
                        Some((found_key, value)) => {
                            let encoded = Self::encode::<T>(&found_key);
                            if encoded[0] == shard.0 {
                                Some(((found_key, value), CursorPosition::Local { key: encoded }))
                            } else {
                                self.find_after_shard(inner, shard)?
                            }
                        }
                        None => self.find_after_shard(inner, shard)?,
                    }
                } else {
                    // A replicated key in a shard read from owners.
                    self.find(inner, key, true)?
                }
            }
            CursorPosition::Remote { key, complete, .. } => {
                let in_shard = if *complete { None } else { self.remote_page::<T>(key, true)? };
                match in_shard {
                    Some(found) => Some(found),
                    None => self.find_after_shard(inner, Shard(key[0]))?,
                }
            }
        };
        Ok(match found {
            Some((row, next)) => {
                self.position = next;
                Some(row)
            }
            None => {
                self.position = CursorPosition::PastEnd { last: Box::new(position) };
                None
            }
        })
    }

    /// `DbCursorRO::current`.
    pub fn current<T: Table>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        match &self.position {
            CursorPosition::Local { .. } => inner.current(),
            CursorPosition::Remote { key, value, .. }
            | CursorPosition::LocalSplit { key, value } => Self::decode::<T>(key, value).map(Some),
            CursorPosition::PastEnd { last } => match last.as_ref() {
                // Later shards may have moved the MDBX cursor since.
                CursorPosition::Local { key } => inner.seek_exact(T::Key::decode(key)?),
                CursorPosition::Remote { key, value, .. }
                | CursorPosition::LocalSplit { key, value } => {
                    Self::decode::<T>(key, value).map(Some)
                }
                _ => Ok(None),
            },
            CursorPosition::Unset | CursorPosition::Missed => Ok(None),
        }
    }

    /// `DbCursorRO::seek_exact`; for a duplicate-key table, the key's first duplicate.
    pub fn seek_exact<T: Table>(&mut self, inner: &mut Cursor<K, T>, key: T::Key) -> PairResult<T> {
        let encoded = Self::encode::<T>(&key);
        if self.is_split(&encoded) {
            let deadline = Instant::now() + ReadSnapshot::SPLIT_READ_BUDGET;
            let first = self.table.first_subkey();
            let found = self.find_split::<T>(&encoded, &first, false, deadline)?;
            return Ok(self.settle::<T>(found));
        }
        if self.snapshot.node().holds_by_key(self.table, &encoded)? {
            let row = inner.seek_exact(key)?;
            self.position = if row.is_some() || self.table.is_duplicate() {
                CursorPosition::Local { key: encoded }
            } else {
                CursorPosition::Missed
            };
            return Ok(row);
        }
        let found = if self.table.is_duplicate() {
            self.remote_duplicates::<T>(encoded, &self.table.first_subkey(), false, None)?
        } else {
            match self.snapshot.get(self.table, &encoded)? {
                Some(value) => {
                    let row = Self::decode::<T>(&encoded, &value)?;
                    let rest = VecDeque::new();
                    Some((
                        row,
                        CursorPosition::Remote { key: encoded, value, rest, complete: false },
                    ))
                }
                None => None,
            }
        };
        Ok(self.settle::<T>(found))
    }

    /// `DbDupCursorRO::seek_by_key_subkey`.
    pub fn seek_by_key_subkey<T: DupSort>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: T::Key,
        subkey: T::SubKey,
    ) -> ValueOnlyResult<T> {
        let encoded = Self::encode::<T>(&key);
        if self.is_split(&encoded) {
            let deadline = Instant::now() + ReadSnapshot::SPLIT_READ_BUDGET;
            let subkey = subkey.encode().into_vec();
            let found = self.find_split::<T>(&encoded, &subkey, false, deadline)?;
            return Ok(self.settle::<T>(found).map(|(_, value)| value));
        }
        if self.snapshot.node().holds_by_key(self.table, &encoded)? {
            self.position = CursorPosition::Local { key: encoded };
            return inner.seek_by_key_subkey(key, subkey);
        }
        let subkey = subkey.encode().into_vec();
        let found = self.remote_duplicates::<T>(encoded, &subkey, false, None)?;
        Ok(self.settle::<T>(found).map(|(_, value)| value))
    }

    /// `DbDupCursorRO::next_dup`. For a split contract the next duplicate may be in a later range,
    /// and the search for it is one split read with its own deadline.
    pub fn next_dup<T: DupSort>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        if let CursorPosition::Remote { key, value, rest, .. } = &mut self.position
            && let Some((next_key, next_value)) = rest.pop_front()
        {
            let row = Self::decode::<T>(&next_key, &next_value)?;
            *key = next_key;
            *value = next_value;
            return Ok(Some(row));
        }
        let mut position = mem::replace(&mut self.position, CursorPosition::Missed);
        let mut unsplit_key_ended = false;
        let found = match &position {
            CursorPosition::Local { .. } => {
                self.position = position;
                return inner.next_dup();
            }
            CursorPosition::LocalSplit { key, value } => {
                let deadline = Instant::now() + ReadSnapshot::SPLIT_READ_BUDGET;
                self.next_local_split::<T>(key, value, deadline)?
            }
            CursorPosition::Remote { key, value, complete, .. } => {
                let subkey = self.subkey_of(value)?;
                let deadline =
                    self.is_split(key).then(|| Instant::now() + ReadSnapshot::SPLIT_READ_BUDGET);
                let in_page = if *complete {
                    None
                } else {
                    self.remote_duplicates::<T>(key.clone(), subkey, true, deadline)?
                };
                match (in_page, deadline) {
                    (Some(found), _) => Some(found),
                    (None, Some(deadline)) => {
                        self.find_split_after_range::<T>(key, Shard(subkey[0]), deadline)?
                    }
                    (None, None) => {
                        unsplit_key_ended = true;
                        None
                    }
                }
            }
            CursorPosition::Unset | CursorPosition::Missed | CursorPosition::PastEnd { .. } => {
                self.position = position;
                return Ok(None);
            }
        };
        Ok(match found {
            Some((row, next)) => {
                self.position = next;
                Some(row)
            }
            // The last remote duplicate stays current, as in an exhausted MDBX duplicate walk.
            None if unsplit_key_ended => {
                if let CursorPosition::Remote { complete, .. } = &mut position {
                    *complete = true;
                }
                self.position = position;
                None
            }
            None => {
                self.position = CursorPosition::PastEnd { last: Box::new(position) };
                None
            }
        })
    }

    fn is_split(&self, key: &[u8]) -> bool {
        self.snapshot.node().cluster().is_split(self.table, key)
    }

    fn subkey_of<'a>(&self, value: &'a [u8]) -> Result<&'a [u8], DatabaseError> {
        self.table.subkey_of(value).ok_or_else(|| {
            DatabaseError::Other(format!("{:?} value shorter than its subkey", self.table))
        })
    }

    /// Where a duplicate with encoded `subkey` of a split contract lives.
    fn split_placement(&self, subkey: &[u8]) -> Option<KeyPlacement> {
        self.table.subkey_layout().and_then(|layout| layout.placement(subkey))
    }

    fn split_cursor(&mut self) -> Result<&mut SplitCursor<K>, DatabaseError> {
        let table = self.table;
        self.split.as_mut().ok_or_else(|| {
            DatabaseError::Other(format!("no split cursor to read a split contract of {table:?}"))
        })
    }

    /// The duplicate after the local duplicate `value` of split contract `key`: the next local
    /// duplicate while it stays in the same range, otherwise a search of the following ranges.
    fn next_local_split<T: Table>(
        &mut self,
        key: &[u8],
        value: &[u8],
        deadline: Instant,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let subkey = self.subkey_of(value)?;
        let range = match self.split_placement(subkey) {
            Some(KeyPlacement::Shard(range)) => range,
            _ => return self.find_split::<T>(key, subkey, true, deadline),
        };
        if let Some(next) = self.split_cursor()?.next_dup()?
            && self.split_placement(self.subkey_of(&next)?) == Some(KeyPlacement::Shard(range))
        {
            let row = Self::decode::<T>(key, &next)?;
            return Ok(Some((row, CursorPosition::LocalSplit { key: key.to_vec(), value: next })));
        }
        self.find_split_after_range::<T>(key, range, deadline)
    }

    /// The first duplicate of split contract `key` with subkey `>= start` (`> start` when
    /// `after`), from `start`'s range on, with every remote read finished by `deadline`.
    fn find_split<T: Table>(
        &mut self,
        key: &[u8],
        start: &[u8],
        after: bool,
        deadline: Instant,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let range = Shard(*start.first().ok_or_else(|| self.unsupported("an empty subkey"))?);
        let bound = Some((start, after));
        if let Some(found) = self.find_split_in_range::<T>(key, range, bound, deadline)? {
            return Ok(Some(found));
        }
        self.find_split_after_range::<T>(key, range, deadline)
    }

    /// The first duplicate of split contract `key` in the first non-empty range after `range`,
    /// with every remote read finished by `deadline`.
    fn find_split_after_range<T: Table>(
        &mut self,
        key: &[u8],
        range: Shard,
        deadline: Instant,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        for next in (range.0..=u8::MAX).skip(1) {
            if let Some(found) = self.find_split_in_range::<T>(key, Shard(next), None, deadline)? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    /// The first duplicate of split contract `key` in `range` at or after the bound: replicated
    /// storage-trie paths first (local), then the range (local when owned, otherwise from an
    /// owner by `deadline`, whose page ends at the range boundary).
    fn find_split_in_range<T: Table>(
        &mut self,
        key: &[u8],
        range: Shard,
        bound: Option<(&[u8], bool)>,
        deadline: Instant,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let layout = self.table.subkey_layout().ok_or_else(|| self.unsupported("a split key"))?;
        let beyond = |subkey: &[u8]| match bound {
            None => true,
            Some((start, true)) => subkey > start,
            Some((start, false)) => subkey >= start,
        };
        for candidate in layout.replicated_keys(range) {
            if beyond(&candidate)
                && let Some(value) = self.split_cursor()?.seek_by_key_subkey(key, &candidate)?
                && value.starts_with(&candidate)
            {
                let row = Self::decode::<T>(key, &value)?;
                return Ok(Some((row, CursorPosition::LocalSplit { key: key.to_vec(), value })));
            }
        }

        let floor = layout.shard_floor(range);
        let (from, after) = match bound {
            Some((start, after)) if start >= floor.as_slice() => (start.to_vec(), after),
            _ => (floor, false),
        };
        if !self.owns(range) {
            return self.remote_duplicates::<T>(key.to_vec(), &from, after, Some(deadline));
        }
        let split = self.split_cursor()?;
        let mut value = split.seek_by_key_subkey(key, &from)?;
        if after && value.as_ref().is_some_and(|value| value.starts_with(&from)) {
            value = split.next_dup()?;
        }
        let Some(value) = value else { return Ok(None) };
        if self.split_placement(self.subkey_of(&value)?) != Some(KeyPlacement::Shard(range)) {
            return Ok(None);
        }
        let row = Self::decode::<T>(key, &value)?;
        Ok(Some((row, CursorPosition::LocalSplit { key: key.to_vec(), value })))
    }

    /// The first entry with key `>= start` (`> start` when `after`), from `start`'s shard on.
    fn find<T: Table>(
        &self,
        inner: &mut Cursor<K, T>,
        start: &[u8],
        after: bool,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let shard = Shard(start[0]);
        if let Some(found) = self.find_in_shard(inner, shard, Some((start, after)))? {
            return Ok(Some(found));
        }
        self.find_after_shard(inner, shard)
    }

    /// The first entry of the first non-empty shard after `shard`.
    fn find_after_shard<T: Table>(
        &self,
        inner: &mut Cursor<K, T>,
        shard: Shard,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        for next in (shard.0..=u8::MAX).skip(1) {
            if let Some(found) = self.find_in_shard(inner, Shard(next), None)? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    /// The first entry of `shard` at or after the bound: replicated keys first (local), then
    /// sharded keys (local when owned, otherwise from an owner).
    fn find_in_shard<T: Table>(
        &self,
        inner: &mut Cursor<K, T>,
        shard: Shard,
        bound: Option<(&[u8], bool)>,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let beyond = |key: &[u8]| match bound {
            None => true,
            Some((start, true)) => key > start,
            Some((start, false)) => key >= start,
        };
        for candidate in self.table.replicated_keys(shard) {
            if beyond(&candidate)
                && let Some(row) = inner.seek_exact(T::Key::decode(&candidate)?)?
            {
                return Ok(Some((row, CursorPosition::Local { key: candidate })));
            }
        }

        let floor = self.table.shard_floor(shard);
        let (from, after) = match bound {
            Some((start, after)) if start >= floor.as_slice() => (start.to_vec(), after),
            _ => (floor, false),
        };
        if !self.owns(shard) {
            return self.remote_page::<T>(&from, after);
        }
        let mut row = inner.seek(T::Key::decode(&from)?)?;
        if after && row.as_ref().is_some_and(|(key, _)| Self::encode::<T>(key) == from) {
            row = inner.next()?;
        }
        Ok(row.and_then(|(key, value)| {
            let encoded = Self::encode::<T>(&key);
            (encoded[0] == shard.0)
                .then_some(((key, value), CursorPosition::Local { key: encoded }))
        }))
    }

    /// The first entry of an owner's `SeekGe` page from `from` (skipping `from` when `after`).
    fn remote_page<T: Table>(
        &self,
        from: &[u8],
        after: bool,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let page = self.snapshot.seek_ge(self.table, from)?;
        let mut rest = VecDeque::from(page.entries);
        if after && rest.front().is_some_and(|(key, _)| key.as_slice() == from) {
            rest.pop_front();
        }
        self.first_of_page::<T>(rest, page.end_of_shard)
    }

    /// The first duplicate of an owner's `SeekDupGe` page of `key` from `subkey` (skipping
    /// `subkey` when `after`), read by `deadline` when there is one.
    fn remote_duplicates<T: Table>(
        &self,
        key: Vec<u8>,
        subkey: &[u8],
        after: bool,
        deadline: Option<Instant>,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let limit = ReadSnapshot::PAGE_LEN;
        let page = self.snapshot.seek_dup_ge(self.table, &key, subkey, limit, deadline)?;
        let subkey_len = self.table.subkey_len();
        let mut rest: VecDeque<_> =
            page.values.into_iter().map(|value| (key.clone(), value)).collect();
        if after && rest.front().is_some_and(|(_, value)| value.get(..subkey_len) == Some(subkey)) {
            rest.pop_front();
        }
        self.first_of_page::<T>(rest, page.end_of_key)
    }

    fn first_of_page<T: Table>(
        &self,
        mut rest: VecDeque<(Vec<u8>, Vec<u8>)>,
        complete: bool,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        match rest.pop_front() {
            Some((key, value)) => {
                let row = Self::decode::<T>(&key, &value)?;
                Ok(Some((row, CursorPosition::Remote { key, value, rest, complete })))
            }
            None if complete => Ok(None),
            None => Err(DatabaseError::Other(format!(
                "an owner returned an empty, incomplete {:?} page",
                self.table
            ))),
        }
    }
}
