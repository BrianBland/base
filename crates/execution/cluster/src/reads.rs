//! Read-only cursor routing over one sharded table: local MDBX for held keys, owners for the
//! rest, with the cursor position kept here rather than in MDBX.

use std::{collections::VecDeque, mem, sync::Arc};

use reth_db::{
    DatabaseError,
    common::{PairResult, ValueOnlyResult},
    cursor::{DbCursorRO, DbDupCursorRO},
    mdbx::{TransactionKind, cursor::Cursor},
    table::{Decode, Decompress, DupSort, Encode, IntoVec, Table, TableRow},
};

use crate::{ReadSnapshot, Shard, ShardedTable};

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
    /// At an entry read from an owner, followed by the rest of its page.
    Remote {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded value.
        value: Vec<u8>,
        /// Entries of the page after this one, ascending.
        rest: VecDeque<(Vec<u8>, Vec<u8>)>,
        /// No entry of the shard (or duplicate of the key) follows the page.
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
/// [Read rule](crate#read-rule). The MDBX cursor of the same table is passed to every call.
#[derive(Debug)]
pub struct ShardedReads {
    snapshot: Arc<ReadSnapshot>,
    table: ShardedTable,
    position: CursorPosition,
}

impl ShardedReads {
    /// A fresh cursor over `table` reading at `snapshot`.
    pub const fn new(snapshot: Arc<ReadSnapshot>, table: ShardedTable) -> Self {
        Self { snapshot, table, position: CursorPosition::Unset }
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
    pub fn first<K: TransactionKind, T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
    ) -> PairResult<T> {
        self.reject_duplicate("first")?;
        let found = self.find(inner, &self.table.first_key(), false)?;
        Ok(self.settle::<T>(found))
    }

    /// `DbCursorRO::seek`.
    pub fn seek<K: TransactionKind, T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: T::Key,
    ) -> PairResult<T> {
        self.reject_duplicate("seek")?;
        let start = Self::encode::<T>(&key);
        self.snapshot.node().holds(self.table, &start)?;
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
    pub fn next<K: TransactionKind, T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
    ) -> PairResult<T> {
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
            CursorPosition::Missed | CursorPosition::PastEnd { .. } => {
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
    pub fn current<K: TransactionKind, T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
    ) -> PairResult<T> {
        match &self.position {
            CursorPosition::Local { .. } => inner.current(),
            CursorPosition::Remote { key, value, .. } => Self::decode::<T>(key, value).map(Some),
            CursorPosition::PastEnd { last } => match last.as_ref() {
                // Later shards may have moved the MDBX cursor since.
                CursorPosition::Local { key } => inner.seek_exact(T::Key::decode(key)?),
                CursorPosition::Remote { key, value, .. } => {
                    Self::decode::<T>(key, value).map(Some)
                }
                _ => Ok(None),
            },
            CursorPosition::Unset | CursorPosition::Missed => Ok(None),
        }
    }

    /// `DbCursorRO::seek_exact`; for a duplicate-key table, the key's first duplicate.
    pub fn seek_exact<K: TransactionKind, T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: T::Key,
    ) -> PairResult<T> {
        let encoded = Self::encode::<T>(&key);
        if self.snapshot.node().holds(self.table, &encoded)? {
            let row = inner.seek_exact(key)?;
            self.position = if row.is_some() || self.table.is_duplicate() {
                CursorPosition::Local { key: encoded }
            } else {
                CursorPosition::Missed
            };
            return Ok(row);
        }
        let found = if self.table.is_duplicate() {
            self.remote_duplicates::<T>(encoded, &self.table.first_subkey(), false)?
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
    pub fn seek_by_key_subkey<K: TransactionKind, T: DupSort>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: T::Key,
        subkey: T::SubKey,
    ) -> ValueOnlyResult<T> {
        let encoded = Self::encode::<T>(&key);
        if self.snapshot.node().holds(self.table, &encoded)? {
            self.position = CursorPosition::Local { key: encoded };
            return inner.seek_by_key_subkey(key, subkey);
        }
        let found = self.remote_duplicates::<T>(encoded, &subkey.encode().into_vec(), false)?;
        Ok(self.settle::<T>(found).map(|(_, value)| value))
    }

    /// `DbDupCursorRO::next_dup`.
    pub fn next_dup<K: TransactionKind, T: DupSort>(
        &mut self,
        inner: &mut Cursor<K, T>,
    ) -> PairResult<T> {
        let subkey_len = self.table.subkey_len();
        let (key, subkey) = match &mut self.position {
            CursorPosition::Local { .. } => return inner.next_dup(),
            CursorPosition::Remote { key, value, rest, complete } => {
                if let Some((next_key, next_value)) = rest.pop_front() {
                    let row = Self::decode::<T>(&next_key, &next_value)?;
                    *key = next_key;
                    *value = next_value;
                    return Ok(Some(row));
                }
                if *complete {
                    return Ok(None);
                }
                let subkey = value.get(..subkey_len).ok_or_else(|| {
                    DatabaseError::Other(format!("{:?} value shorter than its subkey", self.table))
                })?;
                (key.clone(), subkey.to_vec())
            }
            CursorPosition::Unset | CursorPosition::Missed | CursorPosition::PastEnd { .. } => {
                return Ok(None);
            }
        };
        match self.remote_duplicates::<T>(key, &subkey, true)? {
            Some((row, position)) => {
                self.position = position;
                Ok(Some(row))
            }
            None => {
                if let CursorPosition::Remote { complete, .. } = &mut self.position {
                    *complete = true;
                }
                Ok(None)
            }
        }
    }

    /// The first entry with key `>= start` (`> start` when `after`), from `start`'s shard on.
    fn find<K: TransactionKind, T: Table>(
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
    fn find_after_shard<K: TransactionKind, T: Table>(
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
    fn find_in_shard<K: TransactionKind, T: Table>(
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
    /// `subkey` when `after`).
    fn remote_duplicates<T: Table>(
        &self,
        key: Vec<u8>,
        subkey: &[u8],
        after: bool,
    ) -> Result<Option<FoundEntry<T>>, DatabaseError> {
        let page = self.snapshot.seek_dup_ge(self.table, &key, subkey, ReadSnapshot::PAGE_LEN)?;
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
