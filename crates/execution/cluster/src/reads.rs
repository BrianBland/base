//! Cursor routing over one sharded table, following the spec's [Read rule](crate#read-rule):
//! local reads that see only the rows this node holds, local reads as MDBX has them, and exact
//! reads of other rows from their owners. The cursor position is kept here rather than in MDBX.

use std::{mem, sync::Arc};

use alloy_primitives::B256;
use alloy_trie::TrieMask;
use reth_db::{
    DatabaseError,
    common::{PairResult, ValueOnlyResult},
    cursor::{DbCursorRO, DbDupCursorRO},
    mdbx::{TransactionKind, cursor::Cursor},
    table::{Compress, Decode, Decompress, DupSort, Encode, IntoVec, Table, TableRow},
};
use reth_trie_common::BranchNodeCompact;

use crate::{
    ClusterNode, KeyPlacement, LocalReads, ReadSnapshot, Shard, ShardedTable, SplitCursor,
};

/// Where a [`ShardedReads`] cursor is.
#[derive(Debug)]
pub enum CursorPosition {
    /// Not positioned yet: `next` starts at the first entry.
    Unset,
    /// At a visible row of local MDBX; the MDBX cursor is positioned there.
    Local {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded subkey, for a duplicate of a split contract.
        subkey: Option<Vec<u8>>,
    },
    /// At a visible duplicate of a split contract found by the [`SplitCursor`], which is
    /// positioned there.
    LocalSplit {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded value.
        value: Vec<u8>,
    },
    /// At a row read from an owner; moving on from it is unsupported.
    Remote {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded value.
        value: Vec<u8>,
    },
    /// A positioning call found nothing: `current` and `next` return nothing.
    Missed,
    /// `next` or `next_dup` ran past the last visible entry; `current` still returns that entry.
    PastEnd {
        /// The position of the last visible entry.
        last: Box<Self>,
    },
    /// A write moved the MDBX cursor: reads continue from wherever MDBX left it.
    Detached,
}

/// How a cursor operation reaches its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// Local MDBX as it is: the hashed tables and `Bytecodes` in a read-write transaction.
    Raw,
    /// Local MDBX, showing only the rows this node holds: the trie tables always, the hashed
    /// tables inside a [`LocalReads`] scope.
    Filtered,
    /// Held rows from local MDBX, other rows as exact reads from their owners; ordered reads only
    /// inside owned shards. The hashed tables and `Bytecodes` of a read-only transaction.
    Remote,
}

/// Where a row is for this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowPlace {
    /// Held: in a shard (or range) this node owns, or replicated.
    Held,
    /// In this shard (or range of a split contract), which this node does not own.
    Foreign(Shard),
}

/// A point read of a non-duplicate table, decided by the row's key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointRead {
    /// Read local MDBX.
    Local,
    /// The row is invisible here: absent.
    Invisible,
    /// The encoded value an owner returned.
    Remote(Option<Vec<u8>>),
}

/// Routes the reads of one cursor over a sharded table. The MDBX cursor of the same table is
/// passed to every call; duplicates of split contracts found by `seek_exact` past a range this
/// node does not own are read through this type's own [`SplitCursor`].
#[derive(Debug)]
pub struct ShardedReads<K: TransactionKind> {
    node: Arc<ClusterNode>,
    table: ShardedTable,
    /// The snapshot of a read-only transaction; `None` in a read-write one.
    snapshot: Option<Arc<ReadSnapshot>>,
    position: CursorPosition,
    split: Option<SplitCursor<K>>,
}

impl<K: TransactionKind> ShardedReads<K> {
    /// A fresh cursor over `table` of `node`, reading at `snapshot` in a read-only transaction.
    /// `split` is required to read a split contract's duplicates by subkey.
    pub const fn new(
        node: Arc<ClusterNode>,
        table: ShardedTable,
        snapshot: Option<Arc<ReadSnapshot>>,
        split: Option<SplitCursor<K>>,
    ) -> Self {
        Self { node, table, snapshot, position: CursorPosition::Unset, split }
    }

    /// This node.
    pub const fn node(&self) -> &Arc<ClusterNode> {
        &self.node
    }

    /// The table read.
    pub const fn table(&self) -> ShardedTable {
        self.table
    }

    /// How the next operation reaches its rows.
    pub fn mode(&self) -> ReadMode {
        match (self.table, &self.snapshot) {
            (ShardedTable::AccountsTrie | ShardedTable::StoragesTrie, _) => ReadMode::Filtered,
            (_, None) => ReadMode::Raw,
            (ShardedTable::HashedAccounts | ShardedTable::HashedStorages, Some(_))
                if LocalReads::is_active() =>
            {
                ReadMode::Filtered
            }
            (_, Some(_)) => ReadMode::Remote,
        }
    }

    /// Marks that a write moved the MDBX cursor.
    pub fn detach(&mut self) {
        self.position = CursorPosition::Detached;
    }

    /// The error returned by an operation that is not supported on a sharded table.
    pub fn unsupported(&self, operation: &str) -> DatabaseError {
        DatabaseError::Other(format!(
            "{operation} is not supported on the sharded table {:?} of a clustered database",
            self.table
        ))
    }

    fn not_held(&self, shard: Shard) -> DatabaseError {
        self.unsupported(&format!(
            "an ordered read reaching shard {:#04x}, which this node does not hold,",
            shard.0
        ))
    }

    fn storage_not_held(&self, address: &[u8]) -> DatabaseError {
        self.unsupported(&format!(
            "walking the storage of account {}, which this node does not hold and which has \
             storage,",
            B256::from_slice(address)
        ))
    }

    fn reject_duplicate(&self, operation: &str) -> Result<(), DatabaseError> {
        if self.table.is_duplicate() { Err(self.unsupported(operation)) } else { Ok(()) }
    }

    fn snapshot(&self) -> Result<&ReadSnapshot, DatabaseError> {
        self.snapshot.as_deref().ok_or_else(|| self.unsupported("a remote read in a write"))
    }

    fn encode<T: Table>(key: &T::Key) -> Vec<u8> {
        key.clone().encode().into_vec()
    }

    fn decode<T: Table>(key: &[u8], value: &[u8]) -> Result<TableRow<T>, DatabaseError> {
        Ok((T::Key::decode(key)?, T::Value::decompress(value)?))
    }

    fn owns(&self, shard: Shard) -> bool {
        self.node.cluster().owns(shard)
    }

    fn is_split(&self, key: &[u8]) -> bool {
        self.node.cluster().is_split(self.table, key)
    }

    fn place(&self, placement: Option<KeyPlacement>) -> Result<RowPlace, DatabaseError> {
        match placement {
            Some(KeyPlacement::Replicated) => Ok(RowPlace::Held),
            Some(KeyPlacement::Shard(shard)) if self.owns(shard) => Ok(RowPlace::Held),
            Some(KeyPlacement::Shard(shard)) => Ok(RowPlace::Foreign(shard)),
            None => Err(DatabaseError::Other(format!("invalid {:?} key or subkey", self.table))),
        }
    }

    /// Where the row with encoded `key` is, placed by key alone.
    fn place_key(&self, key: &[u8]) -> Result<RowPlace, DatabaseError> {
        self.place(self.table.placement(key))
    }

    /// Where the duplicate of a split contract with encoded `subkey` is.
    fn place_subkey(&self, subkey: &[u8]) -> Result<RowPlace, DatabaseError> {
        self.place(self.table.subkey_layout().and_then(|layout| layout.placement(subkey)))
    }

    /// The encoded subkey a duplicate value starts with.
    fn subkey_of<'a>(&self, value: &'a [u8]) -> Result<&'a [u8], DatabaseError> {
        self.table.subkey_of(value).ok_or_else(|| {
            DatabaseError::Other(format!("{:?} value shorter than its subkey", self.table))
        })
    }

    fn value_subkey<T: Table>(&self, value: &T::Value) -> Result<Vec<u8>, DatabaseError> {
        let mut encoded = Vec::new();
        value.compress_to_buf(&mut encoded);
        Ok(self.subkey_of(&encoded)?.to_vec())
    }

    /// The first encoded subkey of the range after `range`, or `None` after the last range.
    fn next_range_start(&self, range: Shard) -> Option<Vec<u8>> {
        let layout = self.table.subkey_layout()?;
        range.0.checked_add(1).map(|next| layout.shard_start(Shard(next)))
    }

    fn require_owned(&self, from: Shard, to: Shard) -> Result<(), DatabaseError> {
        let missing = (from.0..=to.0).map(Shard).find(|shard| !self.owns(*shard));
        missing.map_or(Ok(()), |shard| Err(self.not_held(shard)))
    }

    /// Whether a row read from MDBX is visible here.
    fn visible<T: Table>(&self, key: &T::Key, value: &T::Value) -> Result<bool, DatabaseError> {
        let key = Self::encode::<T>(key);
        let place = if self.is_split(&key) {
            self.place_subkey(&self.value_subkey::<T>(value)?)?
        } else {
            self.place_key(&key)?
        };
        Ok(place == RowPlace::Held)
    }

    /// `value` of the visible row at encoded `key` as this node shows it: a replicated
    /// one-nibble account-trie node claims stored children (tree-mask bits) only in shards this
    /// node owns, as in a database holding only the visible rows. A stored child that a tree bit
    /// claims but the filter hides breaks reth's trie walker: seeking it, the walker takes the
    /// next visible node, under the next nibble, for a descendant and skips that whole nibble,
    /// hashing every account under it from the leaves. Hashes and the other masks are kept, so
    /// a hidden child is still skipped by its hash.
    fn shown<T: Table>(&self, key: &[u8], value: T::Value) -> Result<T::Value, DatabaseError> {
        if self.table != ShardedTable::AccountsTrie
            || ShardedTable::packed_path_nibbles(key) != Some(1)
        {
            return Ok(value);
        }
        let mut encoded = Vec::new();
        value.compress_to_buf(&mut encoded);
        let mut node = BranchNodeCompact::decompress(&encoded)?;
        let nibble = key[0] >> 4;
        let owned_children = (0..16u8)
            .filter(|child| self.owns(Shard((nibble << 4) | child)))
            .fold(TrieMask::default(), |mask, child| mask | TrieMask::from_nibble(child));
        node.tree_mask &= owned_children;
        encoded.clear();
        node.compress_to_buf(&mut encoded);
        Ok(T::Value::decompress(&encoded)?)
    }

    /// [`Self::shown`] of a visible row read at encoded `key`.
    fn shown_row<T: Table>(&self, key: &[u8], row: Option<TableRow<T>>) -> PairResult<T> {
        row.map(|(key_value, value)| Ok((key_value, self.shown::<T>(key, value)?))).transpose()
    }

    /// A point read of encoded `key` of a non-duplicate table.
    pub fn point(&self, key: &[u8]) -> Result<PointRead, DatabaseError> {
        let mode = self.mode();
        if mode == ReadMode::Raw || self.place_key(key)? == RowPlace::Held {
            return Ok(PointRead::Local);
        }
        match mode {
            ReadMode::Filtered => Ok(PointRead::Invisible),
            ReadMode::Raw | ReadMode::Remote => self.remote_value(key).map(PointRead::Remote),
        }
    }

    /// The encoded value of `key` of a non-duplicate table from an owner.
    fn remote_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        match self.table {
            ShardedTable::HashedAccounts => self.snapshot()?.account(B256::from_slice(key)),
            ShardedTable::Bytecodes => self.snapshot()?.bytecode(key),
            ShardedTable::HashedStorages
            | ShardedTable::AccountsTrie
            | ShardedTable::StoragesTrie => Err(self.unsupported("a remote read")),
        }
    }

    /// Moves to a row read from an owner, or to [`CursorPosition::Missed`] when it is absent.
    fn settle_remote<T: Table>(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> PairResult<T> {
        let Some(value) = value else {
            self.position = CursorPosition::Missed;
            return Ok(None);
        };
        let row = Self::decode::<T>(&key, &value)?;
        self.position = CursorPosition::Remote { key, value };
        Ok(Some(row))
    }

    /// Moves to a row of local MDBX read with MDBX's own semantics.
    fn settle_local<R>(&mut self, key: Vec<u8>, row: Option<R>) -> Option<R> {
        self.position = match row {
            Some(_) => CursorPosition::Local { key, subkey: None },
            None => CursorPosition::Missed,
        };
        row
    }

    /// `DbCursorRO::first`.
    pub fn first<T: Table>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        if self.mode() == ReadMode::Raw {
            return inner.first();
        }
        self.reject_duplicate("first")?;
        let row = inner.first()?;
        self.walk(inner, row, Shard(0))
    }

    /// `DbCursorRO::seek`.
    pub fn seek<T: Table>(&mut self, inner: &mut Cursor<K, T>, key: T::Key) -> PairResult<T> {
        if self.mode() == ReadMode::Raw {
            return inner.seek(key);
        }
        self.reject_duplicate("seek")?;
        let start = Self::encode::<T>(&key);
        self.place_key(&start)?;
        let row = inner.seek(key)?;
        self.walk(inner, row, Shard(start[0]))
    }

    /// The first visible row from `row` on, for a walk that started in shard `start`. A filtered
    /// walk seeks past every shard this node does not own; a remote-mode walk fails on reaching
    /// one.
    fn walk<T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
        mut row: Option<TableRow<T>>,
        start: Shard,
    ) -> PairResult<T> {
        let remote = self.mode() == ReadMode::Remote;
        loop {
            let Some((key, value)) = row else {
                if remote {
                    self.require_owned(start, Shard(u8::MAX))?;
                }
                self.position = CursorPosition::Missed;
                return Ok(None);
            };
            let encoded = Self::encode::<T>(&key);
            match self.place_key(&encoded)? {
                RowPlace::Held => {
                    if remote {
                        self.require_owned(start, Shard(encoded[0]))?;
                    }
                    let value = self.shown::<T>(&encoded, value)?;
                    self.position = CursorPosition::Local { key: encoded, subkey: None };
                    return Ok(Some((key, value)));
                }
                RowPlace::Foreign(shard) if remote => return Err(self.not_held(shard)),
                RowPlace::Foreign(shard) => {
                    row = match shard.0.checked_add(1) {
                        Some(next) => {
                            let start = self.table.shard_start(Shard(next));
                            inner.seek(T::Key::decode(&start)?)?
                        }
                        None => None,
                    };
                }
            }
        }
    }

    /// `DbCursorRO::next`.
    pub fn next<T: Table>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        if self.mode() == ReadMode::Raw {
            return inner.next();
        }
        self.reject_duplicate("next")?;
        match mem::replace(&mut self.position, CursorPosition::Missed) {
            CursorPosition::Unset => self.first(inner),
            CursorPosition::Local { key, subkey } => {
                let row = inner.next()?;
                let found = self.walk(inner, row, Shard(key[0]))?;
                if found.is_none() {
                    // The walk may have left MDBX past invisible rows; `current` needs the last
                    // visible one.
                    inner.seek_exact(T::Key::decode(&key)?)?;
                    let last = Box::new(CursorPosition::Local { key, subkey });
                    self.position = CursorPosition::PastEnd { last };
                }
                Ok(found)
            }
            CursorPosition::Detached => {
                let row = inner.next()?;
                self.walk(inner, row, Shard(0))
            }
            position @ CursorPosition::Remote { .. } => {
                self.position = position;
                Err(self.unsupported("next after a read from an owner"))
            }
            position => {
                self.position = position;
                Ok(None)
            }
        }
    }

    /// `DbCursorRO::current`.
    pub fn current<T: Table>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        if self.mode() == ReadMode::Raw {
            return inner.current();
        }
        let position = match &self.position {
            CursorPosition::PastEnd { last } => last.as_ref(),
            position => position,
        };
        match position {
            CursorPosition::Local { key, .. } => self.shown_row::<T>(key, inner.current()?),
            CursorPosition::LocalSplit { key, value } | CursorPosition::Remote { key, value } => {
                Self::decode::<T>(key, value).map(Some)
            }
            CursorPosition::Detached => match inner.current()? {
                Some((key, value)) if self.visible::<T>(&key, &value)? => {
                    let value = self.shown::<T>(&Self::encode::<T>(&key), value)?;
                    Ok(Some((key, value)))
                }
                _ => Ok(None),
            },
            CursorPosition::Unset | CursorPosition::Missed | CursorPosition::PastEnd { .. } => {
                Ok(None)
            }
        }
    }

    /// `DbCursorRO::seek_exact`; for a duplicate-key table, the key's first visible duplicate.
    pub fn seek_exact<T: Table>(&mut self, inner: &mut Cursor<K, T>, key: T::Key) -> PairResult<T> {
        let mode = self.mode();
        if mode == ReadMode::Raw {
            return inner.seek_exact(key);
        }
        let encoded = Self::encode::<T>(&key);
        if self.table.is_duplicate() {
            return self.seek_exact_duplicate(inner, key, encoded, mode);
        }
        match (self.place_key(&encoded)?, mode) {
            (RowPlace::Held, _) => {
                let row = self.shown_row::<T>(&encoded, inner.seek_exact(key)?)?;
                Ok(self.settle_local(encoded, row))
            }
            (RowPlace::Foreign(_), ReadMode::Filtered) => {
                self.position = CursorPosition::Missed;
                Ok(None)
            }
            (RowPlace::Foreign(_), ReadMode::Raw | ReadMode::Remote) => {
                let value = self.remote_value(&encoded)?;
                self.settle_remote::<T>(encoded, value)
            }
        }
    }

    fn seek_exact_duplicate<T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: T::Key,
        encoded: Vec<u8>,
        mode: ReadMode,
    ) -> PairResult<T> {
        let split = self.is_split(&encoded);
        if split && mode == ReadMode::Filtered {
            return self.seek_exact_split(inner, key, encoded);
        }
        if split {
            return Err(self.storage_not_held(&encoded));
        }
        match (self.place_key(&encoded)?, mode) {
            (RowPlace::Held, _) => {
                let row = inner.seek_exact(key)?;
                Ok(self.settle_local(encoded, row))
            }
            (RowPlace::Foreign(_), ReadMode::Filtered) => {
                self.position = CursorPosition::Missed;
                Ok(None)
            }
            (RowPlace::Foreign(_), ReadMode::Raw | ReadMode::Remote) => self.probe(encoded),
        }
    }

    /// The first-row probe of the storage of an account this node does not hold: nothing when
    /// the account has no storage at `P`, otherwise an unsupported-operation error.
    fn probe<R>(&mut self, address: Vec<u8>) -> Result<Option<R>, DatabaseError> {
        if self.snapshot()?.has_storage(B256::from_slice(&address))? {
            return Err(self.storage_not_held(&address));
        }
        self.position = CursorPosition::Missed;
        Ok(None)
    }

    /// The first visible duplicate of split contract `key`: the first duplicate when it is held,
    /// otherwise a search of the following ranges through the split cursor.
    fn seek_exact_split<T: Table>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: T::Key,
        encoded: Vec<u8>,
    ) -> PairResult<T> {
        let Some((found_key, value)) = inner.seek_exact(key)? else {
            self.position = CursorPosition::Missed;
            return Ok(None);
        };
        let subkey = self.value_subkey::<T>(&value)?;
        let range = match self.place_subkey(&subkey)? {
            RowPlace::Held => {
                self.position = CursorPosition::Local { key: encoded, subkey: Some(subkey) };
                return Ok(Some((found_key, value)));
            }
            RowPlace::Foreign(range) => range,
        };
        let found = match self.next_range_start(range) {
            Some(start) => self.split_from(&encoded, start)?,
            None => None,
        };
        let Some(value) = found else {
            self.position = CursorPosition::Missed;
            return Ok(None);
        };
        let row = Self::decode::<T>(&encoded, &value)?;
        self.position = CursorPosition::LocalSplit { key: encoded, value };
        Ok(Some(row))
    }

    fn split_cursor(&mut self) -> Result<&mut SplitCursor<K>, DatabaseError> {
        let table = self.table;
        self.split.as_mut().ok_or_else(|| {
            DatabaseError::Other(format!("no split cursor to read a split contract of {table:?}"))
        })
    }

    /// The encoded first visible duplicate of split contract `key` from the split cursor's
    /// `found`, seeking past every range this node does not own.
    fn split_visible(
        &mut self,
        key: &[u8],
        mut found: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        loop {
            let Some(value) = found else { return Ok(None) };
            match self.place_subkey(self.subkey_of(&value)?)? {
                RowPlace::Held => return Ok(Some(value)),
                RowPlace::Foreign(range) => {
                    found = match self.next_range_start(range) {
                        Some(start) => self.split_cursor()?.seek_by_key_subkey(key, &start)?,
                        None => None,
                    };
                }
            }
        }
    }

    fn split_from(&mut self, key: &[u8], start: Vec<u8>) -> Result<Option<Vec<u8>>, DatabaseError> {
        let found = self.split_cursor()?.seek_by_key_subkey(key, &start)?;
        self.split_visible(key, found)
    }

    /// `DbDupCursorRO::seek_by_key_subkey`.
    pub fn seek_by_key_subkey<T: DupSort>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: T::Key,
        subkey: T::SubKey,
    ) -> ValueOnlyResult<T> {
        let mode = self.mode();
        if mode == ReadMode::Raw {
            return inner.seek_by_key_subkey(key, subkey);
        }
        let encoded = Self::encode::<T>(&key);
        let encoded_subkey = subkey.clone().encode().into_vec();
        let split = self.is_split(&encoded);
        if mode == ReadMode::Filtered {
            if split {
                let found = inner.seek_by_key_subkey(key.clone(), subkey)?;
                return self.typed_split_visible(inner, &key, encoded, found);
            }
            return match self.place_key(&encoded)? {
                RowPlace::Held => {
                    let found = inner.seek_by_key_subkey(key, subkey)?;
                    Ok(self.settle_local(encoded, found))
                }
                RowPlace::Foreign(_) => {
                    self.position = CursorPosition::Missed;
                    Ok(None)
                }
            };
        }
        let probe = encoded_subkey.iter().all(|byte| *byte == 0);
        if split {
            if probe {
                return Err(self.storage_not_held(&encoded));
            }
            return match self.place_subkey(&encoded_subkey)? {
                RowPlace::Held => {
                    let found = inner.seek_by_key_subkey(key, subkey)?;
                    let exact = match found {
                        Some(value) if self.value_subkey::<T>(&value)? == encoded_subkey => {
                            Some(value)
                        }
                        _ => None,
                    };
                    self.position = match exact {
                        Some(_) => {
                            CursorPosition::Local { key: encoded, subkey: Some(encoded_subkey) }
                        }
                        None => CursorPosition::Missed,
                    };
                    Ok(exact)
                }
                RowPlace::Foreign(_) => self.remote_storage::<T>(encoded, encoded_subkey),
            };
        }
        match self.place_key(&encoded)? {
            RowPlace::Held => {
                let found = inner.seek_by_key_subkey(key, subkey)?;
                Ok(self.settle_local(encoded, found))
            }
            RowPlace::Foreign(_) if probe => self.probe(encoded),
            RowPlace::Foreign(_) => self.remote_storage::<T>(encoded, encoded_subkey),
        }
    }

    /// The storage entry of exactly `slot` of `address` from an owner.
    fn remote_storage<T: DupSort>(
        &mut self,
        address: Vec<u8>,
        slot: Vec<u8>,
    ) -> ValueOnlyResult<T> {
        let value =
            self.snapshot()?.storage(B256::from_slice(&address), B256::from_slice(&slot))?;
        Ok(self.settle_remote::<T>(address, value)?.map(|(_, value)| value))
    }

    /// The first visible duplicate of split contract `key` from the MDBX cursor's `found`,
    /// seeking past every range this node does not own.
    fn typed_split_visible<T: DupSort>(
        &mut self,
        inner: &mut Cursor<K, T>,
        key: &T::Key,
        encoded: Vec<u8>,
        mut found: Option<T::Value>,
    ) -> ValueOnlyResult<T> {
        loop {
            let Some(value) = found else {
                self.position = CursorPosition::Missed;
                return Ok(None);
            };
            let subkey = self.value_subkey::<T>(&value)?;
            match self.place_subkey(&subkey)? {
                RowPlace::Held => {
                    self.position = CursorPosition::Local { key: encoded, subkey: Some(subkey) };
                    return Ok(Some(value));
                }
                RowPlace::Foreign(range) => {
                    found = match self.next_range_start(range) {
                        Some(start) => {
                            inner.seek_by_key_subkey(key.clone(), T::SubKey::decode(&start)?)?
                        }
                        None => None,
                    };
                }
            }
        }
    }

    /// `DbDupCursorRO::next_dup`.
    pub fn next_dup<T: DupSort>(&mut self, inner: &mut Cursor<K, T>) -> PairResult<T> {
        let mode = self.mode();
        if mode == ReadMode::Raw {
            return inner.next_dup();
        }
        match mem::replace(&mut self.position, CursorPosition::Missed) {
            // Every duplicate of a key placed by key is visible: MDBX's own walk, which leaves
            // the last duplicate current at the end.
            position @ CursorPosition::Local { subkey: None, .. } => {
                self.position = position;
                inner.next_dup()
            }
            position @ (CursorPosition::Local { subkey: Some(_), .. }
            | CursorPosition::LocalSplit { .. })
                if mode == ReadMode::Remote =>
            {
                self.position = position;
                Err(self.unsupported("next_dup of a split contract outside a LocalReads scope"))
            }
            CursorPosition::Local { key, subkey: Some(subkey) } => {
                let typed_key = T::Key::decode(&key)?;
                let found = inner.next_dup()?.map(|(_, value)| value);
                let found = self.typed_split_visible(inner, &typed_key, key.clone(), found)?;
                if found.is_none() {
                    inner.seek_by_key_subkey(typed_key.clone(), T::SubKey::decode(&subkey)?)?;
                    let last = Box::new(CursorPosition::Local { key, subkey: Some(subkey) });
                    self.position = CursorPosition::PastEnd { last };
                }
                Ok(found.map(|value| (typed_key, value)))
            }
            CursorPosition::LocalSplit { key, value } => {
                let next = self.split_cursor()?.next_dup()?;
                match self.split_visible(&key, next)? {
                    Some(next) => {
                        let row = Self::decode::<T>(&key, &next)?;
                        self.position = CursorPosition::LocalSplit { key, value: next };
                        Ok(Some(row))
                    }
                    None => {
                        let last = Box::new(CursorPosition::LocalSplit { key, value });
                        self.position = CursorPosition::PastEnd { last };
                        Ok(None)
                    }
                }
            }
            CursorPosition::Detached => match inner.next_dup()? {
                Some((key, value)) if self.visible::<T>(&key, &value)? => {
                    let encoded = Self::encode::<T>(&key);
                    let subkey = if self.is_split(&encoded) {
                        Some(self.value_subkey::<T>(&value)?)
                    } else {
                        None
                    };
                    self.position = CursorPosition::Local { key: encoded, subkey };
                    Ok(Some((key, value)))
                }
                Some((key, value)) if self.is_split(&Self::encode::<T>(&key)) => {
                    let encoded = Self::encode::<T>(&key);
                    let found = self.typed_split_visible(inner, &key, encoded, Some(value))?;
                    Ok(found.map(|value| (key, value)))
                }
                // A key placed by key whose duplicates are all invisible.
                Some(_) | None => Ok(None),
            },
            position @ CursorPosition::Remote { .. } => {
                self.position = position;
                Err(self.unsupported("next_dup after a read from an owner"))
            }
            position => {
                self.position = position;
                Ok(None)
            }
        }
    }
}
