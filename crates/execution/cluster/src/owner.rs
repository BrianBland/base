//! Owner-side store: answers peer reads of owned shards as of a requested block `P`.
//!
//! State at `P` is a held MDBX read snapshot at a persistence point `S <= P` with the sorted
//! hashed-state and trie updates of the retained canonical blocks `(S, P]` layered on top through
//! reth's in-memory cursor overlays. The store reads only its own snapshots, never a peer.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use parking_lot::{Mutex, RwLock};
use reth_db_api::{
    DatabaseError,
    table::{Compress, Decode},
    tables::{self, RawKey, RawTable},
    transaction::DbTx,
};
use reth_primitives_traits::StorageEntry;
use reth_trie::{
    HashedPostStateSorted, PackedStorageTrieEntry, PackedStoredNibbles, PackedStoredNibblesSubKey,
    hashed_cursor::{HashedCursor, HashedCursorFactory, HashedPostStateCursorFactory},
    trie_cursor::{InMemoryTrieCursorFactory, TrieCursor, TrieCursorFactory},
    updates::TrieUpdatesSorted,
};
use reth_trie_db::{DatabaseHashedCursorFactory, DatabaseTrieCursorFactory, PackedKeyAdapter};
use thiserror::Error;

use crate::{
    Cluster, DuplicateValues, KeyPlacement, Query, ReadError, Request, Response, ServableRange,
    Shard, ShardEntries, ShardedTable, SnapshotId,
};

/// The sorted state changes of one canonical block, as carried by canonical-state notifications.
#[derive(Debug, Clone)]
pub struct BlockUpdates {
    /// Hashed accounts and storage written by the block. Deletions are explicit per key.
    pub hashed_state: Arc<HashedPostStateSorted>,
    /// Trie nodes written by the block. Removed nodes are explicit per path.
    pub trie_updates: Arc<TrieUpdatesSorted>,
}

/// A canonical block retained by an owner.
#[derive(Debug, Clone)]
pub struct CanonicalBlock {
    /// Number and hash of the block.
    pub block: BlockNumHash,
    /// Hash of the block's parent.
    pub parent_hash: B256,
    /// The block's state changes, or `None` when they are unknown. An owner cannot serve past a
    /// block without updates.
    pub updates: Option<BlockUpdates>,
}

/// A held read snapshot of the owner's MDBX at one of its persistence points.
#[derive(Debug)]
pub struct PersistencePoint<TX> {
    block: BlockNumHash,
    tx: Arc<TX>,
}

/// Why the owner store could not answer a request.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The owner refuses; the refusal is sent to the asker.
    #[error(transparent)]
    Refused(#[from] ReadError),
    /// The owner's database failed; the asker sees the owner as unreachable.
    #[error("owner database: {0}")]
    Database(#[from] DatabaseError),
}

/// The retained persistence points and canonical blocks of an owner, keyed by block number.
#[derive(Debug)]
pub struct RetainedChain<TX> {
    points: BTreeMap<u64, PersistencePoint<TX>>,
    blocks: BTreeMap<u64, CanonicalBlock>,
    retention_blocks: u64,
}

/// The base snapshot and the updates that reproduce a requested block on top of it.
#[derive(Debug)]
pub struct ResolvedSnapshot<TX> {
    tx: Arc<TX>,
    base: B256,
    target: B256,
    /// Updates of blocks `(S, P]`, newest first.
    updates: Vec<BlockUpdates>,
}

impl<TX> RetainedChain<TX> {
    /// Creates an empty chain that retains the last `retention_blocks` blocks.
    pub const fn new(retention_blocks: u64) -> Self {
        Self { points: BTreeMap::new(), blocks: BTreeMap::new(), retention_blocks }
    }

    /// Retains `block`, replacing any retained block at or above its number.
    pub fn insert_block(&mut self, block: CanonicalBlock) {
        self.blocks.split_off(&block.block.number);
        self.blocks.insert(block.block.number, block);
        self.prune();
    }

    /// Retains a persistence point, replacing any retained point at or above its number.
    pub fn insert_point(&mut self, block: BlockNumHash, tx: TX) {
        self.points.split_off(&block.number);
        self.points.insert(block.number, PersistencePoint { block, tx: Arc::new(tx) });
        self.prune();
    }

    /// The newest persistence point.
    pub fn newest_point(&self) -> Option<&PersistencePoint<TX>> {
        self.points.values().next_back()
    }

    fn head(&self) -> Option<u64> {
        let newest_block = self.blocks.keys().next_back();
        let newest_point = self.points.keys().next_back();
        newest_block.max(newest_point).copied()
    }

    const fn window_start(&self, head: u64) -> u64 {
        head.saturating_sub(self.retention_blocks.saturating_sub(1))
    }

    /// Drops points and blocks no block inside the window needs.
    fn prune(&mut self) {
        let Some(head) = self.head() else { return };
        let window_start = self.window_start(head);
        match self.points.range(..=window_start).next_back().map(|(number, _)| *number) {
            Some(base) => {
                self.points = self.points.split_off(&base);
                self.blocks = self.blocks.split_off(&(base + 1));
            }
            None => self.blocks = self.blocks.split_off(&window_start),
        }
    }

    /// Highest block reachable from `point` through an unbroken chain of blocks with updates.
    fn reach(&self, point: &PersistencePoint<TX>) -> u64 {
        let mut number = point.block.number;
        let mut hash = point.block.hash;
        while let Some(block) = self.blocks.get(&(number + 1)) {
            if block.parent_hash != hash || block.updates.is_none() {
                break;
            }
            number += 1;
            hash = block.block.hash;
        }
        number
    }

    /// Lowest and highest block servable from any point within the window.
    pub fn servable(&self) -> Option<ServableRange> {
        let window_start = self.window_start(self.head()?);
        self.points
            .values()
            .map(|point| ServableRange {
                lowest: point.block.number.max(window_start),
                highest: self.reach(point),
            })
            .filter(|range| range.lowest <= range.highest)
            .reduce(|a, b| ServableRange {
                lowest: a.lowest.min(b.lowest),
                highest: a.highest.max(b.highest),
            })
    }

    /// Finds a persistence point `S <= P` and the updates `(S, P]` that reproduce `snapshot`.
    pub fn resolve(&self, snapshot: SnapshotId) -> Result<ResolvedSnapshot<TX>, ReadError> {
        let unavailable = || ReadError::SnapshotUnavailable { servable: self.servable() };
        let head = self.head().ok_or_else(unavailable)?;
        let target = snapshot.number;
        if target < self.window_start(head) || target > head {
            return Err(unavailable());
        }
        if let Some(hash) = snapshot.hash {
            let retained_block = self.blocks.get(&target).map(|block| block.block.hash);
            let retained_point = self.points.get(&target).map(|point| point.block.hash);
            if retained_block.into_iter().chain(retained_point).any(|known| known != hash) {
                return Err(ReadError::WrongFork);
            }
        }
        self.points
            .range(..=target)
            .rev()
            .find_map(|(_, point)| self.chain_from(point, target))
            .ok_or_else(unavailable)
    }

    fn chain_from(
        &self,
        point: &PersistencePoint<TX>,
        target: u64,
    ) -> Option<ResolvedSnapshot<TX>> {
        let mut hash = point.block.hash;
        let mut updates = Vec::new();
        for number in point.block.number + 1..=target {
            let block = self.blocks.get(&number)?;
            if block.parent_hash != hash {
                return None;
            }
            updates.push(block.updates.clone()?);
            hash = block.block.hash;
        }
        updates.reverse();
        Some(ResolvedSnapshot {
            tx: Arc::clone(&point.tx),
            base: point.block.hash,
            target: hash,
            updates,
        })
    }
}

/// The merged updates of blocks `(S, P]`.
#[derive(Debug, Default)]
pub struct StateOverlay {
    hashed_state: HashedPostStateSorted,
    trie_updates: TrieUpdatesSorted,
}

impl StateOverlay {
    /// Merges block updates given newest first; newer blocks win.
    pub fn merge(updates: &[BlockUpdates]) -> Self {
        let hashed: Vec<_> = updates.iter().map(|u| Arc::clone(&u.hashed_state)).collect();
        let trie: Vec<_> = updates.iter().map(|u| Arc::clone(&u.trie_updates)).collect();
        Self {
            hashed_state: HashedPostStateSorted::merge_slice(&hashed),
            trie_updates: TrieUpdatesSorted::merge_slice(&trie),
        }
    }
}

/// A merged overlay keyed by the hashes of its base snapshot and target block.
pub type CachedOverlay = ((B256, B256), Arc<StateOverlay>);

/// The state of the owner's sharded tables as of one block: a snapshot plus an overlay.
#[derive(Debug)]
pub struct SnapshotView<TX> {
    tx: Arc<TX>,
    overlay: Arc<StateOverlay>,
}

impl<TX: DbTx> SnapshotView<TX> {
    /// Largest number of entries or values in one response; the asker seeks again for more.
    pub const MAX_ENTRIES_PER_RESPONSE: usize = 1024;

    fn hashed_cursors(
        &self,
    ) -> HashedPostStateCursorFactory<DatabaseHashedCursorFactory<&TX>, &HashedPostStateSorted>
    {
        HashedPostStateCursorFactory::new(
            DatabaseHashedCursorFactory::new(&*self.tx),
            &self.overlay.hashed_state,
        )
    }

    fn trie_cursors(
        &self,
    ) -> InMemoryTrieCursorFactory<
        DatabaseTrieCursorFactory<&TX, PackedKeyAdapter>,
        &TrieUpdatesSorted,
    > {
        InMemoryTrieCursorFactory::new(
            DatabaseTrieCursorFactory::new(&*self.tx),
            &self.overlay.trie_updates,
        )
    }

    /// The encoded value of `key` in `table` (`HashedAccounts` or `AccountsTrie`).
    pub fn get(&self, table: ShardedTable, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        match table {
            ShardedTable::HashedAccounts => {
                let key = B256::from_slice(key);
                let factory = self.hashed_cursors();
                let entry = factory.hashed_account_cursor()?.seek(key)?;
                Ok(entry.filter(|(found, _)| *found == key).map(|(_, a)| a.compress()))
            }
            ShardedTable::AccountsTrie => {
                let path = PackedStoredNibbles::decode(key)?.0;
                let factory = self.trie_cursors();
                let entry = factory.account_trie_cursor()?.seek_exact(path)?;
                Ok(entry.map(|(_, node)| node.compress()))
            }
            _ => Err(ReadError::Unsupported.into()),
        }
    }

    /// Up to `limit` entries of `shard` with key `>= key`, ascending. Replicated account-trie
    /// keys are skipped.
    pub fn seek_ge(
        &self,
        table: ShardedTable,
        key: &[u8],
        shard: Shard,
        limit: usize,
    ) -> Result<ShardEntries, StoreError> {
        match table {
            ShardedTable::HashedAccounts => {
                let factory = self.hashed_cursors();
                let mut cursor = factory.hashed_account_cursor()?;
                let mut entry = cursor.seek(B256::from_slice(key))?;
                Self::collect_shard(table, shard, limit, || {
                    let current = entry.take().map(|(k, a)| (k.to_vec(), a.compress()));
                    entry = cursor.next()?;
                    Ok(current)
                })
            }
            ShardedTable::AccountsTrie => {
                let factory = self.trie_cursors();
                let mut cursor = factory.account_trie_cursor()?;
                let mut entry = cursor.seek(PackedStoredNibbles::decode(key)?.0)?;
                Self::collect_shard(table, shard, limit, || {
                    let current = entry.take().map(|(path, node)| {
                        (PackedStoredNibbles(path).to_compact_array().to_vec(), node.compress())
                    });
                    entry = cursor.next()?;
                    Ok(current)
                })
            }
            _ => Err(ReadError::Unsupported.into()),
        }
    }

    fn collect_shard(
        table: ShardedTable,
        shard: Shard,
        limit: usize,
        mut next: impl FnMut() -> Result<Option<(Vec<u8>, Vec<u8>)>, DatabaseError>,
    ) -> Result<ShardEntries, StoreError> {
        let limit = limit.min(Self::MAX_ENTRIES_PER_RESPONSE);
        let mut entries = Vec::new();
        while let Some((key, value)) = next()? {
            match table.placement(&key) {
                Some(KeyPlacement::Replicated) => continue,
                Some(KeyPlacement::Shard(found)) if found == shard => {
                    if entries.len() == limit {
                        return Ok(ShardEntries { entries, end_of_shard: false });
                    }
                    entries.push((key, value));
                }
                _ => break,
            }
        }
        Ok(ShardEntries { entries, end_of_shard: true })
    }

    /// Up to `limit` duplicate values of `key` with subkey `>= subkey`, ascending
    /// (`HashedStorages` or `StoragesTrie`).
    pub fn seek_dup_ge(
        &self,
        table: ShardedTable,
        key: &[u8],
        subkey: &[u8],
        limit: usize,
    ) -> Result<DuplicateValues, StoreError> {
        let address = B256::from_slice(key);
        match table {
            ShardedTable::HashedStorages => {
                let factory = self.hashed_cursors();
                let mut cursor = factory.hashed_storage_cursor(address)?;
                let mut entry = cursor.seek(B256::from_slice(subkey))?;
                Self::collect_key(limit, || {
                    let current =
                        entry.take().map(|(key, value)| StorageEntry { key, value }.compress());
                    entry = cursor.next()?;
                    Ok(current)
                })
            }
            ShardedTable::StoragesTrie => {
                let factory = self.trie_cursors();
                let mut cursor = factory.storage_trie_cursor(address)?;
                let mut entry = cursor.seek(PackedStoredNibblesSubKey::decode(subkey)?.0)?;
                Self::collect_key(limit, || {
                    let current = entry.take().map(|(path, node)| {
                        PackedStorageTrieEntry { nibbles: PackedStoredNibblesSubKey(path), node }
                            .compress()
                    });
                    entry = cursor.next()?;
                    Ok(current)
                })
            }
            _ => Err(ReadError::Unsupported.into()),
        }
    }

    fn collect_key(
        limit: usize,
        mut next: impl FnMut() -> Result<Option<Vec<u8>>, DatabaseError>,
    ) -> Result<DuplicateValues, StoreError> {
        let limit = limit.min(Self::MAX_ENTRIES_PER_RESPONSE);
        let mut values = Vec::new();
        while let Some(value) = next()? {
            if values.len() == limit {
                return Ok(DuplicateValues { values, end_of_key: false });
            }
            values.push(value);
        }
        Ok(DuplicateValues { values, end_of_key: true })
    }
}

/// Answers peer reads of the shards this node owns, as of any block inside the retention window.
#[derive(Debug)]
pub struct OwnerStore<TX> {
    cluster: Arc<Cluster>,
    retained: RwLock<RetainedChain<TX>>,
    overlays: Mutex<VecDeque<CachedOverlay>>,
}

impl<TX: DbTx + Sync> OwnerStore<TX> {
    /// Default retention window `W`: Base's persistence backpressure threshold. The spec requires
    /// `W >= 160`.
    pub const DEFAULT_RETENTION_BLOCKS: u64 = 160;
    /// Merged overlays kept for reuse, keyed by base and target block hash.
    pub const OVERLAY_CACHE_CAPACITY: usize = 4;

    /// Creates a store for `cluster`'s node that serves the last `retention_blocks` blocks.
    pub const fn new(cluster: Arc<Cluster>, retention_blocks: u64) -> Self {
        Self {
            cluster,
            retained: RwLock::new(RetainedChain::new(retention_blocks)),
            overlays: Mutex::new(VecDeque::new()),
        }
    }

    /// Registers a read snapshot `tx` of this node's MDBX, opened right after it persisted
    /// `block`. Long-read safety is disabled on it because it is held for up to `W` blocks.
    pub fn register_persistence_point(&self, block: BlockNumHash, mut tx: TX) {
        tx.disable_long_read_transaction_safety();
        self.retained.write().insert_point(block, tx);
    }

    /// Retains a canonical block's updates, replacing any block at or above its number.
    pub fn insert_block(&self, block: CanonicalBlock) {
        self.retained.write().insert_block(block);
    }

    /// Answers `request` as of its snapshot. Execution results are not state, so
    /// [`Query::ExecutedBlock`] is `Unsupported` here; the owner server answers it.
    pub fn read(&self, request: &Request) -> Result<Response, StoreError> {
        let table = request.table;
        match &request.query {
            Query::Get { key } => {
                if !matches!(
                    table,
                    ShardedTable::HashedAccounts
                        | ShardedTable::AccountsTrie
                        | ShardedTable::Bytecodes
                ) {
                    return Err(ReadError::Unsupported.into());
                }
                self.check_holds(table, key)?;
                if table == ShardedTable::Bytecodes {
                    return self.bytecode(request.snapshot, key);
                }
                Ok(Response::Value(self.view(request.snapshot)?.get(table, key)?))
            }
            Query::SeekGe { key, limit } => {
                if !matches!(table, ShardedTable::HashedAccounts | ShardedTable::AccountsTrie)
                    || table.placement(key).is_none()
                {
                    return Err(ReadError::Unsupported.into());
                }
                let shard = Shard(key[0]);
                if !self.cluster.owns(shard) {
                    return Err(ReadError::NotOwner.into());
                }
                let view = self.view(request.snapshot)?;
                Ok(Response::Entries(view.seek_ge(table, key, shard, *limit as usize)?))
            }
            Query::SeekDupGe { key, subkey, limit } => {
                let valid_subkey = match table {
                    ShardedTable::HashedStorages => subkey.len() == ShardedTable::HASH_KEY_LEN,
                    ShardedTable::StoragesTrie => {
                        ShardedTable::packed_path_nibbles(subkey).is_some()
                    }
                    _ => false,
                };
                if !valid_subkey {
                    return Err(ReadError::Unsupported.into());
                }
                self.check_holds(table, key)?;
                let view = self.view(request.snapshot)?;
                Ok(Response::Duplicates(view.seek_dup_ge(table, key, subkey, *limit as usize)?))
            }
            Query::ExecutedBlock { .. } => Err(ReadError::Unsupported.into()),
        }
    }

    fn check_holds(&self, table: ShardedTable, key: &[u8]) -> Result<(), ReadError> {
        match self.cluster.holds(table, key) {
            None => Err(ReadError::Unsupported),
            Some(false) => Err(ReadError::NotOwner),
            Some(true) => Ok(()),
        }
    }

    /// Bytecodes never change, so the newest snapshot answers any `P`; absence is trusted only
    /// once the owner has persisted up to `P`. A refusal advertises the newest persistence point
    /// as the highest servable block, so an asker ahead of it retries until the owner persists.
    fn bytecode(&self, snapshot: SnapshotId, key: &[u8]) -> Result<Response, StoreError> {
        let retained = self.retained.read();
        let Some(newest) = retained.newest_point() else {
            return Err(ReadError::SnapshotUnavailable { servable: None }.into());
        };
        let value = newest.tx.get::<RawTable<tables::Bytecodes>>(RawKey::from_vec(key.to_vec()))?;
        match value {
            Some(value) => Ok(Response::Value(Some(value.into_value()))),
            None if newest.block.number >= snapshot.number => Ok(Response::Value(None)),
            None => Err(ReadError::SnapshotUnavailable {
                servable: Some(ServableRange { lowest: 0, highest: newest.block.number }),
            }
            .into()),
        }
    }

    fn view(&self, snapshot: SnapshotId) -> Result<SnapshotView<TX>, ReadError> {
        let resolved = self.retained.read().resolve(snapshot)?;
        let overlay =
            if resolved.updates.is_empty() { Arc::default() } else { self.overlay(&resolved) };
        Ok(SnapshotView { tx: resolved.tx, overlay })
    }

    fn overlay(&self, resolved: &ResolvedSnapshot<TX>) -> Arc<StateOverlay> {
        let key = (resolved.base, resolved.target);
        if let Some((_, cached)) = self.overlays.lock().iter().find(|(k, _)| *k == key) {
            return Arc::clone(cached);
        }
        let overlay = Arc::new(StateOverlay::merge(&resolved.updates));
        let mut overlays = self.overlays.lock();
        if overlays.len() == Self::OVERLAY_CACHE_CAPACITY {
            overlays.pop_front();
        }
        overlays.push_back((key, Arc::clone(&overlay)));
        overlay
    }
}
