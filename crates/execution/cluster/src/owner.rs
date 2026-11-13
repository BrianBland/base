//! Owner side: answers peers' exact reads of the rows this node holds as of a requested block
//! `P`, from its database at its persisted tip `T`, a history index of the blocks up to `T` built
//! from reth's persisted changesets, and the canonical blocks above `T` from notifications.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, OnceLock},
};

use alloy_eips::BlockNumHash;
use alloy_primitives::{B256, U256};
use parking_lot::Mutex;
use reth_db_api::{
    Database, DatabaseError,
    cursor::{DbCursorRO, DbDupCursorRO},
    table::Compress,
    tables::{self, RawKey, RawTable},
    transaction::DbTx,
};
use reth_primitives_traits::{Account, StorageEntry};
use reth_trie::HashedPostStateSorted;
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{
    BlockHashLookup, ChangesetReader, ChangesetSource, Cluster, ClusterNode, HistoryIndex,
    OwnerAnswer, OwnerStats, Query, ReadError, Request, Response, ServableRange, Shard,
    ShardedTable, SnapshotId,
};

/// A canonical block as an owner keeps it above its persisted tip.
#[derive(Debug, Clone)]
pub struct CanonicalBlock {
    /// Number and hash of the block.
    pub block: BlockNumHash,
    /// Hash of the block's parent.
    pub parent_hash: B256,
    /// The new values of the hashed accounts and storage slots the block wrote (a deleted slot is
    /// zero), or `None` when the notification lacks them. An owner cannot serve at or past a
    /// block without them.
    pub hashed_state: Option<Arc<HashedPostStateSorted>>,
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

/// The blocks an owner's history index covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexCoverage {
    /// Lowest servable block `L`: the changes of every block above it are indexed.
    pub lowest: u64,
    /// Highest indexed block `I`.
    pub indexed: u64,
}

/// Where the value of a row at `P` comes from, besides the database row at `T`.
#[derive(Debug)]
pub enum ValueSource<'a> {
    /// `P <= T`: the value before the row's first change in blocks `(after, through]`.
    Index {
        /// `P`.
        after: u64,
        /// `T`.
        through: u64,
    },
    /// `P > T`: the row's newest value in these hashed states of blocks `(T, P]`, newest first.
    Chain(Vec<&'a HashedPostStateSorted>),
    /// The read transaction predates a persistence commit that dropped blocks above its `T`
    /// from the chain; a fresh transaction answers.
    StaleTransaction,
}

/// What an owner knows about its rows' history beyond its database, consulted under one lock.
#[derive(Debug, Default)]
pub struct OwnerHistory {
    index: HistoryIndex,
    coverage: Option<IndexCoverage>,
    /// `T` as of the last persistence commit the owner saw.
    persisted: u64,
    /// Canonical blocks above `persisted`, by number.
    chain: BTreeMap<u64, CanonicalBlock>,
    /// A reorg reached persisted blocks: nothing is served any more.
    stopped: bool,
}

impl OwnerHistory {
    /// Moves the persisted tip to `tip` and drops the chain's blocks at or below it.
    fn set_persisted(&mut self, tip: u64) {
        self.persisted = tip;
        self.chain = self.chain.split_off(&(tip + 1));
    }

    /// Keeps `block` above the persisted tip, replacing any block at or above its number.
    fn insert(&mut self, block: CanonicalBlock) {
        if block.block.number <= self.persisted {
            return;
        }
        self.chain.split_off(&block.block.number);
        self.chain.insert(block.block.number, block);
    }

    /// Highest block reachable from the persisted tip through linked chain blocks with state.
    fn chain_reach(&self, hash: &impl Fn(u64) -> Option<B256>) -> u64 {
        let mut number = self.persisted;
        let Some(mut parent) = hash(number) else { return number };
        while let Some(block) = self.chain.get(&(number + 1)) {
            if block.parent_hash != parent || block.hashed_state.is_none() {
                break;
            }
            number += 1;
            parent = block.block.hash;
        }
        number
    }

    /// The blocks servable now, or `None` when none are: no index yet, an index that has not
    /// reached the persisted tip, or a stopped owner.
    fn servable(&self, hash: &impl Fn(u64) -> Option<B256>) -> Option<ServableRange> {
        let coverage = self.coverage.filter(|coverage| coverage.indexed >= self.persisted)?;
        (!self.stopped)
            .then(|| ServableRange { lowest: coverage.lowest, highest: self.chain_reach(hash) })
    }

    /// Where the value at `snapshot` comes from for a read transaction at persisted tip `tip`,
    /// following the spec's [Owner side](crate#owner-side) rule.
    fn locate(
        &self,
        snapshot: SnapshotId,
        tip: u64,
        hash: &impl Fn(u64) -> Option<B256>,
    ) -> Result<ValueSource<'_>, ReadError> {
        let unavailable = |servable| ReadError::SnapshotUnavailable { servable };
        if self.stopped {
            return Err(ReadError::WrongFork);
        }
        let target = snapshot.number;
        if target <= tip {
            let Some(coverage) = self.coverage.filter(|coverage| coverage.indexed >= tip) else {
                return Err(unavailable(None));
            };
            if target < coverage.lowest {
                let range = ServableRange { lowest: coverage.lowest, highest: coverage.indexed };
                return Err(unavailable(Some(range)));
            }
            if let Some(asked) = snapshot.hash {
                match hash(target) {
                    Some(canonical) if canonical == asked => {}
                    Some(_) => return Err(ReadError::WrongFork),
                    None => return Err(unavailable(None)),
                }
            }
            return Ok(ValueSource::Index { after: target, through: tip });
        }
        if tip < self.persisted {
            return Ok(ValueSource::StaleTransaction);
        }
        let Some(mut parent) = hash(tip) else { return Err(unavailable(None)) };
        let mut states = Vec::new();
        for number in tip + 1..=target {
            let state = self
                .chain
                .get(&number)
                .filter(|block| block.parent_hash == parent)
                .and_then(|block| block.hashed_state.as_deref().map(|state| (block, state)));
            let Some((block, state)) = state else {
                return Err(unavailable(self.servable(hash)));
            };
            states.push(state);
            parent = block.block.hash;
        }
        if snapshot.hash.is_some_and(|asked| asked != parent) {
            return Err(ReadError::WrongFork);
        }
        states.reverse();
        Ok(ValueSource::Chain(states))
    }

    fn chain_account(state: &HashedPostStateSorted, address: B256) -> Option<Option<Account>> {
        let index = state.accounts.binary_search_by_key(&address, |(key, _)| *key).ok()?;
        Some(state.accounts[index].1)
    }

    fn chain_slot(state: &HashedPostStateSorted, address: B256, slot: B256) -> Option<U256> {
        let slots = &state.storages.get(&address)?.storage_slots;
        let index = slots.binary_search_by_key(&slot, |(key, _)| *key).ok()?;
        Some(slots[index].1)
    }

    /// The account at `P`, given the database row at `T`.
    fn account(
        &self,
        source: &ValueSource<'_>,
        address: B256,
        row: Option<Account>,
    ) -> Option<Account> {
        match source {
            ValueSource::Index { after, through } => {
                self.index.account_before(address, *after, *through).unwrap_or(row)
            }
            ValueSource::Chain(states) => {
                states.iter().find_map(|state| Self::chain_account(state, address)).unwrap_or(row)
            }
            ValueSource::StaleTransaction => row,
        }
    }

    /// The slot's value at `P` (zero: absent), given the database value at `T`.
    fn storage(&self, source: &ValueSource<'_>, address: B256, slot: B256, row: U256) -> U256 {
        match source {
            ValueSource::Index { after, through } => {
                self.index.storage_before(address, slot, *after, *through).unwrap_or(row)
            }
            ValueSource::Chain(states) => states
                .iter()
                .find_map(|state| Self::chain_slot(state, address, slot))
                .unwrap_or(row),
            ValueSource::StaleTransaction => row,
        }
    }

    /// The slots of `address` whose value at `P` differs from the database's at `T` may, with
    /// their values at `P`.
    fn storage_changes(&self, source: &ValueSource<'_>, address: B256) -> BTreeMap<B256, U256> {
        match source {
            ValueSource::Index { after, through } => {
                self.index.storage_changes(address, *after, *through).into_iter().collect()
            }
            ValueSource::Chain(states) => {
                let mut changes = BTreeMap::new();
                for storage in states.iter().filter_map(|state| state.storages.get(&address)) {
                    for (slot, value) in &storage.storage_slots {
                        changes.entry(*slot).or_insert(*value);
                    }
                }
                changes
            }
            ValueSource::StaleTransaction => BTreeMap::new(),
        }
    }
}

/// Answers peers' exact reads of the rows this node holds, as of any block inside its history
/// window, from its database `DB`.
pub struct OwnerStore<DB> {
    cluster: Arc<Cluster>,
    db: DB,
    retention_blocks: u64,
    history: Mutex<OwnerHistory>,
    block_hashes: OnceLock<BlockHashLookup>,
    changesets: OnceLock<ChangesetReader>,
    stats: Arc<OwnerStats>,
}

impl<DB> fmt::Debug for OwnerStore<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnerStore")
            .field("node_id", &self.cluster.node_id())
            .field("retention_blocks", &self.retention_blocks)
            .field("history", &self.history)
            .finish_non_exhaustive()
    }
}

impl<DB: Database> OwnerStore<DB> {
    /// Fresh read transactions tried per read when a transaction predates a persistence commit.
    pub const STALE_RETRIES: usize = 3;
    /// Blocks of changesets read at once while building the index.
    pub const CHANGESET_CHUNK_BLOCKS: u64 = 1_000;

    /// A store for `cluster`'s node over `db`, keeping a history index of the last
    /// `retention_blocks` persisted blocks once a changeset reader is installed.
    pub fn new(
        cluster: Arc<Cluster>,
        db: DB,
        retention_blocks: u64,
    ) -> Result<Self, DatabaseError> {
        let persisted = ClusterNode::finish_checkpoint(&db.tx()?)?;
        Ok(Self {
            cluster,
            db,
            retention_blocks,
            history: Mutex::new(OwnerHistory { persisted, ..Default::default() }),
            block_hashes: OnceLock::new(),
            changesets: OnceLock::new(),
            stats: Arc::default(),
        })
    }

    /// The counters of this store's answers.
    pub const fn stats(&self) -> &Arc<OwnerStats> {
        &self.stats
    }

    /// Installs the lookup of canonical block hashes. Only the first lookup installed is kept;
    /// returns whether this one was.
    pub fn install_block_hashes(&self, lookup: BlockHashLookup) -> bool {
        self.block_hashes.set(lookup).is_ok()
    }

    /// The hash of canonical block `number` from the installed lookup, if any.
    pub fn block_hash(&self, number: u64) -> Option<B256> {
        self.block_hashes.get().and_then(|lookup| lookup(number))
    }

    /// Installs the changeset reader and builds the history index from it. Only the first
    /// reader installed is kept; returns whether this one was.
    pub fn install_changesets(&self, reader: ChangesetReader) -> Result<bool, DatabaseError> {
        if self.changesets.set(reader).is_err() {
            return Ok(false);
        }
        let mut history = self.history.lock();
        self.build(&mut history)?;
        self.record_history(&history);
        Ok(true)
    }

    /// The index coverage, or `None` before the index is built.
    pub fn coverage(&self) -> Option<IndexCoverage> {
        self.history.lock().coverage
    }

    /// The blocks this store can serve now.
    pub fn servable(&self) -> Option<ServableRange> {
        self.history.lock().servable(&|number| self.block_hash(number))
    }

    fn build(&self, history: &mut OwnerHistory) -> Result<(), DatabaseError> {
        let Some(reader) = self.changesets.get() else { return Ok(()) };
        let tip = ClusterNode::finish_checkpoint(&self.db.tx()?)?;
        history.set_persisted(tip);
        let window_lowest = tip.saturating_sub(self.retention_blocks);
        let available = reader.available()?;
        let lowest = available
            .as_ref()
            .map_or(tip, |blocks| window_lowest.max(blocks.start().saturating_sub(1)));
        let through = available.map_or(lowest, |blocks| (*blocks.end()).min(tip)).max(lowest);
        history.index = HistoryIndex::default();
        history.coverage = Some(IndexCoverage { lowest, indexed: lowest });
        self.index_through(history, reader.as_ref(), through)?;
        info!(
            owned_shards = Shard::all().filter(|shard| self.cluster.owns(*shard)).count(),
            lowest,
            highest = through,
            entries = history.index.entries(),
            window_lowest,
            persisted_tip = tip,
            "cluster owner history built"
        );
        Ok(())
    }

    /// Indexes the blocks after the covered ones through `through`, raising the coverage after
    /// every chunk read.
    fn index_through(
        &self,
        history: &mut OwnerHistory,
        reader: &dyn ChangesetSource,
        through: u64,
    ) -> Result<(), DatabaseError> {
        while let Some(mut coverage) =
            history.coverage.filter(|coverage| coverage.indexed < through)
        {
            let end = through.min(coverage.indexed + Self::CHANGESET_CHUNK_BLOCKS);
            let changesets = reader.read(coverage.indexed + 1..=end)?;
            history.index.extend(&self.cluster, changesets);
            coverage.indexed = end;
            history.coverage = Some(coverage);
        }
        Ok(())
    }

    /// Runs `commit`, the MDBX commit that moves the persisted tip to `tip`, with the history
    /// locked, then indexes the newly persisted blocks, prunes below the window and drops the
    /// chain's persisted blocks, so a read that sees the new tip also sees the index reach it.
    /// Failing to index is logged and leaves the owner behind until a later commit succeeds.
    pub fn commit_persisted(
        &self,
        tip: u64,
        commit: impl FnOnce() -> Result<(), DatabaseError>,
    ) -> Result<(), DatabaseError> {
        let mut history = self.history.lock();
        commit()?;
        history.set_persisted(tip);
        if let Err(error) = self.extend(&mut history, tip) {
            warn!(error = %error, block = tip, "cluster owner could not index persisted blocks");
        }
        self.record_history(&history);
        Ok(())
    }

    fn extend(&self, history: &mut OwnerHistory, tip: u64) -> Result<(), DatabaseError> {
        let (Some(reader), Some(coverage)) = (self.changesets.get(), history.coverage) else {
            return Ok(());
        };
        if tip > coverage.indexed {
            let Some(available) = reader.available()? else { return Ok(()) };
            if *available.start() > coverage.indexed + 1 {
                warn!(
                    indexed = coverage.indexed,
                    available = ?available,
                    "changesets of blocks the cluster owner needs are missing"
                );
                return Ok(());
            }
            self.index_through(history, reader.as_ref(), tip.min(*available.end()))?;
        }
        let window_lowest = tip.saturating_sub(self.retention_blocks);
        if let Some(coverage) = history.coverage.as_mut()
            && window_lowest > coverage.lowest
        {
            history.index.prune_through(window_lowest);
            coverage.lowest = window_lowest;
        }
        Ok(())
    }

    /// Keeps canonical blocks from a commit notification, in ascending order. A block at or below
    /// the persisted tip is ignored; one at a number already kept replaces it and every block
    /// above it.
    pub fn commit_blocks(&self, blocks: Vec<CanonicalBlock>) {
        let mut history = self.history.lock();
        for block in blocks {
            history.insert(block);
        }
        self.record_history(&history);
    }

    /// Applies a reorg notification that reverted the blocks from `reverted_from` on and added
    /// `blocks`. A reorg that reverts persisted blocks stops this owner for good.
    pub fn reorg_blocks(&self, reverted_from: u64, blocks: Vec<CanonicalBlock>) {
        let mut history = self.history.lock();
        if reverted_from <= history.persisted {
            error!(
                reverted_from,
                persisted_tip = history.persisted,
                "a reorg reached persisted blocks; the cluster owner stops serving"
            );
            history.stopped = true;
        } else {
            history.chain.split_off(&reverted_from);
            blocks.into_iter().for_each(|block| history.insert(block));
        }
        self.record_history(&history);
    }

    fn record_history(&self, history: &OwnerHistory) {
        let servable = history.servable(&|number| self.block_hash(number));
        self.stats.record_history(history.index.entries(), servable);
    }

    /// Answers `request` as of its snapshot in fresh read transactions, and counts the answer.
    /// Execution results are not state, so [`Query::ExecutedBlock`] is `Unsupported` here; the
    /// owner server answers it.
    pub fn read(&self, request: &Request) -> Result<Response, StoreError> {
        let result = self.read_fresh(request);
        match &result {
            Ok(_) => self.stats.record(OwnerAnswer::Served),
            Err(StoreError::Refused(refusal)) if refusal.is_behind(request.snapshot.number) => {
                self.stats.record(OwnerAnswer::Behind)
            }
            Err(StoreError::Refused(_)) => self.stats.record(OwnerAnswer::Refused),
            Err(StoreError::Database(_)) => {}
        }
        result
    }

    fn read_fresh(&self, request: &Request) -> Result<Response, StoreError> {
        for _ in 0..Self::STALE_RETRIES {
            if let Some(response) = self.answer(&self.db.tx()?, request)? {
                return Ok(response);
            }
        }
        Err(ReadError::SnapshotUnavailable { servable: None }.into())
    }

    /// Answers `request` in the read transaction `tx`, whose `Finish` checkpoint is the
    /// persisted tip `T` used. A transaction that predates a persistence commit is refused as
    /// behind where only a fresh one could answer.
    pub fn read_in(&self, tx: &DB::TX, request: &Request) -> Result<Response, StoreError> {
        self.answer(tx, request)?
            .ok_or_else(|| ReadError::SnapshotUnavailable { servable: None }.into())
    }

    /// `None` when `tx` is stale (see [`ValueSource::StaleTransaction`]).
    fn answer(&self, tx: &DB::TX, request: &Request) -> Result<Option<Response>, StoreError> {
        let snapshot = request.snapshot;
        let hash = |number| self.block_hash(number);
        match &request.query {
            Query::Get { key } if request.table == ShardedTable::Bytecodes => {
                self.check_holds(ShardedTable::Bytecodes, key, &[])?;
                self.bytecode(tx, snapshot, key).map(Some)
            }
            Query::Get { .. }
            | Query::SeekGe { .. }
            | Query::SeekDupGe { .. }
            | Query::ExecutedBlock { .. } => Err(ReadError::Unsupported.into()),
            Query::Status => Ok(Some(Response::Status(self.servable()))),
            Query::AccountValue { address } => {
                Self::check_table(request, ShardedTable::HashedAccounts)?;
                self.check_holds(ShardedTable::HashedAccounts, address.as_slice(), &[])?;
                let tip = ClusterNode::finish_checkpoint(tx)?;
                let row = tx.get::<tables::HashedAccounts>(*address)?;
                let history = self.history.lock();
                let source = history.locate(snapshot, tip, &hash)?;
                if matches!(source, ValueSource::StaleTransaction) {
                    return Ok(None);
                }
                let account = history.account(&source, *address, row);
                Ok(Some(Response::Value(account.map(|account| account.compress()))))
            }
            Query::StorageValue { address, slot } => {
                Self::check_table(request, ShardedTable::HashedStorages)?;
                self.check_holds(
                    ShardedTable::HashedStorages,
                    address.as_slice(),
                    slot.as_slice(),
                )?;
                let tip = ClusterNode::finish_checkpoint(tx)?;
                let row = tx
                    .cursor_dup_read::<tables::HashedStorages>()?
                    .seek_by_key_subkey(*address, *slot)?
                    .filter(|entry| entry.key == *slot)
                    .map_or(U256::ZERO, |entry| entry.value);
                let history = self.history.lock();
                let source = history.locate(snapshot, tip, &hash)?;
                if matches!(source, ValueSource::StaleTransaction) {
                    return Ok(None);
                }
                let value = history.storage(&source, *address, *slot, row);
                let entry =
                    (!value.is_zero()).then(|| StorageEntry { key: *slot, value }.compress());
                Ok(Some(Response::Value(entry)))
            }
            Query::HasStorage { address } => {
                Self::check_table(request, ShardedTable::HashedStorages)?;
                if self.cluster.is_split(ShardedTable::HashedStorages, address.as_slice()) {
                    return Err(ReadError::Unsupported.into());
                }
                self.check_holds(ShardedTable::HashedStorages, address.as_slice(), &[])?;
                let tip = ClusterNode::finish_checkpoint(tx)?;
                let history = self.history.lock();
                let source = history.locate(snapshot, tip, &hash)?;
                if matches!(source, ValueSource::StaleTransaction) {
                    return Ok(None);
                }
                let changes = history.storage_changes(&source, *address);
                if changes.values().any(|value| !value.is_zero()) {
                    return Ok(Some(Response::HasStorage(true)));
                }
                let mut cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;
                let mut row = cursor.seek_exact(*address)?.map(|(_, entry)| entry);
                while let Some(entry) = row {
                    if !changes.contains_key(&entry.key) {
                        return Ok(Some(Response::HasStorage(true)));
                    }
                    row = cursor.next_dup_val()?;
                }
                Ok(Some(Response::HasStorage(false)))
            }
        }
    }

    fn check_table(request: &Request, table: ShardedTable) -> Result<(), ReadError> {
        if request.table == table { Ok(()) } else { Err(ReadError::Unsupported) }
    }

    fn check_holds(&self, table: ShardedTable, key: &[u8], subkey: &[u8]) -> Result<(), ReadError> {
        match self.cluster.holds(table, key, subkey) {
            None => Err(ReadError::Unsupported),
            Some(false) => Err(ReadError::NotOwner),
            Some(true) => Ok(()),
        }
    }

    /// Bytecodes never change, so the newest data answers any `P`; absence is trusted only once
    /// the owner has persisted up to `P`. A refusal advertises the persisted tip as the highest
    /// servable block, so an asker ahead of it retries until the owner persists.
    fn bytecode(
        &self,
        tx: &DB::TX,
        snapshot: SnapshotId,
        key: &[u8],
    ) -> Result<Response, StoreError> {
        let tip = ClusterNode::finish_checkpoint(tx)?;
        match tx.get::<RawTable<tables::Bytecodes>>(RawKey::from_vec(key.to_vec()))? {
            Some(value) => Ok(Response::Value(Some(value.into_value()))),
            None if tip >= snapshot.number => Ok(Response::Value(None)),
            None => Err(ReadError::SnapshotUnavailable {
                servable: Some(ServableRange { lowest: 0, highest: tip }),
            }
            .into()),
        }
    }
}
