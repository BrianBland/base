//! This node's side of a cluster: its owner store and server, its peer client, the commit
//! guard, shedding, and the execution results it serves and applies.

use std::{
    fmt, io,
    net::TcpListener,
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use alloy_primitives::B256;
use parking_lot::Mutex;
use reth_db::{
    Database, DatabaseEnv, DatabaseError,
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
    mdbx::{RW, TransactionKind, tx::Tx},
    table::{DupSort, Table},
    tables::{self, RawDupSort, RawKey, RawTable},
    transaction::{DbTx, DbTxMut},
};
use reth_stages_types::StageId;
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{
    ApplyCounts, ApplyStats, ChangesetReader, ClientOptions, Cluster, ClusterClient,
    ExecutedBlocks, KeyPlacement, NodeId, OwnerServer, OwnerState, OwnerStates, OwnerStore,
    OwnersNotReady, RemoteReadCounts, RemoteReadStats, Shard, ShardedTable, TrieSlice,
    TrieSliceCounts, TrieSliceStats,
};

/// Returns the hash of the canonical block with the given number, when known.
pub type BlockHashLookup = Box<dyn Fn(u64) -> Option<B256> + Send + Sync>;

/// Why a clustered database could not be opened.
#[derive(Debug, Error)]
pub enum ClusterStartError {
    /// The owner server could not be started.
    #[error("failed to start the cluster owner server: {0}")]
    Server(#[from] io::Error),
    /// A thread logging remote reads or apply-mode outcomes could not be started.
    #[error("failed to start a cluster stats logger: {0}")]
    StatsLogger(io::Error),
    /// The database could not be read.
    #[error("failed to read the database: {0}")]
    Database(#[from] DatabaseError),
}

/// Why a read-write commit of a clustered database is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CommitRefusal {
    /// A download stage checkpoint is ahead of `Finish`: reth's staged pipeline is running, and it
    /// computes from state inside write transactions.
    #[error(
        "the staged pipeline is running ({stage} checkpoint {stage_block} is ahead of Finish \
         {finish}); a clustered node cannot run it and must be re-cloned"
    )]
    PipelineRun {
        /// The stage that is ahead.
        stage: StageId,
        /// That stage's checkpoint.
        stage_block: u64,
        /// The `Finish` checkpoint.
        finish: u64,
    },
    /// `Finish` moved below the last committed `Finish`: persisted blocks are being unwound.
    #[error(
        "persisted blocks are being unwound (Finish {finish} is below the last committed \
         {committed}); a clustered node cannot unwind and must be re-cloned"
    )]
    Unwind {
        /// The `Finish` checkpoint being committed.
        finish: u64,
        /// The last committed `Finish` checkpoint.
        committed: u64,
    },
}

impl From<CommitRefusal> for DatabaseError {
    fn from(refusal: CommitRefusal) -> Self {
        Self::Other(refusal.to_string())
    }
}

/// This node's side of a cluster, shared by every transaction of a clustered
/// [`ClusteredDatabase`](crate::ClusteredDatabase).
pub struct ClusterNode {
    env: DatabaseEnv,
    cluster: Arc<Cluster>,
    client: ClusterClient,
    owner_store: Arc<OwnerStore<DatabaseEnv>>,
    server: OwnerServer,
    /// `Finish` of the last commit.
    committed_finish: Mutex<u64>,
    executed_blocks: Arc<ExecutedBlocks>,
    apply_stats: Arc<ApplyStats>,
    trie_slice: TrieSlice,
    trie_slice_stats: Arc<TrieSliceStats>,
}

impl fmt::Debug for ClusterNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterNode")
            .field("node_id", &self.cluster.node_id())
            .field("owner_server", &self.server.local_addr())
            .field("committed_finish", &*self.committed_finish.lock())
            .finish_non_exhaustive()
    }
}

impl ClusterNode {
    /// Rows deleted per shed write transaction, so that the engine's persistence writer never
    /// waits long for the MDBX write lock.
    pub const SHED_BATCH_ROWS: u64 = 10_000;

    /// Serves `env`'s owned shards on `listener` and reads other shards through peers.
    pub fn start(
        env: DatabaseEnv,
        cluster: Arc<Cluster>,
        listener: TcpListener,
        options: ClientOptions,
    ) -> Result<Self, ClusterStartError> {
        let finish = Self::finish_checkpoint(&env.tx()?)?;
        let retention = cluster.config().retention_blocks;
        let owner_store = Arc::new(OwnerStore::new(Arc::clone(&cluster), env.clone(), retention)?);
        owner_store
            .stats()
            .spawn_logger(RemoteReadStats::LOG_INTERVAL)
            .map_err(ClusterStartError::StatsLogger)?;
        let executed_blocks = Arc::new(ExecutedBlocks::default());
        let server =
            OwnerServer::spawn(listener, Arc::clone(&owner_store), Arc::clone(&executed_blocks))?;
        let client = ClusterClient::new(Arc::clone(&cluster), options);
        client
            .remote_read_stats()
            .spawn_logger(RemoteReadStats::LOG_INTERVAL)
            .map_err(ClusterStartError::StatsLogger)?;
        let apply_stats = Arc::new(ApplyStats::default());
        apply_stats
            .spawn_logger(RemoteReadStats::LOG_INTERVAL)
            .map_err(ClusterStartError::StatsLogger)?;
        let trie_slice_stats = Arc::new(TrieSliceStats::default());
        trie_slice_stats
            .spawn_logger(TrieSliceStats::LOG_INTERVAL)
            .map_err(ClusterStartError::StatsLogger)?;
        let trie_slice = TrieSlice::new(Arc::clone(&cluster));
        let owned = Shard::all().filter(|shard| cluster.owns(*shard)).count();
        info!(
            node_id = cluster.node_id().0,
            owned_shards = owned,
            owner_server = %server.local_addr(),
            persisted_tip = finish,
            apply_from = ?cluster.apply_from(),
            "cluster node started"
        );
        Ok(Self {
            client,
            env,
            cluster,
            owner_store,
            server,
            committed_finish: Mutex::new(finish),
            executed_blocks,
            apply_stats,
            trie_slice,
            trie_slice_stats,
        })
    }

    /// The cluster this node belongs to.
    pub const fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    /// The client reading shards this node does not own.
    pub const fn client(&self) -> &ClusterClient {
        &self.client
    }

    /// The store answering peers' reads of this node's shards.
    pub const fn owner_store(&self) -> &Arc<OwnerStore<DatabaseEnv>> {
        &self.owner_store
    }

    /// The running owner server.
    pub const fn server(&self) -> &OwnerServer {
        &self.server
    }

    /// Whether this node holds the row with encoded `key` and `subkey` of `table` (see
    /// [`Cluster::placement`]). A row that is not valid for the table is an error: a clustered
    /// database cannot place it.
    pub fn holds(
        &self,
        table: ShardedTable,
        key: &[u8],
        subkey: &[u8],
    ) -> Result<bool, DatabaseError> {
        self.cluster.holds(table, key, subkey).ok_or_else(|| {
            DatabaseError::Other(format!(
                "key of length {} and subkey of length {} is not a {table:?} row",
                key.len(),
                subkey.len()
            ))
        })
    }

    /// Whether this node holds encoded `key` of `table`, placed by key alone: the caller knows it
    /// is not a key of a split contract. A key that is not valid for the table is an error.
    pub fn holds_by_key(&self, table: ShardedTable, key: &[u8]) -> Result<bool, DatabaseError> {
        let placement = table.placement(key).ok_or_else(|| {
            DatabaseError::Other(format!("key of length {} is not a {table:?} key", key.len()))
        })?;
        Ok(self.cluster.holds_placement(placement))
    }

    /// This node's remote reads since it started.
    pub fn remote_reads(&self) -> RemoteReadCounts {
        self.client.remote_read_stats().counts()
    }

    /// The execution results of this node's recent blocks, served to apply nodes.
    pub const fn executed_blocks(&self) -> &Arc<ExecutedBlocks> {
        &self.executed_blocks
    }

    /// The counters of this node's apply-mode outcomes.
    pub const fn apply_stats(&self) -> &Arc<ApplyStats> {
        &self.apply_stats
    }

    /// This node's apply-mode outcomes since it started.
    pub fn applied_blocks(&self) -> ApplyCounts {
        self.apply_stats.counts()
    }

    /// The planner of this node's trie slice.
    pub const fn trie_slice(&self) -> &TrieSlice {
        &self.trie_slice
    }

    /// The counters of this node's trie-slice work and skipped message-passer checks.
    pub const fn trie_slice_stats(&self) -> &Arc<TrieSliceStats> {
        &self.trie_slice_stats
    }

    /// This node's trie-slice work and skipped message-passer checks since it started.
    pub fn trie_slices(&self) -> TrieSliceCounts {
        self.trie_slice_stats.counts()
    }

    /// Installs the canonical block-hash lookup on the owner store. Only the first lookup
    /// installed is kept; returns whether this one was.
    pub fn install_block_hashes(&self, lookup: BlockHashLookup) -> bool {
        self.owner_store.install_block_hashes(lookup)
    }

    /// Installs the changeset reader on the owner store, which builds its history index from it.
    /// Only the first reader installed is kept; returns whether this one was.
    pub fn install_changesets(&self, reader: ChangesetReader) -> Result<bool, DatabaseError> {
        self.owner_store.install_changesets(reader)
    }

    /// The `Finish` stage checkpoint visible in `tx`: the block its state tables are at.
    pub fn finish_checkpoint(tx: &impl DbTx) -> Result<u64, DatabaseError> {
        Self::stage_checkpoint(tx, StageId::Finish)
    }

    fn stage_checkpoint(tx: &impl DbTx, stage: StageId) -> Result<u64, DatabaseError> {
        let checkpoint = tx.get::<tables::StageCheckpoints>(stage.to_string())?;
        Ok(checkpoint.map(|checkpoint| checkpoint.block_number).unwrap_or_default())
    }

    /// Commits `tx` unless it would run the pipeline or unwind persisted blocks. The owner store
    /// indexes the blocks the commit persists before any of its reads sees them (see
    /// [`OwnerStore::commit_persisted`]).
    pub fn commit<K: TransactionKind>(&self, tx: Tx<K>) -> Result<(), DatabaseError> {
        let finish = Self::finish_checkpoint(&tx)?;
        if let Some(refusal) = self.commit_refusal(&tx, finish)? {
            error!(error = %refusal, "refusing a clustered database commit");
            tx.abort();
            return Err(refusal.into());
        }
        self.owner_store.commit_persisted(finish, || tx.commit().map(drop))?;
        *self.committed_finish.lock() = finish;
        Ok(())
    }

    /// Why committing `tx`, whose `Finish` checkpoint is `finish`, must be refused, if it must.
    pub fn commit_refusal<K: TransactionKind>(
        &self,
        tx: &Tx<K>,
        finish: u64,
    ) -> Result<Option<CommitRefusal>, DatabaseError> {
        let committed = *self.committed_finish.lock();
        if finish < committed {
            return Ok(Some(CommitRefusal::Unwind { finish, committed }));
        }
        for stage in [StageId::Headers, StageId::Bodies] {
            let stage_block = Self::stage_checkpoint(tx, stage)?;
            if stage_block > finish {
                return Ok(Some(CommitRefusal::PipelineRun { stage, stage_block, finish }));
            }
        }
        Ok(None)
    }

    /// Deletes up to `max_rows` rows of sharded tables this node does not hold, in one write
    /// transaction of the raw database. Returns the number deleted; 0 means shedding is done.
    pub fn shed_batch(&self, max_rows: u64) -> Result<u64, DatabaseError> {
        let tx = self.env.tx_mut()?;
        let mut budget = max_rows;
        self.shed_table::<tables::HashedAccounts>(&tx, ShardedTable::HashedAccounts, &mut budget)?;
        self.shed_table::<tables::HashedStorages>(&tx, ShardedTable::HashedStorages, &mut budget)?;
        self.shed_table::<tables::PackedAccountsTrie>(
            &tx,
            ShardedTable::AccountsTrie,
            &mut budget,
        )?;
        self.shed_table::<tables::PackedStoragesTrie>(
            &tx,
            ShardedTable::StoragesTrie,
            &mut budget,
        )?;
        self.shed_table::<tables::Bytecodes>(&tx, ShardedTable::Bytecodes, &mut budget)?;
        self.shed_split::<tables::HashedStorages>(&tx, ShardedTable::HashedStorages, &mut budget)?;
        self.shed_split::<tables::PackedStoragesTrie>(
            &tx,
            ShardedTable::StoragesTrie,
            &mut budget,
        )?;
        tx.commit()?;
        Ok(max_rows - budget)
    }

    /// Deletes rows of `table` in the shards this node does not own, by key. Split contracts are
    /// skipped whole; [`Self::shed_split`] sheds them by range.
    fn shed_table<T: Table>(
        &self,
        tx: &Tx<RW>,
        table: ShardedTable,
        budget: &mut u64,
    ) -> Result<(), DatabaseError> {
        let mut cursor = tx.cursor_write::<RawTable<T>>()?;
        for shard in Shard::all().filter(|shard| !self.cluster.owns(*shard)) {
            let mut start = table.first_key();
            start[0] = shard.0;
            let mut entry = cursor.seek(RawKey::from_vec(start))?;
            while *budget > 0 {
                let Some((key, _)) = entry else { break };
                let key = key.into_key();
                if self.cluster.is_split(table, &key) {
                    entry = match Self::key_after(&key) {
                        Some(next) => cursor.seek(RawKey::from_vec(next))?,
                        None => None,
                    };
                    continue;
                }
                match table.placement(&key) {
                    Some(KeyPlacement::Shard(found)) if found == shard => {
                        cursor.delete_current()?;
                        *budget -= 1;
                    }
                    Some(KeyPlacement::Replicated) if key[0] == shard.0 => {}
                    _ => break,
                }
                entry = cursor.next()?;
            }
            if *budget == 0 {
                break;
            }
        }
        Ok(())
    }

    /// The smallest key of the same length after `key`, or `None` when `key` is the largest.
    fn key_after(key: &[u8]) -> Option<Vec<u8>> {
        let mut next = key.to_vec();
        for byte in next.iter_mut().rev() {
            let (incremented, overflowed) = byte.overflowing_add(1);
            *byte = incremented;
            if !overflowed {
                return Some(next);
            }
        }
        None
    }

    /// Deletes the duplicates of split contracts in the ranges of shards this node does not own.
    fn shed_split<T: DupSort>(
        &self,
        tx: &Tx<RW>,
        table: ShardedTable,
        budget: &mut u64,
    ) -> Result<(), DatabaseError> {
        let Some(layout) = table.subkey_layout() else { return Ok(()) };
        let mut cursor = tx.cursor_dup_write::<RawDupSort<T>>()?;
        for contract in self.cluster.split_storage() {
            for range in Shard::all().filter(|shard| !self.cluster.owns(*shard)) {
                let floor = RawKey::from_vec(layout.shard_floor(range));
                let mut found = cursor
                    .seek_by_key_subkey(RawKey::from_vec(contract.to_vec()), floor)?
                    .map(|value| value.into_value());
                while *budget > 0 {
                    let Some(value) = found else { break };
                    let in_range =
                        table.subkey_of(&value).and_then(|subkey| layout.placement(subkey))
                            == Some(KeyPlacement::Shard(range));
                    if !in_range {
                        break;
                    }
                    cursor.delete_current()?;
                    *budget -= 1;
                    found = cursor.next()?.and_then(|(key, value)| {
                        (key.into_key() == contract.as_slice()).then(|| value.into_value())
                    });
                }
                if *budget == 0 {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Waits, for at most `budget`, until every shard this node does not hold has an owner that
    /// can serve state reads as of this node's persisted block `P`, so the block about to execute
    /// does not run out of time inside a read. An owner unreachable, not ready or behind `P` is
    /// waited for; a shard whose every owner keeps history only above `P` fails at once. A node
    /// holding every shard returns at once.
    pub fn wait_for_owners(&self, budget: Duration) -> Result<(), OwnersNotReady> {
        let foreign: Vec<_> = Shard::all().filter(|shard| !self.cluster.owns(*shard)).collect();
        if foreign.is_empty() {
            return Ok(());
        }
        let me = self.cluster.node_id();
        let mut peers: Vec<_> = foreign
            .iter()
            .flat_map(|shard| self.cluster.owners(*shard).iter().copied())
            .filter(|node| *node != me)
            .collect();
        peers.sort_unstable();
        peers.dedup();

        let started = Instant::now();
        let interval = self.client.options().retry_interval;
        let mut warned = false;
        loop {
            let block = Self::finish_checkpoint(&self.env.tx()?)?;
            let owners =
                OwnerStates(peers.iter().map(|node| (*node, self.owner_state(*node))).collect());
            let mut waiting = 0;
            for shard in &foreign {
                let shard_owners: Vec<_> = self
                    .cluster()
                    .owners(*shard)
                    .iter()
                    .copied()
                    .filter(|node| *node != me)
                    .collect();
                if shard_owners.iter().any(|node| owners.serves(*node, block)) {
                    continue;
                }
                if shard_owners.iter().all(|node| owners.below_history(*node, block)) {
                    return Err(OwnersNotReady::BelowHistory { block, shard: shard.0, owners });
                }
                waiting += 1;
            }
            if waiting == 0 {
                return Ok(());
            }
            let waited = started.elapsed();
            if waited + interval > budget {
                return Err(OwnersNotReady::Behind { block, shards: waiting, waited, owners });
            }
            if !warned && waited > ClusterClient::SLOW_WAIT {
                warned = true;
                warn!(
                    block,
                    shards = waiting,
                    owners = %owners,
                    "cluster node waiting for owners to reach its persisted block before executing"
                );
            }
            std::thread::sleep(interval);
        }
    }

    fn owner_state(&self, node: NodeId) -> OwnerState {
        match self.client.status(node) {
            Ok(Some(range)) => OwnerState::Serves(range),
            Ok(None) => OwnerState::Nothing,
            Err(_) => OwnerState::Unreachable,
        }
    }

    /// Sheds in batches until nothing is left. Returns the number of rows deleted.
    pub fn shed(&self) -> Result<u64, DatabaseError> {
        let mut total = 0;
        loop {
            let deleted = self.shed_batch(Self::SHED_BATCH_ROWS)?;
            if deleted == 0 {
                return Ok(total);
            }
            total += deleted;
            info!(deleted = total, "shedding keys this cluster node does not hold");
        }
    }

    /// Sheds on a background thread, logging progress and completion. A failure is logged and
    /// shedding resumes on the next start.
    pub fn spawn_shed(self: &Arc<Self>) -> io::Result<JoinHandle<()>> {
        let node = Arc::clone(self);
        std::thread::Builder::new().name("cluster-shed".into()).spawn(move || match node.shed() {
            Ok(deleted) => info!(deleted, "cluster shed complete"),
            Err(error) => error!(error = %error, "cluster shed failed; it resumes on restart"),
        })
    }
}
