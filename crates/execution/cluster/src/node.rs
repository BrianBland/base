//! This node's side of a cluster: its owner store and server, its peer client, persistence
//! points, the commit guard, and shedding.

use std::{
    fmt, io,
    net::TcpListener,
    sync::{Arc, OnceLock},
    thread::JoinHandle,
};

use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use parking_lot::Mutex;
use reth_db::{
    Database, DatabaseEnv, DatabaseError,
    cursor::DbCursorRO,
    mdbx::{RO, RW, TransactionKind, tx::Tx},
    table::Table,
    tables::{self, RawKey, RawTable},
    transaction::{DbTx, DbTxMut},
};
use reth_stages_types::StageId;
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{
    ClientOptions, Cluster, ClusterClient, KeyPlacement, OwnerServer, OwnerStore, RemoteReadCounts,
    RemoteReadStats, Shard, ShardedTable,
};

/// Returns the hash of the canonical block with the given number, when known.
pub type BlockHashLookup = Box<dyn Fn(u64) -> Option<B256> + Send + Sync>;

/// Why a clustered database could not be opened.
#[derive(Debug, Error)]
pub enum ClusterStartError {
    /// The owner server could not be started.
    #[error("failed to start the cluster owner server: {0}")]
    Server(#[from] io::Error),
    /// The thread logging remote reads could not be started.
    #[error("failed to start the cluster read stats logger: {0}")]
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
    owner_store: Arc<OwnerStore<Tx<RO>>>,
    server: OwnerServer,
    block_hashes: OnceLock<BlockHashLookup>,
    /// `Finish` of the last commit.
    committed_finish: Mutex<u64>,
    /// Block of the last persistence point registered.
    registered_point: Mutex<Option<u64>>,
    remote_reads: Arc<RemoteReadStats>,
}

impl fmt::Debug for ClusterNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterNode")
            .field("node_id", &self.cluster.node_id())
            .field("owner_server", &self.server.local_addr())
            .field("committed_finish", &*self.committed_finish.lock())
            .field("registered_point", &*self.registered_point.lock())
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
        let owner_store = Arc::new(OwnerStore::new(Arc::clone(&cluster), retention));
        let server = OwnerServer::spawn(listener, Arc::clone(&owner_store))?;
        let remote_reads = Arc::new(RemoteReadStats::default());
        remote_reads
            .spawn_logger(RemoteReadStats::LOG_INTERVAL)
            .map_err(ClusterStartError::StatsLogger)?;
        let owned = Shard::all().filter(|shard| cluster.owns(*shard)).count();
        info!(
            node_id = cluster.node_id().0,
            owned_shards = owned,
            owner_server = %server.local_addr(),
            persisted_tip = finish,
            "cluster node started"
        );
        Ok(Self {
            client: ClusterClient::new(Arc::clone(&cluster), options),
            env,
            cluster,
            owner_store,
            server,
            block_hashes: OnceLock::new(),
            committed_finish: Mutex::new(finish),
            registered_point: Mutex::new(None),
            remote_reads,
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
    pub const fn owner_store(&self) -> &Arc<OwnerStore<Tx<RO>>> {
        &self.owner_store
    }

    /// The running owner server.
    pub const fn server(&self) -> &OwnerServer {
        &self.server
    }

    /// Whether this node holds the encoded `key` of `table`. A key that is not a valid encoded
    /// key of the table is an error: a clustered database cannot place it.
    pub fn holds(&self, table: ShardedTable, key: &[u8]) -> Result<bool, DatabaseError> {
        self.cluster.holds(table, key).ok_or_else(|| {
            DatabaseError::Other(format!("key of length {} is not a {table:?} key", key.len()))
        })
    }

    /// The counters of this node's remote reads.
    pub const fn remote_read_stats(&self) -> &Arc<RemoteReadStats> {
        &self.remote_reads
    }

    /// This node's remote reads since it started.
    pub fn remote_reads(&self) -> RemoteReadCounts {
        self.remote_reads.counts()
    }

    /// Installs the block-hash lookup persistence points are registered with. Only the first
    /// lookup installed is kept; returns whether this one was.
    pub fn install_block_hashes(&self, lookup: BlockHashLookup) -> bool {
        self.block_hashes.set(lookup).is_ok()
    }

    /// The hash of block `number` from the installed lookup, if any.
    pub fn block_hash(&self, number: u64) -> Option<B256> {
        self.block_hashes.get().and_then(|lookup| lookup(number))
    }

    /// The `Finish` stage checkpoint visible in `tx`: the block its state tables are at.
    pub fn finish_checkpoint<K: TransactionKind>(tx: &Tx<K>) -> Result<u64, DatabaseError> {
        Self::stage_checkpoint(tx, StageId::Finish)
    }

    fn stage_checkpoint<K: TransactionKind>(
        tx: &Tx<K>,
        stage: StageId,
    ) -> Result<u64, DatabaseError> {
        let checkpoint = tx.get::<tables::StageCheckpoints>(stage.to_string())?;
        Ok(checkpoint.map(|checkpoint| checkpoint.block_number).unwrap_or_default())
    }

    /// Commits `tx` unless it would run the pipeline or unwind persisted blocks, then registers a
    /// persistence point when `Finish` moved.
    pub fn commit<K: TransactionKind>(&self, tx: Tx<K>) -> Result<(), DatabaseError> {
        let finish = Self::finish_checkpoint(&tx)?;
        if let Some(refusal) = self.commit_refusal(&tx, finish)? {
            error!(error = %refusal, "refusing a clustered database commit");
            tx.abort();
            return Err(refusal.into());
        }
        tx.commit()?;
        *self.committed_finish.lock() = finish;
        if let Err(error) = self.register_persistence_point() {
            warn!(error = %error, "failed to register a cluster persistence point");
        }
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

    /// Registers a raw read snapshot of the database with the owner store at its `Finish` block,
    /// unless that block is already registered or its hash is unknown. Returns the block
    /// registered.
    pub fn register_persistence_point(&self) -> Result<Option<BlockNumHash>, DatabaseError> {
        let Some(lookup) = self.block_hashes.get() else { return Ok(None) };
        let mut registered = self.registered_point.lock();
        let tx = self.env.tx()?;
        let number = Self::finish_checkpoint(&tx)?;
        if *registered == Some(number) {
            return Ok(None);
        }
        let Some(hash) = lookup(number) else {
            warn!(block = number, "no hash for the persisted tip; persistence point skipped");
            return Ok(None);
        };
        let block = BlockNumHash::new(number, hash);
        self.owner_store.register_persistence_point(block, tx);
        *registered = Some(number);
        Ok(Some(block))
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
        tx.commit()?;
        Ok(max_rows - budget)
    }

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
            let mut walker = cursor.walk(Some(RawKey::from_vec(start)))?;
            while *budget > 0 {
                let Some(entry) = walker.next() else { break };
                let key = entry?.0.into_key();
                match table.placement(&key) {
                    Some(KeyPlacement::Shard(found)) if found == shard => {
                        walker.delete_current()?;
                        *budget -= 1;
                    }
                    Some(KeyPlacement::Replicated) if key[0] == shard.0 => {}
                    _ => break,
                }
            }
            if *budget == 0 {
                break;
            }
        }
        Ok(())
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
