//! Writing clustered nodes' state databases directly from a stopped full node's database, keeping
//! for each node only the rows it holds, in one pass over the source.

use std::{
    borrow::Cow,
    collections::HashSet,
    fmt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use alloy_primitives::Bytes;
use reth_db::{
    DatabaseEnv, DatabaseEnvKind, DatabaseError, create_db, is_database_empty,
    lockfile::StorageLock,
    mdbx::{
        Cursor, DatabaseArguments, MaxReadTransactionDuration, RO, RW, SyncMode, Transaction,
        WriteFlags, ffi::MDBX_dbi,
    },
    table::Table,
    tables::{self, Tables},
};
use reth_stages_types::StageId;
use thiserror::Error;
use tracing::info;

use crate::{Cluster, KeyPlacement, NodeId, ShardedTable, StorageCounter, StorageReport};

/// Why a bootstrap or a storage report failed or was refused.
#[derive(Debug, Error)]
pub enum BootstrapError {
    /// No destination was given.
    #[error("no destination to bootstrap")]
    NoDestination,
    /// Two destinations are the same path.
    #[error("destination {0} is given more than once")]
    DuplicateDestination(PathBuf),
    /// Two destinations are for the same node.
    #[error("node {0:?} is bootstrapped more than once")]
    DuplicateNode(NodeId),
    /// A destination's cluster config differs from the first one in more than `node_id` and
    /// `apply_from`, so the nodes would not agree on placement.
    #[error(
        "the cluster config of node {0:?} differs from the first in more than node_id and apply_from"
    )]
    ConfigMismatch(NodeId),
    /// A destination database exists and is not empty.
    #[error("destination database {0} exists and is not empty")]
    DestinationNotEmpty(PathBuf),
    /// A live process holds the source database's lock: the source node is running.
    #[error("the source database is in use (stop the source node first): {0}")]
    SourceInUse(String),
    /// The source has no `Finish` stage checkpoint: it is not an initialised full node.
    #[error("the source database has no Finish stage checkpoint")]
    NoFinishCheckpoint,
    /// A destination database could not be created.
    #[error("failed to create destination database {path}: {reason}")]
    CreateDestination {
        /// The destination.
        path: PathBuf,
        /// Why it failed.
        reason: String,
    },
    /// A row of a sharded table cannot be placed: its key, or a split contract's subkey, is not
    /// a valid encoding for the table.
    #[error("a row of {table} with key {key} cannot be placed")]
    InvalidRow {
        /// The table.
        table: &'static str,
        /// The row's encoded key.
        key: Bytes,
    },
    /// A database operation failed.
    #[error(transparent)]
    Database(#[from] DatabaseError),
    /// An MDBX operation failed.
    #[error("mdbx: {0}")]
    Mdbx(#[from] reth_db::mdbx::Error),
}

/// When the destinations commit: once the rows, or the encoded bytes, written to all of them
/// together since the last commit reach a limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimit {
    /// Rows written to all destinations since the last commit.
    pub rows: u64,
    /// Key and value bytes written to all destinations since the last commit.
    pub bytes: u64,
}

impl BatchLimit {
    /// 100 000 rows or 256 mebibytes: short enough to bound the memory of a write transaction's dirty
    /// pages, long enough that commits do not dominate.
    pub const DEFAULT: Self = Self { rows: 100_000, bytes: 256 << 20 };
}

/// One node to bootstrap: its cluster view and the database directory to create.
#[derive(Debug)]
pub struct BootstrapTarget {
    /// The node's cluster view; its `node_id` decides the rows kept.
    pub cluster: Cluster,
    /// The database directory (`<datadir>/db`) to create.
    pub destination: PathBuf,
}

/// Rows and their key and value bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowCount {
    /// Rows.
    pub rows: u64,
    /// Encoded key and value bytes.
    pub bytes: u64,
}

impl RowCount {
    const fn add(&mut self, key: &[u8], value: &[u8]) {
        self.rows += 1;
        self.bytes += (key.len() + value.len()) as u64;
    }
}

/// What one table's copy read from the source and wrote to each destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCopy {
    /// The table's name.
    pub table: &'static str,
    /// Whether the table is sharded (filtered by placement) rather than copied in full.
    pub sharded: bool,
    /// Rows read from the source.
    pub read: RowCount,
    /// Rows written to each destination, in target order.
    pub written: Vec<RowCount>,
}

/// The outcome of a bootstrap.
#[derive(Debug, Clone)]
pub struct BootstrapReport {
    /// The node of each destination, in target order.
    pub nodes: Vec<NodeId>,
    /// The destinations, in target order.
    pub destinations: Vec<PathBuf>,
    /// Every table of the schema, in schema order.
    pub tables: Vec<TableCopy>,
    /// The source's contracts with the most storage rows.
    pub storage: StorageReport,
    /// Time the whole bootstrap took.
    pub elapsed: Duration,
}

/// A summary per destination: each sharded table's rows and bytes kept against the source, and
/// the unsharded tables together; then the elapsed time and the storage report.
impl fmt::Display for BootstrapReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, (node, destination)) in self.nodes.iter().zip(&self.destinations).enumerate() {
            writeln!(f, "node {} -> {}", node.0, destination.display())?;
            let mut unsharded = (RowCount::default(), RowCount::default());
            for table in &self.tables {
                let written = table.written[index];
                if table.sharded {
                    writeln!(
                        f,
                        "  {:<16} rows {:>14} of {:>14} ({:>6.2}%), bytes {:>16} of {:>16} ({:>6.2}%)",
                        table.table,
                        written.rows,
                        table.read.rows,
                        Self::percent(written.rows, table.read.rows),
                        written.bytes,
                        table.read.bytes,
                        Self::percent(written.bytes, table.read.bytes),
                    )?;
                } else {
                    unsharded.0.rows += table.read.rows;
                    unsharded.0.bytes += table.read.bytes;
                    unsharded.1.rows += written.rows;
                    unsharded.1.bytes += written.bytes;
                }
            }
            writeln!(
                f,
                "  {:<16} rows {:>14} of {:>14}, bytes {:>16} of {:>16}",
                "unsharded",
                unsharded.1.rows,
                unsharded.0.rows,
                unsharded.1.bytes,
                unsharded.0.bytes,
            )?;
        }
        writeln!(f, "elapsed {:.1?}", self.elapsed)?;
        write!(f, "{}", self.storage)
    }
}

impl BootstrapReport {
    fn percent(part: u64, whole: u64) -> f64 {
        if whole == 0 { 100.0 } else { part as f64 * 100.0 / whole as f64 }
    }
}

/// Writes clustered nodes' state databases from a stopped full node's database in one pass: every
/// table of reth's schema, unsharded tables in full, sharded tables filtered by each node's
/// placement. The result for each node equals a full copy on which shed has finished.
#[derive(Debug)]
pub struct Bootstrap {
    targets: Vec<BootstrapTarget>,
    batch: BatchLimit,
    arguments: DatabaseArguments,
}

impl Bootstrap {
    /// Rows of one table between progress logs.
    pub const PROGRESS_ROWS: u64 = 10_000_000;

    /// A bootstrap of `targets`, refused when there are none, when two share a destination or a
    /// node, or when their configs differ in more than `node_id` and `apply_from`.
    pub fn new(targets: Vec<BootstrapTarget>) -> Result<Self, BootstrapError> {
        let first = targets.first().ok_or(BootstrapError::NoDestination)?;
        let mut destinations = HashSet::new();
        let mut nodes = HashSet::new();
        for target in &targets {
            if !destinations.insert(&target.destination) {
                return Err(BootstrapError::DuplicateDestination(target.destination.clone()));
            }
            let node = target.cluster.node_id();
            if !nodes.insert(node) {
                return Err(BootstrapError::DuplicateNode(node));
            }
            if !Self::same_placement(&first.cluster, &target.cluster) {
                return Err(BootstrapError::ConfigMismatch(node));
            }
        }
        Ok(Self {
            targets,
            batch: BatchLimit::DEFAULT,
            arguments: DatabaseArguments::new(Default::default()),
        })
    }

    /// Whether two views share everything but `node_id` and `apply_from`.
    fn same_placement(a: &Cluster, b: &Cluster) -> bool {
        a.config().nodes == b.config().nodes
            && a.config().replication == b.config().replication
            && a.config().retention_blocks == b.config().retention_blocks
            && a.split_storage() == b.split_storage()
    }

    /// Commits the destinations at `batch` instead of [`BatchLimit::DEFAULT`].
    pub const fn with_batch_limit(mut self, batch: BatchLimit) -> Self {
        self.batch = batch;
        self
    }

    /// Opens the source and creates the destinations with `arguments` (geometry, page size)
    /// instead of reth's defaults. The source's read-transaction limit and the destinations'
    /// sync mode are set by the bootstrap.
    pub fn with_database_arguments(mut self, arguments: DatabaseArguments) -> Self {
        self.arguments = arguments;
        self
    }

    /// Copies the database at `source` (`<datadir>/db` of a stopped full node) into every
    /// target's destination. Refuses when a destination is not empty, when a live process holds
    /// the source's lock, or when the source has no `Finish` checkpoint; then nothing is
    /// created. The source's lock is held during the copy and no source table is written.
    pub fn run(&self, source: &Path) -> Result<BootstrapReport, BootstrapError> {
        let started = Instant::now();
        if let Some(target) =
            self.targets.iter().find(|target| !is_database_empty(&target.destination))
        {
            return Err(BootstrapError::DestinationNotEmpty(target.destination.clone()));
        }
        let source_arguments = self
            .arguments
            .clone()
            .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));
        let source_env = DatabaseEnv::open(source, DatabaseEnvKind::RO, source_arguments)?;
        let _source_lock = StorageLock::try_acquire(source)
            .map_err(|error| BootstrapError::SourceInUse(error.to_string()))?;
        let txn = source_env.begin_ro_txn()?;
        let checkpoints = txn.open_db(Some(tables::StageCheckpoints::NAME))?.dbi();
        if txn.get::<()>(checkpoints, StageId::Finish.to_string().as_bytes())?.is_none() {
            return Err(BootstrapError::NoFinishCheckpoint);
        }

        let destination_arguments =
            self.arguments.clone().with_sync_mode(Some(SyncMode::SafeNoSync));
        let destinations = self
            .targets
            .iter()
            .map(|target| {
                let mut env = create_db(&target.destination, destination_arguments.clone())
                    .map_err(|error| BootstrapError::CreateDestination {
                        path: target.destination.clone(),
                        reason: error.to_string(),
                    })?;
                env.create_tables()?;
                Ok(env)
            })
            .collect::<Result<Vec<_>, BootstrapError>>()?;
        info!(
            nodes = ?self.targets.iter().map(|target| target.cluster.node_id().0).collect::<Vec<_>>(),
            source = %source.display(),
            "cluster bootstrap started"
        );

        let mut storage = StorageCounter::new(StorageReport::DEFAULT_TOP);
        let tables = Tables::ALL
            .iter()
            .map(|table| self.copy_table(&txn, &destinations, *table, &mut storage))
            .collect::<Result<Vec<_>, _>>()?;
        for env in &destinations {
            env.sync(true)?;
        }
        let report = BootstrapReport {
            nodes: self.targets.iter().map(|target| target.cluster.node_id()).collect(),
            destinations: self.targets.iter().map(|target| target.destination.clone()).collect(),
            tables,
            storage: storage.finish(),
            elapsed: started.elapsed(),
        };
        info!(elapsed = ?report.elapsed, "cluster bootstrap finished");
        Ok(report)
    }

    /// Copies `table` from the source transaction to every destination, in source order, in
    /// append mode, committing all destinations together at the batch limit. Keeps a row of a
    /// sharded table only on the destinations whose node holds it.
    fn copy_table(
        &self,
        source: &Transaction<RO>,
        destinations: &[DatabaseEnv],
        table: Tables,
        storage: &mut StorageCounter,
    ) -> Result<TableCopy, BootstrapError> {
        let name = table.name();
        let sharded = ShardedTable::from_table_name(name);
        let counts_storage = sharded == Some(ShardedTable::HashedStorages);
        let dupsort = table.is_dupsort();
        let placement = &self.targets[0].cluster;
        let mut copy = TableCopy {
            table: name,
            sharded: sharded.is_some(),
            read: RowCount::default(),
            written: vec![RowCount::default(); destinations.len()],
        };
        info!(table = name, "copying table");

        let mut reader = source.cursor(source.open_db(Some(name))?.dbi())?;
        let mut txns =
            destinations.iter().map(|env| env.begin_rw_txn()).collect::<Result<Vec<_>, _>>()?;
        let dbis = txns
            .iter()
            .map(|txn| txn.open_db(Some(name)).map(|db| db.dbi()))
            .collect::<Result<Vec<_>, _>>()?;
        let mut writers = Self::writers(&txns, &dbis)?;
        let mut pending = RowCount::default();

        let mut row = reader.first::<Cow<'_, [u8]>, Cow<'_, [u8]>>()?;
        while let Some((key, value)) = row {
            copy.read.add(&key, &value);
            let placed = match sharded {
                None => KeyPlacement::Replicated,
                Some(sharded) => {
                    let subkey = sharded.subkey_of(&value).unwrap_or_default();
                    placement.placement(sharded, &key, subkey).ok_or_else(|| {
                        BootstrapError::InvalidRow {
                            table: name,
                            key: Bytes::copy_from_slice(&key),
                        }
                    })?
                }
            };
            if counts_storage {
                storage.count(StorageCounter::address(&key)?);
            }
            for ((target, writer), written) in
                self.targets.iter().zip(&mut writers).zip(&mut copy.written)
            {
                let holds = match placed {
                    KeyPlacement::Replicated => true,
                    KeyPlacement::Shard(shard) => target.cluster.owns(shard),
                };
                if holds {
                    let append = if dupsort { WriteFlags::APPEND_DUP } else { WriteFlags::APPEND };
                    writer.put(&key, &value, append)?;
                    written.add(&key, &value);
                    pending.add(&key, &value);
                }
            }
            if pending.rows >= self.batch.rows || pending.bytes >= self.batch.bytes {
                drop(writers);
                for txn in txns {
                    txn.commit()?;
                }
                txns = destinations
                    .iter()
                    .map(|env| env.begin_rw_txn())
                    .collect::<Result<Vec<_>, _>>()?;
                writers = Self::writers(&txns, &dbis)?;
                pending = RowCount::default();
            }
            if copy.read.rows.is_multiple_of(Self::PROGRESS_ROWS) {
                info!(
                    table = name,
                    rows_read = copy.read.rows,
                    rows_written = ?copy.written.iter().map(|written| written.rows).collect::<Vec<_>>(),
                    "copying table"
                );
            }
            row = reader.next()?;
        }
        drop(writers);
        for txn in txns {
            txn.commit()?;
        }
        info!(
            table = name,
            rows_read = copy.read.rows,
            rows_written = ?copy.written.iter().map(|written| written.rows).collect::<Vec<_>>(),
            "table copied"
        );
        Ok(copy)
    }

    /// A cursor of each destination's write transaction on its table `dbis[i]`.
    fn writers(
        txns: &[Transaction<RW>],
        dbis: &[MDBX_dbi],
    ) -> Result<Vec<Cursor<RW>>, BootstrapError> {
        Ok(txns.iter().zip(dbis).map(|(txn, dbi)| txn.cursor(*dbi)).collect::<Result<_, _>>()?)
    }
}
