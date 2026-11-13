//! Writing clustered nodes' state databases directly from a stopped full node's database, keeping
//! for each node only the rows it holds, in one pass over the source.

use std::{
    collections::HashSet,
    fmt,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use alloy_primitives::Bytes;
use reth_db::{
    DatabaseEnv, DatabaseEnvKind, DatabaseError, create_db, is_database_empty,
    lockfile::StorageLock,
    mdbx::{DatabaseArguments, MaxReadTransactionDuration, RO, SyncMode, Transaction, WriteFlags},
    table::Table,
    tables::{self, Tables},
};
use reth_stages_types::StageId;
use thiserror::Error;
use tracing::info;

use crate::{
    Cluster, KeyPlacement, NodeId, OrderedPartitions, PartitionConsumer, PartitionRows,
    ReadParallelism, Shard, ShardedTable, StorageCounter, StorageReport, TablePartitions,
};

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

/// When a destination commits: once the rows, or the encoded bytes, written to it since its last
/// commit reach a limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimit {
    /// Rows written to the destination since its last commit.
    pub rows: u64,
    /// Key and value bytes written to the destination since its last commit.
    pub bytes: u64,
}

impl BatchLimit {
    /// 100 000 rows or 256 mebibytes: short enough to bound the memory of a write transaction's dirty
    /// pages, long enough that commits do not dominate.
    pub const DEFAULT: Self = Self { rows: 100_000, bytes: 256 << 20 };
}

/// Consecutive rows of one table read from the source, each with where it lives: what the reader
/// hands every destination's writer at once, shared.
#[derive(Debug, Default)]
pub struct RowBatch {
    /// The rows' keys and values, back to back.
    bytes: Vec<u8>,
    /// For each row, the end offsets of its key and value in `bytes`, and its placement.
    rows: Vec<(usize, usize, KeyPlacement)>,
}

impl RowBatch {
    /// Rows at which a batch is full.
    pub const MAX_ROWS: usize = 16 * 1024;
    /// Key and value bytes at which a batch is full; a single larger row makes a batch of its own.
    pub const MAX_BYTES: usize = 1 << 20;

    /// An empty batch with room for a full one.
    pub fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(Self::MAX_BYTES),
            rows: Vec::with_capacity(Self::MAX_ROWS),
        }
    }

    /// Adds a row at the end.
    pub fn push(&mut self, key: &[u8], value: &[u8], placement: KeyPlacement) {
        self.bytes.extend_from_slice(key);
        let key_end = self.bytes.len();
        self.bytes.extend_from_slice(value);
        self.rows.push((key_end, self.bytes.len(), placement));
    }

    /// Whether the batch holds [`Self::MAX_ROWS`] rows or [`Self::MAX_BYTES`] bytes.
    pub const fn is_full(&self) -> bool {
        self.rows.len() >= Self::MAX_ROWS || self.bytes.len() >= Self::MAX_BYTES
    }

    /// Whether the batch holds no row.
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The rows in order: key, value, and placement.
    pub fn rows(&self) -> impl Iterator<Item = (&[u8], &[u8], KeyPlacement)> {
        let starts = std::iter::once(0).chain(self.rows.iter().map(|(_, value_end, _)| *value_end));
        starts.zip(&self.rows).map(|(start, (key_end, value_end, placement))| {
            (&self.bytes[start..*key_end], &self.bytes[*key_end..*value_end], *placement)
        })
    }
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
    read: ReadParallelism,
}

impl Bootstrap {
    /// Rows of one table between progress logs.
    pub const PROGRESS_ROWS: u64 = 10_000_000;
    /// Row batches queued for each destination's writer: with [`RowBatch::MAX_BYTES`], bounds the
    /// rows in flight between the dispatching thread and the writers.
    pub const QUEUED_BATCHES: usize = 8;

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
            read: ReadParallelism::DEFAULT,
        })
    }

    /// Whether two views share everything but `node_id` and `apply_from`.
    fn same_placement(a: &Cluster, b: &Cluster) -> bool {
        a.config().nodes == b.config().nodes
            && a.config().replication == b.config().replication
            && a.config().retention_blocks == b.config().retention_blocks
            && a.split_storage() == b.split_storage()
    }

    /// Commits each destination at `batch` instead of [`BatchLimit::DEFAULT`].
    pub const fn with_batch_limit(mut self, batch: BatchLimit) -> Self {
        self.batch = batch;
        self
    }

    /// Reads the source with `read` instead of [`ReadParallelism::DEFAULT`].
    pub const fn with_read_parallelism(mut self, read: ReadParallelism) -> Self {
        self.read = read;
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
            .map(|table| self.copy_table(&source_env, &txn, &destinations, *table, &mut storage))
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

    /// Copies `table` from the source to every destination, in source order: a pool of reader
    /// threads reads and places the table's partitions, this thread hands their rows on in
    /// partition order, and one writer thread per destination appends the rows its node holds.
    /// The first error of a writer, else of a reader, else of this thread is returned; any
    /// failure or panic stops every thread of the table, and a panic is resumed here.
    fn copy_table(
        &self,
        source: &DatabaseEnv,
        txn: &Transaction<RO>,
        destinations: &[DatabaseEnv],
        table: Tables,
        storage: &mut StorageCounter,
    ) -> Result<TableCopy, BootstrapError> {
        let name = table.name();
        let sharded = ShardedTable::from_table_name(name);
        let split_keys: Vec<_> = match sharded {
            Some(sharded) if sharded.is_duplicate() => {
                let mut keys: Vec<_> =
                    self.targets[0].cluster.split_storage().iter().copied().collect();
                keys.sort_unstable();
                keys
            }
            _ => Vec::new(),
        };
        let dbi = txn.open_db(Some(name))?.dbi();
        let partitions = OrderedPartitions::new(
            TablePartitions::plan(txn, dbi, &split_keys, self.read.partition_bytes)?,
            self.read.window,
            self.read.queued_batches,
        );
        let readers = self.read.threads.get().min(partitions.len());
        info!(table = name, partitions = partitions.len(), readers, "copying table");
        let (read, readers, written) = thread::scope(|scope| {
            let (senders, writers): (Vec<_>, Vec<_>) = destinations
                .iter()
                .zip(&self.targets)
                .map(|(env, target)| {
                    let (sender, batches) = mpsc::sync_channel(Self::QUEUED_BATCHES);
                    let writer =
                        scope.spawn(move || self.write_table(env, &target.cluster, table, batches));
                    (sender, writer)
                })
                .unzip();
            let readers: Vec<_> = (0..readers)
                .map(|_| {
                    scope.spawn(|| {
                        let read = panic::catch_unwind(AssertUnwindSafe(|| {
                            self.read_partitions(source, name, sharded, &partitions)
                        }));
                        if !matches!(read, Ok(Ok(()))) {
                            partitions.stop();
                        }
                        read.unwrap_or_else(|panic| panic::resume_unwind(panic))
                    })
                })
                .collect();
            let counted = (sharded == Some(ShardedTable::HashedStorages)).then_some(storage);
            let read = self.dispatch(partitions.consumer(), name, counted, &senders);
            drop(senders);
            let readers = Self::join_all(readers);
            let written = Self::join_all(writers);
            (read, readers, written)
        });
        let written = written?;
        readers?;
        let copy = TableCopy { table: name, sharded: sharded.is_some(), written, read: read? };
        info!(
            table = name,
            rows_read = copy.read.rows,
            rows_written = ?copy.written.iter().map(|written| written.rows).collect::<Vec<_>>(),
            "table copied"
        );
        Ok(copy)
    }

    /// Joins `threads`, resuming the first panic, and returns their outcomes in order or the
    /// first error.
    fn join_all<T>(
        threads: Vec<thread::ScopedJoinHandle<'_, Result<T, BootstrapError>>>,
    ) -> Result<Vec<T>, BootstrapError> {
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap_or_else(|panic| panic::resume_unwind(panic)))
            .collect()
    }

    /// Claims partitions of table `name` until none is left, and reads each from its own read
    /// transaction of the source: places each row of a sharded table and sends the rows in
    /// batches to the partition's queue, then its end. Returns early, without error, when the
    /// reading stops or the consumer closes a queue.
    fn read_partitions(
        &self,
        source: &DatabaseEnv,
        name: &'static str,
        sharded: Option<ShardedTable>,
        partitions: &OrderedPartitions,
    ) -> Result<(), BootstrapError> {
        let placement = &self.targets[0].cluster;
        let txn = source.begin_ro_txn()?;
        let mut cursor = txn.cursor(txn.open_db(Some(name))?.dbi())?;
        while let Some(partition) = partitions.claim() {
            let mut batch = RowBatch::default();
            let mut row = partition.range.start.seek(&mut cursor)?;
            while let Some((key, value)) = row {
                if partition.range.is_passed_by(&key, &value) {
                    break;
                }
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
                batch.push(&key, &value, placed);
                if batch.is_full() {
                    let full = std::mem::replace(&mut batch, RowBatch::new());
                    if partitions.is_stopped()
                        || partition.rows.send(PartitionRows::Batch(full)).is_err()
                    {
                        return Ok(());
                    }
                }
                row = cursor.next()?;
            }
            let last = (!batch.is_empty()).then_some(PartitionRows::Batch(batch));
            for message in last.into_iter().chain([PartitionRows::End]) {
                if partition.rows.send(message).is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Takes the partitions of table `name` from `partitions` in order and sends each batch of
    /// rows to every writer, counting the rows read and, when `storage` is given, each
    /// `HashedStorages` row by contract. Stops early, without error, when a writer has stopped
    /// (that writer's outcome is the copy's) or a partition ends before its end is sent (a
    /// reader stopped, and its outcome is the copy's).
    fn dispatch(
        &self,
        mut partitions: PartitionConsumer<'_>,
        name: &'static str,
        mut storage: Option<&mut StorageCounter>,
        writers: &[SyncSender<Arc<RowBatch>>],
    ) -> Result<RowCount, BootstrapError> {
        let mut read = RowCount::default();
        while let Some(partition) = partitions.next_partition() {
            loop {
                let batch = match partition.recv() {
                    Ok(PartitionRows::Batch(batch)) => batch,
                    Ok(PartitionRows::End) => break,
                    Err(_) => return Ok(read),
                };
                if let Some(storage) = storage.as_deref_mut() {
                    for (key, _, _) in batch.rows() {
                        storage.count(StorageCounter::address(key)?);
                    }
                }
                let before = read.rows;
                read.rows += batch.rows.len() as u64;
                read.bytes += batch.bytes.len() as u64;
                if before / Self::PROGRESS_ROWS != read.rows / Self::PROGRESS_ROWS {
                    info!(table = name, rows_read = read.rows, "copying table");
                }
                let batch = Arc::new(batch);
                if !writers.iter().all(|writer| writer.send(Arc::clone(&batch)).is_ok()) {
                    return Ok(read);
                }
            }
        }
        Ok(read)
    }

    /// Appends the rows of `table` that `cluster`'s node holds, from `batches` until the reader
    /// closes them, to the destination `env` in append mode, committing at the batch limit and
    /// at the end.
    fn write_table(
        &self,
        env: &DatabaseEnv,
        cluster: &Cluster,
        table: Tables,
        batches: Receiver<Arc<RowBatch>>,
    ) -> Result<RowCount, BootstrapError> {
        let mut owned = [false; Shard::COUNT];
        for shard in Shard::all() {
            owned[usize::from(shard.0)] = cluster.owns(shard);
        }
        let dupsort = table.is_dupsort();
        let mut txn = env.begin_rw_txn()?;
        let dbi = txn.open_db(Some(table.name()))?.dbi();
        let mut cursor = txn.cursor(dbi)?;
        let mut written = RowCount::default();
        let mut pending = RowCount::default();
        for batch in batches {
            for (key, value, placement) in batch.rows() {
                let holds = match placement {
                    KeyPlacement::Replicated => true,
                    KeyPlacement::Shard(shard) => owned[usize::from(shard.0)],
                };
                if !holds {
                    continue;
                }
                let append = if dupsort { WriteFlags::APPEND_DUP } else { WriteFlags::APPEND };
                cursor.put(key, value, append)?;
                written.add(key, value);
                pending.add(key, value);
                if pending.rows >= self.batch.rows || pending.bytes >= self.batch.bytes {
                    drop(cursor);
                    txn.commit()?;
                    txn = env.begin_rw_txn()?;
                    cursor = txn.cursor(dbi)?;
                    pending = RowCount::default();
                }
            }
        }
        drop(cursor);
        txn.commit()?;
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_returns_its_rows_in_order_and_fills_at_its_limits() {
        let mut batch = RowBatch::new();
        assert!(batch.is_empty());
        let row = |index: usize| {
            let key = index.to_be_bytes().to_vec();
            let value = vec![index as u8; index % 5];
            let placement = if index.is_multiple_of(3) {
                KeyPlacement::Replicated
            } else {
                KeyPlacement::Shard(Shard(index as u8))
            };
            (key, value, placement)
        };
        let mut pushed = 0;
        while !batch.is_full() {
            let (key, value, placement) = row(pushed);
            batch.push(&key, &value, placement);
            pushed += 1;
        }
        assert_eq!(pushed, RowBatch::MAX_ROWS);
        let rows: Vec<_> = batch.rows().map(|(k, v, p)| (k.to_vec(), v.to_vec(), p)).collect();
        assert_eq!(rows, (0..pushed).map(row).collect::<Vec<_>>());

        let mut large = RowBatch::new();
        large.push(&[1], &vec![7; RowBatch::MAX_BYTES], KeyPlacement::Replicated);
        assert!(large.is_full(), "a row of the byte limit fills a batch on its own");
        assert_eq!(large.rows().count(), 1);
    }
}
