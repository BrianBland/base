//! Bootstrapping a clustered node's database from a full node's: equivalence with a full copy
//! that has been shed, batching, refusals, and the storage report.

use std::{
    collections::BTreeMap,
    net::TcpListener,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy_primitives::{B256, Bytes, U256, keccak256};
use base_execution_cluster::{
    BatchLimit, Bootstrap, BootstrapError, BootstrapTarget, ClientOptions, Cluster, ClusterConfig,
    ClusteredDatabase, NodeConfig, NodeId, ReadParallelism, RowBatch, Shard, StorageReport,
};
use rand::{Rng, rngs::StdRng};
use reth_chainspec::MAINNET;
use reth_db::{
    ClientVersion, DatabaseEnv, DatabaseEnvKind, init_db,
    mdbx::{DatabaseArguments, WriteFlags},
};
use reth_db_api::{
    Database,
    models::StorageSettings,
    table::Table,
    tables::{self, RawKey, RawTable, RawValue, Tables},
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_provider::{
    ProviderFactory, StageCheckpointWriter, StateWriter, StorageSettingsCache, TrieWriter,
    providers::{RocksDBBuilder, StaticFileProvider},
    test_utils::MockNodeTypesWithDB,
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_trie::{
    HashedPostStateSorted, HashedStorageSorted, Nibbles,
    updates::{StorageTrieUpdatesSorted, TrieUpdatesSorted},
};
use tempfile::TempDir;

use crate::common::{KeyUniverse, random_node, random_updates, rng};

/// Storing nodes are 1..=4; node 5 stores nothing.
const NODES: u64 = 5;
/// Largest size of a test MDBX: each environment reserves this much address space.
const DB_MAX_SIZE: usize = 1 << 30;
/// Slots and storage-trie paths of the large contract, spread over every range.
const LARGE_SLOTS: usize = 400;
const LARGE_PATHS: usize = 120;

/// One reader thread, as a single walk of the source.
const ONE_READER: ReadParallelism = ReadParallelism::with_threads(NonZeroUsize::MIN);

/// Several reader threads, a window smaller than the threads, queues of two batches, and
/// partitions of a fraction of a page, so that readers block and partitions interleave.
const READERS: ReadParallelism = ReadParallelism {
    threads: NonZeroUsize::new(4).unwrap(),
    window: NonZeroUsize::new(3).unwrap(),
    queued_batches: 2,
    partition_bytes: NonZeroU64::new(256).unwrap(),
};

/// Every reading of the source the equivalence tests run.
const READINGS: [ReadParallelism; 2] = [ONE_READER, READERS];

fn arguments() -> DatabaseArguments {
    DatabaseArguments::new(ClientVersion::default()).with_geometry_max_size(Some(DB_MAX_SIZE))
}

/// Every row of every table of the schema, as raw bytes, by table name.
type FullDump = BTreeMap<&'static str, Vec<(Vec<u8>, Vec<u8>)>>;

fn dump(env: &DatabaseEnv) -> FullDump {
    let txn = env.begin_ro_txn().unwrap();
    Tables::ALL
        .iter()
        .map(|table| {
            let db = txn.open_db(Some(table.name())).unwrap();
            let mut cursor = txn.cursor(db.dbi()).unwrap();
            let rows =
                cursor.iter_start::<Vec<u8>, Vec<u8>>().map(|row| row.unwrap()).collect::<Vec<_>>();
            (table.name(), rows)
        })
        .collect()
}

fn dump_path(db: &Path) -> FullDump {
    dump(&DatabaseEnv::open(db, DatabaseEnvKind::RO, arguments()).unwrap())
}

/// A full node's datadir: its `db` holds state of random accounts, a large contract whose slots
/// and storage-trie nodes spread over every range, bytecodes, a few unsharded rows, and stage
/// checkpoints (`Finish` only when `finished`).
struct Source {
    dir: TempDir,
    large_contract: B256,
}

impl Source {
    fn new(seed: u64, finished: bool) -> Self {
        let mut rng = rng(seed);
        let dir = TempDir::new().unwrap();
        let env = init_db(dir.path().join("db"), arguments()).unwrap();
        let factory = ProviderFactory::<MockNodeTypesWithDB<DatabaseEnv>>::new(
            env,
            MAINNET.clone(),
            StaticFileProvider::read_write(dir.path().join("static_files")).unwrap(),
            RocksDBBuilder::new(dir.path().join("rocksdb")).with_default_tables().build().unwrap(),
            reth_tasks::Runtime::test(),
        )
        .unwrap();
        factory.set_storage_settings_cache(StorageSettings::v2());

        let shards: Vec<_> = (0..32).map(|_| Shard(rng.random())).collect();
        let keys = KeyUniverse::new(&mut rng, &shards);
        let large_contract = B256::from(rng.random::<[u8; 32]>());
        let provider = factory.provider_rw().unwrap();
        for _ in 0..3 {
            let updates = random_updates(&mut rng, &keys);
            provider.write_hashed_state(&updates.hashed_state).unwrap();
            provider.write_trie_updates_sorted(&updates.trie_updates).unwrap();
        }
        let (storage, trie) = Self::large_contract_state(&mut rng, large_contract);
        provider.write_hashed_state(&storage).unwrap();
        provider.write_trie_updates_sorted(&trie).unwrap();
        let tx = provider.tx_ref();
        for _ in 0..12 {
            let code: Vec<u8> = (0..rng.random_range(1..60)).map(|_| rng.random()).collect();
            tx.put::<tables::Bytecodes>(keccak256(&code), Bytecode::new_raw(code.into())).unwrap();
        }
        for number in 0..5u64 {
            tx.put::<tables::CanonicalHeaders>(number, keccak256(number.to_be_bytes())).unwrap();
            let address = rng.random::<[u8; 20]>().into();
            let account = Account { nonce: number, ..Default::default() };
            tx.put::<tables::PlainAccountState>(address, account).unwrap();
            for slot in 0..3u8 {
                let entry =
                    StorageEntry { key: B256::with_last_byte(slot), value: U256::from(number + 1) };
                tx.put::<tables::PlainStorageState>(address, entry).unwrap();
            }
        }
        let mut stages = vec![StageId::Headers, StageId::Bodies];
        if finished {
            stages.push(StageId::Finish);
        }
        for stage in stages {
            provider.save_stage_checkpoint(stage, StageCheckpoint::new(4)).unwrap();
        }
        provider.commit().unwrap();
        Self { dir, large_contract }
    }

    /// Storage and storage-trie nodes of a contract large enough to have rows in every range:
    /// all sixteen one-nibble trie paths (replicated when split) and deeper ones.
    fn large_contract_state(
        rng: &mut StdRng,
        address: B256,
    ) -> (HashedPostStateSorted, TrieUpdatesSorted) {
        let mut slots = BTreeMap::new();
        while slots.len() < LARGE_SLOTS {
            slots.insert(
                B256::from(rng.random::<[u8; 32]>()),
                U256::from(rng.random_range(1..u64::MAX)),
            );
        }
        let mut paths: BTreeMap<_, _> =
            (0..16u8).map(|nibble| (Nibbles::from_nibbles([nibble]), ())).collect();
        while paths.len() < 16 + LARGE_PATHS {
            let len = rng.random_range(2..6);
            let path: Vec<u8> = (0..len).map(|_| rng.random_range(0..16u8)).collect();
            paths.insert(Nibbles::from_nibbles(path), ());
        }
        let storage = HashedStorageSorted { storage_slots: slots.into_iter().collect() };
        let nodes = paths.into_keys().map(|path| (path, Some(random_node(rng)))).collect();
        (
            HashedPostStateSorted::new(Vec::new(), std::iter::once((address, storage)).collect()),
            TrieUpdatesSorted::new(
                Vec::new(),
                std::iter::once((address, StorageTrieUpdatesSorted { storage_nodes: nodes }))
                    .collect(),
            ),
        )
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("db")
    }
}

/// Node `node_id`'s view of the five-node cluster (`R = 2`, node 5 stores nothing), splitting
/// the storage of `split`. Owner servers are never asked, so the addresses are placeholders.
fn cluster(node_id: u64, split: &[B256]) -> Cluster {
    let nodes = (1..=NODES)
        .map(|id| NodeConfig {
            id: NodeId(id),
            address: format!("127.0.0.1:{}", 9000 + id).parse().unwrap(),
            storage: id < NODES,
        })
        .collect();
    Cluster::new(ClusterConfig {
        node_id: NodeId(node_id),
        replication: 2,
        retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
        nodes,
        apply_from: Vec::new(),
        split_storage: split.iter().map(|hash| Bytes::copy_from_slice(hash.as_slice())).collect(),
    })
    .unwrap()
}

/// One target per node of `nodes`, each with its own destination under `dir`.
fn targets(dir: &Path, nodes: &[u64], split: &[B256]) -> Vec<BootstrapTarget> {
    nodes
        .iter()
        .map(|id| BootstrapTarget {
            cluster: cluster(*id, split),
            destination: dir.join(format!("node-{id}")).join("db"),
        })
        .collect()
}

fn bootstrap(targets: Vec<BootstrapTarget>) -> Bootstrap {
    Bootstrap::new(targets).unwrap().with_database_arguments(arguments())
}

/// The reference: a full copy of the source's database, shed to completion by node `node_id`.
fn copied_and_shed(source: &Path, node_id: u64, split: &[B256]) -> FullDump {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("db");
    std::fs::create_dir_all(&db).unwrap();
    for file in std::fs::read_dir(source).unwrap() {
        let file = file.unwrap();
        let name = file.file_name();
        if name != "lock" && name != "mdbx.lck" {
            std::fs::copy(file.path(), db.join(name)).unwrap();
        }
    }
    let mut env = DatabaseEnv::open(&db, DatabaseEnvKind::RW, arguments()).unwrap();
    env.create_tables().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let cluster = Arc::new(cluster(node_id, split));
    let clustered =
        ClusteredDatabase::clustered(env.clone(), cluster, listener, ClientOptions::default())
            .unwrap();
    clustered.cluster().unwrap().shed().unwrap();
    dump(&env)
}

/// Bootstraps `nodes` from `source` in one pass and dumps each node's database, in node order.
fn bootstrapped(source: &Path, nodes: &[u64], split: &[B256], batch: BatchLimit) -> Vec<FullDump> {
    bootstrapped_with(source, nodes, split, batch, ReadParallelism::DEFAULT)
}

/// [`bootstrapped`], reading the source with `read`.
fn bootstrapped_with(
    source: &Path,
    nodes: &[u64],
    split: &[B256],
    batch: BatchLimit,
    read: ReadParallelism,
) -> Vec<FullDump> {
    let dir = TempDir::new().unwrap();
    let targets = targets(dir.path(), nodes, split);
    let destinations: Vec<_> = targets.iter().map(|target| target.destination.clone()).collect();
    let report =
        bootstrap(targets).with_batch_limit(batch).with_read_parallelism(read).run(source).unwrap();
    assert_eq!(report.destinations, destinations);
    destinations.iter().map(|destination| dump_path(destination)).collect()
}

fn rows(dump: &FullDump, table: &str) -> usize {
    dump[table].len()
}

/// Rows of `contract` in `HashedStorages`.
fn contract_rows(dump: &FullDump, contract: B256) -> usize {
    dump[tables::HashedStorages::NAME]
        .iter()
        .filter(|(key, _)| key.as_slice() == contract.as_slice())
        .count()
}

const ALL_NODES: [u64; NODES as usize] = [1, 2, 3, 4, 5];

/// Every node of the cluster, bootstrapped in one pass with one reader or several, equals a full
/// copy shed by that node.
fn assert_equivalent(split: bool) {
    let source = Source::new(1, true);
    let split = if split { vec![source.large_contract] } else { Vec::new() };
    let full = dump_path(&source.db());
    let references: Vec<_> =
        ALL_NODES.iter().map(|node_id| copied_and_shed(&source.db(), *node_id, &split)).collect();
    for read in READINGS {
        let copies = bootstrapped_with(&source.db(), &ALL_NODES, &split, BatchLimit::DEFAULT, read);
        assert_copies(&source, &split, &full, &references, &copies, read);
    }
}

fn assert_copies(
    source: &Source,
    split: &[B256],
    full: &FullDump,
    references: &[FullDump],
    copies: &[FullDump],
    read: ReadParallelism,
) {
    for ((node_id, copy), reference) in ALL_NODES.into_iter().zip(copies).zip(references) {
        let context = format!("node {node_id}, split {}, {read:?}", !split.is_empty());
        for table in Tables::ALL {
            assert_eq!(copy[table.name()], reference[table.name()], "{context}: {}", table.name());
        }
        assert!(
            rows(copy, tables::HashedStorages::NAME) < rows(full, tables::HashedStorages::NAME),
            "{context}: a node holds part of the storage"
        );
        assert_eq!(copy[tables::PlainStorageState::NAME], full[tables::PlainStorageState::NAME]);
        assert_eq!(copy[tables::VersionHistory::NAME], full[tables::VersionHistory::NAME]);
        assert_eq!(copy[tables::StageCheckpoints::NAME], full[tables::StageCheckpoints::NAME]);
    }
    let held = copies.iter().map(|copy| contract_rows(copy, source.large_contract));
    if split.is_empty() {
        assert_eq!(held.filter(|rows| *rows == LARGE_SLOTS).count(), 2, "R owners hold it whole");
    } else {
        assert!(
            held.clone().all(|rows| rows < LARGE_SLOTS) && held.sum::<usize>() == 2 * LARGE_SLOTS,
            "a split contract's ranges spread over the storing nodes, R copies of each"
        );
    }
}

#[test]
fn bootstrap_equals_a_shed_full_copy() {
    assert_equivalent(false);
}

#[test]
fn bootstrap_equals_a_shed_full_copy_with_a_split_contract() {
    assert_equivalent(true);
}

#[test]
fn small_batches_and_single_destinations_give_the_same_databases() {
    let source = Source::new(2, true);
    let split = [source.large_contract];
    let whole = bootstrapped(&source.db(), &ALL_NODES, &split, BatchLimit::DEFAULT);
    for read in READINGS {
        let by_rows = BatchLimit { rows: 7, bytes: u64::MAX };
        let by_rows = bootstrapped_with(&source.db(), &ALL_NODES, &split, by_rows, read);
        let by_bytes = BatchLimit { rows: u64::MAX, bytes: 1 };
        let by_bytes = bootstrapped_with(&source.db(), &ALL_NODES, &split, by_bytes, read);
        assert_eq!(by_rows, whole, "7-row batches, {read:?}");
        assert_eq!(by_bytes, whole, "one-row batches by size, {read:?}");
        for (node_id, copy) in ALL_NODES.into_iter().zip(&whole) {
            let alone =
                bootstrapped_with(&source.db(), &[node_id], &split, BatchLimit::DEFAULT, read);
            assert_eq!(&alone[0], copy, "node {node_id} bootstrapped alone, {read:?}");
        }
    }
}

#[test]
fn a_contract_whose_duplicates_span_batches_is_copied_whole() {
    let source = Source::new(3, true);
    let shard = Shard(source.large_contract[0]);
    let owner = cluster(NODES, &[]).owners(shard)[0].0;
    let reference = copied_and_shed(&source.db(), owner, &[]);
    for read in READINGS {
        let batch = BatchLimit { rows: 7, bytes: u64::MAX };
        let copy = bootstrapped_with(&source.db(), &[owner], &[], batch, read);
        assert_eq!(contract_rows(&copy[0], source.large_contract), LARGE_SLOTS, "{read:?}");
        assert_eq!(copy[0], reference, "{read:?}");
    }
}

#[test]
fn the_source_is_not_modified() {
    let source = Source::new(4, true);
    let before = dump_path(&source.db());
    let batch = BatchLimit { rows: 7, bytes: u64::MAX };
    for read in READINGS {
        bootstrapped_with(&source.db(), &ALL_NODES, &[source.large_contract], batch, read);
    }
    assert_eq!(dump_path(&source.db()), before);
}

#[test]
fn refuses_a_destination_that_is_not_empty() {
    let source = Source::new(5, true);
    let dir = TempDir::new().unwrap();
    let targets = targets(dir.path(), &[1, 2], &[]);
    let occupied = targets[1].destination.clone();
    std::fs::create_dir_all(&occupied).unwrap();
    std::fs::write(occupied.join("keep"), b"operator data").unwrap();
    let free = targets[0].destination.clone();
    let result = bootstrap(targets).run(&source.db());
    assert!(matches!(result, Err(BootstrapError::DestinationNotEmpty(_))), "{result:?}");
    assert_eq!(std::fs::read_dir(&occupied).unwrap().count(), 1, "left as it was");
    assert!(!free.exists(), "no destination is created");
}

#[test]
fn refuses_a_source_without_a_finish_checkpoint() {
    let source = Source::new(6, false);
    let dir = TempDir::new().unwrap();
    let targets = targets(dir.path(), &[1], &[]);
    let destination = targets[0].destination.clone();
    let result = bootstrap(targets).run(&source.db());
    assert!(matches!(result, Err(BootstrapError::NoFinishCheckpoint)), "{result:?}");
    assert!(!destination.exists(), "nothing is created");
}

#[test]
fn refuses_targets_that_disagree_or_repeat() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    assert!(matches!(Bootstrap::new(Vec::new()), Err(BootstrapError::NoDestination)));

    let mut same_destination = targets(path, &[1, 2], &[]);
    same_destination[1].destination = same_destination[0].destination.clone();
    assert!(matches!(
        Bootstrap::new(same_destination),
        Err(BootstrapError::DuplicateDestination(_))
    ));

    let mut same_node = targets(path, &[1, 2], &[]);
    same_node[1].cluster = cluster(1, &[]);
    assert!(matches!(Bootstrap::new(same_node), Err(BootstrapError::DuplicateNode(NodeId(1)))));

    let mut other_split = targets(path, &[1, 2], &[]);
    other_split[1].cluster = cluster(2, &[B256::repeat_byte(7)]);
    assert!(matches!(Bootstrap::new(other_split), Err(BootstrapError::ConfigMismatch(NodeId(2)))));

    let mut other_replication = targets(path, &[1, 2], &[]);
    let mut config = other_replication[1].cluster.config().clone();
    config.replication = 3;
    other_replication[1].cluster = Cluster::new(config).unwrap();
    assert!(matches!(
        Bootstrap::new(other_replication),
        Err(BootstrapError::ConfigMismatch(NodeId(2)))
    ));

    let mut other_apply_from = targets(path, &[1, 2], &[]);
    let mut config = other_apply_from[1].cluster.config().clone();
    config.apply_from = vec![NodeId(1)];
    other_apply_from[1].cluster = Cluster::new(config).unwrap();
    assert!(Bootstrap::new(other_apply_from).is_ok(), "apply_from may differ");
}

#[test]
fn a_sharded_row_that_cannot_be_placed_is_an_error() {
    let source = Source::new(7, true);
    {
        let env = DatabaseEnv::open(&source.db(), DatabaseEnvKind::RW, arguments()).unwrap();
        let tx = env.tx_mut().unwrap();
        tx.put::<RawTable<tables::HashedAccounts>>(
            RawKey::from_vec(vec![0xff; 31]),
            RawValue::from_vec(vec![0]),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    for read in READINGS {
        let dir = TempDir::new().unwrap();
        let bootstrap = bootstrap(targets(dir.path(), &[1, 2, 3], &[])).with_read_parallelism(read);
        let result = bootstrap.run(&source.db());
        assert!(matches!(result, Err(BootstrapError::InvalidRow { .. })), "{read:?}: {result:?}");
    }
}

#[test]
fn storage_report_lists_the_contracts_with_the_most_rows() {
    let source = Source::new(8, true);
    let full = dump_path(&source.db());
    let mut counts: BTreeMap<&[u8], u64> = BTreeMap::new();
    for (key, _) in &full[tables::HashedStorages::NAME] {
        *counts.entry(key.as_slice()).or_default() += 1;
    }
    let mut want: Vec<_> =
        counts.iter().map(|(key, rows)| (*rows, B256::from_slice(key))).collect();
    want.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    want.truncate(3);

    let report = StorageReport::scan(&source.db(), 3).unwrap();
    assert_eq!(report.total_rows, full[tables::HashedStorages::NAME].len() as u64);
    let top: Vec<_> = report.top.iter().map(|entry| (entry.rows, entry.hashed_address)).collect();
    assert_eq!(top, want);
    assert_eq!(report.top[0].hashed_address, source.large_contract);
    assert_eq!(report.split_candidates(), vec![source.large_contract]);
    let printed = report.to_string();
    let last = printed.lines().last().unwrap();
    assert_eq!(last, format!("split_storage = [\"{}\"]", source.large_contract));

    let dir = TempDir::new().unwrap();
    let summary = bootstrap(targets(dir.path(), &[1], &[])).run(&source.db()).unwrap();
    assert_eq!(summary.storage.total_rows, report.total_rows);
    assert_eq!(summary.storage.top.len(), StorageReport::DEFAULT_TOP.min(counts.len()));
    assert_eq!(summary.storage.top[..3], report.top[..]);
    let storage = summary.storage.to_string();
    assert!(summary.to_string().ends_with(&storage), "the summary ends with the storage report");
}

/// Rows of the unlisted contract of [`parallel_source`]: more than its partition's queue holds,
/// so its reader blocks until the partition's turn comes.
const UNLISTED_ROWS: usize = 3 * RowBatch::MAX_ROWS + 5;

/// [`Source::new`] plus an unlisted contract of [`UNLISTED_ROWS`] storage rows; returns it too.
fn parallel_source(seed: u64) -> (Source, B256) {
    let source = Source::new(seed, true);
    let mut rng = rng(seed);
    let unlisted = B256::from(rng.random::<[u8; 32]>());
    let env = DatabaseEnv::open(&source.db(), DatabaseEnvKind::RW, arguments()).unwrap();
    let tx = env.tx_mut().unwrap();
    for _ in 0..UNLISTED_ROWS {
        let entry = StorageEntry {
            key: B256::from(rng.random::<[u8; 32]>()),
            value: U256::from(rng.random_range(1..u64::MAX)),
        };
        tx.put::<tables::HashedStorages>(unlisted, entry).unwrap();
    }
    tx.commit().unwrap();
    (source, unlisted)
}

#[test]
fn several_readers_write_the_same_databases_as_one() {
    let (source, unlisted) = parallel_source(9);
    let split = [source.large_contract];
    let full = dump_path(&source.db());
    assert_eq!(contract_rows(&full, unlisted), UNLISTED_ROWS);
    let one = bootstrapped_with(&source.db(), &ALL_NODES, &split, BatchLimit::DEFAULT, ONE_READER);
    let rendezvous = ReadParallelism {
        threads: NonZeroUsize::new(8).unwrap(),
        window: NonZeroUsize::new(8).unwrap(),
        queued_batches: 0,
        partition_bytes: NonZeroU64::new(64).unwrap(),
    };
    let one_window = ReadParallelism { window: NonZeroUsize::MIN, ..READERS };
    for read in [READERS, rendezvous, one_window] {
        let several =
            bootstrapped_with(&source.db(), &ALL_NODES, &split, BatchLimit::DEFAULT, read);
        assert_eq!(several, one, "{read:?}");
    }
    let view = cluster(NODES, &split);
    let owners = view.owners(Shard(unlisted[0]));
    for (node_id, copy) in ALL_NODES.into_iter().zip(&one) {
        let held = if owners.contains(&NodeId(node_id)) { UNLISTED_ROWS } else { 0 };
        assert_eq!(contract_rows(copy, unlisted), held, "node {node_id}");
    }
}

#[test]
fn storage_report_counts_the_same_with_any_readers_and_hand_off() {
    let (source, unlisted) = parallel_source(10);
    let full = dump_path(&source.db());
    let mut counts: BTreeMap<&[u8], u64> = BTreeMap::new();
    for (key, _) in &full[tables::HashedStorages::NAME] {
        *counts.entry(key.as_slice()).or_default() += 1;
    }
    let mut want: Vec<_> =
        counts.iter().map(|(key, rows)| (*rows, B256::from_slice(key))).collect();
    want.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

    let top = counts.len() + 1;
    let one = StorageReport::scan_with(&source.db(), top, ONE_READER, u64::MAX).unwrap();
    let listed: Vec<_> = one.top.iter().map(|entry| (entry.rows, entry.hashed_address)).collect();
    assert_eq!(listed, want);
    assert_eq!(one.total_rows, full[tables::HashedStorages::NAME].len() as u64);
    assert_eq!(one.top[0].hashed_address, unlisted);
    for (read, hand_off_rows) in
        [(READERS, 50), (READERS, 1), (ONE_READER, 50), (READERS, u64::MAX)]
    {
        let report = StorageReport::scan_with(&source.db(), top, read, hand_off_rows).unwrap();
        assert_eq!(report, one, "{read:?}, hand-off at {hand_off_rows} rows");
        let short = StorageReport::scan_with(&source.db(), 3, read, hand_off_rows).unwrap();
        assert_eq!(short.top, one.top[..3], "{read:?}, hand-off at {hand_off_rows} rows");
    }
}

/// Rows of the throughput benchmark's unsharded table, written to every destination, at scale 1.
const BENCH_UNSHARDED_ROWS: u64 = 2_000_000;
/// Rows of the throughput benchmark's `HashedStorages` at scale 1, 99.7% of them in one split
/// contract.
const BENCH_STORAGE_ROWS: u64 = 4_000_000;
/// Multiplies the benchmark's row counts.
const BENCH_SCALE_VAR: &str = "BOOTSTRAP_BENCH_SCALE";

/// The 32-byte key at position `index` of `count` ascending keys spread evenly over the key
/// space, so that they fall in every shard.
fn spread_key(index: u64, count: u64, tag: u8) -> [u8; 32] {
    let mut key = [tag; 32];
    key[..8].copy_from_slice(&(index * (u64::MAX / count)).to_be_bytes());
    key
}

/// A finished full node's `db` whose only non-empty table besides the stage checkpoints is
/// `table`, holding `rows` (ascending raw key and value pairs).
fn bench_source(table: &str, rows: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) -> TempDir {
    let dir = TempDir::new().unwrap();
    let env = init_db(dir.path().join("db"), DatabaseArguments::default()).unwrap();
    {
        let tx = env.tx_mut().unwrap();
        tx.put::<tables::StageCheckpoints>(StageId::Finish.to_string(), StageCheckpoint::new(1))
            .unwrap();
        tx.commit().unwrap();
    }
    let txn = env.begin_rw_txn().unwrap();
    let mut cursor = txn.cursor(txn.open_db(Some(table)).unwrap().dbi()).unwrap();
    for (key, value) in rows {
        cursor.put(&key, &value, WriteFlags::APPEND_DUP).unwrap();
    }
    drop(cursor);
    txn.commit().unwrap();
    dir
}

/// Reader thread counts the benchmark runs, comma separated; 1 and 16 when unset.
const BENCH_THREADS_VAR: &str = "BOOTSTRAP_BENCH_THREADS";

fn bench_threads() -> Vec<NonZeroUsize> {
    std::env::var(BENCH_THREADS_VAR)
        .unwrap_or_else(|_| "1,16".to_owned())
        .split(',')
        .map(|threads| threads.trim().parse().unwrap())
        .collect()
}

/// Bootstraps all five nodes from `source` with each of [`bench_threads`] reader threads and
/// prints the source rows per second of `table`.
fn bench_run(source: &TempDir, table: &str, split: &[B256]) {
    for threads in bench_threads() {
        bench_run_with(source, table, split, threads);
    }
}

fn bench_run_with(source: &TempDir, table: &str, split: &[B256], threads: NonZeroUsize) {
    let dir = TempDir::new().unwrap();
    let started = std::time::Instant::now();
    let report = Bootstrap::new(targets(dir.path(), &ALL_NODES, split))
        .unwrap()
        .with_read_parallelism(ReadParallelism::with_threads(threads))
        .run(&source.path().join("db"))
        .unwrap();
    let elapsed = started.elapsed();
    let copy = report.tables.iter().find(|copy| copy.table == table).unwrap();
    let appends: u64 = copy.written.iter().map(|written| written.rows).sum();
    let busiest = copy.written.iter().map(|written| written.rows).max().unwrap();
    println!(
        "{table}, {threads} reader threads: {} source rows, {appends} appends (at most {busiest} to one destination) in \
         {elapsed:.2?}: {:.0} source rows/s, {:.0} appends/s",
        copy.read.rows,
        copy.read.rows as f64 / elapsed.as_secs_f64(),
        appends as f64 / elapsed.as_secs_f64(),
    );
}

/// Throughput of a bootstrap of five nodes from a source shaped like a real one: an unsharded
/// table written to every destination, and `HashedStorages` with one split contract holding
/// 99.7% of its rows; each with the reader thread counts of `BOOTSTRAP_BENCH_THREADS` (default
/// `1,16`). Run in release mode with
/// `cargo test --release -p base-execution-cluster bootstrap_throughput -- --ignored --nocapture`.
#[test]
#[ignore = "benchmark: run in release mode with --ignored --nocapture"]
fn bootstrap_throughput() {
    let scale = std::env::var(BENCH_SCALE_VAR).map_or(1, |scale| scale.parse::<u64>().unwrap());
    let unsharded_rows = BENCH_UNSHARDED_ROWS * scale;
    let unsharded = bench_source(
        tables::HeaderNumbers::NAME,
        (0..unsharded_rows).map(|index| {
            (spread_key(index, unsharded_rows, 0x11).to_vec(), index.to_be_bytes().to_vec())
        }),
    );
    bench_run(&unsharded, tables::HeaderNumbers::NAME, &[]);
    drop(unsharded);

    let storage_rows = BENCH_STORAGE_ROWS * scale;
    let small_contracts = storage_rows * 3 / 1000;
    let large_rows = storage_rows - small_contracts;
    let large_index = small_contracts / 2;
    let small = |index: u64| spread_key(index, small_contracts, 0x55).to_vec();
    let slot = |index: u64, count: u64| {
        let mut value = spread_key(index, count, 0x33).to_vec();
        value.extend_from_slice(&index.to_be_bytes());
        value
    };
    let large = B256::from(spread_key(large_index, small_contracts, 0x77));
    let rows = (0..=large_index)
        .map(|index| (small(index), slot(0, 1)))
        .chain((0..large_rows).map(|index| (large.to_vec(), slot(index, large_rows))))
        .chain((large_index + 1..small_contracts).map(|index| (small(index), slot(0, 1))));
    let storage = bench_source(tables::HashedStorages::NAME, rows);
    bench_run(&storage, tables::HashedStorages::NAME, &[large]);
}
