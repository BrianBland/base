//! Bootstrapping a clustered node's database from a full node's: equivalence with a full copy
//! that has been shed, batching, refusals, and the storage report.

use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy_primitives::{B256, Bytes, U256, keccak256};
use base_execution_cluster::{
    BatchLimit, Bootstrap, BootstrapError, BootstrapTarget, ClientOptions, Cluster, ClusterConfig,
    ClusteredDatabase, NodeConfig, NodeId, Shard, StorageReport,
};
use rand::{Rng, rngs::StdRng};
use reth_chainspec::MAINNET;
use reth_db::{ClientVersion, DatabaseEnv, DatabaseEnvKind, init_db, mdbx::DatabaseArguments};
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
    let dir = TempDir::new().unwrap();
    let targets = targets(dir.path(), nodes, split);
    let destinations: Vec<_> = targets.iter().map(|target| target.destination.clone()).collect();
    let report = bootstrap(targets).with_batch_limit(batch).run(source).unwrap();
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

/// Every node of the cluster, bootstrapped in one pass, equals a full copy shed by that node.
fn assert_equivalent(split: bool) {
    let source = Source::new(1, true);
    let split = if split { vec![source.large_contract] } else { Vec::new() };
    let full = dump_path(&source.db());
    let copies = bootstrapped(&source.db(), &ALL_NODES, &split, BatchLimit::DEFAULT);
    for (node_id, copy) in ALL_NODES.into_iter().zip(&copies) {
        let context = format!("node {node_id}, split {}", !split.is_empty());
        let reference = copied_and_shed(&source.db(), node_id, &split);
        for table in Tables::ALL {
            assert_eq!(copy[table.name()], reference[table.name()], "{context}: {}", table.name());
        }
        assert!(
            rows(copy, tables::HashedStorages::NAME) < rows(&full, tables::HashedStorages::NAME),
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
    let by_rows =
        bootstrapped(&source.db(), &ALL_NODES, &split, BatchLimit { rows: 7, bytes: u64::MAX });
    let by_bytes =
        bootstrapped(&source.db(), &ALL_NODES, &split, BatchLimit { rows: u64::MAX, bytes: 1 });
    assert_eq!(by_rows, whole, "7-row batches");
    assert_eq!(by_bytes, whole, "one-row batches by size");
    for (node_id, copy) in ALL_NODES.into_iter().zip(&whole) {
        let alone = bootstrapped(&source.db(), &[node_id], &split, BatchLimit::DEFAULT);
        assert_eq!(&alone[0], copy, "node {node_id} bootstrapped alone");
    }
}

#[test]
fn a_contract_whose_duplicates_span_batches_is_copied_whole() {
    let source = Source::new(3, true);
    let shard = Shard(source.large_contract[0]);
    let owner = cluster(NODES, &[]).owners(shard)[0].0;
    let copy = bootstrapped(&source.db(), &[owner], &[], BatchLimit { rows: 7, bytes: u64::MAX });
    assert_eq!(contract_rows(&copy[0], source.large_contract), LARGE_SLOTS);
    assert_eq!(copy[0], copied_and_shed(&source.db(), owner, &[]));
}

#[test]
fn the_source_is_not_modified() {
    let source = Source::new(4, true);
    let before = dump_path(&source.db());
    let batch = BatchLimit { rows: 7, bytes: u64::MAX };
    bootstrapped(&source.db(), &ALL_NODES, &[source.large_contract], batch);
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
    let dir = TempDir::new().unwrap();
    let result = bootstrap(targets(dir.path(), &[1], &[])).run(&source.db());
    assert!(matches!(result, Err(BootstrapError::InvalidRow { .. })), "{result:?}");
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
