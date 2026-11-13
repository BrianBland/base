//! Clustered databases: a seeded differential test of five clustered nodes against a full
//! reference MDBX, with and without a split contract (local filtered trie reads, local filtered
//! hashed reads inside a `LocalReads` scope, remote exact reads outside it), the write rule, the
//! commit guard, and shedding; and for split contracts, cursor states at range boundaries against
//! a database holding only the visible rows, and shedding in small batches.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{B256, U256, keccak256, map::B256Map};
use base_execution_cluster::{
    ClientOptions, Cluster, ClusterConfig, ClusteredDatabase, KeyPlacement, LocalReads, NodeConfig,
    NodeId, OwnersNotReady, ProviderChangesets, ReadError, Shard, ShardedTable,
};
use rand::{Rng, rngs::StdRng};
use reth_chainspec::MAINNET;
use reth_db::{ClientVersion, DatabaseEnv, init_db, mdbx::DatabaseArguments};
use reth_db_api::{
    Database,
    cursor::{DbCursorRO, DbDupCursorRO},
    models::StorageSettings,
    table::{Compress, Decode, Decompress, DupSort, Table, TableRow},
    tables::{self, RawDupSort, RawKey, RawTable, RawValue},
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_provider::{
    DatabaseProviderRW, ProviderFactory, StageCheckpointWriter, StateWriteConfig, StateWriter,
    StorageSettingsCache, TrieWriter,
    providers::{RocksDBBuilder, StaticFileProvider},
    test_utils::MockNodeTypesWithDB,
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_trie::{
    BranchNodeCompact, HashedPostStateSorted, HashedStorageSorted, Nibbles, PackedStorageTrieEntry,
    PackedStoredNibbles,
    hashed_cursor::{HashedCursor, HashedCursorFactory, HashedStorageCursor},
    trie_cursor::{TrieCursor, TrieCursorFactory},
    updates::{StorageTrieUpdatesSorted, TrieUpdatesSorted},
};
use reth_trie_db::{DatabaseHashedCursorFactory, DatabaseTrieCursorFactory, PackedKeyAdapter};
use revm::database::states::PlainStateReverts;
use tempfile::TempDir;

use crate::common::{
    BlockUpdates, ChainBlock, KeyUniverse, PlainKeys, PlainState, TableDump, block_hash, canonical,
    factory, next_block, packed, random_node, random_updates, rng, snapshot,
};

/// Blocks applied while every node runs, and after one storing node stops.
const BLOCKS: u64 = 14;
const BLOCKS_AFTER_STOP: u64 = 4;
/// Storing nodes are 1..=4; node 5 stores nothing.
const NODES: u64 = 5;
/// Blocks between persistence of each node, so that their persisted tips differ.
const CADENCES: [u64; NODES as usize] = [1, 2, 3, 4, 3];

type NodeFactory = ProviderFactory<MockNodeTypesWithDB<ClusteredDatabase>>;

/// One block: changesets when it is part of a plain-key chain, state and trie updates, and new
/// bytecodes.
#[derive(Debug, Clone)]
struct Block {
    number: u64,
    reverts: Option<PlainStateReverts>,
    updates: BlockUpdates,
    codes: Vec<(B256, Bytecode)>,
}

/// A provider factory over `db` using storage v2, with its static files and `RocksDB` in `dir`.
fn provider_factory(db: ClusteredDatabase, dir: &Path) -> NodeFactory {
    let static_files = dir.join("static_files");
    std::fs::create_dir_all(&static_files).unwrap();
    let factory = ProviderFactory::new(
        db,
        MAINNET.clone(),
        StaticFileProvider::read_write(static_files).unwrap(),
        RocksDBBuilder::new(dir.join("rocksdb")).with_default_tables().build().unwrap(),
        reth_tasks::Runtime::test(),
    )
    .unwrap();
    factory.set_storage_settings_cache(StorageSettings::v2());
    factory
}

/// Writes `blocks` with reth's writers in one transaction and advances `Finish` to the last one.
fn persist<DB>(provider: DatabaseProviderRW<DB, MockNodeTypesWithDB<DB>>, blocks: &[Block])
where
    DB: Database + Clone + Unpin + 'static,
{
    for block in blocks {
        if let Some(reverts) = &block.reverts {
            let config = StateWriteConfig::default();
            provider.write_state_reverts(reverts.clone(), block.number, config).unwrap();
        }
        provider.write_hashed_state(&block.updates.hashed_state).unwrap();
        provider.write_trie_updates_sorted(&block.updates.trie_updates).unwrap();
        for (hash, code) in &block.codes {
            provider.tx_ref().put::<tables::Bytecodes>(*hash, code.clone()).unwrap();
        }
    }
    let tip = blocks.last().expect("blocks to persist").number;
    provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(tip)).unwrap();
    provider.commit().unwrap();
}

/// One clustered node of the run.
struct Node {
    id: u64,
    cadence: u64,
    env: DatabaseEnv,
    db: ClusteredDatabase,
    factory: Arc<NodeFactory>,
    persisted: u64,
    pending: Vec<Block>,
    _dir: TempDir,
}

impl Node {
    const fn storing(&self) -> bool {
        self.id < NODES
    }

    fn cluster(&self) -> &Cluster {
        self.db.cluster().unwrap().cluster()
    }
}

/// Node `node_id`'s view of the five-node cluster, splitting the storage of `split`.
fn cluster_view(node_id: u64, addresses: &[std::net::SocketAddr], split: &[B256]) -> Arc<Cluster> {
    let nodes = (1..=NODES)
        .zip(addresses)
        .map(|(id, address)| NodeConfig { id: NodeId(id), address: *address, storage: id < NODES })
        .collect();
    Arc::new(
        Cluster::new(ClusterConfig {
            node_id: NodeId(node_id),
            replication: 2,
            retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
            nodes,
            apply_from: Vec::new(),
            split_storage: split
                .iter()
                .map(|hash| alloy_primitives::Bytes::copy_from_slice(hash.as_slice()))
                .collect(),
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
            root_check: Default::default(),
        })
        .unwrap(),
    )
}

/// Client options of the test nodes: a 2-second retry budget.
fn options() -> ClientOptions {
    ClientOptions { retry_for: Duration::from_secs(2), ..Default::default() }
}

/// Largest size of a node's MDBX. Each environment reserves this much address space, and the
/// tests open dozens at once.
const NODE_DB_MAX_SIZE: usize = 1 << 30;

fn arguments() -> DatabaseArguments {
    DatabaseArguments::new(ClientVersion::default()).with_geometry_max_size(Some(NODE_DB_MAX_SIZE))
}

/// An MDBX environment holding the full state of `initial`, written through a pass-through
/// database with its static files in `dir`, where the node's provider factory finds them.
fn populated_env(dir: &Path, initial: &[Block]) -> DatabaseEnv {
    let env = init_db(dir.join("db"), arguments()).unwrap();
    let populate = provider_factory(ClusteredDatabase::new(env.clone()), dir);
    persist(populate.provider_rw().unwrap(), initial);
    env
}

fn random_codes(rng: &mut StdRng) -> Vec<(B256, Bytecode)> {
    (0..2)
        .map(|_| {
            let code: Vec<u8> = (0..rng.random_range(1..40)).map(|_| rng.random()).collect();
            (keccak256(&code), Bytecode::new_raw(code.into()))
        })
        .collect()
}

fn raw_walk<T: Table>(tx: &impl DbTx) -> Vec<(Vec<u8>, Vec<u8>)> {
    tx.cursor_read::<RawTable<T>>()
        .unwrap()
        .walk(None)
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.into_key(), value.into_value())
        })
        .collect()
}

/// A random encoded key of `table` within or near the universe's shards.
fn random_key(rng: &mut StdRng, table: ShardedTable, shards: &[Shard]) -> Vec<u8> {
    let first = if rng.random_bool(0.7) {
        shards[rng.random_range(0..shards.len())].0
    } else {
        rng.random()
    };
    if table == ShardedTable::AccountsTrie {
        let mut nibbles = vec![first >> 4, first & 0xf];
        nibbles.truncate(rng.random_range(0..=2));
        nibbles.extend((0..rng.random_range(0..4)).map(|_| rng.random_range(0..16u8)));
        return packed(Nibbles::from_nibbles(nibbles));
    }
    let mut key: [u8; 32] = rng.random();
    key[0] = first;
    key.to_vec()
}

/// Whether `cluster`'s node holds the row `(key, value)` of `table`.
fn holds(cluster: &Cluster, table: ShardedTable, key: &[u8], value: &[u8]) -> bool {
    cluster.holds(table, key, table.subkey_of(value).unwrap_or_default()) == Some(true)
}

/// Account-trie row `(key, value)` as `cluster`'s node reads it: a replicated one-nibble node
/// claims stored children (tree-mask bits) only in shards the node owns.
fn shown_account_node(cluster: &Cluster, key: &[u8], value: &[u8]) -> Vec<u8> {
    if ShardedTable::packed_path_nibbles(key) != Some(1) {
        return value.to_vec();
    }
    let mut node = BranchNodeCompact::decompress(value).unwrap();
    let mut tree_mask = node.tree_mask.get();
    for child in 0..16u8 {
        if !cluster.owns(Shard((key[0] & 0xf0) | child)) {
            tree_mask &= !(1 << child);
        }
    }
    node.tree_mask = tree_mask.into();
    let mut shown = Vec::new();
    node.compress_to_buf(&mut shown);
    shown
}

/// The rows of `dump` `cluster`'s node holds, as it sees them through a filtered read.
fn visible(cluster: &Cluster, dump: &TableDump) -> TableDump {
    let rows = |table, map: &BTreeMap<Vec<u8>, Vec<u8>>| -> BTreeMap<_, _> {
        map.iter()
            .filter(|(key, value)| holds(cluster, table, key, value))
            .map(|(key, value)| {
                let value = if table == ShardedTable::AccountsTrie {
                    shown_account_node(cluster, key, value)
                } else {
                    value.clone()
                };
                (key.clone(), value)
            })
            .collect()
    };
    let duplicates = |table, map: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>| -> BTreeMap<_, _> {
        map.iter()
            .map(|(key, values)| {
                let kept: Vec<_> = values
                    .iter()
                    .filter(|value| holds(cluster, table, key, value))
                    .cloned()
                    .collect();
                (key.clone(), kept)
            })
            .filter(|(_, values)| !values.is_empty())
            .collect()
    };
    TableDump {
        hashed_accounts: rows(ShardedTable::HashedAccounts, &dump.hashed_accounts),
        accounts_trie: rows(ShardedTable::AccountsTrie, &dump.accounts_trie),
        hashed_storages: duplicates(ShardedTable::HashedStorages, &dump.hashed_storages),
        storages_trie: duplicates(ShardedTable::StoragesTrie, &dump.storages_trie),
        bytecodes: rows(ShardedTable::Bytecodes, &dump.bytecodes),
    }
}

/// Ordered walks, seeks, and current-after-end of non-duplicate table `T` through `tx`.
fn check_ordered<T: Table>(
    tx: &impl DbTx,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    rng: &mut StdRng,
    shards: &[Shard],
    context: &str,
) {
    let table = ShardedTable::from_table_name(T::NAME).unwrap();
    let all: Vec<_> = expected.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut cursor = tx.cursor_read::<RawTable<T>>().unwrap();
    let walked: Vec<_> = cursor
        .walk(None)
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.into_key(), value.into_value())
        })
        .collect();
    assert_eq!(walked, all, "{context}: full walk of {table:?}");
    let current = cursor.current().unwrap().map(|(k, v)| (k.into_key(), v.into_value()));
    assert_eq!(current, all.last().cloned(), "{context}: current after the end of {table:?}");

    for _ in 0..12 {
        let start = random_key(rng, table, shards);
        let mut cursor = tx.cursor_read::<RawTable<T>>().unwrap();
        let mut seen = Vec::new();
        let mut entry = cursor.seek(RawKey::from_vec(start.clone())).unwrap();
        while let Some((key, value)) = entry {
            seen.push((key.into_key(), value.into_value()));
            if seen.len() == 4 {
                break;
            }
            entry = cursor.next().unwrap();
        }
        let want: Vec<_> =
            expected.range(start.clone()..).take(4).map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(seen, want, "{context}: seek {table:?} from {start:02x?}");
    }
}

/// Point reads, per-key duplicate walks, and `seeks` walks from a random subkey per key of
/// duplicate-key table `T` through `tx`, against `expected` (the visible rows).
fn check_duplicates<T: DupSort>(
    tx: &impl DbTx,
    expected: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    keys: &[B256],
    seeks: usize,
    rng: &mut StdRng,
    context: &str,
) {
    let table = ShardedTable::from_table_name(T::NAME).unwrap();
    for key in keys {
        let want = expected.get(key.as_slice()).cloned().unwrap_or_default();
        let raw_key = RawKey::<T::Key>::from_vec(key.to_vec());
        let first = tx.get::<RawTable<T>>(raw_key.clone()).unwrap().map(|value| value.into_value());
        assert_eq!(first, want.first().cloned(), "{context}: get first {table:?} of {key}");

        let mut cursor = tx.cursor_dup_read::<RawDupSort<T>>().unwrap();
        let walked: Vec<_> = cursor
            .walk_dup(Some(raw_key.clone()), None)
            .unwrap()
            .map(|entry| entry.unwrap().1.into_value())
            .collect();
        assert_eq!(walked, want, "{context}: walk_dup {table:?} of {key}");
        let current = cursor.current().unwrap().map(|(_, value)| value.into_value());
        assert_eq!(current, want.last().cloned(), "{context}: current after walk_dup of {key}");

        let subkey_len = table.subkey_len();
        for _ in 0..seeks {
            let subkey = match want.get(rng.random_range(0..=want.len())) {
                Some(value) if rng.random_bool(0.5) => value[..subkey_len].to_vec(),
                _ if table == ShardedTable::StoragesTrie => {
                    let len = rng.random_range(1..6);
                    let nibbles = (0..len).map(|_| rng.random_range(0..16u8)).collect::<Vec<_>>();
                    packed(Nibbles::from_nibbles(nibbles))
                }
                _ => rng.random::<[u8; 32]>().to_vec(),
            };
            let mut seen = Vec::new();
            let mut value = cursor
                .seek_by_key_subkey(raw_key.clone(), RawKey::from_vec(subkey.clone()))
                .unwrap();
            while let Some(found) = value {
                seen.push(found.into_value());
                value = cursor.next_dup_val().unwrap();
            }
            let want_from: Vec<_> =
                want.iter().filter(|value| value[..subkey_len] >= subkey[..]).cloned().collect();
            assert_eq!(
                seen, want_from,
                "{context}: seek_by_key_subkey {table:?} of {key} from {subkey:02x?}"
            );
        }
    }
}

/// Seeks per check from a random subkey of a split contract, so that some land in ranges the node
/// does not hold.
const SPLIT_SEEKS: usize = 6;

/// The keys a run reads: hashed addresses and slots, and trie paths.
struct ReadKeys {
    plain: PlainKeys,
    accounts: Vec<B256>,
    contracts: Vec<B256>,
    trie: KeyUniverse,
    shards: Vec<Shard>,
}

/// The trie rows a node sees: local and filtered, through the raw tables and reth's cursors.
fn check_trie(tx: &impl DbTx, seen: &TableDump, keys: &ReadKeys, rng: &mut StdRng, context: &str) {
    check_ordered::<tables::PackedAccountsTrie>(
        tx,
        &seen.accounts_trie,
        rng,
        &keys.shards,
        context,
    );
    for (contracts, seeks) in [(&keys.accounts, 1), (&keys.contracts, SPLIT_SEEKS)] {
        check_duplicates::<tables::PackedStoragesTrie>(
            tx,
            &seen.storages_trie,
            contracts,
            seeks,
            rng,
            context,
        );
    }
    let trie = DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(tx);
    let mut account_trie = trie.account_trie_cursor().unwrap();
    let mut read = Vec::new();
    let mut entry = account_trie.seek(Nibbles::default()).unwrap();
    while let Some(found) = entry {
        read.push(found);
        entry = account_trie.next().unwrap();
    }
    let want: Vec<_> = seen
        .accounts_trie
        .iter()
        .map(|(k, v)| {
            (PackedStoredNibbles::decode(k).unwrap().0, BranchNodeCompact::decompress(v).unwrap())
        })
        .collect();
    assert_eq!(read, want, "{context}: reth account trie cursor");
    for contract in &keys.contracts {
        let nodes: Vec<(Nibbles, BranchNodeCompact)> = seen
            .storages_trie
            .get(contract.as_slice())
            .into_iter()
            .flatten()
            .map(|value| {
                let entry = PackedStorageTrieEntry::decompress(value).unwrap();
                (entry.nibbles.0, entry.node)
            })
            .collect();
        let mut storage_trie = trie.storage_trie_cursor(*contract).unwrap();
        let mut read = Vec::new();
        let mut entry = storage_trie.seek(Nibbles::default()).unwrap();
        while let Some(found) = entry {
            read.push(found);
            entry = storage_trie.next().unwrap();
        }
        assert_eq!(read, nodes, "{context}: reth storage trie cursor of {contract}");
    }
}

/// The hashed rows a node sees inside a [`LocalReads`] scope: local and filtered.
fn check_scoped(
    tx: &impl DbTx,
    seen: &TableDump,
    keys: &ReadKeys,
    rng: &mut StdRng,
    context: &str,
) {
    check_ordered::<tables::HashedAccounts>(tx, &seen.hashed_accounts, rng, &keys.shards, context);
    for (contracts, seeks) in [(&keys.accounts, 1), (&keys.contracts, SPLIT_SEEKS)] {
        check_duplicates::<tables::HashedStorages>(
            tx,
            &seen.hashed_storages,
            contracts,
            seeks,
            rng,
            context,
        );
    }
    let hashed = DatabaseHashedCursorFactory::new(tx);
    let mut accounts = hashed.hashed_account_cursor().unwrap();
    let mut read = Vec::new();
    let mut entry = accounts.seek(B256::ZERO).unwrap();
    while let Some(found) = entry {
        read.push(found);
        entry = accounts.next().unwrap();
    }
    let want: Vec<_> = seen
        .hashed_accounts
        .iter()
        .map(|(k, v)| (B256::from_slice(k), Account::decompress(v).unwrap()))
        .collect();
    assert_eq!(read, want, "{context}: reth hashed account cursor");
    for contract in &keys.contracts {
        let slots: Vec<(B256, U256)> = seen
            .hashed_storages
            .get(contract.as_slice())
            .into_iter()
            .flatten()
            .map(|value| {
                let entry = StorageEntry::decompress(value).unwrap();
                (entry.key, entry.value)
            })
            .collect();
        let mut storage = hashed.hashed_storage_cursor(*contract).unwrap();
        assert_eq!(storage.is_storage_empty().unwrap(), slots.is_empty(), "{context}: empty");
        let mut read = Vec::new();
        let mut entry = storage.seek(B256::ZERO).unwrap();
        while let Some(found) = entry {
            read.push(found);
            entry = storage.next().unwrap();
        }
        assert_eq!(read, slots, "{context}: reth storage cursor of {contract}");
    }
}

/// Counts of the remote exact-read cases a run must cover.
#[derive(Debug, Default)]
struct Coverage {
    remote_accounts: usize,
    absent_slot_with_later_slot: usize,
    empty_probes: usize,
    refused_probes: usize,
}

/// The point reads outside a scope: held rows with MDBX's semantics, other rows exact from an
/// owner at the node's `P`.
fn check_points(
    tx: &impl DbTx,
    cluster: &Cluster,
    full: &TableDump,
    keys: &ReadKeys,
    coverage: &mut Coverage,
    context: &str,
) {
    let held = |table, key: &B256, subkey: &[u8]| {
        cluster.holds(table, key.as_slice(), subkey) == Some(true)
    };
    for address in keys.accounts.iter().chain(&keys.contracts) {
        let want =
            full.hashed_accounts.get(address.as_slice()).map(|v| Account::decompress(v).unwrap());
        let found = tx.get_by_encoded_key::<tables::HashedAccounts>(address).unwrap();
        assert_eq!(found, want, "{context}: account {address}");
        let mut cursor = tx.cursor_read::<tables::HashedAccounts>().unwrap();
        let found = cursor.seek_exact(*address).unwrap().map(|(_, account)| account);
        assert_eq!(found, want, "{context}: account seek_exact {address}");
        coverage.remote_accounts += usize::from(!held(ShardedTable::HashedAccounts, address, &[]));
    }

    let entries = |address: &B256| -> Vec<StorageEntry> {
        full.hashed_storages
            .get(address.as_slice())
            .into_iter()
            .flatten()
            .map(|value| StorageEntry::decompress(value).unwrap())
            .collect()
    };
    for (plain, slot) in keys.plain.storage_keys() {
        let (address, slot) = (keccak256(plain), keccak256(slot));
        let rows = entries(&address);
        let split = cluster.is_split(ShardedTable::HashedStorages, address.as_slice());
        let row_held = held(ShardedTable::HashedStorages, &address, slot.as_slice());
        let mut cursor = tx.cursor_dup_read::<tables::HashedStorages>().unwrap();
        let found = cursor.seek_by_key_subkey(address, slot).unwrap();
        let exact = rows.iter().find(|entry| entry.key == slot).copied();
        let want = if row_held && !split {
            rows.iter().find(|entry| entry.key >= slot).copied()
        } else {
            exact
        };
        assert_eq!(found, want, "{context}: slot {slot} of {address}");
        if !row_held && exact.is_none() && rows.iter().any(|entry| entry.key > slot) {
            coverage.absent_slot_with_later_slot += 1;
        }
    }

    for address in keys.accounts.iter().chain(&keys.contracts) {
        let rows = entries(address);
        let mut cursor = tx.cursor_dup_read::<tables::HashedStorages>().unwrap();
        let probe = cursor.seek_exact(*address);
        if cluster.is_split(ShardedTable::HashedStorages, address.as_slice()) {
            assert!(probe.is_err(), "{context}: probe of split contract {address}");
        } else if held(ShardedTable::HashedStorages, address, &[]) {
            let first = probe.unwrap().map(|(_, entry)| entry);
            assert_eq!(first, rows.first().copied(), "{context}: first slot of {address}");
        } else if rows.is_empty() {
            assert_eq!(probe.unwrap(), None, "{context}: probe of storage-less {address}");
            coverage.empty_probes += 1;
        } else {
            let error = probe.unwrap_err().to_string();
            assert!(error.contains(&address.to_string()), "{context}: {error}");
            coverage.refused_probes += 1;
        }
    }
}

/// Every read rule of the sharded tables through `node` at its persisted block, against the
/// reference `full` dump at that block. Bytecodes are checked only with `latest_codes`, the
/// newest reference, which an owner may answer from.
fn check_node(
    node: &Node,
    full: &TableDump,
    keys: &ReadKeys,
    latest_codes: Option<&TableDump>,
    coverage: &mut Coverage,
    rng: &mut StdRng,
    context: &str,
) {
    let cluster = node.cluster();
    let seen = visible(cluster, full);
    let tx = node.db.tx().unwrap();
    let before = node.db.cluster().unwrap().remote_reads().total();
    check_trie(&tx, &seen, keys, rng, context);
    {
        let _scope = LocalReads::enter();
        check_scoped(&tx, &seen, keys, rng, context);
    }
    let after = node.db.cluster().unwrap().remote_reads().total();
    assert_eq!(after, before, "{context}: trie and scoped reads asked a peer");

    check_points(&tx, cluster, full, keys, coverage, context);
    let mut accounts = tx.cursor_read::<tables::HashedAccounts>().unwrap();
    if !cluster.owns(Shard(0)) {
        assert!(accounts.first().is_err(), "{context}: a walk into a shard not held");
    }

    if let Some(latest) = latest_codes {
        for (hash, latest_code) in &latest.bytecodes {
            let found = tx.get::<RawTable<tables::Bytecodes>>(RawKey::from_vec(hash.clone()));
            let found = found.unwrap().map(|value| value.into_value());
            match full.bytecodes.get(hash) {
                Some(code) => assert_eq!(found.as_ref(), Some(code), "{context}: code {hash:02x?}"),
                // Bytecodes are immutable, so an owner may answer with code persisted after `P`.
                None => assert!(
                    found.as_ref().is_none_or(|code| code == latest_code),
                    "{context}: code {hash:02x?} inserted after P"
                ),
            }
        }
        let absent = B256::from(rng.random::<[u8; 32]>());
        assert_eq!(tx.get::<tables::Bytecodes>(absent).unwrap(), None, "{context}: absent code");
    }
}

/// Five clustered nodes over the state of `genesis` at block 0, splitting the storage of `split`
/// (hashed addresses), each with its owner history built from its changesets. Nodes listed in
/// `unshed` never shed.
fn start_nodes(genesis: &Block, split: &[B256], unshed: &[u64]) -> Vec<Node> {
    start_nodes_but(genesis, split, unshed, &[]).0
}

/// Like [`start_nodes`], except that the nodes listed in `silent` are not started: their
/// listeners are returned unserved, so connections to them complete but are never answered.
fn start_nodes_but(
    genesis: &Block,
    split: &[B256],
    unshed: &[u64],
    silent: &[u64],
) -> (Vec<Node>, Vec<TcpListener>) {
    let listeners: Vec<_> = (0..NODES).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let addresses: Vec<_> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    let (silent, started): (Vec<_>, Vec<_>) =
        (1..=NODES).zip(listeners).partition(|(id, _)| silent.contains(id));
    let nodes = started
        .into_iter()
        .map(|(id, listener)| {
            let dir = TempDir::new().unwrap();
            let env = populated_env(dir.path(), std::slice::from_ref(genesis));
            let cluster = cluster_view(id, &addresses, split);
            let db =
                ClusteredDatabase::clustered(env.clone(), cluster, listener, options()).unwrap();
            let node = db.cluster().unwrap();
            if !unshed.contains(&id) {
                assert!(node.shed().unwrap() > 0, "node {id} sheds the keys it does not hold");
            }
            let factory = Arc::new(provider_factory(db.clone(), dir.path()));
            assert!(node.install_block_hashes(Box::new(|number| Some(block_hash(0, number)))));
            let changesets = Box::new(ProviderChangesets::new(&factory));
            assert!(node.install_changesets(changesets).unwrap());
            Node {
                id,
                cadence: CADENCES[id as usize - 1],
                env,
                db,
                factory,
                persisted: 0,
                pending: Vec::new(),
                _dir: dir,
            }
        })
        .collect();
    (nodes, silent.into_iter().map(|(_, listener)| listener).collect())
}

/// The trie paths of a run: account paths in its shards and storage paths of its contracts.
fn trie_keys(rng: &mut StdRng, shards: &[Shard], contracts: &[B256]) -> KeyUniverse {
    let mut trie = KeyUniverse::new(rng, shards);
    trie.addresses = contracts.to_vec();
    trie
}

/// Runs the cluster differential test, with a split contract when `split`. Nodes listed in
/// `unshed` never shed.
fn run_cluster(seed: u64, unshed: &[u64], split: bool) {
    let mut rng = rng(seed);
    let mut shards: BTreeSet<Shard> = [0x00, 0x10, 0x11, 0x7f, 0xf0, 0xff].map(Shard).into();
    shards.extend((0..6).map(|_| Shard(rng.random())));
    let shards: Vec<_> = shards.into_iter().collect();
    let plain = PlainKeys::new(&mut rng, &shards, split);
    let accounts: Vec<B256> = plain.addresses.iter().map(keccak256).collect();
    let contracts: Vec<B256> = plain.split.iter().map(keccak256).collect();
    let all_contracts: Vec<B256> = accounts.iter().chain(&contracts).copied().collect();
    let trie = trie_keys(&mut rng, &shards, &all_contracts);
    let keys = ReadKeys { plain, accounts, contracts, trie, shards };

    let split_paths = split.then(|| DenseContract::new(&mut rng).paths);
    let mut state = PlainState::default();
    let mut new_block = |rng: &mut StdRng, number: u64| {
        let (chain, after): (ChainBlock, _) = next_block(rng, &keys.plain, &state, number);
        state = after;
        let mut trie_updates = random_updates(rng, &keys.trie).trie_updates;
        if let (Some(paths), Some(contract)) = (&split_paths, keys.contracts.first()) {
            let mut storage_nodes = Vec::new();
            for path in paths {
                if number == 0 || rng.random_bool(0.1) {
                    let kept = number == 0 || !rng.random_bool(0.3);
                    storage_nodes.push((*path, kept.then(|| random_node(rng))));
                }
            }
            let mut tries = trie_updates.storage_tries_ref().clone();
            tries.insert(*contract, StorageTrieUpdatesSorted { storage_nodes });
            let accounts = trie_updates.account_nodes_ref().to_vec();
            trie_updates = Arc::new(TrieUpdatesSorted::new(accounts, tries));
        }
        let updates = BlockUpdates { hashed_state: Arc::clone(&chain.hashed), trie_updates };
        Block { number, reverts: Some(chain.reverts), updates, codes: random_codes(rng) }
    };

    let genesis = new_block(&mut rng, 0);
    let reference = factory();
    persist(reference.provider_rw().unwrap(), std::slice::from_ref(&genesis));
    let mut history = vec![TableDump::read(&reference)];
    let mut nodes = start_nodes(&genesis, &keys.contracts, unshed);
    if let Some(contract) = keys.contracts.first()
        && unshed.is_empty()
    {
        assert_split_spread(&nodes, &history[0], *contract);
    }
    let mut coverage = Coverage::default();

    let mut check_all = |nodes: &[Node], history: &[TableDump], rng: &mut StdRng, label: &str| {
        let codes_tip = nodes.iter().filter(|n| n.storing()).map(|n| n.persisted).min().unwrap();
        for node in nodes {
            let context = format!("seed {seed}, {label}, node {} at {}", node.id, node.persisted);
            let check_codes = node.persisted <= codes_tip;
            check_node(
                node,
                &history[node.persisted as usize],
                &keys,
                check_codes.then(|| history.last().unwrap()),
                &mut coverage,
                rng,
                &context,
            );
        }
    };

    for number in 1..=BLOCKS + BLOCKS_AFTER_STOP {
        if number == BLOCKS + 1 {
            let stopped = nodes.remove(0);
            assert!(stopped.storing());
            drop(stopped);
            check_all(&nodes, &history, &mut rng, "after stopping node 1");
        }
        let block = new_block(&mut rng, number);
        persist(reference.provider_rw().unwrap(), std::slice::from_ref(&block));
        history.push(TableDump::read(&reference));
        for node in &mut nodes {
            let owner = node.db.cluster().unwrap().owner_store();
            owner.commit_blocks(vec![canonical(number, &block.updates.hashed_state)]);
            node.pending.push(block.clone());
            if number % node.cadence == 0 {
                persist(node.factory.provider_rw().unwrap(), &node.pending);
                node.pending.clear();
                node.persisted = number;
            }
        }
        check_all(&nodes, &history, &mut rng, &format!("block {number}"));
    }

    for node in &mut nodes {
        if !node.pending.is_empty() {
            persist(node.factory.provider_rw().unwrap(), &node.pending);
            node.pending.clear();
            node.persisted = BLOCKS + BLOCKS_AFTER_STOP;
        }
    }
    check_all(&nodes, &history, &mut rng, "all persisted");
    assert!(coverage.remote_accounts > 0, "{coverage:?}");
    assert!(coverage.absent_slot_with_later_slot > 0, "{coverage:?}");
    assert!(coverage.empty_probes > 0, "{coverage:?}");
    assert!(coverage.refused_probes > 0, "{coverage:?}");

    for node in &nodes {
        let cluster = node.cluster();
        let context = format!("seed {seed}, node {}", node.id);
        if unshed.contains(&node.id) {
            let tx = node.env.tx().unwrap();
            let stale = raw_walk::<tables::PackedAccountsTrie>(&tx)
                .iter()
                .any(|(key, _)| cluster.holds(ShardedTable::AccountsTrie, key, &[]) == Some(false));
            assert!(stale, "{context}: an unshed node keeps trie rows it does not hold");
        } else {
            assert_holds_only_own_keys(&node.env, cluster, &context);
        }
    }
}

/// Raw rows of one duplicate-key table in a dump.
type DuplicateRows = fn(&TableDump) -> &BTreeMap<Vec<u8>, Vec<Vec<u8>>>;

/// After shed, the rows of split `contract` on each node are exactly those placement gives it:
/// every storing node holds some but not all of its sharded rows (about `R/N`), every node holds
/// its replicated paths, and every row of `reference` is on exactly `R` storing nodes (on every
/// node when replicated) and nowhere else.
fn assert_split_spread(nodes: &[Node], reference: &TableDump, contract: B256) {
    let cluster = nodes[0].db.cluster().unwrap().cluster();
    let replication = cluster.config().replication;
    let dumps: Vec<_> =
        nodes.iter().map(|node| TableDump::read_tx(&node.env.tx().unwrap())).collect();
    let tables: [(ShardedTable, DuplicateRows); 2] = [
        (ShardedTable::HashedStorages, |dump| &dump.hashed_storages),
        (ShardedTable::StoragesTrie, |dump| &dump.storages_trie),
    ];
    for (table, rows) in tables {
        let rows_of = |dump: &TableDump| -> BTreeSet<Vec<u8>> {
            rows(dump).get(contract.as_slice()).into_iter().flatten().cloned().collect()
        };
        let want = rows_of(reference);
        let placement = |row: &Vec<u8>| {
            cluster.placement(table, contract.as_slice(), table.subkey_of(row).unwrap()).unwrap()
        };
        let sharded = want.iter().filter(|row| placement(row) != KeyPlacement::Replicated).count();
        assert!(sharded > 100, "{table:?}: {sharded} sharded rows");
        let held: Vec<_> = dumps.iter().map(rows_of).collect();
        for (node, rows) in nodes.iter().zip(&held) {
            assert!(
                rows.is_subset(&want),
                "{table:?}: node {} holds rows not in the reference",
                node.id
            );
            let own = rows.iter().filter(|row| placement(row) != KeyPlacement::Replicated).count();
            if node.storing() {
                assert!(
                    sharded / 5 < own && own < sharded * 4 / 5,
                    "{table:?}: node {} holds {own} of {sharded} rows",
                    node.id
                );
            } else {
                assert_eq!(own, 0, "{table:?}: node {} stores nothing", node.id);
            }
        }
        for row in &want {
            let copies = held.iter().filter(|rows| rows.contains(row)).count();
            let expected = match placement(row) {
                KeyPlacement::Replicated => nodes.len(),
                KeyPlacement::Shard(_) => replication,
            };
            assert_eq!(copies, expected, "{table:?}: copies of {row:02x?}");
        }
        if table == ShardedTable::StoragesTrie {
            assert!(want.iter().any(|row| placement(row) == KeyPlacement::Replicated));
        }
    }
}

/// Every row in `env`'s sharded tables is held by `cluster`'s node.
fn assert_holds_only_own_keys(env: &DatabaseEnv, cluster: &Cluster, context: &str) -> usize {
    let tx = env.tx().unwrap();
    let mut keys = 0;
    for (table, entries) in [
        (ShardedTable::HashedAccounts, raw_walk::<tables::HashedAccounts>(&tx)),
        (ShardedTable::HashedStorages, raw_walk::<tables::HashedStorages>(&tx)),
        (ShardedTable::AccountsTrie, raw_walk::<tables::PackedAccountsTrie>(&tx)),
        (ShardedTable::StoragesTrie, raw_walk::<tables::PackedStoragesTrie>(&tx)),
        (ShardedTable::Bytecodes, raw_walk::<tables::Bytecodes>(&tx)),
    ] {
        for (key, value) in entries {
            let subkey = table.subkey_of(&value).unwrap();
            let held = cluster.holds(table, &key, subkey);
            assert_eq!(held, Some(true), "{context}: {table:?} {key:02x?} {subkey:02x?}");
            keys += 1;
        }
    }
    keys
}

#[test]
fn clustered_nodes_read_by_the_read_rule() {
    for seed in [1, 2] {
        run_cluster(seed, &[], false);
    }
}

#[test]
fn unshed_nodes_read_like_shed_nodes() {
    run_cluster(3, &[2, 5], false);
}

#[test]
fn split_contracts_are_read_by_the_read_rule() {
    run_cluster(4, &[], true);
}

#[test]
fn unshed_nodes_read_a_split_contract_like_shed_nodes() {
    run_cluster(5, &[2, 5], true);
}

#[test]
fn a_trie_read_or_scoped_read_of_rows_not_held_never_asks_a_peer() {
    // Every peer is down: a remote read would fail, and the reads below must not fail.
    let (db, _env, full, _dir) = lone_node(NODES, 6);
    let cluster = db.cluster().unwrap().cluster();
    let seen = visible(cluster, &full);
    let tx = db.tx().unwrap();
    let walked = raw_walk::<tables::PackedAccountsTrie>(&tx);
    assert_eq!(walked, seen.accounts_trie.into_iter().collect::<Vec<_>>());
    assert!(
        walked.iter().all(|(key, _)| key[32] < 2),
        "a stateless node sees replicated paths only"
    );
    let _scope = LocalReads::enter();
    assert_eq!(raw_walk::<tables::HashedAccounts>(&tx), Vec::new());
    let address = B256::from_slice(full.hashed_storages.keys().next().unwrap());
    let mut storages = tx.cursor_dup_read::<tables::HashedStorages>().unwrap();
    assert_eq!(storages.seek_exact(address).unwrap(), None);
    assert_eq!(db.cluster().unwrap().remote_reads().total(), 0);
}

/// Slots of the split contract spread over all ranges.
const SPREAD_SLOTS: usize = 300;
/// Slots, and storage-trie paths, crowded into one range.
const CROWDED: usize = 80;
/// The range crowded slots and paths are in.
const CROWDED_RANGE: u8 = 0x5a;
/// Storage-trie paths of two to five nibbles spread over all ranges.
const SPREAD_PATHS: usize = 120;

/// A large contract whose storage is split: slots in most ranges and none in some, one crowded
/// range, and storage-trie nodes one nibble deep (replicated; the root
/// path is never stored), two nibbles deep and deeper.
#[derive(Debug, Clone)]
struct DenseContract {
    address: B256,
    slots: Vec<B256>,
    paths: Vec<Nibbles>,
}

impl DenseContract {
    fn new(rng: &mut StdRng) -> Self {
        let address = B256::from(rng.random::<[u8; 32]>());
        let mut slots = BTreeSet::new();
        while slots.len() < SPREAD_SLOTS {
            slots.insert(B256::from(rng.random::<[u8; 32]>()));
        }
        while slots.len() < SPREAD_SLOTS + CROWDED {
            let mut slot = B256::from(rng.random::<[u8; 32]>());
            slot.0[0] = CROWDED_RANGE;
            slots.insert(slot);
        }
        let mut paths: BTreeSet<_> =
            (0..16u8).map(|nibble| Nibbles::from_nibbles([nibble])).collect();
        let mut random_path =
            |rng: &mut StdRng, prefix: &[u8], extra: std::ops::RangeInclusive<usize>| {
                let mut nibbles = prefix.to_vec();
                nibbles.extend((0..rng.random_range(extra)).map(|_| rng.random_range(0..16u8)));
                paths.insert(Nibbles::from_nibbles(nibbles));
                paths.len()
            };
        while random_path(rng, &[], 2..=5) < 16 + SPREAD_PATHS {}
        let crowded = [CROWDED_RANGE >> 4, CROWDED_RANGE & 0xf];
        while random_path(rng, &crowded, 1..=3) < 16 + SPREAD_PATHS + CROWDED {}
        Self { address, slots: slots.into_iter().collect(), paths: paths.into_iter().collect() }
    }

    /// Every slot and trie node written: the contract's state at genesis.
    fn genesis(&self, rng: &mut StdRng) -> BlockUpdates {
        let slots = self
            .slots
            .iter()
            .map(|slot| (*slot, U256::from(rng.random_range(1..u64::MAX))))
            .collect();
        let nodes = self.paths.iter().map(|path| (*path, Some(random_node(rng)))).collect();
        self.updates(slots, nodes)
    }

    fn updates(
        &self,
        storage_slots: Vec<(B256, U256)>,
        storage_nodes: Vec<(Nibbles, Option<BranchNodeCompact>)>,
    ) -> BlockUpdates {
        let storage = HashedStorageSorted { storage_slots };
        let trie = StorageTrieUpdatesSorted { storage_nodes };
        BlockUpdates {
            hashed_state: Arc::new(HashedPostStateSorted::new(
                Vec::new(),
                std::iter::once((self.address, storage)).collect(),
            )),
            trie_updates: Arc::new(TrieUpdatesSorted::new(
                Vec::new(),
                std::iter::once((self.address, trie)).collect(),
            )),
        }
    }
}

/// `updates` with the storage and storage-trie changes of `extra` added; `extra`'s win for a
/// contract in both.
fn with_storage(updates: &BlockUpdates, extra: &BlockUpdates) -> BlockUpdates {
    let mut storages = updates.hashed_state.storages.clone();
    storages.extend(extra.hashed_state.storages.iter().map(|(k, v)| (*k, v.clone())));
    let mut tries: B256Map<_> = updates.trie_updates.storage_tries_ref().clone();
    tries.extend(extra.trie_updates.storage_tries_ref().iter().map(|(k, v)| (*k, v.clone())));
    BlockUpdates {
        hashed_state: Arc::new(HashedPostStateSorted::new(
            updates.hashed_state.accounts.clone(),
            storages,
        )),
        trie_updates: Arc::new(TrieUpdatesSorted::new(
            updates.trie_updates.account_nodes_ref().to_vec(),
            tries,
        )),
    }
}

/// One node of the five-node layout over a fully populated database, with its peers down.
fn lone_node(id: u64, seed: u64) -> (ClusteredDatabase, DatabaseEnv, TableDump, TempDir) {
    let mut rng = rng(seed);
    let shards: Vec<_> = (0..24).map(|_| Shard(rng.random())).collect();
    let keys = KeyUniverse::new(&mut rng, &shards);
    let blocks: Vec<_> = (0..3)
        .map(|number| Block {
            number,
            reverts: None,
            updates: random_updates(&mut rng, &keys),
            codes: random_codes(&mut rng),
        })
        .collect();
    let dir = TempDir::new().unwrap();
    let env = populated_env(dir.path(), &blocks);
    let full = TableDump::read_tx(&env.tx().unwrap());
    let listeners: Vec<_> = (0..NODES).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let addresses: Vec<_> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    let listener = listeners.into_iter().nth(id as usize - 1).unwrap();
    let db = ClusteredDatabase::clustered(
        env.clone(),
        cluster_view(id, &addresses, &[]),
        listener,
        options(),
    )
    .unwrap();
    (db, env, full, dir)
}

#[test]
fn shed_deletes_exactly_the_keys_this_node_does_not_hold_and_is_idempotent() {
    let (db, env, full, _dir) = lone_node(2, 7);
    let node = db.cluster().unwrap();
    let cluster = node.cluster();
    let held = |table, key: &Vec<u8>| cluster.holds(table, key, &[]) == Some(true);

    assert_eq!(node.shed_batch(5).unwrap(), 5, "a batch deletes at most its row budget");
    let deleted = 5 + node.shed().unwrap();
    assert_eq!(node.shed().unwrap(), 0, "a second shed finds nothing");

    let after = TableDump::read_tx(&env.tx().unwrap());
    let keep = |table, map: &BTreeMap<Vec<u8>, Vec<u8>>| -> BTreeMap<_, _> {
        map.iter()
            .filter(|(key, _)| held(table, key))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let keep_dup = |table, map: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>| -> BTreeMap<_, _> {
        map.iter()
            .filter(|(key, _)| held(table, key))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    assert_eq!(after.hashed_accounts, keep(ShardedTable::HashedAccounts, &full.hashed_accounts));
    assert_eq!(after.accounts_trie, keep(ShardedTable::AccountsTrie, &full.accounts_trie));
    assert_eq!(
        after.hashed_storages,
        keep_dup(ShardedTable::HashedStorages, &full.hashed_storages)
    );
    assert_eq!(after.storages_trie, keep_dup(ShardedTable::StoragesTrie, &full.storages_trie));
    let tx = env.tx().unwrap();
    let codes_before = full.bytecodes.len();
    let codes_after = raw_walk::<tables::Bytecodes>(&tx);
    assert!(codes_after.iter().all(|(key, _)| held(ShardedTable::Bytecodes, key)));
    let rows = |dump: &TableDump| {
        dump.hashed_accounts.len()
            + dump.accounts_trie.len()
            + dump.hashed_storages.values().map(Vec::len).sum::<usize>()
            + dump.storages_trie.values().map(Vec::len).sum::<usize>()
    };
    assert_eq!(deleted as usize, rows(&full) - rows(&after) + codes_before - codes_after.len());
    assert!(after.accounts_trie.keys().any(|key| key[32] < 2), "replicated trie paths are kept");
}

#[test]
fn commits_that_run_the_pipeline_or_unwind_are_refused() {
    let (db, _env, _full, _dir) = lone_node(1, 8);
    let checkpoint = |stage: StageId| {
        db.tx().unwrap().get::<tables::StageCheckpoints>(stage.to_string()).unwrap()
    };
    let commit = |stages: &[(StageId, u64)]| {
        let tx = db.tx_mut().unwrap();
        for (stage, block) in stages {
            tx.put::<tables::StageCheckpoints>(stage.to_string(), StageCheckpoint::new(*block))
                .unwrap();
        }
        tx.commit()
    };

    commit(&[(StageId::Headers, 5), (StageId::Bodies, 5), (StageId::Finish, 5)]).unwrap();
    assert!(commit(&[(StageId::Headers, 9)]).is_err(), "headers ahead of Finish");
    assert!(commit(&[(StageId::Bodies, 9)]).is_err(), "bodies ahead of Finish");
    assert_eq!(checkpoint(StageId::Headers).map(|c| c.block_number), Some(5), "nothing written");
    assert!(
        commit(&[(StageId::Headers, 3), (StageId::Bodies, 3), (StageId::Finish, 3)]).is_err(),
        "Finish moving back"
    );
    assert_eq!(checkpoint(StageId::Finish).map(|c| c.block_number), Some(5));
    commit(&[(StageId::Headers, 6), (StageId::Bodies, 6), (StageId::Finish, 6)]).unwrap();
}

#[test]
fn reads_no_live_path_needs_are_errors_on_sharded_tables() {
    let (db, _env, full, _dir) = lone_node(1, 9);
    let tx = db.tx().unwrap();
    let mut accounts = tx.cursor_read::<tables::HashedAccounts>().unwrap();
    assert!(accounts.last().is_err());
    assert!(accounts.prev().is_err());
    assert!(accounts.walk_back(None).unwrap().next().unwrap().is_err());
    let mut storages = tx.cursor_dup_read::<tables::HashedStorages>().unwrap();
    assert!(storages.first().is_err());
    assert!(storages.next().is_err());
    assert!(storages.seek(B256::ZERO).is_err());
    assert!(storages.walk_dup(None, None).unwrap().next().unwrap().is_err());
    let address = B256::from_slice(full.hashed_storages.keys().next().unwrap());
    let _ = storages.seek_exact(address);
    assert!(storages.next_no_dup().is_err());
    assert!(storages.last_dup().is_err());
    assert!(storages.prev_dup().is_err());

    // Tables that are not sharded read MDBX as before.
    let mut checkpoints = tx.cursor_read::<tables::StageCheckpoints>().unwrap();
    assert!(checkpoints.last().unwrap().is_some());
}

/// The last subkey of a 32-byte slot that is not the largest possible, so that a seek from the
/// largest finds nothing.
const fn near_last_slot() -> B256 {
    let mut slot = B256::repeat_byte(0xff);
    slot.0[31] = 0xfe;
    slot
}

/// The smallest slot of range `0xff`.
fn last_range_slot() -> Vec<u8> {
    let mut slot = vec![0; 32];
    slot[0] = 0xff;
    slot
}

/// One step of a cursor script run on the reference and on a node, with what it returned.
type Step = (&'static str, Option<Vec<u8>>);

/// Runs a fixed script of positioning calls, duplicate walks and `current` over duplicate-key
/// table `T` through `tx`: misses, walks to the end of split `contract` through its `0xff`
/// range, and reseeks of `other` (an unlisted contract) and of an absent key afterwards.
fn boundary_script<T: DupSort>(
    tx: &impl DbTx,
    contract: B256,
    other: B256,
    mid: Vec<u8>,
    last_range: Vec<u8>,
    past_last: Vec<u8>,
) -> Vec<Step> {
    let value = |row: Option<TableRow<RawDupSort<T>>>| row.map(|(_, value)| value.into_value());
    let mut cursor = tx.cursor_dup_read::<RawDupSort<T>>().unwrap();
    let mut steps = Vec::new();
    let key = |hash: B256| RawKey::<T::Key>::from_vec(hash.to_vec());

    let missed = cursor
        .seek_by_key_subkey(key(contract), RawKey::from_vec(past_last))
        .unwrap()
        .map(|found| found.into_value());
    steps.push(("seek past the last duplicate", missed));
    steps.push(("current after a miss", value(cursor.current().unwrap())));

    let found = cursor
        .seek_by_key_subkey(key(contract), RawKey::from_vec(mid))
        .unwrap()
        .map(|found| found.into_value());
    steps.push(("seek inside a range", found));
    for _ in 0..3 {
        steps.push(("next_dup across ranges", value(cursor.next_dup().unwrap())));
    }
    steps.push(("current inside a range", value(cursor.current().unwrap())));

    let found = cursor
        .seek_by_key_subkey(key(contract), RawKey::from_vec(last_range))
        .unwrap()
        .map(|found| found.into_value());
    steps.push(("seek into the 0xff range", found));
    loop {
        let next = value(cursor.next_dup().unwrap());
        let end = next.is_none();
        steps.push(("next_dup to the end", next));
        if end {
            break;
        }
    }
    steps.push(("current after exhaustion", value(cursor.current().unwrap())));
    steps.push(("next_dup after exhaustion", value(cursor.next_dup().unwrap())));
    steps.push(("current still at the last duplicate", value(cursor.current().unwrap())));

    steps.push(("seek_exact another contract", value(cursor.seek_exact(key(other)).unwrap())));
    let mut walked = vec![value(cursor.next_dup().unwrap())];
    while walked.last().unwrap().is_some() {
        walked.push(value(cursor.next_dup().unwrap()));
    }
    steps.extend(walked.into_iter().map(|next| ("its next_dup to the end", next)));
    steps.push(("its current after exhaustion", value(cursor.current().unwrap())));
    steps.push(("seek_exact the split contract", value(cursor.seek_exact(key(contract)).unwrap())));
    steps.push(("its next_dup", value(cursor.next_dup().unwrap())));
    let absent = B256::repeat_byte(0x01);
    steps.push(("seek_exact an absent key", value(cursor.seek_exact(key(absent)).unwrap())));
    steps.push(("current after that miss", value(cursor.current().unwrap())));
    steps
}

/// A database holding exactly the rows of `dump`, written raw.
fn visible_database(dump: &TableDump) -> (DatabaseEnv, TempDir) {
    let dir = TempDir::new().unwrap();
    let env = init_db(dir.path(), arguments()).unwrap();
    let tx = env.tx_mut().unwrap();
    fn put<T: Table>(tx: &impl DbTxMut, rows: &BTreeMap<Vec<u8>, Vec<u8>>) {
        for (key, value) in rows {
            tx.put::<RawTable<T>>(RawKey::from_vec(key.clone()), RawValue::from_vec(value.clone()))
                .unwrap();
        }
    }
    fn put_duplicates<T: DupSort>(tx: &impl DbTxMut, rows: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>) {
        for (key, values) in rows {
            for value in values {
                let (key, value) =
                    (RawKey::from_vec(key.clone()), RawValue::from_vec(value.clone()));
                tx.put::<RawDupSort<T>>(key, value).unwrap();
            }
        }
    }
    put::<tables::HashedAccounts>(&tx, &dump.hashed_accounts);
    put::<tables::PackedAccountsTrie>(&tx, &dump.accounts_trie);
    put::<tables::Bytecodes>(&tx, &dump.bytecodes);
    put_duplicates::<tables::HashedStorages>(&tx, &dump.hashed_storages);
    put_duplicates::<tables::PackedStoragesTrie>(&tx, &dump.storages_trie);
    tx.commit().unwrap();
    (env, dir)
}

#[test]
fn split_cursor_states_at_range_boundaries_match_a_database_of_the_visible_rows() {
    let mut rng = rng(12);
    let mut dense = DenseContract::new(&mut rng);
    dense.slots.push(near_last_slot());
    dense.slots.sort();
    dense.slots.dedup();
    dense.paths.push(Nibbles::from_nibbles([0xf, 0xf, 0xf]));
    dense.paths.sort();
    dense.paths.dedup();
    let other = B256::from(rng.random::<[u8; 32]>());
    let mut other_slots: Vec<_> =
        (0..5).map(|n| (B256::from(rng.random::<[u8; 32]>()), U256::from(n + 1))).collect();
    other_slots.sort();
    let other_nodes = vec![(Nibbles::from_nibbles([1, 2, 3]), Some(random_node(&mut rng)))];
    let unlisted = DenseContract { address: other, slots: Vec::new(), paths: Vec::new() }
        .updates(other_slots, other_nodes);
    let genesis_updates = with_storage(&unlisted, &dense.genesis(&mut rng));
    let genesis = Block { number: 0, reverts: None, updates: genesis_updates, codes: Vec::new() };
    let reference = factory();
    persist(reference.provider_rw().unwrap(), std::slice::from_ref(&genesis));
    let nodes = start_nodes(&genesis, &[dense.address], &[]);

    let (storage, trie) = boundary_scripts(&snapshot(&reference), dense.address, other);
    assert_eq!(storage[0], ("seek past the last duplicate", None));
    let last = storage.iter().find(|(step, _)| *step == "current after exhaustion").unwrap();
    assert_eq!(last.1.as_deref().map(|value| &value[..32]), Some(near_last_slot().as_slice()));
    assert!(trie.iter().filter(|(step, _)| *step == "next_dup to the end").count() > 1);
    let full = TableDump::read(&reference);
    for node in [&nodes[0], &nodes[1], nodes.last().unwrap()] {
        let (visible_env, _dir) = visible_database(&visible(node.cluster(), &full));
        let want = boundary_scripts(&visible_env.tx().unwrap(), dense.address, other);
        let tx = node.db.tx().unwrap();
        let _scope = LocalReads::enter();
        let found = boundary_scripts(&tx, dense.address, other);
        assert_eq!(found, want, "node {}: cursor scripts", node.id);
    }
}

/// [`boundary_script`] over both duplicate-key tables, seeking inside the crowded range (more
/// than a page), into range `0xff`, and past the last slot or path.
fn boundary_scripts(tx: &impl DbTx, contract: B256, other: B256) -> (Vec<Step>, Vec<Step>) {
    let mut mid_slot = vec![CROWDED_RANGE; 32];
    mid_slot[1] = 0x80;
    let storage = boundary_script::<tables::HashedStorages>(
        tx,
        contract,
        other,
        mid_slot,
        last_range_slot(),
        vec![0xff; 32],
    );
    let mid_path = packed(Nibbles::from_nibbles([CROWDED_RANGE >> 4, CROWDED_RANGE & 0xf, 8]));
    let trie = boundary_script::<tables::PackedStoragesTrie>(
        tx,
        contract,
        other,
        mid_path,
        packed(Nibbles::from_nibbles([0xf, 0xf])),
        packed(Nibbles::from_nibbles([0xf; 6])),
    );
    (storage, trie)
}

#[test]
fn shedding_a_split_contract_one_row_at_a_time_keeps_exactly_the_held_rows() {
    let mut rng = rng(13);
    let dense = DenseContract::new(&mut rng);
    let genesis =
        Block { number: 0, reverts: None, updates: dense.genesis(&mut rng), codes: Vec::new() };
    let dir = TempDir::new().unwrap();
    let env = populated_env(dir.path(), std::slice::from_ref(&genesis));
    let full = TableDump::read_tx(&env.tx().unwrap());
    let listeners: Vec<_> = (0..NODES).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let addresses: Vec<_> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    let view = cluster_view(2, &addresses, &[dense.address]);
    let held = |table: ShardedTable, rows: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>| {
        let mut kept = rows.clone();
        for (key, values) in &mut kept {
            values.retain(|value| {
                view.holds(table, key, table.subkey_of(value).unwrap()) == Some(true)
            });
        }
        kept.retain(|_, values| !values.is_empty());
        kept
    };
    let want_storages = held(ShardedTable::HashedStorages, &full.hashed_storages);
    let want_trie = held(ShardedTable::StoragesTrie, &full.storages_trie);
    let rows = |dump: &BTreeMap<Vec<u8>, Vec<Vec<u8>>>| dump.values().map(Vec::len).sum::<usize>();
    let to_delete = rows(&full.hashed_storages) + rows(&full.storages_trie)
        - rows(&want_storages)
        - rows(&want_trie);
    assert!(to_delete > 100, "{to_delete} rows to shed");

    let open = |listener: TcpListener| {
        ClusteredDatabase::clustered(env.clone(), Arc::clone(&view), listener, options()).unwrap()
    };
    let mut listeners = listeners.into_iter();
    let db = open(listeners.nth(1).unwrap());
    let node = db.cluster().unwrap();
    for _ in 0..to_delete / 2 {
        assert_eq!(node.shed_batch(1).unwrap(), 1);
    }
    drop(db);

    // Reopened mid-shed, the node resumes where it stopped.
    let db = open(TcpListener::bind("127.0.0.1:0").unwrap());
    let node = db.cluster().unwrap();
    let mut deleted = to_delete / 2;
    loop {
        match node.shed_batch(1).unwrap() {
            0 => break,
            1 => deleted += 1,
            more => panic!("a batch of one deleted {more} rows"),
        }
    }
    assert_eq!(deleted, to_delete);
    let after = TableDump::read_tx(&env.tx().unwrap());
    assert_eq!(after.hashed_storages, want_storages);
    assert_eq!(after.storages_trie, want_trie);
}

/// A genesis block over a few random shards, with changesets.
fn small_genesis(seed: u64) -> Block {
    let mut rng = rng(seed);
    let shards: Vec<_> = (0..8).map(|_| Shard(rng.random())).collect();
    let plain = PlainKeys::new(&mut rng, &shards, false);
    let (chain, _) = next_block(&mut rng, &plain, &PlainState::default(), 0);
    let trie_keys = KeyUniverse::new(&mut rng, &shards);
    let trie_updates = random_updates(&mut rng, &trie_keys).trie_updates;
    Block {
        number: 0,
        reverts: Some(chain.reverts),
        updates: BlockUpdates { hashed_state: chain.hashed, trie_updates },
        codes: random_codes(&mut rng),
    }
}

#[test]
fn the_pre_execution_wait_stops_asking_an_unresponsive_replica_whose_shards_are_covered() {
    let (nodes, _silent) = start_nodes_but(&small_genesis(41), &[], &[], &[1]);
    let stateless = nodes.iter().find(|node| !node.storing()).unwrap();
    let node = stateless.db.cluster().unwrap();
    let budget = Duration::from_secs(30);
    node.wait_for_owners(budget).unwrap();

    let started = Instant::now();
    node.wait_for_owners(budget).unwrap();

    let waited = started.elapsed();
    assert!(waited < Duration::from_secs(1), "the silent replica delayed the wait by {waited:?}");
}

#[test]
fn the_pre_execution_wait_ends_within_its_budget_when_needed_owners_do_not_answer() {
    let (nodes, _silent) = start_nodes_but(&small_genesis(42), &[], &[], &[1, 2]);
    let stateless = nodes.iter().find(|node| !node.storing()).unwrap();
    let node = stateless.db.cluster().unwrap();
    let budget = Duration::from_secs(1);
    let started = Instant::now();

    let error = node.wait_for_owners(budget).unwrap_err();

    assert!(matches!(error, OwnersNotReady::Behind { .. }), "{error}");
    let waited = started.elapsed();
    assert!(waited < budget + Duration::from_millis(500), "waited {waited:?}");
}

#[test]
fn a_restarted_node_refuses_results_of_blocks_it_persisted_at_once() {
    let (db, _env, _full, _dir) = lone_node(1, 43);
    let executed = db.cluster().unwrap().executed_blocks();
    let started = Instant::now();

    let refusal = executed.serve(B256::repeat_byte(0x01), 1, Duration::from_secs(5));

    assert!(matches!(refusal, Err(ReadError::NotRetained { newest: 2, .. })), "{refusal:?}");
    assert!(started.elapsed() < Duration::from_secs(1), "waited {:?}", started.elapsed());
}
