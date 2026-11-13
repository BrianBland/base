//! Clustered databases: a seeded differential test of five clustered nodes against a full
//! reference MDBX, with and without a split contract, the write rule, the commit guard, and
//! shedding; and for split contracts, the read deadline, storage roots and proofs, cursor states
//! at range boundaries, and shedding in small batches.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, Bytes, U256, keccak256, map::B256Map};
use base_execution_cluster::{
    BlockUpdates, ClientOptions, Cluster, ClusterConfig, ClusteredDatabase, DuplicateValues, Frame,
    KeyPlacement, NodeConfig, NodeId, ReadError, ReadResult, ReadSnapshot, Request, Response,
    Shard, ShardedTable,
};
use rand::{Rng, rngs::StdRng};
use reth_chainspec::MAINNET;
use reth_db::{ClientVersion, DatabaseEnv, init_db, mdbx::DatabaseArguments};
use reth_db_api::{
    Database,
    cursor::{DbCursorRO, DbDupCursorRO},
    models::StorageSettings,
    table::{Decode, Decompress, DupSort, Table, TableRow},
    tables::{self, RawDupSort, RawKey, RawTable},
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_provider::{
    DatabaseProviderRW, ProviderFactory, StageCheckpointWriter, StateWriter, StorageSettingsCache,
    TrieWriter,
    providers::{RocksDBBuilder, StaticFileProvider},
    test_utils::MockNodeTypesWithDB,
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_trie::{
    BranchNodeCompact, HashedPostStateSorted, HashedStorageSorted, Nibbles, PackedStorageTrieEntry,
    PackedStoredNibbles, StorageMultiProof, StorageRoot, TrieInputSorted,
    hashed_cursor::{HashedCursor, HashedCursorFactory, HashedStorageCursor},
    proof::StorageProof,
    trie_cursor::{TrieCursor, TrieCursorFactory},
    updates::{StorageTrieUpdatesSorted, TrieUpdatesSorted},
};
use reth_trie_db::{
    DatabaseHashedCursorFactory, DatabaseStorageProof, DatabaseStorageRoot,
    DatabaseTrieCursorFactory, PackedKeyAdapter,
};
use tempfile::TempDir;

use crate::common::{
    KeyUniverse, TableDump, block_hash, canonical, factory, packed, random_node, random_updates,
    rng, snapshot,
};

/// Blocks applied while every node runs, and after one storing node stops.
const BLOCKS: u64 = 18;
const BLOCKS_AFTER_STOP: u64 = 4;
/// Storing nodes are 1..=4; node 5 stores nothing.
const NODES: u64 = 5;
/// Blocks between persistence of each node, so that their persisted tips differ.
const CADENCES: [u64; NODES as usize] = [1, 2, 3, 4, 3];

type NodeFactory = ProviderFactory<MockNodeTypesWithDB<ClusteredDatabase>>;

/// One block of the run: state and trie updates plus new bytecodes.
#[derive(Debug, Clone)]
struct Block {
    number: u64,
    updates: BlockUpdates,
    codes: Vec<(B256, Bytecode)>,
}

/// A provider factory over `db` using storage v2, with its own static files.
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
    factory: NodeFactory,
    persisted: u64,
    pending: Vec<Block>,
    _dir: TempDir,
}

impl Node {
    const fn storing(&self) -> bool {
        self.id < NODES
    }
}

/// Node `node_id`'s view of the five-node cluster, splitting the storage of `split` (hashed
/// addresses).
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
                .map(|hash| Bytes::copy_from_slice(hash.as_slice()))
                .collect(),
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
        })
        .unwrap(),
    )
}

/// Client options of the test nodes: a 2-second retry budget, which is also the deadline of a
/// split read that may cross all 256 ranges on a loaded test machine.
fn options() -> ClientOptions {
    ClientOptions { retry_for: Duration::from_secs(2), ..Default::default() }
}

/// Largest size of a node's MDBX. Each environment reserves this much address space, and the
/// tests open dozens at once.
const NODE_DB_MAX_SIZE: usize = 1 << 30;

/// An MDBX environment holding the full state of `initial`, written through a pass-through
/// database.
fn populated_env(dir: &Path, initial: &[Block]) -> DatabaseEnv {
    let arguments = DatabaseArguments::new(ClientVersion::default())
        .with_geometry_max_size(Some(NODE_DB_MAX_SIZE));
    let env = init_db(dir.join("db"), arguments).unwrap();
    let populate = provider_factory(ClusteredDatabase::new(env.clone()), &dir.join("populate"));
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
/// duplicate-key table `T` through `tx`.
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

/// Every read of the five sharded tables through `db` equals the reference `dump`. Bytecodes are
/// checked only with `latest_codes`, the newest reference, which an owner may answer from.
fn check_node(
    db: &ClusteredDatabase,
    dump: &TableDump,
    keys: &KeyUniverse,
    shards: &[Shard],
    latest_codes: Option<&TableDump>,
    rng: &mut StdRng,
    context: &str,
) {
    let tx = db.tx().unwrap();
    check_ordered::<tables::HashedAccounts>(&tx, &dump.hashed_accounts, rng, shards, context);
    check_ordered::<tables::PackedAccountsTrie>(&tx, &dump.accounts_trie, rng, shards, context);
    let split = keys.split_contracts.as_slice();
    for (contracts, seeks) in [(keys.addresses.as_slice(), 1), (split, SPLIT_SEEKS)] {
        check_duplicates::<tables::HashedStorages>(
            &tx,
            &dump.hashed_storages,
            contracts,
            seeks,
            rng,
            context,
        );
        check_duplicates::<tables::PackedStoragesTrie>(
            &tx,
            &dump.storages_trie,
            contracts,
            seeks,
            rng,
            context,
        );
    }

    // reth's own readers over the clustered transaction.
    let hashed = DatabaseHashedCursorFactory::new(&tx);
    let mut accounts = hashed.hashed_account_cursor().unwrap();
    let mut read = Vec::new();
    let mut entry = accounts.seek(B256::ZERO).unwrap();
    while let Some(found) = entry {
        read.push(found);
        entry = accounts.next().unwrap();
    }
    let want: Vec<_> = dump
        .hashed_accounts
        .iter()
        .map(|(k, v)| (B256::from_slice(k), Account::decompress(v).unwrap()))
        .collect();
    assert_eq!(read, want, "{context}: reth hashed account cursor");
    for address in &keys.addresses {
        assert_eq!(
            tx.get_by_encoded_key::<tables::HashedAccounts>(address).unwrap(),
            dump.hashed_accounts.get(address.as_slice()).map(|v| Account::decompress(v).unwrap()),
            "{context}: get account {address}"
        );
    }
    let trie = DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&tx);
    let mut account_trie = trie.account_trie_cursor().unwrap();
    let mut read = Vec::new();
    let mut entry = account_trie.seek(Nibbles::default()).unwrap();
    while let Some(found) = entry {
        read.push(found);
        entry = account_trie.next().unwrap();
    }
    let want: Vec<_> = dump
        .accounts_trie
        .iter()
        .map(|(k, v)| {
            (PackedStoredNibbles::decode(k).unwrap().0, BranchNodeCompact::decompress(v).unwrap())
        })
        .collect();
    assert_eq!(read, want, "{context}: reth account trie cursor");
    for path in &keys.account_paths {
        let found = account_trie.seek_exact(*path).unwrap().map(|(_, node)| node);
        let want = dump
            .accounts_trie
            .get(&packed(*path))
            .map(|v| BranchNodeCompact::decompress(v).unwrap());
        assert_eq!(found, want, "{context}: reth account trie seek_exact {path:?}");
    }
    for contract in split {
        check_storage_cursors(&tx, dump, *contract, rng, context);
    }

    if let Some(latest) = latest_codes {
        for (hash, latest_code) in &latest.bytecodes {
            let found = tx.get::<RawTable<tables::Bytecodes>>(RawKey::from_vec(hash.clone()));
            let found = found.unwrap().map(|value| value.into_value());
            match dump.bytecodes.get(hash) {
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

/// Seeks per check from a random subkey of a split contract, so that some land in empty ranges.
const SPLIT_SEEKS: usize = 6;

/// reth's storage and storage-trie cursors over `contract` through `tx` read like `dump`.
fn check_storage_cursors(
    tx: &impl DbTx,
    dump: &TableDump,
    contract: B256,
    rng: &mut StdRng,
    context: &str,
) {
    let slots: Vec<(B256, U256)> = dump
        .hashed_storages
        .get(contract.as_slice())
        .into_iter()
        .flatten()
        .map(|value| {
            let entry = StorageEntry::decompress(value).unwrap();
            (entry.key, entry.value)
        })
        .collect();
    let hashed = DatabaseHashedCursorFactory::new(tx);
    let mut storage = hashed.hashed_storage_cursor(contract).unwrap();
    assert_eq!(storage.is_storage_empty().unwrap(), slots.is_empty(), "{context}: storage empty");
    let mut read = Vec::new();
    let mut entry = storage.seek(B256::ZERO).unwrap();
    while let Some(found) = entry {
        read.push(found);
        entry = storage.next().unwrap();
    }
    assert_eq!(read, slots, "{context}: reth storage cursor of {contract}");
    for _ in 0..SPLIT_SEEKS {
        let from = B256::from(rng.random::<[u8; 32]>());
        let found = storage.seek(from).unwrap();
        let want = slots.iter().find(|(slot, _)| *slot >= from).copied();
        assert_eq!(found, want, "{context}: reth storage seek {from} of {contract}");
    }

    let nodes: Vec<(Nibbles, BranchNodeCompact)> = dump
        .storages_trie
        .get(contract.as_slice())
        .into_iter()
        .flatten()
        .map(|value| {
            let entry = PackedStorageTrieEntry::decompress(value).unwrap();
            (entry.nibbles.0, entry.node)
        })
        .collect();
    let trie = DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(tx);
    let mut storage_trie = trie.storage_trie_cursor(contract).unwrap();
    let mut read = Vec::new();
    let mut entry = storage_trie.seek(Nibbles::default()).unwrap();
    while let Some(found) = entry {
        read.push(found);
        entry = storage_trie.next().unwrap();
    }
    assert_eq!(read, nodes, "{context}: reth storage trie cursor of {contract}");
    for (path, node) in &nodes {
        let found = storage_trie.seek_exact(*path).unwrap();
        assert_eq!(found.as_ref(), Some(&(*path, node.clone())), "{context}: seek_exact {path:?}");
    }
}

/// Slots of the split contract spread over all ranges.
const SPREAD_SLOTS: usize = 300;
/// Slots, and storage-trie paths, crowded into one range: more than a remote page.
const CROWDED: usize = ReadSnapshot::PAGE_LEN as usize + 16;
/// The range crowded slots and paths are in.
const CROWDED_RANGE: u8 = 0x5a;
/// Storage-trie paths of two to five nibbles spread over all ranges.
const SPREAD_PATHS: usize = 120;

/// A large contract whose storage is split: slots in most ranges and none in some, one range
/// holding more than a remote page, and storage-trie nodes one nibble deep (replicated; the root
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

    /// One block's changes: about a tenth of the slots and trie nodes rewritten, a third of them
    /// deleted. Deleted ones come back in later blocks as new slots and nodes.
    fn random(&self, rng: &mut StdRng) -> BlockUpdates {
        let mut slots = Vec::new();
        for slot in &self.slots {
            if rng.random_bool(0.1) {
                let deleted = rng.random_bool(0.3);
                let value =
                    if deleted { U256::ZERO } else { U256::from(rng.random_range(1..u64::MAX)) };
                slots.push((*slot, value));
            }
        }
        let mut nodes = Vec::new();
        for path in &self.paths {
            if rng.random_bool(0.1) {
                nodes.push((*path, (!rng.random_bool(0.3)).then(|| random_node(rng))));
            }
        }
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
                    sharded / 4 < own && own < sharded * 3 / 4,
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

/// Five clustered nodes over the state of `genesis` at block 0, splitting the storage of `split`
/// (hashed addresses), each serving that persistence point. Nodes listed in `unshed` never shed.
fn start_nodes(genesis: &Block, split: &[B256], unshed: &[u64]) -> Vec<Node> {
    let listeners: Vec<_> = (0..NODES).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let addresses: Vec<_> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    (1..=NODES)
        .zip(listeners)
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
            assert!(node.install_block_hashes(Box::new(|number| Some(block_hash(0, number)))));
            assert!(node.register_persistence_point().unwrap().is_some());
            let factory = provider_factory(db.clone(), dir.path());
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
        .collect()
}

/// Runs the cluster differential test, with a split contract when `split`. Nodes listed in
/// `unshed` never shed.
fn run_cluster(seed: u64, unshed: &[u64], split: bool) {
    let mut rng = rng(seed);
    let mut shards: BTreeSet<Shard> = [0x00, 0x10, 0x11, 0x7f, 0xf0, 0xff].map(Shard).into();
    shards.extend((0..10).map(|_| Shard(rng.random())));
    let shards: Vec<_> = shards.into_iter().collect();
    let mut keys = KeyUniverse::new(&mut rng, &shards);
    let dense = split.then(|| DenseContract::new(&mut rng));
    keys.split_contracts = dense.iter().map(|dense| dense.address).collect();
    let new_block = |rng: &mut StdRng, number: u64| {
        let mut updates = random_updates(rng, &keys);
        if let Some(dense) = &dense {
            let extra = if number == 0 { dense.genesis(rng) } else { dense.random(rng) };
            updates = with_storage(&updates, &extra);
        }
        Block { number, updates, codes: random_codes(rng) }
    };

    let genesis = new_block(&mut rng, 0);
    let reference = factory();
    persist(reference.provider_rw().unwrap(), std::slice::from_ref(&genesis));
    let mut history = vec![TableDump::read(&reference)];

    let mut nodes = start_nodes(&genesis, &keys.split_contracts, unshed);
    if let Some(dense) = &dense
        && unshed.is_empty()
    {
        assert_split_spread(&nodes, &history[0], dense.address);
    }

    let check_all = |nodes: &[Node], history: &[TableDump], rng: &mut StdRng, label: &str| {
        let codes_tip = nodes.iter().filter(|n| n.storing()).map(|n| n.persisted).min().unwrap();
        for node in nodes {
            let context = format!("seed {seed}, {label}, node {} at {}", node.id, node.persisted);
            let check_codes = node.persisted <= codes_tip;
            check_node(
                &node.db,
                &history[node.persisted as usize],
                &keys,
                &shards,
                check_codes.then(|| history.last().unwrap()),
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
            let parent = block_hash(0, number - 1);
            node.db.cluster().unwrap().owner_store().insert_block(canonical(
                number,
                block_hash(0, number),
                parent,
                &block.updates,
            ));
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

    for node in &nodes {
        let cluster = node.db.cluster().unwrap().cluster();
        let context = format!("seed {seed}, node {}", node.id);
        if unshed.contains(&node.id) {
            let tx = node.env.tx().unwrap();
            let stale = raw_walk::<tables::HashedAccounts>(&tx).iter().any(|(key, _)| {
                cluster.holds(ShardedTable::HashedAccounts, key, &[]) == Some(false)
            });
            assert!(stale, "{context}: an unshed node keeps keys it does not hold");
        } else {
            assert_holds_only_own_keys(&node.env, cluster, &context);
        }
    }
}

#[test]
fn clustered_nodes_read_like_a_full_database() {
    for seed in [1, 2] {
        run_cluster(seed, &[], false);
    }
}

#[test]
fn unshed_nodes_read_like_shed_nodes() {
    run_cluster(3, &[2, 5], false);
}

#[test]
fn split_contracts_spread_over_nodes_and_read_like_a_full_database() {
    run_cluster(4, &[], true);
}

#[test]
fn unshed_nodes_read_a_split_contract_like_shed_nodes() {
    run_cluster(5, &[2, 5], true);
}

/// One node of the five-node layout over a fully populated database, with its peers down.
fn lone_node(id: u64, seed: u64) -> (ClusteredDatabase, DatabaseEnv, TableDump, TempDir) {
    let mut rng = rng(seed);
    let shards: Vec<_> = (0..24).map(|_| Shard(rng.random())).collect();
    let keys = KeyUniverse::new(&mut rng, &shards);
    let blocks: Vec<_> = (0..3)
        .map(|number| Block {
            number,
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

/// More entries than two remote pages hold, so a walk continues across page boundaries.
const PAST_TWO_PAGES: usize = 2 * ReadSnapshot::PAGE_LEN as usize + 8;

/// One shard with [`PAST_TWO_PAGES`] accounts, the first of which has [`PAST_TWO_PAGES`] slots.
fn crowded_shard_block(shard: Shard, rng: &mut StdRng) -> Block {
    let accounts: BTreeMap<B256, Option<Account>> = (0..PAST_TWO_PAGES)
        .map(|nonce| {
            let mut address = B256::from(rng.random::<[u8; 32]>());
            address.0[0] = shard.0;
            (address, Some(Account { nonce: nonce as u64, ..Default::default() }))
        })
        .collect();
    let busy = *accounts.keys().next().unwrap();
    let slots: BTreeMap<B256, U256> = (0..PAST_TWO_PAGES)
        .map(|_| (B256::from(rng.random::<[u8; 32]>()), U256::from(rng.random_range(1..u64::MAX))))
        .collect();
    let storage = HashedStorageSorted { storage_slots: slots.into_iter().collect() };
    Block {
        number: 0,
        updates: BlockUpdates {
            hashed_state: Arc::new(HashedPostStateSorted::new(
                accounts.into_iter().collect(),
                std::iter::once((busy, storage)).collect(),
            )),
            trie_updates: Arc::default(),
        },
        codes: Vec::new(),
    }
}

#[test]
fn remote_walks_continue_across_pages() {
    let shard = Shard(0x42);
    let block = crowded_shard_block(shard, &mut rng(10));
    let reference = factory();
    persist(reference.provider_rw().unwrap(), std::slice::from_ref(&block));
    let expected = TableDump::read(&reference);

    let mut listeners: Vec<_> =
        (0..NODES).map(|_| Some(TcpListener::bind("127.0.0.1:0").unwrap())).collect();
    let addresses: Vec<_> =
        listeners.iter().map(|l| l.as_ref().unwrap().local_addr().unwrap()).collect();
    let mut start_node = |id: u64| {
        let dir = TempDir::new().unwrap();
        let env = populated_env(dir.path(), std::slice::from_ref(&block));
        let listener = listeners[id as usize - 1].take().unwrap();
        let db = ClusteredDatabase::clustered(
            env,
            cluster_view(id, &addresses, &[]),
            listener,
            options(),
        )
        .unwrap();
        let node = db.cluster().unwrap();
        node.shed().unwrap();
        assert!(node.install_block_hashes(Box::new(|number| Some(block_hash(0, number)))));
        assert!(node.register_persistence_point().unwrap().is_some());
        (db, dir)
    };
    let owner = cluster_view(NODES, &addresses, &[]).owners(shard)[0];
    let (_owner, _owner_dir) = start_node(owner.0);
    // Node 5 stores nothing, so it reads every sharded key from an owner.
    let (reader, _reader_dir) = start_node(NODES);
    drop(listeners);

    let tx = reader.tx().unwrap();
    let mut accounts = tx.cursor_read::<RawTable<tables::HashedAccounts>>().unwrap();
    let mut first_key = vec![0; 32];
    first_key[0] = shard.0;
    let mut entry = accounts.seek(RawKey::from_vec(first_key)).unwrap();
    let mut walked = Vec::new();
    while let Some((key, value)) = entry {
        walked.push((key.into_key(), value.into_value()));
        if walked.len() == PAST_TWO_PAGES {
            break;
        }
        entry = accounts.next().unwrap();
    }
    let want: Vec<_> = expected.hashed_accounts.into_iter().collect();
    assert_eq!(want.len(), PAST_TWO_PAGES);
    assert_eq!(walked, want, "accounts of a remote shard");

    let (busy, slots) = expected.hashed_storages.into_iter().next().unwrap();
    assert_eq!(slots.len(), PAST_TWO_PAGES);
    let mut storages = tx.cursor_dup_read::<RawDupSort<tables::HashedStorages>>().unwrap();
    let walked: Vec<_> = storages
        .walk_dup(Some(RawKey::from_vec(busy)), None)
        .unwrap()
        .map(|entry| entry.unwrap().1.into_value())
        .collect();
    assert_eq!(walked, slots, "duplicates of a remote key");

    let counts = reader.cluster().unwrap().remote_reads();
    assert!(counts.seeks >= 3, "{counts:?}");
    assert!(counts.duplicate_seeks >= 3, "{counts:?}");
    assert_eq!(counts.failures, 0, "{counts:?}");
}

/// How long a fake owner takes to answer each request.
const SLOW_ANSWER: Duration = Duration::from_millis(20);

/// How late past its budget a split read may return.
const SPLIT_READ_SLACK: Duration = Duration::from_millis(500);

/// A peer that answers every request after [`SLOW_ANSWER`] with an empty, complete duplicate
/// page: each answer is quick, but one per range adds up to longer than a split read may take.
fn slow_owner() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            std::thread::spawn(move || {
                while Frame::read::<Request>(&mut stream).is_ok() {
                    std::thread::sleep(SLOW_ANSWER);
                    let empty = DuplicateValues { values: Vec::new(), end_of_key: true };
                    let answer: ReadResult = Ok(Response::Duplicates(empty));
                    if Frame::write(&mut stream, &answer).is_err() {
                        return;
                    }
                }
            });
        }
    });
    address
}

#[test]
fn a_split_read_through_slow_owners_fails_within_its_budget() {
    let mut addresses: Vec<_> = (1..NODES).map(|_| slow_owner()).collect();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    addresses.push(listener.local_addr().unwrap());
    let contract = B256::repeat_byte(0x33);
    let empty = Block {
        number: 0,
        updates: BlockUpdates { hashed_state: Arc::default(), trie_updates: Arc::default() },
        codes: Vec::new(),
    };
    let dir = TempDir::new().unwrap();
    let env = populated_env(dir.path(), &[empty]);
    // Node 5 stores nothing, so every range of the empty contract is asked of a slow owner.
    let view = cluster_view(NODES, &addresses, &[contract]);
    let db = ClusteredDatabase::clustered(env, view, listener, options()).unwrap();

    let tx = db.tx().unwrap();
    let mut storages = tx.cursor_dup_read::<tables::HashedStorages>().unwrap();
    let started = Instant::now();
    let found = storages.seek_exact(contract);
    let elapsed = started.elapsed();
    assert!(found.is_err(), "running out of time is an error, not absence: {found:?}");
    assert!(elapsed < options().retry_for + SPLIT_READ_SLACK, "took {elapsed:?}");
}

/// Longer than the former fixed 2-second deadline of a split read.
const PAST_OLD_SPLIT_BUDGET: Duration = Duration::from_millis(2500);

/// A peer that refuses every request as behind until `catch_up`, then answers it with an empty,
/// complete duplicate page.
fn behind_owner(catch_up: Instant) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            std::thread::spawn(move || {
                while Frame::read::<Request>(&mut stream).is_ok() {
                    let answer: ReadResult = if Instant::now() < catch_up {
                        Err(ReadError::SnapshotUnavailable { servable: None })
                    } else {
                        let empty = DuplicateValues { values: Vec::new(), end_of_key: true };
                        Ok(Response::Duplicates(empty))
                    };
                    if Frame::write(&mut stream, &answer).is_err() {
                        return;
                    }
                }
            });
        }
    });
    address
}

#[test]
fn a_split_read_waits_within_the_client_budget_for_owners_behind() {
    let catch_up = Instant::now() + PAST_OLD_SPLIT_BUDGET;
    let mut addresses: Vec<_> = (1..NODES).map(|_| behind_owner(catch_up)).collect();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    addresses.push(listener.local_addr().unwrap());
    let contract = B256::repeat_byte(0x33);
    let empty = Block {
        number: 0,
        updates: BlockUpdates { hashed_state: Arc::default(), trie_updates: Arc::default() },
        codes: Vec::new(),
    };
    let dir = TempDir::new().unwrap();
    let env = populated_env(dir.path(), &[empty]);
    // Node 5 stores nothing, so every range of the empty contract is asked of an owner.
    let view = cluster_view(NODES, &addresses, &[contract]);
    let options = ClientOptions { retry_for: Duration::from_secs(10), ..Default::default() };
    let db = ClusteredDatabase::clustered(env, view, listener, options).unwrap();

    let tx = db.tx().unwrap();
    let mut storages = tx.cursor_dup_read::<tables::HashedStorages>().unwrap();
    assert_eq!(storages.seek_exact(contract).unwrap(), None);
    let counts = db.cluster().unwrap().remote_reads();
    assert_eq!(counts.failures, 0, "{counts:?}");
    assert_eq!(counts.waits, 1, "{counts:?}");
}

/// A reth storage root calculator over a database transaction with storage v2 trie keys.
type DbStorageRoot<'a, TX> = StorageRoot<
    DatabaseTrieCursorFactory<&'a TX, PackedKeyAdapter>,
    DatabaseHashedCursorFactory<&'a TX>,
>;
/// A reth storage proof calculator over a database transaction with storage v2 trie keys.
type DbStorageProof<'a, TX> = StorageProof<
    DatabaseTrieCursorFactory<&'a TX, PackedKeyAdapter>,
    DatabaseHashedCursorFactory<&'a TX>,
>;

/// The storage root and storage multiproof of `address` that live execution computes over `tx`
/// with `changes` laid over it, for the slots `targets` (not hashed).
fn overlay_root_and_proof<TX: DbTx>(
    tx: &TX,
    address: Address,
    changes: &TrieInputSorted,
    targets: &[B256],
) -> (B256, StorageMultiProof) {
    let root = DbStorageRoot::overlay_root(tx, address, changes.clone()).unwrap();
    let proof =
        DbStorageProof::overlay_storage_multiproof(tx, address, targets, changes.clone()).unwrap();
    (root, proof)
}

/// The storage root and storage multiproof of `address` over the trie stored in `tx`.
fn stored_root_and_proof<TX: DbTx>(
    tx: &TX,
    address: Address,
    targets: &[B256],
) -> (B256, StorageMultiProof) {
    let root = DbStorageRoot::from_tx_hashed(tx, keccak256(address)).root().unwrap();
    let hashed_targets = targets.iter().map(keccak256).collect();
    let proof = DbStorageProof::from_tx(tx, address).storage_multiproof(hashed_targets).unwrap();
    (root, proof)
}

/// Slots of the contract whose real storage trie the root test builds: enough for branch nodes
/// two nibbles deep in every range.
const TRIE_SLOTS: usize = 1500;

/// One block writing `slots` (hashed) of the contract at `hashed` and its storage-trie `nodes`.
fn storage_block(
    number: u64,
    hashed: B256,
    slots: Vec<(B256, U256)>,
    nodes: StorageTrieUpdatesSorted,
) -> Block {
    let storage = HashedStorageSorted { storage_slots: slots };
    Block {
        number,
        updates: BlockUpdates {
            hashed_state: Arc::new(HashedPostStateSorted::new(
                Vec::new(),
                std::iter::once((hashed, storage)).collect(),
            )),
            trie_updates: Arc::new(TrieUpdatesSorted::new(
                Vec::new(),
                std::iter::once((hashed, nodes)).collect(),
            )),
        },
        codes: Vec::new(),
    }
}

/// Writes `slots` of the contract at `hashed` to `reference` at block `number`, then computes its
/// storage trie there with reth's code, only below the changed slots when `changed_only`, and
/// writes it too. Returns the block the nodes persist: the slots and the trie nodes written.
fn persist_with_trie(
    reference: &crate::common::TestFactory,
    number: u64,
    hashed: B256,
    slots: Vec<(B256, U256)>,
    changed_only: bool,
) -> Block {
    let block = storage_block(number, hashed, slots, StorageTrieUpdatesSorted::default());
    persist(reference.provider_rw().unwrap(), std::slice::from_ref(&block));
    let tx = snapshot(reference);
    let mut calculator = DbStorageRoot::from_tx_hashed(&tx, hashed);
    if changed_only {
        let mut prefix_sets = block.updates.hashed_state.construct_prefix_sets();
        let changed = prefix_sets.storage_prefix_sets.remove(&hashed).unwrap();
        calculator = calculator.with_prefix_set(changed.freeze());
    }
    let (_, _, nodes) = calculator.root_with_updates().unwrap();
    drop(tx);
    let nodes = nodes.into_sorted();
    let slots = block.updates.hashed_state.storages[&hashed].storage_slots.clone();
    let trie_only = storage_block(number, hashed, Vec::new(), nodes.clone());
    persist(reference.provider_rw().unwrap(), std::slice::from_ref(&trie_only));
    storage_block(number, hashed, slots, nodes)
}

#[test]
fn split_storage_roots_and_proofs_match_the_reference() {
    let mut rng = rng(11);
    let address = Address::from(rng.random::<[u8; 20]>());
    let hashed = keccak256(address);
    let preimages: Vec<B256> =
        (0..TRIE_SLOTS).map(|_| B256::from(rng.random::<[u8; 32]>())).collect();
    let value = |rng: &mut StdRng| U256::from(rng.random_range(1..u64::MAX));
    let mut slots: Vec<_> =
        preimages.iter().map(|slot| (keccak256(slot), value(&mut rng))).collect();
    slots.sort();

    let reference = factory();
    let genesis = persist_with_trie(&reference, 0, hashed, slots, false);
    let nodes = start_nodes(&genesis, &[hashed], &[]);
    let storing = &nodes[0];
    let stateless = nodes.last().unwrap();
    assert!(!stateless.storing());

    // Block 1 rewrites and deletes slots, and adds new ones, in held and non-held ranges alike.
    let added: Vec<B256> = (0..100).map(|_| B256::from(rng.random::<[u8; 32]>())).collect();
    let mut changes: BTreeMap<B256, U256> = BTreeMap::new();
    for slot in &preimages {
        if !rng.random_bool(0.2) {
            continue;
        }
        let new = if rng.random_bool(0.3) { U256::ZERO } else { value(&mut rng) };
        changes.insert(keccak256(slot), new);
    }
    for slot in &added {
        changes.insert(keccak256(slot), value(&mut rng));
    }
    let cluster = storing.db.cluster().unwrap().cluster();
    let held = changes
        .keys()
        .filter(|slot| {
            cluster.holds(ShardedTable::HashedStorages, hashed.as_slice(), slot.as_slice())
                == Some(true)
        })
        .count();
    assert!(0 < held && held < changes.len(), "{held} of {} changes held", changes.len());
    let changes: Vec<_> = changes.into_iter().collect();
    let overlay = storage_block(1, hashed, changes.clone(), StorageTrieUpdatesSorted::default());
    let state = Arc::clone(&overlay.updates.hashed_state);
    let input =
        TrieInputSorted::new(Arc::default(), Arc::clone(&state), state.construct_prefix_sets());
    let mut targets: Vec<B256> = preimages.iter().step_by(97).copied().collect();
    targets.extend(added.iter().take(3));
    targets.push(B256::repeat_byte(0x77));

    // At block 0, the root and proof with block 1's changes laid over the stored trie.
    let want = overlay_root_and_proof(&snapshot(&reference), address, &input, &targets);
    for node in [storing, stateless] {
        let found = overlay_root_and_proof(&node.db.tx().unwrap(), address, &input, &targets);
        assert_eq!(found, want, "node {}: overlay root and proof at block 0", node.id);
    }

    // At block 1, with block 1's slots and trie nodes persisted, the root and proof over the
    // stored trie.
    let block = persist_with_trie(&reference, 1, hashed, changes, true);
    let want_stored = stored_root_and_proof(&snapshot(&reference), address, &targets);
    assert_eq!(want_stored.0, want.0, "the persisted trie has the overlay's root");
    for node in &nodes {
        let owner_store = node.db.cluster().unwrap().owner_store();
        owner_store.insert_block(canonical(1, block_hash(0, 1), block_hash(0, 0), &block.updates));
        persist(node.factory.provider_rw().unwrap(), std::slice::from_ref(&block));
    }
    for node in [storing, stateless] {
        let found = stored_root_and_proof(&node.db.tx().unwrap(), address, &targets);
        assert_eq!(found, want_stored, "node {}: stored root and proof at block 1", node.id);
    }
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

#[test]
fn split_cursor_states_at_range_boundaries_match_the_reference() {
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
    let genesis = Block { number: 0, updates: genesis_updates, codes: Vec::new() };
    let reference = factory();
    persist(reference.provider_rw().unwrap(), std::slice::from_ref(&genesis));
    let nodes = start_nodes(&genesis, &[dense.address], &[]);

    let want = boundary_scripts(&snapshot(&reference), dense.address, other);
    let (storage, trie) = &want;
    assert_eq!(storage[0], ("seek past the last duplicate", None));
    let last = storage.iter().find(|(step, _)| *step == "current after exhaustion").unwrap();
    assert_eq!(last.1.as_deref().map(|value| &value[..32]), Some(near_last_slot().as_slice()));
    assert!(trie.iter().filter(|(step, _)| *step == "next_dup to the end").count() > 1);
    for node in [&nodes[0], &nodes[1], nodes.last().unwrap()] {
        let found = boundary_scripts(&node.db.tx().unwrap(), dense.address, other);
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
    let genesis = Block { number: 0, updates: dense.genesis(&mut rng), codes: Vec::new() };
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
