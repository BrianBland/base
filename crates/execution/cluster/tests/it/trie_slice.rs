//! Trie slices against a full-node oracle: a seeded run of blocks over one full reference MDBX,
//! maintained with reth's full incremental state root, and over simulated clustered nodes that
//! keep only their slice with [`TrieSlice`] plans. After every block, every meaningful row a node
//! holds must equal the reference row byte for byte, starting from replicated nodes whose foreign
//! child hashes were overwritten with garbage.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    path::Path,
    sync::Arc,
};

use alloy_primitives::{B256, Bytes, U256, map::B256Map};
use base_execution_cluster::{
    ClientOptions, Cluster, ClusterConfig, ClusteredDatabase, LocalReads, NodeConfig, NodeId,
    Shard, TrieSlice,
};
use rand::{Rng, rngs::StdRng, seq::IndexedRandom};
use reth_chainspec::MAINNET;
use reth_db::{ClientVersion, DatabaseEnv, init_db, mdbx::DatabaseArguments};
use reth_db_api::{
    Database,
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
    models::StorageSettings,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::Account;
use reth_provider::{
    ProviderFactory, StateWriter, StorageSettingsCache, TrieWriter,
    providers::{RocksDBBuilder, StaticFileProvider},
    test_utils::MockNodeTypesWithDB,
};
use reth_trie::{
    BranchNodeCompact, HashedPostState, HashedStorage, Nibbles, PackedStorageTrieEntry,
    PackedStoredNibbles, StateRoot, TrieInput, TrieInputSorted, updates::TrieUpdates,
};
use reth_trie_db::{
    DatabaseHashedCursorFactory, DatabaseStateRoot, DatabaseTrieCursorFactory, PackedKeyAdapter,
};
use tempfile::TempDir;

use crate::common::{BlockUpdates, TableDump, TestFactory, factory, rng, snapshot, write};

/// Storing nodes of the simulated cluster; each shard has two owners.
const STORING_NODES: u64 = 4;
/// Blocks of the run.
const BLOCKS: u64 = 10;
/// Accounts without storage at genesis: dense enough that every two-nibble account-trie node is
/// a stored branch, as on a real network.
const PLAIN_ACCOUNTS: usize = 3_000;
/// Unsplit contracts with storage, and slots of each at genesis.
const CONTRACTS: usize = 16;
const CONTRACT_SLOTS: usize = 40;
/// Slots of each split contract at genesis: dense enough that every two-nibble storage-trie node
/// is a stored branch.
const SPLIT_SLOTS: usize = 5_000;
/// Hash written over the foreign children of replicated nodes before the run.
const POISON: B256 = B256::repeat_byte(0xee);

type DbStateRoot<'a, TX> = StateRoot<
    DatabaseTrieCursorFactory<&'a TX, PackedKeyAdapter>,
    DatabaseHashedCursorFactory<&'a TX>,
>;
type NodeFactory = ProviderFactory<MockNodeTypesWithDB<ClusteredDatabase>>;

/// Node `node_id` of four storing nodes, splitting `split`.
fn cluster_view(node_id: u64, split: &[B256]) -> Arc<Cluster> {
    let nodes = (1..=STORING_NODES)
        .map(|id| NodeConfig {
            id: NodeId(id),
            address: format!("127.0.0.1:{}", 9100 + id).parse().unwrap(),
            storage: true,
        })
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

fn random_key(rng: &mut StdRng, shard: Option<u8>) -> B256 {
    let mut key = B256::from(rng.random::<[u8; 32]>());
    if let Some(shard) = shard {
        key.0[0] = shard;
    }
    key
}

fn random_account(rng: &mut StdRng) -> Account {
    Account {
        nonce: rng.random_range(0..1_000),
        balance: U256::from(rng.random::<u64>()),
        bytecode_hash: None,
    }
}

fn random_value(rng: &mut StdRng) -> U256 {
    U256::from(rng.random_range(1..u64::MAX))
}

/// The latest state, to draw block changes from.
#[derive(Debug, Default)]
struct Model {
    plain: BTreeSet<B256>,
    contracts: BTreeMap<B256, BTreeMap<B256, U256>>,
    /// Slots of each contract that were set and then zeroed, to set again.
    zeroed: BTreeMap<B256, Vec<B256>>,
}

impl Model {
    /// `count` new slots of `contract`.
    fn new_slots(&mut self, rng: &mut StdRng, contract: B256, count: usize) -> HashedStorage {
        let slots = self.contracts.entry(contract).or_default();
        let storage = (0..count).map(|_| (random_key(rng, None), random_value(rng))).collect();
        slots.extend(&storage);
        HashedStorage { storage }
    }

    /// Changes of `count` slots of `contract`: new slots, zeroed slots, and zeroed slots set again.
    fn slot_changes(&mut self, rng: &mut StdRng, contract: B256, count: usize) -> HashedStorage {
        let mut changes = B256Map::default();
        for _ in 0..count {
            let slots = self.contracts.entry(contract).or_default();
            let zeroed = self.zeroed.entry(contract).or_default();
            match rng.random_range(0..3) {
                0 if !slots.is_empty() => {
                    let slot = *slots.keys().nth(rng.random_range(0..slots.len())).unwrap();
                    slots.remove(&slot);
                    zeroed.push(slot);
                    changes.insert(slot, U256::ZERO);
                }
                1 if !zeroed.is_empty() => {
                    let slot = zeroed.swap_remove(rng.random_range(0..zeroed.len()));
                    let value = random_value(rng);
                    slots.insert(slot, value);
                    changes.insert(slot, value);
                }
                _ => {
                    let (slot, value) = (random_key(rng, None), random_value(rng));
                    slots.insert(slot, value);
                    changes.insert(slot, value);
                }
            }
        }
        HashedStorage { storage: changes }
    }
}

/// The run's keys: the split contracts and the genesis state.
struct Universe {
    split: [B256; 2],
    destroyed: B256,
}

/// Genesis: plain accounts, unsplit contracts in every kind of shard, and two split contracts:
/// one whose account shard node 1 holds and node 2 does not, and one the other way round.
fn genesis(rng: &mut StdRng, model: &mut Model) -> (HashedPostState, Universe) {
    let (one, two) = (cluster_view(1, &[]), cluster_view(2, &[]));
    let only = |a: &Cluster, b: &Cluster| {
        Shard::all().find(|shard| a.owns(*shard) && !b.owns(*shard)).unwrap().0
    };
    let split = [random_key(rng, Some(only(&one, &two))), random_key(rng, Some(only(&two, &one)))];
    let mut state = HashedPostState::default();
    for _ in 0..PLAIN_ACCOUNTS {
        let address = random_key(rng, None);
        state.accounts.insert(address, Some(random_account(rng)));
        model.plain.insert(address);
    }
    let mut contracts: Vec<_> = (0..CONTRACTS).map(|_| random_key(rng, None)).collect();
    contracts.extend(split);
    for contract in &contracts {
        state.accounts.insert(*contract, Some(random_account(rng)));
        let slots = if split.contains(contract) { SPLIT_SLOTS } else { CONTRACT_SLOTS };
        let storage = model.new_slots(rng, *contract, slots);
        state.storages.insert(*contract, storage);
    }
    let destroyed = contracts[0];
    (state, Universe { split, destroyed })
}

/// Block `number`'s changes: accounts created, changed and emptied, a contract destroyed in block
/// 3 and created again in block 6, contracts whose only change is storage, and changes in many
/// ranges of both split contracts.
fn block(rng: &mut StdRng, model: &mut Model, universe: &Universe, number: u64) -> HashedPostState {
    let mut state = HashedPostState::default();
    let plain: Vec<_> = model.plain.iter().copied().collect();
    for address in plain.choose_multiple(rng, 40) {
        state.accounts.insert(*address, Some(random_account(rng)));
    }
    for address in plain.choose_multiple(rng, 6) {
        state.accounts.insert(*address, None);
        model.plain.remove(address);
    }
    for _ in 0..15 {
        let address = random_key(rng, None);
        state.accounts.insert(address, Some(random_account(rng)));
        model.plain.insert(address);
    }
    let unsplit: Vec<_> = model
        .contracts
        .keys()
        .filter(|contract| !universe.split.contains(contract) && **contract != universe.destroyed)
        .copied()
        .collect();
    for contract in unsplit.choose_multiple(rng, 5) {
        let storage = model.slot_changes(rng, *contract, 8);
        state.storages.insert(*contract, storage);
    }
    for contract in universe.split {
        let storage = model.slot_changes(rng, contract, 150);
        state.storages.insert(contract, storage);
        if rng.random_bool(0.5) {
            state.accounts.insert(contract, Some(random_account(rng)));
        }
    }
    let destroyed = universe.destroyed;
    match number {
        3 => {
            let slots = model.contracts.remove(&destroyed).unwrap_or_default();
            model.zeroed.remove(&destroyed);
            state.accounts.insert(destroyed, None);
            let zeroed = slots.keys().map(|slot| (*slot, U256::ZERO)).collect();
            state.storages.insert(destroyed, HashedStorage { storage: zeroed });
        }
        6 => {
            state.accounts.insert(destroyed, Some(random_account(rng)));
            let storage = model.slot_changes(rng, destroyed, 5);
            state.storages.insert(destroyed, storage);
        }
        _ => {}
    }
    state
}

fn updates(state: HashedPostState, trie: TrieUpdates) -> BlockUpdates {
    BlockUpdates {
        hashed_state: Arc::new(state.into_sorted()),
        trie_updates: Arc::new(trie.into_sorted()),
    }
}

/// The full root computation of `state` over `factory`'s database: the reference.
fn full_root(factory: &TestFactory, state: &HashedPostState) -> TrieUpdates {
    let tx = snapshot(factory);
    let input = TrieInputSorted::from_state(state.clone().into_sorted());
    DbStateRoot::overlay_root_from_nodes_with_updates(&tx, input).unwrap().1
}

/// A simulated clustered node: its MDBX holds only its rows; it computes from that MDBX directly,
/// as trie computation inside a local-reads scope does, and persists through the clustered
/// database's write rule.
struct SliceNode {
    cluster: Arc<Cluster>,
    slice: TrieSlice,
    env: DatabaseEnv,
    factory: NodeFactory,
    db: ClusteredDatabase,
    _dir: TempDir,
}

impl SliceNode {
    /// Node `node_id`, cloned from the full genesis `initial`, and shed when `shed` is set.
    fn start(node_id: u64, split: &[B256], initial: &BlockUpdates, shed: bool) -> Self {
        let dir = TempDir::new().unwrap();
        let arguments =
            DatabaseArguments::new(ClientVersion::default()).with_geometry_max_size(Some(1 << 30));
        let env = init_db(dir.path().join("db"), arguments).unwrap();
        let populate = node_factory(ClusteredDatabase::new(env.clone()), &dir.path().join("full"));
        let provider = populate.provider_rw().unwrap();
        provider.write_hashed_state(&initial.hashed_state).unwrap();
        provider.write_trie_updates_sorted(&initial.trie_updates).unwrap();
        provider.commit().unwrap();

        let cluster = cluster_view(node_id, split);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let db = ClusteredDatabase::clustered(
            env.clone(),
            Arc::clone(&cluster),
            listener,
            ClientOptions::default(),
        )
        .unwrap();
        if shed {
            assert!(db.cluster().unwrap().shed().unwrap() > 0, "node {node_id} sheds foreign rows");
        }
        let factory = node_factory(db.clone(), &dir.path().join("node"));
        let slice = TrieSlice::new(Arc::clone(&cluster));
        Self { cluster, slice, env, factory, db, _dir: dir }
    }

    /// Computes and persists this node's slice of a block with hashed post-state `state`.
    fn apply(&self, state: &HashedPostState) {
        let plan = self.slice.plan(state).unwrap();
        let tx = self.env.tx().unwrap();
        let trie = plan
            .compute(|input: TrieInput| {
                DbStateRoot::overlay_root_from_nodes_with_updates(
                    &tx,
                    TrieInputSorted::from_unsorted(input),
                )
                .map(|(_, updates)| updates)
            })
            .unwrap();
        drop(tx);
        let block = updates(state.clone(), trie);
        let provider = self.factory.provider_rw().unwrap();
        provider.write_hashed_state(&block.hashed_state).unwrap();
        provider.write_trie_updates_sorted(&block.trie_updates).unwrap();
        provider.commit().unwrap();
    }

    /// Computes this node's slice of a block with hashed post-state `state` as the engine
    /// does: through the clustered database inside a [`LocalReads`] scope, over the unpersisted
    /// ancestors' hashed state and this node's trie updates of them.
    fn compute_clustered(
        &self,
        state: &HashedPostState,
        ancestors: &(TrieUpdates, HashedPostState),
    ) -> TrieUpdates {
        let plan = self.slice.plan(state).unwrap();
        let _local = LocalReads::enter();
        let tx = self.db.tx().unwrap();
        plan.compute(|mut input: TrieInput| {
            input.prepend_cached(ancestors.0.clone(), ancestors.1.clone());
            DbStateRoot::overlay_root_from_nodes_with_updates(
                &tx,
                TrieInputSorted::from_unsorted(input),
            )
            .map(|(_, updates)| updates)
        })
        .unwrap()
    }

    /// Persists `blocks` (hashed state and this node's trie updates) in order, in one commit.
    fn persist(&self, blocks: &[(HashedPostState, TrieUpdates)]) {
        let provider = self.factory.provider_rw().unwrap();
        for (state, trie) in blocks {
            let block = updates(state.clone(), trie.clone());
            provider.write_hashed_state(&block.hashed_state).unwrap();
            provider.write_trie_updates_sorted(&block.trie_updates).unwrap();
        }
        provider.commit().unwrap();
    }

    /// Overwrites every row this node does not hold, as an unfinished shed leaves them, with
    /// garbage: foreign accounts get a different balance, foreign two-nibble account-trie nodes
    /// [`POISON`] hashes. Returns how many rows were overwritten.
    fn poison_leftovers(&self) -> usize {
        let tx = self.env.tx_mut().unwrap();
        let accounts: Vec<_> = tx
            .cursor_read::<tables::HashedAccounts>()
            .unwrap()
            .walk(None)
            .unwrap()
            .map(Result::unwrap)
            .filter(|(address, _)| !self.cluster.owns(Shard(address[0])))
            .collect();
        let nodes: Vec<_> = tx
            .cursor_read::<tables::PackedAccountsTrie>()
            .unwrap()
            .walk(None)
            .unwrap()
            .map(Result::unwrap)
            .filter(|(path, node)| {
                path.0.len() >= 2 && !self.owns(&path.0) && !node.hashes.is_empty()
            })
            .collect();
        let poisoned = accounts.len() + nodes.len();
        for (address, account) in accounts {
            let garbage = Account { balance: account.balance + U256::from(1), ..account };
            tx.put::<tables::HashedAccounts>(address, garbage).unwrap();
        }
        for (path, node) in nodes {
            let hashes = vec![POISON; node.hashes.len()];
            let garbage = BranchNodeCompact::new(
                node.state_mask,
                node.tree_mask,
                node.hash_mask,
                hashes,
                node.root_hash,
            );
            tx.put::<tables::PackedAccountsTrie>(path, garbage).unwrap();
        }
        tx.commit().unwrap();
        poisoned
    }

    fn owns(&self, path: &Nibbles) -> bool {
        self.cluster.owns(Shard(path.get_unchecked(0) << 4 | path.get_unchecked(1)))
    }

    /// Overwrites the hashes of foreign children in every replicated one-nibble node with
    /// [`POISON`].
    fn poison(&self, split: &[B256]) {
        let tx = self.env.tx_mut().unwrap();
        let nodes: Vec<_> = tx
            .cursor_read::<tables::PackedAccountsTrie>()
            .unwrap()
            .walk(None)
            .unwrap()
            .map(Result::unwrap)
            .filter(|(path, _)| path.0.len() == 1)
            .collect();
        for (path, node) in nodes {
            let node = self.poisoned(&path.0, &node);
            tx.put::<tables::PackedAccountsTrie>(path, node).unwrap();
        }
        let mut cursor = tx.cursor_dup_write::<tables::PackedStoragesTrie>().unwrap();
        for contract in split {
            let entries: Vec<_> = cursor
                .walk_dup(Some(*contract), None)
                .unwrap()
                .map(|entry| entry.unwrap().1)
                .filter(|entry| entry.nibbles.0.len() == 1)
                .collect();
            for entry in entries {
                let node = self.poisoned(&entry.nibbles.0, &entry.node);
                cursor.seek_by_key_subkey(*contract, entry.nibbles.clone()).unwrap();
                cursor.delete_current().unwrap();
                cursor
                    .upsert(*contract, &PackedStorageTrieEntry { nibbles: entry.nibbles, node })
                    .unwrap();
            }
        }
        drop(cursor);
        tx.commit().unwrap();
    }

    fn poisoned(&self, path: &Nibbles, node: &BranchNodeCompact) -> BranchNodeCompact {
        let hashes = (0..16u8)
            .filter(|child| node.hash_mask.is_bit_set(*child))
            .map(|child| {
                let mut child_path = *path;
                child_path.push(child);
                if self.owns(&child_path) { node.hash_for_nibble(child) } else { POISON }
            })
            .collect();
        BranchNodeCompact::new(
            node.state_mask,
            node.tree_mask,
            node.hash_mask,
            hashes,
            node.root_hash,
        )
    }
}

fn node_factory(db: ClusteredDatabase, dir: &Path) -> NodeFactory {
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

/// The nibbles of an encoded packed trie path: 32 bytes of packed nibbles, then the count.
fn path_of(encoded: &[u8]) -> Nibbles {
    Nibbles::unpack(&encoded[..32]).slice(..usize::from(encoded[32]))
}

/// A child of a replicated one-nibble node: the storage trie's contract (`None` for the account
/// trie), the node's path, and the child nibble.
type ChildPosition = (Option<B256>, Nibbles, u8);

/// A child's state and tree mask bits and, when its hash bit is set, its hash.
#[derive(Debug, PartialEq, Eq)]
struct ChildFlags {
    present: bool,
    stored: bool,
    hash: Option<B256>,
}

/// What a node's slice holds that must equal the full trie, in raw MDBX bytes.
#[derive(Debug, PartialEq, Eq)]
struct Meaningful {
    accounts_trie: BTreeMap<Vec<u8>, Vec<u8>>,
    storages_trie: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>,
    /// Per replicated one-nibble node (account trie, or a split contract's storage trie): the
    /// state, tree and hash bits and the hash of each held child.
    held_children: BTreeMap<ChildPosition, ChildFlags>,
}

impl SliceNode {
    /// The meaningful rows of `tx`'s tables as this node holds them: account-trie paths of two
    /// or more nibbles in held shards, except those on the path to a split contract's account
    /// leaf; storage tries of held unsplit accounts; held ranges of split contracts; and held
    /// children of replicated one-nibble nodes, except the shards of split contracts' accounts.
    fn meaningful(&self, tx: &impl DbTx, split: &[B256]) -> Meaningful {
        let dump = TableDump::read_tx(tx);
        let split_paths: Vec<_> = split.iter().map(Nibbles::unpack).collect();
        let accounts_trie = dump
            .accounts_trie
            .into_iter()
            .filter(|(key, _)| {
                let path = path_of(key);
                path.len() >= 2
                    && self.owns(&path)
                    && !split_paths.iter().any(|leaf| leaf.starts_with(&path))
            })
            .collect();
        let storages_trie = dump
            .storages_trie
            .into_iter()
            .filter_map(|(address, values)| {
                let address_hash = B256::from_slice(&address);
                let values: BTreeSet<_> = if split.contains(&address_hash) {
                    values
                        .into_iter()
                        .filter(|value| {
                            let path = path_of(&value[..33]);
                            path.len() >= 2 && self.owns(&path)
                        })
                        .collect()
                } else if self.cluster.owns(Shard(address[0])) {
                    values.into_iter().collect()
                } else {
                    BTreeSet::new()
                };
                (!values.is_empty()).then_some((address, values))
            })
            .collect();

        let mut held_children = BTreeMap::new();
        let mut record = |owner: Option<B256>, path: Nibbles, node: &BranchNodeCompact| {
            for child in 0..16u8 {
                let mut child_path = path;
                child_path.push(child);
                let excluded =
                    owner.is_none() && split_paths.iter().any(|leaf| leaf.starts_with(&child_path));
                if !self.owns(&child_path) || excluded {
                    continue;
                }
                let hashed = node.hash_mask.is_bit_set(child);
                held_children.insert(
                    (owner, path, child),
                    ChildFlags {
                        present: node.state_mask.is_bit_set(child),
                        stored: node.tree_mask.is_bit_set(child),
                        hash: hashed.then(|| node.hash_for_nibble(child)),
                    },
                );
            }
        };
        for entry in tx.cursor_read::<tables::PackedAccountsTrie>().unwrap().walk(None).unwrap() {
            let (path, node) = entry.unwrap();
            if path.0.len() == 1 {
                record(None, path.0, &node);
            }
        }
        let mut cursor = tx.cursor_dup_read::<tables::PackedStoragesTrie>().unwrap();
        for contract in split {
            for entry in cursor.walk_dup(Some(*contract), None).unwrap() {
                let entry = entry.unwrap().1;
                if entry.nibbles.0.len() == 1 {
                    record(Some(*contract), entry.nibbles.0, &entry.node);
                }
            }
        }
        Meaningful { accounts_trie, storages_trie, held_children }
    }

    /// Whether a replicated one-nibble account node still carries [`POISON`] for a foreign child.
    fn still_poisoned(&self) -> bool {
        let tx = self.env.tx().unwrap();
        tx.cursor_read::<tables::PackedAccountsTrie>()
            .unwrap()
            .walk(None)
            .unwrap()
            .map(Result::unwrap)
            .any(|(path, node): (PackedStoredNibbles, _)| {
                path.0.len() == 1 && node.hashes.contains(&POISON)
            })
    }
}

#[test]
fn held_trie_rows_match_a_full_node_after_every_block() {
    let mut rng = rng(0x511ce);
    let mut model = Model::default();
    let (genesis_state, universe) = genesis(&mut rng, &mut model);
    let split = universe.split;

    let reference = factory();
    let genesis_trie = full_root(&reference, &genesis_state);
    let initial = updates(genesis_state, genesis_trie);
    write(&reference, &initial);

    let nodes: Vec<_> = [1, 2].map(|id| SliceNode::start(id, &split, &initial, true)).into();
    let at_genesis: Vec<_> =
        nodes.iter().map(|node| node.meaningful(&snapshot(&reference), &split)).collect();
    for node in &nodes {
        node.poison(&split);
        assert!(node.still_poisoned());
    }

    for number in 1..=BLOCKS {
        let state = block(&mut rng, &mut model, &universe, number);
        let trie = full_root(&reference, &state);
        write(&reference, &updates(state.clone(), trie));
        for node in &nodes {
            node.apply(&state);
            let expected = node.meaningful(&snapshot(&reference), &split);
            let actual = node.meaningful(&node.env.tx().unwrap(), &split);
            assert_eq!(
                actual.accounts_trie,
                expected.accounts_trie,
                "block {number}: held account-trie rows of node {:?}",
                node.cluster.node_id()
            );
            assert_eq!(
                actual.storages_trie,
                expected.storages_trie,
                "block {number}: held storage-trie rows of node {:?}",
                node.cluster.node_id()
            );
            assert_eq!(
                actual.held_children,
                expected.held_children,
                "block {number}: held children of replicated nodes of node {:?}",
                node.cluster.node_id()
            );
        }
    }

    for (node, at_genesis) in nodes.iter().zip(at_genesis) {
        let end = node.meaningful(&snapshot(&reference), &split);
        let id = node.cluster.node_id();
        assert!(end.accounts_trie.len() > 100, "node {id:?} compares few account-trie rows");
        assert!(end.held_children.len() > 100, "node {id:?} compares few replicated children");
        assert_ne!(end.accounts_trie, at_genesis.accounts_trie, "node {id:?}: no held row changed");
        assert_ne!(
            end.held_children, at_genesis.held_children,
            "node {id:?}: no held child changed"
        );
        let foreign_account_split = split
            .iter()
            .find(|contract| !node.cluster.owns(Shard(contract[0])))
            .expect("one split contract's account is foreign to each node");
        let key = foreign_account_split.to_vec();
        assert!(
            end.storages_trie.get(&key).is_some_and(|rows| rows.len() > 10)
                && end.storages_trie.get(&key) != at_genesis.storages_trie.get(&key),
            "node {id:?}: held ranges of the split contract whose account it does not hold \
             did not change"
        );
        assert!(node.still_poisoned(), "node {id:?}: the poisoned hashes were all rewritten");
    }
}

/// Unpersisted blocks a node keeps in memory before it persists them together.
const PERSIST_EVERY: u64 = 3;

/// A shard both simulated nodes hold, without contracts, whose neighbour above is foreign to
/// node 1: its two-nibble branch is deleted and created again at a shard boundary.
fn boundary_shard(model: &Model, split: &[B256]) -> u8 {
    let (one, two) = (cluster_view(1, split), cluster_view(2, split));
    (0..255u8)
        .find(|shard| {
            one.owns(Shard(*shard))
                && two.owns(Shard(*shard))
                && !one.owns(Shard(shard + 1))
                && !model.contracts.keys().any(|contract| contract[0] == *shard)
        })
        .expect("a shard held by both nodes next to one node 1 does not hold")
}

/// The engine's path: each node computes through its clustered database inside a
/// [`LocalReads`] scope, over unpersisted ancestors held in memory, on a datadir whose rows of
/// shards it does not hold were never shed and are garbage. Every account of one held shard is
/// deleted in block 4 (removing its two-nibble branch) and the shard is populated again in block
/// 7. After every persistence, held rows equal the full node's.
#[test]
fn held_trie_rows_match_through_the_clustered_database_with_leftovers_and_ancestors() {
    let mut rng = rng(0x1eff);
    let mut model = Model::default();
    let (genesis_state, universe) = genesis(&mut rng, &mut model);
    let split = universe.split;
    let emptied = boundary_shard(&model, &split);

    let reference = factory();
    let genesis_trie = full_root(&reference, &genesis_state);
    let initial = updates(genesis_state, genesis_trie);
    write(&reference, &initial);

    let nodes: Vec<_> = [1, 2].map(|id| SliceNode::start(id, &split, &initial, false)).into();
    for node in &nodes {
        node.poison(&split);
        assert!(node.poison_leftovers() > 100, "few leftover rows to poison");
    }
    let mut pending: Vec<Vec<(HashedPostState, TrieUpdates)>> = vec![Vec::new(); nodes.len()];

    for number in 1..=BLOCKS {
        let mut state = block(&mut rng, &mut model, &universe, number);
        match number {
            4 => {
                let doomed: Vec<_> = model
                    .plain
                    .iter()
                    .chain(state.accounts.keys())
                    .filter(|address| address[0] == emptied)
                    .copied()
                    .collect();
                for address in doomed {
                    state.accounts.insert(address, None);
                    model.plain.remove(&address);
                }
            }
            7 => {
                for _ in 0..12 {
                    let address = random_key(&mut rng, Some(emptied));
                    state.accounts.insert(address, Some(random_account(&mut rng)));
                    model.plain.insert(address);
                }
            }
            _ => {}
        }
        let trie = full_root(&reference, &state);
        write(&reference, &updates(state.clone(), trie));
        let branch = Nibbles::from_nibbles([emptied >> 4, emptied & 0x0f]);
        let reference_has_branch = || {
            snapshot(&reference)
                .get::<tables::PackedAccountsTrie>(PackedStoredNibbles(branch))
                .unwrap()
                .is_some()
        };
        if number == 4 {
            assert!(!reference_has_branch(), "the emptied shard's branch remains");
        }
        if number == 7 {
            assert!(reference_has_branch(), "the shard's branch was not recreated");
        }

        for (node, pending) in nodes.iter().zip(&mut pending) {
            let mut ancestors = (TrieUpdates::default(), HashedPostState::default());
            for (state, trie) in pending.iter() {
                ancestors.0.extend(trie.clone());
                ancestors.1.extend(state.clone());
            }
            let trie = node.compute_clustered(&state, &ancestors);
            pending.push((state.clone(), trie));
            if number % PERSIST_EVERY != 0 && number != BLOCKS {
                continue;
            }
            node.persist(pending);
            pending.clear();
            let expected = node.meaningful(&snapshot(&reference), &split);
            let actual = node.meaningful(&node.env.tx().unwrap(), &split);
            let id = node.cluster.node_id();
            assert_eq!(actual.accounts_trie, expected.accounts_trie, "block {number}: node {id:?}");
            assert_eq!(actual.storages_trie, expected.storages_trie, "block {number}: node {id:?}");
            assert_eq!(actual.held_children, expected.held_children, "block {number}: node {id:?}");
        }
    }
}
