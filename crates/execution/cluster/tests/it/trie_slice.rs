//! Trie slices against a full-node oracle: a seeded run of blocks over one full reference MDBX,
//! maintained with reth's full incremental state root, and over simulated clustered nodes that
//! keep only their slice with [`TrieSlice`] plans. After every block, every meaningful row a node
//! holds must equal the reference row byte for byte, starting from replicated nodes whose foreign
//! child hashes were overwritten with garbage. The same run checks the state-root check: every
//! block's subtree hashes, captured by each node and combined across owners, must assemble to
//! the reference state root.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    path::Path,
    sync::Arc,
};

use alloy_primitives::{
    B256, Bytes, U256, keccak256,
    map::{B256Map, B256Set},
};
use base_execution_cluster::{
    BlockShardHashes, CaptureReads, CapturedBlock, CheckOutcome, CheckedBlock, ClientOptions,
    Cluster, ClusterConfig, ClusteredDatabase, LocalReads, NodeConfig, NodeId, RootCaptures,
    RootCheckMode, RootChecker, RootMismatchMarker, Shard, ShardHash, ShardHashSource, ShardSource,
    SkipReason, SourceFailure, TrieHashCapture, TrieSlice,
};
use rand::{Rng, rngs::StdRng, seq::IndexedRandom};
use reth_chainspec::MAINNET;
use reth_db::{ClientVersion, DatabaseEnv, init_db, mdbx::DatabaseArguments};
use reth_db_api::{
    Database, DatabaseError,
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
    BranchNodeCompact, HashedPostState, HashedStorage, MultiProofTargets, Nibbles,
    PackedStorageTrieEntry, PackedStoredNibbles, StateRoot, TrieInput, TrieInputSorted,
    prefix_set::TriePrefixSetsMut,
    proof::Proof,
    trie_cursor::{TrieCursor, TrieCursorFactory},
    updates::TrieUpdates,
};
use reth_trie_db::{
    DatabaseHashedCursorFactory, DatabaseProof, DatabaseStateRoot, DatabaseTrieCursorFactory,
    PackedKeyAdapter,
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
type DbProof<'a, TX> =
    Proof<DatabaseTrieCursorFactory<&'a TX, PackedKeyAdapter>, DatabaseHashedCursorFactory<&'a TX>>;

/// The node that stores nothing and only verifies.
const VERIFIER: u64 = STORING_NODES + 1;

/// Node `node_id` of four storing nodes and the non-storing [`VERIFIER`], splitting `split`.
fn cluster_view(node_id: u64, split: &[B256]) -> Arc<Cluster> {
    let nodes = (1..=VERIFIER)
        .map(|id| NodeConfig {
            id: NodeId(id),
            address: format!("127.0.0.1:{}", 9100 + id).parse().unwrap(),
            storage: id <= STORING_NODES,
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
            root_check: Default::default(),
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
    full_root_and_updates(factory, state).1
}

/// The reference state root and trie updates of `state` over `factory`'s database.
fn full_root_and_updates(factory: &TestFactory, state: &HashedPostState) -> (B256, TrieUpdates) {
    let tx = snapshot(factory);
    let input = TrieInputSorted::from_state(state.clone().into_sorted());
    DbStateRoot::overlay_root_from_nodes_with_updates(&tx, input).unwrap()
}

/// The hash of simulated block `number`.
fn block_hash(number: u64) -> B256 {
    keccak256(number.to_be_bytes())
}

/// Capture reads over a node's database at the parent block, as the engine's overlay provider
/// gives them: stored nodes as they are, and proofs with the block's held post-state and trie
/// updates layered on top.
struct DbCaptureReads<'a, TX> {
    tx: &'a TX,
    capture: &'a TrieHashCapture,
    state: &'a HashedPostState,
    updates: &'a TrieUpdates,
}

impl<TX: DbTx> CaptureReads for DbCaptureReads<'_, TX> {
    type Error = DatabaseError;

    fn parent_account_node(
        &mut self,
        path: Nibbles,
    ) -> Result<Option<BranchNodeCompact>, DatabaseError> {
        let factory = DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(self.tx);
        Ok(factory.account_trie_cursor()?.seek_exact(path)?.map(|(_, node)| node))
    }

    fn parent_storage_node(
        &mut self,
        contract: B256,
        path: Nibbles,
    ) -> Result<Option<BranchNodeCompact>, DatabaseError> {
        let factory = DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(self.tx);
        Ok(factory.storage_trie_cursor(contract)?.seek_exact(path)?.map(|(_, node)| node))
    }

    fn proof_nodes(&mut self, targets: &[B256]) -> Result<Vec<(Nibbles, Bytes)>, DatabaseError> {
        let input = TrieInput::new(
            self.updates.clone(),
            self.capture.held_state(self.state),
            TriePrefixSetsMut::default(),
        );
        let targets = MultiProofTargets::from_iter(
            targets.iter().map(|target| (*target, B256Set::default())),
        );
        let proof = DbProof::from_tx(self.tx)
            .overlay_multiproof(input, targets)
            .map_err(|error| DatabaseError::Other(error.to_string()))?;
        Ok(proof.account_subtree.into_inner().into_iter().collect())
    }
}

/// A simulated clustered node: its MDBX holds only its rows; it computes from that MDBX directly,
/// as trie computation inside a local-reads scope does, and persists through the clustered
/// database's write rule.
struct SliceNode {
    cluster: Arc<Cluster>,
    slice: TrieSlice,
    capture: TrieHashCapture,
    captures: RootCaptures,
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
        let capture = TrieHashCapture::new(&cluster);
        let captures = RootCaptures::default();
        Self { cluster, slice, capture, captures, env, factory, db, _dir: dir }
    }

    /// Computes, captures and persists this node's slice of block `number` with hashed
    /// post-state `state`.
    fn apply(&self, number: u64, state: &HashedPostState) {
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
        let block =
            CapturedBlock { number, hash: block_hash(number), parent_hash: block_hash(number - 1) };
        let mut reads = DbCaptureReads { tx: &tx, capture: &self.capture, state, updates: &trie };
        self.capture.capture(&self.captures, block, state, &trie, &mut reads).unwrap();
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
            node.apply(number, &state);
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

/// Blocks of the state-root check runs.
const CHECKED_BLOCKS: u64 = 6;

/// Owners' captures as a verifier reaches them, without the network. A node mapped to `None` is
/// unreachable.
struct InProcess<'a> {
    nodes: BTreeMap<NodeId, Option<&'a RootCaptures>>,
}

impl<'a> InProcess<'a> {
    fn of(nodes: &'a [SliceNode]) -> Self {
        Self {
            nodes: nodes
                .iter()
                .map(|node| (node.cluster.node_id(), Some(&node.captures)))
                .collect(),
        }
    }

    fn captures(&self, owner: NodeId) -> Result<&'a RootCaptures, SourceFailure> {
        self.nodes.get(&owner).copied().flatten().ok_or(SourceFailure::Unreachable)
    }
}

impl ShardHashSource for InProcess<'_> {
    fn shard_hashes(
        &self,
        owner: NodeId,
        number: u64,
        hash: B256,
    ) -> Result<BlockShardHashes, SourceFailure> {
        let capture = self.captures(owner)?.get(number, hash).ok_or(SourceFailure::NotAvailable)?;
        Ok(capture.shard_hashes())
    }

    fn split_shard_hashes(
        &self,
        owner: NodeId,
        number: u64,
        hash: B256,
        storage_roots: &[(B256, B256)],
    ) -> Result<Vec<(Shard, ShardHash)>, SourceFailure> {
        let capture = self.captures(owner)?.get(number, hash).ok_or(SourceFailure::NotAvailable)?;
        Ok(capture.split_shard_hashes(&storage_roots.iter().copied().collect()))
    }
}

/// A run of [`CHECKED_BLOCKS`] blocks over four poisoned storing nodes and the reference; the
/// last block is quiet.
struct RootRun {
    split: [B256; 2],
    nodes: Vec<SliceNode>,
    /// The reference header of every block, by number - 1.
    headers: Vec<CheckedBlock>,
}

impl RootRun {
    /// Runs the blocks; `after_block` sees the run after each one. Every block also creates an
    /// account in each split contract's account shard, a sibling of its leaf.
    fn run(seed: u64, mut after_block: impl FnMut(&Self, u64)) -> Self {
        let mut rng = rng(seed);
        let mut model = Model::default();
        let (genesis_state, universe) = genesis(&mut rng, &mut model);
        let split = universe.split;
        let reference = factory();
        let genesis_trie = full_root(&reference, &genesis_state);
        let initial = updates(genesis_state, genesis_trie);
        write(&reference, &initial);
        let nodes: Vec<_> =
            (1..=STORING_NODES).map(|id| SliceNode::start(id, &split, &initial, true)).collect();
        for node in &nodes {
            node.poison(&split);
        }
        let mut run = Self { split, nodes, headers: Vec::new() };
        for number in 1..=CHECKED_BLOCKS {
            let mut state = if number == CHECKED_BLOCKS {
                Self::quiet_block(&mut rng, &mut model, split[0])
            } else {
                block(&mut rng, &mut model, &universe, number)
            };
            for contract in split {
                let sibling = random_key(&mut rng, Some(contract[0]));
                state.accounts.insert(sibling, Some(random_account(&mut rng)));
            }
            let (state_root, trie) = full_root_and_updates(&reference, &state);
            write(&reference, &updates(state.clone(), trie));
            for node in &run.nodes {
                node.apply(number, &state);
            }
            run.headers.push(CheckedBlock { number, hash: block_hash(number), state_root });
            after_block(&run, number);
        }
        run
    }

    /// A block that changes one slot of `contract` and, with the split contracts' siblings, few
    /// account shards: most subtree hashes must be carried from the parent's capture.
    fn quiet_block(rng: &mut StdRng, model: &mut Model, contract: B256) -> HashedPostState {
        HashedPostState {
            accounts: B256Map::default(),
            storages: B256Map::from_iter([(contract, model.slot_changes(rng, contract, 1))]),
        }
    }

    fn header(&self, number: u64) -> CheckedBlock {
        self.headers[number as usize - 1]
    }

    fn check(&self, view: u64, source: &InProcess<'_>, number: u64) -> CheckOutcome {
        let cluster = cluster_view(view, &self.split);
        RootChecker::new(&cluster, source).check(self.header(number))
    }

    fn node(&self, id: NodeId) -> &SliceNode {
        self.nodes.iter().find(|node| node.cluster.node_id() == id).unwrap()
    }
}

#[test]
fn captured_hashes_combined_across_owners_verify_every_block_and_stay_pinned() {
    const PINNED: u64 = 2;
    let mut pinned_answers = None;
    let run = RootRun::run(0x2007, |run, number| {
        let source = InProcess::of(&run.nodes);
        for view in [VERIFIER, 1] {
            assert_eq!(
                run.check(view, &source, number),
                CheckOutcome::Verified,
                "block {number} from node {view}'s view"
            );
        }
        if number == PINNED {
            pinned_answers = Some(answers(run, PINNED));
        }
    });

    let split_shards: Vec<_> = run.split.iter().map(|contract| Shard(contract[0])).collect();
    for contract in run.split {
        let holders = run.nodes.iter().filter(|node| node.cluster.owns(Shard(contract[0])));
        assert!(holders.clone().count() == 2 && holders.count() < run.nodes.len());
    }
    assert_eq!(
        answers(&run, PINNED),
        pinned_answers.unwrap(),
        "answers for block {PINNED} changed after the nodes persisted later blocks"
    );
    let source = InProcess::of(&run.nodes);
    for number in 1..=CHECKED_BLOCKS {
        assert_eq!(run.check(VERIFIER, &source, number), CheckOutcome::Verified, "block {number}");
    }
    assert!(split_shards.iter().all(|shard| run.nodes.iter().any(|node| {
        node.captures.get(PINNED, block_hash(PINNED)).unwrap().split_shards.contains_key(shard)
    })));
}

/// Every node's answers for block `number`: its captured hashes and its recomputed split shards
/// with the reference storage roots it would be sent.
fn answers(run: &RootRun, number: u64) -> Vec<(BlockShardHashes, Vec<(Shard, ShardHash)>)> {
    let roots = B256Map::from_iter(run.split.map(|contract| (contract, B256::repeat_byte(7))));
    run.nodes
        .iter()
        .map(|node| {
            let capture = node.captures.get(number, block_hash(number)).unwrap();
            (capture.shard_hashes(), capture.split_shard_hashes(&roots))
        })
        .collect()
}

#[test]
fn a_flipped_captured_hash_is_a_mismatch_naming_its_shard_and_owner() {
    let run = RootRun::run(0xbad, |_, _| {});
    let number = CHECKED_BLOCKS;
    let verifier = cluster_view(VERIFIER, &run.split);
    let shard = Shard::all()
        .find(|shard| !run.split.iter().any(|contract| contract[0] == shard.0))
        .unwrap();
    let owner = verifier.owners(shard)[0];
    let node = run.node(owner);
    let mut corrupt = (*node.captures.get(number, block_hash(number)).unwrap()).clone();
    let flipped = ShardHash::Hash(B256::repeat_byte(0x99));
    assert_ne!(corrupt.accounts.insert(shard, flipped), Some(flipped));
    node.captures.insert(corrupt);

    let outcome = run.check(VERIFIER, &InProcess::of(&run.nodes), number);

    let CheckOutcome::Mismatch(mismatch) = outcome else {
        panic!("expected a mismatch: {outcome:?}")
    };
    assert_eq!(mismatch.block, run.header(number));
    assert!(
        mismatch.sources.contains(&ShardSource { contract: None, shard, owner, value: flipped }),
        "the mismatch names shard {shard:?} and owner {owner:?}"
    );
    let datadir = tempfile::tempdir().unwrap();
    assert!(RootCheckMode::Exit.on_mismatch(&mismatch, datadir.path()));
    assert!(RootMismatchMarker::ensure_absent(datadir.path()).is_err(), "start-up is refused");
    assert_eq!(
        run.check(VERIFIER, &InProcess::of(&run.nodes), number - 1),
        CheckOutcome::Verified,
        "other blocks are unaffected"
    );
}

#[test]
fn owners_without_a_capture_are_passed_over_and_a_block_without_any_is_skipped() {
    let run = RootRun::run(0x5c1b, |_, _| {});
    let number = CHECKED_BLOCKS;
    let restarted = RootCaptures::default();
    let (first, second) = (NodeId(1), NodeId(2));
    let verifier = cluster_view(VERIFIER, &run.split);
    assert!(Shard::all().any(|shard| {
        let owners = verifier.owners(shard);
        owners.contains(&first) && owners.contains(&second)
    }));

    let mut one_restarted = InProcess::of(&run.nodes);
    one_restarted.nodes.insert(first, Some(&restarted));
    assert_eq!(run.check(VERIFIER, &one_restarted, number), CheckOutcome::Verified);
    assert_eq!(run.check(1, &one_restarted, number), CheckOutcome::Verified);

    let mut one_unreachable = InProcess::of(&run.nodes);
    one_unreachable.nodes.insert(first, None);
    assert_eq!(run.check(VERIFIER, &one_unreachable, number), CheckOutcome::Verified);

    let mut both_restarted = InProcess::of(&run.nodes);
    both_restarted.nodes.insert(first, Some(&restarted));
    both_restarted.nodes.insert(second, Some(&restarted));
    assert_eq!(
        run.check(VERIFIER, &both_restarted, number),
        CheckOutcome::Skipped(SkipReason::NotAvailable)
    );

    let mut both_unreachable = InProcess::of(&run.nodes);
    both_unreachable.nodes.insert(first, None);
    both_unreachable.nodes.insert(second, None);
    assert_eq!(
        run.check(VERIFIER, &both_unreachable, number),
        CheckOutcome::Skipped(SkipReason::Unreachable)
    );

    let other_fork = CheckedBlock { hash: B256::repeat_byte(0x0f), ..run.header(number) };
    let cluster = cluster_view(VERIFIER, &run.split);
    assert_eq!(
        RootChecker::new(&cluster, &InProcess::of(&run.nodes)).check(other_fork),
        CheckOutcome::Skipped(SkipReason::NotAvailable),
        "a different block at the same number is not available"
    );
}
