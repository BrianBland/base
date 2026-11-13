//! Shared fixtures: a three-node cluster, MDBX test databases written with reth's own writers,
//! seeded random block updates, chains of blocks over plain keys with real changesets, and raw
//! dumps of the sharded tables.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy_eips::BlockNumHash;
use alloy_primitives::{Address, B256, Bytes, U256, keccak256, map::B256Map};
use base_execution_cluster::{
    CanonicalBlock, Cluster, ClusterConfig, NodeConfig, NodeId, OwnerStore, ProviderChangesets,
    Shard,
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use reth_db::{DatabaseEnv, test_utils::TempDatabase};
use reth_db_api::{
    Database, DatabaseError,
    cursor::DbCursorRO,
    models::StorageSettings,
    table::Table,
    tables::{self, RawTable},
    transaction::DbTx,
};
use reth_primitives_traits::Account;
use reth_provider::{
    ProviderFactory, ProviderResult, StageCheckpointWriter, StateWriteConfig, StateWriter,
    StorageSettingsCache, TrieWriter,
    test_utils::{MockNodeTypesWithDB, create_test_provider_factory},
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_trie::{
    BranchNodeCompact, HashedPostStateSorted, HashedStorageSorted, Nibbles, PackedStoredNibbles,
    updates::{StorageTrieUpdatesSorted, TrieUpdatesSorted},
};
use revm::{
    database::states::{PlainStateReverts, PlainStorageRevert, RevertToSlot},
    state::AccountInfo,
};

/// The sorted state and trie changes of one block, as reth writes them.
#[derive(Debug, Clone)]
pub(crate) struct BlockUpdates {
    /// Hashed accounts and storage written by the block. Deletions are explicit per key.
    pub(crate) hashed_state: Arc<HashedPostStateSorted>,
    /// Trie nodes written by the block. Removed nodes are explicit per path.
    pub(crate) trie_updates: Arc<TrieUpdatesSorted>,
}

/// A provider factory over a temporary MDBX using storage v2 (packed trie keys).
pub(crate) type TestFactory = ProviderFactory<MockNodeTypesWithDB>;

/// Read transaction type of [`TestFactory`].
pub(crate) type TestTx = <Arc<TempDatabase<DatabaseEnv>> as Database>::TX;

/// Creates an empty storage-v2 test database.
pub(crate) fn factory() -> TestFactory {
    let factory = create_test_provider_factory();
    factory.set_storage_settings_cache(StorageSettings::v2());
    factory
}

/// A read snapshot of `factory`'s current state.
pub(crate) fn snapshot(factory: &TestFactory) -> TestTx {
    factory.db_ref().tx().unwrap()
}

/// Writes `updates` with reth's writers in one committed transaction.
pub(crate) fn write(factory: &TestFactory, updates: &BlockUpdates) {
    let provider = factory.provider_rw().unwrap();
    provider.write_hashed_state(&updates.hashed_state).unwrap();
    provider.write_trie_updates_sorted(&updates.trie_updates).unwrap();
    provider.commit().unwrap();
}

/// Three storage nodes, `R = 2`, seen from `node_id`.
pub(crate) fn cluster(node_id: u64) -> Arc<Cluster> {
    cluster_splitting(node_id, &[])
}

/// Three storage nodes, `R = 2`, seen from `node_id`, splitting the storage of `split`.
pub(crate) fn cluster_splitting(node_id: u64, split: &[Address]) -> Arc<Cluster> {
    let nodes = (1..=3)
        .map(|id| NodeConfig {
            id: NodeId(id),
            address: format!("127.0.0.1:{}", 9000 + id).parse().unwrap(),
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
                .map(|address| Bytes::copy_from_slice(address.as_slice()))
                .collect(),
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
        })
        .unwrap(),
    )
}

/// Encoded packed account-trie key of `path`.
pub(crate) fn packed(path: Nibbles) -> Vec<u8> {
    PackedStoredNibbles(path).to_compact_array().to_vec()
}

/// Canonical block `number` of fork 0 with its hashed state.
pub(crate) fn canonical(number: u64, hashed_state: &Arc<HashedPostStateSorted>) -> CanonicalBlock {
    CanonicalBlock {
        block: BlockNumHash::new(number, block_hash(0, number)),
        parent_hash: block_hash(0, number.wrapping_sub(1)),
        hashed_state: Some(Arc::clone(hashed_state)),
    }
}

/// Deterministic block hash of `number` on fork `fork`.
pub(crate) fn block_hash(fork: u8, number: u64) -> B256 {
    keccak256([&[fork][..], &number.to_be_bytes()].concat())
}

/// Keys the generator draws from, spread over owned and unowned shards.
#[derive(Debug, Clone)]
pub(crate) struct KeyUniverse {
    pub(crate) addresses: Vec<B256>,
    pub(crate) slots: Vec<B256>,
    pub(crate) account_paths: Vec<Nibbles>,
    pub(crate) storage_paths: Vec<Nibbles>,
}

impl KeyUniverse {
    /// Draws keys whose first byte is one of `shards`. Account-trie paths include replicated
    /// one-nibble paths; storage-trie paths have one to five nibbles.
    pub(crate) fn new(rng: &mut StdRng, shards: &[Shard]) -> Self {
        let mut addresses = BTreeSet::new();
        for shard in shards {
            for _ in 0..4 {
                let mut address = B256::from(rng.random::<[u8; 32]>());
                address.0[0] = shard.0;
                addresses.insert(address);
            }
        }
        let slots = (0..6).map(|_| B256::from(rng.random::<[u8; 32]>())).collect();
        let mut account_paths = BTreeSet::new();
        for shard in shards {
            account_paths.insert(Nibbles::from_nibbles([shard.0 >> 4]));
            for _ in 0..5 {
                let mut path = vec![shard.0 >> 4, shard.0 & 0xf];
                path.extend((0..rng.random_range(0..5)).map(|_| rng.random_range(0..16u8)));
                account_paths.insert(Nibbles::from_nibbles(path));
            }
        }
        let storage_paths = (0..8)
            .map(|_| {
                let len = rng.random_range(1..6);
                Nibbles::from_nibbles(
                    (0..len).map(|_| rng.random_range(0..16u8)).collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self {
            addresses: addresses.into_iter().collect(),
            slots,
            account_paths: account_paths.into_iter().collect(),
            storage_paths,
        }
    }
}

pub(crate) fn random_node(rng: &mut StdRng) -> BranchNodeCompact {
    let state_mask: u16 = rng.random_range(1..=u16::MAX);
    let tree_mask = state_mask & rng.random::<u16>();
    let hash_mask = state_mask & rng.random::<u16>();
    let hashes =
        (0..hash_mask.count_ones()).map(|_| B256::from(rng.random::<[u8; 32]>())).collect();
    let root_hash = rng.random_bool(0.5).then(|| B256::from(rng.random::<[u8; 32]>()));
    BranchNodeCompact::new(state_mask, tree_mask, hash_mask, hashes, root_hash)
}

/// Random updates of one block: account writes and deletions, storage writes and zeroed slots,
/// trie node writes and removals.
pub(crate) fn random_updates(rng: &mut StdRng, keys: &KeyUniverse) -> BlockUpdates {
    const DELETE: f64 = 0.3;
    let mut accounts = BTreeMap::new();
    let mut storages = B256Map::default();
    let mut account_nodes = BTreeMap::new();
    let mut storage_tries = B256Map::default();

    for address in &keys.addresses {
        if rng.random_bool(0.4) {
            let account = (!rng.random_bool(DELETE)).then(|| Account {
                nonce: rng.random(),
                balance: U256::from(rng.random::<u64>()),
                bytecode_hash: rng.random_bool(0.5).then(|| B256::from(rng.random::<[u8; 32]>())),
            });
            accounts.insert(*address, account);
        }
        if rng.random_bool(0.4) {
            let slots: BTreeMap<_, _> = keys
                .slots
                .iter()
                .filter_map(|slot| {
                    if !rng.random_bool(0.5) {
                        return None;
                    }
                    let value = if rng.random_bool(DELETE) {
                        U256::ZERO
                    } else {
                        U256::from(rng.random_range(1..u64::MAX))
                    };
                    Some((*slot, value))
                })
                .collect();
            storages.insert(
                *address,
                HashedStorageSorted { storage_slots: slots.into_iter().collect() },
            );
        }
        if rng.random_bool(0.4) {
            let nodes: BTreeMap<_, _> = keys
                .storage_paths
                .iter()
                .filter_map(|path| {
                    if !rng.random_bool(0.5) {
                        return None;
                    }
                    Some((*path, (!rng.random_bool(DELETE)).then(|| random_node(rng))))
                })
                .collect();
            storage_tries.insert(
                *address,
                StorageTrieUpdatesSorted { storage_nodes: nodes.into_iter().collect() },
            );
        }
    }
    for path in &keys.account_paths {
        if rng.random_bool(0.4) {
            account_nodes.insert(*path, (!rng.random_bool(DELETE)).then(|| random_node(rng)));
        }
    }

    BlockUpdates {
        hashed_state: Arc::new(HashedPostStateSorted::new(
            accounts.into_iter().collect(),
            storages,
        )),
        trie_updates: Arc::new(TrieUpdatesSorted::new(
            account_nodes.into_iter().collect(),
            storage_tries,
        )),
    }
}

/// Plain keys a chain of blocks draws from: addresses spread over chosen shards, slots, and
/// optionally a split contract with many slots.
#[derive(Debug, Clone)]
pub(crate) struct PlainKeys {
    pub(crate) addresses: Vec<Address>,
    pub(crate) slots: Vec<B256>,
    pub(crate) split: Option<Address>,
    pub(crate) split_slots: Vec<B256>,
}

impl PlainKeys {
    /// Three addresses whose hashed address is in each of `shards`, eight slots, and with
    /// `split` a contract with 300 slots.
    pub(crate) fn new(rng: &mut StdRng, shards: &[Shard], split: bool) -> Self {
        let mut addresses = Vec::new();
        for shard in shards {
            let mut found = 0;
            while found < 3 {
                let address = Address::from(rng.random::<[u8; 20]>());
                if keccak256(address)[0] == shard.0 {
                    addresses.push(address);
                    found += 1;
                }
            }
        }
        let slots = (0..8).map(|_| B256::from(rng.random::<[u8; 32]>())).collect();
        let split = split.then(|| Address::from(rng.random::<[u8; 20]>()));
        let split_slots = split
            .map(|_| (0..300).map(|_| B256::from(rng.random::<[u8; 32]>())).collect())
            .unwrap_or_default();
        Self { addresses, slots, split, split_slots }
    }

    /// Every (address, slot) pair the chain may write.
    pub(crate) fn storage_keys(&self) -> Vec<(Address, B256)> {
        let plain = self
            .addresses
            .iter()
            .flat_map(|address| self.slots.iter().map(|slot| (*address, *slot)));
        let split = self
            .split
            .iter()
            .flat_map(|address| self.split_slots.iter().map(|slot| (*address, *slot)));
        plain.chain(split).collect()
    }
}

/// The plain state after a block.
#[derive(Debug, Clone, Default)]
pub(crate) struct PlainState {
    pub(crate) accounts: BTreeMap<Address, Account>,
    pub(crate) storage: BTreeMap<(Address, B256), U256>,
}

impl PlainState {
    /// The slot's value, zero when absent.
    pub(crate) fn slot(&self, address: Address, slot: B256) -> U256 {
        self.storage.get(&(address, slot)).copied().unwrap_or_default()
    }

    /// Whether the account has any storage.
    pub(crate) fn has_storage(&self, address: Address) -> bool {
        self.storage
            .range((address, B256::ZERO)..=(address, B256::repeat_byte(0xff)))
            .next()
            .is_some()
    }
}

/// One block of a chain over plain keys: its changesets (previous values) and hashed state (new
/// values).
#[derive(Debug, Clone)]
pub(crate) struct ChainBlock {
    pub(crate) number: u64,
    pub(crate) reverts: PlainStateReverts,
    pub(crate) hashed: Arc<HashedPostStateSorted>,
}

/// The next block after `state`: accounts created, changed, emptied (deleted) and recreated,
/// slots set, zeroed and set again. Returns the block and the state after it.
pub(crate) fn next_block(
    rng: &mut StdRng,
    keys: &PlainKeys,
    state: &PlainState,
    number: u64,
) -> (ChainBlock, PlainState) {
    let mut after = state.clone();
    let mut account_reverts = Vec::new();
    let mut accounts = Vec::new();
    for address in &keys.addresses {
        let before = state.accounts.get(address).copied();
        let new = match rng.random_range(0..10) {
            0..=2 => Some(Account {
                nonce: rng.random_range(0..1000),
                balance: U256::from(rng.random_range(1..u64::MAX)),
                bytecode_hash: None,
            }),
            3 if before.is_some() => None,
            _ => continue,
        };
        let info = |account: Account| AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            ..Default::default()
        };
        account_reverts.push((*address, before.map(info)));
        accounts.push((keccak256(address), new));
        match new {
            Some(account) => after.accounts.insert(*address, account),
            None => after.accounts.remove(address),
        };
    }
    let mut storage_reverts = Vec::new();
    let mut storages: B256Map<Vec<(B256, U256)>> = B256Map::default();
    let mut write = |rng: &mut StdRng, address: Address, slot: B256, after: &mut PlainState| {
        let before = state.slot(address, slot);
        let new = if rng.random_bool(0.35) {
            U256::ZERO
        } else {
            U256::from(rng.random_range(1..u64::MAX))
        };
        if new == before {
            return;
        }
        storage_reverts.push(PlainStorageRevert {
            address,
            wiped: false,
            storage_revert: vec![(U256::from_be_bytes(slot.0), RevertToSlot::Some(before))],
        });
        storages.entry(keccak256(address)).or_default().push((keccak256(slot), new));
        if new.is_zero() {
            after.storage.remove(&(address, slot));
        } else {
            after.storage.insert((address, slot), new);
        }
    };
    for address in &keys.addresses {
        if rng.random_bool(0.4) {
            for slot in &keys.slots {
                if rng.random_bool(0.5) {
                    write(rng, *address, *slot, &mut after);
                }
            }
        }
    }
    if let Some(split) = keys.split {
        for slot in &keys.split_slots {
            if number == 0 || rng.random_bool(0.2) {
                write(rng, split, *slot, &mut after);
            }
        }
    }
    accounts.sort_by_key(|(address, _)| *address);
    let storages = storages
        .into_iter()
        .map(|(address, mut slots)| {
            slots.sort_by_key(|(slot, _)| *slot);
            (address, HashedStorageSorted { storage_slots: slots })
        })
        .collect();
    let block = ChainBlock {
        number,
        reverts: PlainStateReverts {
            accounts: vec![account_reverts],
            storage: vec![storage_reverts],
        },
        hashed: Arc::new(HashedPostStateSorted::new(accounts, storages)),
    };
    (block, after)
}

/// A chain of `blocks` blocks from genesis (block 0) and the plain state after each.
pub(crate) fn plain_chain(
    rng: &mut StdRng,
    keys: &PlainKeys,
    blocks: u64,
) -> (Vec<ChainBlock>, Vec<PlainState>) {
    let mut states: Vec<PlainState> = Vec::new();
    let mut chain = Vec::new();
    for number in 0..blocks {
        let empty = PlainState::default();
        let (block, state) = next_block(rng, keys, states.last().unwrap_or(&empty), number);
        chain.push(block);
        states.push(state);
    }
    (chain, states)
}

/// Writes `block`'s changesets and hashed state with reth's writers.
pub(crate) fn write_chain_block(
    provider: &impl StateWriter,
    block: &ChainBlock,
) -> ProviderResult<()> {
    provider.write_state_reverts(
        block.reverts.clone(),
        block.number,
        StateWriteConfig::default(),
    )?;
    provider.write_hashed_state(&block.hashed)
}

/// Raw contents of the sharded tables, exactly as MDBX stores them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TableDump {
    pub(crate) hashed_accounts: BTreeMap<Vec<u8>, Vec<u8>>,
    pub(crate) accounts_trie: BTreeMap<Vec<u8>, Vec<u8>>,
    pub(crate) hashed_storages: BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    pub(crate) storages_trie: BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    pub(crate) bytecodes: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl TableDump {
    pub(crate) fn read(factory: &TestFactory) -> Self {
        Self::read_tx(&snapshot(factory))
    }

    /// The sharded tables as `tx` sees them.
    pub(crate) fn read_tx(tx: &impl DbTx) -> Self {
        Self {
            hashed_accounts: walk::<tables::HashedAccounts>(tx).into_iter().collect(),
            accounts_trie: walk::<tables::PackedAccountsTrie>(tx).into_iter().collect(),
            hashed_storages: group(walk::<tables::HashedStorages>(tx)),
            storages_trie: group(walk::<tables::PackedStoragesTrie>(tx)),
            bytecodes: walk::<tables::Bytecodes>(tx).into_iter().collect(),
        }
    }
}

fn walk<T: Table>(tx: &impl DbTx) -> Vec<(Vec<u8>, Vec<u8>)> {
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

fn group(entries: Vec<(Vec<u8>, Vec<u8>)>) -> BTreeMap<Vec<u8>, Vec<Vec<u8>>> {
    let mut grouped: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (key, value) in entries {
        grouped.entry(key).or_default().push(value);
    }
    grouped
}

/// A seeded generator.
pub(crate) fn rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

/// An owner store over a [`TestFactory`]'s database.
pub(crate) type Store = OwnerStore<Arc<TempDatabase<DatabaseEnv>>>;

/// An owner over `factory`'s database with the block-hash lookup of fork 0 and a changeset reader,
/// so its history is built from disk. The returned factory must outlive the reads.
pub(crate) fn open_owner(
    factory: &TestFactory,
    cluster: &Arc<Cluster>,
    retention: u64,
) -> (Store, Arc<TestFactory>) {
    let store =
        OwnerStore::new(Arc::clone(cluster), Arc::clone(factory.db_ref()), retention).unwrap();
    assert!(store.install_block_hashes(Box::new(|number| Some(block_hash(0, number)))));
    let shared = Arc::new(factory.clone());
    assert!(store.install_changesets(Box::new(ProviderChangesets::new(&shared))).unwrap());
    (store, shared)
}

/// Persists `blocks` (changesets, hashed state, `Finish`) in one commit, through `store`'s
/// persistence hook when given.
pub(crate) fn persist_chain(factory: &TestFactory, store: Option<&Store>, blocks: &[ChainBlock]) {
    let provider = factory.provider_rw().unwrap();
    for block in blocks {
        write_chain_block(&*provider, block).unwrap();
    }
    let tip = blocks.last().unwrap().number;
    provider.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(tip)).unwrap();
    let commit = || provider.commit().map_err(|error| DatabaseError::Other(error.to_string()));
    match store {
        Some(store) => store.commit_persisted(tip, commit).unwrap(),
        None => commit().unwrap(),
    }
}
