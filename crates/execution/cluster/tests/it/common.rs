//! Shared fixtures: a three-node cluster, MDBX test databases written with reth's own writers,
//! seeded random block updates, and raw dumps of the sharded tables.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy_eips::BlockNumHash;
use alloy_primitives::{B256, U256, keccak256, map::B256Map};
use base_execution_cluster::{
    BlockUpdates, CanonicalBlock, Cluster, ClusterConfig, NodeConfig, NodeId, Shard, ShardedTable,
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use reth_db::{DatabaseEnv, test_utils::TempDatabase};
use reth_db_api::{
    Database,
    cursor::DbCursorRO,
    models::StorageSettings,
    table::Table,
    tables::{self, RawKey, RawTable},
    transaction::DbTx,
};
use reth_primitives_traits::Account;
use reth_provider::{
    ProviderFactory, StateWriter, StorageSettingsCache, TrieWriter,
    test_utils::{MockNodeTypesWithDB, create_test_provider_factory},
};
use reth_trie::{
    BranchNodeCompact, HashedPostStateSorted, HashedStorageSorted, Nibbles, PackedStoredNibbles,
    updates::{StorageTrieUpdatesSorted, TrieUpdatesSorted},
};

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
            split_storage: Vec::new(),
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
        })
        .unwrap(),
    )
}

/// Encoded packed account-trie key of `path`.
pub(crate) fn packed(path: Nibbles) -> Vec<u8> {
    PackedStoredNibbles(path).to_compact_array().to_vec()
}

/// The canonical block wrapper for generated updates.
pub(crate) fn canonical(
    number: u64,
    hash: B256,
    parent_hash: B256,
    updates: &BlockUpdates,
) -> CanonicalBlock {
    CanonicalBlock {
        block: BlockNumHash::new(number, hash),
        parent_hash,
        updates: Some(updates.clone()),
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
    /// Contracts whose storage is split, read through their own checks; empty unless a test
    /// sets them, and never written by [`random_updates`].
    pub(crate) split_contracts: Vec<B256>,
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
            split_contracts: Vec::new(),
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

/// `updates` restricted to the keys `cluster`'s node holds, as that node would persist them.
/// `cluster` splits no contract, so storage is placed by address.
pub(crate) fn held_by(cluster: &Cluster, updates: &BlockUpdates) -> BlockUpdates {
    let holds = |table, key: &[u8]| cluster.holds(table, key, &[]).unwrap();
    let hashed = &updates.hashed_state;
    let accounts = hashed
        .accounts
        .iter()
        .filter(|(address, _)| holds(ShardedTable::HashedAccounts, address.as_slice()))
        .copied()
        .collect();
    let storages = hashed
        .storages
        .iter()
        .filter(|(address, _)| holds(ShardedTable::HashedStorages, address.as_slice()))
        .map(|(address, storage)| (*address, storage.clone()))
        .collect();
    let trie = &updates.trie_updates;
    let account_nodes = trie
        .account_nodes_ref()
        .iter()
        .filter(|(path, _)| holds(ShardedTable::AccountsTrie, &packed(*path)))
        .cloned()
        .collect();
    let storage_tries = trie
        .storage_tries_ref()
        .iter()
        .filter(|(address, _)| holds(ShardedTable::StoragesTrie, address.as_slice()))
        .map(|(address, nodes)| (*address, nodes.clone()))
        .collect();
    BlockUpdates {
        hashed_state: Arc::new(HashedPostStateSorted::new(accounts, storages)),
        trie_updates: Arc::new(TrieUpdatesSorted::new(account_nodes, storage_tries)),
    }
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

/// The raw stored value of `key` in table `T`.
pub(crate) fn raw_get<T: Table>(factory: &TestFactory, key: &[u8]) -> Option<Vec<u8>> {
    snapshot(factory)
        .get::<RawTable<T>>(RawKey::from_vec(key.to_vec()))
        .unwrap()
        .map(|value| value.into_value())
}
