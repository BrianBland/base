//! Owner store: differential test against a full reference MDBX, and refusals.

use std::sync::Arc;

use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use base_execution_cluster::{
    BlockUpdates, CanonicalBlock, Cluster, KeyPlacement, OwnerStore, Query, ReadError, Request,
    Response, ServableRange, Shard, ShardedTable, SnapshotId, StoreError,
};
use rand::Rng;
use reth_db_api::{tables, transaction::DbTxMut};
use reth_primitives_traits::Bytecode;
use reth_trie::Nibbles;

use crate::common::{
    KeyUniverse, TableDump, TestTx, block_hash, canonical, cluster, factory, held_by, packed,
    random_updates, rng, snapshot, write,
};

const BLOCKS: u64 = 40;
const PAGE: u32 = 3;

type Store = OwnerStore<TestTx>;

fn new_store(cluster: &Arc<Cluster>) -> Store {
    OwnerStore::new(Arc::clone(cluster), Store::DEFAULT_RETENTION_BLOCKS)
}

fn read(store: &Store, snapshot: SnapshotId, table: ShardedTable, query: Query) -> Response {
    store.read(&Request { snapshot, table, query }).unwrap()
}

fn refusal(store: &Store, snapshot: SnapshotId, table: ShardedTable, query: Query) -> ReadError {
    match store.read(&Request { snapshot, table, query }) {
        Err(StoreError::Refused(refusal)) => refusal,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Walks `table` with `SeekGe` pages from `start` to the end of its shard.
fn walk_shard(
    store: &Store,
    snapshot: SnapshotId,
    table: ShardedTable,
    start: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut from = start.to_vec();
    loop {
        let query = Query::SeekGe { key: from, limit: PAGE };
        let Response::Entries(page) = read(store, snapshot, table, query) else {
            panic!("SeekGe answered with another response kind")
        };
        for (key, value) in page.entries {
            if entries.last().is_none_or(|(last, _)| *last != key) {
                entries.push((key, value));
            }
        }
        if page.end_of_shard {
            return entries;
        }
        from = entries.last().expect("a page that is not the last has entries").0.clone();
    }
}

/// Walks the duplicates of `key` with `SeekDupGe` pages from `subkey` to the end of the key.
fn walk_key(
    store: &Store,
    snapshot: SnapshotId,
    table: ShardedTable,
    key: &[u8],
    subkey: &[u8],
) -> Vec<Vec<u8>> {
    let subkey_len = subkey.len();
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut from = subkey.to_vec();
    loop {
        let query = Query::SeekDupGe { key: key.to_vec(), subkey: from, limit: PAGE };
        let Response::Duplicates(page) = read(store, snapshot, table, query) else {
            panic!("SeekDupGe answered with another response kind")
        };
        for value in page.values {
            if values.last().is_none_or(|last| *last != value) {
                values.push(value);
            }
        }
        if page.end_of_key {
            return values;
        }
        from =
            values.last().expect("a page that is not the last has values")[..subkey_len].to_vec();
    }
}

fn reference_shard(
    table: ShardedTable,
    entries: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    start: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let shard = Shard(start[0]);
    entries
        .range(start.to_vec()..)
        .take_while(|(key, _)| key[0] == shard.0)
        .filter(|(key, _)| table.placement(key) == Some(KeyPlacement::Shard(shard)))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn reference_key(
    entries: &std::collections::BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    key: &[u8],
    subkey: &[u8],
) -> Vec<Vec<u8>> {
    entries
        .get(key)
        .map(|values| values.iter().filter(|v| v[..subkey.len()] >= *subkey).cloned().collect())
        .unwrap_or_default()
}

/// The smallest encoded account-trie path in `shard`.
fn shard_start_path(shard: Shard) -> Vec<u8> {
    let (high, low) = (shard.0 >> 4, shard.0 & 0x0f);
    if low == 0 {
        packed(Nibbles::from_nibbles([high]))
    } else {
        packed(Nibbles::from_nibbles([high, low]))
    }
}

/// Checks every owned shard and held key of `keys` against `dump`, the reference tables at `P`.
fn assert_owner_matches(
    store: &Store,
    cluster: &Cluster,
    keys: &KeyUniverse,
    dump: &TableDump,
    snapshot: SnapshotId,
    rng: &mut rand::rngs::StdRng,
) {
    let context = format!("at block {}", snapshot.number);
    for shard in Shard::all().filter(|shard| cluster.owns(*shard)) {
        let mut account_start = [0u8; 32];
        account_start[0] = shard.0;
        assert_eq!(
            walk_shard(store, snapshot, ShardedTable::HashedAccounts, &account_start),
            reference_shard(ShardedTable::HashedAccounts, &dump.hashed_accounts, &account_start),
            "HashedAccounts shard {shard:?} {context}"
        );
        let path_start = shard_start_path(shard);
        assert_eq!(
            walk_shard(store, snapshot, ShardedTable::AccountsTrie, &path_start),
            reference_shard(ShardedTable::AccountsTrie, &dump.accounts_trie, &path_start),
            "AccountsTrie shard {shard:?} {context}"
        );
    }

    for address in &keys.addresses {
        let key = address.as_slice();
        if !cluster.holds(ShardedTable::HashedAccounts, key).unwrap() {
            continue;
        }
        let get =
            read(store, snapshot, ShardedTable::HashedAccounts, Query::Get { key: key.to_vec() });
        assert_eq!(get, Response::Value(dump.hashed_accounts.get(key).cloned()), "{context}");
        assert_eq!(
            walk_shard(store, snapshot, ShardedTable::HashedAccounts, key),
            reference_shard(ShardedTable::HashedAccounts, &dump.hashed_accounts, key),
            "HashedAccounts from {address} {context}"
        );

        let slot_start = if rng.random_bool(0.5) {
            B256::ZERO
        } else {
            keys.slots[rng.random_range(0..keys.slots.len())]
        };
        assert_eq!(
            walk_key(store, snapshot, ShardedTable::HashedStorages, key, slot_start.as_slice()),
            reference_key(&dump.hashed_storages, key, slot_start.as_slice()),
            "HashedStorages of {address} from {slot_start} {context}"
        );
        let path_start = if rng.random_bool(0.5) {
            vec![0u8; 33]
        } else {
            packed(keys.storage_paths[rng.random_range(0..keys.storage_paths.len())])
        };
        assert_eq!(
            walk_key(store, snapshot, ShardedTable::StoragesTrie, key, &path_start),
            reference_key(&dump.storages_trie, key, &path_start),
            "StoragesTrie of {address} {context}"
        );
    }

    for path in &keys.account_paths {
        let key = packed(*path);
        if !cluster.holds(ShardedTable::AccountsTrie, &key).unwrap() {
            continue;
        }
        let get =
            read(store, snapshot, ShardedTable::AccountsTrie, Query::Get { key: key.clone() });
        assert_eq!(
            get,
            Response::Value(dump.accounts_trie.get(&key).cloned()),
            "{path:?} {context}"
        );
    }
}

fn run_differential(seed: u64) {
    let mut rng = rng(seed);
    let cluster = cluster(1);
    let owned = Shard::all().filter(|shard| cluster.owns(*shard)).take(3);
    let unowned = Shard::all().filter(|shard| !cluster.owns(*shard)).take(2);
    let keys = KeyUniverse::new(&mut rng, &owned.chain(unowned).collect::<Vec<_>>());

    let reference = factory();
    let owner_db = factory();
    let store = new_store(&cluster);
    store.register_persistence_point(BlockNumHash::new(0, block_hash(0, 0)), snapshot(&owner_db));
    let mut dumps = vec![TableDump::read(&reference)];
    let mut pending: Vec<BlockUpdates> = Vec::new();
    let mut persisted = 0;
    let (mut persisted_behind, mut persisted_ahead) = (0, 0);

    for number in 1..=BLOCKS {
        let updates = random_updates(&mut rng, &keys);
        write(&reference, &updates);
        dumps.push(TableDump::read(&reference));
        store.insert_block(canonical(
            number,
            block_hash(0, number),
            block_hash(0, number - 1),
            &updates,
        ));
        pending.push(updates);

        if rng.random_bool(0.3) {
            for updates in std::mem::take(&mut pending) {
                write(&owner_db, &held_by(&cluster, &updates));
            }
            let point = BlockNumHash::new(number, block_hash(0, number));
            store.register_persistence_point(point, snapshot(&owner_db));
            persisted = number;
        }

        for target in 0..=number {
            match persisted.cmp(&target) {
                std::cmp::Ordering::Less => persisted_behind += 1,
                std::cmp::Ordering::Greater => persisted_ahead += 1,
                std::cmp::Ordering::Equal => {}
            }
            let hash = (target % 2 == 0).then(|| block_hash(0, target));
            let snapshot = SnapshotId { number: target, hash };
            assert_owner_matches(
                &store,
                &cluster,
                &keys,
                &dumps[target as usize],
                snapshot,
                &mut rng,
            );
        }
    }

    let final_dump = dumps.last().unwrap();
    assert!(final_dump.hashed_accounts.keys().any(|k| cluster.owns(Shard(k[0]))), "seed {seed}");
    assert!(final_dump.hashed_accounts.keys().any(|k| !cluster.owns(Shard(k[0]))), "seed {seed}");
    assert!(persisted_behind > 0 && persisted_ahead > 0, "seed {seed}");
}

#[test]
fn owner_answers_equal_reference_tables_at_every_block_in_window() {
    for seed in [7, 11, 2026] {
        run_differential(seed);
    }
}

/// Retains a linear chain of empty blocks `1..=head` on fork 0 over a point at block 0.
fn store_with_chain(head: u64) -> (Store, Arc<Cluster>) {
    let cluster = cluster(1);
    let store = new_store(&cluster);
    store.register_persistence_point(BlockNumHash::new(0, block_hash(0, 0)), snapshot(&factory()));
    for number in 1..=head {
        store.insert_block(canonical(
            number,
            block_hash(0, number),
            block_hash(0, number - 1),
            &BlockUpdates { hashed_state: Arc::default(), trie_updates: Arc::default() },
        ));
    }
    (store, cluster)
}

fn owned_account(cluster: &Cluster) -> Vec<u8> {
    let shard = Shard::all().find(|shard| cluster.owns(*shard)).unwrap();
    let mut key = vec![0x11; 32];
    key[0] = shard.0;
    key
}

fn get_account(
    store: &Store,
    cluster: &Cluster,
    snapshot: SnapshotId,
) -> Result<Response, StoreError> {
    let query = Query::Get { key: owned_account(cluster) };
    store.read(&Request { snapshot, table: ShardedTable::HashedAccounts, query })
}

const fn at(number: u64, hash: Option<B256>) -> SnapshotId {
    SnapshotId { number, hash }
}

#[test]
fn refuses_snapshots_outside_the_window() {
    let head = 200;
    let (store, cluster) = store_with_chain(head);
    let oldest = head - Store::DEFAULT_RETENTION_BLOCKS + 1;
    let servable = Some(ServableRange { lowest: oldest, highest: head });

    assert!(get_account(&store, &cluster, at(oldest, None)).is_ok());
    assert!(get_account(&store, &cluster, at(head, Some(block_hash(0, head)))).is_ok());
    for outside in [oldest - 1, head + 1] {
        assert!(matches!(
            get_account(&store, &cluster, at(outside, None)),
            Err(StoreError::Refused(ReadError::SnapshotUnavailable { servable: s })) if s == servable
        ));
    }
}

#[test]
fn refuses_past_a_gap_or_a_block_without_updates() {
    let (store, cluster) = store_with_chain(3);
    store.insert_block(CanonicalBlock {
        block: BlockNumHash::new(4, block_hash(0, 4)),
        parent_hash: block_hash(0, 3),
        updates: None,
    });
    store.insert_block(canonical(
        6,
        block_hash(0, 6),
        block_hash(0, 5),
        &BlockUpdates { hashed_state: Arc::default(), trie_updates: Arc::default() },
    ));

    let servable = Some(ServableRange { lowest: 0, highest: 3 });
    assert!(get_account(&store, &cluster, at(3, None)).is_ok());
    for refused in [4, 5, 6] {
        assert!(matches!(
            get_account(&store, &cluster, at(refused, None)),
            Err(StoreError::Refused(ReadError::SnapshotUnavailable { servable: s })) if s == servable
        ));
    }
}

#[test]
fn refuses_another_fork_and_follows_a_reorg() {
    let (store, cluster) = store_with_chain(5);
    assert!(matches!(
        get_account(&store, &cluster, at(3, Some(block_hash(1, 3)))),
        Err(StoreError::Refused(ReadError::WrongFork))
    ));
    assert!(matches!(
        get_account(&store, &cluster, at(0, Some(block_hash(1, 0)))),
        Err(StoreError::Refused(ReadError::WrongFork))
    ));

    store.insert_block(canonical(
        3,
        block_hash(1, 3),
        block_hash(0, 2),
        &BlockUpdates { hashed_state: Arc::default(), trie_updates: Arc::default() },
    ));
    assert!(get_account(&store, &cluster, at(3, Some(block_hash(1, 3)))).is_ok());
    assert!(matches!(
        get_account(&store, &cluster, at(3, Some(block_hash(0, 3)))),
        Err(StoreError::Refused(ReadError::WrongFork))
    ));
    assert!(matches!(
        get_account(&store, &cluster, at(4, None)),
        Err(StoreError::Refused(ReadError::SnapshotUnavailable { .. }))
    ));
}

#[test]
fn refuses_unowned_shards_and_unsupported_queries() {
    let (store, cluster) = store_with_chain(1);
    let snapshot = at(1, None);
    let unowned = Shard::all().find(|shard| !cluster.owns(*shard)).unwrap();
    let mut unowned_key = vec![0x22; 32];
    unowned_key[0] = unowned.0;
    let mut unowned_path = vec![0u8; 33];
    unowned_path[0] = unowned.0;
    unowned_path[32] = 2;
    let owned_key = owned_account(&cluster);

    let not_owner = [
        (ShardedTable::HashedAccounts, Query::Get { key: unowned_key.clone() }),
        (ShardedTable::Bytecodes, Query::Get { key: unowned_key.clone() }),
        (ShardedTable::AccountsTrie, Query::Get { key: unowned_path.clone() }),
        (ShardedTable::HashedAccounts, Query::SeekGe { key: unowned_key.clone(), limit: 1 }),
        (ShardedTable::AccountsTrie, Query::SeekGe { key: unowned_path, limit: 1 }),
        (
            ShardedTable::HashedStorages,
            Query::SeekDupGe { key: unowned_key.clone(), subkey: vec![0; 32], limit: 1 },
        ),
        (
            ShardedTable::StoragesTrie,
            Query::SeekDupGe { key: unowned_key, subkey: vec![0; 33], limit: 1 },
        ),
    ];
    for (table, query) in not_owner {
        assert_eq!(
            refusal(&store, snapshot, table, query.clone()),
            ReadError::NotOwner,
            "{query:?}"
        );
    }

    let unsupported = [
        (ShardedTable::HashedStorages, Query::Get { key: owned_key.clone() }),
        (ShardedTable::StoragesTrie, Query::SeekGe { key: owned_key.clone(), limit: 1 }),
        (ShardedTable::Bytecodes, Query::SeekGe { key: owned_key.clone(), limit: 1 }),
        (
            ShardedTable::HashedAccounts,
            Query::SeekDupGe { key: owned_key.clone(), subkey: vec![0; 32], limit: 1 },
        ),
        (ShardedTable::HashedAccounts, Query::Get { key: owned_key[..31].to_vec() }),
        (
            ShardedTable::StoragesTrie,
            Query::SeekDupGe { key: owned_key, subkey: vec![0; 32], limit: 1 },
        ),
    ];
    for (table, query) in unsupported {
        assert_eq!(
            refusal(&store, snapshot, table, query.clone()),
            ReadError::Unsupported,
            "{query:?}"
        );
    }
}

#[test]
fn serves_bytecodes_from_the_newest_snapshot_and_refuses_absence_when_behind() {
    let cluster = cluster(1);
    let shard = Shard::all().find(|shard| cluster.owns(*shard)).unwrap();
    let mut code_hash = B256::repeat_byte(0x33);
    code_hash.0[0] = shard.0;
    let mut missing_hash = B256::repeat_byte(0x44);
    missing_hash.0[0] = shard.0;

    let db = factory();
    let provider = db.provider_rw().unwrap();
    provider
        .tx_ref()
        .put::<tables::Bytecodes>(code_hash, Bytecode::new_raw(vec![0x60, 0x00].into()))
        .unwrap();
    provider.commit().unwrap();
    let expected = crate::common::raw_get::<tables::Bytecodes>(&db, code_hash.as_slice());

    let store = new_store(&cluster);
    store.register_persistence_point(BlockNumHash::new(10, block_hash(0, 10)), snapshot(&db));
    let get = |hash: B256, number| {
        store.read(&Request {
            snapshot: at(number, None),
            table: ShardedTable::Bytecodes,
            query: Query::Get { key: hash.to_vec() },
        })
    };

    assert_eq!(get(code_hash, 12).unwrap(), Response::Value(expected));
    assert_eq!(get(missing_hash, 10).unwrap(), Response::Value(None));
    assert!(matches!(
        get(missing_hash, 11),
        Err(StoreError::Refused(ReadError::SnapshotUnavailable { .. }))
    ));
}
