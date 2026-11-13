//! Owner store: a differential test against the true history of a chain written with reth's own
//! writers (real changesets on disk), and refusals.

use std::sync::Arc;

use alloy_primitives::{B256, keccak256};
use base_execution_cluster::{
    Cluster, IndexCoverage, Query, ReadError, ReadResult, Request, Response, ServableRange, Shard,
    ShardedTable, SnapshotId, StoreError,
};
use rand::Rng;
use reth_db_api::{Database, table::Compress, tables, transaction::DbTxMut};
use reth_primitives_traits::{Bytecode, StorageEntry};

use crate::common::{
    PlainKeys, PlainState, Store, block_hash, canonical, cluster, cluster_splitting, factory,
    open_owner as open, persist_chain as persist, plain_chain, rng,
};

const fn at(number: u64) -> SnapshotId {
    SnapshotId { number, hash: None }
}

fn hashed_at(number: u64) -> SnapshotId {
    SnapshotId { number, hash: Some(block_hash(0, number)) }
}

const fn request(snapshot: SnapshotId, query: Query) -> Request {
    let table = match query {
        Query::AccountValue { .. } => ShardedTable::HashedAccounts,
        _ => ShardedTable::HashedStorages,
    };
    Request { snapshot, table, query }
}

fn refusal(result: Result<Response, StoreError>) -> ReadResult {
    match result {
        Ok(response) => Ok(response),
        Err(StoreError::Refused(refusal)) => Err(refusal),
        Err(StoreError::Database(error)) => panic!("database error {error}"),
    }
}

fn answer(store: &Store, snapshot: SnapshotId, query: Query) -> ReadResult {
    refusal(store.read(&request(snapshot, query)))
}

/// Every state query of `keys`.
fn queries(keys: &PlainKeys) -> Vec<Query> {
    let accounts = keys.addresses.iter().chain(&keys.split).flat_map(|address| {
        let address = keccak256(address);
        [Query::AccountValue { address }, Query::HasStorage { address }]
    });
    let storages = keys.storage_keys().into_iter().map(|(address, slot)| Query::StorageValue {
        address: keccak256(address),
        slot: keccak256(slot),
    });
    accounts.chain(storages).collect()
}

/// What an owner of `cluster` must answer to `query` when the true state is `state`.
fn expected(cluster: &Cluster, keys: &PlainKeys, state: &PlainState, query: &Query) -> ReadResult {
    let holds = |table, key: B256, subkey: &[u8]| cluster.holds(table, key.as_slice(), subkey);
    let address_of = |hashed: B256| {
        keys.addresses.iter().chain(&keys.split).copied().find(|a| keccak256(a) == hashed).unwrap()
    };
    match query {
        Query::AccountValue { address } => {
            if holds(ShardedTable::HashedAccounts, *address, &[]) != Some(true) {
                return Err(ReadError::NotOwner);
            }
            let account = state.accounts.get(&address_of(*address)).copied();
            Ok(Response::Value(account.map(|account| account.compress())))
        }
        Query::StorageValue { address, slot } => {
            if holds(ShardedTable::HashedStorages, *address, slot.as_slice()) != Some(true) {
                return Err(ReadError::NotOwner);
            }
            let plain = address_of(*address);
            let plain_slot =
                keys.slots.iter().chain(&keys.split_slots).find(|s| keccak256(s) == *slot).unwrap();
            let value = state.slot(plain, *plain_slot);
            let entry = (!value.is_zero()).then(|| StorageEntry { key: *slot, value }.compress());
            Ok(Response::Value(entry))
        }
        Query::HasStorage { address } => {
            if cluster.is_split(ShardedTable::HashedStorages, address.as_slice()) {
                return Err(ReadError::Unsupported);
            }
            if holds(ShardedTable::HashedStorages, *address, &[]) != Some(true) {
                return Err(ReadError::NotOwner);
            }
            Ok(Response::HasStorage(state.has_storage(address_of(*address))))
        }
        other => panic!("not a state query: {other:?}"),
    }
}

/// The owner answers every query of `keys` at every block of `blocks` as the true history says,
/// and serves some held rows of each kind.
fn assert_history(
    store: &Store,
    cluster: &Cluster,
    keys: &PlainKeys,
    states: &[PlainState],
    blocks: std::ops::RangeInclusive<u64>,
    context: &str,
) {
    let queries = queries(keys);
    for number in blocks {
        let mut served = 0;
        for query in &queries {
            let want = expected(cluster, keys, &states[number as usize], query);
            served += usize::from(want.is_ok());
            let snapshot = if number % 2 == 0 { hashed_at(number) } else { at(number) };
            assert_eq!(
                answer(store, snapshot, query.clone()),
                want,
                "{context}: {query:?} at {number}"
            );
        }
        assert!(served > 20, "{context}: only {served} queries served at {number}");
    }
}

fn assert_behind(store: &Store, cluster: &Cluster, keys: &PlainKeys, number: u64, context: &str) {
    let query = Query::AccountValue { address: held_address(cluster, keys) };
    match answer(store, at(number), query) {
        Err(refusal) => assert!(refusal.is_behind(number), "{context}: {refusal:?} at {number}"),
        Ok(response) => panic!("{context}: answered {response:?} at {number}, expected behind"),
    }
}

/// The address whose rows `cluster`'s node holds, so a query of it is not refused as unowned.
fn held_address(cluster: &Cluster, keys: &PlainKeys) -> B256 {
    keys.addresses
        .iter()
        .map(keccak256)
        .find(|address| {
            cluster.holds(ShardedTable::HashedAccounts, address.as_slice(), &[]) == Some(true)
        })
        .unwrap()
}

fn run_differential(seed: u64, split: bool) {
    let mut rng = rng(seed);
    let shards: Vec<Shard> = (0..6).map(|_| Shard(rng.random())).collect();
    let keys = PlainKeys::new(&mut rng, &shards, split);
    let cluster = cluster_splitting(1, keys.split.as_slice());
    let (chain, states) = plain_chain(&mut rng, &keys, 20);
    let db = factory();

    // History already on disk when the owner starts.
    persist(&db, None, &chain[..=4]);
    persist(&db, None, &chain[5..=8]);
    let (store, _shared) = open(&db, &cluster, 30_000);
    assert_eq!(store.coverage(), Some(IndexCoverage { lowest: 0, indexed: 8 }));
    assert_history(&store, &cluster, &keys, &states, 0..=8, "built at startup");

    // Persisted while it runs.
    persist(&db, Some(&store), &chain[9..=10]);
    persist(&db, Some(&store), &chain[11..=12]);
    assert_history(&store, &cluster, &keys, &states, 0..=12, "extended at commits");

    // Above the persisted tip, from notifications: a batch, then block 15 without block 14.
    store.commit_blocks(chain[13..=13].iter().map(|b| canonical(b.number, &b.hashed)).collect());
    store.commit_blocks(vec![canonical(15, &chain[15].hashed)]);
    assert_history(&store, &cluster, &keys, &states, 0..=13, "chain up to the gap");
    assert_behind(&store, &cluster, &keys, 14, "missing notification");
    assert_behind(&store, &cluster, &keys, 15, "past the missing notification");
    let batch = chain[14..=16].iter().map(|b| canonical(b.number, &b.hashed)).collect();
    store.commit_blocks(batch);
    assert_history(&store, &cluster, &keys, &states, 0..=16, "chain in a batch");
    assert_behind(&store, &cluster, &keys, 17, "not notified yet");

    // A wrong hash for P, below and above the persisted tip.
    let held = held_address(&cluster, &keys);
    for number in [10, 15] {
        let wrong = SnapshotId { number, hash: Some(block_hash(1, number)) };
        let query = Query::AccountValue { address: held };
        assert_eq!(
            answer(&store, wrong, query),
            Err(ReadError::WrongFork),
            "wrong hash at {number}"
        );
    }

    // Persistence advancing between a transaction's tip read and the index lookup.
    let old = db.db_ref().tx().unwrap();
    persist(&db, Some(&store), &chain[13..=14]);
    for number in 0..=12 {
        for query in queries(&keys) {
            let want = expected(&cluster, &keys, &states[number as usize], &query);
            let found = refusal(store.read_in(&old, &request(at(number), query.clone())));
            assert_eq!(found, want, "transaction older than the index: {query:?} at {number}");
        }
    }
    let stale =
        refusal(store.read_in(&old, &request(at(15), Query::AccountValue { address: held })));
    assert!(stale.is_err_and(|refusal| refusal.is_behind(15)), "a stale transaction above its tip");
    drop(old);
    assert_history(&store, &cluster, &keys, &states, 0..=16, "after persisting notified blocks");

    // Restart: a fresh owner over the same disk answers the same up to the persisted tip.
    drop(store);
    let (restarted, _shared) = open(&db, &cluster, 30_000);
    assert_eq!(restarted.coverage(), Some(IndexCoverage { lowest: 0, indexed: 14 }));
    assert_history(&restarted, &cluster, &keys, &states, 0..=14, "after a restart");
    assert_behind(&restarted, &cluster, &keys, 15, "restarted without notifications");
}

#[test]
fn owner_answers_like_the_true_history() {
    for seed in [1, 2] {
        run_differential(seed, false);
    }
}

#[test]
fn owner_answers_a_split_contract_like_the_true_history() {
    run_differential(3, true);
}

#[test]
fn refuses_blocks_below_its_history_for_good() {
    let mut rng = rng(4);
    let shards: Vec<Shard> = (0..6).map(|_| Shard(rng.random())).collect();
    let keys = PlainKeys::new(&mut rng, &shards, false);
    let cluster = cluster(1);
    let (chain, states) = plain_chain(&mut rng, &keys, 14);
    let db = factory();
    persist(&db, None, &chain[..=12]);
    let (store, _shared) = open(&db, &cluster, 5);
    assert_eq!(store.coverage(), Some(IndexCoverage { lowest: 7, indexed: 12 }));

    let held = held_address(&cluster, &keys);
    let below = answer(&store, at(6), Query::AccountValue { address: held });
    let Err(ReadError::SnapshotUnavailable { servable: Some(range) }) = below else {
        panic!("expected a refusal with the servable range, got {below:?}")
    };
    assert_eq!(range, ServableRange { lowest: 7, highest: 12 });
    assert!(!below.unwrap_err().is_behind(6), "a block below the history is not waited for");
    assert_history(&store, &cluster, &keys, &states, 7..=12, "inside the window");

    persist(&db, Some(&store), &chain[13..=13]);
    assert_eq!(store.coverage(), Some(IndexCoverage { lowest: 8, indexed: 13 }));
    assert!(answer(&store, at(7), Query::AccountValue { address: held }).is_err());
    let status = answer(&store, at(0), Query::Status);
    assert_eq!(status, Ok(Response::Status(Some(ServableRange { lowest: 8, highest: 13 }))));
}

#[test]
fn a_reorg_replaces_unpersisted_blocks_and_stops_the_owner_below_its_tip() {
    let mut rng = rng(5);
    let shards: Vec<Shard> = (0..6).map(|_| Shard(rng.random())).collect();
    let keys = PlainKeys::new(&mut rng, &shards, false);
    let cluster = cluster(1);
    let (chain, states) = plain_chain(&mut rng, &keys, 12);
    let db = factory();
    persist(&db, None, &chain[..=8]);
    let (store, _shared) = open(&db, &cluster, 30_000);
    let held = held_address(&cluster, &keys);

    // Blocks 9 and 10 of another fork, then a reorg back to fork 0.
    let other_fork = |number: u64| base_execution_cluster::CanonicalBlock {
        block: alloy_eips::BlockNumHash::new(number, block_hash(1, number)),
        parent_hash: if number == 9 { block_hash(0, 8) } else { block_hash(1, number - 1) },
        hashed_state: Some(Arc::clone(&chain[11].hashed)),
    };
    store.commit_blocks(vec![other_fork(9), other_fork(10)]);
    let fork_snapshot = SnapshotId { number: 10, hash: Some(block_hash(1, 10)) };
    assert!(answer(&store, fork_snapshot, Query::AccountValue { address: held }).is_ok());
    let replacement = chain[9..=10].iter().map(|b| canonical(b.number, &b.hashed)).collect();
    store.reorg_blocks(9, replacement);
    assert_history(&store, &cluster, &keys, &states, 8..=10, "after an in-memory reorg");
    assert_eq!(
        answer(&store, fork_snapshot, Query::AccountValue { address: held }),
        Err(ReadError::WrongFork)
    );

    store.reorg_blocks(8, Vec::new());
    assert_eq!(
        answer(&store, at(5), Query::AccountValue { address: held }),
        Err(ReadError::WrongFork)
    );
}

#[test]
fn refuses_unowned_rows_and_the_previous_ordered_and_trie_queries() {
    let cluster = cluster(1);
    let db = factory();
    let mut rng = rng(6);
    let keys = PlainKeys::new(&mut rng, &[Shard(0x10)], false);
    let (chain, _) = plain_chain(&mut rng, &keys, 1);
    persist(&db, None, &chain);
    let (store, _shared) = open(&db, &cluster, 30_000);
    let unowned = Shard::all().find(|shard| !cluster.owns(*shard)).unwrap();
    let mut unowned_key = B256::ZERO;
    unowned_key.0[0] = unowned.0;
    let owned = Shard::all().find(|shard| cluster.owns(*shard)).unwrap();
    let mut owned_key = B256::ZERO;
    owned_key.0[0] = owned.0;

    let read = |table, query| refusal(store.read(&Request { snapshot: at(0), table, query }));
    let key = owned_key.to_vec();
    let path = vec![owned.0; 33];
    for (table, query) in [
        (ShardedTable::HashedAccounts, Query::Get { key: key.clone() }),
        (ShardedTable::AccountsTrie, Query::Get { key: path.clone() }),
        (ShardedTable::HashedAccounts, Query::SeekGe { key: key.clone(), limit: 4 }),
        (ShardedTable::AccountsTrie, Query::SeekGe { key: path, limit: 4 }),
        (
            ShardedTable::HashedStorages,
            Query::SeekDupGe { key: key.clone(), subkey: vec![0; 32], limit: 4 },
        ),
        (ShardedTable::StoragesTrie, Query::SeekDupGe { key, subkey: vec![0; 33], limit: 4 }),
        (ShardedTable::HashedStorages, Query::AccountValue { address: owned_key }),
    ] {
        assert_eq!(read(table, query.clone()), Err(ReadError::Unsupported), "{query:?}");
    }
    assert_eq!(
        read(ShardedTable::HashedAccounts, Query::AccountValue { address: unowned_key }),
        Err(ReadError::NotOwner)
    );
    assert_eq!(
        read(ShardedTable::HashedStorages, Query::HasStorage { address: unowned_key }),
        Err(ReadError::NotOwner)
    );
}

#[test]
fn serves_bytecodes_from_its_newest_data_and_refuses_absence_when_behind() {
    let cluster = cluster(1);
    let shard = Shard::all().find(|shard| cluster.owns(*shard)).unwrap();
    let mut code_hash = B256::repeat_byte(0x66);
    code_hash.0[0] = shard.0;
    let db = factory();
    let mut rng = rng(7);
    let keys = PlainKeys::new(&mut rng, &[Shard(0x10)], false);
    let (chain, _) = plain_chain(&mut rng, &keys, 4);
    persist(&db, None, &chain[..=2]);
    let (store, _shared) = open(&db, &cluster, 30_000);
    let get = |number| {
        let query = Query::Get { key: code_hash.to_vec() };
        refusal(store.read(&Request {
            snapshot: at(number),
            table: ShardedTable::Bytecodes,
            query,
        }))
    };

    assert_eq!(get(2), Ok(Response::Value(None)));
    assert_eq!(
        get(3),
        Err(ReadError::SnapshotUnavailable {
            servable: Some(ServableRange { lowest: 0, highest: 2 })
        })
    );
    let provider = db.provider_rw().unwrap();
    let code = Bytecode::new_raw(vec![0x60, 0x00].into());
    provider.tx_ref().put::<tables::Bytecodes>(code_hash, code.clone()).unwrap();
    provider.commit().unwrap();
    assert_eq!(get(3), Ok(Response::Value(Some(code.clone().compress()))));
    assert_eq!(get(0), Ok(Response::Value(Some(code.compress()))));
}
