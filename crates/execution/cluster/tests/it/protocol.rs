//! Owner server and client over real localhost sockets.

use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{B256, Bytes, keccak256};
use base_execution_cluster::{
    ApplyFallback, ClientError, ClientOptions, Cluster, ClusterClient, ClusterConfig,
    ExecutedBlocks, Frame, NodeConfig, NodeId, OwnerFailure, OwnerServer, OwnerStore, Query,
    ReadError, ReadResult, Request, Response, ServableRange, Shard, ShardedTable, SnapshotId,
    StoreError,
};
use reth_db::test_utils::create_test_rw_db;
use reth_db_api::{table::Compress, tables, transaction::DbTxMut};
use reth_primitives_traits::{Bytecode, StorageEntry};

use crate::common::{
    ChainBlock, PlainKeys, PlainState, Store, TestFactory, block_hash, factory, open_owner,
    persist_chain, plain_chain, rng,
};

/// Storage nodes 1 and 2 at the given addresses and a stateless node 3, seen from `node_id`.
fn cluster_of(
    node_id: u64,
    replication: usize,
    owner_1: SocketAddr,
    owner_2: SocketAddr,
) -> Arc<Cluster> {
    let node = |id, address, storage| NodeConfig { id: NodeId(id), address, storage };
    let nodes = vec![
        node(1, owner_1, true),
        node(2, owner_2, true),
        node(3, "127.0.0.1:1".parse().unwrap(), false),
    ];
    Arc::new(
        Cluster::new(ClusterConfig {
            node_id: NodeId(node_id),
            replication,
            retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
            nodes,
            apply_from: Vec::new(),
            split_storage: Vec::new(),
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
        })
        .unwrap(),
    )
}

fn closed_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

/// A chain of plain-key blocks with addresses in `shard`, persisted through block 4 with real
/// changesets, plus a bytecode in `shard`; returns the database, keys, true states and the
/// bytecode's hash.
fn populated_db(shard: Shard) -> (TestFactory, PlainKeys, Vec<PlainState>, B256) {
    let db = factory();
    let mut rng = rng(5);
    let keys = PlainKeys::new(&mut rng, &[shard], false);
    let (chain, states) = plain_chain(&mut rng, &keys, 5);
    persist_chain(&db, None, &chain);
    let mut code_hash = B256::repeat_byte(0x55);
    code_hash.0[0] = shard.0;
    let provider = db.provider_rw().unwrap();
    provider
        .tx_ref()
        .put::<tables::Bytecodes>(code_hash, Bytecode::new_raw(vec![0x00].into()))
        .unwrap();
    provider.commit().unwrap();
    (db, keys, states, code_hash)
}

/// An owner of `cluster`'s node over `db` serving on `listener`.
fn serve(
    cluster: &Arc<Cluster>,
    listener: TcpListener,
    db: &TestFactory,
    retention: u64,
) -> (Arc<Store>, OwnerServer, Arc<TestFactory>) {
    let (store, shared) = open_owner(db, cluster, retention);
    let store = Arc::new(store);
    let server = OwnerServer::spawn(listener, Arc::clone(&store), Arc::default()).unwrap();
    (store, server, shared)
}

fn local(store: &Store, request: &Request) -> ReadResult {
    match store.read(request) {
        Ok(response) => Ok(response),
        Err(StoreError::Refused(refusal)) => Err(refusal),
        Err(StoreError::Database(error)) => panic!("database error {error}"),
    }
}

/// Node 1's view of the cluster seen by `client_view`.
fn owner_view(client_view: &Cluster) -> Arc<Cluster> {
    Arc::new(
        Cluster::new(ClusterConfig { node_id: NodeId(1), ..client_view.config().clone() }).unwrap(),
    )
}

/// An address of `keys` with an account and storage in `state`.
fn busy_address(keys: &PlainKeys, state: &PlainState) -> alloy_primitives::Address {
    *keys
        .addresses
        .iter()
        .find(|address| state.accounts.contains_key(*address) && state.has_storage(**address))
        .expect("an address with an account and storage")
}

#[test]
fn every_request_and_error_round_trips_over_a_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let cluster = cluster_of(1, 1, listener.local_addr().unwrap(), closed_port());
    let owned = Shard::all().find(|shard| cluster.owns(*shard)).unwrap();
    let unowned = Shard::all().find(|shard| !cluster.owns(*shard)).unwrap();

    let (db, keys, states, code_hash) = populated_db(owned);
    let (store, server, _shared) = serve(&cluster, listener, &db, 160);
    let tip = SnapshotId { number: 4, hash: Some(block_hash(0, 4)) };
    let plain = busy_address(&keys, &states[4]);
    let address = keccak256(plain);
    let slot = keys.slots.iter().find(|slot| !states[4].slot(plain, **slot).is_zero()).unwrap();
    let mut unowned_key = B256::ZERO;
    unowned_key.0[0] = unowned.0;
    let request = |snapshot, table, query| Request { snapshot, table, query };
    let accounts = ShardedTable::HashedAccounts;
    let storages = ShardedTable::HashedStorages;
    let cases = [
        request(tip, accounts, Query::AccountValue { address }),
        request(tip, storages, Query::StorageValue { address, slot: keccak256(slot) }),
        request(tip, ShardedTable::Bytecodes, Query::Get { key: code_hash.to_vec() }),
        request(tip, storages, Query::HasStorage { address }),
        request(tip, accounts, Query::Status),
        request(SnapshotId { number: 9, hash: None }, accounts, Query::AccountValue { address }),
        request(
            SnapshotId { number: 4, hash: Some(block_hash(1, 4)) },
            accounts,
            Query::AccountValue { address },
        ),
        request(tip, accounts, Query::AccountValue { address: unowned_key }),
        request(tip, accounts, Query::Get { key: address.to_vec() }),
    ];

    let mut stream = TcpStream::connect(server.local_addr()).unwrap();
    let mut answers = Vec::new();
    for case in &cases {
        Frame::write(&mut stream, case).unwrap();
        let remote: ReadResult = Frame::read(&mut stream).unwrap();
        assert_eq!(remote, local(&store, case), "{case:?}");
        answers.push(remote);
    }
    assert!(matches!(&answers[0], Ok(Response::Value(Some(_)))));
    assert!(matches!(&answers[1], Ok(Response::Value(Some(_)))));
    assert!(matches!(&answers[2], Ok(Response::Value(Some(_)))));
    assert_eq!(answers[3], Ok(Response::HasStorage(true)));
    assert_eq!(answers[4], Ok(Response::Status(Some(ServableRange { lowest: 0, highest: 4 }))));
    assert!(matches!(&answers[5], Err(refusal) if refusal.is_behind(9)));
    assert_eq!(answers[6], Err(ReadError::WrongFork));
    assert_eq!(answers[7], Err(ReadError::NotOwner));
    assert_eq!(answers[8], Err(ReadError::Unsupported));
}

#[test]
fn client_fails_over_when_the_first_owner_is_down() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let shard = Shard::all().find(|shard| client_view.owners(*shard)[0] == NodeId(2)).unwrap();
    let owner_view = owner_view(&client_view);
    let (db, keys, states, code_hash) = populated_db(shard);
    let (_store, _server, _shared) = serve(&owner_view, listener, &db, 160);
    let tip = SnapshotId { number: 4, hash: None };
    let state = &states[4];

    let client = ClusterClient::new(client_view, ClientOptions::default());
    let plain = busy_address(&keys, state);
    let address = keccak256(plain);
    let account = client.account(tip, address).unwrap();
    assert_eq!(account, state.accounts.get(&plain).map(|account| account.compress()));
    for slot in &keys.slots {
        let value = state.slot(plain, *slot);
        let want =
            (!value.is_zero()).then(|| StorageEntry { key: keccak256(slot), value }.compress());
        assert_eq!(client.storage(tip, address, keccak256(slot)).unwrap(), want);
    }
    assert!(client.has_storage(tip, address).unwrap());
    assert!(client.bytecode(tip, code_hash.as_slice()).unwrap().is_some());
    assert_eq!(client.status(NodeId(1)).unwrap(), Some(ServableRange { lowest: 0, highest: 4 }));
}

/// An owner of node 1 at persisted block 0 over a fresh database, with the chain that follows.
fn owner_at_genesis(
    client_view: &Cluster,
    listener: TcpListener,
) -> (Arc<Store>, OwnerServer, Arc<TestFactory>, TestFactory, Vec<ChainBlock>) {
    let db = factory();
    let mut rng = rng(6);
    let keys = PlainKeys::new(&mut rng, &[Shard(0x42)], false);
    let (chain, _) = plain_chain(&mut rng, &keys, 2);
    persist_chain(&db, None, &chain[..=0]);
    let (store, server, shared) = serve(&owner_view(client_view), listener, &db, 160);
    (store, server, shared, db, chain)
}

#[test]
fn client_retries_while_every_owner_is_behind() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let (store, _server, _shared, db, chain) = owner_at_genesis(&client_view, listener);

    let catch_up = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        persist_chain(&db, Some(&store), &chain[1..=1]);
    });
    let options = ClientOptions { retry_for: Duration::from_secs(10), ..Default::default() };
    let client = ClusterClient::new(client_view, options);
    let tip = SnapshotId { number: 1, hash: Some(block_hash(0, 1)) };
    assert_eq!(client.account(tip, B256::repeat_byte(0x42)).unwrap(), None);
    catch_up.join().unwrap();
}

#[test]
fn client_waits_while_every_owner_is_unreachable_until_one_starts() {
    let address = closed_port();
    let client_view = cluster_of(3, 2, address, closed_port());
    let owner = std::thread::spawn({
        let client_view = Arc::clone(&client_view);
        move || {
            std::thread::sleep(Duration::from_millis(300));
            owner_at_genesis(&client_view, TcpListener::bind(address).unwrap())
        }
    });
    let options = ClientOptions { retry_for: Duration::from_secs(10), ..Default::default() };
    let client = ClusterClient::new(client_view, options);
    let genesis = SnapshotId { number: 0, hash: Some(block_hash(0, 0)) };
    let started = Instant::now();
    assert_eq!(client.account(genesis, B256::repeat_byte(0x42)).unwrap(), None);
    assert!(started.elapsed() >= Duration::from_millis(300), "answered before any owner ran");
    drop(owner.join().unwrap());
}

/// Longer than the client's former 2-second retry budget.
const PAST_OLD_RETRY_BUDGET: Duration = Duration::from_millis(2500);

#[test]
fn client_waits_by_default_for_an_owner_behind_longer_than_two_seconds() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let (store, _server, _shared, db, chain) = owner_at_genesis(&client_view, listener);

    let catch_up = std::thread::spawn(move || {
        std::thread::sleep(PAST_OLD_RETRY_BUDGET);
        persist_chain(&db, Some(&store), &chain[1..=1]);
    });
    let client = ClusterClient::new(client_view, ClientOptions::default());
    let tip = SnapshotId { number: 1, hash: Some(block_hash(0, 1)) };
    assert_eq!(client.account(tip, B256::repeat_byte(0x42)).unwrap(), None);
    catch_up.join().unwrap();
    assert_eq!(client.remote_read_stats().counts().waits, 1);
}

#[test]
fn client_refuses_at_once_when_owners_no_longer_have_the_block() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let db = factory();
    let mut rng = rng(7);
    let keys = PlainKeys::new(&mut rng, &[Shard(0x42)], false);
    let (chain, _) = plain_chain(&mut rng, &keys, 13);
    persist_chain(&db, None, &chain);
    let (_store, _server, _shared) = serve(&owner_view(&client_view), listener, &db, 5);

    let client = ClusterClient::new(client_view, ClientOptions::default());
    let discarded = SnapshotId { number: 2, hash: Some(block_hash(0, 2)) };
    let started = Instant::now();
    let Err(ClientError::OwnersFailed(attempts)) =
        client.account(discarded, B256::repeat_byte(0x42))
    else {
        panic!("expected every owner to fail")
    };
    assert!(started.elapsed() < ClusterClient::SLOW_WAIT, "took {:?}", started.elapsed());
    assert!(attempts.iter().any(|attempt| matches!(
        attempt.failure,
        OwnerFailure::Refused(ReadError::SnapshotUnavailable { servable: Some(range) })
            if range.lowest > 2
    )));
    assert_eq!(client.remote_read_stats().counts().waits, 0);
}

#[test]
fn client_waits_for_an_owner_to_persist_requested_code() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let owner_view = owner_view(&client_view);
    let shard = Shard::all().find(|shard| owner_view.owns(*shard)).unwrap();
    let mut code_hash = B256::repeat_byte(0x66);
    code_hash.0[0] = shard.0;
    let (store, _server, _shared, db, chain) = owner_at_genesis(&client_view, listener);

    let persist_code = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        let provider = db.provider_rw().unwrap();
        provider
            .tx_ref()
            .put::<tables::Bytecodes>(code_hash, Bytecode::new_raw(vec![0x60, 0x00].into()))
            .unwrap();
        provider.commit().unwrap();
        persist_chain(&db, Some(&store), &chain[1..=1]);
    });
    let options = ClientOptions { retry_for: Duration::from_secs(10), ..Default::default() };
    let client = ClusterClient::new(client_view, options);
    let tip = SnapshotId { number: 1, hash: Some(block_hash(0, 1)) };
    assert!(client.bytecode(tip, code_hash.as_slice()).unwrap().is_some());
    persist_code.join().unwrap();
}

#[test]
fn client_returns_a_refusal_that_retrying_cannot_fix() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let (_store, _server, _shared, _db, _chain) = owner_at_genesis(&client_view, listener);

    let options = ClientOptions { retry_for: Duration::from_secs(10), ..Default::default() };
    let client = ClusterClient::new(client_view, options);
    let other_fork = SnapshotId { number: 0, hash: Some(block_hash(1, 0)) };
    let started = Instant::now();
    let Err(ClientError::OwnersFailed(attempts)) =
        client.account(other_fork, B256::repeat_byte(0x42))
    else {
        panic!("expected every owner to fail")
    };
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        attempts
            .iter()
            .any(|attempt| matches!(attempt.failure, OwnerFailure::Refused(ReadError::WrongFork)))
    );
    assert!(attempts.iter().any(|attempt| matches!(attempt.failure, OwnerFailure::Unreachable(_))));
}

/// Node 1 serving execution results on `listener`.
fn serve_results(listener: TcpListener) -> (Arc<ExecutedBlocks>, OwnerServer) {
    let view = cluster_of(1, 2, listener.local_addr().unwrap(), closed_port());
    let executed = Arc::new(ExecutedBlocks::default());
    let store = Arc::new(OwnerStore::new(view, create_test_rw_db(), 160).unwrap());
    let server = OwnerServer::spawn(listener, store, Arc::clone(&executed)).unwrap();
    (executed, server)
}

/// Stateless node 3 applying from node 1 at `executor`.
fn applying_client(executor: SocketAddr) -> ClusterClient {
    let view = cluster_of(3, 2, executor, closed_port());
    let config = ClusterConfig { apply_from: vec![NodeId(1)], ..view.config().clone() };
    ClusterClient::new(Arc::new(Cluster::new(config).unwrap()), ClientOptions::default())
}

const FETCH_BUDGET: Duration = Duration::from_millis(400);

#[test]
fn executor_serves_a_stored_result_by_hash() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = applying_client(listener.local_addr().unwrap());
    let (executed, _server) = serve_results(listener);
    let result = Bytes::from_static(b"encoded result");
    executed.insert(B256::repeat_byte(1), result.clone());
    executed.insert(B256::repeat_byte(2), Bytes::from_static(b"another block"));

    assert_eq!(client.executed_block(B256::repeat_byte(1), FETCH_BUDGET), Ok(result));
}

#[test]
fn unknown_block_is_not_available_once_the_executor_stops_waiting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = applying_client(listener.local_addr().unwrap());
    let (_executed, _server) = serve_results(listener);

    let started = Instant::now();
    let result = client.executed_block(B256::repeat_byte(9), FETCH_BUDGET);
    let elapsed = started.elapsed();
    assert_eq!(result, Err(ApplyFallback::Unavailable));
    assert!(elapsed >= ExecutedBlocks::SERVE_WAIT, "answered before waiting: {elapsed:?}");
    assert!(elapsed < FETCH_BUDGET + Duration::from_millis(200), "waited {elapsed:?}");
}

#[test]
fn fetch_gives_up_at_its_budget_while_the_executor_still_waits() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = applying_client(listener.local_addr().unwrap());
    let (_executed, _server) = serve_results(listener);

    let budget = Duration::from_millis(50);
    let started = Instant::now();
    let result = client.executed_block(B256::repeat_byte(9), budget);
    let elapsed = started.elapsed();
    assert_eq!(result, Err(ApplyFallback::Unavailable));
    assert!(elapsed < ExecutedBlocks::SERVE_WAIT, "waited {elapsed:?}");
}

#[test]
fn result_stored_after_the_request_arrives_answers_that_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = applying_client(listener.local_addr().unwrap());
    let (executed, _server) = serve_results(listener);
    let hash = B256::repeat_byte(3);
    let result = Bytes::from_static(b"finished late");

    let finish = {
        let executed = Arc::clone(&executed);
        let result = result.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            executed.insert(hash, result);
        })
    };
    assert_eq!(client.executed_block(hash, FETCH_BUDGET), Ok(result));
    finish.join().unwrap();
}

#[test]
fn unreachable_executor_is_reported_as_unreachable() {
    let client = applying_client(closed_port());
    assert_eq!(
        client.executed_block(B256::repeat_byte(1), FETCH_BUDGET),
        Err(ApplyFallback::Unreachable)
    );
}

#[test]
fn executor_keeps_only_its_most_recent_results() {
    let executed = ExecutedBlocks::default();
    let hash = |index: usize| B256::left_padding_from(&index.to_be_bytes());
    for index in 0..=ExecutedBlocks::CAPACITY {
        executed.insert(hash(index), Bytes::from(index.to_be_bytes().to_vec()));
    }
    assert_eq!(executed.wait_for(hash(0), Duration::ZERO), None);
    for index in 1..=ExecutedBlocks::CAPACITY {
        assert!(executed.wait_for(hash(index), Duration::ZERO).is_some(), "lost block {index}");
    }
}

/// A peer that answers one request with a valid execution result, sent in chunks
/// [`TRICKLE_GAP`] apart, each well within any read timeout but together taking
/// [`TRICKLE_TOTAL`], longer than the fetch budget.
fn trickling_executor() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _: Request = Frame::read(&mut stream).unwrap();
        let answer: ReadResult = Ok(Response::ExecutedBlock(Bytes::from(vec![7; 1000])));
        let mut frame = Vec::new();
        Frame::write(&mut frame, &answer).unwrap();
        let chunks = (TRICKLE_TOTAL.as_millis() / TRICKLE_GAP.as_millis()) as usize;
        for chunk in frame.chunks(frame.len().div_ceil(chunks)) {
            std::thread::sleep(TRICKLE_GAP);
            if std::io::Write::write_all(&mut stream, chunk).is_err() {
                return;
            }
        }
    });
    address
}

const TRICKLE_GAP: Duration = Duration::from_millis(50);
const TRICKLE_TOTAL: Duration = Duration::from_millis(1000);

/// How late past its budget a fetch may return.
const DEADLINE_SLACK: Duration = Duration::from_millis(150);

/// Stateless node 3 applying from nodes 1 and 2, in that order.
fn client_applying_from(owner_1: SocketAddr, owner_2: SocketAddr) -> ClusterClient {
    let view = cluster_of(3, 2, owner_1, owner_2);
    let config = ClusterConfig { apply_from: vec![NodeId(1), NodeId(2)], ..view.config().clone() };
    ClusterClient::new(Arc::new(Cluster::new(config).unwrap()), ClientOptions::default())
}

#[test]
fn fetch_budget_bounds_a_result_that_arrives_in_slow_chunks() {
    let client = applying_client(trickling_executor());

    let started = Instant::now();
    let result = client.executed_block(B256::repeat_byte(1), FETCH_BUDGET);
    let elapsed = started.elapsed();
    assert_eq!(result, Err(ApplyFallback::Unavailable));
    assert!(elapsed < FETCH_BUDGET + DEADLINE_SLACK, "waited {elapsed:?}");
}

#[test]
fn fetch_budget_is_shared_by_every_source() {
    let client = client_applying_from(trickling_executor(), trickling_executor());

    let started = Instant::now();
    let result = client.executed_block(B256::repeat_byte(1), FETCH_BUDGET);
    let elapsed = started.elapsed();
    assert_eq!(result, Err(ApplyFallback::Unavailable));
    assert!(elapsed < FETCH_BUDGET + DEADLINE_SLACK, "waited {elapsed:?}");
}
