//! Owner server and client over real localhost sockets.

use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use base_execution_cluster::{
    ClientError, ClientOptions, Cluster, ClusterClient, ClusterConfig, Frame, NodeConfig, NodeId,
    OwnerFailure, OwnerServer, OwnerStore, Query, ReadError, ReadResult, Request, Response,
    ServableRange, Shard, ShardedTable, SnapshotId, StoreError,
};
use reth_db_api::{tables, transaction::DbTxMut};
use reth_primitives_traits::Bytecode;

use crate::common::{
    KeyUniverse, TableDump, TestFactory, TestTx, block_hash, factory, random_updates, rng,
    snapshot, write,
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
    Arc::new(Cluster::new(ClusterConfig { node_id: NodeId(node_id), replication, nodes }).unwrap())
}

fn closed_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

/// A database with random state in shard `shard` and a bytecode, and the bytecode's hash.
fn populated_db(shard: Shard) -> (TestFactory, B256) {
    let db = factory();
    let mut rng = rng(5);
    let keys = KeyUniverse::new(&mut rng, &[shard]);
    for _ in 0..3 {
        write(&db, &random_updates(&mut rng, &keys));
    }
    let mut code_hash = B256::repeat_byte(0x55);
    code_hash.0[0] = shard.0;
    let provider = db.provider_rw().unwrap();
    provider
        .tx_ref()
        .put::<tables::Bytecodes>(code_hash, Bytecode::new_raw(vec![0x00].into()))
        .unwrap();
    provider.commit().unwrap();
    (db, code_hash)
}

fn serve(cluster: &Arc<Cluster>, listener: TcpListener) -> (Arc<OwnerStore<TestTx>>, OwnerServer) {
    let store = Arc::new(OwnerStore::new(Arc::clone(cluster), 160));
    let server = OwnerServer::spawn(listener, Arc::clone(&store)).unwrap();
    (store, server)
}

fn local(store: &OwnerStore<TestTx>, request: &Request) -> ReadResult {
    match store.read(request) {
        Ok(response) => Ok(response),
        Err(StoreError::Refused(refusal)) => Err(refusal),
        Err(StoreError::Database(error)) => panic!("database error {error}"),
    }
}

#[test]
fn every_request_and_error_round_trips_over_a_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let cluster = cluster_of(1, 1, listener.local_addr().unwrap(), closed_port());
    let owned = Shard::all().find(|shard| cluster.owns(*shard)).unwrap();
    let unowned = Shard::all().find(|shard| !cluster.owns(*shard)).unwrap();

    let (db, code_hash) = populated_db(owned);
    let (store, server) = serve(&cluster, listener);
    let tip = SnapshotId { number: 0, hash: Some(block_hash(0, 0)) };
    store.register_persistence_point(BlockNumHash::new(0, block_hash(0, 0)), snapshot(&db));

    let dump = TableDump::read(&db);
    let address = dump.hashed_storages.keys().find(|k| dump.storages_trie.contains_key(*k));
    let address = address.expect("an address with storage and a storage trie").clone();
    let path = dump.accounts_trie.keys().find(|k| k[32] >= 2).expect("a sharded trie node").clone();
    let mut unowned_key = vec![0u8; 32];
    unowned_key[0] = unowned.0;
    let request = |snapshot, table, query| Request { snapshot, table, query };
    let cases = [
        request(tip, ShardedTable::HashedAccounts, Query::Get { key: address.clone() }),
        request(tip, ShardedTable::AccountsTrie, Query::Get { key: path.clone() }),
        request(tip, ShardedTable::Bytecodes, Query::Get { key: code_hash.to_vec() }),
        request(
            tip,
            ShardedTable::HashedAccounts,
            Query::SeekGe { key: address.clone(), limit: 4 },
        ),
        request(tip, ShardedTable::AccountsTrie, Query::SeekGe { key: path, limit: 4 }),
        request(
            tip,
            ShardedTable::HashedStorages,
            Query::SeekDupGe { key: address.clone(), subkey: vec![0; 32], limit: 4 },
        ),
        request(
            tip,
            ShardedTable::StoragesTrie,
            Query::SeekDupGe { key: address.clone(), subkey: vec![0; 33], limit: 4 },
        ),
        request(
            SnapshotId { number: 9, hash: None },
            ShardedTable::HashedAccounts,
            Query::Get { key: address.clone() },
        ),
        request(
            SnapshotId { number: 0, hash: Some(block_hash(1, 0)) },
            ShardedTable::HashedAccounts,
            Query::Get { key: address.clone() },
        ),
        request(tip, ShardedTable::HashedAccounts, Query::Get { key: unowned_key }),
        request(tip, ShardedTable::HashedStorages, Query::Get { key: address }),
    ];

    let mut stream = TcpStream::connect(server.local_addr()).unwrap();
    let mut seen_values = 0;
    let mut seen_errors = Vec::new();
    for case in &cases {
        Frame::write(&mut stream, case).unwrap();
        let remote: ReadResult = Frame::read(&mut stream).unwrap();
        assert_eq!(remote, local(&store, case), "{case:?}");
        match remote {
            Ok(Response::Value(Some(_))) => seen_values += 1,
            Ok(Response::Entries(entries)) => assert!(!entries.entries.is_empty()),
            Ok(Response::Duplicates(values)) => assert!(!values.values.is_empty()),
            Ok(Response::Value(None)) => panic!("expected stored data for {case:?}"),
            Err(error) => seen_errors.push(error),
        }
    }
    assert_eq!(seen_values, 3);
    assert_eq!(
        seen_errors,
        [
            ReadError::SnapshotUnavailable {
                servable: Some(ServableRange { lowest: 0, highest: 0 })
            },
            ReadError::WrongFork,
            ReadError::NotOwner,
            ReadError::Unsupported,
        ]
    );
}

#[test]
fn client_fails_over_when_the_first_owner_is_down() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let shard = Shard::all().find(|shard| client_view.owners(*shard)[0] == NodeId(2)).unwrap();

    let owner_view = Arc::new(
        Cluster::new(ClusterConfig { node_id: NodeId(1), ..client_view.config().clone() }).unwrap(),
    );
    let (db, code_hash) = populated_db(shard);
    let (store, _server) = serve(&owner_view, listener);
    store.register_persistence_point(BlockNumHash::new(0, block_hash(0, 0)), snapshot(&db));
    let tip = SnapshotId { number: 0, hash: None };

    let client = ClusterClient::new(client_view, ClientOptions::default());
    let dump = TableDump::read(&db);
    let address = dump.hashed_storages.keys().find(|k| dump.hashed_accounts.contains_key(*k));
    let address = address.expect("an address with an account and storage").clone();
    let expected = |table, query| local(&store, &Request { snapshot: tip, table, query }).unwrap();

    let get = client.get(tip, ShardedTable::HashedAccounts, &address).unwrap();
    assert_eq!(
        Response::Value(get),
        expected(ShardedTable::HashedAccounts, Query::Get { key: address.clone() })
    );
    let code = client.get(tip, ShardedTable::Bytecodes, code_hash.as_slice()).unwrap();
    assert!(code.is_some());
    let entries = client.seek_ge(tip, ShardedTable::HashedAccounts, &address, 8).unwrap();
    assert_eq!(
        Response::Entries(entries),
        expected(ShardedTable::HashedAccounts, Query::SeekGe { key: address.clone(), limit: 8 })
    );
    let values =
        client.seek_dup_ge(tip, ShardedTable::HashedStorages, &address, &[0; 32], 8).unwrap();
    assert_eq!(
        Response::Duplicates(values),
        expected(
            ShardedTable::HashedStorages,
            Query::SeekDupGe { key: address, subkey: vec![0; 32], limit: 8 }
        )
    );
}

#[test]
fn client_retries_while_every_owner_is_behind() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let owner_view = Arc::new(
        Cluster::new(ClusterConfig { node_id: NodeId(1), ..client_view.config().clone() }).unwrap(),
    );
    let (store, _server) = serve(&owner_view, listener);
    let db = factory();

    let catch_up = {
        let store = Arc::clone(&store);
        let tx = snapshot(&db);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            store.register_persistence_point(BlockNumHash::new(0, block_hash(0, 0)), tx);
        })
    };
    let options = ClientOptions { retry_for: Duration::from_secs(10), ..Default::default() };
    let client = ClusterClient::new(client_view, options);
    let tip = SnapshotId { number: 0, hash: Some(block_hash(0, 0)) };
    assert_eq!(client.get(tip, ShardedTable::HashedAccounts, &[0x42; 32]).unwrap(), None);
    catch_up.join().unwrap();
}

#[test]
fn client_returns_a_refusal_that_retrying_cannot_fix() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_view = cluster_of(3, 2, listener.local_addr().unwrap(), closed_port());
    let owner_view = Arc::new(
        Cluster::new(ClusterConfig { node_id: NodeId(1), ..client_view.config().clone() }).unwrap(),
    );
    let (store, _server) = serve(&owner_view, listener);
    store.register_persistence_point(BlockNumHash::new(0, block_hash(0, 0)), snapshot(&factory()));

    let options = ClientOptions { retry_for: Duration::from_secs(10), ..Default::default() };
    let client = ClusterClient::new(client_view, options);
    let other_fork = SnapshotId { number: 0, hash: Some(block_hash(1, 0)) };
    let started = Instant::now();
    let Err(ClientError::OwnersFailed(attempts)) =
        client.get(other_fork, ShardedTable::HashedAccounts, &[0x42; 32])
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
