//! Stopping an owner server. A test binary of its own because its test re-runs it as a child
//! process: spawning a process while another test opens a socket can leak that socket into the
//! child (macOS marks sockets close-on-exec only after creating them), which would keep ports of
//! other tests open.

use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use base_execution_cluster::{Cluster, ClusterConfig, NodeConfig, NodeId, OwnerServer, OwnerStore};
use reth_db::test_utils::create_test_rw_db;

/// How long dropping an owner server may take.
const SERVER_STOP_LIMIT: Duration = Duration::from_millis(500);

/// Set in the child process of
/// [`dropping_a_server_returns_promptly_even_when_no_socket_can_be_opened`], which runs its
/// descriptor-exhaustion case.
const NO_DESCRIPTORS_CHILD: &str = "BASE_CLUSTER_TEST_NO_DESCRIPTORS_CHILD";

/// How long the child process may take.
const CHILD_LIMIT: Duration = Duration::from_secs(30);

/// An owner server of a one-node cluster with an empty store, listening on a free local port.
fn serve() -> (SocketAddr, OwnerServer) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cluster = Cluster::new(ClusterConfig {
        node_id: NodeId(1),
        replication: 1,
        retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
        nodes: vec![NodeConfig { id: NodeId(1), address, storage: true }],
        apply_from: Vec::new(),
        split_storage: Vec::new(),
        read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
        root_check: Default::default(),
    })
    .unwrap();
    let store = Arc::new(OwnerStore::new(Arc::new(cluster), create_test_rw_db(), 160).unwrap());
    (address, OwnerServer::spawn(listener, store, Arc::default()).unwrap())
}

/// Dropping a server returns promptly and closes its listener, also with every file descriptor
/// of the process in use, when nothing (in particular no connection to the server's own port)
/// can wake a blocked accept. That case runs in a child process with a small descriptor limit, so
/// that using them all disturbs nothing else; it is the only test here, so no other test opens a
/// socket while the child is spawned.
#[test]
fn dropping_a_server_returns_promptly_even_when_no_socket_can_be_opened() {
    if std::env::var_os(NO_DESCRIPTORS_CHILD).is_some() {
        let (address, server) = serve();
        let mut descriptors = Vec::new();
        while let Ok(file) = std::fs::File::open("/dev/null") {
            descriptors.push(file);
        }
        assert!(TcpStream::connect(address).is_err(), "no socket can be opened");
        drop(server);
        return;
    }

    let (address, server) = serve();
    let started = Instant::now();
    drop(server);
    assert!(started.elapsed() < SERVER_STOP_LIMIT, "took {:?}", started.elapsed());
    assert!(TcpStream::connect(address).is_err(), "the listener is closed");

    let mut child = Command::new("sh")
        .args(["-c", "ulimit -n 256 && exec \"$0\" \"$@\""])
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", "dropping_a_server_returns_promptly_even_when_no_socket_can_be_opened"])
        .env(NO_DESCRIPTORS_CHILD, "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > CHILD_LIMIT {
            child.kill().unwrap();
            panic!("dropping the server hung");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(status.success(), "child failed: {status}");
}
