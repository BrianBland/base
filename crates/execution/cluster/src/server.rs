//! Owner server: answers peer read requests from an [`OwnerStore`], execution-result requests
//! from [`ExecutedBlocks`], and captured-hash requests from [`RootCaptures`], on its own threads.

use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use alloy_primitives::B256;
use parking_lot::Mutex;
use reth_db_api::Database;
use tracing::{debug, error, warn};

use crate::{
    CapturedHashes, ExecutedBlocks, Frame, OwnerStore, Query, ReadError, ReadResult, Request,
    Response, RootCaptures, StoreError,
};

/// A running owner server. Dropping it stops accepting and closes open connections.
#[derive(Debug)]
pub struct OwnerServer {
    local_addr: SocketAddr,
    stopping: Arc<AtomicBool>,
    connections: Arc<Mutex<HashMap<u64, TcpStream>>>,
    acceptor: Option<JoinHandle<()>>,
}

impl OwnerServer {
    /// How long the accepting thread waits between polls for a connection, and so at most how
    /// long it takes to notice that the server is dropped.
    pub const ACCEPT_POLL: Duration = Duration::from_millis(10);

    /// How long [`Self::bind`] keeps retrying an address in use: longer than Linux keeps a
    /// closed connection in TIME-WAIT (60 seconds).
    pub const BIND_WAIT: Duration = Duration::from_secs(90);

    /// Pause between two attempts of [`Self::bind`].
    pub const BIND_RETRY_INTERVAL: Duration = Duration::from_secs(1);

    /// Binds the owner listener on `address`, retrying for at most `wait` while the address is
    /// in use.
    ///
    /// std marks the listener address-reusable, which is enough for connections this node left
    /// in TIME-WAIT. A socket that was not marked reusable (an old peer's connection that
    /// connected to itself on this port, for example) still blocks the port until it leaves
    /// TIME-WAIT, so the bind is retried rather than failing the start.
    pub fn bind(address: SocketAddr, wait: Duration) -> io::Result<TcpListener> {
        let deadline = Instant::now() + wait;
        loop {
            match TcpListener::bind(address) {
                Err(error)
                    if error.kind() == io::ErrorKind::AddrInUse
                        && Instant::now() + Self::BIND_RETRY_INTERVAL <= deadline =>
                {
                    warn!(
                        address = %address,
                        error = %error,
                        retry_in = ?Self::BIND_RETRY_INTERVAL,
                        "cluster owner port in use; retrying the bind (a closed connection in \
                         TIME-WAIT holds it for up to a minute)"
                    );
                    std::thread::sleep(Self::BIND_RETRY_INTERVAL);
                }
                result => return result,
            }
        }
    }

    /// Serves `store` and `executed` on `listener`, with no captured hashes: one thread accepts,
    /// and each connection is served on its own thread, one request at a time.
    pub fn spawn<DB: Database + 'static>(
        listener: TcpListener,
        store: Arc<OwnerStore<DB>>,
        executed: Arc<ExecutedBlocks>,
    ) -> io::Result<Self> {
        Self::spawn_with_root_captures(listener, store, executed, Arc::default())
    }

    /// Like [`Self::spawn`], also serving the captured hashes in `root_captures`.
    pub fn spawn_with_root_captures<DB: Database + 'static>(
        listener: TcpListener,
        store: Arc<OwnerStore<DB>>,
        executed: Arc<ExecutedBlocks>,
        root_captures: Arc<RootCaptures>,
    ) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        // Accepting never blocks, so the acceptor notices the stop flag without a connection.
        listener.set_nonblocking(true)?;
        let stopping = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let acceptor = {
            let stopping = Arc::clone(&stopping);
            let connections = Arc::clone(&connections);
            std::thread::Builder::new().name("cluster-owner-accept".into()).spawn(move || {
                let mut next_id = 0u64;
                while !stopping.load(Ordering::Acquire) {
                    let stream = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) => {
                            if error.kind() != io::ErrorKind::WouldBlock {
                                debug!(error = %error, "cluster owner accept failed");
                            }
                            std::thread::sleep(Self::ACCEPT_POLL);
                            continue;
                        }
                    };
                    let served = Self::serve_connection(
                        next_id,
                        stream,
                        (&store, &executed, &root_captures),
                        &connections,
                    );
                    next_id += 1;
                    if let Err(error) = served {
                        error!(error = %error, "failed to start cluster owner connection");
                    }
                }
            })?
        };
        Ok(Self { local_addr, stopping, connections, acceptor: Some(acceptor) })
    }

    /// Address the server listens on.
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn serve_connection<DB: Database + 'static>(
        id: u64,
        stream: TcpStream,
        (store, executed, root_captures): (
            &Arc<OwnerStore<DB>>,
            &Arc<ExecutedBlocks>,
            &Arc<RootCaptures>,
        ),
        connections: &Arc<Mutex<HashMap<u64, TcpStream>>>,
    ) -> io::Result<()> {
        // Some platforms hand out accepted connections non-blocking like their listener.
        stream.set_nonblocking(false)?;
        stream.set_nodelay(true)?;
        connections.lock().insert(id, stream.try_clone()?);
        let store = Arc::clone(store);
        let executed = Arc::clone(executed);
        let root_captures = Arc::clone(root_captures);
        let connections = Arc::clone(connections);
        std::thread::Builder::new().name("cluster-owner-conn".into()).spawn(move || {
            Self::answer_requests(stream, &store, &executed, &root_captures);
            connections.lock().remove(&id);
        })?;
        Ok(())
    }

    /// The capture of block `number` with hash `hash`, served only once `store`'s history index
    /// is published: an owner still building it is not ready, and answers no query with data,
    /// so a verifier moves on to another owner.
    fn captured<DB: Database>(
        store: &OwnerStore<DB>,
        root_captures: &RootCaptures,
        number: u64,
        hash: B256,
    ) -> Option<Arc<CapturedHashes>> {
        store.coverage()?;
        root_captures.get(number, hash)
    }

    /// Answers requests until the peer disconnects, a frame is malformed, or the store fails.
    fn answer_requests<DB: Database>(
        mut stream: TcpStream,
        store: &OwnerStore<DB>,
        executed: &ExecutedBlocks,
        root_captures: &RootCaptures,
    ) {
        loop {
            let request: Request = match Frame::read(&mut stream) {
                Ok(request) => request,
                Err(error) => {
                    debug!(error = %error, "cluster owner connection closed");
                    return;
                }
            };
            let result: ReadResult = match request.query {
                Query::ExecutedBlock { hash } => executed
                    .wait_for(hash, ExecutedBlocks::SERVE_WAIT)
                    .map(Response::ExecutedBlock)
                    .ok_or(ReadError::NotAvailable),
                Query::ShardHashes { number, hash } => {
                    Self::captured(store, root_captures, number, hash)
                        .map(|capture| Response::ShardHashes(capture.shard_hashes()))
                        .ok_or(ReadError::NotAvailable)
                }
                Query::SplitShardHashes { number, hash, storage_roots } => {
                    let storage_roots = storage_roots.into_iter().collect();
                    Self::captured(store, root_captures, number, hash)
                        .map(|capture| {
                            Response::SplitShardHashes(capture.split_shard_hashes(&storage_roots))
                        })
                        .ok_or(ReadError::NotAvailable)
                }
                Query::ExecutedBlockAt { hash, number } => executed
                    .serve(hash, number, ExecutedBlocks::SERVE_WAIT)
                    .map(Response::ExecutedBlock),
                _ => match store.read(&request) {
                    Ok(response) => Ok(response),
                    Err(StoreError::Refused(refusal)) => Err(refusal),
                    Err(StoreError::Database(error)) => {
                        error!(error = %error, table = ?request.table, "cluster owner read failed");
                        return;
                    }
                },
            };
            if let Err(error) = Frame::write(&mut stream, &result) {
                debug!(error = %error, "cluster owner reply failed");
                return;
            }
        }
    }
}

impl Drop for OwnerServer {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for (_, connection) in self.connections.lock().drain() {
            let _ = connection.shutdown(std::net::Shutdown::Both);
        }
    }
}
