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
    time::Duration,
};

use parking_lot::Mutex;
use reth_db_api::Database;
use tracing::{debug, error};

use crate::{
    ExecutedBlocks, Frame, OwnerStore, Query, ReadError, ReadResult, Request, Response,
    RootCaptures, StoreError,
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
                Query::ShardHashes { number, hash } => root_captures
                    .get(number, hash)
                    .map(|capture| Response::ShardHashes(capture.shard_hashes()))
                    .ok_or(ReadError::NotAvailable),
                Query::SplitShardHashes { number, hash, storage_roots } => {
                    let storage_roots = storage_roots.into_iter().collect();
                    root_captures
                        .get(number, hash)
                        .map(|capture| {
                            Response::SplitShardHashes(capture.split_shard_hashes(&storage_roots))
                        })
                        .ok_or(ReadError::NotAvailable)
                }
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
