//! Owner server: answers peer read requests from an [`OwnerStore`], and execution-result requests
//! from [`ExecutedBlocks`], on its own threads.

use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};

use parking_lot::Mutex;
use reth_db_api::transaction::DbTx;
use tracing::{debug, error};

use crate::{
    ExecutedBlocks, Frame, OwnerStore, Query, ReadError, ReadResult, Request, Response, StoreError,
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
    /// Serves `store` and `executed` on `listener`: one thread accepts, and each connection is
    /// served on its own thread, one request at a time.
    pub fn spawn<TX: DbTx + Sync + 'static>(
        listener: TcpListener,
        store: Arc<OwnerStore<TX>>,
        executed: Arc<ExecutedBlocks>,
    ) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        let stopping = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let acceptor = {
            let stopping = Arc::clone(&stopping);
            let connections = Arc::clone(&connections);
            std::thread::Builder::new().name("cluster-owner-accept".into()).spawn(move || {
                for (id, stream) in (0u64..).zip(listener.incoming()) {
                    if stopping.load(Ordering::Acquire) {
                        break;
                    }
                    let stream = match stream {
                        Ok(stream) => stream,
                        Err(error) => {
                            debug!(error = %error, "cluster owner accept failed");
                            continue;
                        }
                    };
                    let served =
                        Self::serve_connection(id, stream, &store, &executed, &connections);
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

    fn serve_connection<TX: DbTx + Sync + 'static>(
        id: u64,
        stream: TcpStream,
        store: &Arc<OwnerStore<TX>>,
        executed: &Arc<ExecutedBlocks>,
        connections: &Arc<Mutex<HashMap<u64, TcpStream>>>,
    ) -> io::Result<()> {
        stream.set_nodelay(true)?;
        connections.lock().insert(id, stream.try_clone()?);
        let store = Arc::clone(store);
        let executed = Arc::clone(executed);
        let connections = Arc::clone(connections);
        std::thread::Builder::new().name("cluster-owner-conn".into()).spawn(move || {
            Self::answer_requests(stream, &store, &executed);
            connections.lock().remove(&id);
        })?;
        Ok(())
    }

    /// Answers requests until the peer disconnects, a frame is malformed, or the store fails.
    fn answer_requests<TX: DbTx + Sync>(
        mut stream: TcpStream,
        store: &OwnerStore<TX>,
        executed: &ExecutedBlocks,
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
                Query::Get { .. } | Query::SeekGe { .. } | Query::SeekDupGe { .. } => {
                    match store.read(&request) {
                        Ok(response) => Ok(response),
                        Err(StoreError::Refused(refusal)) => Err(refusal),
                        Err(StoreError::Database(error)) => {
                            error!(error = %error, table = ?request.table, "cluster owner read failed");
                            return;
                        }
                    }
                }
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
        // Wake the blocked accept so the acceptor sees the stop flag.
        let _ = TcpStream::connect(self.local_addr);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for (_, connection) in self.connections.lock().drain() {
            let _ = connection.shutdown(std::net::Shutdown::Both);
        }
    }
}
