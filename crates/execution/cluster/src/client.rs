//! Blocking peer read client with a small per-peer connection pool.

use std::{
    collections::HashMap,
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use thiserror::Error;

use crate::{
    Cluster, DuplicateValues, Frame, FrameError, NodeId, Query, ReadError, ReadResult, Request,
    Response, Shard, ShardEntries, ShardedTable, SnapshotId,
};

/// Timeouts, retry budget, and pool size of a [`ClusterClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientOptions {
    /// Timeout for opening a connection to an owner.
    pub connect_timeout: Duration,
    /// Timeout for each socket read or write.
    pub io_timeout: Duration,
    /// How long to keep retrying while every owner is behind the requested snapshot.
    pub retry_for: Duration,
    /// Pause between retry rounds.
    pub retry_interval: Duration,
    /// Idle connections kept per owner.
    pub max_idle_per_owner: usize,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            io_timeout: Duration::from_secs(5),
            retry_for: Duration::from_secs(2),
            retry_interval: Duration::from_millis(20),
            max_idle_per_owner: 4,
        }
    }
}

/// Why one owner did not answer a read.
#[derive(Debug)]
pub enum OwnerFailure {
    /// The owner refused.
    Refused(ReadError),
    /// The owner could not be reached, closed the connection, or sent a malformed frame.
    Unreachable(FrameError),
    /// The owner answered with a response of the wrong kind.
    UnexpectedResponse,
}

/// The outcome of asking one owner in the last round.
#[derive(Debug)]
pub struct OwnerAttempt {
    /// The owner asked.
    pub node: NodeId,
    /// Why it did not answer.
    pub failure: OwnerFailure,
}

/// Why a remote read failed.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The key is not a valid encoded key of the table.
    #[error("invalid key for table")]
    InvalidKey,
    /// No owner of the shard other than this node exists.
    #[error("no remote owner of shard {0:?}")]
    NoOwner(Shard),
    /// Every owner failed in the last round tried.
    #[error("every owner failed: {0:?}")]
    OwnersFailed(Vec<OwnerAttempt>),
}

/// Reads keys of shards this node does not own from their owners.
///
/// Calls block the calling thread. Owners are tried in rendezvous order, skipping this node;
/// the round is retried within [`ClientOptions::retry_for`] only while every owner is unreachable
/// or behind the snapshot and at least one is behind.
#[derive(Debug)]
pub struct ClusterClient {
    cluster: Arc<Cluster>,
    options: ClientOptions,
    pools: HashMap<NodeId, Mutex<Vec<TcpStream>>>,
}

impl ClusterClient {
    /// Creates a client for `cluster`'s node.
    pub fn new(cluster: Arc<Cluster>, options: ClientOptions) -> Self {
        let pools =
            cluster.config().nodes.iter().map(|node| (node.id, Mutex::new(Vec::new()))).collect();
        Self { cluster, options, pools }
    }

    /// The encoded value of `key` in `table` as of `snapshot`, or `None` when absent.
    pub fn get(
        &self,
        snapshot: SnapshotId,
        table: ShardedTable,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let query = Query::Get { key: key.to_vec() };
        match self.read(Request { snapshot, table, query }, key)? {
            Response::Value(value) => Ok(value),
            Response::Entries(_) | Response::Duplicates(_) => unreachable!("checked by read"),
        }
    }

    /// Up to `limit` entries of `key`'s shard with key `>= key`, as of `snapshot`.
    pub fn seek_ge(
        &self,
        snapshot: SnapshotId,
        table: ShardedTable,
        key: &[u8],
        limit: u32,
    ) -> Result<ShardEntries, ClientError> {
        let query = Query::SeekGe { key: key.to_vec(), limit };
        match self.read(Request { snapshot, table, query }, key)? {
            Response::Entries(entries) => Ok(entries),
            Response::Value(_) | Response::Duplicates(_) => unreachable!("checked by read"),
        }
    }

    /// Up to `limit` duplicate values of `key` with subkey `>= subkey`, as of `snapshot`.
    pub fn seek_dup_ge(
        &self,
        snapshot: SnapshotId,
        table: ShardedTable,
        key: &[u8],
        subkey: &[u8],
        limit: u32,
    ) -> Result<DuplicateValues, ClientError> {
        let query = Query::SeekDupGe { key: key.to_vec(), subkey: subkey.to_vec(), limit };
        match self.read(Request { snapshot, table, query }, key)? {
            Response::Duplicates(values) => Ok(values),
            Response::Value(_) | Response::Entries(_) => unreachable!("checked by read"),
        }
    }

    /// Sends `request` to the owners of `key`'s shard until one answers with the response kind
    /// the query expects.
    fn read(&self, request: Request, key: &[u8]) -> Result<Response, ClientError> {
        if request.table.placement(key).is_none() {
            return Err(ClientError::InvalidKey);
        }
        let shard = Shard(key[0]);
        let owners: Vec<_> = self
            .cluster
            .owners(shard)
            .iter()
            .copied()
            .filter(|node| *node != self.cluster.node_id())
            .collect();
        if owners.is_empty() {
            return Err(ClientError::NoOwner(shard));
        }

        let deadline = Instant::now() + self.options.retry_for;
        loop {
            let mut attempts = Vec::with_capacity(owners.len());
            for &node in &owners {
                match self.ask(node, &request) {
                    Ok(response) => return Ok(response),
                    Err(failure) => attempts.push(OwnerAttempt { node, failure }),
                }
            }
            let behind = |attempt: &OwnerAttempt| match &attempt.failure {
                OwnerFailure::Refused(refusal) => refusal.is_behind(request.snapshot.number),
                OwnerFailure::Unreachable(_) | OwnerFailure::UnexpectedResponse => false,
            };
            let retryable = attempts.iter().any(behind)
                && attempts.iter().all(|attempt| {
                    behind(attempt) || matches!(attempt.failure, OwnerFailure::Unreachable(_))
                });
            if !retryable || Instant::now() + self.options.retry_interval > deadline {
                return Err(ClientError::OwnersFailed(attempts));
            }
            std::thread::sleep(self.options.retry_interval);
        }
    }

    /// Asks one owner, on a pooled connection when one is idle. A pooled connection that fails is
    /// replaced by a fresh one once, since the owner may have closed it while idle.
    fn ask(&self, node: NodeId, request: &Request) -> Result<Response, OwnerFailure> {
        let pool = &self.pools[&node];
        let idle = pool.lock().pop();
        let result = match idle.map(|stream| self.exchange(node, stream, request)) {
            Some(Ok(result)) => result,
            Some(Err(_)) | None => {
                let stream = self.connect(node).map_err(OwnerFailure::Unreachable)?;
                self.exchange(node, stream, request).map_err(OwnerFailure::Unreachable)?
            }
        };
        let response = result.map_err(OwnerFailure::Refused)?;
        let expected = matches!(
            (&request.query, &response),
            (Query::Get { .. }, Response::Value(_))
                | (Query::SeekGe { .. }, Response::Entries(_))
                | (Query::SeekDupGe { .. }, Response::Duplicates(_))
        );
        if expected { Ok(response) } else { Err(OwnerFailure::UnexpectedResponse) }
    }

    fn connect(&self, node: NodeId) -> Result<TcpStream, FrameError> {
        let address = self.cluster.address(node).expect("owners are listed cluster nodes");
        let stream = TcpStream::connect_timeout(&address, self.options.connect_timeout)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(self.options.io_timeout))?;
        stream.set_write_timeout(Some(self.options.io_timeout))?;
        Ok(stream)
    }

    /// Sends `request` and reads the reply, returning the connection to the pool on success.
    fn exchange(
        &self,
        node: NodeId,
        mut stream: TcpStream,
        request: &Request,
    ) -> Result<ReadResult, FrameError> {
        Frame::write(&mut stream, request)?;
        let result = Frame::read(&mut stream)?;
        let mut idle = self.pools[&node].lock();
        if idle.len() < self.options.max_idle_per_owner {
            idle.push(stream);
        }
        Ok(result)
    }
}
