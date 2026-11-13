//! Blocking peer read client with a small per-peer connection pool.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{B256, Bytes};
use parking_lot::Mutex;
use thiserror::Error;
use tracing::debug;

use crate::{
    ApplyFallback, Cluster, DuplicateValues, Frame, FrameError, NodeId, Query, ReadError,
    ReadResult, Request, Response, Shard, ShardEntries, ShardedTable, SnapshotId,
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
        match self.read(Request { snapshot, table, query }, Self::key_shard(table, key)?)? {
            Response::Value(value) => Ok(value),
            Response::Entries(_) | Response::Duplicates(_) | Response::ExecutedBlock(_) => {
                unreachable!("checked by read")
            }
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
        match self.read(Request { snapshot, table, query }, Self::key_shard(table, key)?)? {
            Response::Entries(entries) => Ok(entries),
            Response::Value(_) | Response::Duplicates(_) | Response::ExecutedBlock(_) => {
                unreachable!("checked by read")
            }
        }
    }

    /// Up to `limit` duplicate values of `key` with subkey `>= subkey`, as of `snapshot`. For a
    /// split contract the owners of the subkey's range answer, within that range only.
    pub fn seek_dup_ge(
        &self,
        snapshot: SnapshotId,
        table: ShardedTable,
        key: &[u8],
        subkey: &[u8],
        limit: u32,
    ) -> Result<DuplicateValues, ClientError> {
        let shard = if self.cluster.is_split(table, key) {
            self.cluster.placement(table, key, subkey).ok_or(ClientError::InvalidKey)?;
            Shard(subkey[0])
        } else {
            Self::key_shard(table, key)?
        };
        let query = Query::SeekDupGe { key: key.to_vec(), subkey: subkey.to_vec(), limit };
        match self.read(Request { snapshot, table, query }, shard)? {
            Response::Duplicates(values) => Ok(values),
            Response::Value(_) | Response::Entries(_) | Response::ExecutedBlock(_) => {
                unreachable!("checked by read")
            }
        }
    }

    /// The encoded execution result of block `hash` from the first of the cluster's `apply_from`
    /// nodes that has it, asking them in order. One deadline, `budget` from now, bounds every
    /// connect, write and read across all of them, and an answer completed after it is a miss.
    /// Fails with [`ApplyFallback::Unavailable`] when a node was reached but none answered with
    /// the result in time, and [`ApplyFallback::Unreachable`] when no node was reached.
    pub fn executed_block(&self, hash: B256, budget: Duration) -> Result<Bytes, ApplyFallback> {
        let deadline = Instant::now() + budget;
        let request = Request {
            snapshot: SnapshotId { number: 0, hash: None },
            table: ShardedTable::HashedAccounts,
            query: Query::ExecutedBlock { hash },
        };
        let mut failure = ApplyFallback::Unreachable;
        for &node in self.cluster.apply_from() {
            if Instant::now() >= deadline {
                return Err(ApplyFallback::Unavailable);
            }
            match self.ask_before(node, &request, deadline) {
                Ok(Ok(Response::ExecutedBlock(result))) => return Ok(result),
                Ok(Ok(_)) => {
                    debug!(node = node.0, "unexpected answer to an execution result request")
                }
                Ok(Err(refusal)) => {
                    debug!(node = node.0, refusal = %refusal, "execution result refused");
                    failure = ApplyFallback::Unavailable;
                }
                Err(error) => {
                    debug!(node = node.0, error = %error, "execution result request failed");
                    if Self::timed_out(&error) {
                        failure = ApplyFallback::Unavailable;
                    }
                }
            }
        }
        Err(failure)
    }

    /// Whether `error` is a socket timeout: the peer was reached but did not answer in time.
    fn timed_out(error: &FrameError) -> bool {
        matches!(error, FrameError::Io(io) if matches!(io.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut))
    }

    /// Asks `node` and waits for its answer until `deadline`, on a pooled connection when one is
    /// idle, replaced by a fresh one once if it fails and time remains.
    fn ask_before(
        &self,
        node: NodeId,
        request: &Request,
        deadline: Instant,
    ) -> Result<ReadResult, FrameError> {
        let idle = self.pools[&node].lock().pop();
        if let Some(stream) = idle {
            match self.exchange_before(node, stream, request, deadline) {
                Ok(result) => return Ok(result),
                Err(error) if Self::timed_out(&error) => return Err(error),
                Err(_) => {}
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(DeadlineStream::expired().into());
        }
        let stream = self.connect_within(node, remaining)?;
        self.exchange_before(node, stream, request, deadline)
    }

    /// Like [`Self::exchange`], with every write and read of the exchange finished by `deadline`.
    /// The connection goes back to the pool with its usual timeouts.
    fn exchange_before(
        &self,
        node: NodeId,
        stream: TcpStream,
        request: &Request,
        deadline: Instant,
    ) -> Result<ReadResult, FrameError> {
        let mut bounded = DeadlineStream { stream, deadline };
        Frame::write(&mut bounded, request)?;
        let result = Frame::read(&mut bounded)?;
        if Instant::now() > deadline {
            return Err(DeadlineStream::expired().into());
        }
        bounded.stream.set_read_timeout(Some(self.options.io_timeout))?;
        bounded.stream.set_write_timeout(Some(self.options.io_timeout))?;
        self.release(node, bounded.stream);
        Ok(result)
    }

    /// The shard whose owners serve encoded `key` of `table`: the shard of its first byte.
    fn key_shard(table: ShardedTable, key: &[u8]) -> Result<Shard, ClientError> {
        table.placement(key).ok_or(ClientError::InvalidKey)?;
        Ok(Shard(key[0]))
    }

    /// Sends `request` to the owners of `shard` until one answers with the response kind the
    /// query expects.
    fn read(&self, request: Request, shard: Shard) -> Result<Response, ClientError> {
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
                | (Query::ExecutedBlock { .. }, Response::ExecutedBlock(_))
        );
        if expected { Ok(response) } else { Err(OwnerFailure::UnexpectedResponse) }
    }

    fn connect(&self, node: NodeId) -> Result<TcpStream, FrameError> {
        self.connect_within(node, self.options.connect_timeout)
    }

    /// Connects to `node`, waiting at most `within` (and never longer than the configured connect
    /// timeout).
    fn connect_within(&self, node: NodeId, within: Duration) -> Result<TcpStream, FrameError> {
        let address = self.cluster.address(node).expect("asked nodes are listed cluster nodes");
        let timeout = within.min(self.options.connect_timeout);
        let stream = TcpStream::connect_timeout(&address, timeout)?;
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
        self.release(node, stream);
        Ok(result)
    }

    /// Returns a healthy connection to `node`'s pool, unless the pool is full.
    fn release(&self, node: NodeId, stream: TcpStream) {
        let mut idle = self.pools[&node].lock();
        if idle.len() < self.options.max_idle_per_owner {
            idle.push(stream);
        }
    }
}

/// A connection whose every read and write must finish by `deadline`: each operation's socket
/// timeout is the time left, and an operation started at or after it fails with `TimedOut`.
#[derive(Debug)]
pub struct DeadlineStream {
    /// The connection.
    pub stream: TcpStream,
    /// When every operation must have finished.
    pub deadline: Instant,
}

impl DeadlineStream {
    /// The error of an operation that missed the deadline.
    pub fn expired() -> io::Error {
        io::ErrorKind::TimedOut.into()
    }

    fn remaining(&self) -> io::Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() { Err(Self::expired()) } else { Ok(remaining) }
    }
}

impl Read for DeadlineStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.set_read_timeout(Some(self.remaining()?))?;
        self.stream.read(buf)
    }
}

impl Write for DeadlineStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}
