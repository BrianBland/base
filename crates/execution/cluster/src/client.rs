//! Blocking peer read client with a small per-peer connection pool.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_primitives::{B256, Bytes};
use parking_lot::Mutex;
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    ApplyFallback, BlockShardHashes, Cluster, Frame, FrameError, KeyPlacement, NodeId, Query,
    ReadError, ReadResult, RemoteReadStats, Request, Response, ServableRange, Shard, ShardHash,
    ShardedTable, SnapshotId,
};

/// Timeouts, retry budget, and pool size of a [`ClusterClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientOptions {
    /// Timeout for opening a connection to an owner.
    pub connect_timeout: Duration,
    /// Timeout for each socket read or write.
    pub io_timeout: Duration,
    /// How long one read keeps retrying while every owner is unreachable or behind the requested
    /// snapshot.
    pub retry_for: Duration,
    /// Pause between retry rounds.
    pub retry_interval: Duration,
    /// Idle connections kept per owner.
    pub max_idle_per_owner: usize,
}

impl ClientOptions {
    /// Default [`Self::retry_for`]. An owner behind the requested block normally catches up
    /// within a few seconds; giving up sooner fails the block being executed.
    pub const DEFAULT_RETRY_FOR: Duration = Duration::from_secs(60);
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            io_timeout: Duration::from_secs(5),
            retry_for: Self::DEFAULT_RETRY_FOR,
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
/// (including timing out) or behind the snapshot. A read that waits on owners for longer than
/// [`Self::SLOW_WAIT`] logs one warning and counts one wait in [`Self::remote_read_stats`].
#[derive(Debug)]
pub struct ClusterClient {
    cluster: Arc<Cluster>,
    options: ClientOptions,
    pools: HashMap<NodeId, Mutex<Vec<TcpStream>>>,
    remote_reads: Arc<RemoteReadStats>,
    execute_below: AtomicU64,
}

impl ClusterClient {
    /// How long a read may wait on behind owners before it is logged and counted as a wait.
    pub const SLOW_WAIT: Duration = Duration::from_secs(1);

    /// Creates a client for `cluster`'s node.
    pub fn new(cluster: Arc<Cluster>, options: ClientOptions) -> Self {
        let pools =
            cluster.config().nodes.iter().map(|node| (node.id, Mutex::new(Vec::new()))).collect();
        Self {
            cluster,
            options,
            pools,
            remote_reads: Arc::default(),
            execute_below: AtomicU64::new(0),
        }
    }

    /// The options this client was created with.
    pub const fn options(&self) -> &ClientOptions {
        &self.options
    }

    /// The counters of this client's remote reads.
    pub const fn remote_read_stats(&self) -> &Arc<RemoteReadStats> {
        &self.remote_reads
    }

    /// The encoded account at hashed `address` as of `snapshot`, or `None` when absent.
    pub fn account(
        &self,
        snapshot: SnapshotId,
        address: B256,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let table = ShardedTable::HashedAccounts;
        let shard = Self::key_shard(table, address.as_slice())?;
        match self
            .read(Request { snapshot, table, query: Query::AccountValue { address } }, shard)?
        {
            Response::Value(value) => Ok(value),
            _ => unreachable!("checked by read"),
        }
    }

    /// The encoded storage entry of exactly hashed `slot` of hashed `address` as of `snapshot`,
    /// or `None` when the slot is absent. For a split contract the owners of the slot's range
    /// answer.
    pub fn storage(
        &self,
        snapshot: SnapshotId,
        address: B256,
        slot: B256,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let table = ShardedTable::HashedStorages;
        let shard = match self.cluster.placement(table, address.as_slice(), slot.as_slice()) {
            Some(KeyPlacement::Shard(shard)) => shard,
            Some(KeyPlacement::Replicated) | None => return Err(ClientError::InvalidKey),
        };
        let query = Query::StorageValue { address, slot };
        match self.read(Request { snapshot, table, query }, shard)? {
            Response::Value(value) => Ok(value),
            _ => unreachable!("checked by read"),
        }
    }

    /// Whether hashed `address` has any storage as of `snapshot`.
    pub fn has_storage(&self, snapshot: SnapshotId, address: B256) -> Result<bool, ClientError> {
        let table = ShardedTable::HashedStorages;
        let shard = Self::key_shard(table, address.as_slice())?;
        match self.read(Request { snapshot, table, query: Query::HasStorage { address } }, shard)? {
            Response::HasStorage(has) => Ok(has),
            _ => unreachable!("checked by read"),
        }
    }

    /// The encoded bytecode with hash `key`, or `None` when absent.
    pub fn bytecode(
        &self,
        snapshot: SnapshotId,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let table = ShardedTable::Bytecodes;
        let query = Query::Get { key: key.to_vec() };
        match self.read(Request { snapshot, table, query }, Self::key_shard(table, key)?)? {
            Response::Value(value) => Ok(value),
            _ => unreachable!("checked by read"),
        }
    }

    /// The range of blocks `node` can serve state reads as of, asked once.
    pub fn status(&self, node: NodeId) -> Result<Option<ServableRange>, OwnerFailure> {
        let request = Request {
            snapshot: SnapshotId { number: 0, hash: None },
            table: ShardedTable::HashedAccounts,
            query: Query::Status,
        };
        match self.ask(node, &request)? {
            Response::Status(servable) => Ok(servable),
            _ => unreachable!("checked by ask"),
        }
    }

    /// The subtree hashes `node` captured for block `number` with hash `hash`, asked once.
    pub fn shard_hashes(
        &self,
        node: NodeId,
        number: u64,
        hash: B256,
    ) -> Result<BlockShardHashes, OwnerFailure> {
        match self.ask(node, &Self::unsnapshotted(Query::ShardHashes { number, hash }))? {
            Response::ShardHashes(hashes) => Ok(hashes),
            _ => unreachable!("checked by ask"),
        }
    }

    /// `node`'s hashes, at block `number` with hash `hash`, of its held account shards that
    /// contain split contracts, recomputed with `storage_roots`, asked once.
    pub fn split_shard_hashes(
        &self,
        node: NodeId,
        number: u64,
        hash: B256,
        storage_roots: &[(B256, B256)],
    ) -> Result<Vec<(Shard, ShardHash)>, OwnerFailure> {
        let storage_roots = storage_roots.to_vec();
        let query = Query::SplitShardHashes { number, hash, storage_roots };
        match self.ask(node, &Self::unsnapshotted(query))? {
            Response::SplitShardHashes(hashes) => Ok(hashes),
            _ => unreachable!("checked by ask"),
        }
    }

    /// A request of `query`, which ignores the snapshot and table.
    const fn unsnapshotted(query: Query) -> Request {
        Request {
            snapshot: SnapshotId { number: 0, hash: None },
            table: ShardedTable::HashedAccounts,
            query,
        }
    }

    /// The encoded execution result of block `hash`, numbered `number`, from the first of the
    /// cluster's `apply_from` nodes that has it, asking them in order. One deadline, `budget`
    /// from now, bounds every connect, write and read across all of them, and an answer completed
    /// after it is a miss. Fails with [`ApplyFallback::Unavailable`] when a node was reached but
    /// none answered with the result in time, and [`ApplyFallback::Unreachable`] when no node was
    /// reached.
    ///
    /// When every node answers that it keeps no block that old ([`ReadError::NotRetained`] with
    /// its oldest kept block above `number`), this node is behind them all: later blocks below
    /// the lowest of those oldest numbers fail with [`ApplyFallback::Behind`] without asking, so
    /// catching up costs no fetch time until the blocks are back within the nodes' window.
    pub fn executed_block(
        &self,
        hash: B256,
        number: u64,
        budget: Duration,
    ) -> Result<Bytes, ApplyFallback> {
        if number < self.execute_below.load(Ordering::Relaxed) {
            return Err(ApplyFallback::Behind);
        }
        let deadline = Instant::now() + budget;
        let request = Request {
            snapshot: SnapshotId { number: 0, hash: None },
            table: ShardedTable::HashedAccounts,
            query: Query::ExecutedBlockAt { hash, number },
        };
        let mut failure = ApplyFallback::Unreachable;
        let mut retained_from = Some(u64::MAX);
        for &node in self.cluster.apply_from() {
            if Instant::now() >= deadline {
                return Err(ApplyFallback::Unavailable);
            }
            match self.ask_before(node, &request, deadline) {
                Ok(Ok(Response::ExecutedBlock(result))) => return Ok(result),
                Ok(Ok(_)) => {
                    debug!(node = node.0, "unexpected answer to an execution result request");
                    retained_from = None;
                }
                Ok(Err(refusal)) => {
                    debug!(node = node.0, refusal = %refusal, "execution result refused");
                    failure = ApplyFallback::Unavailable;
                    retained_from = match refusal {
                        ReadError::NotRetained { oldest, .. } => {
                            retained_from.map(|lowest| lowest.min(oldest))
                        }
                        _ => None,
                    };
                }
                Err(error) => {
                    debug!(node = node.0, error = %error, "execution result request failed");
                    if Self::timed_out(&error) {
                        failure = ApplyFallback::Unavailable;
                    }
                    retained_from = None;
                }
            }
        }
        if let Some(oldest) = retained_from.filter(|&oldest| oldest > number) {
            self.execute_below.fetch_max(oldest, Ordering::Relaxed);
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
        let mut bounded = DeadlineStream { stream, deadline, io_timeout: self.options.io_timeout };
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

        let started = Instant::now();
        let retry_until = started + self.options.retry_for;
        let mut warned = false;
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
            let retryable = attempts.iter().all(|attempt| {
                behind(attempt) || matches!(attempt.failure, OwnerFailure::Unreachable(_))
            });
            if !retryable || Instant::now() + self.options.retry_interval > retry_until {
                return Err(ClientError::OwnersFailed(attempts));
            }
            if !warned && started.elapsed() > Self::SLOW_WAIT {
                warned = true;
                self.remote_reads.record_wait();
                Self::warn_waiting(&request, &attempts, started.elapsed());
            }
            std::thread::sleep(self.options.retry_interval);
        }
    }

    /// Logs that `request` has waited `waited` on owners behind its snapshot, with the highest
    /// block each owner of the last round could serve (`None`: unreachable or nothing servable).
    fn warn_waiting(request: &Request, attempts: &[OwnerAttempt], waited: Duration) {
        let owners_highest: Vec<_> = attempts
            .iter()
            .map(|attempt| {
                let highest = match &attempt.failure {
                    OwnerFailure::Refused(ReadError::SnapshotUnavailable {
                        servable: Some(range),
                    }) => Some(range.highest),
                    _ => None,
                };
                (attempt.node.0, highest)
            })
            .collect();
        warn!(
            table = ?request.table,
            block = request.snapshot.number,
            owners_highest = ?owners_highest,
            waited = ?waited,
            "cluster read waiting for owners behind the requested block"
        );
    }

    /// Asks one owner, on a pooled connection when one is idle. A pooled connection that fails is
    /// replaced by a fresh one once, since the owner may have closed it while idle.
    fn ask(&self, node: NodeId, request: &Request) -> Result<Response, OwnerFailure> {
        let result = self.ask_within_timeouts(node, request);
        let response = result.map_err(OwnerFailure::Unreachable)?.map_err(OwnerFailure::Refused)?;
        let expected = matches!(
            (&request.query, &response),
            (
                Query::Get { .. } | Query::AccountValue { .. } | Query::StorageValue { .. },
                Response::Value(_)
            ) | (Query::HasStorage { .. }, Response::HasStorage(_))
                | (Query::Status, Response::Status(_))
                | (
                    Query::ExecutedBlock { .. } | Query::ExecutedBlockAt { .. },
                    Response::ExecutedBlock(_)
                )
                | (Query::ShardHashes { .. }, Response::ShardHashes(_))
                | (Query::SplitShardHashes { .. }, Response::SplitShardHashes(_))
        );
        if expected { Ok(response) } else { Err(OwnerFailure::UnexpectedResponse) }
    }

    /// Asks `node`, each connect, write and read waiting at most its configured timeout.
    fn ask_within_timeouts(
        &self,
        node: NodeId,
        request: &Request,
    ) -> Result<ReadResult, FrameError> {
        let idle = self.pools[&node].lock().pop();
        if let Some(Ok(result)) = idle.map(|stream| self.exchange(node, stream, request)) {
            return Ok(result);
        }
        let stream = self.connect_within(node, self.options.connect_timeout)?;
        self.exchange(node, stream, request)
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
/// timeout is the time left, at most `io_timeout`, and an operation started at or after the
/// deadline fails with `TimedOut`.
#[derive(Debug)]
pub struct DeadlineStream {
    /// The connection.
    pub stream: TcpStream,
    /// When every operation must have finished.
    pub deadline: Instant,
    /// The longest one operation may wait, however far the deadline is.
    pub io_timeout: Duration,
}

impl DeadlineStream {
    /// The error of an operation that missed the deadline.
    pub fn expired() -> io::Error {
        io::ErrorKind::TimedOut.into()
    }

    fn remaining(&self) -> io::Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() { Err(Self::expired()) } else { Ok(remaining.min(self.io_timeout)) }
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
