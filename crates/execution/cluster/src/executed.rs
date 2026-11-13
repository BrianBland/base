//! Execution results of recent blocks: their wire encoding, the executor's bounded store, and the
//! bounded worker that fills it.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use alloy_eips::eip7685::Requests;
use alloy_primitives::{Address, B256, Bytes};
use metrics::Counter;
use parking_lot::{Condvar, Mutex};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::Receipt;
use reth_trie_common::{
    SortedTrieData,
    serde_bincode_compat::{hashed_state::HashedPostStateSorted, updates::TrieUpdatesSorted},
};
use revm::database::BundleState;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{Frame, ReadError};

/// What an apply node needs, besides the block it already has, to insert a block as executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedBlockParts<R> {
    /// The block's execution output: receipts, requests, gas used and the bundle state with
    /// reverts.
    pub output: BlockExecutionOutput<R>,
    /// The block's transaction senders, in transaction order.
    pub senders: Vec<Address>,
    /// The block's sorted hashed state and trie updates.
    pub trie: SortedTrieData,
}

/// The wire form of [`ExecutedBlockParts`], borrowing from them when encoding. Receipts travel
/// RLP-encoded, so the form does not depend on the receipt type.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutedBlockResult<'a> {
    /// Transaction senders, in transaction order.
    pub senders: Cow<'a, [Address]>,
    /// RLP-encoded receipts, in transaction order.
    pub receipts: Vec<Bytes>,
    /// EIP-7685 requests.
    pub requests: Cow<'a, Requests>,
    /// Gas used by the block.
    pub gas_used: u64,
    /// Blob gas used by the block.
    pub blob_gas_used: u64,
    /// State changes of the block, with reverts.
    pub state: Cow<'a, BundleState>,
    /// Sorted hashed state of the block.
    pub hashed_state: HashedPostStateSorted<'a>,
    /// Sorted trie updates of the block.
    pub trie_updates: TrieUpdatesSorted<'a>,
}

impl<'a> ExecutedBlockResult<'a> {
    /// The wire form of a block's execution output, senders and trie data.
    pub fn new<R: Receipt>(
        output: &'a BlockExecutionOutput<R>,
        senders: &'a [Address],
        trie: &'a SortedTrieData,
    ) -> Self {
        Self {
            senders: Cow::Borrowed(senders),
            receipts: output
                .result
                .receipts
                .iter()
                .map(|receipt| Bytes::from(alloy_rlp::encode(receipt)))
                .collect(),
            requests: Cow::Borrowed(&output.result.requests),
            gas_used: output.result.gas_used,
            blob_gas_used: output.result.blob_gas_used,
            state: Cow::Borrowed(&output.state),
            hashed_state: trie.hashed_state.as_ref().into(),
            trie_updates: trie.trie_updates.as_ref().into(),
        }
    }

    /// Encodes the result as bincode (standard configuration), like every protocol message.
    pub fn encode(&self) -> Result<Bytes, bincode::error::EncodeError> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).map(Bytes::from)
    }
}

impl ExecutedBlockResult<'static> {
    /// Decodes a result encoded by [`Self::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (result, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        Ok(result)
    }

    /// The parts this result carries, decoding the receipts as `R`.
    pub fn into_parts<R: Receipt>(self) -> Result<ExecutedBlockParts<R>, alloy_rlp::Error> {
        let receipts = self
            .receipts
            .iter()
            .map(|receipt| R::decode(&mut receipt.as_ref()))
            .collect::<Result<_, _>>()?;
        Ok(ExecutedBlockParts {
            output: BlockExecutionOutput {
                result: BlockExecutionResult {
                    receipts,
                    requests: self.requests.into_owned(),
                    gas_used: self.gas_used,
                    blob_gas_used: self.blob_gas_used,
                },
                state: self.state.into_owned(),
            },
            senders: self.senders.into_owned(),
            trie: SortedTrieData::new(
                Arc::new(self.hashed_state.into()),
                Arc::new(self.trie_updates.into()),
            ),
        })
    }
}

/// The encoded execution results of this node's most recently inserted blocks, keyed by block
/// hash, for apply nodes to fetch.
#[derive(Debug, Default)]
pub struct ExecutedBlocks {
    results: Mutex<RecentResults>,
    inserted: Condvar,
}

/// The retained results, oldest first; the blocks whose result is still being encoded; and the
/// highest block number this node has passed: persisted before it started, or inserted since,
/// whether or not its result was kept.
#[derive(Debug, Default)]
pub struct RecentResults {
    by_hash: HashMap<B256, Bytes>,
    order: VecDeque<(B256, u64)>,
    pending: HashSet<B256>,
    newest: Option<u64>,
}

impl RecentResults {
    /// The refusal for missing block `hash`, numbered `number`, that will never be kept, because
    /// a block at or above its number was already passed and `hash` is not being encoded:
    /// [`ReadError::NotRetained`]. `None` while the block may still arrive.
    fn passed(&self, hash: B256, number: u64) -> Option<ReadError> {
        if self.pending.contains(&hash) {
            return None;
        }
        let newest = self.newest.filter(|&newest| newest >= number)?;
        let oldest = self.order.iter().map(|&(_, number)| number).min().unwrap_or(newest);
        Some(ReadError::NotRetained { oldest, newest })
    }
}

impl ExecutedBlocks {
    /// Number of most recent results kept.
    pub const CAPACITY: usize = 64;

    /// Longest time an owner waits for a block it has not inserted yet before answering
    /// `NotAvailable`: an executor usually receives a payload at the same time as its apply nodes
    /// and finishes executing it shortly after they ask.
    pub const SERVE_WAIT: Duration = Duration::from_millis(250);

    /// Largest result kept: a larger one would not fit in a response frame of at most
    /// [`Frame::MAX_LEN`] bytes, which adds a few bytes of framing to the result.
    pub const MAX_RESULT_LEN: usize = Frame::MAX_LEN - 64;

    /// A store for a node whose persisted head is block `persisted`: every block up to it was
    /// inserted before this node started, so a request for a missing one is refused at once.
    pub fn after_persisted(persisted: u64) -> Self {
        let results = RecentResults { newest: Some(persisted), ..RecentResults::default() };
        Self { results: Mutex::new(results), inserted: Condvar::new() }
    }

    /// Notes that the result of block `hash` is being encoded, so requests for it wait for it
    /// even after a later block was passed. [`Self::insert`] or [`Self::pass`] ends the note.
    pub fn expect(&self, hash: B256) {
        self.results.lock().pending.insert(hash);
    }

    /// Notes that block `hash`, numbered `number`, was inserted but its result will not be kept
    /// (dropped or not encodable), so requests for it and older missing blocks are refused at
    /// once.
    pub fn pass(&self, hash: B256, number: u64) {
        let mut results = self.results.lock();
        results.pending.remove(&hash);
        results.newest = results.newest.max(Some(number));
        drop(results);
        self.inserted.notify_all();
    }

    /// Keeps the encoded result of block `hash`, numbered `number`, dropping the oldest beyond
    /// [`Self::CAPACITY`], and wakes requests waiting for it. Returns `false`, keeping nothing
    /// but passing the block (see [`Self::pass`]), when the result is longer than
    /// [`Self::MAX_RESULT_LEN`].
    pub fn insert(&self, hash: B256, number: u64, encoded: Bytes) -> bool {
        if encoded.len() > Self::MAX_RESULT_LEN {
            self.pass(hash, number);
            return false;
        }
        let mut results = self.results.lock();
        results.pending.remove(&hash);
        if results.by_hash.insert(hash, encoded).is_none() {
            results.order.push_back((hash, number));
        }
        results.newest = results.newest.max(Some(number));
        while results.order.len() > Self::CAPACITY {
            if let Some((oldest, _)) = results.order.pop_front() {
                results.by_hash.remove(&oldest);
            }
        }
        drop(results);
        self.inserted.notify_all();
        true
    }

    /// The encoded result of block `hash`, waiting up to `wait` for it to be inserted.
    pub fn wait_for(&self, hash: B256, wait: Duration) -> Option<Bytes> {
        let deadline = Instant::now() + wait;
        let mut results = self.results.lock();
        loop {
            if let Some(result) = results.by_hash.get(&hash) {
                return Some(result.clone());
            }
            if self.inserted.wait_until(&mut results, deadline).timed_out() {
                return results.by_hash.get(&hash).cloned();
            }
        }
    }

    /// The encoded result of block `hash`, numbered `number`. Waits up to `wait` for it only
    /// while it is being encoded or no block numbered `number` or higher has been passed: once
    /// one has, a missing block was evicted, inserted before this node started, dropped, or is
    /// on another fork, and the answer is [`ReadError::NotRetained`] at once. Otherwise a block
    /// still missing after `wait` is [`ReadError::NotAvailable`].
    pub fn serve(&self, hash: B256, number: u64, wait: Duration) -> Result<Bytes, ReadError> {
        let deadline = Instant::now() + wait;
        let mut results = self.results.lock();
        let mut timed_out = false;
        loop {
            if let Some(result) = results.by_hash.get(&hash) {
                return Ok(result.clone());
            }
            if let Some(refusal) = results.passed(hash, number) {
                return Err(refusal);
            }
            if timed_out {
                return Err(ReadError::NotAvailable);
            }
            timed_out = self.inserted.wait_until(&mut results, deadline).timed_out();
        }
    }
}

/// Encodes captured blocks into an [`ExecutedBlocks`] store on one worker thread, in capture
/// order, so a result never evicts one captured after it. At most [`Self::QUEUE`] captured blocks
/// wait; a block captured while the queue is full is dropped, passed in the store (see
/// [`ExecutedBlocks::pass`]) and counted in the metric `base_cluster_dropped_results` (apply
/// nodes asking for it execute instead), so a slow encoder never holds more than that many
/// blocks.
#[derive(Debug)]
pub struct ResultCapture<T> {
    store: Arc<ExecutedBlocks>,
    queue: SyncSender<(B256, u64, T)>,
    stopped: Arc<AtomicBool>,
    worker: JoinHandle<()>,
    dropped: AtomicU64,
    dropped_metric: Counter,
}

impl<T: Send + 'static> ResultCapture<T> {
    /// Captured blocks waiting at most: a result queued behind more than the store keeps would
    /// be evicted as soon as the newer ones are inserted.
    pub const QUEUE: usize = ExecutedBlocks::CAPACITY;

    /// Starts the worker, which turns each captured block into its encoded result with `encode`
    /// and inserts it into `store`; a block that does not encode (`None`) is passed.
    pub fn spawn(
        store: Arc<ExecutedBlocks>,
        mut encode: impl FnMut(T) -> Option<Bytes> + Send + 'static,
    ) -> io::Result<Self> {
        let (queue, captured) = mpsc::sync_channel::<(B256, u64, T)>(Self::QUEUE);
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::clone(&stopped);
        let worker_store = Arc::clone(&store);
        let worker = thread::Builder::new().name("cluster-results".into()).spawn(move || {
            while let Ok((hash, number, block)) = captured.recv() {
                if worker_stopped.load(Ordering::Relaxed) {
                    return;
                }
                let Some(encoded) = encode(block) else {
                    worker_store.pass(hash, number);
                    continue;
                };
                let len = encoded.len();
                if !worker_store.insert(hash, number, encoded) {
                    warn!(block = %hash, len, "execution result too large to serve");
                }
            }
        })?;
        Ok(Self {
            store,
            queue,
            stopped,
            worker,
            dropped: AtomicU64::new(0),
            dropped_metric: metrics::counter!("base_cluster_dropped_results"),
        })
    }

    /// Queues `block`, whose hash is `hash` and number `number`, for encoding. Returns `false`,
    /// dropping, passing and counting it, when the queue is full or the worker has stopped.
    pub fn submit(&self, hash: B256, number: u64, block: T) -> bool {
        self.store.expect(hash);
        match self.queue.try_send((hash, number, block)) {
            Ok(()) => true,
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.store.pass(hash, number);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                self.dropped_metric.increment(1);
                false
            }
        }
    }

    /// Blocks dropped so far.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Stops the worker, discarding the blocks still queued, and waits for it to finish the one
    /// it is encoding. Fails with the worker's panic, if it panicked.
    pub fn stop(self) -> thread::Result<()> {
        self.stopped.store(true, Ordering::Relaxed);
        drop(self.queue);
        self.worker.join()
    }
}
