//! Ahead-of-builder execution; predictions never authorize a commit.
//!
//! Safety rests on owner-thread validation against committed State, not scheduling or forwarding.
//! Identity and environment must match; all recorded account/slot values and balance ranges must
//! admit committed state. Rebasing then preserves the execution's deltas and lifecycle flags,
//! restores committed-prefix storage originals and adds deferred fees. Rejected predictions only
//! cost time. Parent factories must read one immutable parent with authentic code and block hashes.
//! Reset invalidates the entire epoch. Providers are constructed and used on their worker thread.
//! The owner compares the full transaction environment and runs its first ordinary transaction
//! inline to initialize its L1 fee cache. A later committed L1 storage/lifecycle change disables
//! speculation. Use the same production factory and uninspected environment on both sides.
//! Code-by-hash reads verify content hashes; historical block hashes are fixed by the parent.
//! The provider facade is infallible: fallible backends must abort a candidate on failure, never
//! silently fabricate state. Cancellation does not interrupt a provider call; I/O must be bounded.

use std::{
    collections::VecDeque,
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use alloy_consensus::transaction::Recovered;
use alloy_evm::{EvmEnv, FromRecoveredTx};
use alloy_primitives::{
    Address, B256, U256,
    map::{HashMap, HashSet},
};
use base_common_consensus::BaseTxEnvelope;
use revm::{
    Database,
    context::result::ResultAndState,
    database::EmptyDB,
    state::{Account, EvmState, TransactionId},
};

use crate::{
    AtomicSchedule, BaseEvmFactory, BaseHaltReason, BaseSpecId, BaseTransaction, ExecutionStatus,
    Loc, MvMemory, ParallelDatabase, Read, RecordingDb, Store, Value, WorkSignal,
};

/// Recorded execution before committed-state validation and fee rebasing.
#[derive(Debug)]
pub struct SpeculativeResult {
    /// Exact transaction environment executed by the worker.
    pub transaction: BaseTransaction<revm::context::TxEnv>,
    /// Immutable execution environment, checked again at consumption.
    pub environment: EvmEnv<BaseSpecId>,
    /// Immutable parent identity, checked again at consumption.
    pub parent_hash: B256,
    /// EVM output, excluding deferred fees until rebased.
    pub output: ResultAndState<BaseHaltReason>,
    /// All state observations, including L1 fee parameters.
    pub reads: Vec<Read>,
    /// Deferred fee credits, including zero-valued touches.
    pub fees: Vec<(Address, U256)>,
    /// Owner validation-only time, excluding rebase.
    pub read_validation_nanos: u64,
    /// Owner balance/storage/fee rebase time.
    pub rebase_nanos: u64,
    /// Owner database lookups made by validation.
    pub validation_lookups: usize,
}

impl SpeculativeResult {
    /// Validates on the database's owner thread. A false result must be discarded.
    /// Call at most once: successful validation consumes the recorded balance/fee deltas.
    pub fn validate_and_rebase<DB: Database>(&mut self, db: &mut DB) -> Result<bool, DB::Error> {
        let started = Instant::now();
        for read in &self.reads {
            self.validation_lookups += match read {
                Read::Account(..) => 1,
                Read::Slot(..) => 2,
            };
            let valid = match read {
                Read::Account(address, seen, balance) => {
                    let now = db.basic(*address)?;
                    *seen == now.as_ref().map(|info| (info.nonce, info.code_hash))
                        && balance.admits(now.as_ref().map_or(U256::ZERO, |info| info.balance))
                }
                Read::Slot(address, slot, seen) => {
                    db.basic(*address)?;
                    db.storage(*address, *slot)? == *seen
                }
            };
            if !valid {
                self.read_validation_nanos = started.elapsed().as_nanos() as u64;
                return Ok(false);
            }
        }
        self.read_validation_nanos = started.elapsed().as_nanos() as u64;
        let started = Instant::now();
        Self::rebase(&mut self.output.state, &self.reads, &self.fees, db)?;
        self.rebase_nanos = started.elapsed().as_nanos() as u64;
        Ok(true)
    }

    /// Shared rebasing for validator and builder paths, after successful validation.
    pub fn rebase<DB: Database>(
        state: &mut EvmState,
        reads: &[Read],
        fees: &[(Address, U256)],
        db: &mut DB,
    ) -> Result<bool, DB::Error> {
        let mut rebased = false;
        let mut balances = HashSet::new();
        for read in reads {
            if let Read::Account(address, Some(_), balance) = read
                && balances.insert(*address)
            {
                let now = db.basic(*address)?.map_or(U256::ZERO, |info| info.balance);
                if now != balance.seen {
                    rebased = true;
                    if let Some(account) = state.get_mut(address) {
                        account.info.balance =
                            account.info.balance.wrapping_add(now).wrapping_sub(balance.seen);
                    }
                }
            }
        }
        for (address, account) in state.iter_mut() {
            let created = account.is_created();
            db.basic(*address)?;
            for (slot, value) in &mut account.storage {
                value.original_value =
                    if created { U256::ZERO } else { db.storage(*address, *slot)? };
            }
        }
        for (recipient, amount) in fees {
            if !state.contains_key(recipient) {
                let account = db
                    .basic(*recipient)?
                    .map(Account::from)
                    .unwrap_or_else(|| Account::new_not_existing(TransactionId::ZERO));
                state.insert(*recipient, account);
            }
            let account = state.get_mut(recipient).expect("fee recipient inserted");
            account.mark_touch();
            account.info.balance += *amount;
        }
        Ok(rebased)
    }
}

/// Immutable execution epoch. Workers reuse their own provider throughout the epoch.
pub struct SpeculationParent {
    /// Parent identity, checked by the owning executor.
    pub hash: B256,
    /// Fixed block and configuration environments.
    pub env: EvmEnv<BaseSpecId>,
    /// Production EVM configuration.
    pub factory: BaseEvmFactory,
    /// Thread-local provider constructor; providers need not be Send.
    /// Workers reuse one per epoch. A rare owner repair constructs its own reader.
    pub database: Arc<dyn Fn() -> Box<dyn ParallelDatabase> + Send + Sync>,
    overlay: Store<'static>,
}

impl fmt::Debug for SpeculationParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpeculationParent")
            .field("hash", &self.hash)
            .field("env", &self.env)
            .finish_non_exhaustive()
    }
}

impl SpeculationParent {
    /// Starts with an empty committed overlay; publish system changes and deposits before feeding.
    pub fn new(
        hash: B256,
        env: EvmEnv<BaseSpecId>,
        factory: BaseEvmFactory,
        database: Arc<dyn Fn() -> Box<dyn ParallelDatabase> + Send + Sync>,
    ) -> Self {
        static EMPTY: OnceLock<EmptyDB> = OnceLock::new();
        let mut overlay = Store::new(EMPTY.get_or_init(EmptyDB::default));
        overlay.cache_parent_reads();
        Self { hash, env, factory, database, overlay }
    }
}

/// Per-epoch counters. Waste equals executions minus accepted results after workers settle.
#[derive(Debug, Default, Clone, Copy)]
pub struct SpeculatorStats {
    /// Execution attempts, including cancelled attempts once workers settle.
    pub executions: usize,
    /// Results accepted by committed-state validation.
    pub consumed: usize,
    /// Completed results rejected by committed-state validation.
    pub validation_failures: usize,
    /// Requested predictions that were still executing or queued.
    pub not_ready: usize,
    /// Requested identities absent from the prediction.
    pub absent: usize,
    /// Reader invalidations caused by publication, removal or inline commits.
    pub invalidations: usize,
    /// Executions stopped on an ESTIMATE dependency.
    pub blocked: usize,
    /// Store-side owner repairs plus owner-State failures scheduled for worker retry.
    pub frontier_retries: usize,
    /// Takes that waited and exhausted their budget without returning a result.
    pub timeouts: usize,
    /// Owner validation plus rebasing time in nanoseconds.
    pub validation_nanos: u64,
    /// Owner-State validation-only nanoseconds.
    pub read_validation_nanos: u64,
    /// Owner-State rebase nanoseconds.
    pub rebase_nanos: u64,
    /// Owner-State validation database lookups.
    pub validation_lookups: usize,
    /// Store validation nanoseconds within take.
    pub store_validation_nanos: u64,
    /// Store validation observations checked.
    pub store_validation_reads: usize,
    /// Owner repair nanoseconds within take, including provider construction.
    pub repair_nanos: u64,
    /// Feeder transaction clone nanoseconds; results themselves are moved, not cloned.
    pub submit_clone_nanos: u64,
    /// Owner time in take, including queue contention and bounded frontier waits.
    pub take_nanos: u64,
    /// Owner time feeding or replanning the prediction.
    pub submit_nanos: u64,
    /// Owner time in ordinary inline EVM execution.
    pub inline_nanos: u64,
    /// Number of ordinary inline executions.
    pub inline_executions: usize,
    /// Owner time in the normal receipt/state commit path.
    pub commit_nanos: u64,
    /// Legacy queue-wait CSV placeholder (zero); epoch metadata locking is not instrumented.
    pub owner_queue_wait_nanos: u64,
    /// Legacy queue-wait CSV placeholder (zero); workers never lock the queue per transaction.
    pub worker_queue_wait_nanos: u64,
    /// Aggregate execution and prefix-reuse check time, excluding publication/late invalidation.
    pub worker_busy_nanos: u64,
    /// Aggregate worker condition-variable sleep time, including idle prewarm.
    pub worker_idle_nanos: u64,
}

/// Worker counters do not contend with the builder's metadata lock.
#[derive(Debug, Default)]
pub struct SpeculationCounters {
    executions: AtomicUsize,
    blocked: AtomicUsize,
    invalidations: AtomicUsize,
    busy: AtomicU64,
    idle: AtomicU64,
}

/// Fixed-capacity external-commit plan using the ordered engine's atomic claim protocol.
#[derive(Debug)]
pub struct Prediction {
    parent: Arc<SpeculationParent>,
    mv: MvMemory,
    core: AtomicSchedule,
    slots: Vec<OnceLock<SpeculationJob>>,
    tail: AtomicUsize,
    requested: AtomicUsize,
    stop: AtomicBool,
    signal: WorkSignal,
    counters: Arc<SpeculationCounters>,
}

impl Prediction {
    /// Minimum generation size, amortizing rollovers without unbounded reader bitsets.
    pub const MIN_CAPACITY: usize = 1024;
    /// Headroom for incremental submissions.
    pub const GENERATION_WINDOWS: usize = 4;

    /// Invalidates readers after publication, removal or inline commit.
    pub fn invalidate(&self, index: Option<usize>, changed: &[Loc]) {
        fence(Ordering::SeqCst);
        let first = self.core.frontier.load(Ordering::SeqCst);
        let readers = index.map_or_else(
            || self.mv.readers_from(first, changed),
            |index| self.mv.affected_readers(index, changed),
        );
        for reader in readers {
            if reader < first {
                continue;
            }
            let job = self.slots[reader].get().unwrap();
            if job.retired.load(Ordering::SeqCst) {
                continue;
            }
            self.core.invalidate(reader);
            if job.retired.load(Ordering::SeqCst) {
                self.core.status[reader].store(ExecutionStatus::EXECUTED, Ordering::SeqCst);
            }
            self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Removes speculative writes without applying a declined proposal.
    pub fn retire(&self, index: usize) {
        let job = self.slots[index].get().unwrap();
        let mut slot = job.slot.lock().unwrap();
        job.retired.store(true, Ordering::SeqCst);
        self.mv.remove(index, &slot.writes);
        self.invalidate(Some(index), &MvMemory::removed_locations(&slot.writes));
        slot.result = None;
        slot.writes.clear();
        self.core.status[index].store(ExecutionStatus::EXECUTED, Ordering::SeqCst);
        drop(slot);
        self.advance();
    }

    /// Moves past retired positions; only the builder calls this.
    pub fn advance(&self) {
        let mut first = self.core.frontier.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::Acquire);
        while first < tail && self.slots[first].get().unwrap().retired.load(Ordering::SeqCst) {
            first += 1;
        }
        self.core.frontier.store(first, Ordering::SeqCst);
        self.signal.notify_one();
    }
}

/// Immutable candidate identity and per-position publication ownership.
#[derive(Debug)]
pub struct SpeculationJob {
    transaction: Recovered<BaseTxEnvelope>,
    retired: AtomicBool,
    delivered: AtomicBool,
    slot: Mutex<SpeculationSlot>,
}

/// Latest incarnation. Provider I/O never holds this publication lock.
#[derive(Debug, Default)]
pub struct SpeculationSlot {
    result: Option<SpeculativeResult>,
    writes: Vec<(Loc, Value)>,
    reads: Vec<Read>,
}

/// Builder metadata. Workers access it only when entering/leaving a generation.
#[derive(Debug, Default)]
pub struct SpeculatorQueue {
    parent: Option<Arc<SpeculationParent>>,
    prediction: Option<Arc<Prediction>>,
    order: VecDeque<B256>,
    slots: HashMap<B256, usize>,
    requested: Option<B256>,
    stats: SpeculatorStats,
    counters: Arc<SpeculationCounters>,
    active: usize,
    stopped: bool,
}

impl SpeculatorQueue {
    /// Retires a prediction without applying its state.
    pub fn remove(&mut self, hash: B256) {
        if let Some(index) = self.slots.remove(&hash) {
            self.prediction.as_ref().unwrap().retire(index);
        }
        self.order.retain(|item| *item != hash);
        if self.requested == Some(hash) {
            self.requested = None;
        }
    }

    /// Cancels a generation; late workers cannot publish into its replacement.
    pub fn clear(&mut self) {
        if let Some(prediction) = self.prediction.take() {
            prediction.stop.store(true, Ordering::SeqCst);
            prediction.signal.notify();
        }
        self.slots.clear();
        self.order.clear();
        self.requested = None;
    }
}

/// Persistent ordered workers. Admission and Store commits belong exclusively to the builder.
#[derive(Debug)]
pub struct Speculator {
    shared: Arc<(Mutex<SpeculatorQueue>, Condvar, Condvar)>,
    workers: Vec<JoinHandle<()>>,
    /// Bounded frontier wait; zero retains nonblocking inline-fallback behavior.
    pub frontier_wait: Duration,
}

impl Speculator {
    /// Maximum worker wait; provider calls themselves must be bounded by the caller.
    pub const FRONTIER_WAIT: Duration = Duration::from_millis(10);
    /// Cancellation polling interval after bounded owner spinning.
    pub const FRONTIER_POLL: Duration = Duration::from_micros(20);

    /// Starts persistent workers. Zero threads is rejected.
    pub fn new(threads: usize, forwarding: bool) -> std::io::Result<Self> {
        if threads == 0 {
            return Err(std::io::Error::other("speculator requires workers"));
        }
        let mut this = Self {
            shared: Arc::default(),
            workers: Vec::new(),
            frontier_wait: Self::FRONTIER_WAIT,
        };
        for index in 0..threads {
            let shared = Arc::clone(&this.shared);
            this.workers.push(
                std::thread::Builder::new()
                    .name(format!("base-speculator-{index}"))
                    .spawn(move || Self::work(shared, forwarding))?,
            );
        }
        Ok(this)
    }

    /// Cancels outstanding predictions and installs a fresh immutable parent epoch.
    pub fn reset(&self, parent: SpeculationParent) {
        let mut queue = self.shared.0.lock().unwrap();
        queue.clear();
        queue.stats = SpeculatorStats::default();
        queue.counters = Arc::default();
        queue.parent = Some(Arc::new(parent));
        self.shared.1.notify_all();
        self.shared.2.notify_all();
    }

    /// Starts measurement after the untimed prefix/prewarm.
    pub fn reset_owner_timing(&self) {
        let mut queue = self.shared.0.lock().unwrap();
        queue.stats.take_nanos = 0;
        queue.stats.submit_nanos = 0;
        queue.stats.inline_nanos = 0;
        queue.stats.inline_executions = 0;
        queue.stats.commit_nanos = 0;
    }

    /// Records ordinary owner execution or commit time.
    pub fn record_owner_work(&self, inline: Option<Duration>, commit: Duration) {
        let mut queue = self.shared.0.lock().unwrap();
        if let Some(inline) = inline {
            queue.stats.inline_executions += 1;
            queue.stats.inline_nanos += inline.as_nanos() as u64;
        }
        queue.stats.commit_nanos += commit.as_nanos() as u64;
    }

    /// Retains the matching suffix, retires skipped choices and appends into free tail slots.
    pub fn submit(&self, candidates: &[Recovered<BaseTxEnvelope>]) {
        let started = Instant::now();
        let mut queue = self.shared.0.lock().unwrap();
        let Some(parent) = queue.parent.clone() else { return };
        let matching_suffix = queue.order.len() <= candidates.len()
            && queue
                .order
                .iter()
                .copied()
                .eq(candidates.iter().take(queue.order.len()).map(|tx| tx.inner().tx_hash()));
        if !matching_suffix {
            let wanted: HashSet<_> = candidates.iter().map(|tx| tx.inner().tx_hash()).collect();
            let removed: Vec<_> =
                queue.order.iter().copied().filter(|h| !wanted.contains(h)).collect();
            for hash in removed {
                queue.remove(hash);
            }
            let retained: Vec<_> = candidates
                .iter()
                .map(|tx| tx.inner().tx_hash())
                .filter(|hash| queue.slots.contains_key(hash))
                .collect();
            let append_only = candidates
                .iter()
                .take(retained.len())
                .map(|tx| tx.inner().tx_hash())
                .eq(retained.iter().copied());
            if !append_only || !retained.iter().eq(queue.order.iter()) {
                queue.clear();
            }
        }
        if queue.prediction.as_ref().is_some_and(|p| {
            p.tail.load(Ordering::Relaxed) + candidates.len() - queue.order.len() > p.slots.len()
        }) {
            queue.clear();
        }
        if queue.prediction.is_none() {
            let capacity = candidates
                .len()
                .saturating_mul(Prediction::GENERATION_WINDOWS)
                .max(Prediction::MIN_CAPACITY);
            queue.prediction = Some(Arc::new(Prediction {
                parent,
                mv: MvMemory::new(capacity),
                core: AtomicSchedule::new(capacity),
                slots: (0..capacity).map(|_| OnceLock::new()).collect(),
                tail: AtomicUsize::new(0),
                requested: AtomicUsize::new(usize::MAX),
                stop: AtomicBool::new(false),
                signal: WorkSignal::default(),
                counters: Arc::clone(&queue.counters),
            }));
            self.shared.1.notify_all();
        }
        let prediction = Arc::clone(queue.prediction.as_ref().unwrap());
        let retained = queue.order.len();
        for transaction in &candidates[retained..] {
            let hash = transaction.inner().tx_hash();
            if queue.slots.contains_key(&hash) {
                continue;
            }
            let index = prediction.tail.load(Ordering::Relaxed);
            let clone_started = Instant::now();
            let transaction = transaction.clone();
            queue.stats.submit_clone_nanos += clone_started.elapsed().as_nanos() as u64;
            prediction.slots[index]
                .set(SpeculationJob {
                    transaction,
                    retired: AtomicBool::new(false),
                    delivered: AtomicBool::new(false),
                    slot: Mutex::default(),
                })
                .unwrap();
            queue.order.push_back(hash);
            queue.slots.insert(hash, index);
            prediction.tail.store(index + 1, Ordering::Release);
        }
        if candidates.len() > retained {
            prediction.signal.notify_one();
        }
        queue.stats.submit_nanos += started.elapsed().as_nanos() as u64;
    }

    /// Applies only admitted owner state, including fees, inline and system changes.
    pub fn on_commit(&self, hash: B256, state: &EvmState) {
        let mut queue = self.shared.0.lock().unwrap();
        let Some(parent) = &queue.parent else { return };
        if let Some(&index) = queue.slots.get(&hash) {
            let prediction = queue.prediction.as_ref().unwrap();
            let job = prediction.slots[index].get().unwrap();
            let mut slot = job.slot.lock().unwrap();
            let writes = MvMemory::candidate_writes(state, &slot.reads);
            let changed = MvMemory::committed_locations(&parent.overlay, &writes, &slot.writes);
            parent.overlay.apply_state(state, &[]);
            job.retired.store(true, Ordering::SeqCst);
            prediction.mv.remove(index, &slot.writes);
            prediction.invalidate(Some(index), &changed);
            slot.result = None;
            slot.writes.clear();
            prediction.core.status[index].store(ExecutionStatus::EXECUTED, Ordering::SeqCst);
            drop(slot);
            prediction.advance();
            queue.slots.remove(&hash);
            queue.order.retain(|item| *item != hash);
        } else {
            let changed = MvMemory::committed_locations(
                &parent.overlay,
                &MvMemory::candidate_writes(state, &[]),
                &[],
            );
            parent.overlay.apply_state(state, &[]);
            if let Some(prediction) = &queue.prediction {
                prediction.invalidate(None, &changed);
                prediction.signal.notify_one();
            }
        }
        queue.requested = None;
    }

    /// Waits for the frontier, checks Store and repairs invalid work locally.
    /// Store is unchanged until `on_commit`; independent owner-State validation remains mandatory.
    pub fn take(&self, hash: B256, signer: Address) -> Option<SpeculativeResult> {
        let started = Instant::now();
        let (prediction, index) = {
            let mut queue = self.shared.0.lock().unwrap();
            let position = queue.slots.get(&hash).copied();
            let matching = position.is_some_and(|index| {
                queue.prediction.as_ref().unwrap().slots[index].get().unwrap().transaction.signer()
                    == signer
            });
            if !matching {
                queue.stats.absent += 1;
                if let Some(previous) = queue.requested {
                    queue.remove(previous);
                }
                return None;
            }
            while queue.order.front().is_some_and(|front| *front != hash) {
                let skipped = *queue.order.front().unwrap();
                queue.remove(skipped);
            }
            queue.requested = Some(hash);
            (Arc::clone(queue.prediction.as_ref().unwrap()), position.unwrap())
        };
        let job = prediction.slots[index].get().unwrap();
        prediction.requested.store(index, Ordering::SeqCst);
        prediction.core.waiting[index].store(usize::MAX, Ordering::SeqCst);
        if job.delivered.swap(false, Ordering::SeqCst) {
            prediction.core.status[index].store(ExecutionStatus::PENDING, Ordering::SeqCst);
        }
        if prediction.core.status[index].load(Ordering::SeqCst) != ExecutionStatus::EXECUTED {
            prediction.signal.notify_one();
        }
        let mut waited = false;
        let mut spins = 0;
        let mut result = None;
        while !prediction.stop.load(Ordering::SeqCst) {
            if prediction.core.status[index].load(Ordering::SeqCst) == ExecutionStatus::EXECUTED {
                let mut slot = job.slot.lock().unwrap();
                if prediction.core.status[index]
                    .compare_exchange(
                        ExecutionStatus::EXECUTED,
                        ExecutionStatus::TAKEN,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
                {
                    continue;
                }
                job.delivered.store(true, Ordering::SeqCst);
                result = slot.result.take();
                break;
            }
            waited = true;
            if started.elapsed() >= self.frontier_wait {
                break;
            }
            if spins < 16 {
                spins += 1;
                std::hint::spin_loop();
            } else {
                std::thread::park_timeout(Self::FRONTIER_POLL);
            }
        }
        let validation_started = Instant::now();
        let mut checked = 0;
        let valid = result.as_ref().is_none_or(|result| {
            result.reads.iter().all(|read| {
                checked += 1;
                prediction.parent.overlay.is_cached_current(read)
            })
        });
        let validation_nanos = validation_started.elapsed().as_nanos() as u64;
        let mut repair_nanos = 0;
        if !valid && !prediction.stop.load(Ordering::SeqCst) {
            let repair_started = Instant::now();
            prediction.counters.executions.fetch_add(1, Ordering::Relaxed);
            let pre = catch_unwind(AssertUnwindSafe(|| (prediction.parent.database)()));
            result = pre.ok().and_then(|pre| {
                let store = prediction.parent.overlay.fork(pre.as_ref());
                self.shared.0.lock().unwrap().stats.frontier_retries += 1;
                catch_unwind(AssertUnwindSafe(|| {
                    let mut evm = RecordingDb::candidate_evm(
                        &prediction.parent.factory,
                        prediction.parent.env.clone(),
                        &store,
                    );
                    RecordingDb::execute_reusing(
                        &mut evm,
                        prediction.parent.env.clone(),
                        BaseTransaction::from_recovered_tx(job.transaction.inner(), signer),
                        None,
                        false,
                    )
                    .ok()
                    .flatten()
                    .map(|mut result| {
                        result.parent_hash = prediction.parent.hash;
                        result
                    })
                }))
                .ok()
                .flatten()
            });
            repair_nanos = repair_started.elapsed().as_nanos() as u64;
        }
        let mut queue = self.shared.0.lock().unwrap();
        queue.stats.store_validation_nanos += validation_nanos;
        queue.stats.store_validation_reads += checked;
        queue.stats.repair_nanos += repair_nanos;
        queue.stats.not_ready += usize::from(waited);
        queue.stats.timeouts +=
            usize::from(waited && result.is_none() && started.elapsed() >= self.frontier_wait);
        queue.stats.take_nanos += started.elapsed().as_nanos() as u64;
        if prediction.stop.load(Ordering::SeqCst) { None } else { result }
    }

    /// Retries after independent owner-State validation rejects a proposal.
    pub fn retry(&self, hash: B256) {
        let mut queue = self.shared.0.lock().unwrap();
        if let Some(&index) = queue.slots.get(&hash) {
            let prediction = queue.prediction.as_ref().unwrap();
            let job = prediction.slots[index].get().unwrap();
            let mut slot = job.slot.lock().unwrap();
            slot.result = None;
            job.delivered.store(false, Ordering::SeqCst);
            prediction.core.waiting[index].store(usize::MAX, Ordering::SeqCst);
            prediction.core.status[index].store(ExecutionStatus::PENDING, Ordering::SeqCst);
            prediction.signal.notify_one();
            drop(slot);
            queue.stats.frontier_retries += 1;
        }
    }

    /// Epoch identity and environment are part of every result's validity.
    pub fn matches(&self, hash: B256, env: &EvmEnv<BaseSpecId>) -> bool {
        self.shared
            .0
            .lock()
            .unwrap()
            .parent
            .as_ref()
            .is_some_and(|parent| parent.hash == hash && parent.env == *env)
    }

    /// Records mandatory consume-time validation.
    pub fn record_validation(&self, valid: bool, elapsed: Duration, result: &SpeculativeResult) {
        let mut queue = self.shared.0.lock().unwrap();
        if valid {
            queue.stats.consumed += 1;
        } else {
            queue.stats.validation_failures += 1;
        }
        queue.stats.validation_nanos += elapsed.as_nanos() as u64;
        queue.stats.read_validation_nanos += result.read_validation_nanos;
        queue.stats.rebase_nanos += result.rebase_nanos;
        queue.stats.validation_lookups += result.validation_lookups;
    }

    /// Cancels without waiting for provider I/O.
    pub fn cancel(&self) {
        let mut queue = self.shared.0.lock().unwrap();
        queue.parent = None;
        queue.clear();
        self.shared.1.notify_all();
        self.shared.2.notify_all();
    }

    /// Current epoch counters; worker counts settle after cancellation and `wait_idle`.
    pub fn stats(&self) -> SpeculatorStats {
        let queue = self.shared.0.lock().unwrap();
        SpeculatorStats {
            executions: queue.counters.executions.load(Ordering::Relaxed),
            blocked: queue.counters.blocked.load(Ordering::Relaxed),
            invalidations: queue.counters.invalidations.load(Ordering::Relaxed),
            worker_busy_nanos: queue.counters.busy.load(Ordering::Relaxed),
            worker_idle_nanos: queue.counters.idle.load(Ordering::Relaxed),
            ..queue.stats
        }
    }

    /// Bounded diagnostic wait; never required for builder progress.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let started = Instant::now();
        loop {
            let queue = self.shared.0.lock().unwrap();
            let idle = queue.prediction.as_ref().map_or(queue.active == 0, |p| {
                if p.stop.load(Ordering::SeqCst) {
                    return queue.active == 0;
                }
                (0..p.tail.load(Ordering::Acquire)).all(|index| {
                    let job = p.slots[index].get().unwrap();
                    job.retired.load(Ordering::SeqCst)
                        || job.delivered.load(Ordering::SeqCst)
                        || p.core.status[index].load(Ordering::SeqCst) == ExecutionStatus::EXECUTED
                })
            });
            if idle {
                return true;
            }
            drop(queue);
            if started.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    /// Persistent generation loop; factories and execution are panic-contained.
    pub fn work(shared: Arc<(Mutex<SpeculatorQueue>, Condvar, Condvar)>, forwarding: bool) {
        let mut provider: Option<(Arc<SpeculationParent>, Box<dyn ParallelDatabase>)> = None;
        loop {
            let prediction = {
                let mut queue = shared.0.lock().unwrap();
                while queue.prediction.as_ref().is_none_or(|p| p.stop.load(Ordering::SeqCst))
                    && !queue.stopped
                {
                    queue = shared.1.wait(queue).unwrap();
                }
                if queue.stopped {
                    return;
                }
                queue.active += 1;
                Arc::clone(queue.prediction.as_ref().unwrap())
            };
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                if provider
                    .as_ref()
                    .is_none_or(|(epoch, _)| !Arc::ptr_eq(epoch, &prediction.parent))
                {
                    provider =
                        Some((Arc::clone(&prediction.parent), (prediction.parent.database)()));
                }
                Self::work_prediction(
                    &prediction,
                    provider.as_ref().unwrap().1.as_ref(),
                    forwarding,
                );
            }));
            if outcome.is_err() {
                prediction.stop.store(true, Ordering::SeqCst);
                prediction.signal.notify();
                provider = None;
            }
            let mut queue = shared.0.lock().unwrap();
            queue.active -= 1;
            shared.2.notify_all();
        }
    }

    /// Lowest-PENDING CAS scan and per-slot publication, with reusable worker EVMs.
    pub fn work_prediction(prediction: &Prediction, pre: &dyn ParallelDatabase, forwarding: bool) {
        let parent = &prediction.parent;
        let store = parent.overlay.fork(pre);
        let mut evm = RecordingDb::candidate_evm(&parent.factory, parent.env.clone(), &store);
        let mut scans = 0;
        while !prediction.stop.load(Ordering::SeqCst) {
            let generation = prediction.signal.generation();
            let from = prediction.core.frontier.load(Ordering::SeqCst);
            let to = prediction.tail.load(Ordering::Acquire);
            let choice = (from..to).find(|&index| {
                let job = prediction.slots[index].get().unwrap();
                !job.retired.load(Ordering::SeqCst)
                    && !job.delivered.load(Ordering::SeqCst)
                    && prediction.core.claim(index)
            });
            let Some(index) = choice else {
                let idle = Instant::now();
                if scans < 8 {
                    scans += 1;
                    std::hint::spin_loop();
                } else {
                    prediction.signal.wait(generation, &prediction.stop);
                    scans = 0;
                }
                prediction
                    .counters
                    .idle
                    .fetch_add(idle.elapsed().as_nanos() as u64, Ordering::Relaxed);
                continue;
            };
            scans = 0;
            prediction.signal.notify_one();
            let job = prediction.slots[index].get().unwrap();
            let seen = prediction.core.invalidations[index].load(Ordering::SeqCst);
            let frontier = prediction.requested.load(Ordering::SeqCst) == index;
            let previous = job.slot.lock().unwrap().result.take();
            let busy = Instant::now();
            let outcome = previous
                .filter(|result| prediction.mv.reads_visible(&store, index, &result.reads))
                .map_or_else(
                    || {
                        prediction.counters.executions.fetch_add(1, Ordering::Relaxed);
                        let speculative = (forwarding && !frontier).then_some((
                            &prediction.mv,
                            prediction.core.status.as_slice(),
                            index,
                        ));
                        RecordingDb::execute_reusing(
                            &mut evm,
                            parent.env.clone(),
                            BaseTransaction::from_recovered_tx(
                                job.transaction.inner(),
                                job.transaction.signer(),
                            ),
                            speculative,
                            !frontier,
                        )
                        .map(|result| {
                            result.map(|mut result| {
                                result.parent_hash = parent.hash;
                                result
                            })
                        })
                    },
                    |result| Ok(Some(result)),
                );
            prediction.counters.busy.fetch_add(busy.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let affected = prediction.core.invalidations[index].load(Ordering::SeqCst) != seen
                && outcome
                    .as_ref()
                    .ok()
                    .and_then(|result| result.as_ref())
                    .is_some_and(|r| !prediction.mv.reads_visible(&store, index, &r.reads));
            let mut slot = job.slot.lock().unwrap();
            if job.retired.load(Ordering::SeqCst) || prediction.stop.load(Ordering::SeqCst) {
                continue;
            }
            match outcome {
                Err(crate::Blocked(writer)) => {
                    prediction.counters.blocked.fetch_add(1, Ordering::Relaxed);
                    prediction.core.waiting[index].store(writer, Ordering::SeqCst);
                    prediction.core.status[index].store(ExecutionStatus::PENDING, Ordering::SeqCst);
                }
                Ok(result) => {
                    let writes = result
                        .as_ref()
                        .map(|r| MvMemory::candidate_writes(&r.output.state, &r.reads))
                        .unwrap_or_default();
                    if forwarding {
                        let changed = prediction.mv.publish_observed(
                            index,
                            &writes,
                            &slot.writes,
                            result.as_ref().map_or(&[], |r| r.reads.as_slice()),
                        );
                        prediction.invalidate(Some(index), &changed);
                    }
                    slot.reads = result.as_ref().map(|r| r.reads.clone()).unwrap_or_default();
                    slot.result = result;
                    slot.writes = writes;
                    prediction.core.waiting[index].store(usize::MAX, Ordering::SeqCst);
                    prediction.core.status[index].store(
                        if affected { ExecutionStatus::PENDING } else { ExecutionStatus::EXECUTED },
                        Ordering::SeqCst,
                    );
                }
            }
            drop(slot);
            prediction.signal.notify_one();
        }
    }
}

impl Drop for Speculator {
    fn drop(&mut self) {
        {
            let mut queue = self.shared.0.lock().unwrap();
            queue.stopped = true;
            queue.clear();
            self.shared.1.notify_all();
            self.shared.2.notify_all();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
pub use tests::GatedDb;

#[cfg(test)]
mod tests {
    //! The external `DatabaseRef` wrapper gates a read while execution is in flight; automock on
    //! the internal marker trait cannot express this cross-thread cancellation handshake.
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_evm::{
        Evm, EvmFactory,
        block::{BlockExecutor, BlockExecutorFactory},
    };
    use alloy_primitives::{Signature, TxKind};
    use base_common_chains::ChainUpgrades;
    use revm::{
        context::{BlockEnv, CfgEnv, TxEnv},
        database::{CacheDB, EmptyDB},
        state::AccountInfo,
    };

    use super::*;
    use crate::{
        AlloyReceiptBuilder, BaseBlockExecutionCtx, BaseBlockExecutorFactory, BaseTransaction,
        BaseUpgrade,
    };

    /// External provider wrapper that pauses a sender read for cancellation tests.
    #[derive(Debug)]
    pub struct GatedDb {
        base: Box<dyn ParallelDatabase>,
        started: std::sync::mpsc::Sender<()>,
        release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }

    impl revm::DatabaseRef for GatedDb {
        type Error = std::convert::Infallible;

        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            if address == Address::repeat_byte(1)
                && let Some(release) = self.release.lock().unwrap().take()
            {
                self.started.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            self.base.basic_ref(address)
        }

        fn storage_ref(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
            self.base.storage_ref(address, slot)
        }

        fn code_by_hash_ref(&self, hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
            self.base.code_by_hash_ref(hash)
        }

        fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
            self.base.block_hash_ref(number)
        }
    }

    #[test]
    fn panic_stops_waiters_but_idle_wait_still_waits_for_inflight_reads() {
        let workers = Speculator::new(2, true).unwrap();
        let mut epoch = parent();
        let database = Arc::clone(&epoch.database);
        let factories = AtomicUsize::new(0);
        let (started, start) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let released = Mutex::new(Some(released));
        let (panic_now, panic_wait) = std::sync::mpsc::channel();
        let panic_wait = Mutex::new(panic_wait);
        epoch.database = Arc::new(move || {
            if factories.fetch_add(1, Ordering::SeqCst) != 0 {
                panic_wait.lock().unwrap().recv_timeout(Duration::from_secs(5)).unwrap();
                panic!("injected sibling provider failure");
            }
            Box::new(GatedDb {
                base: database(),
                started: started.clone(),
                release: Mutex::new(released.lock().unwrap().take()),
            })
        });
        workers.reset(epoch);
        workers.submit(&[transaction(10), transaction(20)]);
        start.recv_timeout(Duration::from_secs(5)).unwrap();
        panic_now.send(()).unwrap();
        let prematurely_idle = workers.wait_idle(Duration::from_millis(100));
        release.send(()).unwrap();
        assert!(!prematurely_idle, "stopping a generation does not finish provider I/O");
        workers.cancel();
        assert!(workers.wait_idle(Duration::from_secs(5)));
    }

    #[test]
    fn cancellation_counts_executions_that_finish_after_cancel() {
        let workers = Speculator::new(1, true).unwrap();
        let mut epoch = parent();
        let database = Arc::clone(&epoch.database);
        let (started, start) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let released = Mutex::new(Some(released));
        epoch.database = Arc::new(move || {
            Box::new(GatedDb {
                base: database(),
                started: started.clone(),
                release: Mutex::new(released.lock().unwrap().take()),
            })
        });
        workers.reset(epoch);
        workers.submit(&[transaction(10)]);
        start.recv_timeout(Duration::from_secs(5)).unwrap();
        workers.cancel();
        release.send(()).unwrap();
        assert!(workers.wait_idle(Duration::from_secs(5)));
        assert_eq!(workers.stats().executions, 1);
    }

    fn transaction(value: u64) -> Recovered<BaseTxEnvelope> {
        Recovered::new_unchecked(
            BaseTxEnvelope::Legacy(
                TxLegacy {
                    gas_limit: 21_000,
                    value: U256::from(value),
                    to: TxKind::Call(Address::repeat_byte(2)),
                    ..Default::default()
                }
                .into_signed(Signature::new(U256::from(1), U256::from(2), false)),
            ),
            Address::repeat_byte(1),
        )
    }

    fn environment() -> EvmEnv<BaseSpecId> {
        EvmEnv::new(
            CfgEnv::new_with_spec(BaseSpecId::new(BaseUpgrade::Bedrock)),
            BlockEnv { gas_limit: 1_000_000, ..Default::default() },
        )
    }

    fn parent() -> SpeculationParent {
        SpeculationParent::new(
            B256::ZERO,
            environment(),
            BaseEvmFactory::default(),
            Arc::new(|| {
                let mut db = CacheDB::new(EmptyDB::default());
                db.insert_account_info(
                    Address::repeat_byte(1),
                    AccountInfo { balance: U256::from(1_000_000), ..Default::default() },
                );
                Box::new(db)
            }),
        )
    }

    #[test]
    fn take_rejects_a_nonce_invalid_at_the_committed_frontier() {
        let workers = Speculator::new(2, true).unwrap();
        workers.reset(parent());
        let tx = transaction(10);
        let mut sender = Account::from(AccountInfo {
            balance: U256::from(1_000_000),
            nonce: 1,
            ..Default::default()
        });
        sender.mark_touch();
        workers.on_commit(B256::ZERO, &EvmState::from_iter([(tx.signer(), sender)]));
        workers.submit(std::slice::from_ref(&tx));
        assert!(workers.wait_idle(Duration::from_secs(5)));
        assert!(workers.take(tx.inner().tx_hash(), tx.signer()).is_none());
    }

    #[test]
    fn declined_or_cancelled_proposals_never_enter_the_committed_store() {
        for cancel in [false, true] {
            let workers = Speculator::new(2, true).unwrap();
            let epoch = parent();
            let mut owner = CacheDB::new((epoch.database)());
            workers.reset(epoch);
            let first = transaction(10);
            let second = transaction(20);
            workers.submit(&[first.clone(), second.clone()]);
            assert!(workers.wait_idle(Duration::from_secs(5)));
            let proposal = workers.take(first.inner().tx_hash(), first.signer()).unwrap();
            drop(proposal);
            if cancel {
                workers.cancel();
                assert!(workers.take(second.inner().tx_hash(), second.signer()).is_none());
                workers.reset(parent());
            }
            workers.submit(std::slice::from_ref(&second));
            let mut proposal = workers.take(second.inner().tx_hash(), second.signer()).unwrap();
            assert!(proposal.validate_and_rebase(&mut owner).unwrap());
            assert_eq!(proposal.output.state[&second.signer()].info.balance, U256::from(999_980));
        }
    }

    #[test]
    fn take_waits_for_a_pending_prediction() {
        let workers = Speculator::new(1, true).unwrap();
        let mut epoch = parent();
        let database = Arc::clone(&epoch.database);
        epoch.database = Arc::new(move || {
            std::thread::sleep(Duration::from_millis(2));
            database()
        });
        workers.reset(epoch);
        let tx = transaction(10);
        workers.submit(std::slice::from_ref(&tx));
        assert!(workers.take(tx.inner().tx_hash(), tx.signer()).is_some());
    }

    #[test]
    fn frontier_repairs_predictions_after_an_inline_storage_commit() {
        let workers = Speculator::new(2, true).unwrap();
        let mut epoch = parent();
        epoch.database = Arc::new(|| {
            let mut db = CacheDB::new(EmptyDB::default());
            db.insert_account_info(
                Address::repeat_byte(1),
                AccountInfo { balance: U256::from(1_000_000), ..Default::default() },
            );
            db.insert_account_info(
                Address::repeat_byte(2),
                AccountInfo::default().with_code(revm::state::Bytecode::new_raw(
                    alloy_primitives::Bytes::from_static(&[
                        0x60, 0, 0x54, 0x60, 1, 0x01, 0x60, 0, 0x55, 0,
                    ]),
                )),
            );
            Box::new(db)
        });
        let mut owner = CacheDB::new((epoch.database)());
        workers.reset(epoch);
        let tx = Recovered::new_unchecked(
            BaseTxEnvelope::Legacy(
                TxLegacy {
                    gas_limit: 100_000,
                    to: TxKind::Call(Address::repeat_byte(2)),
                    ..Default::default()
                }
                .into_signed(Signature::new(U256::from(1), U256::from(2), false)),
            ),
            Address::repeat_byte(1),
        );
        workers.submit(std::slice::from_ref(&tx));
        assert!(workers.wait_idle(Duration::from_secs(5)));
        let target = Address::repeat_byte(2);
        let mut changed = Account::from(owner.basic(target).unwrap().unwrap());
        changed.mark_touch();
        changed.storage.insert(
            U256::ZERO,
            revm::state::EvmStorageSlot::new_changed(
                U256::ZERO,
                U256::from(7),
                TransactionId::ZERO,
            ),
        );
        let state = EvmState::from_iter([(target, changed)]);
        workers.on_commit(B256::repeat_byte(9), &state);
        revm::DatabaseCommit::commit(&mut owner, state);
        let mut result = workers.take(tx.inner().tx_hash(), tx.signer()).unwrap();
        assert!(result.validate_and_rebase(&mut owner).unwrap());
        assert_eq!(result.output.state[&target].storage[&U256::ZERO].present_value, U256::from(8));
    }

    #[test]
    fn identity_cancel_reset_and_declined_commit_preserve_builder_choices() {
        for forwarding in [false, true] {
            let workers = Arc::new(Speculator::new(2, forwarding).unwrap());
            let first = transaction(10);
            let second = transaction(20);
            workers.reset(parent());
            workers.submit(std::slice::from_ref(&first));
            assert!(workers.wait_idle(Duration::from_secs(5)));
            assert!(workers.take(first.inner().tx_hash(), Address::ZERO).is_none());
            workers.submit(std::slice::from_ref(&first));
            assert!(workers.wait_idle(Duration::from_secs(5)));
            workers.cancel();
            assert!(workers.take(first.inner().tx_hash(), first.signer()).is_none());
            workers.reset(parent());
            workers.submit(std::slice::from_ref(&first));
            assert!(workers.wait_idle(Duration::from_secs(5)));
            workers.reset(parent());
            assert!(workers.take(first.inner().tx_hash(), first.signer()).is_none());

            let mut db = revm::database::State::builder()
                .with_database(CacheDB::new(EmptyDB::default()))
                .with_bundle_update()
                .build();
            db.insert_account(
                first.signer(),
                AccountInfo { balance: U256::from(1_000_000), ..Default::default() },
            );
            let factory = BaseBlockExecutorFactory::new(
                AlloyReceiptBuilder::default(),
                ChainUpgrades::mainnet(),
                BaseEvmFactory::default(),
            );
            let evm = factory.evm_factory().create_evm(&mut db, environment());
            let mut executor = factory.create_executor(
                evm,
                BaseBlockExecutionCtx {
                    speculator: Some(Arc::clone(&workers)),
                    ..Default::default()
                },
            );
            workers.submit(&[first.clone(), second.clone()]);
            assert!(workers.wait_idle(Duration::from_secs(5)));
            let declined = executor.execute_transaction_without_commit(&first).unwrap();
            drop(declined);
            assert!(executor.receipts.is_empty());
            assert!(
                executor
                    .execute_transaction_with_commit_condition(&first, |_| {
                        alloy_evm::block::CommitChanges::No
                    })
                    .unwrap()
                    .is_none()
            );
            assert_eq!(workers.stats().consumed, 1);
            assert!(executor.receipts.is_empty());
            assert_eq!(
                executor.evm.db_mut().basic(first.signer()).unwrap().unwrap().balance,
                U256::from(1_000_000)
            );
            workers.submit(&[first.clone(), second.clone()]);
            assert!(workers.wait_idle(Duration::from_secs(5)));
            let mut altered = BaseTransaction::from_recovered_tx(first.inner(), first.signer());
            altered.base.gas_limit = 1;
            assert!(executor.execute_transaction_without_commit((altered, &first)).is_err());
            assert_eq!(workers.stats().validation_failures, 1);
            workers.submit(&[first.clone(), second.clone()]);
            assert!(workers.wait_idle(Duration::from_secs(5)));
            executor.evm.db_mut().insert_account(
                first.signer(),
                AccountInfo { balance: U256::from(1_000_000), nonce: 1, ..Default::default() },
            );
            assert!(executor.execute_transaction_without_commit(&first).is_err());
            assert_eq!(workers.stats().validation_failures, 3);
            executor.evm.db_mut().insert_account(
                first.signer(),
                AccountInfo { balance: U256::from(1_000_000), ..Default::default() },
            );
            workers.submit(&[]);
            workers.submit(std::slice::from_ref(&second));
            assert!(workers.wait_idle(Duration::from_secs(5)));
            executor.execute_transaction(&second).unwrap();
            assert_eq!(executor.receipts.len(), 1);
            assert_eq!(workers.stats().consumed, 2);
            assert_eq!(
                executor.evm.db_mut().basic(first.signer()).unwrap().unwrap().balance,
                U256::from(999_980)
            );
        }
    }

    #[test]
    fn provider_panic_and_reset_do_not_strand_workers_or_publish_old_work() {
        let workers = Speculator::new(1, false).unwrap();
        let mut broken = parent();
        broken.database = Arc::new(|| panic!("injected provider failure"));
        workers.reset(broken);
        let tx = transaction(10);
        workers.submit(std::slice::from_ref(&tx));
        assert!(workers.wait_idle(Duration::from_secs(5)));
        assert!(workers.take(tx.inner().tx_hash(), tx.signer()).is_none());
        workers.reset(parent());
        workers.submit(std::slice::from_ref(&tx));
        assert!(workers.wait_idle(Duration::from_secs(5)));
        assert!(workers.take(tx.inner().tx_hash(), tx.signer()).is_some());
    }

    #[test]
    fn reset_discards_an_old_epoch_finishing_while_the_new_epoch_is_pending() {
        let workers = Speculator::new(1, false).unwrap();
        let (old_started, old_start) = std::sync::mpsc::channel();
        let (old_release, old_wait) = std::sync::mpsc::channel();
        let old_wait = Mutex::new(old_wait);
        let mut old = parent();
        let old_database = Arc::clone(&old.database);
        old.database = Arc::new(move || {
            old_started.send(()).unwrap();
            old_wait.lock().unwrap().recv_timeout(Duration::from_secs(5)).unwrap();
            old_database()
        });
        let (new_started, new_start) = std::sync::mpsc::channel();
        let (new_release, new_wait) = std::sync::mpsc::channel();
        let new_wait = Mutex::new(new_wait);
        let mut new = parent();
        let new_database = Arc::clone(&new.database);
        new.database = Arc::new(move || {
            new_started.send(()).unwrap();
            new_wait.lock().unwrap().recv_timeout(Duration::from_secs(5)).unwrap();
            new_database()
        });
        let tx = transaction(10);
        workers.reset(old);
        workers.submit(std::slice::from_ref(&tx));
        old_start.recv_timeout(Duration::from_secs(5)).unwrap();
        workers.reset(new);
        workers.submit(std::slice::from_ref(&tx));
        old_release.send(()).unwrap();
        new_start.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(workers.take(tx.inner().tx_hash(), tx.signer()).is_none());
        new_release.send(()).unwrap();
        assert!(workers.wait_idle(Duration::from_secs(5)));
        workers.submit(std::slice::from_ref(&tx));
        assert!(workers.wait_idle(Duration::from_secs(5)));
        assert!(workers.take(tx.inner().tx_hash(), tx.signer()).is_some());
    }

    #[test]
    fn corrupt_code_is_a_terminal_miss_not_an_estimate_dependency() {
        let workers = Speculator::new(1, true).unwrap();
        let mut epoch = parent();
        epoch.database = Arc::new(|| {
            let mut db = CacheDB::new(EmptyDB::default());
            db.insert_account_info(
                Address::repeat_byte(1),
                AccountInfo { balance: U256::from(1_000_000), ..Default::default() },
            );
            db.insert_account_info(
                Address::repeat_byte(2),
                AccountInfo { code_hash: B256::repeat_byte(9), code: None, ..Default::default() },
            );
            Box::new(db)
        });
        workers.reset(epoch);
        let tx = transaction(10);
        workers.submit(std::slice::from_ref(&tx));
        assert!(workers.wait_idle(Duration::from_secs(1)));
        assert!(workers.take(tx.inner().tx_hash(), tx.signer()).is_none());
    }

    #[test]
    fn unsupported_transactions_never_produce_candidates() {
        let pre = (parent().database)();
        let store = Store::new(pre.as_ref());
        let ordinary =
            BaseTransaction::from_recovered_tx(transaction(10).inner(), Address::repeat_byte(1));
        for tx_type in [crate::DEPOSIT_TRANSACTION_TYPE, crate::EIP8130_TRANSACTION_TYPE] {
            let mut tx = ordinary.clone();
            tx.base.tx_type = tx_type;
            assert!(
                RecordingDb::execute_candidate(
                    &BaseEvmFactory::default(),
                    environment(),
                    &store,
                    tx,
                    None
                )
                .is_none()
            );
        }
    }

    #[test]
    fn committed_validation_rebases_fees_and_rejects_nonce_and_storage_drift() {
        let sender = Address::repeat_byte(1);
        let target = Address::repeat_byte(2);
        let mut pre = CacheDB::new(EmptyDB::default());
        pre.insert_account_info(
            sender,
            AccountInfo { balance: U256::from(1_000_000), nonce: 1, ..Default::default() },
        );
        let env = EvmEnv::new(
            CfgEnv::new_with_spec(BaseSpecId::new(BaseUpgrade::Bedrock)),
            BlockEnv { gas_limit: 1_000_000, ..Default::default() },
        );
        let tx = BaseTransaction {
            base: TxEnv {
                caller: sender,
                nonce: 1,
                gas_limit: 21_000,
                kind: TxKind::Call(target),
                value: U256::from(10),
                ..Default::default()
            },
            enveloped_tx: Some(Default::default()),
            ..Default::default()
        };
        let store = Store::new(&pre);
        let mut result = RecordingDb::execute_candidate(
            &BaseEvmFactory::default(),
            env.clone(),
            &store,
            tx,
            None,
        )
        .unwrap();
        drop(store);
        let duplicate = result
            .reads
            .iter()
            .find(|read| matches!(read, Read::Account(address, ..) if *address == sender))
            .unwrap()
            .clone();
        result.reads.push(duplicate);
        pre.insert_account_info(
            sender,
            AccountInfo { balance: U256::from(2_000_000), nonce: 1, ..Default::default() },
        );
        assert!(result.validate_and_rebase(&mut pre).unwrap());
        assert_eq!(result.output.state[&sender].info.balance, U256::from(1_999_990));
        assert!(result.output.state.contains_key(&env.block_env.beneficiary));
        pre.insert_account_info(
            sender,
            AccountInfo { balance: U256::from(2_000_000), nonce: 2, ..Default::default() },
        );
        assert!(!result.validate_and_rebase(&mut pre).unwrap());
        result.reads = vec![Read::Slot(target, U256::ZERO, U256::from(7))];
        assert!(!result.validate_and_rebase(&mut pre).unwrap());
    }
}
