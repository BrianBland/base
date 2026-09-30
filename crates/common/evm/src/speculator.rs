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
        atomic::{AtomicU8, Ordering, fence},
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
    BaseEvmFactory, BaseHaltReason, BaseSpecId, BaseTransaction, ExecutionStatus, Loc, MvMemory,
    ParallelDatabase, Read, RecordingDb, Store, Value,
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
}

impl SpeculativeResult {
    /// Validates on the database's owner thread. A false result must be discarded.
    /// Call at most once: successful validation consumes the recorded balance/fee deltas.
    pub fn validate_and_rebase<DB: Database>(&mut self, db: &mut DB) -> Result<bool, DB::Error> {
        for read in &self.reads {
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
                return Ok(false);
            }
        }
        Self::rebase(&mut self.output.state, &self.reads, &self.fees, db)?;
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

/// Immutable execution epoch. The provider factory runs on each worker, once per epoch.
pub struct SpeculationParent {
    /// Parent identity, checked by the owning executor.
    pub hash: B256,
    /// Fixed block and configuration environments.
    pub env: EvmEnv<BaseSpecId>,
    /// Production EVM configuration.
    pub factory: BaseEvmFactory,
    /// Worker-local provider constructor; providers need not be Send.
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
        Self {
            hash,
            env,
            factory,
            database,
            overlay: Store::new(EMPTY.get_or_init(EmptyDB::default)),
        }
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
    /// Owner validation failures scheduled for a worker frontier retry.
    pub frontier_retries: usize,
    /// Bounded frontier waits that expired.
    pub timeouts: usize,
    /// Owner validation plus rebasing time in nanoseconds.
    pub validation_nanos: u64,
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
    /// Owner wall time waiting to acquire the scheduler queue mutex.
    pub owner_queue_wait_nanos: u64,
    /// Worker wall time waiting to acquire the scheduler queue mutex.
    pub worker_queue_wait_nanos: u64,
    /// Aggregate worker time in execution or read revalidation, excluding publication.
    pub worker_busy_nanos: u64,
    /// Aggregate worker condition-variable sleep time, including idle prewarm.
    pub worker_idle_nanos: u64,
}

/// One bounded, append-only generation of predicted positions.
#[derive(Debug)]
pub struct Prediction {
    parent: Arc<SpeculationParent>,
    mv: MvMemory,
    status: Vec<AtomicU8>,
}

impl Prediction {
    /// Minimum generation size, amortizing rollovers without unbounded reader bitsets.
    pub const MIN_CAPACITY: usize = 1024;
    /// Number of initially submitted windows that fit before a larger generation rolls over.
    pub const GENERATION_WINDOWS: usize = 4;
}

/// Candidate identity and its stable predicted position.
#[derive(Debug)]
pub struct SpeculationJob {
    prediction: Arc<Prediction>,
    transaction: Recovered<BaseTxEnvelope>,
    index: usize,
}

/// One prediction and its latest incarnation.
#[derive(Debug)]
pub struct SpeculationSlot {
    job: Arc<SpeculationJob>,
    result: Option<SpeculativeResult>,
    writes: Vec<(Loc, Value)>,
    reads: Vec<Read>,
    invalidations: usize,
    waiting: Option<usize>,
    delivered: bool,
}

/// Builder-controlled ordered scheduler. All publication and retirement holds this lock;
/// EVM execution and provider reads never hold it.
#[derive(Debug, Default)]
pub struct SpeculatorQueue {
    parent: Option<Arc<SpeculationParent>>,
    stats_parent: Option<Arc<SpeculationParent>>,
    prediction: Option<Arc<Prediction>>,
    order: VecDeque<B256>,
    slots: HashMap<B256, SpeculationSlot>,
    positions: Vec<Option<B256>>,
    next: usize,
    requested: Option<B256>,
    stats: SpeculatorStats,
    active: usize,
    stopped: bool,
}

impl SpeculatorQueue {
    /// Acquires the scheduler lock and measures contention separately from execution.
    pub fn lock(
        shared: &(Mutex<Self>, Condvar, Condvar),
        worker: bool,
    ) -> std::sync::MutexGuard<'_, Self> {
        let started = Instant::now();
        let mut queue = shared.0.lock().unwrap();
        let elapsed = started.elapsed().as_nanos() as u64;
        if worker {
            queue.stats.worker_queue_wait_nanos += elapsed;
        } else {
            queue.stats.owner_queue_wait_nanos += elapsed;
        }
        queue
    }

    /// Marks registered higher readers pending, including incarnations currently executing.
    pub fn invalidate(&mut self, prediction: &Prediction, index: usize, changed: &[Loc]) {
        fence(Ordering::SeqCst);
        for reader in prediction.mv.affected_readers(index, changed) {
            if let Some(hash) = self.positions.get(reader).copied().flatten()
                && let Some(slot) = self.slots.get_mut(&hash)
            {
                slot.invalidations += 1;
                self.stats.invalidations += 1;
                let _ = prediction.status[reader].compare_exchange(
                    ExecutionStatus::EXECUTED,
                    ExecutionStatus::PENDING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                slot.waiting = None;
            }
        }
    }

    /// Retires a prediction without applying any of its state.
    pub fn remove(&mut self, hash: B256) {
        if let Some(slot) = self.slots.remove(&hash) {
            self.positions[slot.job.index] = None;
            let prediction = &slot.job.prediction;
            prediction.mv.remove(slot.job.index, &slot.writes);
            prediction.status[slot.job.index].store(ExecutionStatus::EXECUTED, Ordering::SeqCst);
            self.invalidate(prediction, slot.job.index, &MvMemory::removed_locations(&slot.writes));
        }
        self.order.retain(|item| *item != hash);
        if self.requested == Some(hash) {
            self.requested = None;
        }
    }

    /// Discards a generation; late workers cannot publish into its replacement.
    pub fn clear(&mut self) {
        self.slots.clear();
        self.order.clear();
        self.positions.clear();
        self.prediction = None;
        self.requested = None;
        self.next = 0;
    }
}

/// Persistent ordered workers. The owner remains the only authority for admission and commit.
#[derive(Debug)]
pub struct Speculator {
    shared: Arc<(Mutex<SpeculatorQueue>, Condvar, Condvar)>,
    workers: Vec<JoinHandle<()>>,
    /// Bounded frontier wait; zero retains nonblocking inline-fallback behavior.
    pub frontier_wait: Duration,
}

impl Speculator {
    /// Maximum time one take waits for workers; a timeout permits ordinary inline execution.
    pub const FRONTIER_WAIT: Duration = Duration::from_millis(10);

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
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        queue.clear();
        queue.stats = SpeculatorStats::default();
        let parent = Arc::new(parent);
        queue.stats_parent = Some(Arc::clone(&parent));
        queue.parent = Some(parent);
        self.shared.1.notify_all();
        self.shared.2.notify_all();
    }

    /// Starts the measured builder choice loop after its untimed prefix/prewarm.
    pub fn reset_owner_timing(&self) {
        let mut queue = self.shared.0.lock().unwrap();
        queue.stats.take_nanos = 0;
        queue.stats.submit_nanos = 0;
        queue.stats.inline_nanos = 0;
        queue.stats.inline_executions = 0;
        queue.stats.commit_nanos = 0;
        queue.stats.owner_queue_wait_nanos = 0;
    }

    /// Records ordinary owner execution or commit time without changing engine state.
    pub fn record_owner_work(&self, inline: Option<Duration>, commit: Duration) {
        let mut queue = self.shared.0.lock().unwrap();
        if let Some(inline) = inline {
            queue.stats.inline_executions += 1;
            queue.stats.inline_nanos += inline.as_nanos() as u64;
        }
        queue.stats.commit_nanos += commit.as_nanos() as u64;
    }

    /// Retains a matching suffix, removes skipped choices and appends newly visible candidates.
    /// Reordering or exhausting the bounded generation starts a new advisory plan.
    pub fn submit(&self, candidates: &[Recovered<BaseTxEnvelope>]) {
        let started = Instant::now();
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        let Some(parent) = queue.parent.clone() else { return };
        let wanted: HashSet<_> = candidates.iter().map(|tx| tx.inner().tx_hash()).collect();
        let removed: Vec<_> = queue.order.iter().copied().filter(|h| !wanted.contains(h)).collect();
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
        if !append_only
            || !retained.iter().eq(queue.order.iter())
            || queue
                .prediction
                .as_ref()
                .is_some_and(|p| queue.next + candidates.len() - retained.len() > p.status.len())
        {
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
                status: (0..capacity).map(|_| AtomicU8::new(ExecutionStatus::PENDING)).collect(),
            }));
        }
        let prediction = Arc::clone(queue.prediction.as_ref().unwrap());
        for transaction in candidates {
            let hash = transaction.inner().tx_hash();
            if queue.slots.contains_key(&hash) {
                continue;
            }
            let index = queue.next;
            queue.next += 1;
            let job = Arc::new(SpeculationJob {
                prediction: Arc::clone(&prediction),
                transaction: transaction.clone(),
                index,
            });
            queue.order.push_back(hash);
            queue.positions.push(Some(hash));
            queue.slots.insert(
                hash,
                SpeculationSlot {
                    job,
                    result: None,
                    writes: Vec::new(),
                    reads: Vec::new(),
                    invalidations: 0,
                    waiting: None,
                    delivered: false,
                },
            );
        }
        queue.stats.submit_nanos += started.elapsed().as_nanos() as u64;
        drop(queue);
        self.shared.1.notify_one();
        self.shared.2.notify_all();
    }

    /// Publishes the owner's actual commit, including fees, inline execution and system changes.
    pub fn on_commit(&self, tx_hash: B256, state: &EvmState) {
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        if let Some(slot) = queue.slots.remove(&tx_hash) {
            let prediction = &slot.job.prediction;
            let writes = MvMemory::candidate_writes(state, &slot.reads);
            queue.positions[slot.job.index] = None;
            let changed =
                MvMemory::committed_locations(&prediction.parent.overlay, &writes, &slot.writes);
            prediction.parent.overlay.apply_state(state, &[]);
            prediction.mv.remove(slot.job.index, &slot.writes);
            prediction.status[slot.job.index].store(ExecutionStatus::EXECUTED, Ordering::SeqCst);
            queue.invalidate(prediction, slot.job.index, &changed);
            queue.order.retain(|hash| *hash != tx_hash);
        } else {
            if let Some(parent) = &queue.parent {
                parent.overlay.apply_state(state, &[]);
            }
            for slot in queue.slots.values_mut() {
                slot.invalidations += 1;
                slot.waiting = None;
                let _ = slot.job.prediction.status[slot.job.index].compare_exchange(
                    ExecutionStatus::EXECUTED,
                    ExecutionStatus::PENDING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
        queue.requested = None;
        drop(queue);
        self.shared.1.notify_one();
        self.shared.2.notify_all();
    }

    /// Bounded, cancellation-aware extraction of the builder's chosen frontier.
    /// Results must still pass owner-state validation; timeout and identity mismatch miss inline.
    pub fn take(&self, hash: B256, signer: Address) -> Option<SpeculativeResult> {
        let started = Instant::now();
        let until = started + self.frontier_wait;
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        if queue.slots.get(&hash).is_none_or(|slot| slot.job.transaction.signer() != signer) {
            queue.stats.take_nanos += started.elapsed().as_nanos() as u64;
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
        let job = Arc::clone(&queue.slots[&hash].job);
        if queue.slots[&hash].delivered {
            queue.slots.get_mut(&hash).unwrap().delivered = false;
            job.prediction.status[job.index].store(ExecutionStatus::PENDING, Ordering::SeqCst);
        }
        queue.requested = Some(hash);
        queue.slots.get_mut(&hash).unwrap().waiting = None;
        let mut waited = false;
        loop {
            let slot = queue.slots.get_mut(&hash)?;
            if !Arc::ptr_eq(&job, &slot.job) {
                return None;
            }
            if job.prediction.status[job.index].load(Ordering::SeqCst) == ExecutionStatus::EXECUTED
            {
                slot.delivered = true;
                let result = slot.result.take();
                queue.stats.take_nanos += started.elapsed().as_nanos() as u64;
                return result;
            }
            if !waited {
                queue.stats.not_ready += 1;
                waited = true;
                self.shared.1.notify_one();
            }
            let Some(remaining) = until.checked_duration_since(Instant::now()) else {
                queue.stats.take_nanos += started.elapsed().as_nanos() as u64;
                queue.stats.timeouts += 1;
                return None;
            };
            queue = self.shared.2.wait_timeout(queue, remaining).unwrap().0;
        }
    }

    /// Schedules a failed owner validation for exact-prefix execution on the next idle worker.
    pub fn retry(&self, hash: B256) {
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        if let Some(slot) = queue.slots.get_mut(&hash) {
            slot.result = None;
            slot.delivered = false;
            slot.waiting = None;
            slot.job.prediction.status[slot.job.index]
                .store(ExecutionStatus::PENDING, Ordering::SeqCst);
            queue.stats.frontier_retries += 1;
            drop(queue);
            self.shared.1.notify_one();
        }
    }

    /// Epoch identity and execution environment are part of every result's validity.
    pub fn matches(&self, hash: B256, env: &EvmEnv<BaseSpecId>) -> bool {
        self.shared
            .0
            .lock()
            .unwrap()
            .parent
            .as_ref()
            .is_some_and(|parent| parent.hash == hash && parent.env == *env)
    }

    /// Records the consume-time validation decision.
    pub fn record_validation(&self, valid: bool, elapsed: Duration) {
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        if valid {
            queue.stats.consumed += 1;
        } else {
            queue.stats.validation_failures += 1;
        }
        queue.stats.validation_nanos += elapsed.as_nanos() as u64;
    }

    /// Cancels without waiting for provider I/O; waiting owners wake immediately.
    pub fn cancel(&self) {
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        queue.parent = None;
        queue.clear();
        self.shared.1.notify_all();
        self.shared.2.notify_all();
    }

    /// Current epoch counters; execution counts become stable once idle.
    pub fn stats(&self) -> SpeculatorStats {
        self.shared.0.lock().unwrap().stats
    }

    /// Bounded diagnostic wait for scheduled executions to settle.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let until = Instant::now() + timeout;
        let mut queue = SpeculatorQueue::lock(&self.shared, false);
        while queue.active != 0
            || queue.slots.values().any(|slot| {
                !slot.delivered
                    && slot.job.prediction.status[slot.job.index].load(Ordering::SeqCst)
                        != ExecutionStatus::EXECUTED
            })
        {
            let Some(remaining) = until.checked_duration_since(Instant::now()) else {
                return false;
            };
            queue = self.shared.2.wait_timeout(queue, remaining).unwrap().0;
        }
        true
    }

    /// Worker-side execution, forwarding, ESTIMATE blocking and incarnation invalidation.
    pub fn work(shared: Arc<(Mutex<SpeculatorQueue>, Condvar, Condvar)>, forwarding: bool) {
        let mut provider: Option<(Arc<SpeculationParent>, Box<dyn ParallelDatabase>)> = None;
        loop {
            let prediction = {
                let mut queue = SpeculatorQueue::lock(&shared, true);
                while queue.prediction.is_none() && !queue.stopped {
                    queue = shared.1.wait(queue).unwrap();
                }
                if queue.stopped {
                    return;
                }
                Arc::clone(queue.prediction.as_ref().unwrap())
            };
            let parent = &prediction.parent;
            if provider.as_ref().is_none_or(|(epoch, _)| !Arc::ptr_eq(epoch, parent)) {
                let created = catch_unwind(AssertUnwindSafe(|| (parent.database)()));
                let Ok(database) = created else {
                    let mut queue = SpeculatorQueue::lock(&shared, true);
                    if queue.prediction.as_ref().is_some_and(|p| Arc::ptr_eq(p, &prediction)) {
                        queue.clear();
                    }
                    shared.1.notify_all();
                    shared.2.notify_all();
                    continue;
                };
                provider = Some((Arc::clone(parent), database));
            }
            Self::work_prediction(
                &shared,
                &prediction,
                provider.as_ref().unwrap().1.as_ref(),
                forwarding,
            );
        }
    }

    /// Reuses one EVM and worker-local database across a bounded prediction generation.
    pub fn work_prediction(
        shared: &Arc<(Mutex<SpeculatorQueue>, Condvar, Condvar)>,
        prediction: &Arc<Prediction>,
        pre: &dyn ParallelDatabase,
        forwarding: bool,
    ) {
        let parent = &prediction.parent;
        let store = parent.overlay.fork(pre);
        let mut evm = None;
        loop {
            let (job, seen, frontier, previous_result) = {
                let mut queue = SpeculatorQueue::lock(shared, true);
                loop {
                    if queue.stopped
                        || queue.prediction.as_ref().is_none_or(|p| !Arc::ptr_eq(p, prediction))
                    {
                        return;
                    }
                    let choice = queue.order.iter().find_map(|hash| {
                        let slot = &queue.slots[hash];
                        let status = &slot.job.prediction.status;
                        (!slot.delivered
                            && status[slot.job.index].load(Ordering::SeqCst)
                                == ExecutionStatus::PENDING
                            && slot.waiting.is_none_or(|dep| {
                                status[dep].load(Ordering::SeqCst) == ExecutionStatus::EXECUTED
                            }))
                        .then_some(*hash)
                    });
                    if let Some(hash) = choice {
                        let frontier = queue.requested == Some(hash);
                        let slot = queue.slots.get_mut(&hash).unwrap();
                        let job = Arc::clone(&slot.job);
                        job.prediction.status[job.index]
                            .store(ExecutionStatus::EXECUTING, Ordering::SeqCst);
                        let seen = slot.invalidations;
                        let result = slot.result.take();
                        queue.active += 1;
                        shared.1.notify_one();
                        break (job, seen, frontier, result);
                    }
                    let idle_started = Instant::now();
                    queue = shared.1.wait(queue).unwrap();
                    if queue.prediction.as_ref().is_some_and(|p| Arc::ptr_eq(p, prediction)) {
                        queue.stats.worker_idle_nanos += idle_started.elapsed().as_nanos() as u64;
                    }
                }
            };
            let busy_started = Instant::now();
            let mut executed = false;
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                if let Some(result) = previous_result
                    && job.prediction.mv.reads_visible(&store, job.index, &result.reads)
                {
                    return Ok(Some(result));
                }
                let forwarding = (forwarding && !frontier).then_some((
                    &prediction.mv,
                    prediction.status.as_slice(),
                    job.index,
                ));
                executed = true;
                let evm = evm.get_or_insert_with(|| {
                    RecordingDb::candidate_evm(&parent.factory, parent.env.clone(), &store)
                });
                let mut result = RecordingDb::execute_reusing(
                    evm,
                    parent.env.clone(),
                    BaseTransaction::from_recovered_tx(
                        job.transaction.inner(),
                        job.transaction.signer(),
                    ),
                    forwarding,
                    !frontier,
                )?;
                if let Some(result) = &mut result {
                    result.parent_hash = parent.hash;
                }
                Ok(result)
            }))
            .unwrap_or_else(|_| {
                evm = None;
                Ok(None)
            });
            let busy_nanos = busy_started.elapsed().as_nanos() as u64;
            let mut queue = SpeculatorQueue::lock(shared, true);
            queue.active -= 1;
            if queue.stats_parent.as_ref().is_some_and(|p| Arc::ptr_eq(p, &job.prediction.parent)) {
                queue.stats.executions += usize::from(executed);
                queue.stats.worker_busy_nanos += busy_nanos;
            }
            let hash = job.transaction.inner().tx_hash();
            if queue.slots.get(&hash).is_none_or(|slot| !Arc::ptr_eq(&slot.job, &job)) {
                shared.2.notify_all();
                continue;
            }
            match outcome {
                Err(crate::Blocked(writer)) => {
                    queue.stats.blocked += 1;
                    queue.slots.get_mut(&hash).unwrap().waiting = Some(writer);
                    job.prediction.status[job.index]
                        .store(ExecutionStatus::PENDING, Ordering::SeqCst);
                }
                Ok(result) => {
                    let writes = result
                        .as_ref()
                        .map(|r| MvMemory::candidate_writes(&r.output.state, &r.reads))
                        .unwrap_or_default();
                    let slot = &queue.slots[&hash];
                    let changed = if forwarding {
                        job.prediction.mv.publish_observed(
                            job.index,
                            &writes,
                            &slot.writes,
                            result.as_ref().map_or(&[], |r| r.reads.as_slice()),
                        )
                    } else {
                        Vec::new()
                    };
                    let affected = slot.invalidations != seen && result.is_some();
                    queue.invalidate(&job.prediction, job.index, &changed);
                    let slot = queue.slots.get_mut(&hash).unwrap();
                    slot.reads = result.as_ref().map(|r| r.reads.clone()).unwrap_or_default();
                    slot.result = result;
                    slot.writes = writes;
                    slot.waiting = None;
                    job.prediction.status[job.index].store(
                        if affected { ExecutionStatus::PENDING } else { ExecutionStatus::EXECUTED },
                        Ordering::SeqCst,
                    );
                }
            }
            drop(queue);
            shared.2.notify_all();
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
