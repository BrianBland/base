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

use crate::{
    BaseEvmFactory, BaseHaltReason, BaseSpecId, BaseTransaction, MvMemory, ParallelDatabase, Read,
    RecordingDb, Store,
};
use alloy_consensus::transaction::Recovered;
use alloy_evm::{EvmEnv, FromRecoveredTx};
use alloy_primitives::{Address, B256, U256};
use base_common_consensus::BaseTxEnvelope;
use revm::{
    Database,
    context::result::ResultAndState,
    database::EmptyDB,
    state::{Account, EvmState, TransactionId},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, OnceLock, atomic::AtomicU8},
    thread::JoinHandle,
    time::{Duration, Instant},
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
    /// Started speculative executions.
    pub executions: usize,
    /// Results accepted by committed-state validation.
    pub consumed: usize,
    /// Completed results rejected by committed-state validation.
    pub validation_failures: usize,
}

/// One advisory predicted order shared by its worker jobs.
#[derive(Debug)]
pub struct Prediction {
    parent: Arc<SpeculationParent>,
    mv: MvMemory,
    status: Vec<AtomicU8>,
    forwarding: bool,
}

/// Queued candidate identity and its predicted position.
#[derive(Debug)]
pub struct SpeculationJob {
    prediction: Arc<Prediction>,
    transaction: Recovered<BaseTxEnvelope>,
    index: usize,
}

/// Pending or completed work for a transaction identity.
#[derive(Debug)]
pub struct SpeculationSlot {
    job: Arc<SpeculationJob>,
    result: Option<SpeculativeResult>,
}

/// Scheduler state protected by the work condition variable.
#[derive(Debug, Default)]
pub struct SpeculatorQueue {
    parent: Option<Arc<SpeculationParent>>,
    jobs: VecDeque<Arc<SpeculationJob>>,
    slots: HashMap<B256, SpeculationSlot>,
    stats: SpeculatorStats,
    active: usize,
    stopped: bool,
}

/// Persistent ahead-of-builder workers. Drop joins workers; providers must bound their I/O.
#[derive(Debug)]
pub struct Speculator {
    shared: Arc<(Mutex<SpeculatorQueue>, Condvar)>,
    workers: Vec<JoinHandle<()>>,
    forwarding: bool,
}

impl Speculator {
    /// Starts persistent workers. Zero threads is rejected.
    pub fn new(threads: usize, forwarding: bool) -> std::io::Result<Self> {
        if threads == 0 {
            return Err(std::io::Error::other("speculator requires workers"));
        }
        let mut this = Self { shared: Arc::default(), workers: Vec::new(), forwarding };
        for index in 0..threads {
            let shared = Arc::clone(&this.shared);
            this.workers.push(
                std::thread::Builder::new()
                    .name(format!("base-speculator-{index}"))
                    .spawn(move || Self::work(shared))?,
            );
        }
        Ok(this)
    }

    /// Cancels outstanding predictions and installs a fresh immutable parent epoch.
    pub fn reset(&self, parent: SpeculationParent) {
        let mut queue = self.shared.0.lock().unwrap();
        queue.jobs.clear();
        queue.slots.clear();
        queue.stats = SpeculatorStats::default();
        queue.parent = Some(Arc::new(parent));
        self.shared.1.notify_all();
    }

    /// Replaces the ordered lookahead window, retaining matching in-flight and completed work.
    pub fn submit(&self, candidates: &[Recovered<BaseTxEnvelope>]) {
        let mut queue = self.shared.0.lock().unwrap();
        let Some(parent) = queue.parent.clone() else { return };
        let wanted: HashSet<_> = candidates.iter().map(|tx| tx.inner().tx_hash()).collect();
        queue.slots.retain(|hash, _| wanted.contains(hash));
        queue.jobs.retain(|job| wanted.contains(&job.transaction.inner().tx_hash()));
        let prediction = Arc::new(Prediction {
            parent,
            mv: MvMemory::new(candidates.len()),
            status: (0..candidates.len()).map(|_| AtomicU8::new(2)).collect(),
            forwarding: self.forwarding,
        });
        for (index, transaction) in candidates.iter().enumerate() {
            let hash = transaction.inner().tx_hash();
            if queue.slots.contains_key(&hash) {
                continue;
            }
            let job = Arc::new(SpeculationJob {
                prediction: Arc::clone(&prediction),
                transaction: transaction.clone(),
                index,
            });
            queue.slots.insert(hash, SpeculationSlot { job: Arc::clone(&job), result: None });
            queue.jobs.push_back(job);
        }
        self.shared.1.notify_all();
    }

    /// Publishes only actual committed changes. A refused commit must not call this method.
    pub fn on_commit(&self, tx_hash: B256, state: &EvmState) {
        let mut queue = self.shared.0.lock().unwrap();
        if let Some(parent) = &queue.parent {
            parent.overlay.apply_state(state, &[]);
        }
        queue.slots.remove(&tx_hash);
        queue.jobs.retain(|job| job.transaction.inner().tx_hash() != tx_hash);
    }

    /// Nonblocking extraction; absence or signer mismatch is an ordinary inline miss.
    pub fn take(&self, hash: B256, signer: Address) -> Option<SpeculativeResult> {
        let mut queue = self.shared.0.lock().unwrap();
        let slot = queue.slots.remove(&hash)?;
        (slot.job.transaction.signer() == signer).then_some(slot.result).flatten()
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
    pub fn record_validation(&self, valid: bool) {
        let mut queue = self.shared.0.lock().unwrap();
        if valid {
            queue.stats.consumed += 1;
        } else {
            queue.stats.validation_failures += 1;
        }
    }

    /// Stops accepting work and discards all predictions without waiting on workers.
    pub fn cancel(&self) {
        let mut queue = self.shared.0.lock().unwrap();
        queue.parent = None;
        queue.jobs.clear();
        queue.slots.clear();
        self.shared.1.notify_all();
    }

    /// Current epoch counters; execution counts become stable once idle.
    pub fn stats(&self) -> SpeculatorStats {
        self.shared.0.lock().unwrap().stats
    }

    /// Bounded diagnostic wait, never used by the consumption hook.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let until = Instant::now() + timeout;
        let mut queue = self.shared.0.lock().unwrap();
        while queue.active != 0 || !queue.jobs.is_empty() {
            let Some(remaining) = until.checked_duration_since(Instant::now()) else {
                return false;
            };
            queue = self.shared.1.wait_timeout(queue, remaining).unwrap().0;
        }
        true
    }

    /// Worker loop, catching provider and execution panics without poisoning the queue.
    pub fn work(shared: Arc<(Mutex<SpeculatorQueue>, Condvar)>) {
        let mut provider: Option<(Arc<SpeculationParent>, Box<dyn ParallelDatabase>)> = None;
        loop {
            let job = {
                let mut queue = shared.0.lock().unwrap();
                while queue.jobs.is_empty() && !queue.stopped {
                    queue = shared.1.wait(queue).unwrap();
                }
                if queue.stopped {
                    return;
                }
                let job = queue.jobs.pop_front().unwrap();
                queue.active += 1;
                queue.stats.executions += 1;
                job
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                let parent = &job.prediction.parent;
                if provider.as_ref().is_none_or(|(epoch, _)| !Arc::ptr_eq(epoch, parent)) {
                    provider = Some((Arc::clone(parent), (parent.database)()));
                }
                let store = parent.overlay.fork(provider.as_ref().unwrap().1.as_ref());
                let forwarding = job.prediction.forwarding.then_some((
                    &job.prediction.mv,
                    job.prediction.status.as_slice(),
                    job.index,
                ));
                let mut result = RecordingDb::execute_candidate(
                    &parent.factory,
                    parent.env.clone(),
                    &store,
                    BaseTransaction::from_recovered_tx(
                        job.transaction.inner(),
                        job.transaction.signer(),
                    ),
                    forwarding,
                )?;
                result.parent_hash = parent.hash;
                if job.prediction.forwarding {
                    job.prediction.mv.publish_candidate(&store, job.index, &result.output.state);
                }
                Some(result)
            }))
            .ok()
            .flatten();
            let mut queue = shared.0.lock().unwrap();
            queue.active -= 1;
            if let Some(slot) = queue.slots.get_mut(&job.transaction.inner().tx_hash())
                && Arc::ptr_eq(&slot.job, &job)
            {
                slot.result = result;
            }
            shared.1.notify_all();
        }
    }
}

impl Drop for Speculator {
    fn drop(&mut self) {
        {
            let mut queue = self.shared.0.lock().unwrap();
            queue.stopped = true;
            queue.jobs.clear();
            self.shared.1.notify_all();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BaseTransaction, BaseUpgrade};
    use alloy_primitives::TxKind;
    use revm::{
        context::{BlockEnv, CfgEnv, TxEnv},
        database::{CacheDB, EmptyDB},
        state::AccountInfo,
    };

    use crate::{AlloyReceiptBuilder, BaseBlockExecutionCtx, BaseBlockExecutorFactory};
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_evm::{
        Evm, EvmFactory,
        block::{BlockExecutor, BlockExecutorFactory},
    };
    use alloy_primitives::Signature;
    use base_common_chains::ChainUpgrades;

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
            assert_eq!(workers.stats().validation_failures, 2);
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
