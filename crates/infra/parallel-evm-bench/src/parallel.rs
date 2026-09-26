//! Optimistic parallel block execution with in-order, value-validated commits.
//!
//! Workers speculatively execute transactions against the latest committed state and record
//! every value they read. A single committer walks transactions in block order: if every value a
//! speculative execution read still matches the committed state, its writes are applied as-is
//! (value-based validation is sound because execution is a deterministic function of the values
//! read); otherwise the transaction is re-executed on the committer against the exact prefix
//! state. Fee credits to the beneficiary and fee vaults are deferred to commit time so they do not
//! serialize every transaction.

use std::{
    cell::RefCell,
    convert::Infallible,
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use alloy_evm::{EvmEnv, FromRecoveredTx};
use alloy_primitives::{Address, B256, Log, U256, map::HashMap as FastMap};
use base_common_consensus::{BaseBlock, Predeploys};
use base_common_evm::{
    BaseContextTr, BaseHaltReason, BaseHandler, BaseSpecId, BaseTransaction, BaseTransactionError,
    BaseTxTr, DEPOSIT_TRANSACTION_TYPE, IsTxError,
};
use base_common_genesis::BaseUpgrade;
use base_execution_evm::BaseEvmConfig;
use dashmap::DashMap;
use eyre::{Result, eyre};
use reth_evm::{ConfigureEvm, execute::BlockExecutor};
use reth_primitives_traits::RecoveredBlock;
use reth_revm::{State, db::BundleState};
use revm::{
    Database, DatabaseRef,
    context::{ContextSetters, TxEnv},
    context_interface::{
        Block, Cfg, ContextTr, JournalTr, Transaction,
        cfg::gas::InitialAndFloorGas,
        result::{EVMError, ExecutionResult, FromStringError, ResultGas},
    },
    handler::{EvmTr, FrameResult, Handler, evm::FrameTr, handler::EvmTrError},
    interpreter::{GasTracker, interpreter_action::FrameInit},
    state::{AccountInfo, Bytecode, EvmState},
};

use crate::data::PreDb;

/// Per-transaction outcome compared against the sequential executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxOutcome {
    /// Success flag.
    pub success: bool,
    /// Cumulative gas used.
    pub cumulative_gas: u64,
    /// Logs.
    pub logs: Vec<Log>,
}

/// A state location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Loc {
    /// Account balance / nonce / code.
    Account(Address),
    /// Storage slot.
    Slot(Address, U256),
}

/// A recorded read.
#[derive(Debug, Clone)]
pub enum Read {
    /// Account read and the `(balance, nonce, code_hash)` observed.
    Account(Address, Option<(U256, u64, B256)>),
    /// Storage read and the value observed.
    Slot(Address, U256, U256),
}

impl Read {
    const fn loc(&self) -> Loc {
        match self {
            Self::Account(address, _) => Loc::Account(*address),
            Self::Slot(address, slot, _) => Loc::Slot(*address, *slot),
        }
    }
}

fn account_key(info: &Option<AccountInfo>) -> Option<(U256, u64, B256)> {
    info.as_ref().map(|info| (info.balance, info.nonce, info.code_hash))
}

/// Committed state: the read-only pre-state plus an overlay of committed writes.
#[derive(Debug)]
pub struct Store<'a> {
    pre: &'a PreDb,
    accounts: DashMap<Address, Option<AccountInfo>>,
    storage: DashMap<(Address, U256), U256>,
    codes: DashMap<B256, Bytecode>,
}

impl<'a> Store<'a> {
    /// Creates an empty overlay over `pre`.
    pub fn new(pre: &'a PreDb) -> Self {
        Self { pre, accounts: DashMap::new(), storage: DashMap::new(), codes: DashMap::new() }
    }

    /// Committed account.
    pub fn account(&self, address: Address) -> Option<AccountInfo> {
        if let Some(account) = self.accounts.get(&address) {
            return account.clone();
        }
        self.pre.basic_ref(address).unwrap()
    }

    /// Committed storage slot.
    pub fn slot(&self, address: Address, slot: U256) -> U256 {
        if let Some(value) = self.storage.get(&(address, slot)) {
            return *value;
        }
        self.pre.storage_ref(address, slot).unwrap()
    }

    fn is_current(&self, read: &Read) -> bool {
        match read {
            Read::Account(address, seen) => account_key(&self.account(*address)) == *seen,
            Read::Slot(address, slot, seen) => self.slot(*address, *slot) == *seen,
        }
    }

    // ponytail: destroyed accounts do not wipe their pre-existing storage; post-Cancun
    // SELFDESTRUCT only destroys same-transaction contracts, which have none. The final state
    // comparison against the sequential executor catches any violation.
    fn apply_state(&self, state: &EvmState) {
        for (address, account) in state {
            if !account.is_touched() {
                continue;
            }
            if account.is_selfdestructed() || account.is_empty() {
                self.accounts.insert(*address, None);
                continue;
            }
            if let Some(code) = &account.info.code {
                self.codes.entry(account.info.code_hash).or_insert_with(|| code.clone());
            }
            self.accounts.insert(*address, Some(account.info.clone()));
            for (slot, value) in account.changed_storage_slots() {
                self.storage.insert((*address, *slot), value.present_value);
            }
        }
    }

    fn apply_bundle(&self, bundle: &BundleState) {
        for (address, account) in &bundle.state {
            let info = account.info.clone().map(|mut info| {
                if info.code.is_none() {
                    info.code = bundle
                        .contracts
                        .get(&info.code_hash)
                        .cloned()
                        .or_else(|| self.pre.code_by_hash_ref(info.code_hash).ok());
                }
                info
            });
            self.accounts.insert(*address, info);
            for (slot, value) in &account.storage {
                self.storage.insert((*address, *slot), value.present_value);
            }
        }
    }

    fn credit(&self, address: Address, amount: U256) {
        if amount.is_zero() {
            return;
        }
        let mut info = self.account(address).unwrap_or_default();
        info.balance += amount;
        self.accounts.insert(address, Some(info));
    }
}

impl Store<'_> {
    /// Differences between this committed state and a sequential executor's bundle.
    pub fn diff(&self, bundle: &BundleState) -> Vec<String> {
        let mut diffs = Vec::new();
        let expected_account = |address: &Address| {
            bundle.state.get(address).map_or_else(
                || self.pre.basic_ref(*address).unwrap(),
                |account| account.info.clone(),
            )
        };
        let expected_slot = |address: &Address, slot: &U256| {
            bundle
                .state
                .get(address)
                .and_then(|a| a.storage.get(slot))
                .map_or_else(|| self.pre.storage_ref(*address, *slot).unwrap(), |s| s.present_value)
        };
        let addresses = bundle.state.keys().copied().chain(self.accounts.iter().map(|e| *e.key()));
        for address in addresses {
            let (want, got) = (account_key(&expected_account(&address)), account_key(&self.account(address)));
            if want != got {
                diffs.push(format!("account {address}: want {want:?} got {got:?}"));
            }
        }
        let slots = bundle
            .state
            .iter()
            .flat_map(|(a, acc)| acc.storage.keys().map(move |s| (*a, *s)))
            .chain(self.storage.iter().map(|e| *e.key()));
        for (address, slot) in slots {
            let (want, got) = (expected_slot(&address, &slot), self.slot(address, slot));
            if want != got {
                diffs.push(format!("slot {address}[{slot}]: want {want} got {got}"));
            }
        }
        diffs
    }
}

impl DatabaseRef for Store<'_> {
    type Error = Infallible;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(self.account(address))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(code) = self.codes.get(&code_hash) {
            return Ok(code.clone());
        }
        self.pre.code_by_hash_ref(code_hash)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        Ok(self.slot(address, index))
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.pre.block_hash_ref(number)
    }
}

/// Database view that records every value a transaction reads from the committed state.
#[derive(Debug)]
pub struct RecordingDb<'a> {
    store: &'a Store<'a>,
    reads: Vec<Read>,
}

impl Database for RecordingDb<'_> {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let account = self.store.account(address);
        self.reads.push(Read::Account(address, account_key(&account)));
        Ok(account)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.store.code_by_hash_ref(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let value = self.store.slot(address, index);
        self.reads.push(Read::Slot(address, index, value));
        Ok(value)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.store.block_hash_ref(number)
    }
}

/// [`BaseHandler`] that records fee credits instead of writing them to the fee recipients.
#[derive(Debug)]
pub struct LazyFeeHandler<EVM, ERROR, FRAME> {
    inner: BaseHandler<EVM, ERROR, FRAME>,
    /// Deferred `(recipient, amount)` credits of the last transaction.
    pub fees: RefCell<Vec<(Address, U256)>>,
}

impl<EVM, ERROR, FRAME> Default for LazyFeeHandler<EVM, ERROR, FRAME> {
    fn default() -> Self {
        Self { inner: BaseHandler::new(), fees: RefCell::default() }
    }
}

impl<EVM, ERROR, FRAME> Handler for LazyFeeHandler<EVM, ERROR, FRAME>
where
    EVM: EvmTr<Context: BaseContextTr, Frame = FRAME>,
    ERROR: EvmTrError<EVM> + From<BaseTransactionError> + FromStringError + IsTxError,
    FRAME: FrameTr<FrameResult = FrameResult, FrameInit = FrameInit>,
{
    type Evm = EVM;
    type Error = ERROR;
    type HaltReason = BaseHaltReason;

    fn validate_env(&self, evm: &mut Self::Evm) -> Result<(), Self::Error> {
        self.inner.validate_env(evm)
    }

    fn validate_against_state_and_deduct_caller(
        &self,
        evm: &mut Self::Evm,
        gas: &mut InitialAndFloorGas,
    ) -> Result<(), Self::Error> {
        self.inner.validate_against_state_and_deduct_caller(evm, gas)
    }

    fn last_frame_result(
        &mut self,
        evm: &mut Self::Evm,
        frame_result: &mut FrameResult,
        parent_gas: &mut GasTracker,
    ) -> Result<(), Self::Error> {
        self.inner.last_frame_result(evm, frame_result, parent_gas)
    }

    fn reimburse_caller(
        &self,
        evm: &mut Self::Evm,
        frame_result: &mut FrameResult,
    ) -> Result<(), Self::Error> {
        self.inner.reimburse_caller(evm, frame_result)
    }

    fn refund(
        &self,
        evm: &mut Self::Evm,
        frame_result: &mut FrameResult,
        eip7702_refund: i64,
    ) -> Result<(), Self::Error> {
        self.inner.refund(evm, frame_result, eip7702_refund)
    }

    // Mirrors `BaseHandler::reward_beneficiary` (and revm's mainnet beneficiary reward) but
    // defers the credits.
    fn reward_beneficiary(
        &self,
        evm: &mut Self::Evm,
        frame_result: &mut FrameResult,
    ) -> Result<(), Self::Error> {
        let ctx = evm.ctx();
        if ctx.tx().tx_type() == DEPOSIT_TRANSACTION_TYPE {
            return Ok(());
        }
        let basefee = ctx.block().basefee() as u128;
        let gas = frame_result.gas();
        let used = gas.used();
        let tip_price = ctx.tx().effective_gas_price(basefee).saturating_sub(basefee);
        let tip = U256::from(tip_price * used.saturating_sub(gas.reservoir()) as u128);
        let spec = ctx.cfg().spec();
        let beneficiary = ctx.block().beneficiary();
        let Some(enveloped) = ctx.tx().enveloped_tx().cloned() else {
            return Err(ERROR::from_string("missing enveloped transaction".into()));
        };
        let l1_block_info = ctx.chain_mut();
        let l1_cost = l1_block_info.calculate_tx_l1_cost(&enveloped, spec);
        let operator_fee = if spec.is_enabled_in(BaseUpgrade::Isthmus) {
            l1_block_info.operator_fee_charge(&enveloped, U256::from(used), spec)
        } else {
            U256::ZERO
        };
        self.fees.replace(vec![
            (beneficiary, tip),
            (Predeploys::L1_FEE_VAULT, l1_cost),
            (Predeploys::BASE_FEE_VAULT, U256::from(basefee.saturating_mul(used as u128))),
            (Predeploys::OPERATOR_FEE_VAULT, operator_fee),
        ]);
        Ok(())
    }

    fn execution_result(
        &mut self,
        evm: &mut Self::Evm,
        result: FrameResult,
        result_gas: ResultGas,
    ) -> Result<ExecutionResult<Self::HaltReason>, Self::Error> {
        self.inner.execution_result(evm, result, result_gas)
    }

    fn catch_error(
        &self,
        evm: &mut Self::Evm,
        error: Self::Error,
    ) -> Result<ExecutionResult<Self::HaltReason>, Self::Error> {
        self.inner.catch_error(evm, error)
    }
}

/// One execution of one transaction.
#[derive(Debug)]
struct Speculation {
    result: Option<ExecutionResult<BaseHaltReason>>,
    state: EvmState,
    reads: Vec<Read>,
    fees: Vec<(Address, U256)>,
    nanos: u64,
}

/// Per-transaction trace of the committed (exact) execution, for dependency analysis.
#[derive(Debug, Clone)]
pub struct TxTrace {
    /// Locations read.
    pub reads: Vec<Loc>,
    /// Locations written, including deferred fee credits.
    pub writes: Vec<Loc>,
    /// Execution time of the committed incarnation.
    pub nanos: u64,
}

/// Result of a parallel block execution.
#[derive(Debug)]
pub struct ParallelOutcome {
    /// Per-transaction outcomes.
    pub txs: Vec<TxOutcome>,
    /// Transactions re-executed on the committer.
    pub reexecuted: usize,
    /// Committed-execution traces (only when requested).
    pub traces: Vec<TxTrace>,
}

type Config = BaseEvmConfig;

fn execute(
    config: &Config,
    env: &EvmEnv<BaseSpecId>,
    store: &Store<'_>,
    tx: &BaseTransaction<TxEnv>,
) -> Speculation {
    let start = Instant::now();
    let db = RecordingDb { store, reads: Vec::new() };
    let mut evm = config.evm_with_env(db, env.clone());
    evm.ctx_mut().set_tx(tx.clone());
    let mut handler: LazyFeeHandler<_, EVMError<Infallible, BaseTransactionError>, _> =
        LazyFeeHandler::default();
    let result = handler.run(&mut evm).ok();
    let state = evm.ctx_mut().journal_mut().finalize();
    let reads = std::mem::take(&mut evm.ctx_mut().db_mut().reads);
    Speculation {
        result,
        state,
        reads,
        fees: handler.fees.take(),
        nanos: start.elapsed().as_nanos() as u64,
    }
}

impl ParallelOutcome {
    /// Executes `block` with `threads` total threads (one committer plus `threads - 1` workers).
    pub fn execute(
        config: &Config,
        block: &RecoveredBlock<BaseBlock>,
        store: &Store<'_>,
        threads: usize,
        trace: bool,
    ) -> Result<Self> {
        let env = config.evm_env(block.header())?;
        let recovered: Vec<_> = block.transactions_recovered().collect();

        // Pre-execution system calls and the L1 info deposit run through the production executor;
        // every later transaction reads the L1 block info it writes.
        let mut txs = Vec::with_capacity(recovered.len());
        {
            let mut state =
                State::builder().with_database_ref(store).with_bundle_update().build();
            let mut executor = config.executor_for_block(&mut state, block.sealed_block())?;
            executor.apply_pre_execution_changes()?;
            let gas = executor.execute_transaction(recovered[0])?;
            let receipt = &executor.receipts()[0];
            txs.push(TxOutcome {
                success: alloy_consensus::TxReceipt::status(receipt),
                cumulative_gas: gas.tx_gas_used(),
                logs: alloy_consensus::TxReceipt::logs(receipt).to_vec(),
            });
            drop(executor);
            state.merge_transitions(reth_revm::db::states::bundle_state::BundleRetention::PlainState);
            store.apply_bundle(&state.take_bundle());
        }

        let rest: Vec<BaseTransaction<TxEnv>> = recovered[1..]
            .iter()
            .map(|tx| BaseTransaction::from_recovered_tx(tx.inner(), tx.signer()))
            .collect();
        let n = rest.len();
        let claimed: Vec<AtomicBool> = (0..n).map(|_| AtomicBool::new(false)).collect();
        let done: Vec<OnceLock<Speculation>> = (0..n).map(|_| OnceLock::new()).collect();
        let cursor = AtomicUsize::new(0);
        let mut reexecuted = 0;
        let mut traces = Vec::new();
        let mut cumulative_gas = txs[0].cumulative_gas;

        std::thread::scope(|scope| -> Result<()> {
            for _ in 1..threads {
                scope.spawn(|| {
                    loop {
                        let i = cursor.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        if !claimed[i].swap(true, Ordering::AcqRel) {
                            let _ = done[i].set(execute(config, &env, store, &rest[i]));
                        }
                    }
                });
            }

            let finish = |cursor: &AtomicUsize| cursor.store(n, Ordering::Relaxed);
            for i in 0..n {
                let spec = if claimed[i].swap(true, Ordering::AcqRel) {
                    let spec = loop {
                        if let Some(spec) = done[i].get() {
                            break spec;
                        }
                        std::hint::spin_loop();
                    };
                    if spec.result.is_some() && spec.reads.iter().all(|r| store.is_current(r)) {
                        None
                    } else {
                        reexecuted += 1;
                        Some(execute(config, &env, store, &rest[i]))
                    }
                } else {
                    Some(execute(config, &env, store, &rest[i]))
                };
                let spec = spec.as_ref().unwrap_or_else(|| done[i].get().unwrap());
                let Some(result) = &spec.result else {
                    finish(&cursor);
                    return Err(eyre!("transaction {} failed at its exact prefix state", i + 1));
                };
                cumulative_gas += result.tx_gas_used();
                txs.push(TxOutcome {
                    success: result.is_success(),
                    cumulative_gas,
                    logs: result.logs().to_vec(),
                });
                if trace {
                    traces.push(TxTrace {
                        reads: spec.reads.iter().map(Read::loc).collect(),
                        writes: spec
                            .state
                            .iter()
                            .filter(|(_, a)| a.is_touched())
                            .flat_map(|(address, account)| {
                                let after = (!account.is_selfdestructed() && !account.is_empty())
                                    .then(|| account.info.clone());
                                (account_key(&after) != account_key(&store.account(*address)))
                                    .then_some(Loc::Account(*address))
                                    .into_iter()
                                    .chain(
                                        account
                                            .changed_storage_slots()
                                            .map(|(slot, _)| Loc::Slot(*address, *slot)),
                                    )
                            })
                            .chain(spec.fees.iter().filter(|(_, a)| !a.is_zero()).map(|(r, _)| Loc::Account(*r)))
                            .collect(),
                        nanos: spec.nanos,
                    });
                }
                store.apply_state(&spec.state);
                for (recipient, amount) in &spec.fees {
                    store.credit(*recipient, *amount);
                }
            }
            Ok(())
        })?;

        Ok(Self { txs, reexecuted, traces })
    }
}

/// Dependency analysis over committed traces.
#[derive(Debug)]
pub struct CriticalPath {
    /// Sum of transaction execution times.
    pub total_nanos: u64,
    /// Longest chain of read-after-write dependencies, weighted by execution time.
    pub path_nanos: u64,
    /// Transactions that read a location written by an earlier transaction.
    pub dependent_txs: usize,
    /// Locations most often on a transaction's binding dependency, with counts.
    pub hot: Vec<(Loc, usize)>,
}

impl CriticalPath {
    /// Computes the critical path. Fee credits count as writes, so any transaction that reads a
    /// fee recipient depends on every earlier fee-paying transaction.
    pub fn new(traces: &[TxTrace]) -> Self {
        // Max finish time over all earlier writers of each location (conservative for blind
        // writes, exact for commutative fee credits).
        let mut written: FastMap<Loc, u64> = FastMap::default();
        let mut finish = vec![0u64; traces.len()];
        let mut dependent_txs = 0;
        let mut hot: FastMap<Loc, usize> = FastMap::default();
        for (i, trace) in traces.iter().enumerate() {
            let binding = trace
                .reads
                .iter()
                .filter_map(|loc| written.get(loc).map(|f| (*f, *loc)))
                .max_by_key(|(f, _)| *f);
            if let Some((_, loc)) = binding {
                *hot.entry(loc).or_default() += 1;
            }
            let ready = binding.map(|(f, _)| f);
            dependent_txs += usize::from(ready.is_some());
            finish[i] = ready.unwrap_or_default() + trace.nanos;
            for loc in &trace.writes {
                let entry = written.entry(*loc).or_default();
                *entry = (*entry).max(finish[i]);
            }
        }
        Self {
            total_nanos: traces.iter().map(|t| t.nanos).sum(),
            path_nanos: finish.iter().copied().max().unwrap_or_default(),
            dependent_txs,
            hot: {
                let mut hot: Vec<_> = hot.into_iter().collect();
                hot.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
                hot.truncate(5);
                hot
            },
        }
    }
}
