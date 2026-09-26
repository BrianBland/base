//! Optimistic parallel block execution with in-order, value-validated commits.
//!
//! Workers speculatively execute transactions against the latest committed state and record
//! every value they read. A single committer walks transactions in block order: if every value a
//! speculative execution read still matches the committed state, its writes are applied as-is
//! (value-based validation is sound because execution is a deterministic function of the values
//! read); otherwise the transaction is re-executed on the committer against the exact prefix
//! state. Fee credits to the beneficiary and fee vaults are deferred to commit time so they do not
//! serialize every transaction.
//!
//! Balances are validated by what execution observed rather than by value: see [`BalanceRead`].

use std::{
    cell::RefCell,
    collections::BTreeMap,
    convert::Infallible,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use alloy_evm::{EvmEnv, FromRecoveredTx};
use alloy_primitives::{Address, B256, Log, U256, map::HashMap as FastMap};
use base_common_consensus::{BaseBlock, Predeploys};
use base_common_evm::{
    BaseContext, BaseHaltReason, BaseHandler, BaseSpecId, BaseTransaction, BaseTransactionError,
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
        Block, ContextTr, JournalTr, Transaction,
        cfg::gas::InitialAndFloorGas,
        result::{EVMError, ExecutionResult, FromStringError, ResultGas},
    },
    handler::{EvmTr, FrameResult, Handler, evm::FrameTr, handler::EvmTrError},
    interpreter::{GasTracker, interpreter_action::FrameInit},
    primitives::KECCAK_EMPTY,
    state::{AccountInfo, Bytecode, EvmState},
};

use crate::{balance::BalanceOpcodes, data::PreDb};

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
    /// Account existence, nonce and code.
    Account(Address),
    /// Account balance.
    Balance(Address),
    /// Storage slot.
    Slot(Address, U256),
}

impl Loc {
    /// Locations whose value differs between `before` and `after`, both written to `self`.
    fn changes(self, before: &Value, after: &Value) -> impl Iterator<Item = Self> + use<> {
        let changed = match (self, before, after) {
            (Self::Account(address), Value::Account(a), Value::Account(b)) => [
                (info_key(a) != info_key(b)).then_some(self),
                (balance(a) != balance(b)).then_some(Self::Balance(address)),
            ],
            (_, Value::Slot(a), Value::Slot(b)) => [(a != b).then_some(self), None],
            _ => unreachable!("values written to one location have one kind"),
        };
        changed.into_iter().flatten()
    }
}

/// The committed balances a transaction's execution stays valid for.
///
/// Execution changes an account's balance only by credits and by debits guarded by sufficiency
/// checks (`current >= amount`), and otherwise observes it only through `BALANCE`, `SELFBALANCE`
/// and `SELFDESTRUCT`. If none of those observed it, running the transaction against committed
/// balance `b` instead of `seen` takes the same path and ends with `b + (written - seen)`, so it
/// commits by rebasing its write. Each sufficiency check narrows the range to the committed
/// balances under which it has the same outcome; an absolute observation narrows it to `seen`.
/// Accounts that may be empty stay exact, because EIP-161 emptiness depends on the balance.
#[derive(Debug, Clone, Copy)]
pub struct BalanceRead {
    /// Balance the execution read.
    pub seen: U256,
    /// Lowest committed balance the execution is valid for.
    pub min: U256,
    /// Highest committed balance the execution is valid for.
    pub max: U256,
}

impl BalanceRead {
    const fn exact(seen: U256) -> Self {
        Self { seen, min: seen, max: seen }
    }

    const fn unobserved(seen: U256) -> Self {
        Self { seen, min: U256::ZERO, max: U256::MAX }
    }

    /// Whether the execution is valid for committed balance `balance`.
    pub fn admits(&self, balance: U256) -> bool {
        self.min <= balance && balance <= self.max
    }

    fn is_exact(&self) -> bool {
        self.min == self.max
    }

    /// Records that execution compared its `current` balance (`seen` plus local changes) against
    /// `amount`: from committed balance `b` it would compare `b + current - seen` instead.
    fn require(&mut self, current: U256, amount: U256) {
        let Some(threshold) = amount.checked_add(self.seen) else {
            *self = Self::exact(self.seen);
            return;
        };
        if current >= amount {
            self.min = self.min.max(threshold.saturating_sub(current));
        } else {
            // `threshold - current > seen >= 0`.
            self.max = self.max.min(threshold - current - U256::from(1));
        }
    }
}

/// A recorded read.
#[derive(Debug, Clone)]
pub enum Read {
    /// Account read: `(nonce, code_hash)` if it existed, and the constraint on its balance.
    Account(Address, Option<(u64, B256)>, BalanceRead),
    /// Storage read and the value observed.
    Slot(Address, U256, U256),
}

impl Read {
    const fn loc(&self) -> Loc {
        match self {
            Self::Account(address, ..) => Loc::Account(*address),
            Self::Slot(address, slot, _) => Loc::Slot(*address, *slot),
        }
    }
}

fn account_key(info: &Option<AccountInfo>) -> Option<(U256, u64, B256)> {
    info.as_ref().map(|info| (info.balance, info.nonce, info.code_hash))
}

fn info_key(info: &Option<AccountInfo>) -> Option<(u64, B256)> {
    info.as_ref().map(|info| (info.nonce, info.code_hash))
}

fn balance(info: &Option<AccountInfo>) -> U256 {
    info.as_ref().map_or(U256::ZERO, |info| info.balance)
}

/// Whether EIP-161 emptiness of the account can depend on its balance.
fn may_be_empty(info: &Option<AccountInfo>) -> bool {
    info.as_ref().is_none_or(|info| {
        info.nonce == 0 && (info.code_hash == KECCAK_EMPTY || info.code_hash.is_zero())
    })
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
            Read::Account(address, seen, balance) => match self.account(*address) {
                Some(now) => {
                    *seen == Some((now.nonce, now.code_hash)) && balance.admits(now.balance)
                }
                None => seen.is_none(),
            },
            Read::Slot(address, slot, seen) => self.slot(*address, *slot) == *seen,
        }
    }

    // ponytail: destroyed accounts do not wipe their pre-existing storage; post-Cancun
    // SELFDESTRUCT only destroys same-transaction contracts, which have none. The final state
    // comparison against the sequential executor catches any violation.
    /// Applies a validated execution, rebasing each balance it read by the committed drift.
    /// Returns whether any read balance had drifted.
    fn apply_state(&self, state: &EvmState, reads: &[Read]) -> bool {
        let rebased: Vec<(Address, U256, U256)> = reads
            .iter()
            .filter_map(|read| {
                let Read::Account(address, Some(_), balance) = read else { return None };
                let now = self.account(*address)?.balance;
                (now != balance.seen).then_some((*address, balance.seen, now))
            })
            .collect();
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
            let mut info = account.info.clone();
            if let Some((_, seen, now)) = rebased.iter().find(|(a, ..)| a == address) {
                info.balance = info.balance.wrapping_add(*now).wrapping_sub(*seen);
            }
            self.accounts.insert(*address, Some(info));
            for (slot, value) in account.changed_storage_slots() {
                self.storage.insert((*address, *slot), value.present_value);
            }
        }
        !rebased.is_empty()
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
            let (want, got) =
                (account_key(&expected_account(&address)), account_key(&self.account(address)));
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

/// A value written to a [`Loc`].
#[derive(Debug, Clone)]
pub enum Value {
    /// Account (`None` = destroyed).
    Account(Option<AccountInfo>),
    /// Storage value.
    Slot(U256),
}

/// Multi-version memory: the latest speculative write of each location by each uncommitted
/// transaction, plus which transactions read each location.
#[derive(Debug, Default)]
pub struct MvMemory {
    writes: DashMap<Loc, BTreeMap<usize, Value>>,
    readers: DashMap<Loc, Vec<usize>>,
}

impl MvMemory {
    fn latest(&self, loc: &Loc, tx: usize) -> Option<(usize, Value)> {
        self.writes.get(loc)?.range(..tx).next_back().map(|(w, v)| (*w, v.clone()))
    }

    fn visible(&self, store: &Store<'_>, loc: &Loc, tx: usize) -> Value {
        self.latest(loc, tx).map(|(_, v)| v).unwrap_or_else(|| match loc {
            Loc::Account(address) | Loc::Balance(address) => {
                Value::Account(store.account(*address))
            }
            Loc::Slot(address, slot) => Value::Slot(store.slot(*address, *slot)),
        })
    }

    /// Publishes a new incarnation's writes, returning the locations whose visible value
    /// changed for higher transactions.
    fn publish(
        &self,
        store: &Store<'_>,
        tx: usize,
        writes: &[(Loc, Value)],
        previous: &[(Loc, Value)],
    ) -> Vec<Loc> {
        let mut changed = Vec::new();
        for (loc, value) in writes {
            // Compute before taking the entry guard: both touch the same shard.
            let below = self.visible(store, loc, tx);
            let mut entry = self.writes.entry(*loc).or_default();
            let old = entry.insert(tx, value.clone());
            changed.extend(loc.changes(old.as_ref().unwrap_or(&below), value));
        }
        for (loc, old) in previous {
            if !writes.iter().any(|(l, _)| l == loc) {
                if let Some(mut entry) = self.writes.get_mut(loc) {
                    entry.remove(&tx);
                }
                changed.extend(loc.changes(old, &self.visible(store, loc, tx)));
            }
        }
        changed
    }

    fn remove(&self, tx: usize, writes: &[(Loc, Value)]) {
        for (loc, _) in writes {
            if let Some(mut entry) = self.writes.get_mut(loc) {
                entry.remove(&tx);
            }
        }
    }
}

/// A speculative read hit a value written by a transaction that is being re-executed (Block-STM's
/// ESTIMATE): abort and retry once that transaction finishes.
#[derive(Debug)]
pub struct Blocked(pub usize);

impl std::fmt::Display for Blocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "blocked on transaction {}", self.0)
    }
}

impl std::error::Error for Blocked {}
impl revm::context::DBErrorMarker for Blocked {}

/// Database view for one execution: resolves reads through the multi-version memory (when
/// speculating) or the committed state (at the commit frontier), recording every value read.
#[derive(Debug)]
pub struct RecordingDb<'a> {
    store: &'a Store<'a>,
    speculation: Option<(&'a MvMemory, &'a [AtomicU8])>,
    tx: usize,
    reads: Vec<Read>,
    /// Index in `reads` of each account read.
    accounts: FastMap<Address, usize>,
}

impl RecordingDb<'_> {
    fn register(&self, loc: Loc) {
        if let Some((mv, _)) = self.speculation {
            mv.readers.entry(loc).or_default().push(self.tx);
        }
    }

    fn resolve(&self, loc: Loc) -> Result<Value, Blocked> {
        let Some((mv, status)) = self.speculation else {
            return Ok(match loc {
                Loc::Account(address) | Loc::Balance(address) => {
                    Value::Account(self.store.account(address))
                }
                Loc::Slot(address, slot) => Value::Slot(self.store.slot(address, slot)),
            });
        };
        // Register before reading so a concurrent writer either sees us or we see it.
        self.register(loc);
        match mv.latest(&loc, self.tx) {
            Some((writer, _)) if status[writer].load(Ordering::SeqCst) != EXECUTED => {
                Err(Blocked(writer))
            }
            Some((_, value)) => Ok(value),
            None => Ok(mv.visible(self.store, &loc, self.tx)),
        }
    }

    fn balance_read(&mut self, address: Address) -> Option<&mut BalanceRead> {
        match &mut self.reads[*self.accounts.get(&address)?] {
            Read::Account(_, _, balance) => Some(balance),
            Read::Slot(..) => unreachable!("account reads index account entries"),
        }
    }

    /// Records that execution observed `address`'s absolute balance.
    pub fn observe_balance(&mut self, address: Address) {
        let Some(balance) = self.balance_read(address) else { return };
        if !balance.is_exact() {
            *balance = BalanceRead::exact(balance.seen);
            self.register(Loc::Balance(address));
        }
    }

    /// Records that execution compared `address`'s `current` balance against `amount`.
    pub fn require_balance(&mut self, address: Address, current: U256, amount: U256) {
        if let Some(balance) = self.balance_read(address) {
            balance.require(current, amount);
        }
    }
}

impl Database for RecordingDb<'_> {
    type Error = Blocked;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let Value::Account(account) = self.resolve(Loc::Account(address))? else { unreachable!() };
        let balance_read = if may_be_empty(&account) {
            self.register(Loc::Balance(address));
            BalanceRead::exact(balance(&account))
        } else {
            BalanceRead::unobserved(balance(&account))
        };
        self.accounts.entry(address).or_insert(self.reads.len());
        self.reads.push(Read::Account(address, info_key(&account), balance_read));
        Ok(account)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(self.store.code_by_hash_ref(code_hash).unwrap())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let Value::Slot(value) = self.resolve(Loc::Slot(address, index))? else { unreachable!() };
        self.reads.push(Read::Slot(address, index, value));
        Ok(value)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.store.block_hash_ref(number).unwrap())
    }
}

/// [`BaseHandler`] that records fee credits instead of writing them to the fee recipients, and
/// records the caller balance checks of transaction validation.
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

impl<'db, EVM, ERROR, FRAME> Handler for LazyFeeHandler<EVM, ERROR, FRAME>
where
    EVM: EvmTr<Context = BaseContext<RecordingDb<'db>>, Frame = FRAME>,
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
        self.inner.validate_against_state_and_deduct_caller(evm, gas)?;
        let ctx = evm.ctx();
        let caller = ctx.tx.caller();
        let db = &mut ctx.journaled_state.database;
        if ctx.tx.tx_type() == DEPOSIT_TRANSACTION_TYPE {
            db.observe_balance(caller);
            return Ok(());
        }
        let Some(seen) = db.balance_read(caller).map(|balance| balance.seen) else {
            return Ok(());
        };
        // Validation required `seen >= additional_cost + max_balance_spending`, then deducted
        // `spent = additional_cost + effective_balance_spending - value`.
        let remaining = ctx.journaled_state.inner.state[&caller].info.balance;
        let basefee = ctx.block.basefee() as u128;
        let blob_price = ctx.block.blob_gasprice().unwrap_or_default();
        let threshold = seen
            .checked_sub(remaining)
            .and_then(|spent| spent.checked_add(ctx.tx.value()))
            .zip(ctx.tx.effective_balance_spending(basefee, blob_price).ok())
            .and_then(|(v, effective)| v.checked_sub(effective))
            .zip(ctx.tx.max_balance_spending().ok())
            .and_then(|(additional_cost, max)| additional_cost.checked_add(max));
        let db = &mut ctx.journaled_state.database;
        match threshold {
            Some(threshold) => db.require_balance(caller, seen, threshold),
            None => db.observe_balance(caller),
        }
        Ok(())
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
        let spec = ctx.cfg.spec;
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
        let result = self.inner.catch_error(evm, error);
        let ctx = evm.ctx();
        if ctx.tx.tx_type() == DEPOSIT_TRANSACTION_TYPE {
            let caller = ctx.tx.caller();
            ctx.journaled_state.database.observe_balance(caller);
        }
        result
    }
}

/// One execution of one transaction.
#[derive(Debug)]
struct Speculation {
    result: Option<ExecutionResult<BaseHaltReason>>,
    state: EvmState,
    reads: Vec<Read>,
    writes: Vec<(Loc, Value)>,
    fees: Vec<(Address, U256)>,
    nanos: u64,
}

/// Per-transaction trace of the committed execution, for dependency analysis.
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
    /// Total executions, including re-executions.
    pub executions: usize,
    /// Transactions whose speculative result failed commit validation.
    pub reexecuted: usize,
    /// Speculative executions aborted on a read of a not-yet-executed write.
    pub blocked: usize,
    /// Transactions committed although a balance they read had since changed.
    pub rebased: usize,
    /// Committed-execution traces (only when requested).
    pub traces: Vec<TxTrace>,
}

type Config = BaseEvmConfig;

const PENDING: u8 = 0;
const EXECUTING: u8 = 1;
const EXECUTED: u8 = 2;

/// Shared scheduler state for one block.
struct Scheduler<'a> {
    config: &'a Config,
    env: EvmEnv<BaseSpecId>,
    store: &'a Store<'a>,
    mv: MvMemory,
    txs: Vec<BaseTransaction<TxEnv>>,
    status: Vec<AtomicU8>,
    invalidations: Vec<AtomicU64>,
    /// Transaction each one last blocked on (`usize::MAX` = none).
    waiting: Vec<AtomicUsize>,
    slots: Vec<Mutex<Option<Speculation>>>,
    frontier: AtomicUsize,
    stop: AtomicBool,
    executions: AtomicUsize,
    blocked: AtomicUsize,
}

impl Scheduler<'_> {
    fn execute(&self, tx: usize, speculative: bool) -> Result<Speculation, usize> {
        let start = Instant::now();
        let speculation = speculative.then_some((&self.mv, self.status.as_slice()));
        let db = RecordingDb {
            store: self.store,
            speculation,
            tx,
            reads: Vec::new(),
            accounts: FastMap::default(),
        };
        let mut evm = self.config.evm_with_env(db, self.env.clone());
        BalanceOpcodes::install(evm.all_mut().1);
        evm.ctx_mut().set_tx(self.txs[tx].clone());
        let mut handler: LazyFeeHandler<_, EVMError<Blocked, BaseTransactionError>, _> =
            LazyFeeHandler::default();
        self.executions.fetch_add(1, Ordering::Relaxed);
        let result = match handler.run(&mut evm) {
            Ok(result) => Some(result),
            Err(EVMError::Database(Blocked(writer))) => return Err(writer),
            Err(_) => None,
        };
        let state = evm.ctx_mut().journal_mut().finalize();
        let reads = std::mem::take(&mut evm.ctx_mut().db_mut().reads);
        let writes = state
            .iter()
            .filter(|(_, a)| a.is_touched())
            .flat_map(|(address, account)| {
                let info = (!account.is_selfdestructed() && !account.is_empty())
                    .then(|| account.info.clone());
                std::iter::once((Loc::Account(*address), Value::Account(info))).chain(
                    account.changed_storage_slots().map(|(slot, v)| {
                        (Loc::Slot(*address, *slot), Value::Slot(v.present_value))
                    }),
                )
            })
            .collect();
        Ok(Speculation {
            result,
            state,
            reads,
            writes,
            fees: handler.fees.take(),
            nanos: start.elapsed().as_nanos() as u64,
        })
    }

    /// Runs a transaction the caller moved to `EXECUTING`, publishes its writes, and invalidates
    /// higher transactions that read a location whose value changed.
    fn run(&self, tx: usize, speculative: bool) {
        let seen = self.invalidations[tx].load(Ordering::SeqCst);
        let spec = match self.execute(tx, speculative) {
            Ok(spec) => spec,
            Err(writer) => {
                self.blocked.fetch_add(1, Ordering::Relaxed);
                self.waiting[tx].store(writer, Ordering::SeqCst);
                self.status[tx].store(PENDING, Ordering::SeqCst);
                return;
            }
        };
        let previous = self.slots[tx].lock().unwrap().take().map(|s| s.writes).unwrap_or_default();
        for loc in self.mv.publish(self.store, tx, &spec.writes, &previous) {
            if let Some(readers) = self.mv.readers.get(&loc) {
                for &reader in readers.iter().filter(|&&r| r > tx) {
                    self.invalidations[reader].fetch_add(1, Ordering::SeqCst);
                    let _ = self.status[reader].compare_exchange(
                        EXECUTED,
                        PENDING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
            }
        }
        *self.slots[tx].lock().unwrap() = Some(spec);
        self.status[tx].store(EXECUTED, Ordering::SeqCst);
        if self.invalidations[tx].load(Ordering::SeqCst) != seen {
            let _ = self.status[tx].compare_exchange(
                EXECUTED,
                PENDING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
    }

    fn claim(&self, tx: usize) -> bool {
        let waiting = self.waiting[tx].load(Ordering::SeqCst);
        if waiting != usize::MAX && self.status[waiting].load(Ordering::SeqCst) != EXECUTED {
            return false;
        }
        self.status[tx]
            .compare_exchange(PENDING, EXECUTING, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Executes the lowest pending transaction above the commit frontier, if any.
    fn step(&self) -> bool {
        let from = self.frontier.load(Ordering::SeqCst) + 1;
        (from..self.txs.len()).find(|&tx| self.claim(tx)).map(|tx| self.run(tx, true)).is_some()
    }

    fn work(&self) {
        while !self.stop.load(Ordering::Relaxed) {
            if !self.step() {
                std::thread::yield_now();
            }
        }
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
            let mut state = State::builder().with_database_ref(store).with_bundle_update().build();
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
            state.merge_transitions(
                reth_revm::db::states::bundle_state::BundleRetention::PlainState,
            );
            store.apply_bundle(&state.take_bundle());
        }

        let rest: Vec<BaseTransaction<TxEnv>> = recovered[1..]
            .iter()
            .map(|tx| BaseTransaction::from_recovered_tx(tx.inner(), tx.signer()))
            .collect();
        let n = rest.len();
        let scheduler = Scheduler {
            config,
            env,
            store,
            mv: MvMemory::default(),
            txs: rest,
            status: (0..n).map(|_| AtomicU8::new(PENDING)).collect(),
            invalidations: (0..n).map(|_| AtomicU64::new(0)).collect(),
            waiting: (0..n).map(|_| AtomicUsize::new(usize::MAX)).collect(),
            slots: (0..n).map(|_| Mutex::new(None)).collect(),
            frontier: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            executions: AtomicUsize::new(0),
            blocked: AtomicUsize::new(0),
        };
        let (mut reexecuted, mut rebased) = (0, 0);
        let mut traces = Vec::new();
        let mut cumulative_gas = txs[0].cumulative_gas;

        std::thread::scope(|scope| -> Result<()> {
            for _ in 1..threads {
                scope.spawn(|| scheduler.work());
            }
            let result = (|| {
                for i in 0..n {
                    scheduler.frontier.store(i, Ordering::SeqCst);
                    // Every lower transaction is committed, so an execution by the committer
                    // reads the exact prefix state and needs no validation.
                    let spec = loop {
                        if scheduler.claim(i) {
                            scheduler.run(i, false);
                            break scheduler.slots[i].lock().unwrap().take().unwrap();
                        }
                        if scheduler.status[i].load(Ordering::SeqCst) == EXECUTED {
                            let spec = scheduler.slots[i].lock().unwrap().take().unwrap();
                            if spec.result.is_some()
                                && spec.reads.iter().all(|r| store.is_current(r))
                            {
                                break spec;
                            }
                            reexecuted += 1;
                            *scheduler.slots[i].lock().unwrap() = Some(spec);
                            scheduler.status[i].store(EXECUTING, Ordering::SeqCst);
                            scheduler.run(i, false);
                            break scheduler.slots[i].lock().unwrap().take().unwrap();
                        }
                        // Help speculate while a worker finishes the frontier transaction.
                        if scheduler.step() {
                            continue;
                        }
                        std::hint::spin_loop();
                    };
                    let Some(result) = &spec.result else {
                        return Err(eyre!(
                            "transaction {} failed at its exact prefix state",
                            i + 1
                        ));
                    };
                    cumulative_gas += result.tx_gas_used();
                    txs.push(TxOutcome {
                        success: result.is_success(),
                        cumulative_gas,
                        logs: result.logs().to_vec(),
                    });
                    if trace {
                        // A balance constraint binds only if the block's pre-state violates it.
                        let reads = spec
                            .reads
                            .iter()
                            .flat_map(|read| {
                                let balance_dep = match read {
                                    Read::Account(address, _, constraint) => (!constraint
                                        .admits(balance(&store.pre.basic_ref(*address).unwrap())))
                                    .then_some(Loc::Balance(*address)),
                                    Read::Slot(..) => None,
                                };
                                std::iter::once(read.loc()).chain(balance_dep)
                            })
                            .collect();
                        let writes = spec
                            .writes
                            .iter()
                            .flat_map(|(loc, value)| {
                                loc.changes(&scheduler.mv.visible(store, loc, 0), value)
                            })
                            .chain(
                                spec.fees
                                    .iter()
                                    .filter(|(_, a)| !a.is_zero())
                                    .map(|(r, _)| Loc::Balance(*r)),
                            )
                            .collect();
                        traces.push(TxTrace { reads, writes, nanos: spec.nanos });
                    }
                    rebased += usize::from(store.apply_state(&spec.state, &spec.reads));
                    for (recipient, amount) in &spec.fees {
                        store.credit(*recipient, *amount);
                    }
                    scheduler.mv.remove(i, &spec.writes);
                }
                Ok(())
            })();
            scheduler.stop.store(true, Ordering::Relaxed);
            result
        })?;

        Ok(Self {
            txs,
            executions: scheduler.executions.into_inner(),
            reexecuted,
            blocked: scheduler.blocked.into_inner(),
            rebased,
            traces,
        })
    }
}

/// Dependency analysis over committed traces.
#[derive(Debug)]
pub struct CriticalPath {
    /// Sum of transaction execution times.
    pub total_nanos: u64,
    /// Longest chain of read-after-write dependencies, weighted by execution time.
    pub path_nanos: u64,
    /// Longest chain of read-after-write dependencies in transactions (unbounded cores).
    pub path_txs: usize,
    /// Transactions that read a location written by an earlier transaction.
    pub dependent_txs: usize,
    /// Locations most often on a transaction's binding dependency, with counts.
    pub hot: Vec<(Loc, usize)>,
}

impl CriticalPath {
    /// Computes the critical path (list-scheduled in block order on `cores` workers, unbounded
    /// when `None`). Fee credits count as writes, so any transaction that observes a fee
    /// recipient's balance depends on every earlier fee-paying transaction. With `per_account`,
    /// every account read conflicts with any earlier write to that account, balance included.
    pub fn new(traces: &[TxTrace], cores: Option<usize>, per_account: bool) -> Self {
        let key = |loc: &Loc| match *loc {
            Loc::Balance(address) if per_account => Loc::Account(address),
            loc => loc,
        };
        let mut free = vec![0u64; cores.unwrap_or(traces.len()).max(1)];
        // Max finish time over all earlier writers of each location (conservative for blind
        // writes, exact for commutative fee credits).
        let mut written: FastMap<Loc, u64> = FastMap::default();
        let mut finish = vec![0u64; traces.len()];
        let mut written_depth: FastMap<Loc, usize> = FastMap::default();
        let mut depth = vec![0usize; traces.len()];
        let mut dependent_txs = 0;
        let mut hot: FastMap<Loc, usize> = FastMap::default();
        for (i, trace) in traces.iter().enumerate() {
            let binding = trace
                .reads
                .iter()
                .filter_map(|loc| written.get(&key(loc)).map(|f| (*f, key(loc))))
                .max_by_key(|(f, _)| *f);
            if let Some((_, loc)) = binding {
                *hot.entry(loc).or_default() += 1;
            }
            let ready = binding.map(|(f, _)| f);
            dependent_txs += usize::from(ready.is_some());
            let core = (0..free.len()).min_by_key(|&c| free[c]).unwrap();
            finish[i] = ready.unwrap_or_default().max(free[core]) + trace.nanos;
            free[core] = finish[i];
            depth[i] = 1 + trace
                .reads
                .iter()
                .filter_map(|loc| written_depth.get(&key(loc)).copied())
                .max()
                .unwrap_or_default();
            for loc in trace.writes.iter().map(key) {
                let entry = written.entry(loc).or_default();
                *entry = (*entry).max(finish[i]);
                let entry = written_depth.entry(loc).or_default();
                *entry = (*entry).max(depth[i]);
            }
        }
        Self {
            total_nanos: traces.iter().map(|t| t.nanos).sum(),
            path_nanos: finish.iter().copied().max().unwrap_or_default(),
            path_txs: depth.iter().copied().max().unwrap_or_default(),
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
