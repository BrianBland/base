//! Optimistic parallel block execution with in-order, value-validated commits.
//!
//! Threads speculatively execute transactions against the latest committed state and record
//! every value they read. Whichever thread finds the frontier transaction committable takes the
//! single commit role and walks transactions in block order: if every value a speculative
//! execution read still matches the committed state, its writes are applied as-is (value-based
//! validation is sound because execution is a deterministic function of the values read);
//! otherwise the transaction is re-executed against the exact prefix state. Fee credits to the
//! beneficiary and fee vaults are deferred to commit time so they do not serialize every
//! transaction.
//!
//! Balances are validated by what execution observed rather than by value: see [`BalanceRead`].

use std::{
    cell::RefCell,
    collections::BTreeMap,
    convert::Infallible,
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering, fence},
    },
    time::Instant,
};

use alloy_consensus::transaction::SignerRecoverable;
use alloy_evm::{EvmEnv, EvmFactory, FromRecoveredTx};
use alloy_primitives::{
    Address, B256, Log, U256,
    map::{DefaultHashBuilder, HashMap as FastMap},
};
use base_common_consensus::{BaseTxEnvelope, Predeploys};
use base_common_genesis::BaseUpgrade;
use dashmap::{DashMap, DashSet};
use eyre::{Result, eyre};
use rayon::{ThreadPool, ThreadPoolBuilder};
use revm::{
    Database, DatabaseRef,
    context::{ContextSetters, TxEnv},
    context_interface::{
        Block, ContextTr, JournalTr, Transaction,
        cfg::gas::InitialAndFloorGas,
        result::{EVMError, ExecutionResult, FromStringError, ResultAndState, ResultGas},
    },
    database::BundleState,
    database_interface::WrapDatabaseRef,
    handler::{EvmTr, FrameResult, Handler, evm::FrameTr, handler::EvmTrError},
    inspector::NoOpInspector,
    interpreter::{GasTracker, interpreter_action::FrameInit},
    primitives::KECCAK_EMPTY,
    state::{AccountInfo, Bytecode, EvmState},
};

use crate::{
    BalanceOpcodes, BaseContext, BaseEvm, BaseEvmFactory, BaseHaltReason, BaseHandler, BaseSpecId,
    BaseTransaction, BaseTransactionError, BaseTxTr, DEPOSIT_TRANSACTION_TYPE, IsTxError,
    L1BlockInfo,
};

/// Thread-safe read-only state underlying a parallel execution.
pub trait ParallelDatabase: DatabaseRef<Error = Infallible> + Sync + std::fmt::Debug {}

impl<T: DatabaseRef<Error = Infallible> + Sync + std::fmt::Debug> ParallelDatabase for T {}

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

/// Immutable parent observations shared by worker-local readers and the external commit owner.
#[derive(Debug, Default)]
pub struct ParentReadCache {
    accounts: DashMap<Address, Option<AccountInfo>, DefaultHashBuilder>,
    storage: DashMap<(Address, U256), U256, DefaultHashBuilder>,
}

/// Committed state: the read-only pre-state plus an overlay of committed writes.
#[derive(Debug)]
pub struct Store<'a> {
    pre: &'a dyn ParallelDatabase,
    parent_cache: Option<Arc<ParentReadCache>>,
    accounts: Arc<DashMap<Address, Option<AccountInfo>, DefaultHashBuilder>>,
    storage: Arc<DashMap<(Address, U256), U256, DefaultHashBuilder>>,
    cleared_storage: Arc<DashSet<Address, DefaultHashBuilder>>,
    codes: Arc<DashMap<B256, Bytecode, DefaultHashBuilder>>,
}

impl<'a> Store<'a> {
    /// Creates an empty overlay over `pre`.
    pub fn new(pre: &'a dyn ParallelDatabase) -> Self {
        Self {
            pre,
            accounts: Arc::default(),
            parent_cache: None,
            storage: Arc::default(),
            cleared_storage: Arc::default(),
            codes: Arc::default(),
        }
    }

    /// Retains immutable worker reads for owner-side validation without constructing a provider.
    pub fn cache_parent_reads(&mut self) {
        self.parent_cache = Some(Arc::default());
    }

    /// Checks only authentic cached/committed values. Missing parent observations fail closed.
    pub fn is_cached_current(&self, read: &Read) -> bool {
        match read {
            Read::Account(address, seen, funds) => {
                let now = self.accounts.get(address).map(|v| v.clone()).or_else(|| {
                    self.parent_cache.as_ref()?.accounts.get(address).map(|v| v.clone())
                });
                now.is_some_and(|now| *seen == info_key(&now) && funds.admits(balance(&now)))
            }
            Read::Slot(address, key, seen) => {
                let now = self.storage.get(&(*address, *key)).map(|v| *v).or_else(|| {
                    if self.cleared_storage.contains(address) {
                        return Some(U256::ZERO);
                    }
                    self.parent_cache.as_ref()?.storage.get(&(*address, *key)).map(|v| *v)
                });
                now == Some(*seen)
            }
        }
    }

    /// Shares committed writes while reading untouched locations through a worker-owned parent.
    pub fn fork<'b>(&self, pre: &'b dyn ParallelDatabase) -> Store<'b> {
        Store {
            pre,
            accounts: Arc::clone(&self.accounts),
            parent_cache: self.parent_cache.clone(),
            storage: Arc::clone(&self.storage),
            cleared_storage: Arc::clone(&self.cleared_storage),
            codes: Arc::clone(&self.codes),
        }
    }

    /// Committed account.
    pub fn account(&self, address: Address) -> Option<AccountInfo> {
        if let Some(account) = self.accounts.get(&address) {
            return account.clone();
        }
        if let Some(cache) = &self.parent_cache {
            if let Some(value) = cache.accounts.get(&address) {
                return value.clone();
            }
            let value = self.pre.basic_ref(address).unwrap();
            cache.accounts.insert(address, value.clone());
            return value;
        }
        self.pre.basic_ref(address).unwrap()
    }

    /// Committed storage slot.
    pub fn slot(&self, address: Address, slot: U256) -> U256 {
        if let Some(value) = self.storage.get(&(address, slot)) {
            return *value;
        }
        if self.cleared_storage.contains(&address) {
            return U256::ZERO;
        }
        if let Some(cache) = &self.parent_cache {
            if let Some(value) = cache.storage.get(&(address, slot)) {
                return *value;
            }
            let value = self.pre.storage_ref(address, slot).unwrap();
            cache.storage.insert((address, slot), value);
            return value;
        }
        self.pre.storage_ref(address, slot).unwrap()
    }

    /// Whether a recorded observation admits the committed value.
    pub fn is_current(&self, read: &Read) -> bool {
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

    /// Prevents reads from reaching storage belonging to an earlier account incarnation.
    pub fn clear_storage(&self, address: Address) {
        self.storage.retain(|(owner, _), _| *owner != address);
        self.cleared_storage.insert(address);
    }

    /// Applies a validated execution, rebasing each balance it read by the committed drift.
    /// Returns whether any read balance had drifted.
    pub fn apply_state(&self, state: &EvmState, reads: &[Read]) -> bool {
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
            if account.is_selfdestructed() || account.is_created() {
                self.clear_storage(*address);
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

    /// Applies the sequential pre-execution and deposit prefix.
    pub fn apply_bundle(&self, bundle: &BundleState) {
        for (address, account) in &bundle.state {
            if account.status.is_storage_known() {
                self.clear_storage(*address);
            }
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

    /// Reconstructs the normal commit input from a validated speculative execution.
    pub fn reconstruct(
        &self,
        state: &mut EvmState,
        reads: &[Read],
        fees: &[(Address, U256)],
    ) -> bool {
        crate::SpeculativeResult::rebase(state, reads, fees, &mut WrapDatabaseRef(self))
            .unwrap_or_else(|never| match never {})
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
            bundle.state.get(address).and_then(|a| a.storage.get(slot)).map_or_else(
                || {
                    if bundle.state.get(address).is_some_and(|a| a.status.is_storage_known()) {
                        U256::ZERO
                    } else {
                        self.pre.storage_ref(*address, *slot).unwrap()
                    }
                },
                |s| s.present_value,
            )
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
#[derive(Debug)]
pub struct MvMemory {
    writes: DashMap<Loc, BTreeMap<usize, Value>, DefaultHashBuilder>,
    readers: DashMap<Loc, Readers, DefaultHashBuilder>,
    txs: usize,
}

/// Set of transactions that read a location. Registration after the first reader of a location
/// only takes a shared shard lock and sets a bit.
#[derive(Debug)]
pub struct Readers(Box<[AtomicU64]>);

impl Readers {
    /// Creates an empty set for a block of `txs` transactions.
    pub fn new(txs: usize) -> Self {
        Self((0..txs.div_ceil(64)).map(|_| AtomicU64::new(0)).collect())
    }

    /// Records `tx` as a reader.
    pub fn insert(&self, tx: usize) {
        self.0[tx / 64].fetch_or(1 << (tx % 64), Ordering::SeqCst);
    }

    /// Readers with an index above `tx`.
    pub fn above(&self, tx: usize) -> impl Iterator<Item = usize> + '_ {
        self.at_or_above(tx.saturating_add(1))
    }

    /// Readers at or above an external commit frontier.
    pub fn at_or_above(&self, first: usize) -> impl Iterator<Item = usize> + '_ {
        self.0.iter().enumerate().skip(first / 64).flat_map(move |(word, bits)| {
            let mut bits = bits.load(Ordering::SeqCst);
            if word == first / 64 {
                bits &= !0 << (first % 64);
            }
            std::iter::from_fn(move || {
                (bits != 0).then(|| {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    word * 64 + bit
                })
            })
        })
    }
}

impl MvMemory {
    /// Creates an empty multi-version memory for a block of `txs` transactions.
    pub fn new(txs: usize) -> Self {
        Self { writes: DashMap::default(), readers: DashMap::default(), txs }
    }

    /// Publishes without provider I/O while the builder holds this position's publication lock.
    /// Recorded observations supply the baseline for first writes; missing observations cause
    /// conservative invalidation instead of inventing committed state.
    pub fn publish_observed(
        &self,
        tx: usize,
        writes: &[(Loc, Value)],
        previous: &[(Loc, Value)],
        reads: &[Read],
    ) -> Vec<Loc> {
        let mut changed = Vec::new();
        for (loc, value) in writes {
            let below = self.latest(loc, tx).map(|(_, value)| value).or_else(|| {
                reads.iter().find_map(|read| match (loc, read) {
                    (Loc::Account(address), Read::Account(a, info, funds)) if address == a => {
                        Some(Value::Account(info.map(|(nonce, code_hash)| AccountInfo {
                            nonce,
                            code_hash,
                            balance: funds.seen,
                            code: None,
                            ..Default::default()
                        })))
                    }
                    (Loc::Slot(address, slot), Read::Slot(a, key, seen))
                        if address == a && slot == key =>
                    {
                        Some(Value::Slot(*seen))
                    }
                    _ => None,
                })
            });
            let old = self.writes.entry(*loc).or_default().insert(tx, value.clone());
            if let Some(old) = old.or(below) {
                changed.extend(loc.changes(&old, value));
            } else {
                changed.extend(Self::removed_locations(&[(*loc, value.clone())]));
            }
        }
        for (loc, value) in previous {
            if !writes.iter().any(|(key, _)| key == loc) {
                self.remove(tx, &[(*loc, value.clone())]);
                changed.extend(Self::removed_locations(&[(*loc, value.clone())]));
            }
        }
        changed
    }

    /// Actual writes, excluding call targets whose account was merely touched.
    pub fn candidate_writes(state: &EvmState, reads: &[Read]) -> Vec<(Loc, Value)> {
        state
            .iter()
            .filter(|(_, a)| a.is_touched())
            .flat_map(|(address, account)| {
                let info = (!account.is_selfdestructed() && !account.is_empty())
                    .then(|| account.info.clone());
                let unchanged = reads.iter().any(|read| {
                    matches!(read,
                Read::Account(a, seen, funds) if a == address
                    && *seen == info_key(&info) && funds.seen == balance(&info))
                });
                (!unchanged)
                    .then(|| (Loc::Account(*address), Value::Account(info)))
                    .into_iter()
                    .chain(account.changed_storage_slots().map(|(slot, value)| {
                        (Loc::Slot(*address, *slot), Value::Slot(value.present_value))
                    }))
            })
            .collect()
    }

    /// Readers invalidated by a lower writer. Duplicate indices are allowed.
    pub fn affected_readers(&self, tx: usize, changed: &[Loc]) -> Vec<usize> {
        changed
            .iter()
            .flat_map(|loc| {
                self.readers.get(loc).map(|r| r.above(tx).collect::<Vec<_>>()).unwrap_or_default()
            })
            .collect()
    }

    /// Registered readers affected by an unpredicted commit before the frontier.
    pub fn readers_from(&self, first: usize, changed: &[Loc]) -> Vec<usize> {
        changed
            .iter()
            .flat_map(|loc| {
                self.readers
                    .get(loc)
                    .map(|r| r.at_or_above(first).collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Read locations affected when a prediction is removed without committing.
    pub fn removed_locations(writes: &[(Loc, Value)]) -> Vec<Loc> {
        writes
            .iter()
            .flat_map(|(loc, _)| match loc {
                Loc::Account(address) => vec![*loc, Loc::Balance(*address)],
                _ => vec![*loc],
            })
            .collect()
    }

    /// Changes between the prediction and the owner's actual commit, including fees.
    pub fn committed_locations(
        store: &Store<'_>,
        writes: &[(Loc, Value)],
        previous: &[(Loc, Value)],
    ) -> Vec<Loc> {
        let mut changed = Vec::new();
        for (loc, value) in writes {
            let old = previous
                .iter()
                .find(|(key, _)| key == loc)
                .map(|(_, value)| value.clone())
                .or_else(|| match loc {
                    Loc::Account(address) | Loc::Balance(address) => {
                        store.accounts.get(address).map(|info| Value::Account(info.clone()))
                    }
                    Loc::Slot(address, slot) => {
                        store.storage.get(&(*address, *slot)).map(|value| Value::Slot(*value))
                    }
                });
            if let Some(old) = old {
                changed.extend(loc.changes(&old, value));
            } else {
                changed.extend(Self::removed_locations(&[(*loc, value.clone())]));
            }
        }
        for (loc, value) in previous {
            if !writes.iter().any(|(key, _)| key == loc) {
                changed.extend(Self::removed_locations(&[(*loc, value.clone())]));
            }
        }
        changed
    }

    /// Whether a result still admits its currently visible speculative prefix.
    pub fn reads_visible(&self, store: &Store<'_>, tx: usize, reads: &[Read]) -> bool {
        reads.iter().all(|read| match read {
            Read::Slot(address, slot, seen) => matches!(
                self.visible(store, &Loc::Slot(*address, *slot), tx),
                Value::Slot(now) if now == *seen
            ),
            Read::Account(address, info, funds) => {
                let Value::Account(now) = self.visible(store, &Loc::Account(*address), tx) else {
                    return false;
                };
                *info == info_key(&now) && funds.admits(balance(&now))
            }
        })
    }

    fn register(&self, loc: Loc, tx: usize) {
        match self.readers.get(&loc) {
            Some(readers) => readers.insert(tx),
            None => self.readers.entry(loc).or_insert_with(|| Readers::new(self.txs)).insert(tx),
        }
    }

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

    /// Locations whose visible value changes for higher transactions when frontier transaction
    /// `tx`, with `previous` published, commits `writes` without publishing them.
    fn frontier_changes(
        &self,
        store: &Store<'_>,
        tx: usize,
        writes: &[(Loc, Value)],
        previous: &[(Loc, Value)],
    ) -> Vec<Loc> {
        let written = writes
            .iter()
            .flat_map(|(loc, value)| loc.changes(&self.visible(store, loc, tx + 1), value));
        let unwritten = previous
            .iter()
            .filter(|(loc, _)| !writes.iter().any(|(l, _)| l == loc))
            .flat_map(|(loc, old)| loc.changes(old, &self.visible(store, loc, tx)));
        written.chain(unwritten).collect()
    }

    /// Removes every published write of a retired prediction.
    pub fn remove(&self, tx: usize, writes: &[(Loc, Value)]) {
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

impl Blocked {
    /// A provider returned code that does not match its requested hash; not a dependency.
    pub const INVALID_CODE: usize = usize::MAX;
}

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
    /// Caller and nonce of a speculated non-deposit transaction (see [`Self::assume_nonce`]).
    sender: Option<(Address, u64)>,
    verify_code: bool,
}

impl RecordingDb<'_> {
    /// Executes one candidate with recorded fee-parameter and EVM reads.
    /// Failure (including an unsupported transaction) is only a speculation miss.
    pub fn execute_candidate(
        factory: &BaseEvmFactory,
        env: EvmEnv<BaseSpecId>,
        store: &Store<'_>,
        tx: BaseTransaction<TxEnv>,
        forwarding: Option<(&MvMemory, &[AtomicU8], usize)>,
    ) -> Option<crate::SpeculativeResult> {
        Self::execute_recorded(factory, env, store, tx, forwarding, true).ok().flatten()
    }

    /// Constructs a reusable worker EVM with all balance-observing opcodes installed.
    pub fn candidate_evm<'a>(
        factory: &BaseEvmFactory,
        env: EvmEnv<BaseSpecId>,
        store: &'a Store<'a>,
    ) -> BaseEvm<RecordingDb<'a>, NoOpInspector> {
        let db = RecordingDb {
            store,
            speculation: None,
            tx: 0,
            reads: Vec::new(),
            accounts: FastMap::default(),
            sender: None,
            verify_code: true,
        };
        let mut evm = factory.create_evm(db, env);
        BalanceOpcodes::install(evm.all_mut().1);
        evm
    }

    /// Executes a prediction, retaining ESTIMATE dependencies for ordered rescheduling.
    pub fn execute_recorded(
        factory: &BaseEvmFactory,
        env: EvmEnv<BaseSpecId>,
        store: &Store<'_>,
        tx: BaseTransaction<TxEnv>,
        forwarding: Option<(&MvMemory, &[AtomicU8], usize)>,
        speculative: bool,
    ) -> Result<Option<crate::SpeculativeResult>, Blocked> {
        let mut evm = Self::candidate_evm(factory, env.clone(), store);
        Self::execute_reusing(&mut evm, env, tx, forwarding, speculative)
    }

    /// Runs a candidate on a worker-local EVM, resetting journal and error state after any result.
    pub fn execute_reusing<'a>(
        evm: &mut BaseEvm<RecordingDb<'a>, NoOpInspector>,
        env: EvmEnv<BaseSpecId>,
        tx: BaseTransaction<TxEnv>,
        forwarding: Option<(&'a MvMemory, &'a [AtomicU8], usize)>,
        speculative: bool,
    ) -> Result<Option<crate::SpeculativeResult>, Blocked> {
        if tx.tx_type() == DEPOSIT_TRANSACTION_TYPE
            || tx.tx_type() == crate::EIP8130_TRANSACTION_TYPE
            || env
                .cfg_env
                .spec
                .into_eth_spec()
                .is_enabled_in(revm::primitives::hardfork::SpecId::AMSTERDAM)
        {
            return Ok(None);
        }
        let db = evm.ctx_mut().db_mut();
        db.speculation = forwarding.map(|(mv, status, _)| (mv, status));
        db.tx = forwarding.map_or(0, |(_, _, index)| index);
        db.reads.clear();
        db.accounts.clear();
        db.sender = speculative.then_some((tx.caller(), tx.nonce()));
        let l1_info = L1BlockInfo::try_fetch(db, env.block_env.number, env.cfg_env.spec)?;
        evm.ctx_mut().set_tx(tx.clone());
        *evm.ctx_mut().chain_mut() = l1_info;
        let mut handler: LazyFeeHandler<_, EVMError<Blocked, BaseTransactionError>, _> =
            LazyFeeHandler::default();
        let result = handler.run(evm);
        *evm.ctx_mut().error() = Ok(());
        let state = evm.ctx_mut().journal_mut().finalize();
        let mut reads = std::mem::take(&mut evm.ctx_mut().db_mut().reads);
        let output = match result {
            Ok(result) => Ok(ResultAndState { result, state }),
            Err(EVMError::Transaction(error)) => {
                for read in &mut reads {
                    if let Read::Account(_, _, balance) = read {
                        *balance = BalanceRead::exact(balance.seen);
                    }
                }
                Err(error)
            }
            Err(EVMError::Database(blocked)) if blocked.0 == Blocked::INVALID_CODE => {
                return Ok(None);
            }
            Err(EVMError::Database(blocked)) => return Err(blocked),
            Err(_) => return Ok(None),
        };
        Ok(Some(crate::SpeculativeResult {
            read_validation_nanos: 0,
            rebase_nanos: 0,
            validation_lookups: 0,
            transaction: tx,
            environment: env,
            parent_hash: B256::ZERO,
            fees: if output.is_ok() { handler.fees.take() } else { Vec::new() },
            output,
            reads,
        }))
    }

    fn register(&self, loc: Loc) {
        if let Some((mv, _)) = self.speculation {
            mv.register(loc, self.tx);
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
        // Register before reading so a concurrent writer either sees us or we see it; pairs with
        // the fence between publishing and reading readers in `Scheduler::run`.
        self.register(loc);
        fence(Ordering::SeqCst);
        match mv.latest(&loc, self.tx) {
            Some((writer, _)) if status[writer].load(Ordering::SeqCst) != EXECUTED => {
                Err(Blocked(writer))
            }
            Some((_, value)) => Ok(value),
            None => Ok(mv.visible(self.store, &loc, self.tx)),
        }
    }

    /// Reads the sender without waiting on its earlier transactions: the latest executed version
    /// (or the committed one), carrying the nonce the transaction was signed with. Commit
    /// validation checks the assumed nonce like any read and rebases the balance, so the read is
    /// not registered for invalidation by the sender's earlier transactions.
    fn assume_nonce(&self, address: Address, nonce: u64) -> Option<AccountInfo> {
        let latest = self.speculation.and_then(|(mv, status)| {
            match mv.latest(&Loc::Account(address), self.tx)? {
                (writer, Value::Account(account))
                    if status[writer].load(Ordering::SeqCst) == EXECUTED =>
                {
                    Some(account)
                }
                _ => None,
            }
        });
        let account = latest.unwrap_or_else(|| self.store.account(address));
        account.map(|account| AccountInfo { nonce, ..account })
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
        let account = match self.sender {
            Some((sender, nonce)) if sender == address => self.assume_nonce(address, nonce),
            _ => {
                let Value::Account(account) = self.resolve(Loc::Account(address))? else {
                    unreachable!()
                };
                account
            }
        };
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
        let code = self.store.code_by_hash_ref(code_hash).unwrap();
        if self.verify_code && code.hash_slow() != code_hash {
            return Err(Blocked(Blocked::INVALID_CODE));
        }
        Ok(code)
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

/// Scheduling counters of one parallel execution. Every execution ends exactly once as committed,
/// blocked, invalidated, or failing commit validation, so
/// `executions == txs + blocked + invalidated + commit_fails`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    /// Total executions, including wasted ones.
    pub executions: usize,
    /// Executions aborted on reading a write of a transaction that is not executed.
    pub blocked: usize,
    /// Completed executions invalidated by a lower transaction's write before commit.
    pub invalidated: usize,
    /// Speculative results that failed commit validation.
    pub commit_fails: usize,
    /// Transactions committed although a balance they read had since changed.
    pub rebased: usize,
    /// Time spent holding the commit role: executing, validating, and applying frontier
    /// transactions.
    pub commit_nanos: u64,
    /// Thread time spent spinning or sleeping with nothing to execute or commit.
    pub idle_nanos: u64,
    /// Number of blocking waits after bounded idle polling.
    pub idle_waits: usize,
    /// Time spent in all executions, including wasted ones.
    pub execution_nanos: u64,
    /// Time spent in executions that aborted as blocked.
    pub blocked_nanos: u64,
    /// Time spent in the executions that were committed.
    pub committed_execution_nanos: u64,
    /// Reads recorded across all executions.
    pub reads: usize,
    /// Reads checked by commit validation.
    pub validated_reads: usize,
    /// Distinct locations with a registered speculative reader.
    pub reader_locs: usize,
    /// Pre-execution system calls, the L1 info deposit and scheduler setup.
    pub setup_nanos: u64,
    /// Commit-role time executing frontier transactions no thread had claimed.
    pub commit_exec_nanos: u64,
    /// Commit-role time validating speculative reads.
    pub validate_nanos: u64,
    /// Commit-role time re-executing transactions that failed validation.
    pub reexec_nanos: u64,
    /// Commit-role time applying writes, fee credits and multi-version cleanup.
    pub apply_nanos: u64,
    /// Wall time not spent with every thread in the scheduling loop: setup, thread start (EVM
    /// construction included), join and teardown.
    pub fixed_nanos: u64,
}

impl Stats {
    /// Adds `other`'s counters to `self`.
    pub const fn accumulate(&mut self, other: &Self) {
        self.executions += other.executions;
        self.blocked += other.blocked;
        self.invalidated += other.invalidated;
        self.commit_fails += other.commit_fails;
        self.rebased += other.rebased;
        self.commit_nanos += other.commit_nanos;
        self.idle_nanos += other.idle_nanos;
        self.idle_waits += other.idle_waits;
        self.execution_nanos += other.execution_nanos;
        self.blocked_nanos += other.blocked_nanos;
        self.committed_execution_nanos += other.committed_execution_nanos;
        self.reads += other.reads;
        self.validated_reads += other.validated_reads;
        self.reader_locs += other.reader_locs;
        self.setup_nanos += other.setup_nanos;
        self.commit_exec_nanos += other.commit_exec_nanos;
        self.validate_nanos += other.validate_nanos;
        self.reexec_nanos += other.reexec_nanos;
        self.apply_nanos += other.apply_nanos;
        self.fixed_nanos += other.fixed_nanos;
    }
}

/// Result of a parallel block execution.
#[derive(Debug)]
pub struct ParallelOutcome {
    /// Per-transaction outcomes.
    pub txs: Vec<TxOutcome>,
    /// In-order, rebased changes for normal executor commits.
    pub states: Vec<ResultAndState<BaseHaltReason>>,
    /// Senders recovered by workers while preparing transaction environments.
    pub signers: Vec<Address>,
    /// Scheduling counters.
    pub stats: Stats,
    /// Committed-execution traces (only when requested).
    pub traces: Vec<TxTrace>,
}

/// Scheduling policy of a parallel execution.
#[derive(Debug, Default, Clone, Copy)]
pub struct Schedule {
    /// Defer speculating a transaction until the same sender's previous transaction has
    /// executed. Speculative sender reads already assume the signed nonce, so this only avoids
    /// executions that would read the sender's stale balance or code.
    pub sender_gate: bool,
    /// Speculate only on transactions at most this far above the commit frontier (`None` =
    /// unbounded).
    pub window: Option<usize>,
}

/// Atomic execution phases shared by validator and builder scheduling.
#[derive(Debug)]
pub struct ExecutionStatus;

impl ExecutionStatus {
    /// Ready to execute, or an ESTIMATE of an invalidated incarnation.
    pub const PENDING: u8 = 0;
    /// Exclusively owned by one worker until final publication.
    pub const EXECUTING: u8 = 1;
    /// Published incarnation, including retired positions that no longer have writes.
    pub const EXECUTED: u8 = 2;
    /// External owner holds a proposal pending its admission decision.
    pub const TAKEN: u8 = 3;
}

/// Shared lock-free claim and invalidation state for ordered and external-commit execution.
#[derive(Debug)]
pub struct AtomicSchedule {
    /// Per-position execution phase.
    pub status: Vec<AtomicU8>,
    /// Lower publications observed while an incarnation executes.
    pub invalidations: Vec<AtomicU64>,
    /// ESTIMATE dependency, or `usize::MAX` when runnable.
    pub waiting: Vec<AtomicUsize>,
    /// First uncommitted position.
    pub frontier: AtomicUsize,
}

impl AtomicSchedule {
    /// Allocates a fixed-capacity plan. Uninitialized tail positions must not be scanned.
    pub fn new(capacity: usize) -> Self {
        Self {
            status: (0..capacity).map(|_| AtomicU8::new(PENDING)).collect(),
            invalidations: (0..capacity).map(|_| AtomicU64::new(0)).collect(),
            waiting: (0..capacity).map(|_| AtomicUsize::new(usize::MAX)).collect(),
            frontier: AtomicUsize::new(0),
        }
    }

    /// Exclusively claims a pending position whose ESTIMATE dependency has completed.
    pub fn claim(&self, tx: usize) -> bool {
        let waiting = self.waiting[tx].load(Ordering::SeqCst);
        if waiting != usize::MAX && self.status[waiting].load(Ordering::SeqCst) != EXECUTED {
            return false;
        }
        self.status[tx]
            .compare_exchange(PENDING, EXECUTING, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Marks an incarnation stale; final committed-value validation remains mandatory.
    pub fn invalidate(&self, tx: usize) -> bool {
        self.invalidations[tx].fetch_add(1, Ordering::SeqCst);
        self.status[tx]
            .compare_exchange(EXECUTED, PENDING, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

type Config = BaseEvmFactory;

const PENDING: u8 = ExecutionStatus::PENDING;
const EXECUTING: u8 = ExecutionStatus::EXECUTING;
const EXECUTED: u8 = ExecutionStatus::EXECUTED;

/// Commit-side state, only touched by the thread holding the commit role.
#[derive(Debug, Default)]
struct Committed {
    txs: Vec<TxOutcome>,
    states: Vec<ResultAndState<BaseHaltReason>>,
    cumulative_gas: u64,
    traces: Vec<TxTrace>,
    commit_fails: usize,
    nanos: u64,
    rebased: usize,
    execution_nanos: u64,
    validated_reads: usize,
    exec_nanos: u64,
    validate_nanos: u64,
    reexec_nanos: u64,
    apply_nanos: u64,
}

/// Generation-based notification; sampling before searching prevents lost wakeups.
#[derive(Debug, Default)]
pub struct WorkSignal {
    generation: AtomicU64,
    sleepers: AtomicUsize,
    sleep_lock: Mutex<()>,
    changed: Condvar,
}

impl WorkSignal {
    /// Captures the generation before looking for runnable work.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Notifies waiters after publishing work or stopping the scheduler.
    pub fn notify(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        if self.sleepers.load(Ordering::SeqCst) != 0 {
            let _guard = self.sleep_lock.lock().unwrap();
            self.changed.notify_all();
        }
    }

    /// Wakes one worker; a successful claim can chain the wake to another sleeper.
    pub fn notify_one(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        if self.sleepers.load(Ordering::SeqCst) != 0 {
            let _guard = self.sleep_lock.lock().unwrap();
            self.changed.notify_one();
        }
    }

    /// Sleeps unless a publisher has already advanced the generation.
    pub fn wait(&self, observed: u64, stop: &AtomicBool) {
        let guard = self.sleep_lock.lock().unwrap();
        self.sleepers.fetch_add(1, Ordering::SeqCst);
        let _guard = self
            .changed
            .wait_while(guard, |_| {
                self.generation.load(Ordering::SeqCst) == observed && !stop.load(Ordering::Relaxed)
            })
            .unwrap();
        self.sleepers.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Stops sibling workers before Rayon waits for them during panic propagation.
#[derive(Debug)]
pub struct StopOnUnwind<'a> {
    /// Shared cancellation flag.
    pub stop: &'a AtomicBool,
    /// Wakes sleeping siblings during unwinding.
    pub signal: &'a WorkSignal,
}

impl Drop for StopOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.stop.store(true, Ordering::Relaxed);
            self.signal.notify();
        }
    }
}

/// Shared scheduler state for one block.
struct Scheduler<'a> {
    config: &'a Config,
    env: EvmEnv<BaseSpecId>,
    /// L1 block info as the sequential handler caches it for the block.
    l1_info: L1BlockInfo,
    store: &'a Store<'a>,
    mv: &'a MvMemory,
    transactions: &'a [BaseTxEnvelope],
    /// Transaction environments, built by the first execution of each transaction so workers
    /// build them in parallel instead of serially before execution starts.
    txs: Vec<OnceLock<BaseTransaction<TxEnv>>>,
    core: AtomicSchedule,
    /// Previous transaction from the same sender when [`Schedule::sender_gate`] is set: its nonce
    /// and balance changes are known dependencies, so speculating before it executes is wasted.
    sender_prev: Vec<Option<usize>>,
    slots: Vec<Mutex<Option<Speculation>>>,
    stop: AtomicBool,
    signal: WorkSignal,
    idle_waits: AtomicUsize,
    /// Speculate only on transactions at most this far above the frontier (`None` = unbounded).
    window: Option<usize>,
    trace: bool,
    /// Whether a thread holds the commit role.
    committing: AtomicBool,
    committed: Mutex<Committed>,
    error: Mutex<Option<eyre::Report>>,
    idle_nanos: AtomicU64,
    executions: AtomicUsize,
    blocked: AtomicUsize,
    invalidated: AtomicUsize,
    execution_nanos: AtomicU64,
    blocked_nanos: AtomicU64,
    reads: AtomicUsize,
    #[cfg(test)]
    before_execution: Option<fn()>,
    #[cfg(test)]
    after_execution: Option<fn(&Scheduler<'_>)>,
    started: Instant,
    /// Nanoseconds after `started` at which the last thread entered the scheduling loop.
    all_entered: AtomicU64,
    /// Nanoseconds after `started` at which the first thread left the scheduling loop.
    first_left: AtomicU64,
}

/// An EVM owned by one thread and reused for every execution it runs, as the sequential executor
/// reuses one EVM for a whole block. The balance-recording opcodes are installed once.
type WorkerEvm<'a> = BaseEvm<RecordingDb<'a>, NoOpInspector>;

impl std::ops::Deref for Scheduler<'_> {
    type Target = AtomicSchedule;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Scheduler<'_> {
    #[cfg(feature = "scheduler-watchdog")]
    fn watch(&self, done: std::sync::mpsc::Receiver<()>) {
        use std::{sync::mpsc::RecvTimeoutError, time::Duration};

        let interval =
            Duration::from_millis(std::env::var("PEVM_STALL_MS").map_or(5000, |value| {
                value.parse::<u64>().expect("PEVM_STALL_MS must be milliseconds")
            }));
        let mut previous = None;
        while matches!(done.recv_timeout(interval), Err(RecvTimeoutError::Timeout)) {
            let frontier = self.frontier.load(Ordering::SeqCst);
            if previous == Some(frontier) {
                eprintln!(
                    "scheduler stalled: frontier={frontier}/{} committing={} stop={}",
                    self.txs.len(),
                    self.committing.load(Ordering::SeqCst),
                    self.stop.load(Ordering::SeqCst),
                );
                for tx in 0..self.txs.len() {
                    eprintln!(
                        "tx={tx} status={} waiting={} sender_prev={:?} invalidations={} slot={:?}",
                        self.status[tx].load(Ordering::SeqCst),
                        self.waiting[tx].load(Ordering::SeqCst),
                        self.sender_prev[tx],
                        self.invalidations[tx].load(Ordering::SeqCst),
                        self.slots[tx].try_lock().as_deref(),
                    );
                }
                std::process::exit(1);
            }
            previous = Some(frontier);
        }
    }

    fn tx_env(&self, tx: usize) -> &BaseTransaction<TxEnv> {
        self.txs[tx].get_or_init(|| {
            let transaction = &self.transactions[tx];
            let signer = transaction.recover_signer().expect("parallel sender recovery failed");
            BaseTransaction::from_recovered_tx(transaction, signer)
        })
    }

    fn evm<'s>(&'s self) -> WorkerEvm<'s> {
        let db = RecordingDb {
            store: self.store,
            speculation: None,
            tx: 0,
            reads: Vec::new(),
            accounts: FastMap::default(),
            sender: None,
            verify_code: false,
        };
        let mut evm = self.config.create_evm(db, self.env.clone());
        BalanceOpcodes::install(evm.all_mut().1);
        evm
    }

    fn execute<'s>(
        &'s self,
        evm: &mut WorkerEvm<'s>,
        tx: usize,
        speculative: bool,
    ) -> Result<Speculation, usize> {
        let start = Instant::now();
        #[cfg(test)]
        if let Some(before_execution) = self.before_execution {
            before_execution();
        }
        let db = evm.ctx_mut().db_mut();
        db.speculation = speculative.then_some((self.mv, self.status.as_slice()));
        db.tx = tx;
        db.accounts.clear();
        let tx_env = self.tx_env(tx);
        db.sender = (speculative && tx_env.tx_type() != DEPOSIT_TRANSACTION_TYPE)
            .then(|| (tx_env.caller(), tx_env.nonce()));
        evm.ctx_mut().set_tx(tx_env.clone());
        *evm.ctx_mut().chain_mut() = self.l1_info.clone();
        let mut handler: LazyFeeHandler<_, EVMError<Blocked, BaseTransactionError>, _> =
            LazyFeeHandler::default();
        self.executions.fetch_add(1, Ordering::Relaxed);
        let result = handler.run(evm);
        // Reset everything a failed run can leave behind so the next execution starts clean.
        *evm.ctx_mut().error() = Ok(());
        let state = evm.ctx_mut().journal_mut().finalize();
        let reads = std::mem::take(&mut evm.ctx_mut().db_mut().reads);
        let nanos = start.elapsed().as_nanos() as u64;
        self.execution_nanos.fetch_add(nanos, Ordering::Relaxed);
        let result = match result {
            Ok(result) => Some(result),
            Err(EVMError::Database(Blocked(writer))) => {
                self.blocked_nanos.fetch_add(nanos, Ordering::Relaxed);
                return Err(writer);
            }
            Err(_) => None,
        };
        self.reads.fetch_add(reads.len(), Ordering::Relaxed);
        // Merely touched accounts (every call target) keep the value the transaction read, so
        // publishing them would only make higher readers block on this transaction.
        let writes = MvMemory::candidate_writes(&state, &reads);
        Ok(Speculation { result, state, reads, writes, fees: handler.fees.take(), nanos })
    }

    /// Speculatively runs a transaction the caller moved to `EXECUTING`, publishes its writes, and
    /// invalidates higher transactions that read a location whose value changed.
    fn run<'s>(&'s self, evm: &mut WorkerEvm<'s>, tx: usize) {
        let seen = self.invalidations[tx].load(Ordering::SeqCst);
        let spec = match self.execute(evm, tx, true) {
            Ok(spec) => spec,
            Err(writer) => {
                self.blocked.fetch_add(1, Ordering::Relaxed);
                self.waiting[tx].store(writer, Ordering::SeqCst);
                self.status[tx].store(PENDING, Ordering::SeqCst);
                self.signal.notify();
                return;
            }
        };
        let previous = self.slots[tx].lock().unwrap().take().map(|s| s.writes).unwrap_or_default();
        let changed = self.mv.publish(self.store, tx, &spec.writes, &previous);
        fence(Ordering::SeqCst);
        self.invalidate_readers(tx, changed);
        let affected = self.invalidations[tx].load(Ordering::SeqCst) != seen
            && !self.reads_visible(tx, &spec.reads);
        *self.slots[tx].lock().unwrap() = Some(spec);
        // Publication transfers ownership: a newer incarnation may execute and commit immediately.
        // Racing invalidations missed here are still caught by committed-value validation.
        let status = if affected {
            self.invalidated.fetch_add(1, Ordering::Relaxed);
            PENDING
        } else {
            EXECUTED
        };
        self.status[tx].store(status, Ordering::SeqCst);
        self.signal.notify();
        #[cfg(test)]
        if let Some(after_execution) = self.after_execution {
            after_execution(self);
        }
    }

    /// Whether every read of `tx`'s execution still admits the value now visible to it, so a
    /// lower write published while it ran did not affect it. Commit validation decides
    /// regardless; this only spares re-executing transactions the write did not touch.
    fn reads_visible(&self, tx: usize, reads: &[Read]) -> bool {
        reads.iter().all(|read| match read {
            Read::Slot(address, slot, seen) => matches!(
                self.mv.visible(self.store, &Loc::Slot(*address, *slot), tx),
                Value::Slot(now) if now == *seen
            ),
            Read::Account(address, info, balance) => {
                let Value::Account(now) = self.mv.visible(self.store, &Loc::Account(*address), tx)
                else {
                    return false;
                };
                let Value::Account(funds) =
                    self.mv.visible(self.store, &Loc::Balance(*address), tx)
                else {
                    return false;
                };
                *info == info_key(&now)
                    && balance.admits(funds.as_ref().map_or(U256::ZERO, |a| a.balance))
            }
        })
    }

    /// Invalidates the transactions above `tx` that read any of the `changed` locations.
    fn invalidate_readers(&self, tx: usize, changed: Vec<Loc>) {
        for loc in changed {
            if let Some(readers) = self.mv.readers.get(&loc) {
                for reader in readers.above(tx) {
                    if self.core.invalidate(reader) {
                        self.invalidated.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn claim(&self, tx: usize) -> bool {
        if self.sender_prev[tx]
            .is_some_and(|dep| self.status[dep].load(Ordering::SeqCst) != EXECUTED)
        {
            return false;
        }
        self.core.claim(tx)
    }

    /// Executes the lowest pending transaction within the window above the commit frontier, if
    /// any.
    fn step<'s>(&'s self, evm: &mut WorkerEvm<'s>) -> bool {
        let from = self.frontier.load(Ordering::SeqCst) + 1;
        let to = self.window.map_or(self.txs.len(), |w| self.txs.len().min(from + w));
        (from..to).find(|&tx| self.claim(tx)).map(|tx| self.run(evm, tx)).is_some()
    }

    /// Commits frontier transactions while any is committable. Every thread calls this between
    /// executions, so whichever thread makes the frontier transaction committable commits it
    /// instead of waiting on a dedicated committer.
    fn commit<'s>(&'s self, evm: &mut WorkerEvm<'s>) {
        while !self.stop.load(Ordering::Relaxed) {
            let i = self.frontier.load(Ordering::SeqCst);
            if i == self.txs.len() {
                self.stop.store(true, Ordering::Relaxed);
                return;
            }
            // A thread that makes the frontier committable while another holds the role fails
            // the exchange; the holder re-checks after releasing, so the frontier never strands.
            if self.status[i].load(Ordering::SeqCst) == EXECUTING
                || self
                    .committing
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                return;
            }
            let result = self.commit_ready(evm);
            self.committing.store(false, Ordering::SeqCst);
            self.signal.notify();
            if let Err(error) = result {
                *self.error.lock().unwrap() = Some(error);
                self.stop.store(true, Ordering::Relaxed);
                return;
            }
        }
    }

    /// Commits transactions in order from the frontier until one is still executing.
    fn commit_ready<'s>(&'s self, evm: &mut WorkerEvm<'s>) -> Result<()> {
        let started = Instant::now();
        let mut committed = self.committed.lock().unwrap();
        let mut i = self.frontier.load(Ordering::SeqCst);
        while i < self.txs.len() && !self.stop.load(Ordering::Relaxed) {
            // Every lower transaction is committed, so an execution here reads the exact prefix
            // state, needs no validation, and is committed without being published.
            const AT_FRONTIER: &str =
                "executions at the frontier read committed state and never block";
            let phase = Instant::now();
            let (spec, previous) = if self.claim(i) {
                let previous = self.slots[i].lock().unwrap().take().map(|s| s.writes);
                let spec = self.execute(evm, i, false).expect(AT_FRONTIER);
                committed.exec_nanos += phase.elapsed().as_nanos() as u64;
                (spec, Some(previous.unwrap_or_default()))
            } else if self.status[i].load(Ordering::SeqCst) == EXECUTED {
                let spec = self.slots[i].lock().unwrap().take().unwrap();
                committed.validated_reads += spec.reads.len();
                let valid =
                    spec.result.is_some() && spec.reads.iter().all(|r| self.store.is_current(r));
                committed.validate_nanos += phase.elapsed().as_nanos() as u64;
                if valid {
                    (spec, None)
                } else {
                    let phase = Instant::now();
                    committed.commit_fails += 1;
                    self.status[i].store(EXECUTING, Ordering::SeqCst);
                    let reexecuted = self.execute(evm, i, false).expect(AT_FRONTIER);
                    committed.reexec_nanos += phase.elapsed().as_nanos() as u64;
                    (reexecuted, Some(spec.writes))
                }
            } else {
                break;
            };
            self.apply(&mut committed, i, spec, previous)?;
            i += 1;
            self.frontier.store(i, Ordering::SeqCst);
            self.signal.notify();
        }
        committed.nanos += started.elapsed().as_nanos() as u64;
        Ok(())
    }

    /// Commits `spec` as transaction `i`. `previous` is `None` if `spec` is the published
    /// speculative execution, and otherwise holds the writes `i`'s earlier execution published,
    /// which `spec` (executed at the frontier, unpublished) supersedes.
    fn apply(
        &self,
        committed: &mut Committed,
        i: usize,
        mut spec: Speculation,
        previous: Option<Vec<(Loc, Value)>>,
    ) -> Result<()> {
        let Some(result) = &spec.result else {
            let index = committed.txs.len();
            return Err(eyre!("transaction {index} failed at its exact prefix state"));
        };
        committed.cumulative_gas += result.tx_gas_used();
        committed.execution_nanos += spec.nanos;
        committed.txs.push(TxOutcome {
            success: result.is_success(),
            cumulative_gas: committed.cumulative_gas,
            logs: result.logs().to_vec(),
        });
        if self.trace {
            // A balance constraint binds only if the block's pre-state violates it; the sender's
            // nonce is assumed, so only a code change binds its read.
            let tx = self.tx_env(i);
            let sender = (tx.tx_type() != DEPOSIT_TRANSACTION_TYPE).then(|| tx.caller());
            let reads = spec
                .reads
                .iter()
                .flat_map(|read| {
                    let Read::Account(address, info, constraint) = read else {
                        return [Some(read.loc()), None];
                    };
                    let pre = self.store.pre.basic_ref(*address).unwrap();
                    let code = |info: Option<(u64, B256)>| info.map(|(_, code)| code);
                    let assumed = sender == Some(*address) && code(*info) == code(info_key(&pre));
                    [
                        (!assumed).then_some(read.loc()),
                        (!constraint.admits(balance(&pre))).then_some(Loc::Balance(*address)),
                    ]
                })
                .flatten()
                .collect();
            let writes = spec
                .writes
                .iter()
                .flat_map(|(loc, value)| loc.changes(&self.mv.visible(self.store, loc, 0), value))
                .chain(
                    spec.fees.iter().filter(|(_, a)| !a.is_zero()).map(|(r, _)| Loc::Balance(*r)),
                )
                .collect();
            committed.traces.push(TxTrace { reads, writes, nanos: spec.nanos });
        }
        let phase = Instant::now();
        let changed = previous
            .as_ref()
            .map(|previous| self.mv.frontier_changes(self.store, i, &spec.writes, previous));
        committed.rebased +=
            usize::from(self.store.reconstruct(&mut spec.state, &spec.reads, &spec.fees));
        self.store.apply_state(&spec.state, &[]);
        committed
            .states
            .push(ResultAndState { result: spec.result.take().unwrap(), state: spec.state });
        self.mv.remove(i, previous.as_deref().unwrap_or(&spec.writes));
        if let Some(changed) = changed {
            // Pairs with the fence between registering and reading in `RecordingDb::resolve`:
            // a reader this misses reads the committed value.
            fence(Ordering::SeqCst);
            self.invalidate_readers(i, changed);
            self.status[i].store(EXECUTED, Ordering::SeqCst);
        }
        committed.apply_nanos += phase.elapsed().as_nanos() as u64;
        Ok(())
    }

    fn work(&self) {
        let _stop_on_unwind = StopOnUnwind { stop: &self.stop, signal: &self.signal };
        let mut idle_scans = 0;
        let since_start = || self.started.elapsed().as_nanos() as u64;
        let mut evm = self.evm();
        self.all_entered.fetch_max(since_start(), Ordering::Relaxed);
        while !self.stop.load(Ordering::Relaxed) {
            let generation = self.signal.generation();
            self.commit(&mut evm);
            if !self.stop.load(Ordering::Relaxed) && !self.step(&mut evm) {
                let idle = Instant::now();
                if idle_scans < 8 {
                    idle_scans += 1;
                    std::hint::spin_loop();
                } else {
                    self.idle_waits.fetch_add(1, Ordering::Relaxed);
                    self.signal.wait(generation, &self.stop);
                    idle_scans = 0;
                }
                self.idle_nanos.fetch_add(idle.elapsed().as_nanos() as u64, Ordering::Relaxed);
            } else {
                idle_scans = 0;
            }
        }
        self.signal.notify();
        self.first_left.fetch_min(since_start(), Ordering::Relaxed);
    }
}

/// The threads that execute blocks: the calling thread plus a persistent pool for the rest, so a
/// block does not pay for spawning and joining threads.
#[derive(Debug)]
pub struct Workers {
    pool: Option<ThreadPool>,
}

impl Workers {
    /// Creates `threads` workers, starting a pool for all but the calling thread.
    pub fn new(threads: usize) -> Result<Self> {
        let pool = (threads > 1)
            .then(|| ThreadPoolBuilder::new().num_threads(threads - 1).build())
            .transpose()?;
        Ok(Self { pool })
    }

    /// Runs a coordinator on the persistent pool while the caller services its reads.
    pub fn dispatch<T>(&self, work: impl FnOnce() + Send, service: impl FnOnce() -> T) -> T {
        self.pool.as_ref().expect("database service requires a worker pool").in_place_scope(
            |scope| {
                scope.spawn(|_| work());
                service()
            },
        )
    }

    /// Runs `work` once on every worker and returns when all have finished.
    pub fn run(&self, work: impl Fn() + Sync) {
        match &self.pool {
            Some(pool) => pool.in_place_scope(|scope| {
                scope.spawn_broadcast(|_, _| work());
                work();
            }),
            None => work(),
        }
    }

    /// Drops `value` on a pool thread, off the block's critical path.
    pub fn discard(&self, value: impl Send + 'static) {
        match &self.pool {
            Some(pool) => pool.spawn(move || drop(value)),
            None => drop(value),
        }
    }
}

impl ParallelOutcome {
    /// Executes transactions against an already prepared prefix state.
    /// The caller must discard the entire overlay on failure or unwind.
    pub fn execute(
        config: &Config,
        env: EvmEnv<BaseSpecId>,
        transactions: &[BaseTxEnvelope],
        store: &Store<'_>,
        workers: &Workers,
        schedule: Schedule,
        trace: bool,
    ) -> Result<Self> {
        let started = Instant::now();
        let l1_info = L1BlockInfo::try_fetch(
            &mut WrapDatabaseRef(store),
            env.block_env.number,
            env.cfg_env.spec,
        )
        .unwrap_or_else(|never| match never {});

        let n = transactions.len();
        let mut last_by_sender = FastMap::<Address, usize>::default();
        let sender_prev = transactions
            .iter()
            .enumerate()
            .map(|(tx, transaction)| {
                if schedule.sender_gate {
                    let signer =
                        transaction.recover_signer().expect("parallel sender recovery failed");
                    last_by_sender.insert(signer, tx)
                } else {
                    None
                }
            })
            .collect();
        let mv = MvMemory::new(n);
        let scheduler = Scheduler {
            config,
            env,
            l1_info,
            store,
            mv: &mv,
            transactions,
            txs: (0..n).map(|_| OnceLock::new()).collect(),
            core: AtomicSchedule::new(n),
            sender_prev,
            slots: (0..n).map(|_| Mutex::new(None)).collect(),
            stop: AtomicBool::new(false),
            signal: WorkSignal::default(),
            idle_waits: AtomicUsize::new(0),
            window: schedule.window,
            trace,
            committing: AtomicBool::new(false),
            committed: Mutex::default(),
            error: Mutex::new(None),
            idle_nanos: AtomicU64::new(0),
            executions: AtomicUsize::new(0),
            blocked: AtomicUsize::new(0),
            invalidated: AtomicUsize::new(0),
            execution_nanos: AtomicU64::new(0),
            blocked_nanos: AtomicU64::new(0),
            reads: AtomicUsize::new(0),
            #[cfg(test)]
            before_execution: None,
            #[cfg(test)]
            after_execution: None,
            started,
            all_entered: AtomicU64::new(0),
            first_left: AtomicU64::new(u64::MAX),
        };
        let setup_nanos = started.elapsed().as_nanos() as u64;
        #[cfg(feature = "scheduler-watchdog")]
        std::thread::scope(|scope| {
            let (done, receiver) = std::sync::mpsc::channel();
            scope.spawn(|| scheduler.watch(receiver));
            workers.run(|| scheduler.work());
            drop(done);
        });
        #[cfg(not(feature = "scheduler-watchdog"))]
        workers.run(|| scheduler.work());
        let error = scheduler.error.lock().unwrap().take();
        let committed = std::mem::take(&mut *scheduler.committed.lock().unwrap());
        let mut stats = Stats {
            executions: scheduler.executions.load(Ordering::Relaxed),
            blocked: scheduler.blocked.load(Ordering::Relaxed),
            invalidated: scheduler.invalidated.load(Ordering::Relaxed),
            commit_fails: committed.commit_fails,
            rebased: committed.rebased,
            commit_nanos: committed.nanos,
            idle_nanos: scheduler.idle_nanos.load(Ordering::Relaxed),
            idle_waits: scheduler.idle_waits.load(Ordering::Relaxed),
            execution_nanos: scheduler.execution_nanos.load(Ordering::Relaxed),
            blocked_nanos: scheduler.blocked_nanos.load(Ordering::Relaxed),
            committed_execution_nanos: committed.execution_nanos,
            reads: scheduler.reads.load(Ordering::Relaxed),
            validated_reads: committed.validated_reads,
            reader_locs: scheduler.mv.readers.len(),
            setup_nanos,
            commit_exec_nanos: committed.exec_nanos,
            validate_nanos: committed.validate_nanos,
            reexec_nanos: committed.reexec_nanos,
            apply_nanos: committed.apply_nanos,
            fixed_nanos: 0,
        };
        let loop_nanos = scheduler
            .first_left
            .load(Ordering::Relaxed)
            .saturating_sub(scheduler.all_entered.load(Ordering::Relaxed));
        let signers =
            scheduler.txs.iter().filter_map(|tx| tx.get().map(|tx| tx.caller())).collect();
        drop(scheduler);
        // Every written location and reader set is a separate allocation.
        workers.discard(mv);
        stats.fixed_nanos = started.elapsed().as_nanos() as u64 - loop_nanos;
        if let Some(error) = error {
            return Err(error);
        }
        Ok(Self {
            txs: committed.txs,
            states: committed.states,
            signers,
            stats,
            traces: committed.traces,
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

#[cfg(test)]
mod tests {
    use alloy_primitives::TxKind;
    use revm::{
        context::{BlockEnv, CfgEnv},
        database::{CacheDB, EmptyDB},
    };

    use super::*;

    fn finish_predecessor_and_commit(scheduler: &Scheduler<'_>) {
        let mut evm = scheduler.evm();
        scheduler.commit_ready(&mut evm).unwrap();
        assert_eq!(scheduler.committed.lock().unwrap().txs.len(), 2);
    }

    fn late_execution_tail(sender_gate: bool, panic: bool) {
        let config = Config::default();
        let sender = Address::repeat_byte(1);
        let recipient = Address::repeat_byte(2);
        let mut pre = CacheDB::new(EmptyDB::default());
        pre.insert_account_info(
            sender,
            AccountInfo { balance: U256::from(1_000_000_000), ..Default::default() },
        );
        let store = Store::new(&pre);
        let scheduler = Scheduler {
            config: &config,
            env: EvmEnv::new(
                CfgEnv::new_with_spec(BaseSpecId::new(BaseUpgrade::Bedrock)),
                BlockEnv { gas_limit: 30_000_000, ..Default::default() },
            ),
            l1_info: L1BlockInfo::default(),
            store: &store,
            mv: &MvMemory::new(3),
            transactions: &[],
            txs: (0..3)
                .map(|nonce| {
                    OnceLock::from(BaseTransaction {
                        base: TxEnv {
                            caller: sender,
                            kind: TxKind::Call(recipient),
                            nonce,
                            value: U256::from(1),
                            gas_limit: 100_000,
                            chain_id: None,
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                })
                .collect(),
            core: AtomicSchedule {
                status: [PENDING, PENDING, EXECUTING].map(AtomicU8::new).into(),
                invalidations: (0..3).map(|_| AtomicU64::new(0)).collect(),
                waiting: [usize::MAX, usize::MAX, if sender_gate { usize::MAX } else { 1 }]
                    .map(AtomicUsize::new)
                    .into(),
                frontier: AtomicUsize::new(0),
            },
            sender_prev: vec![None, None, sender_gate.then_some(1)],
            slots: (0..3).map(|_| Mutex::new(None)).collect(),
            stop: AtomicBool::new(false),
            signal: WorkSignal::default(),
            idle_waits: AtomicUsize::new(0),
            window: None,
            trace: false,
            committing: AtomicBool::new(false),
            committed: Mutex::default(),
            error: Mutex::default(),
            idle_nanos: AtomicU64::new(0),
            executions: AtomicUsize::new(0),
            blocked: AtomicUsize::new(0),
            invalidated: AtomicUsize::new(0),
            execution_nanos: AtomicU64::new(0),
            blocked_nanos: AtomicU64::new(0),
            reads: AtomicUsize::new(0),
            before_execution: panic.then_some(|| panic!("injected execution panic")),
            after_execution: Some(finish_predecessor_and_commit),
            started: Instant::now(),
            all_entered: AtomicU64::new(0),
            first_left: AtomicU64::new(u64::MAX),
        };
        if panic {
            Workers::new(4).unwrap().run(|| scheduler.work());
            return;
        }
        let mut evm = scheduler.evm();
        assert!(scheduler.claim(1));
        scheduler.run(&mut evm, 1);
        scheduler.status[2].store(PENDING, Ordering::SeqCst);
        assert!(!scheduler.claim(1), "committed transaction must not execute again");
        scheduler.commit_ready(&mut evm).unwrap();
        assert_eq!(scheduler.committed.lock().unwrap().txs.len(), 3);
        assert_eq!(store.account(recipient).unwrap().balance, U256::from(3));
        assert_eq!(scheduler.executions.load(Ordering::Relaxed), 4);
        assert_eq!(scheduler.invalidated.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn recreation_clears_parent_and_overlay_storage() {
        let address = Address::repeat_byte(3);
        let mut pre = CacheDB::new(EmptyDB::default());
        pre.insert_account_storage(address, U256::ZERO, U256::from(7)).unwrap();
        let store = Store::new(&pre);
        store.storage.insert((address, U256::from(1)), U256::from(8));
        let mut account =
            revm::state::Account::from(AccountInfo { nonce: 1, ..Default::default() });
        account.mark_touch();
        account.mark_selfdestruct();
        store.apply_state(&[(address, account.clone())].into_iter().collect(), &[]);
        account.unmark_selfdestruct();
        account.mark_created();
        store.apply_state(&[(address, account)].into_iter().collect(), &[]);
        assert_eq!(store.slot(address, U256::ZERO), U256::ZERO);
        assert_eq!(store.slot(address, U256::from(1)), U256::ZERO);
    }

    #[test]
    fn panicking_execution_releases_all_workers() {
        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| late_execution_tail(false, true));
            sent.send(result.is_err()).unwrap();
        });
        assert!(received.recv_timeout(std::time::Duration::from_secs(5)).expect("workers hung"));
    }

    #[test]
    fn late_execution_tail_does_not_strand_blocked_reader() {
        late_execution_tail(false, false);
    }

    #[test]
    fn late_execution_tail_does_not_strand_sender_gate() {
        late_execution_tail(true, false);
    }

    #[test]
    fn readers_above_returns_strictly_higher_readers_across_words() {
        let readers = Readers::new(200);
        for tx in [0, 5, 63, 64, 65, 127, 128, 199] {
            readers.insert(tx);
        }
        readers.insert(64);
        assert_eq!(readers.above(0).collect::<Vec<_>>(), [5, 63, 64, 65, 127, 128, 199]);
        assert_eq!(readers.above(63).collect::<Vec<_>>(), [64, 65, 127, 128, 199]);
        assert_eq!(readers.above(64).collect::<Vec<_>>(), [65, 127, 128, 199]);
        assert_eq!(readers.above(127).collect::<Vec<_>>(), [128, 199]);
        assert_eq!(readers.above(199).count(), 0);
        assert_eq!(readers.above(usize::MAX).count(), 0);
    }

    #[test]
    fn workers_run_work_once_on_each_of_their_threads_every_time() {
        for threads in [1, 3] {
            let workers = Workers::new(threads).unwrap();
            for _ in 0..2 {
                let ran = Mutex::new(Vec::new());
                workers.run(|| ran.lock().unwrap().push(std::thread::current().id()));
                let ran = ran.into_inner().unwrap();
                let distinct: std::collections::HashSet<_> = ran.iter().collect();
                assert_eq!((ran.len(), distinct.len()), (threads, threads));
            }
        }
    }
}
