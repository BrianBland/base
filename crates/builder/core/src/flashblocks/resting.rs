//! Payload iterators that hold back validity transactions resting under an unchanged predicate.

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use alloy_primitives::{TxHash, map::B256Set};
use base_execution_payload_builder::ParkedPredicateIndex;
use base_execution_txpool::{BasePooledTx, ParkingFilter, ValidityPredicate};
use parking_lot::Mutex;
use reth_transaction_pool::ValidPoolTransaction;
use revm::state::EvmState;

use crate::RejectionCache;

/// Work an iterator spent holding back resting transactions since it was last asked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestingStats {
    /// Resting transactions parked without being yielded.
    pub parked: u64,
    /// Time spent filtering resting transactions, from the first resting transaction parked
    /// during each `next` call of the iterator until it returns, so it includes advancing the
    /// iterator between resting transactions.
    pub duration: Duration,
}

/// Lifecycle callbacks for validity transactions resting in a payload job.
///
/// A transaction rests once the build loop finds one of its predicates unsatisfied. It stays
/// unsatisfied until a later commit in the same block changes the state that predicate reads, so
/// an iterator implementing this trait may hold it back in later flashblocks instead of yielding
/// it to be evaluated again. The default methods hold nothing back.
pub trait RestingPayloadTransactions {
    /// Records that `predicate` was unsatisfied for `transaction_hash` at the current build
    /// position.
    fn rest(&mut self, _transaction_hash: TxHash, _predicate: &ValidityPredicate) {}

    /// Wakes transactions resting on state changed by one committed transaction.
    fn record_committed_state(&mut self, _state: &EvmState) {}

    /// Returns whether a transaction with `predicates` rests under an unchanged predicate.
    fn is_resting(&self, _transaction_hash: TxHash, _predicates: &[ValidityPredicate]) -> bool {
        false
    }

    /// Returns and resets the work spent holding back resting transactions.
    fn take_resting_stats(&mut self) -> RestingStats {
        RestingStats::default()
    }
}

/// Block-lived resting bookkeeping guarded by [`RestingFilter`].
#[derive(Debug, Default)]
pub struct RestingState {
    /// Transactions resting in this block, indexed by the predicate last found unsatisfied.
    ///
    /// A committed transaction never rests, so the filter need not consult the committed set.
    pub resting: ParkedPredicateIndex<()>,
    /// Resting transactions the filter parked in the current candidate iterator. Transactions
    /// the build loop parked are woken by its own predicate index instead.
    pub parked: B256Set,
    /// Work accumulated since the stats were last taken.
    pub stats: RestingStats,
    /// When the filter first parked a transaction during the current `next` call.
    pub first_park: Option<Instant>,
    /// Every resting park in order, so tests can compare park sequences.
    #[cfg(test)]
    pub park_sequence: Vec<TxHash>,
}

impl RestingState {
    /// A hash re-added to the pool with a batch that no longer contains the recorded predicate
    /// does not rest.
    pub fn is_resting(&self, transaction_hash: TxHash, predicates: &[ValidityPredicate]) -> bool {
        self.resting.predicate(transaction_hash).is_some_and(|blocker| predicates.contains(blocker))
    }
}

/// Parks resting transactions inside the candidate iterator at the moment it would yield them.
///
/// The filter is shared between [`BestFlashblocksTxs`](crate::BestFlashblocksTxs), which records
/// rests, wakes and commits, and the lane-parking iterator, which asks it about each candidate.
/// It parks exactly the candidates that the flashblocks adapter would park on receipt: resting
/// ones that are neither committed nor permanently rejected.
#[derive(Debug)]
pub struct RestingFilter {
    rejection_cache: RejectionCache,
    state: Mutex<RestingState>,
    parked_during_next: AtomicBool,
}

impl RestingFilter {
    /// Creates an empty filter that defers to `rejection_cache` for rejected transactions.
    pub fn new(rejection_cache: RejectionCache) -> Self {
        Self {
            rejection_cache,
            state: Mutex::default(),
            parked_during_next: AtomicBool::new(false),
        }
    }

    /// Locks the resting bookkeeping.
    pub fn state(&self) -> parking_lot::MutexGuard<'_, RestingState> {
        self.state.lock()
    }

    /// Records `transaction` as parked by this filter if it rests, without consulting the
    /// rejection cache.
    ///
    /// The flashblocks adapter calls this after its own rejection-cache miss for a candidate the
    /// filter let through. The filter only lets a resting candidate through when the cache holds
    /// it, so this parks exactly the candidates whose cache entry expired or was evicted in
    /// between, as the adapter's resting check did before the filter existed.
    pub fn park_if_resting_after_cache_miss<T: BasePooledTx>(
        &self,
        transaction: &ValidPoolTransaction<T>,
    ) -> bool {
        self.park_if_resting(transaction, |_| false)
    }

    /// Records `transaction` as parked by this filter if it rests under an unchanged predicate
    /// and is not `rejected`.
    fn park_if_resting<T: BasePooledTx>(
        &self,
        transaction: &ValidPoolTransaction<T>,
        rejected: impl FnOnce(&TxHash) -> bool,
    ) -> bool {
        let predicates = transaction.transaction.validity_predicates();
        if predicates.is_empty() {
            return false;
        }
        let hash = *transaction.hash();
        let mut state = self.state.lock();
        if !state.is_resting(hash, predicates) || rejected(&hash) {
            return false;
        }
        state.parked.insert(hash);
        #[cfg(test)]
        state.park_sequence.push(hash);
        state.stats.parked += 1;
        if state.first_park.is_none() {
            state.first_park = Some(Instant::now());
            self.parked_during_next.store(true, Ordering::Relaxed);
        }
        true
    }

    /// Ends a `next` call, adding the time since its first resting park to the stats.
    pub fn finish_next(&self) {
        if !self.parked_during_next.load(Ordering::Relaxed) {
            return;
        }
        self.parked_during_next.store(false, Ordering::Relaxed);
        let mut state = self.state.lock();
        if let Some(start) = state.first_park.take() {
            state.stats.duration += start.elapsed();
        }
    }
}

impl<T> ParkingFilter<T> for RestingFilter
where
    T: BasePooledTx,
{
    fn should_park(&self, transaction: &ValidPoolTransaction<T>) -> bool {
        self.park_if_resting(transaction, |hash| self.rejection_cache.is_rejected(hash))
    }
}
