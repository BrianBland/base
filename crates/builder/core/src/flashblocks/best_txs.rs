//! Flashblocks adapters for parkable best-transaction iterators.

use std::{marker::PhantomData, sync::Arc};

use alloy_primitives::{Address, TxHash, map::B256Set};
use base_execution_payload_builder::ParkablePayloadTransactions;
use base_execution_txpool::{BasePooledTx, ValidityPredicate};
use reth_payload_util::PayloadTransactions;
use revm::state::EvmState;

use crate::{
    BuilderMetrics, RejectionCache, RestingFilter, RestingPayloadTransactions,
    RestingPredicateMode, RestingStats,
};

/// An adapter that skips transactions already committed or permanently rejected by flashblocks.
///
/// It also holds back validity transactions resting under an unchanged predicate, see
/// [`RestingPayloadTransactions`]. In enforce mode the inner iterator parks a resting transaction
/// through a [`RestingFilter`] instead of yielding it, so its nonce lane stays blocked, and it is
/// promoted back at its priority position once a commit changes the state its predicate reads.
pub struct BestFlashblocksTxs<T, I>
where
    T: BasePooledTx,
    I: ParkablePayloadTransactions<Pooled = T>,
{
    inner: I,
    // Transactions that were already committed to the state. Using them again would cause NonceTooLow
    // so we skip them
    committed_transactions: B256Set,
    // Shared cross-block rejection cache (survives across blocks, TTL-bounded)
    rejection_cache: RejectionCache,
    // Identity of the transaction most recently returned to the build loop.
    current_transaction: Option<(TxHash, Address, u64)>,
    resting_predicate_mode: RestingPredicateMode,
    resting: Arc<RestingFilter>,
    transaction: PhantomData<T>,
}

impl<T, I> std::fmt::Debug for BestFlashblocksTxs<T, I>
where
    T: BasePooledTx,
    I: ParkablePayloadTransactions<Pooled = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BestFlashblocksTxs")
            .field("committed_transactions", &self.committed_transactions)
            .field("rejection_cache_size", &self.rejection_cache.entry_count())
            .field("resting_predicate_mode", &self.resting_predicate_mode)
            .field("resting", &self.resting)
            .finish_non_exhaustive()
    }
}

impl<T, I> BestFlashblocksTxs<T, I>
where
    T: BasePooledTx,
    I: ParkablePayloadTransactions<Pooled = T>,
{
    /// Creates a new [`BestFlashblocksTxs`] wrapping the given payload transaction iterator.
    pub fn new(inner: I, rejection_cache: RejectionCache) -> Self {
        Self {
            inner,
            committed_transactions: Default::default(),
            resting: Arc::new(RestingFilter::new(rejection_cache.clone())),
            rejection_cache,
            current_transaction: None,
            resting_predicate_mode: RestingPredicateMode::Off,
            transaction: PhantomData,
        }
    }

    /// Sets whether resting validity transactions are tracked and held back.
    #[must_use]
    pub fn with_resting_predicate_mode(mut self, mode: RestingPredicateMode) -> Self {
        self.resting_predicate_mode = mode;
        self.install_resting_filter();
        self
    }

    /// Replaces current iterator with new one. We use it on new flashblock building, to refresh
    /// priority boundaries
    pub fn refresh_iterator(&mut self, inner: I) {
        self.inner = inner;
        self.current_transaction = None;
        self.resting.state().parked.clear();
        self.install_resting_filter();
    }

    /// Lets the inner iterator park resting transactions as it would yield them.
    pub fn install_resting_filter(&mut self) {
        if self.resting_predicate_mode.is_enforced() {
            self.inner.set_parking_filter(Arc::clone(&self.resting) as _);
        }
    }

    /// Remove transaction from next iteration since it is already in the state
    pub fn mark_committed(&mut self, txs: &[TxHash]) {
        self.committed_transactions.extend(txs);
        let mut state = self.resting.state();
        if !state.resting.is_empty() {
            for transaction_hash in txs {
                state.resting.remove(*transaction_hash);
            }
        }
    }

    /// Mark transactions as permanently rejected. They will be skipped in all
    /// subsequent flashblocks within this block and across future blocks via
    /// the shared rejection cache.
    pub fn mark_rejected(&mut self, tx_hashes: &[TxHash]) {
        self.rejection_cache.mark_rejected(tx_hashes);
        BuilderMetrics::rejection_cache_insertions().increment(tx_hashes.len() as u64);
        BuilderMetrics::rejection_cache_size().set(self.rejection_cache.entry_count() as f64);
    }
}

impl<T, I> PayloadTransactions for BestFlashblocksTxs<T, I>
where
    T: BasePooledTx,
    I: ParkablePayloadTransactions<Pooled = T>,
{
    type Transaction = I::Transaction;

    /// Resting transactions are parked by the inner iterator's [`RestingFilter`] before they
    /// reach this loop.
    fn next(&mut self, ctx: ()) -> Option<Self::Transaction> {
        let next = loop {
            let Some(pooled) = self.inner.next(ctx) else { break None };
            let tx = &pooled.transaction;
            let hash = *tx.hash();
            self.current_transaction = Some((hash, tx.sender(), tx.nonce()));

            if self.committed_transactions.contains(&hash) {
                self.inner.mark_current_committed();
                self.current_transaction = None;
                continue;
            }

            if self.rejection_cache.is_rejected(&hash) {
                BuilderMetrics::rejection_cache_hits().increment(1);
                // Only intrinsically invalid transactions enter this cache. Their nonce-lane
                // descendants cannot execute across the resulting gap, so exclude the lane for
                // this iterator rather than treating the rejected head as committed.
                self.inner.mark_invalid(tx.sender(), tx.nonce());
                self.current_transaction = None;
                continue;
            }

            break Some(pooled);
        };
        self.resting.finish_next();
        next
    }

    /// Proxy to inner iterator
    fn mark_invalid(&mut self, sender: Address, nonce: u64) {
        let matches_current =
            self.current_transaction.is_some_and(|(_, current_sender, current_nonce)| {
                current_sender == sender && current_nonce == nonce
            });
        debug_assert!(matches_current, "mark_invalid must identify the current transaction");
        if !matches_current {
            return;
        }
        self.inner.mark_invalid(sender, nonce);
        self.current_transaction = None;
    }
}

impl<T, I> ParkablePayloadTransactions for BestFlashblocksTxs<T, I>
where
    T: BasePooledTx,
    I: ParkablePayloadTransactions<Pooled = T>,
{
    type Pooled = T;

    fn park_current(&mut self) {
        self.inner.park_current();
        self.current_transaction = None;
    }

    fn mark_current_committed(&mut self) {
        self.inner.mark_current_committed();
        if let Some((transaction_hash, _, _)) = self.current_transaction.take() {
            self.committed_transactions.insert(transaction_hash);
            let mut state = self.resting.state();
            if !state.resting.is_empty() {
                state.resting.remove(transaction_hash);
            }
        }
    }

    fn promote(&mut self, transaction_hash: TxHash) -> bool {
        self.inner.promote(transaction_hash)
    }

    fn discard_parked(&mut self, transaction_hash: TxHash) -> bool {
        self.inner.discard_parked(transaction_hash)
    }
}

impl<T, I> RestingPayloadTransactions for BestFlashblocksTxs<T, I>
where
    T: BasePooledTx,
    I: ParkablePayloadTransactions<Pooled = T>,
{
    /// Flashblock-index predicates are not recorded because the index changes between
    /// flashblocks without any commit.
    fn rest(&mut self, transaction_hash: TxHash, predicate: &ValidityPredicate) {
        if !self.resting_predicate_mode.is_enabled()
            || matches!(predicate, ValidityPredicate::FlashblockIndex { .. })
        {
            return;
        }
        self.resting.state().resting.park(transaction_hash, (), predicate.clone());
    }

    fn record_committed_state(&mut self, state: &EvmState) {
        let mut resting = self.resting.state();
        if resting.resting.is_empty() {
            return;
        }
        for transaction_hash in resting.resting.affected_by_state(state).affected_transactions {
            resting.resting.remove(transaction_hash);
            if resting.parked.remove(&transaction_hash) {
                self.inner.promote(transaction_hash);
            }
        }
    }

    fn is_resting(&self, transaction_hash: TxHash, predicates: &[ValidityPredicate]) -> bool {
        self.resting.state().is_resting(transaction_hash, predicates)
    }

    fn take_resting_stats(&mut self) -> RestingStats {
        std::mem::take(&mut self.resting.state().stats)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{
        Address, Bytes, Signature, TxHash, TxKind, U256,
        map::{B256Map, B256Set},
    };
    use base_common_consensus::{
        BasePooledTransaction as ConsensusPooledTransaction, BaseTransactionSigned, BaseTxEnvelope,
        Eip8130Signed, TxEip8130,
    };
    use base_execution_payload_builder::ParkedPredicateIndex;
    use base_execution_txpool::{
        BaseOrdering, BasePooledTransaction, BasePooledTx, MergeBestTransactions,
        ParkedBestTransactions, ValidityOperator, ValidityPredicate,
    };
    use reth_payload_util::PayloadTransactions;
    use reth_primitives_traits::Recovered;
    use reth_transaction_pool::{
        TransactionOrigin, ValidPoolTransaction, identifier::TransactionId, pool::PendingPool,
    };
    use revm::state::{Account, EvmState};

    use crate::{
        BestFlashblocksTxs, ParkableBestPayloadTransactions, ParkablePayloadTransactions,
        RejectionCache, RestingPayloadTransactions, RestingPredicateMode, RestingStats,
    };

    type Ordering = BaseOrdering<BasePooledTransaction>;
    type Parkable = ParkableBestPayloadTransactions<BasePooledTransaction>;

    const WATCHED: Address = Address::repeat_byte(0xaa);
    const UNRELATED: Address = Address::repeat_byte(0xbb);

    fn test_rejection_cache() -> RejectionCache {
        RejectionCache::new(1000, Duration::from_secs(60))
    }

    fn sender_address(sender: u64) -> Address {
        Address::from_word(U256::from(sender + 1).into())
    }

    fn transaction(
        sender: u64,
        nonce: u64,
        priority_fee: u128,
    ) -> Arc<ValidPoolTransaction<BasePooledTransaction>> {
        validity_transaction(sender, nonce, priority_fee, Vec::new())
    }

    fn validity_transaction(
        sender: u64,
        nonce: u64,
        priority_fee: u128,
        predicates: Vec<ValidityPredicate>,
    ) -> Arc<ValidPoolTransaction<BasePooledTransaction>> {
        let tx = TxEip1559 {
            chain_id: 1,
            nonce,
            gas_limit: 21_000,
            max_fee_per_gas: priority_fee + 100,
            max_priority_fee_per_gas: priority_fee,
            to: TxKind::Call(Address::ZERO),
            // Distinguishes otherwise identical transactions from different senders, which share
            // the test signature.
            value: U256::from(sender),
            ..Default::default()
        };
        let envelope = BaseTxEnvelope::Eip1559(tx.into_signed(Signature::test_signature()));
        let encoded_length = envelope.encode_2718_len();
        let transaction = BasePooledTransaction::new(
            Recovered::new_unchecked(BaseTransactionSigned::from(envelope), sender_address(sender)),
            encoded_length,
        )
        .with_validity_predicates(predicates);
        Arc::new(ValidPoolTransaction {
            transaction_id: TransactionId::new(sender.into(), nonce),
            transaction,
            propagate: true,
            timestamp: std::time::Instant::now(),
            origin: TransactionOrigin::External,
            authority_ids: None,
        })
    }

    fn pending_pool(
        transactions: &[Arc<ValidPoolTransaction<BasePooledTransaction>>],
    ) -> PendingPool<Ordering> {
        let mut pool = PendingPool::new(Ordering::coinbase_tip());
        for transaction in transactions {
            pool.add_transaction(Arc::clone(transaction), 0);
        }
        pool
    }

    /// Builds the production lane-parking iterator over a snapshot of `pool`.
    fn parkable(
        pool: &PendingPool<Ordering>,
    ) -> ParkableBestPayloadTransactions<BasePooledTransaction> {
        ParkableBestPayloadTransactions::new(Box::new(ParkedBestTransactions::new(
            pool.best(),
            Ordering::coinbase_tip(),
            0,
        )))
    }

    /// Drains `iterator`, marking each yielded transaction invalid for this scan only, and
    /// returns the yielded hashes.
    fn drain_without_including<I>(iterator: &mut I) -> Vec<TxHash>
    where
        I: PayloadTransactions<Transaction = Arc<ValidPoolTransaction<BasePooledTransaction>>>,
    {
        std::iter::from_fn(|| {
            let transaction = iterator.next(())?;
            iterator.mark_invalid(transaction.sender(), transaction.nonce());
            Some(*transaction.hash())
        })
        .collect()
    }

    #[test]
    fn yields_the_pool_transaction_handle() {
        let pooled = transaction(0, 0, 1);
        let pool = pending_pool(&[Arc::clone(&pooled)]);
        let mut iterator = BestFlashblocksTxs::new(parkable(&pool), test_rejection_cache());

        let yielded = iterator.next(()).unwrap();

        assert!(Arc::ptr_eq(&yielded, &pooled));
    }

    #[test]
    fn test_simple_case() {
        let pool =
            pending_pool(&[transaction(0, 0, 1), transaction(1, 0, 1), transaction(2, 0, 1)]);

        let mut iterator = BestFlashblocksTxs::new(parkable(&pool), test_rejection_cache());
        // ### First flashblock
        iterator.refresh_iterator(parkable(&pool));
        // Accept first tx
        let tx1 = iterator.next(()).unwrap();
        iterator.mark_current_committed();
        // Invalidate second tx
        let tx2 = iterator.next(()).unwrap();
        iterator.mark_invalid(tx2.sender(), tx2.nonce());
        // Accept third tx
        let tx3 = iterator.next(()).unwrap();
        iterator.mark_current_committed();
        // Check that it's empty
        assert!(iterator.next(()).is_none(), "Iterator should be empty");
        // Mark transaction as committed
        iterator.mark_committed(&[*tx1.hash(), *tx3.hash()]);

        // ### Second flashblock
        // It should not return txs 1 and 3, but should return 2
        iterator.refresh_iterator(parkable(&pool));
        let tx2 = iterator.next(()).unwrap();
        iterator.mark_current_committed();
        // Check that it's empty
        assert!(iterator.next(()).is_none(), "Iterator should be empty");
        // Mark transaction as committed
        iterator.mark_committed(&[*tx2.hash()]);

        // ### Third flashblock
        iterator.refresh_iterator(parkable(&pool));
        // Check that it's empty
        assert!(iterator.next(()).is_none(), "Iterator should be empty");
    }

    #[test]
    fn hashes_marked_committed_are_skipped_after_refresh() {
        let committed = transaction(0, 0, 2);
        let uncommitted = transaction(1, 0, 1);
        let committed_hash = *committed.hash();
        let uncommitted_hash = *uncommitted.hash();
        let pool = pending_pool(&[committed, uncommitted]);
        let mut iterator = BestFlashblocksTxs::new(parkable(&pool), test_rejection_cache());

        iterator.mark_committed(&[committed_hash]);
        iterator.refresh_iterator(parkable(&pool));

        assert_eq!(drain_without_including(&mut iterator), vec![uncommitted_hash]);
    }

    /// Rejected transactions are skipped across flashblock boundaries within the same block.
    #[test]
    fn test_rejected_txs_persist_across_refresh() {
        let tx_2 = transaction(1, 0, 1);
        let tx_2_hash = *tx_2.hash();
        let pool = pending_pool(&[transaction(0, 0, 1), tx_2, transaction(2, 0, 1)]);

        let mut iterator = BestFlashblocksTxs::new(parkable(&pool), test_rejection_cache());

        // FB1: none of the transactions are included, and the second is rejected permanently
        assert_eq!(drain_without_including(&mut iterator).len(), 3);
        iterator.mark_rejected(&[tx_2_hash]);

        // FB2: refresh iterator — tx2 should still be skipped
        iterator.refresh_iterator(parkable(&pool));
        let seen_hashes = drain_without_including(&mut iterator);
        assert!(!seen_hashes.contains(&tx_2_hash), "rejected tx should not reappear after refresh");
        assert_eq!(seen_hashes.len(), 2, "only non-rejected txs should appear");
    }

    /// Rejected transactions in the shared cache are skipped by a new iterator instance
    /// (simulating cross-block persistence).
    #[test]
    fn test_rejection_cache_persists_across_blocks() {
        let tx_2 = transaction(1, 0, 1);
        let tx_2_hash = *tx_2.hash();
        let pool = pending_pool(&[transaction(0, 0, 1), tx_2]);

        let cache = test_rejection_cache();

        // Block 1: reject tx_2
        let mut iter1 = BestFlashblocksTxs::new(parkable(&pool), cache.clone());
        assert_eq!(drain_without_including(&mut iter1).len(), 2);
        iter1.mark_rejected(&[tx_2_hash]);

        // Block 2: new iterator, same cache — tx_2 should be skipped
        let mut iter2 = BestFlashblocksTxs::new(parkable(&pool), cache);
        let seen_hashes = drain_without_including(&mut iter2);
        assert!(
            !seen_hashes.contains(&tx_2_hash),
            "tx rejected in block 1 should be skipped in block 2"
        );
        assert_eq!(seen_hashes.len(), 1, "only non-rejected tx should appear");
    }

    #[test]
    fn rejection_cache_hit_excludes_nonce_descendants() {
        let rejected = transaction(0, 0, 3);
        let descendant = transaction(0, 1, 2);
        let other = transaction(1, 0, 1);
        let rejected_hash = *rejected.hash();
        let other_hash = *other.hash();
        let pool = pending_pool(&[rejected, descendant, other]);
        let cache = test_rejection_cache();
        cache.insert(rejected_hash);
        let mut iterator = BestFlashblocksTxs::new(parkable(&pool), cache);

        let yielded_hashes = drain_without_including(&mut iterator);

        assert_eq!(yielded_hashes, vec![other_hash]);
    }

    /// This test simulates the nonce-chain gating fix across flashblock boundaries.
    ///
    /// Scenario (based on real Base Mainnet block 41628995):
    /// - Sender A has `TX_A` (nonce 0, LOW tip) and `TX_B` (nonce 1, HIGH tip) in the pool
    /// - Sender B has `TX_C` (MEDIUM tip)
    ///
    /// `TX_A` is in the mempool, `TX_B` and `TX_C` arrive later after the first flashblock has
    /// started building already.
    ///
    /// - In flashblock 1, `TX_A` gets consumed (`TX_B` unlocks after `TX_A`)
    /// - Only `TX_A` is marked as committed (simulating flashblock timer expiring)
    /// - In flashblock 2, `TX_B` (HIGH tip) should come before `TX_C` (MEDIUM tip)
    ///
    /// Expected: `TX_B` (100 gwei) before `TX_C` (10 gwei) in flashblock 2.
    ///
    /// The upstream reth PR (<https://github.com/paradigmxyz/reth/pull/21765>) that added
    /// `prune_transactions` to the pool trait has been merged. The production fix calls
    /// `pool.prune_transactions` after `mark_committed` between flashblocks, which removes
    /// the already-executed `TX_A` from the pool so the iterator sees the correct priority
    /// ordering. This test simulates that behavior by recreating the pool without `TX_A`
    /// and verifies that `TX_B` (100 gwei) is correctly ordered before `TX_C` (10 gwei).
    #[test]
    fn test_nonce_chain_gating_bug_across_flashblocks() {
        let tx_a = transaction(0, 0, 1_000_000_000); // 1 gwei - LOW
        let tx_b = transaction(0, 1, 100_000_000_000); // 100 gwei - HIGH (depends on TX_A)
        let tx_c = transaction(1, 0, 10_000_000_000); // 10 gwei - MEDIUM
        let mut pool = pending_pool(&[Arc::clone(&tx_a)]);

        // === FLASHBLOCK 1 ===
        let mut iterator = BestFlashblocksTxs::new(parkable(&pool), test_rejection_cache());

        // Simulate: Flashblock 1 starts building
        // Start consuming txns from the txpool
        let first = iterator.next(()).unwrap();
        assert_eq!(*first.hash(), *tx_a.hash(), "First should be TX_A (1 gwei)");
        iterator.mark_current_committed();

        // TX_B and TX_C arrive late, but we have already yielded lower-priority transactions
        // from the iterator, so these do not immediately get added to the best txns
        pool.add_transaction(Arc::clone(&tx_b), 0);
        pool.add_transaction(Arc::clone(&tx_c), 0);
        assert!(iterator.next(()).is_none());

        // Simulate: flashblock 1 is complete after TX_A was executed
        iterator.mark_committed(&[*tx_a.hash()]);
        // Simulate pool.prune_transactions by recreating the pool without TX_A
        let pool = pending_pool(&[Arc::clone(&tx_b), Arc::clone(&tx_c)]);

        // === FLASHBLOCK 2 ===
        // We refresh the iterator with the latest best transactions
        iterator.refresh_iterator(parkable(&pool));

        // TX_A has already been executed, so TX_B (100 gwei) is the best txn and TX_C
        // (10 gwei) the second best
        assert_eq!(drain_without_including(&mut iterator), vec![*tx_b.hash(), *tx_c.hash()]);
    }

    /// Reproduces the nonce-chain queuing bug caused by `prune_transactions`.
    ///
    /// After FB1 prunes executed nonce-0 txs, the pool's on-chain nonce view is stale
    /// (block not sealed), so nonce-1 txs from the same senders land in `queued`
    /// instead of `pending`, making them invisible to FB2+.
    #[tokio::test]
    async fn test_prune_transactions_causes_nonce_chain_queuing() {
        use alloy_primitives::{Address, U256};
        use reth_execution_types::ChangedAccount;
        use reth_transaction_pool::{
            BestTransactionsAttributes, TransactionOrigin, TransactionPool, TransactionPoolExt,
            test_utils::{MockTransaction, testing_pool},
        };

        let pool = testing_pool();

        let senders: Vec<Address> = (0..3).map(|_| Address::random()).collect();

        // All senders submit nonce-0 txs
        for sender in &senders {
            let tx = MockTransaction::eip1559()
                .with_sender(*sender)
                .with_nonce(0)
                .with_gas_limit(21_000)
                .with_priority_fee(5_000_000_000)
                .with_max_fee(100_000_000_000);
            pool.add_transaction(TransactionOrigin::External, tx).await.unwrap();
        }
        assert_eq!(pool.pool_size().pending, 3);

        // Simulate FB1: consume all nonce-0 txs, then prune them
        let best_attrs = BestTransactionsAttributes::new(0, None);
        let mut best_iter = pool.best_transactions_with_attributes(best_attrs);
        let mut executed_hashes = Vec::new();
        for tx in best_iter.by_ref() {
            executed_hashes.push(*tx.hash());
        }
        drop(best_iter);
        assert_eq!(executed_hashes.len(), 3);
        pool.prune_transactions(executed_hashes);
        assert_eq!(pool.pool_size().pending, 0);

        // Senders submit nonce-1 txs (arrive between FB1 and FB2)
        for sender in &senders {
            let tx = MockTransaction::eip1559()
                .with_sender(*sender)
                .with_nonce(1)
                .with_gas_limit(21_000)
                .with_priority_fee(5_000_000_000)
                .with_max_fee(100_000_000_000);
            pool.add_transaction(TransactionOrigin::External, tx).await.unwrap();
        }

        // Bug: nonce-1 txs are queued (nonce gap) because pool still thinks on-chain nonce is 0
        assert_eq!(pool.pool_size().pending, 0, "nonce-1 txs should be queued without fix");
        assert_eq!(pool.pool_size().queued, 3, "nonce-1 txs land in queued due to stale nonce");

        // Fix: update_accounts corrects the pool's nonce view, promoting queued -> pending.
        // U256::MAX balance is fine here — testing_pool has no revm state to read from.
        // Production code uses state.basic(address) for real balances.
        let changed_accounts: Vec<ChangedAccount> = senders
            .iter()
            .map(|&address| ChangedAccount { address, nonce: 1, balance: U256::MAX })
            .collect();
        pool.update_accounts(changed_accounts);
        assert_eq!(pool.pool_size().pending, 3, "nonce-1 txs should be pending after fix");
        assert_eq!(pool.pool_size().queued, 0, "no txs should be queued after fix");

        // FB2's iterator must see all 3 nonce-1 txs
        let mut fb2_iter = pool.best_transactions_with_attributes(best_attrs);
        let mut count = 0;
        while fb2_iter.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    /// A rejected transaction becomes eligible again after the cache TTL expires.
    #[test]
    fn test_rejected_tx_eligible_after_ttl_expiry() {
        let tx_2 = transaction(1, 0, 1);
        let tx_2_hash = *tx_2.hash();
        let pool = pending_pool(&[transaction(0, 0, 1), tx_2]);

        // TTL is short, 1ms
        let cache = RejectionCache::new(1000, Duration::from_millis(1));

        // Reject tx_2
        let mut iter1 = BestFlashblocksTxs::new(parkable(&pool), cache.clone());
        assert_eq!(drain_without_including(&mut iter1).len(), 2);
        iter1.mark_rejected(&[tx_2_hash]);

        // Wait for TTL to expire and flush pending evictions
        std::thread::sleep(Duration::from_millis(50));
        cache.run_pending_tasks();

        // New iterator — tx_2 should be back
        let mut iter2 = BestFlashblocksTxs::new(parkable(&pool), cache);
        let seen_hashes = drain_without_including(&mut iter2);
        assert!(seen_hashes.contains(&tx_2_hash), "tx should be eligible again after TTL expiry");
        assert_eq!(seen_hashes.len(), 2, "both txs should appear");
    }

    fn balance_at_least(address: Address, value: u64) -> ValidityPredicate {
        ValidityPredicate::Balance {
            address,
            op: ValidityOperator::GreaterThanOrEqual,
            value: U256::from(value),
        }
    }

    fn balance_change(address: Address, old: u64, new: u64) -> EvmState {
        let mut account = Account::default();
        account.info.balance = U256::from(new);
        account.original_info_mut().balance = U256::from(old);
        EvmState::from_iter([(address, account)])
    }

    fn resting_iterator(
        pool: &PendingPool<Ordering>,
    ) -> BestFlashblocksTxs<BasePooledTransaction, Parkable> {
        BestFlashblocksTxs::new(parkable(pool), test_rejection_cache())
            .with_resting_predicate_mode(RestingPredicateMode::Enforce)
    }

    /// Plays the build loop's part for a candidate whose predicate is unsatisfied.
    fn park_unsatisfied(
        iterator: &mut BestFlashblocksTxs<BasePooledTransaction, Parkable>,
        transaction: &ValidPoolTransaction<BasePooledTransaction>,
    ) {
        iterator.park_current();
        iterator.rest(*transaction.hash(), &transaction.transaction.validity_predicates()[0]);
    }

    #[test]
    fn resting_transaction_is_held_back_until_its_state_changes() {
        let resting = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 1)]);
        let unrelated = transaction(1, 0, 5);
        let trigger = transaction(2, 0, 4);
        let low = transaction(3, 0, 1);
        let pool = pending_pool(&[
            Arc::clone(&resting),
            Arc::clone(&unrelated),
            Arc::clone(&trigger),
            Arc::clone(&low),
        ]);
        let mut iterator = resting_iterator(&pool);

        // Flashblock 1: the build loop finds the predicate unsatisfied.
        let first = iterator.next(()).unwrap();
        assert_eq!(*first.hash(), *resting.hash());
        park_unsatisfied(&mut iterator, &first);

        // Flashblock 2: the resting transaction is not yielded, and a commit to unrelated state
        // does not release it.
        iterator.refresh_iterator(parkable(&pool));
        assert_eq!(*iterator.next(()).unwrap().hash(), *unrelated.hash());
        iterator.record_committed_state(&balance_change(UNRELATED, 0, 1));
        iterator.mark_current_committed();
        assert_eq!(iterator.take_resting_stats().parked, 1);

        // A commit to the watched balance releases it ahead of lower-priority candidates.
        assert_eq!(*iterator.next(()).unwrap().hash(), *trigger.hash());
        iterator.record_committed_state(&balance_change(WATCHED, 0, 1));
        iterator.mark_current_committed();
        assert_eq!(*iterator.next(()).unwrap().hash(), *resting.hash());
        iterator.mark_current_committed();
        assert_eq!(*iterator.next(()).unwrap().hash(), *low.hash());
    }

    #[test]
    fn nonce_descendant_waits_for_resting_parent() {
        let parent = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 1)]);
        let child = transaction(0, 1, 100);
        let other = transaction(1, 0, 1);
        let pool = pending_pool(&[Arc::clone(&parent), Arc::clone(&child), Arc::clone(&other)]);
        let mut iterator = resting_iterator(&pool);

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);

        iterator.refresh_iterator(parkable(&pool));
        assert_eq!(*iterator.next(()).unwrap().hash(), *other.hash());
        iterator.mark_current_committed();
        assert!(iterator.next(()).is_none());

        iterator.record_committed_state(&balance_change(WATCHED, 0, 1));
        assert_eq!(*iterator.next(()).unwrap().hash(), *parent.hash());
        iterator.mark_current_committed();
        assert_eq!(*iterator.next(()).unwrap().hash(), *child.hash());
    }

    /// A transaction parked by the build loop in the current flashblock is woken by the build
    /// loop's own predicate index, which re-evaluates it first, so the iterator does not promote
    /// it. It no longer rests, so the next flashblock yields it for evaluation.
    #[test]
    fn transaction_parked_by_build_loop_is_not_promoted_by_a_wake() {
        let resting = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 2)]);
        let pool = pending_pool(&[Arc::clone(&resting)]);
        let mut iterator = resting_iterator(&pool);

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);
        iterator.record_committed_state(&balance_change(WATCHED, 0, 1));
        assert!(iterator.next(()).is_none());

        iterator.refresh_iterator(parkable(&pool));
        assert_eq!(*iterator.next(()).unwrap().hash(), *resting.hash());
    }

    #[test]
    fn resting_work_is_reported_when_the_iterator_is_exhausted() {
        let resting = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 1)]);
        let pool = pending_pool(&[Arc::clone(&resting)]);
        let mut iterator = resting_iterator(&pool);

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);
        assert_eq!(iterator.take_resting_stats(), RestingStats::default());

        iterator.refresh_iterator(parkable(&pool));
        assert!(iterator.next(()).is_none());
        let stats = iterator.take_resting_stats();
        assert_eq!(stats.parked, 1);
        assert!(!stats.duration.is_zero());
    }

    #[test]
    fn latest_rested_predicate_replaces_the_previous_one() {
        let resting = validity_transaction(
            0,
            0,
            10,
            vec![balance_at_least(WATCHED, 1), balance_at_least(UNRELATED, 1)],
        );
        let pool = pending_pool(&[Arc::clone(&resting)]);
        let mut iterator = resting_iterator(&pool);

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);
        iterator.rest(*resting.hash(), &first.transaction.validity_predicates()[1]);

        iterator.refresh_iterator(parkable(&pool));
        assert!(iterator.next(()).is_none());
        iterator.record_committed_state(&balance_change(WATCHED, 0, 1));
        assert!(iterator.next(()).is_none());
        iterator.record_committed_state(&balance_change(UNRELATED, 0, 1));
        assert_eq!(*iterator.next(()).unwrap().hash(), *resting.hash());
    }

    #[test]
    fn readded_hash_without_the_rested_predicate_is_yielded() {
        let original = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 1)]);
        let readded = validity_transaction(0, 0, 10, vec![balance_at_least(UNRELATED, 1)]);
        assert_eq!(*original.hash(), *readded.hash());
        let mut iterator = resting_iterator(&pending_pool(&[original]));

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);

        iterator.refresh_iterator(parkable(&pending_pool(&[Arc::clone(&readded)])));
        assert_eq!(*iterator.next(()).unwrap().hash(), *readded.hash());
    }

    /// A hash re-added without its rested predicate can commit; once committed, a later re-add
    /// with that predicate is skipped as committed, releasing its lane, rather than resting.
    #[test]
    fn committed_transaction_does_not_rest() {
        let original = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 1)]);
        let readded = validity_transaction(0, 0, 10, vec![balance_at_least(UNRELATED, 1)]);
        let child = transaction(0, 1, 5);
        let mut iterator = resting_iterator(&pending_pool(&[Arc::clone(&original)]));

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);

        iterator.refresh_iterator(parkable(&pending_pool(&[readded])));
        iterator.next(()).unwrap();
        iterator.mark_current_committed();

        iterator.refresh_iterator(parkable(&pending_pool(&[original, Arc::clone(&child)])));
        assert_eq!(*iterator.next(()).unwrap().hash(), *child.hash());
        assert_eq!(iterator.take_resting_stats().parked, 0);
    }

    #[test]
    fn flashblock_index_predicate_does_not_rest() {
        let resting = validity_transaction(
            0,
            0,
            10,
            vec![ValidityPredicate::FlashblockIndex {
                op: ValidityOperator::GreaterThanOrEqual,
                value: U256::from(3),
            }],
        );
        let pool = pending_pool(&[Arc::clone(&resting)]);
        let mut iterator = resting_iterator(&pool);

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);

        iterator.refresh_iterator(parkable(&pool));
        assert_eq!(*iterator.next(()).unwrap().hash(), *resting.hash());
    }

    #[test]
    fn shadow_mode_tracks_resting_transactions_without_holding_them_back() {
        let resting = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 1)]);
        let pool = pending_pool(&[Arc::clone(&resting)]);
        let mut iterator = BestFlashblocksTxs::new(parkable(&pool), test_rejection_cache())
            .with_resting_predicate_mode(RestingPredicateMode::Shadow);

        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);

        iterator.refresh_iterator(parkable(&pool));
        let yielded = iterator.next(()).unwrap();
        assert_eq!(*yielded.hash(), *resting.hash());
        assert!(iterator.is_resting(*resting.hash(), yielded.transaction.validity_predicates()));
    }

    #[test]
    fn off_mode_does_not_track_resting_transactions() {
        let resting = validity_transaction(0, 0, 10, vec![balance_at_least(WATCHED, 1)]);
        let predicates = resting.transaction.validity_predicates();
        let mut iterator = BestFlashblocksTxs::new(
            parkable(&pending_pool(&[Arc::clone(&resting)])),
            test_rejection_cache(),
        );

        iterator.rest(*resting.hash(), &predicates[0]);

        assert!(!iterator.is_resting(*resting.hash(), predicates));
    }

    /// An EIP-8130 transaction on its own nonce channel, which the production pool serves from
    /// the 2D nonce sidecar.
    fn sidecar_transaction(
        sender: u64,
        priority_fee: u128,
        predicates: Vec<ValidityPredicate>,
    ) -> Arc<ValidPoolTransaction<BasePooledTransaction>> {
        let tx = TxEip8130 {
            chain_id: 1,
            sender: Some(sender_address(sender)),
            nonce_key: U256::from(sender + 1),
            nonce_sequence: 0,
            valid_after: 0,
            valid_before: 0,
            max_priority_fee_per_gas: priority_fee,
            max_fee_per_gas: priority_fee + 100,
            gas_limit: 50_000,
            account_changes: Vec::new(),
            calls: Vec::new(),
            metadata: Bytes::new(),
            payer: None,
        };
        let pooled =
            ConsensusPooledTransaction::Eip8130(Eip8130Signed::new(tx, Bytes::new(), Bytes::new()));
        let encoded_length = pooled.encode_2718_len();
        let transaction = BasePooledTransaction::new(
            Recovered::new_unchecked(BaseTransactionSigned::from(pooled), sender_address(sender)),
            encoded_length,
        )
        .with_validity_predicates(predicates);
        assert!(transaction.is_eip8130_sidecar_transaction());
        Arc::new(ValidPoolTransaction {
            transaction_id: TransactionId::new(sender.into(), 0),
            transaction,
            propagate: true,
            timestamp: std::time::Instant::now(),
            origin: TransactionOrigin::External,
            authority_ids: None,
        })
    }

    /// Builds the production candidate stack: protocol and sidecar sources merged under lane
    /// parking.
    fn merged_parkable(
        protocol: &PendingPool<Ordering>,
        sidecar: &PendingPool<Ordering>,
    ) -> Parkable {
        let merged = MergeBestTransactions::new(
            Box::new(protocol.best()),
            Box::new(sidecar.best()),
            Ordering::coinbase_tip(),
            0,
        );
        ParkableBestPayloadTransactions::new(Box::new(ParkedBestTransactions::new(
            merged,
            Ordering::coinbase_tip(),
            0,
        )))
    }

    /// The flashblocks adapter as it was before resting transactions were parked by the
    /// lane-parking iterator: every candidate reaches this adapter, which parks resting ones
    /// itself after its committed and rejection checks. It is the reference the production
    /// adapter must match.
    struct PerYieldRestingTxs {
        inner: Parkable,
        committed: B256Set,
        rejection_cache: RejectionCache,
        current: Option<(TxHash, Address, u64)>,
        resting: ParkedPredicateIndex<()>,
        parked_resting: B256Set,
        parked: u64,
    }

    impl PayloadTransactions for PerYieldRestingTxs {
        type Transaction = Arc<ValidPoolTransaction<BasePooledTransaction>>;

        fn next(&mut self, ctx: ()) -> Option<Self::Transaction> {
            loop {
                let pooled = self.inner.next(ctx)?;
                let hash = *pooled.hash();
                self.current = Some((hash, pooled.sender(), pooled.nonce()));
                if self.committed.contains(&hash) {
                    self.inner.mark_current_committed();
                    self.current = None;
                    continue;
                }
                if self.rejection_cache.is_rejected(&hash) {
                    self.inner.mark_invalid(pooled.sender(), pooled.nonce());
                    self.current = None;
                    continue;
                }
                if self.is_resting(hash, pooled.transaction.validity_predicates()) {
                    self.inner.park_current();
                    self.parked_resting.insert(hash);
                    self.current = None;
                    self.parked += 1;
                    continue;
                }
                return Some(pooled);
            }
        }

        fn mark_invalid(&mut self, sender: Address, nonce: u64) {
            self.inner.mark_invalid(sender, nonce);
            self.current = None;
        }
    }

    impl ParkablePayloadTransactions for PerYieldRestingTxs {
        type Pooled = BasePooledTransaction;

        fn park_current(&mut self) {
            self.inner.park_current();
            self.current = None;
        }

        fn mark_current_committed(&mut self) {
            self.inner.mark_current_committed();
            if let Some((hash, _, _)) = self.current.take() {
                self.committed.insert(hash);
            }
        }

        fn promote(&mut self, transaction_hash: TxHash) -> bool {
            self.inner.promote(transaction_hash)
        }

        fn discard_parked(&mut self, transaction_hash: TxHash) -> bool {
            self.inner.discard_parked(transaction_hash)
        }
    }

    impl RestingPayloadTransactions for PerYieldRestingTxs {
        fn rest(&mut self, transaction_hash: TxHash, predicate: &ValidityPredicate) {
            self.resting.park(transaction_hash, (), predicate.clone());
        }

        fn record_committed_state(&mut self, state: &EvmState) {
            for transaction_hash in self.resting.affected_by_state(state).affected_transactions {
                self.resting.remove(transaction_hash);
                if self.parked_resting.remove(&transaction_hash) {
                    self.inner.promote(transaction_hash);
                }
            }
        }

        fn is_resting(&self, transaction_hash: TxHash, predicates: &[ValidityPredicate]) -> bool {
            self.resting
                .predicate(transaction_hash)
                .is_some_and(|blocker| predicates.contains(blocker))
        }

        fn take_resting_stats(&mut self) -> RestingStats {
            RestingStats { parked: std::mem::take(&mut self.parked), ..Default::default() }
        }
    }

    /// The flashblock lifecycle `payload.rs` drives on top of the build loop.
    trait FlashblockCandidates:
        ParkablePayloadTransactions<Pooled = BasePooledTransaction> + RestingPayloadTransactions
    {
        fn refresh(&mut self, inner: Parkable);

        fn finish_flashblock(&mut self, committed: &[TxHash], rejected: &[TxHash]);
    }

    impl FlashblockCandidates for PerYieldRestingTxs {
        fn refresh(&mut self, inner: Parkable) {
            self.inner = inner;
            self.current = None;
            self.parked_resting.clear();
        }

        fn finish_flashblock(&mut self, committed: &[TxHash], rejected: &[TxHash]) {
            self.committed.extend(committed);
            self.rejection_cache.mark_rejected(rejected);
        }
    }

    impl FlashblockCandidates for BestFlashblocksTxs<BasePooledTransaction, Parkable> {
        fn refresh(&mut self, inner: Parkable) {
            self.refresh_iterator(inner);
        }

        fn finish_flashblock(&mut self, committed: &[TxHash], rejected: &[TxHash]) {
            self.mark_committed(committed);
            self.mark_rejected(rejected);
        }
    }

    /// Transactions of the differential scenario, shared by both runs so that their arrival
    /// timestamps, which break priority ties, are identical.
    struct Scenario {
        names: B256Map<&'static str>,
        /// Balance a committed transaction writes.
        writes: B256Map<(Address, u64)>,
        /// Transactions whose execution fails once their predicates are satisfied.
        failing: B256Set,
        protocol: Vec<Arc<ValidPoolTransaction<BasePooledTransaction>>>,
        sidecar: Vec<Arc<ValidPoolTransaction<BasePooledTransaction>>>,
        /// Protocol transactions that arrive after the first commit of the second flashblock.
        arrivals: Vec<Arc<ValidPoolTransaction<BasePooledTransaction>>>,
    }

    impl Scenario {
        const WRITER_FLASHBLOCK: usize = 1;
        const FLASHBLOCKS: usize = 3;

        /// A resting protocol head `r0` with buffered descendants is woken mid-flashblock by the
        /// sidecar writer `w`, while the merge holds the unrelated protocol head `u` behind the
        /// sidecar head `s2`. `q` and the sidecar `z` keep resting, and `a` and the descendant
        /// `r3` arrive live.
        fn new(woken_head_fails: bool) -> Self {
            let resting_head = validity_transaction(0, 0, 50, vec![balance_at_least(WATCHED, 1)]);
            let writer = sidecar_transaction(10, 10, Vec::new());
            let protocol = [
                ("r0", Arc::clone(&resting_head)),
                ("r1", transaction(0, 1, 45)),
                ("r2", transaction(0, 2, 1)),
                ("p0", transaction(1, 0, 20)),
                ("p1", transaction(1, 1, 19)),
                ("u", transaction(2, 0, 4)),
                ("q", validity_transaction(3, 0, 2, vec![balance_at_least(UNRELATED, 1)])),
            ];
            let sidecar = [
                ("w", Arc::clone(&writer)),
                ("s2", sidecar_transaction(11, 8, Vec::new())),
                ("z", sidecar_transaction(12, 7, vec![balance_at_least(UNRELATED, 1)])),
                ("s", sidecar_transaction(13, 3, Vec::new())),
            ];
            let arrivals = [("a", transaction(4, 0, 30)), ("r3", transaction(0, 3, 40))];
            let names = protocol
                .iter()
                .chain(&sidecar)
                .chain(&arrivals)
                .map(|(name, transaction)| (*transaction.hash(), *name))
                .collect();
            let failing = if woken_head_fails {
                B256Set::from_iter([*resting_head.hash()])
            } else {
                B256Set::default()
            };
            Self {
                names,
                writes: B256Map::from_iter([(*writer.hash(), (WATCHED, 1))]),
                failing,
                protocol: protocol.into_iter().map(|(_, transaction)| transaction).collect(),
                sidecar: sidecar.into_iter().map(|(_, transaction)| transaction).collect(),
                arrivals: arrivals.into_iter().map(|(_, transaction)| transaction).collect(),
            }
        }

        /// The first flashblock only sees the resting candidates and the head's descendants.
        fn pools(&self, flashblock: usize) -> (PendingPool<Ordering>, PendingPool<Ordering>) {
            if flashblock == 0 {
                let resting = |transaction: &&Arc<ValidPoolTransaction<BasePooledTransaction>>| {
                    !matches!(self.names[transaction.hash()], "p0" | "p1" | "u" | "w" | "s2" | "s")
                };
                let protocol: Vec<_> = self.protocol.iter().filter(resting).cloned().collect();
                let sidecar: Vec<_> = self.sidecar.iter().filter(resting).cloned().collect();
                return (pending_pool(&protocol), pending_pool(&sidecar));
            }
            (pending_pool(&self.protocol), pending_pool(&self.sidecar))
        }

        fn name(&self, transaction_hash: &TxHash) -> &'static str {
            self.names[transaction_hash]
        }

        /// Plays the build loop and `payload.rs` against `candidates`, returning every
        /// observable selection event.
        fn run<C: FlashblockCandidates>(&self, new: impl FnOnce(Parkable) -> C) -> Vec<String> {
            let mut balances: std::collections::HashMap<Address, u64> = Default::default();
            let first =
                |predicates: &[ValidityPredicate],
                 balances: &std::collections::HashMap<Address, u64>| {
                    predicates.iter().position(|predicate| match predicate {
                        ValidityPredicate::Balance { address, value, .. } => {
                            U256::from(balances.get(address).copied().unwrap_or_default()) < *value
                        }
                        _ => unreachable!("scenario uses balance predicates only"),
                    })
                };
            let mut log = Vec::new();
            let (mut protocol, mut sidecar) = self.pools(0);
            let mut candidates = new(merged_parkable(&protocol, &sidecar));
            for flashblock in 0..Self::FLASHBLOCKS {
                if flashblock > 0 {
                    (protocol, sidecar) = self.pools(flashblock);
                    candidates.refresh(merged_parkable(&protocol, &sidecar));
                }
                log.push(format!("flashblock {flashblock}"));
                let mut build_loop_parked = Vec::new();
                let (mut committed, mut rejected) = (Vec::new(), Vec::new());
                while let Some(candidate) = candidates.next(()) {
                    let hash = *candidate.hash();
                    let predicates = candidate.transaction.validity_predicates();
                    if let Some(blocker) = first(predicates, &balances) {
                        log.push(format!("park {}", self.name(&hash)));
                        candidates.park_current();
                        candidates.rest(hash, &predicates[blocker]);
                        build_loop_parked.push(candidate);
                        continue;
                    }
                    if self.failing.contains(&hash) {
                        log.push(format!("reject {}", self.name(&hash)));
                        candidates.mark_invalid(candidate.sender(), candidate.nonce());
                        rejected.push(hash);
                        continue;
                    }
                    log.push(format!("commit {}", self.name(&hash)));
                    let written = self.writes.get(&hash).copied();
                    let state = written.map_or_else(EvmState::default, |(address, value)| {
                        balance_change(address, balances.get(&address).copied().unwrap_or(0), value)
                    });
                    candidates.record_committed_state(&state);
                    if let Some((address, value)) = written {
                        balances.insert(address, value);
                    }
                    candidates.mark_current_committed();
                    committed.push(hash);
                    if flashblock == Self::WRITER_FLASHBLOCK && committed.len() == 1 {
                        for arrival in &self.arrivals {
                            protocol.add_transaction(Arc::clone(arrival), 0);
                        }
                    }
                    let Some((address, _)) = written else { continue };
                    build_loop_parked.retain(|parked| {
                        let parked_predicates = parked.transaction.validity_predicates();
                        let watches = parked_predicates.iter().any(|predicate| {
                            matches!(predicate, ValidityPredicate::Balance { address: watched, .. } if *watched == address)
                        });
                        if !watches {
                            return true;
                        }
                        match first(parked_predicates, &balances) {
                            Some(blocker) => {
                                candidates.rest(*parked.hash(), &parked_predicates[blocker]);
                                true
                            }
                            None => {
                                let promoted = candidates.promote(*parked.hash());
                                log.push(format!(
                                    "promote {} {promoted}",
                                    self.name(parked.hash())
                                ));
                                false
                            }
                        }
                    });
                }
                log.push(format!("resting parked {}", candidates.take_resting_stats().parked));
                candidates.finish_flashblock(&committed, &rejected);
            }
            log
        }
    }

    /// Parking resting transactions inside the lane-parking iterator yields exactly the
    /// selection of the adapter that parked each one on receipt, through the production merge
    /// and lane-parking stack.
    #[test]
    fn resting_filter_matches_per_yield_parking() {
        for woken_head_fails in [false, true] {
            let scenario = Scenario::new(woken_head_fails);
            let reference = scenario.run(|inner| PerYieldRestingTxs {
                inner,
                committed: B256Set::default(),
                rejection_cache: test_rejection_cache(),
                current: None,
                resting: ParkedPredicateIndex::default(),
                parked_resting: B256Set::default(),
                parked: 0,
            });
            let filtered = scenario.run(|inner| {
                BestFlashblocksTxs::new(inner, test_rejection_cache())
                    .with_resting_predicate_mode(RestingPredicateMode::Enforce)
            });

            assert_eq!(filtered, reference, "woken head fails: {woken_head_fails}");
            let woken = reference
                .iter()
                .position(|event| event == "commit w")
                .expect("the writer commits in the second flashblock");
            let outcome = if woken_head_fails { "reject r0" } else { "commit r0" };
            assert_eq!(reference[woken + 1], outcome, "{reference:#?}");
            assert!(reference.contains(&"resting parked 3".to_owned()), "{reference:#?}");
        }
    }
}
