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
        if self.resting_predicate_mode.is_enforced() {
            self.resting.state().committed.extend(txs);
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
    /// reach this loop, except one the filter let through as rejected whose rejection-cache
    /// entry is gone by the time this loop checks it; that one is parked here.
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

            if self.resting_predicate_mode.is_enforced()
                && self.resting.park_if_resting_after_cache_miss(&pooled)
            {
                self.inner.park_current();
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
            if self.resting_predicate_mode.is_enforced() {
                self.resting.state().committed.insert(transaction_hash);
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
        Call, Eip8130Constants, Eip8130Signed, Predeploys, TxEip8130,
    };
    use base_execution_payload_builder::{
        DEFAULT_PREDICATE_BUCKET_ORDERED_THRESHOLD, ParkedPredicateIndex,
    };
    use base_execution_txpool::{
        BaseOrdering, BasePooledTransaction, BasePooledTx, MergeBestTransactions,
        ParkedBestTransactions, ValidityOperator, ValidityPredicate, sidecar_best_transactions,
    };
    use parking_lot::Mutex;
    use reth_payload_util::PayloadTransactions;
    use reth_primitives_traits::Recovered;
    use reth_transaction_pool::{
        BestTransactions, TransactionOrigin, ValidPoolTransaction,
        error::InvalidPoolTransactionError, identifier::TransactionId, pool::PendingPool,
    };
    use revm::state::{Account, EvmState};

    use crate::{
        BestFlashblocksTxs, ParkableBestPayloadTransactions, ParkablePayloadTransactions,
        RejectionCache, RestingPayloadTransactions, RestingPredicateMode, RestingStats,
    };

    type Ordering = BaseOrdering<BasePooledTransaction>;
    type Parkable = ParkableBestPayloadTransactions<BasePooledTransaction>;
    type Tx = Arc<ValidPoolTransaction<BasePooledTransaction>>;

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
        validity_transaction_with_max_fee(
            sender,
            nonce,
            priority_fee,
            priority_fee + 100,
            predicates,
        )
    }

    fn validity_transaction_with_max_fee(
        sender: u64,
        nonce: u64,
        priority_fee: u128,
        max_fee_per_gas: u128,
        predicates: Vec<ValidityPredicate>,
    ) -> Arc<ValidPoolTransaction<BasePooledTransaction>> {
        let tx = TxEip1559 {
            chain_id: 1,
            nonce,
            gas_limit: 21_000,
            max_fee_per_gas,
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
        pending_pool_at(transactions, 0)
    }

    fn pending_pool_at(
        transactions: &[Arc<ValidPoolTransaction<BasePooledTransaction>>],
        base_fee: u64,
    ) -> PendingPool<Ordering> {
        let mut pool = PendingPool::new(Ordering::coinbase_tip());
        for transaction in transactions {
            pool.add_transaction(Arc::clone(transaction), base_fee);
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

    /// A committed hash stays in the resting index, so with 31 others resting under its stale
    /// blocker that bucket is ordered, and a write that crosses no threshold wakes none of them.
    #[test]
    fn committed_hash_keeps_its_resting_bucket_ordered() {
        let blocked = vec![balance_at_least(WATCHED, 2)];
        let stale = validity_transaction(0, 0, 50, blocked.clone());
        let replaced = validity_transaction(0, 0, 50, Vec::new());
        let resting: Vec<_> =
            (1..=31).map(|sender| validity_transaction(sender, 0, 20, blocked.clone())).collect();
        let writer = transaction(40, 0, 5);
        let mut iterator = resting_iterator(&pending_pool(&[stale]));
        let first = iterator.next(()).unwrap();
        park_unsatisfied(&mut iterator, &first);

        let mut second = vec![Arc::clone(&replaced)];
        second.extend(resting.iter().cloned());
        iterator.refresh_iterator(parkable(&pending_pool(&second)));
        assert_eq!(*iterator.next(()).unwrap().hash(), *replaced.hash());
        iterator.mark_current_committed();
        for _ in &resting {
            let candidate = iterator.next(()).unwrap();
            park_unsatisfied(&mut iterator, &candidate);
        }

        let mut third = resting.clone();
        third.push(Arc::clone(&writer));
        iterator.refresh_iterator(parkable(&pending_pool(&third)));
        assert_eq!(*iterator.next(()).unwrap().hash(), *writer.hash());
        iterator.record_committed_state(&balance_change(WATCHED, 0, 1));
        iterator.mark_current_committed();

        assert!(iterator.next(()).is_none());
        assert_eq!(iterator.take_resting_stats().parked, 31);
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
        let tx = eip8130_transaction(sender, U256::from(sender + 1), 0, priority_fee, predicates);
        assert!(tx.transaction.is_eip8130_sidecar_transaction());
        tx
    }

    /// An EIP-8130 transaction on nonce channel `nonce_key` that bids `priority_fee`.
    fn eip8130_transaction(
        sender: u64,
        nonce_key: U256,
        nonce_sequence: u64,
        priority_fee: u128,
        predicates: Vec<ValidityPredicate>,
    ) -> Arc<ValidPoolTransaction<BasePooledTransaction>> {
        eip8130_transaction_with(
            sender,
            TxEip8130 {
                chain_id: 1,
                sender: Some(sender_address(sender)),
                nonce_key,
                nonce_sequence,
                valid_after: 0,
                valid_before: 0,
                max_priority_fee_per_gas: priority_fee,
                max_fee_per_gas: priority_fee + 100,
                gas_limit: 50_000,
                account_changes: Vec::new(),
                calls: Vec::new(),
                metadata: Bytes::new(),
                payer: None,
            },
            predicates,
        )
    }

    /// Wraps an EIP-8130 body from `sender_address(sender)` as a pool transaction.
    fn eip8130_transaction_with(
        sender: u64,
        tx: TxEip8130,
        predicates: Vec<ValidityPredicate>,
    ) -> Arc<ValidPoolTransaction<BasePooledTransaction>> {
        let nonce_sequence = tx.nonce_sequence;
        let pooled =
            ConsensusPooledTransaction::Eip8130(Eip8130Signed::new(tx, Bytes::new(), Bytes::new()));
        let encoded_length = pooled.encode_2718_len();
        let transaction = BasePooledTransaction::new(
            Recovered::new_unchecked(BaseTransactionSigned::from(pooled), sender_address(sender)),
            encoded_length,
        )
        .with_validity_predicates(predicates);
        Arc::new(ValidPoolTransaction {
            transaction_id: TransactionId::new(sender.into(), nonce_sequence),
            transaction,
            propagate: true,
            timestamp: std::time::Instant::now(),
            origin: TransactionOrigin::External,
            authority_ids: None,
        })
    }

    /// Builds the production candidate stack: the protocol pool and the production 2D nonce
    /// sidecar snapshot of `sidecar`, merged under lane parking, recording what lane parking
    /// claims from the protocol source into `claims`.
    ///
    /// The protocol source is the pending pool's plain snapshot, which is what the production
    /// pool serves when the attributes' base fee is the pool's pending base fee; the fee-checked
    /// variant is covered by reth's `next_skipping` tests.
    fn merged_parkable(
        protocol: &PendingPool<Ordering>,
        sidecar: &[Tx],
        base_fee: u64,
        claims: &Arc<Mutex<Vec<TxHash>>>,
    ) -> Parkable {
        let protocol =
            ClaimRecorder { inner: Box::new(protocol.best()), claims: Arc::clone(claims) };
        let merged = MergeBestTransactions::new(
            Box::new(protocol),
            sidecar_best_transactions(sidecar.iter().cloned(), Ordering::coinbase_tip(), base_fee),
            Ordering::coinbase_tip(),
            base_fee,
        );
        ParkableBestPayloadTransactions::new(Box::new(ParkedBestTransactions::new(
            merged,
            Ordering::coinbase_tip(),
            base_fee,
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
        park_sequence: Vec<TxHash>,
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
                    self.park_sequence.push(hash);
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

        /// Drains the resting parks made since the last call, in order.
        fn take_resting_parks(&mut self) -> Vec<TxHash>;
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

        fn take_resting_parks(&mut self) -> Vec<TxHash> {
            std::mem::take(&mut self.park_sequence)
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

        fn take_resting_parks(&mut self) -> Vec<TxHash> {
            std::mem::take(&mut self.resting.state().park_sequence)
        }
    }

    /// Records the protocol transactions the lane-parking iterator claims from its source
    /// through [`BestTransactions::next_skipping`], so tests can tell which path a resting
    /// transaction took.
    struct ClaimRecorder {
        inner: Box<dyn BestTransactions<Item = Tx>>,
        claims: Arc<Mutex<Vec<TxHash>>>,
    }

    impl Iterator for ClaimRecorder {
        type Item = Tx;

        fn next(&mut self) -> Option<Tx> {
            self.inner.next()
        }
    }

    impl BestTransactions for ClaimRecorder {
        fn next_skipping(&mut self, skip: &mut dyn FnMut(&Tx) -> bool) -> Option<Tx> {
            let claims = &self.claims;
            self.inner.next_skipping(&mut |transaction| {
                let claimed = skip(transaction);
                if claimed {
                    claims.lock().push(*transaction.hash());
                }
                claimed
            })
        }

        fn mark_invalid(&mut self, transaction: &Tx, kind: InvalidPoolTransactionError) {
            self.inner.mark_invalid(transaction, kind);
        }

        fn no_updates(&mut self) {
            self.inner.no_updates();
        }

        fn set_skip_blobs(&mut self, skip_blobs: bool) {
            self.inner.set_skip_blobs(skip_blobs);
        }
    }

    /// A protocol transaction delivered to the live pool during a flashblock.
    struct Arrival {
        flashblock: usize,
        /// Delivered once the flashblock has committed this many transactions, or right after the
        /// iterator refresh for zero.
        after_commits: usize,
        transaction: Tx,
        /// Replaces the pooled transaction with the same id instead of adding a new one.
        replaces: bool,
    }

    /// The pool contents and fees one flashblock is built from.
    #[derive(Clone, Default)]
    struct Flashblock {
        protocol: Vec<Tx>,
        sidecar: Vec<Tx>,
        base_fee: u64,
        /// Transactions the shared rejection cache learns about at the end of the flashblock, as
        /// if a concurrent payload job rejected them.
        rejected_elsewhere: Vec<TxHash>,
    }

    /// A differential scenario: transactions shared by both runs, so that their arrival
    /// timestamps, which break priority ties, are identical.
    struct Scenario {
        names: B256Map<String>,
        by_name: std::collections::HashMap<String, Tx>,
        /// Balance a committed transaction writes.
        writes: B256Map<(Address, u64)>,
        /// Transactions whose execution fails once their predicates are satisfied.
        failing: B256Set,
        flashblocks: Vec<Flashblock>,
        arrivals: Vec<Arrival>,
        /// Bucket size at which the build loop's predicate index switches to ordered buckets.
        ordered_threshold: usize,
    }

    impl Scenario {
        /// Three flashblocks that all see every listed transaction, at a zero base fee.
        fn new(protocol: Vec<(&str, Tx)>, sidecar: Vec<(&str, Tx)>) -> Self {
            let mut scenario = Self {
                names: B256Map::default(),
                by_name: Default::default(),
                writes: B256Map::default(),
                failing: B256Set::default(),
                flashblocks: Vec::new(),
                arrivals: Vec::new(),
                ordered_threshold: DEFAULT_PREDICATE_BUCKET_ORDERED_THRESHOLD,
            };
            let protocol: Vec<_> =
                protocol.into_iter().map(|(name, tx)| scenario.register(name, tx)).collect();
            let sidecar: Vec<_> =
                sidecar.into_iter().map(|(name, tx)| scenario.register(name, tx)).collect();
            let flashblock = Flashblock { protocol, sidecar, ..Default::default() };
            scenario.flashblocks = vec![flashblock; 3];
            scenario
        }

        fn register(&mut self, name: &str, transaction: Tx) -> Tx {
            self.names.insert(*transaction.hash(), name.to_owned());
            self.by_name.insert(name.to_owned(), Arc::clone(&transaction));
            transaction
        }

        fn tx(&self, name: &str) -> Tx {
            Arc::clone(&self.by_name[name])
        }

        /// Restricts the first flashblock to `names`, so they rest before the others appear.
        fn only_first(mut self, names: &[&str]) -> Self {
            let keep = |tx: &Tx| names.contains(&self.names[tx.hash()].as_str());
            let first = &self.flashblocks[0];
            let protocol = first.protocol.iter().filter(|tx| keep(tx)).cloned().collect();
            let sidecar = first.sidecar.iter().filter(|tx| keep(tx)).cloned().collect();
            self.flashblocks[0].protocol = protocol;
            self.flashblocks[0].sidecar = sidecar;
            self
        }

        /// Sets the protocol pool of `flashblock` to `names`, in submission order.
        fn protocol_pool(mut self, flashblock: usize, names: &[&str]) -> Self {
            self.flashblocks[flashblock].protocol =
                names.iter().map(|name| self.tx(name)).collect();
            self
        }

        fn write(mut self, name: &str, address: Address, value: u64) -> Self {
            self.writes.insert(*self.by_name[name].hash(), (address, value));
            self
        }

        fn fail_if(mut self, fails: bool, name: &str) -> Self {
            if fails {
                self.failing.insert(*self.by_name[name].hash());
            }
            self
        }

        fn base_fee(mut self, flashblock: usize, base_fee: u64) -> Self {
            self.flashblocks[flashblock].base_fee = base_fee;
            self
        }

        fn rejected_elsewhere(mut self, flashblock: usize, name: &str) -> Self {
            let hash = *self.by_name[name].hash();
            self.flashblocks[flashblock].rejected_elsewhere.push(hash);
            self
        }

        fn arrive(
            mut self,
            flashblock: usize,
            after_commits: usize,
            name: &str,
            transaction: Tx,
        ) -> Self {
            let transaction = self.register(name, transaction);
            self.arrivals.push(Arrival { flashblock, after_commits, transaction, replaces: false });
            self
        }

        fn replace(
            mut self,
            flashblock: usize,
            after_commits: usize,
            name: &str,
            transaction: Tx,
        ) -> Self {
            let transaction = self.register(name, transaction);
            self.arrivals.push(Arrival { flashblock, after_commits, transaction, replaces: true });
            self
        }

        fn name(&self, transaction_hash: &TxHash) -> &str {
            &self.names[transaction_hash]
        }

        fn pools(&self, flashblock: usize) -> (PendingPool<Ordering>, Vec<Tx>) {
            let block = &self.flashblocks[flashblock];
            (pending_pool_at(&block.protocol, block.base_fee), block.sidecar.clone())
        }

        fn deliver(&self, protocol: &mut PendingPool<Ordering>, flashblock: usize, commits: usize) {
            let base_fee = self.flashblocks[flashblock].base_fee;
            for arrival in &self.arrivals {
                if arrival.flashblock != flashblock || arrival.after_commits != commits {
                    continue;
                }
                let transaction = Arc::clone(&arrival.transaction);
                if arrival.replaces {
                    protocol.replace_transaction(transaction, base_fee);
                } else {
                    protocol.add_transaction(transaction, base_fee);
                }
            }
        }

        /// Plays the build loop and `payload.rs` against `candidates`, returning every
        /// observable selection event, including each resting park, and the transactions
        /// claimed from the protocol source, as `flashblock:name`.
        ///
        /// Build-loop parks follow the production lifecycle: they are indexed under their
        /// first unsatisfied predicate, a commit rescans only the transactions its state change
        /// affects through that index, and a rescan re-rests and reindexes or promotes them.
        fn run<C: FlashblockCandidates>(
            &self,
            new: impl FnOnce(Parkable) -> C,
        ) -> (Vec<String>, Vec<String>) {
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
            let claims = Arc::new(Mutex::new(Vec::new()));
            let mut claimed = Vec::new();
            let mut log = Vec::new();
            let (mut protocol, mut sidecar) = self.pools(0);
            let base_fee = self.flashblocks[0].base_fee;
            let mut candidates = new(merged_parkable(&protocol, &sidecar, base_fee, &claims));
            for flashblock in 0..self.flashblocks.len() {
                if flashblock > 0 {
                    (protocol, sidecar) = self.pools(flashblock);
                    let base_fee = self.flashblocks[flashblock].base_fee;
                    candidates.refresh(merged_parkable(&protocol, &sidecar, base_fee, &claims));
                }
                log.push(format!("flashblock {flashblock}"));
                self.deliver(&mut protocol, flashblock, 0);
                let mut build_loop_parked = ParkedPredicateIndex::<Tx>::new(self.ordered_threshold);
                let (mut committed, mut rejected) = (Vec::new(), Vec::new());
                loop {
                    let next = candidates.next(());
                    for parked in candidates.take_resting_parks() {
                        log.push(format!("rest {}", self.name(&parked)));
                    }
                    let Some(candidate) = next else { break };
                    let hash = *candidate.hash();
                    let predicates = candidate.transaction.validity_predicates();
                    if let Some(blocker) = first(predicates, &balances) {
                        log.push(format!("park {}", self.name(&hash)));
                        candidates.park_current();
                        candidates.rest(hash, &predicates[blocker]);
                        build_loop_parked.park(
                            hash,
                            Arc::clone(&candidate),
                            predicates[blocker].clone(),
                        );
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
                    let mut affected = if build_loop_parked.is_empty() {
                        Vec::new()
                    } else {
                        build_loop_parked.affected_by_state(&state).affected_transactions
                    };
                    // The index reports affected transactions in hash-set order, which differs
                    // between runs; promotion order does not change selection, so fix it.
                    affected.sort_unstable();
                    candidates.record_committed_state(&state);
                    if let Some((address, value)) = written {
                        balances.insert(address, value);
                    }
                    candidates.mark_current_committed();
                    committed.push(hash);
                    self.deliver(&mut protocol, flashblock, committed.len());
                    for parked_hash in affected {
                        let Some(parked) = build_loop_parked.transaction(parked_hash).cloned()
                        else {
                            continue;
                        };
                        let parked_predicates = parked.transaction.validity_predicates();
                        match first(parked_predicates, &balances) {
                            Some(blocker) => {
                                let predicate = parked_predicates[blocker].clone();
                                candidates.rest(parked_hash, &predicate);
                                build_loop_parked.reindex(parked_hash, predicate);
                            }
                            None => {
                                build_loop_parked.remove(parked_hash);
                                let promoted = candidates.promote(parked_hash);
                                log.push(format!("promote {} {promoted}", self.name(&parked_hash)));
                            }
                        }
                    }
                }
                log.push(format!("resting parked {}", candidates.take_resting_stats().parked));
                rejected.extend(&self.flashblocks[flashblock].rejected_elsewhere);
                candidates.finish_flashblock(&committed, &rejected);
                claimed.extend(
                    claims
                        .lock()
                        .drain(..)
                        .map(|hash| format!("{flashblock}:{}", self.name(&hash))),
                );
            }
            (log, claimed)
        }

        /// Asserts that the production adapter selects exactly what the per-yield reference
        /// selects, and returns the shared log with the production run's claims.
        fn assert_parity(&self) -> (Vec<String>, Vec<String>) {
            let (reference, reference_claims) = self.run(|inner| PerYieldRestingTxs {
                inner,
                committed: B256Set::default(),
                rejection_cache: test_rejection_cache(),
                current: None,
                resting: ParkedPredicateIndex::default(),
                parked_resting: B256Set::default(),
                parked: 0,
                park_sequence: Vec::new(),
            });
            assert!(reference_claims.is_empty(), "the reference installs no parking filter");
            let (filtered, claims) = self.run(|inner| {
                BestFlashblocksTxs::new(inner, test_rejection_cache())
                    .with_resting_predicate_mode(RestingPredicateMode::Enforce)
            });
            assert_eq!(filtered, reference, "claims: {claims:?}");
            (reference, claims)
        }
    }

    /// Asserts that `events` occur in `log` in this order, not necessarily adjacently.
    fn assert_in_order(log: &[String], events: &[&str]) {
        let mut remaining = log.iter();
        for event in events {
            assert!(
                remaining.any(|logged| logged == event),
                "missing {event:?} in order: {log:#?}"
            );
        }
    }

    /// A resting protocol head `r0` with buffered descendants is woken mid-flashblock by the
    /// sidecar writer `w`, while the merge holds the unrelated protocol head `u` behind the
    /// sidecar head `s2`. `q` and the sidecar `z` keep resting, and `a` and the descendant `r3`
    /// arrive live.
    fn wake_mid_flashblock(woken_head_fails: bool) -> Scenario {
        Scenario::new(
            vec![
                ("r0", validity_transaction(0, 0, 50, vec![balance_at_least(WATCHED, 1)])),
                ("r1", transaction(0, 1, 45)),
                ("r2", transaction(0, 2, 1)),
                ("p0", transaction(1, 0, 20)),
                ("p1", transaction(1, 1, 19)),
                ("u", transaction(2, 0, 4)),
                ("q", validity_transaction(3, 0, 2, vec![balance_at_least(UNRELATED, 1)])),
            ],
            vec![
                ("w", sidecar_transaction(10, 10, Vec::new())),
                ("s2", sidecar_transaction(11, 8, Vec::new())),
                ("z", sidecar_transaction(12, 7, vec![balance_at_least(UNRELATED, 1)])),
                ("s", sidecar_transaction(13, 3, Vec::new())),
            ],
        )
        .only_first(&["r0", "r1", "r2", "q", "z"])
        .write("w", WATCHED, 1)
        .fail_if(woken_head_fails, "r0")
        .arrive(1, 1, "a", transaction(4, 0, 30))
        .arrive(1, 1, "r3", transaction(0, 3, 40))
    }

    /// Parking resting transactions inside the lane-parking iterator yields exactly the
    /// selection of the adapter that parked each one on receipt, through the production merge
    /// and lane-parking stack.
    #[test]
    fn resting_filter_matches_per_yield_parking() {
        for woken_head_fails in [false, true] {
            let (log, claims) = wake_mid_flashblock(woken_head_fails).assert_parity();
            let woken = log
                .iter()
                .position(|event| event == "commit w")
                .expect("the writer commits in the second flashblock");
            let outcome = if woken_head_fails { "reject r0" } else { "commit r0" };
            assert_eq!(log[woken + 1], outcome, "{log:#?}");
            assert!(log.contains(&"resting parked 3".to_owned()), "{log:#?}");
            assert!(claims.contains(&"1:r0".to_owned()), "r0 is parked where it is popped");
            assert!(!claims.iter().any(|claim| claim.ends_with(":z")), "sidecar is never claimed");
        }
    }

    /// Review P1: a claimed resting head is woken, promoted and rejected while the merge holds
    /// the protocol head `u` behind the sidecar head `s2`. The merge drops `u` exactly as it
    /// does for the reference, which never hands `r` to the merge at another time.
    #[test]
    fn claimed_head_rejected_after_wake_drops_the_merge_head_as_before() {
        for fails in [false, true] {
            let (log, claims) = Scenario::new(
                vec![
                    ("r", validity_transaction(0, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                    ("u", transaction(1, 0, 4)),
                    ("p", transaction(2, 0, 2)),
                ],
                vec![
                    ("w", sidecar_transaction(10, 10, Vec::new())),
                    ("s2", sidecar_transaction(11, 8, Vec::new())),
                    ("s", sidecar_transaction(12, 3, Vec::new())),
                ],
            )
            .only_first(&["r"])
            .write("w", WATCHED, 1)
            .fail_if(fails, "r")
            .assert_parity();
            assert_eq!(claims, ["1:r"], "{log:#?}");
            let outcome = if fails { "reject r" } else { "commit r" };
            assert_in_order(&log, &["flashblock 1", "commit w", outcome, "commit s2"]);
        }
    }

    /// Review P2: a different-hash replacement `r'` of the claimed `r` arrives live and is
    /// stashed. The wake still promotes the original `r`, which the lane-parking iterator
    /// kept.
    #[test]
    fn replacement_of_a_claimed_transaction_does_not_erase_it() {
        for fails in [false, true] {
            let scenario = Scenario::new(
                vec![
                    ("r", validity_transaction(0, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                    ("x", transaction(1, 0, 20)),
                ],
                vec![("w", sidecar_transaction(10, 10, Vec::new()))],
            )
            .only_first(&["r"])
            .write("w", WATCHED, 1)
            .fail_if(fails, "r")
            .replace(
                1,
                1,
                "r'",
                validity_transaction_with_max_fee(
                    0,
                    0,
                    40,
                    141,
                    vec![balance_at_least(WATCHED, 1)],
                ),
            );
            let scenario = scenario.protocol_pool(2, &["r'", "x"]);
            let (log, claims) = scenario.assert_parity();
            assert_eq!(claims, ["1:r"], "{log:#?}");
            let outcome = if fails { "reject r" } else { "commit r" };
            assert_in_order(
                &log,
                &["flashblock 1", "commit x", "commit w", outcome, "flashblock 2"],
            );
        }
    }

    /// Review P2: a live descendant of the claimed `r` arrives after a lower-priority pop. When
    /// it outranks that pop it is stashed and waits for the refresh; otherwise the source
    /// unlocks it and the lane buffers it behind `r` until `r` resolves.
    #[test]
    fn live_descendant_of_a_claimed_transaction_keeps_its_natural_path() {
        for (child_priority, fails) in [(25, false), (25, true), (15, false), (15, true)] {
            let (log, claims) = Scenario::new(
                vec![
                    ("r", validity_transaction(0, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                    ("x", transaction(1, 0, 20)),
                ],
                vec![("w", sidecar_transaction(10, 5, Vec::new()))],
            )
            .only_first(&["r"])
            .write("w", WATCHED, 1)
            .fail_if(fails, "r")
            .arrive(1, 1, "c", transaction(0, 1, child_priority))
            .assert_parity();
            assert_eq!(claims, ["1:r"], "{log:#?}");
            let fb1 = log.iter().position(|event| event == "flashblock 1").unwrap();
            let fb2 = log.iter().position(|event| event == "flashblock 2").unwrap();
            let child_in_fb1 = log[fb1..fb2].contains(&"commit c".to_owned());
            assert_eq!(child_in_fb1, child_priority < 20 && !fails, "{log:#?}");
        }
    }

    /// Review P2: the descendant `c` of the claimed `r` ties `y` in priority, with `y` submitted
    /// first and `c` created first. `c` reaches the lane through its own source pop after `y`,
    /// as for the reference, instead of being buffered early and winning the timestamp tie.
    #[test]
    fn descendant_of_a_claimed_transaction_keeps_its_tie_break() {
        let c = transaction(0, 1, 4);
        let y = transaction(1, 0, 4);
        let (log, claims) = Scenario::new(
            vec![
                ("r", validity_transaction(0, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                ("y", y),
                ("c", c),
            ],
            vec![("w", sidecar_transaction(10, 10, Vec::new()))],
        )
        .only_first(&["r", "c"])
        .write("w", WATCHED, 1)
        .assert_parity();
        assert_eq!(claims, ["1:r"], "{log:#?}");
        assert_in_order(&log, &["flashblock 1", "commit w", "commit r", "commit y", "commit c"]);
    }

    /// Review gap 1: a promoted ready entry `x` outranks the resting `r`, so `r` is not claimed
    /// but becomes the held source head; `x` commits and wakes `r` inside that window, and `r`
    /// is yielded at its natural position ahead of its priority twin `y`.
    #[test]
    fn resting_head_behind_a_ready_entry_sees_a_wake_in_its_window() {
        for fails in [false, true] {
            let (log, claims) = Scenario::new(
                vec![
                    ("x", validity_transaction(0, 0, 40, vec![balance_at_least(WATCHED, 1)])),
                    ("p", transaction(1, 0, 35)),
                    ("r", validity_transaction(2, 0, 30, vec![balance_at_least(UNRELATED, 1)])),
                    // An always satisfied predicate gives `y` the same pool priority as `r`.
                    ("y", validity_transaction(3, 0, 30, vec![balance_at_least(UNRELATED, 0)])),
                    ("l", transaction(4, 0, 1)),
                ],
                Vec::new(),
            )
            .only_first(&["r"])
            .write("p", WATCHED, 1)
            .write("x", UNRELATED, 1)
            .fail_if(fails, "r")
            .assert_parity();
            assert!(claims.is_empty(), "{log:#?}");
            let outcome = if fails { "reject r" } else { "commit r" };
            assert_in_order(
                &log,
                &["flashblock 1", "park x", "commit p", "promote x true", "commit x", outcome],
            );
            assert_in_order(&log, &[outcome, "commit y", "commit l"]);
        }
    }

    /// The sidecar head `s` outranks the resting `r`, so the merge would return `s` first and
    /// `r` is not claimed; `s` commits and wakes `r`, which is then yielded from the merge.
    #[test]
    fn resting_head_behind_the_sidecar_head_is_not_claimed() {
        for fails in [false, true] {
            let (log, claims) = Scenario::new(
                vec![
                    ("r", validity_transaction(0, 0, 5, vec![balance_at_least(WATCHED, 1)])),
                    ("p", transaction(1, 0, 2)),
                ],
                vec![
                    ("s", sidecar_transaction(10, 10, Vec::new())),
                    ("t", sidecar_transaction(11, 3, Vec::new())),
                ],
            )
            .only_first(&["r"])
            .write("s", WATCHED, 1)
            .fail_if(fails, "r")
            .assert_parity();
            assert!(claims.is_empty(), "{log:#?}");
            let outcome = if fails { "reject r" } else { "commit r" };
            assert_in_order(&log, &["flashblock 1", "commit s", outcome, "commit t", "commit p"]);
        }
    }

    /// The resting `r1` is popped while its lane is occupied by the build-loop-parked `r0`, so
    /// it is buffered rather than claimed; once `r0` commits it is released and parked from the
    /// ready set. When `r0` fails the lane is invalidated with `r1` in it.
    #[test]
    fn resting_descendant_in_an_occupied_lane_is_not_claimed() {
        for fails in [false, true] {
            let (log, claims) = Scenario::new(
                vec![
                    ("r0", validity_transaction(0, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                    ("r1", validity_transaction(0, 1, 40, vec![balance_at_least(UNRELATED, 1)])),
                    ("p", transaction(1, 0, 20)),
                ],
                vec![("w", sidecar_transaction(10, 10, Vec::new()))],
            )
            .only_first(&["r1"])
            .write("w", WATCHED, 1)
            .fail_if(fails, "r0")
            .assert_parity();
            assert!(!claims.contains(&"1:r1".to_owned()), "{log:#?}");
            assert_in_order(&log, &["flashblock 0", "park r1", "flashblock 1", "park r0"]);
        }
    }

    /// At a base fee the resting `r` cannot pay, the merge marks it invalid together with its
    /// descendant instead of the filter parking it, so it does not count as resting parked.
    #[test]
    fn underpriced_resting_transaction_is_invalidated_not_claimed() {
        let (log, claims) = Scenario::new(
            vec![
                (
                    "r",
                    validity_transaction_with_max_fee(
                        0,
                        0,
                        30,
                        40,
                        vec![balance_at_least(WATCHED, 1)],
                    ),
                ),
                ("rc", transaction(0, 1, 25)),
                ("p", transaction(1, 0, 20)),
            ],
            Vec::new(),
        )
        .only_first(&["r"])
        .base_fee(1, 50)
        .base_fee(2, 50)
        .assert_parity();
        assert!(claims.is_empty(), "{log:#?}");
        assert!(!log.iter().any(|event| event == "commit r" || event == "commit rc"), "{log:#?}");
    }

    /// A resting transaction another job rejected falls through the filter to the adapter's
    /// rejection path, and a committed resting transaction re-added later takes the committed
    /// path.
    #[test]
    fn rejected_or_committed_resting_transactions_are_not_claimed() {
        let (log, claims) = Scenario::new(
            vec![
                ("r", validity_transaction(0, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                ("k", validity_transaction(1, 0, 25, vec![balance_at_least(UNRELATED, 1)])),
                ("x", transaction(2, 0, 2)),
            ],
            vec![("w", sidecar_transaction(10, 10, Vec::new()))],
        )
        .only_first(&["r", "k", "x"])
        .write("x", UNRELATED, 1)
        .rejected_elsewhere(0, "r")
        .assert_parity();
        assert!(!claims.iter().any(|claim| claim.ends_with(":r")), "{log:#?}");
        assert_in_order(&log, &["flashblock 0", "park r", "park k", "commit x", "promote k true"]);
        assert!(!log.iter().any(|event| event == "commit r"), "{log:#?}");
    }

    /// A claimed resting head's higher-priority descendant, already in the snapshot, is
    /// buffered behind it and released once the head commits.
    #[test]
    fn snapshot_descendant_of_a_claimed_head_waits_for_it() {
        for fails in [false, true] {
            let (log, claims) = Scenario::new(
                vec![
                    ("r0", validity_transaction(0, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                    ("r1", transaction(0, 1, 45)),
                    ("p", transaction(1, 0, 20)),
                ],
                vec![("w", sidecar_transaction(10, 10, Vec::new()))],
            )
            .only_first(&["r0"])
            .write("w", WATCHED, 1)
            .fail_if(fails, "r0")
            .assert_parity();
            assert_eq!(claims, ["1:r0"], "{log:#?}");
            if fails {
                assert!(!log.iter().any(|event| event == "commit r1"), "{log:#?}");
            } else {
                assert_in_order(&log, &["flashblock 1", "commit w", "commit r0", "commit r1"]);
            }
        }
    }

    /// More live updates than one source round admits are queued when `r` is claimed. The
    /// first round is processed against the previous pop's priority and the rest against
    /// `r`'s, so updates between the two are stashed exactly as when `r` is yielded.
    #[test]
    fn claimed_transaction_bounds_the_next_update_round() {
        for fails in [false, true] {
            let mut scenario = Scenario::new(
                vec![
                    ("x", transaction(0, 0, 40)),
                    ("r", validity_transaction(1, 0, 30, vec![balance_at_least(WATCHED, 1)])),
                ],
                vec![("w", sidecar_transaction(10, 5, Vec::new()))],
            )
            .only_first(&["r"])
            .write("w", WATCHED, 1)
            .fail_if(fails, "r");
            let first_round = (41..49).chain(20..28).map(|priority| ("a", priority));
            let second_round = [35, 33, 25, 38].map(|priority| ("b", priority));
            for (index, (round, priority)) in first_round.chain(second_round).enumerate() {
                let sender = 100 + index as u64;
                let name = format!("{round}{priority}");
                scenario = scenario.arrive(1, 1, &name, transaction(sender, 0, priority));
            }
            let (log, claims) = scenario.assert_parity();
            assert_eq!(claims, ["1:r"], "{log:#?}");
            let fb2 = log.iter().position(|event| event == "flashblock 2").unwrap();
            for stashed in ["b33", "b35", "b38", "a41"] {
                let committed = format!("commit {stashed}");
                assert!(!log[..fb2].contains(&committed), "{stashed} must be stashed: {log:#?}");
            }
            assert!(log[..fb2].contains(&"commit b25".to_owned()), "{log:#?}");
        }
    }

    /// Resting sidecar transactions are never claimed; the lane-parking iterator parks them when
    /// it would yield them.
    #[test]
    fn resting_sidecar_transaction_is_parked_on_yield() {
        let (log, claims) = Scenario::new(
            vec![("p", transaction(0, 0, 2))],
            vec![
                ("z", sidecar_transaction(10, 7, vec![balance_at_least(WATCHED, 1)])),
                ("w", sidecar_transaction(11, 5, Vec::new())),
            ],
        )
        .only_first(&["z"])
        .write("w", WATCHED, 1)
        .assert_parity();
        assert!(claims.is_empty(), "{log:#?}");
        assert_in_order(&log, &["flashblock 1", "commit w", "commit z"]);
    }

    /// A protocol-lane EIP-8130 transaction that pays `tip` to the sequencer fee vault and
    /// declares `gas_limit`.
    fn coinbase_tip_transaction(sender: u64, tip: U256, gas_limit: u64) -> Tx {
        let tip = Call { to: Predeploys::SEQUENCER_FEE_VAULT, value: tip, data: Bytes::new() };
        let tx = eip8130_transaction_with(
            sender,
            TxEip8130 {
                chain_id: 1,
                sender: Some(sender_address(sender)),
                nonce_key: U256::ZERO,
                nonce_sequence: 0,
                valid_after: 0,
                valid_before: 0,
                max_priority_fee_per_gas: 0,
                max_fee_per_gas: 0,
                gas_limit,
                account_changes: Vec::new(),
                calls: vec![vec![tip]],
                metadata: Bytes::new(),
                payer: None,
            },
            Vec::new(),
        );
        assert!(!tx.transaction.is_eip8130_sidecar_transaction());
        tx
    }

    /// Coinbase-tip priorities saturate when cross-multiplied, so the ordering is not
    /// transitive. The snapshot must keep the pool's historical pop order there: of B (tip
    /// `M/2G+1`, gas `3G`), A (tip `M/G+1`, gas `2G`) and a live C (tip `M/2G+1`, gas `G`), the
    /// pending pool's `BTreeSet` pops B.
    #[test]
    fn snapshot_keeps_historical_pop_order_under_saturating_tips() {
        let gas = 100_000;
        let half = U256::MAX / U256::from(2 * gas) + U256::from(1);
        let b = coinbase_tip_transaction(0, half, 3 * gas);
        let a = coinbase_tip_transaction(1, U256::MAX / U256::from(gas) + U256::from(1), 2 * gas);
        let c = coinbase_tip_transaction(2, half, gas);
        let mut pool = pending_pool(&[Arc::clone(&b), Arc::clone(&a)]);
        let mut best = pool.best();
        pool.add_transaction(Arc::clone(&c), 0);

        assert_eq!(best.next().map(|tx| *tx.hash()), Some(*b.hash()));
    }

    /// Yields every candidate of `inner` only after `delay`, as a slow pool snapshot would.
    struct DelayedYield {
        inner: Parkable,
        delay: Duration,
    }

    impl PayloadTransactions for DelayedYield {
        type Transaction = Tx;

        fn next(&mut self, ctx: ()) -> Option<Tx> {
            let next = self.inner.next(ctx);
            std::thread::sleep(self.delay);
            next
        }

        fn mark_invalid(&mut self, sender: Address, nonce: u64) {
            self.inner.mark_invalid(sender, nonce);
        }
    }

    impl ParkablePayloadTransactions for DelayedYield {
        type Pooled = BasePooledTransaction;

        fn park_current(&mut self) {
            self.inner.park_current();
        }

        fn mark_current_committed(&mut self) {
            self.inner.mark_current_committed();
        }

        fn promote(&mut self, transaction_hash: TxHash) -> bool {
            self.inner.promote(transaction_hash)
        }

        fn discard_parked(&mut self, transaction_hash: TxHash) -> bool {
            self.inner.discard_parked(transaction_hash)
        }

        fn set_parking_filter(
            &mut self,
            filter: Arc<dyn base_execution_txpool::ParkingFilter<BasePooledTransaction>>,
        ) {
            self.inner.set_parking_filter(filter);
        }
    }

    /// A resting transaction the parking filter lets through because it is rejected stays
    /// parked when its rejection expires before the adapter checks the cache, instead of being
    /// yielded to the build loop with its predicate still unsatisfied.
    #[test]
    fn resting_transaction_stays_parked_when_its_rejection_expires_during_next() {
        let predicate = balance_at_least(WATCHED, 1);
        let resting = validity_transaction(0, 0, 10, vec![predicate.clone()]);
        let pool = pending_pool(&[Arc::clone(&resting)]);
        let ttl = Duration::from_millis(200);
        let delayed = || DelayedYield { inner: parkable(&pool), delay: ttl + ttl / 4 };
        let mut iterator = BestFlashblocksTxs::new(delayed(), RejectionCache::new(100, ttl))
            .with_resting_predicate_mode(RestingPredicateMode::Enforce);
        let hash = *resting.hash();
        assert_eq!(iterator.next(()).map(|tx| *tx.hash()), Some(hash));
        iterator.park_current();
        iterator.rest(hash, &predicate);
        iterator.take_resting_stats();
        iterator.mark_rejected(&[hash]);

        iterator.refresh_iterator(delayed());

        assert!(iterator.next(()).is_none());
        assert_eq!(iterator.take_resting_stats().parked, 1);
    }

    /// The sidecar is the production 2D nonce snapshot: equal bids pop in arrival order whatever
    /// order they are listed in, a failed channel head invalidates only its own channel, and a
    /// resting nonce-free transaction is parked by the filter and woken by its writer.
    #[test]
    fn resting_filter_matches_per_yield_parking_over_sidecar_channels() {
        let predicate = balance_at_least(WATCHED, 1);
        let writer = eip8130_transaction(50, U256::from(1), 0, 5, Vec::new());
        let failing_head = eip8130_transaction(51, U256::from(1), 0, 9, Vec::new());
        let blocked_descendant = eip8130_transaction(51, U256::from(1), 1, 9, Vec::new());
        let other_channel = eip8130_transaction(51, U256::from(2), 0, 8, Vec::new());
        let nonce_free =
            eip8130_transaction(52, Eip8130Constants::NONCE_KEY_MAX, 0, 7, vec![predicate]);
        let same_bid = eip8130_transaction(53, U256::from(1), 0, 5, Vec::new());
        let scenario = Scenario::new(
            vec![],
            vec![
                ("same_bid", same_bid),
                ("other_channel", other_channel),
                ("blocked_descendant", blocked_descendant),
                ("failing_head", failing_head),
                ("nonce_free", nonce_free),
                ("writer", writer),
            ],
        )
        .write("same_bid", WATCHED, 0)
        .write("writer", WATCHED, 1)
        .fail_if(true, "failing_head");

        let (log, _) = scenario.assert_parity();

        assert!(!log.iter().any(|event| event == "commit blocked_descendant"), "{log:#?}");
        assert_in_order(
            &log,
            &[
                "flashblock 0",
                "reject failing_head",
                "commit other_channel",
                "park nonce_free",
                "commit writer",
                "promote nonce_free true",
                "commit nonce_free",
                "commit same_bid",
                "flashblock 1",
            ],
        );
    }

    /// The build loop indexes a parked transaction under its first unsatisfied predicate and
    /// only rescans it when that predicate's state changes, so a later write that breaks an
    /// earlier predicate leaves it resting on the original blocker, as production does.
    #[test]
    fn resting_filter_matches_per_yield_parking_when_a_satisfied_predicate_breaks() {
        let other = Address::repeat_byte(0xcc);
        let both = vec![balance_at_least(WATCHED, 1), balance_at_least(other, 1)];
        let scenario = Scenario::new(
            vec![
                ("raise", validity_transaction(0, 0, 50, Vec::new())),
                ("resting", validity_transaction(1, 0, 40, both)),
                ("lower", validity_transaction(2, 0, 30, Vec::new())),
                ("raise_again", validity_transaction(3, 0, 45, Vec::new())),
            ],
            vec![],
        )
        .only_first(&["raise", "resting", "lower"])
        .protocol_pool(1, &["raise_again", "resting"])
        .write("raise", WATCHED, 1)
        .write("lower", WATCHED, 0)
        .write("raise_again", WATCHED, 1);

        let (log, _) = scenario.assert_parity();

        assert_in_order(
            &log,
            &[
                "commit raise",
                "park resting",
                "commit lower",
                "flashblock 1",
                "commit raise_again",
                "rest resting",
            ],
        );
        let second = log.iter().position(|event| event == "flashblock 1").expect("two flashblocks");
        assert!(!log[second..].iter().any(|event| event == "park resting"), "{log:#?}");
    }

    /// A deterministic xorshift generator, so failures reproduce from their seed.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }

        fn chance(&mut self, percent: u64) -> bool {
            self.below(100) < percent
        }
    }

    /// A random scenario over protocol chains and sidecar channels and nonce-free transactions
    /// with up to two predicates each, writes that move balances up and down, failures, live
    /// heads, descendants and replacements, rejections by other jobs and base fees.
    fn random_scenario(seed: u64) -> Scenario {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let watched = [WATCHED, UNRELATED, Address::repeat_byte(0xcc)];
        let predicates = |rng: &mut Rng| {
            let first = rng.below(3) as usize;
            let mut predicates = Vec::new();
            if rng.chance(40) {
                predicates.push(balance_at_least(watched[first], 1 + rng.below(2)));
                if rng.chance(40) {
                    predicates.push(balance_at_least(watched[(first + 1) % 3], 1 + rng.below(2)));
                }
            }
            predicates
        };
        let tx = |rng: &mut Rng, sender: u64, nonce: u64| {
            let priority = 1 + rng.below(8) as u128;
            let predicates = predicates(rng);
            let max_fee = priority + rng.below(5) as u128;
            validity_transaction_with_max_fee(sender, nonce, priority, max_fee, predicates)
        };
        let mut protocol = Vec::new();
        let mut chains = Vec::new();
        let mut original_chains = Vec::new();
        let senders = 3 + rng.below(4);
        for sender in 0..senders {
            let length = 1 + rng.below(3);
            for nonce in 0..length {
                protocol.push((format!("p{sender}.{nonce}"), tx(&mut rng, sender, nonce)));
            }
            chains.push(length);
            original_chains.push(length);
        }
        let mut sidecar = Vec::new();
        for sender in 50..50 + rng.below(3) {
            for channel in 1..=1 + rng.below(2) {
                for sequence in 0..1 + rng.below(2) {
                    let priority = 1 + rng.below(8) as u128;
                    let predicates = predicates(&mut rng);
                    let nonce_key = U256::from(channel);
                    let tx = eip8130_transaction(sender, nonce_key, sequence, priority, predicates);
                    sidecar.push((format!("s{sender}.{channel}.{sequence}"), tx));
                }
            }
            if rng.chance(40) {
                let priority = 1 + rng.below(8) as u128;
                let predicates = predicates(&mut rng);
                let nonce_key = Eip8130Constants::NONCE_KEY_MAX;
                let tx = eip8130_transaction(sender, nonce_key, 0, priority, predicates);
                sidecar.push((format!("s{sender}.free"), tx));
            }
        }
        let names: Vec<String> =
            protocol.iter().chain(&sidecar).map(|(name, _)| name.clone()).collect();
        let mut scenario = Scenario::new(
            protocol.iter().map(|(name, tx)| (name.as_str(), Arc::clone(tx))).collect(),
            sidecar.iter().map(|(name, tx)| (name.as_str(), Arc::clone(tx))).collect(),
        );
        if rng.chance(50) {
            scenario.flashblocks.push(scenario.flashblocks[0].clone());
        }
        let first: Vec<&str> =
            names.iter().filter(|_| rng.chance(60)).map(String::as_str).collect();
        scenario = scenario.only_first(&first);
        if rng.chance(50) {
            scenario.ordered_threshold = 1;
        }
        for name in &names {
            if rng.chance(30) {
                let value = rng.below(3);
                scenario = scenario.write(name, watched[rng.below(3) as usize], value);
            }
            let fails = rng.chance(12);
            scenario = scenario.fail_if(fails, name);
        }
        for flashblock in 1..scenario.flashblocks.len() {
            if rng.chance(20) {
                scenario = scenario.base_fee(flashblock, 1 + rng.below(4));
            }
            if rng.chance(20) {
                let name = &names[rng.below(names.len() as u64) as usize];
                scenario = scenario.rejected_elsewhere(flashblock - 1, name);
            }
        }
        let mut next_sender = 100;
        for index in 0..rng.below(7) {
            let flashblock = rng.below(scenario.flashblocks.len() as u64) as usize;
            let after_commits = rng.below(4) as usize;
            let name = format!("a{index}");
            scenario = match rng.below(3) {
                0 => {
                    next_sender += 1;
                    let arrival = tx(&mut rng, next_sender, 0);
                    scenario.arrive(flashblock, after_commits, &name, arrival)
                }
                1 => {
                    let sender = rng.below(senders);
                    let nonce = chains[sender as usize];
                    chains[sender as usize] += 1;
                    let arrival = tx(&mut rng, sender, nonce);
                    scenario.arrive(flashblock, after_commits, &name, arrival)
                }
                _ => {
                    let sender = rng.below(senders);
                    let nonce = rng.below(original_chains[sender as usize]);
                    let arrival = tx(&mut rng, sender, nonce);
                    scenario.replace(flashblock, after_commits, &name, arrival)
                }
            };
        }
        scenario
    }

    /// Random scenarios select identically under the production adapter and the per-yield
    /// reference.
    #[test]
    fn resting_filter_matches_per_yield_parking_on_random_scenarios() {
        let mut claimed = 0;
        for seed in 0..1_500 {
            let scenario = random_scenario(seed);
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scenario.assert_parity()));
            match result {
                Ok((_, claims)) => claimed += claims.len(),
                Err(panic) => {
                    eprintln!("seed {seed} failed");
                    std::panic::resume_unwind(panic);
                }
            }
        }
        assert!(claimed > 500, "random scenarios must exercise claiming: {claimed}");
    }
}
