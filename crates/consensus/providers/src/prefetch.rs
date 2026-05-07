//! Online data-availability prefetching for derivation.

use std::{collections::VecDeque, fmt::Debug, sync::Arc};

use alloy_primitives::{Address, B256, Bytes};
use async_trait::async_trait;
use base_common_genesis::RollupConfig;
use base_consensus_derive::{
    BlobProvider, ChainProvider, DataAvailabilityProvider, EthereumDataSource, PipelineError,
    PipelineErrorKind, PipelineResult, ResetError,
};
use base_protocol::BlockInfo;
use tokio::task::JoinHandle;
use tracing::debug;

use crate::Metrics;

/// The maximum number of completed L1 data prefetch results retained locally.
pub const PREFETCH_BUFFER_CAPACITY: usize = 4;

/// Completed data-availability items for one L1 block and batcher address.
pub type PrefetchedData = (BlockInfo, Address, VecDeque<Bytes>);

/// The data collected by an L1 data-availability prefetch task.
pub type PrefetchResult = PipelineResult<PrefetchedData>;

/// A one-block lookahead wrapper for [`EthereumDataSource`].
#[derive(Debug)]
pub struct PrefetchingEthereumDataSource<C, B>
where
    C: ChainProvider + Send + Clone + Debug,
    B: BlobProvider + Send + Clone + Debug,
{
    /// The synchronous fallback source used for the block currently requested by the pipeline.
    pub source: EthereumDataSource<C, B>,
    /// Chain provider clone used by the background prefetch task.
    pub prefetch_chain_provider: C,
    /// Blob provider clone used by the background prefetch task.
    pub prefetch_blob_provider: B,
    /// Rollup config used to construct per-task data sources.
    pub rollup_config: Arc<RollupConfig>,
    /// Completed prefetched data, keyed by block and batcher address.
    pub prefetched: VecDeque<PrefetchedData>,
    /// Target metadata for the in-flight prefetch task: block number, expected parent hash,
    /// batcher address.
    pub prefetch_target: Option<(u64, B256, Address)>,
    /// The in-flight prefetch task.
    pub prefetch: Option<JoinHandle<PrefetchResult>>,
}

impl<C, B> PrefetchingEthereumDataSource<C, B>
where
    C: ChainProvider + Send + Sync + Clone + Debug + 'static,
    B: BlobProvider + Send + Sync + Clone + Debug + 'static,
{
    /// Creates a new prefetching source from an active source and provider clones.
    pub const fn new(
        source: EthereumDataSource<C, B>,
        prefetch_chain_provider: C,
        prefetch_blob_provider: B,
        rollup_config: Arc<RollupConfig>,
    ) -> Self {
        Self {
            source,
            prefetch_chain_provider,
            prefetch_blob_provider,
            rollup_config,
            prefetched: VecDeque::new(),
            prefetch_target: None,
            prefetch: None,
        }
    }

    /// Creates a new prefetching source from provider parts.
    pub fn new_from_parts(provider: C, blobs: B, cfg: Arc<RollupConfig>) -> Self {
        Self::new(
            EthereumDataSource::new_from_parts(provider.clone(), blobs.clone(), &cfg),
            provider,
            blobs,
            cfg,
        )
    }

    /// Prefetches all data-availability items for the block after `current_block_ref`.
    pub async fn prefetch_next_block(
        mut chain_provider: C,
        blob_provider: B,
        rollup_config: Arc<RollupConfig>,
        current_block_ref: BlockInfo,
        batcher_address: Address,
    ) -> PrefetchResult {
        let next_number = current_block_ref.number + 1;
        let next_block_ref =
            chain_provider.block_info_by_number(next_number).await.map_err(Into::into)?;

        if next_block_ref.parent_hash != current_block_ref.hash {
            return Err(ResetError::ReorgDetected(
                current_block_ref.hash,
                next_block_ref.parent_hash,
            )
            .into());
        }

        let mut source = EthereumDataSource::new_from_parts(
            chain_provider,
            blob_provider,
            rollup_config.as_ref(),
        );
        let mut prefetched = VecDeque::new();
        loop {
            match source.next(&next_block_ref, batcher_address).await {
                Ok(data) => prefetched.push_back(data),
                Err(PipelineErrorKind::Temporary(PipelineError::Eof)) => {
                    return Ok((next_block_ref, batcher_address, prefetched));
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Polls a finished background prefetch task into the local cache.
    pub async fn collect_finished_prefetch(&mut self) {
        let Some(prefetch) = self.prefetch.as_ref() else {
            return;
        };
        if !prefetch.is_finished() {
            return;
        }

        let Some(prefetch) = self.prefetch.take() else {
            return;
        };
        self.prefetch_target = None;
        match prefetch.await {
            Ok(Ok(result)) => self.store_prefetch_result(result),
            Ok(Err(error)) => {
                Metrics::l1_prefetch_outcomes("error").increment(1);
                debug!(target: "l1_prefetch", error = %error, "L1 data prefetch failed");
            }
            Err(error) => {
                Metrics::l1_prefetch_outcomes("error").increment(1);
                debug!(target: "l1_prefetch", error = %error, "L1 data prefetch task failed");
            }
        }
    }

    /// Starts a background prefetch for the block after `block_ref`.
    pub fn start_prefetch(&mut self, block_ref: &BlockInfo, batcher_address: Address) {
        let Some(next_number) = block_ref.number.checked_add(1) else {
            return;
        };
        let target = (next_number, block_ref.hash, batcher_address);

        if self.prefetch_target == Some(target) {
            return;
        }
        if self.prefetched.iter().any(|(block, batcher, _)| {
            block.number == next_number
                && block.parent_hash == block_ref.hash
                && *batcher == batcher_address
        }) {
            return;
        }

        if let Some(prefetch) = self.prefetch.take() {
            Metrics::l1_prefetch_outcomes("aborted").increment(1);
            prefetch.abort();
        }

        let chain_provider = self.prefetch_chain_provider.clone();
        let blob_provider = self.prefetch_blob_provider.clone();
        let rollup_config = Arc::clone(&self.rollup_config);
        let current_block_ref = *block_ref;
        self.prefetch_target = Some(target);
        self.prefetch = Some(tokio::spawn(async move {
            Self::prefetch_next_block(
                chain_provider,
                blob_provider,
                rollup_config,
                current_block_ref,
                batcher_address,
            )
            .await
        }));
    }

    /// Returns whether the completed prefetch cache matches the requested block.
    pub fn prefetched_matches(&self, block_ref: &BlockInfo, batcher_address: Address) -> bool {
        self.prefetched.front().is_some_and(|(block, batcher, _)| {
            block.hash == block_ref.hash && *batcher == batcher_address
        })
    }

    /// Moves a matching cached prefetch to the front of the ring buffer.
    pub fn promote_matching_prefetch(&mut self, block_ref: &BlockInfo, batcher_address: Address) {
        if self.prefetched_matches(block_ref, batcher_address) {
            return;
        }
        let Some(index) = self.prefetched.iter().position(|(block, batcher, _)| {
            block.hash == block_ref.hash && *batcher == batcher_address
        }) else {
            return;
        };
        let Some(result) = self.prefetched.remove(index) else {
            return;
        };
        self.prefetched.push_front(result);
    }

    /// Serves one item from the completed prefetch cache, or EOF if the cached block is empty.
    pub fn pop_prefetched(&mut self) -> Option<Bytes> {
        let (_, _, data) = self.prefetched.front_mut()?;
        match data.pop_front() {
            Some(data) => Some(data),
            None => {
                self.prefetched.pop_front();
                None
            }
        }
    }

    /// Awaits the in-flight prefetch if it targets the requested block.
    pub async fn await_matching_prefetch(
        &mut self,
        block_ref: &BlockInfo,
        batcher_address: Address,
    ) {
        let target_matches = self.prefetch_target.is_some_and(|(number, parent_hash, batcher)| {
            number == block_ref.number
                && parent_hash == block_ref.parent_hash
                && batcher == batcher_address
        });
        if !target_matches {
            return;
        }

        let Some(prefetch) = self.prefetch.take() else {
            self.prefetch_target = None;
            return;
        };
        self.prefetch_target = None;
        match prefetch.await {
            Ok(Ok(result)) => {
                let (prefetched_block, prefetched_batcher, _) = &result;
                if prefetched_block.hash == block_ref.hash && *prefetched_batcher == batcher_address
                {
                    self.store_prefetch_result(result);
                } else {
                    debug!(
                        target: "l1_prefetch",
                        requested_hash = %block_ref.hash,
                        prefetched_hash = %prefetched_block.hash,
                        "Dropping mismatched L1 data prefetch result"
                    );
                }
            }
            Ok(Err(error)) => {
                Metrics::l1_prefetch_outcomes("error").increment(1);
                debug!(target: "l1_prefetch", error = %error, "L1 data prefetch failed");
            }
            Err(error) => {
                Metrics::l1_prefetch_outcomes("error").increment(1);
                debug!(target: "l1_prefetch", error = %error, "L1 data prefetch task failed");
            }
        }
    }

    /// Records the number of completed prefetch results available to the pipeline.
    pub fn record_buffer_len(&self) {
        Metrics::l1_prefetch_buffer_len().set(self.prefetched.len() as f64);
    }

    /// Stores a completed prefetch result without overwriting a block that is still draining.
    pub fn store_prefetch_result(&mut self, result: PrefetchedData) {
        let (result_block, result_batcher, _) = &result;
        if let Some(index) = self.prefetched.iter().position(|(block, batcher, _)| {
            block.hash == result_block.hash && *batcher == *result_batcher
        }) {
            self.prefetched.remove(index);
        }
        if self.prefetched.len() == PREFETCH_BUFFER_CAPACITY {
            self.prefetched.pop_front();
            Metrics::l1_prefetch_outcomes("evicted").increment(1);
        }
        self.prefetched.push_back(result);
        Metrics::l1_prefetch_outcomes("stored").increment(1);
        self.record_buffer_len();
    }

    /// Drops completed prefetches that are neither the requested block nor its immediate child.
    pub fn drop_stale_prefetches(&mut self, block_ref: &BlockInfo, batcher_address: Address) {
        let before = self.prefetched.len();
        self.prefetched
            .retain(|prefetched| !Self::prefetch_is_stale(prefetched, block_ref, batcher_address));
        let dropped = before - self.prefetched.len();
        if dropped > 0 {
            Metrics::l1_prefetch_outcomes("stale").increment(dropped as u64);
            self.record_buffer_len();
        }
    }

    /// Returns whether a completed prefetch is stale relative to the current request.
    pub fn prefetch_is_stale(
        prefetched: &PrefetchedData,
        block_ref: &BlockInfo,
        batcher_address: Address,
    ) -> bool {
        let (block, batcher, _) = prefetched;
        if block.hash == block_ref.hash && *batcher == batcher_address {
            return false;
        }
        let next_number = block_ref.number.checked_add(1);
        Some(block.number) != next_number
            || block.parent_hash != block_ref.hash
            || *batcher != batcher_address
    }
}

#[async_trait]
impl<C, B> DataAvailabilityProvider for PrefetchingEthereumDataSource<C, B>
where
    C: ChainProvider + Send + Sync + Clone + Debug + 'static,
    B: BlobProvider + Send + Sync + Clone + Debug + 'static,
{
    type Item = Bytes;

    async fn next(
        &mut self,
        block_ref: &BlockInfo,
        batcher_address: Address,
    ) -> PipelineResult<Self::Item> {
        self.collect_finished_prefetch().await;
        self.await_matching_prefetch(block_ref, batcher_address).await;
        self.promote_matching_prefetch(block_ref, batcher_address);
        if self.prefetched_matches(block_ref, batcher_address) {
            Metrics::l1_prefetch_outcomes("hit").increment(1);
            self.start_prefetch(block_ref, batcher_address);
            let result = self.pop_prefetched().ok_or(PipelineError::Eof.temp());
            self.record_buffer_len();
            return result;
        }

        self.drop_stale_prefetches(block_ref, batcher_address);

        Metrics::l1_prefetch_outcomes("miss").increment(1);
        self.start_prefetch(block_ref, batcher_address);
        self.source.next(block_ref, batcher_address).await
    }

    fn clear(&mut self) {
        self.source.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fmt::{Display, Formatter},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        vec,
    };

    use alloy_consensus::{Header, Receipt, TxEnvelope};
    use alloy_eips::eip4844::Blob;
    use alloy_primitives::b256;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestProviderError;

    impl Display for TestProviderError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("test provider error")
        }
    }

    impl From<TestProviderError> for PipelineErrorKind {
        fn from(_: TestProviderError) -> Self {
            PipelineError::Provider("test provider error".to_string()).temp()
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestCounters {
        block_by_number: Arc<AtomicUsize>,
        block_by_hash: Arc<AtomicUsize>,
    }

    #[derive(Debug, Clone, Default)]
    struct TestChainProvider {
        blocks_by_number: Arc<HashMap<u64, BlockInfo>>,
        blocks_by_hash: Arc<HashMap<B256, (BlockInfo, Vec<TxEnvelope>)>>,
        counters: TestCounters,
    }

    impl TestChainProvider {
        fn new(blocks: Vec<BlockInfo>) -> Self {
            let blocks_by_number = blocks.iter().map(|block| (block.number, *block)).collect();
            let blocks_by_hash =
                blocks.into_iter().map(|block| (block.hash, (block, Vec::new()))).collect();
            Self {
                blocks_by_number: Arc::new(blocks_by_number),
                blocks_by_hash: Arc::new(blocks_by_hash),
                counters: TestCounters::default(),
            }
        }
    }

    #[async_trait]
    impl ChainProvider for TestChainProvider {
        type Error = TestProviderError;

        async fn header_by_hash(&mut self, _: B256) -> Result<Header, Self::Error> {
            Err(TestProviderError)
        }

        async fn block_info_by_number(&mut self, number: u64) -> Result<BlockInfo, Self::Error> {
            self.counters.block_by_number.fetch_add(1, Ordering::Relaxed);
            self.blocks_by_number.get(&number).copied().ok_or(TestProviderError)
        }

        async fn receipts_by_hash(&mut self, _: B256) -> Result<Vec<Receipt>, Self::Error> {
            Err(TestProviderError)
        }

        async fn block_info_and_transactions_by_hash(
            &mut self,
            hash: B256,
        ) -> Result<(BlockInfo, Vec<TxEnvelope>), Self::Error> {
            self.counters.block_by_hash.fetch_add(1, Ordering::Relaxed);
            self.blocks_by_hash.get(&hash).cloned().ok_or(TestProviderError)
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestBlobProvider;

    #[async_trait]
    impl BlobProvider for TestBlobProvider {
        type Error = TestProviderError;

        async fn get_and_validate_blobs(
            &mut self,
            _: &BlockInfo,
            _: &[B256],
        ) -> Result<Vec<Box<Blob>>, Self::Error> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn serves_matching_empty_prefetch_without_live_l1_hash_fetch() {
        let block_1 = BlockInfo {
            number: 1,
            hash: b256!("0000000000000000000000000000000000000000000000000000000000000001"),
            parent_hash: B256::ZERO,
            timestamp: 1,
        };
        let block_2 = BlockInfo {
            number: 2,
            hash: b256!("0000000000000000000000000000000000000000000000000000000000000002"),
            parent_hash: block_1.hash,
            timestamp: 2,
        };
        let active_chain = TestChainProvider::new(vec![block_1, block_2]);
        let prefetch_chain = TestChainProvider::new(vec![block_1, block_2]);
        let active_counters = active_chain.counters.clone();
        let prefetch_counters = prefetch_chain.counters.clone();
        let cfg = Arc::new(RollupConfig::default());
        let active_source =
            EthereumDataSource::new_from_parts(active_chain, TestBlobProvider, cfg.as_ref());
        let mut source = PrefetchingEthereumDataSource::new(
            active_source,
            prefetch_chain,
            TestBlobProvider,
            cfg,
        );

        assert!(matches!(
            source.next(&block_1, Address::ZERO).await,
            Err(PipelineErrorKind::Temporary(PipelineError::Eof))
        ));
        source.clear();
        assert!(matches!(
            source.next(&block_2, Address::ZERO).await,
            Err(PipelineErrorKind::Temporary(PipelineError::Eof))
        ));

        assert_eq!(active_counters.block_by_hash.load(Ordering::Relaxed), 1);
        assert_eq!(prefetch_counters.block_by_number.load(Ordering::Relaxed), 1);
        assert_eq!(prefetch_counters.block_by_hash.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn keeps_next_lookahead_while_draining_current_prefetch() {
        let block_1 = BlockInfo {
            number: 1,
            hash: b256!("0000000000000000000000000000000000000000000000000000000000000001"),
            parent_hash: B256::ZERO,
            timestamp: 1,
        };
        let block_2 = BlockInfo {
            number: 2,
            hash: b256!("0000000000000000000000000000000000000000000000000000000000000002"),
            parent_hash: block_1.hash,
            timestamp: 2,
        };
        let block_3 = BlockInfo {
            number: 3,
            hash: b256!("0000000000000000000000000000000000000000000000000000000000000003"),
            parent_hash: block_2.hash,
            timestamp: 3,
        };
        let active_chain = TestChainProvider::new(vec![block_1, block_2, block_3]);
        let prefetch_chain = TestChainProvider::new(vec![block_1, block_2, block_3]);
        let active_source = EthereumDataSource::new_from_parts(
            active_chain,
            TestBlobProvider,
            &RollupConfig::default(),
        );
        let mut source = PrefetchingEthereumDataSource::new(
            active_source,
            prefetch_chain,
            TestBlobProvider,
            Arc::new(RollupConfig::default()),
        );
        source.prefetched.push_back((
            block_2,
            Address::ZERO,
            VecDeque::from([Bytes::from_static(b"first"), Bytes::from_static(b"second")]),
        ));
        source.prefetched.push_back((
            block_3,
            Address::ZERO,
            VecDeque::from([Bytes::from_static(b"third")]),
        ));

        assert_eq!(
            source.next(&block_2, Address::ZERO).await.unwrap(),
            Bytes::from_static(b"first")
        );
        assert_eq!(
            source.next(&block_2, Address::ZERO).await.unwrap(),
            Bytes::from_static(b"second")
        );
        assert!(matches!(
            source.next(&block_2, Address::ZERO).await,
            Err(PipelineErrorKind::Temporary(PipelineError::Eof))
        ));
        assert_eq!(
            source.next(&block_3, Address::ZERO).await.unwrap(),
            Bytes::from_static(b"third")
        );
    }
}
