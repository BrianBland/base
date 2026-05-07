//! Criterion benches for stale span-batch handling in `BatchQueue`.

use std::{hint::black_box, sync::Arc};

use alloy_eips::BlockNumHash;
use alloy_primitives::{B256, address, b256};
use async_trait::async_trait;
use base_common_consensus::BaseBlock;
use base_common_genesis::{ChainGenesis, HardForkConfig, RollupConfig, SystemConfig};
use base_consensus_derive::{
    BatchQueue, L2ChainProvider, NextBatchProvider, OriginAdvancer, OriginProvider, PipelineError,
    PipelineErrorKind, PipelineResult, StageReset,
};
use base_protocol::{
    Batch, BatchReader, BatchValidationProvider, BatchWithInclusionBlock, BlockInfo, L2BlockInfo,
    SpanBatch,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn fixture_zlib() -> Vec<u8> {
    let fixture = include_str!("../testdata/batch.hex").trim();
    alloy_primitives::hex::decode(fixture).expect("fixture should decode")
}

fn rollup_config() -> Arc<RollupConfig> {
    Arc::new(RollupConfig {
        block_time: 100,
        max_sequencer_drift: 10_000_000_000,
        seq_window_size: 10_000_000,
        batch_inbox_address: address!("6887246668a3b87f54deb3b94ba47a6f63f32985"),
        genesis: ChainGenesis {
            l1: BlockNumHash { number: 16988980031808077784, ..Default::default() },
            l2: BlockNumHash {
                number: 8,
                hash: b256!("4444444444444444444444444444444444444444444444444444444444444444"),
            },
            ..Default::default()
        },
        hardforks: HardForkConfig {
            delta_time: Some(0),
            fjord_time: Some(0),
            isthmus_time: Some(0),
            holocene_time: Some(0),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn new_reader(data: Vec<u8>) -> BatchReader {
    BatchReader::new(data, RollupConfig::MAX_RLP_BYTES_PER_CHANNEL_FJORD as usize)
}

fn decompressed_fixture_len() -> usize {
    let mut reader = new_reader(fixture_zlib());
    reader.decompress().expect("fixture should decompress");
    reader.decompressed.len()
}

fn first_span(cfg: &RollupConfig) -> SpanBatch {
    let mut reader = new_reader(fixture_zlib());
    while let Some(batch) = reader.next_batch(cfg) {
        if let Batch::Span(span) = batch {
            return span;
        }
    }
    panic!("fixture should contain a span batch");
}

fn checked_hash(prefix: &[u8]) -> B256 {
    let mut hash = B256::ZERO;
    hash[..prefix.len()].copy_from_slice(prefix);
    hash
}

fn origin_hash(span: &SpanBatch) -> B256 {
    checked_hash(span.l1_origin_check.as_slice())
}

fn parent_hash(span: &SpanBatch) -> B256 {
    checked_hash(span.parent_check.as_slice())
}

fn l1_origins(span: &SpanBatch) -> Vec<BlockInfo> {
    let origin_hash = origin_hash(span);
    let mut origins = Vec::new();
    for batch in &span.batches {
        if origins.iter().any(|origin: &BlockInfo| origin.number == batch.epoch_num) {
            continue;
        }
        origins.push(BlockInfo {
            number: batch.epoch_num,
            timestamp: batch.timestamp,
            hash: origin_hash,
            ..Default::default()
        });
    }
    origins
}

fn past_parent(span: &SpanBatch) -> L2BlockInfo {
    L2BlockInfo {
        block_info: BlockInfo {
            number: 9,
            timestamp: span.final_timestamp(),
            hash: parent_hash(span),
            ..Default::default()
        },
        l1_origin: BlockNumHash { number: span.starting_epoch_num(), hash: origin_hash(span) },
        ..Default::default()
    }
}

fn queue_with_origin(
    cfg: Arc<RollupConfig>,
    span: &SpanBatch,
) -> BatchQueue<BenchNextBatchProvider, BenchL2Provider> {
    let origins = l1_origins(span);
    let origin = origins[0];
    let prev = BenchNextBatchProvider { origin: Some(origin), ..Default::default() };
    let mut queue = BatchQueue::new(cfg, prev, BenchL2Provider::default());
    queue.origin = Some(origin);
    queue.l1_blocks = origins;
    queue
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum BenchProviderError {
    #[error("block not found")]
    BlockNotFound,
    #[error("system config not found")]
    SystemConfigNotFound,
}

impl From<BenchProviderError> for PipelineErrorKind {
    fn from(error: BenchProviderError) -> Self {
        PipelineError::Provider(error.to_string()).temp()
    }
}

#[derive(Debug, Default)]
struct BenchNextBatchProvider {
    origin: Option<BlockInfo>,
    batches: Vec<PipelineResult<Batch>>,
    flushed: bool,
    reset: bool,
}

#[async_trait]
impl NextBatchProvider for BenchNextBatchProvider {
    async fn next_batch(&mut self, _: L2BlockInfo, _: &[BlockInfo]) -> PipelineResult<Batch> {
        self.batches.pop().ok_or(PipelineError::Eof.temp())?
    }

    fn span_buffer_size(&self) -> usize {
        self.batches.len()
    }

    fn flush(&mut self) {
        self.flushed = true;
    }
}

impl OriginProvider for BenchNextBatchProvider {
    fn origin(&self) -> Option<BlockInfo> {
        self.origin
    }
}

#[async_trait]
impl OriginAdvancer for BenchNextBatchProvider {
    async fn advance_origin(&mut self) -> PipelineResult<()> {
        self.origin = self.origin.map(|mut origin| {
            origin.number += 1;
            origin
        });
        Ok(())
    }
}

#[async_trait]
impl StageReset for BenchNextBatchProvider {
    async fn reset(&mut self, _: BlockNumHash, _: SystemConfig) -> PipelineResult<()> {
        self.reset = true;
        Ok(())
    }

    async fn activate(&mut self) -> PipelineResult<()> {
        Ok(())
    }

    async fn flush_channel(&mut self) -> PipelineResult<()> {
        self.flushed = true;
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct BenchL2Provider {
    blocks: Vec<L2BlockInfo>,
    op_blocks: Vec<BaseBlock>,
}

#[async_trait]
impl BatchValidationProvider for BenchL2Provider {
    type Error = BenchProviderError;

    async fn l2_block_info_by_number(&mut self, number: u64) -> Result<L2BlockInfo, Self::Error> {
        self.blocks
            .iter()
            .find(|block| block.block_info.number == number)
            .copied()
            .ok_or(BenchProviderError::BlockNotFound)
    }

    async fn block_by_number(&mut self, number: u64) -> Result<BaseBlock, Self::Error> {
        self.op_blocks
            .iter()
            .find(|block| block.header.number == number)
            .cloned()
            .ok_or(BenchProviderError::BlockNotFound)
    }
}

#[async_trait]
impl L2ChainProvider for BenchL2Provider {
    type Error = BenchProviderError;

    async fn system_config_by_number(
        &mut self,
        _: u64,
        _: Arc<RollupConfig>,
    ) -> Result<SystemConfig, <Self as L2ChainProvider>::Error> {
        Err(BenchProviderError::SystemConfigNotFound)
    }
}

fn add_past_batches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should build");
    let cfg = rollup_config();
    let span = first_span(&cfg);
    let compressed_len = fixture_zlib().len();
    let decompressed_len = decompressed_fixture_len();
    let parent = past_parent(&span);
    let mut group = c.benchmark_group("batch_queue_add_past_span");
    group.throughput(Throughput::Bytes((compressed_len + decompressed_len) as u64));

    for batch_count in [1usize, 4, 16] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_count),
            &batch_count,
            |b, batch_count| {
                b.iter_batched(
                    || queue_with_origin(Arc::clone(&cfg), &span),
                    |mut queue| {
                        for _ in 0..*batch_count {
                            runtime
                                .block_on(queue.add_batch(Batch::Span(span.clone()), parent))
                                .expect("past span should be dropped cleanly");
                        }
                        assert!(queue.batches.is_empty());
                        black_box(queue)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn derive_queued_past_batches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should build");
    let cfg = rollup_config();
    let span = first_span(&cfg);
    let origin = l1_origins(&span)[0];
    let parent = past_parent(&span);
    let mut group = c.benchmark_group("batch_queue_derive_queued_past_spans");

    for batch_count in [1usize, 4, 16, 64] {
        group.throughput(Throughput::Elements(batch_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_count),
            &batch_count,
            |b, batch_count| {
                b.iter_batched(
                    || {
                        let mut queue = queue_with_origin(Arc::clone(&cfg), &span);
                        queue.batches = (0..*batch_count)
                            .map(|_| BatchWithInclusionBlock {
                                inclusion_block: origin,
                                batch: Batch::Span(span.clone()),
                            })
                            .collect();
                        queue
                    },
                    |mut queue| {
                        let result = runtime.block_on(queue.derive_next_batch(false, parent));
                        assert!(result.is_err());
                        assert!(queue.batches.is_empty());
                        black_box(result)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, add_past_batches, derive_queued_past_batches);
criterion_main!(benches);
