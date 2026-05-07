//! Criterion benches for compressed channel and span-batch decode costs.

use std::hint::black_box;

use alloy_eips::BlockNumHash;
use alloy_primitives::{B256, address, b256};
use async_trait::async_trait;
use base_common_consensus::BaseBlock;
use base_common_genesis::{ChainGenesis, HardForkConfig, RollupConfig};
use base_protocol::{
    Batch, BatchReader, BatchValidationProvider, BatchValidity, BlockInfo, L2BlockInfo, SpanBatch,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use miniz_oxide::inflate::decompress_to_vec_zlib;

fn fixture_zlib() -> Vec<u8> {
    let fixture = include_str!("../../derive/testdata/batch.hex").trim();
    alloy_primitives::hex::decode(fixture).expect("fixture should decode")
}

fn fixture_brotli() -> Vec<u8> {
    let decompressed = decompress_to_vec_zlib(&fixture_zlib()).expect("fixture should decompress");
    let params = brotli::enc::BrotliEncoderParams::default();
    let mut compressed = Vec::new();
    let mut input = &decompressed[..];
    brotli::BrotliCompress(&mut input, &mut compressed, &params)
        .expect("fixture should brotli-compress");

    let mut channel = Vec::with_capacity(compressed.len() + 1);
    channel.push(BatchReader::CHANNEL_VERSION_BROTLI);
    channel.extend_from_slice(&compressed);
    channel
}

fn rollup_config(holocene_time: Option<u64>) -> RollupConfig {
    RollupConfig {
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
            isthmus_time: holocene_time.map(|_| 0),
            holocene_time,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn new_reader(data: Vec<u8>) -> BatchReader {
    BatchReader::new(data, RollupConfig::MAX_RLP_BYTES_PER_CHANNEL_FJORD as usize)
}

const fn decompressed_reader(decompressed: Vec<u8>) -> BatchReader {
    BatchReader {
        data: None,
        decompressed,
        cursor: 0,
        max_rlp_bytes_per_channel: RollupConfig::MAX_RLP_BYTES_PER_CHANNEL_FJORD as usize,
        brotli_used: false,
    }
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

fn parent_for(span: &SpanBatch, cfg: &RollupConfig, scenario: Scenario) -> L2BlockInfo {
    let timestamp = match scenario {
        Scenario::Accept => span.starting_timestamp() - cfg.block_time,
        Scenario::Past => span.final_timestamp(),
        Scenario::Future => span.starting_timestamp() - cfg.block_time * 2,
    };
    L2BlockInfo {
        block_info: BlockInfo {
            number: 7,
            timestamp,
            hash: parent_hash(span),
            ..Default::default()
        },
        l1_origin: BlockNumHash { number: span.starting_epoch_num(), hash: origin_hash(span) },
        ..Default::default()
    }
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    Accept,
    Past,
    Future,
}

impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Past => "past",
            Self::Future => "future",
        }
    }

    const fn expected_validity(self) -> BatchValidity {
        match self {
            Self::Accept => BatchValidity::Accept,
            Self::Past => BatchValidity::Past,
            Self::Future => BatchValidity::Future,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum BenchValidationError {
    #[error("block not found")]
    BlockNotFound,
}

#[derive(Debug, Default)]
struct BenchValidator {
    blocks: Vec<L2BlockInfo>,
    op_blocks: Vec<BaseBlock>,
}

#[async_trait]
impl BatchValidationProvider for BenchValidator {
    type Error = BenchValidationError;

    async fn l2_block_info_by_number(&mut self, number: u64) -> Result<L2BlockInfo, Self::Error> {
        self.blocks
            .iter()
            .find(|block| block.block_info.number == number)
            .copied()
            .ok_or(BenchValidationError::BlockNotFound)
    }

    async fn block_by_number(&mut self, number: u64) -> Result<BaseBlock, Self::Error> {
        self.op_blocks
            .iter()
            .find(|block| block.header.number == number)
            .cloned()
            .ok_or(BenchValidationError::BlockNotFound)
    }
}

fn decompression_benches(c: &mut Criterion) {
    let zlib = fixture_zlib();
    let brotli = fixture_brotli();
    let mut group = c.benchmark_group("batch_reader_decompress");

    for (name, data) in [("zlib", zlib), ("brotli", brotli)] {
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_function(name, |b| {
            b.iter_batched(
                || new_reader(data.clone()),
                |mut reader| reader.decompress().expect("fixture should decompress"),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn next_batch_benches(c: &mut Criterion) {
    let cfg = rollup_config(Some(0));
    let decompressed = decompress_to_vec_zlib(&fixture_zlib()).expect("fixture should decompress");
    let mut group = c.benchmark_group("batch_reader_next_batch");
    group.throughput(Throughput::Bytes(decompressed.len() as u64));

    group.bench_function("rlp_decode_only", |b| {
        b.iter_batched(
            || decompressed_reader(decompressed.clone()),
            |mut reader| black_box(reader.next_batch(&cfg).expect("fixture should decode")),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("decompress_and_decode", |b| {
        b.iter_batched(
            || new_reader(fixture_zlib()),
            |mut reader| black_box(reader.next_batch(&cfg).expect("fixture should decode")),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn span_validation_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should build");
    let cfg_holocene = rollup_config(Some(0));
    let cfg_pre_holocene = rollup_config(None);
    let span = first_span(&cfg_holocene);
    let origins = l1_origins(&span);
    let mut group = c.benchmark_group("span_batch_validation");

    for scenario in [Scenario::Accept, Scenario::Past, Scenario::Future] {
        let cfg =
            if matches!(scenario, Scenario::Future) { &cfg_pre_holocene } else { &cfg_holocene };
        let parent = parent_for(&span, cfg, scenario);
        group.bench_with_input(
            BenchmarkId::new("check_batch", scenario.name()),
            &scenario,
            |b, scenario| {
                b.iter_batched(
                    BenchValidator::default,
                    |mut validator| {
                        let validity = runtime.block_on(span.check_batch(
                            cfg,
                            &origins,
                            parent,
                            &origins[0],
                            &mut validator,
                        ));
                        assert_eq!(validity, scenario.expected_validity());
                        black_box(validity)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.bench_function("get_singular_batches", |b| {
        let parent = parent_for(&span, &cfg_holocene, Scenario::Accept);
        b.iter(|| {
            let singles =
                span.get_singular_batches(&origins, parent).expect("accept path should expand");
            assert!(!singles.is_empty());
            black_box(singles)
        });
    });

    group.finish();
}

criterion_group!(benches, decompression_benches, next_batch_benches, span_validation_benches);
criterion_main!(benches);
