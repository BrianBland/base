//! Compression comparison for current span batches versus zk sender-compressed batches.

use std::{collections::HashSet, sync::Arc};

use alloy_consensus::proofs::ordered_trie_root_encoded;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{B256, BlockHash, Bytes, FixedBytes};
use base_alloy_consensus::{OpTransactionSigned, OpTxEnvelope, TxZkSequencer, ZkSequencerTxBody};
use base_comp::{ChannelOut, CompressionAlgo, VariantCompressor};
use base_consensus_genesis::RollupConfig;
use base_protocol::{
    Batch, ChannelId, SingleBatch, SpanBatch, SpanBatchTransactions, ZkSpanBatch,
    ZkSpanBatchTransactions,
};
use rand::{RngCore, SeedableRng, rngs::SmallRng};
use serde_json::Value;

const BASE_MAINNET_CHAIN_ID: u64 = 8_453;
const GROTH16_SEAL_BYTES: usize = 260;

#[derive(Debug)]
struct SenderEncodingStats {
    unique_senders: usize,
    duplicate_sender_slots: usize,
    sender_stream_bytes: usize,
    encoded_len: usize,
}

#[derive(Debug)]
struct BrotliBatchStats {
    span_brotli_bytes: usize,
    zk_span_brotli_bytes: usize,
    zk_span_brotli_with_roots_bytes: usize,
    zk_span_brotli_with_groth16_bytes: usize,
}

#[derive(Debug)]
struct FixtureBlocks {
    blocks: Vec<Vec<OpTransactionSigned>>,
    excluded_deposits: usize,
}

fn load_fixture_blocks() -> FixtureBlocks {
    let fixture_data =
        include_str!("../../../client/flashblocks-node/benches/fixtures/base_mainnet_blocks.json");
    let json: Value = serde_json::from_str(fixture_data).expect("valid JSON fixture");

    let mut excluded_deposits = 0usize;
    let blocks = json["blocks"]
        .as_array()
        .expect("blocks array")
        .iter()
        .map(|block| {
            block["transactions"]
                .as_array()
                .expect("transactions array")
                .iter()
                .filter_map(|tx_value| {
                    let tx = serde_json::from_value::<OpTransactionSigned>(tx_value.clone())
                        .expect("fixture transaction should deserialize");
                    match tx {
                        OpTxEnvelope::Deposit(_) => {
                            excluded_deposits += 1;
                            None
                        }
                        _ => Some(tx),
                    }
                })
                .collect()
        })
        .collect();

    FixtureBlocks { blocks, excluded_deposits }
}

fn encoded_txs(txs: &[OpTransactionSigned]) -> Vec<Bytes> {
    txs.iter().map(|tx| tx.encoded_2718().into()).collect()
}

fn zk_encoded_txs(txs: &[OpTransactionSigned]) -> Vec<Bytes> {
    txs.iter().map(zk_encode_tx).collect()
}

fn zk_encode_tx(tx: &OpTransactionSigned) -> Bytes {
    let tx = match tx {
        OpTxEnvelope::Legacy(tx) => TxZkSequencer::new(
            tx.recover_signer().expect("recoverable signer"),
            ZkSequencerTxBody::Legacy(tx.tx().clone()),
        ),
        OpTxEnvelope::Eip2930(tx) => TxZkSequencer::new(
            tx.recover_signer().expect("recoverable signer"),
            ZkSequencerTxBody::Eip2930(tx.tx().clone()),
        ),
        OpTxEnvelope::Eip1559(tx) => TxZkSequencer::new(
            tx.recover_signer().expect("recoverable signer"),
            ZkSequencerTxBody::Eip1559(tx.tx().clone()),
        ),
        OpTxEnvelope::Eip7702(tx) => TxZkSequencer::new(
            tx.recover_signer().expect("recoverable signer"),
            ZkSequencerTxBody::Eip7702(tx.tx().clone()),
        ),
        OpTxEnvelope::ZkSequencer(tx) => tx.clone().into_inner(),
        OpTxEnvelope::Deposit(_) => panic!("fixture batches should not include deposits"),
    };

    let mut encoded = Vec::new();
    OpTxEnvelope::from(tx).encode_2718(&mut encoded);
    encoded.into()
}

fn current_span_tx_encoding_len(txs: &[OpTransactionSigned]) -> (usize, usize) {
    let mut span_txs = SpanBatchTransactions::default();
    span_txs.add_txs(encoded_txs(txs), BASE_MAINNET_CHAIN_ID).expect("valid span tx encoding");

    let mut encoded = Vec::new();
    span_txs.encode(&mut encoded).expect("span tx encoding succeeds");

    let mut signatures = Vec::new();
    span_txs.encode_tx_sigs(&mut signatures).expect("signature encoding succeeds");

    (encoded.len(), signatures.len())
}

fn zk_span_tx_encoding_stats(txs: &[OpTransactionSigned]) -> SenderEncodingStats {
    let mut zk_span_txs = ZkSpanBatchTransactions::default();
    zk_span_txs
        .add_txs(zk_encoded_txs(txs), BASE_MAINNET_CHAIN_ID)
        .expect("valid zk span tx encoding");

    let mut encoded = Vec::new();
    zk_span_txs.encode(&mut encoded).expect("zk span tx encoding succeeds");

    let unique_senders = zk_span_txs.tx_senders.iter().copied().collect::<HashSet<_>>().len();

    SenderEncodingStats {
        unique_senders,
        duplicate_sender_slots: zk_span_txs.tx_senders.len().saturating_sub(unique_senders),
        sender_stream_bytes: zk_span_txs.tx_senders.len() * 20,
        encoded_len: encoded.len(),
    }
}

fn transactions_root(txs: &[OpTransactionSigned]) -> B256 {
    ordered_trie_root_encoded(&encoded_txs(txs))
}

fn print_summary(label: &str, raw_len: usize, current_len: usize, no_proof_len: usize) {
    let current_ratio = current_len as f64 / raw_len as f64;
    let no_proof_ratio = no_proof_len as f64 / raw_len as f64;
    let break_even = current_len.saturating_sub(no_proof_len);

    println!(
        "{label}: raw_bytes={raw_len} current_span_bytes={current_len} current_ratio={current_ratio:.4} zk_span_no_proof_bytes={no_proof_len} zk_span_no_proof_ratio={no_proof_ratio:.4} break_even_proof_bytes={break_even}"
    );

    for proof_size in [1_024usize, 4_096, 16_384] {
        let total = no_proof_len + proof_size;
        let ratio = total as f64 / raw_len as f64;
        println!(
            "{label}: proof_bytes={proof_size} zk_span_bytes={total} zk_span_ratio={ratio:.4}"
        );
    }
}

fn synthetic_single_batches(
    blocks: &[Vec<OpTransactionSigned>],
    use_zk_transactions: bool,
) -> Vec<SingleBatch> {
    blocks
        .iter()
        .enumerate()
        .map(|(index, block_txs)| SingleBatch {
            parent_hash: synthetic_hash(index as u8),
            epoch_num: index as u64,
            epoch_hash: synthetic_hash(index as u8 ^ 0x80),
            timestamp: 1_000 + index as u64 * 2,
            transactions: if use_zk_transactions {
                zk_encoded_txs(block_txs)
            } else {
                encoded_txs(block_txs)
            },
        })
        .collect()
}

fn synthetic_hash(byte: u8) -> BlockHash {
    FixedBytes::<32>::repeat_byte(byte)
}

fn synthetic_groth16_seal() -> Vec<u8> {
    let mut proof = vec![0u8; GROTH16_SEAL_BYTES];
    let mut rng = SmallRng::seed_from_u64(0xB45E_BA7C);
    rng.fill_bytes(&mut proof);
    proof
}

fn brotli_channel_len(batch: Batch) -> usize {
    let config =
        Arc::new(RollupConfig { l2_chain_id: BASE_MAINNET_CHAIN_ID.into(), ..Default::default() });
    let mut channel = ChannelOut::new(
        ChannelId::default(),
        config,
        VariantCompressor::from(CompressionAlgo::Brotli10),
    );
    channel.add_batch(batch).expect("batch should fit in channel");
    channel.flush().expect("channel flush should succeed");
    channel.close();

    let mut total = 0usize;
    while channel.ready_bytes() > 0 {
        let frame = channel.output_frame(1_000_000).expect("frame output should succeed");
        total += frame.data.len();
    }
    total
}

fn brotli_batch_stats(blocks: &[Vec<OpTransactionSigned>]) -> BrotliBatchStats {
    let span_batches = synthetic_single_batches(blocks, false);
    let mut span = SpanBatch { chain_id: BASE_MAINNET_CHAIN_ID, ..Default::default() };
    for (index, single) in span_batches.into_iter().enumerate() {
        span.append_singular_batch(single, if index == 0 { 0 } else { 1 })
            .expect("span batch append should succeed");
    }

    let zk_span_batches = synthetic_single_batches(blocks, true);
    let mut zk_span = ZkSpanBatch { chain_id: BASE_MAINNET_CHAIN_ID, ..Default::default() };
    for (index, single) in zk_span_batches.into_iter().enumerate() {
        zk_span
            .append_singular_batch(
                single,
                if index == 0 { 0 } else { 1 },
                transactions_root(&blocks[index]),
            )
            .expect("zk span batch append should succeed");
    }

    let span_brotli_bytes = brotli_channel_len(Batch::Span(span.clone()));
    let zk_span_brotli_bytes = brotli_channel_len(Batch::ZkSpan(zk_span.clone()));
    let zk_span_brotli_with_roots_bytes = zk_span_brotli_bytes;
    zk_span.proof = synthetic_groth16_seal();
    let zk_span_brotli_with_groth16_bytes = brotli_channel_len(Batch::ZkSpan(zk_span));

    BrotliBatchStats {
        span_brotli_bytes,
        zk_span_brotli_bytes,
        zk_span_brotli_with_roots_bytes,
        zk_span_brotli_with_groth16_bytes,
    }
}

#[test]
fn analyzes_base_mainnet_sender_dedup_compression() {
    let fixture = load_fixture_blocks();
    let blocks = fixture.blocks;
    let all_txs: Vec<_> = blocks.iter().flat_map(|block| block.iter().cloned()).collect();

    println!(
        "fixture: sequencer_txs={} excluded_deposits={}",
        all_txs.len(),
        fixture.excluded_deposits
    );

    let raw_total_len: usize = all_txs.iter().map(|tx| tx.encoded_2718().len()).sum();
    let (current_total_len, _current_signature_len) = current_span_tx_encoding_len(&all_txs);
    let sender_stats = zk_span_tx_encoding_stats(&all_txs);
    let zk_total_no_proof = sender_stats.encoded_len;
    let brotli_stats = brotli_batch_stats(&blocks);

    println!(
        "aggregate: txs={} unique_senders={} duplicate_sender_slots={} sender_stream_bytes={}",
        all_txs.len(),
        sender_stats.unique_senders,
        sender_stats.duplicate_sender_slots,
        sender_stats.sender_stream_bytes
    );
    print_summary("aggregate", raw_total_len, current_total_len, zk_total_no_proof);
    println!(
        "aggregate_brotli: span_bytes={} zk_span_no_proof_bytes={} zk_span_with_roots_bytes={} zk_span_with_groth16_260b_bytes={} groth16_delta={}",
        brotli_stats.span_brotli_bytes,
        brotli_stats.zk_span_brotli_bytes,
        brotli_stats.zk_span_brotli_with_roots_bytes,
        brotli_stats.zk_span_brotli_with_groth16_bytes,
        brotli_stats
            .zk_span_brotli_with_groth16_bytes
            .saturating_sub(brotli_stats.zk_span_brotli_bytes)
    );

    for (index, block_txs) in blocks.iter().enumerate() {
        let raw_len: usize = block_txs.iter().map(|tx| tx.encoded_2718().len()).sum();
        let (current_len, _current_sig_len) = current_span_tx_encoding_len(block_txs);
        let sender_stats = zk_span_tx_encoding_stats(block_txs);
        let zk_no_proof_len = sender_stats.encoded_len;

        println!(
            "block[{index}]: txs={} unique_senders={} duplicate_sender_slots={} sender_stream_bytes={}",
            block_txs.len(),
            sender_stats.unique_senders,
            sender_stats.duplicate_sender_slots,
            sender_stats.sender_stream_bytes
        );
        print_summary(&format!("block[{index}]"), raw_len, current_len, zk_no_proof_len);
    }

    assert_eq!(sender_stats.sender_stream_bytes, all_txs.len() * 20);
    assert!(brotli_stats.zk_span_brotli_with_groth16_bytes > brotli_stats.zk_span_brotli_bytes);
}
