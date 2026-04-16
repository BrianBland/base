//! ZK span batch implementation.

use alloc::vec::Vec;

use alloy_primitives::{B256, FixedBytes};
use base_consensus_genesis::RollupConfig;

use crate::{
    BatchValidationProvider, BlockInfo, L2BlockInfo, RawZkSpanBatch, SingleBatch, SpanBatch,
    SpanBatchBits, SpanBatchElement, SpanBatchError, ZkSpanBatchPayload, ZkSpanBatchTransactions,
};

/// Container for a sender-compressed span of canonical zk-backed sequencer transactions.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ZkSpanBatch {
    /// First 20 bytes of the parent hash of the first block in the span.
    pub parent_check: FixedBytes<20>,
    /// First 20 bytes of the L1 origin hash of the last block in the span.
    pub l1_origin_check: FixedBytes<20>,
    /// Genesis block timestamp for relative timestamp calculations.
    pub genesis_timestamp: u64,
    /// Chain ID for transaction reconstruction.
    pub chain_id: u64,
    /// Ordered list of block elements contained in this span.
    pub batches: Vec<SpanBatchElement>,
    /// Cached bit array indicating L1 origin changes between consecutive blocks.
    pub origin_bits: SpanBatchBits,
    /// Cached transaction count for each block in the span.
    pub block_tx_counts: Vec<u64>,
    /// Cached sender-compressed canonical zk transactions for all blocks in the span.
    pub txs: ZkSpanBatchTransactions,
    /// Original per-block transaction trie roots.
    pub tx_roots: Vec<B256>,
    /// Batch-level zk proof bytes.
    pub proof: Vec<u8>,
}

impl ZkSpanBatch {
    /// Returns the starting timestamp for the first batch in the span.
    pub fn starting_timestamp(&self) -> u64 {
        self.batches[0].timestamp
    }

    /// Returns the final timestamp for the last batch in the span.
    pub fn final_timestamp(&self) -> u64 {
        self.batches[self.batches.len() - 1].timestamp
    }

    /// Returns the L1 epoch number for the first batch in the span.
    pub fn starting_epoch_num(&self) -> u64 {
        self.batches[0].epoch_num
    }

    /// Validates that the L1 origin hash matches the span's L1 origin check.
    pub fn check_origin_hash(&self, hash: FixedBytes<32>) -> bool {
        self.l1_origin_check == hash[..20]
    }

    /// Validates that the parent hash matches the span's parent check.
    pub fn check_parent_hash(&self, hash: FixedBytes<32>) -> bool {
        self.parent_check == hash[..20]
    }

    /// Accesses the nth element from the end of the batch list.
    pub fn peek(&self, n: usize) -> &SpanBatchElement {
        &self.batches[self.batches.len() - 1 - n]
    }

    /// Converts this zk span batch to its raw serializable format.
    pub fn to_raw_zk_span_batch(&self) -> Result<RawZkSpanBatch, SpanBatchError> {
        if self.batches.is_empty() {
            return Err(SpanBatchError::EmptySpanBatch);
        }

        let span_start = self.batches.first().ok_or(SpanBatchError::EmptySpanBatch)?;
        let span_end = self.batches.last().ok_or(SpanBatchError::EmptySpanBatch)?;

        Ok(RawZkSpanBatch {
            prefix: crate::SpanBatchPrefix {
                rel_timestamp: span_start.timestamp - self.genesis_timestamp,
                l1_origin_num: span_end.epoch_num,
                parent_check: self.parent_check,
                l1_origin_check: self.l1_origin_check,
            },
            payload: ZkSpanBatchPayload {
                block_count: self.batches.len() as u64,
                origin_bits: self.origin_bits.clone(),
                block_tx_counts: self.block_tx_counts.clone(),
                txs: self.txs.clone(),
                tx_roots: self.tx_roots.clone(),
                proof: self.proof.clone(),
            },
        })
    }

    /// Converts all [`SpanBatchElement`]s after the L2 safe head to [`SingleBatch`]es.
    pub fn get_singular_batches(
        &self,
        l1_origins: &[BlockInfo],
        l2_safe_head: L2BlockInfo,
    ) -> Result<Vec<SingleBatch>, SpanBatchError> {
        self.as_span_batch().get_singular_batches(l1_origins, l2_safe_head)
    }

    /// Append a [`SingleBatch`] to the [`ZkSpanBatch`].
    pub fn append_singular_batch(
        &mut self,
        singular_batch: SingleBatch,
        seq_num: u64,
        tx_root: B256,
    ) -> Result<(), SpanBatchError> {
        if !self.batches.is_empty() && self.peek(0).timestamp > singular_batch.timestamp {
            panic!("Batch is not ordered");
        }

        let SingleBatch { epoch_hash, parent_hash, .. } = singular_batch;

        self.batches.push(singular_batch.into());
        self.l1_origin_check = epoch_hash[..20].try_into().expect("Sub-slice cannot fail");

        let epoch_bit = if self.batches.len() == 1 {
            self.parent_check = parent_hash[..20].try_into().expect("Sub-slice cannot fail");
            seq_num == 0
        } else {
            self.peek(1).epoch_num < self.peek(0).epoch_num
        };

        self.origin_bits.set_bit(self.batches.len() - 1, epoch_bit);

        let new_txs = self.peek(0).transactions.clone();
        self.block_tx_counts.push(new_txs.len() as u64);
        self.tx_roots.push(tx_root);
        self.txs.add_txs(new_txs, self.chain_id)
    }

    /// Checks if the zk span batch is valid.
    pub async fn check_batch<BV: BatchValidationProvider>(
        &self,
        cfg: &RollupConfig,
        l1_blocks: &[BlockInfo],
        l2_safe_head: L2BlockInfo,
        inclusion_block: &BlockInfo,
        fetcher: &mut BV,
    ) -> crate::BatchValidity {
        self.as_span_batch()
            .check_batch(cfg, l1_blocks, l2_safe_head, inclusion_block, fetcher)
            .await
    }

    /// Checks if the zk span batch prefix is valid.
    pub async fn check_batch_prefix<BF: BatchValidationProvider>(
        &self,
        cfg: &RollupConfig,
        l1_origins: &[BlockInfo],
        l2_safe_head: L2BlockInfo,
        inclusion_block: &BlockInfo,
        fetcher: &mut BF,
    ) -> (crate::BatchValidity, Option<L2BlockInfo>) {
        self.as_span_batch()
            .check_batch_prefix(cfg, l1_origins, l2_safe_head, inclusion_block, fetcher)
            .await
    }

    /// Converts the shared fields into a [`SpanBatch`] for validation delegation.
    pub fn as_span_batch(&self) -> SpanBatch {
        SpanBatch {
            parent_check: self.parent_check,
            l1_origin_check: self.l1_origin_check,
            genesis_timestamp: self.genesis_timestamp,
            chain_id: self.chain_id,
            batches: self.batches.clone(),
            origin_bits: self.origin_bits.clone(),
            block_tx_counts: self.block_tx_counts.clone(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use alloy_consensus::{TxEip1559, proofs::ordered_trie_root_encoded};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{TxKind, address};
    use base_alloy_consensus::{OpTxEnvelope, TxZkSequencer, ZkSequencerTxBody};

    use super::*;

    #[test]
    fn test_append_singular_batch() {
        let tx = OpTxEnvelope::from(TxZkSequencer::new(
            address!("1111111111111111111111111111111111111111"),
            ZkSequencerTxBody::Eip1559(TxEip1559 {
                chain_id: 1,
                nonce: 1,
                gas_limit: 21_000,
                max_fee_per_gas: 2,
                max_priority_fee_per_gas: 1,
                to: TxKind::Call(address!("2222222222222222222222222222222222222222")),
                ..Default::default()
            }),
        ));
        let mut encoded = Vec::new();
        tx.encode_2718(&mut encoded);

        let singular_batch = SingleBatch {
            epoch_hash: FixedBytes::from([3u8; 32]),
            parent_hash: FixedBytes::from([2u8; 32]),
            timestamp: 10,
            transactions: vec![encoded.clone().into()],
            ..Default::default()
        };

        let mut batch = ZkSpanBatch { chain_id: 1, ..Default::default() };
        batch
            .append_singular_batch(singular_batch, 0, ordered_trie_root_encoded(&[encoded]))
            .unwrap();

        assert_eq!(batch.block_tx_counts, vec![1]);
        assert_eq!(batch.txs.total_block_tx_count, 1);
        assert_eq!(batch.tx_roots.len(), 1);
    }
}
