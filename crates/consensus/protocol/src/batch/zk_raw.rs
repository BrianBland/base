//! Module containing the [`RawZkSpanBatch`] struct.

use alloc::{vec, vec::Vec};

use alloy_primitives::bytes;

use crate::{
    BatchType, SpanBatchElement, SpanBatchError, SpanBatchPrefix, SpanDecodingError, ZkSpanBatch,
    ZkSpanBatchPayload,
};

/// Raw zk span batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawZkSpanBatch {
    /// The span batch prefix.
    pub prefix: SpanBatchPrefix,
    /// The zk span batch payload.
    pub payload: ZkSpanBatchPayload,
}

impl RawZkSpanBatch {
    /// Returns the batch type.
    pub const fn get_batch_type(&self) -> BatchType {
        BatchType::ZkSpan
    }

    /// Encodes the [`RawZkSpanBatch`] into a writer.
    pub fn encode(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        self.prefix.encode_prefix(w);
        self.payload.encode_payload(w)
    }

    /// Decodes the [`RawZkSpanBatch`] from a reader.
    pub fn decode(r: &mut &[u8]) -> Result<Self, SpanBatchError> {
        let prefix = SpanBatchPrefix::decode_prefix(r)?;
        let payload = ZkSpanBatchPayload::decode_payload(r)?;
        Ok(Self { prefix, payload })
    }

    /// Converts a [`RawZkSpanBatch`] into a [`ZkSpanBatch`].
    pub fn derive(
        &mut self,
        block_time: u64,
        genesis_time: u64,
        chain_id: u64,
    ) -> Result<ZkSpanBatch, SpanBatchError> {
        if self.payload.block_count == 0 {
            return Err(SpanBatchError::EmptySpanBatch);
        }

        let mut block_origin_nums = vec![0u64; self.payload.block_count as usize];
        let mut l1_origin_number = self.prefix.l1_origin_num;
        for i in (0..self.payload.block_count).rev() {
            block_origin_nums[i as usize] = l1_origin_number;
            if self
                .payload
                .origin_bits
                .get_bit(i as usize)
                .ok_or(SpanBatchError::Decoding(SpanDecodingError::L1OriginCheck))?
                == 1
                && i > 0
            {
                l1_origin_number -= 1;
            }
        }

        let enveloped_txs = self.payload.txs.full_txs(chain_id)?;

        let mut tx_idx = 0usize;
        let batches = (0..self.payload.block_count).fold(Vec::new(), |mut acc, i| {
            let transactions =
                (0..self.payload.block_tx_counts[i as usize]).fold(Vec::new(), |mut acc, _| {
                    acc.push(enveloped_txs[tx_idx].clone());
                    tx_idx += 1;
                    acc
                });
            acc.push(SpanBatchElement {
                epoch_num: block_origin_nums[i as usize],
                timestamp: genesis_time + self.prefix.rel_timestamp + block_time * i,
                transactions: transactions.into_iter().map(Into::into).collect(),
            });
            acc
        });

        Ok(ZkSpanBatch {
            parent_check: self.prefix.parent_check,
            l1_origin_check: self.prefix.l1_origin_check,
            genesis_timestamp: genesis_time,
            chain_id,
            batches,
            origin_bits: self.payload.origin_bits.clone(),
            block_tx_counts: self.payload.block_tx_counts.clone(),
            txs: self.payload.txs.clone(),
            tx_roots: self.payload.tx_roots.clone(),
            proof: self.payload.proof.clone(),
        })
    }
}
