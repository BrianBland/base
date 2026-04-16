//! Raw zk span batch payload.

use alloc::vec::Vec;

use alloy_primitives::{B256, bytes};

use super::MAX_SPAN_BATCH_ELEMENTS;
use crate::{SpanBatchBits, SpanBatchError, SpanDecodingError, ZkSpanBatchTransactions};

/// ZK span batch payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZkSpanBatchPayload {
    /// Number of L2 blocks in the span.
    pub block_count: u64,
    /// Standard span-batch bitlist of blockCount bits. Each bit indicates if the L1 origin is
    /// changed at the L2 block.
    pub origin_bits: SpanBatchBits,
    /// List of transaction counts for each L2 block.
    pub block_tx_counts: Vec<u64>,
    /// Sender-compressed canonical zk transactions.
    pub txs: ZkSpanBatchTransactions,
    /// Original per-block transaction trie roots.
    pub tx_roots: Vec<B256>,
    /// Batch-level zk proof bytes.
    pub proof: Vec<u8>,
}

impl ZkSpanBatchPayload {
    /// Decodes a [`ZkSpanBatchPayload`] from a reader.
    pub fn decode_payload(r: &mut &[u8]) -> Result<Self, SpanBatchError> {
        let mut payload = Self::default();
        payload.decode_block_count(r)?;
        payload.decode_origin_bits(r)?;
        payload.decode_block_tx_counts(r)?;
        payload.decode_txs(r)?;
        payload.decode_tx_roots(r)?;
        payload.decode_proof(r)?;
        Ok(payload)
    }

    /// Encodes a [`ZkSpanBatchPayload`] into a writer.
    pub fn encode_payload(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        self.encode_block_count(w);
        self.encode_origin_bits(w)?;
        self.encode_block_tx_counts(w);
        self.encode_txs(w)?;
        self.encode_tx_roots(w);
        self.encode_proof(w);
        Ok(())
    }

    /// Decodes the origin bits from a reader.
    pub fn decode_origin_bits(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        if self.block_count > MAX_SPAN_BATCH_ELEMENTS {
            return Err(SpanBatchError::TooBigSpanBatchSize);
        }

        self.origin_bits = SpanBatchBits::decode(r, self.block_count as usize)?;
        Ok(())
    }

    /// Decode a block count from a reader.
    pub fn decode_block_count(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let (block_count, remaining) = unsigned_varint::decode::u64(r)
            .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::BlockCount))?;
        if block_count > MAX_SPAN_BATCH_ELEMENTS {
            return Err(SpanBatchError::TooBigSpanBatchSize);
        }
        if block_count == 0 {
            return Err(SpanBatchError::EmptySpanBatch);
        }
        self.block_count = block_count;
        *r = remaining;
        Ok(())
    }

    /// Decode block transaction counts from a reader.
    pub fn decode_block_tx_counts(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let mut block_tx_counts = Vec::with_capacity(self.block_count as usize);

        for _ in 0..self.block_count {
            let (block_tx_count, remaining) = unsigned_varint::decode::u64(r)
                .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::BlockTxCounts))?;
            if block_tx_count > MAX_SPAN_BATCH_ELEMENTS {
                return Err(SpanBatchError::TooBigSpanBatchSize);
            }
            block_tx_counts.push(block_tx_count);
            *r = remaining;
        }
        self.block_tx_counts = block_tx_counts;
        Ok(())
    }

    /// Decode transactions from a reader.
    pub fn decode_txs(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        if self.block_tx_counts.is_empty() {
            return Err(SpanBatchError::EmptySpanBatch);
        }

        let total_block_tx_count =
            self.block_tx_counts.iter().try_fold(0u64, |acc, block_tx_count| {
                acc.checked_add(*block_tx_count).ok_or(SpanBatchError::TooBigSpanBatchSize)
            })?;

        if total_block_tx_count > MAX_SPAN_BATCH_ELEMENTS {
            return Err(SpanBatchError::TooBigSpanBatchSize);
        }
        self.txs.total_block_tx_count = total_block_tx_count;
        self.txs.decode(r)?;
        Ok(())
    }

    /// Decodes the original per-block transaction roots from a reader.
    pub fn decode_tx_roots(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let mut tx_roots = Vec::with_capacity(self.block_count as usize);

        for _ in 0..self.block_count {
            if r.len() < B256::len_bytes() {
                return Err(SpanBatchError::Decoding(SpanDecodingError::TxRoots));
            }

            tx_roots.push(B256::from_slice(&r[..B256::len_bytes()]));
            *r = &r[B256::len_bytes()..];
        }

        self.tx_roots = tx_roots;
        Ok(())
    }

    /// Decodes the batch proof from a reader.
    pub fn decode_proof(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let (proof_len, remaining) = unsigned_varint::decode::u64(r)
            .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?;
        *r = remaining;
        if r.len() < proof_len as usize {
            return Err(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData));
        }
        self.proof = r[..proof_len as usize].to_vec();
        *r = &r[proof_len as usize..];
        Ok(())
    }

    /// Encode the origin bits into a writer.
    pub fn encode_origin_bits(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        SpanBatchBits::encode(w, self.block_count as usize, &self.origin_bits)
    }

    /// Encode the block count into a writer.
    pub fn encode_block_count(&self, w: &mut dyn bytes::BufMut) {
        let mut u64_varint_buf = [0u8; 10];
        w.put_slice(unsigned_varint::encode::u64(self.block_count, &mut u64_varint_buf));
    }

    /// Encode the block transaction counts into a writer.
    pub fn encode_block_tx_counts(&self, w: &mut dyn bytes::BufMut) {
        let mut u64_varint_buf = [0u8; 10];
        for block_tx_count in &self.block_tx_counts {
            u64_varint_buf.fill(0);
            w.put_slice(unsigned_varint::encode::u64(*block_tx_count, &mut u64_varint_buf));
        }
    }

    /// Encode the transactions into a writer.
    pub fn encode_txs(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        self.txs.encode(w)
    }

    /// Encodes the original per-block transaction roots into a writer.
    pub fn encode_tx_roots(&self, w: &mut dyn bytes::BufMut) {
        for tx_root in &self.tx_roots {
            w.put_slice(tx_root.as_slice());
        }
    }

    /// Encode the batch proof into a writer.
    pub fn encode_proof(&self, w: &mut dyn bytes::BufMut) {
        let mut u64_varint_buf = [0u8; 10];
        w.put_slice(unsigned_varint::encode::u64(self.proof.len() as u64, &mut u64_varint_buf));
        w.put_slice(&self.proof);
    }
}
