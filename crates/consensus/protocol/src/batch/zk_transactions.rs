//! Sender-compressed transactions for a zk-backed span batch.

use alloc::vec::Vec;

use alloy_consensus::{Transaction, TxType};
use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_primitives::{Address, Bytes, bytes};
use alloy_rlp::{Buf, Decodable, Encodable};
use base_alloy_consensus::OpTxEnvelope;

use crate::{
    MAX_SPAN_BATCH_ELEMENTS, SpanBatchBits, SpanBatchError, SpanBatchTransactionData,
    SpanDecodingError,
};

/// This struct contains the decoded information for transactions in a zk span batch.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ZkSpanBatchTransactions {
    /// The total number of transactions in a zk span batch. Must be manually set.
    pub total_block_tx_count: u64,
    /// The contract creation bits, standard span-batch bitlist.
    pub contract_creation_bits: SpanBatchBits,
    /// The senders of the transactions, in transaction order.
    pub tx_senders: Vec<Address>,
    /// The transaction nonces.
    pub tx_nonces: Vec<u64>,
    /// The transaction gas limits.
    pub tx_gases: Vec<u64>,
    /// The `to` addresses of the transactions.
    pub tx_tos: Vec<Address>,
    /// The transaction data.
    pub tx_data: Vec<Vec<u8>>,
    /// The protected bits, standard span-batch bitlist.
    pub protected_bits: SpanBatchBits,
    /// The types of the transactions.
    pub tx_types: Vec<TxType>,
    /// Total legacy transaction count in the zk span batch.
    pub legacy_tx_count: u64,
}

impl ZkSpanBatchTransactions {
    /// Encodes the [`ZkSpanBatchTransactions`] into a writer.
    pub fn encode(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        self.encode_contract_creation_bits(w)?;
        self.encode_tx_senders(w);
        self.encode_tx_tos(w)?;
        self.encode_tx_data(w)?;
        self.encode_tx_nonces(w);
        self.encode_tx_gases(w);
        self.encode_protected_bits(w)?;
        Ok(())
    }

    /// Decodes the [`ZkSpanBatchTransactions`] from a reader.
    pub fn decode(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        self.decode_contract_creation_bits(r)?;
        self.decode_tx_senders(r)?;
        self.decode_tx_tos(r)?;
        self.decode_tx_data(r)?;
        self.decode_tx_nonces(r)?;
        self.decode_tx_gases(r)?;
        self.decode_protected_bits(r)?;
        Ok(())
    }

    /// Encodes the contract creation bits into a writer.
    pub fn encode_contract_creation_bits(
        &self,
        w: &mut dyn bytes::BufMut,
    ) -> Result<(), SpanBatchError> {
        SpanBatchBits::encode(w, self.total_block_tx_count as usize, &self.contract_creation_bits)?;
        Ok(())
    }

    /// Encodes the transaction senders into a writer.
    pub fn encode_tx_senders(&self, w: &mut dyn bytes::BufMut) {
        for sender in &self.tx_senders {
            w.put_slice(sender.as_ref());
        }
    }

    /// Encodes the protected bits into a writer.
    pub fn encode_protected_bits(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        SpanBatchBits::encode(w, self.legacy_tx_count as usize, &self.protected_bits)?;
        Ok(())
    }

    /// Encodes the transaction nonces into a writer.
    pub fn encode_tx_nonces(&self, w: &mut dyn bytes::BufMut) {
        let mut buf = [0u8; 10];
        for nonce in &self.tx_nonces {
            buf.fill(0);
            w.put_slice(unsigned_varint::encode::u64(*nonce, &mut buf));
        }
    }

    /// Encodes the transaction gas limits into a writer.
    pub fn encode_tx_gases(&self, w: &mut dyn bytes::BufMut) {
        let mut buf = [0u8; 10];
        for gas in &self.tx_gases {
            buf.fill(0);
            w.put_slice(unsigned_varint::encode::u64(*gas, &mut buf));
        }
    }

    /// Encodes the `to` addresses of the transactions into a writer.
    pub fn encode_tx_tos(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        for to in &self.tx_tos {
            w.put_slice(to.as_ref());
        }
        Ok(())
    }

    /// Encodes the transaction data into a writer.
    pub fn encode_tx_data(&self, w: &mut dyn bytes::BufMut) -> Result<(), SpanBatchError> {
        for data in &self.tx_data {
            w.put_slice(data);
        }
        Ok(())
    }

    /// Decodes the contract creation bits from a reader.
    pub fn decode_contract_creation_bits(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        if self.total_block_tx_count > MAX_SPAN_BATCH_ELEMENTS {
            return Err(SpanBatchError::TooBigSpanBatchSize);
        }

        self.contract_creation_bits = SpanBatchBits::decode(r, self.total_block_tx_count as usize)?;
        Ok(())
    }

    /// Decodes the transaction senders from a reader.
    pub fn decode_tx_senders(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let mut senders = Vec::with_capacity(self.total_block_tx_count as usize);
        for _ in 0..self.total_block_tx_count {
            if r.len() < 20 {
                return Err(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData));
            }
            senders.push(Address::from_slice(&r[..20]));
            r.advance(20);
        }
        self.tx_senders = senders;
        Ok(())
    }

    /// Decodes the protected bits from a reader.
    pub fn decode_protected_bits(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        if self.legacy_tx_count > MAX_SPAN_BATCH_ELEMENTS {
            return Err(SpanBatchError::TooBigSpanBatchSize);
        }

        self.protected_bits = SpanBatchBits::decode(r, self.legacy_tx_count as usize)?;
        Ok(())
    }

    /// Decodes the transaction nonces from a reader.
    pub fn decode_tx_nonces(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let mut nonces = Vec::with_capacity(self.total_block_tx_count as usize);
        for _ in 0..self.total_block_tx_count {
            let (nonce, remaining) = unsigned_varint::decode::u64(r)
                .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::TxNonces))?;
            nonces.push(nonce);
            *r = remaining;
        }
        self.tx_nonces = nonces;
        Ok(())
    }

    /// Decodes the transaction gas limits from a reader.
    pub fn decode_tx_gases(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let mut gases = Vec::with_capacity(self.total_block_tx_count as usize);
        for _ in 0..self.total_block_tx_count {
            let (gas, remaining) = unsigned_varint::decode::u64(r)
                .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::TxNonces))?;
            gases.push(gas);
            *r = remaining;
        }
        self.tx_gases = gases;
        Ok(())
    }

    /// Decodes the `to` addresses of the transactions from a reader.
    pub fn decode_tx_tos(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let mut tos = Vec::with_capacity(self.total_block_tx_count as usize);
        let contract_creation_count = self.contract_creation_count();
        for _ in 0..(self.total_block_tx_count - contract_creation_count) {
            if r.len() < 20 {
                return Err(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData));
            }
            let to = Address::from_slice(&r[..20]);
            tos.push(to);
            r.advance(20);
        }
        self.tx_tos = tos;
        Ok(())
    }

    /// Decodes the transaction data from a reader.
    pub fn decode_tx_data(&mut self, r: &mut &[u8]) -> Result<(), SpanBatchError> {
        let mut tx_data = Vec::new();
        let mut tx_types = Vec::new();

        for _ in 0..self.total_block_tx_count {
            let (tx_data_item, tx_type) = crate::read_tx_data(r)?;
            tx_data.push(tx_data_item);
            tx_types.push(tx_type);
            if matches!(tx_type, TxType::Legacy) {
                self.legacy_tx_count += 1;
            }
        }

        self.tx_data = tx_data;
        self.tx_types = tx_types;
        Ok(())
    }

    /// Returns the number of contract creation transactions in the zk span batch.
    pub fn contract_creation_count(&self) -> u64 {
        self.contract_creation_bits.as_ref().iter().map(|b| b.count_ones() as u64).sum()
    }

    /// Retrieve all of the raw transactions from the [`ZkSpanBatchTransactions`].
    pub fn full_txs(&self, chain_id: u64) -> Result<Vec<Vec<u8>>, SpanBatchError> {
        let mut txs = Vec::new();
        let mut to_idx = 0usize;
        let mut protected_bit_idx = 0usize;

        for idx in 0..self.total_block_tx_count {
            let sender = *self
                .tx_senders
                .get(idx as usize)
                .ok_or(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?;

            let mut data = self.tx_data[idx as usize].as_slice();
            let tx = SpanBatchTransactionData::decode(&mut data)
                .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?;
            let nonce = self
                .tx_nonces
                .get(idx as usize)
                .ok_or(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?;
            let gas = self
                .tx_gases
                .get(idx as usize)
                .ok_or(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?;
            let bit = self
                .contract_creation_bits
                .get_bit(idx as usize)
                .ok_or(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?;
            let to = if bit == 0 {
                if self.tx_tos.len() <= to_idx {
                    return Err(SpanBatchError::Decoding(
                        SpanDecodingError::InvalidTransactionData,
                    ));
                }
                to_idx += 1;
                Some(self.tx_tos[to_idx - 1])
            } else {
                None
            };
            let is_protected = if tx.tx_type() == TxType::Legacy {
                protected_bit_idx += 1;
                self.protected_bits.get_bit(protected_bit_idx - 1).unwrap_or_default() == 1
            } else {
                true
            };
            let tx_envelope = OpTxEnvelope::from(tx.to_zk_tx(
                sender,
                *nonce,
                *gas,
                to,
                chain_id,
                is_protected,
            )?);
            let mut buf = Vec::new();
            tx_envelope.encode_2718(&mut buf);
            txs.push(buf);
        }
        Ok(txs)
    }

    /// Add raw transactions into the [`ZkSpanBatchTransactions`].
    pub fn add_txs(&mut self, txs: Vec<Bytes>, _chain_id: u64) -> Result<(), SpanBatchError> {
        let total_block_tx_count = txs.len() as u64;
        let offset = self.total_block_tx_count as usize;

        for (i, tx) in txs.iter().enumerate() {
            let tx_enveloped = OpTxEnvelope::decode_2718(&mut tx.as_ref())
                .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?;

            let OpTxEnvelope::ZkSequencer(tx) = tx_enveloped else {
                return Err(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData));
            };

            let tx = tx.into_inner();
            let span_batch_tx = SpanBatchTransactionData::try_from(&tx)?;
            let tx_type = tx.body.inner_tx_type();

            if matches!(tx_type, TxType::Legacy) {
                let is_protected = matches!(
                    &tx.body,
                    base_alloy_consensus::ZkSequencerTxBody::Legacy(inner) if inner.chain_id.is_some()
                );
                self.protected_bits.set_bit(self.legacy_tx_count as usize, is_protected);
                self.legacy_tx_count += 1;
            }

            let (to, nonce, gas) = match &tx.body {
                base_alloy_consensus::ZkSequencerTxBody::Legacy(inner) => {
                    (inner.to(), inner.nonce(), inner.gas_limit())
                }
                base_alloy_consensus::ZkSequencerTxBody::Eip2930(inner) => {
                    (inner.to(), inner.nonce(), inner.gas_limit())
                }
                base_alloy_consensus::ZkSequencerTxBody::Eip1559(inner) => {
                    (inner.to(), inner.nonce(), inner.gas_limit())
                }
                base_alloy_consensus::ZkSequencerTxBody::Eip7702(inner) => {
                    (Some(inner.to), inner.nonce(), inner.gas_limit())
                }
            };

            let contract_creation_bit = match to {
                Some(address) => {
                    self.tx_tos.push(address);
                    0
                }
                None => 1,
            };
            let mut tx_data_buf = Vec::new();
            span_batch_tx.encode(&mut tx_data_buf);

            self.contract_creation_bits.set_bit(offset + i, contract_creation_bit == 1);
            self.tx_senders.push(tx.sender);
            self.tx_nonces.push(nonce);
            self.tx_data.push(tx_data_buf);
            self.tx_gases.push(gas);
            self.tx_types.push(tx_type);
        }

        self.total_block_tx_count += total_block_tx_count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use alloy_consensus::{SignableTransaction, TxEip1559};
    use alloy_primitives::{Signature, TxKind, address};
    use base_alloy_consensus::{TxZkSequencer, ZkSequencerTxBody};

    use super::*;

    #[test]
    fn test_full_txs_roundtrip_with_reused_senders() {
        let sender = address!("1111111111111111111111111111111111111111");
        let to = address!("2222222222222222222222222222222222222222");
        let tx_a = OpTxEnvelope::from(TxZkSequencer::new(
            sender,
            ZkSequencerTxBody::Eip1559(TxEip1559 {
                chain_id: 1,
                nonce: 1,
                gas_limit: 21_000,
                max_fee_per_gas: 2,
                max_priority_fee_per_gas: 1,
                to: TxKind::Call(to),
                ..Default::default()
            }),
        ));
        let tx_b = OpTxEnvelope::from(TxZkSequencer::new(
            sender,
            ZkSequencerTxBody::Eip1559(TxEip1559 {
                chain_id: 1,
                nonce: 2,
                gas_limit: 22_000,
                max_fee_per_gas: 3,
                max_priority_fee_per_gas: 1,
                to: TxKind::Call(to),
                ..Default::default()
            }),
        ));

        let mut encoded_a = Vec::new();
        tx_a.encode_2718(&mut encoded_a);
        let mut encoded_b = Vec::new();
        tx_b.encode_2718(&mut encoded_b);

        let mut txs = ZkSpanBatchTransactions::default();
        txs.add_txs(vec![encoded_a.clone().into(), encoded_b.clone().into()], 1).unwrap();

        let mut encoded = Vec::new();
        txs.encode(&mut encoded).unwrap();

        let mut decoded = ZkSpanBatchTransactions { total_block_tx_count: 2, ..Default::default() };
        decoded.decode(&mut encoded.as_slice()).unwrap();

        assert_eq!(decoded.tx_senders, vec![sender, sender]);
        assert_eq!(decoded.full_txs(1).unwrap(), vec![encoded_a, encoded_b]);
    }

    #[test]
    fn test_add_txs_rejects_non_zk_transactions() {
        let tx = TxEip1559 {
            chain_id: 1,
            nonce: 1,
            gas_limit: 21_000,
            max_fee_per_gas: 2,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(address!("3333333333333333333333333333333333333333")),
            ..Default::default()
        }
        .into_signed(Signature::test_signature());

        let mut encoded = Vec::new();
        OpTxEnvelope::Eip1559(tx).encode_2718(&mut encoded);

        let mut txs = ZkSpanBatchTransactions::default();
        assert_eq!(
            txs.add_txs(vec![encoded.into()], 1),
            Err(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))
        );
    }
}
