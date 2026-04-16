//! This module contains the top level span batch transaction data type.

use alloy_consensus::{Transaction, TxEip1559, TxEip2930, TxEip7702, TxEnvelope, TxLegacy, TxType};
use alloy_primitives::{Address, Signature, U256};
use alloy_rlp::{Bytes, Decodable, Encodable};
use base_alloy_consensus::{OpTxEnvelope, TxZkSequencer, ZkSequencerTxBody};

use crate::{
    SpanBatchEip1559TransactionData, SpanBatchEip2930TransactionData,
    SpanBatchEip7702TransactionData, SpanBatchError, SpanBatchLegacyTransactionData,
    SpanDecodingError,
};

/// The typed transaction data for a transaction within a span batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanBatchTransactionData {
    /// Legacy transaction data.
    Legacy(SpanBatchLegacyTransactionData),
    /// EIP-2930 transaction data.
    Eip2930(SpanBatchEip2930TransactionData),
    /// EIP-1559 transaction data.
    Eip1559(SpanBatchEip1559TransactionData),
    /// EIP-7702 transaction data.
    Eip7702(SpanBatchEip7702TransactionData),
}

impl Encodable for SpanBatchTransactionData {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        match self {
            Self::Legacy(data) => {
                data.encode(out);
            }
            Self::Eip2930(data) => {
                out.put_u8(TxType::Eip2930 as u8);
                data.encode(out);
            }
            Self::Eip1559(data) => {
                out.put_u8(TxType::Eip1559 as u8);
                data.encode(out);
            }
            Self::Eip7702(data) => {
                out.put_u8(TxType::Eip7702 as u8);
                data.encode(out);
            }
        }
    }
}

impl Decodable for SpanBatchTransactionData {
    fn decode(r: &mut &[u8]) -> Result<Self, alloy_rlp::Error> {
        if !r.is_empty() && r[0] > 0x7F {
            // Legacy transaction
            return Ok(Self::Legacy(SpanBatchLegacyTransactionData::decode(r)?));
        }
        // Non-legacy transaction (EIP-2718 envelope encoding)
        Self::decode_typed(r)
    }
}

impl TryFrom<&TxEnvelope> for SpanBatchTransactionData {
    type Error = SpanBatchError;

    fn try_from(tx_envelope: &TxEnvelope) -> Result<Self, Self::Error> {
        match tx_envelope {
            TxEnvelope::Legacy(s) => {
                let s = s.tx();
                Ok(Self::Legacy(SpanBatchLegacyTransactionData {
                    value: s.value,
                    gas_price: U256::from(s.gas_price),
                    data: Bytes::from(s.input().to_vec()),
                }))
            }
            TxEnvelope::Eip2930(s) => {
                let s = s.tx();
                Ok(Self::Eip2930(SpanBatchEip2930TransactionData {
                    value: s.value,
                    gas_price: U256::from(s.gas_price),
                    data: Bytes::from(s.input().to_vec()),
                    access_list: s.access_list.clone(),
                }))
            }
            TxEnvelope::Eip1559(s) => {
                let s = s.tx();
                Ok(Self::Eip1559(SpanBatchEip1559TransactionData {
                    value: s.value,
                    max_fee_per_gas: U256::from(s.max_fee_per_gas),
                    max_priority_fee_per_gas: U256::from(s.max_priority_fee_per_gas),
                    data: Bytes::from(s.input().to_vec()),
                    access_list: s.access_list.clone(),
                }))
            }
            TxEnvelope::Eip7702(s) => {
                let s = s.tx();
                Ok(Self::Eip7702(SpanBatchEip7702TransactionData {
                    value: s.value,
                    max_fee_per_gas: U256::from(s.max_fee_per_gas),
                    max_priority_fee_per_gas: U256::from(s.max_priority_fee_per_gas),
                    data: Bytes::from(s.input().to_vec()),
                    access_list: s.access_list.clone(),
                    authorization_list: s.authorization_list.clone(),
                }))
            }
            _ => Err(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionType)),
        }
    }
}

impl TryFrom<&TxZkSequencer> for SpanBatchTransactionData {
    type Error = SpanBatchError;

    fn try_from(value: &TxZkSequencer) -> Result<Self, Self::Error> {
        match &value.body {
            ZkSequencerTxBody::Legacy(s) => Ok(Self::Legacy(SpanBatchLegacyTransactionData {
                value: s.value,
                gas_price: U256::from(s.gas_price),
                data: Bytes::from(s.input().to_vec()),
            })),
            ZkSequencerTxBody::Eip2930(s) => Ok(Self::Eip2930(SpanBatchEip2930TransactionData {
                value: s.value,
                gas_price: U256::from(s.gas_price),
                data: Bytes::from(s.input().to_vec()),
                access_list: s.access_list.clone(),
            })),
            ZkSequencerTxBody::Eip1559(s) => Ok(Self::Eip1559(SpanBatchEip1559TransactionData {
                value: s.value,
                max_fee_per_gas: U256::from(s.max_fee_per_gas),
                max_priority_fee_per_gas: U256::from(s.max_priority_fee_per_gas),
                data: Bytes::from(s.input().to_vec()),
                access_list: s.access_list.clone(),
            })),
            ZkSequencerTxBody::Eip7702(s) => Ok(Self::Eip7702(SpanBatchEip7702TransactionData {
                value: s.value,
                max_fee_per_gas: U256::from(s.max_fee_per_gas),
                max_priority_fee_per_gas: U256::from(s.max_priority_fee_per_gas),
                data: Bytes::from(s.input().to_vec()),
                access_list: s.access_list.clone(),
                authorization_list: s.authorization_list.clone(),
            })),
        }
    }
}

impl SpanBatchTransactionData {
    /// Returns the transaction type of the [`SpanBatchTransactionData`].
    pub const fn tx_type(&self) -> TxType {
        match self {
            Self::Legacy(_) => TxType::Legacy,
            Self::Eip2930(_) => TxType::Eip2930,
            Self::Eip1559(_) => TxType::Eip1559,
            Self::Eip7702(_) => TxType::Eip7702,
        }
    }

    /// Decodes a typed transaction into a [`SpanBatchTransactionData`] from a byte slice.
    pub fn decode_typed(b: &[u8]) -> Result<Self, alloy_rlp::Error> {
        if b.len() <= 1 {
            return Err(alloy_rlp::Error::Custom("Invalid transaction data"));
        }

        match b[0].try_into().map_err(|_| alloy_rlp::Error::Custom("Invalid tx type"))? {
            TxType::Eip2930 => {
                Ok(Self::Eip2930(SpanBatchEip2930TransactionData::decode(&mut &b[1..])?))
            }
            TxType::Eip1559 => {
                Ok(Self::Eip1559(SpanBatchEip1559TransactionData::decode(&mut &b[1..])?))
            }
            TxType::Eip7702 => {
                Ok(Self::Eip7702(SpanBatchEip7702TransactionData::decode(&mut &b[1..])?))
            }
            _ => Err(alloy_rlp::Error::Custom("Invalid transaction type")),
        }
    }

    /// Converts an [`OpTxEnvelope`] into span batch transaction data.
    pub fn try_from_op_tx_envelope(tx_envelope: &OpTxEnvelope) -> Result<Self, SpanBatchError> {
        match tx_envelope {
            OpTxEnvelope::Legacy(s) => {
                let s = s.tx();
                Ok(Self::Legacy(SpanBatchLegacyTransactionData {
                    value: s.value,
                    gas_price: U256::from(s.gas_price),
                    data: Bytes::from(s.input().to_vec()),
                }))
            }
            OpTxEnvelope::Eip2930(s) => {
                let s = s.tx();
                Ok(Self::Eip2930(SpanBatchEip2930TransactionData {
                    value: s.value,
                    gas_price: U256::from(s.gas_price),
                    data: Bytes::from(s.input().to_vec()),
                    access_list: s.access_list.clone(),
                }))
            }
            OpTxEnvelope::Eip1559(s) => {
                let s = s.tx();
                Ok(Self::Eip1559(SpanBatchEip1559TransactionData {
                    value: s.value,
                    max_fee_per_gas: U256::from(s.max_fee_per_gas),
                    max_priority_fee_per_gas: U256::from(s.max_priority_fee_per_gas),
                    data: Bytes::from(s.input().to_vec()),
                    access_list: s.access_list.clone(),
                }))
            }
            OpTxEnvelope::Eip7702(s) => {
                let s = s.tx();
                Ok(Self::Eip7702(SpanBatchEip7702TransactionData {
                    value: s.value,
                    max_fee_per_gas: U256::from(s.max_fee_per_gas),
                    max_priority_fee_per_gas: U256::from(s.max_priority_fee_per_gas),
                    data: Bytes::from(s.input().to_vec()),
                    access_list: s.access_list.clone(),
                    authorization_list: s.authorization_list.clone(),
                }))
            }
            OpTxEnvelope::ZkSequencer(s) => Self::try_from(s.inner()),
            OpTxEnvelope::Deposit(_) => {
                Err(SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionType))
            }
        }
    }

    /// Converts the [`SpanBatchTransactionData`] into a signed transaction as [`TxEnvelope`].
    pub fn to_signed_tx(
        &self,
        nonce: u64,
        gas: u64,
        to: Option<Address>,
        chain_id: u64,
        signature: Signature,
        is_protected: bool,
    ) -> Result<TxEnvelope, SpanBatchError> {
        Ok(match self {
            Self::Legacy(data) => TxEnvelope::Legacy(data.to_signed_tx(
                nonce,
                gas,
                to,
                chain_id,
                signature,
                is_protected,
            )?),
            Self::Eip2930(data) => {
                TxEnvelope::Eip2930(data.to_signed_tx(nonce, gas, to, chain_id, signature)?)
            }
            Self::Eip1559(data) => {
                TxEnvelope::Eip1559(data.to_signed_tx(nonce, gas, to, chain_id, signature)?)
            }
            Self::Eip7702(data) => {
                let Some(addr) = to else {
                    return Err(SpanBatchError::Decoding(
                        SpanDecodingError::InvalidTransactionData,
                    ));
                };
                TxEnvelope::Eip7702(data.to_signed_tx(nonce, gas, addr, chain_id, signature)?)
            }
        })
    }

    /// Converts the [`SpanBatchTransactionData`] into a canonical zk-backed sequencer transaction.
    pub fn to_zk_tx(
        &self,
        sender: Address,
        nonce: u64,
        gas: u64,
        to: Option<Address>,
        chain_id: u64,
        is_protected: bool,
    ) -> Result<TxZkSequencer, SpanBatchError> {
        let body = match self {
            Self::Legacy(data) => ZkSequencerTxBody::Legacy(TxLegacy {
                chain_id: is_protected.then_some(chain_id),
                nonce,
                gas_price: u256_to_u128(data.gas_price)?,
                gas_limit: gas,
                to: to.map_or(alloy_primitives::TxKind::Create, alloy_primitives::TxKind::Call),
                value: data.value,
                input: data.data.clone().into(),
            }),
            Self::Eip2930(data) => ZkSequencerTxBody::Eip2930(TxEip2930 {
                chain_id,
                nonce,
                gas_price: u256_to_u128(data.gas_price)?,
                gas_limit: gas,
                to: to.map_or(alloy_primitives::TxKind::Create, alloy_primitives::TxKind::Call),
                value: data.value,
                input: data.data.clone().into(),
                access_list: data.access_list.clone(),
            }),
            Self::Eip1559(data) => ZkSequencerTxBody::Eip1559(TxEip1559 {
                chain_id,
                nonce,
                max_fee_per_gas: u256_to_u128(data.max_fee_per_gas)?,
                max_priority_fee_per_gas: u256_to_u128(data.max_priority_fee_per_gas)?,
                gas_limit: gas,
                to: to.map_or(alloy_primitives::TxKind::Create, alloy_primitives::TxKind::Call),
                value: data.value,
                input: data.data.clone().into(),
                access_list: data.access_list.clone(),
            }),
            Self::Eip7702(data) => {
                let Some(to) = to else {
                    return Err(SpanBatchError::Decoding(
                        SpanDecodingError::InvalidTransactionData,
                    ));
                };
                ZkSequencerTxBody::Eip7702(TxEip7702 {
                    chain_id,
                    nonce,
                    max_fee_per_gas: u256_to_u128(data.max_fee_per_gas)?,
                    max_priority_fee_per_gas: u256_to_u128(data.max_priority_fee_per_gas)?,
                    gas_limit: gas,
                    to,
                    value: data.value,
                    input: data.data.clone().into(),
                    access_list: data.access_list.clone(),
                    authorization_list: data.authorization_list.clone(),
                })
            }
        };

        Ok(TxZkSequencer::new(sender, body))
    }
}

fn u256_to_u128(value: U256) -> Result<u128, SpanBatchError> {
    u128::from_be_bytes(
        value.to_be_bytes::<32>()[16..]
            .try_into()
            .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))?,
    )
    .try_into()
    .map_err(|_| SpanBatchError::Decoding(SpanDecodingError::InvalidTransactionData))
}
