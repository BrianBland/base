use alloy_consensus::TxType;
use alloy_primitives::{Address, B256, Signature, keccak256};
use bincode::{config, serde as bincode_serde};
use serde::{Deserialize, Serialize};

use crate::ZkSpanProofError;

/// Prepared witness transaction for zk span signature proving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZkSpanSignatureProofTransaction {
    /// Inner Ethereum transaction type.
    pub tx_type: u8,
    /// Raw 65-byte Ethereum signature in `(r, s, v)` form.
    pub signature: Vec<u8>,
    /// RLP-encoded unsigned transaction body exactly as committed by the batch.
    pub unsigned_body: Vec<u8>,
}

impl ZkSpanSignatureProofTransaction {
    /// Creates a prepared witness transaction.
    pub fn new(tx_type: u8, signature: Vec<u8>, unsigned_body: Vec<u8>) -> Self {
        Self { tx_type, signature, unsigned_body }
    }

    /// Returns the exact byte stream that Ethereum signs for this transaction.
    pub fn signing_payload(&self) -> Result<Vec<u8>, ZkSpanProofError> {
        match self.tx_type {
            value if value == TxType::Legacy as u8 => Ok(self.unsigned_body.clone()),
            value
                if value == TxType::Eip2930 as u8
                    || value == TxType::Eip1559 as u8
                    || value == TxType::Eip7702 as u8 =>
            {
                let mut payload = Vec::with_capacity(self.unsigned_body.len() + 1);
                payload.push(self.tx_type);
                payload.extend_from_slice(&self.unsigned_body);
                Ok(payload)
            }
            _ => Err(ZkSpanProofError::UnsupportedTransactionType("proof_input_tx_type")),
        }
    }

    /// Returns the Ethereum signing hash for this prepared witness transaction.
    pub fn signing_hash(&self) -> Result<B256, ZkSpanProofError> {
        Ok(keccak256(self.signing_payload()?))
    }

    /// Recovers the Ethereum sender address from the transaction signature and signing hash.
    pub fn recovered_sender(&self) -> Result<Address, ZkSpanProofError> {
        let mut signature = [0u8; 65];
        signature.copy_from_slice(&self.signature);
        let signature =
            Signature::from_raw_array(&signature).map_err(|_| ZkSpanProofError::SenderRecovery)?;
        signature
            .recover_address_from_prehash(&self.signing_hash()?)
            .map_err(|_| ZkSpanProofError::SenderRecovery)
    }

    /// Returns the normalized tuple committed by the proof journal.
    pub fn normalized_transaction(&self) -> Result<(u8, Address, Vec<u8>), ZkSpanProofError> {
        Ok((self.tx_type, self.recovered_sender()?, self.unsigned_body.clone()))
    }
}

/// Witness input for proving sender-elided sequencer transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZkSpanSignatureProofInput {
    /// Prepared witness transactions in canonical order.
    pub transactions: Vec<ZkSpanSignatureProofTransaction>,
    /// Number of transactions in each original L2 block.
    pub block_tx_counts: Vec<u64>,
}

impl ZkSpanSignatureProofInput {
    /// Creates a new proof input from prepared witness transactions.
    pub fn new(
        transactions: Vec<ZkSpanSignatureProofTransaction>,
        block_tx_counts: Vec<u64>,
    ) -> Self {
        Self { transactions, block_tx_counts }
    }

    /// Creates a new proof input from signed sequencer transaction bytes.
    pub fn from_signed_transactions(
        signed_transactions: &[Vec<u8>],
    ) -> Result<Self, ZkSpanProofError> {
        Self::from_signed_transactions_with_blocks(
            signed_transactions,
            vec![signed_transactions.len() as u64],
        )
    }

    /// Creates a new proof input from signed sequencer transaction bytes with explicit
    /// per-block transaction counts.
    pub fn from_signed_transactions_with_blocks(
        signed_transactions: &[Vec<u8>],
        block_tx_counts: Vec<u64>,
    ) -> Result<Self, ZkSpanProofError> {
        let transactions = signed_transactions
            .iter()
            .map(|signed_transaction| {
                crate::ZkSpanSignatureProofStatement::proof_transaction_from_signed_bytes(
                    signed_transaction,
                )
            })
            .collect::<Result<Vec<ZkSpanSignatureProofTransaction>, ZkSpanProofError>>()?;
        Self::new(transactions, block_tx_counts).validated()
    }

    /// Returns the same input after validating the transaction partition.
    pub fn validated(self) -> Result<Self, ZkSpanProofError> {
        self.validate()?;
        Ok(self)
    }

    /// Validates the per-block transaction counts against the transaction stream.
    pub fn validate(&self) -> Result<(), ZkSpanProofError> {
        if self.block_tx_counts.is_empty() {
            return Err(ZkSpanProofError::InvalidBlockPartition);
        }

        let total_transactions =
            self.block_tx_counts.iter().try_fold(0u64, |acc, block_tx_count| {
                acc.checked_add(*block_tx_count).ok_or(ZkSpanProofError::InvalidBlockPartition)
            })?;

        if total_transactions != self.transactions.len() as u64 {
            return Err(ZkSpanProofError::InvalidBlockPartition);
        }

        Ok(())
    }

    /// Bincode-encodes the proof input for guest execution.
    pub fn encode(&self) -> Result<Vec<u8>, ZkSpanProofError> {
        self.validate()?;
        bincode_serde::encode_to_vec(self, config::standard())
            .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))
    }

    /// Bincode-decodes the proof input from guest stdin bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ZkSpanProofError> {
        let (value, _) = bincode_serde::decode_from_slice(bytes, config::standard())
            .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))?;
        Ok(value)
    }
}
