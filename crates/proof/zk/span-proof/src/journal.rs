use alloy_primitives::{Address, B256, keccak256};
use bincode::{config, serde as bincode_serde};
use serde::{Deserialize, Serialize};

use crate::ZkSpanProofError;

/// Public journal committed by the zk span proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZkSpanSignatureProofJournal {
    /// Commitment to the full zk span proof statement.
    pub statement_hash: [u8; 32],
}

impl ZkSpanSignatureProofJournal {
    /// Creates a journal from normalized `(tx_type, sender, unsigned_body_rlp)` tuples and
    /// original per-block transaction trie roots.
    pub fn from_normalized_transactions_and_roots(
        transactions: &[(u8, Address, Vec<u8>)],
        tx_roots: &[B256],
    ) -> Self {
        Self {
            statement_hash: Self::statement_hash(
                Self::normalized_transactions_hash(transactions),
                Self::tx_roots_hash(tx_roots),
            ),
        }
    }

    /// Returns the preimage hashed into `normalized_txs_hash`.
    pub fn normalized_transactions_preimage(transactions: &[(u8, Address, Vec<u8>)]) -> Vec<u8> {
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&(transactions.len() as u64).to_be_bytes());
        for (tx_type, sender, body) in transactions {
            preimage.push(*tx_type);
            preimage.extend_from_slice(sender.as_slice());
            preimage.extend_from_slice(&(body.len() as u64).to_be_bytes());
            preimage.extend_from_slice(body);
        }
        preimage
    }

    /// Returns the normalized transaction-stream commitment hash.
    pub fn normalized_transactions_hash(transactions: &[(u8, Address, Vec<u8>)]) -> [u8; 32] {
        keccak256(Self::normalized_transactions_preimage(transactions)).0
    }

    /// Returns the preimage hashed into `tx_roots_hash`.
    pub fn tx_roots_preimage(tx_roots: &[B256]) -> Vec<u8> {
        let mut preimage = Vec::with_capacity(8 + tx_roots.len() * B256::len_bytes());
        preimage.extend_from_slice(&(tx_roots.len() as u64).to_be_bytes());
        for tx_root in tx_roots {
            preimage.extend_from_slice(tx_root.as_slice());
        }
        preimage
    }

    /// Returns the per-block transaction-root commitment hash.
    pub fn tx_roots_hash(tx_roots: &[B256]) -> [u8; 32] {
        keccak256(Self::tx_roots_preimage(tx_roots)).0
    }

    /// Returns the final statement hash committed by the proof.
    pub fn statement_hash(normalized_txs_hash: [u8; 32], tx_roots_hash: [u8; 32]) -> [u8; 32] {
        let mut preimage = Vec::with_capacity(14 + 32 + 32);
        preimage.extend_from_slice(b"base.zkspan.v2");
        preimage.extend_from_slice(&normalized_txs_hash);
        preimage.extend_from_slice(&tx_roots_hash);
        keccak256(preimage).0
    }

    /// Bincode-encodes the journal as committed guest output.
    pub fn encode(&self) -> Result<Vec<u8>, ZkSpanProofError> {
        bincode_serde::encode_to_vec(self, config::standard())
            .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))
    }

    /// Bincode-decodes the journal from guest output bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ZkSpanProofError> {
        let (value, _) = bincode_serde::decode_from_slice(bytes, config::standard())
            .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))?;
        Ok(value)
    }
}
