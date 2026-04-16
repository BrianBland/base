use bincode::{config, serde as bincode_serde};
use serde::{Deserialize, Serialize};

use crate::ZkSpanProofError;

/// Encoded receipt payload stored inside a zk span batch proof field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZkSpanSignatureProof {
    /// Proof format version.
    pub version: u8,
    /// Serialized RISC Zero receipt bytes.
    pub receipt: Vec<u8>,
}

impl ZkSpanSignatureProof {
    /// Current proof envelope format version.
    pub const VERSION: u8 = 1;

    /// Creates a new proof envelope from serialized receipt bytes.
    pub fn new(receipt: Vec<u8>) -> Self {
        Self { version: Self::VERSION, receipt }
    }

    /// Encodes the proof envelope into batch payload bytes.
    pub fn encode(&self) -> Result<Vec<u8>, ZkSpanProofError> {
        bincode_serde::encode_to_vec(self, config::standard())
            .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))
    }

    /// Decodes the proof envelope from batch payload bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ZkSpanProofError> {
        let (value, _) = bincode_serde::decode_from_slice(bytes, config::standard())
            .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))?;
        Ok(value)
    }
}
