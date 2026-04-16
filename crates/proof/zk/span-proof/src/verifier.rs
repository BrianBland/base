use base_protocol::{ZkSpanBatch, ZkSpanBatchTransactions};

use crate::{
    ZkSpanProofError, ZkSpanSignatureProof, ZkSpanSignatureProofJournal,
    ZkSpanSignatureProofStatement,
};

/// Verifies zk span batch proofs against block-hash-preserving transaction commitments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZkSpanSignatureProofVerifier {
    /// Expected image ID for the proving guest.
    pub image_id: [u32; 8],
}

impl ZkSpanSignatureProofVerifier {
    /// Creates a verifier for the provided proving guest image ID.
    pub const fn new(image_id: [u32; 8]) -> Self {
        Self { image_id }
    }

    /// Verifies proof bytes against the expected journal.
    pub fn verify_expected_journal(
        &self,
        proof: &ZkSpanSignatureProof,
        expected_journal: &ZkSpanSignatureProofJournal,
    ) -> Result<(), ZkSpanProofError> {
        let actual_journal = self.verify_proof(proof)?;
        if actual_journal != *expected_journal {
            return Err(ZkSpanProofError::JournalMismatch);
        }
        Ok(())
    }

    /// Verifies proof bytes against sender-compressed batch transactions.
    pub fn verify_batch_transactions(
        &self,
        proof: &ZkSpanSignatureProof,
        transactions: &ZkSpanBatchTransactions,
        block_tx_counts: &[u64],
        tx_roots: &[alloy_primitives::B256],
        chain_id: u64,
    ) -> Result<(), ZkSpanProofError> {
        let expected_journal = ZkSpanSignatureProofStatement::journal_for_batch_transactions(
            transactions,
            block_tx_counts,
            tx_roots,
            chain_id,
        )?;
        self.verify_expected_journal(proof, &expected_journal)
    }

    /// Verifies the proof stored inside a decoded zk span batch.
    pub fn verify_batch(&self, batch: &ZkSpanBatch) -> Result<(), ZkSpanProofError> {
        if batch.proof.is_empty() {
            return Err(ZkSpanProofError::MissingProof);
        }
        let proof = ZkSpanSignatureProof::decode(&batch.proof)?;
        let expected_journal = ZkSpanSignatureProofStatement::journal_for_batch(batch)?;
        self.verify_expected_journal(&proof, &expected_journal)
    }

    /// Verifies a proof envelope and returns its committed journal.
    #[cfg(feature = "prove")]
    pub fn verify_proof(
        &self,
        proof: &ZkSpanSignatureProof,
    ) -> Result<ZkSpanSignatureProofJournal, ZkSpanProofError> {
        use bincode::{config, serde as bincode_serde};
        use risc0_zkvm::Receipt;

        let (receipt, _) =
            bincode_serde::decode_from_slice::<Receipt, _>(&proof.receipt, config::standard())
                .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))?;
        receipt
            .verify(self.image_id)
            .map_err(|error| ZkSpanProofError::ReceiptVerification(error.to_string()))?;
        ZkSpanSignatureProofJournal::decode(&receipt.journal.bytes)
    }

    /// Returns an error when the crate was built without the `prove` feature.
    #[cfg(not(feature = "prove"))]
    pub fn verify_proof(
        &self,
        _proof: &ZkSpanSignatureProof,
    ) -> Result<ZkSpanSignatureProofJournal, ZkSpanProofError> {
        Err(ZkSpanProofError::UnsupportedTransactionType("prove_feature_disabled"))
    }
}
