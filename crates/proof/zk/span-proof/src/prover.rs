use std::sync::Arc;

use bincode::{config, serde as bincode_serde};
use risc0_zkvm::{
    ExecutorEnv, Prover, ProverOpts, Receipt, SessionInfo, SessionStats, compute_image_id,
    default_executor, default_prover,
};

use crate::{
    ZK_SPAN_SIGNATURE_PROOF_ELF, ZK_SPAN_SIGNATURE_PROOF_ID, ZkSpanProofError,
    ZkSpanSignatureProof, ZkSpanSignatureProofInput, ZkSpanSignatureProofVerifier,
};

/// Host-side prover for zk span signature proofs.
#[derive(Debug, Clone)]
pub struct ZkSpanSignatureProofProver {
    /// Embedded guest ELF bytes.
    pub elf: Arc<[u8]>,
    /// Computed image ID for receipt verification.
    pub image_id: [u32; 8],
}

impl ZkSpanSignatureProofProver {
    /// Creates a prover from raw guest ELF bytes and computes the image ID.
    pub fn new(elf: Vec<u8>) -> Result<Self, ZkSpanProofError> {
        let image_id: [u32; 8] = compute_image_id(&elf)
            .map_err(|error| ZkSpanProofError::ProofGeneration(error.to_string()))?
            .into();
        Ok(Self { elf: Arc::from(elf), image_id })
    }

    /// Creates a prover from the feature-gated embedded guest method.
    pub fn embedded() -> Result<Self, ZkSpanProofError> {
        let prover = Self::new(ZK_SPAN_SIGNATURE_PROOF_ELF.to_vec())?;
        if prover.image_id != ZK_SPAN_SIGNATURE_PROOF_ID {
            return Err(ZkSpanProofError::ProofGeneration(
                "embedded guest image ID mismatch".to_string(),
            ));
        }
        Ok(prover)
    }

    /// Returns a verifier bound to this prover's image ID.
    pub const fn verifier(&self) -> ZkSpanSignatureProofVerifier {
        ZkSpanSignatureProofVerifier::new(self.image_id)
    }

    /// Builds the executor environment for the provided witness input.
    fn executor_env_for_input(
        input: &ZkSpanSignatureProofInput,
    ) -> Result<ExecutorEnv<'static>, ZkSpanProofError> {
        let input_bytes = input.encode()?;
        ExecutorEnv::builder()
            .write_slice(&input_bytes)
            .build()
            .map_err(|error| ZkSpanProofError::ExecutorEnvironment(error.to_string()))
    }

    /// Executes the guest without proving and returns execution statistics.
    pub fn execute_input(
        &self,
        input: &ZkSpanSignatureProofInput,
    ) -> Result<SessionInfo, ZkSpanProofError> {
        let env = Self::executor_env_for_input(input)?;
        default_executor()
            .execute(env, &self.elf)
            .map_err(|error| ZkSpanProofError::ProofGeneration(error.to_string()))
    }

    /// Generates a real receipt for the provided witness input and returns prover statistics.
    pub fn prove_input_with_opts_and_stats(
        &self,
        input: &ZkSpanSignatureProofInput,
        opts: &ProverOpts,
    ) -> Result<(ZkSpanSignatureProof, SessionStats), ZkSpanProofError> {
        let env = Self::executor_env_for_input(input)?;
        let prove_info = default_prover()
            .prove_with_opts(env, &self.elf, opts)
            .map_err(|error| ZkSpanProofError::ProofGeneration(error.to_string()))?;
        let stats = prove_info.stats.clone();
        let receipt_bytes = Self::encode_receipt(&prove_info.receipt)?;

        Ok((ZkSpanSignatureProof::new(receipt_bytes), stats))
    }

    /// Generates a real receipt for the provided witness input using explicit prover options.
    pub fn prove_input_with_opts(
        &self,
        input: &ZkSpanSignatureProofInput,
        opts: &ProverOpts,
    ) -> Result<ZkSpanSignatureProof, ZkSpanProofError> {
        self.prove_input_with_opts_and_stats(input, opts).map(|(proof, _)| proof)
    }

    /// Generates a real composite receipt for the provided witness input.
    pub fn prove_input_composite(
        &self,
        input: &ZkSpanSignatureProofInput,
    ) -> Result<ZkSpanSignatureProof, ZkSpanProofError> {
        self.prove_input_with_opts(input, &ProverOpts::fast())
    }

    /// Generates a real composite receipt and returns prover statistics.
    pub fn prove_input_composite_with_stats(
        &self,
        input: &ZkSpanSignatureProofInput,
    ) -> Result<(ZkSpanSignatureProof, SessionStats), ZkSpanProofError> {
        self.prove_input_with_opts_and_stats(input, &ProverOpts::fast())
    }

    /// Generates a real succinct receipt for the provided witness input.
    pub fn prove_input(
        &self,
        input: &ZkSpanSignatureProofInput,
    ) -> Result<ZkSpanSignatureProof, ZkSpanProofError> {
        self.prove_input_with_opts(input, &ProverOpts::succinct())
    }

    /// Serializes a receipt into proof payload bytes.
    pub fn encode_receipt(receipt: &Receipt) -> Result<Vec<u8>, ZkSpanProofError> {
        bincode_serde::encode_to_vec(receipt, config::standard())
            .map_err(|error| ZkSpanProofError::ReceiptSerialization(error.to_string()))
    }
}
