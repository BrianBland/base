use std::fmt;

use base_zk_span_proof::{ZkSpanSignatureProofInput, ZkSpanSignatureProofJournal};
use sp1_sdk::blocking::{EnvProver, EnvProvingKey};
use sp1_sdk::{
    ExecutionReport, ProvingKey, SP1ProofMode, SP1ProofWithPublicValues, SP1Stdin,
    blocking::{ProveRequest, Prover, ProverClient},
    include_elf,
};

use crate::ZkSpanProofSp1Error;

/// The proof modes supported by the experimental SP1 zk-span prover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZkSpanSignatureProofSp1Mode {
    /// Execute the program and return public values without proving.
    Execute,
    /// Generate a core proof.
    Core,
    /// Generate a compressed proof.
    Compressed,
    /// Generate a Plonk proof.
    Plonk,
    /// Generate a Groth16 proof.
    Groth16,
}

impl ZkSpanSignatureProofSp1Mode {
    /// Returns `true` if this mode skips proof generation.
    pub const fn is_execute_only(self) -> bool {
        matches!(self, Self::Execute)
    }

    /// Converts this mode into the corresponding SP1 proof mode when proving.
    pub const fn proof_mode(self) -> Option<SP1ProofMode> {
        match self {
            Self::Execute => None,
            Self::Core => Some(SP1ProofMode::Core),
            Self::Compressed => Some(SP1ProofMode::Compressed),
            Self::Plonk => Some(SP1ProofMode::Plonk),
            Self::Groth16 => Some(SP1ProofMode::Groth16),
        }
    }
}

/// Execute-only SP1 result for the zk-span statement.
#[derive(Debug, Clone)]
pub struct ZkSpanSignatureProofSp1Execution {
    /// Decoded zk-span journal committed by the SP1 program.
    pub journal: ZkSpanSignatureProofJournal,
    /// SP1 execution report with instruction counts and gas estimates.
    pub report: ExecutionReport,
}

/// SP1 proof result for the zk-span statement.
#[derive(Debug, Clone)]
pub struct ZkSpanSignatureProofSp1Proof {
    /// Decoded zk-span journal committed by the SP1 program.
    pub journal: ZkSpanSignatureProofJournal,
    /// Raw SP1 proof bundle and public values.
    pub proof: SP1ProofWithPublicValues,
}

/// Host-side SP1 prover for the experimental zk-span statement.
#[derive(Clone)]
pub struct ZkSpanSignatureProofSp1Prover {
    /// SP1 prover selected from the environment.
    pub prover: EnvProver,
    /// Proving key for the embedded SP1 program.
    pub proving_key: EnvProvingKey,
}

impl fmt::Debug for ZkSpanSignatureProofSp1Prover {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ZkSpanSignatureProofSp1Prover")
            .field("prover", &"<sp1-env-prover>")
            .field("proving_key", &"<sp1-env-proving-key>")
            .finish()
    }
}

impl ZkSpanSignatureProofSp1Prover {
    /// Creates a prover using the backend selected by `SP1_PROVER`.
    pub fn from_env() -> Result<Self, ZkSpanProofSp1Error> {
        let prover = ProverClient::from_env();
        let proving_key = prover
            .setup(Self::elf())
            .map_err(|error| ZkSpanProofSp1Error::Setup(error.to_string()))?;
        Ok(Self { prover, proving_key })
    }

    /// Returns the embedded SP1 program ELF.
    pub fn elf() -> sp1_sdk::Elf {
        include_elf!("zk_span_signature_proof_sp1")
    }

    /// Executes the SP1 program without generating a proof.
    pub fn execute_input(
        &self,
        input: &ZkSpanSignatureProofInput,
    ) -> Result<ZkSpanSignatureProofSp1Execution, ZkSpanProofSp1Error> {
        let stdin = Self::stdin_for_input(input)?;
        let (public_values, report) = self
            .prover
            .execute(Self::elf(), stdin)
            .run()
            .map_err(|error| ZkSpanProofSp1Error::Execution(error.to_string()))?;
        let journal = Self::decode_public_values(public_values.as_slice())?;
        Ok(ZkSpanSignatureProofSp1Execution { journal, report })
    }

    /// Generates an SP1 proof for the provided input using the selected proof mode.
    pub fn prove_input(
        &self,
        input: &ZkSpanSignatureProofInput,
        mode: ZkSpanSignatureProofSp1Mode,
    ) -> Result<ZkSpanSignatureProofSp1Proof, ZkSpanProofSp1Error> {
        let Some(proof_mode) = mode.proof_mode() else {
            return Err(ZkSpanProofSp1Error::ProofGeneration(
                "execute-only mode does not generate an SP1 proof".to_string(),
            ));
        };

        let stdin = Self::stdin_for_input(input)?;
        let proof_request = self.prover.prove(&self.proving_key, stdin);
        let proof = match proof_mode {
            SP1ProofMode::Core => proof_request.core().run(),
            SP1ProofMode::Compressed => proof_request.compressed().run(),
            SP1ProofMode::Plonk => proof_request.plonk().run(),
            SP1ProofMode::Groth16 => proof_request.groth16().run(),
        }
        .map_err(|error| ZkSpanProofSp1Error::ProofGeneration(error.to_string()))?;
        let journal = Self::decode_public_values(proof.public_values.as_slice())?;

        Ok(ZkSpanSignatureProofSp1Proof { journal, proof })
    }

    /// Verifies an SP1 proof against this prover's embedded verifying key.
    pub fn verify_proof(
        &self,
        proof: &ZkSpanSignatureProofSp1Proof,
    ) -> Result<ZkSpanSignatureProofJournal, ZkSpanProofSp1Error> {
        self.prover
            .verify(&proof.proof, self.proving_key.verifying_key(), None)
            .map_err(|error| ZkSpanProofSp1Error::Verification(error.to_string()))?;
        Ok(proof.journal.clone())
    }

    /// Encodes a zk-span input into SP1 stdin.
    pub fn stdin_for_input(
        input: &ZkSpanSignatureProofInput,
    ) -> Result<SP1Stdin, ZkSpanProofSp1Error> {
        let input_bytes = input
            .encode()
            .map_err(|error| ZkSpanProofSp1Error::InputEncoding(error.to_string()))?;
        let mut stdin = SP1Stdin::new();
        stdin.write(&input_bytes);
        Ok(stdin)
    }

    /// Decodes SP1 public values into the zk-span journal type used by the RISC Zero path.
    pub fn decode_public_values(
        public_values: &[u8],
    ) -> Result<ZkSpanSignatureProofJournal, ZkSpanProofSp1Error> {
        ZkSpanSignatureProofJournal::decode(public_values)
            .map_err(|error| ZkSpanProofSp1Error::PublicValuesDecoding(error.to_string()))
    }
}
