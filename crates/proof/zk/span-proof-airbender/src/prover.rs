use std::path::{Path, PathBuf};

use airbender_host::{Commit, Inputs, Program};
use base_zk_span_proof::{ZkSpanSignatureProofInput, ZkSpanSignatureProofJournal};

use crate::ZkSpanProofAirbenderError;

/// Proof modes supported by the Airbender zk-span benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZkSpanSignatureProofAirbenderMode {
    /// Run the guest without generating a proof.
    Execute,
    /// Generate a development proof.
    Dev,
    /// Generate a CPU base-layer proof.
    Cpu,
    /// Generate a real GPU recursion proof.
    Gpu,
}

impl ZkSpanSignatureProofAirbenderMode {
    /// Returns `true` when the mode skips proof generation.
    pub const fn is_execute_only(self) -> bool {
        matches!(self, Self::Execute)
    }
}

/// 32-byte public commitment used by the Airbender spike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZkSpanSignatureProofAirbenderCommitment {
    /// Final statement hash committed by the current zk-span proof statement.
    pub statement_hash: [u8; 32],
}

impl ZkSpanSignatureProofAirbenderCommitment {
    /// Builds the committed digest from the current zk-span journal.
    pub fn from_journal(journal: &ZkSpanSignatureProofJournal) -> Self {
        Self { statement_hash: journal.statement_hash }
    }

    /// Decodes the guest output registers into the committed digest bytes.
    pub fn from_words(words: [u32; 8]) -> Self {
        let mut statement_hash = [0u8; 32];
        for (index, word) in words.into_iter().enumerate() {
            statement_hash[index * 4..(index + 1) * 4].copy_from_slice(&word.to_be_bytes());
        }
        Self { statement_hash }
    }

    /// Returns the commitment in the same word layout committed by the guest.
    pub fn words(self) -> [u32; 8] {
        let mut words = [0u32; 8];
        for (index, chunk) in self.statement_hash.chunks_exact(4).enumerate() {
            words[index] = u32::from_be_bytes(chunk.try_into().expect("4-byte commitment chunk"));
        }
        words
    }
}

impl Commit for ZkSpanSignatureProofAirbenderCommitment {
    fn commit_words(&self) -> [u32; 8] {
        self.words()
    }
}

/// Stateless helpers for the Airbender zk-span spike.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ZkSpanSignatureProofAirbender;

impl ZkSpanSignatureProofAirbender {
    /// Returns the default dist directory produced by `cargo airbender build`.
    pub fn default_dist_dir() -> PathBuf {
        let manifest_dist_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("guest/dist/app");
        if manifest_dist_dir.exists() {
            return manifest_dist_dir;
        }

        if let Ok(current_exe) = std::env::current_exe() {
            if let Some(exe_dir) = current_exe.parent() {
                for relative in ["guest-dist/app", "guest/dist/app"] {
                    let candidate = exe_dir.join(relative);
                    if candidate.exists() {
                        return candidate;
                    }
                }
            }
        }

        manifest_dist_dir
    }

    /// Loads the Airbender guest program from the default dist directory.
    pub fn load_default_program() -> Result<Program, ZkSpanProofAirbenderError> {
        Self::load_program(Self::default_dist_dir())
    }

    /// Loads the Airbender guest program from an explicit dist directory.
    pub fn load_program(dist_dir: impl AsRef<Path>) -> Result<Program, ZkSpanProofAirbenderError> {
        Program::load(dist_dir)
            .map_err(|error| ZkSpanProofAirbenderError::ProgramLoad(error.to_string()))
    }

    /// Serializes a zk-span witness input into Airbender host inputs.
    pub fn inputs_for_input(
        input: &ZkSpanSignatureProofInput,
    ) -> Result<Inputs, ZkSpanProofAirbenderError> {
        let mut inputs = Inputs::new();
        inputs
            .push(input)
            .map_err(|error| ZkSpanProofAirbenderError::InputEncoding(error.to_string()))?;
        Ok(inputs)
    }

    /// Computes the expected Airbender public commitment for a zk-span journal.
    pub fn commitment_for_journal(
        journal: &ZkSpanSignatureProofJournal,
    ) -> ZkSpanSignatureProofAirbenderCommitment {
        ZkSpanSignatureProofAirbenderCommitment::from_journal(journal)
    }

    /// Decodes the guest output registers into the expected commitment type.
    pub fn commitment_for_output_words(words: [u32; 8]) -> ZkSpanSignatureProofAirbenderCommitment {
        ZkSpanSignatureProofAirbenderCommitment::from_words(words)
    }
}
