use thiserror::Error;

/// Errors returned by the experimental SP1 zk span prover.
#[derive(Debug, Error)]
pub enum ZkSpanProofSp1Error {
    /// Failed to initialize the SP1 proving key for the embedded program.
    #[error("failed to initialize SP1 proving key: {0}")]
    Setup(String),
    /// Failed to serialize the zk-span witness input for SP1 execution.
    #[error("failed to encode zk span input: {0}")]
    InputEncoding(String),
    /// SP1 execution failed.
    #[error("SP1 execution failed: {0}")]
    Execution(String),
    /// SP1 proving failed.
    #[error("SP1 proving failed: {0}")]
    ProofGeneration(String),
    /// SP1 proof verification failed.
    #[error("SP1 proof verification failed: {0}")]
    Verification(String),
    /// The committed public values could not be decoded as a zk-span journal.
    #[error("failed to decode SP1 public values as zk span journal: {0}")]
    PublicValuesDecoding(String),
}
