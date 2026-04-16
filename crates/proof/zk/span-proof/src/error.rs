use thiserror::Error;

/// Errors returned by zk span proof generation and verification.
#[derive(Debug, Error)]
pub enum ZkSpanProofError {
    /// The provided batch did not carry any proof bytes.
    #[error("zk span batch is missing proof bytes")]
    MissingProof,
    /// Batch transaction decoding or reconstruction failed.
    #[error("zk span batch transaction error: {0}")]
    BatchTransaction(String),
    /// The proof witness contained an unsupported transaction kind.
    #[error("unsupported transaction for zk span proof: {0}")]
    UnsupportedTransactionType(&'static str),
    /// Signed transaction bytes could not be decoded.
    #[error("failed to decode signed transaction")]
    TransactionDecoding,
    /// Sender recovery failed for a signed transaction witness.
    #[error("failed to recover sender from signed transaction")]
    SenderRecovery,
    /// The witness signature did not have the expected 65-byte Ethereum encoding.
    #[error("invalid signature length in witness transaction")]
    InvalidSignatureLength,
    /// The provided block partition does not match the transaction stream.
    #[error("block transaction counts did not match the transaction stream")]
    InvalidBlockPartition,
    /// Receipt bytes could not be serialized or deserialized.
    #[error("receipt serialization error: {0}")]
    ReceiptSerialization(String),
    /// RISC Zero receipt verification failed.
    #[cfg(feature = "prove")]
    #[error("receipt verification failed: {0}")]
    ReceiptVerification(String),
    /// RISC Zero proving failed.
    #[cfg(feature = "prove")]
    #[error("proof generation failed: {0}")]
    ProofGeneration(String),
    /// Building the prover executor environment failed.
    #[cfg(feature = "prove")]
    #[error("executor environment failed: {0}")]
    ExecutorEnvironment(String),
    /// The verified receipt journal did not match the expected batch journal.
    #[error("proof journal did not match expected zk span statement commitment")]
    JournalMismatch,
}
