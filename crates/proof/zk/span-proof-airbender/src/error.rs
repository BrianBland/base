use thiserror::Error;

/// Errors returned by the experimental Airbender zk-span spike.
#[derive(Debug, Error)]
pub enum ZkSpanProofAirbenderError {
    /// Failed to load the built Airbender guest artifacts.
    #[error("failed to load Airbender guest artifacts: {0}")]
    ProgramLoad(String),
    /// Failed to serialize zk-span witness input for Airbender execution.
    #[error("failed to encode zk span input for Airbender: {0}")]
    InputEncoding(String),
}
