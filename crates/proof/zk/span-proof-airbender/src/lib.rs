#![doc = include_str!("../README.md")]

mod error;
pub use error::ZkSpanProofAirbenderError;

mod prover;
pub use prover::{
    ZkSpanSignatureProofAirbender, ZkSpanSignatureProofAirbenderCommitment,
    ZkSpanSignatureProofAirbenderMode,
};
