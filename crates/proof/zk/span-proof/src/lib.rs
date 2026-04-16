#![doc = include_str!("../README.md")]

mod error;
pub use error::ZkSpanProofError;

mod input;
pub use input::{ZkSpanSignatureProofInput, ZkSpanSignatureProofTransaction};

mod journal;
pub use journal::ZkSpanSignatureProofJournal;

mod proof;
pub use proof::ZkSpanSignatureProof;

mod statement;
pub use statement::ZkSpanSignatureProofStatement;

mod verifier;
pub use verifier::ZkSpanSignatureProofVerifier;

#[cfg(feature = "prove")]
mod guest_methods;
#[cfg(feature = "prove")]
pub use guest_methods::{
    ZK_SPAN_SIGNATURE_PROOF_ELF, ZK_SPAN_SIGNATURE_PROOF_ID, ZK_SPAN_SIGNATURE_PROOF_PATH,
};

#[cfg(feature = "prove")]
mod prover;
#[cfg(feature = "prove")]
pub use prover::ZkSpanSignatureProofProver;
