#![doc = include_str!("../README.md")]

mod error;
pub use error::ZkSpanProofSp1Error;

mod prover;
pub use prover::{
    ZkSpanSignatureProofSp1Execution, ZkSpanSignatureProofSp1Mode, ZkSpanSignatureProofSp1Proof,
    ZkSpanSignatureProofSp1Prover,
};
