mod generated {
    include!(concat!(env!("OUT_DIR"), "/methods.rs"));
}

/// Embedded guest ELF bytes for the zk span signature proof method.
pub use generated::ZK_SPAN_SIGNATURE_PROOF_ELF;
/// Image ID for the compiled zk span signature proof guest ELF.
pub use generated::ZK_SPAN_SIGNATURE_PROOF_ID;
/// Filesystem path to the compiled zk span signature proof guest ELF.
pub use generated::ZK_SPAN_SIGNATURE_PROOF_PATH;
