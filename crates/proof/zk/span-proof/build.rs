//! Build script for embedding the RISC Zero guest when `prove` is enabled.

fn main() {
    if std::env::var_os("CARGO_FEATURE_PROVE").is_some() {
        risc0_build::embed_methods();
    }
}
