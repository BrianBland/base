# base-zk-span-proof-airbender

Experimental Airbender prover spike for the zk-span signature-elision statement.

This spike keeps the existing zk-span witness shape:

- witness: prepared per-transaction `(tx_type, signature, unsigned_body)` records
- witness: explicit `block_tx_counts` matching the current block-hash-preserving statement

Airbender's guest output is limited to eight `u32` words, which now matches the current zk-span
public output exactly:

- public output: `statement_hash = keccak256("base.zkspan.v2" || normalized_txs_hash || tx_roots_hash)`

The goal is to compare Airbender execute/prove performance against the existing RISC Zero and SP1
spikes while keeping the proof logic as close as possible to the current statement.

## Build

Install `cargo-airbender` on the pinned nightly toolchain.

On a machine without a local CUDA toolkit, install it without default features:

```sh
cargo +nightly-2026-02-10 install --path /path/to/airbender-platform/crates/cargo-airbender --force --no-default-features
```

On a CUDA-capable Linux builder, install it with GPU support enabled. This requires a local CUDA
toolkit with `nvcc` plus `cmake >= 3.28`:

```sh
cargo +nightly-2026-02-10 install --path /path/to/airbender-platform/crates/cargo-airbender --force
```

Build the guest once:

```sh
cd crates/proof/zk/span-proof-airbender
cargo +nightly-2026-02-10 airbender build --project guest --release
```

Then run the benchmark from the host crate:

```sh
cd crates/proof/zk/span-proof-airbender
cargo +nightly-2026-02-10 run --release --example zk_span_airbender_bench -- --tx-count 1 --mode execute
```

To build the real GPU proving path, enable the crate feature:

```sh
cd crates/proof/zk/span-proof-airbender
cargo +nightly-2026-02-10 run --release --example zk_span_airbender_bench --features gpu-prover -- --tx-count 1 --mode gpu
```

On a shared GPU, start with the low-VRAM path:

```sh
cd crates/proof/zk/span-proof-airbender
ZKSYNC_AIRBENDER_LOW_VRAM_MODE=1 ZK_AIRBENDER_GPU_LOW_VRAM=1 cargo +nightly-2026-02-10 run --release --example zk_span_airbender_bench --features gpu-prover -- --tx-count 1 --mode gpu
```

The GPU mode also accepts these tuning environment variables:

- `ZK_AIRBENDER_GPU_MAX_DEVICE_MB`
- `ZK_AIRBENDER_GPU_CONTEXT_HOST_MB`
- `ZK_AIRBENDER_GPU_PINNED_BUFFER_MB`
- `ZK_AIRBENDER_GPU_HOST_ALLOCATORS_PER_JOB`
- `ZK_AIRBENDER_GPU_HOST_ALLOCATORS_PER_DEVICE`
- `ZK_AIRBENDER_GPU_EXPECTED_JOBS`
- `ZK_AIRBENDER_GPU_MIN_FREE_ALLOCATORS_PER_JOB`

When packaging the benchmark for another machine, place the guest artifacts next to the binary
under `guest-dist/app`. The benchmark will auto-discover that layout if the original build-machine
path is unavailable.
