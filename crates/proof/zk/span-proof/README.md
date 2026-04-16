# base-zk-span-proof

Real zk proofs for sender-elided span batches.

This crate defines a concrete proof statement for zk-span batches:

- witness: prepared per-transaction records containing `(tx_type, signature, unsigned_body)`
- public output: a commitment to the normalized `(sender, tx_type, unsigned_body)` stream derived
  from them

The host verifier compares that commitment against the same normalized transaction stream
reconstructed from `ZkSpanBatchTransactions`, allowing end-to-end proof verification without
reintroducing per-transaction signatures into the batch payload.

The guest no longer decodes signed transaction envelopes. Instead, it reconstructs the exact
Ethereum signing preimage from `tx_type` plus `unsigned_body`, hashes it, recovers the signer from
`signature`, and commits the normalized sender + body stream.

The `prove` feature enables the RISC Zero host prover and embedded guest method build. In the zkVM
guest, proving uses the RISC Zero-patched `k256` stack together with the patched `tiny-keccak`
syscall path so the tiny benchmark witness exercises the accelerated crypto path instead of the
plain Rust fallback.

For local benchmarking, the example harness supports both execute-only and proving flows:

```bash
cargo run -p base-zk-span-proof --example zk_span_bench --features prove --release -- --tx-count 1000 --execute-only
cargo run -p base-zk-span-proof --example zk_span_bench --features prove --release -- --tx-count 1 --mode composite
```
