# base-parallel-evm-bench

Experiment harness: does parallel transaction execution speed up real Base blocks?

- `fetch` executes canonical blocks against an archive RPC and records every piece of parent
  state they touch into JSON fixtures (verified against the header's gas used and logs bloom).
- `bench` executes each fixture with the production sequential executor and with a minimal
  optimistic parallel executor (speculative parallel execution, in-order value-validated commit,
  deferred fee-vault credits, balances validated by what execution observed and rebased onto the
  committed balance), checks receipts and post-state match exactly, and reports timings
  plus a dependency critical-path bound on the achievable speedup.

```sh
cargo run --release -p base-parallel-evm-bench -- fetch --rpc http://127.0.0.1:18545 --from N --count 50 --out fixtures
cargo run --release -p base-parallel-evm-bench -- bench --data fixtures --threads 1,2,4,8,12
```

All state is in memory, so timings exclude database I/O.
