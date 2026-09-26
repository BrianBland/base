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

## Scheduler stress diagnostics

Every benchmark iteration must match the sequential receipts and full post-state, not only
the initial correctness pass. `stress.sh` repeats the benchmark with an external 300-second
timeout; any nonzero exit (including timeout) fails the run and preserves its log.

Build with `--features scheduler-watchdog` to enable an independent watchdog thread per block.
If the commit frontier does not advance for two consecutive polling intervals, it dumps the
frontier, commit role, stop flag, and every transaction's status, dependencies, invalidation
count and result slot, then exits unsuccessfully. Dumps are best-effort concurrent snapshots;
slot locks are never waited on. `PEVM_STALL_MS` sets the polling interval (default 5000 ms).
The feature is disabled by default and adds no worker-loop instructions.

```sh
CARGO_TARGET_DIR=/Users/brianbland/code/scratch/target-live cargo build --release -p base-parallel-evm-bench --features scheduler-watchdog
bash crates/infra/parallel-evm-bench/stress.sh /Users/brianbland/code/scratch/target-live/release/base-parallel-evm-bench /Users/brianbland/code/scratch/fixtures-dev 20 /tmp/pevm-stress
```
