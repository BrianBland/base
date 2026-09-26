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

The execution read path uses Alloy's non-SipHash `DefaultHashBuilder` consistently:
the pre-state and account-read index use `alloy_primitives::map::HashMap`, and committed-state
and multi-version maps use that same builder in `DashMap`. Fixture serialization and RPC
recording are outside this hot path and retain their existing maps.

Precompiles use the production node's native crypto backends: `blst`, `c-kzg`,
`p256-aws-lc-rs`, and `secp256k1`. Both sequential and parallel benchmark arms share these
features. Verify the resolved configuration with
`cargo tree -p base-parallel-evm-bench -e features -i revm-precompile`.

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

### Execution publication contract

Publishing a completed incarnation's `PENDING` or `EXECUTED` status transfers ownership:
the old executor must not mutate scheduler state afterward. Otherwise a paused executor can
invalidate a newer incarnation that another thread has already committed, leaving dependents
waiting forever on a committed transaction marked `PENDING`.

The invalidation counter is an early-retry hint, not the commit correctness gate. Check it
while the incarnation is still `EXECUTING`, count any invalidated execution, and publish its
final status once. A lower writer racing between that check and publication can miss an
early retry, but the unchanged in-order committed-value validation rejects stale reads and
re-executes against the exact prefix. No balance, nonce, read-resolution, or apply semantics
change. All lower publishers have finished their invalidation loops before their final status
can be committed; with no post-publication mutation, committed statuses remain `EXECUTED`.

```sh
CARGO_TARGET_DIR=/Users/brianbland/code/scratch/target-live cargo build --release -p base-parallel-evm-bench --features scheduler-watchdog
bash crates/infra/parallel-evm-bench/stress.sh /Users/brianbland/code/scratch/target-live/release/base-parallel-evm-bench /Users/brianbland/code/scratch/fixtures-dev 20 /tmp/pevm-stress
```
