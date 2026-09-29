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

## Payload validation integration

The scheduler, balance observations, multi-version state, and persistent workers live in
`base-common-evm` behind its std-only `parallel` feature. The node enables
`base-execution-evm/parallel`; proof and zkVM builds do not enable it by default.

Set `BASE_PARALLEL_EXECUTION_THREADS=N` before starting the node. The value is read once;
unset, zero, malformed values, or pool construction failure disable parallel execution.
Only Engine API payload contexts carry the transaction list. Block execution, building, RPC,
and flashblocks remain sequential. Restart the node to change the worker count.

Leading deposits and system calls execute normally. At the first non-deposit, workers execute
the remaining suffix against an overlay. A read-through cache requests misses from the calling
thread, which owns the actual State database; no Send/Sync assumption or unsafe access is made
about that database. Speculative reads only warm its cache. On failure, the complete overlay
is discarded before returning any result and ordinary sequential execution resumes. EIP-8130
payloads currently take this fallback; Amsterdam/BAL execution also stays sequential because
speculative reads do not preserve BAL transaction indices. A transaction hash/signer mismatch can fall back before
the first result, but becomes an error after any parallel result has been returned.

Every accepted result commits through the existing executor. Account balances are rebased to
the committed prefix, deferred fees (including zero-fee touched recipients) are included, and
storage original values are taken from that prefix. Creation/destruction clears storage in
the overlay. The ordinary commit path retains receipts, hooks, bundles, and reverts.

Validate fixtures through the real executor (the wrapper injects a payload context for fixtures
only; its explicit worker counts do not mutate the process-wide environment gate):

```sh
export CARGO_TARGET_DIR=/Users/brianbland/code/scratch/target-live2
cargo build --release -p base-parallel-evm-bench
timeout 600 "$CARGO_TARGET_DIR/release/base-parallel-evm-bench" bench --via-executor --data /Users/brianbland/code/scratch/fixtures-dev --threads 1,4,8 --iters 1
timeout 600 "$CARGO_TARGET_DIR/release/base-parallel-evm-bench" bench --via-executor --data /Users/brianbland/code/scratch/fixtures-final --threads 1,4,8 --iters 1
```

Executor mode requires a successful parallel run, compares full receipts and `BundleState`
(including storage and reverts), and replays every recorded state-hook commit against the
same pre-state to compare the committed values after each transaction. Hooks are installed
with `execute_one_with_state_hook` on the executor's outer State, not its nested input DB.
The legacy benchmark still compares receipts and post-state at every timed iteration.

Deploy by building `cargo build --release -p base-reth-node` and starting the node with
`BASE_PARALLEL_EXECUTION_THREADS=8` (or the desired count). Fallback reasons are debug-level
structured events from `base_common_evm::executor::block_executor`. Database-service overhead
and suffix-result retention need live measurement; fixture timings are not node speedup claims.

## Basic-builder simulation (Phase 1 only)

`builder-sim` chooses fixture transactions in canonical order, rather than taking an unordered
parallel block suffix. Deposits/system calls are an untimed sequential prefix. Each subsequent
choice goes through the production `BaseBlockExecutor` without-commit/commit pair; the production
builder and flashblocks are not wired. This models fair ordering, not live pool selection.

```sh
export CARGO_TARGET_DIR=/Users/brianbland/code/scratch/target-spec
cargo build --release -p base-parallel-evm-bench
timeout 900 "$CARGO_TARGET_DIR/release/base-parallel-evm-bench" builder-sim --data /Users/brianbland/code/scratch/fixtures-dev --threads 4,8 --k 32,128 --iters 3 --forwarding false --idle-prewarm-ms 0 --inject-invalid 0
```

Repeat with `--forwarding true`, `--idle-prewarm-ms 20`, and `--inject-invalid 0.1`. The fault
fraction is in [0,1], seeded from transaction hash/block/index; selected envelopes are replaced
with synthetic high-nonce transactions using the fixture signer. Signatures are deliberately not
recovered for those invalid candidates. Rejected nonce lanes are skipped, and a skip clears the
prediction window before refilling it. Faulted paths can touch fixture-missing state, which `PreDb`
treats as empty in both arms; these gates are loop-parity tests, not claims about canonical roots.

Gas reservation and cumulative Jovian DA-footprint checks are enforced by the executor.
`--tx-da-limit` and `--block-da-limit` add optional Fjord-estimated byte limits matching the basic
builder's accounting. Predicate evaluation, pool priorities, sender-tip prechecks, elapsed-time
cutoffs and resource-metering policies are not simulated. The choice stream substitutes for them;
the real builder's existing checks and commit-condition callback remain authoritative in Phase 2.

Worker pools persist across blocks/iterations. Each iteration interleaves a sequential reference
and rotating K/thread arms, retaining each arm's minimum loop time and its counters. Every timed
arm must match included hashes/order, full receipts and full `BundleState` including reverts.
Prewarm is excluded from loop time; it represents otherwise idle downtime, not free end-to-end
speedup. Output reports per-block timings, hits/considered choices, validation failures, waste
(started executions minus consumed results, after bounded worker settling), and nearest-rank
p50/p99 of per-block minima. Admission counts are deterministic; worker counts vary with scheduling.

Forwarding is advisory and batch-local: overlapping submissions retain matching jobs, and newly
queued jobs share the new batch's `MvMemory`. A rolling extension does not replay all retained
predictions into its new batch. Candidate execution reconstructs an EVM per job. Both limitations
are deliberate Phase 1 measurement baselines, not production scheduling recommendations.

See [measurements](BUILDER_SIM_RESULTS.md) and the library's [contract](../../common/evm/SPECULATOR.md).
