# base-replay-build-bench

Replayed-building benchmark against a private Base mainnet state snapshot.

For each canonical block `i` in the replay range, the harness runs the **real
builder path** and the **real canonical execution path**, in that order:

1. **Build (timed).** Construct `BasePayloadBuilderAttributes` from the
   canonical header of block `i` (timestamp, prev-randao, fee recipient, gas
   limit, Jovian EIP-1559 parameters decoded from the canonical extra data,
   deposit transactions from the canonical block body). Inject the canonical
   block's regular transactions into a real `PendingPool` and drive
   `base_execution_payload_builder::Builder::build` — the same code path the
   live sequencer uses (selection, predicates, ordering, execution, sealing
   including a synchronous state root). The built payload is **discarded**;
   only timings and payload statistics are recorded.
2. **Advance (untimed).** Execute the canonical block `i` with the real block
   executor and insert the resulting bundle state into an `ExecutionCache`,
   mirroring how the live node keeps uncommitted blocks in memory. Every
   subsequent build therefore observes canonical post-state through a
   `CachedStateProvider`, exactly like the live cross-block cache.
3. **Validate.** Receipts and gas usage of the executed canonical block are
   compared against the receipts stored in the snapshot; any divergence aborts
   the run with evidence.

The loop is: build `i` from state at `i-1`, discard the payload, execute
canonical `i` to advance state, then inject `i+1`'s transactions and repeat.

## Scope and known caveats

- The build phase computes the state root **synchronously inside the timed
  interval**; the live sequencer overlaps it with a parallel state-root job.
  Build timings are therefore an upper bound on the sealing component.
- The state root of a *built* payload can diverge from canonical for accounts
  touched by earlier replayed (in-cache) blocks, because trie nodes are read
  from the anchor provider. This does not affect execution semantics, which
  always observe the correct post-state; validation is anchored to the
  canonical execution receipts, not to built-payload roots.
- No transaction broadcast, no writes to any database: the snapshot is opened
  read-only and all state advancement stays in the process-local
  `ExecutionCache`. Unwinding is discarding the snapshot.

## Usage

```bash
cargo build --release -p base-replay-build-bench
target/release/base-replay-build-bench --datadir <private-snapshot> --inspect
target/release/base-replay-build-bench --datadir <private-snapshot> \
    --run --count 64 --output results.json
```

`--datadir` must be a private snapshot root (private `db/mdbx.dat` plus shared
read-only `rocksdb`/`static_files` links; see `~/perf-tools` on the devbox).
`--from` defaults to `head - count` so the whole replay range is contained in
the snapshot. `--no-build` runs canonical execution + validation only (state
correctness smoke test). Run arms as fresh processes under `perf abba`.
