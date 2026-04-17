# `base-prefetch`

Prefetching database wrapper utilities for Base execution.

## Overview

- `PrefetchingDb`: Wraps a database and consults a prefetch buffer for storage reads.
- `PrefetchHintBuilder`: Derives ERC-20 storage hints for `transfer`/`transferFrom`-style flows.
- `PrefetchHintBuilder::erc20_swap`: Derives hints for swap-style multi-leg flows (e.g. repeated
  pool transfers).
- `LatencyInjectingDb`: Synthetic in-memory database that injects cold-read latency to model
  cache misses.
- `PrefetchExperiment`: Benchmark-only experiment runner with `Baseline`, `Synchronous`, and
  `Asynchronous` prefetch modes.
- `PrefetchSyntheticTarget`: benchmark-only scripted target type for deeper-tree traces that mix
  account, code, storage, and block-hash reads.
- `PrefetchPlanner`: Cost-aware planner that can skip prefetching when projected gain is negative.
- `DowseHintTable`: Dowse-compatible JSON hint table keyed by code hash and selector.
- `DowseHintTableStore`: Atomically swappable active hint table that can reload JSON from disk.
- `PrefetchMetrics`: Shared per-execution telemetry for useful-prefetch rate and fallback latency.
- `PrefetchRuntimePolicy`: Adaptive runtime controller that can disable prefetch when observed gain
  collapses and periodically schedule probe runs to re-evaluate.
- `PrefetchStateViewFactory`: Helper for building consistent prefetch state views from an existing
  `StateProviderFactory`, rather than opening raw MDBX transactions.
- `FrontierPlannerRegistry`: Registry for deeper-tree frame-entry and callsite planners.
- `Erc20FrontierPlanner`, `Weth9FrontierPlanner`, `UniswapV2PairFrontierPlanner`, and
  `UniswapV2RouterFrontierPlanner`: first concrete deeper-tree planners for token, WETH, pool,
  and router flows.
- `PrefetchFrontierScheduler`: bounded scheduler for deeper-tree tasks with dirty-slot
  suppression, depth budgets, and runtime-aware speculative gating.

## Dowse JSON Hints

`base-prefetch` can load a dowse-compatible hint table from JSON and resolve it against a concrete
EVM call context. v1 currently supports JSON input only.

Reloads are atomic:

- successful reloads replace the active table immediately
- failed reloads return an error and keep the previous table active

Example JSON:

```json
{
  "version": 1,
  "metadata": {
    "description": "basic ERC-20 transfer hints",
    "source": "manual",
    "contract_name": "Token"
  },
  "entries": {
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {
      "0x23b872dd": [
        {
          "kind": "Storage",
          "slot": {
            "type": "Keccak256",
            "inputs": [
              {
                "type": "CalldataWord",
                "offset": 4
              },
              {
                "type": "Concrete",
                "value": "0x0000000000000000000000000000000000000000000000000000000000000000"
              }
            ]
          }
        }
      ],
      "*": [
        {
          "kind": "Account",
          "address": "0x4200000000000000000000000000000000000006"
        }
      ]
    }
  },
  "code_hashes": {
    "0x4200000000000000000000000000000000000006": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
}
```

Example usage:

```rust,ignore
use alloy_primitives::Address;
use base_prefetch::{DowseHintTableStore, DowsePrefetchContext};

let store = DowseHintTableStore::from_json_path("hints.json")?;
let token = Address::with_last_byte(6);
let caller = Address::with_last_byte(9);
let calldata = hex::decode("23b872dd0000000000000000000000000000000000000000000000000000000000001337")?;

let context = DowsePrefetchContext::new(token, &calldata, caller);
let storage_targets = store.resolve_storage_targets(&context);

store.reload_json_path("hints.json")?;
```

## Runtime Policy

`PrefetchBuffer` and `PrefetchingDb` now share execution telemetry:

- how many hints were prefetched
- how many storage reads hit prefetched values
- how many reads still fell through to the backing database
- average fallback latency for non-prefetched reads

This telemetry can feed `PrefetchRuntimePolicy`, which keeps an EWMA per
`(code_hash, selector, scenario, depth, task_class)` key and can:

- keep prefetch enabled when observed gain remains positive
- reduce hint count when only a subset of hints are consistently useful
- disable prefetch when observed benefit turns negative
- periodically run small probe batches while disabled so the system can recover if cache/miss
  conditions change

## Deeper Trees

`base-prefetch` now includes a frontier-planning layer for deeper-tree prefetching.

The intended execution model is:

- current frame exact storage reads
- likely child account/code reads
- exact child-frame storage reads once the child call context is known

The frontier API centers on:

- `PrefetchFrameContext`
- `PrefetchCallsiteContext`
- `PrefetchTask`
- `PrefetchTaskClass`
- `ChildCallPrediction`
- `FrontierPrefetchPlan`
- `FrontierPlannerRegistry`
- `PrefetchFrontierScheduler`

The first planners are aimed at common Base swap/payment paths:

- `Erc20FrontierPlanner` for `transfer`, `transferFrom`, and `balanceOf`
- `Weth9FrontierPlanner` for `deposit`, `withdraw`, and ERC-20-compatible reads
- `UniswapV2PairFrontierPlanner` for reserve-slot warming plus token child-call prediction
- `UniswapV2RouterFrontierPlanner` for path parsing, pair-address derivation, and token/pair
  account+code warming ahead of swaps

This is intentionally staged. Router-level prediction warms likely downstream contracts first;
exact child storage reads still wait for the real child call context so the planner does not
speculate on calldata it has not actually seen yet.

`PrefetchFrontierScheduler` is the queueing layer that turns planner output into executable work.
It:

- rejects stale generations and low-confidence tasks
- caps speculative depth with per-depth and per-class budgets
- suppresses storage tasks for slots already dirtied by local execution
- adapts each `(depth delta, task class)` bucket independently using `PrefetchRuntimePolicy`

## Consistent Views

For production integration, prefer building prefetch state from an existing
`StateProviderFactory` via `PrefetchStateViewFactory`:

- `Latest`
- `Pending`
- `BlockHash`
- `BlockNumberOrTag`

That keeps the prefetcher aligned with the same canonical or pending state view used by the
executor instead of issuing direct raw MDBX reads against a potentially inconsistent snapshot.

## Benchmark

Run the synthetic prefetch benchmark:

```text
cargo bench -p base-prefetch --bench prefetch_experiment
```

Override simulated storage miss latencies (microseconds, comma-separated) to match
`mdbx_state_lookup` measurements:

```text
BASE_PREFETCH_SIM_MISS_US=3,8,25 \
  cargo bench -p base-prefetch --bench prefetch_experiment
```

`prefetch_experiment` now reports both `planner=on` and `planner=off` variants so you can
quantify planner gating overhead in warm paths.

The same benchmark now also includes a scripted `universal_router_2hop` deeper-tree case that
approximates a Universal Router swap traversing Permit2, tokens, and two V2-style pairs.

Run the MDBX plain-storage lookup calibration benchmark:

```text
cargo bench -p base-prefetch --bench mdbx_state_lookup
```

Run against an existing Base node MDBX database (recommended for realistic latency):

```text
BASE_PREFETCH_MDBX_PATH=/path/to/node/datadir/db \
  cargo bench -p base-prefetch --bench mdbx_state_lookup
```

Optional tuning for real-db sampling:

```text
BASE_PREFETCH_LOOKUP_KEY_COUNT=64000
```

Note: the benchmark requires state tables (`PlainStorageState` or `HashedStorages`) to be present
in the target MDBX database. A heavily pruned DB may not include these tables.

Use the measured `mdbx_state_lookup` per-element timing as `miss_latency` input for
`PrefetchExperimentConfig`, then model partially warm state by setting
`prewarmed_storage_hints` and `prewarm_*_read` options.

For swap modeling, set `context.tx_shape = TxShape::Swap` and provide `swap_legs`. For
cost-aware gating, keep `use_prefetch_planner = true` and tune `prefetch_cost_model`.

## License

[MIT License](https://github.com/base/base/blob/main/LICENSE)
