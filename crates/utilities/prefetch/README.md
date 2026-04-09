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
- `PrefetchPlanner`: Cost-aware planner that can skip prefetching when projected gain is negative.

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
