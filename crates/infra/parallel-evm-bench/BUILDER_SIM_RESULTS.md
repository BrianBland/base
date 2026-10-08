# Phase 1d: fault outcomes and stable duplicate predictions (experimental; strict fault cap NOT met)

Outcome: every main-matrix cohort/fault/K/thread arm beats its same-run sequential loop, but some individual blocks still exceed the 5% slowdown budget. No production builder wiring or adaptive K was added.
Commits: `2b9251c52` fault outcomes/dedup/counters; `5cde1c2f0` owner lookups; `5b9049f91` samples/load-gated script; `46a1afe3e` equivalent error-conversion/format cleanup; `714589695` ablation script/docs. Main/after-ablation binary is `5b9049f91`; final-source correctness gates also cover the later cleanup. Earlier Phase 1/1b/1c reports remain in Git (`2c398d662`/`74f16cda8`/`311544766`).
Method: frozen fixtures, forwarding ON, prewarm0, wait10ms, K32/128 × 4/8/10, clean/.1 faults, interleaved min-of-5; every invocation bounded by `timeout 900`. Times exclude prefix/setup/prewarm/shutdown. Ordered references execute canonical full blocks and have their own same-run sequential baseline; builder workers exclude the owner, ordered threads include it. Ratios are context, not identical-workload comparisons or production claims.
Approved budget concession: load gate ONCE per cohort invocation, <6, polling30s, cap300s; no internal per-arm re-gating. All six main gates reached <6 (0–242s waits); recorded invocation/iteration loads **4.46–8.39**. This is not the originally requested per-arm/20-minute gate. Raw iteration samples, start/end loads and fixture/binary hashes are archived.
Snapshot at 2026-09-30 02:25 UTC captured **8 bursts**: the requested7 plus newly arrived51961418. Both cohorts are reported. Mgas by block: 50699993=119.541,50700012=132.120,50700053=109.200,51700821=87.406,51701002=91.054,51961418=123.038,51961520=172.289,51961575=140.948.
Diagnosis: builder workers inherited `assume_nonce`, so invalid high nonces could execute contract bodies and publish fictitious writes; EVM errors were discarded and re-executed inline. Workers now read the real visible nonce and preserve transaction errors with readsets, exact error balances, no writes and no fees. Both Store and independent owner-State validation remain mandatory, including errors; stale errors retry instead of skipping now-valid transactions.
The larger measured cascade was **duplicate synthetic hashes**: nonce replacement collapses envelopes, while submit compared duplicate-filled snapshots with unique slots and repeatedly discarded entire plans. One instrumented pre-dedup block (51770970) had119 generations/118 replans. Ordered unique-first reconciliation retains completed work; final dev fault K128 has26 generations/4 replans across22 blocks, at every thread count.
Diagnostic pre-dedup dev fault K128 waste was7439/15554 at4/8; final min-of-5 waste is1181/1785/2185 at4/8/10. Final invalid outcomes796/860/919 include512 consumed errors at each thread count; removal invalidations=0, retired writers=0, owner validation failures=0, invalid owner repairs=0, timeouts=0. Store frontier repairs=19/27/20; ESTIMATE blocks=308/491/610. Consumed/hit counts now include validated errors, NOT just admissions. Diagnostic one-iteration runs are not causal performance comparisons.
Residual diagnosis: burst50700053 fault K128/4 spends10ms waiting then13.2ms executing inline (one timeout), producing8.6% slowdown. Its K32/8 result spends19.8ms in commit and17.0ms in submit, with zero timeouts, yielding33.6% slowdown. Invalid-slot cascades are gone there; long-execution fallback and owner/notification/feeding costs remain. Some final66 blocks also exceed5% (worst33.1% among these faulted arms).

Owner ablation: alternate before(`2b9251c52`)/after invocations five times, choose per-block minima; dev K128,5309 considered choices. Gate capped after302s (poll overhead); loads21.14–23.69, so small timing changes are directional, not isolated production gains. Units below are µs/choice; triples are4/8/10.
|owner phase|before|after|
|---|---|---|
|State validation|2.52/2.66/2.72|2.15/2.35/2.42|
|Store validation|2.88/3.11/3.32|2.51/2.80/3.08|
|rebase|1.73/1.89/1.93|1.57/1.79/1.85|
|normal commit/apply|7.30/10.00/10.62|6.94/9.35/10.65|
|sum of these serial phases|14.43/17.66/18.59|13.18/16.29/18.00|
Owner validation lookups fall57.7→37.1/choice (adjacent slots reuse loaded accounts); Store checks borrow metadata rather than clone it; frontier retirement pops rather than scans K. No Store/State mirror assumption or O(1) validation bypass. Ablation loop totals170.3/168.5/171.3→167.6/161.3/172.0ms: no uniform total-speed claim. Historical Phase1c serial sums24.9/31.6/29.6µs versus main-run14.2/17.6/18.3µs are host-confounded; the paired ablation above is the relevant comparison. Bulk Store batching/off-owner bookkeeping remain undone.

Main results: seq ms sums per-block minima; speedup triples are **4/8/10**. `burst7` excludes51961418; `burst8` includes it. Every burst is shown individually, including unfavorable rows.
|set/block|fault|seq ms|K32 builder x|K128 builder x|ordered reference x|
|---|---:|---:|---|---|---|
|dev22|0|385.1|1.96/1.72/1.65|2.34/2.40/2.31|1.48/2.26/2.33|
|dev22|.1|341.5|1.84/1.60/1.56|1.92/1.82/1.73|1.47/2.36/2.53|
|final66|0|1040.8|1.85/1.66/1.59|2.18/2.36/2.22|1.45/2.28/2.43|
|final66|.1|909.1|1.62/1.44/1.38|1.87/1.68/1.68|1.44/2.26/2.39|
|burst7|0|445.4|1.29/1.30/1.17|1.59/1.81/1.65|1.38/1.84/1.93|
|burst7|.1|333.9|1.49/1.21/1.18|1.28/1.32/1.29|1.39/1.80/1.89|
|burst8|0|493.1|1.31/1.30/1.18|1.60/1.79/1.65|1.40/1.90/1.97|
|burst8|.1|375.8|1.46/1.19/1.18|1.30/1.33/1.29|1.41/1.86/1.95|
|50699993|0|68.8|0.98/1.15/0.93|1.28/1.27/1.22|1.26/1.46/1.45|
|50699993|.1|41.2|1.10/1.00/0.97|1.04/1.01/1.33|1.30/1.47/1.54|
|50700012|0|68.5|1.31/1.41/1.16|1.42/2.31/2.12|1.47/1.98/2.21|
|50700012|.1|61.0|1.31/1.14/1.04|1.20/1.40/1.15|1.51/2.02/1.95|
|50700053|0|67.0|0.80/0.77/0.74|1.13/1.40/1.13|1.17/1.30/1.31|
|50700053|.1|39.0|1.46/0.75/0.84|0.92/1.43/1.47|1.15/1.24/1.29|
|51700821|0|39.8|2.24/1.94/1.85|2.41/2.87/2.65|1.41/2.56/2.88|
|51700821|.1|34.8|2.06/1.68/1.35|1.93/1.57/1.51|1.43/2.59/2.88|
|51701002|0|41.8|2.20/2.18/1.94|2.42/3.38/2.75|1.57/2.65/3.01|
|51701002|.1|38.4|2.11/1.62/1.56|1.97/1.25/1.69|1.58/2.58/2.74|
|51961418 (new)|0|47.8|1.50/1.35/1.28|1.68/1.58/1.66|1.59/2.74/2.52|
|51961418 (new)|.1|41.9|1.24/1.10/1.17|1.41/1.45/1.27|1.63/2.69/2.65|
|51961520|0|86.8|1.63/1.52/1.44|2.18/1.91/1.76|1.47/2.20/2.39|
|51961520|.1|70.8|1.62/1.36/1.39|1.32/1.30/1.27|1.43/2.17/2.38|
|51961575|0|72.7|1.50/1.42/1.38|1.65/1.71/1.70|1.44/1.87/1.95|
|51961575|.1|48.7|1.43/1.45/1.44|1.35/1.41/1.05|1.44/1.76/1.93|

Burst K128 block-minimum p99 (not production latency tails): clean seq86.8ms→sim59.4/54.1/59.3; fault seq70.8→53.7/54.5/55.8. Faulted cohorts all improve, and clean K128 improves4→8 across all cohorts. The strict per-block fault cap and a uniform owner/10-thread speedup remain **unmet**; no adaptive policy or strict internal per-arm load gate was measured.
Correctness: main matrix **5760 builder block-iterations +2880 ordered references**, checking included order/set, full receipts and bundle/reverts each time. Paired ablation adds660 builder iterations. Legacy1/2/4/8/10 and via-executor1/4/8 pass all96 blocks. Final-source forwarding-OFF/nonblocking K32/128,4/8,clean/fault adds768 builder iterations, all equal (performance not claimed). 148 library tests +20×16 speculator repeats, bench tests(0), no-default-features, watchdog compile, fmt and clippy pass; clippy retains only the pre-existing parallel_payload.rs warning, and one pre-existing doctest is ignored. Three observed red-first regressions: retained invalid nonce, no invalid-slot writes, duplicate snapshot reuse. Supplemental tests cover nonce/balance/fee errors at Bedrock/Jovian and executor skip without inline re-execution.
Reproduce: `CARGO_TARGET_DIR=/Users/brianbland/code/scratch/target-spec4 cargo build --release -p base-parallel-evm-bench`; `MODE=full WAIT_SECS=300 ITERS=5 bash crates/infra/parallel-evm-bench/measure-builder-sim.sh`. `measure-owner-cost.sh BEFORE_BINARY AFTER_BINARY FROZEN_DEV_FIXTURES LOG_DIR` reproduces the paired ablation; build BEFORE_BINARY from2b9251c52 separately. The original full 20-minute per-arm gating requirement was NOT run; all requested fixed K/thread/cohort/fault arms were measured.
Raw committed artifacts: `results/phase1d-{main,owner,validation}.tar.gz` (extract with `tar -xzf`); `results/phase1d-manifest.txt` records commands/source boundaries. Local originals: `/tmp/spec1d-measured`, `/tmp/spec1d-owner`, `/tmp/spec1d-logs`. Frozen fixture contents stay local; hashes are archived. Main binary and final formatting-equivalent validation binary hashes are distinct and recorded.
Risks/cleanup candidates: provider I/O remains non-interruptible; generation rollover and retained parent/slot caches still need memory/deadline measurement. Heavy-transaction wait expiry can duplicate execution; commit/notification and filtered-feeder costs still dominate bad arms. Error balances are deliberately conservative exact observations. Experimental `SpeculativeResult.output` is now a Result, so external users must adapt. Fix the unrelated existing Clippy warning separately. These fixture-loop results do not authorize production wiring, gas-limit changes or weakened owner validation.
