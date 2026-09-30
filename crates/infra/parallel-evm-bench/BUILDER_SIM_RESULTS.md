# Phase 1c: shared atomic builder scheduling (experimental; performance targets NOT met)

Code: `9554d6ac3` (shared core), `c4ef3f570` (bench/reference/counters), `77f72e70b` (panic-settling fix/lints), `46c79f477` (out-of-range reader scan fix). Initial matrix timings use the recorded `c4ef3f570` binary; final five-burst and dev-repeat runs use `46c79f477`. Follow-ups preserve normal scheduling semantics.
Phase 1b diagnosis remains relevant: dev K64 reached 1.86x at 4 workers but regressed at 8; owner queue acquisition grew 32.9→74.5ms. Its full report is preserved at `74f16cda8:crates/infra/parallel-evm-bench/BUILDER_SIM_RESULTS.md`. Different host load and min-of-3 versus min-of-5 prevent causal cross-phase speedup claims.
Design: both modes use `AtomicSchedule` (per-position phases/invalidation counters/dependencies, CAS claims and frontier), `MvMemory` bitset readers, and generation notifications. Builder jobs occupy fixed-capacity append-only slots with per-slot publication locks; workers never acquire the owner metadata queue per transaction. Clean suffix feeding avoids temporary hash sets; worker-local EVMs/providers persist. Chained wakeups avoid wake-all on every append.
`take` waits, validates authentic cached-parent/Store reads and repairs stale frontier work on the owner. It does NOT apply Store state: only `on_commit`, after mandatory owner-State validation and admission, applies/advances. Skips remove writes and invalidate readers; inline commits invalidate after a fence. Cancellation and panic stop publication, and diagnostic settling waits for in-flight provider reads.
Measurement: dev22/final66; bursts grew from 3→5 during the initial run, then all five were rerun from a fixed snapshot. Forwarding on/off × prewarm 0/20ms × fault 0/.1 × K32/64/128 × workers4/8/10, rotating arms, min-of-5; every invocation bounded by `timeout 900`. Per-iteration `uptime` recorded 1-minute loads **8.26–46.50**. Prover/independent Rust builds saturated the host; low-priority single-job validation builds also overlapped some arms. These are fixture results, not production claims.
Builder times exclude prefix, setup, prewarm and shutdown. Plain ordered references use exactly `bench` timing (full canonical block), with their own sequential baseline in the same iteration. Builder workers are additional to its owner; ordered counts include the caller. These boundary differences mean the reference is not a mathematical upper bound. Faulted choices differ from the always-canonical reference: compare each arm to its own same-run sequential baseline, not faulted time to canonical time.

Initial matrix, forwarding ON, idle=0. Times sum per-block minima; triples are **4/8/10**. Original loaded-host burst rows are retained below, not replaced by more favorable reruns.
|set|K|fault|seq ms|sim ms 4/8/10|sim x 4/8/10|ordered x 4/8/10|
|---|---:|---:|---:|---|---|---|
|dev|32|0|414.9|287.4/361.9/331.6|1.44/1.15/1.25|1.313/1.785/1.784|
|dev|64|0|414.9|247.0/311.1/325.2|1.68/1.33/1.28|1.313/1.785/1.784|
|dev|128|0|414.9|242.2/277.0/289.6|1.71/1.50/1.43|1.313/1.785/1.784|
|dev|32|.1|356.8|312.6/390.8/373.1|1.14/.91/.96|1.417/1.900/1.976|
|dev|64|.1|356.8|326.4/422.6/467.9|1.09/.84/.76|1.417/1.900/1.976|
|dev|128|.1|356.8|461.7/628.4/623.1|.77/.57/.57|1.417/1.900/1.976|
|final|32|0|1137.3|844.0/999.8/1044.5|1.35/1.14/1.09|1.286/1.721/1.690|
|final|64|0|1137.3|745.7/877.0/838.5|1.53/1.30/1.36|1.286/1.721/1.690|
|final|128|0|1137.3|679.6/772.2/771.5|1.67/1.47/1.47|1.286/1.721/1.690|
|final|32|.1|986.7|934.1/1065.9/1057.9|1.06/.93/.93|1.337/1.816/1.749|
|final|64|.1|986.7|892.3/1099.6/1206.1|1.11/.90/.82|1.337/1.816/1.749|
|final|128|.1|986.7|1168.1/1327.8/1439.3|.84/.74/.69|1.337/1.816/1.749|
|50700012|32|0|71.8|50.1/75.9/77.8|1.43/.95/.92|1.449/1.692/1.788|
|50700012|64|0|71.8|53.9/66.5/67.0|1.33/1.08/1.07|1.449/1.692/1.788|
|50700012|128|0|71.8|56.2/41.7/41.0|1.28/1.72/1.75|1.449/1.692/1.788|
|50700012|32|.1|63.5|67.0/102.5/80.6|.95/.62/.79|1.353/1.625/1.748|
|50700012|64|.1|63.5|72.0/86.6/108.7|.88/.73/.58|1.353/1.625/1.748|
|50700012|128|.1|63.5|76.5/121.2/120.8|.83/.52/.53|1.353/1.625/1.748|
|51701002|32|0|44.0|21.8/29.0/45.5|2.01/1.52/.97|1.536/2.408/2.576|
|51701002|64|0|44.0|20.5/32.2/36.0|2.15/1.37/1.22|1.536/2.408/2.576|
|51701002|128|0|44.0|19.4/19.1/34.1|2.27/2.31/1.29|1.536/2.408/2.576|
|51701002|32|.1|43.9|29.6/51.2/44.3|1.48/.86/.99|1.644/2.381/2.328|
|51701002|64|.1|43.9|23.5/42.3/53.5|1.86/1.04/.82|1.644/2.381/2.328|
|51701002|128|.1|43.9|31.8/45.4/72.0|1.38/.97/.61|1.644/2.381/2.328|
|51961520|32|0|95.9|75.8/96.2/95.0|1.26/1.00/1.01|1.419/1.890/1.851|
|51961520|64|0|95.9|63.0/67.8/78.5|1.52/1.41/1.22|1.419/1.890/1.851|
|51961520|128|0|95.9|46.9/56.2/79.1|2.04/1.71/1.21|1.419/1.890/1.851|
|51961520|32|.1|72.8|73.9/68.7/77.9|.98/1.06/.93|1.479/1.850/1.727|
|51961520|64|.1|72.8|69.0/85.1/84.2|1.06/.86/.86|1.479/1.850/1.727|
|51961520|128|.1|72.8|77.9/69.7/70.3|.93/1.04/1.04|1.479/1.850/1.727|

Other initial-matrix arms, K32; triples are speedups **4/8/10**. Remaining K/individual-burst rows are in the raw logs.
|forward/idle/fault|dev sim x|dev ordered x|final sim x|final ordered x|
|---|---|---|---|---|
|true/20/0|1.42/1.22/1.21|1.417/1.873/1.876|1.30/1.08/1.12|1.215/1.536/1.588|
|true/20/.1|1.03/.84/.94|1.385/1.630/1.770|1.12/1.04/.99|1.366/1.783/1.837|
|false/0/0|.94/.81/.82|1.257/1.741/1.778|.81/.79/.74|1.169/1.562/1.644|
|false/0/.1|.72/.73/.70|1.290/1.662/1.762|.81/.68/.71|1.348/1.639/1.728|
|false/20/0|.92/.78/.80|1.439/2.152/2.055|.89/.76/.77|1.419/2.056/2.064|
|false/20/.1|.89/.84/.85|1.242/1.603/1.694|.89/.83/.80|1.393/1.980/1.968|

Final consistent five-burst snapshot, forwarding ON/idle0: clean K128 and faulted K32 shown; all K/forwarding/prewarm/fault combinations ran. Loads 8.52–41.66. Each burst is individual.
|block|K|fault|seq ms|sim ms 4/8/10|sim x 4/8/10|ordered x 4/8/10|
|---|---:|---:|---:|---|---|---|
|50700012|128|0|67.5|47.8/29.4/32.8|1.41/2.29/2.06|1.475/2.048/2.067|
|50700012|32|.1|60.9|44.7/47.8/52.1|1.36/1.27/1.17|1.461/2.098/2.204|
|50700053|128|0|66.8|59.1/59.1/48.3|1.13/1.13/1.38|1.176/1.327/1.327|
|50700053|32|.1|37.7|38.6/36.7/42.1|.98/1.03/.90|1.198/1.337/1.355|
|51701002|128|0|41.5|17.4/12.9/16.3|2.38/3.21/2.54|1.562/2.588/2.942|
|51701002|32|.1|37.8|20.2/22.9/23.8|1.87/1.65/1.59|1.624/2.688/2.900|
|51961520|128|0|85.2|44.6/47.2/49.2|1.91/1.81/1.73|1.430/2.226/2.373|
|51961520|32|.1|70.3|56.3/57.7/60.1|1.25/1.22/1.17|1.417/2.121/2.320|
|51961575|128|0|71.4|45.6/42.2/48.1|1.57/1.69/1.48|1.443/1.682/1.789|
|51961575|32|.1|47.3|34.2/34.9/36.8|1.38/1.35/1.29|1.413/1.955/2.019|

Owner cost, dev clean K128, microseconds per considered choice (5309 choices), **4/8/10**: State validation **3.84/3.81/3.75**, rebase **2.78/2.63/3.33**, Store validation **4.82/5.44/4.14**, exact repair **.70/.37/.61**, normal commit/apply **13.41/19.74/18.38**, submit **.65/2.32/2.86**. Validation makes ~57.7 owner-DB lookups plus ~32.4 Store observation checks per choice. AccountInfo/bytecode clone costs remain inside these phases.
Total `take` is 18.98/17.74/19.86us, INCLUDING Store checks, repair and frontier waits; do not add its subphases twice. Envelope clone cost is only .070/.077/.129us per choice, including untimed initial-plan clones (an upper bound on timed-loop cloning); results move, never clone. Legacy queue-wait columns are zero placeholders, not measurements. Owner Store/State work and apply/notification contention remain even after removing the shared work queue.
Final-source dev K128 clean repeat at load **12.20–13.25**: seq387.4ms → sim169.7/161.4/174.2ms = **2.28/2.40/2.22x**, versus ordered **1.457/2.309/2.385x** in that run. It meets the clean/no-4→8-regression goals in this smaller arm sweep; it does not replace the loaded full-matrix rows or establish an isolated causal gain.
Targets: overall NOT met. Clean K128 gets within 15% of the ordered reference in several arms, but dev8 misses narrowly and many 10-worker/small-window arms miss substantially. Dev/final clean regress 14% from 4→8 at K128; 50700012/51701002 improve at that window. Faulted dev K128/8 is 76% slower than sequential, far outside 5%; even K32/8 dev/final exceed 5%. Larger windows increase invalidated/abandoned work (dev fault K128 waste 8976/16929/19791), and owner repair plus ordinary inline fallback adds serial work. Loaded-host variance is material, not an excuse to claim the targets passed.
Burst metadata (tx/Mgas): 50700012=779/132.120; 50700053=444/109.200; 51701002=515/91.054; 51961520=935/172.289; 51961575=704/140.948. Snapshot cutoff: 2026-09-30 02:00 UTC; hashes are recorded alongside the final burst logs.
Validation: all required cohorts (dev22/final66/final-burst5) passed **216 aggregate arms / 33,480 builder block-iterations / 11,160 ordered references**, checking included order/set, full receipts and bundle/reverts on every builder iteration, including faults. Original changing-burst runs also passed. Final binary legacy 1/2/4/8/10 and via-executor 1/4/8 passed all cohorts; nonblocking dev/burst K32/128, 4/8, both faults/prewarm settings passed. **144 library tests**, 20×12 speculator tests, bench tests (zero cases), no-default-features, watchdog compile and fmt passed. Three red-first regressions: frontier nonce, panic settling, maximum reader index. Clippy has only the pre-existing parallel_payload.rs warning; one pre-existing doctest is ignored.
Reproduce: `CARGO_TARGET_DIR=/Users/brianbland/code/scratch/target-spec3 cargo build --release -p base-parallel-evm-bench`; run `bash crates/infra/parallel-evm-bench/phase1c.sh` with a fresh log directory (the script now snapshots fixtures first). Raw initial rows/load/references: `/tmp/spec1c-final/{dev,final,burst}-{true,false}-{0,20}-{0,0.1}.log`; final five-burst matrix: `/tmp/spec1c-burst-final/`; final-source gates/dev repeat: `/tmp/spec1c-final-validation/`. `environment.txt` files record revision/binary SHA256; final burst metadata includes fixture hashes.
Phase 2: do NOT wire production yet. Preserve selection, predicates, gas/DA accounting and commit-condition callbacks. Submit an independent pool snapshot, append visible successors, retire declined proposals, publish every admitted inline/system/deposit/fee change, and cancel on parent replacement/deadline. Factories need authentic immutable readers, including rare owner repairs; add fallible-provider poisoning and bounded I/O. Keep independent owner validation, environment/parent identity and L1-cache guards. Measure real I/O, memory and deadline tails before wiring flashblocks or raising gas limits.
Remaining risks/cleanup candidates: fixed-capacity rollover still discards advisory work; provider I/O cannot be interrupted; parent-read caches and retained retired slots need memory measurement. Reader-list/readset allocations and repeated Store/State metadata lookups remain possible measured follow-ups. The pre-existing unrelated Clippy warning was not changed.
