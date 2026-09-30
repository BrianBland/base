# Phase 1b: ordered builder speculation (experimental; performance targets NOT met)

Interleaved min-of-3, owner plus 4/8/10 workers, K=32/64/128; complete forwarding × idle(0/20ms) × faults(0/.1) matrix on dev22, final66 and three bursts. Every measured iteration matched included order/set, full receipts and bundle/reverts. All 216 aggregate arms passed. Times exclude prefix, prewarm, setup and shutdown; these are shared-host directional measurements, not production claims.
Final/burst measurements encountered concurrent RocksDB builds and a zk prover. One aggregate 900s command timed out after dev/final; bursts were rerun separately, each command bounded by 900s. Heavy-load results below are retained, not replaced by earlier favorable samples.
Baseline diagnosis (instrumented Phase 1, dev clean): K32/4 had 3745 hits, 1092 validation failures and 450 not-ready misses; K128/4 had 3367 hits, 1811 failures and only 109 not-ready misses, out of 5309 choices. Nonblocking misses matter, but stale work is the larger loss. Rolling Phase 1 batches also disconnected newly appended work from retained MvMemory.
Design: stable bounded plans, changed-write forwarding, reader invalidation, ESTIMATE blocking, worker frontier retries, worker-local reusable EVMs/providers. Builder alone retires/commits; skipped writes are removed and unrelated predictions retained. This adapts engine primitives, NOT the immutable auto-committing Scheduler itself. Consume-time owner-State validation/rebase remains mandatory; Store equivalence is not assumed as a soundness shortcut.

Forwarding ON below. Triples are workers **4/8/10**; `P` is fault fraction; `idle` is ms. Hits are out of dev=5309, final=14175, bursts=778/513/934 choices. `fail` counts owner validation attempts, including faults; `waste` is settled execution attempts minus consumed. Totals sum rounded per-block CSV minima.
|set|K|idle|P|seq ms|sim ms 4/8/10|hits 4/8/10|fail 4/8/10|waste 4/8/10|
|---|---:|---:|---:|---:|---|---|---|---|
|dev|32|0|0|392.354|226.584/258.549/269.186|5287/5287/5287|0/0/0|671/863/807|
|dev|64|0|0|392.354|210.836/250.627/265.836|5287/5287/5287|0/1/0|687/917/899|
|dev|128|0|0|392.354|221.284/263.836/277.290|5286/5287/5287|0/1/0|712/1138/1216|
|dev|32|0|0.1|340.286|260.242/313.782/323.856|4549/4549/4549|500/502/502|2193/2646/2810|
|dev|64|0|0.1|340.286|301.445/359.839/381.692|4549/4548/4548|471/473/474|3990/6117/7190|
|dev|128|0|0.1|340.286|381.954/528.216/565.588|4550/4549/4550|426/426/428|7222/14116/17423|
|dev|32|20|0|391.525|213.070/248.613/252.688|5287/5287/5287|0/1/1|646/842/762|
|dev|32|20|0.1|343.329|266.818/283.978/287.760|4549/4549/4549|504/503/505|2240/2636/2871|
|final|32|0|0|1132.803|1098.823/1135.113/1172.194|14108/14108/14107|1/1/2|1379/1513/1639|
|final|64|0|0|1132.803|1001.291/1108.394/1152.625|14108/14109/14108|2/3/3|1489/1788/1823|
|final|128|0|0|1132.803|1048.891/1200.936/1242.771|14109/14109/14109|0/3/1|1630/2360/2653|
|final|32|0|0.1|1033.596|1265.303/1318.495/1318.557|11952/11951/11951|1302/1303/1303|5381/6200/6446|
|final|64|0|0.1|1033.596|1346.029/1636.176/1675.282|11952/11952/11951|1259/1271/1264|9701/15919/18410|
|final|128|0|0.1|1033.596|1668.862/2475.916/2738.379|11951/11952/11951|1180/1185/1182|17796/36287/47847|
|final|32|20|0|1097.698|809.398/987.267/993.264|14109/14109/14109|4/3/3|1416/1480/1481|
|final|32|20|0.1|1084.633|1624.529/1785.439/1657.426|11951/11952/11952|1310/1306/1307|5596/6317/7390|
|50700012|32|0|0|110.442|86.162/126.386/78.050|777/777/777|0/0/0|246/200/257|
|50700012|64|0|0|110.442|93.447/97.480/152.190|777/777/776|0/0/0|209/265/287|
|50700012|128|0|0|110.442|111.179/86.253/110.926|777/777/777|0/0/0|192/261/282|
|50700012|32|0|0.1|68.737|125.768/124.472/162.963|693/693/693|67/67/66|642/703/1057|
|50700012|64|0|0.1|68.737|128.495/140.827/164.567|693/693/693|64/65/63|844/1406/2069|
|50700012|128|0|0.1|68.737|208.191/278.314/275.860|693/693/693|60/59/59|2075/3412/3873|
|50700012|32|20|0|77.497|99.132/113.599/85.390|777/776/777|0/0/0|223/263/178|
|50700012|32|20|0.1|83.863|135.378/141.012/149.002|691/693/692|66/67/66|587/1160/1253|
|51701002|32|0|0|64.716|47.959/82.575/61.117|512/512/512|0/0/0|83/121/87|
|51701002|64|0|0|64.716|57.025/53.085/59.629|512/512/512|0/0/0|72/94/114|
|51701002|128|0|0|64.716|53.880/62.593/67.495|512/512/512|0/0/0|111/147/176|
|51701002|32|0|0.1|39.170|61.895/55.411/69.052|447/447/447|50/50/50|263/273/293|
|51701002|64|0|0.1|39.170|59.787/110.783/90.338|447/447/447|50/50/50|335/368/384|
|51701002|128|0|0.1|39.170|104.498/116.412/113.592|447/447/447|50/50/50|598/739/672|
|51701002|32|20|0|87.230|47.892/42.418/76.632|512/512/512|0/0/0|85/118/135|
|51701002|32|20|0.1|57.444|62.563/60.942/66.572|447/447/447|50/50/50|291/290/293|
|51961520|32|0|0|93.963|101.083/134.675/111.364|933/933/933|0/0/0|141/176/150|
|51961520|64|0|0|93.963|88.585/124.912/136.701|933/933/933|0/0/0|163/279/271|
|51961520|128|0|0|93.963|115.497/135.549/130.791|933/933/933|0/0/0|161/271/442|
|51961520|32|0|0.1|76.930|109.039/170.516/119.241|777/776/777|105/105/105|485/479/520|
|51961520|64|0|0.1|76.930|109.597/140.109/163.529|777/777/777|104/105/105|664/733/750|
|51961520|128|0|0.1|76.930|162.158/198.483/200.814|777/777/776|105/105/105|869/1079/1284|
|51961520|32|20|0|175.798|101.357/117.122/187.567|933/933/933|0/0/0|143/174/176|
|51961520|32|20|0.1|94.350|101.167/128.838/146.187|777/776/777|104/105/105|502/489/493|

Forwarding OFF, K32/idle0 (same 4/8/10 ordering); larger-K/prewarm arms are in the raw logs.
|set|P|seq ms|sim ms 4/8/10|
|---|---:|---:|---|
|dev|0|391.608|347.569/369.999/382.214|
|dev|.1|339.025|333.177/352.562/352.082|
|final|0|1037.440|953.097/1027.084/1001.361|
|final|.1|905.258|861.402/950.036/944.043|
|burst3|0|215.233|392.385/438.407/386.385|
|burst3|.1|192.315|324.395/368.927/377.618|

Dev clean K32/4 p50 sequential/sim=16.442/8.375ms; p99=36.157/18.753ms; hit=5287/5309 (the 22 first ordinary transactions intentionally stay inline). K64/4 reaches 1.86x, but K64/8 only 1.57x: the ~3x target and no-thread-regression target fail. Large-K fault arms greatly exceed the 5% slowdown budget even without host saturation.
Burst metadata: 50700012=779 tx/132.120 Mgas; 51701002=515 tx/91.054 Mgas; 51961520=935 tx/172.289 Mgas. An earlier lighter-host full-matrix pass measured K32/4 clean at 66.762→41.110, 41.913→19.328 and 85.695→54.093ms respectively; final K128/4 was 1030.179→567.100ms. These are context, not replacements for the final loaded-host rows.
Bounded-wait comparison: dev K32/4, forwarding on, idle0, nonblocking `--frontier-wait-ms 0` measured 393.180→286.156ms, 5116 hits; default 10ms wait measured 392.353→226.587ms, 5287 hits. Separate interleaved runs, not an isolated causal estimate. Both settings also passed faults/prewarm on dev and bursts (K32/128, 4/8).
Owner bottleneck diagnostic (lighter-host dev K128, 4/8): loop208.8/263.6ms; take52.6/35.9; validation+rebase25.4/29.4; inline1.5/1.6 (22 tx); commit53.2/74.7; submit23.4/29.8. Queue acquisition wait owner32.9/74.5, workers85.8/212.9ms; worker busy540/617, CV idle151/1161ms; waste712/1095. Queue waits overlap phases; worker totals include untimed prewarm/settling. Owner non-take time already caps scaling; removing validation alone cannot reach 3x.
Quick 5s `sample` at 8/K128: top-of-stack CV wait26318, mutexwait2164, mutexdrop300, CV signal233 samples. The release binary is stripped, so Rust-frame attribution is unavailable. `/tmp/spec1b-sample.txt` and `/tmp/spec1b-profile-counters.log` retain evidence. Separate work/result condition variables, chained wakeups, O(1) reader lookup and EVM reuse helped; per-slot publication/retirement locks and a lighter incremental feeder are the next remedies.
Validation: 141 library tests passed; added tests failed first for bounded pending take, inline-prefix repair, cancelled-work accounting and invalid-code terminal handling. Twenty repetitions of all nine speculator tests passed. Legacy dev/final/burst passed 1/2/4/8/10; via-executor passed 1/4/8. No-default-features passed; Clippy has only the pre-existing parallel_payload.rs manual-is-multiple-of warning. One pre-existing doctest is ignored.
Reproduce: `CARGO_TARGET_DIR=/Users/brianbland/code/scratch/target-spec2 cargo build --release -p base-parallel-evm-bench`; wrap every README builder-sim invocation in `timeout 900`, crossing forwarding=false/true, idle=0/20 and inject-invalid=0/.1 with threads=4,8,10, K=32,64,128, iters=3 on all three fixture directories. Raw per-block and aggregate logs: `/tmp/spec1b-{dev,final,burst}-{false,true}-{0,20}-{0,0.1}.log`; nonblocking comparison: `/tmp/spec1b-nowait-{dev,burst}-{0,20}-{0,0.1}.log`.
Phase 2: do NOT wire production yet. Preserve fair selection, predicates, gas/DA accounting and commit-condition callbacks; feed an independent pool snapshot, append newly visible lane successors, publish every system/deposit/inline/fee change, and cancel on parent replacement/deadline. Add fallible-provider error poisoning and bounded worker-local providers. Keep owner validation, immutable parent/environment identity and L1-cache safeguards; measure real I/O, memory and deadline tails. No flashblocks wiring and no gas-limit increase justified.
