# Phase 1 offline builder speculation measurements

Shared-host directional measurements, min-of-3 with sequential/K/thread arms interleaved; no production speedup claim.
Loop time excludes system/deposit prefix, provider/pool setup, idle prewarm and shutdown. Workers are additional to the owner.
All rows passed included-hash order, full receipt and full bundle/revert equality on every iteration.
`fwd` is batch-local forwarding; `idle` is milliseconds; `P` is injected invalid fraction. Times are milliseconds.
`hit` is consumed/considered; `fail` is consume validation failures; `waste` is executions minus consumed after settling.
`p50`/`p99` columns show sequential/simulator block percentiles. Bursts currently contain only block 51701002 (515 txs, ~91 Mgas).

|set|K/t|fwd|idle|P|seq total|sim total|p50 seq/sim|p99 seq/sim|hit|fail|waste|
|---|---|---|---:|---:|---:|---:|---|---|---|---:|---:|
|dev22|32/4|off|0|0|392.269|299.674|16.655/11.576|36.698/29.178|3943/5309|1263|1366|
|dev22|32/8|off|0|0|392.269|366.111|16.655/13.530|36.698/38.502|3925/5309|1298|1384|
|dev22|128/4|off|0|0|392.269|346.320|16.655/12.261|36.698/36.002|3423/5309|1815|1886|
|dev22|128/8|off|0|0|392.269|411.996|16.655/15.070|36.698/45.013|3328/5309|1927|1980|
|dev22|32/4|off|20|0|387.262|275.070|15.760/10.435|36.557/32.239|3972/5309|1288|1337|
|dev22|32/8|off|20|0|387.262|380.219|15.760/14.209|36.557/36.997|3955/5309|1318|1354|
|dev22|128/4|off|20|0|387.262|367.076|15.760/14.502|36.557/44.716|3251/5309|2036|2058|
|dev22|128/8|off|20|0|387.262|454.590|15.760/18.054|36.557/57.752|3251/5309|2036|2058|
|dev22|32/4|on|0|0|396.573|350.866|16.587/12.771|36.243/35.381|3966/5309|1234|1343|
|dev22|32/8|on|0|0|396.573|494.101|16.587/17.810|36.243/62.412|3935/5309|1284|1374|
|dev22|128/4|on|0|0|396.573|397.887|16.587/12.561|36.243/58.805|3827/5309|1400|1481|
|dev22|128/8|on|0|0|396.573|480.485|16.587/17.949|36.243/67.825|3705/5309|1546|1604|
|dev22|32/4|on|20|0|396.876|306.199|16.710/10.973|36.897/34.197|4032/5309|1220|1277|
|dev22|32/8|on|20|0|396.876|386.289|16.710/15.440|36.897/35.241|3980/5309|1291|1329|
|dev22|128/4|on|20|0|396.876|327.680|16.710/10.763|36.897/37.228|3791/5309|1496|1518|
|dev22|128/8|on|20|0|396.876|415.472|16.710/14.711|36.897/42.013|3708/5309|1579|1601|
|dev22|32/4|off|0|.1|345.207|333.916|14.066/14.446|32.270/30.299|2732/5309|841|8884|
|dev22|32/8|off|0|.1|345.207|390.867|14.066/17.548|32.270/34.871|2907/5309|906|12270|
|dev22|128/4|off|0|.1|345.207|357.869|14.066/15.576|32.270/32.593|2765/5309|827|10715|
|dev22|128/8|off|0|.1|345.207|414.626|14.066/17.685|32.270/39.248|2940/5309|912|19821|
|dev22|32/4|off|20|.1|343.582|341.811|14.565/13.448|32.814/32.306|2781/5309|849|9302|
|dev22|32/8|off|20|.1|343.582|411.509|14.565/15.189|32.814/36.219|2959/5309|923|12698|
|dev22|128/4|off|20|.1|343.582|359.599|14.565/14.214|32.814/31.464|2857/5309|862|13505|
|dev22|128/8|off|20|.1|343.582|415.545|14.565/15.487|32.814/33.807|2959/5309|925|21393|
|dev22|32/4|on|0|.1|343.097|327.589|14.112/13.054|33.124/30.776|2835/5309|641|8081|
|dev22|32/8|on|0|.1|343.097|394.892|14.112/15.528|33.124/36.787|2947/5309|750|11963|
|dev22|128/4|on|0|.1|343.097|350.683|14.112/13.947|33.124/33.211|2964/5309|625|9409|
|dev22|128/8|on|0|.1|343.097|406.855|14.112/15.329|33.124/40.952|3014/5309|731|17126|
|dev22|32/4|on|20|.1|368.015|379.672|15.717/14.839|33.231/36.827|2744/5309|606|7936|
|dev22|32/8|on|20|.1|368.015|436.320|15.717/17.274|33.231/37.599|2798/5309|701|10614|
|dev22|128/4|on|20|.1|368.015|437.170|15.717/16.721|33.231/45.191|2794/5309|613|11411|
|dev22|128/8|on|20|.1|368.015|477.994|15.717/17.084|33.231/67.966|2794/5309|688|17366|
|burst1|32/4|off|0|0|42.874|38.643|42.874/38.643|42.874/38.643|283/513|225|230|
|burst1|32/4|off|20|0|42.189|35.662|42.189/35.662|42.189/35.662|283/513|227|230|
|burst1|32/4|on|0|0|43.201|37.852|43.201/37.852|43.201/37.852|284/513|224|229|
|burst1|32/4|on|20|0|42.554|36.329|42.554/36.329|42.554/36.329|284/513|226|229|
|burst1|32/4|off|0|.1|38.765|32.243|38.765/32.243|38.765/32.243|211/513|117|858|
|burst1|32/4|off|20|.1|37.492|32.472|37.492/32.472|37.492/32.472|220/513|119|915|
|burst1|32/4|on|0|.1|37.871|35.519|37.871/35.519|37.871/35.519|225/513|99|793|
|burst1|32/4|on|20|.1|37.465|35.054|37.465/35.054|37.465/35.054|223/513|103|833|
|final66|32/4|off|0|0|1049.949|807.041|14.777/11.158|36.502/30.243|10807/14175|3068|3368|
|final66|128/8|off|0|0|1049.949|1158.335|14.777/15.026|36.502/44.019|9369/14175|4623|4806|
|final66|32/4|on|20|.1|922.279|931.039|12.703/12.530|34.499/45.223|7342/14175|1543|21629|
|final66|128/8|on|20|.1|922.279|1177.798|12.703/16.227|34.499/56.352|7780/14175|1769|47694|

Deterministic admissions: dev 5,331 clean / 4,594 faulted; final 14,241 clean / 12,084 faulted (includes deposits).
All 32 burst arms passed; larger K/thread clean burst totals ranged 42.48–52.53ms, usually worse than K32/4.
Raw per-block local logs: `/tmp/spec-{dev,burst}-{false,true}-{0,20}-{0,0.1}.log`, `/tmp/spec-final{,-fault}.log`.
Reproduce using README builder-sim command: K=32,128; threads=4,8; iters=3; the forwarding/prewarm/fault cross product.
Final66 ran clean/off/0 and faulted/on/20 combinations. Every build/run was bounded by `timeout 900`.
Legacy dev gate passed threads 1,2,4,8,10; via-executor dev+final passed 1,4,8 (receipts/bundles/reverts/hooks).
Library: 137 tests passed; five speculator tests passed 20 repetitions; no-default-features check passed.
Clippy passed with one pre-existing `parallel_payload.rs:180` manual-is-multiple-of warning; no new warnings.

Interpretation: K32/4 without forwarding is a promising clean-fixture baseline, not justification to raise production gas yet.
Prediction divergence can erase savings; larger windows increase stale work. One burst cannot characterize burst p99.
Phase 2 needs an independent best-transactions snapshot feeder, bounded per-worker StateProviderFactory providers with
explicit error poisoning (the current engine read facade is infallible+Sync), prefix/system-change publication, and an opt-in flag.
Keep fair builder selection/checks/commit conditions unchanged. Measure real provider I/O, memory, deadlines, cancellation,
CPU contention and burst tails before rollout; do not wire flashblocks. Preserve the immutable parent/factory/environment contract.
