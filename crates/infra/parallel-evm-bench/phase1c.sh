#!/usr/bin/env bash
# Interleaved min-of-5 builder arms, including canonical ordered-engine references.
set -euo pipefail
binary=${1:-/Users/brianbland/code/scratch/target-spec3/release/base-parallel-evm-bench}
fixtures=${2:-/Users/brianbland/code/scratch}
logs=${3:-/tmp/spec1c-final}
mkdir -p "$logs"
mkdir "$logs/fixtures"
for set in dev final burst; do
  mkdir "$logs/fixtures/$set"
  cp "$fixtures/fixtures-$set/"*.json "$logs/fixtures/$set/"
done
{ date -u; git rev-parse HEAD; uptime; shasum -a 256 "$binary" "$logs"/fixtures/*/*.json; } > "$logs/environment.txt"
for forwarding in true false; do
  for idle in 0 20; do
    for invalid in 0 0.1; do
      for set in dev final burst; do
        name="$set-$forwarding-$idle-$invalid"
        echo "running $name"
        timeout 900 "$binary" builder-sim --data "$logs/fixtures/$set" \
          --threads 4,8,10 --k 32,64,128 --iters 5 --forwarding "$forwarding" \
          --idle-prewarm-ms "$idle" --inject-invalid "$invalid" > "$logs/$name.log" 2>&1
      done
    done
  done
done
for set in dev final burst; do
  timeout 900 "$binary" bench --data "$logs/fixtures/$set" --threads 1,2,4,8,10 \
    --iters 1 > "$logs/legacy-$set.log" 2>&1
  timeout 900 "$binary" bench --via-executor --data "$logs/fixtures/$set" \
    --threads 1,4,8 --iters 1 > "$logs/executor-$set.log" 2>&1
done
