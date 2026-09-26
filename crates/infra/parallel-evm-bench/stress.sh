#!/usr/bin/env bash
set -euo pipefail

binary=${1:?usage: stress.sh BINARY FIXTURES [RUNS=20] [LOG_DIR]}
data=${2:?usage: stress.sh BINARY FIXTURES [RUNS=20] [LOG_DIR]}
runs=${3:-20}
logs=${4:-$(mktemp -d)}
mkdir -p "$logs"
for ((run=1; run<=runs; run++)); do
    log="$logs/run-$run.log"
    if timeout 300 "$binary" bench --data "$data" --threads 4,8,10 --iters 5 >"$log" 2>&1; then
        printf 'PASS run=%s/%s log=%s\n' "$run" "$runs" "$log"
    else
        status=$?
        printf 'FAIL run=%s/%s exit=%s log=%s (124 means hang)\n' "$run" "$runs" "$status" "$log" >&2
        tail -40 "$log" >&2
        exit "$status"
    fi
done
