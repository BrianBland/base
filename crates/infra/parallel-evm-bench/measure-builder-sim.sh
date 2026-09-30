#!/usr/bin/env bash
# Phase 1d: immutable cohorts, interleaved same-process references, and bounded load gating.
# Full quiet-host reproduction: MODE=full WAIT_SECS=1200 ITERS=5 bash "$0".
# Budgeted shared-host run: MODE=priority WAIT_SECS=300 ITERS=3 bash "$0".
# The load gate is ONCE PER COHORT INVOCATION, not before each internal timed block arm.
# This explicit budget concession avoids hours of waits inside a timeout-900 invocation.
set -euo pipefail
binary=${1:-/Users/brianbland/code/scratch/target-spec4/release/base-parallel-evm-bench}
fixtures=${2:-/Users/brianbland/code/scratch}
logs=${3:-/tmp/spec1d-$(date -u +%Y%m%dT%H%M%SZ)}
mode=${MODE:-full}
wait_secs=${WAIT_SECS:-1200}
case "$mode" in
  full) sets=(dev final burst); threads=${THREADS:-4,8,10}; windows=${WINDOWS:-32,128}; iters=${ITERS:-5} ;;
  priority) sets=(dev burst); threads=${THREADS:-4,8}; windows=${WINDOWS:-128}; iters=${ITERS:-3} ;;
  *) echo "MODE must be full or priority" >&2; exit 1 ;;
esac
mkdir -p "$logs"
mkdir "$logs/fixtures"
for set in "${sets[@]}"; do
  mkdir "$logs/fixtures/$set"
  cp "$fixtures/fixtures-$set/"*.json "$logs/fixtures/$set/"
done
{
  date -u
  git rev-parse HEAD
  git status --short
  uptime
  printf 'mode=%s threads=%s windows=%s iters=%s load_threshold=6 poll_secs=30 max_wait_secs=%s gate=cohort-invocation\n' "$mode" "$threads" "$windows" "$iters" "$wait_secs"
  shasum -a 256 "$binary" "$logs"/fixtures/*/*.json
} > "$logs/environment.txt"
load_gate() {
  local name=$1 started=$SECONDS load elapsed delay
  while :; do
    load=$(python3 -c 'import os; print(os.getloadavg()[0])')
    elapsed=$((SECONDS - started))
    printf '%s arm=%s load1=%s waited_secs=%s\n' "$(date -u +%FT%TZ)" "$name" "$load" "$elapsed" | tee -a "$logs/load-gates.log"
    if awk -v load="$load" 'BEGIN {exit !(load < 6)}'; then
      break
    fi
    if (( elapsed >= wait_secs )); then
      printf '# load gate capped; running under recorded load\n' | tee -a "$logs/load-gates.log"
      break
    fi
    delay=$((wait_secs - elapsed))
    if (( delay > 30 )); then delay=30; fi
    sleep "$delay"
  done
}
for invalid in 0 0.1; do
  for set in "${sets[@]}"; do
    name="$set-$invalid"
    load_gate "$name"
    echo "running $name"
    {
      printf '# invocation-start '; date -u
      uptime | sed 's/^/# /'
      timeout 900 "$binary" builder-sim --data "$logs/fixtures/$set" \
        --threads "$threads" --k "$windows" --iters "$iters" --forwarding true \
        --inject-invalid "$invalid"
      printf '# invocation-end '; date -u
      uptime | sed 's/^/# /'
    } > "$logs/$name.log" 2>&1
  done
done
for set in "${sets[@]}"; do
  timeout 900 "$binary" bench --data "$logs/fixtures/$set" --threads 1,2,4,8,10 \
    --iters 1 > "$logs/legacy-$set.log" 2>&1
  timeout 900 "$binary" bench --via-executor --data "$logs/fixtures/$set" \
    --threads 1,4,8 --iters 1 > "$logs/executor-$set.log" 2>&1
done
printf 'completed: %s\n' "$logs"
