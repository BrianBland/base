#!/usr/bin/env bash
# Paired owner-cost ablation: build 2b9251c52 separately for BEFORE_BINARY.
# Usage: bash measure-owner-cost.sh BEFORE_BINARY AFTER_BINARY FROZEN_DEV_FIXTURES LOG_DIR
# One bounded cohort gate, then alternating before/after invocations; no concurrent builds.
set -euo pipefail
before=$1
after=$2
fixtures=$3
logs=$4
mkdir "$logs"
{ date -u; git rev-parse HEAD; uptime; shasum -a 256 "$before" "$after" "$fixtures/"*.json; } > "$logs/environment.txt"
started=$SECONDS
while :; do
  load=$(python3 -c 'import os; print(os.getloadavg()[0])')
  printf '%s load1=%s waited_secs=%s\n' "$(date -u +%FT%TZ)" "$load" "$((SECONDS - started))" | tee -a "$logs/load-gate.log"
  if awk -v load="$load" 'BEGIN {exit !(load < 6)}'; then break; fi
  if (( SECONDS - started >= 300 )); then
    echo '# cohort load wait capped at 300s' | tee -a "$logs/load-gate.log"
    break
  fi
  delay=$((300 - (SECONDS - started)))
  if (( delay > 30 )); then delay=30; fi
  if (( delay > 0 )); then sleep "$delay"; fi
done
for iteration in 0 1 2 3 4; do
  labels=(before after)
  if (( iteration % 2 )); then labels=(after before); fi
  for label in "${labels[@]}"; do
    binary=$before
    if [[ "$label" == after ]]; then binary=$after; fi
    {
      printf '# invocation-start '; date -u
      uptime | sed 's/^/# /'
      timeout 900 "$binary" builder-sim --data "$fixtures" --threads 4,8,10 \
        --k 128 --iters 1 --forwarding true
      printf '# invocation-end '; date -u
      uptime | sed 's/^/# /'
    } > "$logs/$label-$iteration.log" 2>&1
  done
done
