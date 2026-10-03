#!/usr/bin/env bash
# Usage: prove_matrix.sh <label> <host args...>
# Appends exec metrics + Mac CPU app-proof time to results/matrix.txt.
set -euo pipefail
cd "$(dirname "$0")/../openvm/host"
label=$1
shift
out=$(OUTPUT_PATH=../../results/$label.json /usr/bin/time -l ./target/release/zk-sig-removal-host \
  --mode app --seg-mem-gib 4 "$@" 2>&1)
{
  echo "== $label $*"
  echo "$out" | grep -E "^blocks=|trace_cells=|app_prove_ms=|peak memory footprint|panic|rror"
} | tee -a ../../results/matrix.txt
