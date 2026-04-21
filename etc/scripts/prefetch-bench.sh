#!/usr/bin/env bash
# Run base-prefetch benchmarks inside a Linux container so posix_fadvise-based
# OS page-cache eviction works on macOS dev boxes without sudo.
#
# Usage:
#   etc/scripts/prefetch-bench.sh                                  # both bench targets
#   etc/scripts/prefetch-bench.sh --bench mdbx_state_lookup        # one bench
#   BASE_PREFETCH_COLD_CACHE=0 etc/scripts/prefetch-bench.sh       # warm only
#   BASE_PREFETCH_SIM_MISS_US=3,8,25 etc/scripts/prefetch-bench.sh
#   BASE_PREFETCH_MDBX_PATH=/datadir/db etc/scripts/prefetch-bench.sh
#
# By default the cold-cache mode is enabled. Set BASE_PREFETCH_COLD_CACHE=0 to
# disable. Override BASE_PREFETCH_BENCH_IMAGE to use a pre-built image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

IMAGE_TAG="${BASE_PREFETCH_BENCH_IMAGE:-base/prefetch-bench:latest}"
DOCKERFILE="$REPO_ROOT/etc/docker/Dockerfile.prefetch-bench"

if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
  echo "[prefetch-bench] building $IMAGE_TAG from $DOCKERFILE" >&2
  docker build -t "$IMAGE_TAG" -f "$DOCKERFILE" "$REPO_ROOT"
fi

DOCKER_RUN_FLAGS=(--rm)
if [[ -t 0 && -t 1 ]]; then
  DOCKER_RUN_FLAGS+=(-it)
fi

# Allow the bench to write to /proc/sys/vm/drop_caches:
#   --cap-add SYS_ADMIN          → grants the capability
#   --security-opt systempaths=unconfined → removes Docker's read-only mask on
#                                  /proc/sys, which CAP_SYS_ADMIN alone cannot
#                                  bypass
# Without these the bench falls back to posix_fadvise(POSIX_FADV_DONTNEED),
# which is advisory and rarely evicts mmap-backed pages under no memory
# pressure (so cold-cache numbers end up basically warm).
DOCKER_RUN_FLAGS+=(--cap-add SYS_ADMIN --security-opt systempaths=unconfined)

ENV_FLAGS=(-e "BASE_PREFETCH_COLD_CACHE=${BASE_PREFETCH_COLD_CACHE:-1}")
for var in BASE_PREFETCH_SIM_MISS_US BASE_PREFETCH_MDBX_PATH BASE_PREFETCH_LOOKUP_KEY_COUNT; do
  if [[ -n "${!var:-}" ]]; then
    ENV_FLAGS+=(-e "$var=${!var}")
  fi
done

EXTRA_MOUNTS=()
if [[ -n "${BASE_PREFETCH_MDBX_PATH:-}" ]]; then
  if [[ ! -d "$BASE_PREFETCH_MDBX_PATH" ]]; then
    echo "[prefetch-bench] BASE_PREFETCH_MDBX_PATH=$BASE_PREFETCH_MDBX_PATH is not a directory" >&2
    exit 1
  fi
  EXTRA_MOUNTS+=(-v "$BASE_PREFETCH_MDBX_PATH:$BASE_PREFETCH_MDBX_PATH:ro")
fi

exec docker run \
  "${DOCKER_RUN_FLAGS[@]}" \
  -v "$REPO_ROOT:/work" \
  -v base-prefetch-cargo-registry:/usr/local/cargo/registry \
  -v base-prefetch-cargo-git:/usr/local/cargo/git \
  -v base-prefetch-target:/work/target \
  "${EXTRA_MOUNTS[@]}" \
  "${ENV_FLAGS[@]}" \
  -w /work \
  "$IMAGE_TAG" \
  bash -c "cargo bench -p base-prefetch $*"
