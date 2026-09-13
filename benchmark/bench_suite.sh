#!/bin/bash
# Full benchmark matrix for one version. Assumes the matching OpenResty
# instance is already running.
# usage: bench_suite.sh <version> <port>
set -euo pipefail

VERSION=$1
PORT=$2
REPS=${3:-3}
DUR=${4:-10}

# Pattern A: client per request (documented usage) - primary comparison.
for C in 1 8 32 64; do
  for R in $(seq 1 "$REPS"); do
    /tmp/grpc-bench/bench_one.sh "$VERSION" "$PORT" a "$C" "$DUR"
    sleep 1
  done
done

# Pattern C: 4 calls on one client per request (amortizes connection setup,
# isolates per-call cost).
for C in 1 16; do
  for R in $(seq 1 "$REPS"); do
    /tmp/grpc-bench/bench_one.sh "$VERSION" "$PORT" c "$C" "$DUR"
    sleep 1
  done
done

# Pattern B: persistent client across requests. Only supported by the new
# version (old version's per-client ngx.thread read loop keeps the request
# open unless close() is called in the same request).
if [ "$VERSION" = "new" ]; then
  for R in $(seq 1 "$REPS"); do
    /tmp/grpc-bench/bench_one.sh new "$PORT" b 1 "$DUR"
    sleep 1
  done
fi
