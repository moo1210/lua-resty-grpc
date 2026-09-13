#!/bin/bash
# Run one benchmark point against a running OpenResty instance and append
# the result (including nginx worker CPU usage) to results.tsv.
#
# usage: bench_one.sh <version> <port> <tag: a|b> <concurrency> <duration_s>
set -euo pipefail

VERSION=$1
PORT=$2
TAG=$3
CONC=$4
DUR=$5

case "$TAG" in
  a) PATH_URI="/unary" ;;
  b) PATH_URI="/unary_b" ;;
  c) PATH_URI="/unary_c" ;;
  *) echo "unknown tag $TAG" >&2; exit 1 ;;
esac
URL="http://127.0.0.1:${PORT}${PATH_URI}"

MASTER_PID=$(cat /tmp/grpc-bench/run/${VERSION}.pid)
WORKER_PID=$(pgrep -P "$MASTER_PID" | head -1)
CLK_TCK=$(getconf CLK_TCK)

ticks() { awk '{print $14+$15}' /proc/$1/stat; }

T0=$(ticks "$WORKER_PID")
START=$(date +%s.%N)
OUT=$(/tmp/grpc-bench/bin/loadgen -url "$URL" -c "$CONC" -d "${DUR}s" -warmup 2s 2>/tmp/grpc-bench/logs/loadgen-${VERSION}-${TAG}-c${CONC}.stderr)
END=$(date +%s.%N)
T1=$(ticks "$WORKER_PID")

CPU_S=$(awk -v a="$T0" -v b="$T1" -v hz="$CLK_TCK" 'BEGIN{printf "%.3f", (b-a)/hz}')
WALL_S=$(awk -v a="$START" -v b="$END" 'BEGIN{printf "%.3f", b-a}')
OK=$(echo "$OUT" | sed -n 's/.*ok=\([0-9]*\).*/\1/p')
# For tag c each HTTP request performs 4 gRPC calls; normalize CPU per gRPC call.
if [ "$TAG" = "c" ]; then CALLS=$((OK * 4)); else CALLS=$OK; fi
CPU_US_PER_REQ=$(awk -v c="$CPU_S" -v n="$CALLS" 'BEGIN{ if (n>0) printf "%.2f", c*1e6/n; else printf "n/a" }')

echo -e "${VERSION}\t${TAG}\t${CONC}\t${DUR}\t${OK}\t${CPU_S}\t${CPU_US_PER_REQ}\t${WALL_S}\t${OUT}" >> /tmp/grpc-bench/results.tsv
echo "[${VERSION} tag=${TAG} c=${CONC}] $OUT cpu=${CPU_S}s (${CPU_US_PER_REQ} us/req)"
