#!/bin/bash
# Capped burst benchmark: restart the worker, run until N successful responses
# (or 20s), and record worker CPU + latency. Restarting the OpenResty worker
# before every burst isolates each measurement from the old version's
# callback-slot exhaustion (~896 callbacks per worker lifetime).
#
# usage: bench_burst.sh <version> <port> <tag: a|b|c> <concurrency> <n>
set -euo pipefail

VERSION=$1
PORT=$2
TAG=$3
CONC=$4
N=$5

CONF=/tmp/grpc-bench/conf/${VERSION}.conf
case "$TAG" in
  a) URI="/unary" ;;
  b) URI="/unary_b" ;;
  c) URI="/unary_c" ;;
  *) echo "unknown tag $TAG" >&2; exit 1 ;;
esac

/tmp/grpc-bench/openresty/bin/openresty -s stop -c "$CONF" 2>/dev/null || true
sleep 0.3
/tmp/grpc-bench/openresty/bin/openresty -c "$CONF"
sleep 0.3

MASTER_PID=$(cat /tmp/grpc-bench/run/${VERSION}.pid)
WORKER_PID=$(pgrep -P "$MASTER_PID" | head -1)
CLK_TCK=$(getconf CLK_TCK)
ticks() { awk '{print $14+$15}' /proc/$1/stat; }

T0=$(ticks "$WORKER_PID")
OUT=$(/tmp/grpc-bench/bin/loadgen -url "http://127.0.0.1:${PORT}${URI}" -c "$CONC" -d 20s -warmup 0s -n "$N")
T1=$(ticks "$WORKER_PID")

/tmp/grpc-bench/openresty/bin/openresty -s stop -c "$CONF" 2>/dev/null || true

CPU_S=$(awk -v a="$T0" -v b="$T1" -v hz="$CLK_TCK" 'BEGIN{printf "%.3f", (b-a)/hz}')
OK=$(echo "$OUT" | sed -n 's/.*ok=\([0-9]*\).*/\1/p')
DUR_S=$(echo "$OUT" | sed -n 's/.*dur_s=\([0-9.]*\).*/\1/p')
if [ "$TAG" = "c" ]; then CALLS=$((OK * 4)); else CALLS=$OK; fi
CPU_US=$(awk -v c="$CPU_S" -v n="$CALLS" 'BEGIN{ if (n>0) printf "%.2f", c*1e6/n; else printf "n/a" }')

echo -e "${VERSION}\t${TAG}\t${CONC}\t${N}\t${OK}\t${CPU_S}\t${CPU_US}\t${DUR_S}\t${OUT}" >> /tmp/grpc-bench/results_burst.tsv
echo "[burst ${VERSION} tag=${TAG} c=${CONC}] $OUT cpu=${CPU_S}s (${CPU_US} us/call)"
