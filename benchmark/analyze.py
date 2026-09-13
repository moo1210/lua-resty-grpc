#!/usr/bin/env python3
"""Aggregate a results TSV into median tables.

usage: analyze.py [results-file] [--raw]
"""
import re
import statistics
import sys
from collections import defaultdict

path = "/tmp/grpc-bench/results.tsv"
args = [a for a in sys.argv[1:] if not a.startswith("--")]
if args:
    path = args[0]

FIELDS = ["non200", "err", "rps", "mean_us", "p50_us", "p90_us", "p99_us", "p999_us", "max_us"]

rows = []
with open(path) as f:
    header = f.readline()
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 9:
            continue
        version, tag, conc, dur, ok, cpu_s, us_per_call, wall_s = parts[:8]
        raw = parts[8]
        m = dict(re.findall(r"([a-z0-9_]+)=([0-9.]+)", raw))
        rec = {
            "version": version,
            "tag": tag,
            "conc": int(conc),
            "ok": int(ok),
            "cpu_s": float(cpu_s),
            "us_per_call": float(us_per_call) if us_per_call != "n/a" else None,
        }
        for k in FIELDS:
            rec[k] = float(m.get(k, 0))
        rows.append(rec)

groups = defaultdict(list)
for r in rows:
    groups[(r["version"], r["tag"], r["conc"])].append(r)

def med(vals):
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else float("nan")

def fmt(v, nd=1):
    return f"{v:.{nd}f}"

print("| version | pattern | conc | runs | ok (median) | errors | RPS (median) | mean (us) | p50 (us) | p90 (us) | p99 (us) | p999 (us) | max (us) | nginx CPU s (median) | CPU us/call |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for key in sorted(groups, key=lambda k: (k[0], k[1], k[2])):
    version, tag, conc = key
    g = groups[key]
    print(
        f"| {version} | {tag} | {conc} | {len(g)} | "
        f"{fmt(med([r['ok'] for r in g]),0)} | "
        f"{fmt(med([r['err'] + r['non200'] for r in g]),0)} | "
        f"{fmt(med([r['rps'] for r in g]),0)} | "
        f"{fmt(med([r['mean_us'] for r in g]))} | "
        f"{fmt(med([r['p50_us'] for r in g]))} | "
        f"{fmt(med([r['p90_us'] for r in g]))} | "
        f"{fmt(med([r['p99_us'] for r in g]))} | "
        f"{fmt(med([r['p999_us'] for r in g]))} | "
        f"{fmt(med([r['max_us'] for r in g]))} | "
        f"{fmt(med([r['cpu_s'] for r in g]),2)} | "
        f"{fmt(med([r['us_per_call'] for r in g]),2)} |"
    )

if len(sys.argv) > 1 and sys.argv[1] == "--raw":
    print("\nRaw runs:")
    for r in rows:
        print(r)
