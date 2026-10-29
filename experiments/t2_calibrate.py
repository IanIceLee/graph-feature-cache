#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T2 calibration: measure the real publication operation used by the owner.

Same function (service_op.finish_publication) and same ledger/flush policy as the live
service. Warm-up first, then 3 independent groups of at least 200 samples per payload size;
raw samples are kept. q_hat is the median per operation class and is frozen before any test
run uses it.
"""
import hashlib, json, os, statistics, sys, time

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import _bootstrap  # noqa: F401
from graphcache_framework.common import make_feature
from graphcache_core.service_op import finish_publication

CLASSES = {"S": [32, 64], "M": [128, 256], "L": [512]}
WARMUP, GROUPS, PER_GROUP, FLUSH_EVERY = 50, 3, 250, 64


class Sink:
    """Same register/flush policy as the owner: append a line, fsync every FLUSH_EVERY."""
    def __init__(self, path):
        self.f = open(path, "a", encoding="utf-8"); self.n = 0; self.flushes = 0
    def __call__(self, rec):
        self.f.write(json.dumps({"event": "register", "bytes": rec["bytes"],
                                 "payload_sha256": rec["payload_sha256"]}) + "\n")
        self.n += 1
        if self.n % FLUSH_EVERY == 0:
            self.f.flush(); os.fsync(self.f.fileno()); self.flushes += 1
    def close(self):
        self.f.flush(); os.fsync(self.f.fileno()); self.f.close()


def main():
    out = os.path.join(ROOT, "calibration"); os.makedirs(out, exist_ok=True)
    led = Sink(os.path.join(out, "calibration_ledger.jsonl"))
    rows = []; summary = {}
    for cls, sizes in CLASSES.items():
        samples = []
        for size in sizes:
            for i in range(WARMUP):
                make_feature(f"warm{size}", 0, size)
                finish_publication(f"warm{size}", 0, make_feature(f"warm{size}", 0, size), led)
            for g in range(GROUPS):
                for k in range(PER_GROUP):
                    oid = f"cal{cls}{size}"
                    t0 = time.perf_counter_ns()
                    body = make_feature(oid, 0, size)
                    finish_publication(oid, 0, body, led)
                    dt = time.perf_counter_ns() - t0
                    samples.append(dt)
                    rows.append(dict(cls=cls, size=size, group=g, idx=k, ns=dt))
        med = statistics.median(samples)
        p95 = float(sorted(samples)[int(0.95 * (len(samples) - 1))])
        summary[cls] = dict(sizes=sizes, n=len(samples), q_hat_us=round(med / 1e3, 3),
                            median_ns=int(med), p95_us=round(p95 / 1e3, 3),
                            mean_us=round(statistics.fmean(samples) / 1e3, 3),
                            min_us=round(min(samples) / 1e3, 3), max_us=round(max(samples) / 1e3, 3))
        print(cls, summary[cls], flush=True)
    led.close()
    import csv
    with open(os.path.join(out, "cost_samples.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    meta = dict(operation="materialize+encode+decode_check+digest+register",
                flush_every=FLUSH_EVERY, warmup=WARMUP, groups=GROUPS, per_group=PER_GROUP,
                samples=len(rows), classes=summary,
                q_hat_global_us=round(statistics.median([s["q_hat_us"] for s in summary.values()]), 3),
                frozen_before_test_runs=True,
                samples_sha256=hashlib.sha256(
                    open(os.path.join(out, "cost_samples.csv"), "rb").read()).hexdigest(),
                note="q_hat is the per-class median of the measured service time; it is used as a "
                     "declared cost in scoring and never re-estimated from test runs")
    json.dump(meta, open(os.path.join(out, "calibration.json"), "w"), indent=1)
    print(json.dumps({k: meta[k] for k in ("samples", "q_hat_global_us")}, indent=1))


if __name__ == "__main__":
    main()
