#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Small end-to-end synthetic demo: owner + receiver + verifier, no external data.

Run from the repository root:

    python core/examples/synthetic_demo.py

It starts one owner process and one consumer process on loopback for each strategy,
then independently verifies the recorded events. Output is written under runs/ (ignored).
"""
import json
import os
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for _p in (os.path.join(ROOT, "framework"), os.path.join(ROOT, "core")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from graphcache_framework import runner
from graphcache_framework.sources import SyntheticSource
from graphcache_framework.verifier import verify, read_owner_ledger

SIZES = [32, 48, 64, 96, 128, 192]
TRACE = ["o0", "o1", "o2", "o0", "o1", "o3", "o0", "o4", "o1", "o0", "o5", "o2"]
STRATEGIES = ["none", "comm_only", "joint", "full_retain"]
RULE = "source_reuse"


def build():
    objects = [dict(id=f"o{i}", b=SIZES[i], m=SIZES[i], q=1.0 + i) for i in range(6)]
    counts = Counter(TRACE)
    items = [dict(id=o["id"], b=o["b"], m=o["m"], q=o["q"], q_hat=o["q"],
                  k_comm=max(float(counts[o["id"]]) - 1.0, 0.0), k_pub=0.0)
             for o in objects]
    requests = [dict(request_id=f"q{i}", step=i, objects=[[oid, 0]])
                for i, oid in enumerate(TRACE)]
    spec = dict(kind="synthetic", objects=[dict(id=o["id"], b=o["b"]) for o in objects])
    return objects, items, requests, spec


def main():
    objects, items, requests, spec = build()
    total = sum(o["m"] for o in objects)
    cap = int(0.5 * total)
    os.makedirs(os.path.join(ROOT, "configs"), exist_ok=True)
    source_path = os.path.join(ROOT, "configs", "synthetic_demo_source.json")
    json.dump(spec, open(source_path, "w", encoding="utf-8"), indent=1)
    oracle = SyntheticSource(spec).oracle

    print(f"{'strategy':12s} {'transfer_B':>10s} {'pubs':>5s} {'hits':>5s} {'verified':>8s}")
    failures = 0
    for strat in STRATEGIES:
        c = total if strat == "full_retain" else cap
        plan = dict(run_id=f"demo-{strat}", attempt_id="a1", strategy=strat, rule=RULE,
                    cap=c, lam=100.0, algorithm="three_way", seed=0, retry_prob=0.0,
                    items=items, requests=requests)
        out_root = os.path.join(ROOT, "runs", "synthetic_demo", strat)
        res = runner.attempt(plan, os.path.join(ROOT, "configs", "synthetic_demo_plan.json"),
                             source_path, RULE, out_root)
        if res["consumer"] is None:
            print(f"{strat:12s} FAILED: {res['stderr'][-200:]}")
            failures += 1
            continue
        events = [json.loads(line) for line in open(res["events_path"], encoding="utf-8")]
        recs = read_owner_ledger(res["ledger_path"]) if os.path.exists(res["ledger_path"]) else []
        verdict = verify(events, requests, objects, c, RULE, plan["attempt_id"], oracle,
                         owner_records=recs, allow_full_retain=(strat == "full_retain"))
        summary = res["consumer"]
        ok = verdict.violations == 0
        failures += 0 if ok else 1
        print(f"{strat:12s} {summary['transfer_bytes']:10d} {summary['publications']:5d} "
              f"{summary['hits']:5d} {str(ok):>8s}")
    print("PASS" if failures == 0 else f"FAIL ({failures} strategy/strategies)")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
