#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T2 trial: the heterogeneous publication service runs for real, four strategies x two rules.

Every miss executes materialize -> encode -> digest -> register on the owner; the consumer
decodes the returned frame and uses the values. Cost is reported three ways: actual
publication count, actual owner service time, and a proxy cost computed from the frozen
calibration q (never mixed with the measured time).
"""
import csv, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: F401
from graphcache_core import plans
from graphcache_framework import runner
from graphcache_framework.runlib import run_one
from graphcache_framework.sources import open_source
from graphcache_core.policy import STRATEGIES

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAL = ["none", "comm_only", "joint", "full_retain"]


def main():
    wl = json.load(open(os.path.join(ROOT, "configs", "workload_40x240.json")))
    src_path = os.path.join(ROOT, "configs", "t2_source.json")
    src = open_source(src_path)
    objects, requests = wl["objects"], wl["requests"]
    qmap = {o["id"]: o["q"] for o in objects}
    cap = wl["cap50"]
    rows = []
    for rule in ("source_reuse", "fresh"):
        for strat in TRIAL:
            c = wl["total_bytes"] if strat == "full_retain" else cap
            plan = plans.build(objects, requests, rule, c, estimator="plan",
                               q_mode="per_object", run_id=f"t2-{rule}-{strat}",
                               run_id_suffix=None) if False else \
                plans.build(objects, requests, rule, c, estimator="plan", q_mode="per_object",
                            run_id=f"t2-{rule}-{strat}", attempt_id="a1")
            o = run_one(plan, strat, src_path, os.path.join(ROOT, "runs", "T2", rule),
                        src.oracle, objects, requests, c, rule,
                        allow_full=(strat == "full_retain"), config_dir=os.path.join(ROOT, "configs"))
            if o["verdict"] is None:
                rows.append(dict(rule=rule, strategy=strat, ok=False,
                                 error=(o["res"]["stderr"] or "")[-300:])); continue
            v, sm = o["verdict"], o["res"]["consumer"]
            proxy_us = sum(qmap[r["object_id"]] for r in o["registers"])
            J = round(v.transfer_bytes + 100.0 * proxy_us, 3)
            rows.append(dict(rule=rule, strategy=strat, attempt_root=o["res"]["root"],
                             requests=sm["requests"], hits=v.hits,
                             transfer_bytes=v.transfer_bytes, publications=v.publications,
                             owner_publications=v.owner_publications,
                             owner_pub_service_us=round(sm["owner_pub_ns"] / 1e3, 3),
                             proxy_pub_cost_us=round(proxy_us, 3),
                             J_bytes=J, peak_resident=v.peak_resident, cap=c,
                             violations=v.violations, codes=json.dumps(dict(v.codes)),
                             wall_s=sm.get("wall_s", sm.get("end_to_end_s")), ok=bool(v.violations == 0)))
            print(f'{rule:13s} {strat:11s} ok={rows[-1]["ok"]} bytes={v.transfer_bytes} '
                  f'pubs={v.publications}/{v.owner_publications} '
                  f'svc={rows[-1]["owner_pub_service_us"]}us proxy={rows[-1]["proxy_pub_cost_us"]}us '
                  f'J={J}', flush=True)
    os.makedirs(os.path.join(ROOT, "metrics"), exist_ok=True)
    with open(os.path.join(ROOT, "metrics", "t2_trial.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list({k for r in rows for k in r})); w.writeheader(); w.writerows(rows)
    ok = all(r.get("ok") for r in rows)
    json.dump(dict(configs=len(rows), all_ok=ok, runs=rows),
              open(os.path.join(ROOT, "checks", "T2_trial.json"), "w"), indent=1)
    print("T2 trial", "PASS" if ok else "FAIL")


if __name__ == "__main__":
    main()
