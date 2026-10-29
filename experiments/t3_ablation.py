#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T3: component ablation on a fixed task with a causal history protocol.

Prefix (requests 0..59) is statistics only; the scored suffix (60..239) starts from an empty
payload cache and a fresh owner, identical for every strategy. Estimates come from the
prefix and are frozen; the estimator never sees suffix frequencies.

22 configs = 8 strategies x 2 rules (primary 50% budget)
           + 3 component ablations x 2 rules (also joint, one input replaced)
A secondary 25% budget repeats the 22 to check a pre-frozen capacity trade-off.
Three known-plan configs under fresh are reported separately and are never used to attribute
anything to the joint term.
"""
import csv, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: F401
from graphcache_core import plans
from graphcache_framework.runlib import run_one
from graphcache_core.policy import STRATEGIES, gain, select

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VARIANTS = ["no_reuse_estimate", "uniform_q_estimate", "density_skip_only"]


def candidate_stats(items, cap, lam, strategy, algorithm):
    mode = "joint"
    if strategy == "comm_only": mode = "comm_only"
    elif strategy == "pub_only": mode = "pub_only"
    pos = [i for i in items if i["m"] <= cap and gain(i, lam, mode) > 0]
    chosen = select(items, cap, lam, mode, algorithm)
    return dict(candidates=len(pos), candidate_bytes=sum(i["m"] for i in pos),
                budget_binds=sum(i["m"] for i in pos) > cap,
                chosen=len(chosen), chosen_bytes=sum(i["m"] for i in chosen))


def main():
    wl = json.load(open(os.path.join(ROOT, "configs", "workload_40x240.json")))
    cal = json.load(open(os.path.join(ROOT, "calibration", "calibration.json")))
    src_path = os.path.join(ROOT, "configs", "t2_source.json")
    from graphcache_framework.sources import open_source
    src = open_source(src_path)
    objects = wl["objects"]; qmap = {o["id"]: o["q"] for o in objects}
    suffix = wl["requests"][wl["prefix"]:]
    prefix = wl["requests"][:wl["prefix"]]
    pc = {}
    for r in prefix:
        for oid, _ in r["objects"]: pc[oid] = pc.get(oid, 0) + 1
    scale = wl["suffix"] / wl["prefix"]
    q_uniform = cal["q_hat_global_us"]
    os.makedirs(os.path.join(ROOT, "metrics"), exist_ok=True)
    rows = []

    def one(budget_name, cap, rule, strategy, variant=None, estimator="causal",
            q_mode="per_object", algorithm="three_way", tag_prefix="T3"):
        plan = plans.build(objects, suffix, rule, cap, estimator=estimator, q_mode=q_mode,
                           q_uniform=q_uniform, algorithm=algorithm, prefix_counts=pc,
                           scale=scale, run_id=f"{tag_prefix}-{budget_name}-{rule}-{strategy}"
                                              + (f"-{variant}" if variant else ""),
                           attempt_id="a1", seed_counts=pc if strategy == "lfu" else None)
        mode_strat = "joint" if variant else strategy
        if mode_strat in ("lru", "lfu", "gds"):
            cs = dict(candidates=-1, candidate_bytes=-1, budget_binds=None,
                      chosen=-1, chosen_bytes=-1)     # online policies choose per access
        else:
            cs = candidate_stats(plan["items"], cap, 100.0, mode_strat, algorithm)
        c = wl["total_bytes"] if mode_strat == "full_retain" else cap
        o = run_one(plan, mode_strat, src_path, os.path.join(ROOT, "runs", "T3", budget_name, rule),
                    src.oracle, objects, suffix, c, rule,
                    allow_full=(mode_strat == "full_retain"),
                    config_dir=os.path.join(ROOT, "configs"),
                    tag=f"{budget_name}-{rule}-{strategy}" + (f"-{variant}" if variant else ""))
        if o["verdict"] is None:
            rows.append(dict(budget=budget_name, rule=rule, strategy=strategy, variant=variant or "",
                             ok=False, error=(o["res"]["stderr"] or "")[-300:])); return
        v = o["verdict"]
        proxy_us = sum(qmap[r["object_id"]] for r in o["registers"])
        J = round(v.transfer_bytes + 100.0 * proxy_us, 3)
        rows.append(dict(budget=budget_name, rule=rule, strategy=mode_strat, variant=variant or "",
                         attempt_root=o["res"]["root"],
                         estimator=estimator, q_mode=q_mode, algorithm=algorithm,
                         cap=cap, **cs,
                         transfer_bytes=v.transfer_bytes, publications=v.publications,
                         owner_publications=v.owner_publications,
                         owner_service_us=round(o["res"]["consumer"]["owner_pub_ns"] / 1e3, 3),
                         proxy_pub_us=round(proxy_us, 3), J_bytes=J,
                         hits=v.hits, peak_resident=v.peak_resident,
                         maintain_s=round(o["res"]["consumer"]["maintain_s"], 4),
                         select_s=round(o["res"]["consumer"]["select_s"], 4),
                         wall_s=o["res"]["consumer"].get("wall_s", o["res"]["consumer"].get("end_to_end_s")),
                         violations=v.violations, codes=json.dumps(dict(v.codes)),
                         ok=bool(v.violations == 0)))
        print(f'{budget_name:8s} {rule:13s} {strategy:11s} {variant or "-":18s} '
              f'ok={rows[-1]["ok"]} bytes={v.transfer_bytes:6d} pubs={v.publications:3d} '
              f'J={J:9.1f} cand={cs["candidates"]}/{cs["candidate_bytes"]} '
              f'binds={cs["budget_binds"]}', flush=True)

    for budget_name, cap in (("primary50", wl["cap50"]), ("secondary25", wl["cap25"])):
        for rule in ("source_reuse", "fresh"):
            for strat in STRATEGIES:
                one(budget_name, cap, rule, strat)
            for var in VARIANTS:
                kw = dict(estimator="causal", q_mode="per_object", algorithm="three_way")
                if var == "no_reuse_estimate": kw["estimator"] = "uniform_reuse"
                elif var == "uniform_q_estimate": kw["q_mode"] = "uniform"
                elif var == "density_skip_only": kw["algorithm"] = "density_skip"
                one(budget_name, cap, rule, "joint", variant=var, **kw)

    for strat in ("joint", "comm_only", "pub_only"):
        one("plan_ref", wl["cap50"], "fresh", strat, estimator="plan", tag_prefix="T3plan")

    keys = list(dict.fromkeys([k for r in rows for k in r]))
    with open(os.path.join(ROOT, "metrics", "ablation_runs.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys); w.writeheader(); w.writerows(rows)
    matrix = [r for r in rows if r["budget"] in ("primary50", "secondary25")]
    ok = all(r.get("ok") for r in rows)
    json.dump(dict(configs=len(rows), matrix_configs=sum(1 for r in rows if r["budget"] == "primary50"),
                   secondary_configs=sum(1 for r in rows if r["budget"] == "secondary25"),
                   plan_ref=sum(1 for r in rows if r["budget"] == "plan_ref"),
                   all_ok=ok, runs=rows), open(os.path.join(ROOT, "checks", "T3_runs.json"), "w"), indent=1)
    print("T3", "PASS" if ok else "FAIL", "rows", len(rows))


if __name__ == "__main__":
    main()
