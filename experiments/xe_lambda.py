#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""E: lambda sensitivity by local replay on this host.

Same policy version and frozen q as D; owner and consumer both run on the 3090 over loopback.
This is a rule replay: it measures how the selection responds to the price weight, and says
nothing about network or end-to-end time. lambda=100 rows come from the D cross-machine batch
(same counts) and are reused instead of re-run.
"""
import csv, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import _bootstrap  # noqa: F401
from graphcache_core import plans
from graphcache_framework import runner
from graphcache_framework.runlib import run_one
from graphcache_framework.sources import open_source

LAMBDAS = [0.0, 10.0, 100.0, 1000.0]
STRATS = ["joint", "comm_only", "pub_only"]


def main():
    wl = json.load(open(os.path.join(ROOT, "configs", "workload_40x240.json"), encoding="utf-8"))
    cal = json.load(open(os.path.join(ROOT, "calibration", "calibration.json"), encoding="utf-8"))
    src_path = os.path.join(ROOT, "configs", "t2_source.json"); src = open_source(src_path)
    objects = wl["objects"]; suffix = wl["requests"][wl["prefix"]:]
    qmap = {o["id"]: o["q"] for o in objects}
    pc = {}
    for r in wl["requests"][:wl["prefix"]]:
        for oid, _ in r["objects"]: pc[oid] = pc.get(oid, 0) + 1
    rows = []
    if os.path.exists(os.path.join(ROOT, "checks", "D_checks.json")):
        for r in json.load(open(os.path.join(ROOT, "checks", "D_checks.json")))["rows"]:
            if r["rule"] == "fresh" and r["rep"] == 1 and r["strategy"] in STRATS \
                    and not r["variant"] and r["budget"] in ("50", "25"):
                rows.append(dict(source="D cross-machine", lam=100.0, budget=r["budget"],
                                 strategy=r["strategy"], transfer_bytes=r["transfer_bytes"],
                                 publications=r["publications"], proxy_pub_us=r["proxy_pub_us"],
                                 J_bytes=r["J_bytes"], chosen=r["chosen"],
                                 chosen_bytes=r["chosen_bytes"], violations=r["violations"],
                                 wall_s=None))
    for lam in LAMBDAS:
        if lam == 100.0: continue
        for bname, cap in (("50", wl["cap50"]), ("25", wl["cap25"])):
            for strat in STRATS:
                plan = plans.build(objects, suffix, "fresh", cap, estimator="causal",
                                   q_mode="per_object", prefix_counts=pc,
                                   scale=wl["suffix"] / wl["prefix"],
                                   run_id=f"E-lam{int(lam)}-{bname}-{strat}", attempt_id="a1",
                                   q_uniform=cal["q_hat_global_us"])
                plan["lam"] = lam
                root = os.path.join(ROOT, "runs", "E", f"lam{int(lam)}", f"{bname}-{strat}")
                o = run_one(plan, strat, src_path, root, src.oracle, objects, suffix, cap, "fresh",
                            config_dir=os.path.join(ROOT, "configs"),
                            tag=f"E-lam{int(lam)}-{bname}-{strat}")
                if o["verdict"] is None:
                    rows.append(dict(source="local replay", lam=lam, budget=bname, strategy=strat,
                                     error=(o["res"]["stderr"] or "")[-300:])); continue
                v = o["verdict"]
                proxy = sum(qmap[x["object_id"]] for x in o["registers"])
                from graphcache_core import policy as P
                sel = P.choose(strat, plan["items"], cap, lam)
                rows.append(dict(source="local replay", lam=lam, budget=bname, strategy=strat,
                                 transfer_bytes=v.transfer_bytes, publications=v.publications,
                                 proxy_pub_us=round(proxy, 3),
                                 J_bytes=round(v.transfer_bytes + lam * proxy, 3),
                                 chosen=len(sel),
                                 chosen_bytes=sum(i["m"] for i in plan["items"] if i["id"] in sel),
                                 violations=v.violations,
                                 wall_s=o["res"]["consumer"].get("end_to_end_s",
                                                                 o["res"]["consumer"].get("wall_s"))))
                print(f"lam={lam:7.1f} {bname}% {strat:10s} bytes={v.transfer_bytes:6d} "
                      f"pubs={v.publications:3d} J={rows[-1]['J_bytes']:10.1f} "
                      f"chosen={len(sel)} viol={v.violations}", flush=True)
    with open(os.path.join(ROOT, "metrics", "E_lambda.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list({k for r in rows for k in r})); w.writeheader(); w.writerows(rows)
    ok = all(r.get("violations", 0) == 0 and "error" not in r for r in rows)
    json.dump(dict(configs=len(rows), lambdas=LAMBDAS, all_ok=ok, rows=rows),
              open(os.path.join(ROOT, "checks", "E_lambda.json"), "w"), indent=1)
    print(json.dumps(dict(configs=len(rows), all_ok=ok), indent=1))


if __name__ == "__main__":
    main()
