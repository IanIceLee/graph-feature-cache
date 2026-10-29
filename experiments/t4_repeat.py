#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T4: minimum runtime-stability check. Two configurations (T1 joint, T3 joint) are repeated
twice each as independent processes, giving three observations. This estimates wall-clock
spread; it is not a statistical claim of superiority."""
import csv, json, os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: F401
from graphcache_core import plans
from graphcache_framework import runner
from graphcache_framework.runlib import run_one
from graphcache_framework.sources import open_source

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def t1_joint_once(idx):
    cfg = json.load(open(os.path.join(ROOT, "configs", "t1_config.json")))
    base = json.load(open(os.path.join(ROOT, "configs", "t1_plan_base.json")))
    src_path = os.path.join(ROOT, "configs", "t1_source.json"); src = open_source(src_path)
    cap = int(0.5 * len(cfg["remote_nodes"]) * cfg["window"] * 4)
    root = runner.next_dir(os.path.join(ROOT, "runs", "T4", "t1_joint"))
    tgt = os.path.join(root, "consumer", "targets.npy")
    plan = dict(run_id="t1-joint-repeat", attempt_id="a1", strategy="joint", rule="source_reuse",
                cap=cap, lam=100.0, algorithm="three_way", select_mode="rolling", seed=0,
                retry_prob=0.0, items=base["items"], positions=base["positions"],
                requests=base["requests"], nodes=cfg["closure"], targets=cfg["targets"],
                local_nodes=cfg["local_nodes"], target_rows=cfg["target_rows"],
                window=cfg["window"], horizon=cfg["horizon"], t_index=cfg["t_index"],
                local_npy=cfg["local_npy"], adj_npy=cfg["adj_npy"],
                model_ckpt=cfg["model_ckpt"], targets_out=tgt)
    o = run_one(plan, "joint", src_path, root, src.oracle, base["items"], base["requests"],
                cap, "source_reuse", task="metr", config_dir=os.path.join(ROOT, "configs"),
                tag=f"t4-t1-joint-{idx}")
    if o["verdict"] is None: return dict(ok=False, error=(o["res"]["stderr"] or "")[-300:])
    ref = np.load(os.path.join(ROOT, "data", "t1", "reference", "reference_targets.npy"))
    got = np.load(tgt); diff = np.abs(got - ref); eps = 1e-6 + 1e-5 * np.abs(ref)
    sm = o["res"]["consumer"]; v = o["verdict"]
    return dict(ok=bool(v.violations == 0 and not (diff > eps).any()), violations=v.violations,
                transfer_bytes=v.transfer_bytes, hits=v.hits, peak_resident=v.peak_resident,
                max_abs=float(diff.max()),
                wall_s=sm.get("wall_s", sm.get("end_to_end_s")), select_s=round(sm["select_s"], 3),
                fetch_s=round(sm["fetch_s"], 3), maintain_s=round(sm["maintain_s"], 3),
                compute_s=round(sm["compute_s"], 3), log_s=round(sm["log_s"], 3),
                owner_pub_ns=sm["owner_pub_ns"])


def t3_joint_once(idx):
    wl = json.load(open(os.path.join(ROOT, "configs", "workload_40x240.json")))
    src_path = os.path.join(ROOT, "configs", "t2_source.json"); src = open_source(src_path)
    objects = wl["objects"]; qmap = {o["id"]: o["q"] for o in objects}
    suffix = wl["requests"][wl["prefix"]:]
    prefix = wl["requests"][:wl["prefix"]]
    pc = {}
    for r in prefix:
        for oid, _ in r["objects"]: pc[oid] = pc.get(oid, 0) + 1
    plan = plans.build(objects, suffix, "source_reuse", wl["cap50"], estimator="causal",
                       q_mode="per_object", prefix_counts=pc,
                       scale=wl["suffix"] / wl["prefix"], run_id="t3-joint-repeat",
                       attempt_id="a1")
    o = run_one(plan, "joint", src_path, os.path.join(ROOT, "runs", "T4", "t3_joint"),
                src.oracle, objects, suffix, wl["cap50"], "source_reuse",
                config_dir=os.path.join(ROOT, "configs"), tag=f"t4-t3-joint-{idx}")
    if o["verdict"] is None: return dict(ok=False, error=(o["res"]["stderr"] or "")[-300:])
    v = o["verdict"]; sm = o["res"]["consumer"]
    proxy = sum(qmap[r["object_id"]] for r in o["registers"])
    return dict(ok=bool(v.violations == 0), violations=v.violations,
                transfer_bytes=v.transfer_bytes, publications=v.publications,
                proxy_pub_us=round(proxy, 3),
                J_bytes=round(v.transfer_bytes + 100.0 * proxy, 3),
                peak_resident=v.peak_resident, wall_s=sm.get("wall_s", sm.get("end_to_end_s")),
                select_s=round(sm["select_s"], 4), fetch_s=round(sm["fetch_s"], 4),
                maintain_s=round(sm["maintain_s"], 4), compute_s=round(sm["compute_s"], 4),
                log_s=round(sm["log_s"], 4), owner_pub_ns=sm["owner_pub_ns"])


def main():
    out = dict(note="three observations per configuration; spread is a runtime estimate, "
                    "not a significance test")
    t1_existing = next(r for r in json.load(open(os.path.join(ROOT, "checks", "T1_runs.json")))["runs"]
                       if r["strategy"] == "joint")
    out["T1_joint"] = dict(observations=[dict(ok=t1_existing["ok"], source="T1 batch",
                                              transfer_bytes=t1_existing["transfer_bytes"],
                                              hits=t1_existing["hits"],
                                              peak_resident=t1_existing["peak_resident"],
                                              max_abs=t1_existing["subgraph_vs_full_max_abs"],
                                              wall_s=t1_existing["consumer_wall_s"],
                                              select_s=t1_existing["select_s"],
                                              fetch_s=t1_existing["fetch_s"],
                                              maintain_s=t1_existing["maintain_s"],
                                              compute_s=t1_existing["compute_s"],
                                              log_s=t1_existing["log_s"])])
    t3 = json.load(open(os.path.join(ROOT, "checks", "T3_runs.json")))["runs"]
    t3e = next(r for r in t3 if r["budget"] == "primary50" and r["rule"] == "source_reuse"
               and r["strategy"] == "joint" and not r["variant"])
    out["T3_joint"] = dict(observations=[dict(ok=t3e["ok"], source="T3 batch",
                                              transfer_bytes=t3e["transfer_bytes"],
                                              publications=t3e["publications"],
                                              J_bytes=t3e["J_bytes"],
                                              peak_resident=t3e["peak_resident"],
                                              wall_s=t3e["wall_s"])])
    for k in (1, 2):
        out["T1_joint"]["observations"].append(t1_joint_once(k))
        out["T3_joint"]["observations"].append(t3_joint_once(k))
        print("repeat", k, "done", flush=True)
    for key in ("T1_joint", "T3_joint"):
        obs = out[key]["observations"]
        out[key]["all_ok"] = all(o["ok"] for o in obs)
        out[key]["transfer_bytes_equal"] = len({o["transfer_bytes"] for o in obs}) == 1
        out[key]["wall_s"] = [o["wall_s"] for o in obs]
        out[key]["peak_resident"] = [o["peak_resident"] for o in obs]
    json.dump(out, open(os.path.join(ROOT, "checks", "T4_repeat.json"), "w"), indent=1)
    for key in ("T1_joint", "T3_joint"):
        print(key, json.dumps({k: out[key][k] for k in ("all_ok", "transfer_bytes_equal",
                                                        "wall_s", "peak_resident")}))


if __name__ == "__main__":
    main()
