#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""B regression: GDS hand trajectory with heterogeneous sizes/costs, window_full over three
rolling requests, input-correctness on the real graph, and the retry positive/negative pair."""
import json, os, sys
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import _bootstrap  # noqa: F401
from graphcache_framework import runner
from graphcache_core.policy import GDS, single_fetch_cost
from graphcache_framework.sources import open_source
from graphcache_framework.verifier import verify, read_owner_ledger

LAM = 100.0


def gds_hand():
    """A: b=200 m=200 q=5 ; B: b=100 m=100 q=20 ; cap=300.
    source_reuse cost: A=200, B=100.  fresh cost: A=200+100*5=700, B=100+100*20=2100."""
    out = {}
    for rule, ca, cb in (("source_reuse", 200.0, 100.0), ("fresh", 700.0, 2100.0)):
        g = GDS(300)
        g.put("A", 200, ca); g.put("B", 100, cb)
        h_after_put = dict(A=g.H["A"], B=g.H["B"], L=g.L)
        hit = g.get("A", ca, 200)                      # refresh must use the same cost
        h_after_hit = dict(A=g.H["A"], L=g.L)
        ev = g.put("C", 200, 50.0)                     # needs 200: evicts the cheaper-priority key
        out[rule] = dict(
            costs=dict(A=ca, B=cb), hit=hit,
            H_after_put=h_after_put, H_after_hit=h_after_hit,
            evicted=ev, L_after_evict=g.L, resident=sorted(g.d),
            expect_put=dict(A=ca / 200, B=cb / 100),
            expect_hit=ca / 200,
            expect_L=ca / 200,
            expect_resident=["B", "C"],
            ok=bool(abs(h_after_put["A"] - ca / 200) < 1e-12
                    and abs(h_after_put["B"] - cb / 100) < 1e-12
                    and abs(h_after_hit["A"] - ca / 200) < 1e-12
                    and ev == ["A"] and abs(g.L - ca / 200) < 1e-12
                    and sorted(g.d) == ["B", "C"]))
    # the round03 defect is observable: charging only b under fresh would give H_A = 1.0
    out["defect_would_give"] = dict(fresh_H_A_if_cost_were_b=200.0 / 200, fixed=700.0 / 200)
    out["all_ok"] = all(out[k]["ok"] for k in ("source_reuse", "fresh"))
    return out


def _per_step_evicts_first(events):
    """Within each request, expired slices must be evicted before new ones are admitted."""
    by_step = {}
    for e in events:
        if e["event"] in ("evict", "admit"):
            by_step.setdefault(e["step"], []).append(e["event"])
    for kinds in by_step.values():
        first_admit = kinds.index("admit") if "admit" in kinds else len(kinds)
        if "evict" in kinds[first_admit:]: return False
    return True


def window_full_three():
    cfg = json.load(open(os.path.join(ROOT, "configs", "t1_config.json"), encoding="utf-8"))
    base = json.load(open(os.path.join(ROOT, "configs", "t1_plan_base.json"), encoding="utf-8"))
    src = open_source(os.path.join(ROOT, "configs", "t1_source.json"))
    reqs = base["requests"][:3]
    cap = 576
    root = runner.next_dir(os.path.join(ROOT, "runs", "B_window_full"))
    tgt = os.path.join(root, "consumer", "targets.npy")
    plan = dict(run_id="B-window-full", attempt_id="a1", strategy="window_full",
                rule="source_reuse", cap=cap, lam=LAM, algorithm="three_way",
                select_mode="rolling", seed=0, retry_prob=0.0, items=base["items"],
                positions=base["positions"], requests=reqs, nodes=cfg["closure"],
                targets=cfg["targets"], local_nodes=cfg["local_nodes"],
                target_rows=cfg["target_rows"], window=cfg["window"], horizon=cfg["horizon"],
                t_index=cfg["t_index"], local_npy=cfg["local_npy"], adj_npy=cfg["adj_npy"],
                model_ckpt=cfg["model_ckpt"], targets_out=tgt)
    res = runner.attempt(plan, os.path.join(ROOT, "configs", "plan_B_window_full.json"),
                         os.path.join(ROOT, "configs", "t1_source.json"), "source_reuse", root,
                         task="metr")
    if res["consumer"] is None:
        return dict(ok=False, error=(res["stderr"] or "")[-400:])
    ev = [json.loads(l) for l in open(res["events_path"], encoding="utf-8")]
    v = verify(ev, reqs, base["items"], cap, "source_reuse", "a1", src.oracle,
               owner_records=read_owner_ledger(res["ledger_path"]))
    ref = np.load(os.path.join(ROOT, "data", "t1", "reference", "reference_targets.npy"))[:3]
    got = np.load(tgt); diff = np.abs(got - ref); eps = 1e-6 + 1e-5 * np.abs(ref)
    return dict(ok=bool(v.violations == 0 and v.peak_resident <= 576 and not (diff > eps).any()),
                peak_resident=v.peak_resident, cap=cap, violations=v.violations,
                hits=v.hits, bytes=v.transfer_bytes, max_abs=float(diff.max()),
                failed_elements=int((diff > eps).sum()),
                evicts_before_admits=_per_step_evicts_first(ev),
                root=res["root"])


def retry_pair():
    """One real run with every response lost once, then the mutated retry event must be rejected."""
    wl = json.load(open(os.path.join(ROOT, "configs", "workload_40x240.json"), encoding="utf-8"))
    src_path = os.path.join(ROOT, "configs", "t2_source.json"); src = open_source(src_path)
    objs = wl["objects"]; reqs = wl["requests"][:12]
    plan = dict(run_id="B-retry", attempt_id="a1", strategy="lru", rule="source_reuse",
                cap=wl["cap50"], lam=LAM, algorithm="three_way", select_mode="static", seed=0,
                retry_prob=1.0, items=[dict(id=o["id"], b=o["b"], m=o["m"], q_hat=o["q"],
                                            k_comm=0.0, k_pub=0.0) for o in objs],
                positions={}, requests=reqs)
    root = runner.next_dir(os.path.join(ROOT, "runs", "B_retry"))
    res = runner.attempt(plan, os.path.join(ROOT, "configs", "plan_B_retry.json"), src_path,
                         "source_reuse", root)
    if res["consumer"] is None: return dict(ok=False, error=(res["stderr"] or "")[-400:])
    ev = [json.loads(l) for l in open(res["events_path"], encoding="utf-8")]
    recs = read_owner_ledger(res["ledger_path"])
    v = verify(ev, reqs, plan["items"], wl["cap50"], "source_reuse", "a1", src.oracle,
               owner_records=recs)
    sm = res["consumer"]
    pos_ok = bool(v.violations == 0 and v.retries > 0
                  and v.owner_publications == sm["publications"])
    bad = json.loads(json.dumps(ev))
    for e in bad:
        if e["event"] == "retry_reuse": e["publication_id"] = "pubX"
    v2 = verify(bad, reqs, plan["items"], wl["cap50"], "source_reuse", "a1", src.oracle,
                owner_records=recs)
    return dict(ok=bool(pos_ok and "retry_new_pub" in v2.codes), retries=v.retries,
                publications=v.publications, owner_publications=v.owner_publications,
                neg_codes=dict(v2.codes), root=res["root"])


def main():
    out = dict(gds=gds_hand(), window_full=window_full_three(), retry=retry_pair())
    out["passed"] = bool(out["gds"]["all_ok"] and out["window_full"]["ok"] and out["retry"]["ok"])
    json.dump(out, open(os.path.join(ROOT, "checks", "B_regression.json"), "w"), indent=1)
    print(json.dumps({k: out[k] for k in ("passed",)}, indent=1))
    print("gds", out["gds"]["all_ok"], "| window_full", {k: out["window_full"].get(k) for k in
                                                         ("ok", "peak_resident", "violations")},
          "| retry", {k: out["retry"].get(k) for k in ("ok", "retries", "owner_publications")})


if __name__ == "__main__":
    main()
