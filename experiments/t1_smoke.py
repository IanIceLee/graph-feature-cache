#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T1 pre-gate: check the first three rolling windows before the 288-request batch."""
import hashlib, json, os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: F401
from graphcache_framework import runner
from graphcache_framework.sources import open_source
from graphcache_framework.verifier import verify, read_owner_ledger

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
N = 3

def main():
    cfg = json.load(open(os.path.join(ROOT, "configs", "t1_config.json")))
    base = json.load(open(os.path.join(ROOT, "configs", "t1_plan_base.json")))
    src_path = os.path.join(ROOT, "configs", "t1_source.json"); src = open_source(src_path)
    reqs = base["requests"][:N]; W = cfg["window"]
    ref = np.load(os.path.join(ROOT, "data", "t1", "reference", "reference_targets.npy"))[:N]
    z = np.load(os.path.join(os.environ.get("GRAPH_CACHE_METR_DIR", os.path.join(ROOT, "data", "metr_la")), "processed.npz"))
    obs = z["obs"][:, :, 0].astype(np.float32)
    closure = cfg["closure"]; row = {int(n): i for i, n in enumerate(closure)}
    local = np.load(cfg["local_npy"] if os.path.isabs(cfg["local_npy"]) else os.path.join(ROOT, cfg["local_npy"])); local_rows = [row[int(n)] for n in cfg["local_nodes"]]
    exp = {}
    for i in range(N):
        x = np.zeros((len(closure), W), dtype=np.float32); x[local_rows] = local[i]
        for n in cfg["remote_nodes"]: x[row[int(n)], :] = obs[cfg["t_index"] + i:cfg["t_index"] + i + W, int(n)]
        exp[f"q{i}"] = hashlib.sha256(x.tobytes()).hexdigest()
    cap = int(0.5 * len(cfg["remote_nodes"]) * W * 4)
    out = []
    for strat in ("lru", "joint"):
        root = runner.next_dir(os.path.join(ROOT, "runs", "T1_smoke3", strat))
        tgt = os.path.join(root, "consumer", "targets.npy")
        plan = dict(run_id=f"t1s-{strat}", attempt_id="a1", strategy=strat, rule="source_reuse",
                    cap=cap, lam=100.0, algorithm="three_way", select_mode="rolling", seed=0,
                    retry_prob=0.0, items=base["items"], positions=base["positions"], requests=reqs,
                    nodes=closure, targets=cfg["targets"], local_nodes=cfg["local_nodes"],
                    target_rows=cfg["target_rows"], window=W, horizon=cfg["horizon"],
                    t_index=cfg["t_index"],
                    local_npy=cfg["local_npy"] if os.path.isabs(cfg["local_npy"]) else os.path.join(ROOT, cfg["local_npy"]),
                    adj_npy=cfg["adj_npy"] if os.path.isabs(cfg["adj_npy"]) else os.path.join(ROOT, cfg["adj_npy"]),
                    model_ckpt=cfg["model_ckpt"] if os.path.isabs(cfg["model_ckpt"]) else os.path.join(ROOT, cfg["model_ckpt"]), targets_out=tgt)
        res = runner.attempt(plan, os.path.join(ROOT, "configs", f"t1_smoke_{strat}.json"),
                             src_path, "source_reuse", root, task="metr")
        if res["consumer"] is None:
            out.append(dict(strategy=strat, ok=False, error=(res["stderr"] or "")[-500:])); continue
        ev = [json.loads(l) for l in open(res["events_path"], encoding="utf-8")]
        v = verify(ev, reqs, base["items"], cap, "source_reuse", "a1", src.oracle,
                   owner_records=read_owner_ledger(res["ledger_path"]),
                   compute_callback=lambda e, g, c: [] if e.get("input_sha256") == exp.get(e["request_id"])
                   else ["task_specific"])
        got = np.load(tgt); diff = np.abs(got - ref); eps = 1e-6 + 1e-5 * np.abs(ref)
        out.append(dict(strategy=strat, ok=bool(v.violations == 0 and not (diff > eps).any()),
                        violations=v.violations, codes=dict(v.codes),
                        max_abs=float(diff.max()), failed_elements=int((diff > eps).sum()),
                        hits=v.hits, bytes=v.transfer_bytes, peak=v.peak_resident, root=root))
        print(strat, out[-1]["ok"], out[-1]["max_abs"], out[-1]["violations"], flush=True)
    ok = all(o["ok"] for o in out)
    json.dump(dict(requests=N, configs=out, passed=ok),
              open(os.path.join(ROOT, "checks", "T1_smoke3.json"), "w"), indent=1)
    print("SMOKE3", "PASS" if ok else "FAIL")

if __name__ == "__main__":
    main()
