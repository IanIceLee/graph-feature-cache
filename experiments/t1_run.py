#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T1 runner: eight strategies on the same 288 rolling requests, real dual-process runs.

Each request is a real prediction: the consumer fetches its remote slices from the owner or
from its cache, assembles the request input, runs the depth-2 model on the dependency
closure, and emits a compute event. The verifier rebuilds the input independently; the
reference process supplies the full-graph prediction for the same targets.
"""
import copy, csv, hashlib, json, os, sys
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import _bootstrap  # noqa: F401
from graphcache_framework import runner
from graphcache_core.policy import STRATEGIES
from graphcache_framework.sources import open_source
from graphcache_framework.verifier import verify, read_owner_ledger

CAP_RATIO = 0.5


def main():
    cfg = json.load(open(os.path.join(ROOT, "configs", "t1_config.json")))
    base = json.load(open(os.path.join(ROOT, "configs", "t1_plan_base.json")))
    src_path = os.path.join(ROOT, "configs", "t1_source.json")
    src = open_source(src_path)
    items, positions, requests = base["items"], base["positions"], base["requests"]
    W = cfg["window"]; horizon = cfg["horizon"]; t0 = cfg["t_index"]
    R = cfg["requests"]; n_remote = len(cfg["remote_nodes"])
    window_remote_bytes = n_remote * W * 4
    cap = int(CAP_RATIO * window_remote_bytes)
    ref = np.load(os.path.join(ROOT, "data", "t1", "reference", "reference_targets.npy"))

    # independent expected input digest per request (verifier side, from the source data)
    z = np.load(os.path.join(os.environ.get("GRAPH_CACHE_METR_DIR", os.path.join(ROOT, "data", "metr_la")), "processed.npz"))
    obs = z["obs"][:, :, 0].astype(np.float32)
    closure = cfg["closure"]; row = {int(n): i for i, n in enumerate(closure)}
    local_rows = [row[int(n)] for n in cfg["local_nodes"]]
    local = np.load(cfg["local_npy"] if os.path.isabs(cfg["local_npy"]) else os.path.join(ROOT, cfg["local_npy"]))
    expected = {}
    for i in range(R):
        x = np.zeros((len(closure), W), dtype=np.float32)
        x[local_rows] = local[i]
        for j, n in enumerate(cfg["remote_nodes"]):
            x[row[int(n)], :] = obs[t0 + i:t0 + i + W, int(n)]
        expected[f"q{i}"] = hashlib.sha256(x.tobytes()).hexdigest()

    rows = []; ref_rows = []; mem_rows = []
    for strat in STRATEGIES:
        c = window_remote_bytes if strat == "full_retain" else cap
        os.makedirs(os.path.join(ROOT, "runs", "T1"), exist_ok=True)
        root = runner.next_dir(os.path.join(ROOT, "runs", "T1", strat))
        tgt = os.path.join(root, "consumer", "targets.npy")
        plan = dict(run_id=f"t1-{strat}", attempt_id="a1", strategy=strat, rule="source_reuse",
                    cap=c, lam=100.0, algorithm="three_way", select_mode="rolling", seed=0,
                    retry_prob=0.0, items=items, positions=positions, requests=requests,
                    nodes=closure, targets=cfg["targets"], local_nodes=cfg["local_nodes"],
                    target_rows=cfg["target_rows"], window=W, horizon=horizon, t_index=t0,
                    local_npy=cfg["local_npy"] if os.path.isabs(cfg["local_npy"]) else os.path.join(ROOT, cfg["local_npy"]),
                    adj_npy=cfg["adj_npy"] if os.path.isabs(cfg["adj_npy"]) else os.path.join(ROOT, cfg["adj_npy"]),
                    model_ckpt=cfg["model_ckpt"] if os.path.isabs(cfg["model_ckpt"]) else os.path.join(ROOT, cfg["model_ckpt"]), targets_out=tgt)
        res = runner.attempt(plan, os.path.join(ROOT, "configs", f"t1_plan_{strat}.json"),
                             src_path, "source_reuse", root, task="metr")
        if res["consumer"] is None:
            rows.append(dict(strategy=strat, rc=res["rc"], ok=False,
                             error=(res["stderr"] or res["stdout"])[-500:]))
            print(f"{strat:11s} FAILED rc={res['rc']} {(res['stderr'] or '')[-300:]}", flush=True)
            continue
        events = [json.loads(l) for l in open(res["events_path"], encoding="utf-8")]
        recs = read_owner_ledger(res["ledger_path"])
        exp = expected

        def cb(ev, got, ctx, _exp=exp):
            out = []
            if ev.get("input_sha256") != _exp.get(ev["request_id"]): out.append("task_specific")
            return out
        v = verify(events, requests, items, c, "source_reuse", "a1", src.oracle,
                   owner_records=recs, compute_callback=cb,
                   allow_full_retain=(strat == "full_retain"))
        sm = res["consumer"]
        # numeric comparison against the full-graph reference at the same targets
        got = np.load(tgt)
        diff = np.abs(got - ref)
        eps = 1e-6 + 1e-5 * np.abs(ref)
        fail = diff > eps
        per_req = fail.reshape(R, -1).any(axis=1)
        row = dict(strategy=strat, attempt_root=res["root"], cap=c, requests=R, hits=v.hits,
                   transfer_bytes=v.transfer_bytes,
                   retry_bytes=v.retry_bytes, publications=v.publications,
                   owner_publications=v.owner_publications, peak_resident=v.peak_resident,
                   resident_end=v.resident_end, violations=v.violations,
                   verifier_codes=json.dumps(dict(v.codes)),
                   subgraph_vs_full_max_abs=float(diff.max()),
                   failed_elements=int(fail.sum()), failed_requests=int(per_req.sum()),
                   threshold="1e-6+1e-5|ref|",
                   consumer_wall_s=sm.get("wall_s", sm.get("end_to_end_s")), select_s=round(sm["select_s"], 3),
                   fetch_s=round(sm["fetch_s"], 3), maintain_s=round(sm["maintain_s"], 3),
                   compute_s=round(sm["compute_s"], 3), log_s=round(sm["log_s"], 3),
                   rss_mb=sm["rss_mb"], ok=bool(v.violations == 0 and per_req.sum() == 0))
        rows.append(row)
        for i in range(R):
            d = diff[i]
            ref_rows.append(dict(strategy=strat, attempt_root=res["root"], request=f"q{i}", step=i,
                                 max_abs=float(d.max()),
                                 failed_elements=int((d > eps[i]).sum()),
                                 max_ref_abs=float(np.abs(ref[i]).max()),
                                 rel=float(d.max() / (np.abs(ref[i]).max() + 1e-12)),
                                 ok=bool(not (d > eps[i]).any())))
        mem_rows.append(dict(strategy=strat, attempt_root=res["root"], budget_bytes=c,
                             persistent_payload_peak=v.peak_resident,
                             resident_entries_end=v.resident_end,
                             temp_input_bytes=sm["temp_input_bytes"],
                             metadata_bytes=0, cache_bytes_end=sm["cache_bytes_end"],
                             consumer_rss_mb=sm["rss_mb"],
                             note="temp input is transient and is not added to the payload quota"))
        print(f'{strat:11s} ok={row["ok"]} viol={v.violations} bytes={v.transfer_bytes} '
              f'hits={v.hits} peak={v.peak_resident} maxabs={row["subgraph_vs_full_max_abs"]:.3e} '
              f'failed_req={row["failed_requests"]}', flush=True)

    mdir = os.path.join(ROOT, "metrics"); os.makedirs(mdir, exist_ok=True)
    with open(os.path.join(mdir, "graph_runs.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    with open(os.path.join(mdir, "graph_reference.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(ref_rows[0].keys())); w.writeheader(); w.writerows(ref_rows)
    with open(os.path.join(mdir, "graph_memory.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(mem_rows[0].keys())); w.writeheader(); w.writerows(mem_rows)
    ok = all(r.get("ok") for r in rows)
    json.dump(dict(configs=len(rows), cap=cap, window_remote_bytes=window_remote_bytes,
                   all_ok=ok, runs=rows), open(os.path.join(ROOT, "checks", "T1_runs.json"), "w"), indent=1)
    print("T1 all_ok", ok)


if __name__ == "__main__":
    main()
