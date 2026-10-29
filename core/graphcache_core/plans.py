#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plan construction for T2/T3: one place where estimators are defined.

Estimator modes (frozen before any run):
  causal        n_hat = n_prefix * (suffix_len / prefix_len); unseen objects -> 0
  plan          n_hat = exact count in the executed window (known-plan reference only)
  uniform_reuse every valid candidate gets the same n_hat = mean over causal candidates
q modes:
  per_object    q_hat = calibrated q of the object's class
  uniform       q_hat = global median q from the calibration set
"""
import statistics


def items_for(objects, counts, rule, q_mode="per_object", q_uniform=None, cap=None):
    out = []
    for o in objects:
        n = float(counts.get(o["id"], 0.0))
        k = max(0.0, n - 1.0)
        out.append(dict(id=o["id"], b=o["b"], m=o["m"],
                        q_hat=(o["q"] if q_mode == "per_object" else q_uniform),
                        k_comm=k, k_pub=(k if rule == "fresh" else 0.0)))
    return out


def build(objects, requests, rule, cap, estimator="causal", q_mode="per_object",
          q_uniform=None, algorithm="three_way", prefix_counts=None, scale=3.0,
          run_id="", attempt_id="a1", seed_counts=None, retry_prob=0.0, seed=0):
    if estimator == "causal":
        counts = {k: float(v) * scale for k, v in (prefix_counts or {}).items()}
    elif estimator == "plan":
        counts = {}
        for r in requests:
            for oid, _ in r["objects"]: counts[oid] = counts.get(oid, 0) + 1
    elif estimator == "uniform_reuse":
        base = {k: float(v) * scale for k, v in (prefix_counts or {}).items()}
        pos = [v for v in base.values() if v > 1]
        mean = statistics.fmean(pos) if pos else 0.0
        counts = {o["id"]: mean for o in objects if base.get(o["id"], 0) > 0}
    else:
        raise ValueError(f"unknown estimator {estimator}")
    items = items_for(objects, counts, rule, q_mode, q_uniform)
    return dict(run_id=run_id, attempt_id=attempt_id, strategy="", rule=rule, cap=cap,
                lam=100.0, algorithm=algorithm, select_mode="static", seed=seed,
                retry_prob=retry_prob, items=items, requests=requests, seed_counts=seed_counts,
                estimator=estimator, q_mode=q_mode, counts=counts)
