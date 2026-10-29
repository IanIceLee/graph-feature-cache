#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One attempt + verification, shared by T2 and T3."""
import json, os

from . import runner
from .verifier import verify, read_owner_ledger


def run_one(plan, strategy, source_path, out_root, oracle, objects, requests, cap, rule,
            task="controlled", allow_full=False, compute_callback=None, config_dir=None,
            tag=None):
    plan = dict(plan); plan["strategy"] = strategy
    name = tag or f"{rule}-{strategy}"
    plan_path = os.path.join(config_dir, f"plan_{name}.json")
    res = runner.attempt(plan, plan_path, source_path, rule, out_root, task=task)
    out = dict(res=res, verdict=None, events_path=res["events_path"])
    if res["consumer"] is None: return out
    events = [json.loads(l) for l in open(res["events_path"], encoding="utf-8")]
    recs = read_owner_ledger(res["ledger_path"])
    v = verify(events, requests, objects, cap, rule, plan["attempt_id"], oracle,
               owner_records=recs, compute_callback=compute_callback,
               allow_full_retain=allow_full)
    out["verdict"] = v
    out["registers"] = recs
    return out
