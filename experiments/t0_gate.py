#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T0 gate: protocol invariants under real dual-process runs, plus a negative battery.

Positives: 8 strategies x 2 source rules on a 6-object / 12-request workload, each a real
owner+consumer pair. Negatives: mutations of the base stream (and of the owner ledger) that
the verifier must reject with a specific error code.
"""
import copy, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import _bootstrap  # noqa: F401
from graphcache_framework import runner
from graphcache_core.policy import STRATEGIES
from graphcache_framework.sources import SyntheticSource
from graphcache_framework.verifier import verify, read_owner_ledger

SIZES = [32, 48, 64, 96, 128, 192]
TRACE = ["o0", "o1", "o2", "o0", "o1", "o3", "o0", "o4", "o1", "o0", "o5", "o2"]


def build():
    objs = [dict(id=f"o{i}", b=SIZES[i], m=SIZES[i], q=1.0 + i) for i in range(6)]
    cnt = {}
    for o in TRACE: cnt[o] = cnt.get(o, 0) + 1
    return objs, cnt


def items_for(objs, cnt, rule):
    return [dict(id=o["id"], b=o["b"], m=o["m"], q=o["q"], q_hat=o["q"],
                 k_comm=max(0.0, float(cnt.get(o["id"], 0)) - 1.0),
                 k_pub=(max(0.0, float(cnt.get(o["id"], 0)) - 1.0) if rule == "fresh" else 0.0))
            for o in objs]


def plan_for(objs, strategy, rule, cap, run_id, attempt_id, cnt=None, retry_prob=0.0):
    cnt = cnt or {}
    return dict(run_id=run_id, attempt_id=attempt_id, strategy=strategy, rule=rule, cap=cap,
                lam=100.0, algorithm="three_way", seed=0, retry_prob=retry_prob,
                items=items_for(objs, cnt, rule),
                requests=[dict(request_id=f"q{i}", step=i, objects=[[o, 0]])
                          for i, o in enumerate(TRACE)])


def verify_run(res, objs, plan, rule, cap, oracle, allow_full=False):
    events = [json.loads(l) for l in open(res["events_path"], encoding="utf-8")]
    recs = read_owner_ledger(res["ledger_path"]) if os.path.exists(res["ledger_path"]) else []
    v = verify(events, plan["requests"], objs, cap, rule, plan["attempt_id"], oracle,
               owner_records=recs, allow_full_retain=allow_full)
    return v, events, recs


def mutate_cases(events, recs, plan):
    """Each case: (mutated events, mutated owner records, expected error code)."""
    def cp(): return copy.deepcopy(events)
    def cr(): return copy.deepcopy(recs)
    cases = {}
    base = cp()
    first_send = next(i for i, e in enumerate(base) if e["event"] == "send")
    m = cp(); x = m[first_send]; x["event"] = "admit"; x["instance_id"] = x["instance_id"]
    cases["fake_admit_then_hit"] = (m, cr(), "admit_without_send")
    m = cp(); i = next(i for i, e in enumerate(m) if e["event"] == "admit"); m.insert(i, dict(m[i]))
    cases["duplicate_admit"] = (m, cr(), "duplicate_admit")
    m = cp(); m.insert(0, dict(m[0], event="evict", object_id="o5", content_version=None,
                               instance_id=None, payload_bytes=0))
    cases["evict_nonresident"] = (m, cr(), "evict_nonresident")
    m = cp(); h = next(e for e in m if e["event"] == "hit"); h["instance_id"] = "bogus#1"
    cases["wrong_instance_hit"] = (m, cr(), "instance_mismatch")
    m = cp(); h = next(e for e in m if e["event"] == "hit"); h["content_version"] = 1
    cases["old_version_hit"] = (m, cr(), "stale_instance")
    m = cp(); i = next(i for i, e in enumerate(m) if e["event"] in ("hit", "send")); del m[i]
    cases["missing_input"] = (m, cr(), "missing_input")
    m = cp(); m[0]["attempt_id"] = "other"
    cases["foreign_attempt"] = (m, cr(), "foreign_attempt")
    m = cp(); c = next(e for e in m if e["event"] == "compute"); c["consumed_sha256"] = "00" * 32
    cases["tampered_compute"] = (m, cr(), "compute_digest")
    m = cp(); c = next(i for i, e in enumerate(m) if e["event"] == "compute")
    j = max(i for i, e in enumerate(m[:c]) if e["event"] in ("hit", "send"))
    m.insert(c, dict(m[j]))
    cases["duplicate_consume"] = (m, cr(), "duplicate_consume")
    m = cp(); c = next(e for e in m if e["event"] == "compute")
    m.insert(0, dict(c, request_id="q99", step=0))
    cases["unplanned_request"] = (m, cr(), "unknown_request")
    return cases


def main():
    objs, cnt = build()
    total = sum(o["m"] for o in objs)
    cap = int(0.5 * total)
    spec = dict(kind="synthetic", objects=[dict(id=o["id"], b=o["b"]) for o in objs])
    os.makedirs(os.path.join(ROOT, "configs"), exist_ok=True)
    src_path = os.path.join(ROOT, "configs", "t0_source.json")
    json.dump(spec, open(src_path, "w", encoding="utf-8"), indent=1)
    oracle = SyntheticSource(spec).oracle
    ev_dir = os.path.join(ROOT, "checks", "t0_events"); os.makedirs(ev_dir, exist_ok=True)

    positives = []
    for rule in ("source_reuse", "fresh"):
        for strat in STRATEGIES:
            c = total if strat == "full_retain" else cap
            plan = plan_for(objs, strat, rule, c, f"t0-{rule}-{strat}", "a1", cnt)
            out = os.path.join(ROOT, "runs", "T0", f"{rule}-{strat}")
            res = runner.attempt(plan, os.path.join(ROOT, "configs", "t0_plan.json"), src_path,
                                 rule, out)
            row = dict(rule=rule, strategy=strat, rc=res["rc"])
            if res["consumer"] is None:
                row.update(ok=False, error=(res["stderr"] or res["stdout"])[-400:])
                positives.append(row); print("FAIL", rule, strat, row.get("error")[-200:]); continue
            v, events, recs = verify_run(res, objs, plan, rule, c, oracle, allow_full=(strat == "full_retain"))
            sm = res["consumer"]
            row.update(violations=v.violations, codes=dict(v.codes),
                       transfer_match=(v.transfer_bytes == sm["transfer_bytes"]),
                       publications_match=(v.publications == sm["publications"]
                                           == v.owner_publications),
                       peak_match=(v.peak_resident == sm["peak_payload"]),
                       hits_match=(v.hits == sm["hits"]), retries=v.retries)
            row["ok"] = bool(res["rc"] == 0 and v.violations == 0 and row["transfer_match"]
                             and row["publications_match"] and row["peak_match"] and row["hits_match"])
            positives.append(row)
            print(f'{rule:13s} {strat:11s} ok={row["ok"]} viol={v.violations} '
                  f'bytes={v.transfer_bytes} pubs={v.publications} hits={v.hits} peak={v.peak_resident}')

    # retry semantics: a lost response must reuse the instance and publish nothing new
    plan = plan_for(objs, "lru", "source_reuse", cap, "t0-retry", "a1", cnt, retry_prob=1.0)
    res = runner.attempt(plan, os.path.join(ROOT, "configs", "t0_plan_retry.json"), src_path,
                         "source_reuse", os.path.join(ROOT, "runs", "T0", "retry"))
    v, rev, rrec = verify_run(res, objs, plan, "source_reuse", cap, oracle)
    retry_sm = res["consumer"] or {}
    retry = dict(rc=res["rc"], violations=v.violations, codes=dict(v.codes),
                 retries=v.retries, retry_bytes=v.retry_bytes,
                 owner_publications=v.owner_publications,
                 consumer_publications=retry_sm.get("publications"),
                 no_new_publication_on_retry=(v.owner_publications == retry_sm.get("publications")
                                              and v.retries > 0))
    retry["ok"] = bool(res["rc"] == 0 and v.violations == 0 and retry["no_new_publication_on_retry"])
    print("retry", json.dumps({k: retry[k] for k in ("ok", "retries", "violations",
                                                     "owner_publications", "consumer_publications")}))

    # negatives from the base lru/source_reuse stream
    base_plan = plan_for(objs, "lru", "source_reuse", cap, "t0-base", "a1", cnt)
    bres = runner.attempt(base_plan, os.path.join(ROOT, "configs", "t0_plan_base.json"), src_path,
                          "source_reuse", os.path.join(ROOT, "runs", "T0", "base"))
    _, base_ev, base_rec = verify_run(bres, objs, base_plan, "source_reuse", cap, oracle)
    open(os.path.join(ev_dir, "base_events.jsonl"), "w", encoding="utf-8").write(
        "".join(json.dumps(e) + "\n" for e in base_ev))
    open(os.path.join(ev_dir, "retry_events.jsonl"), "w", encoding="utf-8").write(
        "".join(json.dumps(e) + "\n" for e in rev))

    neg = {}
    for name, (evs, recs, want) in mutate_cases(base_ev, base_rec, base_plan).items():
        v = verify(evs, base_plan["requests"], objs, cap, "source_reuse", "a1", oracle,
                   owner_records=recs)
        neg[name] = dict(expected=want, codes=dict(v.codes), rejected=(want in v.codes))
        open(os.path.join(ev_dir, f"neg_{name}.jsonl"), "w", encoding="utf-8").write(
            "".join(json.dumps(e) + "\n" for e in evs))
    # retry-specific negatives
    v = verify(rev, plan["requests"], objs, cap, "source_reuse", "a1", oracle,
               owner_records=rrec)
    neg["retry_republishes"] = dict(expected="retry_new_pub", codes=dict(v.codes),
                                    rejected=False)
    evs = copy.deepcopy(rev)
    for e in evs:
        if e["event"] == "retry_reuse": e["publication_id"] = "pubX"
    v = verify(evs, plan["requests"], objs, cap, "source_reuse", "a1", oracle, owner_records=rrec)
    neg["retry_republishes"] = dict(expected="retry_new_pub", codes=dict(v.codes),
                                    rejected=("retry_new_pub" in v.codes))
    open(os.path.join(ev_dir, "neg_retry_republishes.jsonl"), "w", encoding="utf-8").write(
        "".join(json.dumps(e) + "\n" for e in evs))
    extra = dict(rrec[-1]) if rrec else None
    if extra:
        recs2 = copy.deepcopy(rrec)
        dup = dict(extra); dup["publication_id"] = "pub999"; dup["instance_id"] = extra["instance_id"] + "x"
        recs2.append(dup)
        v = verify(rev, plan["requests"], objs, cap, "source_reuse", "a1", oracle,
                   owner_records=recs2)
        neg["owner_republishes_on_retry"] = dict(expected="orphan_publication", codes=dict(v.codes),
                                                 rejected=("orphan_publication" in v.codes))

    gate = dict(
        workload=dict(objects=len(objs), total_bytes=total, cap=cap, requests=len(TRACE),
                      trace=TRACE, sizes=SIZES),
        positives=positives, retry=retry, negatives=neg,
        positives_ok=all(r["ok"] for r in positives),
        negatives_ok=all(r["rejected"] for r in neg.values()),
        passed=all(r["ok"] for r in positives) and retry["ok"]
               and all(r["rejected"] for r in neg.values()))
    os.makedirs(os.path.join(ROOT, "checks"), exist_ok=True)
    json.dump(gate, open(os.path.join(ROOT, "checks", "gate_T0.json"), "w"), indent=1)
    print("POSITIVES", gate["positives_ok"], "NEGATIVES", gate["negatives_ok"],
          "RETRY", retry["ok"], "-> GATE", "PASS" if gate["passed"] else "FAIL")
    for k, v in neg.items():
        if not v["rejected"]: print("NEG MISS", k, v)


if __name__ == "__main__":
    main()
