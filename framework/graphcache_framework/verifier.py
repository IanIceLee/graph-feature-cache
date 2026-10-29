#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Strict independent verifier: reads both sides (consumer events + owner ledger) and the
request plan, and rebuilds every state transition itself.

Never trusts a self-reported count. Residency, publications, transfer and consumption are
reconstructed from the chain; digests are recomputed from an independent oracle.

Errors:
  foreign_attempt, out_of_order, unknown_event, unknown_object, unknown_request,
  duplicate_consume, consume_after_compute, send_size, send_digest, wire_digest,
  publication_not_in_owner, owner_content_mismatch, duplicate_publication_id, dup_pub,
  missing_publication, instance_mismatch, hit_payload, hit_without_fetch, hit_after_evict,
  stale_instance, admit_without_send, admit_foreign_instance, duplicate_admit,
  evict_nonresident, over_capacity, missing_input, duplicate_compute, compute_digest,
  request_missing, retry_new_pub, retry_without_publication, orphan_publication
"""
import hashlib, json
from collections import Counter

from .common import encode_frame


def _digest(items):
    s = "\n".join(f"{o}|{v}|{i}|{h}" for o, v, i, h in sorted(items))
    return hashlib.sha256(s.encode()).hexdigest()


class Verdict:
    def __init__(self):
        self.codes = Counter(); self.detail = []
        self.transfer_bytes = 0; self.retry_bytes = 0
        self.publications = 0; self.owner_publications = 0; self.retries = 0
        self.hits = 0; self.peak_resident = 0; self.resident_end = 0; self.computes = 0

    def err(self, code, msg=None):
        self.codes[code] += 1
        if len(self.detail) < 25: self.detail.append(dict(code=code, msg=str(msg)[:300]))

    @property
    def violations(self):
        return sum(self.codes.values())


def verify(events, plan, objects, cap, rule, attempt_id, oracle, owner_records=(),
           compute_callback=None, allow_full_retain=False):
    v = Verdict()
    objs = {o["id"]: o for o in objects}
    plan_entry = {r["request_id"]: {(o, int(ver)) for o, ver in r["objects"]} for r in plan}
    req_ids = set(plan_entry)

    # ---- owner side: publication registry ----
    registry = {}; published = {}
    for rec in owner_records:
        if rec.get("attempt_id") != attempt_id:
            v.err("foreign_attempt", f"owner {rec.get('attempt_id')}"); continue
        pid = rec["publication_id"]
        if pid in registry: v.err("duplicate_publication_id", pid)
        oid, ver = rec["object_id"], int(rec["content_version"])
        if oid not in objs: v.err("unknown_object", oid); continue
        want = hashlib.sha256(oracle(oid, ver)).hexdigest()
        if rec["payload_sha256"] != want or int(rec["bytes"]) != objs[oid]["b"]:
            v.err("owner_content_mismatch", f"{oid}@{ver}")
        registry[pid] = rec
        published[(oid, ver)] = rec["instance_id"]
    v.owner_publications = len(registry)

    resident = {}; received = {}; used = {}; consumed = {}
    computed = set(); ref_send = set(); ref_retry = {}; last_step = -1

    for e in events:
        if e.get("attempt_id") != attempt_id:
            v.err("foreign_attempt", e.get("request_id")); continue
        kind, req = e.get("event"), e.get("request_id")
        step = e.get("step", -1)
        if step < last_step: v.err("out_of_order", f"step {step} after {last_step}")
        last_step = max(last_step, step)
        if kind not in ("hit", "send", "admit", "evict", "retry_reuse", "compute"):
            v.err("unknown_event", kind); continue
        if req not in req_ids:
            v.err("unknown_request", req); continue
        oid = e.get("object_id")
        cv = e.get("content_version")
        ver = int(cv) if cv is not None else None
        if kind in ("hit", "send", "admit", "evict") and oid not in objs:
            v.err("unknown_object", oid); continue

        if kind in ("hit", "send", "retry_reuse"):
            if req in computed: v.err("consume_after_compute", f"{req}/{oid}")
            if (oid, ver) not in plan_entry[req]: v.err("unknown_object", f"{req}/{oid}@{ver} unplanned")
            if (oid, ver) in consumed.get(req, set()): v.err("duplicate_consume", f"{req}/{oid}")
            consumed.setdefault(req, set()).add((oid, ver))

        if kind == "send":
            o = objs[oid]
            if int(e["payload_bytes"]) != o["b"]: v.err("send_size", oid)
            ref = oracle(oid, ver)
            if e.get("payload_sha256") != hashlib.sha256(ref).hexdigest(): v.err("send_digest", f"{oid}@{ver}")
            if e.get("wire_sha256") and e["wire_sha256"] != hashlib.sha256(encode_frame(oid, ver, ref)).hexdigest():
                v.err("wire_digest", f"{oid}@{ver}")
            v.transfer_bytes += int(e["payload_bytes"])
            key = (oid, ver); inst = e.get("instance_id"); pid = e.get("publication_id")
            if pid not in (None, "", "reuse"):
                v.publications += 1; ref_send.add(pid)
                rec = registry.get(pid)
                if rec is None: v.err("publication_not_in_owner", pid)
                elif (rec["object_id"], int(rec["content_version"]), rec["instance_id"]) != (oid, ver, inst):
                    v.err("instance_mismatch", f"send {pid} vs owner {rec['instance_id']}")
                if rule == "source_reuse" and key in received and received[key] != inst:
                    v.err("dup_pub", f"{oid}@{ver}")
                published[key] = inst
            else:
                if key not in published: v.err("missing_publication", f"{oid}@{ver}")
                elif published[key] != inst: v.err("instance_mismatch", f"{oid}@{ver} reuse inst")
            received[key] = inst; used[(req, oid, ver)] = inst

        elif kind == "hit":
            v.hits += 1
            used[(req, oid, ver)] = e.get("instance_id")
            if int(e["payload_bytes"]) != 0: v.err("hit_payload", oid)
            r = resident.get(oid)
            if r is None:
                v.err("hit_after_evict" if any(k[0] == oid for k in received) else "hit_without_fetch",
                      f"{req}/{oid}")
            else:
                if r[0] != ver: v.err("stale_instance", f"{oid} resident v{r[0]} hit as v{ver}")
                if r[1] != e.get("instance_id"): v.err("instance_mismatch", f"{req}/{oid} hit inst")

        elif kind == "admit":
            key = (oid, ver); inst = e.get("instance_id")
            if key not in received: v.err("admit_without_send", f"{oid}@{ver}")
            elif received[key] != inst: v.err("admit_foreign_instance", f"{oid}@{ver}")
            if oid in resident: v.err("duplicate_admit", f"{req}/{oid}")
            resident[oid] = (ver, inst)
            tot = sum(objs[k]["m"] for k in resident if k in objs)
            v.peak_resident = max(v.peak_resident, tot)
            if tot > cap and not allow_full_retain: v.err("over_capacity", f"{tot}>{cap}")

        elif kind == "evict":
            if resident.pop(oid, None) is None: v.err("evict_nonresident", f"{req}/{oid}")

        elif kind == "retry_reuse":
            v.retries += 1; v.retry_bytes += int(e["payload_bytes"])
            key = (oid, ver); inst = e.get("instance_id")
            if e.get("publication_id") not in (None, "", "reuse"): v.err("retry_new_pub", f"{oid}@{ver}")
            if key not in published: v.err("retry_without_publication", f"{oid}@{ver}")
            elif published[key] != inst: v.err("instance_mismatch", f"{oid}@{ver} retry inst")
            ref_retry.setdefault((req, oid, ver), set()).add(inst)
            received[key] = inst; used[(req, oid, ver)] = inst

        elif kind == "compute":
            if req in computed: v.err("duplicate_compute", req)
            want = plan_entry[req]; got = consumed.get(req, set())
            if got != want:
                v.err("missing_input", f"{req} missing={sorted(want - got)[:3]} extra={sorted(got - want)[:3]}")
            items = [(o, ver2, used.get((req, o, ver2)),
                      hashlib.sha256(oracle(o, ver2)).hexdigest()) for (o, ver2) in got]
            if e.get("consumed_sha256") != _digest(items): v.err("compute_digest", req)
            computed.add(req); v.computes += 1
            if compute_callback is not None:
                for code in compute_callback(e, got, dict(violations=v.violations)):
                    v.err(code, req)

    missing = [r for r in plan_entry if r not in computed]
    if missing: v.err("request_missing", f"{len(missing)} e.g. {missing[:3]}")
    for pid, rec in registry.items():
        if pid in ref_send: continue
        k = (rec["request_id"], rec["object_id"], int(rec["content_version"]))
        if rec["instance_id"] in ref_retry.get(k, set()): continue
        v.err("orphan_publication", pid)
    v.resident_end = sum(objs[k]["m"] for k in resident if k in objs)
    return v


def read_owner_ledger(path):
    """Registers from the owner ledger, in file order."""
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r.get("event") == "register": out.append(r)
    return out
