#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Owner process (round04): a real feature-publication service with an explicit bind address.

Every publication executes materialize -> encode -> digest -> register. Requests carry
run_id / attempt_id / request_id and an idempotency key; a replayed request returns the same
instance and does not publish again. The service also reports its own table sizes so the
retention rules are visible instead of assumed.
"""
import argparse, base64, json, os, socket, sys, threading, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graphcache_framework.common import make_feature
from graphcache_core.service_op import finish_publication
from graphcache_framework.sources import open_source


class Owner:
    def __init__(self, root, rule, source, flush_every=64):
        self.root = root; os.makedirs(root, exist_ok=True)
        self.rule = rule; self.src = source; self.flush_every = int(flush_every)
        self.path = os.path.join(root, "owner_ledger.jsonl")
        self.led = open(self.path, "a", encoding="utf-8")
        self.lock = threading.Lock()
        self.instance = {}      # (oid, ver) -> rec; source_reuse keeps it, fresh overwrites in place
        self.idem = {}          # (run, attempt, request, oid, ver) -> response; never reclaimed in-run
        self.n_inst = 0; self.n_pub = 0; self.n_send = 0; self.n_replay = 0
        self.pub_ns = 0; self.serve_ns = 0; self.n_flush = 0; self.frame_bytes = 0
        self.inst_slots_peak = 0; self.idem_peak = 0
        self._since_flush = 0

    def warmup(self, n=40):
        oid = self.src.objects[0]["id"]
        for _ in range(n):
            body = (make_feature(oid, 0, self.src.by_id[oid]["b"]) if self.src.kind == "synthetic"
                    else self.src.oracle(oid, 0))
            finish_publication(oid, 0, body, None)

    def _log(self, **kw):
        kw.setdefault("ts_ns", time.time_ns())
        self.led.write(json.dumps(kw) + "\n")
        self._since_flush += 1
        if self._since_flush >= self.flush_every:
            self.led.flush(); os.fsync(self.led.fileno()); self._since_flush = 0; self.n_flush += 1

    def _publish(self, oid, ver, ctx):
        t0 = time.perf_counter_ns()
        body = (make_feature(oid, ver, self.src.by_id[oid]["b"]) if self.src.kind == "synthetic"
                else self.src.oracle(oid, ver))
        self.n_inst += 1; self.n_pub += 1
        inst = f"{oid}@{ver}#{self.n_inst}"; pid = f"pub{self.n_pub}"

        def sink(r):
            self._log(event="register", kind="publication", object_id=oid, content_version=ver,
                      instance_id=inst, publication_id=pid, payload_sha256=r["payload_sha256"],
                      wire_sha256=r["wire_sha256"], bytes=r["bytes"], **ctx)

        rec = finish_publication(oid, ver, body, sink)
        self.pub_ns += time.perf_counter_ns() - t0
        rec.update(instance_id=inst, publication_id=pid, _sent=False)
        self.instance[(oid, ver)] = rec
        self.inst_slots_peak = max(self.inst_slots_peak, len(self.instance))
        return rec

    def get(self, ids, ctx):
        out = []
        with self.lock:
            for oid, ver in ids:
                ver = int(ver); key = (oid, ver)
                ikey = (ctx["run_id"], ctx["attempt_id"], ctx["request_id"], oid, ver)
                t0 = time.perf_counter_ns()
                if oid not in self.src.by_id:
                    out.append(dict(id=oid, version=ver, error="unknown_object")); continue
                if ikey in self.idem:
                    r = dict(self.idem[ikey]); r["replayed"] = True
                    self.n_replay += 1; self.n_send += 1
                    self.serve_ns += time.perf_counter_ns() - t0
                    out.append(r); continue
                if self.rule == "fresh" or key not in self.instance:
                    rec = self._publish(oid, ver, ctx); pub = rec["publication_id"]
                else:
                    rec = self.instance[key]
                    pub = "reuse" if rec.get("_sent") else rec["publication_id"]
                rec["_sent"] = True
                resp = dict(id=oid, version=ver, instance_id=rec["instance_id"], publication_id=pub,
                            payload_sha256=rec["payload_sha256"], wire_sha256=rec["wire_sha256"],
                            bytes=rec["bytes"], frame_bytes=len(rec["frame"]), replayed=False,
                            frame_b64=base64.b64encode(rec["frame"]).decode())
                self.idem[ikey] = resp
                self.idem_peak = max(self.idem_peak, len(self.idem))
                self.n_send += 1
                self.frame_bytes += len(rec["frame"])
                self.serve_ns += time.perf_counter_ns() - t0
                self._log(event="send", kind="serve", object_id=oid, content_version=ver,
                          instance_id=resp["instance_id"], publication_id=resp["publication_id"],
                          payload_sha256=resp["payload_sha256"], bytes=resp["bytes"],
                          frame_bytes=resp["frame_bytes"], replayed=False, **ctx)
                out.append(resp)
        return out

    def stats(self):
        with self.lock:
            self.led.flush()
            return dict(ok=True, publications=self.n_pub, instances_created=self.n_inst,
                        sends=self.n_send, replays=self.n_replay, pub_ns=self.pub_ns,
                        serve_ns=self.serve_ns, rule=self.rule, flushes=self.n_flush,
                        frame_bytes=self.frame_bytes, ledger=self.path,
                        instance_table=dict(
                            slots=len(self.instance), peak_slots=self.inst_slots_peak,
                            retention=("one record per (object, version), kept for the whole run"
                                       if self.rule == "source_reuse" else
                                       "one slot per (object, version), overwritten on every "
                                       "publication; old instances are not retained")),
                        idem_table=dict(entries=len(self.idem), peak_entries=self.idem_peak,
                                        retention="not reclaimed within a run"))


def handle(o, c):
    f = c.makefile("rwb")
    try:
        while True:
            line = f.readline()
            if not line: return
            req = json.loads(line.decode())
            if req.get("op") == "get":
                ctx = {k: req[k] for k in ("run_id", "attempt_id", "request_id")}
                r = {"ok": True, "objects": o.get([tuple(x) for x in req["ids"]], ctx)}
            elif req.get("op") == "stats":
                r = o.stats()
            else:
                r = {"ok": False, "error": "unknown_op"}
            f.write((json.dumps(r) + "\n").encode()); f.flush()
    except Exception as e:
        try:
            f.write((json.dumps({"ok": False, "error": repr(e)}) + "\n").encode()); f.flush()
        except Exception: pass
    finally:
        try: f.close(); c.close()
        except Exception: pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True); ap.add_argument("--rule", default="source_reuse")
    ap.add_argument("--port", type=int, required=True); ap.add_argument("--source", required=True)
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--flush-every", type=int, default=64)
    a = ap.parse_args()
    o = Owner(a.root, a.rule, open_source(a.source), a.flush_every)
    o.warmup()
    s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((a.bind, a.port)); s.listen(8)
    print("OWNER_READY", a.bind, a.port, flush=True)
    while True:
        c, _ = s.accept(); threading.Thread(target=handle, args=(o, c), daemon=True).start()


if __name__ == "__main__":
    main()
