#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Consumer process (round04).

Adds over round03:
  * --owner-host / cfg.owner_host: the consumer connects to the owner address, not to itself
  * GDS single-fetch cost = b (+ lambda*q_hat under fresh) for both hit-refresh and insert
  * window_full: keeps only slices with future demand, evicting expired ones BEFORE admitting,
    so the peak never exceeds one full input window
  * setup vs runtime timing, with rolling selection timed separately from transfers
  * owner table sizes (instance / idempotency) reported from the owner side
  * metadata and physical-storage fields are null when unmeasured; wire_bytes is null unless a
    network layer meters it
"""
import argparse, base64, hashlib, json, os, random, socket, sys, time
from bisect import bisect_right

T_START = time.perf_counter()
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from graphcache_framework.common import decode_frame, sha
from graphcache_core import policy as P


class Client:
    def __init__(self, host, port, timeout=300):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.s.makefile("rwb")
    def call(self, req):
        self.f.write((json.dumps(req) + "\n").encode()); self.f.flush()
        return json.loads(self.f.readline().decode())
    def close(self):
        try: self.f.close(); self.s.close()
        except Exception: pass


def consumed_digest(items):
    s = "\n".join(f"{o}|{v}|{i}|{h}" for o, v, i, h in sorted(items))
    return hashlib.sha256(s.encode()).hexdigest()


def rss_mb():
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"): return round(int(line.split()[1]) / 1024, 2)
    except Exception:
        pass
    try:
        import psutil; return round(psutil.Process().memory_info().rss / 1e6, 2)
    except Exception:
        return None


def device_probe(use_cuda_active):
    """Facts that are only meaningful on the machine that ran the consumer."""
    d = dict(platform=sys.platform, python=sys.version.split()[0], cpu_count=os.cpu_count(),
             rss_mb=rss_mb(), cuda_active=use_cuda_active)
    try:
        with open("/etc/nv_tegra_release", encoding="utf-8") as f:
            d["l4t"] = f.readline().strip()
    except Exception:
        d["l4t"] = None
    try:
        with open("/var/lib/nvpmodel/status", encoding="utf-8") as f:
            d["nvpmodel_status"] = f.read().strip()[:200]
    except Exception:
        d["nvpmodel_status"] = None
    temps = {}
    try:
        base = "/sys/class/thermal"
        for z in sorted(os.listdir(base)):
            if not z.startswith("thermal_zone"): continue
            try:
                t = int(open(f"{base}/{z}/temp").read().strip()) / 1000.0
                typ = open(f"{base}/{z}/type").read().strip()
                temps[typ] = t
            except Exception: pass
    except Exception: pass
    d["thermal_c"] = temps or None
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    d["mem_total_kb"] = int(line.split()[1]); break
    except Exception:
        d["mem_total_kb"] = None
    d["cuda"] = None
    if use_cuda_active:
        try:
            import torch
            d["cuda"] = dict(device=torch.cuda.get_device_name(0),
                             allocated_mb=round(torch.cuda.max_memory_allocated() / 1e6, 1),
                             reserved_mb=round(torch.cuda.max_memory_reserved() / 1e6, 1))
        except Exception as e:
            d["cuda"] = dict(error=repr(e))
    return d


def run(cfg, task, host, port, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    ev_path = os.path.join(out_dir, "consumer_events.jsonl")
    pr_path = os.path.join(out_dir, "per_request.jsonl")
    fe = open(ev_path, "w", encoding="utf-8"); fp = open(pr_path, "w", encoding="utf-8")
    strategy, cap, lam = cfg["strategy"], int(cfg["cap"]), float(cfg.get("lam", 100.0))
    rule, attempt, run_id = cfg["rule"], cfg["attempt_id"], cfg["run_id"]
    sel_mode = cfg.get("select_mode", "static")
    objs = {o["id"]: o for o in cfg["items"]}
    positions = cfg.get("positions", {})
    t_setup0 = T_START
    cli = Client(host, port)
    chosen_static = (P.choose(strategy, cfg["items"], cap, lam, cfg.get("algorithm", "three_way"))
                     if strategy not in P.ONLINE and sel_mode == "static" else set())
    cache = P.make_cache(strategy, cap, cfg.get("seed_counts"))
    resident = {}          # oid -> dict(ver, inst, body, sha)
    rng = random.Random(cfg.get("seed", 0)); retry_prob = float(cfg.get("retry_prob", 0.0))
    setup_s = time.perf_counter() - t_setup0

    m = dict(requests=0, hits=0, misses=0, transfer_bytes=0, retry_bytes=0, frame_bytes=0,
             wire_bytes=None, publications=0, retries=0, evictions=0, admits=0,
             peak_payload=0, peak_entries=0, select_s=0.0, roll_select_s=0.0, fetch_s=0.0,
             maintain_s=0.0, compute_s=0.0, log_s=0.0, cache_bytes_end=0, temp_input_bytes=None,
             metadata_bytes=None, physical_storage_bytes=None, chosen=-1, resident_end=0,
             setup_s=round(setup_s, 4), runtime_s=None, end_to_end_s=None)
    t_run0 = time.perf_counter()

    def emit(**kw):
        t0 = time.perf_counter()
        base = dict(run_id=run_id, attempt_id=attempt, step=kw.pop("step", -1))
        base.update(kw); fe.write(json.dumps(base) + "\n")
        m["log_s"] += time.perf_counter() - t0

    def evict(oid, rid, step):
        resident.pop(oid, None); m["evictions"] += 1
        emit(step=step, event="evict", request_id=rid, object_id=oid, content_version=None,
             instance_id=None, payload_bytes=0)

    def admit(oid, ver, inst, body, ph, rid, step):
        resident[oid] = dict(ver=ver, inst=inst, body=body, sha=ph); m["admits"] += 1
        emit(step=step, event="admit", request_id=rid, object_id=oid, content_version=ver,
             instance_id=inst, payload_bytes=0)

    for req in task.requests:
        rid, step, ridx = req["request_id"], req["step"], req.get("index", req["step"])
        m["requests"] += 1
        consumed = {}; misses = []
        pr = dict(request_id=rid, step=step, bytes=0, hits=0, misses=0, retries=0,
                  publications=0, select_s=0.0, roll_select_s=0.0, fetch_s=0.0,
                  maintain_s=0.0, compute_s=0.0)
        for oid, ver in req["objects"]:
            t0 = time.perf_counter()
            if cache is not None:
                cost = P.single_fetch_cost(objs[oid]["b"], objs[oid]["q_hat"], rule, lam)
                hit = cache.get(oid, cost, objs[oid]["m"]) if strategy == "gds" else cache.get(oid)
            else:
                hit = oid in resident
            dt = time.perf_counter() - t0; m["select_s"] += dt; pr["select_s"] += dt
            if hit and resident[oid]["ver"] != ver: hit = False
            if hit:
                m["hits"] += 1; pr["hits"] += 1
                r = resident[oid]
                emit(step=step, event="hit", request_id=rid, object_id=oid, content_version=ver,
                     instance_id=r["inst"], publication_id="reuse", payload_bytes=0, hit=True)
                consumed[(oid, ver)] = (r["inst"], r["sha"], r["body"])
            else:
                misses.append((oid, ver))
        fetched = []
        if misses:
            t0 = time.perf_counter()
            resp = cli.call(dict(op="get", run_id=run_id, attempt_id=attempt, request_id=rid,
                                 idem=rid, ids=[[o, v] for o, v in misses]))
            dt = time.perf_counter() - t0; m["fetch_s"] += dt; pr["fetch_s"] += dt
            if not resp.get("ok"): raise RuntimeError(f"owner error: {resp}")
            got = {f"{o['id']}|{o['version']}": o for o in resp["objects"]}
            for oid, ver in misses:
                r = got.get(f"{oid}|{ver}")
                if r is None or "error" in r: raise RuntimeError(f"owner refused {oid}@{ver}: {r}")
                if retry_prob > 0 and rng.random() < retry_prob:
                    t1 = time.perf_counter()
                    rr = cli.call(dict(op="get", run_id=run_id, attempt_id=attempt,
                                       request_id=rid, idem=rid, ids=[[oid, ver]]))
                    m["fetch_s"] += time.perf_counter() - t1
                    r2 = rr["objects"][0]
                    if not r2.get("replayed"): raise RuntimeError("retry did not replay")
                    if r2["instance_id"] != r["instance_id"]: raise RuntimeError("retry new instance")
                    if r2["publication_id"] != r["publication_id"]: raise RuntimeError("retry republished")
                    m["retries"] += 1; pr["retries"] += 1
                    if r["publication_id"] != "reuse":
                        m["publications"] += 1; pr["publications"] += 1
                    emit(step=step, event="retry_reuse", request_id=rid, object_id=oid,
                         content_version=ver, instance_id=r["instance_id"], publication_id="reuse",
                         payload_bytes=int(r["bytes"]), replayed=True)
                    m["retry_bytes"] += int(r["bytes"]); pr["bytes"] += int(r["bytes"])
                else:
                    m["misses"] += 1; pr["misses"] += 1
                    m["transfer_bytes"] += int(r["bytes"]); pr["bytes"] += int(r["bytes"])
                    if r["publication_id"] != "reuse":
                        m["publications"] += 1; pr["publications"] += 1
                    emit(step=step, event="send", request_id=rid, object_id=oid,
                         content_version=ver, instance_id=r["instance_id"],
                         publication_id=r["publication_id"], payload_bytes=int(r["bytes"]),
                         payload_sha256=r["payload_sha256"], wire_sha256=r["wire_sha256"],
                         hit=False, replayed=False)
                m["frame_bytes"] += int(r.get("frame_bytes", 0))
                frame = base64.b64decode(r["frame_b64"])
                oid2, ver2, body = decode_frame(frame)
                if oid2 != oid or ver2 != ver: raise RuntimeError("frame identity mismatch")
                if sha(body) != r["payload_sha256"]: raise RuntimeError("decode digest mismatch")
                consumed[(oid, ver)] = (r["instance_id"], r["payload_sha256"], body)
                fetched.append((oid, ver, r["instance_id"], r["payload_sha256"], body))
        # ---- admission ----
        t2 = time.perf_counter()
        if cache is not None:
            for oid, ver, inst, ph, body in fetched:
                cost = P.single_fetch_cost(objs[oid]["b"], objs[oid]["q_hat"], rule, lam)
                for k in (cache.put(oid, objs[oid]["m"], cost) if strategy == "gds"
                          else cache.put(oid, objs[oid]["m"])):
                    evict(k, rid, step)
                if oid in getattr(cache, "d", {}):
                    admit(oid, ver, inst, body, ph, rid, step)
        elif strategy == "full_retain":
            for oid, ver, inst, ph, body in fetched: admit(oid, ver, inst, body, ph, rid, step)
        elif strategy == "window_full":
            # expired slices leave BEFORE new ones are admitted: no transient overlap, so the
            # peak equals one full input window and nothing above it is ever claimed
            for k in list(resident):
                pos = positions.get(k, ())
                if len(pos) - bisect_right(pos, ridx) == 0: evict(k, rid, step)
            for oid, ver, inst, ph, body in fetched: admit(oid, ver, inst, body, ph, rid, step)
        elif sel_mode == "rolling":
            t3 = time.perf_counter()
            cand = set(resident) | {f[0] for f in fetched}
            items_i = []
            for oid in cand:
                pos = positions.get(oid, ())
                k = float(max(0, len(pos) - bisect_right(pos, ridx)))
                items_i.append(dict(id=oid, b=objs[oid]["b"], m=objs[oid]["m"],
                                    q_hat=objs[oid]["q_hat"], k_comm=k,
                                    k_pub=(k if rule == "fresh" else 0.0)))
            sel = P.choose(strategy, items_i, cap, lam, cfg.get("algorithm", "three_way"))
            m["roll_select_s"] += time.perf_counter() - t3; pr["roll_select_s"] = m["roll_select_s"]
            for k in list(resident):
                if k not in sel: evict(k, rid, step)
            for oid, ver, inst, ph, body in fetched:
                if oid in sel: admit(oid, ver, inst, body, ph, rid, step)
        else:
            for oid, ver, inst, ph, body in fetched:
                if oid in chosen_static: admit(oid, ver, inst, body, ph, rid, step)
        m["maintain_s"] += time.perf_counter() - t2; pr["maintain_s"] = m["maintain_s"]
        cur = sum(objs[k]["m"] for k in resident)
        m["peak_payload"] = max(m["peak_payload"], cur)
        m["peak_entries"] = max(m["peak_entries"], len(resident))
        # ---- compute ----
        t4 = time.perf_counter()
        items = [(o, v, inst, ph) for (o, v), (inst, ph, _b) in consumed.items()]
        extra = task.compute({k: val[2] for k, val in consumed.items()}, req) or {}
        dt = time.perf_counter() - t4; m["compute_s"] += dt; pr["compute_s"] = dt
        emit(step=step, event="compute", request_id=rid, object_id=None, content_version=None,
             instance_id=None, payload_bytes=0, consumed_sha256=consumed_digest(items),
             consumed_count=len(consumed), **extra)
        consumed.clear()
        pr.update(peak_payload=cur, resident_entries=len(resident), cache_bytes_end=cur)
        fp.write(json.dumps(pr) + "\n")

    runtime_s = time.perf_counter() - t_run0
    if hasattr(task, "finish"): task.finish()
    stats = cli.call({"op": "stats"}); cli.close()
    fe.close(); fp.close()
    m.update(owner_publications=stats["publications"], owner_pub_ns=stats["pub_ns"],
             owner_serve_ns=stats["serve_ns"], owner_replays=stats["replays"],
             owner_ledger=stats["ledger"], owner_frame_bytes=stats["frame_bytes"],
             owner_instance_table=stats["instance_table"], owner_idem_table=stats["idem_table"],
             cache_bytes_end=sum(objs[k]["m"] for k in resident), resident_end=len(resident),
             chosen=(len(chosen_static) if isinstance(chosen_static, set) else -1),
             runtime_s=round(runtime_s, 4),
             end_to_end_s=round(setup_s + runtime_s, 4),
             select_mode=sel_mode, events_path=ev_path, per_request_path=pr_path,
             owner_host=f"{host}:{port}")
    m["temp_input_bytes"] = int(getattr(task, "temp_input_bytes", 0)) or None
    m["total_transfer_bytes"] = m["transfer_bytes"] + m["retry_bytes"]
    json.dump(m, open(os.path.join(out_dir, "consumer_summary.json"), "w"), indent=1)
    return m


class ControlledTask:
    def __init__(self, plan):
        self.requests = plan["requests"]
    def compute(self, bodies, req):
        h = hashlib.sha256()
        for (oid, ver) in sorted(bodies):
            h.update(f"{oid}|{ver}|".encode()); h.update(bodies[(oid, ver)])
        return dict(input_sha256=h.hexdigest(), input_bytes=sum(len(b) for b in bodies.values()))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True); ap.add_argument("--out", required=True)
    ap.add_argument("--owner-host", default=None); ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--task", default="controlled")
    ap.add_argument("--retry-prob", type=float, default=None)
    a = ap.parse_args()
    plan = json.load(open(a.plan, encoding="utf-8"))
    host = a.owner_host or plan.get("owner_host") or "127.0.0.1"
    cfg = dict(run_id=plan["run_id"], attempt_id=plan["attempt_id"], strategy=plan["strategy"],
               rule=plan["rule"], cap=plan["cap"], lam=plan.get("lam", 100.0),
               items=plan["items"], positions=plan.get("positions"),
               seed_counts=plan.get("seed_counts"), select_mode=plan.get("select_mode", "static"),
               algorithm=plan.get("algorithm", "three_way"), seed=plan.get("seed", 0),
               retry_prob=(a.retry_prob if a.retry_prob is not None else plan.get("retry_prob", 0.0)))
    if a.task == "controlled":
        task = ControlledTask(plan)
    elif a.task == "metr":
        from t1_task import MetrTask
        task = MetrTask(plan)
    else:
        raise SystemExit(f"unknown task {a.task}")
    m = run(cfg, task, host, a.port, a.out)
    probe = device_probe(bool(getattr(task, "uses_cuda", False)))
    m["device"] = probe
    json.dump(m, open(os.path.join(a.out, "consumer_summary.json"), "w"), indent=1)
    print(json.dumps({k: m[k] for k in ("requests", "hits", "misses", "transfer_bytes",
                                        "publications", "retries", "peak_payload",
                                        "setup_s", "runtime_s", "end_to_end_s")}))


if __name__ == "__main__":
    main()
