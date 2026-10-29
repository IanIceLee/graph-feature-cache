#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Process helpers: start one owner per attempt, run the consumer as a separate process."""
import json, os, socket, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
CORE_DIR = os.path.join(ROOT, "core", "graphcache_core")


def _env():
    env = os.environ.copy()
    paths = [os.path.join(ROOT, "framework"), os.path.join(ROOT, "core")]
    old = env.get("PYTHONPATH")
    if old:
        paths.append(old)
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


def port_is_free(port):
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", port)); return True
    except OSError:
        return False
    finally:
        s.close()


def pick_port(base, span=400):
    for p in range(base, base + span):
        if port_is_free(p): return p
    raise RuntimeError("no free port")


def start_owner(root, rule, source_path, port, flush_every=64, timeout=60, bind="127.0.0.1"):
    os.makedirs(root, exist_ok=True)
    p = subprocess.Popen([sys.executable, os.path.join(CORE_DIR, "owner.py"),
                          "--root", root, "--rule", rule, "--port", str(port),
                          "--source", source_path, "--flush-every", str(flush_every),
                          "--bind", bind],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=_env(), cwd=ROOT)
    t0 = time.time()
    line = p.stdout.readline().strip()
    if not line.startswith("OWNER_READY"):
        p.kill()
        raise RuntimeError(f"owner failed to start: {line or p.stderr.read()}")
    if time.time() - t0 > timeout: raise RuntimeError("owner start timeout")
    return p


def stop(p):
    p.terminate()
    try: p.wait(timeout=10)
    except subprocess.TimeoutExpired: p.kill()


def run_consumer(plan_path, port, out_dir, task="controlled", retry_prob=None, timeout=3600,
                 host=None):
    os.makedirs(out_dir, exist_ok=True)
    cmd = [sys.executable, os.path.join(CORE_DIR, "consumer.py"), "--plan", plan_path,
           "--port", str(port), "--out", out_dir, "--task", task]
    if host: cmd += ["--owner-host", host]
    if retry_prob is not None: cmd += ["--retry-prob", str(retry_prob)]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=_env(), cwd=ROOT)


def next_dir(base):
    """Never reuse an existing attempt directory: a ledger is append-only evidence."""
    if not os.path.exists(base): return base
    for k in range(2, 10000):
        cand = f"{base}__run{k}"
        if not os.path.exists(cand): return cand
    raise RuntimeError(f"no free attempt directory under {base}")


def attempt(plan, plan_path, source_path, rule, out_root, task="controlled",
            retry_prob=None, flush_every=64, port_base=57100, bind="127.0.0.1", host=None):
    """One real dual-process attempt. Returns dict(rc, stdout, stderr, consumer, events_path,
    ledger_path, wall_s, root)."""
    out_root = next_dir(out_root)
    os.makedirs(out_root, exist_ok=True)
    os.makedirs(os.path.dirname(plan_path), exist_ok=True)
    json.dump(plan, open(plan_path, "w", encoding="utf-8"), indent=0)
    port = pick_port(port_base)
    owner_dir = os.path.join(out_root, "owner"); cons_dir = os.path.join(out_root, "consumer")
    t0 = time.perf_counter()
    cmd_last_line = [f"owner bind={bind}:{port} rule={rule}"]
    op = start_owner(owner_dir, rule, source_path, port, flush_every, bind=bind)
    try:
        c = run_consumer(plan_path, port, cons_dir, task, retry_prob, host=host)
        wall = round(time.perf_counter() - t0, 3)
    finally:
        stop(op)
    summ_path = os.path.join(cons_dir, "consumer_summary.json")
    os.makedirs(os.path.join(cons_dir), exist_ok=True)
    open(os.path.join(cons_dir, "stdout.txt"), "w", encoding="utf-8").write(c.stdout or "")
    open(os.path.join(cons_dir, "stderr.txt"), "w", encoding="utf-8").write(c.stderr or "")
    json.dump(dict(rc=c.returncode, argv=cmd_last_line[0] if cmd_last_line else None),
              open(os.path.join(cons_dir, "exit.json"), "w"), indent=1)
    out = dict(rc=c.returncode, stdout=(c.stdout or "")[-800:], stderr=(c.stderr or "")[-2000:],
               wall_s=wall, root=out_root, events_path=os.path.join(cons_dir, "consumer_events.jsonl"),
               ledger_path=os.path.join(owner_dir, "owner_ledger.jsonl"),
               summary_path=summ_path if os.path.exists(summ_path) else None)
    out["consumer"] = json.load(open(summ_path)) if out["summary_path"] else None
    return out
