#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Jetson-side launcher: build one plan, run one consumer as a fresh process, keep its logs.

    remote_run.py --job job.json

job.json: {base, overrides, out, port, host, task, retry_prob}
`base` is a plan template; `overrides` replaces strategy / cap / run_id / targets_out etc.
"""
import argparse, json, os, subprocess, sys, time

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True)
    a = ap.parse_args()
    job = json.load(open(a.job, encoding="utf-8"))
    os.makedirs(job["out"], exist_ok=True)
    plan = json.load(open(job["base"], encoding="utf-8"))
    plan.update(job.get("overrides") or {})
    plan_path = os.path.join(job["out"], "plan.json")
    json.dump(plan, open(plan_path, "w", encoding="utf-8"))
    cmd = [sys.executable, os.path.join(CORE_DIR, "consumer.py"), "--plan", plan_path,
           "--owner-host", job["host"], "--port", str(job["port"]), "--out", job["out"],
           "--task", job.get("task", "controlled")]
    if job.get("retry_prob") is not None:
        cmd += ["--retry-prob", str(job["retry_prob"])]
    t0 = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True, env=_env(), cwd=ROOT)
    wall = time.perf_counter() - t0
    open(os.path.join(job["out"], "stdout.txt"), "w", encoding="utf-8").write(p.stdout or "")
    open(os.path.join(job["out"], "stderr.txt"), "w", encoding="utf-8").write(p.stderr or "")
    json.dump(dict(rc=p.returncode, wall_s=round(wall, 3), cmd=cmd, ts=time.time()),
              open(os.path.join(job["out"], "exit.json"), "w"), indent=1)
    print(json.dumps(dict(rc=p.returncode, wall_s=round(wall, 3), out=job["out"])))
    sys.exit(0 if p.returncode == 0 else p.returncode)


if __name__ == "__main__":
    main()
