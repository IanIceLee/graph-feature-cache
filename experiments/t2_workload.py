#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T2/T3 workload: 40 objects x 240 requests, heterogeneous sizes, measured publication cost.

Object sizes come from the calibrated classes, so `q` is a measured service time and not a
declared constant. The trace is the round02 construct (Zipf 1.1, permuted counts) so results
stay comparable, and the first 60 requests are a causal prefix that later rounds only
estimate from.
"""
import hashlib, json, os, sys
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
SIZES = [32, 64, 128, 256, 512]
N_OBJ, N_REQ, SEED, ZIPF_S, PREFIX = 40, 240, 7, 1.1, 60


def cls_of(n):
    return "S" if n <= 64 else ("M" if n <= 256 else "L")


def main():
    cal = json.load(open(os.path.join(ROOT, "calibration", "calibration.json")))
    rng = np.random.default_rng(SEED)
    sizes = [SIZES[i % len(SIZES)] for i in range(N_OBJ)]
    q_by_cls = {c: v["q_hat_us"] for c, v in cal["classes"].items()}
    w = 1.0 / np.arange(1, N_OBJ + 1) ** ZIPF_S; w /= w.sum()
    counts = rng.multinomial(N_REQ, w)
    counts = counts[rng.permutation(N_OBJ)]        # popularity independent of index and size
    trace = []
    for i, c in enumerate(counts): trace += [f"o{i:02d}"] * int(c)
    rng.shuffle(trace)
    objs = [dict(id=f"o{i:02d}", b=sizes[i], m=sizes[i], q=q_by_cls[cls_of(sizes[i])],
                 q_class=cls_of(sizes[i])) for i in range(N_OBJ)]
    requests = [dict(request_id=f"q{i}", step=i, objects=[[o, 0]]) for i, o in enumerate(trace)]
    total = sum(o["m"] for o in objs)
    wl = dict(objects=objs, requests=requests, prefix=PREFIX, suffix=N_REQ - PREFIX,
              cap50=int(0.5 * total), cap25=int(0.25 * total), total_bytes=total,
              sizes_class={str(s): cls_of(s) for s in SIZES},
              q_source="calibration/calibration.json (per-class median, frozen)",
              seed=SEED, zipf=ZIPF_S,
              trace_sha256=hashlib.sha256(json.dumps(trace).encode()).hexdigest(),
              prefix_trace_sha256=hashlib.sha256(json.dumps(trace[:PREFIX]).encode()).hexdigest(),
              suffix_trace_sha256=hashlib.sha256(json.dumps(trace[PREFIX:]).encode()).hexdigest())
    os.makedirs(os.path.join(ROOT, "configs"), exist_ok=True)
    json.dump(wl, open(os.path.join(ROOT, "configs", "workload_40x240.json"), "w"), indent=1)
    spec = dict(kind="synthetic", objects=[dict(id=o["id"], b=o["b"]) for o in objs])
    json.dump(spec, open(os.path.join(ROOT, "configs", "t2_source.json"), "w"), indent=1)
    cnt = {o: trace[:PREFIX].count(o) for o in set(trace)}
    print(json.dumps(dict(objects=N_OBJ, requests=N_REQ, prefix=PREFIX, cap50=wl["cap50"],
                          cap25=wl["cap25"], total=total, q_by_class=q_by_cls,
                          prefix_distinct=len([k for k, v in cnt.items() if v > 0]),
                          prefix_reuse_positive=len([k for k, v in cnt.items() if v > 1])), indent=1))


if __name__ == "__main__":
    main()
