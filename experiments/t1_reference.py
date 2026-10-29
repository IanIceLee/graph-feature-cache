#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T1 reference process: for every rolling request, a full-graph prediction with the same
checkpoint, from which only the same 164 target nodes are extracted.

Runs in its own process and holds the complete observation array; the consumer never does.
Also writes the three-request small package (full input, closure input, local slice, remote
values, reference output) for independent re-checking.
"""
import hashlib, json, os, sys
import numpy as np
import torch

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import _bootstrap  # noqa: F401
from graphcache_framework.model import GraphDepth

DATA = os.environ.get("GRAPH_CACHE_METR_DIR", os.path.join(ROOT, "data", "metr_la"))
SMALL_REQUESTS = [0, 1, 2]   # contiguous prefix: a sliced stream is not independently verifiable


def main():
    ap = __import__("argparse").ArgumentParser()
    ap.add_argument("--data-dir", default=DATA)
    ap.add_argument("--config", default=os.path.join(ROOT, "configs", "t1_config.json"))
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    out = args.out or os.path.join(ROOT, "data", "t1", "reference"); os.makedirs(out, exist_ok=True)
    z = np.load(os.path.join(args.data_dir, "processed.npz"))
    obs = z["obs"][:, :, 0].astype(np.float32); adj = z["adj"].astype(np.float32)
    ckpt = cfg["model_ckpt"] if os.path.isabs(cfg["model_ckpt"]) else os.path.join(ROOT, cfg["model_ckpt"])
    st = torch.load(ckpt, map_location="cpu", weights_only=False)
    m = GraphDepth(st["L"], st["H"], st.get("hidden", 64), st["depth"])
    m.load_state_dict(st["state"]); m.eval()

    targets = np.array(cfg["targets"]); closure = np.array(cfg["closure"])
    trow = np.array(cfg["target_rows"]); t0 = cfg["t_index"]; W = cfg["window"]
    ref = np.zeros((cfg["requests"], len(targets), cfg["horizon"]), dtype=np.float32)
    tgt_idx = torch.from_numpy(adj)
    for i in range(cfg["requests"]):
        xf = np.ascontiguousarray(obs[t0 + i:t0 + i + W].T)          # [207,12]
        with torch.no_grad():
            y = m(tgt_idx, torch.from_numpy(xf)[None]).numpy()[0]
        ref[i] = y[targets]
    np.save(os.path.join(out, "reference_targets.npy"), ref)
    meta = dict(requests=cfg["requests"], targets=len(targets), horizon=cfg["horizon"],
                checkpoint=cfg["model_ckpt"], t_index=t0, window=W,
                reference_sha256=hashlib.sha256(ref.tobytes()).hexdigest(),
                note="full-graph forward with the same checkpoint; only the target nodes are kept")
    json.dump(meta, open(os.path.join(out, "reference_meta.json"), "w"), indent=1)

    small = os.path.join(out, "small"); os.makedirs(small, exist_ok=True)
    idx = {int(n): k for k, n in enumerate(closure)}
    for i in SMALL_REQUESTS:
        xf = np.ascontiguousarray(obs[t0 + i:t0 + i + W].T)
        np.save(os.path.join(small, f"q{i}_full_input.npy"), xf)
        np.save(os.path.join(small, f"q{i}_closure_input.npy"), xf[closure])
        np.save(os.path.join(small, f"q{i}_local_slice.npy"), xf[targets])
        remote = [int(n) for n in cfg["remote_nodes"]]
        np.save(os.path.join(small, f"q{i}_remote_values.npy"),
                np.ascontiguousarray(obs[t0 + i:t0 + i + W][:, remote].T))
        np.save(os.path.join(small, f"q{i}_reference_targets.npy"), ref[i])
    json.dump(dict(requests=SMALL_REQUESTS, files="<req>_{full_input,closure_input,local_slice,"
                 "remote_values,reference_targets}.npy",
                 remote_nodes=cfg["remote_nodes"], targets=cfg["targets"],
                 t_index=t0, window=W,
                 layout="full_input/closure_input are [nodes, 12]; remote_values is [12 remote, 12]"),
              open(os.path.join(small, "README.json"), "w"), indent=1)
    print(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
