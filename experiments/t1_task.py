#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T1 task: assemble one rolling prediction window from local history + fetched remote
slices, run the depth-2 model on the dependency closure, and keep only the targets.

The consumer owns the local sensors' history; every remote value must arrive through the
cache or the owner. The request's input tensor is transient: built, used, released.
"""
import hashlib, os, sys
import numpy as np
import torch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import _bootstrap  # noqa: F401
from graphcache_framework.model import GraphDepth


class MetrTask:
    def __init__(self, plan):
        self.requests = plan["requests"]
        self.nodes = [int(n) for n in plan["nodes"]]
        self.row = {n: i for i, n in enumerate(self.nodes)}
        self.trow = [int(r) for r in plan["target_rows"]]
        self.local_rows = [self.row[int(n)] for n in plan["local_nodes"]]
        self.local = np.load(plan["local_npy"])                    # [R, |local|, 12]
        self.A = torch.from_numpy(np.load(plan["adj_npy"]))
        st = torch.load(plan["model_ckpt"], map_location="cpu", weights_only=False)
        self.model = GraphDepth(st["L"], st["H"], st.get("hidden", 64), st["depth"])
        self.model.load_state_dict(st["state"]); self.model.eval()
        self.t0 = int(plan["t_index"]); self.window = int(plan["window"])
        self.horizon = int(plan["horizon"])
        self.temp_input_bytes = len(self.nodes) * self.window * 4
        self.out = np.lib.format.open_memmap(plan["targets_out"], mode="w+", dtype=np.float32,
                                             shape=(len(self.requests), len(self.trow), self.horizon))

    def compute(self, bodies, req):
        i = req["index"]
        x = np.zeros((len(self.nodes), self.window), dtype=np.float32)
        x[self.local_rows] = self.local[i]                          # local history only
        for (oid, _ver), body in bodies.items():                    # remote values as received
            n, t = oid[1:].split("|")
            col = int(t) - self.t0 - i
            if not (0 <= col < self.window): raise ValueError(f"slice out of window: {oid} req {i}")
            x[self.row[int(n)], col] = np.frombuffer(body, dtype=np.float32)[0]
        with torch.no_grad():
            out = self.model(self.A, torch.from_numpy(x)[None]).numpy()[0]
        tg = np.ascontiguousarray(out[self.trow], dtype=np.float32)
        self.out[i] = tg
        return dict(input_sha256=hashlib.sha256(x.tobytes()).hexdigest(),
                    targets_sha256=hashlib.sha256(tg.tobytes()).hexdigest())

    def finish(self):
        self.out.flush()
