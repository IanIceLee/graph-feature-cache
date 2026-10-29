#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Source specifications shared by owner, consumer and verifier.

The owner serves payloads defined here; the verifier rebuilds the same bytes from the same
specification without looking at anything the consumer sent. Two sources:

  synthetic : objects are (id, version) with a fixed byte size; bytes are deterministic.
  metr      : objects are sensor--timeslice scalars, id "n<node>|<t0>", payload = float32
              observation at (t0, node); version is 0.
"""
import json
import os
import numpy as np

from .common import make_feature

METR_DIR = os.environ.get("GRAPH_CACHE_METR_DIR", "data/metr_la")


class SyntheticSource:
    kind = "synthetic"
    def __init__(self, spec):
        self.objects = [dict(id=o["id"], b=int(o["b"]), m=int(o.get("m", o["b"])),
                             q=float(o.get("q", 1.0))) for o in spec["objects"]]
        self.by_id = {o["id"]: o for o in self.objects}
    def oracle(self, oid, version):
        return make_feature(oid, int(version), self.by_id[oid]["b"])


class MetrSource:
    kind = "metr"
    def __init__(self, spec):
        self.dir = spec.get("data_dir", METR_DIR)
        self.window = int(spec.get("window", 12))
        self.t_index = int(spec.get("t_index", 0))    # absolute time index of slice 0
        z = np.load(f"{self.dir}/processed.npz")
        self.obs = z["obs"][:, :, 0].astype(np.float32)
        nodes = [int(x) for x in spec["nodes"]]
        ts = range(self.t_index, self.t_index + int(spec["n_slices"]))
        self.objects = [dict(id=f"n{n}|{t}", b=4, m=4, q=float(spec.get("q", 1.0)))
                        for n in nodes for t in ts]
        self.by_id = {o["id"]: o for o in self.objects}
        self._obs = self.obs
    def oracle(self, oid, version):
        node, t0 = oid[1:].split("|")
        return np.float32(self._obs[int(t0), int(node)]).tobytes()


def open_source(path):
    spec = json.load(open(path, encoding="utf-8"))
    kind = spec["kind"]
    if kind == "metr":
        d = spec.get("data_dir", METR_DIR)
        if not os.path.isabs(d):
            root = os.path.dirname(os.path.dirname(os.path.abspath(path)))
            spec["data_dir"] = os.path.join(root, d)
    if kind == "synthetic": return SyntheticSource(spec)
    if kind == "metr": return MetrSource(spec)
    raise ValueError(f"unknown source kind {kind}")


def table(src):
    return {"kind": src.kind, "objects": src.objects}
