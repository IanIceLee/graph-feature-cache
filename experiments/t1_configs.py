#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T1 configuration: 288 rolling METR-LA prediction windows consumed request by request.

Node sets are kept separate and are never collapsed:
  targets  164 region sensors the consumer must predict (its own partition)
  closure  176 nodes needed by a depth-2 forward pass from those targets (row direction)
  local    the 164 target sensors, whose history the consumer owns
  remote   closure \\ local, 12 sensors whose history must be fetched
Objects are sensor--timeslice scalars with a content version, so the 11 overlapping steps
of adjacent windows can hit.
"""
import hashlib, json, os, sys
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

DATA = os.environ.get("GRAPH_CACHE_METR_DIR", os.path.join(ROOT, "data", "metr_la"))
CKPT = os.environ.get("GRAPH_CACHE_MODEL", os.path.join(ROOT, "models", "metr_la_d2_s42_best.pt"))
WINDOW, HORIZON, DEPTH, N_REQ, Q = 12, 12, 2, 288, 1.0


def closure_rows(A, targets, d):
    """Dependency closure following the propagation direction used by the forward pass:
    output[v] sums over u with A[v,u] != 0, so v depends on the nonzeros of row v."""
    R = np.zeros_like(A); P = A.copy()
    for _ in range(d):
        R |= P; P = (P @ A) > 0
    nodes = set(targets.tolist())
    for t in targets: nodes |= set(np.where(R[t])[0].tolist())
    return np.array(sorted(nodes), dtype=np.int64)


def main():
    z = np.load(os.path.join(DATA, "processed.npz"))
    obs = z["obs"][:, :, 0].astype(np.float32); adj = z["adj"].astype(np.float32)
    va = int(z["val_end"])
    lab = np.load(os.path.join(DATA, "partition_2.npz"))["labels"]
    region = int(np.argmax([int((lab == i).sum()) for i in range(int(lab.max()) + 1)]))
    targets = np.where(lab == region)[0].astype(np.int64)
    closure = closure_rows(adj > 0, targets, DEPTH)
    remote = np.array([n for n in closure if n not in set(targets.tolist())], dtype=np.int64)
    assert len(closure) == 176 and len(remote) == 12, (len(closure), len(remote))

    t0 = va - WINDOW
    n_slices = WINDOW + N_REQ
    cfg_dir = os.path.join(ROOT, "configs"); os.makedirs(cfg_dir, exist_ok=True)
    data_dir = os.path.join(ROOT, "data", "t1"); os.makedirs(data_dir, exist_ok=True)

    # local history the consumer owns: ONLY the 164 target sensors
    loc = np.stack([np.ascontiguousarray(obs[t0 + i:t0 + i + WINDOW, targets].T)
                    for i in range(N_REQ)])
    local_path = os.path.join(data_dir, "local_history.npy")
    np.save(local_path, np.ascontiguousarray(loc, dtype=np.float32))
    A = np.ascontiguousarray(adj[np.ix_(closure, closure)], dtype=np.float32)
    adj_path = os.path.join(data_dir, "closure_adj.npy"); np.save(adj_path, A)

    # object plan: one request per prediction window, 144 remote slices each
    requests = []
    for i in range(N_REQ):
        objs = [[f"n{int(n)}|{t0 + i + s}", 0] for s in range(WINDOW) for n in remote]
        requests.append(dict(request_id=f"q{i}", step=i, index=i, objects=objs))
    positions = {}
    for i, r in enumerate(requests):
        for oid, _ in r["objects"]: positions.setdefault(oid, []).append(i)
    items = [dict(id=f"n{int(n)}|{t}", b=4, m=4, q=Q, q_hat=Q) for n in remote
             for t in range(t0, t0 + n_slices)]

    spec = dict(kind="metr", data_dir="data/metr_la", nodes=[int(x) for x in remote],
                t_index=int(t0), n_slices=int(n_slices), q=Q, window=WINDOW)
    src_path = os.path.join(cfg_dir, "t1_source.json")
    json.dump(spec, open(src_path, "w", encoding="utf-8"), indent=1)

    base = dict(kind="t1", window=WINDOW, horizon=HORIZON, requests=N_REQ,
                targets=[int(x) for x in targets], closure=[int(x) for x in closure],
                local_nodes=[int(x) for x in targets],
                remote_nodes=[int(x) for x in remote],
                target_rows=[int(np.where(closure == t)[0][0]) for t in targets],
                local_npy=os.path.relpath(local_path, ROOT).replace(os.sep, "/"),
                adj_npy=os.path.relpath(adj_path, ROOT).replace(os.sep, "/"),
                model_ckpt="models/metr_la_d2_s42_best.pt", t_index=int(t0),
                n_slices=int(n_slices), val_end=int(va),
                local_sha256=hashlib.sha256(np.load(local_path).tobytes()).hexdigest(),
                adj_sha256=hashlib.sha256(A.tobytes()).hexdigest(),
                targets_sha256=hashlib.sha256(targets.tobytes()).hexdigest(),
                closure_sha256=hashlib.sha256(closure.tobytes()).hexdigest(),
                remote_sha256=hashlib.sha256(remote.tobytes()).hexdigest(),
                objects=len(items), requests_plan_sha256=hashlib.sha256(
                    json.dumps(requests).encode()).hexdigest())
    json.dump(base, open(os.path.join(cfg_dir, "t1_config.json"), "w", encoding="utf-8"), indent=1)
    # per-strategy plans share the same request plan; only strategy / cap / selection differ
    json.dump(dict(items=items, positions=positions, requests=requests),
              open(os.path.join(cfg_dir, "t1_plan_base.json"), "w", encoding="utf-8"))
    print(json.dumps({k: base[k] for k in ("targets", "closure", "local_nodes", "remote_nodes",
                                          "objects", "t_index", "n_slices")}, default=len)[:400])
    print("remote", [int(x) for x in remote])
    print("local_npy", local_path, np.load(local_path).shape)


if __name__ == "__main__":
    main()
