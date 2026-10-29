#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Jetson porting pre-check. Prepares only: it never connects to another machine.

Checks configuration, model mapping, data readability, device availability and environment
compatibility, and lists the settings that must be changed for a two-machine run.
"""
import argparse, json, os, platform, socket, sys

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
DEFAULT = os.path.join(ROOT, "configs", "jetson_ports.json")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=DEFAULT)
    ap.add_argument("--role", default="consumer", choices=["owner", "consumer", "both"])
    a = ap.parse_args()
    cfg = json.load(open(a.config, encoding="utf-8"))
    rep = dict(role=a.role, platform=dict(system=platform.system(), release=platform.release(),
                                          machine=platform.machine(), python=platform.python_version()))
    try:
        import numpy as np, torch
        rep["libs"] = dict(numpy=np.__version__, torch=torch.__version__,
                           cuda_available=bool(torch.cuda.is_available()),
                           device_count=int(torch.cuda.device_count()) if torch.cuda.is_available() else 0)
        if torch.cuda.is_available():
            rep["libs"]["device_name"] = torch.cuda.get_device_name(0)
            rep["libs"]["capability"] = ".".join(str(x) for x in torch.cuda.get_device_capability(0))
    except Exception as e:
        rep["libs"] = dict(error=repr(e))

    # model mapping
    try:
        import torch
        from .model import GraphDepth
        ck = cfg["model"]["checkpoint"]
        st = torch.load(ck, map_location="cpu", weights_only=False)
        m = GraphDepth(st["L"], st["H"], st.get("hidden", 64), st["depth"])
        missing = m.load_state_dict(st["state"], strict=True)
        rep["model"] = dict(checkpoint=ck, exists=os.path.exists(ck), depth=int(st["depth"]),
                            L=int(st["L"]), H=int(st["H"]), hidden=int(st.get("hidden", 64)),
                            keys=sorted(st["state"].keys()), load_strict_ok=True,
                            params=int(sum(p.numel() for p in m.parameters())))
    except Exception as e:
        rep["model"] = dict(error=repr(e))

    # data readability
    data = cfg["data"]
    rep["data"] = {}
    for name, path in data.items():
        if not isinstance(path, str): continue
        isdir = os.path.isdir(path)
        rep["data"][name] = dict(path=path, exists=os.path.exists(path), is_dir=isdir,
                                 bytes=(None if isdir or not os.path.exists(path)
                                        else os.path.getsize(path)))
    try:
        import numpy as np
        z = np.load(os.path.join(data["metr_dir"], "processed.npz"))
        rep["data"]["processed_shapes"] = {k: list(np.shape(z[k])) for k in ("obs", "mask", "adj")}
        rep["data"]["val_end"] = int(z["val_end"])
    except Exception as e:
        rep["data"]["error"] = repr(e)

    # network settings: describe, do not connect
    net = cfg["network"]
    rep["network"] = dict(owner_bind=net["owner_bind"], port=int(net["port"]),
                          consumer_host=net["consumer_host"], mode=net.get("mode", "loopback"),
                          connected=False,
                          note="pre-check never opens a socket to consumer_host")
    free = True
    try:
        s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((net["owner_bind"], int(net["port"]))); s.close()
    except OSError as e:
        free = False; rep["network"]["bind_error"] = repr(e)
    rep["network"]["port_bindable_locally"] = free

    rep["absolute_paths"] = dict(files=[], note="public release uses relative paths and CLI arguments")
    rep["device_plan"] = dict(owner=cfg["device"]["owner"], consumer=cfg["device"]["consumer"],
                              note="CPU inference is bit-reproducible here; switching the consumer to "
                                   "CUDA changes float summation order and would invalidate the "
                                   "1e-6 comparison against a CPU reference -- re-run the reference "
                                   "on the same device if that changes.")
    rep["ready_for_two_machine"] = bool(rep["model"].get("load_strict_ok") and free
                                        and all(v.get("exists") is not False
                                                for k, v in rep["data"].items()
                                                if isinstance(v, dict)))
    print(json.dumps(rep, indent=1, default=str))
    os.makedirs(os.path.join(ROOT, "checks"), exist_ok=True)
    json.dump(rep, open(os.path.join(ROOT, "checks", "jetson_precheck.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
