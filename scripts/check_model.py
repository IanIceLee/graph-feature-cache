#!/usr/bin/env python3
"""Check a local graph-prediction checkpoint; weights are not redistributed."""
import argparse
import hashlib
import os
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--checkpoint", default="models/metr_la_d2_s42_best.pt")
    a = ap.parse_args()
    if not os.path.exists(a.checkpoint):
        print(f"MISSING {a.checkpoint}")
        return 1
    data = open(a.checkpoint, "rb").read()
    print("bytes", len(data))
    print("sha256", hashlib.sha256(data).hexdigest())
    try:
        import torch
        st = torch.load(a.checkpoint, map_location="cpu", weights_only=False)
        print("keys", sorted(st.keys()))
        print("L", st.get("L"), "H", st.get("H"), "hidden", st.get("hidden"), "depth", st.get("depth"))
    except Exception as e:
        print("torch_load_error", repr(e))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
