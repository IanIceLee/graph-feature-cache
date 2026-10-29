#!/usr/bin/env python3
"""Check the local METR-LA data layout without redistributing the data."""
import argparse
import os
import sys

import numpy as np


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data/metr_la")
    a = ap.parse_args()
    p = os.path.join(a.data_dir, "processed.npz")
    q = os.path.join(a.data_dir, "partition_2.npz")
    ok = True
    for path in (p, q):
        if not os.path.exists(path):
            print(f"MISSING {path}")
            ok = False
    if ok:
        z = np.load(p)
        print("processed.npz keys", sorted(z.files))
        for k in ("obs", "mask", "adj"):
            if k not in z:
                print(f"MISSING key {k}")
                ok = False
        if "val_end" not in z:
            print("MISSING key val_end")
            ok = False
        lab = np.load(q)
        if "labels" not in lab:
            print("MISSING key labels")
            ok = False
        else:
            print("partition labels", int(lab["labels"].max()) + 1)
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
