"""Make the monorepo packages importable when scripts are run from this folder."""
import os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (os.path.join(ROOT, "framework"), os.path.join(ROOT, "core")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
