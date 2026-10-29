#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="framework:core${PYTHONPATH:+:$PYTHONPATH}"
python core/examples/synthetic_demo.py
