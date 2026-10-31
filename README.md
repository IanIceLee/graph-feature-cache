# graph-feature-cache

Code for communication- and publication-cost-aware receiver-side feature caching for
distributed graph inference.

The repository is split into two packages:

- `framework/` — codec, source adapters, graph predictor, process runner, event verifier,
  and cross-process transport.
- `core/` — cache policies, estimators, budgeted selection, the publication operation,
  and the owner/consumer cache execution.

The repository contains the source code, experiment configurations, documentation, and a
frozen calibration summary. Raw datasets and model checkpoints are not redistributed; see
`data/README.md`, `models/README.md`, and `docs/data.md`.

## Quick start

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -e ./framework -e ./core
pip install -r requirements.txt
pytest -q
python core/examples/synthetic_demo.py
```

The synthetic demo starts one owner process and one receiver process on loopback, then
independently verifies the recorded events. It does not need METR-LA, PyTorch, or a Jetson.

## Repository layout

```text
framework/graphcache_framework/   # base framework
core/graphcache_core/             # core algorithms and runtime
core/examples/                    # synthetic end-to-end demo
experiments/                      # archived experiment drivers (see experiments/README.md)
configs/                          # small templates and frozen configurations
calibration/                      # frozen publication-cost summary
data/                             # README only; raw data not included
models/                           # README only; checkpoint not included
scripts/                          # data/model checks and demo launcher
docs/                             # architecture, protocol, data/model, limitations
tests/                            # unit tests
```

## What is and is not included

- Included: code, tests, templates, documentation, the frozen calibration summary, and the
  synthetic demo.
- Not included: METR-LA/PEMS raw data, the model checkpoint, raw run logs, result CSVs,
  private cross-machine credentials, and internal packaging scripts.
- The paper's tables were produced from frozen runs; the archived drivers are provided for
  auditability, not as a turnkey benchmark. See `docs/reproducibility.md`.

## License

Code is licensed under the Apache License 2.0. Copyright 2026 Yanbing Li. See `LICENSE`
and `NOTICE`. Data and model weights are not covered by this license.

## Citation

If you use this software, please cite the software using `CITATION.cff` and cite the
accompanying manuscript when its final bibliographic information is available:

Yanbing Li and Zhong Li. “Communication and Publication Cost-Aware Receiver Feature
Caching for Distributed Graph Computing.” Submitted to *Cluster Computing*.

The manuscript DOI and final publication details can be added after publication.
