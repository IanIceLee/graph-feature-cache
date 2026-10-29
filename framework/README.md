# graphcache-framework

Base framework code:

| module | role |
|---|---|
| `common.py` | deterministic feature bytes, wire codec, digests |
| `sources.py` | synthetic and METR source adapters |
| `model.py` | depth-parameterised graph predictor (optional PyTorch dependency) |
| `runner.py` | start owner/consumer processes, choose ports, collect outputs |
| `runlib.py` | one attempt plus independent verification |
| `verifier.py` | reconstruct hits, residency, transfers and publications from both sides |
| `remote_run.py` | run a consumer on a remote host from a job file |
| `jetson_precheck.py` | environment/data/model pre-check |

Install with `pip install -e ./framework`. The graph predictor needs the optional `torch`
extra: `pip install -e "./framework[torch]"`.
