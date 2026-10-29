# graphcache-core

Core algorithm and runtime code:

| module | role |
|---|---|
| `policy.py` | eight strategies, LRU/LFU/GDS, gain and budgeted selector |
| `plans.py` | causal/plan estimators and candidate construction |
| `service_op.py` | publication operation: encode, decode check, digest, register |
| `consumer.py` | receiver-side cache, hit/miss, admission, eviction and complete input |
| `owner.py` | source-side publication service with idempotent requests |

The receiver and owner import the framework package. From the repository root:

```bash
pip install -e ./framework -e ./core
python core/examples/synthetic_demo.py
```
