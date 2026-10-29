# Model checkpoint

The graph-prediction checkpoint is **not** included in this repository and is **not**
covered by the Apache-2.0 license. The archived experiments used:

```text
models/metr_la_d2_s42_best.pt
```

| field | value |
|---|---|
| bytes | 24255 |
| SHA256 | `878bfa90a89cc2f3a424fe6a0b95a9b2d6d1a79fb43caffa66cb1b1207ed157d` |
| architecture | depth-2 graph propagation with persistence residual |
| checkpoint fields | `L`, `H`, `hidden`, `depth`, `state` |

Place a compatible checkpoint at `models/metr_la_d2_s42_best.pt` if you want to run the
METR-LA experiment drivers. The checkpoint is produced by the paper's fixed predictor
training procedure; this repository does not redistribute the weights.

Use `python scripts/check_model.py --checkpoint models/metr_la_d2_s42_best.pt` to check
the file and print its metadata.
