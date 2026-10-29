# Paper-to-code mapping

| Paper item | Driver / module | Notes |
|---|---|---|
| Figure 1 | `docs/architecture.md` | mechanism schematic; no data |
| Table 1 | `experiments/t1_configs.py` | node sets and closure; needs METR data |
| Table 2 | `graphcache_core/policy.py`, `graphcache_framework/sources.py` | symbols and strategies |
| Table 3, Figure 2 | `experiments/t1_run.py` | METR-LA graph task; needs data + checkpoint |
| Table 4, Figure 3 | `experiments/t3_ablation.py` | 50% controlled service suffix; no external data |
| Table 5 | `experiments/t3_ablation.py` | 25% controlled service suffix |
| Table 6, Figure 4 | `experiments/t3_ablation.py` | component ablations |
| Table 7, Figure 5 | `experiments/xe_lambda.py` | local lambda replay |
| Table 8 | `experiments/t1_run.py` | resource/phase observation |
| Table 9 | `experiments/t3_ablation.py` / `t2_trial.py` | owner service time and end-to-end repeats; `t2_trial` is a legacy 240-request driver |
| Table 10 | post-hoc diagnostic (not shipped) | J0-J3 decomposition; see paper appendix/source data |
| Table B1-B3 | archived historical data (not shipped) | prior application/training cases |

Cross-machine launchers and internal packaging scripts are intentionally excluded. The
single-host drivers implement the same cache protocol; the paper's cross-machine run used
the same owner/consumer code with a remote consumer.
