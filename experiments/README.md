# Archived experiment drivers

These scripts are the drivers used to produce the archived runs. They are provided for
auditability, not as a polished benchmark suite.

- `t0_gate.py` runs protocol positives and negative mutations on a small synthetic
  workload; it needs no external data.
- `t1_*.py` run the METR-LA rolling-window task; they need `data/metr_la` and a compatible
  checkpoint under `models/`.
- `t2_*.py`, `t3_ablation.py`, `t4_repeat.py` run the controlled publication service.
- `b_regression.py` runs a small GDS/window regression.

The paper's exact tables come from frozen runs; the drivers document the protocol but do
not by themselves regenerate every paper number. See `docs/experiment_protocol.md` and
`docs/reproducibility.md`.

Cross-machine launchers and internal packaging scripts are intentionally not included in
the public repository.

Note: `t2_trial.py` is a legacy 240-request driver. The paper's controlled-service tables use the 180-request suffix protocol in `t3_ablation.py`.
