# Reproducibility

## Minimal path (no external data, no PyTorch)

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -e ./framework -e ./core
pip install -r requirements.txt
pytest -q
python core/examples/synthetic_demo.py
```

Expected demo output (fixed 6-object/12-request workload):

```text
strategy     transfer_B  pubs  hits verified
none                816     6     0     True
comm_only           560     6     6     True
joint               560     6     6     True
full_retain         560     6     6     True
PASS
```

The exact numbers are for this small demo, not the paper's METR-LA table.

## Protocol gate (no external data)

```bash
cd experiments
python t0_gate.py
```

It runs eight strategies under both source rules plus a negative mutation battery; the
last line should be `GATE PASS`.

## Full graph task (METR-LA)

1. Put `processed.npz` and `partition_2.npz` under `data/metr_la/` (see `data/README.md`).
2. Put the compatible checkpoint at `models/metr_la_d2_s42_best.pt`.
3. From the repository root:
   ```bash
   python experiments/t1_configs.py
   python experiments/t1_reference.py
   python experiments/t1_smoke.py
   python experiments/t1_run.py
   ```
4. The drivers write `runs/`, `checks/` and `metrics/`; those are ignored by git.

## Controlled service

```bash
python experiments/t2_workload.py
python experiments/t2_trial.py
python experiments/t3_ablation.py
python experiments/t4_repeat.py
```

The paper's exact tables come from frozen runs; the drivers are provided for auditability
and may produce slightly different wall-clock values on another machine. No raw run logs
or result CSVs are shipped.

## Hardware notes

The archived cross-machine setup used an RTX 3090 host as the owner and a Jetson Orin Nano
8 GB as the receiver over USB RNDIS. The formal graph model and publication operations ran
on CPU. The public demo uses loopback and does not need either device.
