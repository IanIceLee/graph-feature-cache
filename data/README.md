# Data

Raw datasets are **not** included in this repository and are **not** covered by the
Apache-2.0 license. The code expects the following local layout (ignored by git):

```text
data/
└─ metr_la/
   ├─ processed.npz     # keys: obs, mask, adj, val_end
   └─ partition_2.npz   # key: labels
```

Sources:

- METR-LA / PEMS04 / PEMS08 are public traffic datasets distributed through the
  [DCRNN repository](https://github.com/liyaguang/DCRNN) and the
  [STGCN repository](https://github.com/VeritasYin/STGCN_IJCAI-18).
- Please follow the terms of the original data providers. This repository does not
  redistribute the raw data.

Reference files used for the archived experiments (for provenance only; not shipped):

| file | bytes | SHA256 |
|---|---:|---|
| `processed.npz` | 10876579 | `a5ffbfd33d40faa3bf775e72f343ee7203a27ceaf65579657d5099e823442d3c` |
| `partition_2.npz` | 267 | `bed256d998bfe58a49fbe6b77faf5e86dcb7d6fdae830ba70d684fa76215958c` |

Use `python scripts/check_data.py --data-dir data/metr_la` to check the layout.
