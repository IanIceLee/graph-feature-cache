# Experiment protocol (summary)

## Graph task (T1 / C)

- METR-LA, 207 nodes, 164 target sensors, 176-node two-hop closure, 12 remote sensors.
- 288 rolling requests of 12-step history and 12-step horizon.
- One persistent payload budget of 288 B; the full-window reference uses 576 B.
- Strategies: none, LRU, LFU, GDS, comm_only, pub_only, joint, window_full.
- Three independent processes per strategy; predictions are compared with a same-device
  full-graph reference (element-wise tolerance `1e-6 + 1e-5*|ref|`).

## Controlled service (T2 / D)

- 40 heterogeneous objects (32-512 B), 240 requests, seed 7, Zipf 1.1.
- First 60 requests are a causal prefix; the last 180 are evaluated.
- Budgets: 3968 B (50%) and 1984 B (25%).
- `source_reuse` and `fresh` rules; every publication is a real materialize/encode/digest/
  register operation.
- `full_retain` is a reference with a 7296 B peak, not a same-budget competitor.

## Metrics

- object payload bytes, new publication events, hits, persistent peak, setup/runtime and
  end-to-end time;
- the combined objective `J = payload bytes + 100 * publication microseconds` is a proxy
  objective, not a wall-clock measurement;
- the frozen median publication cost is a declared price; actual owner service time is
  reported separately.

## Negative and boundary results

The paper keeps the 25% fresh case where the joint score changes the selected set but does
not reduce publications, the cases where online LRU/LFU/GDS have lower J, and the
candidate-coverage decomposition. See the paper's tables 4-10 and `docs/paper_mapping.md`.
