# Architecture

## Roles

- **owner** (`graphcache_core/owner.py`): holds remote feature objects and serves them.
  Under `source_reuse`, one instance per `(object, content_version)` is retained for the
  run. Under `fresh`, every new miss creates a new instance. Every publication executes
  materialize -> encode -> decode check -> digest -> register.
- **receiver** (`graphcache_core/consumer.py`): owns local history, queries the persistent
  cache, fetches misses from the owner, validates content, assembles the complete input,
  and runs the graph task. The current request's temporary input is separate from the
  persistent cache; eviction never removes an input already used by the current request.

## Object identity

A feature object is identified by `(object_id, content_version)`.
`object_id` encodes dataset/node-or-sensor/time slice; the request id is not part of the
identity. The owner assigns an instance id and a publication id when it materializes a new
instance. The verifier checks content identity, instance identity and publication events
separately.

## Event flow

- owner: `register`, `send`
- receiver: `send`, `hit`, `admit`, `evict`, `compute`
- request ids, instance ids and publication ids join the two sides.

`graphcache_framework/verifier.py` rebuilds residency, transfers and publications from
both event streams; it does not trust self-reported counters. `runlib.py` wraps one attempt
and the verifier.

## Selection

The receiver scores each acquired candidate with
`g_i = k_comm,i * b_i + lambda * k_pub,i * q_i`
and selects a set under the persistent payload budget. The selector considers a greedy
prefix, a density-skip greedy set and the best single item, returning the best total score.
The 1/2 static guarantee is in the paper appendix.

## Source rules

- `source_reuse`: the owner keeps an instance; a receiver miss retransmits it, so the
  publication term is zero.
- `fresh`: each new miss materializes a new instance; a cache hit avoids both transfer and
  a new publication.
