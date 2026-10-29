# Limitations

- The empirical results cover a single owner and a single receiver.
- Object versions are fixed; TTL and content revision are not evaluated.
- The graph task uses 4 B scalar objects; application frames are about 7.35x the object
  payload. Object granularity and encapsulation are separate issues.
- The controlled service uses one synthetic Zipf trace and one seed; it is not a natural
  production trace.
- The reported `J` uses a frozen median publication cost; the owner's measured service time
  includes ledger/flush and is reported separately.
- The 288 B / 576 B budgets are persistent payload quotas, not the 8 GB device's physical
  memory limit; the experiments do not simulate physical memory exhaustion.
- Multi-receiver shared publication, source-side bounded materialization caches and real
  working-set quotas are future work.
- Candidate coverage, not solver precision, dominates the gap at wider budgets; this is a
  post-hoc diagnostic, not a deployable policy.
