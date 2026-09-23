# Worst-case scenario campaign

`make worst-case-scenario` is a manual Pouch diagnostic campaign. It is not a
test-suite alias and nothing in `test-all`, `prerelease`,
`prerelease-hardening`, or `release` invokes it. Its purpose is to make a
deliberately hostile but bounded local workload reproducible when investigating
resource use, recovery, or maintenance liveness.

The default campaign has a 30-minute wall-clock deadline and a 10 GiB local
disk budget. Each phase owns and removes its Pouch root before the next phase
starts, so the budget is a peak-workspace envelope rather than an accumulated
release-artifact budget. The defaults can be adjusted with `WORST_CASE_*`
variables, but an increased workload should also receive an explicit larger
timeout and disk budget.

The command first prepares its native benchmark and Lua e2e binaries. Its
default timed stage limits then total 27 minutes: eleven minutes for the
C89/replay/core phase, thirteen for the two outbox phases, and three for Lua
execution. The remaining three minutes belong to recursive Make and
timeout-process handoff under the outer 30-minute deadline.

The terminal-history cardinality and attachment volume are intentionally capped
below the 10-GiB availability envelope: larger 24,576-by-twelve and
4-GiB-per-root variants spent their entire outbox stage creating one compacted
root before they could exercise recovery or retention. The default mix keeps
the same multi-segment, high-churn shape while reserving time for both the
multiwriter and stateful-retention phases. Operators can scale any
`WORST_CASE_OUTBOX_*` variable for a machine-specific capacity investigation.

The campaign is serial and combines these independent failure pressures:

- the encrypted, shared-writer, unclean C89-shaped replay capture;
- core Pouch state, query, attachment, queue, compaction, and concurrent
  shared-root churn probes, with tighter segments and higher mutation density
  than prerelease hardening;
- a multi-segment Pouch outbox containing 1,024 ready envelopes, 1,024
  terminal envelopes, four historical rewrites per terminal envelope,
  256-KiB payload attachments, a 1-MiB segment target, and four competing
  dispatchers. The core phase separately retains the 64-KiB extreme-segment
  compaction workload;
- the corresponding stateful outbox flow after forced compaction and reopen,
  where every third effect dead-letters and capacity retention is constrained
  to 256 retained envelopes / 2 GiB. Every stateful job writes a 64-KiB
  checkpoint attachment. The benchmark waits for the observable reclaim
  counter, so it proves that retained stateful sidecars are reclaimed rather
  than merely scheduled;
- the Lua outbox end-to-end surface, including stateful checkpoints, handler
  outcomes, replay, dead-letter management, and borrowed-handle lifetimes.

It deliberately does not promise production throughput. The shared-writer
portion is an adversarial liveness and resource-bound test: competing
processes, compaction, retention, and large attachments are expected to reduce
throughput substantially. Success means all work converges, stateful sidecars
remain available until their parent reaches a durable terminal decision,
dead-letter capacity reclaim makes forward progress, the workload stays inside
its documented disk envelope, and every normally completed temporary root is
cleaned.

Useful controls:

```sh
make worst-case-scenario

# Keep the same shape while choosing a smaller exploratory corpus.
make worst-case-scenario WORST_CASE_OUTBOX_ROWS=1024 \
  WORST_CASE_OUTBOX_TERMINAL_ROWS=4096 \
  WORST_CASE_OUTBOX_PAYLOAD_BYTES=131072
```

Before doing work the target requires at least
`WORST_CASE_DISK_BUDGET_BYTES` free under the repository filesystem. The
current workload is intentionally below that envelope; this is a preflight,
not a generic quota mechanism for unrelated processes or an excuse to use
unbounded inputs.

An interrupted benchmark can leave a test-owned `/tmp/liblockdc-pouch-bench-*`
root behind because process termination preempts normal fixture teardown. The
next benchmark creation sweeps stale owned roots before opening its own root;
successful phases remove their roots immediately.
