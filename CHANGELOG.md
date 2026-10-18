# Release notes

## 0.21.0

- Upgrade cpkt to 0.5.0 with OpenSSL 3.6.5 initialization-failure and
  static-plugin pthread-key cleanup fixes; keep cpktdb at 0.3.0. Cover failed
  TLS-key creation and repeated encrypted-Pouch static-plugin unload with
  key-capacity checks and Valgrind.
- Reject Pouch mutations when required thread-local tracking keys cannot be
  initialized. Preserve application-owned thread-local data and unwind locks
  safely across repeated failures in shared and single-writer modes.
- Give Lua stateful-outbox fixtures at least a full second for checkpoint I/O
  while retaining short expiry-recovery coverage across wall-clock boundaries.
  Keep auxiliary Pouch stores inside the cleared test root for repeatable runs.
- Release the disabled fallback logger on SDK unload or process exit, and avoid
  creating it while checking ownership of an application-supplied logger.
- Reclaim thread JSON runtimes and Pouch TLS keys on unload. Report outstanding
  tracked allocations after library-owned lifetime cleanup.
- Enable allocation tracking throughout C/Lua E2E and PostgreSQL integration
  tests. Fail on captured leak diagnostics and cover library unload/reload,
  borrowed logger ownership, failed opens, and static shutdown with Valgrind.

The core shared-library ABI remains **5**; the PostgreSQL provider ABI remains
**0**. The Pouch storage format is unchanged.

## 0.20.0

- Add opt-in process-wide runtime tracking for the built-in allocator, disabled
  by default. Select plain or tracking callbacks once at startup; custom hooks
  bypass tracking and disabled allocation has no tracking-flag check.
- Track live built-in allocations and report remaining
  counts and bytes at ordinary exit or shared-library unload. Use inline headers
  and fixed counters without separate metadata allocations. Reclaim idle Pouch
  process lock entries when closing a store.
- Complete dispatcher worker ownership release before shutdown reports success,
  preventing allocator use after the caller has closed its client.
- Compile core objects once for static/shared libraries and the HTTPS fixture once
  per configuration. Reuse the sanitizer build for E2E, build only selected fuzz
  targets, and avoid repeating identical SDK configuration checks. Acknowledge
  HTTPS fixture upload expectations immediately to eliminate fallback waits.
- Fix background Pouch compaction exhausting musl thread stacks. Reuse a bounded
  heap buffer for payload copying and candidate checksums, preserving large I/O
  chunks and incremental CRC validation without per-record scratch allocation.
- Let source-tree static consumers select the static libatomic archive instead
  of inheriting an absolute shared-library dependency.
- Include integration executables in the cross-runtime build target, so clean
  cross-release builds produce both compaction regression tests before CTest.
- Avoid repeated host fixtures and preliminary package checks during release.
  Build and run native archive consumers together, and build only the selected
  Valgrind test targets. Final artifact verification remains mandatory.
- Stop native checks at the first failure and reject empty test selections.
- Make the local E2E interrupt fixture deterministic during startup and verify
  that interruption tears down the owned Podman stack.

The core shared-library ABI remains **5**; the PostgreSQL provider ABI remains
**0**. The Pouch storage format is unchanged.

## 0.19.0

- Add managed XA coordination across local Pouch and PostgreSQL participants.
  Ordinary state and attachments, SQL, and optional inbox/outbox work share one
  durable decision. Recovery resolves prepared branches without replaying SQL.
- Add the separately linked `liblockdc_postgres` static/shared provider using
  cpktdb's public C89 facade. PostgreSQL remains optional: core artifacts and
  their dependency requirements are identical with the capability on or off.
- Add public scoped connection borrows and Lua transaction orchestration,
  including rollback on callback failure and guarded lease/outbox completion.
- Expose public XA journal, resource and branch operations in Lua, including
  native status, bounded prepared inventory, manual prepare/resolution and
  recovery. Add optional `lockdc.postgres` registration for module loading or
  static preloading, and a separate PostgreSQL Lua rock. Pending raw decisions
  and replay preserve the C result as a fourth Lua return value.
- Add independently configurable completed-record and coordinator-history
  retention, with 30-day age defaults and optional count limits. Incomplete
  transactions and live coordinator ownership remain protected.
- Harden coordinator fencing, abandoned-owner recovery, terminal replay,
  outbox publication gates, receipt ownership and Lua borrow cleanup.
- Verify mixed transactions across two PostgreSQL servers and Pouch on
  PostgreSQL 14–18. Service tests use rootless Podman Kube and assert teardown.
- Replace c.pkt.systems with cpkt 0.3.0 and optional cpktdb 0.3.0. Upgrade to
  lonejson 0.45.0 and libpslog 0.12.0; liblql remains at 0.3.0. The PostgreSQL
  provider ships in a separate SDK archive.
- Honor PostgreSQL provider selection in fresh configurations, so clean release
  builds include the selected provider SDKs. Explicit CMake selections take
  precedence; unselected fresh builds remain PostgreSQL-free.
- Fix strict C89 64-bit limit expressions and large-file access on 32-bit Linux.
- Bound release performance comparisons to smaller fixtures and 90-second
  measurement limits, retaining five samples, all core metrics and the 1.25x
  speedup requirement. Reduce benchmark preparation and server restart waits.

The core shared-library ABI advances from **4 to 5**. Rebuild/relink downstream
consumers for this release; the PostgreSQL provider has its own ABI 0.

PostgreSQL participation uses the local Pouch coordinator. Remote lockd
participation and remote lockd outboxes are unsupported.
