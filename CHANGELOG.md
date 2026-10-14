# Release notes

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
