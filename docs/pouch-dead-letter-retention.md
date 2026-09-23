# Pouch dead-letter retention

Pouch automatically reclaims outbox dead letters. The policy exists to keep a
failed foreign effect observable long enough to diagnose or replay without
letting an unattended dispatcher namespace become a permanent retention sink.
It applies only to local Pouch outboxes. Remote lockd deployments retain their
server-owned policy.

## Default policy

Every new Pouch outbox dead letter is retained for 30 days. A namespace is also
bounded to 1,024 retained dead-letter envelopes or 64 MiB of current logical
envelope, payload, checkpoint, and checkpoint-attachment data, whichever limit
is reached first. Capacity is a service-protection bound rather than a durable
admission quota: a short burst may exceed it while the low-priority reclaimer
works through an existing claim or a crashed terminal transition. Capacity
checks coalesce at most once per second per dispatcher, so a burst cannot turn
each terminal transition into a control-directory scan. Once a default
capacity breach is observed, maintenance reads marker bodies only through the
bounded capacity prefix. It may then walk the remaining directory names,
without opening their marker bodies, to find the next marker after its turn
cursor. It attempts one exact-key reclaim before yielding. Capacity is
therefore conservative—service protection takes precedence over exact
oldest-first archival order during overload.

An explicit `replay_dead_letters_on_startup` request takes precedence over
automatic retention. The dispatcher completes that startup replay before it
can reclaim an otherwise due or over-capacity marker, so an operator cannot
lose a requested replay to the default policy. During capacity reclamation the
advisory turn cursor also advances beyond its prior candidate before yielding;
one busy oversized parent cannot repeatedly select itself and starve later
markers.

`lc_outbox_config.dead_letter_retention_seconds` controls the time limit:

- zero selects the 30-day default;
- a positive value selects a Pouch-only retention period;
- `-1` explicitly disables automatic retention for that outbox namespace.

`dead_letter_max_count` and `dead_letter_max_bytes` similarly use their
defaults when zero. `SIZE_MAX` and `UINT64_MAX`, respectively, disable the
corresponding capacity bound. Applications that need long-lived archives should
export dead letters to dedicated backup storage, then allow the operational
namespace to reclaim them.

All processes using one outbox namespace must use the same retention
configuration. This is the same canonical-configuration rule that already
applies to claim, retry, and recovery settings.

Changing the time setting to `-1` also leaves any existing retention markers
untouched: that is the explicit operator choice to keep the corresponding dead
letters indefinitely. Re-enable retention or use the management APIs to retire
that backlog deliberately.

## Durable scheduling and safety

Retention never uses the query index and never runs a namespace-wide document
scan during delivery. A transition to `dead_letter` first writes a small,
fsynced Pouch control marker containing an opaque marker identity, its expiry,
the parent key, and measured logical retained bytes. Only after that marker is
durable does the claimed parent record receive the matching marker identity and
terminal state.

The dispatcher maintenance thread reads the control directory directly and
reclaims one exact parent key at a time. It does not inspect unrelated outbox,
application, queue, or state records. A marker that precedes a crashed terminal
write is retained while the original claim is active; once that claim resolves,
it is harmlessly discarded unless the parent is a dead letter with the same
identity. Replay clears the identity before returning the record to `pending`,
so an old marker cannot delete a later dead-letter generation of the same
effect. Replay retains the marker until the pending record commits. A failed
commit leaves the original dead letter scheduled, while an interrupted marker
cleanup after commit leaves only stale control data for maintenance to retire.
Explicit `delete_dead_letter()` removes the paired marker as part of normal
management cleanup. If power loss separates that parent deletion from
marker removal, the next reclaimer recognizes the absent parent as stale and
discards the marker safely.

Each live Pouch dispatcher performs the same bounded control-directory
discovery at most once per minute when no earlier known marker is due. This is
how a dispatcher notices a marker published later by another shared writer;
the cadence is private maintenance work and does not alter `next()` or
`next_with_state()`.

Stateful cleanup uses the existing parent-first-safe order: it removes the
checkpoint and its attachments while the dead-letter parent remains retryable,
then removes the payload attachments and parent envelope. A failed cleanup
leaves both the parent and its marker intact for retry. Compaction reclaims only
superseded log generations; the retention operation owns removal of live
dead-letter data.

Multiple Pouch writers can observe the same due marker. The normal outbox lease
is authoritative: one reclaimer obtains the dead-letter lease and the others
leave the marker for a later turn. Each dispatcher advances an in-memory marker
turn cursor even after a busy collision, so one long-held marker cannot starve
other due or over-capacity work. The cursor is advisory only; a restart safely
starts a new bounded turn. No dispatcher lock is held across normal delivery,
and no foreground pull waits for retention work.

When the control hierarchy is first created, Pouch syncs each parent directory
before publishing a marker, so a durable terminal parent cannot rely on a
directory entry lost by a power failure.

## Existing data

Dead letters created before this policy have no control marker. They remain
operator-visible and can be replayed, exported, or deleted through the normal
outbox API. Pouch does not perform an unbounded legacy namespace scan merely to
retrofit retention, because doing that at open or pull would recreate the
startup and memory failure mode this policy is designed to prevent. Operators
that want to retire an existing backlog should export it and use the existing
explicit delete controls.

## Operational consequences

- A live dispatcher is required for automatic reclamation. A stopped process
  cannot perform any local maintenance; the next dispatcher resumes due marker
  processing without reopening or replaying the namespace.
- `dispatcher:next()` and `dispatcher:next_with_state()` keep their existing
  direct-key and claim behavior. Retention is maintenance-thread work, not a
  query, reconciliation, or cache-warming side effect.
- Lua uses the same fields on `client:new_outbox({ ... })` as C. `-1` is the
  explicit time-retention opt-out; leaving fields absent selects the safe
  default. Lua's signed integer representation uses `false` for an unbounded
  `dead_letter_max_count` or `dead_letter_max_bytes` (the C equivalents are
  `SIZE_MAX` and `UINT64_MAX`).
- A retention failure is observable through the dispatcher error/status path
  and preserves the marker. It never silently discards a dead letter.
