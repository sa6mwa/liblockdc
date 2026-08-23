# liblockdc Inbox/Outbox Design Specification

Status: implemented v0 design; operational recovery and dead-letter controls
are part of the public workflow surface

## Purpose

This specification defines a `liblockdc` facility for durable inbox and
outbox workflows. It lets an application make a lockd-backed state transition,
record an external-effect intent, and later perform that effect without the
dual-write failure window.

The implementation uses existing liblockdc state, attachment, transaction,
lease, and query capabilities. It works against either a remote lockd endpoint
or local Pouch storage. It does not use lockd queues.

The component provides at-least-once handoff. It cannot guarantee exactly once
for an effect outside lockd/Pouch. The stable `effect_key` delivered to the
foreign system is the required idempotency mechanism for that boundary.

## Design Decisions

- One configured workflow namespace contains both inbox and outbox records.
- Inbox and outbox records are ordinary state keys. Payloads are immutable
  attachments, not JSON fields.
- State change, inbox/outbox intent, and payload attachment commit in one
  existing lockd/Pouch transaction.
- The first durable inbox or outbox record is acquired without `txn_id` and
  receives the endpoint-minted xid. The workflow propagates that xid to every
  later participant, owns the participant ledger and terminal decision, and
  does not implement a new coordinator.
- The normal dispatch path passes the newly committed outbox key to an
  internal dispatcher. The host sees work only through `workflow->next()`;
  it does not query the namespace or manage dispatcher internals.
- Indexed `query_keys()` is a recovery and reconciliation mechanism only.
- A lease on an outbox key is its time-bounded claim. The lease and the
  post-acquisition record recheck are authoritative; a notification or query
  result is only a candidate.
- The library owns no external broker transport and invokes no host callbacks
  on its dispatcher thread. Host code pulls claimed jobs and reports their
  result.

## Verified Transaction Contract

Remote lockd and Pouch distinguish a normal server-minted one-participant
lease from an explicit XA transaction. They must have the same terminal,
visibility, and recovery semantics for each mode.

### Remote lockd

`lc_acquire_req.txn_id` joins a key to an existing transaction. An acquire
without one returns a server-issued xid. The first workflow record acquire
intentionally omits `txn_id`; every later domain, inbox, outbox, metadata, or
attachment participant carries the returned transaction id and stages its
change.

The initial acquire is a normal one-participant transaction. When another key
joins with its returned xid, lockd performs its existing implicit XA flow. A
terminal workflow commit records and applies one durable commit or rollback
decision for the enrolled participant set; it is not a release-vote barrier.

### Local Pouch

Pouch mints an rs/xid-compatible identifier when an acquire omits `txn_id`.
That normal lease is staged and finalized directly on release. When another
participant joins with the returned xid, Pouch creates the durable XA
participant record; terminal release records and applies the decision for all
enrolled participants. Recovery replays a recorded decision or rolls an
expired undecided XA transaction back.

`lc_xid_new()` remains available as a general helper, but the workflow never
mints or accepts a caller-supplied transaction id. It propagates only the xid
returned by its first endpoint acquire.

### Workflow adapter rules

- `append_outbox()` or `accept_inbox()` is the first workflow operation. It
  acquires its deterministic record key without `txn_id`, receives the
  endpoint-minted xid, and creates the transaction receiver.
- Every later domain, inbox, and outbox lease uses that xid and is recorded
  exactly once as a `(namespace, key)` participant.
- `commit()`/`rollback()` releases the single normal lease directly. For XA it
  records one terminal decision and applies the enrolled participant set.
- A successful terminal decision consumes every enrolled lease. Calling
  `release()` on an enrolled lease independently is impossible through the
  workflow participant surface.
- If staging or the terminal decision fails, no post-commit notification is
  emitted. The transaction remains recoverable according to the selected
  backend's existing transaction recovery rules.

## Scope and Non-goals

The component covers:

- durable idempotent outbox intent recording;
- durable inbox deduplication;
- true streaming of arbitrary payload bytes;
- claims, retries, retry scheduling, expiry recovery, completion, and
  dead-letter state;
- direct same-process dispatch plus cross-process recovery; and
- equivalent observable behavior for Pouch and remote lockd endpoints.

It deliberately does not cover:

- a distributed transaction with a foreign broker, HTTP service, SMTP server,
  filesystem, or any other external-effect system;
- exactly-once foreign effects;
- a queue implementation or use of lockd queues as an implementation detail;
- a generic transport plugin or dispatcher-owned host callback; or
- a global ordering guarantee.

## Terms

**Operation ID**
  Caller-supplied stable identity for one business operation.

**Effect ID**
  Caller-supplied stable identity for one external effect belonging to an
  operation. An operation may create more than one effect.

**Effect key**
  Caller-supplied immutable idempotency key sent to the foreign system. It is
  never regenerated on retry or dead-letter replay, and a repeat operation
  must supply the same value.

**Inbox identity**
  The tuple `(consumer_id, source_kind, source_id, message_id)`. The
  `consumer_id` distinguishes independent consumers of the same source event.

**Claim**
  An active lease on one outbox key. A claim expires automatically if its owner
  stops renewing it.

**Dispatcher notification**
  A bounded in-memory handoff containing an already committed outbox key. It is
  a performance hint, not the durable source of work.

## Consumer Experience and Public Surface

The facility is a first-class receiver-style liblockdc surface, not a set of
key-format helpers or a framework integration. C applications, Lua
applications, and downstream hosts use the same durable concepts and lifecycle
without assembling reserved keys, transaction identifiers, leases, or
dispatcher notifications themselves.

### C receiver surface

The primary C entry point is an opaque workflow handle created from an existing
client. It follows the public library's receiver-function convention and
zero-initializable configuration/request records:

```c
typedef struct lc_workflow lc_workflow;
typedef struct lc_workflow_transaction lc_workflow_transaction;
typedef struct lc_workflow_participant lc_workflow_participant;
typedef struct lc_outbox_job lc_outbox_job;

int (*new_workflow)(lc_client *self, const lc_workflow_config *config,
                    lc_workflow **out, lc_error *error);

struct lc_workflow {
  int (*append_outbox)(lc_workflow *self, const lc_outbox_entry *entry,
                       lc_source *payload, lc_workflow_transaction **out,
                       lc_outbox_receipt *receipt, lc_error *error);
  int (*accept_inbox)(lc_workflow *self, const lc_inbox_message *message,
                      lc_workflow_transaction **out,
                      lc_inbox_accept_result *result, lc_error *error);
  int (*next)(lc_workflow *self, long timeout_ms, lc_outbox_job **out,
              lc_error *error);
  int (*get_stats)(lc_workflow *self, lc_workflow_stats *out,
                   lc_error *error);
  int (*reconcile)(lc_workflow *self, lc_error *error);
  int (*replay_dead_letter)(lc_workflow *self, const char *outbox_key,
                            lc_error *error);
  int (*delete_dead_letter)(lc_workflow *self, const char *outbox_key,
                            lc_error *error);
  int (*export_dead_letters)(lc_workflow *self,
                             const lc_dead_letter_export_opts *options,
                             lc_sink *dst, lc_dead_letter_export_res *out,
                             lc_error *error);
  void (*close)(lc_workflow *self);
};

struct lc_workflow_transaction {
  int (*acquire)(lc_workflow_transaction *self,
                 const lc_workflow_participant_request *request,
                 lc_workflow_participant **out, lc_error *error);
  int (*append_outbox)(lc_workflow_transaction *self,
                        const lc_outbox_entry *entry, lc_source *payload,
                        lc_outbox_receipt *out, lc_error *error);
  int (*commit)(lc_workflow_transaction *self, lc_error *error);
  int (*rollback)(lc_workflow_transaction *self, lc_error *error);
  void (*close)(lc_workflow_transaction *self);
};

struct lc_workflow_participant {
  /* The non-terminal lc_lease operations: describe, get/load, save/update,
     mutate/mutate_local, metadata/remove, keepalive, and attachment access. */
  /* No release or transaction-decision operation is exposed. */
  void (*close)(lc_workflow_participant *self);
};
```

Exact type and method names remain subject to ABI review, but these interaction
boundaries are fixed:

- `lc_workflow_config_init()`, participant acquisition records, and every
  transparent entry/retry/configuration record have matching initializers.
- Callers pass semantic IDs and routing metadata; liblockdc owns reserved-key
  construction, payload attachment naming, participant tracking, post-commit
  notification, and lease references. `effect_key` is caller supplied,
  immutable, and retained for every foreign-effect retry.
- `workflow->append_outbox()` or `workflow->accept_inbox()` is the first
  workflow operation. It acquires its deterministic record lease without a
  transaction id, records the endpoint-minted xid, and returns the transaction
  only after the duplicate barrier succeeds. A duplicate inbox result returns
  a successful structured result and no transaction.
- `txn->acquire()` is the normal way to obtain a domain lease inside the
  workflow transaction. It supplies the workflow transaction id and retains
  the participant for terminal processing. It returns a workflow participant
  receiver whose non-terminal surface mirrors the normal lease state,
  metadata, keepalive, and streaming attachment operations. It deliberately
  exposes no `release()` or transaction-decision operation. Later outbox
  entries use the same endpoint-minted xid through `txn->append_outbox()`.
- An outbox receipt contains the stable outbox identity and `effect_key` needed
  for logs and foreign-system calls without revealing storage layout. A
  matching committed outbox append returns that existing receipt as a successful
  `duplicate` result and returns no transaction; a caller therefore cannot
  accidentally repeat the associated domain mutation.
- Inbox acceptance reports `accepted` or `duplicate` as successful structured
  outcomes. It does not force callers to inspect an error string to distinguish
  a normal duplicate from a conflict.
- Transaction commit is the single success boundary. It invokes the endpoint's
  implicit-XA terminal decision; there is no separate `signal()` call for the
  application to forget.
- Dispatcher jobs expose immutable envelope metadata, streaming payload access,
  and only the terminal/renewal operations valid for their owned claim.

Errors must identify the failed semantic operation and relevant identity
(`operation_id`, inbox identity, or outbox receipt) without logging payload
bytes or credentials. The API must reject contradictory configuration before a
worker thread starts.

### Transaction creation and ownership

There is no public begin, join, or lease-adoption surface. To start outbound
work, `workflow->append_outbox()` derives the deterministic key and acquires
its record without `txn_id`. To start inbound work,
`workflow->accept_inbox()` does the same for the inbox receipt. The endpoint
returns a minted xid with that lease; liblockdc installs it in the owned
workflow transaction and supplies it for every later participant. A duplicate
inbox result is successful and returns no transaction.

Once a lease is acquired through the transaction, terminal ownership belongs
to the workflow. Application code receives only its participant receiver for
staged state/metadata/attachment work; it cannot release that underlying lease
or independently make a transaction decision. `commit()` and `rollback()`
consume all enrolled leases, close their handles, and perform exactly one
backend-appropriate terminal operation.

### Lua surface

The Lua binding exposes the same lifecycle as owned userdata rather than a
second, callback-based workflow model. Its names may be idiomatic Lua, but its
semantics must match the C surface:

```lua
local workflow = client:new_workflow({ namespace = "app-workflow" })
local txn, receipt = workflow:append_outbox(entry, payload_source)

local order = txn:acquire({ namespace = "orders", key = order_id,
                            owner = "orders-api", ttl_seconds = 30 })
order:update_raw(order_update_source)
txn:append_outbox(entry, payload_source)
local result = txn:commit()

local inbound_txn, accepted = workflow:accept_inbox(message)
if accepted.accepted then
  inbound_txn:append_outbox(entry, payload_source)
  inbound_txn:commit()
end

local job = workflow:next(1000)
if job then
  job:write_payload(foreign_request_body_sink)
  -- host-owned Lua code performs the foreign effect here.
  job:complete()
end
```

Lua receives explicit result values and normal `nil, error` failures. Payload
objects retain the binding's streaming semantics. `workflow:next()` runs in the
calling Lua context; the private native dispatcher never enters a Lua VM or
invokes a Lua callback. This keeps the facility usable by any Lua host without
assuming its scheduler, mailbox, or runtime-lifetime rules.

The Lua façade supplies the matching parent operations as `workflow:stats()`,
`workflow:reconcile()`, `workflow:replay_dead_letter(outbox_key)`,
`workflow:delete_dead_letter(outbox_key)`, and
`workflow:export_dead_letters(options, destination)`. `options.format` is
`"json"` or `"jsonl"`; an omitted destination returns the bounded export as a
Lua string, while the usual Lua file/fd destination forms stream directly.

### Cross-language parity

The C and Lua surfaces must have parity for workflow creation, first-operation
outbox/inbox transaction creation, commit/rollback, later outbox append,
parent `next()`, job inspection, payload streaming, renewal, retry, completion,
and dead-lettering. Host integrations may add conveniences, but may not weaken
the durable semantics or replace direct job ownership with callbacks.

## Namespace and Key Layout

`lc_inbox_outbox_config` supplies one non-empty `workflow_namespace`. Both
record classes live in that namespace; it may be the same as, or distinct from,
the application's domain-state namespace.

The following reserved key prefix is owned by liblockdc:

```text
__lockdc_io/v1/inbox/<inbox-identity-digest>
__lockdc_io/v1/outbox/<operation-id-digest>/<effect-id-digest>
```

Key components are deterministic, bounded digests. SHA-256 components use
unpadded base64url (43 characters), not hexadecimal, so the complete reserved
key remains below lockd's 128-character key limit. The inbox digest covers the
consumer and complete source identity. liblockdc never creates a key by
directly concatenating source-supplied identifiers. The original identifiers
remain in the state body and are checked when an existing key is reused. A
mismatch is a conflict, not a duplicate.

This layout makes an outbox key stable for its entire lifetime. Key names are
for direct addressing and namespace ownership, not the current portable query
predicate: existing `query_keys()` selectors match JSON state fields and return
the matching keys.

## Durable Record Model

The record body is compact JSON metadata with `application/json` content type.
Payload bytes are not embedded in it.

### Outbox key

An outbox record has immutable fields:

```text
record_type          "lockdc.outbox.v1"
operation_id
effect_id
effect_key           caller-supplied immutable foreign-effect idempotency key
kind                 caller-defined bounded routing label
destination          caller-defined transport target
content_type
headers              bounded key/value metadata
trace_context        optional bounded trace metadata
payload_digest
payload_bytes
created_at
```

It has mutable delivery fields:

```text
dispatch_state       pending | claimed | retry_wait | completed | dead_letter
attempt_count
not_before
claim_owner
claim_expires_at
last_error_code
last_error_message
completed_at
dead_lettered_at_unix
replay_count
replayed_at_unix
prior_dead_letter_error
```

The attachment name is fixed by the component, `payload`. It is written
transactionally with the record. `lc_outbox_job_write_payload()` streams that
attachment directly into a caller-owned `lc_sink`; it must preserve real
bounded-buffer streaming for both backends and must not materialize the full
payload behind a source-looking facade.

### Inbox key

An inbox record contains:

```text
record_type          "lockdc.inbox.v1"
consumer_id
source_kind
source_id
message_id
payload_digest       when the source payload participates in the contract
accepted_at
operation_id
resulting_outbox_ids
processing_state
```

The inbox key is the durable receipt. It need not retain the source payload
when the application has transformed it into an outbox attachment and no audit
or replay requirement calls for retention. If source-payload retention is
enabled, it uses an attachment with the same streaming rules as an outbox
payload.

## Atomic Operations

`lc_workflow` and its transaction receiver provide the workflow transaction
wrapper over existing lockd and Pouch transaction facilities. The receiver
surface in [Consumer Experience and Public Surface](#consumer-experience-and-public-surface)
is the intended public boundary.

The transaction has an explicit participant ledger. Its first inbox or outbox
record lease is acquired by the workflow without `txn_id`; the endpoint-minted
xid is then propagated to every later participant. The application obtains a
domain participant through `txn->acquire()`, mutates through that receiver,
and leaves terminal processing to the workflow. The wrapper stages inbox/outbox
keys and attachments under that same transaction id.

The workflow must choose its duplicate barrier before the application performs
the associated domain mutation. The first `workflow->append_outbox()` or
`workflow->accept_inbox()` call acquires its deterministic record key with the
create-only precondition before returning a transaction. A committed matching
record yields the normal duplicate outcome before a new domain effect is
staged; an immutable mismatch is a conflict. A currently leased record is
retried or reported as in-progress according to the bounded request policy,
never treated as a fresh duplicate.

### Outbox append

1. The caller calls `workflow->append_outbox()` before staging the associated
   domain mutation. It derives the deterministic outbox key, applies the
   create-only duplicate barrier, acquires it without `txn_id`, and returns a
   transaction carrying the endpoint-minted xid.
2. The caller acquires and stages domain mutation leases through that
   transaction. The attachment and envelope metadata stage with the outbox
   participant. Further effects use `txn->append_outbox()` and the same xid.
3. The backend-specific terminal adapter makes the domain changes, outbox
   intent, and payload visible together, or rolls all of them back.
4. Only after a successful terminal decision does liblockdc notify the local
   dispatcher with the returned outbox key.

Submitting the same `(operation_id, effect_id)` again is idempotent only when
all immutable fields, including `effect_key`, match. A conflicting repeat
fails visibly. A matching committed repeat returns the existing outbox receipt
with `duplicate` set and no transaction; it does not create or stage any new
domain or outbox participant.

### Inbox acceptance

1. The caller calls `workflow->accept_inbox()` for the incoming message. It
   derives the inbox key, applies the create-only duplicate barrier, acquires
   it without `txn_id`, and returns a transaction carrying the endpoint-minted
   xid for an accepted message.
2. The caller acquires domain leases and appends resulting outbox intent(s)
   under that transaction.
3. The terminal adapter persists the receipt, domain changes, and resulting
   intent(s) together. A matching committed receipt reports `duplicate` and
   creates no new logical operation or outbox intent.
4. The source is acknowledged only after successful durable acceptance.

If an incoming source payload is part of duplicate validation, the supplied
digest must match the stored digest. A mismatch is a conflict.

## Dispatcher

The dispatcher is private to `lc_workflow`: it owns a remote client clone or a
retained Pouch-session reference and its coordination thread, but never runs
user code. A Pouch workflow deliberately shares its one local session with the
parent workflow so default exclusive-root ownership remains valid. The host
calls the parent receiver's `workflow->next(timeout_ms, &job, error)`, receives
an owned claimed job, performs the foreign effect, and calls the job's
`complete()`, `retry()`, or `dead_letter()` operation. Dispatcher
configuration belongs to the workflow configuration; there is no public
dispatcher handle, start, stop, or signal surface.

### Direct-key fast path

After an outbox transaction commits, liblockdc internally passes the exact
outbox key to its dispatcher. The dispatcher attempts to acquire that key
directly and does not perform a namespace query.

The in-memory notification path and preclaimed-job handoff are bounded by
`notification_capacity`. The dispatcher stops claiming when the host handoff
is full and resumes when `next()` consumes a job. It may coalesce duplicate
keys. If it cannot retain another notification, it records a recovery-needed
condition and wakes the dispatcher; it never makes durable work depend on an
unbounded memory queue.

The outbox record must be committed before notification. A process failure
between those actions is safe because recovery discovers the durable key later.

### Thread-isolation and shutdown constraints

The dispatcher thread is a liblockdc coordination thread only. It must not
invoke caller-owned C callbacks, Lua, Kore, or any other host-runtime code.
The only handoff to host execution is an owned job returned from
`workflow->next()`; there is no callback registration API. This keeps thread
affinity, runtime lifetime, and host scheduling under the application's
control.

The dispatcher owns its remote client clone or retained Pouch-session reference
and its thread lifecycle. Callers must not rely on its client, lease,
payload-transfer handle, or thread being usable from another process or as a
host-runtime execution context. A job returned from `workflow->next()` is the
explicit owned boundary for a host worker.

`workflow->close()` prevents new claims and notifications, wakes blocked
`next()` callers, cancels a remote dispatcher request, and joins the private
dispatcher. `shutdown_timeout_ms` bounds each remote dispatcher request; zero
inherits an explicit root-client timeout or defaults to 30 seconds. Close never
abandons a live thread. It does not
manufacture completion, retry, or dead-letter transitions for jobs already
handed to host workers, run host work, or forcibly terminate it. The
application gives its workers a bounded shutdown grace period. A job that
remains unfinished is left claimed until its lease expires and is then
recovered by the normal durable recovery path.

### Claim and terminal transitions

```text
pending ---- claim ----> claimed ---- complete ----> completed
  ^                            |
  |                            +---- retry ----> retry_wait
  |                                               |
  +----------------------- eligible at not_before+

claimed ---- expiry recovery ----> pending
claimed / retry_wait ---- dead-letter ----> dead_letter
```

The active outbox-key lease is the claim authority and remains held while the
host owns the job. After acquiring it, the dispatcher performs a private
reread and validates state, timing, and claim generation before handing the
job out. Completion, retry, and dead-letter transitions require that same
lease. A stale claim cannot change a later claimant's record.

The envelope's `claimed` fields are durable diagnostics and recovery inputs,
not the authority. Both endpoints keep transaction-bound changes invisible to
public reads until the terminal release decision. On host loss, recovery rolls
an expired undecided transaction back, after which a later lease holder can
reclaim and recheck the durable envelope.

Retry atomically increments `attempt_count`, records a bounded diagnostic, and
sets `not_before`. Exhausting the configured retry budget transitions to
`dead_letter`; it never silently removes the record. Dead-letter replay retains
the original `effect_key`.

### Dead-letter operations

Dead letters are terminal durable envelopes until an application takes one
explicit management action. `replay_dead_letter(outbox_key)` is valid only for
a current dead-letter envelope. It atomically restores `pending`, resets the
delivery attempt count to zero, retains the immutable `effect_key`, increments
`replay_count`, writes `replayed_at_unix`, and preserves the previous terminal
diagnostic in `prior_dead_letter_error`. The normal dispatcher then receives
the exact key as an internal notification.

`delete_dead_letter(outbox_key)` is also restricted to a current dead letter.
It deletes the envelope and its fixed `payload` attachment under one workflow
lease/transaction, so no orphaned payload survives a successful delete. There
is no automatic retention/deletion scheduler in v0.

`export_dead_letters()` emits envelope state only: it never reads or writes
payload attachment bytes. JSON output is one array; JSONL output is one JSON
envelope per line. Export is deliberately bounded by `options.limit` (or the
workflow notification capacity when zero), so a caller can write a stable,
bounded audit/replay sink without materializing the result in liblockdc.

## Recovery and Indexed Reconciliation

Direct notification is the normal dispatch path. Index querying exists solely
to repair conditions that a local notification cannot cover:

- dispatcher startup;
- notification overflow;
- claims that expired after process or worker loss;
- retry deadlines not retained by a stopped dispatcher; and
- an optional infrequent reconciliation cadence.

`workflow->reconcile()` is an explicit, asynchronous request for this same
private recovery sweep; it does not expose dispatcher coordination or execute
host work in the caller. `replay_dead_letters_on_startup` is opt-in. When set,
the initial reconciliation first scans bounded pages of durable dead letters,
applies the reset-with-provenance replay transition, and signals the same
dispatcher before returning to normal pending/retry reconciliation. The option
is appropriate for an operator-controlled restart, not a substitute for a
foreign system's idempotency contract.

The dispatcher uses `lc_query_keys()` against the workflow namespace with the
indexed query engine and a bounded page size. Its portable indexed predicate
selects dispatchable `pending` and `retry_wait` states; direct-key claim then
rereads and validates the full envelope, `record_type`, timing, and active
lease authority before it can hand work to a host. It receives keys only and
does not load payload attachments during discovery.

Every new sweep establishes one explicit durable index boundary, then paginates
that same sweep without flushing per page. It uses the backend's indexed-query
freshness policy, including `refresh=wait_for`, so a restart can observe
committed state. Its performance and freshness are an implementation acceptance
gate, not an assumption.

In a one-process local Pouch deployment where every writer uses this component,
startup recovery and direct key notification can avoid a routine reconciliation
interval. Remote lockd and shared-root Pouch deployments still use the same
direct local handoff for newly committed records; they retain an infrequent
reconciliation cycle to recover work committed by failed or dispatcher-less
processes.

## Concurrency and Cross-process Behavior

Multiple dispatchers may address the same workflow namespace. Direct
notifications are process-local, but correctness is shared through durable
keys and leases:

- the originating process normally dispatches the key it just committed;
- another process may recover or concurrently attempt the same key;
- only the holder of the current outbox-key lease may run the work; and
- an abandoned claim becomes recoverable after lease expiry.

Pouch's ordinary deployment is local and uses its default exclusive-root writer
mode. Cross-process Pouch dispatch is an explicit shared-root deployment and
must use Pouch's shared-writer mode. It is a supported recovery/distribution
path, not the expected hot path. Remote lockd follows the same component
semantics without changing the direct-key notification model.

## Ordering

V1 makes no ordering guarantee and exposes no `ordering_key`. A client that
needs serialized foreign effects must enforce that serialization in its
business state or at the foreign destination. A future ordered protocol must
define an explicit lease-protected ordering model and version it separately.

## Resource Limits and Observability

Configuration must bound:

- operation, effect, source, consumer, kind, destination, and header sizes;
- attachment size when the application requests a maximum;
- dispatcher notification and preclaimed-job capacity;
- claim TTL, renewal cadence, retry delay, and maximum attempts; and
- retained diagnostics and dead-letter retention.

### Default delivery policy

The defaults are deliberately generous while preserving a bounded, observable
terminal outcome:

```text
claim_ttl                 5 minutes
max_attempts              100, including the first delivery attempt
retry_initial_delay       1 second
retry_multiplier          2
retry_max_delay           15 minutes
retry_jitter              full jitter
host_retry_delay_max      1 hour
shutdown_timeout_ms       30 seconds per dispatcher request
```

The calculated retry delay is exponentially increased to the cap and sampled
with full jitter. A host may provide a retry delay, for example from a foreign
system's backoff instruction, but liblockdc rejects values above
`host_retry_delay_max`. After the final failed attempt the record is
dead-lettered; it is never discarded automatically. These defaults and limits
are exposed through `lc_workflow_config`; zero selects the defaults. A retry
scheduled by a live workflow is held as a bounded delayed direct-key signal,
not a queue entry or a namespace scan. Scheduler overflow and work owned after
workflow shutdown remain recoverable through the durable reconciliation path.
A host renewing a claim for a longer foreign effect must do so before the
five-minute claim expiry.

The component exposes an MVP read-only, process-local snapshot through
`lc_workflow_get_stats()` / `workflow:stats()`, including:

```text
direct_notifications (all accepted internal key notifications)
notification_overflows
recovery_queries
recovered_claims (outbox keys rediscovered by reconciliation)
claim_losses
payload_open_failures
dispatcher lifecycle state
latest dispatcher error text
```

The snapshot also reports the current bounded notification and ready-job
backlogs. It does not query or aggregate durable namespace counts, and it is
reset on workflow recreation. These counters are observability only; durable
state keys remain the sole recovery source of truth.

## Required Verification

Implementation is not complete until the following behavior is proven for both
Pouch and the repository's compose-backed remote lockd E2E environment.

1. The first `append_outbox()` or `accept_inbox()` acquire omits `txn_id` and
   receives an endpoint-minted xid. Every later domain or outbox participant
   propagates that xid. There is no caller transaction-id construction, join,
   or lease-adoption surface.
2. One workflow participant ledger containing domain mutation, inbox/outbox
   key, and payload attachment commits atomically; rollback exposes none of
   them. Both endpoints finalize the explicit XA transaction with one durable
   terminal decision.
3. Repeated outbox append with the same operation/effect identity creates one
   intent; a conflicting immutable repeat fails.
4. Inbox redelivery is idempotent and a payload-digest conflict is rejected.
5. The ordinary append-to-dispatch path performs no recovery query.
6. A process crash after commit and before notification is recovered by another
   dispatcher from an indexed query.
7. A crash after the foreign effect and before completion redelivers the same
   `effect_key`.
8. Stale completion, retry, and dead-letter operations cannot alter a later
   claim.
9. Retry scheduling, retry-budget exhaustion, dead-lettering, and replay are
   durable and observable. Default policy tests prove 100 attempts including
   the first, full-jitter exponential delay from one second capped at fifteen
   minutes, a one-hour host-delay limit, and dead-lettering without deletion.
10. Payload reads remain streaming under fragmented reads, large attachments,
   cancellation, and backend failover/reopen; tests prove no full-payload
   materialization.
11. Competing remote dispatchers and shared-root Pouch dispatchers produce one
    active lease-authorized claim and recover abandoned claims. Tests must not
    assert identical public visibility of a live `claimed` envelope across the
    two backends.
12. The regression baseline seeds 256 pending records with a 16-key
    notification bound, proves paged indexed reconciliation delivers every
    record, and records wall-clock latency under a deliberately broad 30-second
    failure bound. A separate load benchmark before v1 release must extend this
    profile with retained terminal population, continuous state-transition
    churn, and an un-compacted Pouch log; payload size must not change
    discovery cost.
13. Parent close behavior leaves incomplete claims for later expiry recovery
    and never manufactures completion.
14. The dispatcher never invokes host callbacks or host-runtime code on its
    thread; a host worker receives work only through `next()` and owns its
    execution context.
15. Shutdown stops new claims, wakes blocked parent `next()` callers, joins
    the private dispatcher within the configured deadline, and permits an
    active host job to recover through lease expiry after its grace period.
16. C and Lua integration tests prove the same observable workflow outcomes:
    idempotent append/accept, explicit duplicate results, parent `next()`
    streamed-payload handoff, and terminal job transitions without dispatcher
    thread callbacks.
17. Transaction ownership is enforced: a workflow participant exposes no
    release/decision method; the transaction begins only from its deterministic
    first record lease; the Pouch terminal decision contains every enrolled
    participant; and a consumed participant cannot be reused.
18. A matching committed outbox duplicate returns its existing receipt with no
    transaction and cannot stage a second domain mutation or outbox intent.
19. Dead-letter export contains envelope JSON but no payload bytes; explicit
    replay keeps `effect_key`, resets the attempt, records replay provenance,
    and deletion removes both envelope and attachment atomically. The same
    outcomes are covered through the C and Lua surfaces, including opt-in
    startup replay.

## Proof Obligations and Open Decisions

The following are not generic lockd deployment concerns; they are the remaining
component-specific design or proof obligations:

1. Finalize ABI names and request records for first-operation
   `append_outbox()`/`accept_inbox()`, transaction participants, parent
   `next()`, and jobs; the semantic requirements above are fixed.
2. Decide the exact v1 routing metadata surface and its limits.
3. Establish quantitative recovery-query performance budgets from the Pouch and
   remote-lockd benchmark matrix before treating indexing as sufficient.
4. Define terminal-record and optional source-payload retention/replay policy.

## Design Basis

The design follows the transactional outbox rule that the durable intent is
committed with the business state and that a later relay can repeat an external
send. It also follows the idempotent-consumer rule that processed-message
identity is persisted as part of the durable processing transaction.

- [Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox)
- [Idempotent Consumer pattern](https://microservices.io/patterns/communication-style/idempotent-consumer.html)
- [AWS transactional outbox guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)
