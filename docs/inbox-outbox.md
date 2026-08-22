# liblockdc Inbox/Outbox Design Specification

Status: proposed

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
- The normal dispatch path passes the newly committed outbox key directly to a
  local dispatcher thread. It does not query the namespace.
- Indexed `query_keys()` is a recovery and reconciliation mechanism only.
- A lease on an outbox key is its time-bounded claim. The lease and the
  post-acquisition record recheck are authoritative; a notification or query
  result is only a candidate.
- The library owns no external broker transport and invokes no host callbacks
  on its dispatcher thread. Host code pulls claimed jobs and reports their
  result.

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
  Immutable idempotency key sent to the foreign system. It is never regenerated
  on retry or dead-letter replay.

**Inbox identity**
  The tuple `(consumer_id, source_kind, source_id, message_id)`. The
  `consumer_id` distinguishes independent consumers of the same source event.

**Claim**
  An active lease on one outbox key. A claim expires automatically if its owner
  stops renewing it.

**Dispatcher notification**
  A bounded in-memory handoff containing an already committed outbox key. It is
  a performance hint, not the durable source of work.

## Namespace and Key Layout

`lc_inbox_outbox_config` supplies one non-empty `workflow_namespace`. Both
record classes live in that namespace; it may be the same as, or distinct from,
the application's domain-state namespace.

The following reserved key prefix is owned by liblockdc:

```text
__lockdc_io/v1/inbox/<encoded-consumer>/<inbox-identity-digest>
__lockdc_io/v1/outbox/<operation-id-digest>/<effect-id-digest>
```

Key components are deterministic, bounded encodings or digests. liblockdc
never creates a key by directly concatenating unescaped source-supplied
identifiers. The original identifiers remain in the state body and are checked
when an existing key is reused. A mismatch is a conflict, not a duplicate.

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
effect_key
kind                 caller-defined bounded routing label
destination          caller-defined transport target
content_type
headers              bounded key/value metadata
trace_context        optional bounded trace metadata
ordering_key         optional serialization domain
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
```

The attachment name is fixed by the component, for example `payload`. It is
written transactionally with the record. `lc_outbox_job_open_payload()` returns
an `lc_source` over that attachment; it must preserve real bounded-buffer
streaming for both backends.

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

The public API needs a workflow transaction wrapper over the existing lockd and
Pouch transaction facilities. Exact C names remain to be chosen; the required
behavior is:

```c
int lc_workflow_transaction_begin(lc_client *client,
                                  const lc_workflow_config *config,
                                  const char *operation_id,
                                  lc_workflow_transaction **out,
                                  lc_error *error);

int lc_workflow_transaction_append_outbox(
    lc_workflow_transaction *self, const lc_outbox_entry *entry,
    lc_source *payload, lc_outbox_receipt *out, lc_error *error);

int lc_workflow_transaction_accept_inbox(
    lc_workflow_transaction *self, const lc_inbox_message *message,
    lc_inbox_accept_result *out, lc_error *error);

int lc_workflow_transaction_commit(lc_workflow_transaction *self,
                                   lc_error *error);
int lc_workflow_transaction_rollback(lc_workflow_transaction *self,
                                     lc_error *error);
```

The application stages its domain-state mutations through leases enlisted in
the same workflow transaction. The wrapper stages the inbox/outbox key and
attachment in that transaction as well.

### Outbox append

1. The caller begins or joins a workflow transaction and stages domain changes.
2. `append_outbox()` derives the deterministic outbox key and either stages a
   new immutable intent or validates that a matching one already exists.
3. The attachment and metadata stage in the same transaction.
4. Commit makes the domain changes and the outbox intent visible together, or
   makes neither visible.
5. Only after successful commit does liblockdc notify the local dispatcher with
   the returned outbox key.

Submitting the same `(operation_id, effect_id)` again is idempotent only when
all immutable fields match. A conflicting repeat fails visibly.

### Inbox acceptance

1. The caller begins or joins a workflow transaction for the incoming message.
2. `accept_inbox()` derives the inbox key from its inbox identity.
3. A new message persists its receipt, domain changes, and resulting outbox
   intent(s) in one transaction.
4. A matching existing receipt reports a duplicate and creates no new logical
   operation or outbox intent.
5. The source is acknowledged only after successful durable acceptance.

If an incoming source payload is part of duplicate validation, the supplied
digest must match the stored digest. A mismatch is a conflict.

## Dispatcher

The dispatcher owns a thread and a client/session suitable for its backend. It
does not run user code. A host worker calls `next()`, receives an owned claimed
job, performs the foreign effect, and calls `complete()`, `retry()`, or
`dead_letter()`.

Illustrative surface:

```c
int lc_outbox_dispatcher_open(const lc_outbox_dispatcher_config *config,
                              lc_outbox_dispatcher **out, lc_error *error);
int lc_outbox_dispatcher_start(lc_outbox_dispatcher *self, lc_error *error);
int lc_outbox_dispatcher_next(lc_outbox_dispatcher *self, long timeout_ms,
                              lc_outbox_job **out, lc_error *error);
int lc_outbox_dispatcher_stop(lc_outbox_dispatcher *self, lc_error *error);
int lc_outbox_dispatcher_wait(lc_outbox_dispatcher *self, long timeout_ms,
                              lc_error *error);

int lc_outbox_job_open_payload(lc_outbox_job *self, lc_source **out,
                               lc_error *error);
int lc_outbox_job_renew(lc_outbox_job *self, long ttl_seconds,
                        lc_error *error);
int lc_outbox_job_complete(lc_outbox_job *self, lc_error *error);
int lc_outbox_job_retry(lc_outbox_job *self, const lc_retry *retry,
                        lc_error *error);
int lc_outbox_job_dead_letter(lc_outbox_job *self,
                              const lc_failure *failure, lc_error *error);
```

### Direct-key fast path

After an outbox transaction commits, liblockdc passes the exact outbox key to
the dispatcher. The dispatcher attempts to acquire that key directly and does
not perform a namespace query.

The in-memory notification path is bounded. It may coalesce duplicate keys. If
it cannot retain another notification, it records a recovery-needed condition
and wakes the dispatcher; it never makes durable work depend on an unbounded
memory queue.

The outbox record must be committed before notification. A process failure
between those actions is safe because recovery discovers the durable key later.

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

After acquiring a lease, the dispatcher rereads and validates the record:
state, timing, and claim generation must still permit dispatch. Completion,
retry, and dead-letter transitions require the current claim lease. A stale
claim cannot change a later claimant's record.

Retry atomically increments `attempt_count`, records a bounded diagnostic, and
sets `not_before`. Exhausting the configured retry budget transitions to
`dead_letter`; it never silently removes the record. Dead-letter replay retains
the original `effect_key`.

## Recovery and Indexed Reconciliation

Direct notification is the normal dispatch path. Index querying exists solely
to repair conditions that a local notification cannot cover:

- dispatcher startup;
- notification overflow;
- claims that expired after process or worker loss;
- retry deadlines not retained by a stopped dispatcher; and
- an optional infrequent reconciliation cadence.

The dispatcher uses `lc_query_keys()` against the workflow namespace with the
indexed query engine and a bounded page size. It selects envelope fields such
as `record_type`, `dispatch_state`, `not_before`, and claim expiry, then receives
keys only. It does not load payload attachments during discovery.

The recovery query must use an explicit freshness policy appropriate to the
backend, including `refresh=wait_for` where that is required to make the Pouch
index observe committed state after restart. Its performance and freshness are
an implementation acceptance gate, not an assumption.

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

There is no global ordering promise. An optional non-empty `ordering_key`
identifies a serialization domain.

Before ordering is offered as a v1 guarantee, the implementation must define a
deterministic, lease-protected ordering-key lock and prove that it is held from
claim through terminal transition. Without that lock, ordering is explicitly
unsupported rather than best-effort.

## Resource Limits and Observability

Configuration must bound:

- operation, effect, source, consumer, kind, destination, and header sizes;
- attachment size when the application requests a maximum;
- dispatcher notification capacity and host-job capacity;
- claim TTL, renewal cadence, retry delay, and maximum attempts; and
- retained diagnostics and dead-letter retention.

The component exposes read-only statistics and last-error state, including:

```text
direct_notifications
notification_overflows
recovery_queries
recovered_claims
ready | claimed | retry_wait | completed | dead_letter counts
claim_losses
payload_open_failures
dispatcher lifecycle state
```

These counters and logs are observability. Durable state keys remain the sole
recovery source of truth.

## Required Verification

Implementation is not complete until the following behavior is proven for both
Pouch and a remote lockd endpoint.

1. Domain mutation, inbox/outbox key, and payload attachment commit atomically;
   rollback exposes none of them.
2. Repeated outbox append with the same operation/effect identity creates one
   intent; a conflicting immutable repeat fails.
3. Inbox redelivery is idempotent and a payload-digest conflict is rejected.
4. The ordinary append-to-dispatch path performs no recovery query.
5. A process crash after commit and before notification is recovered by another
   dispatcher from an indexed query.
6. A crash after the foreign effect and before completion redelivers the same
   `effect_key`.
7. Stale completion, retry, and dead-letter operations cannot alter a later
   claim.
8. Retry scheduling, retry-budget exhaustion, dead-lettering, and replay are
   durable and observable.
9. Payload reads remain streaming under fragmented reads, large attachments,
   cancellation, and backend failover/reopen; tests prove no full-payload
   materialization.
10. Competing remote dispatchers and shared-root Pouch dispatchers produce one
    active claim and recover abandoned claims.
11. Indexed recovery has measured latency and bounded I/O with a large retained
    terminal population, continuous state-transition churn, and an
    un-compacted Pouch log. The test records index freshness and verifies that
    payload size does not change discovery cost.
12. Stop/wait behavior leaves incomplete claims for later expiry recovery and
    never manufactures completion.

## Proof Obligations and Open Decisions

The following are not generic lockd deployment concerns; they are the remaining
component-specific design or proof obligations:

1. Define the public workflow transaction wrapper so it composes naturally
   with caller-owned domain leases and the existing transaction coordinator.
2. Decide the exact v1 routing metadata surface and its limits.
3. Decide whether ordering-key serialization is part of v1. If yes, define and
   test its lock-key protocol before implementation.
4. Establish quantitative recovery-query performance budgets from the Pouch and
   remote-lockd benchmark matrix before treating indexing as sufficient.
5. Define terminal-record and optional source-payload retention/replay policy.

## Design Basis

The design follows the transactional outbox rule that the durable intent is
committed with the business state and that a later relay can repeat an external
send. It also follows the idempotent-consumer rule that processed-message
identity is persisted as part of the durable processing transaction.

- [Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox)
- [Idempotent Consumer pattern](https://microservices.io/patterns/communication-style/idempotent-consumer.html)
- [AWS transactional outbox guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)
