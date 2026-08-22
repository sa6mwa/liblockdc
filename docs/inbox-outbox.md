# liblockdc Inbox/Outbox Design Specification

Status: proposed; transaction contract reviewed against liblockdc and lockd

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
- The workflow owns the transaction identifier after creation or explicit
  adoption, the participant ledger, and the terminal decision. It does not
  implement a new coordinator.
- The normal dispatch path passes the newly committed outbox key directly to a
  local dispatcher thread. It does not query the namespace.
- Indexed `query_keys()` is a recovery and reconciliation mechanism only.
- A lease on an outbox key is its time-bounded claim. The lease and the
  post-acquisition record recheck are authoritative; a notification or query
  result is only a candidate.
- The library owns no external broker transport and invokes no host callbacks
  on its dispatcher thread. Host code pulls claimed jobs and reports their
  result.

## Verified Transaction Contract

The workflow layer is an adapter over two existing transaction contracts. It
must preserve their durable semantics, rather than forcing both through one
low-level call.

### Remote lockd

`lc_acquire_req.txn_id` joins a key to an existing transaction. A lockd
acquire without one returns a server-issued xid; a workflow-created identifier
must therefore use the xid grammar accepted by lockd. Every state, metadata,
and attachment operation through the resulting `lc_lease` carries that
transaction id and stages its change.

Remote lockd XA is implicit at the client boundary: acquire the first
participant without `txn_id`, take its server-issued xid, and supply that xid
when acquiring every additional participant. Stage through the corresponding
leases. The workflow then releases every enrolled lease as a vote:
`rollback=0` on every participant commits, while a rollback vote makes the
outcome rollback. No state becomes publicly visible merely because one
participant has released; the final decision occurs only after the complete
participant set has voted. Lockd records the durable decision and applies the
staged participant set; it is not a per-key publish loop. The workflow must
not call `lc_txn_prepare()`, `lc_txn_commit()`, `lc_txn_rollback()`, or the TC
decision surface for this path: those are coordinator/resource-manager control
operations, not the ordinary application-client finalizer.

### Local Pouch

Pouch stages every operation made through a lease with a non-empty
transaction id. Its local transaction decision is
`lc_client->txn_commit()` or `lc_client->txn_rollback()` with the exact
participant set. The decision durably merges the participant record, applies
staged state and attachments, clears matching leases, and supports replay on
open/recovery. After a successful Pouch decision, participant lease handles
are closed locally; they are not released one by one.

### Workflow adapter rules

- A normal `begin()` creates an unbound workflow transaction. On remote lockd,
  its first participant acquire omits `txn_id`, captures the backend-issued id
  from the returned lease, and every later participant acquire joins that id.
  On Pouch, the adapter creates its private non-empty/no-slash transaction id
  before the first participant acquire and supplies it to every participant.
  Neither backend detail is exposed as a caller-assembled identifier.
- An advanced join may supply a compatible existing transaction id solely to
  transfer a lease already acquired under that id into the workflow. The
  workflow validates it, then owns its eventual terminal decision; it never
  invents or silently rewrites an id supplied for this purpose.
- Every domain, inbox, and outbox lease obtained or adopted by the workflow is
  recorded exactly once as a `(namespace, key)` participant.
- On remote lockd, `commit()`/`rollback()` release every enrolled lease with
  the corresponding vote. On Pouch, they issue one explicit local decision
  over the ledger.
- The workflow retains every enrolled lease until the terminal outcome. For
  remote lockd it releases every lease in deterministic participant order; for
  Pouch it decides the complete local ledger.
- A successful terminal decision consumes every enrolled lease. Calling
  `release()` on an enrolled lease independently is impossible through the
  workflow participant surface. A remote workflow tracks per-participant vote
  progress so a retry or recovery release never omits an outstanding vote.
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

typedef struct lc_workflow_begin {
  /* Optional: only to adopt a raw lease already bound to this transaction. */
  const char *join_txn_id;
} lc_workflow_begin;

int (*new_workflow)(lc_client *self, const lc_workflow_config *config,
                    lc_workflow **out, lc_error *error);

struct lc_workflow {
  int (*begin)(lc_workflow *self, const lc_workflow_begin *request,
               lc_workflow_transaction **out, lc_error *error);
  int (*new_dispatcher)(lc_workflow *self,
                        const lc_outbox_dispatcher_config *config,
                        lc_outbox_dispatcher **out, lc_error *error);
  void (*close)(lc_workflow *self);
};

struct lc_workflow_transaction {
  int (*acquire)(lc_workflow_transaction *self,
                 const lc_workflow_participant_request *request,
                 lc_workflow_participant **out, lc_error *error);
  int (*adopt_lease)(lc_workflow_transaction *self, lc_lease **lease_io,
                      lc_workflow_participant **out, lc_error *error);
  int (*append_outbox)(lc_workflow_transaction *self,
                        const lc_outbox_entry *entry, lc_source *payload,
                        lc_outbox_receipt *out, lc_error *error);
  int (*accept_inbox)(lc_workflow_transaction *self,
                      const lc_inbox_message *message,
                      lc_inbox_accept_result *out, lc_error *error);
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

- `lc_workflow_config_init()`, `lc_workflow_begin_init()`, participant
  acquisition records, and every transparent entry/retry/configuration record
  have matching initializers.
- Callers pass semantic IDs and routing metadata; liblockdc owns reserved-key
  construction, payload attachment naming, valid transaction-id generation,
  participant tracking, post-commit notification, and lease references. A
  begin request may carry an existing compatible transaction id only for the
  explicit interoperation/adoption case below.
- `txn->acquire()` is the normal way to obtain a domain lease inside the
  workflow transaction. It supplies the workflow transaction id and retains
  the participant for terminal processing. It returns a workflow participant
  receiver whose non-terminal surface mirrors the normal lease state,
  metadata, keepalive, and streaming attachment operations. It deliberately
  exposes no `release()` or transaction-decision operation.
- `txn->adopt_lease()` accepts an already-active transaction-bound lease only
  when its transaction id equals the workflow transaction id. It transfers
  terminal ownership to the transaction, clears the caller's raw pointer on
  success, and returns the same constrained participant receiver. This is the
  compatibility path for a domain lease acquired before entering the workflow
  surface, and therefore requires the advanced begin request to carry that
  lease's transaction id.
- An outbox receipt contains the stable outbox identity and `effect_key` needed
  for logs and foreign-system calls without revealing storage layout.
- Inbox acceptance reports `accepted` or `duplicate` as successful structured
  outcomes. It does not force callers to inspect an error string to distinguish
  a normal duplicate from a conflict.
- Transaction commit is the single success boundary. It invokes the existing
  remote-release or Pouch-decision adapter as appropriate; there is no
  separate `signal()` call for the application to forget.
- Dispatcher jobs expose immutable envelope metadata, streaming payload access,
  and only the terminal/renewal operations valid for their owned claim.

Errors must identify the failed semantic operation and relevant identity
(`operation_id`, inbox identity, or outbox receipt) without logging payload
bytes or credentials. The API must reject contradictory configuration before a
worker thread starts.

### Transaction creation, joining, and ownership

The normal application path is `begin()` followed by `txn->acquire()`; the
application never assembles a transaction id or decides a participant itself.
Without `join_txn_id`, the remote adapter's first `txn->acquire()` omits
`txn_id` and captures the non-empty id returned on its lease; every later
remote acquire carries that exact id. The Pouch adapter instead generates and
supplies its private transaction id before its first acquire, because a Pouch
acquire does not mint one. The begin request's optional `join_txn_id` is for
the exceptional case where a raw liblockdc lease was acquired first. It must
meet the selected backend's identifier rules (the remote lockd contract is a
compact 20-character lowercase base32 xid) and becomes the workflow
transaction id. `adopt_lease()` then accepts only a lease carrying exactly that
id.

Once a lease is acquired through, or transferred to, the transaction, terminal
ownership belongs to the workflow. Application code receives only its
participant receiver for staged state/metadata/attachment work; it cannot
release that underlying lease or independently make a transaction decision.
`commit()` and `rollback()` consume all enrolled leases, close their handles,
and perform exactly one backend-appropriate terminal operation.

### Lua surface

The Lua binding exposes the same lifecycle as owned userdata rather than a
second, callback-based workflow model. Its names may be idiomatic Lua, but its
semantics must match the C surface:

```lua
local workflow = client:new_workflow({ namespace = "app-workflow" })
local txn = workflow:begin({ operation_id = request_id })

local order = txn:acquire({ namespace = "orders", key = order_id,
                            owner = "orders-api", ttl_seconds = 30 })
order:update(order_update_source)
txn:append_outbox(entry, payload_source)
local result = txn:commit()

local dispatcher = workflow:new_dispatcher({ claim_ttl_seconds = 30 })
dispatcher:start()
local job = dispatcher:next(1000)
if job then
  local payload = job:open_payload()
  -- host-owned Lua code performs the foreign effect here.
  job:complete()
end
```

Lua receives explicit result values and normal `nil, error` failures. Payload
objects retain the binding's streaming semantics. `dispatcher:next()` runs in
the calling Lua context; the native dispatcher thread never enters a Lua VM or
invokes a Lua callback. This keeps the facility usable by any Lua host without
assuming its scheduler, mailbox, or runtime-lifetime rules.

### Cross-language parity

The C and Lua surfaces must have parity for workflow creation, transaction
begin/commit/rollback, outbox append, inbox acceptance, dispatcher lifecycle,
job inspection, payload streaming, renewal, retry, completion, and
dead-lettering. Host integrations may add conveniences, but may not weaken the
durable semantics or replace direct job ownership with dispatcher callbacks.

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

`lc_workflow` and its transaction receiver provide the workflow transaction
wrapper over existing lockd and Pouch transaction facilities. The receiver
surface in [Consumer Experience and Public Surface](#consumer-experience-and-public-surface)
is the intended public boundary.

The transaction has an explicit participant ledger. The application obtains a
domain participant through `txn->acquire()` (or transfers an already matching
lease through `txn->adopt_lease()`), mutates through that receiver, and leaves
terminal processing to the workflow. The wrapper acquires and records
inbox/outbox leases itself. It stages the inbox/outbox key and attachment under
the same transaction id.

The workflow must choose its duplicate barrier before the application performs
the associated domain mutation: `append_outbox()` and `accept_inbox()` acquire
their deterministic record keys with the create-only precondition. A committed
matching record yields the normal duplicate outcome before a new domain effect
is staged; an immutable mismatch is a conflict. A currently leased record is
retried or reported as in-progress according to the bounded request policy,
never treated as a fresh duplicate.

### Outbox append

1. The caller begins a workflow transaction and calls `append_outbox()` before
   staging the associated domain mutation.
2. `append_outbox()` derives the deterministic outbox key, applies the
   create-only duplicate barrier, and records its lease as a participant.
3. The caller acquires/adopts and stages domain mutation leases under the same
   transaction id. The attachment and envelope metadata stage with the outbox
   participant.
4. The backend-specific terminal adapter makes the domain changes, outbox
   intent, and payload visible together, or rolls all of them back.
5. Only after a successful terminal decision does liblockdc notify the local
   dispatcher with the returned outbox key.

Submitting the same `(operation_id, effect_id)` again is idempotent only when
all immutable fields match. A conflicting repeat fails visibly.

### Inbox acceptance

1. The caller begins a workflow transaction for the incoming message.
2. `accept_inbox()` derives the inbox key, applies the create-only duplicate
   barrier, and records the inbox participant.
3. For an accepted message, the caller acquires/adopts domain leases and
   appends resulting outbox intent(s) under that transaction.
4. The terminal adapter persists the receipt, domain changes, and resulting
   intent(s) together. A matching committed receipt reports `duplicate` and
   creates no new logical operation or outbox intent.
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

### Thread-isolation and shutdown constraints

The dispatcher thread is a liblockdc coordination thread only. It must not
invoke caller-owned C callbacks, Lua, Kore, or any other host-runtime code.
The only handoff to host execution is an owned job returned from `next()`;
there is no callback registration API. This keeps thread affinity, runtime
lifetime, and host scheduling under the application's control.

The dispatcher owns its client/session and its thread lifecycle. Callers must
not rely on a dispatcher-owned client, lease, payload reader, or thread being
usable from another process or as a host-runtime execution context. A job
returned from `next()` is the explicit owned boundary for a host worker.

`stop()` prevents new claims, stops accepting direct notifications as work, and
wakes blocked `next()` callers. It does not manufacture completion, retry, or
dead-letter transitions for jobs already handed to host workers. `wait()` joins
the dispatcher within its timeout; it does not run or forcibly terminate host
work. The application gives its workers a bounded shutdown grace period. A job
that remains unfinished is left claimed until its lease expires and is then
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
not the authority. In remote lockd they may remain staged and therefore
invisible to public reads until the terminal release; Pouch may expose the
same transition earlier. The v1 portability guarantee is one active lease and
fenced terminal ownership, not identical mid-claim public projection timing.
On host loss, remote lockd rolls back the unfinalized lease transaction; Pouch
allows a later lease holder to reclaim an expired durable claim after the
envelope recheck.

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
Pouch and the repository's compose-backed remote lockd E2E environment.

1. A normal begin starts implicit XA on remote lockd by acquiring its first key
   without a transaction id, captures the backend-issued id, and uses that
   exact id for at least one additional key. On Pouch, it creates a private
   valid transaction id before the first acquire and uses it for at least two
   keys. Neither requires a caller-assembled identifier. An invalid or
   mismatched `join_txn_id` is rejected before ownership transfer.
2. One workflow participant ledger containing domain mutation, inbox/outbox
   key, and payload attachment commits atomically; rollback exposes none of
   them. The remote case finalizes by releasing every enrolled lease with the
   same commit or rollback vote; the Pouch case finalizes through one
   `txn_commit`/`txn_rollback` decision with the ledger.
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
   durable and observable.
10. Payload reads remain streaming under fragmented reads, large attachments,
   cancellation, and backend failover/reopen; tests prove no full-payload
   materialization.
11. Competing remote dispatchers and shared-root Pouch dispatchers produce one
    active lease-authorized claim and recover abandoned claims. Tests must not
    assert identical public visibility of a live `claimed` envelope across the
    two backends.
12. Indexed recovery has measured latency and bounded I/O with a large retained
    terminal population, continuous state-transition churn, and an
    un-compacted Pouch log. The test records index freshness and verifies that
    payload size does not change discovery cost.
13. Stop/wait behavior leaves incomplete claims for later expiry recovery and
    never manufactures completion.
14. The dispatcher never invokes host callbacks or host-runtime code on its
    thread; a host worker receives work only through `next()` and owns its
    execution context.
15. Shutdown stops new claims, wakes blocked `next()` callers, joins the
    dispatcher within the configured deadline, and permits an active host job
    to recover through lease expiry after its grace period.
16. C and Lua integration tests prove the same observable workflow outcomes:
    idempotent append/accept, explicit duplicate results, streamed payload
    handoff, and terminal job transitions without dispatcher-thread callbacks.
17. Transaction ownership is enforced: a workflow participant exposes no
    release/decision method; adoption from another transaction fails; the Pouch
    terminal decision contains every enrolled participant; and a consumed
    participant cannot be reused.

## Proof Obligations and Open Decisions

The following are not generic lockd deployment concerns; they are the remaining
component-specific design or proof obligations:

1. Finalize ABI names and the precise ownership transfer rules for
   `txn->acquire()` and `txn->adopt_lease()`; the semantic requirements above
   are fixed.
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
