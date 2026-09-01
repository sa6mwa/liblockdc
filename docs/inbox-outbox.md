# liblockdc Transactional Messaging Design Specification

Status: inbox, outbox, operational recovery, and dead-letter controls are
implemented for Pouch. This document also specifies the command-receipt
extension, its explicit message/causation envelope identity, and delivery
completion evidence. The public workflow surface remains endpoint-neutral;
remote lockd verification is deferred only because its current implicit-XA
enrollment defect breaks multi-participant atomicity.

## Purpose

This specification defines a `liblockdc` facility for durable transactional
messaging. It lets an application make a lockd-backed state transition, record
command acceptance, record an external-effect intent, and later perform that
effect without the dual-write failure window.

The implementation uses existing liblockdc state, attachment, transaction,
lease, and query capabilities. It works against either a remote lockd endpoint
or local Pouch storage. It does not use lockd queues.

The component provides at-least-once handoff. It cannot guarantee exactly once
for an effect outside lockd/Pouch. The stable `effect_key` delivered to the
foreign system is the required idempotency mechanism for that boundary.

## Design Decisions

- One configured workflow namespace contains command, inbox, and outbox
  records.
- Command receipts, inbox records, and outbox records are distinct durable
  facts. They use the same workflow namespace and transaction machinery, but
  they are never aliases for one another.
- Command, inbox, and outbox records are ordinary state keys. Payloads and
  optional command results are immutable attachments, not JSON fields.
- State change, command/inbox/outbox intent, and attachment commit in one
  existing lockd/Pouch transaction.
- The first durable command, inbox, or outbox acquire omits `txn_id` and
  retains the endpoint-minted rs/xid-compatible identifier from its lease.
  Every later participant supplies that identifier. Pouch durably enrolls the
  first lease when the second participant arrives; terminal releases vote and
  publish only after every enrolled participant commits. The workflow owns the
  participant ledger and terminal releases, and exposes no new coordinator.
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
- The host authenticates an API command before it reaches this API and passes a
  stable, service-owned scope. `liblockdc` persists and compares that scope;
  it does not authenticate an HTTP header, MQ property, or caller identity.

## Verified Transaction Contract

The intended contract is implicit XA: an ordinary first acquire receives an
endpoint-minted xid, and a later acquire with that xid atomically enrolls the
first and later leases in one transaction. A commit vote from an individual
lease is not publication; all enrolled participants must commit before their
staged state becomes public. A rollback or expiry decides rollback for all of
them.

An operation can fail after it has enrolled a later participant but before it
returns its local receipt or view. At that point, rollback is necessarily an
XA-wide decision: liblockdc aborts the workflow transaction, invalidates its
participant views, and rejects a later `commit()`. The application closes that
transaction and begins a new command, inbox, or outbox transaction to retry.

### Remote lockd

`lc_acquire_req.txn_id` binds a later key to the xid returned by the first
acquire. The intended server contract is that this retroactively enrolls that
first normal lease as well. Every domain, command, inbox, outbox, metadata, or
attachment participant then carries the same xid and stages its change.

Ordinary application credentials do not call the privileged transaction
coordinator API. Each terminal workflow release uses the endpoint's implicit
XA path. Current remote lockd incorrectly permits the first, non-explicit
lease to publish on its release after a later participant joins. liblockdc does
not rely on that behavior; remote multi-participant workflow atomicity remains
unavailable until lockd fixes the reported defect. The workflow API remains
available for remote endpoints so a fixed lockd works without a client-library
upgrade: lockd exposes no endpoint-version capability that would let liblockdc
reliably gate only affected server versions. Applications requiring atomic
remote multi-participant workflows must therefore deploy a lockd version with
the reported fix.

### Local Pouch

Pouch mints an rs/xid-compatible identifier when an ordinary acquire omits
`txn_id`. When a later acquire supplies that identifier, Pouch durably creates
one participant record containing both leases. Terminal lease releases record a
commit or rollback vote. Pouch publishes only after every participant has
committed, and immediately rolls the set back if a participant rolls back or
the transaction reaches its earliest lease expiry. Recovery replays a recorded
decision or rolls an expired undecided transaction back.

`lc_xid_new()` remains a public general helper. The workflow accepts no
caller-supplied transaction id and uses only the endpoint-minted one.

### Workflow adapter rules

- `accept_command()`, `append_outbox()`, or `accept_inbox()` is the first
  workflow operation. It acquires its deterministic record key without an xid,
  retains the endpoint-minted xid, and creates the transaction receiver.
- Every later domain, command, inbox, and outbox lease uses that xid and is
  recorded exactly once as a `(namespace, key)` participant.
- `commit()`/`rollback()` invokes the backend's implicit XA terminal release
  path. It does not call the privileged transaction-coordinator API.
- A successful terminal decision consumes every enrolled lease. Calling
  `release()` on an enrolled lease independently is impossible through the
  workflow participant surface.
- If staging or the terminal decision fails, no post-commit notification is
  emitted. The transaction remains recoverable according to the selected
  backend's existing transaction recovery rules.

## Scope and Non-goals

The component covers:

- durable command acceptance, outcome, and status receipts;
- durable idempotent outbox intent recording;
- durable inbox deduplication;
- true streaming of arbitrary payload bytes;
- claims, retries, retry scheduling, expiry recovery, completion, and
  dead-letter state;
- direct same-process dispatch plus cross-process recovery; and
- equivalent observable behavior for Pouch and remote lockd endpoints once the
  remote implicit-XA enrollment defect is fixed.

It deliberately does not cover:

- a distributed transaction with a foreign broker, HTTP service, SMTP server,
  filesystem, or any other external-effect system;
- exactly-once foreign effects;
- a queue implementation or use of lockd queues as an implementation detail;
- a generic transport plugin or dispatcher-owned host callback; or
- request canonicalization, request hashing, authentication, authorization,
  or an HTTP/MQ protocol adapter; or
- a global ordering guarantee.

## Terms

**Command receipt**
  The durable fact that a service accepted one command in its owned scope and
  the current or terminal result of that command. It is the idempotency
  boundary for an HTTP/RPC/UI command and for a message that starts a distinct
  operation in the receiving service.

**Scope**
  A stable service-owned authorization or tenancy boundary. The host derives it
  after authentication. It is part of command identity and is never inferred
  from an untrusted transport header.

**Command type**
  A stable, service-owned operation name, such as `orders.create.v1`. It is
  part of command identity, not a display string.

**Idempotency key**
  An opaque caller-supplied stable identity for retries of one command within a
  `(scope, command_type)` boundary. For an API command this is the usual
  idempotency key. It may be the same value as the cross-service operation ID,
  but the two concepts are not required to be coupled.

**Request digest**
  An opaque, durable binding to the semantic command input. A host normally
  supplies a versioned digest of the canonical request body and the request
  fields that change meaning. liblockdc stores and compares it; it neither
  canonicalizes nor hashes arbitrary HTTP or MQ payloads. The same command
  identity with a different request digest is a conflict, never a duplicate.

**Operation ID**
  Caller-supplied stable identity for one business operation across a workflow
  chain. It supports correlation and may equal an API idempotency key.

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

**Message ID**
  The component-generated, stable identity of one outbox transport message.
  It is sent unchanged on every dispatch attempt and is the normal
  `message_id` supplied to a receiving inbox. It is distinct from both the
  outbox record key and the foreign `effect_key`.

**Causation ID**
  The durable message or command receipt that directly caused a new outbox
  message. It preserves lineage without becoming an ordering guarantee.

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

The primary C entry point is a receiver-style workflow handle with
implementation-private state, created from an existing client. It follows the
public library's receiver-function convention and zero-initializable
configuration/request records. The command-receipt methods, message envelope
fields, and completion evidence below are the current public surface.

```c
typedef struct lc_workflow lc_workflow;
typedef struct lc_workflow_transaction lc_workflow_transaction;
typedef struct lc_workflow_participant lc_workflow_participant;
typedef struct lc_outbox_job lc_outbox_job;
typedef struct lc_command_identity lc_command_identity;
typedef struct lc_command_request lc_command_request;
typedef struct lc_command_receipt lc_command_receipt;
typedef struct lc_command_result lc_command_result;

int (*new_workflow)(lc_client *self, const lc_workflow_config *config,
                    lc_workflow **out, lc_error *error);

struct lc_workflow {
  int (*accept_command)(lc_workflow *self,
                        const lc_command_request *request,
                        lc_workflow_transaction **out,
                        lc_command_receipt *receipt, lc_error *error);
  int (*get_command_receipt)(lc_workflow *self,
                             const lc_command_identity *identity,
                             lc_command_receipt *out, lc_error *error);
  int (*write_command_result)(lc_workflow *self,
                              const lc_command_identity *identity,
                              lc_sink *dst, size_t *written, lc_error *error);
  int (*resume_command)(lc_workflow *self,
                        const lc_command_identity *identity,
                        lc_workflow_transaction **out,
                        lc_command_receipt *receipt, lc_error *error);
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
  int (*accept_command)(lc_workflow_transaction *self,
                        const lc_command_request *request,
                        lc_command_receipt *receipt, lc_error *error);
  int (*acquire)(lc_workflow_transaction *self,
                 const lc_workflow_participant_request *request,
                 lc_workflow_participant **out, lc_error *error);
  int (*append_outbox)(lc_workflow_transaction *self,
                        const lc_outbox_entry *entry, lc_source *payload,
                        lc_outbox_receipt *out, lc_error *error);
  int (*complete_command)(lc_workflow_transaction *self,
                          const lc_command_result *result, lc_error *error);
  int (*fail_command)(lc_workflow_transaction *self,
                      const lc_command_result *result, lc_error *error);
  int (*commit)(lc_workflow_transaction *self, lc_error *error);
  int (*rollback)(lc_workflow_transaction *self, lc_error *error);
  void (*close)(lc_workflow_transaction *self);
};

struct lc_workflow_participant {
  /* The non-terminal lc_lease operations: describe, get, update,
     mutate/mutate_local, metadata/remove, keepalive, and attachment access. */
  /* No release or transaction-decision operation is exposed. */
  void (*close)(lc_workflow_participant *self);
};
```

The command records are deliberately small and transport-neutral:

```c
struct lc_command_identity {
  const char *scope;
  const char *command_type;
  const char *idempotency_key;
};

struct lc_command_request {
  lc_command_identity identity;
  const char *request_digest;
  const char *operation_id; /* optional correlation identity */
};

enum {
  LC_COMMAND_PENDING = 1,
  LC_COMMAND_COMPLETED = 2,
  LC_COMMAND_FAILED = 3
};

struct lc_command_result {
  const char *result_code;       /* completed result class */
  const char *result_reference;  /* optional resource/status reference */
  const char *content_type;      /* required with a result attachment */
  lc_source *body;               /* optional; streamed into `result` */
  const char *failure_code;      /* failed result class */
  const char *failure_message;   /* bounded safe terminal message */
};

struct lc_command_receipt {
  int state;
  int duplicate;
  char *command_id;
  char *scope;
  char *command_type;
  char *idempotency_key;
  char *operation_id;
  char *result_code;
  char *result_reference;
  char *failure_code;
  char *failure_message;
  int has_result_body;
};
```

`lc_command_identity` selects one receipt for status and resume operations.
`lc_command_request` adds immutable acceptance binding; it is not a
request-body carrier. The host remains responsible for parsing, authenticating,
and optionally retaining the incoming request. `request_digest` is required
for an externally retriable command and is compared exactly as an opaque bounded
string. `lc_command_result` is valid in one terminal direction only: a
successful completion supplies no failure fields; a terminal failure supplies
no success body or success result fields. The implementation must reject
contradictory combinations before any state is staged.

The ABI-reviewed names below establish these interaction boundaries:

- `lc_workflow_config_init()`, participant acquisition records, and every
  transparent entry/retry/configuration record have matching initializers.
- Callers pass semantic IDs and routing metadata; liblockdc owns reserved-key
  construction, payload attachment naming, participant tracking, post-commit
  notification, and lease references. `effect_key` is caller supplied,
  immutable, and retained for every foreign-effect retry.
- `workflow->accept_command()`, `workflow->append_outbox()`, or
  `workflow->accept_inbox()` is the first workflow operation. It acquires its
  deterministic record lease without an xid and retains the endpoint-minted
  xid returned on that lease. It returns the transaction only after the
  relevant duplicate barrier succeeds. A duplicate command or inbox result
  returns a successful structured receipt and no transaction.
- `txn->acquire()` is the normal way to obtain a domain lease inside the
  workflow transaction. It supplies the workflow transaction id and retains
  the participant for terminal processing. It returns a workflow participant
  receiver whose non-terminal surface mirrors the normal lease state,
  metadata, keepalive, and streaming attachment operations. It deliberately
  exposes no `release()` or transaction-decision operation. Later outbox
  entries use the same workflow-owned xid through `txn->append_outbox()`.
- An outbox receipt contains the stable outbox identity and `effect_key` needed
  for logs and foreign-system calls without revealing storage layout. A
  matching committed outbox append returns that existing receipt as a successful
  `duplicate` result and returns no transaction; a caller therefore cannot
  accidentally repeat the associated domain mutation.
- Inbox acceptance reports `accepted` or `duplicate` as successful structured
  outcomes. It does not force callers to inspect an error string to distinguish
  a normal duplicate from a conflict.
- Command acceptance reports a durable `pending`, `completed`, or `failed`
  receipt. A duplicate with the same immutable command binding returns that
  receipt; a changed binding is a conflict. A result body, when requested,
  uses a fixed immutable attachment and the parent
  `write_command_result()` streaming receiver rather than a materialized
  result buffer.
- Transaction commit is the single success boundary. It invokes the endpoint's
  implicit-XA terminal decision; there is no separate `signal()` call for the
  application to forget.
- Dispatcher jobs expose immutable envelope metadata, streaming payload access,
  and only the terminal/renewal operations valid for their owned claim.

Errors must identify the failed semantic operation and relevant identity
(`idempotency_key`, operation ID, inbox identity, or outbox receipt) without
logging payload bytes, request digests, or credentials. The API must reject
contradictory configuration before a worker thread starts.

### Transaction creation and ownership

There is no public begin, join, or lease-adoption surface. To start an API
command, `workflow->accept_command()` derives the command-receipt key,
acquires it without an xid, and retains the endpoint-minted xid. To start
outbound or inbound work, `workflow->append_outbox()` or
`workflow->accept_inbox()` does the same for its own durable record. liblockdc
keeps the xid inside the owned workflow transaction and supplies it for every
later participant. A duplicate command or inbox result is successful and
returns no transaction.

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

local command_txn, command = workflow:accept_command({
  scope = tenant_id,
  command_type = "orders.create.v1",
  idempotency_key = client_idempotency_key,
  request_digest = semantic_request_digest,
  operation_id = order_operation_id,
})
if command_txn then
  local order = command_txn:acquire({ namespace = "orders", key = order_id,
                                      owner = "orders-api", ttl_seconds = 30 })
  order:update_raw(order_update_source)
  command_txn:append_outbox(entry, payload_source)
  command_txn:complete_command({ result_code = "created",
                                  result_reference = order_id })
  command_txn:commit()
end
-- A duplicate returns the stored command receipt, including pending or
-- terminal outcome, and never repeats the domain mutation.

local txn, receipt = workflow:append_outbox(entry, payload_source)

local order = txn:acquire({ namespace = "orders", key = order_id,
                            owner = "orders-api", ttl_seconds = 30 })
order:update_raw(order_update_source)
txn:append_outbox(entry, payload_source)
local result = txn:commit()

local inbound_txn, accepted = workflow:accept_inbox(message)
if accepted.accepted then
  local command = inbound_txn:accept_command({
    scope = tenant_id,
    command_type = "orders.fulfill.v1",
    idempotency_key = message.command_id,
    request_digest = message.command_digest,
    operation_id = message.operation_id,
  })
  if not command.duplicate then
    inbound_txn:append_outbox(entry, payload_source)
    inbound_txn:complete_command({ result_code = "accepted" })
  end
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

The command-receipt extension adds `workflow:accept_command(request)`,
`workflow:command_receipt(identity)`, and
`workflow:resume_command(identity)`. Transactions add
`txn:accept_command(request)`, `txn:complete_command(result)`, and
`txn:fail_command(result)`. A receipt result body is exposed only by
`workflow:write_command_result(identity, sink)`; Lua never receives a hidden
materialized string.

### Cross-language parity

The C and Lua surfaces must have parity for workflow creation, first-operation
command/outbox/inbox transaction creation, command status and terminal result,
commit/rollback, later outbox append, parent `next()`, job inspection, payload
streaming, renewal, retry, completion, and dead-lettering. Host integrations
may add conveniences, but may not weaken
the durable semantics or replace direct job ownership with callbacks.

## Namespace and Key Layout

`lc_workflow_config` supplies one non-empty workflow namespace. Command, inbox,
and outbox record classes live in that namespace; it may be the same as, or
distinct from, the application's domain-state namespace.

The following reserved key prefix is owned by liblockdc:

```text
__lockdc_io/v1/command/<command-identity-digest>
__lockdc_io/v1/inbox/<inbox-identity-digest>
__lockdc_io/v1/outbox/<operation-id-digest>/<effect-id-digest>
```

Key components are deterministic, bounded digests. SHA-256 components use
unpadded base64url (43 characters), not hexadecimal, so the complete reserved
key remains below lockd's 128-character key limit. The command digest covers
`scope`, `command_type`, and `idempotency_key`; the inbox digest covers the
consumer and complete source identity. liblockdc never creates a key by
directly concatenating source-supplied identifiers: every identity component
is length-prefixed before hashing. The original identifiers
remain in the state body and are checked when an existing key is reused. A
mismatch is a conflict, not a duplicate.

This layout makes an outbox key stable for its entire lifetime. Key names are
for direct addressing and namespace ownership, not the current portable query
predicate: existing `query_keys()` selectors match JSON state fields and return
the matching keys.

## Durable Record Model

The record body is compact JSON metadata with `application/json` content type.
Payload bytes are not embedded in it.

### Command receipt key

A command receipt has immutable command-binding fields:

```text
record_type          "lockdc.command.v1"
scope
command_type
idempotency_key
request_digest
operation_id         optional cross-service correlation identity
accepted_at_unix
```

It has a mutable, monotonic outcome:

```text
state                pending | completed | failed
result_code          host-defined safe result/status code
result_reference     optional created resource, workflow, or status reference
result_content_type  optional, required when a result attachment exists
completed_at_unix
failed_at_unix
failure_code         host-defined safe terminal business failure code
failure_message      bounded safe terminal business failure message
```

`pending` means the service durably accepted the command but has not recorded a
terminal business outcome. `completed` and `failed` are terminal. A failure to
reach or commit the local transaction is not a terminal command failure: it
leaves no receipt or a recoverable pending receipt, depending on which local
transaction had already committed. The component must never convert a timeout,
process crash, or transient dependency error into `failed` automatically.

When an application needs a replayable result body, the component writes it as
an immutable attachment named `result` in the same transaction as the terminal
receipt update. The result is read only through a streaming sink receiver. The
receipt body contains metadata and a result reference, never materialized
result bytes.

### Outbox key

An outbox record has immutable fields:

```text
record_type          "lockdc.outbox.v1"
operation_id
effect_id
effect_key           caller-supplied immutable foreign-effect idempotency key
payload_digest       caller-supplied immutable digest binding the payload bytes
message_id           component-generated immutable transport-message identity
causation_id         optional command or message that caused this message
kind                 caller-defined bounded routing label
schema_version       optional caller-defined message schema version
destination          caller-defined transport target
content_type
headers_json         optional JSON object string containing envelope headers
trace_context        optional bounded trace metadata
```

It has mutable delivery fields:

```text
dispatch_state       pending | claimed | retry_wait | completed | dead_letter
attempt_count
claim_expires_at_unix
not_before_unix
last_error
delivery_reference
response_digest
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

`payload_digest` is required. The host calculates it over the exact payload
bytes before append and chooses its durable representation (for example,
`sha256:<base64url>`). liblockdc persists and compares the value as opaque
immutable envelope metadata; it does not pre-read, buffer, or spool a payload
source merely to derive a digest. A retry with the same outbox identity must
therefore supply the same digest as the original payload, or it fails visibly
instead of silently delivering the retained attachment.

`message_id` is generated from the immutable durable outbox identity and is
stable across retries, claim expiry, dead-letter replay, and process restart.
It is an opaque transport value, not a storage key and not an `effect_key`.
The host sends it with every MQ delivery or HTTP command. A receiving service
normally uses it as `lc_inbox_message.message_id`.

`causation_id` is immutable once committed. When an outbox entry is appended
inside an accepted inbox transaction and the caller does not provide one, the
component uses the source message ID. When appended inside a command receipt
transaction, it uses the receipt's stable command identity. A caller may set a
different causation ID only when it is the actual immediate cause.

### Inbox key

An inbox record contains:

```text
record_type          "lockdc.inbox.v1"
consumer_id
source_kind
source_id
message_id
payload_digest       when the source payload participates in the contract
operation_id
processing_state
```

The inbox key is the durable receipt. The current component retains only this
metadata; it does not retain source payload bytes or derive resulting outbox
identities. A host that needs either for audit or replay owns that retention.

An inbox receipt and a command receipt must not be collapsed. The inbox says a
specific consumer durably handled one delivery. A command receipt says the
service owns one business operation and records its result. One inbound message
may have no command receipt, or it may transactionally create a distinct local
command receipt.

## Atomic Operations

`lc_workflow` and its transaction receiver provide the workflow transaction
wrapper over existing lockd and Pouch transaction facilities. The receiver
surface in [Consumer Experience and Public Surface](#consumer-experience-and-public-surface)
is the intended public boundary.

The transaction has an explicit participant ledger. Its first command, inbox,
or outbox record lease receives an endpoint-minted xid; every later participant
supplies that xid and, in Pouch, makes the first lease a durable participant
before returning. The application obtains a domain participant through
`txn->acquire()`, mutates through that receiver, and leaves terminal processing
to the workflow. The wrapper stages command/inbox/outbox keys and attachments
under that same transaction id.

The workflow must choose its duplicate barrier before the application performs
the associated domain mutation. The first `workflow->accept_command()`,
`workflow->append_outbox()`, or `workflow->accept_inbox()` call acquires its
deterministic record key with the create-only precondition before returning a
transaction. A committed matching record yields the normal duplicate outcome
before a new domain effect is staged; an immutable mismatch is a conflict. A
currently leased record is neither a duplicate nor fresh work: the operation
reports its acquire/read failure and the host decides whether to retry.

### Command acceptance and result

A command receipt is the duplicate barrier for an API command. The command
identity is exactly `(scope, command_type, idempotency_key)`. Its
`request_digest` is immutable binding data, not another identity component.

1. The authenticated host supplies a trusted scope, stable command type,
   idempotency key, and opaque request digest to
   `workflow->accept_command()` before it stages business state.
2. The component derives the receipt key and applies a create-only precondition
   under the first, endpoint-minted workflow xid. It stages a `pending`
   receipt and returns the owned transaction only for a new command.
3. The caller stages domain mutation and zero or more outbox effects through
   that transaction. A synchronous command may record `completed` or `failed`
   by calling the corresponding transaction method before `commit()`.
4. Commit makes the receipt, business state, terminal result when present, and
   outbox intent visible together. Lost transport responses are safe because a
   retry reads this same receipt.

For a duplicate identity, the component compares every immutable command field,
including scope, command type, idempotency key, request digest, and any
declared operation ID. A matching receipt is a successful result and returns no
transaction. A mismatch fails visibly as a structured error, not as a
duplicate, and identifies the command identity without exposing request bytes
or digest.

A command may intentionally remain `pending` after the acceptance transaction
commits. `workflow->resume_command()` acquires the existing pending receipt as
the first participant of a new workflow transaction. It may then stage the
next owned domain transition, output effects, and exactly one terminal outcome.
It returns the current receipt without a transaction when the receipt is
already terminal. No API permits `completed` or `failed` to become pending or
to overwrite a previous terminal result.

`workflow->get_command_receipt()` is a direct read of this durable state. It is
the required status-resource primitive for long-running HTTP/RPC commands; it
does not claim a dispatcher job, invoke user code, or perform a reconciliation
query.

A command receipt without its associated domain/outbox transaction is not the
feature's correctness goal. The public command-receipt API is endpoint-neutral
and uses the same workflow composition for Pouch and remote lockd. This release
proves it for Pouch first; compose-backed remote E2E proof remains deferred
until lockd repairs its implicit-XA enrollment contract.

### Message consumer that starts an owned command

An inbox duplicate barrier and a command receipt compose in one transaction;
neither replaces the other:

1. A consumer calls `workflow->accept_inbox()` using the immutable incoming
   `message_id`, and receives a transaction only for a new delivery.
2. It calls `txn->accept_command()` if the delivery starts a separately owned
   operation. A matching existing command receipt is a normal result: the
   caller commits the new inbox receipt but stages no duplicate domain effect.
3. For a new command, it stages domain state and any reply or downstream
   outbox effects. The reply's `causation_id` defaults to the input message ID.
4. It commits, then acknowledges the broker delivery. It never acknowledges
   before durable inbox acceptance, and it never lets an acknowledgement decide
   command success.

This supports request/reply MQ chains without turning liblockdc into a broker
client. The receiving host maps the outbox job's immutable `message_id`,
`operation_id`, `causation_id`, routing metadata, and streaming payload into
the broker protocol. A reply is an ordinary outbox effect; the reply consumer
uses an inbox receipt exactly as for any other message.

### Outbox append

1. The caller calls `workflow->append_outbox()` before staging the associated
   domain mutation. It derives the deterministic outbox key, applies the
   create-only duplicate barrier, acquires it without an xid, retains the
   endpoint-minted xid, and returns a transaction carrying that private xid.
2. The caller acquires and stages domain mutation leases through that
   transaction. The attachment and envelope metadata stage with the outbox
   participant. Further effects use `txn->append_outbox()` and the same xid.
3. The backend-specific terminal adapter makes the domain changes, outbox
   intent, and payload visible together, or rolls all of them back.
4. Only after a successful terminal decision does liblockdc notify the local
   dispatcher with the returned outbox key.

Submitting the same `(operation_id, effect_id)` again is idempotent only when
all immutable fields, including `effect_key`, `payload_digest`, `causation_id`,
routing metadata, and schema version, match. A conflicting repeat fails
visibly. A matching
committed repeat returns the existing outbox receipt with `duplicate` set and
no transaction; it does not create or stage any new domain or outbox
participant.

### Inbox acceptance

1. The caller calls `workflow->accept_inbox()` for the incoming message. It
   derives the inbox key, applies the create-only duplicate barrier, acquires
   it without an xid, retains the endpoint-minted xid, and returns a
   transaction carrying that private xid for an accepted message.
2. The caller acquires domain leases and appends resulting outbox intent(s)
   under that transaction.
3. The terminal adapter persists the receipt, domain changes, and resulting
   intent(s) together. A matching committed receipt reports `duplicate` and
   creates no new logical operation or outbox intent.
4. The source is acknowledged only after successful durable acceptance.

If an incoming source payload is part of duplicate validation, the supplied
digest must match the stored digest. A mismatch is a conflict.

## HTTP, RPC, and MQ Boundary Contract

liblockdc owns durable facts and their local transaction. The host owns
protocol parsing, authentication, authorization, broker acknowledgement, HTTP
status formatting, and the actual foreign call. The following logical fields
are the contract; this component intentionally does not prescribe HTTP header
names, CloudEvents fields, AMQP properties, or a serialization format.

| Durable field | HTTP/RPC mapping | MQ mapping |
| --- | --- | --- |
| `scope` | Authenticated service/tenant/actor boundary | Authenticated producer or tenant boundary established by the consumer |
| `command_type` | Stable route/operation name | Stable command message type |
| `idempotency_key` | Caller retry key | Command identity supplied by producer |
| `request_digest` | Host's durable semantic request binding | Host's durable semantic command binding |
| `message_id` | Outbound request/message identity | Broker message identity; inbound inbox identity |
| `operation_id` | Business correlation | Business correlation across hops |
| `causation_id` | Command or received message that caused the call | Received command/event that caused the message |
| `effect_key` | Foreign-service/provider idempotency key | Producer-side dispatch effect key |
| `trace_context` | Trace propagation only | Trace propagation only |

### Idempotent HTTP or RPC command

The host authenticates the request, derives scope, calls
`accept_command()`, stages its local state and zero or more outbox effects, and
commits. A completed or failed receipt maps to the service's recorded safe
response. A pending receipt maps to a status resource/reference selected by the
service. A retried request never re-executes business work merely because the
original response was lost.

An outbound HTTP/RPC command is an outbox job. The host sends its stable
`message_id`, `operation_id`, `causation_id`, and `effect_key` using the target
service's agreed protocol. The target service independently establishes its
own command receipt; the producer's outbox completion only records delivery
progress, not the target's business result.

### Message command and request/reply

The host maps an incoming broker delivery to `lc_inbox_message`, including its
immutable broker `message_id` and any source identity. It commits the inbox
transaction before acknowledgement. If the delivery is a command owned by this
service, it also records a command receipt as described above. A response is a
new outbox effect with the input message as causation; it is not a special
reply channel inside liblockdc. The requester deduplicates that response with
its own inbox record.

Transport redelivery and API retry are normal. Neither is global XA, and a
foreign receiver that declines to deduplicate the stable identity it is given
remains the limiting correctness boundary.

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

For a remote endpoint, the private dispatcher clone uses
`LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT` for its library-owned record reads. The
workflow rejects an outbox envelope whose combined routing and metadata fields
exceed `LC_WORKFLOW_MAX_ENVELOPE_BYTES`; payload attachments remain outside
that bound and stream normally. This is intentionally independent of a lower
`lc_client_config.http_json_response_limit_bytes` chosen for application-facing
typed JSON calls: otherwise a caller could durably commit an outbox envelope
that the dispatcher itself could never load. Payload attachments retain their
streaming boundary and are not materialized by this policy.

`workflow->close()` prevents new claims and notifications, wakes blocked
`next()` callers, cancels a remote dispatcher request, and joins the private
dispatcher. A handed-off job owns a separate uncancelled remote client, so it
remains usable for payload streaming, renewal, and terminal completion after
the workflow closes. `shutdown_timeout_ms` bounds each remote dispatcher
request; zero inherits an explicit root-client timeout or defaults to 30
seconds. Close never abandons a live thread. It does not
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
  +----------------------- eligible at not_before_unix+

claimed ---- expiry recovery ----> pending
claimed / retry_wait ---- dead-letter ----> dead_letter
```

The active outbox-key lease is the claim authority and remains held while the
host owns the job. After acquiring it, the dispatcher performs a private
reread and validates state, timing, and claim generation before handing the
job out. Completion, retry, and dead-letter transitions require that same
lease. A stale claim cannot change a later claimant's record.

Claim admission is a short, single-key durable transition: it stores
`claimed`, increments `attempt_count`, and records `claim_expires_at_unix`
before the job is handed to the host. The component then obtains the active
outbox-key lease that fences the host's terminal action. The deadline is a
recovery schedule marker, not a lease credential or fencing token. A live
worker renews its active lease and moves its local recovery signal forward;
recovery always reacquires and validates the real lease before changing the
durable record.

`attempt_count` is a non-negative durable counter whose next value must fit the
public one-based `int` job attempt field. A malformed negative or out-of-range
counter is rejected as an invalid candidate before any claim transition; it is
never incremented, narrowed, or handed to a receiver.

If a worker disappears, expiry recovery turns the durable claim back to
`pending`; if the durable attempt budget was consumed, it instead transitions
the record to `dead_letter`. This retains attempts across process loss and
prevents repeated crashes from bypassing `max_attempts`. The active lease ID,
owner, and fencing token are never copied into the envelope. Remote lockd is
expected to provide the same single-key lease boundary after its implicit-XA
enrollment defect is fixed.

The outbox envelope is also the external-delivery receipt for its one
`effect_key`; a separate generic external-delivery table is unnecessary.
`job->complete(job, completion, error)` accepts optional completion evidence
that persists a bounded provider/broker delivery reference and response digest
with the `completed` transition. This is evidence for diagnosis and provider
reconciliation, not proof that an uncooperative provider performed exactly one
effect. On an uncertain provider outcome, the host must either retry with the
same `effect_key` when the provider supports idempotency, or retain/dead-letter
the job for explicit provider reconciliation. It must not call completion based
on an assumed success.

A successful terminal job operation consumes its local job handle. A terminal
operation that returns an error leaves the handle active until its claim expires,
so the host may retry the same terminal operation after a transient local
persistence failure. This does not make an uncertain foreign effect safe to
repeat: resolve that outcome with the same `effect_key` before retrying its
terminal transition.

Each successful claim atomically increments `attempt_count`. Retry records a
diagnostic of at most `LC_WORKFLOW_MAX_DIAGNOSTIC_BYTES` (4096) bytes and sets
`not_before_unix`; oversized diagnostics are rejected without changing the
claimed job. Once the configured attempt budget is
exhausted, it transitions to `dead_letter` rather than silently removing the
record. Dead-letter replay retains the original `effect_key`.

### Dead-letter operations

Dead letters are terminal durable envelopes until an application takes one
explicit management action. `replay_dead_letter(outbox_key)` is valid only for
a current dead-letter envelope. It atomically restores `pending`, resets the
delivery attempt count to zero, retains the immutable `effect_key`, increments
`replay_count`, writes `replayed_at_unix`, and preserves the previous terminal
diagnostic in `prior_dead_letter_error`. The normal dispatcher then receives
the exact key as an internal notification. `replay_count` is a non-negative
durable signed-64-bit counter; malformed negative or maximum values are
rejected before replay, so the counter is never incremented past its
representable range.

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

- scope, command type, idempotency key, request digest, operation, effect,
  source, consumer, kind, destination, schema, and header sizes;
- attachment size when the application requests a maximum;
- dispatcher notification and preclaimed-job capacity;
- claim TTL, renewal cadence, retry delay, and maximum attempts; and
- retained diagnostics and dead-letter retention.

### Receipt retention and privacy

Command receipt, inbox, outbox, and external-delivery evidence have different
retention purposes and must have independently configured retention policies.
A receipt must outlive the maximum retry, redelivery, and client-status window
for its boundary; deleting it earlier deliberately reopens duplicate execution
for that identity. v0 defaults to no automatic deletion. A later retention
sweeper may delete only terminal records older than an explicit configured
window and must remove a command result attachment atomically with its receipt.

The durable `request_digest` is comparison material, not audit payload. It and
all result, source, and outbox attachments may be sensitive. Public errors,
logs, workflow stats, dead-letter export, and default command-status responses
must not expose them. A host chooses which terminal result fields are safe to
return to a caller and which records are available to an operator export.

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

The existing inbox/outbox surface is not complete until its applicable behavior
is proven for both Pouch and the repository's compose-backed remote lockd E2E
environment. The command-receipt extension is not complete until every command
composition obligation is proven for Pouch; it gains remote parity only after
the repaired remote implicit-XA contract is present in that E2E environment.

1. The first `accept_command()`, `append_outbox()`, or `accept_inbox()` acquire
   omits `txn_id` and retains the endpoint-minted xid. Every later domain,
   command, inbox, or outbox participant propagates that xid. There is no
   caller transaction-id construction, join, or lease-adoption surface.
2. One workflow participant ledger containing domain mutation, command/inbox/
   outbox key, and attachment commits atomically; rollback exposes none of
   them. Pouch proves that releasing the first participant before the last
   keeps every staged value private. Remote lockd requires the upstream
   implicit-XA enrollment fix before it can satisfy this criterion.
3. A command retry with matching scope, command type, idempotency key, and
   request digest returns the exact durable pending or terminal receipt without
   a transaction or repeated domain mutation. Reuse with any changed immutable
   command field is a conflict.
4. A synchronous command commits its domain state, command result, and outbox
   records together. A lost HTTP/RPC response followed by a retry returns the
   recorded result. Pending-command resume can record exactly one terminal
   result and cannot overwrite a terminal receipt.
5. An incoming message that starts an owned command commits inbox receipt,
   command receipt, domain state, and reply/downstream outbox effects together.
   Broker acknowledgement occurs only after that commit. Duplicate delivery and
   a duplicate command each avoid a second business effect.
6. Repeated outbox append with the same operation/effect identity creates one
   intent; a conflicting immutable repeat fails.
7. Inbox redelivery is idempotent and a payload-digest conflict is rejected.
8. The ordinary append-to-dispatch path performs no recovery query.
9. A process crash after commit and before notification is recovered by another
   dispatcher from an indexed query.
10. A crash after the foreign effect and before completion redelivers the same
   `effect_key`.
11. Stale completion, retry, and dead-letter operations cannot alter a later
   claim.
12. Retry scheduling, retry-budget exhaustion, dead-lettering, and replay are
   durable and observable. Default policy tests prove 100 attempts including
   the first, full-jitter exponential delay from one second capped at fifteen
   minutes, a one-hour host-delay limit, and dead-lettering without deletion.
13. Payload and command-result reads remain streaming under fragmented reads, large attachments,
   cancellation, and backend failover/reopen; tests prove no full-payload
   materialization.
14. Competing remote dispatchers and shared-root Pouch dispatchers produce one
    active lease-authorized claim and recover abandoned claims. Tests must not
    assert identical public visibility of a live `claimed` envelope across the
    two backends.
15. The regression baseline seeds 256 pending records with a 16-key
    notification bound, proves paged indexed reconciliation delivers every
    record, and records wall-clock latency under a deliberately broad 30-second
    failure bound. A separate load benchmark before v1 release must extend this
    profile with retained terminal population, continuous state-transition
    churn, and an un-compacted Pouch log; payload size must not change
    discovery cost.
16. Parent close behavior leaves incomplete claims for later expiry recovery
    and never manufactures completion.
17. The dispatcher never invokes host callbacks or host-runtime code on its
    thread; a host worker receives work only through `next()` and owns its
    execution context.
18. Shutdown stops new claims, wakes blocked parent `next()` callers, joins
    the private dispatcher within the configured deadline, and permits an
    active host job to recover through lease expiry after its grace period.
19. C and Lua integration tests prove the same observable workflow outcomes:
    idempotent command/append/accept, explicit duplicate results, command
    result status/streaming, parent `next()` streamed-payload handoff, and
    terminal job transitions without dispatcher thread callbacks.
20. Transaction ownership is enforced: a workflow participant exposes no
    release/decision method; the transaction begins only from its deterministic
    first record lease; the Pouch terminal decision contains every enrolled
    participant; and a consumed participant cannot be reused.
21. A matching committed outbox duplicate returns its existing receipt with no
    transaction and cannot stage a second domain mutation or outbox intent.
22. Dead-letter export contains envelope JSON but no payload bytes; explicit
    replay keeps `effect_key`, resets the attempt, records replay provenance,
    and deletion removes both envelope and attachment atomically. The same
    outcomes are covered through the C and Lua surfaces, including opt-in
    startup replay.

## Reconciliation Performance Benchmark

`lockdc_bench workflow-reconcile` is the load benchmark for the recovery path.
It seeds public outbox envelopes through the normal client API, then starts a
new workflow so that delivery can occur only through its private indexed
reconciliation path. The workload contains:

- `pending_rows` dispatchable records, each with an optional payload
  attachment;
- `terminal_rows` retained completed records in the same namespace; and
- `churn_updates` complete state rewrites of every terminal record, leaving
  Pouch intentionally un-compacted before recovery starts.

The benchmark validates that every pending record is delivered exactly once to
the benchmark’s own key prefix and that recovery performs at least
`ceil(pending_rows / page_capacity)` bounded indexed pages. It emits
machine-readable first-delivery latency, drain latency, throughput,
recovery-query count, recovered-candidate count, and candidate surplus.
Candidate surplus is expected to expose stale index entries: candidate keys are
always directly reread and validated before a dispatcher hands work to the
host, so it is an efficiency signal rather than a duplicate-delivery count.
Attachment bytes are deliberately excluded from discovery; compare a
zero-payload run with a large-payload run to confirm that discovery cost is
governed by indexed state, not payload size.

The reproducible entry points are:

```sh
make benchmark-workflow-pouch
make benchmark-workflow-hardening
make benchmark-workflow-remote
```

The Pouch command creates and removes an isolated local root. The remote
command resets then starts the repository devenv and uses its disk endpoint and generated
mTLS bundle, supplying both disk nodes so the client follows the active leader.
It uses the shared `default` namespace permitted by the development client but
allocates a unique outbox-key prefix, leaving those records available for
post-run inspection. Both commands default to the
256-pending/1,024-terminal/four-churn/16-page baseline. Set
`WORKFLOW_BENCH_ROWS`, `WORKFLOW_BENCH_TERMINAL_ROWS`,
`WORKFLOW_BENCH_CHURN_UPDATES`, `WORKFLOW_BENCH_PAYLOAD_BYTES`, and
`WORKFLOW_BENCH_PAGE_CAPACITY` to characterize a deployment-sized profile.

`benchmark-workflow-hardening` is the local Pouch hardening lane. It runs the
preflushed-index, persisted-after-reopen, compacted, shared-writer, and
shared-writer-compacted reconciliation cases serially under the same bounded
timeout. The shared-writer cases use two dispatchers by default; set
`WORKFLOW_BENCH_HARDENING_DISPATCHERS` to change that deliberately.

Timing is reported rather than enforced as a universal pass/fail threshold:
storage media, remote TLS, and lockd deployment topology materially affect the
absolute number. Before v1, record representative Pouch and remote baselines
on the supported deployment hardware and promote agreed budgets into an
explicit performance gate.

## Remaining Proof Obligations and Future Work

The public workflow receiver names, request records, envelope metadata, and
liblockdc shared-library ABI 3 line are finalized for this release. Future
public-surface changes require the same API and ABI review. The remaining
component-specific work is:

1. Define the command-result safe-response contract and terminal failure code
   taxonomy without making liblockdc an HTTP status-code policy engine.
2. Define independent terminal retention windows for command, inbox, outbox,
   dead-letter, and optional request/result attachments before adding an
   automatic sweeper.
3. Establish quantitative recovery-query performance budgets from the Pouch and
   remote-lockd benchmark matrix before treating indexing as sufficient.
4. Prove remote command receipt composition only after the upstream implicit-XA
   enrollment fix is available in the repository's pinned E2E lockd image.

## Design Basis

The design follows the transactional outbox rule that the durable intent is
committed with the business state and that a later relay can repeat an external
send. It also follows the idempotent-consumer rule that processed-message
identity is persisted as part of the durable processing transaction, and the
idempotent-command rule that API acceptance and its replayable result are
durable service-owned state.

- [Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox)
- [Idempotent Consumer pattern](https://microservices.io/patterns/communication-style/idempotent-consumer.html)
- [AWS transactional outbox guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)
