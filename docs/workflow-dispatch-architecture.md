# Threadless workflows and explicit dispatchers

Status: implementation authority for the workflow API cutover. This document
supersedes the workflow-construction, dispatcher-lifecycle, and Lua-dispatch
parts of [the transactional messaging specification](inbox-outbox.md). The
durable record format, transaction rules, inbox/outbox identities, claim
semantics, retry policy, dead-letter representation, and endpoint limitations
in that document remain authoritative.

## Purpose

`lc_workflow` currently combines durable transaction production with a private
dispatcher thread. That makes a route-side producer unexpectedly allocate a
long-lived worker, client/session state, notification queues, and recovery
work. It is the wrong ownership model for web runtimes and for any process that
only appends durable effects.

This cutover splits those roles:

- a **workflow** is a threadless transactional producer and receipt reader;
- a **workflow dispatcher** is an explicit, process-local durable-work
  consumer; and
- a host decides its process topology, scheduling, and language-runtime
  ownership.

The cutover is intentionally breaking. ABI generation 4 has not been released
from this branch, so its public workflow receiver layout may be replaced
without changing `LOCKDC_ABI_VERSION`. The next ABI change after the released
ABI must advance by exactly one, as usual. There is no compatibility
constructor and no legacy implicit-thread mode.

## Ownership boundary

liblockdc owns durable workflow semantics:

- transaction enrollment and terminal decisions;
- command receipts, inbox deduplication, immutable outbox records and payloads;
- claims, renewals, retry scheduling, terminal completion, dead letters, and
  indexed recovery;
- bounded direct-key notification and process-local dispatcher coordination.

liblockdc does **not** own a generic supervisor, process manager, IPC router,
web-runtime lifecycle, foreign transport, or application handler policy. In
particular, it never calls a Lua closure from a private thread.

A host such as Vectis owns worker/supervisor process placement, its bounded
route-to-supervisor IPC, handler registration, Lua lane ownership, ingress and
shutdown policy, and any leader election used to choose one dispatcher among
several application runtimes. IPC is a latency hint only; Lockd/Pouch remains
the durable authority.

## Handles and lifetimes

### Threadless `lc_workflow`

`lc_client_new_workflow()` validates, canonicalizes, deep-copies, and retains
an `lc_workflow_config`. It creates no thread, private client clone,
notification queue, recovery loop, claim, or foreign-effect execution.

The workflow receiver retains the producer-side operations:

- command acceptance, lookup, result streaming, and resume;
- inbox acceptance;
- outbox append;
- workflow transaction participant acquisition, commit, rollback, and close;
- receipt/result cleanup and workflow close.

It deliberately has no `next`, stats, reconciliation, dead-letter, claim, or
job-terminal operation. These belong only to a dispatcher.

The first durable command, inbox, or outbox operation remains the transaction
anchor. It obtains the endpoint-minted transaction id; later participants use
that id. There is no generic `begin()` that invents a separate durable anchor.
The first operation must be one of the existing durable workflow facts. This
preserves implicit-XA semantics and avoids a new persistent record type.

### Commit-published outbox receipts

An outbox key is safe to wake only after its transaction has committed. The
cutover therefore separates a staged append from its committed receipt:

- a fresh `workflow->append_outbox()` returns its transaction but no public
  outbox receipt;
- a transaction's `append_outbox()` stages another effect and returns no fresh
  outbox key; and
- `transaction->commit()` returns one owned receipt for every outbox effect
  made durable by that decision.

The C commit result is an initialized/cleanup-owned result record containing a
receipt array:

```c
typedef struct lc_workflow_commit_result {
  lc_outbox_receipt *outbox_receipts;
  size_t outbox_receipt_count;
} lc_workflow_commit_result;

void lc_workflow_commit_result_init(lc_workflow_commit_result *result);
void lc_workflow_commit_result_cleanup(lc_workflow_commit_result *result);
int lc_workflow_transaction_commit(lc_workflow_transaction *transaction,
                                   lc_workflow_commit_result *out,
                                   lc_error *error);
```

The result is populated only after the backend reports its durable terminal
commit. On failure or rollback it is empty. A duplicate top-level or later
transaction append is different: it finds an already committed effect and
returns that existing durable receipt directly. Such a receipt is already safe
to notify. An attached local dispatcher receives fresh committed keys
internally at the same boundary; it may also receive an existing duplicate key.
A separate host forwards only a successful commit result's fresh receipts or an
already committed duplicate receipt. No public API exposes a fresh staged
outbox key that could accidentally be sent before commit.

Commit-result storage is prepared before the terminal backend decision. If the
library cannot allocate or copy the required result, it returns before voting
commit and leaves the transaction available for rollback or retry. Once the
backend confirms a commit, returning its receipts cannot fail due to local
allocation. An indeterminate backend error returns no fresh receipt even if a
later recovery proves the commit durable; reconciliation is then the required
repair path.

Closing a workflow closes any still-open owned transaction by best-effort
rollback, releases any attached dispatcher reference, and releases its client
reference. It never stops a dispatcher.

### `lc_workflow_dispatcher`

`lc_workflow_dispatcher` is an opaque receiver with an independent explicit
lifecycle. It owns:

- bounded direct-key candidate and delayed-retry wake queues;
- a private C-only notification/recovery thread and, for remote endpoints, its
  dedicated bounded dispatcher client;
- targeted claim/read only when a consumer requests work;
- indexed reconciliation, claim-expiry recovery, retry, dead-letter controls,
  and dispatcher statistics.

It does not own application callbacks. Its private dispatcher thread queues
candidate keys and recovery intent, but never claims a job or invokes C or Lua
application code.

One live dispatcher exists at most once for a `(client instance, canonical
workflow configuration)` registry key. The key includes every configuration
field, after defaults and validation have been applied: namespace, owner,
transaction and claim TTLs, retry policy, notification capacity, recovery
cadence, shutdown request timeout, and dead-letter startup policy. Comparing
canonical owned values, rather than caller pointers, makes the result stable.

Different `lc_client` instances never share a registry entry. They can safely
compete for durable claims, but a host that needs one process-level dispatcher
must arrange that policy itself. A Vectis supervisor therefore creates its own
post-fork client and obtains one dispatcher from it; route workers use distinct
threadless producer clients and send committed receipt keys to that supervisor.

### Acquisition, attachment, and closure

The public C shape is:

```c
typedef struct lc_workflow_dispatcher lc_workflow_dispatcher;

int lc_client_new_workflow(lc_client *client,
                           const lc_workflow_config *config,
                           lc_workflow **out, lc_error *error);
int lc_client_new_workflow_with_dispatcher(
    lc_client *client, const lc_workflow_config *config,
    lc_workflow_dispatcher *dispatcher, lc_workflow **out, lc_error *error);
int lc_workflow_get_or_start_dispatcher(lc_workflow *workflow,
                                        lc_workflow_dispatcher **out,
                                        lc_error *error);
int lc_workflow_dispatcher_notify_outbox_key(
    lc_workflow_dispatcher *dispatcher, const char *outbox_key,
    lc_error *error);
```

The client receiver exposes both workflow constructors. The workflow receiver
exposes `get_or_start_dispatcher`; both receivers retain eight trailing
`void *` extension slots. Every fallible receiver operation has the matching
free-function form.

`get_or_start_dispatcher()` is linearizable with stop and registry cleanup:

- concurrent compatible calls obtain retained references to one live instance;
- a dispatcher that is stopping or failed is not returned as running;
- an acquisition racing stop returns a closed/busy error until cleanup permits
  a replacement; and
- no operation temporarily creates two live dispatchers for one registry key.

`new_workflow_with_dispatcher()` attaches an already acquired compatible
dispatcher to a producer. Compatibility requires the same client instance and
canonical configuration. A successful commit on an attached workflow sends
each newly committed outbox receipt key to that dispatcher's local wake queue.
The operation is only a fast path: queue overflow, an unavailable dispatcher,
or a later crash cannot invalidate the durable commit and is repaired by
reconciliation.

An unattached workflow is still complete for production. Its caller receives
outbox keys only in committed receipts and may forward them to a host-owned
dispatcher through host IPC. It must never forward a key from a failed or
rolled-back transaction.

The registry retains a started dispatcher until explicit `stop()` completes or
its client closes. `lc_workflow_dispatcher_close()` releases only one caller's
handle; it does not stop a dispatcher, including when it happens to be the last
external handle. This deliberately lets a lazily started dispatcher remain
available for later producer attachments and asynchronous work. Hosts that no
longer need it call `stop()`; `wait()` is available to a different lifecycle
observer or after a timeout.

`stop(deadline_ms)` is idempotent. It changes the dispatcher to stopping,
rejects new wake and work requests, and waits up to its deadline for private
infrastructure and handed-out jobs to become terminal. A timeout leaves it
stopping; a later `wait(deadline_ms)` observes the same shutdown and never
restarts it. Stop never invalidates an already handed-out job: it remains able
to reach a terminal outcome until its owner closes it or its claim expires.
Client close stops and joins all registered dispatchers before releasing client
state, then invalidates all remaining workflow, dispatcher, and outbox-job
handles. Any unfinished claim is left for normal expiry recovery; no handle
retains a usable freed client.

## Dispatcher activation and work limits

Acquisition starts private notification/recovery infrastructure but is
**passive**: it cannot claim a job merely because it has a wake. Claiming is
demand-driven: a consumer asks for one job only when it has execution capacity.
The first request selects a consumer mode:

- raw C consumption uses `dispatcher->next()`;
- Lua `dispatcher:run()` and `dispatcher:pump()` bind their owner-state sink
  before requesting work; and
- a Lua owner state cannot be replaced, while concurrent raw C `next()` calls
  share the raw pull mode.

`notification_capacity` is one shared bound across direct-key candidates and
delayed retry wakes; it does not authorize preclaiming. `next()` removes or discovers one candidate,
claims it, and returns the owned job to the caller. A Lua lane requests one
job only when it has an idle handler slot. A Vectis source router does the same
for an available logical-supervisor lane. Raw C callers choose their own bounded
worker capacity by issuing only that many concurrent or outstanding `next()`
requests. No unbounded ready queue or library-owned claim set exists.

Stopping first prevents new claims and wake acceptance. It then waits only up
to its deadline for active jobs to reach a terminal outcome. Remaining claims
are released where safe or allowed to expire and are repaired by normal durable
recovery. A committed outbox record is never reported as failed because its
wake arrived after shutdown began.

## Direct-key notifications and recovery

`lc_workflow_dispatcher_notify_outbox_key()` accepts only a non-empty durable
outbox key already known by the caller to have committed. It copies and
deduplicates the key in the existing bounded candidate queue and wakes waiting
consumers. The next consumer request uses a targeted claim/read path. It does
not query or scan the workflow namespace solely because it received a key.

Duplicate notifications coalesce. A full queue increments observable overflow
state and schedules one indexed reconciliation candidate; it never drops
durable work.

Reconciliation discovers candidates but never claims them. It uses a bounded
streaming query and a resumable cursor: when the shared candidate budget is
full, it retains enough cursor state to resume later rather than materializing
the remaining namespace keys. A consumer request drains one candidate before
continuing that cursor. This holds for indexed and explicit scan recovery, so a
large retained outbox population cannot turn a wake or recovery pass into an
unbounded allocation.
Unknown, already-terminal, not-yet-eligible, or actively claimed keys are
normal no-work candidates, not evidence that the durable outbox was lost.

Recovery remains mandatory and is the correctness path for:

- process crash or restart;
- a full or closed host IPC channel;
- direct-notification overflow;
- lease expiry or a failed terminal action;
- missed cross-process or cross-host wakeups; and
- dead-letter startup replay when configured.

Pouch local recovery uses its efficient configured query/index policy. Remote
Lockd and shared Pouch may later supply cross-process change notifications, but
those can only request reconciliation and cannot replace durable recovery.

## C receiver contract

The dispatcher receiver provides the following operations, with free-function
equivalents and public API comments:

```c
struct lc_workflow_dispatcher {
  int (*next)(lc_workflow_dispatcher *self, long timeout_ms,
              lc_outbox_job **out, lc_error *error);
  int (*notify_outbox_key)(lc_workflow_dispatcher *self,
                           const char *outbox_key, lc_error *error);
  int (*get_stats)(lc_workflow_dispatcher *self, lc_workflow_stats *out,
                   lc_error *error);
  int (*reconcile)(lc_workflow_dispatcher *self, lc_error *error);
  int (*replay_dead_letter)(lc_workflow_dispatcher *self,
                            const char *outbox_key, lc_error *error);
  int (*delete_dead_letter)(lc_workflow_dispatcher *self,
                            const char *outbox_key, lc_error *error);
  int (*export_dead_letters)(lc_workflow_dispatcher *self,
                             const lc_dead_letter_export_opts *options,
                             lc_sink *dst, lc_dead_letter_export_res *out,
                             lc_error *error);
  int (*stop)(lc_workflow_dispatcher *self, long deadline_ms, lc_error *error);
  int (*wait)(lc_workflow_dispatcher *self, long deadline_ms, lc_error *error);
  void (*close)(lc_workflow_dispatcher *self);
  void *reserved_extension_slots[8];
};
```

`next()` is the raw expert pull surface and one unit of demand. It waits for or
discovers one candidate, claims it, and returns one owned `lc_outbox_job`; the
job retains its active claim until `complete`, `retry`, `dead_letter`, or
close. Existing terminal operations and payload streaming semantics remain
unchanged. A terminal failure must leave the claim recoverable through retry or
expiry, never silently consume it.

The producer and transaction receivers' `append_outbox` operations return an
optional receipt only for an already committed duplicate. Their `commit`
operation accepts an `lc_workflow_commit_result *` as above. That result is the
only public source of **fresh** wakeable outbox keys.

`lc_workflow_stats` is dispatcher-only in this cutover. Its live gauges are
`pending_candidates`, `delayed_wakes`, and `waiting_consumers`; the old
preclaimed `ready_jobs` gauge is removed. Its monotonic direct-notification,
overflow, recovery-query, recovered-candidate, claim-loss, and payload-failure
counters retain their existing observability role. None of these reads queries
the durable namespace.

## Lua workflow and dispatcher façade

Lua mirrors the ownership split exactly.

```lua
-- Producer or request domain: no dispatcher thread is created.
local workflow = assert(client:new_workflow({
  namespace_name = "myapp.orders",
  max_attempts = 12,
}))

workflow:transaction(function(tx)
  tx:append_outbox(entry, payload_source)
end)

-- Worker/service domain, normally a distinct process with its own client.
local worker_client = assert(lockdc.open(worker_client_config))
local worker_workflow = assert(worker_client:new_workflow({
  namespace_name = "myapp.orders",
  max_attempts = 12,
}))
local dispatcher = assert(worker_workflow:dispatcher())
assert(dispatcher:run({
  handlers = {
    ["order.webhook"] = function(job)
      local ok, err = deliver(job:payload_source(), job.effect_key)
      if not ok then
        return job:retry({ diagnostic = err, delay_seconds = 30 })
      end
      return job:complete()
    end,
  },
}))
```

`client:new_workflow(config[, { dispatcher = dispatcher }])` creates a
threadless producer, optionally with a compatible dispatcher attachment.
`workflow:dispatcher()` is the Lua form of get-or-start and takes no
configuration: the canonical workflow configuration is the sole policy source.

The workflow façade includes transaction production, receipt access, and
close/garbage-collection cleanup. It has no `next`, claim, retry, dead-letter,
or dispatcher statistics methods. The dispatcher façade includes `run`,
bounded `pump`, raw `next`, `notify_outbox_key`, `stats`, `reconcile`,
dead-letter controls, `stop`, `wait`, and close. It has no producer shortcut.

`workflow:transaction(fn)` is a Lua convenience, not a new durable begin
operation. It supplies a lazy transaction proxy. The first proxy operation must
be `append_outbox`, `accept_inbox`, or `accept_command`; that operation creates
the existing durable anchor and its endpoint-minted transaction id. Calling
`acquire` before an anchor fails. On normal callback return, the façade commits
the concrete transaction; on a Lua error, explicit rollback, or a staging
failure, it rolls back and rethrows/returns the structured failure. A duplicate
first operation creates no transaction and is returned as its ordinary durable
duplicate result. Advanced users may keep using the explicit
`workflow:append_outbox`, `accept_inbox`, and `accept_command` methods when
they need to inspect and control each transaction step directly.

Returning normally without an anchor operation is `LC_ERR_INVALID`; the façade
does not commit an empty pseudo-transaction. A duplicate anchor is the sole
normal callback path without a concrete transaction. The callback may inspect
that duplicate result and return, after which the façade returns it without a
terminal decision.

On success, `workflow:transaction(fn)` returns the same commit result as the
explicit transaction's `commit()`, including `outbox_receipts`. This gives a
host its forwardable keys only after the durable decision. A duplicate first
append instead returns its existing `receipt` and no commit result, because no
new effect was committed.

`run` is a blocking owner-state loop for a dedicated worker or service process.
`pump` invokes at most its configured bounded amount of work synchronously on
the calling Lua state, for hosts with their own event loop. Neither permits a
private liblockdc thread to access Lua. Calling either from an HTTP route is
supported only insofar as the host itself allows it, but is documented as an
incorrect deployment for foreign-effect dispatch because it puts effect latency
on the request path.

`pump(options)` defaults to one job and a zero wait. Its `max_jobs` must be a
small positive bounded integer and its `timeout_ms` is bounded by the
dispatcher shutdown/request limit. It returns the number of handler outcomes
processed and never starts a second Lua owner loop. A host schedules another
pump when it is ready; liblockdc does not create an event-loop thread.

`run`/`pump` bind exactly one Lua owner state and one handler map for the
dispatcher lifetime. A second concurrent run/pump, a different Lua state, or a
conflicting handler map fails deterministically. Handler registrations are
validated before activation: every key is a non-empty outbox kind and no key is
registered twice. Unhandled kinds remain available through raw `next` for
advanced applications; liblockdc does not invent a Vectis policy for them.

A Lua handler receives a claimed job with a handler-scoped streaming
`payload_source()`. It may consume the source during that call but cannot
retain it after the handler returns. `job:complete()`, `job:retry(opts)`, and
`job:dead_letter(diagnostic)` create typed outcomes; the façade applies the
actual terminal mutation after the handler returns. An exception, cancellation,
or handler deadline produces the configured retry outcome unless an explicit
adapter policy selects dead-lettering. The façade renews a live claim while a
handler is executing and reports renewal/terminal failures without pretending
the job succeeded.

`retry` supports an optional bounded `delay_seconds` and diagnostic, mapping to
the existing durable retry policy. Rescheduling is therefore a normal retry
terminal outcome, not an in-memory timer.

## Vectis integration contract

Vectis is not implemented in this repository. Its required liblockdc contract
is nevertheless explicit:

```text
route worker                         supervisor process
------------                         ------------------
threadless workflow                  workflow dispatcher + Lua lane
  └─ durable commit ─ receipt key ─> bounded host IPC ─> notify_outbox_key()
       (or no wake)                     full/closed ───> reconcile()
                                                     └──> claim → handler → outcome
```

1. Each application declares one canonical workflow configuration per durable
   namespace.
2. Route workers construct threadless producers using their own post-fork
   clients. They mutate domain state and append outbox effects atomically.
3. After a successful commit, a worker copies every returned receipt key to
   Vectis's bounded workflow-key channel. On its first full send it emits one
   payload-free reconciliation wake; it never blocks a route or attempts effect
   delivery.
4. The supervisor process owns a separate client, obtains the one local
   dispatcher, receives keys/wakes, and calls `notify_outbox_key` or
   `reconcile`.
5. The supervisor requests a job only when its chosen logical supervisor and
   Lua lane have capacity. It applies the job outcome after the handler returns.

The channel carries copied receipt-key bytes only. It is neither a second queue
nor a persistence layer. A failed channel send, overflow, restart, or lost
wake changes latency only; reconciliation recovers durable work. Vectis must
reject duplicate `(workflow namespace, kind)` handler ownership at declaration
time and reject high-level unregistered kinds before committing a transaction.

## Documentation and examples

The user-facing outbox documentation must explain the complete deployment
pattern, not merely individual calls:

1. a request transaction commits domain state and durable intent together;
2. it returns immediately after commit, never after delivery;
3. receipt keys are an optional low-latency wake only after commit;
4. a dedicated dispatcher/worker performs the foreign effect with `effect_key`
   idempotency;
5. completion, retry/reschedule, and dead-lettering are durable terminal
   decisions; and
6. restart and reconciliation repair every missing wake.

Examples must include a direct liblockdc deployment with a producer process
and a dedicated Lua dispatcher process, plus the Vectis-facing lifecycle
diagram above.
They must never demonstrate `dispatcher:run()` inside a route handler or an
implicit dispatcher created by producer construction.

## Required verification

Unit, integration, Lua, and end-to-end coverage must prove observable
invariants, including failure paths:

- `new_workflow()` creates no dispatcher thread, client clone, claim, or scan;
- producer transaction, command, inbox, outbox, XA, rollback, and receipt
  semantics remain unchanged for Pouch;
- compatible concurrent acquisition returns exactly one dispatcher, while a
  configuration mismatch, stop race, failed dispatcher, and client close leave
  no stale or dangling instance;
- workflow attachment performs post-commit local wake only after durable
  success; workflow close does not stop the dispatcher; uncommitted close
  cannot expose a wakeable receipt;
- commit-result allocation failure occurs before a terminal vote; an
  indeterminate terminal error exposes no fresh receipt and is repaired only
  through durable recovery;
- direct notification uses no namespace scan and no claim before consumer
  demand, coalesces duplicates, and queue overflow/restart/closed IPC
  equivalently recover through reconciliation;
- reconciliation uses bounded streamed pages and a resumable cursor under a
  full candidate budget, without materializing or preclaiming the namespace;
- raw C and Lua dispatch preserve payload streaming, claim renewal, complete,
  retry/reschedule, dead-letter, terminal failure, expiry, and startup replay;
- Lua run/pump executes only on its caller Lua state; handler exceptions,
  cancellation, renewal failure, terminal failure, empty transaction, duplicate
  anchor, stop, and duplicate binding are deterministic;
- stop timeout, later wait, and client-close invalidation preserve handed-out
  job recovery without leaving a live thread or dangling client reference;
- multiple Lua worker processes safely compete for a Pouch namespace in its
  supported shared-root configuration; and
- performance gates demonstrate bounded candidate memory and deterministic
  query/claim counts for direct wake and reconciliation, and that passive
  producers add no dispatcher/recovery overhead to request construction or
  commits.

The full release matrix, Lua SDK/package checks, and Pouch e2e coverage must
run after the cutover. Vectis-specific supervisor and IPC tests belong in
Vectis, but liblockdc supplies deterministic dispatcher-notification fixtures
that let Vectis prove its integration without timing sleeps.
