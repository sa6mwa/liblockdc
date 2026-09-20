# Vectis outbox integration handover

Status: implementation handover guide. This is the integration entry point for
a Vectis supervisor. It defines the contract that Vectis must preserve when it
uses liblockdc's transactional outbox. It does not implement Vectis's
supervisor, IPC, handler registry, or application workflow policy.

Read this document first. It condenses the required host behaviour without
replacing the detailed authorities:

- [Outbox dispatch architecture](outbox-dispatch-architecture.md) defines the
  C lifecycle, ownership, threading, and dispatcher contract.
- [Transactional messaging](inbox-outbox.md) defines durable inbox, command,
  outbox, claim, retry, and transaction semantics.
- [Pouch storage](pouch-storage.md) defines local-root operation and recovery.
- [Typed Pouch open settings](pouch-open-settings-api.md) defines the preferred
  C and Lua configuration surface for a local root.
- [Lua bindings](lua.md) defines the complete Lua facade. The Vectis dispatcher
  integration itself uses raw C pull mode, not Lua `run()` or `pump()`.

## Scope and ownership

liblockdc owns durable outbox facts and their transitions:

- atomic enrollment of domain state, command/inbox facts, and outbox intent;
- immutable effect envelopes and streamed payloads;
- commit-published receipts, claims, renewals, retry/reschedule, completion,
  dead letters, and recovery; and
- a bounded local notification queue plus bounded durable reconciliation.

Vectis owns everything that turns durable work into an application effect:

- route-worker and supervisor process topology;
- the bounded route-to-supervisor IPC channel and its wake coalescing;
- logical supervisor lanes, handler registration, Lua-state ownership,
  capacity policy, ingress, and shutdown; and
- the foreign-effect adapter and its idempotency use of `effect_key`.

The IPC channel is not a second queue and cannot be an authority for delivery.
It carries a copied, commit-published outbox key as a latency hint. The Pouch
or Lockd namespace is the sole durable authority. Losing, duplicating, or
overflowing a hint may delay delivery but must never lose durable work.

This facility is a transactional outbox, not a business workflow engine.
Vectis may build activities, graphs, and supervisor policy on top of it; those
concepts are not interpreted or persisted by liblockdc.

This handover covers the implemented and verified Pouch deployment. The remote
Lockd protocol has the same intended outbox model, but its multi-participant
implicit-XA enrollment remains outside the currently verified operational
scope. A Vectis release using this integration must therefore use Pouch until
that remote endpoint limitation is resolved.

## Required process topology

Use one post-fork liblockdc client per process. A route worker owns a
threadless `lc_outbox`; the supervisor owns its own outbox and the one explicit
`lc_outbox_dispatcher` compatible with its canonical configuration.

```text
route worker                                      supervisor process
------------                                      ------------------
client + threadless outbox                         client + outbox + dispatcher
    |                                                        |
    | durable transaction commit                             | key / wake IPC
    |     └── commit-published receipt key ──────────────────> notify_outbox_key()
    |                    channel full/lost ──────────────────> reconcile()
    |                                                        |
    |                                                        v
    |                                          capacity-aware next() -> claimed job
    |                                                        |
    └──────────────────────────────────────── Vectis-owned Lua lane / adapter
                                                             |
                                                  complete | retry | dead_letter
```

Do not inherit a Pouch client, outbox, dispatcher, or its locks across `fork`.
Open each process's client after the fork. For a remote root, route and
supervisor clients are naturally separate connections. For a Pouch root shared
by more than one live process, every client that opens that root must select
the supported shared-root mode with typed settings:

```c
lc_pouch_settings pouch;

lc_pouch_settings_init(&pouch);
pouch.set_mask = LC_POUCH_SETTING_SINGLE_WRITER;
pouch.single_writer = 0;
config.pouch_settings = &pouch;
```

The default remains exclusive `single_writer=true`. Do not use an endpoint
query string in new code; `?single_writer=false` remains only a compatibility
input. Shared-root mode is correct for this topology but has lower throughput
than the exclusive-writer performance path, so use it only when the processes
really share one Pouch root.

One application namespace has one canonical `lc_outbox_config`. Each process
must use compatible values when it creates an outbox or obtains the dispatcher.
Configuration disagreement is an error, not a signal to create another local
dispatcher.

For shared-root Pouch dispatch, set a positive
`lc_outbox_config.recovery_interval_seconds` that meets the application's
maximum delayed-delivery objective. Its Pouch default is zero, which disables
routine recovery scans to keep passive local producers cheap. Startup,
explicit reconciliation, and notification overflow still recover work, but a
positive cadence is the eventual-recovery backstop for a completely lost
route-to-supervisor wake. A Pouch root with `query_indexing=false` is also
supported: dispatcher recovery deliberately uses the bounded explicit `scan`
engine and never re-enables derived index maintenance. It trades recovery scan
cost for the root's non-query write-path benefit.

## Producer contract

The route path is allowed to create an outbox and a lazy transaction. It is
not allowed to start a private dispatcher, perform delivery, scan for pending
work, or block on supervisor capacity.

1. Create a threadless producer with `lc_client_new_outbox()` (or attach an
   already compatible local dispatcher with
   `lc_client_new_outbox_with_dispatcher()` when that process deliberately
   uses the local fast wake path).
2. Begin a lazy transaction with `lc_outbox_begin()`, or use the fresh
   transaction returned by `lc_outbox_append()`, `lc_outbox_accept_command()`,
   or `lc_outbox_accept_inbox()`.
3. Acquire and mutate the domain participant through
   `lc_outbox_transaction_acquire()`, and append the effect through
   `lc_outbox_transaction_append()`. The first participating operation mints
   the endpoint transaction identity; later participants join that decision.
4. Commit with `lc_outbox_transaction_commit()`. Only a successful commit
   returns fresh `lc_outbox_commit_result.outbox_receipts`.
5. Copy every fresh receipt key to the bounded Vectis IPC channel. Never
   derive an outbox key from an operation identity or send a key before a
   successful commit.
6. Return from the route. Receipt forwarding is an optional wake, not effect
   execution and not a condition for accepting the request.

A duplicate receipt from an append/command/inbox call denotes already durable
work. It can be forwarded as a harmless latency hint, but it is not a fresh
commit receipt. A failed, rolled-back, or indeterminate commit returns no
fresh receipt and must not produce a key notification.

If a duplicate is found after a domain participant has joined the transaction,
the transaction becomes rollback-only. Vectis must treat its later commit as
invalid: this prevents a domain update from becoming visible without its
idempotency or outbox boundary. An empty lazy transaction is invalid to commit
and is a local no-op to close.

### Bounded IPC rule

The producer attempts a non-blocking send for each fresh receipt. On the first
full send, it emits one payload-free `OUTBOX_RECONCILE` wake and suppresses
further such wakes until the supervisor has had a chance to reconcile. If the
channel is closed, unavailable, or the worker exits after commit, do not retry
delivery in the route process. Startup and reconciliation repair the delay.

The channel payload is only the copied opaque receipt key. It must not contain
the effect payload, an authority token, a mutable job object, or an instruction
to bypass normal claim validation. Duplicate keys are expected and safe.

## Supervisor and dispatcher contract

The supervisor creates its own client and outbox, then obtains the explicit
dispatcher with `lc_outbox_dispatcher_get_or_start()`. This starts or returns
the one compatible process-local dispatcher. Creating an `lc_outbox` alone
creates neither a thread nor a recovery loop.

On IPC receipt:

- for an outbox key, call `lc_outbox_dispatcher_notify_outbox_key()`;
- for `OUTBOX_RECONCILE`, call `lc_outbox_dispatcher_reconcile()`; and
- after supervisor startup, call `reconcile()` before treating the IPC channel
  as current.

`notify_outbox_key()` copies a committed key into a bounded local candidate
queue. It does not query the namespace. Duplicates, a full candidate queue, or
a lost notification are repaired by reconciliation. `reconcile()` requests
bounded durable recovery: it uses the normal index when available and an
explicit scan when Pouch query indexing is disabled. It does not authorize
Vectis to materialize the full namespace or preclaim its work.

Vectis must call `lc_outbox_dispatcher_next()` only when a selected logical
supervisor lane and its Lua execution lane have capacity for one job. A
returned job is already claimed. The supervisor then streams its payload,
invokes the Vectis-selected handler, and applies exactly one terminal result:

- `lc_outbox_job_complete()` after the foreign effect has succeeded;
- `lc_outbox_job_retry()` for a retry or bounded reschedule; or
- `lc_outbox_job_dead_letter()` for a terminal application failure.

For a long foreign operation, call `lc_outbox_job_renew()` before its claim
expires. `lc_outbox_job_write_payload()` is streaming: the host must send its
sink directly to the foreign adapter rather than materializing the entire
payload. A successful terminal operation consumes the job. On a terminal
operation error, the job remains active and that same terminal operation may
be retried until claim expiry. `lc_outbox_job_close()` only releases the local
handle; it does not decide the durable job.

Use raw C pull mode for the Vectis supervisor. Do not call Lua
`dispatcher:run()` or `dispatcher:pump()` for this integration: those modes
bind a dispatcher to one Lua state and its handler table, while Vectis owns the
router that selects among its logical lanes. The C dispatcher knows only about
durable claimed jobs and their terminal outcomes.

`lc_outbox_dispatcher_next(dispatcher, 0, ...)` is a strictly in-memory,
non-query probe. It is suitable only when a lane has capacity and a direct
candidate is already available. A blocking `next()` call, explicit
`reconcile()`, startup recovery, and a notification overflow are the paths
that can request durable recovery. Never use a periodic `next(0)` polling loop
as a recovery mechanism.

## Failure and recovery matrix

| Event | Required Vectis response | Durable result |
| --- | --- | --- |
| Commit fails, rolls back, or is indeterminate | Do not forward a fresh receipt. Close/rollback as required and begin a new application attempt only when appropriate. | No unsafe wake is published; durable recovery remains authoritative. |
| Commit succeeds but key IPC send fails or worker dies | Do not deliver from the route process. A coalesced wake, the positive shared-Pouch recovery cadence, or supervisor startup calls `reconcile()`. | The effect remains pending and is rediscovered. |
| IPC channel is full | Send one coalesced reconciliation wake when possible; suppress repeated full-channel signals until reconciliation. | Overflow changes latency only. |
| Key notification is duplicated or stale | Call `notify_outbox_key()` normally. | Claim/recheck prevents duplicate terminal ownership. |
| Supervisor restarts | Reopen its client/outbox after process creation and request reconciliation before serving normal lanes. | Pending, delayed, and expired claimed work is recovered. |
| A handler fails before a terminal decision | Close the local job or let its claim expire; do not report a false completion. | The durable claim expires and recovery makes it eligible again. |
| Foreign operation runs longer than claim TTL | Renew before expiry; if renewal fails, stop assuming exclusive ownership and follow the durable error path. | A claim is never silently extended in memory. |
| Terminal mutation fails | Retry the same terminal mutation while the job remains claimed, or relinquish it for expiry recovery. | No terminal state is assumed until durable success. |
| Supervisor is stopping | Stop accepting new lane work, finish or relinquish handed-out jobs, then call dispatcher stop/wait according to the host shutdown deadline. | Unfinished work remains recoverable. |

At-least-once delivery is intentional. `effect_key` is immutable and must be
passed unchanged to the foreign system as its idempotency key. liblockdc cannot
make an external HTTP, mail, payment, or other foreign side effect exactly
once; Vectis's adapter must make a retry with the same `effect_key` safe.

## Ownership and lifecycle rules

- `lc_outbox_close()` releases the producer and its dispatcher reference; it
  never stops shared dispatcher work.
- `lc_outbox_dispatcher_close()` releases a dispatcher handle only.
  `lc_outbox_dispatcher_stop()` requests stopping and
  `lc_outbox_dispatcher_wait()` observes that shutdown. Stopping is an
  explicit supervisor decision.
- A job returned by `next()` belongs to the caller until a successful terminal
  operation or `lc_outbox_job_close()`. Do not pass that mutable handle over
  route IPC or between concurrent Lua lanes.
- Closing a client stops and joins its private dispatcher infrastructure but
  does not turn a handed-out job into a terminal durable decision. The host
  must decide or relinquish the job according to its shutdown path.
- Each process must close its outbox, dispatcher handles, and client in its
  own lifecycle. No handle crosses a fork boundary.

## Observability and operating signals

Poll `lc_outbox_dispatcher_get_stats()` from the supervisor's normal metrics
path. The snapshot is cheap and process-local. Important fields are:

- `running`, `pending_candidates`, `delayed_wakes`, and `waiting_consumers`
  describe current local demand and bounded queues;
- `direct_notifications` and `notification_overflows` reveal IPC/local-wake
  pressure;
- `recovery_queries` and `recovered_claims` reveal durable reconciliation;
- `claim_losses` and `payload_open_failures` identify contention or payload
  access failures; and
- `last_error` exposes the latest dispatcher error text.

Alerting should distinguish a sustained candidate/recovery backlog from a
temporary notification overflow. The former can indicate insufficient handler
capacity or a failing foreign system; the latter is designed to self-repair by
reconciliation. Do not infer durable queue depth from the process-local
candidate count.

## Integration acceptance checklist

Vectis integration is complete only when its tests prove all of the following
observable properties without timing sleeps:

- a committed domain mutation and a fresh outbox intent appear atomically;
- a failed or rolled-back transaction forwards no fresh receipt key;
- a producer never starts a dispatcher or executes an effect;
- every route-to-supervisor key is copied only from
  `outbox_receipts` after commit;
- duplicate, dropped, full, and closed IPC paths converge through
  `reconcile()` without lost durable work;
- the supervisor calls `next()` only after lane capacity is reserved and never
  uses a key notification as a claim bypass;
- handler success, retry, reschedule, dead letter, terminal-write failure,
  claim expiry, renewal failure, and restart each leave the documented durable
  outcome;
- the foreign adapter receives the stable `effect_key` on every attempt;
- Pouch multi-process deployments explicitly use `single_writer=false` in
  every process, while a single-process deployment retains the exclusive
  default; and
- a large pending namespace is reconciled through bounded, paged recovery,
  without full payload materialization or unbounded candidate allocation.

## Lua facade exposed by Vectis

Vectis may expose the public `require("lockdc")` package to application Lua.
That package is the supported façade; `lockdc.core` is its native implementation
detail and is not an application integration surface. The façade wraps the C
receiver ownership model rather than creating a second outbox model:

| Need | Lua façade | Contract |
| --- | --- | --- |
| Producer | `client:new_outbox(config[, { dispatcher = dispatcher }])` | Threadless. Use the C-shaped `namespace_name` field. |
| Fresh durable work | `outbox:append`, `accept_command`, `accept_inbox`, `begin`, or `transaction` | Fresh keys appear only in successful `commit_result.outbox_receipts`. |
| Command status/result | `outbox:get_command_receipt`, `write_command_result`, `read_command_result`, `resume_command` | C-shaped names retain the durable receipt/result distinction. |
| Dispatcher signals | `outbox:dispatcher`, `dispatcher:notify_outbox_key`, `stats`, and `reconcile` | Same canonical-config, bounded-notification, and recovery rules as C. |
| Raw claim | `dispatcher:next(timeout_ms)` | Returns an owned `OutboxJob` or `nil` when no work is immediately available. |
| Managed Lua consumer | `dispatcher:run({ handlers = ... })` or bounded `pump(options)` | Valid for a standalone Lua-owned worker only; it cannot share that live dispatcher with raw pull mode. |
| Claimed job | `info`, `write_payload`, `read_payload`, `read_payload_json`, `renew`, `complete`, `retry`, `dead_letter`, `close` | Terminal and ownership rules below apply unchanged. |

The Lua package preserves the same Pouch typed root-open settings through
`lockdc.open({ pouch = { ... } })`. A Vectis route worker and supervisor that
share a Pouch root must both supply `pouch = { single_writer = false }`, and
the supervisor's Lua-created outbox must use the same positive
`recovery_interval_seconds` as the C configuration described above.

`job:write_payload(sink)` is the façade's true streaming operation. It writes
directly to a supplied path, file descriptor, or callback sink. Callback sinks
receive bounded chunks and can fail the operation by returning `nil, message`
or `false, message`. `job:read_payload_json()` first materializes the complete
payload in Lua memory and then decodes it with lonejson; it is appropriate only
for payloads whose bounded size is an intentional application decision. It
must not be represented as a streaming foreign-effect path. The claimed job is
not re-entrant while its callback sink is active: finish the stream before
renewing, selecting an outcome, or closing it.

For the Vectis supervisor architecture, preserve the raw-C pull boundary above.
Vectis owns Lua-lane scheduling and invokes its chosen Lua state only after a
raw C `next()` has returned a claimed job. Its C adapter can stream the payload
into the foreign transport. Exposing `read_payload_json()` to application Lua does
not change that operational path. In particular, Vectis must not call Lua
`dispatcher:run()` or `dispatcher:pump()` on the dispatcher used by its raw-C
supervisor router: first use permanently binds a live dispatcher to either raw
pull or one Lua state/handler map.

Within a Lua-managed `run`/`pump` handler, `complete`, `retry`, and
`dead_letter` select a deferred outcome that the binding applies after the
handler returns. On a job returned by raw `next`, they immediately attempt the
native terminal mutation. `retry()` accepts no argument or a table such as
`{ delay_seconds = 30, diagnostic = "temporary upstream failure" }`; it does
not accept a diagnostic string shorthand. A handler exception or return without
an outcome follows the configured durable retry path. A job must not outlive its
handler invocation, and `stop`, `wait`, or `job:close()` are rejected from that
handler while it owns the active claim.

Review [Lua bindings](lua.md) with this guide when Vectis packages the façade.
The integration tests must separately prove Lua producer receipt forwarding,
the raw-C supervisor boundary, typed shared-Pouch opening, bounded JSON use,
and the handler-mode/raw-pull exclusion.
