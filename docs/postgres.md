# PostgreSQL participant driver

The optional C participant driver uses PostgreSQL's native two-phase commands
through cpktdb's public C89 facade. Pouch remains the transaction coordinator;
`.txns` holds its immutable decision and `.xa` holds resource/branch identity and
progress. The provider saves no application SQL and performs no SQL replay.

The native C primitives are available through `<lc/postgres.h>`; the managed C
coordinator is declared in `<lc/lc.h>`. The complete implicit-XA integration
is governed by the [XA participation contract](xa.md).
The [Lua facade](lua.md#public-resource-and-branch-parity) exposes those public
operations, including manual branch handling and native diagnostics. The separate
optional `lockdc.postgres` module/rock supplies explicit driver registration;
fully static embeddings preload the same entry point.

## Build, link and install

Core builds default to `LOCKDC_BUILD_POSTGRES=OFF`. They require no cpktdb files
and produce the same `liblockdc.a` and `liblockdc.so` bytes with either selection.
Prepare the optional SDK with `scripts/deps.sh deps-x86_64-linux-gnu --with-postgres`
and configure with `-DLOCKDC_BUILD_POSTGRES=ON` to build `liblockdc_postgres.a` and
`liblockdc_postgres.so`.

Applications explicitly link the provider. There is no runtime plugin loader:

```cmake
find_package(lockdc_postgres CONFIG REQUIRED COMPONENTS static)
target_link_libraries(application PRIVATE lockdc::postgres_static)
```

Use component `shared` and `lockdc::postgres_shared` for shared linkage. Both
variants use the same C registration and operation interfaces. Fully static
applications also select static system/runtime libraries in their toolchain;
the provider target supplies the official static cpktdb closure. Shared
applications require the directly linked provider and native client libraries
at startup. Omitting this capability means omitting its application link group.

Core installation components remain `runtime` and `dev`. Optional installation
components are `postgres-runtime` and `postgres-dev`; these keep provider files
out of core release archives. An unqualified CMake install installs all enabled
components. With the capability enabled, ordinary package assembly additionally
creates `liblockdc-postgres-VERSION-TARGET.tar.gz`. This separate provider SDK
contains both available link variants, its public header, CMake/pkg-config
imports, notices, licenses and consumed SDK provenance. It includes no core
library or native-client SDK. Install it alongside the matching core SDK and
the compatible official cpkt/cpktdb SDKs; their prefixes may differ. The complete
release checksum inventory includes selected provider archives.

The separate pkg-config module is `lockdc-postgres`. As with cpktdb, pkg-config
consumers must perform the upstream CMake compatibility preflight before using
its link flags. cpktdb's minimum compatible core and core's declared compatibility
floor govern that check; producer package IDs are provenance, not a consumer
byte-equality requirement. Core package metadata never discovers cpktdb.

## Managed C transactions

Open `lc_xa_coordinator` with the retained Pouch client. Register each explicitly
linked provider through `lc_xa_coordinator_register_start`, passing
`lc_postgres_driver()` and its `lc_postgres_config`. Registration copies the
configuration before native work begins. Resource IDs remain stable across process
restart; credentials are supplied again from application configuration.

`lc_xa_coordinator_begin_start` creates an independent global ID before any
business lease or SQL branch. After success, use
`lc_xa_coordinator_operation_take_transaction` once. Enroll the registered resource
IDs with `lc_xa_transaction_enroll_start`, and use `lc_xa_transaction_acquire_start`
for Pouch leases under that same ID. Its successful operation transfers a lease
through `lc_xa_coordinator_operation_take_lease`; existing state/attachment methods
then stage application work. Enroll every participant before preparing.

`lc_xa_transaction_borrow` exposes each exact branch connection through the public
generic borrow. Release all SQL borrows, results and protocol work before calling
`lc_xa_transaction_commit_start`. The coordinator seals enrollment, prepares each
native branch and Pouch member, follows the immutable `.txns` decision, resolves
SQL and finally applies Pouch. A failed prepare selects ABORT; a failure after
durable COMMIT preserves COMMIT. `lc_xa_transaction_rollback_start` cannot reverse
an existing COMMIT.

Drive every operation with `lc_xa_coordinator_operation_advance` and its wait
descriptor. Honor the descriptor's absolute deadline, including an immediately
due deadline with a negative fd. `LC_ERR_PENDING` with `result.done == 0` needs
further advancement; `result.done == 1` is terminal even when its return code is
pending. Only one managed operation runs on a coordinator at a time. Close its
operation before starting another. `result.txn_id` exists before the first journal
write and remains available after errors/cancellation; `decision` is observed from
the journal and `applied` acknowledges proven participant application. An error
can accompany an applied ABORT, or an acknowledged COMMIT after a lost response.

The owner lease defaults to 30 seconds. Driven native operations renew it through
the journal worker. During long application SQL work, explicitly drive
`lc_xa_transaction_renew_start`. Native connections stay on their owning
process/thread; journal fsync never runs in the socket loop. Final blocking closes
wait for durability cleanup, invalidate native access and relinquish ownership.
Operations/transactions retain their coordinator, and results/borrows retain native
validity tokens across closure.

After restart, register the same resources and call
`lc_xa_coordinator_recover_start` with the saved global ID. Active ownership returns
pending. Fenced abandoned work without COMMIT becomes ABORT. Recovery never repeats
application SQL.
Closing a rejected recovery attempt does not release a live original claim,
including when both handles use the same coordinator. Cleanup releases only
ownership installed by its own attempt, including partial begin publication.
Ownerless raw terminal replays retain the recorded epoch; they cannot fence a
managed owner by supplying a different term. Ownership changes still require
the fenced recovery claim.
Missing capability retains the journal and keeps Pouch publication
closed. The [compiled integration example](../tests/integration/test_postgres_xa.c)
exercises these operations across two independent PostgreSQL servers and Pouch,
including acknowledgement loss and server restart.

## Completed coordinator history

Zero-initialize `lc_xa_coordinator_config` before setting individual fields.
`completed_retention_seconds` defaults to 30 days when zero; `-1` disables age
retirement. `completed_max_records` defaults to zero (unlimited); a positive
limit retires the oldest eligible histories first. Both policies are independent
of completed outbox envelopes, inbox receipts, commands and dead letters.
Coordinators sharing one Pouch root should use the same policy.

The first proven global application stores a timestamp under `.xa/completed/`.
It contains no SQL or second decision. Native branch completion alone does not
start the clock. Repeating completion or recovering a completed transaction
returns its retained outcome without requiring registered PostgreSQL resources
or refreshing its age. Undecided transactions and decisions with missing
acknowledgements never expire. Live owners remain protected even after completion;
close a completed transaction to release that protection, or let its owner lease
expire. A count cap can temporarily be exceeded while owners are live.

The journal worker maintains history before its first operation and once per
minute while idle. Enabled count caps also run at final transaction release.
`lc_xa_coordinator_retire_start()` runs the same maintenance explicitly; drive
the operation to inspect errors and `result->retired_count`. Automatic errors
are logged and retried on the next maintenance pass.

Retirement first removes recovery ownership under the transaction lock, then
removes branch descriptors and the `.txns` decision, and removes the completion
marker last. Interrupted deletion resumes on a later pass, including after a
restart. A remaining completion marker also prevents adoption from replacing
an expired transaction during interrupted deletion. Recovery past that
ownership barrier returns `LC_ERR_INVALID` with
`server_code="not_found"`; it never reconstructs a decision or repeats SQL.
Business state and outbox/receipt records use their own lifecycles.

## Outbox composition

An outbox transaction reserves a stable ID at `begin()` without writing a marker.
`lc_outbox_transaction_id()` exposes it before any lease/effect exists. For SQL-first
work, begin a managed transaction and bind an empty outbox with
`lc_outbox_transaction_bind_xa()`. For outbox-first work, drive
`lc_xa_coordinator_adopt_start()` with the outbox ID, then bind its returned
transaction. Adoption requires unvoted local work, no external branches and no
previous ownership or decision. A live binding must use the same Pouch root.

Stage domain state/attachments, inbox receipts, command acceptance/completion and
effects through that outbox receiver. They use the managed global ID. Call
`lc_outbox_transaction_commit_start()` or `rollback_start()` and drive the ordinary
coordinator operation. The outbox facade allocates receipt identities before any
terminal work begins; it does not make another decision. After proven COMMIT
application, `lc_outbox_transaction_commit()` transfers the preallocated result
once. Before application it returns pending; ABORT yields no success receipts.
Closing a facade during a pending operation preserves its coordinator-owned
reference and cannot reverse COMMIT.

The common Pouch application path wakes an already registered compatible outbox
dispatcher. Direct completion uses the same bounded notification path. Neither
path starts a dispatcher; notification failure leaves durable rediscovery work.
After a process restart, recover the saved global ID and use the same immutable
effect/command identity for duplicate/status lookup. Recovery never appends or
replays application work to rebuild receipts.

## Registration and ownership

For provider-independent C code, explicitly register the linked provider through
`lc_xa_resource_open(store, lc_postgres_driver(), &config, &resource, &error)`.
`config` is an `lc_postgres_config`. Core copies the public `lc_xa_driver` table,
retains the journal and exposes `lc_xa_resource_*`, `lc_xa_branch_*` and
`lc_xa_operation_*` operations without PostgreSQL types in its headers.
The table ABI has an explicit size/version; missing callbacks or unsupported
versions fail before opening native work. Registration still validates the
backend before any branch starts.

Drive operations with `lc_xa_operation_advance` and `lc_xa_operation_wait`.
`lc_xa_operation_result` provides a terminal snapshot; successful start/recover
operations transfer their branch exactly once through
`lc_xa_operation_take_branch`. Taking a branch allocates nothing. Closing an
operation closes any unclaimed branch. Taking a branch keeps the creating
operation's lifetime pin: closing the branch first defers native destruction
until that operation closes. Closing a resource prevents new starts and borrows,
including through retained branches. Existing operations can finish; retained
branches remain inspectable and closeable. Cancellation preserves the underlying
driver's durable recovery behavior.

The native and generic interfaces have different lifetime responsibilities:

| Interface | Journal lifetime | Branch and operation lifetime |
| --- | --- | --- |
| Native `lc_postgres_*` | Caller keeps the borrowed journal alive through resource close. | Close a branch's active operation before the branch. Resource close consumes remaining children; never reuse their pointers. |
| Generic `lc_xa_resource_*` | Resource retains the journal independently. | Operations retain resources/branches, including transferred branches until operation close. Resource close prevents new work and releases its caller reference. |
| Managed `lc_xa_coordinator_*` | Coordinator retains the local Pouch client/journal. | Operations and transactions retain their coordinator; releasing its caller reference leaves children usable. |

Native start/recovery success returns one owned branch in `result->branch`.
Prepare and resolve results only echo the existing branch; they do not transfer
another reference. Generic callers use `lc_xa_operation_take_branch()` exactly
once instead. In both interfaces, copy result fields before closing the operation.

`lc_xa_branch_borrow` supplies the same public generic connection view as the
native interface. Static and shared consumers use identical registration code;
applications that omit the provider have no PostgreSQL callback registration or
native dependency. These resource primitives still use the existing explicit
transaction decision/replay interfaces; managed orchestration follows the
[XA participation contract](xa.md).

Open a local Pouch client, then call `lc_xa_store_open` to retain its journal.
Open a resource with `lc_postgres_open`, passing that store, a stable generated
resource ID and copied connection configuration. Drive `lc_postgres_register_start`
to completion before starting branches. Keep the store alive until
`lc_postgres_close_blocking` returns; closing the original client does not invalidate
the retained store.

Registration validates PostgreSQL 14 or newer, a primary with positive native
2PC capacity, and cluster system identifier/database OID/role OID. The role needs
EXECUTE on `pg_control_system()` as well as ordinary application access. Configure
`max_prepared_transactions` and provision permissions outside the driver. No
schema, extension, custom function, DDL or extra prepared transaction is installed.
A resource ID cannot be re-registered against a different backend identity.

Provide an explicit Unix socket host, numeric host or numeric `hostaddr`.
Resolve names outside the socket owner loop; retain `host` alongside `hostaddr`
when authentication or TLS requires it. One registered resource uses one server
target; comma-separated failover hosts are rejected. Recovery credentials come
from this resource configuration and are never written into coordinator records.

A resource has one active operation at a time. Multiple enrolled branches can
remain open between operations. Native connections stay on the resource's owning
thread/process; a serialized worker performs journal operations and never calls
application or Lua callbacks. Open handles after fork. Inherited handles fail
validation before native operations.

## Driving a transaction

1. Generate a global ID with `lc_xid_new`, or use the existing implicit-XA ID.
   SQL may be the first or only participant; no Pouch business lease is required.
2. Drive `lc_postgres_branch_start`. It opens the branch connection, obtains
   `pg_current_xact_id()` as exact decimal text and durably enrolls that identity.
   Enroll all other participants before preparing any branch.
3. Borrow the exact connection with `lc_postgres_branch_borrow` and
   `lc_sql_borrow_postgres`. Execute application SQL through the public cpkt
   facade or a compatible query adapter. Drain the protocol, release results,
   then release the borrow. Never free/reset the borrowed connection.
4. Drive `lc_postgres_prepare_start`. It verifies that application code has not
   replaced the native transaction or changed its prepared-owner role, durably
   records intent, sends PREPARE and
   records a YES only after checked acknowledgement retrieval.
5. Follow the existing coordinator's decision. Pouch implicit XA records COMMIT
   when all participants vote YES; ABORT must be selected before that durable
   COMMIT. Once a decision exists, retries must preserve it.
6. Drive `lc_postgres_resolve_start` in that direction. It verifies the durable
   decision before closing owned unprepared work or submitting phase two.
   Finish with existing transaction replay or lease release to apply Pouch
   participants and open publication. `LC_ERR_PENDING` retains the durable
   decision while external application is incomplete.

Use `lc_postgres_operation_advance` to perform bounded nonblocking native work.
On `LC_ERR_PENDING`, inspect `lc_postgres_operation_result`: a non-NULL result
means this operation ended pending native recovery; otherwise wait using
`lc_postgres_operation_wait`. Its descriptor is the current socket or journal
wake pipe; READ is 1 and WRITE is 2. Enforce its absolute monotonic deadline in
the owner loop. Start deadlines use zero for the 30-second default, -1 for
infinity, or a positive absolute CLOCK_MONOTONIC timestamp in milliseconds.
Close each operation before starting the next.

`lc_postgres_operation_close_blocking` and `lc_postgres_close_blocking` can wait
for an already-submitted durability job. They do not wait for network progress.
Cancellation closes owned native work while preserving possible preparation and
journal identity. A prepared branch survives connection/resource close and needs
recovery. A failed operation's result retains the durable identity and SQLSTATE;
raw SQL, parameters, credentials and provider diagnostic bodies are not logged.

Borrow tokens outlive closed branch connections. Stale, thread and fork checks
precede native pointer access. Result copies that retain connection events also
retain these tokens. Results without connection events have independent storage.
All native calls/results remain the application's responsibility on the owning
thread. Releasing a borrow with pending protocol/results invalidates the branch
and leaves cleanup recoverable; it cannot produce a successful prepare vote.
Restore temporary SQL roles before prepare; the prepared owner must match the
registered resource's role identity.

Query adapters use `lc_postgres_branch_borrow_generic` and the core
`lc_xa_borrow` interface. Initialize `lc_xa_connection_view`, then obtain its
validated ABI size/version, resource/transaction/branch IDs, native connection,
owner process/thread identity and generation with `lc_xa_borrow_view`. Retain
the borrow for each cursor/result/callback context and release those references
after unwinding native work. The final release ends the native scope. Closing
the branch invalidates native access immediately while retained adapter shells
remain safe to validate/release. Core headers and this reference/view machinery
require no native database SDK. For PostgreSQL the native pointer is exactly
the public cpkt connection, with no transfer of ownership.

## Recovery and observation

Re-register the same resource identity, then drive `lc_postgres_recover_start`
with the retained global ID, branch ID and immutable decision. The driver reads
its durable descriptor and authoritative YES vote, verifies backend and generated
GID, and inspects exact prepared identity before any destructive command.

A missing GID is checked through the full native XID. A matching native outcome
records native evidence; a contrary outcome is an integrity error and blocks
publication. In-progress native work returns pending. SQL NULL permits protocol
reconciliation only with a durable native preparation acknowledgement/observation
and matching decision/resolution intent;
missing enrollment, query failure or arbitrary unknown work cannot manufacture
completion. Completion evidence distinguishes acknowledgement, native status
and protocol reconciliation. Concurrent/repeated completion returns the first
persisted evidence. Discovery of a matching prepared branch after ABORT is
journaled before phase two, independently of the coordinator vote. A lost
prepare acknowledgement followed by a lost rollback acknowledgement therefore
remains reconcilable after native history expires, without creating a YES vote.

`lc_postgres_status_start` observes the enrolled XID without changing a decision,
resolving work or opening publication. `lc_postgres_inventory_start` returns at
most 64 rows in GID order, scoped to the registered resource prefix/database/role.
Pass the last returned GID to continue. Inventory membership alone does not
prove ownership; resolution always cross-checks the durable journal.
Journal reads distinguish the descriptor's `prepared_observed` (native prepare
acknowledgement or observation) from the separate authoritative coordinator YES
vote. Discovering prepared work after ABORT records observation for cleanup;
it never changes the vote or global decision.

The public-only executable usage and fault cases are in
[`tests/integration/test_postgres_xa.c`](../tests/integration/test_postgres_xa.c).
With the provider selected, build `lc_integration_postgres_static` and
`lc_integration_postgres_shared`, then run `ctest -R '^postgres_xa_native_test$'`.
The rootless Podman Kube runner starts two independent pinned PostgreSQL servers
and uses one Pouch store per consumer. Each cross-resource case stages SQL in
both servers and state in that Pouch store under the same global ID. Cases cover
commit, SQL failure/abort, lost prepare and resolution acknowledgements, owner
reopen before/after COMMIT, prepared-capacity exhaustion and a server outage with
prepared work. The `postgres_xa_pg15_test` through `postgres_xa_pg18_test` checks
run the same cases on pinned PostgreSQL 15 through 18 servers; the base check
uses PostgreSQL 14. Run `ctest -L postgres -j1 --output-on-failure` for this matrix.
Protocol results and checked-retrieval fault barriers determine progress; deadlines bound
waits without depending on an arbitrary iteration count.
The runner probes real readiness and removes/asserts absence of its owned pod
and both containers on every exit.
Its data, credentials and diagnostics remain under ignored `build/devenv/`.
