# liblockdc TODO

This file tracks the real lockd HTTP surface from `../lockd/internal/httpapi/handler.go` and the Go client behavior from `../lockd/client/`.

## Pouch cutover

- [x] Move the unreleased old pouch implementation, tests, and benchmark
  harnesses under `deprecated/pouch-legacy/` as reference-only material.
- [x] Remove the old pouch store, allocator, logstore, index, integration
  tests, e2e shard, and benchmark matrix from live compilation and CTest
  registration.
- [x] Create the new pouch-native `lc_pouch` handle and module boundaries for
  root layout, path escaping, per-namespace segmented layout initialization,
  and client endpoint construction.
- [x] Replace old source/test boundary checks with pouch cutover checks that prove
  retired files are not live and storage modules stay independent of `liblql`.
- [x] Add first executable pouch cutover coverage for root manifest creation,
  per-namespace `segments`/`snapshots`/`markers`/`index` directories, escaped
  namespace paths, `pouch://` client open, and benchmark temp cleanup.
- [x] Rebuild the pouch storage write/read path on the new architecture:
  metadata, state payloads, objects, staged state, queues, transactions, and
  retention.
  - [x] Add committed-state tombstones, public state remove, and pouch
    `acquire_for_update` success/rollback over the segmented state log.
  - [x] Replace the interim `acquire_for_update` committed-log rollback path
    with staged-state writes, link-style promotion, discard, and public
    staging-key rejection.
  - [x] Add the first rebuildable in-memory state projection cache for
    single-writer reads, with local mutation updates and segment replay rebuild
    on first use.
  - [x] Implement bound lease typed `save()` by streaming lonejson-generated
    mapped JSON through the segmented state update path, with lease load and
    reopen replay coverage.
  - [x] Add pouch-native local lease lifecycle coverage for positive TTL
    acquire expiration, keepalive refresh metadata, invalid TTL rejection, and
    release acknowledgement without routing through deprecated pouch paths.
  - [x] Route pouch client-level `mutate`, bound lease `mutate`, and bound
    lease `mutate_local` through the pouch state path by applying the
    shared mutation planner to current pouch state and persisting the result
    through segmented state writes or staged lease updates with CAS coverage.
  - [x] Add the first pouch object surface by routing client and
    bound-lease attachment upload/list/get/delete/delete-all through durable
    internal attachment records in the segmented state path, with selector by
    name or deterministic pouch attachment id and overwrite/max-size coverage.
  - [x] Add the first pouch queue surface: durable enqueue records,
    stats, ordinary dequeue, dequeue batch, ack, nack, extend, and pouch
    message methods over internal segmented queue records with payload
    roundtrip, visibility, redelivery, and ack coverage.
    - [x] Add the first stateful queue delivery path: `dequeue_with_state`
      now attaches a pouch-local state lease for `q/<queue>/state/<message_id>`,
      patches that lease to the local pouch methods, and covers save/read/ack
      behavior through the public message state handle.
    - [x] Persist queue TTL expiry in durable records and make retry exhaustion
      explicit: expired records are excluded from stats/dequeue, and failure
      nacks that hit `max_attempts` persist a terminal queue status with
      stats/dequeue coverage.
    - [x] Add direct pouch polling subscribe paths: `subscribe` and
      `subscribe_with_state` deliver bounded pages from local dequeue, enforce
      the public explicit ack/nack callback contract, auto-nack missing
      terminal callbacks as failure, and cover stateful delivery.
    - [x] Add the baseline pouch queue watch path: `watch_queue` emits an
      initial polling snapshot, polls local queue stats for signature changes,
      emits changed availability/head events without filesystem notifications,
      and surfaces callback stop/error semantics.
    - [x] Touch best-effort per-queue notification hint files under the public
      namespace `queue-notify/` directory after committed enqueue, dequeue
      visibility, nack, ack, and extend mutations, without letting notification
      failures roll back durable queue state.
  - [x] Add typed committed-state query metadata for `query_hidden` through
    lease/client metadata calls, state-log metadata records, snapshot replay,
    reopen, and version-precondition enforcement.
  - [x] Add durable transaction decision records, replay, staged participant
    recovery, and expired staged-state cleanup on reopen.
    - [x] Add private segmented state-log decision records for staged commit
      and discard, replay them without projecting them as user state, and
      recover decided staged participants whose tombstone was interrupted.
    - [x] Add full transaction participant records and expired undecided staged
      cleanup policy on reopen.
      - [x] Persist pouch transaction coordinator prepare/commit/rollback
        records with participant namespace/key/backend tuples in the internal
        segmented `.lockd/txn` namespace and replay them after reopen.
      - [x] Apply foreground `txn_commit` and `txn_rollback` participant
        records to staged state promotion/discard.
      - [x] Replay committed/rolled-back transaction participant records during
        client open and expire prepared transaction participant records into
        rollback cleanup on reopen.
      - [x] Add decided transaction record garbage collection after idempotent
        replay no longer needs the durable decision body.
      - [x] Apply transaction commit/rollback decisions to staged attachment
        side effects: lease attachment uploads with `txn_id` remain hidden
        until participant commit, are discarded on rollback, and replay during
        client open uses the same staged object records.
      - [x] Apply transaction commit/rollback decisions to staged attachment
        delete and clear side effects: delete markers remain hidden until
        commit, rollback preserves committed attachments, and clear also
        covers same-transaction staged uploads.
      - [x] Apply transaction commit/rollback decisions to staged queue
        side effects: transaction-bound dequeue carries `txn_id` onto the
        message, ack/nack/extend write hidden staged queue records, commit
        promotes them, rollback discards them, and open-time decision replay
        applies the same queued side effects.
      - [x] Route public transaction-bound state `update`, `save`, and
        `mutate` through hidden staged state; indexed `refresh=wait_for`
        queries skip staged state before commit, see it after participant
        commit, discard rollback state, and a mixed state-plus-queue ack
        transaction commits both side effects together.
      - [x] Cover restart transaction replay with indexed `refresh=wait_for`
        query visibility: a committed staged state participant promoted during
        client open is visible through the rebuilt local query index.
      - [x] Cover transaction side effects through queue watch polling:
        `watch_queue` observes a transaction-bound queue ack after participant
        commit changes the queue head from available to unavailable.
      - [x] Cover independent-handle watcher refresh for transaction side
        effects: one pouch client can watch while another client on the same
        root commits the transaction-bound queue ack, forcing state visit
        callbacks to tolerate peer marker cache refresh.
      - [x] Cover forked-process watcher refresh for transaction side effects:
        a parent watcher observes a child process opening its own pouch client
        and committing the transaction-bound queue ack.
      - [x] Cover mixed object/queue transaction composition: one transaction
        can commit staged attachment upload plus staged queue ack, and another
        can roll both side-effect types back together.
  - [x] Add the first metadata-driven retention sweep through pouch
    maintenance: state records carry `updated_at_unix`, the sweep deletes
    expired live state with version preconditions, reports scanned/expired/
    deleted/failed counts, and reruns idempotently without removing namespace
    history files directly.
- [x] Rebuild per-namespace manifest/snapshot lifecycle, manifest repair,
  marker invalidation, compaction scheduling, and obsolete-file cleanup on the
  new `lc_pouch` modules.
  - [x] Touch per-namespace writer markers after committed segmented state
    mutations, with monotonic sequence payloads that alternate size for
    coarse-mtime filesystems.
  - [x] Use per-pouch-handle writer marker identities instead of pid-only
    marker names so same-process handles observe each other as peers.
  - [x] Add reader-side peer marker snapshots that ignore the current writer
    marker and compare peers by deterministic name, size, and modification-time
    fingerprints.
  - [x] Add marker-directory fast-path metadata and periodic forced-refresh
    decision state so cached readers can skip unchanged marker scans without
    suppressing full validation indefinitely.
  - [x] Wire peer-marker snapshots into shared-mode cached namespace
    projections so same-segment peer writes and same-handle shared writes do
    not return stale state.
  - [x] Extend the marker-directory fast path with cached peer stat checks for
    unchanged directory metadata so same-file marker rewrites still invalidate
    cached projections without a full marker directory scan.
  - [x] Add the first state snapshot compaction path: scheduled threshold
    checks after state mutations, manifest-installed state snapshots, replay
    from snapshot plus later segment tails, manifest repair from snapshot files,
    and best-effort compacted segment/prior-snapshot cleanup.
  - [x] Add the first explicit namespace maintenance entry point for pouch
    snapshot compaction: `lc_pouch_maintenance_run()` supports forced
    compaction, threshold-gated `if_needed` behavior, interval gating for
    scheduled checks, and diagnostics for disabled, no-candidate,
    below-threshold, interval, and compacted outcomes.
  - [x] Persist manifest obsolete records for compacted segment/snapshot files
    and retry cleanup on namespace manifest open or post-compaction cleanup,
    pruning obsolete entries only after the target file is deleted or already
    missing.
  - [x] Persist compacted state high-water records in namespace snapshots so
    replayed state/index sequence cannot move backwards after compaction
    removes older segment history, while keeping the high-water control record
    out of user-visible state visits.
  - [x] Add cleanup-only maintenance entry points and cleanup retry accounting:
    maintenance now reports deleted and still-pending obsolete segment/snapshot
    cleanup counts, can retry cleanup without running compaction, and preserves
    retryable manifest obsolete entries until deletion succeeds.
  - [x] Extend snapshot compaction to the full storage surface with durable
    validation-drift abort diagnostics and expanded compaction diagnostics.
    - [x] Report foreground maintenance compaction aborts through the
      maintenance result without hiding the original error: candidate read,
      snapshot refresh/prepare/write/install, and obsolete cleanup stages now
      set `aborted` plus a stage-specific diagnostic.
    - [x] Add post-snapshot compaction validation drift detection for the
      segmented state surface: compaction captures manifest bytes plus
      candidate segment bytes before building the snapshot, revalidates them
      before manifest install, aborts with `validation-drift-aborted` if a peer
      write or manifest rewrite lands, removes the uninstalled snapshot, and
      keeps valid manifests from adopting stray unmanifested snapshot files.
    - [x] Strengthen compaction drift validation from aggregate candidate byte
      counts to ordered candidate segment content fingerprints, with regression
      coverage for same-length segment metadata rewrites that must abort before
      snapshot manifest install.
- [x] Rebuild the typed metadata index and `liblql`-backed public query/search
  engine against the new storage model, without fallback to deprecated code.
  - [x] Add the first pouch `query_keys` scan path over the
    segmented state projection, with selector parsing/evaluation owned by
    `liblql`, `query_hidden` filtering, staged-key exclusion, pagination, and
    scan metadata.
  - [x] Add public document `query` row streaming over the pouch state
    model in scan mode, with `liblql` selector evaluation, `query_hidden`
    filtering, staged-key exclusion, pagination, and scan metadata.
  - [x] Add the first pouch `flush_index` implementation over the pouch
    state projection, returning a synchronous local high-water index token that
    includes tombstones and peer-writer marker refreshes.
  - [x] Add the first durable per-namespace query-index sidecar metadata file
    under `index/query.index`, with version validation, high-water persistence,
    and repair on missing, stale, or corrupt sidecars during `flush_index`.
  - [x] Extend `index/query.index` with deterministic live-row summary records
    rebuilt from the pouch state projection, including row-count/hash
    validation, deleted-row exclusion, and `query_hidden` flags for future
    indexed candidate generation.
  - [x] Add the first validated `query.index` summary reader and route
    selectorless `query_keys` through it for default/index engines, including
    synchronous refresh, pagination, hidden-row filtering, and a fail-closed
    boundary for selector predicates before postings are available.
  - [x] Add the first durable scalar field postings to `query.index` and route
    explicit indexed `query_keys` equality/`in` selectors through posting
    candidates with final `liblql` acceptance, including `/tags[]` array
    membership, hidden/deleted suppression, pagination, and fail-closed
    unsupported selector shapes.
  - [x] Route explicit indexed document `query` equality/`in` selectors through
    the same scalar posting candidates, preserving streamed document output,
    final `liblql` acceptance, refresh-wait, pagination, hidden/deleted
    suppression, and fail-closed unsupported selector shapes.
  - [x] Add durable field-presence postings to `query.index` and route
    explicit indexed `query_keys` and document `query` `exists` selectors
    through posting candidates with final `liblql` acceptance, container
    presence support, hidden/deleted suppression, and fail-closed unsupported
    selector shapes.
  - [x] Route explicit indexed `query_keys` and document `query` simple
    case-sensitive `prefix` selectors through durable scalar term postings with
    final `liblql` acceptance, array `/field[]` support, hidden/deleted
    suppression, pagination, and fail-closed unsupported selector shapes.
  - [x] Route explicit indexed `query_keys` and document `query` simple
    case-sensitive `contains` selectors through durable scalar term postings
    with final `liblql` acceptance, array `/field[]` support, hidden/deleted
    suppression, pagination, and fail-closed unsupported selector shapes.
  - [x] Route explicit indexed `query_keys` and document `query` simple
    bounded numeric `range` selectors through durable scalar term postings with
    final `liblql` acceptance, hidden/deleted suppression, pagination, and
    fail-closed unsupported selector shapes.
  - [x] Route explicit indexed `query_keys` and document `query` simple
    case-insensitive `iprefix` and `icontains` selectors through same-field
    durable scalar term postings with ASCII-folded candidate matching, final
    `liblql` acceptance, array `/field[]` support, hidden/deleted suppression,
    pagination, and fail-closed unsupported selector shapes.
  - [x] Build the durable typed index/postings path and make indexed mode the
    preferred query engine.
    - [x] Add typed scalar term postings to `query.index` by bumping the
      sidecar format to version 10 and appending a value-type tag to every
      term row (`s`, `n`, `b`, `z`). Indexed `prefix` / `iprefix` and
      `contains` / `icontains` readers now accept only string postings before
      treating their candidates as exact, preserving liblql string semantics
      while avoiding candidate document re-filtering for indexed key queries.
      Non-case-insensitive `contains` over hex sidecar postings now advances on
      byte boundaries only, preventing odd-nibble substring false positives.
    - [x] Add pouch query-engine configuration to `lc_pouch_open` options and
      `pouch://` endpoint query parameters: `query_engine=index` is the
      default, `query_engine=scan` forces implicit scan routing, explicit
      request `engine` overrides configuration, and
      `query_fallback_engine=index` handles implicit `refresh=wait_for`
      requests from scan-preferred endpoints.
    - [x] Avoid rebuilding current query-index generations on ordinary indexed
      query reads: query paths now do a header-only current-generation check,
      selected-section sidecar validation, and sorted exact-term early stop,
      while explicit `flush_index` still performs full sidecar validation and
      repair.
    - [x] Batch indexed candidate state reads through one namespace manifest
      open, marker refresh, and warmed projection-cache lookup per candidate
      set instead of reopening the manifest for every posting hit.
    - [x] Add a typed temporal candidate path for indexed `DateAfter` over
      durable scalar term postings: pouch now parses LQL date-only,
      naive-UTC, fractional, and offset timestamp term values plus selector
      bounds into UTC instants, excludes invalid/out-of-range terms before
      state reads, and still preserves final `liblql` selector acceptance.
- [x] Rebuild the Go lockd disk vs pouch benchmark/stress harness against the
  new pouch API and restore the comparison scenarios only when they measure the
  pouch implementation.
  - [x] Restore the live `benchmark/` Go/cgo module expected by the
    `benchmark-pouch-go*` Makefile targets, with C-side pouch timing metrics
    and fast/medium benchmark names that compile against the pouch
    `lc_pouch` API.

## Foundation

- [x] Map the registered server routes from the lockd HTTP handler.
- [x] Confirm the client bundle format used by lockd-generated `client.pem` files.
- [x] Establish an initial CMake build with both static and shared library targets.
- [x] Add CMake-managed third-party dependency builds for OpenSSL, nghttp2, and libcurl.
- [x] Add lonejson as the mapped and streaming JSON generator/parser dependency.
- [x] Define the initial public C API around config/request/response structs.
- [x] Add client-level allocator hooks for new stream-first APIs.
- [x] Implement the first transport slice with mTLS bundle loading, HTTP/1.1 + HTTP/2 preference, raw state bodies, multipart queue payloads, and endpoint failover for `node_passive`.
- [x] Implement `POST /v1/acquire`.
- [x] Implement `GET /v1/get` with public-read support.
- [x] Implement `POST /v1/update`.
- [x] Implement `POST /v1/mutate`.
- [x] Implement `POST /v1/metadata`.
- [x] Implement `POST /v1/remove`.
- [x] Implement `GET /v1/describe`.
- [x] Implement `POST /v1/keepalive`.
- [x] Implement `POST /v1/release`.
- [x] Implement `POST /v1/query` with raw selector JSON input.
- [x] Add stream-first `query` API that writes response bytes incrementally.
- [x] Implement `POST /v1/queue/enqueue`.
- [x] Implement `POST /v1/queue/dequeue`.
- [x] Implement `POST /v1/queue/dequeueWithState`.
- [x] Implement `POST /v1/queue/stats`.
- [x] Implement `POST /v1/queue/ack`.
- [x] Implement `POST /v1/queue/nack`.
- [x] Implement `POST /v1/queue/extend`.
- [x] Add `examples/` programs to exercise the public API shape.
- [x] Add a root-level docker-compose e2e environment with disk, S3/MinIO, and mem-backed lockd instances.
- [x] Add initial unit-test targets for stream helpers and public handle-contract wrappers.
- [x] Add standard unit-test targets for JSON helpers, bundle parsing, and response decoding.
- [x] Finish migrating the remaining convenience-only buffered paths onto the allocator-aware/stream-first plumbing.
- [x] Add an integration test harness against a containerized lockd instance.
- [x] Add install/export/package rules for downstream consumers.

## API coverage

- [x] `POST /v1/acquire`
- [x] `POST /v1/keepalive`
- [x] `POST /v1/release`
- [x] `POST /v1/get`
- [x] `POST /v1/attachments`
- [x] `POST /v1/attachment`
- [x] `POST /v1/query`
- [x] `POST /v1/mutate`
- [x] `POST /v1/update`
- [x] `POST /v1/metadata`
- [x] `POST /v1/remove`
- [x] `GET /v1/describe`
- [x] `POST /v1/index/flush`
- [x] `GET /v1/namespace`
- [x] `PUT /v1/namespace`
- [x] `POST /v1/queue/enqueue`
- [x] `POST /v1/queue/stats`
- [x] `POST /v1/queue/dequeue`
- [x] `POST /v1/queue/dequeueWithState`
- [x] `POST /v1/queue/watch`
- [x] `POST /v1/queue/subscribe`
- [x] `POST /v1/queue/subscribeWithState`
- [x] `POST /v1/queue/ack`
- [x] `POST /v1/queue/nack`
- [x] `POST /v1/queue/extend`
- [x] `POST /v1/txn/replay`
- [x] `POST /v1/txn/decide`
- [x] `POST /v1/txn/commit`
- [x] `POST /v1/txn/rollback`
- [x] `POST /v1/tc/lease/acquire`
- [x] `POST /v1/tc/lease/renew`
- [x] `POST /v1/tc/lease/release`
- [x] `GET /v1/tc/leader`
- [x] `POST /v1/tc/cluster/announce`
- [x] `POST /v1/tc/cluster/leave`
- [x] `GET /v1/tc/cluster/list`
- [x] `POST /v1/tc/rm/register`
- [x] `POST /v1/tc/rm/unregister`
- [x] `GET /v1/tc/rm/list`

## Next pass recommendation

- [x] Polish attachment DX around the new `lc_lease` object model.
- [x] Add query key helpers and cover document-return metadata paths explicitly.
- [x] Add stream-first dequeue payload APIs.
- [x] Replace ad hoc JSON handling in client paths with lonejson-backed helpers.
- [x] Add client-driven e2e tests against the root docker-compose lockd environment, including the UDS-backed mem instance.

## Lifecycle and dependency alignment

Latest release targets confirmed on 2026-07-23:

- `lonejson v0.42.0`
- `libpslog v0.9.0`
- `c.pkt.systems v0.9.0`
- `liblql v0.2.0`

### Dependency upgrade and provenance

- [x] Upgrade the native `lonejson` SDK dependency from `0.41.0` to
  `0.42.0`, including ABI/SOVERSION, release asset URLs, SHA-256 pins, Lua
  rock dependency/source-rock pins, dependency interface tests, README, Lua
  docs, examples, package metadata, and release manifest expectations.
- [x] Upgrade `libpslog` from `0.8.0` to `0.9.0`, including all target-specific
  SDK asset hashes, public logging dependency tests, package metadata,
  license/provenance entries, and any generated single-header references.
- [x] Upgrade `c.pkt.systems` dependency bundles from `0.7.0` to `0.9.0`,
  including every supported target asset hash, dependency root identity, package
  verification expectations, and downstream SDK metadata.
- [x] Add `liblql v0.2.0` as a first-class lifecycle dependency from
  `https://github.com/sa6mwa/liblql/releases`, pinned by target ID, exact
  release asset URL, SHA-256, ABI/SOVERSION, license, CMake metadata, and
  pkg-config metadata.
- [x] Add `liblql` dependency interface tests that verify headers, static and
  shared libraries, CMake package config, pkg-config metadata, exported symbols,
  forbidden private artifacts, and license/provenance metadata.
- [x] Add stale-cache failure behavior coverage for `liblql` once the lifecycle
  shared archive cache helper lands.
- [x] Update binary SDK package manifests so `liblql`, `lonejson`, `libpslog`,
  and `c.pkt.systems` record logical dependency identity, exact upstream release
  asset URL, SHA-256, target ID, license, bundled/external role, and no local
  paths.

### Pouch `liblql` integration

- [x] Route public LQL operator selectors through `liblql` parsing and
  spooled document evaluation, while preserving the typed pouch metadata
  selector surface for storage-owned key/owner filtering.
- [x] Replace the current pouch indexed-query placeholder/parser boundary with
  `liblql` for LQL parsing/evaluation; do not add project-local query parser or
  expression evaluator code.
  - [x] Resolve the `liblql v0.1.0` evaluator gap by upgrading to
    `liblql v0.2.0` and enabling general pouch document predicates through
    `stream_apply_spooled` with strict JSON Pointer fields such as
    `{"eq":{"field":"/value","value":"alpha"}}`.
  - [x] Add a native-only `liblql_dependency_interface` probe that exercises
    the `v0.2.0` spooled evaluator directly with full-form strict JSON Pointer
    selectors, decision callbacks, and matched/seen record counts.
  - [x] Reconfirm on 2026-07-23 that `liblql v0.2.0` is the latest upstream
    release used by this repo for LQL document predicate evaluation.
- [x] Keep pouch storage-owned indexes behind the pouch store boundary; use
  `liblql` only for query language semantics and predicate/evaluator behavior.
- [x] Add observable pouch tests for typed key/owner metadata selectors,
  full-form `liblql` document selectors, pagination, hidden metadata filtering,
  removed-candidate skipping, malformed LQL diagnostics, oversized query
  limits, and scan/index fallback parity.
  - [x] Cover typed compound key+owner selectors for scan and index document
    queries plus `query_keys`.
  - [x] Cover malformed strict-field LQL selector diagnostics for scan and
    index document queries plus `query_keys`.
  - [x] Cover full-form strict JSON Pointer LQL document selectors for scan and
    index document queries plus `query_keys`.
  - [x] Cover removed-candidate skipping for index document queries plus
    `query_keys`.
  - [x] Cover hidden metadata filtering for scan and index document queries
    plus `query_keys`.
  - [x] Cover signed oversized query limit rejection for scan and index
    document queries plus `query_keys`.
  - [x] Cover scan/index parity for compound key+owner selectors across
    document queries and `query_keys`.
- [x] Add package and install-tree smoke consumers proving downstream CMake and
  pkg-config users can link `liblockdc` with the transitive `liblql` contract.

### Updated lifecycle alignment

- [x] Align dependency acquisition with the updated lifecycle shared archive
  cache:
  `${CPKT_DEPENDENCY_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/c.pkt.systems/deps}`.
  Cache verified immutable archives by SHA-256, publish through atomic rename,
  keep extracted/build/install state under repo-local `.cache/`, and ensure
  `make clean` never removes the shared archive cache.
- [x] Add a project-owned verified archive acquisition helper and tests for
  initial download, offline cache hit after deleting local dependency roots,
  corrupt cached archive rejection, and package privacy rejection of global-cache
  paths.
- [x] Add concurrent acquisition stress coverage for the shared dependency
  archive cache lock path.
- [x] Align Linux compiler resolution with the updated Bootlin toolchain policy:
  no host compiler/binutils fallback for Linux builds, complete pinned Bootlin
  collections per target, toolchain identity in dependency stamps, and target
  tool discovery from configured build state.
  - [x] Route the native `x86_64-linux-gnu` toolchain file through the vendored
    lifecycle Bootlin resolver instead of `/usr/bin` compiler/binutils paths,
    with a fake-cache contract test and full native release CTest coverage.
  - [x] Route all remaining Linux release toolchain files through the vendored
    lifecycle Bootlin resolver, require explicit Linux `toolchainFile` presets,
    and verify fresh configure plus build across the six-target Linux matrix.
  - [x] Record the resolved Bootlin target ID, source, archive, root, sysroot,
    and target triple in dependency manifests so stale dependency roots are keyed
    by lifecycle toolchain identity instead of host compiler identity.
- [x] Add preset-contract tests for lifecycle-required preset presence, build
  and test preset mirrors, required cache variables, release `LOCKDC_DIST_DIR`
  defaults, `debug-lua`, `valgrind`, and fuzz compatibility visibility.
- [x] Update CMake presets and toolchain files for the lifecycle-required
  Bootlin-backed `valgrind`, pinned AFL++ `fuzz`, release target matrix, and
  optional Darwin/osxcross behavior.
  - [x] Vendor the lifecycle `scripts/cpkt-toolchains.sh` resolver and add the
    shared CMake Bootlin import helper used by the native GNU preset.
  - [x] Apply the shared CMake Bootlin import helper to every Linux release
    toolchain file.
  - [x] Make the `valgrind` preset use the Bootlin x86_64 GNU lifecycle
    toolchain and force a fresh configure so stale host-compiler caches cannot
    satisfy the gate.
- [x] Replace sanitizer-as-primary hardening assumptions with the updated native
  Valgrind gate while preserving any existing useful ASan/UBSan coverage as
  compatibility or optional hardening.
- [x] Align fuzzing with the updated pinned AFL++ GCC-plugin lifecycle for
  native x86_64 Linux only; ensure fuzz targets never rely on host Clang/GCC as
  the project compiler.
  - [x] Remove the explicit host `clang` compiler override from the
    compatibility `fuzz` preset and assert native `x86_64-linux-gnu` target
    metadata in the preset contract.
  - [x] Vendor the pinned `scripts/cpkt-aflpp.sh` resolver, add the
    `fuzz-aflpp` toolchain file, and make the `fuzz` preset compile through
    AFL++ wrappers backed by the Bootlin x86_64 GNU collection.
  - [x] Replace libFuzzer-only smoke execution with an AFL-compatible harness
    and bounded `make fuzz-smoke` AFL++ runs across committed corpora.
- [x] Update Make command surfaces so `make help` is authoritative and includes
  the updated lifecycle targets: `finalize-slice`, `prerelease`,
  `prerelease-live`, `prerelease-hardening`, `release-matrix`, `valgrind`, and
  any compatibility aliases retained for existing documented commands.
- [x] Add or update lifecycle migration documentation while the repo is in
  transition, recording old command behavior, new lifecycle command, preserved
  behavior, verification added, removed/deprecated behavior, and decisions still
  needed.

### Verification gates

- [x] Run narrow dependency gates after each pin change: shell syntax, dry-run
  target mapping, upstream checksum manifest comparison, dependency interface
  CTest, and install-tree consumer checks.
- [x] Run lifecycle-alignment gates after command/toolchain/cache changes:
  preset contract tests, target-tool discovery tests, cache contract tests,
  `make build`, `make test`, and `make valgrind`.
- [x] Run broader gates before declaring the dependency/lifecycle migration
  complete: `make test-all`, `make package-verify`, `make lua-test`, fuzz smoke
  when AFL++ is available, deterministic e2e when relevant, and release artifact
  privacy/relocatability scans.
  - [x] `make test-all`
  - [x] `make package-verify`
  - [x] `make lua-test`
  - [x] `make fuzz-smoke`
  - [x] `make test-e2e`
- [x] Inspect and classify generated state after verification; commit only the
  coherent dependency/lifecycle changes and leave generated caches untracked.
  - [x] Tracked dirty files are limited to the lifecycle/pouch/test/TODO
    change set; generated state remains ignored under `.cache/`, `build/`,
    `dist/`, and `devenv/volumes/`.

## Current release-readiness focus

- [x] Keep API examples aligned with the receiver-function public surface.
- [x] Keep the Lua rock dependency boundary aligned with the pinned
  `lonejson` release.
- [x] Audit `docs/pouch-storage.md` against the current pouch code/tests after
  the `liblql v0.2.0` upgrade.
  - [x] Fix LQL document predicate pagination so scan and indexed `query` /
    `query_keys` apply `limit` to matched rows, not pre-filter storage
    candidates, and return cursors only when another matching row exists.
  - [x] Fully build out the pouch search/index engine toward Go lockd disk
    search parity, while keeping `liblql` as the final public predicate
    authority.
    - [x] Add storage-owned field/range posting indexes for low-match public
      LQL document predicates; indexed string/bool/null/numeric equality and
      top-level conjunctions over those scalar equality terms use durable
      candidate postings, and range-only selector shapes now collect
      storage-owned numeric candidates from ordered field postings instead of
      broad summary candidates.
    - [x] Add the first durable candidate-posting slice for full-form LQL
      exact equality selectors with strict JSON Pointer fields and
      string/bool/null values; final predicate acceptance remains owned by
      `liblql`.
    - [x] Extend document postings to numeric equality predicates with
      canonical numeric equality keys instead of string-token comparison.
    - [x] Extend document postings to numeric range predicates with canonical
      numeric comparison semantics for key-ordered candidate narrowing.
    - [x] Add mixed LQL hint intersection for selectors that combine equality
      and numeric range children in the same `and` expression; equality
      posting candidates are now intersected with numeric range postings while
      final predicate acceptance remains owned by `liblql`.
    - [x] Add storage-owned `exists` candidates for full-form LQL path-string
      selectors, including object/array container presence postings, with
      scan-mode and final predicate acceptance still owned by `liblql`.
    - [x] Add storage-owned `in` candidates for full-form LQL string `any`
      terms; values inside each `in` term are unioned, and separate supported
      `and` children are intersected before final `liblql` acceptance.
    - [x] Add storage-owned `prefix` and `iprefix` candidates for full-form
      LQL string predicate terms over strict JSON Pointer fields, backed by
      text-predicate postings for JSON strings, booleans, and number source
      text.
    - [x] Add storage-owned `contains` and `icontains` candidates for
      full-form LQL string predicate terms over strict JSON Pointer fields,
      backed by text-predicate postings and final `liblql` acceptance.
    - [x] Add true ordered range traversal over numeric field postings for
      range-primary document and key-only scans, with stable key/cursor
      ordering after candidate collection.
    - [x] Extend candidate extraction beyond single `eq` selectors to safe
      top-level `and` conjunction/intersection forms over supported scalar
      equality postings; final predicate acceptance remains owned by `liblql`.
    - [x] Extend candidate extraction to nested full-form `and` trees for
      supported typed equality, range, `in`, prefix/iprefix,
      contains/icontains, and `exists` terms while preserving equality scalar
      type encoding before final `liblql` acceptance.
    - [x] Add a first OR composition slice for full-form `or` branches whose
      children are exact equality selectors over the same strict JSON Pointer
      field; the planner maps those to the existing storage-owned `in`
      candidate path and still lets `liblql` perform final acceptance.
    - [x] Add storage-owned same-field `in` candidate lowering for full-form
      `or` branches whose children are string `in` selectors over the same
      strict JSON Pointer field; branch `any` values are unioned through the
      existing `in` candidate path before final `liblql` acceptance.
    - [x] Add a mixed membership/presence OR composition slice for full-form
      `or` branches that combine strict JSON Pointer string `in` selectors
      with path-string `exists` selectors; indexed scans union membership and
      presence postings, de-duplicate keys, and keep final predicate
      acceptance owned by `liblql`.
    - [x] Add a mixed membership/text OR composition slice for full-form `or`
      branches that combine strict JSON Pointer string `in` selectors with
      strict JSON Pointer `prefix` / `iprefix` selectors; indexed scans union
      membership and text postings, de-duplicate keys, and keep final
      predicate acceptance owned by `liblql`.
    - [x] Add a mixed membership/text OR composition slice for full-form `or`
      branches that combine strict JSON Pointer string `in` selectors with
      strict JSON Pointer `contains` / `icontains` selectors; indexed scans
      union membership and text postings, de-duplicate keys, and keep final
      predicate acceptance owned by `liblql`.
    - [x] Add storage-owned branch-union candidates for mixed-field full-form
      `or` branches whose children are exact equality selectors; pouch
      de-duplicates unioned posting keys, restores stable cursor ordering, and
      final acceptance remains owned by `liblql`.
    - [x] Add storage-owned branch-union candidates for full-form `or`
      branches whose children are path-string `exists` selectors; indexed
      scans union presence postings, de-duplicate keys, preserve cursor order,
      and keep final predicate acceptance owned by `liblql`.
    - [x] Add storage-owned branch-union candidates for full-form `or`
      branches whose children are `prefix` / `iprefix` selectors over strict
      JSON Pointer fields; indexed scans union text postings, de-duplicate
      keys, preserve cursor order, and keep final predicate acceptance owned
      by `liblql`.
    - [x] Add storage-owned branch-union candidates for full-form `or`
      branches whose children are `contains` / `icontains` selectors over
      strict JSON Pointer fields; indexed scans union text postings,
      de-duplicate keys, preserve cursor order, and keep final predicate
      acceptance owned by `liblql`.
    - [x] Add storage-owned branch-union candidates for full-form `or`
      branches whose children are numeric `range` selectors over strict JSON
      Pointer fields; indexed scans union ordered numeric postings,
      de-duplicate keys, preserve cursor order, and keep final predicate
      acceptance owned by `liblql`.
    - [x] Cover safe `not` exclusion planning for full-form `and` selectors
      that contain at least one positive supported candidate predicate; pouch
      uses the positive predicate as the storage-owned candidate superset and
      leaves the exclusion decision to final `liblql` acceptance.
    - [x] Add a mixed non-equality OR composition slice for full-form `or`
      branches that combine path-string `exists` selectors and numeric
      `range` selectors; indexed scans union presence and ordered numeric
      postings, de-duplicate keys, and keep final predicate acceptance owned
      by `liblql`.
    - [x] Add a mixed non-equality OR composition slice for full-form `or`
      branches that combine path-string `exists` selectors and strict JSON
      Pointer `prefix` / `iprefix` selectors; indexed scans union presence and
      text postings, de-duplicate keys, and keep final predicate acceptance
      owned by `liblql`.
    - [x] Add a mixed non-equality OR composition slice for full-form `or`
      branches that combine path-string `exists` selectors and strict JSON
      Pointer `contains` / `icontains` selectors; indexed scans union presence
      and text postings, de-duplicate keys, and keep final predicate
      acceptance owned by `liblql`.
    - [x] Add a mixed text OR composition slice for full-form `or` branches
      that combine strict JSON Pointer `prefix` / `iprefix` selectors with
      strict JSON Pointer `contains` / `icontains` selectors; indexed scans
      union text postings, de-duplicate keys, and keep final predicate
      acceptance owned by `liblql`.
    - [x] Add a mixed range/text OR composition slice for full-form `or`
      branches that combine numeric `range` selectors with strict JSON
      Pointer `prefix` / `iprefix` selectors; indexed scans union ordered
      numeric and text postings, de-duplicate keys, and keep final predicate
      acceptance owned by `liblql`.
    - [x] Add a mixed range/text OR composition slice for full-form `or`
      branches that combine numeric `range` selectors with strict JSON
      Pointer `contains` / `icontains` selectors; indexed scans union ordered
      numeric and text postings, de-duplicate keys, and keep final predicate
      acceptance owned by `liblql`.
    - [x] Add a mixed membership/range OR composition slice for full-form `or`
      branches that combine strict JSON Pointer string `in` selectors with
      numeric `range` selectors; indexed scans union membership and ordered
      numeric postings, de-duplicate keys, and keep final predicate acceptance
      owned by `liblql`.
    - [x] Add a liblql-AST-backed OR candidate extraction slice for selectors
      with one supported OR group inside recursive `and` composition,
      including safe positive-OR plus `not` exclusion forms and root OR groups
      that mix more than two non-equality families; final predicate acceptance
      remains owned by `liblql`.
    - [x] Broaden liblql-AST recursive OR candidate extraction for `and`
      selectors with multiple supported OR groups by using one OR group as a
      storage-owned positive candidate superset and leaving remaining OR groups
      to final `liblql` acceptance.
    - [x] Add liblql-AST equality-only recursive OR extraction using typed
      equality candidate supersets generated from liblql textual equality
      values; final typed exactness remains owned by `liblql`.
    - [x] Add mixed equality/non-equality liblql-AST recursive OR extraction
      by unioning equality, membership, ordered numeric, text, and presence
      postings in storage-owned candidate scans while keeping final predicate
      acceptance in `liblql`.
    - [x] Add recursive boolean composition over supported selector families
      without devolving to full namespace scans, including remaining mixed
      nested OR branch planning and broader exclusion planning for safe `not`
      forms.
      - [x] Add storage-owned exclusion for safe exact `not exists` leaves
        under recursive `and` composition; candidate scans reject keys with live
        postings for the excluded JSON Pointer field while final predicate
        acceptance remains owned by `liblql`.
      - [x] Add storage-owned exclusion for safe scalar `not eq` leaves under
        recursive `and` composition; candidate scans reject keys with live
        exact string, boolean, or null equality postings for the excluded
        field/value while final predicate acceptance remains owned by `liblql`.
      - [x] Add storage-owned field-presence candidate planning for `date`
        selectors and date-containing OR groups; final datetime, relative-date,
        and boundary semantics remain owned by `liblql`.
      - [x] Add storage-owned exclusion for safe `prefix` / `iprefix` and
        `contains` / `icontains` leaves under recursive `and` composition;
        candidate scans reject keys with live matching text postings while
        final predicate acceptance remains owned by `liblql`.
      - [x] Add storage-owned exclusion for safe string `in` and numeric
        `range` leaves under recursive `and` composition; candidate scans
        reject keys with live membership or numeric range postings while final
        predicate acceptance remains owned by `liblql`.
      - [x] Flatten nested `or` nodes whose leaves are already supported
        selector families into the storage-owned OR candidate union; final
        predicate acceptance remains owned by `liblql`.
    - [x] Add wildcard/recursive path expansion support for indexed field
      dictionaries where `liblql` exposes safe planner hints, with bounded
      expansion and deterministic fallback semantics.
      - [x] Detect `liblql` wildcard/recursive path capabilities during pouch
        LQL filter initialization and use deterministic full-candidate indexed
        fallback only for path forms that cannot be planned safely.
      - [x] Add bounded storage-owned expansion for positive single-segment
        wildcard `exists` path selectors by matching candidate JSON Pointer
        fields from the indexed posting dictionary and leaving final acceptance
        to `liblql`.
      - [x] Add storage-owned union expansion for exists-only root `or` groups
        that combine single-segment wildcard path selectors with exact presence
        selectors, while keeping final acceptance owned by `liblql`.
      - [x] Add bounded storage-owned expansion for recursive `**` path
        segments in positive `exists` selectors by matching indexed JSON Pointer
        field dictionaries as a candidate superset and leaving final acceptance
        to `liblql`.
    - [x] Add token/trigram-style candidate structures if pouch needs
      Go-style text filtering performance; keep final contains/full-text
      semantics validated by `liblql`.
    - [x] Persist an index-format/version contract for pouch postings so
      incompatible sidecars rebuild from namespace segments/snapshots instead
      of being trusted.
  - [x] Cut pouch indexed query execution over to Go lockd disk performance
    parity instead of continuing monolithic storage posting
    tweaks.
    - [x] Cut over unreleased pouch API, implementation, tests, docs, and
      benchmark callers from the former disk-prefixed C naming to pouch-native
      `lc_pouch_*`/`src/lc_pouch.c`; Go lockd disk remains the reference
      implementation for storage/index ideas, not the identity of the embedded
      C backend.
    - [x] Keep the active storage subsystem pouch-native after the backend
      skeleton cutover: namespace path escaping, namespace layout creation,
      manifest open/repair/write, active segment selection, segment rollover,
      segment/snapshot naming, marker refresh, and obsolete cleanup now live in
      active `lc_pouch_namespace` / `lc_pouch_state` modules instead of a
      disk-prefixed or deprecated backend object.
    - [x] Keep active segmented replay input discovery in the pouch namespace
      and state modules: manifest replay selects authoritative segment/snapshot
      paths, repairs manifest state from directory scans, cleans obsolete
      manifest records under the configured policy, and counts compaction
      candidates without reviving deprecated history modules.
    - [x] Keep compaction snapshot path mechanics in the active pouch namespace
      and state modules, with deprecated history helpers retained only under
      `deprecated/pouch-legacy/` as reference material.
    - [x] Split the pouch search/index subsystem out of `lc_pouch.c` into
      an internal C index layer with explicit reader, writer, planner, posting,
      visibility, and result-cache boundaries.
      - [x] Introduce the first real private index module boundary in
        `lc_pouch_index`: indexed `DateAfter` temporal parsing and bound
        evaluation now live outside the sidecar reader, so the pouch query path
        starts moving toward an index-owned planner/reader layer without
        deprecated pouch paths.
      - [x] Move result row/docID list ownership into
        `lc_pouch_index_result`: active exact-generation readers use the
        private result-list helpers for sorted key/docID vectors and duplicate
        collapse before page emission.
      - [x] Add result-cache/page orchestration boundaries. Active source now
        has private index-layer page selection for candidate offset, match,
        limit, and next-cursor accounting. It also has bounded one-entry docID
        result cache miss/insert logic in prepared generation readers, plus an
        index-owned sorted result-page cache slot per prepared exact,
        presence, range, text/trigram, and temporal reader. The cache is keyed
        by the immutable reader identity plus the normalized predicate key and
        reuses the sorted key/docID rows before client-visible cursor-page
        emission.
      - [x] Physically split result-cache/page planning into
        `src/lc_pouch_index_result.c`, leaving `lc_pouch_index.c` focused on
        planner/collector orchestration while preserving the same private
        `lc_pouch_index.h` boundary.
        - [x] Add the first physical `lc_pouch_index_result.c` boundary:
          sorted result-key vectors now own key allocation, docID/key/value-slot
          ordering, and adjacent docID compaction for query-index candidate
          emission, leaving sidecar parsing in the pouch query bridge.
        - [x] Move decoded result-row bucket ownership for merged `any`/term
          queries into `lc_pouch_index_result.c`; the query bridge now adapts
          row views for visitor callbacks instead of owning scratch row lists.
        - [x] Move cursor-page accounting into `lc_pouch_index_result.c`: scan,
          summary, exact, and residual indexed query paths now use a private
          result-page primitive for offset, match, limit, emitted, and
          next-cursor decisions.
      - [x] Physically split sparse posting encoding into
        `src/lc_pouch_index_posting.c`, keeping the private posting boundary
        separate from document-table, term-dictionary, and result-cache code.
        Dense bitset primitives and adaptive sparse/dense selection now live in
        the same module.
      - [x] Physically split term dictionaries and term-ID posting tables into
        `src/lc_pouch_index_terms.c`, while keeping prepared reader lifecycle
        handle-owned in the query-index bridge because it owns namespace path
        repair, generation loading, and document-table pairing.
        - [x] Add the first physical `lc_pouch_index_terms.c` boundary:
          sidecar term field/value table records, cleanup, and sorted binary
          lookup now live in the private index term module while the query
          bridge still owns sidecar parsing and reader orchestration.
        - [x] Move sidecar term range selection into
          `lc_pouch_index_terms.c`: merged field span selection now operates
          over private term-key/range records, leaving the query bridge to
          adapt non-exact reader terms and consume slices.
        - [x] Move sidecar `term_field`/`term_value` record parsing into
          `lc_pouch_index_terms.c`, with focused malformed-record coverage so
          the query bridge no longer owns term-table line format validation.
        - [x] Replace the query-local exact-term type with
          `lc_pouch_index_term_key`: sorted term-key compare/find/cleanup now
          live in `lc_pouch_index_terms.c`, and exact-generation readers
          consume the index-owned key vector directly without adapter
          allocations.
        - [x] Move exact scalar term-key construction into
          `lc_pouch_index_terms.c`: raw field/value/type term arrays are
          validated, hex-encoded, sorted, and deduplicated by the index term
          module, leaving query callers to pass the built key vector to sidecar
          readers. Simple equality and single-field value arrays for
          non-wildcard `in` now use the same term-owned construction path
          instead of query-local value-hex setup. Exact matching preserves
          liblql JSON scalar typing, including string/number/boolean/null
          separation and numeric equality across equivalent number spellings
          such as `1` and `1.0`.
      - [x] Physically split private docID set algebra into
        `src/lc_pouch_index_doc.c`, including sorted unique append and
        merge-based union/intersection/subtraction helpers plus the first
        borrowed-key dense document-table primitive. `lc_pouch_index.c`
        remains focused on temporal parsing while the persistent per-generation
        table cutover remains pending.
      - [x] Document the current private index module map in
        `docs/pouch-storage.md`, including the pouch storage bridge responsibility that
        remains in `src/lc_pouch.c`.
    - [x] Replace repeated key-string posting algebra with stable per-index
      integer document IDs, sorted docID sets, pooled scratch buffers, and
      merge-based union/intersection/subtraction.
      - [x] Add the first private document table primitive in
        `lc_pouch_index`: namespace/key pairs are sorted into dense docIDs with
        forward and reverse lookup coverage.
      - [x] Add the first private immutable document-table generation codec in
        `lc_pouch_index`: per-namespace document tables round-trip under index
        sequence plus segmented manifest identity with sorted key order,
        implicit dense docIDs, duplicate rejection, and corruption/truncation
        rejection coverage.
      - [x] Publish immutable per-namespace document-table generation files
        from the pouch storage bridge: query-index flush/rebuild now publishes
        `index/query.index.lcpdtg` inside the pouch namespace index directory,
        validates format/version/index sequence/row count/row hash/sorted
        document rows, and repairs missing or corrupt generation files even
        when the main `index/query.index` sidecar is current.
      - [x] Load identity-matched document-table generation files for exact
        scalar docID emission: equality, non-wildcard `in`, and root equality
        `or` docID visitors now collect docIDs, load/repair the matching
        `index/query.index.lcpdtg`, and resolve page keys/metadata through the
        persisted namespace document table before emitting keys or opening
        document bodies. The exact docID collector keeps only docIDs and
        matched value slots, not term-row key strings, and preserves `liblql`
        JSON scalar equality as the correctness target rather than Go lockd's
        looser scalar coercion.
      - [x] Cut exact-term generation files over to namespace-local docIDs:
        exact generation build stores postings by the matching per-namespace
        document-table generation, exact readers require an
        identity-matched `.lcpdtg`, remap local docIDs back into the current
        global in-memory doc table, and repair corrupt/missing doc generation
        files through the exact query path.
      - [x] Add field-presence generation files over namespace-local docIDs.
        Active `exists` readers now use persisted
        `index/query.index.lcppg` presence-term generations keyed by the same
        immutable sidecar/document-table identity, with a prepared presence
        reader owned by the pouch handle.
      - [x] Add numeric-range generation files over namespace-local docIDs.
        Active range readers now use persisted `index/query.index.lcprg`
        numeric-term generations keyed by the same immutable
        sidecar/document-table identity, with a prepared range reader owned by
        the pouch handle.
      - [x] Add text/trigram generation files over namespace-local docIDs.
        - [x] Add immutable string text generation files over namespace-local
          docIDs. Active prefix/contains readers now use persisted
          `index/query.index.lcptxg` text-term generations keyed by the same
          immutable sidecar/document-table identity, with a prepared text
          reader owned by the pouch handle.
        - [x] Add trigram generations for contains/icontains selectivity.
          Active contains/icontains readers now use persisted
          `index/query.index.lcpt3g` raw/folded trigram generations for needles
          of at least three bytes before final text-term validation.
      - [x] Add temporal generation files over namespace-local docIDs.
        Active date readers now use persisted
        `index/query.index.lcptdg` temporal-term generations keyed by the same
        immutable sidecar/document-table identity, with a prepared temporal
        reader owned by the pouch handle. Temporal values are stored as
        normalized instant terms so readers do not reparse source date strings.
      - [x] Wire the pouch query bridge through the index document table for
        field-predicate candidate docIDs: summary refresh populates
        namespace/key docIDs, candidate readers append table docIDs, and result
        conversion resolves docIDs back through table lookup before summary
        access.
      - [x] Add private C docID set primitives and wire exact primary `in`
        candidate collection through docID accumulation while preserving
        secondary-filter and `liblql` predicate authority.
        Current active source now exposes `lc_pouch_index_docid_set` from the
        private index module and routes query-index docID key emission through
        its sorted unique append contract, with unit coverage for duplicate
        collapse and out-of-order rejection.
      - [x] Add private merge-based docID algebra for sorted unique sets:
        union, intersection, and subtraction now run with linear merge scans,
        reject unsorted or duplicate inputs, and require non-aliased empty
        output sets so future planner scratch buffers have a strict contract.
      - [x] Wire query-index exact generation through the private document
        table for term docID assignment: sorted rows populate borrowed-key doc
        table entries once, terms resolve docIDs through table lookup, and unit
        coverage fixes the sorted unique/lookup/out-of-range contract.
      - [x] Cut multi-term query-index docID emission over to direct sorted
        compaction: collectors append candidate rows once, the emitter sorts by
        docID/key/value slot, drops adjacent duplicate docIDs without a local
        posting round-trip, and preserves `value_index` on the docID reader
        branch for value-specific merge callers.
      - [x] Add an internal docID scratch-buffer primitive and wire equality
        intersection/subtraction collectors through it, so multi-term query
        algebra reuses allocator-owned temporary buffers instead of allocating
        fresh merge sets for every positive or negative equality term.
      - [x] Route primary equality candidate collection through the same
        docID reader/planner boundary and remove the obsolete pouch-local
        equality candidate helper.
      - [x] Route exact positive `exists` candidate collection through a
        field-presence docID reader callback while preserving the existing
        posting-summary freshness check in the pouch query adapter.
      - [x] Route primary numeric range candidate collection through a range
        docID reader callback and remove the obsolete pouch-local equality-span
        range optimization helpers.
      - [x] Move compound positive/negative equality filtering into private
        index collectors, or retire the stale historical blocker if no active
        compound visitor remains. Active source no longer has request-local
        compound range/prefix/contains/exists/`in` sidecar visitors in the
        client query runner: the current planner accepts simple indexed roots
        and exact root `or`, routes generation-backed predicates through
        private docID readers/result caches, and leaves unsupported recursive
        `and`/`not` selector expansion as an explicit future planner feature
        rather than a half-migrated cutover path.
    - [x] Add adaptive posting encodings for dense and sparse terms: sparse
      delta-varint docID streams and dense bitsets selected by posting
      density/encoded size.
      - [x] Add the first private sparse delta-varint posting primitive in
        `lc_pouch_index`: sorted docIDs encode into compact delta-varint bytes,
        decode back into caller-owned docID sets, reject out-of-order appends,
        and detect truncated postings. The active multi-term exact/`in`
        query-index docID bridge now exercises this posting append path while
        collapsing sorted candidate docIDs before key emission.
      - [x] Add the first private dense bitset posting primitive in
        `lc_pouch_index_posting`: sorted docIDs set compact bit positions,
        decode back into caller-owned docID sets, reject out-of-order appends,
        and detect bit/count corruption.
      - [x] Add the first adaptive sparse/dense posting wrapper in
        `lc_pouch_index_posting`: callers append sorted docIDs once, dense
        tracking is disabled when the bitset shape becomes implausibly wide,
        selection prefers dense only when density and encoded size justify it,
        and persisted term-generation readers consume this adaptive boundary.
    - [x] Add compiled field dictionaries with term IDs, doc tables, numeric
      range term tables, and text/trigram term tables so equality, range,
      `in`, prefix, contains, and exists can evaluate without repeated string
      scans.
      - [x] Add the first private term dictionary primitive in
        `lc_pouch_index`: `(field,value,type)` terms are interned into stable
        term IDs with sorted lookup and duplicate preservation coverage. Pouch
        readers now build and consume compiled dictionaries for exact,
        presence, range, text, trigram, and temporal generations.
      - [x] Add the first term-ID posting table primitive in `lc_pouch_index`:
        exact terms now map to adaptive sparse/dense docID postings with
        binary lookup, missing-term, and replacement coverage. Pouch readers
        compile exact, presence, range, text, trigram, and temporal postings
        into this table.
      - [x] Add the first immutable exact-term generation codec in
        `lc_pouch_index`: namespace-scoped term dictionaries and adaptive
        term-ID postings now round-trip under index sequence plus segmented
        manifest identity, preserve `liblql` JSON scalar classes, and reject
        corrupt identity, truncation, and invalid posting payloads.
      - [x] Publish and consume immutable exact-term generation files from the
        pouch storage bridge for exact scalar document and key paths: full
        query-index rebuilds now publish `index/query.index.lcpttg` beside the
        document-table generation, flush repairs missing/stale/corrupt exact
        generation files, and equality/`in` readers load the identity-matched
        typed term postings before document-table result emission.
      - [x] Extend exact-term generation consumption to canonical numeric
        equality and key-return exact visitors. Numeric selector term
        keys are canonicalized before lookup so `liblql` JSON number equality
        preserves equivalent source spellings such as `1` and `1.0`, and the
        old scalar-aware numeric exact sidecar reader has been removed from the
        live query path.
      - [x] Add immutable field-presence generation files and route simple
        `exists` readers through identity-matched namespace-local docID
        postings. Flush now publishes and repairs
        `index/query.index.lcppg`, and `exists` loads the prepared generation
        before document-table result emission.
      - [x] Add immutable numeric-range generation files and route simple
        `range` readers through identity-matched namespace-local docID
        postings. Flush now publishes and repairs
        `index/query.index.lcprg`, and range lookups parse canonical numeric
        generation terms before document-table result emission.
      - [x] Add immutable text/trigram generation files and route simple
        `prefix`/`contains` readers through identity-matched namespace-local
        docID postings.
        - [x] Add immutable string text generations and route
          `prefix`/`contains` readers through identity-matched
          namespace-local docID postings from `index/query.index.lcptxg`.
        - [x] Add trigram generations for selective contains/icontains
          candidate narrowing.
      - [x] Compile filtered exact sidecar candidates into a per-request
        term-ID posting table for equality and `in` docID readers, preserving
        existing live-state and secondary predicate guards while exercising
        adaptive postings in the pouch query path.
      - [x] Compile filtered field-presence sidecar candidates into a
        per-request posting table for positive `exists` docID readers, keeping
        the current posting-summary freshness and secondary predicate guards.
      - [x] Compile filtered numeric sidecar candidates into a per-request
        posting table for positive `range` docID readers, so range planning
        also exercises adaptive sparse/dense postings before decoding through
        the index layer.
      - [x] Route primary positive `prefix` candidate collection through a
        prefix docID reader callback and compile filtered text sidecar
        candidates into adaptive postings before converting back to summary
        keys.
      - [x] Route primary positive `contains` candidate collection through a
        contains docID reader callback, preserving final substring validation
        while compiling results into adaptive postings.
      - [x] Add trigram-backed `contains` compilation so substring validation
        can use a persisted gram-candidate docID set before exact text
        validation.
      - [x] Skip redundant primary `contains` revalidation after the contains
        compiler has already validated the selected text posting; secondary
        positive and negative contains predicates still run through the shared
        candidate guard.
        Verified on 2026-07-26 with the 4096-doc text benchmark:
        `ContainsMessage` measured about 6.3 ms for document results and about
        3.1 ms for key results after the sorted gram-candidate guard; Go disk
        measured about 4.1 ms and 0.98 ms respectively, so contains keys remain
        a pouch perf gap.
      - [x] Skip redundant primary `exists` and `prefix` revalidation in the
        generic docID candidate guard after the active field-presence/text
        reader has already validated the primary predicate; secondary positive
        and negative exists/prefix predicates still run through the shared
        guard.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `ExistsFlag` dropped from about 30.2 ms to about 7.16 ms and
        `PrefixOwner` from about 3.83 ms to about 1.76 ms. Go disk measured
        about 1.49 ms and 1.30 ms respectively, so persisted compiled
        dictionaries remain the next required parity step.
      - [x] Add a simple-primary docID candidate helper for `exists` and
        `prefix` compilation: when there are no residual indexed predicates,
        the reader still checks live state, summary freshness, hidden state,
        owner, and doc-table membership, but skips dispatch through the empty
        secondary predicate families.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `ExistsFlag` measured about 4.38 ms and `PrefixOwner` about
        1.61 ms. The matching Go disk run measured about 41.5 ms for
        `ExistsFlag` and about 1.05 ms for `PrefixOwner`; the Go exists number
        was noisy relative to earlier runs, so this records pouch improvement
        without closing the persisted-dictionary parity gap.
      - [x] Extend the simple-primary docID candidate helper to numeric
        `range` compilation after the active range reader has already validated
        the numeric bound predicate, preserving the same live-state, summary,
        hidden, owner, and doc-table checks.
        Verified on 2026-07-26 with the focused 4096-doc indexed key
        `RangeHalf` benchmark: pouch measured about 5.14 ms and Go disk about
        47.8 ms in the same run.
      - [x] Extend the simple-primary docID candidate helper to exact equality
        and non-wildcard `in` compilation after the active exact-term reader
        has already validated the field/value predicate, preserving the same
        live-state, summary, hidden, owner, and doc-table checks.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `EqSparse` measured about 0.78 ms and Go disk about 49.3 ms;
        pouch `InTags` measured about 4.73 ms and Go disk about 2.46 ms in
        that run. A later focused `benchmark-pouch-go-acceptance` sample on
        2026-07-26 measured pouch `InTags` at about 7.23 ms C-side and Go disk
        at about 46.9 ms, so this is now treated as a benchmark-noise watch
        item rather than a confirmed persisted-dictionary/posting-union gap.
    - [x] Add prepared-reader caching keyed by immutable pouch index identity.
      Active `src/` now has handle-owned prepared exact, presence, range, text,
      and temporal readers keyed by namespace plus immutable `query.index`
      identity.
      - [x] Add the first prepared exact reader cache for immutable
        exact-term/document-table generations and release it with the pouch
        handle.
      - [x] Add a bounded exact-result cache inside the prepared exact reader:
        the last normalized exact term set reuses its docID vector while the
        immutable index identity remains unchanged.
      - [x] Add bounded docID result caches inside the prepared presence,
        range, text, and temporal readers: the last normalized field lookup,
        numeric bounds, text predicate, or temporal bounds reuse their docID
        vector while the immutable index identity remains unchanged.
      - [x] Add the first prepared presence reader cache for immutable
        field-presence/document-table generations and release it with the pouch
        handle.
      - [x] Add the first prepared range reader cache for immutable
        numeric-range/document-table generations and release it with the pouch
        handle.
      - [x] Add the first prepared text reader cache for immutable
        text/document-table generations and release it with the pouch handle.
      - [x] Add the first prepared temporal reader cache for immutable
        temporal/document-table generations and release it with the pouch
        handle.
    - [x] Add sorted matched-key/result caching keyed by index identity plus a
      normalized selector plan. Active `src/` currently has bounded one-entry
      docID result caches in prepared exact, presence, range, text, and
      temporal readers, plus one-entry index-owned sorted result-page caches
      for the corresponding prepared readers. The page caches live in
      `lc_pouch_index_result`, are invalidated with immutable prepared-reader
      identity changes, and store key/docID rows keyed by normalized predicate
      plans before cursor-page emission.
      - [x] Add index-owned docID result paging over the document table and
        route the equality document/key scans through it, so cached equality
        result pages translate only the selected page of docIDs back through
        pouch summaries instead of translating the full match vector before
        cursor/limit handling.
      - [x] Route exact scalar document-result pages through the typed docID
        candidate stream before opening state bodies. Equality, non-wildcard
        `in`, and root equality `or` document queries now choose the visible
        cursor page from exact index metadata, stop after the first lookahead
        candidate, and read only the selected page's current bodies. Final
        `liblql` body evaluation remains skipped only for planner-proven exact
        predicates, preserving typed JSON scalar equality. Focused regression
        coverage asserts indexed `/tags[]` exact-`in` document pagination
        metadata across a resumed cursor, and exact document-return now repairs
        a corrupt `index/query.index.lcpdtg` before resolving the page through
        the persisted doc table. Verified on 2026-07-27 with focused 1024-doc
        indexed `EqDense` document comparison: Go lockd disk measured about
        16.2 ms wall time and pouch measured about 10.2 ms C-side.
      - [x] Route simple positive `exists` document/key scans through the same
        index-owned docID result paging bridge, sharing invalid-docID detection
        and avoiding full cached-match summary translation before cursor/limit
        handling.
      - [x] Route simple positive numeric `range` document/key scans through
        index-owned docID result paging, so cached range result pages translate
        only selected docIDs back through pouch summaries before streaming.
      - [x] Route simple non-wildcard positive `in` document/key scans through
        index-owned docID result paging, while leaving wildcard `in` on the
        existing pouch-side pagination path.
      - [x] Route simple positive `prefix` and `contains` document/key scans
        through index-owned docID result paging, so text predicate pages no
        longer translate the full cached match vector through pouch summaries
        before cursor/limit handling.
      - [x] Add direct document-table key snapshots for key-only numeric
        `range`, non-wildcard `in`, and `contains` scans so cached result pages
        do not allocate summary-index arrays before streaming keys.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `RangeHalf` measured about 5.26 ms, `InTags` about 4.28 ms, and
        `ContainsMessage` about 4.39 ms. Go disk measured about 46.6 ms, 1.95
        ms, and 1.06 ms respectively in the same run, so `InTags` and
        `ContainsMessage` key-return paths remain pouch performance gaps.
      - [x] Extend direct document-table key snapshots to simple key-only
        equality, exists, and prefix scans, so all simple cached docID result
        pages stream keys without allocating summary-index arrays when metadata
        rows are not needed.
        Verified on 2026-07-26 with the same 4096-doc indexed key benchmark:
        pouch `EqSparse` measured about 0.64 ms, `ExistsFlag` about 7.16 ms,
        and `PrefixOwner` about 1.76 ms after the paired primary-revalidation
        skip; Go disk measured about 55.0 ms, 1.49 ms, and 1.30 ms
        respectively.
      - [x] Add index-owned posting append decode for private adaptive
        postings, so callers can append decoded docIDs into a target set
        without allocating a temporary decoded set and copying it item-by-item.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `InTags` measured about 7.04 ms and `ContainsMessage` about 3.22
        ms; Go disk measured about 47.9 ms and 1.19 ms respectively in the
        same run, so contains key-return remains a pouch performance gap.
      - [x] Match the Go lockd disk contains execution order more closely:
        pouch now checks text postings for the substring before docID page
        emission, avoiding candidate-materialization work for text values that
        cannot match.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `ContainsMessage` key-return measured about 2.71 ms. The matching
        Go disk run measured about 47.5 ms and was noisy relative to earlier
        runs, so this records pouch improvement without treating the broader
        contains parity gap as closed.
      - [x] Remove redundant compiled `contains` candidate-key materialization:
        after the raw text posting substring check became authoritative, the
        active contains path no longer allocates borrowed key arrays or
        performs per-match binary searches before docID page emission.
        Verified on 2026-07-26 with the focused 4096-doc
        `ContainsMessage` indexed key benchmark: pouch measured about 2.75 ms;
        the matching Go disk run measured about 47.4 ms.
      - [x] Stop selected-field text term scans after the sorted sidecar has
        passed the target JSON Pointer field: prefix/contains/icontains
        readers now share the same field-bounded validation fast path as exact,
        range, and date readers while preserving final `liblql` acceptance.
        Focused unit coverage corrupts a later-field term after flush and
        proves `/a` prefix/contains queries still use the selected valid field
        slice. Verified on 2026-07-27 with the bounded 4096-doc acceptance
        matrix: pouch `ContainsMessage` measured about 27.9 ms C-side for
        keys and about 31.8 ms C-side for documents; the target completed in
        1m22s under the 3-minute cap. Range, `InTags`, and `DateAfter` remain
        slower than Go disk in that run.
      - [x] Reduce range/date/text term-reader allocation pressure: predicate
        value decoding now uses a reusable reader scratch buffer, and plain
        integer numeric terms take a direct hex parser before falling back to
        the general decoded `strtod` path for decimals and exponent forms.
        Unit range coverage now includes both integer terms and a decimal
        fallback term. Verified on 2026-07-27 with the bounded 4096-doc
        acceptance matrix: pouch `RangeHalf` measured about 29.9 ms C-side for
        keys and 66.6 ms for documents; `DateAfter` measured about 9.9 ms for
        keys and 28.5 ms for documents; the target completed in 1m22s.
        `RangeHalf`, `InTags`, and `DateAfter` still require deeper
        compiled-index/posting work to beat Go disk consistently.
      - [x] Add a durable `query.index` v6 term-field line table so indexed
        term readers can skip unrelated sorted field posting lines before
        parsing target candidates. Verified on 2026-07-27 with a focused
        4096-doc key benchmark: pouch `RangeHalf` measured about 23.8 ms
        C-side, `InTags` about 22.7 ms C-side, and `ContainsMessage` about
        29.6 ms C-side. This improves late-field scans but remains an interim
        sidecar acceleration before typed docID/posting generations. The
        broader `make benchmark-pouch-go-acceptance` gate completed in 1m33s
        on 2026-07-27 with pouch indexed key timings of about 22.4 ms
        `RangeHalf`, 19.8 ms `InTags`, 25.1 ms `ContainsMessage`, 11.8 ms
        `DateAfter`, and 33.9 ms `OrSparseOrFlag` C-side.
      - [x] Add durable exact term-value posting ranges and dense row-ordinal
        docIDs to `query.index`; this historical slice bumped the private
        sidecar to v9 and stored
        posting records before row records so exact equality, non-wildcard
        `in`, and root equality `or` readers can select field/value posting
        ranges without scanning unrelated row records first. Focused pouch
        unit coverage asserted the v9 sidecar, term-value table, docID
        term fields, root OR pagination, and the large default query header
        limit contract. Verified on 2026-07-27 with the bounded 4096-doc
        acceptance matrix in 1m47s: pouch still only beat Go disk on sparse
        equality in that sample (`EqSparse` keys about 27.4 ms C-side vs Go
        about 40.7 ms; documents about 19.4 ms C-side vs Go about 34.6 ms).
        Remaining indexed key gaps were `RangeHalf` about 32.8 ms C-side vs
        Go about 1.67 ms, `InTags` about 30.8 ms vs Go about 1.59 ms,
        `ContainsMessage` about 36.9 ms vs Go about 1.73 ms, `DateAfter`
        about 23.2 ms vs Go about 0.97 ms, and `OrSparseOrFlag` about
        45.1 ms vs Go about 1.20 ms. Remaining indexed document gaps were
        `RangeHalf` about 68.0 ms, `InTags` about 66.1 ms,
        `ContainsMessage` about 39.5 ms, `DateAfter` about 37.1 ms, and
        `OrSparseOrFlag` about 50.5 ms C-side, versus Go document timings of
        about 15.5 ms, 15.8 ms, 8.83 ms, 19.1 ms, and 9.11 ms respectively.
        This confirms the next performance work must move more of these paths
        onto compiled generation readers/result-cache paging rather than
        adding more text-sidecar skips.
      - [x] Fix the query-index docID collector so docID-oriented exact term
        readers stop decoding every matching key before dedupe, then decode
        only the surviving emitted keys. The same-field exact `in` key path
        now uses this docID route as well. Verified on 2026-07-27 with focused
        4096-document key comparisons: pouch `OrSparseOrFlag` measured about
        40.0 ms C-side for 640 rows versus Go lockd disk at about 35.5 ms in
        that sample, and pouch `InTags` measured about 30.0 ms C-side for
        1000 rows versus Go at about 34.3 ms in that focused sample. The
        bounded 4096-document acceptance matrix completed in 1m33s; indexed
        pouch key timings were about 14.5 ms `EqSparse`, 38.3 ms `RangeHalf`,
        29.3 ms `InTags`, 39.8 ms `ContainsMessage`, 19.4 ms `DateAfter`, and
        41.2 ms `OrSparseOrFlag` C-side. Indexed document timings were about
        16.8 ms, 68.3 ms, 66.0 ms, 36.9 ms, 43.0 ms, and 61.5 ms C-side for
        the same scenarios. The broad acceptance matrix still shows Go lockd
        disk ahead on every case except sparse equality, so the next real
        performance cut remains binary/adaptive postings and cached docID page
        results rather than further text-sidecar routing.
    - [x] Preserve final `liblql` predicate authority by treating indexed
      docID sets as candidate supersets whenever the planner cannot prove exact
      acceptance.
      - [x] Add residual-filter pagination coverage for indexed date selectors:
        field-presence candidates include invalid, old, and accepted date
        documents, but both `query_keys` and document `query` emit and cursor
        only rows accepted by `liblql`.
      - [x] Add residual date selector cache-reuse coverage: the first
        `/created_at` field-presence candidate collection populates the
        normalized result cache, resumed key pages and document pages reuse the
        cached candidate vector, and final visible rows still come only from
        `liblql` acceptance.
      - [x] Route indexed key-return residual filters through the row-scan
        visitor so `liblql` evaluates the already-surfaced candidate body
        instead of reopening state by key for every candidate. Focused
        4096-document `DateAfter` key-return timing improved from about
        1.73 s to about 91.6 ms C-side while preserving key-only output.
      - [x] Narrow simple indexed `date after` candidates for canonical UTC
        second strings by skipping text-helper postings, old canonical date
        strings, exact-boundary strings, non-string field values, and
        implausible datetime strings before final `liblql` evaluation. Focused
        unit coverage now proves invalid string, numeric, old, boundary,
        accepted, resumed-key, and resumed-document cases. The 2026-07-26
        4096-document `DateAfter` benchmark now reports 1,048 candidates and
        about 50.4 ms C-side for keys / 55.7 ms C-side for documents.
      - [x] Add storage-owned temporal parsing for simple indexed `date after`
        candidates and final fast filtering, matching liblql's date-only,
        RFC3339/RFC3339Nano offset, fractional, and naive-UTC selector
        semantics closely enough that supported non-canonical datetime strings
        no longer route through the residual evaluator. Focused coverage now
        proves normalized date-only, offset, fractional, and naive UTC
        candidates plus a date-only selector bound; the 2026-07-26
        4096-document `DateAfter` benchmark reported 1,048 candidates and about
        43.4 ms C-side for keys / 60.7 ms C-side for documents.
      - [x] Persist typed temporal postings in immutable compiled index
        generations instead of reparsing temporal string terms. Active source
        now has the index-owned date parsing/bound primitive, repairable
        `.lcptdg` temporal generation file with normalized instant terms, and
        prepared temporal reader cache.
    - [x] Rename or clearly alias benchmark labels from `Rows` to
      `Documents`/`DocumentResults`, because pouch and lockd are document
      stores; `Rows` currently means streamed document-result items, not
      relational rows.
    - [x] Update pouch-vs-Go benchmarks to expose cache-warm page 1/page N
      behavior, matched-key vector reuse, candidate docID counts, and document
      streaming/materialization cost separately.
      - [x] Report pouch C-side first-page and later-page latency, query
        candidate metadata, page counts, and streamed bytes; report equivalent
        lockd disk first-page and later-page latency from the Go client path.
      - [x] Add a direct matched-key vector reuse/cache-hit metric once the
        pouch result cache exposes observable counters.
      - [x] Add CTest smoke coverage for the native benchmark help surface so
        `lockdc_bench --help` and `lockdc_bench -h` print usage without
        accidentally running the benchmark suite.
    - [x] Re-run the expanded 4096+ document benchmark matrix and use it as the
      acceptance gate for the pouch index path, with pouch expected to
      beat or match Go disk on key-only and document-result scenarios unless
      an explicit design tradeoff is documented.
      - [x] Run the first 4096/full-scenario matrix attempt and capture the
        dominant indexed document hotspot: numeric `RangeHalf` spent about
        426 ms in first-page pouch range planning before the primary-range
        revalidation fix.
      - [x] Remove the redundant primary range-term revalidation from the
        docID candidate helper path; focused 4096 indexed `RangeHalf` now
        measures about 22.6 ms for pouch document results vs 65.9 ms for Go
        disk, and about 5.37 ms for pouch key results vs 48.9 ms for Go disk.
      - [x] Replace the current broad 4096/full-scenario run with a bounded
        acceptance matrix that completes inside the intended 3-minute envelope
        while still covering the slow/representative indexed and scan cases.
      - [x] Add the `DateAfter` residual-filter scenario to the bounded
        4096-document pouch-vs-Go acceptance matrix so the opt-in comparison
        gate covers candidate-superset date planning plus final `liblql`
        acceptance across document/key and scan/index paths.
      - [x] Add and verify `make benchmark-pouch-go-acceptance`, a 4096-doc
        matrix over document/key returns, indexed/scan engines, and
        `EqSparse`, `RangeHalf`, `InTags`, and the known slower
        `ContainsMessage` text-search case; medium/acceptance pouch and
        lockd disk harnesses should seed once per document-count/return-mode
        group so the run measures query behavior instead of repeated setup.
        Verified on 2026-07-26: the target completed in 2m14s. Pouch indexed
        document-return cases measured about 0.84 ms (`EqSparse`), 24.6 ms
        (`RangeHalf`), and 18.6 ms (`InTags`); key-return cases measured
        about 0.83 ms, 6.34 ms, and 4.49 ms respectively.
      - [x] Harden the Go comparison benchmark target with an outer process
        timeout after the 2026-07-26 acceptance run exceeded the intended
        envelope without producing final results; a focused 64-document
        indexed `EqSparse` smoke run proved the wrapper path.
      - [x] Move live query-field posting maintenance from sorted insertion to
        append-plus-lazy-sort, matching the append-first direction of the
        segmented pouch design. Pouch regression coverage now proves updates before
        the first query discard stale field values and duplicate wildcard array
        postings produce one key result after the lazy sort barrier.
      - [x] Batch query-field sidecar fsyncs per indexed JSON body and make
        the Go pouch-vs-disk harness seed pouch through direct C pouch
        writes before querying through the public client LQL path. Pouch fsync
        regression coverage proves a multi-field state write emits one
        query-index fsync after the format record exists, and a 64-document
        focused Go `DateAfter` smoke passes with result-cache/page metrics.
      - [x] Re-run `make benchmark-pouch-go-acceptance` after timeout
        hardening and use the completed output as acceptance evidence. Verified
        on 2026-07-26: the bounded 4096-document pouch-vs-Go matrix completed
        in 1m05s under the 3-minute outer timeout. The previously failing
        focused pouch indexed key-return `DateAfter` case now completes in
        about 30s wall time with about 94 ms C-side query time after removing
        forced full query-index replay from generation refresh and making
        `flush_index` perform the lightweight query-field posting
        sort/deduplicate barrier. The same acceptance run still identifies
        slower-than-Go indexed pouch cases for follow-up performance work:
        indexed key/document `EqSparse`, indexed `ContainsMessage`, and indexed
        `DateAfter` remain materially slower than Go lockd disk.
      - [x] Re-run after pouch-native API/source naming cutover. Verified on
        2026-07-26: `make benchmark-pouch-go-acceptance` completed in 1m53s,
        proving the Go/cgo harness compiles against `lc_pouch_open` and still
        enforces the 3m cap. The run continues to show the remaining indexed
        pouch gaps: sparse equality and first-page contains/date-after are still
        materially slower than Go lockd disk and remain the next performance
        targets after the pouch boundaries are in place.
      - [x] Remove avoidable query-time sidecar rebuild/full-validation work
        from indexed reads and fix the Go lockd disk document benchmark drain
        so it does not cancel the query context before consuming streamed
        rows. Verified on 2026-07-27 with focused 4096-document indexed
        `EqSparse`: Go lockd disk measured about 28 ms keys / 35 ms documents;
        pouch measured about 61 ms keys / 62 ms documents C-side. Pouch remains
        slower, but the sparse equality gap is now candidate/state-read work
        rather than unconditional index generation rebuild.
      - [x] Fix the bounded acceptance benchmark selector so
        `make benchmark-pouch-go-acceptance` actually runs both
        `MediumLQL...` pouch cases and `MediumLockdDisk...` reference cases:
        the target now compares indexed document/key scenarios against Go disk
        while retaining pouch scan baselines under the same 3-minute wrapper.
      - [x] Batch candidate state reads after posting collection so indexed
        queries refresh namespace state once per candidate set instead of once
        per key. Verified on 2026-07-27 with focused 4096-document indexed
        runs: `EqSparse` pouch improved to about 7.0 ms keys / 7.4 ms
        documents C-side versus Go lockd disk at about 32 ms / 38 ms;
        `ContainsMessage` pouch measured about 38 ms keys / 40 ms documents
        versus Go lockd disk at about 31 ms / 44 ms; `DateAfter` remained a
        gap at about 166 ms keys / 174 ms documents versus Go lockd disk at
        about 1.1 ms / 12 ms because pouch still used broad presence
        candidates instead of temporal postings.
      - [x] Replace the broad `/created_at` presence-candidate path for
        bounded indexed `DateAfter` with a typed temporal reader over scalar
        term postings, with temporal semantics owned by `lc_pouch_index`.
        Verified on 2026-07-27 with focused 4096-document indexed `DateAfter`:
        pouch measured about 31 ms keys / 31 ms documents C-side versus Go
        lockd disk at about 46 ms / 44 ms in the same run.
      - [x] Make the Go/cgo pouch acceptance benchmark seed one reusable
        pouch fixture per document-count/return-mode group instead of
        reseeding every subbenchmark. Verified on 2026-07-27:
        `make benchmark-pouch-go-acceptance` now completes in 1m08s under the
        3-minute cap. The 4096-document pouch index matrix now identifies
        `RangeHalf` at about 153 ms keys / 174 ms documents C-side and
        `InTags` at about 102 ms keys / 110 ms documents C-side as the next
        concrete index performance targets; `EqSparse` is about 7.5 ms /
        8.1 ms, `ContainsMessage` about 40 ms / 41 ms, and `DateAfter` about
        31 ms / 31 ms.
      - [x] Align pouch query page limits with the lockd public API contract
        and stop indexed batched state reads once the cursor-producing extra
        match has been found; simple equality, `in`, numeric range, and
        bounded date candidates also skip redundant final `liblql` evaluation
        after the planner has proven exact candidate acceptance. Verified on
        2026-07-27 with focused 4096-document indexed `RangeHalf`/`InTags`:
        pouch `RangeHalf` improved to about 56 ms keys / 60 ms documents
        C-side versus Go lockd disk at about 31 ms / 57 ms; pouch `InTags`
        measured about 73 ms keys / 91 ms documents versus Go lockd disk at
        about 1.5 ms / 13 ms.
      - [x] Add metadata-only batched state reads for exact key-only indexed
        pages, so equality, `in`, numeric range, and bounded date key queries
        can verify live/hidden metadata without opening payload bodies.
        Verified on 2026-07-27 with focused 4096-document indexed
        `RangeHalf`/`InTags`: pouch `RangeHalf` measured about 47 ms keys /
        62 ms documents C-side versus Go lockd disk at about 35 ms / 44 ms;
        pouch `InTags` measured about 66 ms keys / 72 ms documents versus Go
        lockd disk at about 2.0 ms / 14 ms, so compiled array membership
        remains the dominant open gap.
        The same 2026-07-27 bounded acceptance target completed in 1m08s with
        pouch indexed key-return at about 6.6 ms `EqSparse`, 49 ms
        `RangeHalf`, 60 ms `InTags`, 38 ms `ContainsMessage`, and 19 ms
        `DateAfter`; indexed document-return measured about 6.6 ms, 60 ms,
        71 ms, 42 ms, and 34 ms respectively.
      - [x] Route exact multi-value `in` selectors through a single
        pouch-native scalar postings reader pass instead of reopening and
        rescanning the sidecar once per `any` value. Verified on 2026-07-27
        with focused 4096-document indexed `InTags`: pouch measured about
        39 ms keys / 50 ms documents C-side versus Go lockd disk at about
        29 ms keys / 53 ms documents in the same run. Key-return array
        membership is still a tracked gap, but the redundant postings pass
        has been removed. The bounded acceptance target completed in 1m10s
        after this change, with pouch indexed `InTags` at about 38 ms keys /
        51 ms documents C-side.
      - [x] Cut the pouch query-index sidecar to v5 and store live metadata on
        scalar term postings, allowing exact key-return queries to sort/dedup
        candidate postings and emit from sidecar metadata without a namespace
        state reread. Verified on 2026-07-27 with focused 4096-document
        indexed key-return `RangeHalf`/`InTags`: pouch measured about
        31 ms / 27 ms C-side versus Go lockd disk at about 31 ms / 1.5 ms.
        The bounded acceptance target completed in 1m08s after this change,
        with pouch indexed key-return at about 5.7 ms `EqSparse`, 30 ms
        `RangeHalf`, 27 ms `InTags`, 59 ms `ContainsMessage`, and 10 ms
        `DateAfter`; document-return stayed body-read-bound at about 7.5 ms,
        70 ms, 58 ms, 46 ms, and 28 ms respectively.
      - [x] Add a sorted-union visitor for exact multi-value scalar postings
        and route key-return `in` predicates through it, avoiding the generic
        global candidate sort/dedup step after posting collection. Verified on
        2026-07-27 with focused 4096-document indexed `InTags`: pouch measured
        about 30 ms C-side versus Go lockd disk at about 32 ms in that sample.
        The bounded acceptance target completed in 1m08s after this change,
        with pouch indexed key-return `InTags` at about 26 ms C-side. This is
        only a modest improvement; the remaining path still collects matching
        postings before merge, so the next larger cut is a key-ordered or
        field/key/value posting layout that can stop after the cursor page.
      - [x] Remove per-line allocation/copy from the hot scalar term sidecar
        parser and tokenize the mutable read buffer directly. Verified on
        2026-07-27 with focused 4096-document indexed `InTags`: pouch measured
        about 25 ms keys C-side versus Go lockd disk at about 39 ms in that
        sample. The bounded acceptance target completed in 1m07s after this
        change, with pouch indexed key-return at about 6.0 ms `EqSparse`,
        29 ms `RangeHalf`, 25 ms `InTags`, 41 ms `ContainsMessage`, and
        9.9 ms `DateAfter`; document-return remained body-read-bound at about
        8.0 ms, 67 ms, 61 ms, 45 ms, and 30 ms respectively.
      - [x] Add byte spans to durable `query.index` term-field and term-value
        tables so selected term readers can seek directly to matching posting
        slices instead of discarding unrelated term lines. The private sidecar
        is now v11 and keeps line spans for validation while using
        term-section-relative byte offsets for exact equality, `in`, root
        equality `or`, range, date, prefix, and contains candidate reads.
        Verified on 2026-07-27 with focused 4096-document indexed key
        comparisons: pouch `InTags` measured about 17.3 ms C-side for 1000
        rows versus Go lockd disk at about 34.2 ms in the same run; pouch
        `OrSparseOrFlag` measured about 33.7 ms C-side for 640 rows versus Go
        at about 2.3 ms, so root OR still needs a compiled docID/posting union
        path.
      - [x] Add indexed `OrSparseOrFlag` to the 4096-document pouch-vs-Go
        acceptance matrix after indexed root `or` support landed. Verified on
        2026-07-27: `make benchmark-pouch-go-acceptance` completed in 1m33s
        under the 3-minute cap. Go lockd disk measured about 1.3 ms keys /
        9.8 ms documents for indexed `OrSparseOrFlag`; pouch measured about
        41 ms keys / 43 ms documents C-side, so multi-field OR union planning
        is now an explicit pouch performance gap.
      - [x] Reduce one part of the indexed root `or` gap by scanning the
        query-index scalar sidecar once for a sorted exact field/value term
        set instead of reopening it once per OR child. Verified on 2026-07-27
        with focused 4096-doc `OrSparseOrFlag` key comparison:
        `make __benchmark-pouch-go-fast POUCH_GO_FAST_BENCH='Fast.*/.*/.*/.*/OrSparseOrFlag' POUCH_GO_FAST_SEED_ROWS=4096 POUCH_GO_FAST_BENCHTIME=1x POUCH_GO_FAST_TIMEOUT=90s`
        measured pouch at about 36.9 ms C-side for 640 rows and Go lockd disk
        at about 32.0 ms wall time for the same row count. Pouch still needs a
        typed docID/posting OR path to close this gap decisively.
        Broader acceptance stayed within the gate on 2026-07-27:
        `make benchmark-pouch-go-acceptance` completed in 1m35s, with pouch
        indexed `OrSparseOrFlag` at about 37.8 ms keys / 37.3 ms documents
        C-side and Go disk at about 1.7 ms keys / 13.3 ms documents.
      - [x] Route exact key-only indexed root `or` through a query-index-owned
        merged scalar-term visitor instead of returning all OR hits to the
        client for a second sort/pagination pass. Focused root-OR pagination
        coverage now asserts stable key order across pages. Verified on
        2026-07-27 with 4096-doc focused `OrSparseOrFlag` key comparison:
        `make __benchmark-pouch-go-fast POUCH_GO_FAST_BENCH='Fast(Pouch|LockdDisk)/Keys/Docs4096/index/OrSparseOrFlag' POUCH_GO_FAST_SEED_ROWS=4096 POUCH_GO_FAST_BENCHTIME=1x POUCH_GO_FAST_TIMEOUT=90s`
        measured pouch at about 32.7 ms C-side for 640 rows and Go lockd disk
        at about 35.4 ms wall time in the same sample. This closes the focused
        key-only sample. The broader 4096-doc acceptance matrix stayed inside
        the 3-minute cap on 2026-07-27, completing in 1m34s; pouch indexed
        `OrSparseOrFlag` measured about 31.5 ms keys / 36.5 ms documents
        C-side while Go disk measured about 1.1 ms keys / 10.0 ms documents,
        so typed docID/posting OR remains required to close the broader gap.
      - [x] Mark exact typed-scalar root `or` plans as exact candidates so
        key-only root `or` can use the docID-oriented scalar-term visitor
        without re-reading candidate documents. This preserves liblql JSON
        scalar semantics: strings, numbers, booleans, and null remain distinct
        scalar classes instead of following Go LQL's looser scalar behavior.
        Focused coverage now checks a root OR over numeric and boolean terms
        does not match string `"1"` or string `"true"`. Verified on
        2026-07-27 with:
        `make __benchmark-pouch-go-fast POUCH_GO_FAST_BENCH='Fast(Pouch|LockdDisk)/Keys/Docs4096/index/OrSparseOrFlag' POUCH_GO_FAST_SEED_ROWS=4096 POUCH_GO_FAST_BENCHTIME=1x POUCH_GO_FAST_TIMEOUT=90s`
        where Go lockd disk measured about 31.8 ms wall time for 640 rows and
        pouch measured about 17.1 ms C-side for the same focused key-only
        root-OR sample.
      - [x] Bypass the intermediate key-list sort/compact pass for key-only
        single-term exact scalar lookups when the scalar class is non-numeric.
        One exact non-numeric typed term cannot overlap itself, and term rows
        are already key/docID ordered because docIDs are assigned from the
        sorted row table. Numeric equality keeps the conservative path because
        liblql treats equivalent JSON number spellings as equal, so one query
        value may match multiple value slices that need a final merge. Focused
        coverage now pages an indexed `bucket=needle` equality query through
        the direct path and proves hidden rows are excluded. Verified on
        2026-07-27 with focused 4096-doc indexed `EqSparse` key comparison:
        Go lockd disk measured about 40.5 ms wall time for 64 rows and pouch
        measured about 13.5 ms C-side. The medium matrix still shows pouch
        indexed 1024-doc dense equality at about 4.3 ms C-side versus Go at
        about 0.8 ms, so closing dense/multi-value search still needs a
        lower-level posting/doc-table sidecar rather than more query-layer
        sorting fixes.
      - [x] Avoid full term-value table materialization for key-only
        single-term exact scalar sidecar reads when the scalar class is
        non-numeric. The reader now validates the sorted term-field table,
        scans the sorted term-value table only until the requested typed
        field/value pair has been passed, seeks directly to the matching term
        slice, and skips the remaining table lines without allocating them.
        This preserves liblql JSON scalar equality as the semantic target:
        strings, numbers, booleans, and null stay distinct real JSON scalars,
        while Go lockd's looser LQL scalar behavior is only a
        benchmark-reference caveat.
        Numeric exact equality now uses canonical exact-generation term keys,
        so equivalent JSON number spellings such as `1` and `1.0` resolve to
        the same typed posting lookup instead of scanning separate scalar
        sidecar value slices.
      - [x] Complete the pouch index cutover performance gate with prepared
        generation signature validation, true header-only sidecar freshness
        reads, single-writer benchmark endpoint coverage, cached sorted
        result-page reuse, cached state payload sources for repeated document
        reads, and a sorted namespace-cache record index for batched document
        reads. Verified on 2026-07-27 with
        `make benchmark-pouch-go-acceptance`: the bounded 4096-document
        matrix completed in 48s. Go lockd disk indexed key-return measured
        about 0.66 ms `EqSparse`, 1.16 ms `RangeHalf`, 1.04 ms `InTags`,
        0.58 ms `ContainsMessage`, 0.98 ms `DateAfter`, and 0.71 ms
        `OrSparseOrFlag`; pouch indexed key-return measured about 0.18 ms,
        0.56 ms, 0.21 ms, 0.18 ms, 0.23 ms, and 0.16 ms C-side respectively.
        Go lockd disk indexed document-return measured about 1.33 ms,
        12.08 ms, 14.66 ms, 8.21 ms, 16.87 ms, and 7.52 ms; pouch measured
        about 0.45 ms, 1.54 ms, 0.78 ms, 0.85 ms, 0.88 ms, and 0.64 ms
        C-side respectively.
  - [x] Cut pouch storage over to the unreleased fresh segmented
    per-namespace history format; no legacy `store.log` import path or
    migration is required because pouch has not shipped.
    - [x] Create the per-namespace layout under
      `<root>/namespaces/<escaped-namespace>/` with `manifest`, `segments/`,
      `snapshots/`, `markers/`, and `queue-notify/`, while keeping shared
      lock/backend identity paths explicit.
      - [x] Establish the initial active-segment scaffold and manifest-open
        record during namespace writes; the root-level `store.log` placeholder
        is not an authoritative record source.
      - [x] Keep the per-namespace segmented mechanics in active
        `lc_pouch_namespace` and `lc_pouch_state` modules, preserving the
        unreleased pouch layout while keeping retired history code out of live
        compilation.
      - [x] Keep active replay path collection, manifest repair, obsolete path
        cleanup, active generation tracking, and compaction candidate counting
        in the active pouch namespace/state modules.
      - [x] Keep compact snapshot path handling and next-segment selection in
        the active pouch namespace/state modules, with deprecated helper code
        retained only as reference material.
    - [x] Implement manifest append/replay for segment open, segment seal,
      snapshot install, obsolete segment, and obsolete snapshot records.
      - [x] Append text manifest lifecycle records for current active-segment
        open, compaction rewrite, and obsolete compaction-backup events.
      - [x] Append manifest `seal` and next-segment `open` records when an
        active namespace segment crosses the current seal threshold.
      - [x] Replay manifest `open`, `compact`, `seal`, and `obsolete` segment
        lifecycle records when choosing authoritative namespace segment tails.
      - [x] Replay manifest `snapshot` records as the authoritative namespace
        base before later non-obsolete segment tails.
      - [x] Append manifest `snapshot` records for compacted namespace output
        and manifest `obsolete` records for superseded segment and snapshot
        files.
      - [x] Preserve segmented snapshot sets during manifest replay; `snapshot`
        records add snapshot files, while `obsolete` records retire superseded
        snapshot or segment files.
    - [x] Implement manifest repair for missing manifests, manifestless
      segments, crash-incomplete manifests, and legacy open-only manifests
      produced during the new segmented development path.
      - [x] Repair missing or empty namespace manifests from active segment
        directory scans during replay/open.
      - [x] Repair non-empty manifests by appending `open` for existing segment
        files that no manifest lifecycle record has mentioned yet, without
        resurrecting manifest-obsoleted files.
      - [x] Repair crash-incomplete manifest tails by separating unterminated
        partial records from appended repair records, then replaying the
        recovered unmentioned segment.
    - [x] Move writes from the root-level `store.log` to active namespace
      segments, including active segment creation, sealing thresholds, fsync
      boundaries, and writer marker refresh.
      - [x] Add explicit payload file identity to live state, object, and queue
        refs so read sources are no longer hard-wired to `store.log` and can
        follow namespace segment paths during the write cutover.
      - [x] Append ordinary memory and fd-backed records to the namespace
        active segment during the segmented write cutover.
      - [x] Move custom object-copy and queue-fd append paths to the namespace
        active segment shadow before segment replay becomes authoritative.
      - [x] Remove root `store.log` replay/import fallback; active namespace
        segments and installed snapshots are the only authoritative record
        sources for the unreleased segmented format.
      - [x] Rotate active namespace segments by size threshold and route all
        segment-shadow append paths through the shared active-segment opener.
      - [x] Resolve active namespace append paths from manifest state after
        snapshot compaction, opening the next segment generation when no active
        tail remains.
      - [x] Store live state, object, and queue payload refs from namespace
        segment append locations instead of root `store.log` offsets.
      - [x] Refresh independent handles from authoritative namespace segment
        generation when segments exist, instead of depending on root
        `store.log` size changes.
      - [x] Stop appending ordinary memory-backed records (metadata puts and
        removes, state removes/links, object removes, queue metadata records)
        to root `store.log`; append them only to namespace segments.
      - [x] Stop appending fd-backed state/object/queue payload records to
        root `store.log`; namespace segments are now the only record
        destination for those append paths.
    - [x] Replay installed snapshots plus non-obsolete segment tails in
      deterministic order, reset replay projections after snapshot or obsolete
      set changes, and preserve corrupt-tail truncation semantics per segment.
      - [x] Decouple record replay from `store->log_fd` / `store->log_path`
        by routing root replay through an explicit fd/path replay input; the
        namespace segment replay path now reuses this input wrapper.
      - [x] Replay active namespace segments in deterministic path order; when
        no namespace segments exist, reset replay projections to an
        empty store instead of importing root `store.log`.
      - [x] Discover and replay every numbered `seg-*.log` file in each
        namespace, so sealed historical segments and the active tail are both
        authoritative.
      - [x] Truncate invalid/trailing bytes from namespace segment files during
        authoritative segment replay, matching root-log corrupt-tail semantics.
      - [x] Replay installed namespace snapshots before later active segment
        tails, and include snapshot files in namespace generation refresh
        detection.
      - [x] Replay multi-file compacted snapshots deterministically so large
        compacted live sets split across snapshot files remain authoritative.
    - [x] Implement snapshot compaction and cleanup: capture live refs,
      validate drift before install, protect live state-link targets, mark old
      segments/snapshots obsolete, and retry obsolete-file cleanup.
      - [x] Make current compaction segment-aware by backing up active segments,
        writing compacted live records into fresh active segments, restoring on
        failure, and reading live payloads from recorded body paths.
      - [x] Count active namespace segment bytes in compaction thresholds and
        result stats so `if_needed` and reported log sizes follow the
        segmented write path instead of root `store.log`.
      - [x] Install compacted live records as namespace snapshot files, replay
        from the installed snapshots, and preserve materialized state-link and
        queue payload bodies across compaction/reopen.
      - [x] Retry cleanup for manifest-obsolete segment and snapshot files
        during namespace history collection/replay, while preserving active snapshot
        sets and tolerating already-missing obsolete files.
      - [x] Stop installing compacted root `store.log` replacements; compaction
        now preserves high-water state in internal namespace snapshots and
        refreshes readers from identity-based segmented log generations.
      - [x] Materialize live state-link payloads into compacted snapshots under
        the writer lock so old segment/snapshot bodies can be obsoleted without
        dangling linked payload references.
    - [x] Add optional background compaction scheduling for pouch, modeled
      after Go lockd disk but scoped to embedded C lifecycle constraints.
      - [x] Add open options for enabling/disabling scheduled compaction and
        configuring the first threshold surface: minimum log bytes and obsolete
        record multiplier.
      - [x] Run scheduled compaction from an explicit store-owned maintenance
        tick that uses the writer lock and never races foreground writes.
      - [x] Report scheduled compaction diagnostics through the private pouch
        control surface and tests, including disabled, skipped, and compacted
        outcomes.
      - [x] Keep manual `compact(force|if_needed)` deterministic and
        foreground-safe even when background scheduling is enabled.
      - [x] Add interval scheduling for explicit scheduled maintenance ticks
        with observable `interval-not-elapsed` diagnostics and no effect on
        foreground-safe manual compaction.
      - [x] Add cleanup-only maintenance passes that retry manifest-obsolete
        segment/snapshot file cleanup without requiring scheduled compaction to
        be enabled.
      - [x] Add a scheduled compaction minimum-candidate-files gate that counts
        active snapshots plus non-current segment tails per namespace and
        reports `below-candidate-threshold` without affecting foreground-safe
        manual compaction.
      - [x] Add obsolete-file delete grace for scheduled/cleanup maintenance
        so manifest-obsolete segment and snapshot files can age before unlink.
      - [x] Add a scheduled compaction minimum-reclaimable-bytes gate that
        estimates compacted live log bytes and reports
        `below-reclaimable-threshold` without entering compaction.
      - [x] Add scheduled not-before deadline gating with observable
        `deadline-not-reached` diagnostics.
      - [x] Add optional scheduled-tick I/O throttling.
    - [x] Add pouch marker lifecycle for segmented namespace logs, using the
      Go lockd disk backend only as a reference for stale-writer detection.
      - [x] Clarify and implement root-level writer-presence markers for
        exclusive writer detection/fencing, separate from namespace history
        query/segment files.
      - [x] Implement per-namespace `markers/writer-*.marker`
        refresh markers that are touched after successful commit groups so
        independent handles can cheaply detect another writer's namespace
        changes before doing full manifest/segment scans.
      - [x] Cache peer writer marker snapshots by name, modtime, and size and
        periodically force segment scans so marker hints cannot suppress all
        fallback validation indefinitely.
      - [x] Add a marker-directory mtime fast path for the peer-marker snapshot
        cache, with periodic forced full marker scans because filesystem/NFS
        mtime granularity can hide changes.
      - [x] In single-writer mode, allow marker-synced handles to skip refresh
        scans unless forced, matching Go disk's native single-writer
        optimization.
      - [x] Cover marker creation, marker payload toggling, peer marker
        detection, unchanged-marker refresh skipping, stale/legacy root marker
        classification, and close vs abort cleanup with focused pouch tests.
    - [x] Move durable query summary/posting sidecars into the segmented
      lifecycle so index rebuild, compaction, and crash recovery are tied to
      namespace log generations.
      - [x] Rebuild query summary and field-posting sidecars from live
        namespace segment/snapshot body refs when the root sidecar is missing,
        including closed-store missing-sidecar recovery with no usable
        `store.log`.
      - [x] Move the durable query sidecar and compaction temp into the
        internal `.lockd` namespace history instead of the pouch root, while
        preserving stale cleanup for earlier root-level compaction temps.
    - [x] Add focused recovery tests for fresh segmented stores, reopen,
      corrupt tails, manifest repair, snapshot install, obsolete cleanup,
      state-link protection, and query/index rebuild from authoritative
      namespace history.
      - [x] Cover manifest-obsolete snapshot cleanup on reopen without losing
        the active installed snapshot body.
      - [x] Cover second-generation snapshot compaction obsoleting and cleaning
        the prior snapshot, then reopening from the new snapshot.
      - [x] Cover query sidecar field-posting rebuild from authoritative
        namespace segment bodies after removing `query.index` and truncating
        the legacy root log.
      - [x] Cover fresh segmented state replay with the root `store.log`
        removed, independent-handle generation refresh, manifest repair,
        corrupt segment tail truncation, snapshot install/tail override,
        obsolete snapshot cleanup, and state-link compaction materialization.
    - [x] Add performance benchmarks that compare segmented pouch against the
      Go lockd disk backend on large data and indexed low-match LQL workloads
      after segmented correctness tests are stable.
      - [x] Add local segmented pouch scan-vs-index benchmark cases for
        low-match strict JSON Pointer LQL field selectors over document and
        key-only query paths.
      - [x] Add opt-in live Go lockd disk benchmark cases for the same
        low-match strict JSON Pointer LQL field selector document and key-only
        query paths, keeping them out of default local `all` runs unless
        `LOCKDC_BENCH_LIVE=1` is set.
      - [x] Use the benchmark iteration argument as the document-count control so
        local pouch and live Go disk cases can be run at 1k, 100k, or larger
        sizes without changing the benchmark binary.
      - [ ] Keep expanding the in-project C benchmark matrix as the release-gate
        candidate surface for liblockdc-local behavior; the Go module is only
        for opt-in cross-backend e2e comparisons and stress work.
        - [x] Add native pouch scan/index document/key benchmark cases for the
          same public LQL selector families used by the Go comparison harness:
          sparse/dense equality, numeric range, scalar `in`, array `in` via
          `/tags[]`, exists, prefix, contains, simple `and`, and simple `or`.
        - [x] Add native pouch scan/index document/key benchmark cases for the
          date residual-filter path: `DateAfter` seeds valid, invalid, and
          out-of-range date values so the benchmark measures indexed
          canonical-date candidate narrowing plus final `liblql` acceptance.
        - [x] Add native pouch scan/index document/key benchmark cases for
          case-insensitive text selectors: `iprefix` and `icontains` over
          `/tags[]`, with the benchmark iteration argument controlling seeded
          document count.
        - [x] Add native pouch scan/index document/key benchmark cases for
          recursive field-presence selectors such as `exists /details/**`, so
          the release-gate matrix covers the indexed container-presence
          candidate path that still relies on final `liblql` acceptance.
        - [x] Add native public-LQL pouch benchmark cases for the same seeded
          document shape as the Go comparison harness: sparse/dense equality,
          numeric range, scalar `in`, array `/tags[]` `in`, contains, date
          after, plus scan-mode root `or`, across key and document result
          modes.
        - [x] Add indexed root `or` benchmark coverage after the pouch index
          planner grows a real union plan for multi-field public LQL
          disjunctions such as `bucket == needle OR flag == true`; indexed
          execution now unions child equality posting candidates and preserves
          final `liblql` acceptance before registering key/document benchmark
          cases.
- [ ] Expand e2e coverage when new lockd server surfaces are added.
  - [ ] Refine pouch e2e coverage around segmented manifest/snapshot
    lifecycle, manifest repair, background compaction scheduling, marker
    recovery, search/index rebuild, and large namespace stress scenarios.
    - [x] Register the dormant `lc_e2e_pouch_direct` shard in the e2e CMake
      graph and add a local pouch lifecycle e2e: it seeds state, attachment,
      queue, and retention namespaces through `pouch://`, forces pouch
      maintenance compaction/cleanup, reopens the store, verifies indexed
      survivor query behavior, verifies namespace retention deletion, verifies
      attachment listing after maintenance, and dequeues/acks the surviving
      queue message. The same slice fixes the shared test temp helper so
      missing-path tracking cannot create orphan owner markers and stale
      cleanup removes orphan `*.liblockdc-test-tmp-owner` sidecars.
    - [x] Add a bounded large-namespace segmented pouch e2e: it writes many
      JSON documents through direct `lc_pouch` storage with a small segment
      target, proves indexed public client queries see every document before
      maintenance, forces compaction with multi-segment candidate diagnostics,
      retries cleanup, reopens through `pouch://`, and proves indexed query
      results survive the snapshot/reopen lifecycle.
    - [x] Add a bounded marker-damage plus query-index rebuild e2e: it writes
      a segmented namespace, flushes the public indexed query sidecar, forces
      snapshot compaction and cleanup, removes `index/query.index`, injects a
      damaged peer writer marker, reopens through `pouch://`, flushes the
      index, and proves indexed query results rebuild from the compacted
      namespace lifecycle.
  - [x] Add a separate Go/cgo benchmark module under `benchmark/` for opt-in
    e2e perf comparison and stress testing outside the liblockdc release gate.
  - [x] Launch a real latest pinned `pkt.systems/lockd` disk backend from the
    Go comparison module instead of using in-process lockd internals.
    Verified with `make __benchmark-pouch-go-fast POUCH_GO_FAST_SEED_ROWS=4
    POUCH_GO_FAST_BENCHTIME=1x POUCH_GO_FAST_TIMEOUT=45s`: the Go/cgo module
    now starts `.cache/go/bin/lockd` with a disk backend under an
    automatically cleaned `/tmp/liblockdc-lockd-disk-bench-*` root.
  - [x] Compare that real lockd disk server against an actual liblockdc
    `pouch://` client instance with the same full-form LQL selector shape.
    - [x] Add the first fast comparison cases for sparse equality, `/tags[]`
      array membership, and recursive exists against real lockd disk and
      actual liblockdc `pouch://` clients.
    - [x] Add `lc_query_req.selector_lql` for public C full-form LQL query
      expressions and switch the pouch Go/cgo benchmark helper to use the same
      full-form LQL strings as the lockd disk client side, while preserving
      `selector_json` as an explicit AST-JSON input for existing callers.
  - [ ] Mirror more of the Go lockd disk benchmark suite shape in the Go/cgo
    module so pouch and Go disk backend results can be compared case by case.
    - [x] Add `BenchmarkMediumLockdDiskKeys` and
      `BenchmarkMediumLockdDiskDocuments` with the same document-count,
      engine, and scenario environment controls as the pouch medium matrix.
    - [x] Add Go/cgo pouch-vs-lockd disk comparison coverage for the same
      `DateAfter` residual-filter scenario as the native C benchmark: seeded
      documents include valid, invalid, and out-of-range `/created_at` values,
      while both backends run `date{field=/created_at,after=...}` through the
      normal document and key-return benchmark matrix.
      Pouch now accepts indexed full-form date selectors by using the
      `/created_at` presence index as the candidate source and the liblql
      selector as the final filter; the bounded medium matrix includes
      `DateAfter` for both pouch and real lockd disk comparisons.
    - [x] Add `OrSparseOrFlag` to the default fast, medium, and acceptance
      Go/cgo comparison scenario sets so the real lockd disk backend and
      liblockdc pouch are compared case by case for public full-form root OR
      queries over the shared seeded document shape.
    - [x] Add Go/cgo pouch-vs-lockd disk comparison coverage for the same
      case-insensitive `/tags[]` text selectors as the native C benchmark:
      `IprefixTags` runs `iprefix{field=/tags[],value=FIN}` and
      `IcontainsTags` runs `icontains{field=/tags[],value=INA}` through both
      the liblockdc pouch C helper and the real Go lockd disk client. The
      bounded medium matrix now includes these scenarios while the 4096-doc
      acceptance matrix remains unchanged to preserve the explicit 3-minute
      cap until the expanded text acceptance run is re-measured.
      Verified on 2026-07-27 with `make benchmark-pouch-go-medium`; the
      expanded 64/1024-row medium matrix completed in 19s under the 3-minute
      cap after the lockd disk harness explicitly enabled namespace scan
      fallback. Current evidence still shows pouch trailing Go lockd disk on
      1024-row indexed `IprefixTags` / `IcontainsTags` and materially trailing
      the Go scan adapter on scan-path text predicates.
      Follow-up on 2026-07-27 after typed string postings and exact
      prefix/contains candidates: focused
      `Medium(LQL|LockdDisk)(Documents|Keys)/Docs1024/index/(IprefixTags|IcontainsTags)`
      completed successfully. Pouch key timings improved materially
      (`IprefixTags` about 9.0 ms C-side, `IcontainsTags` about 7.6 ms
      C-side). Pouch `IprefixTags` documents was roughly tied with Go in that
      sample, while `IcontainsTags` documents still trailed Go lockd disk.
      The full `make benchmark-pouch-go-medium` matrix still completed in 19s
      under the 3-minute cap; in that run 1024-row pouch indexed keys measured
      about 8.1 ms C-side for `IprefixTags` and 7.5 ms C-side for
      `IcontainsTags`, while Go lockd disk reported about 0.66 ms and
      0.63 ms respectively, so text postings still need a more compact
      reader/storage path.
  - [x] Keep pouch timing on the C side and report C-measured operation time
    through Go benchmarks so cgo bridge overhead is excluded.
    Verified on 2026-07-26 with the focused 4096-doc indexed key `InTags`
    acceptance run: the Go benchmark reported pouch `c-ns/op`,
    `page1-c-ns/op`, and `pageN-c-ns/op` metrics from the live C benchmark
    helper while the paired Go lockd disk case reported Go-client page timing.
- [ ] Expand fuzz corpora as new stream parsers or local mutate forms are
  introduced.
  - [ ] Add pouch fuzz targets/corpora for segmented manifest repair, snapshot
    lifecycle replay, marker recovery, query index/search replay, and strict
    JSON Pointer LQL selector planning.
    - [x] Add a `lc_fuzz_pouch_lql_plan` target with seed selectors for
      match-all, strict equality, `/tags[]` array membership, and compound
      range/presence/text planning; the harness runs accepted selectors through
      both pouch index and scan `query_keys` paths and asserts row-count parity.
    - [x] Extend the pouch LQL planning corpus with date residual filtering,
      recursive `exists`, and negated typed equality seeds; the fuzz fixture now
      includes valid, invalid, and out-of-range dates plus a false boolean flag
      so index-vs-scan parity covers candidate-superset planner paths.
    - [x] Extend the pouch LQL planning fuzz harness to close and reopen the
      seeded store after damaging namespace manifests, peer marker files, and
      the durable `query.index` sidecar, covering missing, corrupt, and
      future-version query-index replay/repair modes while asserting
      scan/index row-count parity against the same selector corpus.
    - [x] Extend the same fuzz harness to force a namespace snapshot before
      damage injection, then remove, append garbage to, or replace that
      snapshot before reopening scan and index clients so snapshot lifecycle
      replay/repair stays covered by scan/index parity checks.
    - [x] Extend the separate pouch lifecycle fuzz target with post-maintenance
      marker and query-index damage injection across the state/object/queue
      namespace plus the retention namespace, and register a dedicated damage
      smoke seed so full-surface lifecycle reopen still proves survivor,
      attachment, queue, retention, and query-index repair behavior.
    - [x] Extend the pouch LQL planning fuzz harness to exercise the public
      full-form `selector_lql` path as well as AST `selector_json`: inputs
      prefixed with `lql:` now route through `selector_lql`, and seed corpus
      entries cover strict equality, `/tags[]` array membership, date residual
      filtering, recursive `exists`, and compound range/text selectors.
    - [x] Add a dedicated `lc_fuzz_pouch_lifecycle` target for cross-surface
      pouch lifecycle smoke fuzzing: the harness seeds state, attachment,
      queue, and retention namespaces through the public client API, forces
      pouch maintenance/compaction and optional cleanup, reopens the store,
      verifies survivor query/object/queue behavior, verifies namespace-wide
      retention deletion in a separate namespace, and uses the shared
      `/tmp/liblockdc-*` tracking helper for automatic cleanup.
    - [x] Wire the pouch LQL-planning and lifecycle fuzz targets into
      `scripts/fuzz.sh` so `make fuzz-smoke` / release fuzzing exercises the
      pouch fuzz corpora instead of only registering CTest smokes.
