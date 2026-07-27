# liblockdc TODO

This file tracks the real lockd HTTP surface from `../lockd/internal/httpapi/handler.go` and the Go client behavior from `../lockd/client/`.

## Pouch redesign

- [x] Move the unreleased old pouch implementation, tests, and benchmark
  harnesses under `deprecated/pouch-legacy/` as reference-only material.
- [x] Remove the old pouch store, allocator, logstore, index, integration
  tests, e2e shard, and benchmark matrix from live compilation and CTest
  registration.
- [x] Create the new pouch-native `lc_pouch` handle and module boundaries for
  root layout, path escaping, per-namespace segmented layout initialization,
  and client endpoint construction.
- [x] Replace old source/test boundary checks with redesign checks that prove
  retired files are not live and storage modules stay independent of `liblql`.
- [x] Add first executable redesign coverage for root manifest creation,
  per-namespace `segments`/`snapshots`/`markers`/`index` directories, escaped
  namespace paths, `pouch://` client open, and benchmark temp cleanup.
- [ ] Rebuild the pouch storage write/read path on the new architecture:
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
    release acknowledgement without reviving legacy compatibility paths.
  - [x] Route pouch client-level `mutate`, bound lease `mutate`, and bound
    lease `mutate_local` through the redesigned state path by applying the
    shared mutation planner to current pouch state and persisting the result
    through segmented state writes or staged lease updates with CAS coverage.
  - [x] Add the first redesigned pouch object surface by routing client and
    bound-lease attachment upload/list/get/delete/delete-all through durable
    internal attachment records in the segmented state path, with selector by
    name or deterministic pouch attachment id and overwrite/max-size coverage.
  - [x] Add the first redesigned pouch queue surface: durable enqueue records,
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
- [ ] Rebuild per-namespace manifest/snapshot lifecycle, manifest repair,
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
  - [ ] Extend snapshot compaction to the full storage surface with durable
    validation-drift abort diagnostics and expanded compaction diagnostics.
    - [x] Report foreground maintenance compaction aborts through the
      maintenance result without hiding the original error: candidate read,
      snapshot refresh/prepare/write/install, and obsolete cleanup stages now
      set `aborted` plus a stage-specific diagnostic.
- [ ] Rebuild the typed metadata index and `liblql`-backed public query/search
  engine against the new storage model, without fallback to deprecated code.
  - [x] Add the first redesigned pouch `query_keys` scan path over the
    segmented state projection, with selector parsing/evaluation owned by
    `liblql`, `query_hidden` filtering, staged-key exclusion, pagination, and
    scan metadata.
  - [x] Add public document `query` row streaming over the redesigned state
    model in scan mode, with `liblql` selector evaluation, `query_hidden`
    filtering, staged-key exclusion, pagination, and scan metadata.
  - [x] Add the first pouch `flush_index` implementation over the redesigned
    state projection, returning a synchronous local high-water index token that
    includes tombstones and peer-writer marker refreshes.
  - [x] Add the first durable per-namespace query-index sidecar metadata file
    under `index/query.index`, with version validation, high-water persistence,
    and repair on missing, stale, or corrupt sidecars during `flush_index`.
  - [x] Extend `index/query.index` with deterministic live-row summary records
    rebuilt from the redesigned state projection, including row-count/hash
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
  - [ ] Build the durable typed index/postings path and make indexed mode the
    preferred query engine.
    - [x] Add pouch query-engine configuration to `lc_pouch_open` options and
      `pouch://` endpoint query parameters: `query_engine=index` is the
      default, `query_engine=scan` forces implicit scan routing, explicit
      request `engine` overrides configuration, and
      `query_fallback_engine=index` handles implicit `refresh=wait_for`
      requests from scan-preferred endpoints.
- [ ] Rebuild the Go lockd disk vs pouch benchmark/stress harness against the
  new pouch API and restore the comparison scenarios only when they measure the
  redesigned implementation.
  - [x] Restore the live `benchmark/` Go/cgo module expected by the
    `benchmark-pouch-go*` Makefile targets, with C-side pouch timing metrics
    and fast/medium benchmark names that compile against the redesigned
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
      `or` branches whose children are exact equality selectors; the disk
      backend de-duplicates unioned posting keys, restores stable cursor
      ordering, and final acceptance remains owned by `liblql`.
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
  - [ ] Redesign pouch indexed query execution for Go lockd disk-style
    performance parity instead of continuing monolithic storage posting
    tweaks.
    - [x] Cut over unreleased pouch API, implementation, tests, docs, and
      benchmark callers from the former disk-prefixed C naming to pouch-native
      `lc_pouch_*`/`src/lc_pouch.c`; Go lockd disk remains the reference
      implementation for storage/index ideas, not the identity of the embedded
      C backend.
    - [x] Split the first pouch-native storage subsystem boundary out of
      `src/lc_pouch.c`: `lc_pouch_logstore` now owns namespace path escaping,
      per-namespace logstore creation, manifest append, active segment
      selection, segment rollover, and segment/snapshot name parsing behind a
      small allocator/root/fsync context instead of depending on the concrete
      pouch backend object.
    - [x] Move active segmented replay input discovery into
      `lc_pouch_logstore`: manifest replay now selects authoritative
      segment/snapshot paths, repairs manifestless segment files, cleans
      obsolete manifest records under the configured grace policy, fingerprints
      active logstore generations, and counts compaction candidates outside the
      monolithic backend adapter.
    - [x] Move compaction backup and snapshot path mechanics into
      `lc_pouch_logstore`: compact backup naming, backup restore/delete
      cleanup, backup-backed compact body opens, and next snapshot path
      selection now sit with the segmented logstore owner.
    - [ ] Split the pouch search/index subsystem out of `lc_pouch.c` into
      an internal C index layer with explicit reader, writer, planner, posting,
      visibility, and result-cache boundaries.
      - [x] Introduce the first private reader/planner boundary in
        `lc_pouch_index`: exact-term, field-presence, and range docID reader
        callbacks let the index layer own primary equality, `in` union, exact
        `exists`, and numeric range planning while the pouch backend adapts the
        current sidecar postings.
      - [x] Move the first result-cache/page orchestration boundary into
        `lc_pouch_index`: a generation-scoped cached docID page helper now owns
        normalized plan key lookup, cache miss collection, cache insert, and
        doc-table cursor paging; the pouch equality bridge supplies sidecar
        readers and only translates the selected docID page back to summary
        indices.
      - [x] Route simple positive `range` and `exists` result-cache/page
        orchestration through the same index-owned cached docID page helper;
        the pouch storage bridge now only adapts sidecar range/presence readers and
        translates selected docID pages to summaries for these predicates.
      - [x] Route simple positive `prefix` and `contains` result-cache/page
        orchestration through the same index-owned cached docID page helper;
        the pouch storage bridge now only adapts sidecar text/trigram readers and
        translates selected docID pages to summaries for these predicates.
      - [x] Route indexed `DateAfter` result-cache/page orchestration through
        the same index-owned cached docID page helper; the pouch storage bridge now
        adapts the authoritative temporal generation reader and translates the
        selected docID page back to summaries or key snapshots.
      - [x] Move remapped document-generation result-cache/page orchestration
        into `lc_pouch_index_result`: disk now reads/publishes the
        identity-matched `.lcpdtg` and supplies a global-to-local docID remap
        callback, while the index layer owns miss collection, local docID cache
        insertion, and cursor page selection.
      - [x] Physically split result-cache/page planning into
        `src/lc_pouch_index_result.c`, leaving `lc_pouch_index.c` focused on
        document tables, docID algebra, term dictionaries, and adaptive
        postings while preserving the same private `lc_pouch_index.h`
        boundary.
      - [x] Physically split adaptive sparse/dense posting encoding into
        `src/lc_pouch_index_posting.c`, keeping the private posting boundary
        separate from document-table, term-dictionary, and result-cache code.
      - [x] Physically split term dictionaries, term-ID posting tables, and
        prepared-term cache lifecycle into `src/lc_pouch_index_terms.c`, so
        the private index layer has distinct document, posting, term, and
        result-cache modules.
      - [x] Physically split document table and docID set algebra into
        `src/lc_pouch_index_doc.c`, leaving `lc_pouch_index.c` focused on
        planner/collector orchestration over the private index primitives.
      - [x] Document the current private index module map in
        `docs/pouch-storage.md`, including the pouch storage bridge responsibility that
        remains in `src/lc_pouch.c`.
    - [ ] Replace repeated key-string posting algebra with stable per-index
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
        from the pouch storage bridge: full query-index rebuilds and compaction publish
        `.lcpdtg` files under the backend logstore with client coverage
        decoding default and non-default namespace artifacts.
      - [x] Cut exact-term generation files over to namespace-local docIDs:
        exact generation build stores postings by the matching per-namespace
        document-table generation, prepared exact readers require an
        identity-matched `.lcpdtg`, remap local docIDs back into the current
        global in-memory doc table, and repair corrupt/missing doc generation
        files through the exact query path.
      - [x] Cut field-presence generation files over to namespace-local docIDs:
        exists generation build now stores postings by the matching
        per-namespace document-table generation, prepared exists readers
        require and remap through identity-matched `.lcpdtg` files, and corrupt
        doc generation files repair through the exists query path.
      - [x] Cut numeric-range generation files over to namespace-local docIDs:
        numeric generation build stores value postings by the matching
        per-namespace document-table generation, prepared range readers require
        an identity-matched `.lcpdtg`, remap materialized range docIDs back into
        the current global in-memory doc table, and repair corrupt doc
        generation files through the range query path.
      - [x] Cut text/trigram generation files over to namespace-local docIDs:
        text generation build stores raw text postings by the matching
        per-namespace document-table generation, prepared prefix/contains
        readers require an identity-matched `.lcpdtg`, remap materialized text
        docIDs back into the current global in-memory doc table, and repair
        corrupt doc generation files through the text query path.
      - [x] Cut temporal generation files over to namespace-local docIDs:
        temporal generation build stores normalized-date and residual postings
        by the matching per-namespace document-table generation, DateAfter
        readers require an identity-matched `.lcpdtg`, remap materialized
        temporal docIDs back into the current global in-memory doc table, and
        repair corrupt doc generation files through the temporal query path.
        Result-page consumption also stores/pages namespace-local docIDs
        through the matching document-table generation.
      - [x] Wire the disk query bridge through the index document table for
        field-predicate candidate docIDs: summary refresh populates
        namespace/key docIDs, candidate readers append table docIDs, and result
        conversion resolves docIDs back through table lookup before summary
        access.
      - [x] Add private C docID set primitives and wire exact primary `in`
        candidate collection through docID accumulation while preserving
        secondary-filter and `liblql` predicate authority.
      - [x] Add an internal docID scratch-buffer primitive and wire equality
        intersection/subtraction collectors through it, so multi-term query
        algebra reuses allocator-owned temporary buffers instead of allocating
        fresh merge sets for every positive or negative equality term.
      - [x] Route primary equality candidate collection through the same
        docID reader/planner boundary and remove the obsolete disk-local
        equality candidate helper.
      - [x] Route exact positive `exists` candidate collection through a
        field-presence docID reader callback while preserving the existing
        posting-summary freshness check in the disk adapter.
      - [x] Route primary numeric range candidate collection through a range
        docID reader callback and remove the obsolete disk-local equality-span
        range optimization helpers.
      - [x] Move positive equality filtering for primary numeric range
        candidates into the private index collector: the disk adapter now
        supplies range and exact docID readers, while `lc_pouch_index`
        intersects the primary range docID set with secondary equality
        postings before result-cache paging.
      - [x] Move positive equality filtering for primary `prefix` and
        `contains` candidates into private index collectors as well: disk now
        adapts text/trigram and exact-term readers, while `lc_pouch_index`
        performs sorted docID intersections before page selection.
      - [x] Move positive equality filtering for primary non-wildcard `in`
        candidates into a private index collector: disk adapts exact-term
        readers for the `in` value set and equality filters, while
        `lc_pouch_index` performs sorted docID intersections before
        result-cache page selection.
      - [x] Move negative equality filtering for primary equality candidates
        into a private index collector: disk adapts exact-term readers while
        `lc_pouch_index` subtracts sorted not-eq docID sets from the primary
        equality candidate set before result-cache page selection.
      - [x] Move negative equality filtering for primary non-wildcard `in`
        candidates into the same private index collector path: disk adapts
        exact-term readers while `lc_pouch_index` unions `in` values,
        intersects optional positive equality filters, and subtracts sorted
        not-eq docID sets before result-cache page selection.
      - [x] Move negative equality filtering for primary positive `exists`
        candidates into the same private index collector path: disk adapts
        field-presence and exact-term readers while `lc_pouch_index`
        intersects optional positive equality filters and subtracts sorted
        not-eq docID sets before result-cache page selection.
      - [x] Move negative equality filtering for primary numeric `range`,
        `prefix`, and `contains` candidates into the same private index
        collector path: disk adapts primary candidate and exact-term readers
        while `lc_pouch_index` intersects optional positive equality filters
        and subtracts sorted not-eq docID sets before result-cache page
        selection.
    - [ ] Add adaptive posting encodings for dense and sparse terms: sparse
      delta-varint docID streams and dense bitsets selected by posting
      density/encoded size.
      - [x] Add private sparse delta-varint and dense bitset posting primitives
        with decode/intersect unit coverage; query execution still needs to
        use persisted compiled postings instead of the current sidecar rows.
    - [ ] Add compiled field dictionaries with term IDs, doc tables, numeric
      range term tables, and text/trigram term tables so equality, range,
      `in`, prefix, contains, and exists can evaluate without repeated string
      scans.
      - [x] Add the first private term dictionary primitive in
        `lc_pouch_index`: `(field,value)` terms are interned into stable term
        IDs with sorted lookup and duplicate preservation coverage. Disk
        readers still need to build and consume compiled dictionaries.
      - [x] Add the first term-ID posting table primitive in `lc_pouch_index`:
        exact terms now map to adaptive sparse/dense docID postings with
        binary lookup, missing-term, and replacement coverage. Disk readers
        still need to compile sidecar postings into this table.
      - [x] Add the first immutable exact-term generation codec in
        `lc_pouch_index`: namespace-scoped term dictionaries and adaptive
        term-ID postings now round-trip under index sequence plus segmented
        manifest identity, with corruption/truncation rejection coverage.
      - [x] Publish and consume immutable exact-term generation files from the
        pouch storage bridge: full query-index rebuilds and compaction publish
        per-namespace exact generations, prepared equality/`in` readers merge
        identity-matched files before compiling sidecar fallbacks, and
        missing/stale/corrupt files repair on the exact query path.
      - [x] Publish and consume immutable field-presence generation files from
        the pouch storage bridge: full query-index rebuilds and compaction publish
        per-namespace exists generations, prepared `exists` readers merge
        identity-matched files before compiling sidecar fallbacks, and
        missing/stale/corrupt files repair on the exists query path.
      - [x] Add the first immutable numeric-range generation codec in
        `lc_pouch_index`: namespace-scoped field dictionaries store sorted
        canonical `n:` values plus residual docID postings under index sequence
        and segmented manifest identity, with range-bound lookup and
        corruption/truncation rejection coverage.
      - [x] Publish and consume immutable numeric-range generation files from
        the pouch storage bridge for simple primary `range` plans: full query-index
        rebuilds and compaction publish per-namespace numeric generations,
        prepared simple `range` readers materialize identity-matched files into
        query-bound adaptive postings, and missing/stale/corrupt files repair
        on the simple range query path. Compound range paths still use the
        sidecar compiler so secondary predicate filtering remains explicit.
      - [x] Add the first immutable text/trigram generation codec in
        `lc_pouch_index`: namespace-scoped field dictionaries store raw text
        docID vectors, rebuild lowercase ASCII `g:` trigram term postings on
        build/decode, preserve existing prefix/contains case-folding semantics,
        and round-trip under index sequence plus segmented manifest identity
        with corruption/truncation rejection coverage.
      - [x] Publish and consume immutable text/trigram generation files from
        the pouch storage bridge for simple primary `prefix` and `contains` plans: full
        query-index rebuilds and compaction publish per-namespace text
        generations, prepared simple text readers materialize identity-matched
        files into query-bound adaptive postings, and missing/stale/corrupt
        files repair on the simple text query path. Compound text paths still
        use the sidecar compiler so secondary predicate filtering remains
        explicit.
      - [x] Compile filtered exact sidecar candidates into a per-request
        term-ID posting table for equality and `in` docID readers, preserving
        existing live-state and secondary predicate guards while exercising
        adaptive postings in the disk query path.
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
        contains docID reader callback, preserving trigram candidate narrowing
        and final substring validation while compiling results into adaptive
        postings.
      - [x] For trigram-backed `contains` compilation, guard the field text
        posting pass with the sorted gram-candidate key set, so substring
        validation only runs on candidate keys while avoiding a second
        per-query text-ref sort.
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
    - [x] Add prepared-reader caching keyed by the immutable pouch index
      generation/manifest identity so repeated queries do not rebuild the same
      compiled index view.
      - [x] Add the first generation-scoped prepared exact-term cache in the
        pouch storage bridge: simple equality and non-wildcard `in` plans can reuse
        namespace-qualified adaptive exact postings across compatible query
        shapes, with tests covering namespace separation and generation miss
        after writes.
      - [x] Keep prepared term caches request-independent: owner- and
        key-filtered exact queries use the request-keyed result cache but do not
        populate or consume namespace/field/value prepared exact postings.
        Covered by a focused pouch regression that runs owner-filtered and
        key-filtered exact queries before unfiltered exact queries on the same
        indexed field/value generation.
      - [x] Extend the generation-scoped prepared bridge cache to simple
        positive `exists` plans: namespace-qualified field-presence postings
        are reused across compatible exists scans, with namespace separation
        and post-write generation miss coverage.
      - [x] Extend the generation-scoped prepared bridge cache to simple
        positive numeric `range` plans: namespace-qualified field/range
        postings are reused across compatible range scans, with namespace
        separation and post-write generation miss coverage.
      - [x] Extend the generation-scoped prepared bridge cache to simple
        positive `prefix` plans: namespace-qualified field/text-prefix
        postings are reused across compatible prefix scans, with namespace
        separation and post-write generation miss coverage.
      - [x] Extend the generation-scoped prepared bridge cache to simple
        positive `contains` plans: namespace-qualified field/substring
        postings are reused after trigram narrowing and final substring
        validation, with namespace separation and post-write generation miss
        coverage.
      - [x] Move the shared prepared-term cache lifecycle into
        `lc_pouch_index`: disk readers still populate namespace-qualified
        bridge postings from sidecars, but generation refresh and cleanup now
        use one index-owned cache primitive instead of per-predicate disk-local
        structs.
      - [x] Introduce an explicit private index identity for prepared-term
        caches: the pouch storage bridge now refreshes prepared readers by index
        sequence plus segmented manifest generation instead of a bare sequence
        counter, while the compiled reader contents still come from current
        sidecar scans.
      - [x] Extend prepared-reader caching to typed temporal `DateAfter`
        generations: the pouch storage bridge loads identity-matched per-namespace
        temporal generation files into `lc_pouch_index_prepared_temporal_cache`,
        remaps namespace-local docIDs into the current global doc table once,
        and reuses the compiled temporal table across distinct DateAfter plan
        keys until the index identity changes.
    - [ ] Add sorted matched-key result caching keyed by index generation plus
      normalized selector plan, so multi-page queries reuse the full matching
      key vector instead of recomputing candidates for every page.
      - [x] Add the internal generation + normalized-plan result cache
        primitive in `lc_pouch_index`; it stores sorted docID vectors and
        misses across generation changes. Disk query plans still need to
        provide stable normalized plan keys and use it.
      - [x] Move the result-cache primitive onto the same private index
        identity used by prepared readers: disk result-cache lookups now use
        index sequence plus segmented manifest generation, with generation-only
        wrappers kept for current private tests and bridge compatibility.
      - [x] Wire the result cache into simple primary equality scans using
        the current index sequence plus a length-prefixed normalized equality
        plan key, with tests covering repeated hits and post-write generation
        misses. Broader selector normalization is still pending.
      - [x] Wire the same result cache into simple positive `exists` scans
        using a length-prefixed normalized field-presence plan key, with
        repeated-hit and post-write generation-miss coverage.
      - [x] Wire the result cache into simple non-wildcard positive `in`
        scans using a normalized key with sorted/deduplicated typed values,
        with duplicate-value and post-write generation-miss coverage.
      - [x] Add simple non-wildcard `in` key pagination coverage after
        duplicate-value de-duplication, proving `limit`, `next_start_after`,
        and resumed key-only scans preserve stable key order.
      - [x] Add disk-level result-cache status coverage for simple
        non-wildcard `in` key pagination, proving the first page after an
        index-generation change populates the normalized docID result cache and
        the resumed cursor page reuses it.
      - [x] Wire the result cache into simple positive numeric `range` scans
        using a length-prefixed bound-aware plan key, with post-write
        generation-miss coverage.
      - [x] Wire the result cache into simple positive `prefix` and
        `contains` scans using length-prefixed text plan keys that include the
        case-sensitivity flag; contains coverage includes a post-write
        generation miss.
      - [x] Wire indexed `DateAfter` scans through the same result-cache/page
        helper using a length-prefixed temporal plan key; focused coverage
        proves first-page population and resumed key/document pages reuse the
        cached candidate vector while final `liblql` acceptance still owns
        visible results.
      - [x] Cut indexed `DateAfter` result-cache/page consumption over to
        namespace-local document-table generations: disk converts collected
        global candidate docIDs into the identity-matched `.lcpdtg`, stores
        local docIDs in the normalized result cache, pages over that
        per-namespace document table, and only resolves selected page docIDs
        back to summaries or key snapshots after paging.
      - [x] Cut simple equality result-cache/page consumption over to
        namespace-local document-table generations for both document and key
        pages, with cross-namespace global/local docID divergence coverage.
      - [x] Cut simple `exists` result-cache/page consumption over to
        namespace-local document-table generations for both document and key
        pages, with cross-namespace global/local docID divergence coverage.
      - [x] Cut simple numeric `range` result-cache/page consumption over to
        namespace-local document-table generations for both document and key
        pages, with cross-namespace global/local docID divergence coverage.
      - [x] Cut simple non-wildcard `in` result-cache/page consumption over to
        namespace-local document-table generations for both document and key
        pages, with cross-namespace global/local docID divergence coverage.
      - [x] Cut simple `prefix` and `contains` result-cache/page consumption
        over to namespace-local document-table generations for both document
        and key pages, with cross-namespace global/local docID divergence
        coverage.
      - [x] Move simple result-cache plan key construction into
        `lc_pouch_index`: equality, exists, `in`, range, prefix, and contains
        cacheability/normalization now live with the planner cache boundary
        instead of the pouch backend.
      - [x] Extend normalized result-cache planning to primary numeric range
        scans with positive equality filters: compound range/equality plans
        now use sorted/deduplicated equality predicates in the plan key, reuse
        cached filtered result pages across scan/key-scan entry points, miss
        across generation changes, and avoid unsafe broad prepared-range reuse
        when secondary equality filters are present.
      - [x] Extend normalized result-cache planning and routing to primary
        `prefix` and `contains` scans with positive equality filters: compound
        text/equality plans use the same sorted/deduplicated equality suffix
        in their plan keys and reuse cached filtered pages across document and
        key scans until the index generation advances.
      - [x] Extend normalized result-cache planning and routing to primary
        positive `exists` scans with positive equality filters: compound
        exists/equality plans use the same sorted/deduplicated equality suffix,
        intersect field-presence docIDs with exact-term docIDs in
        `lc_pouch_index`, and reuse cached filtered pages across document and
        key scans until the index generation advances.
      - [x] Extend normalized result-cache planning and routing to primary
        non-wildcard positive `in` scans with positive equality filters:
        compound in/equality plans append the same sorted/deduplicated
        equality suffix after the normalized sorted/deduplicated `in` values,
        intersect exact-term docID sets in `lc_pouch_index`, and reuse cached
        filtered pages across document and key scans until the index
        generation advances.
      - [x] Extend normalized result-cache planning to primary equality scans
        with negative equality filters: equality/not-equality plans append a
        sorted/deduplicated `not_eq` suffix, subtract the negative exact-term
        docID sets in `lc_pouch_index`, and reuse cached filtered pages across
        document and key scans until the index generation advances.
      - [x] Extend normalized result-cache planning to primary non-wildcard
        `in` scans with negative equality filters: in/not-equality plans append
        a sorted/deduplicated `not_eq` suffix after normalized `in` values and
        any positive equality suffix, subtract the negative exact-term docID
        sets in `lc_pouch_index`, and reuse cached filtered pages across
        document and key scans until the index generation advances.
      - [x] Extend normalized result-cache planning to primary positive
        `exists` scans with negative equality filters: exists/not-equality
        plans append a sorted/deduplicated `not_eq` suffix after the
        field-presence key and any positive equality suffix, subtract the
        negative exact-term docID sets in `lc_pouch_index`, and reuse cached
        filtered pages across document and key scans until the index
        generation advances.
      - [x] Extend normalized result-cache planning to primary numeric
        `range`, `prefix`, and `contains` scans with negative equality filters:
        these plans append a sorted/deduplicated `not_eq` suffix after their
        primary selector key and any positive equality suffix, subtract the
        negative exact-term docID sets in `lc_pouch_index`, and reuse cached
        filtered pages across document and key scans until the index
        generation advances.
      - [x] Extend normalized result-cache planning to indexed `DateAfter`
        scans with positive and negative equality filters: DateAfter plans now
        append sorted/deduplicated `eq` and `not_eq` suffixes, allow only the
        parser's same-field implied `exists` guard, reject unsupported
        secondary predicate families, and keep filtered temporal pages from
        sharing broader cached candidate vectors.
      - [x] Add index-owned docID result paging over the document table and
        route the equality document/key scans through it, so cached equality
        result pages translate only the selected page of docIDs back through
        disk summaries instead of translating the full match vector before
        cursor/limit handling.
      - [x] Route simple positive `exists` document/key scans through the same
        index-owned docID result paging bridge, sharing invalid-docID detection
        and avoiding full cached-match summary translation before cursor/limit
        handling.
      - [x] Route simple positive numeric `range` document/key scans through
        index-owned docID result paging, so cached range result pages translate
        only selected docIDs back through disk summaries before streaming.
      - [x] Route simple non-wildcard positive `in` document/key scans through
        index-owned docID result paging, while leaving wildcard `in` on the
        existing disk-side pagination path.
      - [x] Route simple positive `prefix` and `contains` document/key scans
        through index-owned docID result paging, so text predicate pages no
        longer translate the full cached match vector through disk summaries
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
      - [x] Add index-owned posting append decode and route disk exact,
        exists, range, prefix, and contains readers through it, so cached
        adaptive postings append into the caller's docID set without allocating
        a temporary decoded set and copying it item-by-item.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `InTags` measured about 7.04 ms and `ContainsMessage` about 3.22
        ms; Go disk measured about 47.9 ms and 1.19 ms respectively in the
        same run, so contains key-return remains a pouch performance gap.
      - [x] Match the Go lockd disk contains execution order more closely:
        pouch now checks raw text postings for the substring before consulting
        the trigram candidate key set, avoiding candidate-membership searches
        for text values that cannot match.
        Verified on 2026-07-26 with focused 4096-doc indexed key benchmarks:
        pouch `ContainsMessage` key-return measured about 2.71 ms. The matching
        Go disk run measured about 47.5 ms and was noisy relative to earlier
        runs, so this records pouch improvement without treating the broader
        contains parity gap as closed.
      - [x] Remove redundant compiled `contains` candidate-key materialization:
        after the raw text posting substring check became authoritative, the
        trigram key set is only used for zero-candidate rejection and no longer
        allocates borrowed key arrays or performs per-match binary searches.
        Verified on 2026-07-26 with the focused 4096-doc
        `ContainsMessage` indexed key benchmark: pouch measured about 2.75 ms;
        the matching Go disk run measured about 47.4 ms.
    - [ ] Preserve final `liblql` predicate authority by treating indexed
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
        generations instead of reparsing string field postings in the disk
        bridge, so the final reader-cache architecture can avoid the remaining
        DateAfter document-return cost.
        - [x] Add the index-owned temporal primitive and collector API:
          normalized temporal values are stored as per-field sorted docID
          vectors with residual postings for plausible unsupported temporal
          strings, and `lc_pouch_index_collect_date_after_doc_ids` owns bound
          parsing plus sorted result normalization.
        - [x] Add a deterministic private temporal table codec for immutable
          reader files. The codec stores sorted per-field normalized temporal
          docIDs plus residual docIDs with magic/version checks, round-trip
          coverage, and corruption/truncation rejection.
        - [x] Add a file-level temporal generation container codec carrying
          index identity, namespace, and the temporal table payload so disk can
          persist immutable per-namespace DateAfter reader files without
          rebuilding the table on the query hot path.
        - [x] Publish deterministic per-namespace temporal generation files
          under the backend logstore during controlled query-index rebuilds.
          The rebuild pass now compiles live sidecar string postings into the
          generation container and installs the file atomically, with client
          coverage decoding the persisted artifact after reopen/rebuild.
        - [x] Teach the DateAfter reader to consume identity-matched
          per-namespace temporal generation files and then run the resulting
          docIDs through the existing live-state, owner, hidden, key, and
          secondary-predicate guards. Stale or absent generation files remain
          ignored so incremental writes do not lose results before full
          generation publication exists on all update paths.
        - [x] Keep ordinary state writes/removes and metadata changes on the
          live query-index mutation path, and publish temporal generation files
          only from controlled rebuild/compaction or lazy DateAfter repair.
          Pouch regression coverage proves ordinary mutations no longer create
          `.lcptgn` files eagerly, while the first DateAfter read publishes the
          identity-matched generation before returning indexed results.
        - [x] Repair missing, stale, or corrupt temporal generation files on
          DateAfter reads by refreshing query-index replay, republishing the
          namespace generation, and rereading the identity-matched artifact
          before using the generation as the indexed DateAfter source.
        - [x] Make temporal generation files authoritative for indexed
          DateAfter and remove the remaining sidecar-scan bridge. If the
          repaired identity-matched generation cannot be read, indexed DateAfter
          returns the empty candidate set instead of reparsing field postings on
          the hot path. A query-time pouch storage bridge attempt was measured on
          2026-07-26 and rejected because building the temporal table on the
          hot path regressed 4096-document DateAfter page-one latency to
          roughly 110-140 ms C-side.
        - [x] Cache decoded/remapped temporal generations in the
          identity-scoped prepared temporal reader cache. Disk-level regression
          coverage now proves a second DateAfter bound in the same generation
          can use the prepared cache even after the persisted `.lcptgn` file is
          corrupted, avoiding per-query temporal generation decode/repair.
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
    - [ ] Re-run the expanded 4096+ document benchmark matrix and use it as the
      acceptance gate for the redesigned index path, with pouch expected to
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
        segmented redesign. Pouch regression coverage now proves updates before
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
        targets after the redesign boundaries are in place.
  - [x] Cut pouch storage over to the unreleased fresh segmented
    per-namespace logstore format; no legacy `store.log` compatibility or
    import migration is required because pouch has not shipped.
    - [x] Create the per-namespace layout under
      `<root>/<namespace>/logstore/` with `manifest/`, `segments/`,
      `snapshots/`, `markers/`, and queue notification directories, while
      keeping shared lock/backend identity paths explicit.
      - [x] Establish the initial active-segment scaffold and manifest-open
        record during namespace writes; the root-level `store.log` placeholder
        is not an authoritative record source.
      - [x] Move the per-namespace segmented logstore mechanics into
        `src/lc_pouch_logstore.c`, preserving the unreleased pouch layout while
        making namespace pathing, manifest append, active segment selection,
        and rollover testable without the monolithic backend adapter.
      - [x] Move active replay path collection, manifestless segment repair,
        obsolete path cleanup, active generation fingerprinting, and compaction
        candidate counting into `src/lc_pouch_logstore.c`.
      - [x] Move compact backup path handling, backup restore/delete cleanup,
        backup-backed compact body opens, and next snapshot path selection into
        `src/lc_pouch_logstore.c`.
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
        no namespace logstore segments exist, reset replay projections to an
        empty store instead of importing root `store.log`.
      - [x] Discover and replay every numbered `seg-*.log` file in each
        namespace, so sealed historical segments and the active tail are both
        authoritative.
      - [x] Truncate invalid/trailing bytes from namespace segment files during
        authoritative segment replay, matching root-log corrupt-tail semantics.
      - [x] Replay installed namespace snapshots before later active segment
        tails, and include snapshot files in logstore generation refresh
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
        during logstore collection/replay, while preserving active snapshot
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
    - [x] Add Go disk-style marker lifecycle for segmented pouch logstores.
      - [x] Clarify and implement root-level writer-presence markers for
        exclusive writer detection/fencing, separate from namespace logstore
        query/segment files.
      - [x] Implement per-namespace `<logstore>/markers/writer-*.marker`
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
        internal `.lockd` namespace logstore instead of the pouch root, while
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
- [ ] Expand e2e coverage when new lockd server surfaces are added.
  - [ ] Refine pouch e2e coverage around segmented manifest/snapshot
    lifecycle, manifest repair, background compaction scheduling, marker
    recovery, search/index rebuild, and large namespace stress scenarios.
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
    - [ ] Add Go/cgo pouch-vs-lockd disk comparison coverage for the same
      `DateAfter` residual-filter scenario as the native C benchmark: seeded
      documents include valid, invalid, and out-of-range `/created_at` values,
      while both backends run `date{field=/created_at,after=...}` through the
      normal document and key-return benchmark matrix.
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
