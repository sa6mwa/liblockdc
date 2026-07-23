# liblockdc TODO

This file tracks the real lockd HTTP surface from `../lockd/internal/httpapi/handler.go` and the Go client behavior from `../lockd/client/`.

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
  - [ ] Add storage-owned field/range posting indexes for low-match public LQL
    document predicates; indexed string/bool/null/numeric equality and
    top-level conjunctions over those scalar equality terms now use durable
    candidate postings, while range selector shapes still need storage-owned
    candidate sets instead of broad summary candidates.
    - [x] Add the first durable candidate-posting slice for full-form LQL
      exact equality selectors with strict JSON Pointer fields and
      string/bool/null values; final predicate acceptance remains owned by
      `liblql`.
    - [x] Extend document postings to numeric equality predicates with
      canonical numeric equality keys instead of string-token comparison.
    - [x] Extend document postings to numeric range predicates with canonical
      numeric comparison semantics for key-ordered candidate narrowing.
    - [ ] Add mixed LQL hint intersection for selectors that combine equality
      and numeric range children in the same `and` expression; today equality
      narrowing remains all-equality, while range narrowing still safely
      reduces candidates before final `liblql` evaluation.
    - [ ] Add true ordered range traversal if candidate volume requires it;
      current range narrowing preserves query key/cursor ordering by checking
      numeric field postings from the key-ordered summary scan.
    - [x] Extend candidate extraction beyond single `eq` selectors to safe
      top-level `and` conjunction/intersection forms over supported scalar
      equality postings; final predicate acceptance remains owned by `liblql`.
  - [ ] Cut pouch disk storage over to the unreleased fresh segmented
    per-namespace logstore format; no legacy `store.log` compatibility or
    import migration is required because pouch has not shipped.
    - [x] Create the per-namespace layout under
      `<root>/<namespace>/logstore/` with `manifest/`, `segments/`,
      `snapshots/`, `markers/`, and queue notification directories, while
      keeping shared lock/backend identity paths explicit.
      - [x] Establish the initial active-segment scaffold and manifest-open
        record during namespace writes; root-level `store.log` remains the
        temporary write target until the active segment write cutover lands.
    - [ ] Implement manifest append/replay for segment open, segment seal,
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
    - [ ] Implement manifest repair for missing manifests, manifestless
      segments, crash-incomplete manifests, and legacy open-only manifests
      produced during the new segmented development path.
      - [x] Repair missing or empty namespace manifests from active segment
        directory scans during replay/open.
      - [x] Repair non-empty manifests by appending `open` for existing segment
        files that no manifest lifecycle record has mentioned yet, without
        resurrecting manifest-obsoleted files.
    - [ ] Move writes from the root-level `store.log` to active namespace
      segments, including active segment creation, sealing thresholds, fsync
      boundaries, and writer marker refresh.
      - [x] Add explicit payload file identity to live state, object, and queue
        refs so read sources are no longer hard-wired to `store.log` and can
        follow namespace segment paths during the write cutover.
      - [x] Append ordinary memory and fd-backed records to the namespace
        active segment as a byte-identical shadow while root `store.log`
        remains the temporary authoritative replay source.
      - [x] Move custom object-copy and queue-fd append paths to the namespace
        active segment shadow before segment replay becomes authoritative.
      - [x] Prefer active namespace segment replay over the legacy root log
        when segments exist; keep `store.log` only as a transitional refresh
        signal until the write cutover removes it.
      - [x] Rotate active namespace segments by size threshold and route all
        segment-shadow append paths through the shared active-segment opener.
      - [x] Resolve active namespace append paths from manifest state after
        snapshot compaction, opening the next segment generation when no active
        tail remains.
      - [x] Store live state, object, and queue payload refs from namespace
        segment append locations instead of root `store.log` offsets, while
        root writes remain a temporary refresh/compatibility signal.
      - [x] Refresh independent handles from authoritative namespace segment
        generation when segments exist, instead of depending on root
        `store.log` size changes.
      - [x] Stop appending ordinary memory-backed records (metadata puts and
        removes, state removes/links, object removes, queue metadata records)
        to root `store.log`; append them only to namespace segments.
      - [x] Stop appending fd-backed state/object/queue payload records to
        root `store.log`; namespace segments are now the only record
        destination for those append paths.
    - [ ] Replay installed snapshots plus non-obsolete segment tails in
      deterministic order, reset replay projections after snapshot or obsolete
      set changes, and preserve corrupt-tail truncation semantics per segment.
      - [x] Decouple record replay from `store->log_fd` / `store->log_path`
        by routing root replay through an explicit fd/path replay input; the
        namespace segment replay path now reuses this input wrapper.
      - [x] Replay active namespace segments in deterministic path order and
        keep root replay as a no-segment fallback only.
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
    - [ ] Implement snapshot compaction and cleanup: capture live refs,
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
    - [ ] Move durable query summary/posting sidecars into the segmented
      lifecycle so index rebuild, compaction, and crash recovery are tied to
      namespace log generations.
      - [x] Rebuild query summary and field-posting sidecars from live
        namespace segment/snapshot body refs when the root sidecar is missing,
        including closed-store missing-sidecar recovery with no usable
        `store.log`.
    - [ ] Add focused recovery tests for fresh segmented stores, reopen,
      corrupt tails, manifest repair, snapshot install, obsolete cleanup,
      state-link protection, and query/index rebuild from authoritative
      namespace history.
      - [x] Cover manifest-obsolete snapshot cleanup on reopen without losing
        the active installed snapshot body.
      - [x] Cover query sidecar field-posting rebuild from authoritative
        namespace segment bodies after removing `query.index` and truncating
        the legacy root log.
    - [ ] Add performance benchmarks that compare segmented pouch against the
      Go lockd disk backend on large data and indexed low-match LQL workloads
      after segmented correctness tests are stable.
- [ ] Expand e2e coverage when new lockd server surfaces are added.
- [ ] Expand fuzz corpora as new stream parsers or local mutate forms are
  introduced.
