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

Latest release targets confirmed on 2026-07-22:

- `lonejson v0.42.0`
- `libpslog v0.9.0`
- `c.pkt.systems v0.9.0`
- `liblql v0.1.0`

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
- [x] Add `liblql v0.1.0` as a first-class lifecycle dependency from
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

- [x] Route pouch query selector classification through `liblql` parse/build
  and AST inspection instead of project-local selector token parsing, while
  preserving the existing legacy JSON selector compatibility surface.
- [ ] Replace the current pouch indexed-query placeholder/parser boundary with
  `liblql` for LQL parsing/evaluation; do not add project-local query parser or
  expression evaluator code.
  - [ ] Resolve the `liblql v0.1.0` evaluator gap before enabling general
    pouch field predicates: both `stream_apply_spooled` and
    `filter_file_spooled` return `LQL_STATUS_UNSUPPORTED` with
    `direct stream selector is not implemented by scanner` for a parsed JSON
    AST selector such as `{"eq":{"field":"value","value":"alpha"}}`.
- [ ] Keep pouch storage-owned indexes behind the pouch store boundary; use
  `liblql` only for query language semantics and predicate/evaluator behavior.
- [ ] Add observable pouch tests for `liblql`-backed owner/key selectors,
  pagination, hidden metadata filtering, removed-candidate skipping, malformed
  LQL diagnostics, oversized query limits, and scan/index fallback parity.
  - [x] Cover `liblql` compound key+owner selectors for scan and index
    document queries plus `query_keys`.
  - [x] Cover malformed `liblql` key/owner selector diagnostics for scan and
    index document queries plus `query_keys`.
  - [x] Cover removed-candidate skipping for index document queries plus
    `query_keys`.
  - [x] Cover hidden metadata filtering for scan and index document queries
    plus `query_keys`.
  - [x] Cover signed oversized query limit rejection for scan and index
    document queries plus `query_keys`.
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
- [ ] Add concurrent acquisition stress coverage for the shared dependency
  archive cache lock path.
- [ ] Align Linux compiler resolution with the updated Bootlin toolchain policy:
  no host compiler/binutils fallback for Linux builds, complete pinned Bootlin
  collections per target, toolchain identity in dependency stamps, and target
  tool discovery from configured build state.
- [x] Add preset-contract tests for lifecycle-required preset presence, build
  and test preset mirrors, required cache variables, release `LOCKDC_DIST_DIR`
  defaults, `debug-lua`, `valgrind`, and fuzz compatibility visibility.
- [ ] Update CMake presets and toolchain files for the lifecycle-required
  Bootlin-backed `valgrind`, pinned AFL++ `fuzz`, release target matrix, and
  optional Darwin/osxcross behavior.
- [x] Replace sanitizer-as-primary hardening assumptions with the updated native
  Valgrind gate while preserving any existing useful ASan/UBSan coverage as
  compatibility or optional hardening.
- [ ] Align fuzzing with the updated pinned AFL++ GCC-plugin lifecycle for
  native x86_64 Linux only; ensure fuzz targets never rely on host Clang/GCC as
  the project compiler.
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
- [ ] Run lifecycle-alignment gates after command/toolchain/cache changes:
  preset contract tests, target-tool discovery tests, cache contract tests,
  `make build`, `make test`, and `make valgrind`.
- [ ] Run broader gates before declaring the dependency/lifecycle migration
  complete: `make test-all`, `make package-verify`, `make lua-test`, fuzz smoke
  when AFL++ is available, deterministic e2e when relevant, and release artifact
  privacy/relocatability scans.
- [ ] Inspect and classify generated state after verification; commit only the
  coherent dependency/lifecycle changes and leave generated caches untracked.

## Current release-readiness focus

- [x] Keep API examples aligned with the receiver-function public surface.
- [x] Keep the Lua rock dependency boundary aligned with the pinned
  `lonejson` release.
- [ ] Expand e2e coverage when new lockd server surfaces are added.
- [ ] Expand fuzz corpora as new stream parsers or local mutate forms are
  introduced.
