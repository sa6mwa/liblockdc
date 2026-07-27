# Pouch Cutover Audit

Audit timestamp: 2026-07-27T20:49:02Z.

This audit maps the active pouch implementation against `TODO.md` and
`docs/pouch-storage.md` before the single-slice cutover. It treats the current
worktree as authoritative and does not use test results as evidence because the
cutover implementation has not been completed.

## Current Branch State

- Branch: `feat/pouch`
- Upstream status: `feat/pouch...origin/feat/pouch [ahead 408]`
- Default branch: `origin/trunk`
- Dirty tracked files at audit start:
  `TODO.md`, `docs/pouch-storage.md`, `src/lc_core.c`, `src/lc_pouch.c`,
  `src/lc_pouch_client.c`, `src/lc_pouch_index.c`, `src/lc_pouch_index.h`,
  `src/lc_pouch_index_doc.c`, `src/lc_pouch_index_posting.c`,
  `src/lc_pouch_index_terms.c`, `src/lc_pouch_query_index.c`,
  `src/lc_pouch_state.c`, and `tests/unit/test_lc_pouch.c`.

Recent history confirms the active branch moved the former richer pouch
implementation under `deprecated/pouch-legacy/` in the backend skeleton
cutover commit `c771103`, then added 143 commits on top of that smaller active
tree.

## Decisive Findings

### 1. TODO/docs reference modules that are not live

`TODO.md` and `docs/pouch-storage.md` describe these modules as active or
target cutover boundaries:

- `src/lc_pouch_logstore.c`
- `src/lc_pouch_logstore.h`
- `src/lc_pouch_index_temporal.c`
- `src/lc_pouch_index_text.c`
- `src/lc_pouch_temporal.c`
- `src/lc_pouch_temporal.h`

The active `src/` tree does not contain those files. Matching files exist only
under `deprecated/pouch-legacy/src/`.

This was not just documentation drift at audit start. The active boundary test
treated `src/lc_pouch_logstore.c` as a retired source that must not exist,
while TODO/docs still used retired history terminology for the desired
pouch-native storage boundary. This slice reconciles the active docs/TODO to
the namespace-history layout and keeps the retired file names only in explicit
denylist/reference contexts.

### 2. The active implementation is not the documented storage engine

The active `lc_pouch` handle owns root path, open options, marker identity, and
state cache pointers. It does not currently own the full storage-engine state
described by `docs/pouch-storage.md`, including:

- backend vtable
- namespace history object
- append writer/commit group state
- pending record maps
- key lock cache
- read file descriptor cache
- prepared index caches
- result cache identity/lifecycle

The active state path stores line-oriented records in segment files:

- `S` state put
- `M` metadata update
- `D` tombstone
- `T` staged decision
- `L` staged link/promotion
- `H` high-water

Payload bytes are stored as separate files under namespace `payloads/`, and
segment records point at payload leaves. This does not match the documented
compact append-record format with payload spans, CRC-validated inline/streaming
records, and commit-group visibility.

### 3. Object, attachment, queue, and transaction planes are implemented above
the simplified state path

The active client adapter implements a large public pouch surface, including
attachments, queues, transaction decisions, TC surfaces, query routing, and
watch/subscribe behavior. Those records are encoded as state-style records and
client-private storage keys rather than through the object/metadata/storage
planes described by the pouch storage specification.

This means current behavior may be broad, but it is not the layered storage
engine in the spec.

### 4. Allocator discipline was not enforced over the real pouch surface

The current allocator boundary test checks only:

- `src/lc_pouch.h`
- `src/lc_pouch.c`
- `src/lc_pouch_namespace.c`
- `src/lc_pouch_path.c`

The active pouch implementation also includes `src/lc_pouch_client.c`,
`src/lc_pouch_state.c`, `src/lc_pouch_query_index.c`, and the private index
modules. At audit start, `src/lc_pouch_client.c` contained many raw
`malloc`/`calloc`/`realloc`/`free` calls, so the guardrail did not prove the
allocator contract required by the spec. This slice expanded the allocator
contract to the active pouch surface and rewrote active pouch/test allocation
calls through the project allocator helpers.

### 5. Pouch/disk terminology is mixed in docs and TODO

The live source has largely moved to `lc_pouch_*` naming, and the dirty diff
already removes the remaining historical transition error-domain strings from
active source. Remaining problematic terminology is concentrated in `TODO.md`,
`docs/pouch-storage.md`, and the boundary test name/messages.

Legitimate references to the Go lockd disk backend should remain only where it
is explicitly a benchmark or design reference. Pouch-internal implementation
identity must use pouch-native terms. This slice removed live transition-era
cutover labels from active source, docs, and tests outside the deprecated
reference tree.

### 6. Exact-generation numeric cutover was half-present but unwired

At audit start, the query-index module contained a numeric canonicalization
helper for exact-term generation values, but the generation builder did not use
it and the exact readers explicitly bypassed generation lookup when any exact
term was numeric. This meant the implementation could claim typed exact
generation files while still depending on the scalar sidecar path for numeric
equality and key-return exact visitors.

This slice wires numeric selector canonicalization into the shared index term
key builder, publishes exact generation postings through a whole-generation
term-ID accumulator, and routes exact document/key equality and `in` readers
through identity-matched exact generations.

### 7. Prepared/result cache claims were broader than active symbols

`TODO.md` and `docs/pouch-storage.md` describe prepared term caches,
normalized result caches, and index-owned page-planning cache lifecycles. This
slice adds the first active prepared readers: exact equality and `in` queries
reuse a handle-owned prepared exact reader keyed by namespace plus immutable
`query.index` identity, and positive `exists` queries reuse a prepared presence
reader over `query.index.lcppg`. Numeric range queries reuse a prepared range
reader over `query.index.lcprg`. Prefix and contains queries reuse a prepared
text reader over `query.index.lcptxg`. Date queries reuse a prepared temporal
reader over `query.index.lcptdg`. Each prepared reader owns its loaded
predicate generation plus the matching document-table generation and keeps a
bounded one-entry docID result cache for the last normalized predicate lookup.
Active `src/` also publishes `query.index.lcpt3g` trigram generations and uses
them to narrow contains/icontains candidates before final text validation. It
also routes cursor-page accounting through `lc_pouch_index_result`. This slice
adds the first index-owned sorted result-page cache slots to the prepared
reader lifecycle; cache lookup/store/cleanup live in `lc_pouch_index_result`,
and generation-backed readers reuse normalized key/docID row vectors before
client-visible cursor-page emission. Prepared reader freshness now uses
generation-file signatures on cache hits and falls back to full validation only
when the backing file changes; current `query.index` freshness checks are
header-only. Batched document-result reads reuse cached projection payload
bytes and a temporary sorted projection-record index.

## Cutover Base Decision

The correct cutover base is the current active implementation, not a wholesale
restore of `deprecated/pouch-legacy/`.

Reasoning:

- The current branch has 143 commits after the skeleton cutover, including the
  current dirty exact-generation work.
- The deprecated tree contains the missing historical modules, but it also
  carries the old backend identity, retired history terminology, and larger
  monolithic store structure the current branch intentionally retired.
- Reconstituting deprecated wholesale would reverse the active branch direction
  and preserve the unacceptable identity problem unless it were completely
  rewritten.

The single-slice cutover should therefore move the current active implementation
forward to the pouch-native storage design, while selectively reusing code or
ideas from `deprecated/pouch-legacy/` only when they directly satisfy the new
pouch boundary without restoring legacy identity.

## Required Single-Slice Work

The cutover is not complete until all of the following are true in the same
coherent implementation slice:

1. Source/docs/TODO use pouch-native identity.
   Remove live transition-era test names/messages and replace stale
   disk-adapter language with pouch storage bridge/index-layer terms. Keep Go
   lockd disk references only for benchmark/reference comparisons.

2. Boundary tests match the chosen architecture.
   The pouch boundary test must assert the active pouch-native module map,
   reject deprecated implementation references in live compilation, and cover
   the real implementation files instead of only the scaffold files.

3. Storage allocator checks cover the real pouch implementation surface.
   The allocator contract must include active pouch storage/client/query/index
   files or explicitly document and isolate any public API bridge where project
   allocator helpers are not available.

4. The docs/TODO module map must be reconciled with live code.
   Either implement live pouch-native modules for temporal/trigram readers and
   result/cache boundaries, or revise the spec/TODO to state the current
   pouch-native module map without claiming missing files are active.
   The final state must not contain checked TODO items that are contradicted by
   the source tree.

5. Exact-term generation cutover must be completed.
   This slice cut exact document/key equality and `in` readers over to
   identity-matched exact generations, including canonical numeric equality.
   Remaining work is to verify the implementation after the full cutover and
   ensure docs/TODO no longer imply a scalar exact sidecar reader.

6. Query/index architecture must stop presenting scaffolding as complete.
   The active implementation has docID sets, document-table generations,
   adaptive postings, exact, presence, range, text, trigram, and temporal term
   generations, prepared exact, presence, range, text/trigram, and temporal
   reader caches, bounded per-reader docID result caches, an index-layer
   page-selection primitive, and index-owned sorted result-page cache slots
   keyed by normalized predicate identity. It now also has signature-based
   prepared-reader freshness, header-only current-index checks, cached
   projection payload sources, and sorted projection-record lookup for batched
   document reads.

7. Compaction/lifecycle claims must match implementation.
   The active compaction path is state-segment oriented. The spec/TODO must not
   claim full metadata/object/queue compact binary snapshot lifecycle unless
   live code actually implements it.

8. Verification must wait until after the cutover implementation.
   No `make test`, benchmark gate, fuzz smoke, or TDD loop should run until the
   source/docs/TODO cutover is complete.

9. Performance acceptance comes last.
   After correctness verification, the Go lockd disk comparison gate must prove
   pouch is faster than Go lockd disk on all required metrics or leave the goal
   incomplete.

## Immediate Implementation Direction

Start by reconciling terminology and guardrails because they define the
production envelope for the larger implementation work:

- Rename the transition-era boundary test to a pouch cutover/boundary test.
- Update boundary-test source lists to reflect current active modules and reject
  deprecated pouch sources in CMake.
- Expand allocator and liblql boundary checks to the actual active storage
  surfaces.
- Remove transition-progress language from live diagnostics and
  docs.
- Then reconcile or implement the prepared/result-cache architecture claimed by
  the docs/TODO.
