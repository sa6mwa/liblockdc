# liblockdc Lifecycle Migration

This ledger tracks the active migration to the pkt.systems C/CMake lifecycle. It
exists while lifecycle behavior is still converging so command and artifact
behavior is not silently lost.

## Command Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| `make test-host` as the ordinary local confidence gate | `make finalize-slice` | Formatting and host release tests remain the pre-commit slice gate. | `lifecycle_command_surface_test` asserts the target and help entry. | Active |
| Ad hoc ASan/UBSan hardening through `make asan` and `make test-debug` | `make valgrind` | ASan/UBSan compatibility aliases remain available while native Memcheck runs through the dedicated lifecycle gate. | `lifecycle_command_surface_test` asserts the runner wiring and dry-run target set. | Active host runner; pending remaining Bootlin compiler policy |
| Manually choosing release rehearsal commands | `make prerelease` | Deterministic local checks are grouped without requiring live credentials. | `lifecycle_command_surface_test` asserts the graph includes slice, broad tests, package verification, and Lua tests. | Active |
| No live prerelease gate | `make prerelease-live` | Live-provider checks remain opt-in and are not part of normal local confidence. | `lifecycle_command_surface_test` asserts the `LOCKDC_PRERELEASE_LIVE=1` opt-in diagnostic. | Placeholder until live checks exist |
| Expensive release rehearsal split across fuzz, benchmark, package, and matrix commands | `make prerelease-hardening` | Existing fuzz smoke, benchmark gate, and release matrix behavior remain available. | `lifecycle_command_surface_test` asserts the hardening graph. | Active |
| Existing `make release-matrix` | `make release-matrix` | Incremental release matrix rehearsal remains the standard surface. | Existing release targeting tests plus command-surface help coverage. | Active |

## Dependency Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| Repository-local release asset downloads under `.cache/downloads` only | Shared verified archive cache under `CPKT_DEPENDENCY_CACHE`, with repo-local staging under `.cache/downloads` | Existing dependency roots and package manifests still use target IDs and exact release assets. | `dependency_archive_cache_contract_test`, `dependency_download_timeout_test`, and full `make test` coverage. | Active |
| Direct lifecycle use of `lonejson`, `libpslog`, and `c.pkt.systems` pins | Latest release pins plus first-class `liblql` dependency | Downstream CMake and pkg-config consumers still link through `liblockdc`. | Dependency interface tests and install-tree smoke consumers. | Active |

## Preset Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| Lua checks ran through the general `debug` preset | `debug-lua` | Lua bindings still build against the debug dependency root; the preset narrows non-Lua work. | `lifecycle_preset_contract_test` asserts configure/build/test preset coverage. | Active |
| No dedicated Valgrind configure preset | `valgrind` | Debug symbols are preserved without sanitizer instrumentation so Memcheck can inspect runtime behavior. | `lifecycle_preset_contract_test` asserts the preset disables fuzzers and sanitizer debug flags; `bootlin_toolchain_contract_test` proves native GNU imports the pinned Bootlin resolver; `make valgrind` runs a bounded native unit subset under host Memcheck. | Active preset; pending remaining Bootlin compiler policy |
| Fuzz preset selected host Clang directly | Pinned AFL++ GCC-plugin lifecycle | Existing fuzz preset remains available as native x86_64 compatibility without explicit host compiler selection until the AFL++ resolver is wired. | `lifecycle_preset_contract_test` rejects direct host compiler overrides and keeps the AFL++ gap visible through required preset coverage. | Pending AFL++ migration |

## Pouch Storage Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| Placeholder pouch indexed-query parser boundary | `liblql`-backed LQL parsing and evaluation behind the pouch store boundary | Pouch storage owns index layout and scan/index fallback behavior. | Pending owner/key selector, pagination, hidden metadata, removed-candidate, malformed query, oversized query, and parity tests. | Pending implementation |

## Removed Or Deprecated Behavior

- No lifecycle command has been removed in this slice.
- `make asan`, `make test-asan`, and `make build-asan` remain compatibility
  aliases after the native Valgrind gate, so existing sanitizer workflows stay
  available while lifecycle hardening moves to Memcheck.

## Decisions Still Required

- Whether Valgrind becomes part of `make prerelease` immediately when the
  Bootlin-backed compiler policy lands, or first stays as a separate hardening
  gate for one migration slice.
- Whether the existing compatibility fuzz preset should remain as an explicit
  alias after the pinned AFL++ lifecycle migration lands.
- Whether live provider checks should exist for `make prerelease-live`; no live
  prerelease checks are currently defined.
- Whether the migration ledger should be retained as project documentation after
  the lifecycle migration is complete or deleted as transitional state.
