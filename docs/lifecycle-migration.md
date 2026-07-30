# liblockdc Lifecycle Migration

This ledger tracks the active migration to the pkt.systems C/CMake lifecycle. It
exists while lifecycle behavior is still converging so command and artifact
behavior is not silently lost.

## Command Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| `make test-host` as the ordinary local confidence gate | `make finalize-slice` | Formatting and the narrow debug test gate are the pre-commit slice gate; host release tests stay on `make test-host` and `make test-all`. | `lifecycle_command_surface_test` asserts the target, help entry, and graph. | Active |
| Ad hoc ASan/UBSan hardening through `make asan` and `make test-debug` | `make valgrind` | ASan/UBSan compatibility aliases remain available while native Memcheck runs through the dedicated lifecycle gate. | `lifecycle_command_surface_test` asserts the runner wiring and dry-run target set. | Active host runner |
| Manually choosing release rehearsal commands | `make prerelease` | Deterministic local checks are grouped without requiring live credentials: slice gate, Valgrind, fuzz smoke, lockd e2e, and Lua tests. | `lifecycle_command_surface_test` asserts the graph includes those surfaces. | Active |
| No live prerelease gate | `make prerelease-live` | Live-provider checks remain opt-in and are not part of normal local confidence. | `lifecycle_command_surface_test` asserts the `LOCKDC_PRERELEASE_LIVE=1` opt-in diagnostic. | Placeholder until live checks exist |
| Expensive release rehearsal split across benchmark, parity, package, and matrix commands | `make prerelease-hardening` | Pouch parity, benchmark gates, and release matrix behavior remain available after deterministic prerelease checks. | `lifecycle_command_surface_test` asserts the hardening graph. | Active |
| Release version checks embedded in later release work | `make lifecycle-version-contract` | Exact semver release tags are validated before clean release work; only lightweight tags are accepted, and the reserved temporary tag is cleaned up. | `version_resolution_test` rejects annotated tags; `lifecycle_command_surface_test` asserts the release ordering. | Active |
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
| No dedicated Valgrind configure preset | `valgrind` | Debug symbols are preserved without sanitizer instrumentation so Memcheck can inspect runtime behavior. | `lifecycle_preset_contract_test` asserts explicit Linux toolchain files; `bootlin_toolchain_contract_test` proves every Linux toolchain imports the pinned Bootlin resolver; `make valgrind` runs a bounded native unit subset under host Memcheck. | Active preset |
| Fuzz preset selected host Clang directly | Pinned AFL++ GCC-plugin lifecycle | Fuzz targets build as native x86_64 Linux AFL++ executables through the pinned AFL++ resolver and Bootlin-backed wrapper compiler. | `lifecycle_preset_contract_test` asserts the AFL++ toolchain file; `ctest --preset fuzz` runs committed seed smoke tests; `make fuzz-smoke` runs bounded AFL++ jobs. | Active |

## Pouch Storage Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| Placeholder pouch indexed-query parser boundary | `liblql`-backed public LQL document predicates plus typed pouch metadata selectors behind the pouch store boundary | Pouch storage owns index layout, metadata filtering, and scan/index fallback behavior. | `liblql_dependency_interface` plus pouch scan/index document and `query_keys` coverage for strict JSON Pointer LQL selectors, metadata selectors, pagination, malformed query, and parity cases. | Active |

## Removed Or Deprecated Behavior

- No lifecycle command has been removed in this slice.
- `make asan`, `make test-asan`, and `make build-asan` remain compatibility
  aliases after the native Valgrind gate, so existing sanitizer workflows stay
  available while lifecycle hardening moves to Memcheck.

## Decisions Still Required

- Whether live provider checks should exist for `make prerelease-live`; no live
  prerelease checks are currently defined.
- Whether the migration ledger should be retained as project documentation after
  the lifecycle migration is complete or deleted as transitional state.
