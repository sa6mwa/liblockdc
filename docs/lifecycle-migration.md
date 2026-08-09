# liblockdc Lifecycle Migration

This ledger records the completed cutover to the pkt.systems C/CMake lifecycle
so command and artifact behavior remains explicit and regression-tested.

## Command Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| `make test-host` as the ordinary local confidence gate | `make finalize-slice` and `make test-all` | Formatting and the narrow debug test gate are the pre-commit slice gate. `test-all` is the complete local confidence tier: debug, host, QEMU cross, Valgrind, fuzz smoke, deterministic e2e, and bounded benchmarks. | `lifecycle_command_surface_test` asserts the target, help entry, and graph. | Active |
| Implicit installed-SDK and local-service example coverage | `make test-install-tree` and `make example-smoke-local` | Existing CMake/pkg-config install-tree consumers and local lockd example smoke tests now have direct standard lifecycle entrypoints. | `lifecycle_command_surface_test` asserts both command surfaces and their runner wiring. | Active |
| Ad hoc ASan/UBSan hardening through `make asan` and `make test-debug` | `make valgrind` plus `make test-debug` | Public ASan compatibility aliases were removed; the debug test surface and native Memcheck gate remain the lifecycle hardening paths. | `lifecycle_command_surface_test` asserts the runner wiring and dry-run target set. | Cut over |
| Manually choosing local deterministic confidence commands | `make prerelease` | The deterministic prerelease proof runs the ordinary slice, Valgrind, fuzz smoke, lockd e2e, native benchmarks, and the Pouch-versus-disk performance parity gate without an initial clean. Lua coverage runs in the shared `debug` test tree. | `lifecycle_command_surface_test` asserts the deterministic prerelease graph. | Active |
| No live prerelease gate | `make prerelease-live` | Live-provider checks remain opt-in and are not part of normal local confidence. | `lifecycle_command_surface_test` asserts the `LOCKDC_PRERELEASE_LIVE=1` opt-in diagnostic. | Placeholder until live checks exist |
| Expensive release rehearsal split across long fuzzing, parity, and matrix commands | `make prerelease-hardening` | The normal release path gates the complete exclusive Pouch-versus-Go-disk core metric contract in no-sync and strict-durable modes. Prerelease hardening adds the bounded multi-mode core churn soak, reclaim proof, shared-root contention, full fuzzing, and the release matrix; the clean `make release` runs the normal prerelease proof followed by the matrix once. | `lifecycle_command_surface_test` and `benchmark_go_timeout_contract_test` assert the hardening graph and complete core-metric allowlist. | Active |
| Release version checks embedded in later release work | `make lifecycle-version-contract` | Exact semver release tags are validated before clean release work; only lightweight tags are accepted, and the reserved temporary tag is cleaned up. | `version_resolution_test` rejects annotated tags; `lifecycle_command_surface_test` asserts the release ordering. | Active |
| Repo-local release version script | `scripts/release_version.sh` through `make print-release-version` | Git worktrees resolve only exact lightweight `vX.Y.Z` tags, then explicit override, otherwise `0.0.0`; non-git source archives read `VERSION`. | `version_resolution_test` and `lifecycle-version-contract` cover Make and CMake version behavior. | Active |
| `scripts/dev-e2e.sh` as the e2e entrypoint | `scripts/test-e2e.sh` through `make test-e2e` | Default bundle, endpoint, and socket environment setup moved to the standard script; the old script path was removed. | `lifecycle_command_surface_test` asserts the standard runner. | Cut over |
| `scripts/test_release_source.sh` | `scripts/test_release_from_source.sh` through `make package-source-smoke` | Source archive extraction, version agreement, build, and unit smoke behavior moved to the standard script; the old script path was removed. | `release_package_matrix_targeting_test` and command-surface coverage assert the standard runner. | Cut over |
| Existing release-matrix recipe directly expanded in Make | `scripts/run_linux_release_matrix.sh` through `make release-matrix` and `make package-verify` | Incremental release matrix rehearsal builds every Bootlin target, runs the complete host suite on executable host targets and the explicit `cross-runtime` suite on every QEMU target, packages once, then performs read-only source, SDK, Lua, archive, checksum, and recursive privacy verification against those exact artifacts. `cross-runtime` contains portable core, Pouch durability/recovery, cleanup/signal, and representative transport paths; exhaustive parser and lifecycle-policy cases remain in native sanitizer and host-release suites. A full release invokes repository clean exactly once before the proof graph; its artifact pass only clears `dist/`. The release wrapper reports phase timings for version checks, clean, the proof graph, build, per-target tests, cross tests, and packaging. | `release_targeting_test`, `cross_test_targeting_test`, `release_package_matrix_targeting_test`, `release_privacy_recursive_test`, and `lifecycle_command_surface_test` assert delegation, target coverage, cross-runtime selection, package count, clean count, matrix order, timing wiring, and nested-artifact scanning. | Active |
| Lockdc-prefixed Lua rock scripts in release metadata | `scripts/build_lua_rock.sh` and `scripts/validate_luarocks.sh` | Lua source rock build and validation implementations moved to the standard scripts; old lockdc-prefixed script paths were removed. | Lua layout, SDK contract, install/run, and release package tests assert the standard names. | Cut over |
| Existing service, fuzz, benchmark, Go parity, and Lua release commands lacked all standard targets | Standard targets `dev-ps`, `dev-logs`, `fuzz-long`, `bench`, `bench-check`, `benchmarks-go`, `perf-gate`, `release-lua-artifacts`, and `verify-release-privacy` | Existing service inspection, fuzzing, benchmark, Go parity, Lua artifact, and privacy scan behavior is exposed through lifecycle names. | `lifecycle_command_surface_test` asserts help entries and target wiring. | Active |

## Dependency Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| Repository-local release asset downloads under `.cache/downloads` only | Shared verified archive cache under `CPKT_DEPENDENCY_CACHE`, with repo-local staging under `.cache/downloads` | Existing dependency roots and package manifests still use target IDs and exact release assets. | `dependency_archive_cache_contract_test`, `dependency_download_timeout_test`, and full `make test` coverage. | Active |
| Direct lifecycle use of `lonejson`, `libpslog`, and `c.pkt.systems` pins | Latest release pins plus first-class `liblql` dependency | Downstream CMake and pkg-config consumers still link through `liblockdc`. | Dependency interface tests and install-tree smoke consumers. | Active |

## Preset Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| ASan/UBSan debug presets selected the host C compiler | `debug` and its derived Linux presets | Debug, e2e, Lua, ASan, and coverage builds now use the pinned x86_64 GNU Bootlin collection and matching dependency root. | `lifecycle_preset_contract_test` asserts the Bootlin debug toolchain and its derived-preset inheritance; `make finalize-slice` builds and tests the configured debug preset. | Active |
| Valgrind selected the retired `lc_unit_pouch_client` binary | `lc_unit_pouch` | The bounded Valgrind subset runs the unified Pouch unit binary produced after the logstore cutover. | The lifecycle command-surface test asserts the runner's dry-run target; `make valgrind` verifies the executable exists and runs it under Memcheck. | Active |
| Lua checks used a separate debug build tree | Shared `debug` tree with the `debug-lua` configure/build/test selectors | Lua bindings and Lua tests share the already-built sanitizer debug tree; the named selectors retain the lifecycle preset surface while using the same binary directory and filtering the Lua suite. | `lifecycle_preset_contract_test` asserts the shared debug configuration; `make lua-test` exercises the filtered Lua suite. | Active |
| No dedicated Valgrind configure preset | `valgrind` | Debug symbols are preserved without sanitizer instrumentation so Memcheck can inspect runtime behavior. | `lifecycle_preset_contract_test` asserts explicit Linux toolchain files; `bootlin_toolchain_contract_test` proves every Linux toolchain imports the pinned Bootlin resolver; `make valgrind` runs a bounded native unit subset under host Memcheck. | Active preset |
| Fuzz preset selected host Clang directly | Pinned AFL++ GCC-plugin lifecycle | Fuzz targets build as native x86_64 Linux AFL++ executables through the pinned AFL++ resolver and Bootlin-backed wrapper compiler. | `lifecycle_preset_contract_test` asserts the AFL++ toolchain file; `ctest --preset fuzz` runs committed seed smoke tests; `make fuzz-smoke` runs bounded AFL++ jobs. | Active |

## Pouch Storage Surface

| Old command or behavior | New lifecycle surface | Behavior preserved | Verification added | Status |
| --- | --- | --- | --- | --- |
| Placeholder pouch indexed-query parser boundary | `liblql`-backed public LQL document predicates plus typed pouch metadata selectors behind the pouch store boundary | Pouch storage owns index layout, metadata filtering, and scan/index fallback behavior. | `liblql_dependency_interface` plus pouch scan/index document and `query_keys` coverage for strict JSON Pointer LQL selectors, metadata selectors, pagination, malformed query, and parity cases. | Active |

## Removed Or Deprecated Behavior

- Removed obsolete compatibility scripts:
  `scripts/dev-e2e.sh`, `scripts/test_release_source.sh`,
  `scripts/build_lockdc_lua_rock.sh`,
  `scripts/validate_lockdc_luarocks.sh`, and
  `scripts/print-release-version.sh`.
- Removed non-standard ASan compatibility Make targets:
  `make asan`, `make test-asan`, and `make build-asan`.
- Removed duplicate direct package orchestration scripts:
  `scripts/package.sh` and `scripts/package-verify.sh`. The Make-owned
  `package`, `package-verify`, and `release-matrix` targets are now the only
  package orchestration surfaces.
- Removed the duplicate `scripts/test.sh` dispatcher and the undocumented
  `scripts/build.sh dev`/`test` aliases. The Make-owned test tiers are the
  only normal test orchestration surface.

## Optional Extensions

- Whether live provider checks should exist for `make prerelease-live`; no live
  prerelease checks are currently defined.
