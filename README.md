# liblockdc

`liblockdc` is a C89/C90 client library for `lockd`. It provides a receiver-function public API for client, lease, queue delivery, attachment, management, and consumer-service handles, plus stream-based JSON and payload I/O. The project ships both static and shared libraries, a local development environment, a cross-architecture release workflow, and dependency-backed unit, e2e, sanitizer, coverage, fuzz, and benchmark targets.

## Supported targets

Release archives are produced for:

- `x86_64-linux-gnu`
- `x86_64-linux-musl`
- `aarch64-linux-gnu`
- `aarch64-linux-musl`
- `armhf-linux-gnu`
- `armhf-linux-musl`

The library itself is delivered as:

- `liblockdc.so`
- `liblockdc.a`

## Capabilities

- mTLS client authentication through a combined `client.pem` bundle
- HTTP/1.1 and HTTP/2 transport through bundled `libcurl` and `nghttp2`
- Unix domain socket transport support for local `mem://` deployments
- stream-oriented state and payload upload/download with `lc_source` and `lc_sink`
- receiver-function `lc_client`, `lc_lease`, `lc_message`, and `lc_consumer_service` APIs
- lease, queue, attachment, namespace/index, transaction-coordinator, and resource-manager management APIs
- mapped JSON state load/save through `lonejson`
- streamed query-key callbacks and streaming queue subscribe/watch flows
- managed consumer support with blocking and explicit start/stop/wait service modes
- durable command-receipt, inbox, outbox, dispatcher, retry, reconciliation,
  and dead-letter workflow receivers
- integrated SDK logging through `libpslog`

The transactional messaging model, endpoint constraints, and C receiver
surface are specified in [the workflow design](docs/inbox-outbox.md). The Lua
workflow facade is documented in [the Lua SDK guide](docs/lua.md).

## Pouch storage

Pouch is liblockdc's C-native local storage engine. It follows Go lockd disk
storage semantics while intentionally using its own binary records, manifests,
`uint64_t` payload/file-size, generation/index, and coordinator-term
accounting, and `int64_t` Unix timestamps. The current semantic alignment scope
and every remaining format, operational, API, and portability divergence are
recorded in [the Pouch storage specification](docs/pouch-storage.md).

Select Pouch through a single absolute `pouch://` endpoint, for example
`pouch:///var/lib/my-service/lockd-root`. The default is exclusive
single-writer mode: one live process owns the root append path and normal
acquire, update, release, queue, attachment, and query operations use the
resident logstore fast path. Opening a second default writer for the same root
fails instead of silently downgrading. Explicit shared-root writing remains
available for callers that need multiple active local writers by adding
`?single_writer=false`; that mode preserves
correctness and process fencing but is not the primary performance target.

Pouch endpoint options mirror the public C config and direct Pouch storage
options. Common options are:

- `compression=zlib` for streaming at-rest zlib
  compression
- `crypto_key_file=/path/to/pouch.key` with
  `crypto_generate_key_file=true` for encrypted local roots
- `durable_sync=true` and `fsync_batch_max_ops=<u64>` for root-scoped durable
  group commit
- `segment_target_bytes=<u64>` for rolling segment sizing
- `indexer_flush_docs=<u64>` and `indexer_flush_interval_seconds=<u64>` for
  bounded query-index publication: exclusive roots publish at the document
  threshold after all pending leases or transactions have completed, while
  shared roots publish asynchronously
- `background_compaction=false` to disable the default idle-debounced
  compaction worker, and `disable_compaction_throttling=true` to remove its
  default throughput bound
- `retention_seconds=<u64>` and `janitor_interval_seconds=<u64>` for the
  post-mutation retention worker
- `queue_watch=true` to request filesystem-assisted queue wake-up where the
  local filesystem supports it, with polling fallback otherwise
- `query_engine=index|scan` and `query_fallback_engine=index|scan` for the
  namespace query preference used at open

The public API remains the same receiver-function SDK surface for remote and
Pouch clients. State bodies, queue payloads, attachments, scan output,
query-document output, crypto, and compression use real streaming paths unless
the caller explicitly chooses an in-memory source or sink.

## Build system

The repository (<https://github.com/sa6mwa/liblockdc>) uses a Makefile-first workflow with CMake as the build backend:

- `Makefile`
  - primary developer entry point
  - build, test, package, clean, and release orchestration
- `CMake`
  - target graph
  - install graph
  - exported package metadata
  - test registration

Normal host and release Make targets provision the required dependency trees automatically. The low-level `scripts/build.sh` helper assumes that the matching dependency root already exists.

## Build prerequisites

Normal development expects:

- CMake 3.24 or later
- Ninja
- GNU Make
- host `clang-format` for `make format`
- host Valgrind for the native Memcheck gate
- `qemu-aarch64` and `qemu-arm` for the non-host release test matrix
- `nerdctl compose` preferred for the local development environment, with `docker compose` as a fallback

Every Linux build uses its matching pinned Bootlin GCC collection, including
the compiler, linker, binutils, sysroot, headers, and runtime. The Make and
CMake workflows provision those collections automatically; do not substitute
host or distro cross compilers. Toolchains are shared under
`${CPKT_TOOLCHAIN_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/c.pkt.systems/toolchains}`
and verified dependency archives under
`${CPKT_DEPENDENCY_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/c.pkt.systems/deps}`.
Repository-local `.cache/` directories are disposable build and staging state.

## Common workflows

Build the normal host development preset:

```bash
make build
```

`make build` and `make test-debug` use the sanitizer-instrumented debug
preset. Third-party dependencies are still reused from the release-built host
dependency cache.

Run the host release suites for the shipped x86_64 GNU and musl builds:

```bash
make test
```

Run the non-host cross release suites:

```bash
make test-cross
```

Run the complete local confidence path:

```bash
make test-all
```

`make test-all` runs the sanitizer-instrumented debug suite, both
host-executable Bootlin release suites, QEMU cross suites, Valgrind, and
deterministic local e2e. Its CTest suites use a bounded four-job default
(`LOCKDC_CTEST_PARALLEL_LEVEL` overrides it), while tests marked serial remain
serial. Fuzz smoke stays explicit (`make fuzz-smoke`) and in `make prerelease`:
it runs AFL++ only for deterministic unit-level parsers, streams, and Pouch
primitives, then runs the full Pouch LQL and lifecycle scenarios in isolated
normal processes with deterministic input mutation. The AFL++ compiler
bootstrap is hardening work, not an everyday functional invariant. Performance
workloads likewise belong to the explicit
`make bench-gate` command. The complete artifact rehearsal remains `make
release-matrix`.

`make prerelease-hardening` is the longer pre-release layer. It keeps the
normal release gate bounded, then adds native benchmarks, the Pouch-vs-lockd
performance thresholds, the finite multi-mode Pouch core churn soak, reclaim
proof, shared-root contention coverage, full fuzzing, and the release matrix.
The Pouch-vs-lockd performance comparison measures every core storage
operation separately: acquire, update and stale-precondition rejection,
release, public and leased reads, attachment write/read, queue delivery, all
index-publication phases, indexed/scan/full-text queries, and restart recovery.

Run focused verification layers:

```bash
make test-e2e
make test-debug
make coverage
make fuzz
make pouch-integration-fuzz
make benchmarks
```

`make pouch-integration-fuzz` runs the production-code Pouch LQL and lifecycle
mutation runners without AFL++ instrumentation. Each corpus seed and mutation
executes in a fresh process; failures retain the reproducing input artifact.

`make benchmarks` uses tuned per-case defaults; set `BENCH_ITERS=<n>` to force
the same iteration count across all benchmark cases.

All significant Make targets print total elapsed time on completion.

`make format` runs `clang-format` over the C source/header tree and is also
part of the clean-slate release workflow.

## Local development environment

The repository includes a local `lockd` environment for integration testing and example execution. The stack includes:

- a shared-disk `lockd` pair
- an S3-backed `lockd`
- a `mem://` `lockd` exposed over a Unix domain socket
- MinIO

Start the environment:

```bash
make dev-up
```

Reset generated environment state:

```bash
make dev-reset
```

Stop the environment:

```bash
make dev-down
```

The e2e workflow is self-contained. `make test-e2e` resets the generated environment state, starts the compose stack, waits for the generated bundles and listeners, probes the active disk endpoint, and then runs the e2e CTest preset.

Additional development-environment notes are available in the repository at `devenv/README.md`.

## Packaging and release archives

The project ships one combined archive for each supported target.

Create the complete release set:

```bash
make release
```

`make release` is the final clean-slate release workflow. It verifies release
tag semantics, removes generated state, then runs the same proof graph as
`make prerelease`: formatting, debug sanitizer tests including Lua coverage,
Valgrind, fuzz smoke, lockd e2e, bounded benchmark smoke, and the release
matrix. Use
`make release-matrix` when you explicitly want to reuse existing build and
dependency caches for a faster release matrix/package rerun.

Create only the `x86_64-linux-gnu` package:

```bash
make package
```

Package archive names follow this pattern:

- release archive:
  - `liblockdc-<version>-<target>.tar.gz`
- checksum manifest:
  - `liblockdc-<version>-CHECKSUMS`
- standalone Lua source package:
  - `liblockdc-lua-<version>.tar.gz`
- rendered Lua release artifacts:
  - `lockdc-<version>-1.rockspec`
  - `lockdc-<version>-1.src.rock`

### Release archive contents

The release archive contains:

- `liblockdc` public headers
- bundled dependency headers for `libpslog`, `curl`, `OpenSSL`, `nghttp2`, and `lonejson`
- `liblockdc.so*`
- `liblockdc.a`
- the static archives for the bundled third-party dependencies
- bundled shared runtime dependencies
- `pkg-config` metadata
- CMake package metadata
- project documentation and license files
- bundled third-party license files

Archive layout and contents are regression-tested. The release verification suite asserts the complete shipped matrix, expected archive names, checksum manifest, and package contents.

## Public API model

The public API is intentionally receiver-function based rather than a flat RPC
wrapper. `lc_client_open()` returns an `lc_client *`; methods then live on the
returned handle as function pointers such as `client->acquire(...)`,
`lease->update(...)`, `message->ack(...)`, and `service->run(...)`.

The standalone `lc_*` functions remain available as compatibility wrappers and
as useful entry points for callers that prefer a flat symbol lookup, but the
primary public surface and examples use the receiver-function form. New method
slots are appended to preserve layout stability within the current
shared-library ABI line.

The installed [public header](include/lc/lc.h) is the detailed API reference;
the generated `lc/version.h` also documents the compile-time semantic-version
and ABI macros. Their Doxygen contracts cover every public handle,
request/result type, field, callback, helper, and compatibility function. The
conventions are:

- initialize transparent config and request structs with their matching
  `*_init()` helper before setting fields;
- input pointers are borrowed for the call unless their documentation says
  ownership transfers;
- returned handles and heap-backed result fields are caller-owned and are
  released with the matching `close()` or `*_cleanup()` helper;
- close and cleanup helpers accept `NULL`, and cleanup helpers zero their
  object after releasing nested ownership;
- fallible calls return `LC_OK` or an `LC_ERR_*` status and may populate an
  `lc_error`, which is released with `lc_error_cleanup()`;
- `lc_source_from_fd()` and `lc_sink_to_fd()` borrow the descriptor and do not
  close it; path-backed constructors own the descriptor they open;
- streaming APIs use bounded producer-to-consumer buffers. Mapped lonejson
  loads normally materialize mapped fields, while spool-backed mappings may
  keep large fields file-backed.

Portable widths are part of that contract: versions and Unix timestamps are
signed 64-bit values, index sequences and transaction-coordinator terms are
unsigned 64-bit values, and legacy public `long` byte/count fields reject
values that cannot be represented on the calling architecture.

- `lc_client`
  - root client handle
- `lc_lease`
  - lease and state handle
- `lc_message`
  - queue delivery handle
- `lc_source`, `lc_sink`
  - streaming state and payload abstractions

Typical flow:

1. initialize an `lc_client_config`
2. open an `lc_client`
3. acquire a lease or dequeue a message
4. operate on the returned handle
5. finish with `release()`, `ack()`, or `close()`

This keeps lease identity, transaction identifiers, and related lifecycle state on the handle instead of forcing callers to pass those values through every operation manually.

### JSON and lonejson

`liblockdc` depends on `lonejson 0.43.0` with shared-library ABI `26`.
`lonejson` is used for:

- typed JSON response parsing for management, attachment, queue, namespace,
  transaction, and state metadata paths
- mapped state `load()` and `save()` through caller-provided
  `LONEJSON_FIELD_*` maps
- streaming JSON request serialization through lonejson curl upload adapters
- query-key streaming and queue subscribe multipart/metadata parsing
- Lua JSON encode/decode helpers through the `lockdc` Lua rock dependency

The client config field `http_json_response_limit_bytes` caps typed JSON
responses parsed through `lonejson`. Zero uses
`LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT`, currently 100 MiB. The cap applies to
dynamic strings, JSON value capture, and spool-backed mapped fields on response
parsing. It does not cap request-side JSON serialization; large mapped saves
should use lonejson source-backed fields when the source value is naturally
file-backed or stream-backed.

Mapped `load()` destinations may be reused. The SDK prepares destinations with
the runtime `reset` path when they already own lonejson-managed storage and
with `init` on first use, while preserving preconfigured `lonejson_json_value`
capture sinks. Callers still own final cleanup of lonejson-owned mapped fields
with a compatible lonejson runtime after the loaded value is no longer needed.

## Examples

The repository includes standalone example programs under `examples/`.

- C examples: `examples/*.c`
- Lua examples: `examples/lua/*.lua`

The Lua examples assume the `lockdc` and `lonejson` rocks are installed and
show the intended Lua DX directly without wrapper helper modules. Additional
Lua example notes live in `examples/lua/README.md`.

The Lua consumer API is intentionally single-threaded and blocking. Lua
handlers run one message at a time on the calling Lua state; the binding does
not expose the native threaded C callback model into the same Lua VM.

The Lua SDK reference and dependency policy are documented in
`docs/lua.md`.

## Lua SDK

`liblockdc` ships a Lua frontend for the public client API as the `lockdc`
module.

The intended ownership model is:

- `liblockdc` owns the Lua-facing `lockd` client
- `liblockdc` owns the Lua-facing `lonejson` dependency boundary for what it
  exposes by declaring the supported `lonejson` Lua rock version
- downstream components such as `vectis` should consume that shipped Lua
  client distribution instead of maintaining a second `lockd` Lua client or an
  incompatible JSON binding layout

This keeps one coherent SDK import path for downstream Lua workflow runtimes.

For the Lua public surface, consumer behavior, and packaging model, see:

- `docs/lua.md`
- `examples/lua/README.md`

The following C snippets show the expected calling style directly.

Open a client:

```c
lc_client_config config;
lc_client *client;
lc_source *client_bundle;
lc_error error;
const char *endpoints[] = { "https://localhost:19441" };

lc_client_config_init(&config);
lc_error_init(&error);
client_bundle = NULL;

if (lc_source_from_file("./client.pem", &client_bundle, &error) != LC_OK) {
  fprintf(stderr, "failed to open client bundle: %s\n", error.message);
  lc_error_cleanup(&error);
  return 1;
}

config.endpoints = endpoints;
config.endpoint_count = 1;
config.client_bundle_source = client_bundle;
config.default_namespace = "default";

if (lc_client_open(&config, &client, &error) != LC_OK) {
  fprintf(stderr, "lc_client_open failed: %s\n", error.message);
  lc_source_close(client_bundle);
  lc_error_cleanup(&error);
  return 1;
}
lc_source_close(client_bundle);
```

`client_bundle_path` remains available for existing C callers, but new code
should prefer `client_bundle_source` so PEM bundles can come from files, memory,
file descriptors, or callback-backed sources.

Acquire a lease and update JSON state:

```c
lc_acquire_req acquire;
lc_release_req release;
lc_lease *lease;
lc_source *src;

lc_acquire_req_init(&acquire);
lc_release_req_init(&release);

acquire.key = "orders/42";
acquire.owner = "payments";
acquire.ttl_seconds = 60L;

if (client->acquire(client, &acquire, &lease, &error) != LC_OK) {
  fprintf(stderr, "acquire failed: %s\n", error.message);
  lc_error_cleanup(&error);
  client->close(client);
  return 1;
}

if (lc_source_from_memory("{\"status\":\"processing\"}",
                          strlen("{\"status\":\"processing\"}"),
                          &src, &error) != LC_OK) {
  fprintf(stderr, "source failed: %s\n", error.message);
  lc_error_cleanup(&error);
  lease->close(lease);
  client->close(client);
  return 1;
}

if (lease->update(lease, src, NULL, &error) != LC_OK) {
  fprintf(stderr, "update failed: %s\n", error.message);
  lc_error_cleanup(&error);
}

lc_source_close(src);

if (lease->release(lease, &release, &error) != LC_OK) {
  fprintf(stderr, "release failed: %s\n", error.message);
  lc_error_cleanup(&error);
}

client->close(client);
```

For callback-style update flows, `lc_acquire_for_update()` acquires the lease,
fetches a private state snapshot, invokes the handler, closes the snapshot, and
always attempts to release the lease before returning. The lease and snapshot
reader are borrowed and valid only during the callback; the helper owns the
final release. Handler success commits staged changes; handler failure releases
with rollback so partial callback updates are not published:

```c
static int update_order(void *ctx, lc_acquire_for_update_context *af,
                        lc_error *error) {
  lc_source *src;
  int rc;

  (void)ctx;
  rc = lc_source_from_memory("{\"status\":\"processing\"}",
                             strlen("{\"status\":\"processing\"}"),
                             &src, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = af->lease->update(af->lease, src, NULL, error);
  lc_source_close(src);
  return rc;
}

if (lc_acquire_for_update(client, &acquire, update_order, NULL, &error) !=
    LC_OK) {
  fprintf(stderr, "acquire_for_update failed: %s\n", error.message);
  lc_error_cleanup(&error);
}
```

Queue APIs follow the same handle-oriented pattern: dequeue or subscribe, operate on the returned `lc_message`, and then finish with `ack()`, `nack()`, or `close()`.

The examples in the repository at <https://github.com/sa6mwa/liblockdc/tree/main/examples> are complete programs intended to be runnable from the build tree and illustrate how an application would wire client configuration, logging, and request structures directly.

## Repository layout

- `include/lc/lc.h`
  - public API
- `src/`
  - library implementation
- `examples/`
  - standalone example programs
- `tests/unit/`
  - unit and transport tests
- `tests/e2e/`
  - e2e tests against the local `lockd` environment
- `tests/fuzz/`
  - AFL++ unit harnesses and isolated-process Pouch integration mutation
    harnesses
- `scripts/`
  - workflow and environment scripts
- `devenv/`
  - local environment notes

## Low-level entry points

`make help` is the authoritative command index. For direct control over a
configured CMake preset, use CMake after the normal lifecycle entrypoint has
provisioned its inputs:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

The CMake presets resolve the matching pinned Bootlin collection and dependency
root. Use the Make targets for normal builds, package production, verification,
and release orchestration.

See <https://github.com/sa6mwa/liblockdc> for the full source code.
