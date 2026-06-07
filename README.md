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
- integrated SDK logging through `libpslog`

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

Normal host and release Make targets provision the required dependency trees automatically. Low-level scripts such as `scripts/build.sh` and `scripts/test.sh` assume that the matching dependency root already exists.

## Build prerequisites

Normal development expects:

- CMake 3.24 or later
- Ninja
- a C compiler
- `musl-gcc` for host musl packaging
- GNU cross compilers on `PATH` for `aarch64-linux-gnu-*` and `arm-linux-gnueabihf-*`
- musl cross compilers on `PATH` for `aarch64-linux-musl-*` and `arm-linux-musleabihf-*`
- `qemu-aarch64` and `qemu-arm` for the non-host release test matrix
- `nerdctl compose` preferred for the local development environment, with `docker compose` as a fallback

Third-party dependency roots are cached under `.cache/deps`.

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

Run the full release verification path across host and cross targets:

```bash
make test-all
```

`make test-all` starts with the sanitizer-instrumented debug suite before the
host and cross release suites.

Run focused verification layers:

```bash
make test-e2e
make asan
make coverage
make fuzz
make benchmarks
```

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

`make release` is the final clean-slate release workflow. It removes generated
state, formats the C tree, runs the debug sanitizer, host, cross, e2e,
benchmark, and optional fuzz layers, then generates and verifies the release
archive set. Use
`make release-matrix` when you explicitly want to reuse existing build and
dependency caches for a faster release matrix/package rerun.

Create only the `x86_64-linux-gnu` package:

```bash
make package
make package-checksums
```

Package archive names follow this pattern:

- release archive:
  - `liblockdc-<version>-<target>.tar.gz`
- checksum manifest:
  - `liblockdc-<version>-CHECKSUMS`

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

`liblockdc` depends on `lonejson 0.31.0` with shared-library ABI `16`.
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
  - fuzz harnesses
- `scripts/`
  - workflow and environment scripts
- `devenv/`
  - local environment notes

## Low-level entry points

If you need direct control over the underlying build or test preset, the lower-level scripts remain available:

```bash
scripts/deps.sh deps-x86_64-linux-gnu
scripts/build.sh debug
scripts/build.sh e2e
scripts/build.sh x86_64-linux-gnu-release
scripts/cross_build.sh
scripts/cross_test.sh release
scripts/test.sh unit
scripts/test.sh e2e
scripts/fuzz.sh
scripts/package.sh all
scripts/package-verify.sh
```

Unlike the primary Makefile workflow, these lower-level scripts do not generally provision dependency roots implicitly. `scripts/cross_build.sh` prepares the non-host release build trees, and `scripts/cross_test.sh release` runs the cross release tests against those existing build trees. Use them when you want direct preset control and are prepared to manage the prerequisite dependency tree yourself.

See <https://github.com/sa6mwa/liblockdc> for the full source code.
