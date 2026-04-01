# liblockdc

`liblockdc` is a C89/C90 client library for `lockd`. It provides a handle-oriented public API for leases, queue deliveries, attachments, management operations, and stream-based JSON and payload I/O. The project ships both static and shared libraries, a local development environment, a cross-architecture release workflow, and dependency-backed unit, e2e, sanitizer, coverage, fuzz, and benchmark targets.

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
- stream-oriented JSON input with `lc_json`
- stream-oriented payload upload and download with `lc_source` and `lc_sink`
- lease, queue, attachment, and management APIs
- managed consumer support
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

Run the host unit and configuration suite:

```bash
make test
```

Run the full host verification path, including the local mTLS/libcurl e2e suite:

```bash
make test-all
```

Run focused verification layers:

```bash
make test-e2e
make asan
make coverage
make fuzz
make benchmarks
```

All significant Make targets print total elapsed time on completion.

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
make clean-dist
make release
make verify-release-archives
```

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

The public API is intentionally handle-oriented rather than a flat RPC wrapper.

- `lc_client`
  - root client handle
- `lc_lease`
  - lease and state handle
- `lc_message`
  - queue delivery handle
- `lc_json`, `lc_source`, `lc_sink`
  - streaming JSON and payload abstractions

Typical flow:

1. initialize an `lc_client_config`
2. open an `lc_client`
3. acquire a lease or dequeue a message
4. operate on the returned handle
5. finish with `release()`, `ack()`, or `close()`

This keeps lease identity, transaction identifiers, and related lifecycle state on the handle instead of forcing callers to pass those values through every operation manually.

## Examples

The repository includes standalone example programs under `examples/`. The following snippets show the expected calling style directly.

Open a client:

```c
lc_client_config config;
lc_client *client;
lc_error error;
const char *endpoints[] = { "https://localhost:19441" };

lc_client_config_init(&config);
lc_error_init(&error);

config.endpoints = endpoints;
config.endpoint_count = 1;
config.client_bundle_path = "./client.pem";
config.default_namespace = "default";

if (lc_client_open(&config, &client, &error) != LC_OK) {
  fprintf(stderr, "lc_client_open failed: %s\n", error.message);
  lc_error_cleanup(&error);
  return 1;
}
```

Acquire a lease and update JSON state:

```c
lc_acquire_req acquire;
lc_release_req release;
lc_lease *lease;
lc_json json;

lc_acquire_req_init(&acquire);
lc_release_req_init(&release);
lc_json_init(&json);

acquire.key = "orders/42";
acquire.owner = "payments";
acquire.ttl_seconds = 60L;

if (client->acquire(client, &acquire, &lease, &error) != LC_OK) {
  fprintf(stderr, "acquire failed: %s\n", error.message);
  lc_error_cleanup(&error);
  client->close(client);
  return 1;
}

lc_json_set_static(&json, "{\"status\":\"processing\"}");

if (lease->update(lease, &json, &error) != LC_OK) {
  fprintf(stderr, "update failed: %s\n", error.message);
  lc_error_cleanup(&error);
}

if (lease->release(lease, &release, &error) != LC_OK) {
  fprintf(stderr, "release failed: %s\n", error.message);
  lc_error_cleanup(&error);
}

client->close(client);
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
scripts/deps.sh deps-host-debug
scripts/build.sh debug
scripts/build.sh e2e
scripts/build.sh x86_64-linux-gnu-release
scripts/test.sh unit
scripts/test.sh e2e
scripts/fuzz.sh
scripts/package.sh all
scripts/package-verify.sh
```

Unlike the primary Makefile workflow, these lower-level scripts do not generally provision dependency roots implicitly. Use them when you want direct preset control and are prepared to manage the prerequisite dependency tree yourself.

See <https://github.com/sa6mwa/liblockdc> for the full source code.
