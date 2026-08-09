# liblockdc devenv

This repo carries a real `lockd` environment for client development and e2e work.

Preferred runtime:
- `nerdctl compose`

Fallback:
- `docker compose`

The wrapper scripts choose `nerdctl` first and fall back to Docker automatically.

## Start

```bash
nerdctl compose up -d
```

That will:
- start MinIO
- create the `lockd-client-s3` bucket
- have the one-shot `lockd auth new ...` init services generate the certificate bundles on first start
- start four `lockd` instances

Convenience wrapper:

```bash
make dev-up
```

That wrapper now just drives the same compose stack and waits for the generated bundles/socket to appear. It no longer performs out-of-band bootstrap work.

Plain `nerdctl compose up -d` will:
- start MinIO
- create the `lockd-client-s3` bucket via `minio-init`
- have the one-shot `lockd auth new ...` init services generate the certificate bundles on first start
- start four `lockd` instances

## Stop

```bash
make dev-down
```

## Inspect

```bash
make dev-ps
make dev-logs
make test-e2e
```

## Topology

- `lockd-disk-a`
  - `https://localhost:19441`
  - store: shared `disk:///storage`
  - config: shared `./devenv/volumes/lockd-disk-a-config`
- `lockd-disk-b`
  - `https://localhost:19442`
  - store: shared `disk:///storage`
  - config: shared `./devenv/volumes/lockd-disk-a-config`
- `lockd-s3`
  - `https://localhost:19443`
  - store: `s3://minio:9000/lockd-client-s3/liblockdc?insecure=1&path-style=1`
- `lockd-mem`
  - `unix://.../devenv/volumes/lockd-mem-run/lockd.sock`
  - store: `mem://`
  - transport: HTTP over Unix domain socket
  - mTLS: disabled on purpose so the client exercises the UDS path directly

All `lockd` containers are capped at `150m`.

Override host ports if needed with:

```bash
LOCKDC_DISK_A_PORT=20441 \
LOCKDC_DISK_B_PORT=20442 \
LOCKDC_S3_PORT=20443 \
LOCKDC_MINIO_API_PORT=20000 \
LOCKDC_MINIO_CONSOLE_PORT=20001 \
make dev-up
```

## Generated client bundles

Those files are generated automatically by the `lockd` service entrypoints:

- `devenv/volumes/lockd-disk-a-config/client.pem`
- `devenv/volumes/lockd-s3-config/client.pem`

Those bundles contain the CA certificate, client certificate, and private key and are intended to be used directly by the C client.

## Build and test

Normal local flows:

```bash
make build
make test
make test-e2e
make test-all
make coverage
make fuzz-smoke
```

Equivalent CMake preset flows:

```bash
cmake --preset debug
cmake --build --preset debug

cmake --preset e2e
cmake --build --preset e2e
ctest --preset debug
ctest --preset e2e
```

The normal Make and CMake workflows provision the pinned Bootlin compiler
collection and matching dependency roots automatically. Linux builds never
fall back to host or distro compilers. Toolchains and verified dependency
archives are shared outside the checkout under `CPKT_TOOLCHAIN_CACHE` and
`CPKT_DEPENDENCY_CACHE`; repository-local `.cache/` remains disposable build
and staging state. Low-level scripts are implementation helpers, not the
normal developer command surface.

## Packaging

Release packaging is split by ABI:

- `liblockdc-<version>-<abi>.tar.gz`
  - combined release bundle
  - ships `liblockdc` headers, bundled dependency headers, `liblockdc.so*`, `liblockdc.a`, bundled third-party shared and static libraries, and package metadata

Normal packaging flows:

```bash
make release
make package-verify
```

`make release` is the clean-slate release gate. It removes generated state,
runs the release version contract, then executes the same proof graph as
`make prerelease`: formatting, debug sanitizer tests, Valgrind, fuzz smoke,
lockd e2e, Lua tests, benchmark gates, and the release matrix. The primary
Makefile release flow provisions the dependency roots it needs.

`make package-verify` runs the full release matrix:

- `x86_64-linux-gnu`
- `x86_64-linux-musl`
- `aarch64-linux-gnu`
- `aarch64-linux-musl`
- `armhf-linux-gnu`
- `armhf-linux-musl`

## Fuzzing

The repository uses the lifecycle-owned fuzz build for parser, record, stream,
and storage-state hardening:

```bash
make fuzz-smoke
```

Or via the fuzz preset:

```bash
cmake --preset fuzz
cmake --build --preset fuzz
ctest --preset fuzz
```

The fuzz build uses pinned AFL++ GCC-plugin instrumentation over the native
x86_64 Bootlin collection. It remains host-native only, keeps fuzzing outside
ordinary package/test presets, and runs bounded committed-corpus smoke jobs
through `make fuzz-smoke`.

Current fuzz targets cover:

- streamed source/sink/json surfaces
- client bundle parsing during `lc_client_open()`
- attachment response decoding
- queue subscribe meta decoding
- streamed query-key decoding
- local mutate parsing and application

## Build and run the e2e suite

The client e2e suite is a separate CTest layer. Keep it enabled with:

```bash
cmake --preset e2e
cmake --build --preset e2e
ctest --preset e2e
```

Unit coverage excludes the `e2e` label on purpose. The TLS/client bundle/libcurl path is only covered once the e2e layer runs against the local mTLS devenv.

The e2e binary defaults to this devenv layout, but you can override endpoints and bundle paths with:

- `LOCKDC_E2E_DISK_ENDPOINT`
- `LOCKDC_E2E_DISK_BUNDLE`
- `LOCKDC_E2E_S3_ENDPOINT`
- `LOCKDC_E2E_S3_BUNDLE`
- `LOCKDC_E2E_MEM_SOCKET`

Or just run the lifecycle entrypoint:

```bash
make test-e2e
```

`make test-e2e` exports the default bundle/socket paths for this repo layout,
then invokes the e2e CMake/CTest flow.

The current devenv certs are generated for local development only, so the e2e client enables `lc_client_config.insecure_skip_verify=1` for the mTLS test nodes only. The normal client default remains strict verification.
