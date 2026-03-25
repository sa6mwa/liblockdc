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
scripts/dev-up.sh
```

That wrapper now just drives the same compose stack and waits for the generated bundles/socket to appear. It no longer performs out-of-band bootstrap work.

Plain `nerdctl compose up -d` will:
- start MinIO
- create the `lockd-client-s3` bucket via `minio-init`
- have the one-shot `lockd auth new ...` init services generate the certificate bundles on first start
- start four `lockd` instances

## Stop

```bash
scripts/dev-down.sh
```

## Inspect

```bash
scripts/dev-ps.sh
scripts/compose.sh logs -f lockd-disk-a
scripts/deps.sh deps-host-debug
scripts/dev-e2e.sh
make build
make test
make fuzz
scripts/fuzz.sh
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
scripts/dev-up.sh
```

## Generated client bundles

Those files are generated automatically by the `lockd` service entrypoints:

- `devenv/volumes/lockd-disk-a-config/client.pem`
- `devenv/volumes/lockd-s3-config/client.pem`

Those bundles contain the CA certificate, client certificate, and private key and are intended to be used directly by the C client.

## Build and test

Normal local flows:

```bash
scripts/deps.sh deps-host-debug
make build
make test
make test-e2e
make test-all
make coverage
make fuzz
scripts/fuzz.sh
```

Equivalent CMake preset flows:

```bash
scripts/deps.sh deps-host-debug
cmake --preset debug
cmake --build --preset debug

cmake --preset e2e
cmake --build --preset e2e
ctest --preset debug
ctest --preset e2e

cmake --preset fuzz
cmake --build --preset fuzz
ctest --preset fuzz
```

Dependency bootstrap is split from the low-level script workflow:

- `scripts/deps.sh deps-host-debug` builds the host debug third-party stack into `./.cache/deps/host-debug`
- it skips dependency work if the dependency manifest and artifacts still match
- the primary Makefile workflow provisions the required dependency roots automatically for the common host and release paths
- the low-level `scripts/build.sh` and `scripts/test.sh` entry points assume the required dependency tree already exists

## Packaging

Release packaging is split by ABI and by consumption model:

- `liblockdc-<version>-<abi>.tar.gz`
  - runtime bundle
  - ships `liblockdc` headers, bundled dependency headers, `liblockdc.so*`, and the bundled shared runtime dependencies
- `liblockdc-<version>-<abi>-dev.tar.gz`
  - distro-style development bundle
  - ships `liblockdc` headers, bundled dependency headers, `liblockdc.a`, bundled dependency static archives, the unversioned `liblockdc.so` linker name, and package metadata

Normal packaging flows:

```bash
make release
scripts/package-verify.sh
```

Those flows also require the ABI-specific dependency roots to already exist. They do not invoke `scripts/deps.sh` implicitly.

By default `scripts/package.sh` now builds the full release matrix:

- `x86_64-linux-gnu`
- `x86_64-linux-musl`
- `aarch64-linux-gnu`
- `aarch64-linux-musl`
- `armhf-linux-gnu`
- `armhf-linux-musl`

## Fuzzing

The repo also carries a separate clang/libFuzzer build for hermetic parser and stream hardening:

```bash
scripts/fuzz.sh
```

Or via presets:

```bash
cmake --preset fuzz
cmake --build --preset fuzz
ctest --preset fuzz
```

The fuzz build:

- uses `clang`
- enables `libFuzzer`, `ASan`, and `UBSan`
- keeps fuzzing out of the normal package/test presets
- runs bounded smoke fuzzing under CTest so CI/local verification stays finite

Current fuzz targets cover:

- streamed source/sink/json surfaces
- client bundle parsing during `lc_client_open()`
- attachment response decoding
- queue subscribe meta decoding

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

Or just run:

```bash
scripts/dev-e2e.sh
```

`dev-e2e.sh` exports the default bundle/socket paths for this repo layout before invoking CMake/CTest.

The current devenv certs are generated for local development only, so the e2e client enables `lc_client_config.insecure_skip_verify=1` for the mTLS test nodes only. The normal client default remains strict verification.
