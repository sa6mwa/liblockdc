# liblockdc Pouch vs lockd Disk Benchmarks

This module is an opt-in Go benchmark harness for pouch-vs-lockd-disk
performance and stress work. It is intentionally separate from the liblockdc
release gate.

The pouch cases call into C once per benchmark case. Setup, execution, and
timing are performed in C, and the Go benchmark reports the C-measured
`ns/op`. That keeps cgo bridge overhead out of the reported pouch operation
cost while still using Go's benchmark output format.

The lockd disk cases build and launch the latest pinned `pkt.systems/lockd`
command as a real external server process with the disk backend, then drive it
through the Go SDK. That makes this suite an end-to-end comparison harness, not
a replacement for the in-project C benchmarks under `bench/`.

Run from the repository root:

```sh
make benchmark-pouch-go
```

Fast iteration suite, capped by Go's test timeout at 30 seconds:

```sh
make benchmark-pouch-go-fast
```

Useful overrides:

```sh
make benchmark-pouch-go POUCH_GO_BENCH='IndexedLQL' POUCH_GO_BENCHTIME=10s POUCH_GO_SEED_ROWS=100000
```

The initial cases seed a local `pouch://` namespace, then run full-form LQL
selectors through the public client-facing query API. The paired lockd disk
cases seed a real lockd disk server with the same document shape and selector.
`POUCH_GO_SEED_ROWS` defaults to `10000`; use a smaller value for build/smoke
validation and a larger value for stress/perf runs.

- `BenchmarkPouchCIndexedLQLRows10k`
- `BenchmarkPouchCIndexedLQLKeys10k`
- `BenchmarkLockdDiskIndexedLQLRows10k`
- `BenchmarkLockdDiskIndexedLQLKeys10k`

The fast suite runs one pouch and one lockd disk case for each representative
storage scenario:

- state write
- state read
- indexed LQL document query
- indexed LQL key query
