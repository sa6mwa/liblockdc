# liblockdc pouch comparison benchmarks

This module is intentionally outside the CMake release gate. It is used by the
root `benchmark-pouch-go*` Makefile targets for opt-in pouch performance and
stress work.

The pouch benchmark path calls into C through cgo and reports C-measured query
time as `c-ns/op`, so the cgo bridge is not the metric used for pouch query
latency. The Go benchmark `ns/op` still includes harness/setup work and is only
useful as a coarse runner signal.
