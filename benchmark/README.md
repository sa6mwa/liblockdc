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

Fast iteration suite, capped by an outer Go benchmark process timeout at 30
seconds:

```sh
make benchmark-pouch-go-fast
```

Medium comparison suite, capped by an outer Go benchmark process timeout at 3
minutes:

```sh
make benchmark-pouch-go-medium
```

Bounded 4096-document acceptance matrix, also capped at 3 minutes:

```sh
make benchmark-pouch-go-acceptance
```

Useful overrides:

```sh
make benchmark-pouch-go POUCH_GO_BENCH='IndexedLQL' POUCH_GO_BENCHTIME=10s POUCH_GO_SEED_ROWS=100000
make benchmark-pouch-go-medium POUCH_GO_MEDIUM_SCALE_ROWS=64,1024,10000
make benchmark-pouch-go-medium POUCH_GO_MEDIUM_SCALE_SCENARIOS=EqSparse,InTags,OrSparseOrFlag
make benchmark-pouch-go POUCH_GO_BENCH='PouchCMediumLQLKeys/Docs1024/index/RangeHalf' POUCH_GO_MEDIUM_SCALE_ROWS=1024 POUCH_GO_MEDIUM_SCALE_SCENARIOS=RangeHalf
```

The query cases seed a local `pouch://` namespace, then run full-form LQL
selectors through the public client-facing query API. The paired lockd disk
cases seed a real lockd disk server with the same document shape and equivalent
public LQL selector. `POUCH_GO_SEED_ROWS` defaults to `10000`; use a smaller
value for build/smoke validation and a larger value for stress/perf runs.

- `BenchmarkPouchCIndexedLQLDocuments10k`
- `BenchmarkPouchCIndexedLQLKeys10k`
- `BenchmarkLockdDiskIndexedLQLDocuments10k`
- `BenchmarkLockdDiskIndexedLQLKeys10k`

The medium LQL cases compare explicit `index` and `scan` engines over each
configured dataset size for document-return and key-return queries. They reuse
seeded pouch and lockd disk state within each document-count/return-mode group.
Pouch keeps a live C benchmark environment open for those sub-benchmarks so the
reported `c-ns/op` measures query behavior on a ready instance instead of
reopening the store for every scenario.
The default dataset sizes are `64,1024`; `POUCH_GO_MEDIUM_SCALE_ROWS` and
`POUCH_GO_MEDIUM_SCALE_SCENARIOS` can widen the matrix for dedicated perf runs.
The default scenario list is a representative bounded subset; pass the full
scenario list explicitly when doing exhaustive perf characterization.
`benchmark-pouch-go-acceptance` pins the scale suite to 4096 documents and the
`EqSparse`, `RangeHalf`, `InTags`, `ContainsMessage`, and `DateAfter` scenarios
across document-return, key-return, indexed, and scan engines. This keeps the
default gate inside the intended short iteration envelope while covering
representative low-match, broad-match, array-membership, text-search, and
residual date-filter cases.

- `BenchmarkPouchCMediumLQLDocuments`
- `BenchmarkPouchCMediumLQLKeys`
- `BenchmarkLockdDiskMediumLQLDocuments`
- `BenchmarkLockdDiskMediumLQLKeys`

The indexed LQL cases run the same scenario matrix for document-return and
key-return queries:

- `EqSparse`: one matching equality row.
- `EqDense`: half the namespace matches equality.
- `RangeHalf`: half the namespace matches a numeric lower bound.
- `InRegion`: scalar membership over `/region`.
- `InRegionSingle`: scalar membership over `/region` with one string value.
- `InTags`: explicit array-member membership over `/tags[]`.
- `ExistsFlag`: sparse field existence.
- `DateAfter`: date selector over `/created_at` with valid, invalid, and
  out-of-range values.
- `PrefixOwner`: sparse string prefix.
- `ContainsMessage`: sparse substring search.
- `AndEvenRange`: equality plus numeric range.
- `OrSparseOrFlag`: sparse equality unioned with field existence.

The fast suite runs one pouch and one lockd disk case for each representative
storage scenario:

- state write
- state read
- indexed LQL document query
- indexed LQL key query
