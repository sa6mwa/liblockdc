# Raw JSONL versus collection reads

`make compare-collection-read` is an optional Linux diagnostic. It belongs to
neither the benchmark matrix nor a test, prerelease, or release gate. Its
executable is excluded from the default build and is not installed.

```sh
make compare-collection-read
make compare-collection-read COLLECTION_COMPARE_ROWS=65536 COLLECTION_COMPARE_JSON_BYTES=1024
make compare-collection-read COLLECTION_COMPARE_ROWS=1024 COLLECTION_COMPARE_JSON_BYTES=65536
```

The latter two commands compare roughly 64 MiB of output with small and large
objects. Defaults are 8,192 objects of exactly 4,096 compact JSON bytes and
three paired samples. Override `COLLECTION_COMPARE_RUNS` (1..9) or
`COLLECTION_COMPARE_DIR` (an existing filesystem location; Make creates the
parent directory). Each fixture's output is limited to 1 GiB. Only the tool's
newly created temporary subdirectory is removed afterward, including on an
ordinary error or handled interrupt.

The diagnostic generates deterministic varied ASCII string values and writes
identical compact objects into a JSONL file and a Pouch collection. It checks
the complete outputs byte for byte before timing. Timed reads discard output
with byte accounting; the raw reader uses 128 KiB chunks. Collection reads
also verify record counts and completion. There is no LQL, metadata callback,
encryption, compression, deletion history, or compaction. Fixture construction,
validation, handle open/close, synchronization and eviction are outside timing.
Payload buffers are bounded; output is never collected in memory.
The isolated fixture uses Pouch's default single-writer mode, disables query
indexing and background compaction, and seeds without durable synchronization.
Every file is synchronized before eviction; this compares reads, not commits.

## What cold means

Before every timed read, the tool synchronizes the relevant fixture files,
requests page-aligned `POSIX_FADV_DONTNEED`, and queries file-page residency with
`mincore` without accessing the mapped data. It refuses a sample if any checked
page remains resident. Collection checks cover its canonical files and order/ID
projections in the documented collection partition. Root catalogs, inode/dentry
caches, live receiver metadata, filesystem readahead policy, and device or
hypervisor caches are not flushed. No system-wide cache operation or elevated
privilege is used. Page residency is a point-in-time observation, not a promise
that another actor cannot subsequently cache a page.

These limits follow the Linux interfaces: advice alone does not prove eviction,
and dirty or partial pages require special care. See
[posix_fadvise](https://man7.org/linux/man-pages/man2/posix_fadvise.2.html) and
[mincore](https://man7.org/linux/man-pages/man2/mincore.2.html).
The output includes process CPU time and the `/proc/self/io` `read_bytes` delta,
which reports storage-layer reads on block-backed filesystems. See
[proc_pid_io](https://man7.org/linux/man-pages/man5/proc_pid_io.5.html).
A sample with zero storage-read bytes is rejected because it would not
substantiate a disk-read comparison.

Sample order alternates between raw-first and collection-first. The summary
reports separate median times and the median of paired time ratios:

- `median_time_ratio = collection_seconds / raw_seconds`.
- `extra_time_fraction = median_time_ratio - 1`.
- `extra_time_pct = 100 * extra_time_fraction`.
- `throughput_retained_pct = 100 / median_time_ratio`.

For example, a 2x time ratio means 100% extra elapsed time and 50% retained
throughput. No ratio causes a pass/fail decision; failures indicate invalid
input, fixture/output errors, or an inability to establish the stated cache
conditions.

## Expected cost model

There is no fixed theoretical slowdown fraction. Let `R` be payload-plus-LF
bytes, `M` additional metadata/padding bytes actually fetched, `D` sequential
storage bandwidth, `N` object count, `c` fixed per-object SDK work, `h` per-byte
integrity work, and `S` extra seek/setup time. A rough additive model is:

```text
T_raw        ~= R / D
T_collection ~= (R + M) / D + N*c + R*h + S
extra_time_fraction ~= M/R + N*c*D/R + h*D + S*D/R
```

I/O and CPU overlap and filesystem readahead change the actual relationship.
Small objects emphasize metadata, syscalls, hashing setup and sink boundaries;
larger objects amortize fixed work. Collection reads still perform integrity
verification and ordered-state handling that a raw byte read does not. Total
directory size is not `M`: ID tables and other auxiliary files need not be read
in full. Use the reported storage reads and CPU times to interpret results on
the tested machine rather than claiming a universal percentage.
