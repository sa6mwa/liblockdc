#!/usr/bin/env python3
import re
import sys


BENCH_RE = re.compile(
    r"^(BenchmarkProduction(?:PouchPT|PouchCrypto|PouchCompression|PouchCryptoCompression|PouchDurablePT|PouchDurableCrypto|LockdDiskNoCrypto|LockdDiskCrypto|LockdDiskDurableNoCrypto|LockdDiskDurableCrypto)/\S+?)(?:-\d+)?\s+"
)

# These are public end-to-end operations whose Pouch and Go disk measurements
# have matching meanings. Aggregate wall time and Pouch-only C timing remain
# diagnostics, but index publication is a core observable operation.
CORE_METRICS = frozenset(
    (
        "acquire-one-ns/op",
        "update-one-ns/op",
        "release-one-ns/op",
        "stale-ns/op",
        "get-public-ns/op",
        "get-lease-ns/op",
        "attachment-write-ns/op",
        "attachment-read-ns/op",
        "queue-one-ns/op",
        "flush-intermediate-ns/op",
        "flush-final-ns/op",
        "flush-noop-ns/op",
        "flush-reopen-ns/op",
        "index-query-keys-ns/op",
        "index-query-keys-warm-ns/op",
        "index-query-docs-ns/op",
        "scan-query-keys-ns/op",
        "scan-query-docs-ns/op",
        "full-text-index-keys-ns/op",
        "full-text-scan-docs-ns/op",
        "restart-recovery-ns/op",
    )
)

DEFAULT_MIN_SPEEDUP = 1.25


def parse_float(value):
    try:
        return float(value)
    except ValueError:
        return None


def median(values):
    ordered = sorted(values)
    count = len(ordered)
    middle = count // 2
    if count % 2 == 1:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def parse(path):
    samples = {
        "PouchPT": {},
        "PouchCrypto": {},
        "PouchCompression": {},
        "PouchCryptoCompression": {},
        "PouchDurablePT": {},
        "PouchDurableCrypto": {},
        "LockdDiskNoCrypto": {},
        "LockdDiskCrypto": {},
        "LockdDiskDurableNoCrypto": {},
        "LockdDiskDurableCrypto": {},
    }
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = BENCH_RE.match(line)
            if match is None:
                continue
            fields = line.split()
            if len(fields) < 4:
                continue
            name = re.sub(r"-\d+$", "", fields[0])
            variant = None
            scenario = None
            for candidate in samples:
                prefix = "BenchmarkProduction%s/" % candidate
                if name.startswith(prefix):
                    variant = candidate
                    scenario = name[len(prefix) :]
                    break
            if variant is None or scenario is None:
                continue
            metrics = samples[variant].setdefault(scenario, {})
            idx = 2
            while idx + 1 < len(fields):
                value = parse_float(fields[idx])
                unit = fields[idx + 1]
                if value is not None:
                    metrics.setdefault(unit, []).append(value)
                idx += 2
    return {
        variant: {
            scenario: {unit: median(values) for unit, values in metrics.items()}
            for scenario, metrics in scenarios.items()
        }
        for variant, scenarios in samples.items()
    }


def parse_args(argv):
    min_speedup = DEFAULT_MIN_SPEEDUP
    mode = "default"
    args = argv[1:]
    while len(args) > 1:
        if len(args) >= 2 and args[0] == "--min-speedup":
            min_speedup = parse_float(args[1])
            args = args[2:]
            continue
        if len(args) >= 2 and args[0] == "--mode":
            mode = args[1]
            args = args[2:]
            continue
        break
    if (
        len(args) == 1
        and min_speedup is not None
        and min_speedup > 1.0
        and mode in ("default", "durable")
    ):
        return min_speedup, mode, args[0]
    print(
        "usage: pouch_benchmark_parity.py [--min-speedup <greater-than-1>] "
        "[--mode default|durable] "
        "<go-benchmark-output>",
        file=sys.stderr,
    )
    return None


def main(argv):
    args = parse_args(argv)
    if args is None:
        return 2
    min_speedup, mode, path = args
    variants = parse(path)
    failures = []
    # Go disk has no compression mode. Keep those Pouch variants in the
    # production report, but gate only transform-equivalent comparisons.
    if mode == "durable":
        comparisons = (
            ("PouchDurablePT", "LockdDiskDurableNoCrypto"),
            ("PouchDurableCrypto", "LockdDiskDurableCrypto"),
        )
    else:
        comparisons = (
            ("PouchPT", "LockdDiskNoCrypto"),
            ("PouchCrypto", "LockdDiskCrypto"),
        )
    for pouch_variant, disk_variant in comparisons:
        disk = variants[disk_variant]
        if not disk:
            failures.append("missing %s production benchmark results" % disk_variant)
            continue
        if not variants[pouch_variant]:
            failures.append("missing %s production benchmark results" % pouch_variant)
            continue
        for scenario, disk_metrics in sorted(disk.items()):
            pouch_metrics = variants[pouch_variant].get(scenario)
            if pouch_metrics is None:
                failures.append("%s missing scenario %s" % (pouch_variant, scenario))
                continue
            for metric in sorted(CORE_METRICS):
                disk_value = disk_metrics.get(metric)
                if disk_value is None:
                    failures.append(
                        "%s/%s missing core metric %s"
                        % (disk_variant, scenario, metric)
                    )
                    continue
                pouch_value = pouch_metrics.get(metric)
                if pouch_value is None:
                    failures.append(
                        "%s/%s missing metric %s" % (pouch_variant, scenario, metric)
                    )
                    continue
                max_pouch_value = disk_value / min_speedup
                if pouch_value > max_pouch_value:
                    speedup = disk_value / pouch_value if pouch_value > 0 else float("inf")
                    failures.append(
                        "%s/%s %s misses %.2fx speedup: pouch=%.0f disk=%.0f "
                        "speedup=%.3fx"
                        % (
                            pouch_variant,
                            scenario,
                            metric,
                            min_speedup,
                            pouch_value,
                            disk_value,
                            speedup,
                        )
                    )
    if failures:
        print("pouch benchmark parity gate failed:")
        for failure in failures:
            print("  - %s" % failure)
        return 1
    print(
        "pouch benchmark parity gate passed: %s mode, %.2fx minimum speedup"
        % (mode, min_speedup)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
