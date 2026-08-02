#!/usr/bin/env python3
import re
import sys


BENCH_RE = re.compile(
    r"^(BenchmarkProduction(?:PouchPT|PouchCrypto|PouchCompression|PouchCryptoCompression|LockdDiskNoCrypto)/\S+?)(?:-\d+)?\s+"
)

# These are public end-to-end operations whose Pouch and Go disk measurements
# have matching meanings. Aggregate wall time and lifecycle/flush diagnostics
# remain in the report, but are not independent parity gates.
CORE_METRICS = frozenset(
    (
        "acquire-one-ns/op",
        "update-one-ns/op",
        "release-one-ns/op",
        "get-public-ns/op",
        "get-lease-ns/op",
        "attachment-ns/op",
        "queue-one-ns/op",
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
        "LockdDiskNoCrypto": {},
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


def main(argv):
    if len(argv) != 2:
        print("usage: pouch_benchmark_parity.py <go-benchmark-output>", file=sys.stderr)
        return 2
    variants = parse(argv[1])
    failures = []
    disk = variants["LockdDiskNoCrypto"]
    if not disk:
        failures.append("missing LockdDiskNoCrypto production benchmark results")
    for pouch_variant in (
        "PouchPT",
        "PouchCrypto",
        "PouchCompression",
        "PouchCryptoCompression",
    ):
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
                        "LockdDiskNoCrypto/%s missing core metric %s"
                        % (scenario, metric)
                    )
                    continue
                pouch_value = pouch_metrics.get(metric)
                if pouch_value is None:
                    failures.append(
                        "%s/%s missing metric %s" % (pouch_variant, scenario, metric)
                    )
                    continue
                if pouch_value >= disk_value:
                    ratio = pouch_value / disk_value if disk_value > 0 else float("inf")
                    failures.append(
                        "%s/%s %s slower-or-equal: pouch=%.0f disk=%.0f ratio=%.3f"
                        % (pouch_variant, scenario, metric, pouch_value, disk_value, ratio)
                    )
    if failures:
        print("pouch benchmark parity gate failed:")
        for failure in failures:
            print("  - %s" % failure)
        return 1
    print("pouch benchmark parity gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
