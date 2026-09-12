#!/usr/bin/env python3
"""Linux offline public-API replay diagnostic; no service or timing sleeps.

Build lockdc_pouch_replay_probe, then run this script with its path. Fixtures
are disposable and each seed/probe is a separate process. Reports logical I/O
(including cached reads), CPU and wall time separately for open, first access,
remaining keys and close. Assertions bound logical reads, not elapsed time.
This is not a power-loss simulation.
"""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("probe", type=Path)
    parser.add_argument("--keys", type=int, default=256)
    parser.add_argument("--updates", type=int, default=4)
    parser.add_argument("--namespaces", type=int, default=1)
    parser.add_argument("--encrypted", action="store_true")
    parser.add_argument("--unclean", action="store_true")
    parser.add_argument("--shared", action="store_true")
    parser.add_argument("--live-staged", action="store_true")
    parser.add_argument("--segment-bytes", type=int, default=1048576)
    parser.add_argument("--max-startup-read-amplification", type=int, default=64)
    args = parser.parse_args()
    if min(args.keys, args.updates, args.namespaces, args.segment_bytes,
           args.max_startup_read_amplification) <= 0:
        parser.error("fixture dimensions must be positive")
    if args.unclean and not args.shared:
        parser.error("unclean requires --shared to avoid exclusive lease expiry")
    if args.live_staged and not (args.unclean and args.shared):
        parser.error("live-staged requires --unclean --shared")
    # Stay inside the repository/build working directory, including key files.
    with tempfile.TemporaryDirectory(prefix="pouch-replay-", dir=".") as name:
        root = Path(name).resolve()
        endpoint = (
            f"pouch://{root / 'store'}?single_writer={'false' if args.shared else 'true'}"
            f"&durable_sync=false&segment_target_bytes={args.segment_bytes}"
        )
        common = [endpoint, str(args.keys), str(args.updates), str(args.namespaces),
                  str(root / "crypto.key") if args.encrypted else "-",
                  "staged" if args.live_staged else "unclean" if args.unclean else "clean"]
        for mode in ("seed", "probe", "probe"):
            size_before = sum(p.stat().st_size for p in (root / "store").rglob("*")
                              if p.is_file())
            result = subprocess.run([str(args.probe.resolve()), mode, *common],
                                    stdout=subprocess.PIPE, text=True, check=True)
            previous = None
            phases = {}
            for line in result.stdout.splitlines():
                phase, *values = line.split()
                values = list(map(float, values))
                if previous is not None:
                    phases[phase] = dict(zip(
                        ("wall_s", "cpu_s", "rchar", "syscr"),
                        (a - b for a, b in zip(values, previous))))
                previous = values
            size = sum(p.stat().st_size for p in (root / "store").rglob("*")
                       if p.is_file())
            print(json.dumps({"mode": mode, "store_bytes": size,
                              "phases": phases}), flush=True)
            if mode == "probe":
                # A deterministic work bound, not a wall-clock deadline. Leave
                # headroom for metadata while catching the reported >4000x I/O.
                startup_reads = phases["open"]["rchar"] + phases["first"]["rchar"]
                limit = size_before * args.max_startup_read_amplification + 1048576
                if startup_reads > limit:
                    raise AssertionError(f"startup read {startup_reads} bytes; bound {limit}")


if __name__ == "__main__":
    main()
