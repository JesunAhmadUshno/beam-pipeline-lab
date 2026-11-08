"""Drive the measured matrix: for each scale, the uncounted warm-up
pass (run 0) then timed runs 1 to 5, each in its own subprocess under
/usr/bin/time so peak resident memory is recorded per (scale, run).

Appends to results/run_log.jsonl (via src/run_beam.py) and to
results/rss_log.jsonl (here). A failed subprocess is recorded in
results/rss_log.jsonl with its exit code and the run continues with
the next scale, so a resource failure on one scale cannot hide the
other scale's evidence; the failure shows up in the checksum matrix
as missing cells and in the verdict logic of make_results.py.

Usage:
    python scripts/run_matrix.py --scales 0.1 1
"""

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RSS_RE = re.compile(r"Maximum resident set size \(kbytes\): (\d+)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", type=float, nargs="+", default=[0.1, 1.0])
    ap.add_argument("--runs", type=int, default=5)
    args = ap.parse_args()

    rss_log = ROOT / "results" / "rss_log.jsonl"
    rss_log.parent.mkdir(parents=True, exist_ok=True)

    for sf in args.scales:
        for run in range(0, args.runs + 1):
            cmd = [
                "/usr/bin/time", "-v",
                str(ROOT / ".venv" / "bin" / "python"),
                str(ROOT / "src" / "run_beam.py"),
                "--sf", "%g" % sf, "--run", str(run),
            ]
            started = time.time()
            proc = subprocess.run(cmd, capture_output=True, text=True)
            wall = time.time() - started
            match = RSS_RE.search(proc.stderr)
            record = {
                "sf": sf, "run": run,
                "exit_code": proc.returncode,
                "wall_seconds": round(wall, 2),
                "max_rss_kb": int(match.group(1)) if match else None,
            }
            with rss_log.open("a") as f:
                f.write(json.dumps(record) + "\n")
            tail = (proc.stdout.strip().splitlines() or [""])[-1]
            print(f"sf{sf:g} run{run}: exit={proc.returncode} "
                  f"wall={wall:.1f}s rss_kb={record['max_rss_kb']} | {tail}",
                  flush=True)
            if proc.returncode != 0:
                err_tail = "\n".join(proc.stderr.strip().splitlines()[-5:])
                print(f"  STDERR tail:\n{err_tail}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
