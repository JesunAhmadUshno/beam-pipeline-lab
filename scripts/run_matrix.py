"""Drive the measured matrix: for each scale, the uncounted warm-up
pass (run 0) then timed runs 1 to 5, each in its own subprocess.

Peak memory per (scale, run): the run's process tree is sampled every
0.25 s and the current RSS of every process in the tree is summed;
the peak of that sum is recorded. GNU time is not installed on the
machine of record, and the Beam runner spawns worker processes, so a
tree sample is the honest secondary measure (DECISIONS.md D5). RSS
carries no hypothesis; it is reported as secondary evidence only.

Appends to results/run_log.jsonl (via src/run_beam.py) and to
results/rss_log.jsonl (here). A failed subprocess is recorded with
its exit code and the driver continues with the next run, so a
resource failure on one scale cannot hide the other scale's
evidence; the failure shows up in the checksum matrix as missing
cells and in the verdict logic of make_results.py.

Usage:
    python scripts/run_matrix.py --scales 0.1 1
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _tree_pids(root_pid):
    """All descendant PIDs of root_pid (including itself), via /proc."""
    children = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", "rb") as f:
                # comm may contain spaces/parens; ppid follows the last ')'.
                data = f.read().decode("utf-8", "replace")
            ppid = int(data.rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            continue
        children.setdefault(ppid, []).append(int(entry))
    seen, stack = [], [root_pid]
    while stack:
        pid = stack.pop()
        seen.append(pid)
        stack.extend(children.get(pid, []))
    return seen


def _rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def run_sampled(cmd):
    """Run cmd, sampling the peak aggregate RSS of its process tree."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    peak = 0
    while proc.poll() is None:
        total = sum(_rss_kb(pid) for pid in _tree_pids(proc.pid))
        peak = max(peak, total)
        time.sleep(0.25)
    stdout, stderr = proc.communicate()
    return proc.returncode, stdout, stderr, peak


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
                str(ROOT / ".venv" / "bin" / "python"),
                str(ROOT / "src" / "run_beam.py"),
                "--sf", "%g" % sf, "--run", str(run),
            ]
            started = time.time()
            exit_code, stdout, stderr, peak_kb = run_sampled(cmd)
            wall = time.time() - started
            record = {
                "sf": sf, "run": run,
                "exit_code": exit_code,
                "wall_seconds": round(wall, 2),
                "max_rss_kb": int(peak_kb),
            }
            with rss_log.open("a") as f:
                f.write(json.dumps(record) + "\n")
            tail = (stdout.strip().splitlines() or [""])[-1]
            print(f"sf{sf:g} run{run}: exit={exit_code} "
                  f"wall={wall:.1f}s peak_rss_kb={peak_kb} | {tail}",
                  flush=True)
            if exit_code != 0:
                err_tail = "\n".join(stderr.strip().splitlines()[-5:])
                print(f"  STDERR tail:\n{err_tail}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
