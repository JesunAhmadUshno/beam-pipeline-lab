"""Compute canonical checksums for Beam runs 1 and 5 at one scale and
merge them into results/checksums.json (B2's record format).

Usage:
    python src/collect_checksums.py --sf 0.1
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from checksum import canonical_checksum  # noqa: E402
from spec import ORDER  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, required=True)
    ap.add_argument("--runs", type=int, nargs="+", default=[1, 5])
    args = ap.parse_args()

    label = "%g" % args.sf
    out_file = ROOT / "results" / "checksums.json"
    existing = {"records": []}
    if out_file.exists():
        existing = json.loads(out_file.read_text())
    kept = [
        r for r in existing["records"]
        if not (r["sf"] == args.sf and r["run"] in args.runs)
    ]

    for run in args.runs:
        for key in ORDER:
            path = ROOT / "outputs" / "beam" / f"sf{label}" / f"run{run}" / f"{key}.parquet"
            if not path.exists():
                kept.append({
                    "arm": "beam", "sf": args.sf, "transformation": key,
                    "run": run, "sha256": None, "rows": None,
                    "rows_checked": 0, "partial": True, "missing": True,
                })
                print(f"sf{label} run{run} {key}: MISSING output", flush=True)
                continue
            digest, rows = canonical_checksum(str(path), key)
            kept.append({
                "arm": "beam", "sf": args.sf, "transformation": key,
                "run": run, "sha256": digest, "rows": rows,
                "rows_checked": rows, "partial": False,
            })
            print(f"sf{label} run{run} {key}: {digest[:16]} rows={rows}", flush=True)

    kept.sort(key=lambda r: (r["sf"], r["run"], r["transformation"]))
    out_file.write_text(json.dumps({"records": kept}, indent=2) + "\n")


if __name__ == "__main__":
    main()
