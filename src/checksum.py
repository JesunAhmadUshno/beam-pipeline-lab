"""Canonical ordered checksum for a transformation output.

Adapted from JesunAhmadUshno/data-engineering-scale-lab src/checksum.py
(Apache License 2.0, same author). The checksum is SHA-256 over the
output rows ordered by all output columns, exported through one fixed
writer (DuckDB COPY to CSV with fixed options), so the same logical
output gives the same bytes whichever engine produced it. Using B2's
exact method is what makes this study's H2 a comparison of data with
B2's record, not of file formats.

Usage:
    python src/checksum.py --path 'outputs/beam/sf0.1/run1/t2.parquet' --transformation t2
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spec import BY_KEY  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def canonical_checksum(path_glob, transformation, tmp_dir=None):
    spec = BY_KEY[transformation]
    cols = spec["columns"]
    col_list = ", ".join(cols)
    tmp = Path(tmp_dir or (ROOT / "results" / ".checksum_tmp"))
    tmp.mkdir(parents=True, exist_ok=True)
    csv_file = tmp / f"checksum_{transformation}.csv"
    con = duckdb.connect()
    con.execute("SET memory_limit='2.5GB'")
    con.execute(f"SET temp_directory='{tmp}'")
    con.execute(
        f"""
        COPY (
            SELECT {col_list} FROM read_parquet('{path_glob}')
            ORDER BY {col_list}
        ) TO '{csv_file}' (FORMAT CSV, HEADER false, DELIMITER '|')
        """
    )
    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{path_glob}')"
    ).fetchone()[0]
    con.close()
    h = hashlib.sha256()
    with csv_file.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    csv_file.unlink()
    return h.hexdigest(), rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    ap.add_argument("--transformation", required=True)
    args = ap.parse_args()
    digest, rows = canonical_checksum(args.path, args.transformation)
    print(json.dumps({"sha256": digest, "rows": rows}))


if __name__ == "__main__":
    main()
