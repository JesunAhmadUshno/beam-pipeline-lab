"""Generate RESULTS.md and results/results.md from the run log, the
RSS log, the setup samples, and the checksum records.

Verdict logic is PREREGISTRATION.md Sections 2 and 7, implemented
literally. Nothing numeric in the generated files is hand-typed.

Usage:
    python src/make_results.py
"""

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comparison import B2_MEANS, B2_SETUP, B2_TOTALS  # noqa: E402
from spec import ORDER  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SCALES = [0.1, 1.0]
PRIMARY_SF = 1.0


def load_jsonl(name):
    path = ROOT / "results" / name
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_json(name):
    path = ROOT / "results" / name
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def fmt(x, digits=2):
    return "n/a" if x is None else f"{x:.{digits}f}"


def main():
    run_log = [r for r in load_jsonl("run_log.jsonl") if r.get("arm") == "beam"]
    rss_log = load_jsonl("rss_log.jsonl")
    setup_samples = load_jsonl("setup_log.jsonl")
    checksums = load_json("checksums.json").get("records", [])
    reference = {
        (r["sf"], r["transformation"]): r
        for r in load_json("reference_checksums.json").get("records", [])
    }
    versions = load_json("versions.json")

    # Timing stats per (sf, transformation): runs 1..5 only.
    times = {}
    for sf in SCALES:
        for key in ORDER:
            vals = [r["seconds"] for r in run_log
                    if r["sf"] == sf and r["transformation"] == key
                    and 1 <= r["run"] <= 5]
            times[(sf, key)] = {
                "n": len(vals),
                "mean": statistics.mean(vals) if vals else None,
                "sd": statistics.stdev(vals) if len(vals) > 1 else (0.0 if vals else None),
                "runs": vals,
            }

    totals = {}
    for sf in SCALES:
        if all(times[(sf, k)]["mean"] is not None for k in ORDER):
            totals[sf] = sum(times[(sf, k)]["mean"] for k in ORDER)
        else:
            totals[sf] = None

    def ratio(sf, key):
        beam_mean = times[(sf, key)]["mean"]
        spark_mean = B2_MEANS[sf]["spark"][key]
        return None if beam_mean is None else beam_mean / spark_mean

    ratios = {(sf, k): ratio(sf, k) for sf in SCALES for k in ORDER}

    # Checksum matrix.
    cells = {}
    for sf in SCALES:
        for key in ORDER:
            rec1 = next((r for r in checksums if r["sf"] == sf
                         and r["transformation"] == key and r["run"] == 1), None)
            rec5 = next((r for r in checksums if r["sf"] == sf
                         and r["transformation"] == key and r["run"] == 5), None)
            ref = reference.get((sf, key), {})
            if rec1 is None or rec1.get("sha256") is None:
                status = "missing"
            elif rec1["sha256"] != ref.get("sha256"):
                status = "mismatch"
            else:
                status = "match"
            deterministic = None
            if rec1 and rec5 and rec1.get("sha256") and rec5.get("sha256"):
                deterministic = rec1["sha256"] == rec5["sha256"]
            cells[(sf, key)] = {
                "status": status, "rows": None if rec1 is None else rec1.get("rows"),
                "sha256": None if rec1 is None else rec1.get("sha256"),
                "ref_sha256": ref.get("sha256"), "deterministic": deterministic,
            }

    completed = [c for c in cells.values() if c["status"] != "missing"]
    mismatches = [c for c in cells.values() if c["status"] == "mismatch"]
    det_breaks = [c for c in cells.values() if c["deterministic"] is False]

    # Verdicts (PREREGISTRATION.md Section 2).
    if totals[PRIMARY_SF] is None:
        h1 = "INCONCLUSIVE"
        h1_ratio = None
    else:
        h1_ratio = totals[PRIMARY_SF] / B2_TOTALS[PRIMARY_SF]["spark"]
        h1 = "CONFIRMED" if h1_ratio >= 2.0 else "REFUTED"

    if mismatches or det_breaks:
        h2 = "REFUTED"
    elif len(completed) == len(cells) and all(
            c["deterministic"] is not False for c in cells.values()):
        h2 = "CONFIRMED"
    else:
        h2 = "INCONCLUSIVE"

    sf1_ratios = [ratios[(PRIMARY_SF, k)] for k in ORDER]
    if any(r is None for r in sf1_ratios):
        h3, h3_spread = "INCONCLUSIVE", None
    else:
        h3_spread = max(sf1_ratios) / min(sf1_ratios)
        h3 = "CONFIRMED" if h3_spread >= 2.0 else "REFUTED"
    sf01_ratios = [ratios[(0.1, k)] for k in ORDER if ratios[(0.1, k)] is not None]
    sf01_spread = (max(sf01_ratios) / min(sf01_ratios)) if len(sf01_ratios) == 6 else None

    setup_median = (statistics.median([s["setup_seconds"] for s in setup_samples])
                    if setup_samples else None)
    preroll = {}
    for sf in SCALES:
        vals = [r["seconds"] for r in run_log
                if r["sf"] == sf and r["transformation"] == "setup" and 1 <= r["run"] <= 5]
        preroll[sf] = statistics.median(vals) if vals else None
    peak_rss = {}
    for sf in SCALES:
        vals = [r["max_rss_kb"] for r in rss_log
                if r["sf"] == sf and 1 <= r["run"] <= 5 and r["max_rss_kb"]]
        peak_rss[sf] = max(vals) if vals else None
    failed_runs = [r for r in rss_log if r["exit_code"] != 0]

    verdicts = {"H1": h1, "H2": h2, "H3": h3}

    # ---- RESULTS.md (root): verdicts and headlines.
    lines = []
    lines.append("# Results")
    lines.append("")
    lines.append("Generated by src/make_results.py from the run log and the checksum "
                 "records. Full tables: results/results.md. All data derived from "
                 "TPC-H; see LICENSES.md. B2 figures are the published record of "
                 "JesunAhmadUshno/data-engineering-scale-lab on this same machine.")
    lines.append("")
    lines.append("## Verdicts")
    lines.append("")
    if h1_ratio is not None:
        lines.append(f"- H1 (portability tax at SF1): **{h1}**. Beam total "
                     f"{fmt(totals[PRIMARY_SF])} s vs B2 Spark local total "
                     f"{fmt(B2_TOTALS[PRIMARY_SF]['spark'])} s, ratio {fmt(h1_ratio, 3)} "
                     f"against the frozen line of 2.0.")
    else:
        lines.append(f"- H1 (portability tax at SF1): **{h1}**. At least one SF1 "
                     "transformation did not complete, so no Beam total exists.")
    n_match = sum(1 for c in cells.values() if c["status"] == "match")
    n_missing = sum(1 for c in cells.values() if c["status"] == "missing")
    lines.append(f"- H2 (exact reproduction): **{h2}**. {n_match} of {len(cells)} cells "
                 f"match the B2 run-1 checksum, {len(mismatches)} mismatch, "
                 f"{n_missing} did not complete; run 5 reproduces run 1 on "
                 f"{sum(1 for c in cells.values() if c['deterministic'])} cells, "
                 f"with {len(det_breaks)} determinism breaks.")
    if h3_spread is not None:
        slow = max(ORDER, key=lambda k: ratios[(PRIMARY_SF, k)])
        fast = min(ORDER, key=lambda k: ratios[(PRIMARY_SF, k)])
        lines.append(f"- H3 (the tax is uneven, SF1): **{h3}**. Largest per-transformation "
                     f"ratio {fmt(max(sf1_ratios), 2)} ({slow}), smallest "
                     f"{fmt(min(sf1_ratios), 2)} ({fast}), spread {fmt(h3_spread, 2)} "
                     f"against the frozen line of 2.0.")
    else:
        lines.append(f"- H3 (the tax is uneven, SF1): **{h3}**. SF1 is incomplete, so "
                     "no per-transformation spread exists.")
    lines.append("")
    lines.append("## Headline totals (sum of the six transformation means, seconds)")
    lines.append("")
    lines.append("| Scale | Beam (this study) | B2 Spark local | B2 DuckDB | Beam / Spark |")
    lines.append("| --- | --- | --- | --- | --- |")
    for sf in SCALES:
        total_ratio = None if totals[sf] is None else totals[sf] / B2_TOTALS[sf]["spark"]
        lines.append(f"| SF{sf:g} | {fmt(totals[sf])} | {fmt(B2_TOTALS[sf]['spark'])} | "
                     f"{fmt(B2_TOTALS[sf]['duckdb'])} | {fmt(total_ratio, 3)} |")
    lines.append("")
    lines.append(f"Setup overhead, reported separately as in B2: the Beam fresh-process "
                 f"setup (import plus an empty pipeline run, median of "
                 f"{len(setup_samples)} samples) is {fmt(setup_median)} s. The per-run "
                 f"runner-service start inside the matrix (DECISIONS.md D6) medians "
                 f"{fmt(preroll[0.1])} s at SF0.1 and {fmt(preroll[1.0])} s at SF1. "
                 f"B2's analogues: Spark session {fmt(B2_SETUP['spark_sf0.1'])} s, "
                 f"DuckDB connect {fmt(B2_SETUP['duckdb_sf0.1'])} s.")
    lines.append("")
    (ROOT / "RESULTS.md").write_text("\n".join(lines) + "\n")

    # ---- results/results.md: the full record.
    out = []
    out.append("# Results (generated)")
    out.append("")
    out.append("Generated by src/make_results.py. Never hand-edited. All data derived "
               "from TPC-H; see LICENSES.md.")
    out.append("")
    out.append("## Versions")
    out.append("")
    for k, v in versions.items():
        out.append(f"- {k}: {v}")
    out.append("")
    out.append("## Setup overhead (seconds, reported separately)")
    out.append("")
    out.append(f"- Beam fresh-process setup, median of {len(setup_samples)} samples: "
               f"{fmt(setup_median)}")
    for sf in SCALES:
        out.append(f"- Beam per-run runner-service start at SF{sf:g} (median of runs 1 to 5): "
                   f"{fmt(preroll[sf])}")
    out.append(f"- B2 record: Spark session {fmt(B2_SETUP['spark_sf0.1'])}, "
               f"DuckDB connect {fmt(B2_SETUP['duckdb_sf0.1'])} (SF0.1)")
    out.append("")
    for sf in SCALES:
        out.append(f"## SF{sf:g} timings (seconds, 5 runs, one warm-up pass, one setup pre-roll per run)")
        out.append("")
        out.append("| Transformation | Beam mean (SD, n) | B2 Spark mean | Beam / Spark | B2 DuckDB mean | Rows out |")
        out.append("| --- | --- | --- | --- | --- | --- |")
        for key in ORDER:
            t = times[(sf, key)]
            rows = cells[(sf, key)]["rows"]
            out.append(f"| {key} | {fmt(t['mean'])} ({fmt(t['sd'])}, n={t['n']}) | "
                       f"{fmt(B2_MEANS[sf]['spark'][key])} | {fmt(ratios[(sf, key)], 3)} | "
                       f"{fmt(B2_MEANS[sf]['duckdb'][key])} | {rows if rows is not None else 'n/a'} |")
        total_ratio = None if totals[sf] is None else totals[sf] / B2_TOTALS[sf]["spark"]
        out.append(f"| total | {fmt(totals[sf])} | {fmt(B2_TOTALS[sf]['spark'])} | "
                   f"{fmt(total_ratio, 3)} | {fmt(B2_TOTALS[sf]['duckdb'])} |  |")
        out.append("")
        out.append("Raw Beam run times (runs 1 to 5):")
        out.append("")
        for key in ORDER:
            vals = ", ".join(fmt(v, 3) for v in times[(sf, key)]["runs"])
            out.append(f"- {key}: {vals if vals else 'no completed run'}")
        out.append("")
    out.append("## Per-transformation ratio spread (H3)")
    out.append("")
    out.append(f"- SF1 spread (primary): {fmt(h3_spread, 3)}")
    out.append(f"- SF0.1 spread (secondary): {fmt(sf01_spread, 3)}")
    out.append("")
    out.append("## Checksum matrix (H2)")
    out.append("")
    out.append("| Cell | Status vs B2 run-1 | Rows | Run 5 = run 1 | Beam run-1 SHA-256 (prefix) |")
    out.append("| --- | --- | --- | --- | --- |")
    for sf in SCALES:
        for key in ORDER:
            c = cells[(sf, key)]
            sha = (c["sha256"] or "")[:16] or "n/a"
            det = "n/a" if c["deterministic"] is None else str(c["deterministic"])
            out.append(f"| sf{sf:g}:{key} | {c['status']} | "
                       f"{c['rows'] if c['rows'] is not None else 'n/a'} | {det} | {sha} |")
    out.append("")
    out.append("## Peak process-tree RSS per scale (secondary evidence, no hypothesis)")
    out.append("")
    for sf in SCALES:
        peak = peak_rss[sf]
        out.append(f"- SF{sf:g}: {('%.1f MiB' % (peak / 1024)) if peak else 'n/a'} "
                   f"(largest sampled peak over runs 1 to 5)")
    out.append("")
    if failed_runs:
        out.append("## Failed runs")
        out.append("")
        for r in failed_runs:
            out.append(f"- sf{r['sf']:g} run{r['run']}: exit code {r['exit_code']}, "
                       f"wall {fmt(r['wall_seconds'])} s")
        out.append("")
    out.append("## Hypothesis verdicts")
    out.append("")
    out.append(f"- H1: **{h1}**" + (f" (SF1 total ratio {fmt(h1_ratio, 3)} vs line 2.0)" if h1_ratio is not None else " (no SF1 total)"))
    out.append(f"- H2: **{h2}** ({n_match} match, {len(mismatches)} mismatch, {n_missing} missing of {len(cells)} cells)")
    out.append(f"- H3: **{h3}**" + (f" (SF1 spread {fmt(h3_spread, 3)} vs line 2.0)" if h3_spread is not None else " (no SF1 spread)"))
    out.append("")
    (ROOT / "results" / "results.md").write_text("\n".join(out) + "\n")

    print(json.dumps({"verdicts": verdicts, "totals": {str(k): v for k, v in totals.items()},
                      "h1_ratio": h1_ratio, "h3_spread": h3_spread,
                      "cells_match": n_match, "cells_missing": n_missing,
                      "mismatches": len(mismatches)}, indent=2))


if __name__ == "__main__":
    main()
