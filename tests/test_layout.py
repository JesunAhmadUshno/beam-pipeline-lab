"""Portfolio standard checks: the lock holds, required files exist, no
public grade, and the README headline traces to the raw run log."""
import hashlib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from comparison import B2_TOTALS  # noqa: E402
# SHA-256 of PREREGISTRATION.md as committed in 0b74d2f (tag proposal-v1), before any code or run.
LOCKED_SHA256 = "3513f4f78c3ab435f39a331c60a3502e5a02cb0474addbdb3723c488a8d70bd9"


def read(name):
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def test_preregistration_is_unchanged_since_the_lock():
    with open(os.path.join(ROOT, "PREREGISTRATION.md"), "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == LOCKED_SHA256, \
            "PREREGISTRATION.md changed after its lock; record an amendment in DECISIONS.md instead"


def test_required_files_exist():
    need = ["README.md", "PREREGISTRATION.md", "AUDIT.md", "RESULTS.md", "DECISIONS.md",
            "CHANGELOG.md", "CITATION.cff", "SECURITY.md", "LICENSE", "results/results.md",
            "results/run_log.jsonl", "results/checksums.json", "results/reference_checksums.json"]
    missing = [p for p in need if not os.path.exists(os.path.join(ROOT, p))]
    assert not missing, "missing: " + ", ".join(missing)


def test_no_public_grade():
    for name in ("README.md", "AUDIT.md", "RESULTS.md", "results/results.md", "CHANGELOG.md"):
        text = read(name)
        for marker in ("of 10,000", "/10000", "Grade:"):
            assert marker not in text, name + " carries a grade; grades are internal"


def test_readme_headline_traces_to_the_run_log():
    runs = [json.loads(line) for line in read("results/run_log.jsonl").splitlines() if line.strip()]
    per_run = {}
    for r in runs:
        if r["sf"] == 0.1 and 1 <= r["run"] <= 5 and r["transformation"] != "setup":
            per_run.setdefault(r["run"], []).append(r["seconds"])
    assert sorted(per_run) == [1, 2, 3, 4, 5] and all(len(v) == 6 for v in per_run.values())
    totals = [sum(v) for v in per_run.values()]
    mean_total = sum(totals) / 5  # sum of six means equals mean of five per-run sums
    readme = read("README.md")
    for figure in (f"{mean_total:.2f} s", f"{min(totals):.2f} to {max(totals):.2f} s",
                   f"{mean_total / B2_TOTALS[0.1]['spark']:.3f} times"):
        assert figure in readme, "README headline lacks " + figure
