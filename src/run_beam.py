"""Run one (scale, run) pass of the Beam arm: T1 to T6 in dependency
order, each as its own Beam pipeline on the DirectRunner, each writing
Parquet output under outputs/beam/sf<sf>/run<run>/.

Timing follows PREREGISTRATION.md Section 7: the timed region for one
transformation starts with the pipeline graph fully constructed and
covers execution (input read, compute, output write). Interpreter
start, imports, graph construction, data generation, and checksum
computation are outside the timed region. Run 0 is the uncounted
warm-up pass and writes to the warmup directory.

Usage:
    python src/run_beam.py --sf 0.1 --run 1
"""

import argparse
import json
import sys
import time
from pathlib import Path

import apache_beam as beam
import pyarrow.parquet as pq
from apache_beam.options.pipeline_options import PipelineOptions

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipelines import BUILDERS  # noqa: E402
from spec import ORDER  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

# Runner options for every pipeline in this pass. The default is the
# bare DirectRunner. DECISIONS.md D7 records the one registered use
# of PREREGISTRATION.md Section 9: at SF1 the default configuration
# failed at T2 in three consecutive attempts by resource exhaustion,
# and the SF1 matrix was restarted with the documented DirectRunner
# option --direct_num_workers=1, needed to execute at all on this
# machine. The setup record of every pass names the configuration.
RUNNER_FLAGS = ["--runner=DirectRunner"]
RUNNER_LABEL = "default"


def sf_label(sf):
    return "%g" % sf


def log_run(record):
    log = ROOT / "results" / "run_log.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a") as f:
        f.write(json.dumps(record) + "\n")


def run_transformation(key, inputs, out_prefix):
    pipeline = beam.Pipeline(options=PipelineOptions(RUNNER_FLAGS))
    BUILDERS[key](pipeline, *inputs, out_prefix)
    started = time.perf_counter()
    result = pipeline.run()
    result.wait_until_finish()
    return time.perf_counter() - started


def run_setup_preroll():
    """One uncounted empty pipeline, so the runner service start lands
    in setup, not inside T1's timed region (DECISIONS.md D6). B2's
    protocol did the same thing in the other direction: its setup
    figure includes a first action, and its timed runs executed in the
    warm session. Returns the seconds the empty run took."""
    pipeline = beam.Pipeline(options=PipelineOptions(RUNNER_FLAGS))
    _ = pipeline | beam.Create([]) | beam.Map(lambda x: x)
    started = time.perf_counter()
    result = pipeline.run()
    result.wait_until_finish()
    return time.perf_counter() - started


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, required=True)
    ap.add_argument("--run", type=int, required=True)
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--out-root", default="outputs")
    ap.add_argument("--direct-num-workers", type=int, default=None,
                    help="documented DirectRunner option; set only under "
                         "DECISIONS.md D7 (SF1 restart)")
    args = ap.parse_args()

    if args.direct_num_workers is not None:
        global RUNNER_LABEL
        RUNNER_FLAGS.append(f"--direct_num_workers={args.direct_num_workers}")
        RUNNER_LABEL = f"direct_num_workers={args.direct_num_workers}"

    label = sf_label(args.sf)
    base = ROOT / args.data_root / f"sf{label}"
    lineitem = str(base / "lineitem" / "part-000.parquet")
    orders = str(base / "orders" / "part-000.parquet")
    run_name = "warmup" if args.run == 0 else f"run{args.run}"
    out_dir = ROOT / args.out_root / "beam" / f"sf{label}" / run_name
    out_dir.mkdir(parents=True, exist_ok=True)

    inputs = {
        "t1": (lineitem,),
        "t2": (lineitem, orders),
        "t3": (lineitem, orders),
        "t4": (str(out_dir / "t2.parquet"),),
        "t5": (lineitem, orders),
        "t6": (str(out_dir / "t3.parquet"), str(out_dir / "t4.parquet")),
    }

    setup_seconds = run_setup_preroll()
    log_run({
        "arm": "beam", "sf": args.sf, "transformation": "setup",
        "run": args.run, "seconds": round(setup_seconds, 4), "rows_out": None,
        "beam_setup": "runner_service_start_empty_pipeline",
        "runner_options": RUNNER_LABEL,
    })
    print(f"sf{label} run{args.run} setup: {setup_seconds:.3f}s", flush=True)

    for key in ORDER:
        out_prefix = str(out_dir / key)
        seconds = run_transformation(key, inputs[key], out_prefix)
        rows = pq.ParquetFile(out_prefix + ".parquet").metadata.num_rows
        log_run({
            "arm": "beam", "sf": args.sf, "transformation": key,
            "run": args.run, "seconds": round(seconds, 4), "rows_out": rows,
        })
        print(f"sf{label} run{args.run} {key}: {seconds:.3f}s rows={rows}",
              flush=True)


if __name__ == "__main__":
    main()
