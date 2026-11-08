"""One Beam setup-overhead sample, run in a fresh process.

Mirrors how B2 reported setup separately (Spark session start, DuckDB
connect): the cost of importing Apache Beam and running an empty
pipeline to completion on the DirectRunner. Run this script 5 times;
make_results.py reports the median. Each invocation appends one JSON
line to results/setup_log.jsonl.
"""

import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    t0 = time.perf_counter()
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    t_import = time.perf_counter()

    pipeline = beam.Pipeline(options=PipelineOptions(["--runner=DirectRunner"]))
    _ = pipeline | beam.Create([]) | beam.Map(lambda x: x)
    result = pipeline.run()
    result.wait_until_finish()
    t_done = time.perf_counter()

    record = {
        "import_seconds": round(t_import - t0, 4),
        "empty_run_seconds": round(t_done - t_import, 4),
        "setup_seconds": round(t_done - t0, 4),
    }
    log = ROOT / "results" / "setup_log.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a") as f:
        f.write(json.dumps(record) + "\n")
    print(json.dumps(record))


if __name__ == "__main__":
    main()
