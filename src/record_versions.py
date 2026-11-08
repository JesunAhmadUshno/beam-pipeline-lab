"""Write results/versions.json: interpreter, library, and machine facts."""

import json
import platform
import sys
from pathlib import Path

import apache_beam
import duckdb
import pyarrow

ROOT = Path(__file__).resolve().parent.parent


def main():
    record = {
        "python": platform.python_version(),
        "apache_beam": apache_beam.__version__,
        "duckdb": duckdb.__version__,
        "pyarrow": pyarrow.__version__,
        "platform": sys.platform,
        "machine": platform.machine(),
        "hostname": platform.node(),
        "os": platform.platform(),
    }
    out = ROOT / "results" / "versions.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
