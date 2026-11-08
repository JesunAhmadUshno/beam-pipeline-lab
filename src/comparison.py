"""The comparison record's published numbers, frozen as constants.

Source: JesunAhmadUshno/data-engineering-scale-lab, results/results.md
(Track B2, closed 2026-11-07), per-transformation means of 5 runs with
one warm-up pass, on this same machine. These are cited published
values, not measurements of this study; RESULTS.md of this repository
always labels them as the B2 record.
"""

# Per-transformation means in seconds: {sf: {arm: {t: mean}}}
B2_MEANS = {
    0.1: {
        "duckdb": {"t1": 0.16, "t2": 0.71, "t3": 0.05, "t4": 0.25, "t5": 0.29, "t6": 0.02},
        "spark": {"t1": 0.95, "t2": 3.17, "t3": 1.95, "t4": 3.24, "t5": 1.99, "t6": 0.57},
    },
    1.0: {
        "duckdb": {"t1": 1.54, "t2": 8.06, "t3": 0.75, "t4": 2.59, "t5": 3.24, "t6": 0.10},
        "spark": {"t1": 4.27, "t2": 45.16, "t3": 9.39, "t4": 29.40, "t5": 11.18, "t6": 1.20},
    },
    10.0: {
        "duckdb": {"t1": 17.36, "t2": 241.17, "t3": 11.50, "t4": 42.93, "t5": 37.00, "t6": 1.67},
        "spark": {"t1": 37.16, "t2": 635.15, "t3": 153.15, "t4": 391.69, "t5": 194.72, "t6": 6.02},
    },
}

B2_TOTALS = {
    0.1: {"duckdb": 1.48, "spark": 11.86},
    1.0: {"duckdb": 16.27, "spark": 100.59},
    10.0: {"duckdb": 351.62, "spark": 1417.89},
}

# B2 setup overhead (seconds): Spark JVM start, session, first action;
# DuckDB connect and input views. SF0.1 values; see B2 results.md.
B2_SETUP = {"spark_sf0.1": 16.82, "duckdb_sf0.1": 0.49}
