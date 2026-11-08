# Pre-registration: Beam Pipeline Lab

Locked before any measured run exists. The commit that adds this file is
the first content commit in this repository and precedes every harness
file, every pipeline, and every result; git history proves the order.
Any change to this file after locking is an amendment and must be
recorded in DECISIONS.md with its reason, before the runs it affects.

## 1. Research question

What does Apache Beam's portability cost, and what does it buy, on a
fixed transformation workload? Concretely: do Beam pipelines running on
the DirectRunner reproduce, byte for byte under a canonical checksum,
the results of an already published Spark and DuckDB study of the same
six transformations, and how large and how uneven is the runtime tax
for writing the job once in the portable model?

## 2. Hypotheses (falsifiable, with thresholds)

The comparison record is JesunAhmadUshno/data-engineering-scale-lab
(Track B2, closed 2026-11-07), hereafter B2. Its published Spark local
totals (sum of six transformation means, 5 runs, one warm-up pass) are
11.86 s at SF0.1 and 100.59 s at SF1 on this same machine. Its DuckDB
totals are 1.48 s and 16.27 s. The thresholds below are set from that
published record and from the Beam project's own documentation, which
describes the DirectRunner as a runner for testing and development
that executes pipelines in a way that is not optimized for performance.
No Beam timing from this study existed when these lines were set.

- H1 (the portability tax). At SF1, the Beam total workload time
  (defined in Section 7) divided by the B2 Spark local total of
  100.59 s is at least 2.0. CONFIRMED if the ratio is at least 2.0.
  REFUTED if all six SF1 transformations complete and the ratio is
  below 2.0. INCONCLUSIVE if any SF1 transformation cannot complete,
  because then no total exists.
- H2 (exact reproduction). Beam reproduces the B2 run-1 checksum (the
  single SHA-256 value that all three B2 arms agreed on) on every
  attempted cell: SF0.1 and SF1 crossed with T1 to T6, 12 cells.
  Run 5 of the Beam arm must also reproduce Beam run 1 on every cell
  (determinism). CONFIRMED if all 12 cells complete and match and
  determinism holds. REFUTED if any completed cell's checksum differs
  from the B2 reference; a mismatch is reported with the difference
  characterized, never redefined away. INCONCLUSIVE if no cell
  mismatches but at least one cell cannot complete (for example by
  resource exhaustion on this shared machine), because then exact
  reproduction is unproven on the full matrix.
- H3 (the tax is uneven). At SF1, the ratio of the largest
  per-transformation Beam-to-Spark mean-time ratio to the smallest is
  at least 2.0. CONFIRMED at or above 2.0, REFUTED below 2.0 with all
  six transformations complete, INCONCLUSIVE if SF1 is incomplete.
  SF0.1 ratios are reported as secondary evidence only.

## 3. Machine of record

All runs happen on one named machine: the Muse VM, hostname
htch-runtime (Linux 7.0.0-43-generic, AMD EPYC 9D25, 4 vCPUs,
7.7 GiB RAM, 1 GiB swap), Python 3.12.3. This is the same machine that
produced the B2 record. It is a shared machine: other tenants run on
it, and no co-tenant is stopped for this study. Co-tenancy is a stated
caveat on every timing, as it was in B2. No number from any other
machine is reported in this study. Exact library versions are recorded
in results/versions.json.

## 4. Dataset

Derived from TPC-H. Not official TPC-H data or results; no
comparability to official TPC-H results is claimed. See LICENSES.md.

Data is regenerated with the same generator method B2 used: DuckDB's
tpch extension, CALL dbgen, exported to Parquet (lineitem and orders
only, one part file per table per scale). Scales attempted: SF0.1 and
SF1. Input data identity is not assumed: the T1 checksum at each scale
covers a ship-date-filtered subset of lineitem, and the T2 and T5
checksums cover the full lineitem-orders join, so H2 passing on those
cells is direct evidence that the regenerated inputs match B2's.

SF10 is excluded, and the exclusion is registered here, before any
run: B2's Spark arm needed 1,417.89 s of pure execution for SF10 on
this machine, B2's SF10 data alone is about 2.2 GB across all scales
in its local copy, and the DirectRunner materializes intermediate
results in memory on a 7.7 GiB shared VM. With any plausible Beam tax,
SF10 runs would take hours and risk resource exhaustion. No claim in
this study extends past SF1.

## 5. The comparison record (frozen)

B2 is cloned pinned and read-only and is never modified. The reference
values used are its published ones: per-transformation Spark and
DuckDB means in its results/results.md, and the run-1 checksums in its
results/checksums.json, verified during this study's setup to agree
across all three B2 arms on all 18 (scale, transformation) cells.
The 12 reference checksums for SF0.1 and SF1 are copied into
results/reference_checksums.json with their provenance stated there.

## 6. Transformations (frozen)

The six transformations are B2's, with semantics fixed by B2's
sql/t1_filter.sql to sql/t6_sort_limit.sql and its src/spec.py output
columns. In words:

- T1: lineitem rows with ship date in calendar 1995; all 16 lineitem
  columns, unchanged.
- T2: lineitem inner-joined to orders on order key; 21 columns.
- T3: per order date: distinct order count, line count, gross revenue
  sum of extended price times (1 minus discount), net revenue sum of
  gross times (1 plus tax). Money arithmetic is exact decimal:
  gross carries scale 4 and net scale 6, matching the decimal types
  B2's arms stored (DECIMAL(38,4) and DECIMAL(38,6)).
- T4: from T2's output, per order date, line amount (same scale-4
  decimal product) and its RANK within the date, descending by
  amount, with standard RANK semantics: tied amounts share a rank and
  the next rank skips.
- T5: from the T2 join inputs: quantity cast to integer, prices as
  DECIMAL(18,2), dates as ISO text, discount times 100 cast to
  integer. Casts truncate toward zero, matching DuckDB and Spark
  behaviour on these non-negative values.
- T6: T3 joined to T4 rows with rank 1 on order date, max line amount
  per date, ordered by gross revenue descending then order date
  ascending, limit 10. The ordering keys are total (order date is
  unique per group), so the limit cut is deterministic.

Faithfulness rule: if a transformation cannot be expressed faithfully
in Beam's model, that is a finding. It is recorded in DECISIONS.md
before scoring, the closest faithful equivalent is implemented, and
the exact difference is stated in RESULTS.md. Implementation choices
that do not change semantics (for example a side-input join instead
of a keyed join) are not deviations and are documented in the
portability analysis.

## 7. Measurement and correctness protocol

Timing, mirroring B2's protocol shape:

- One uncounted warm-up pass per scale, then 5 timed runs per scale.
- Each timed run executes T1 to T6 in dependency order. The timed
  region for one transformation is the wall time of its Beam pipeline
  run: from the start of pipeline execution (graph fully constructed)
  to completion of the run, including input read, compute, and
  Parquet output write, and excluding interpreter start, library
  import, pipeline graph construction, data generation, and checksum
  computation.
- One subprocess per (scale, run), so a run's memory is released
  before the next begins. B2 ran its 5 runs in one process; this is a
  registered difference, made because the DirectRunner holds
  intermediates in memory on a 7.7 GiB machine. The timed region is
  unaffected by process boundaries.
- Reported per transformation: mean and standard deviation of the 5
  runs. Totals: the sum of the six means, as in B2. Peak resident
  memory per (scale, run) is read from /usr/bin/time and reported as
  secondary evidence, with no hypothesis attached.
- Setup overhead, reported separately as in B2: the cost of starting
  the Beam runtime in a fresh process and running an empty pipeline,
  5 repeats, median reported. B2's analogues were 16.82 s (Spark
  session) and 0.49 s (DuckDB connect) at SF0.1.

Correctness:

- The canonical checksum is B2's method, reimplemented and credited:
  the output rows are read through DuckDB, ordered by all output
  columns, exported with COPY to CSV (no header, delimiter |), and
  hashed with SHA-256. Beam outputs are written as Parquet with the
  same column names and matching logical types as B2's outputs
  (integer, date, text, and decimal at the scales named in Section 6),
  so the checksum compares data, not file formats.
- Checksums are computed for Beam runs 1 and 5 on every cell.
  H2's match test uses run 1 against the B2 reference; determinism
  uses run 5 against run 1.
- A cell that cannot complete is recorded as failed with the failure
  mode characterized (error class, transformation, scale, memory
  reading if any). It is never dropped from the matrix silently.

Verdicts are computed by src/make_results.py from the run log and the
checksum records and printed in RESULTS.md. Nothing numeric in
RESULTS.md or the README is hand-typed.

## 8. Portability analysis (documentation, not measurement)

A written section in the README maps the constructs each pipeline
uses to their portability status across runners (Dataflow, Flink,
and Spark through the Beam runner): what runs unchanged, what is
Python-SDK-limited or DirectRunner-specific, and what this study
deliberately avoided. Each claim carries its evidence (a Beam
documentation statement or a code-level reason). This section is
labelled documentation analysis wherever it appears. No portability
claim is made from measurement, because only the DirectRunner is run.

## 9. What stays out

- SF10 (Section 4) and any runner other than the DirectRunner.
- Beam SQL, the Java and Go SDKs, streaming mode, and any managed
  service. The study is batch, Python SDK, local runner.
- Tuning the DirectRunner beyond documented runner options needed to
  execute at all. If such an option is used, it is recorded in
  DECISIONS.md and named in RESULTS.md. The study measures the
  portable model's cost on the reference runner, not a tuned Beam
  deployment.
- Any modification of the B2 repository or its data.

## 10. Overclaim guard (binds all prose in this repository)

Every number here is one machine, one workload, one runner. The
DirectRunner result must never be stated as Beam's performance on a
production runner; it is the cost of the portable model on the
reference runner, plus a correctness and reproducibility result that
does transfer. Hypothesis verdicts use exactly three words: CONFIRMED,
REFUTED, INCONCLUSIVE. Misses are published with the hits.
