# Decisions

Implementation decisions made during the build. Protocol decisions
are frozen in PREREGISTRATION.md; anything that changes a frozen
protocol element would be an amendment recorded here before the runs
it affects. None of the entries below change semantics or the frozen
protocol.

- D1 (2026-11-08): One subprocess per (scale, run) for memory
  isolation. Registered in PREREGISTRATION.md Section 7 before any
  run; restated here for the repo record. B2 ran its 5 runs in one
  process; the DirectRunner holds intermediates in memory and this
  machine has 7.7 GiB shared with other tenants. The timed region
  (pipeline execution only) is unaffected by process boundaries.
- D2 (2026-11-08): Semantics-preserving Beam implementation choices.
  Dimension joins (T2, T3, T5) use side inputs (AsDict) rather than
  keyed joins; T3 aggregates with CombinePerKey (accumulator: order
  key set, line count, decimal gross and net sums); T4 ranks inside a
  GroupByKey on order date; T6 uses combiners.Top.Of with a
  (gross descending, date ascending) key. Each is the idiomatic Beam
  shape for the same relational semantics fixed by B2's SQL.
- D3 (2026-11-08): Timed runs write one Parquet shard per
  transformation output (num_shards=1), so output files carry the
  same names as B2's arm outputs (t1.parquet, and so on). The
  canonical checksum reads the same logical rows regardless of file
  layout.
- D5 (2026-11-08): Peak memory is sampled from the run's process
  tree (sum of VmRSS over descendants, 0.25 s sampling) by
  scripts/run_matrix.py, because GNU time is not installed on the
  machine of record and the runner spawns worker processes. RSS is
  secondary evidence with no hypothesis attached (PREREGISTRATION.md
  Section 7); only the sampling method changed, not what is claimed.
- D4 (2026-11-08): Grouping keys in T3, T4, and T6 are ISO date
  strings, converted back to dates when output rows are shaped. Beam
  requires deterministic key coders for GroupByKey and CombinePerKey,
  and datetime.date has no deterministic coder (the failure is a
  TypeError from the runner, reproduced during harness testing).
  Internal representation only; keys, values, and outputs are
  unchanged.
