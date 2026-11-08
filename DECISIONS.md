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
- D6 (2026-11-08): Each run subprocess first executes one uncounted
  empty pipeline (the setup pre-roll in src/run_beam.py), and its
  seconds are logged as that run's setup record. Reason: in Beam
  2.77 the DirectRunner submits to a local runner service whose
  start costs about 20 s (measured: five empty-pipeline samples,
  20.2 to 20.7 s of run time each). D1's subprocess isolation meant
  that start would otherwise land inside T1's timed region on every
  run, attributing a service start to a transformation and inflating
  the H3 spread. B2's protocol put the equivalent cost in setup
  (its Spark setup figure includes a first action, and its timed
  runs executed in the warm session); the warm-up pass in
  PREREGISTRATION.md Section 7 exists to remove exactly this class
  of cold-start cost. The pre-roll changes no timed region, no
  verdict rule, and no output; the pre-matrix smoke run at SF0.1
  (T1 30.9 s with the service start inside it) is superseded by the
  matrix runs and is not reported as a result.
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
- D7 (2026-11-08): SF1 under the default DirectRunner configuration
  failed inside T2 in three consecutive passes, and the SF1 matrix
  was restarted under a documented runner option, as
  PREREGISTRATION.md Section 9 allows (documented runner options
  needed to execute at all, recorded here before the restarted runs
  and named in RESULTS.md). Evidence: the warm-up pass was killed by
  signal 9 inside T2 at a peak process-tree RSS of 5,391,040 KB;
  timed run 1 exited 1 inside T2 when the local runner service died
  (gRPC stream removed, ping timeout; the service was destroyed) at
  a peak of 5,369,508 KB; timed run 2 was killed by signal 9 inside
  T2 at a peak of 5,398,492 KB; run 3 was stopped by hand once the
  pattern was proven. T1 completed in every pass (99.5 to 120.8 s,
  914,963 rows). Live sampling during run 2 showed the single SDK
  worker process growing from 0.73 GiB to 2.89 GiB in 40 s inside T2
  while the runner service held about 1.1 GiB: T2's side-input join
  over 1.5M orders rows, run at the runner's default bundle
  parallelism, needs more memory than this 7.7 GiB shared machine
  can give. The failed attempt's records are preserved in
  results/sf1_default_config_attempt.json; no number from it enters
  any mean, total, or verdict. The restart changes exactly one
  thing: --direct_num_workers=1, a DirectRunner option defined in
  apache_beam.options.pipeline_options, applied to every restarted
  SF1 pass including its warm-up and named in each pass's setup
  record as runner_options. SF0.1 is untouched: it completed under
  the default configuration, and the configuration of each scale is
  named wherever its times are reported. If the restarted matrix
  also fails at T2, no second lever is pulled inside this study:
  the SF1 cells are then recorded as unable to complete and the
  verdicts follow the frozen INCONCLUSIVE paths.
- D8 (2026-11-08): Completion record for the SF1 matrix and for the
  study's finishing. The restarted SF1 matrix under
  direct_num_workers=1 (D7) ran to its natural exit: all six passes
  (the warm-up plus timed runs 1 to 5) were killed by signal 9
  inside T2 after T1 completed, at peak process-tree RSS of
  5,429,284 KB (warm-up), then 5,013,392, 4,497,064, 4,521,940,
  4,507,924 and 4,547,304 KB (runs 1 to 5). No second restart was
  attempted and no further runner option was pulled, exactly as D7
  pre-committed. The five SF1 cells T2 to T6 are recorded as unable
  to complete, and the verdicts follow the frozen paths in
  PREREGISTRATION.md Section 2, computed by src/make_results.py.
  Separately: the original build worker died on a runtime
  infrastructure failure at about 12:21 EST on 2026-11-08, after
  launching the restarted matrix as a detached process. The matrix
  survived it. A completion pass shepherded the matrix to its exit,
  collected the SF1 checksums, generated the results, and finished
  README.md and AUDIT.md. No protocol element changed in the
  completion pass.
