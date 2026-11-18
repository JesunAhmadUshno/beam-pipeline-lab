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
- D9 (2026-11-18): Gap-closure completion attempt of the SF1 matrix
  on a machine with memory headroom. Registered before any run, under
  the gap-closure orders of 2026-11-17 and 2026-11-18. Background: D8
  recorded all six SF1 passes (warm-up plus five timed runs) killed by
  signal 9 inside T2 on the 7.7 GiB machine of record, at peak
  process-tree RSS of 4,497,064 to 5,429,284 KB, and the verdicts
  followed the frozen INCONCLUSIVE paths. D7 pre-committed that no
  second runner lever would be pulled inside this study; this
  amendment pulls none: the matrix runs in the D7 restart
  configuration exactly (direct_num_workers=1 on every pass including
  the warm-up, named in each pass setup record). What changes is only
  the machine: the Dell PowerEdge R610 (16 cores, 94 GiB RAM, about
  77 GiB available at registration), against a recorded peak of about
  5.4 GiB. Machine history: the completion was staged on BlackBoxV2
  under this number (route lost before it ran), re-registered on
  BlackBoxV3 (RAM gate failed there on measurement: 4,421 MB
  available), and is executed on the R610. Execution is in a
  path-shifted copy of this repository (beam-pipeline-lab-d9 beside
  the clone); harness and verdict logic are byte-identical to this
  clone. The copy run_log.jsonl and rss_log.jsonl are the run
  records; copies land in this clone as new files
  (results/d9_sf1_run_log.jsonl, results/d9_sf1_rss_log.jsonl) with
  the outcome entry. If the matrix completes, verdicts are computed
  by the repo own src/make_results.py inside the copy, logic
  unchanged, and any movement is reported beside the committed
  INCONCLUSIVE verdicts; this clone RESULTS.md is not edited.
- D9 addendum (2026-11-18, registered before the run it covers): a
  second execution repair in the same class as the first. The first
  D9 attempt on the R610 failed all passes inside T1 at about 306 s
  per pass at about 1.4 GiB peak RSS, not by memory: Beam 2.77 runs
  these pipelines on the Prism portable runner, whose job-server
  request timeout (--job_server_timeout) defaults to 300 s, and the
  driver state stream dies with DEADLINE_EXCEEDED once a single
  transformation runs past it. On the machine of record every
  transformation finished inside 300 s, so the limit never fired;
  on the R610 Xeon E5540, T1 alone needs longer. The option own
  documentation states it does not apply to pipeline run time and
  the driver can wait for completion indefinitely. The repair uses
  the study own mechanism (the RUNNER_FLAGS list in run_beam.py,
  the D7 pattern): the run copy adds --job_server_timeout=3600 to
  every pipeline and its setup records name it
  (runner_options: direct_num_workers=1, job_server_timeout=3600).
  D7 pre-commitment against a second lever governed the original
  study on the machine of record and was executed in D8; this
  amendment is a new gap-closure execution on a different machine,
  and the change is a harness wait limit, not a measurement
  parameter: pipeline graphs, data, timed regions, and verdict
  logic are unchanged. The first D9 attempt records (three setup
  lines, two rss lines, no transformation completed) were reset in
  the copy before this relaunch and are described here so the
  attempt is not lost.
- D9 outcome (2026-11-18): the SF1 matrix COMPLETED on the R610.
  All six passes (warm-up plus timed runs 1 to 5) exited 0 in the D7
  configuration with the addendum harness timeout named in every
  setup record (direct_num_workers=1, job_server_timeout=3600).
  Pass walls 4,223.7 to 4,270.1 s; peak process-tree RSS 26.8 to
  28.8 GiB per pass, against the 4.5 to 5.4 GiB at which the machine
  of record killed every pass: the true SF1 memory need is about
  27 GiB, five times the kill point. The SF1 dataset of record was
  transferred from the machine of record working tree and verified
  byte-identical by md5 before the run (data is generated, not
  committed). Verdicts were computed by the repo own
  src/make_results.py from the executed log and the checksums of
  the R610 run 1 and run 5 outputs (the checksum step ran on the
  study VM because the pinned duckdb wheel crashes with an illegal
  instruction on the R610 Xeon E5540, which has no AVX; the timed
  pipelines never import duckdb). All three verdicts move from
  INCONCLUSIVE to CONFIRMED under the frozen rules: H1 CONFIRMED
  (Beam SF1 total 4,153.73 s vs B2 Spark local 100.59 s, ratio
  41.294 against the frozen 2.0 line); H2 CONFIRMED (12 of 12 cells
  match the B2 run-1 checksum, 0 mismatches; run 5 reproduces run 1
  on all 12 cells, 0 determinism breaks); H3 CONFIRMED (largest
  per-transformation ratio 94.20 at T5, smallest 27.81 at T2,
  spread 3.39 against the frozen 2.0 line). Composition note, for
  honesty: the T1 mean inside the 4,153.73 total pools the five
  timed T1 records from the D8 passes on the machine of record
  (100.7 to 177.1 s) with the five R610 records (314.8 to 323.7 s),
  because the repo run log and verdict logic do not separate them;
  T2 to T6 means are R610 only, as no other SF1 records exist. On
  R610-only timed means the total is 4,236.66 s and the H1 ratio
  42.12: the verdicts are the same. R610 timed means: T1 318.88,
  T2 1,255.94, T3 508.38, T4 1,018.62, T5 1,053.14, T6 81.70 s.
  The generated results files are preserved beside this entry as
  results/d9_RESULTS.md and results/d9_results_tables.md, the
  executed logs as results/d9_sf1_run_log.jsonl and
  results/d9_sf1_rss_log.jsonl, and the merged checksum record as
  results/d9_checksums.json. This clone RESULTS.md is unchanged;
  the paper three INCONCLUSIVE verdicts are superseded by this
  outcome wherever the study is next reported.
