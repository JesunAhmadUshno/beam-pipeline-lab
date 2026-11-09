# Audit

Self-audit of the Beam Pipeline Lab study, written at completion
(2026-11-08). This file is the study's report in the portfolio
standard: what was verified and how, how the evidence is protected,
the threats to validity, and the residuals. The verification work is
stated separately from the claims it checks. Scoring is internal and
is not published here.

## What was verified, and how

- Pre-registration order: PREREGISTRATION.md is the first content
  commit (0b74d2f), ahead of the harness (6a4bf7b) and every run.
  The frozen thresholds were set from the B2 published record and
  Beam's own documentation, before any Beam timing existed.
- Checksum independence: at finishing, every Beam checksum record
  was re-verified against the B2 repository's own
  results/checksums.json, not against this repo's copied reference
  file. The B2 arms agree on a single run-1 value on all 12
  (scale, transformation) cells; this repo's reference file matches
  the B2 repository on 12 of 12 cells; Beam run 1 matches on 7
  cells (SF0.1 T1 to T6, SF1 T1) with 0 mismatches; Beam run 5
  equals run 1 on all 7 matched cells.
- Verdict generation: H1, H2 and H3 were computed by
  src/make_results.py from the run logs and checksum records. The
  SF0.1 figures it printed were recomputed by hand from
  results/run_log.jsonl during this audit (total 169.92 s, ratio
  14.327, spread 3.796) and agree.
- Failure records: the SF1 exit codes and peak RSS values in
  results/results.md come from results/rss_log.jsonl (restarted
  matrix) and results/sf1_default_config_attempt.json (default
  configuration archive), read directly during this audit.
- Tests: the semantic test suite passes (6 tests) and CI runs it on
  Python 3.11 and 3.12 with a compile check and an em dash scan.

## Jev claim check

The headline claim (the three verdicts with their figures, as
transcribed in results/jev-claim-check.json) went through the
standing claim_check gate. Model: jev-1.13.0. Noul: 0.60. Route:
REVIEW, under the standing bands (clear at 0.85 or above, block at
0.10 or below, review between). On the REVIEW route the claim was
hand-verified figure by figure against the artefacts: the SF0.1
means and ratios, the checksum counts from the independent
re-verification above, the failed-pass peaks in both
configurations, and the SF1 T1 mean of 152.97 s recomputed from
the five timed T1 records. Every figure agrees. Spec and raw
response: ~/workspace/tools/jev/beam-pipeline-lab-claim-spec-2026-11-08.json
and beam-pipeline-lab-claim-results-2026-11-08.json (usage: 1,434
input tokens, 20 output tokens).

## How the evidence is protected

- The lock is enforced in code. PREREGISTRATION.md was committed
  alone in 0b74d2f, before any harness file or run, and has never
  changed since. tests/test_layout.py pins its SHA-256 as committed
  there, so one changed byte fails CI.
- Verdicts are computed, not typed. src/make_results.py reads
  results/run_log.jsonl, results/rss_log.jsonl,
  results/setup_log.jsonl, results/checksums.json and
  results/reference_checksums.json, and writes RESULTS.md and
  results/results.md. CI reruns it and fails if either file changes,
  so the published tables always match the recorded evidence.
- The README headline is checked against the raw run log:
  tests/test_layout.py recomputes the SF0.1 total, its per-run range
  and the ratio to the B2 Spark total from results/run_log.jsonl.
- Failed passes are kept, not dropped. The SF1 default-configuration
  attempt is archived in results/sf1_default_config_attempt.json and
  every restarted SF1 pass is in results/rss_log.jsonl with its exit
  code and peak memory.
- The comparison record is read only. B2's reference checksums are
  copied with provenance into results/reference_checksums.json, and
  at finishing every Beam checksum was re-verified against the B2
  repository's own file (section above).

## Threats to validity

What the study got right is on record above and in DECISIONS.md: the
pre-registration was kept, including its own failure handling (D7
pulled exactly the one runner lever Section 9 allows, and D8 records
that no second lever was pulled when the restarted matrix failed the
same way); the SF0.1 matrix is complete, with exact, independently
re-verified reproduction on all six cells plus determinism; the SF1
failure is characterized with memory evidence across two
configurations and nine passes, instead of a quiet scope cut; and
verdicts are computed by script from the logs, with the misses
printed next to the hits. The threats below bound what that record
can say.

- Incomplete primary scale. The SF1 matrix is incomplete, so H1 and
  H3 are INCONCLUSIVE at the primary scale the hypotheses were
  written for. The portability tax is measured only at SF0.1.
- Partial reproduction. H2 is INCONCLUSIVE. Exact reproduction is
  proven on 7 of 12 cells; the five SF1 cells T2 to T6 have no Beam
  output at all, so byte-exact agreement there is unproven, not
  contradicted.
- Correctness gates runs 1 and 5 only. The protocol checksums Beam
  runs 1 and 5 on every cell (PREREGISTRATION.md Section 7); timed
  runs 2 to 4 are counted without their own checksum. Runs 1 and 5
  agree everywhere they were tested, which makes a silent divergence
  in between unlikely, but it is not excluded.
- Co-tenancy, stated but not measured. Every timing was taken on a
  shared VM with co-tenants that were never stopped, and no load
  reading was recorded with each run. SF1 T1's timed runs drifted
  from 100.73 s to 177.10 s across the matrix, which shows the noise
  plainly. The B2 record carried the same caveat.
- Borrowed baseline. The Spark and DuckDB baselines are B2's
  published means from the same machine on 2026-11-07, not re-run
  alongside Beam, so machine load on the two days may differ.
- Narrow scope. One runner, one SDK, one machine, one workload. The
  scope was registered in advance (PREREGISTRATION.md Section 9),
  and it still bounds what the study can say.
- Execution continuity. The original build worker died on a runtime
  infrastructure failure at about 12:21 EST on 2026-11-08; the
  study's finish depended on the detached matrix process surviving
  it and on a completion pass doing the finishing work
  (DECISIONS.md D8).
- Protocol shape. The setup pre-roll (DECISIONS.md D6) departs from
  B2's warm-session protocol shape. It is registered, justified by
  the runner's service-start cost, and does not touch any timed
  region, but it is a comparability wrinkle a reader must hold.
- Memory instrument. Peak memory is process-tree RSS sampling
  (DECISIONS.md D5), not an external meter, because GNU time is not
  installed on the machine of record. RSS carries no hypothesis; the
  limit is the instrument, not the claim.

## Residuals

- SF1 completion is a hardware question now, and the study bounds
  it: SF1 passes died at process-tree peaks between 4,497,064 KB
  and 5,429,284 KB on a 7.7 GiB shared machine. A rerun on a quiet
  machine with clear headroom above those peaks is the named next
  step; this study does not claim a specific memory figure would
  suffice, because the kill point depends on co-tenancy as well.
- A second-runner arm (the same pipelines on the Spark runner or
  Flink) would convert the README's portability section from
  documentation analysis into measurement. It was out of the
  registered scope and remains the obvious follow-up study.
- SF10 stays excluded, as registered. Nothing in this study's
  results extends past SF1.
