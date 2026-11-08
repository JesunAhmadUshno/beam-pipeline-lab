# Audit

Self-audit of the Beam Pipeline Lab study, written at completion
(2026-11-08) in the programme's house form: a grade out of 10,000,
floored, with deductions itemized, and the verification work stated
separately from the claims it checks.

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

## Grade

Grade: 5,750 of 10,000.

What the grade credits: a pre-registration that was actually kept,
including its own failure handling (DECISIONS.md D7 pulled exactly
the one runner lever Section 9 allows, and D8 records that no second
lever was pulled when the restarted matrix failed the same way); a
complete SF0.1 matrix with exact, independently re-verified
reproduction on all six cells plus determinism; an SF1 failure
characterized with memory evidence across two configurations and
nine passes, instead of a quiet scope cut; and verdicts computed by
script from the logs, with the misses printed next to the hits.

Deductions, itemized:

- Minus 2,400: the SF1 matrix is incomplete, so H1 and H3 are
  INCONCLUSIVE at the primary scale the hypotheses were written
  for. The portability tax is measured only at SF0.1.
- Minus 800: H2 is INCONCLUSIVE. Exact reproduction is proven on 7
  of 12 cells; the five SF1 cells T2 to T6 have no Beam output at
  all, so byte-exact agreement there is unproven, not contradicted.
- Minus 300: every timing was taken on a shared VM with co-tenants
  that were never stopped; SF1 T1's timed runs drifted from
  100.73 s to 177.10 s across the matrix, which shows the noise
  plainly. The B2 record carried the same caveat.
- Minus 300: one runner, one SDK, one machine, one workload. The
  scope was registered in advance (PREREGISTRATION.md Section 9),
  and it still bounds what the study can say.
- Minus 200: execution continuity. The original build worker died
  on a runtime infrastructure failure at about 12:21 EST on
  2026-11-08; the study's finish depended on the detached matrix
  process surviving it and on a completion pass doing the finishing
  work (DECISIONS.md D8).
- Minus 150: the setup pre-roll (DECISIONS.md D6) departs from
  B2's warm-session protocol shape. It is registered, justified by
  the runner's service-start cost, and does not touch any timed
  region, but it is a comparability wrinkle a reader must hold.
- Minus 100: peak memory is process-tree RSS sampling (DECISIONS.md
  D5), not an external meter, because GNU time is not installed on
  the machine of record. RSS carries no hypothesis; the deduction
  is for the instrument, not the claim.

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
