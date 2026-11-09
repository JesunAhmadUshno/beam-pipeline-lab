# Changelog

All notable changes to this project. Format: Keep a Changelog 1.1.
Versions follow Semantic Versioning; no release is tagged yet.

## [Unreleased]

### Changed
- 2026-11-08: Adopted the portfolio standard v2. README opens with the
  question, headline, field, job family and verdict (Functional), then
  reproduce commands, lifecycle and a layout table mapping the locked
  names to their roles. AUDIT.md is the report; its grade moved out of
  the public repo (grades are internal), and it gains "How the evidence
  is protected" and "Threats to validity". The lock is 0b74d2f (to be
  tagged proposal-v1), and a test fails if PREREGISTRATION.md changes
  by one byte. CI checks that RESULTS.md and results/results.md
  regenerate unchanged and runs with read-only permissions. CodeQL and
  Dependabot added.

### Added
- 2026-11-08: Results, README and AUDIT: SF0.1 complete (169.92 s,
  14.327 times B2 Spark, checksums 6 of 6); SF1 stopped inside T2 in
  both runner configurations (D7, D8); H1, H2, H3 INCONCLUSIVE.
- 2026-11-08: DECISIONS.md D5 to D7: process-tree RSS sampling, the
  setup pre-roll, and the single documented runner option for SF1.
- 2026-11-08: Harness: six Beam pipelines, runner, canonical checksum,
  semantic tests, CI.
- 2026-11-08: Pre-registration (0b74d2f), before any code or run.
