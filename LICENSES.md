# Licences and data provenance

## Study data

The study data is generated locally by DuckDB's tpch extension
(`CALL dbgen`), by the same generator method as the comparison study
(JesunAhmadUshno/data-engineering-scale-lab, Track B2). Every output
of this study is labelled "derived from TPC-H". No result in this
repository is an official TPC-H result, and no result is comparable
to official TPC results.

TPC-H tools not used; DuckDB tpch extension used instead. No TPC
software was downloaded, and no registration was made with the
Transaction Processing Performance Council or anywhere else for this
study.

## Software

- This repository's own code: Apache License 2.0 (see LICENSE).
- Apache Beam: Apache License 2.0.
- DuckDB: MIT License.
- PyArrow: Apache License 2.0.

## Adapted code

The data generator (data/gen/generate.py), the canonical checksum
(src/checksum.py), and the transformation registry (src/spec.py) are
adapted from JesunAhmadUshno/data-engineering-scale-lab (Apache
License 2.0, same author), because this study's correctness gate is
defined as agreement with that study's record. Each adapted file
says so in its header.

## Generated files

Data files under data/ and run outputs under outputs/ are build
products. They are not committed to this repository. The committed
evidence is the generated results under results/, which the scripts
produce from the run logs and checksum records.
