# Changelog

All notable changes to pqora are recorded here. The version number is
`VERSION` in `pqora.py`; it appears in error messages, logs, part files, the
manifest and the kit file name. Releases are tagged `v<version>` in git and
published on the GitHub Releases page together with the kit tarball
`pqora_kit_<version>.tar.gz`.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/): a major bump means
generated parts or SQL from the previous version are no longer accepted, a
minor bump adds features and options, a patch bump fixes bugs.

## [Unreleased]

### Added
- Open-source release preparation: Apache License 2.0, `NOTICE`,
  `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, this changelog,
  SPDX license headers in every source file.
- GitHub Actions CI: `pyflakes`, `vermin` (Python 3.6 compatibility) and the
  offline self-test (`make`, `generate`, `verify`) on the oldest supported
  pyarrow and on a current one; a release workflow that builds the kit
  tarball when a `v*` tag is pushed.
- README rewritten for a public audience: what Parquet is, the problem pqora
  solves, how it fits an AI data pipeline, a solution overview diagram,
  requirements, project status, licensing and trademarks.

### Changed
- Documentation generalized: internal project references replaced by
  platform descriptions; open points listed per platform.
- The kit tarball now includes `LICENSE` and `NOTICE`.

### Notes
- pyarrow 21.0.0 on Windows returns wrong results from `fill_null` (an Arrow
  bug fixed in 22.0.0); `verify` fails there although the stream is correct
  on Linux. Documented in `docs/DESIGN_NOTES.md` and `CONTRIBUTING.md`.

## [1.4] - 2026-10-27

### Added
- Sampling results cached per sample size in `<table>_sample_cache.json`,
  keyed by a fingerprint of the input files and schema; `--margin` applies
  after the cache, so changing it needs no new sample.
- Queries reading the sampling cache (per sample size, per column, per row
  group) at the end of `<table>.sql`.
- `EXT_<table>_BAD`: an external table over Oracle's bad files with the same
  column definitions, so rejected rows can be fixed and reloaded; a commented
  `REJECT LIMIT UNLIMITED` and reprocessing `INSERT` in `<table>.sql`.
- Sampling progress with rate and ETA; sampling in a `spawn` process pool
  (JSON columns are serialized in Python and were GIL-bound in threads).
- User guide, developer guide, design notes and the "Why pqora" overview with
  diagrams.

### Changed
- `--out-dir` defaults to the current directory.
- Empty size bands are omitted from column comments.
- Kit reruns clean up previous results correctly.

## [1.3] - 2026-10-27

### Added
- Statistics-based date check of every date/timestamp column in every row
  group before the load; values are read only for row groups without usable
  statistics or with a suspicious range.
- `--out-of-range-dates error|null|clamp` for dates Oracle cannot store
  (years outside 0001-9999, the 1582-10-05..14 calendar gap).
- Interval-like date columns (all values in years 0000-0001) detected and
  loaded as `NUMBER`; `--column-type COL=number|text|date` overrides.
- Verification queries at the end of `<table>.sql` that read the manifest
  with `JSON_TABLE(TO_BLOB(BFILENAME(...)))`.

## [1.2] - 2026-10-26

### Added
- `--stage-large-records`: the main table has no LOB columns; records with a
  value larger than its column are diverted to `<table>_CLOB` through
  per-part side files.
- Value counts per Oracle size band (4000 / 32767 / above) in sampled column
  comments.
- Multi-threaded sampling.
- One-line `CREATE TABLE <table>_STG COMPRESS FOR QUERY HIGH AS SELECT` and
  `ALTER SESSION ENABLE RESUMABLE TIMEOUT 3600` at the top of `<table>.sql`.

### Fixed
- `OverflowError: date value out of range` for dates outside years 1-9999
  inside JSON (list/map/struct) columns.

## [1.1] - 2026-10-23

### Added
- `--threads` (pyarrow threads per preprocessor process) and `--pre-buffer`
  (large coalesced reads for row groups up to a size, good for NFS).
- The inspect report embedded in `<table>.sql` as comments.

### Changed
- External tables are named `EXT_<table>`; list columns get the `_LST`
  suffix.

## [1.0] - 2026-10-22

### Added
- First version of pqora: `inspect`, `generate`, stream mode (the
  `ORACLE_LOADER` preprocessor), `verify`, one `.pqpart` per row group,
  length-prefixed record format, flattened structs, JSON lists and maps,
  NULL vs empty LOB indicator, checksum self-test and the unattended test
  kit.
- `resize` command, `--field-size` and `--auto-field-size-fix` to widen
  columns from the failed load's logs without rescanning the data.
- One directory object for everything by default.
- `parquet_inspect.py`, the earlier prototype for the native `ORACLE_BIGDATA`
  driver (2026-10-19), kept for reference.

[Unreleased]: https://github.com/nextria-ca/oracle_parquet_reader/compare/v1.4...HEAD
[1.4]: https://github.com/nextria-ca/oracle_parquet_reader/releases/tag/v1.4
[1.3]: https://github.com/nextria-ca/oracle_parquet_reader/commits/main
[1.2]: https://github.com/nextria-ca/oracle_parquet_reader/commits/main
[1.1]: https://github.com/nextria-ca/oracle_parquet_reader/commits/main
[1.0]: https://github.com/nextria-ca/oracle_parquet_reader/commits/main
