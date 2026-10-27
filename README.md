# oracle_parquet_reader (pqora)

Load Parquet files into Oracle **in parallel, one Parquet row group per parallel
worker**, through an `ORACLE_LOADER` external table whose *preprocessor* is
`pqora.py`. pqora reads the Parquet data with pyarrow and streams it to Oracle
in a binary-safe, length-prefixed text format.

Oracle's own Parquet support (`ORACLE_BIGDATA`) is not used: it rejected some of
our files (format/encoding incompatibilities) and does not split a single
Parquet file across parallel workers. pqora reads any Parquet file pyarrow can
read and parallelizes by row group.

Runs on **Python 3.6+ with pyarrow 6.0.1+ and numpy** (the Oracle Database
Appliance with Oracle Linux 7 is the reference target).

## How it works in one picture

```
Parquet files ──► pqora.py generate ──► <table>_0001.pqpart … (one per row group = parallel work unit)
                                        <table>.sql             (external table, target table, INSERT, checks)
                                        <table>_manifest.json   (everything generate decided)
                                        pqora.sh                (preprocessor wrapper)

SELECT/INSERT … FROM EXT_<table>  ──►  Oracle PX servers, one per .pqpart
                                        └─► pqora.sh <part> ──► pqora.py stream mode ──► stdout records
                                                                 (reads only that part's row groups)
```

## Quick start

```bash
# 1. look at the files: rows, row groups, column types/encodings/codecs, parallelism
python3 pqora.py /data/sales/*.parquet

# 2. generate part files, DDL and wrapper (one run = one table)
python3 pqora.py generate /data/sales/ --table SALES --out-dir /u01/pqora --sample-size all --dop 16

# 3. as DBA, once: one directory object for everything
#      CREATE DIRECTORY PQORA_PARTS AS '/u01/pqora';
#      GRANT READ, WRITE, EXECUTE ON DIRECTORY PQORA_PARTS TO <schema>;

# 4. load
sqlplus <schema>@<pdb> @/u01/pqora/sales.sql
```

`sales.sql` creates `EXT_SALES` (external) and `SALES` (target), loads with a
parallel direct-path INSERT, compares loaded vs expected rows and shows the
manifest and the sampling results as tables.

## Main features

- One parallel work unit per row group (or grouped with `--target-size`).
- Exact data: every value round-trips (verified by checksum comparison in Oracle);
  NULL vs empty LOB preserved; numbers, floats (bit-exact), decimals, timestamps
  to nanoseconds, binary, Unicode, embedded newlines/NUL bytes.
- Column sizing from data (`--sample-size all`), with counts per Oracle size
  band (VARCHAR2 4000 / extended 32767 / CLOB); sampling results are cached
  per sample size (`<table>_sample_cache.json`) so later runs are instant.
- Nested data: structs flattened to `PARENT#CHILD` columns, lists (`_LST`)
  and maps as JSON text.
- Date safety: every date/timestamp checked against Oracle's range (from
  Parquet statistics), `--out-of-range-dates error|null|clamp`, interval-like
  date columns (years 0000-0001) loaded as numbers, `--column-type` overrides.
- Large values: `--stage-large-records` keeps CLOBs out of the bulk load and
  diverts records with large values to a `<table>_CLOB` table.
- Size fixes without rescanning: `resize --field-size` / `--auto-field-size-fix`
  (reads the failed load's logs).
- Tuning: `--threads`, `--pre-buffer` (large coalesced reads, good for NFS),
  multi-threaded sampling.
- A self-contained test kit (`kit/`) that runs every experiment on a database
  server unattended and returns one report.

## Documentation

| Document | For |
|---|---|
| [docs/USER_GUIDE.md](docs/USER_GUIDE.md) | Setup, workflow, all options, generated files, type mapping, dates, large records, tuning, logs, troubleshooting |
| [docs/DEVELOPER.md](docs/DEVELOPER.md) | Architecture, code map, data structures and file formats, stream format, pipelines, testing, release |
| [docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md) | Why it is built this way; verified Oracle / pyarrow behaviours; open questions |
| [kit/README.txt](kit/README.txt) | Running the test kit on a database server |
| [CLAUDE.md](CLAUDE.md) | Orientation for AI assistants working on this repo |

## Repository layout

| Path | What |
|---|---|
| `pqora.py` | The tool (single file): inspect, generate, stream (preprocessor), verify, resize |
| `pqora_selftest.py` | Test data generator and Oracle-vs-pyarrow checksum comparison |
| `kit/run_kit.sh` | Unattended test kit runner (all experiments, one report tarball) |
| `kit/make_kit.sh` | Builds `dist/pqora_kit_<version>.tar.gz` |
| `kit/README.txt` | Kit instructions (shipped inside the tarball) |
| `parquet_inspect.py` | Earlier prototype for the native `ORACLE_BIGDATA` driver (superseded by pqora) |
| `dist/` | Built kit tarballs (git-ignored) |
