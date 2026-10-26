# oracle_parquet_reader

Load Parquet files into Oracle **in parallel, one row group per parallel worker**,
through an `ORACLE_LOADER` external table whose preprocessor (`pqora.py`) reads
the Parquet data with pyarrow. Oracle's own Parquet driver (`ORACLE_BIGDATA`) is
not used, so its format/encoding incompatibilities don't apply and the
parallelism is not limited to one worker per file.

Runs on Python 3.6+ with pyarrow 6.0.1+ and numpy (e.g. Oracle Database
Appliance with Oracle Linux 7).

## Files

| File | Purpose |
|---|---|
| `pqora.py` | the tool: inspect, generate, stream (preprocessor), verify |
| `pqora_selftest.py` | test data generator and Oracle-vs-pyarrow checksum comparison |
| `kit/` | unattended test kit for a database server (`make_kit.sh` builds the tarball) |
| `parquet_inspect.py` | earlier metadata/DDL script for the native `ORACLE_BIGDATA` driver |

## Usage

```
# 1. inspect (default mode): sizes, row groups, per-column physical/logical
#    types, encodings, codecs, parallelism estimate
pqora.py /data/sales/*.parquet

# 2. generate: one .pqpart per row group (or --target-size to group them),
#    the DDL (external table + target table + parallel INSERT) and pqora.sh.
#    One run = one table; loop over files/directories for several tables.
pqora.py generate /data/sales/ --table SALES --out-dir /u01/pqora --sample-size all --dop 16

# 3. as DBA, one directory object for everything (default name PQORA_PARTS):
#      CREATE DIRECTORY PQORA_PARTS AS '/u01/pqora';
#      GRANT READ, WRITE, EXECUTE ON DIRECTORY PQORA_PARTS TO <schema>;
#    then run /u01/pqora/sales.sql
#    (separate objects: --exec-dir-object/--exec-dir, --log-dir-object/--log-dir)

# optional: check the stream framing of parts without Oracle
pqora.py verify /u01/pqora/sales_0001.pqpart
```

### Column sizes

`--sample-size all` sizes `VARCHAR2`/`RAW` columns from the data. When a value
is still too large, the load fails with a message naming every oversize column
of that part and its largest value. To fix without rescanning the data:

```
# set sizes yourself (Oracle column name or Parquet path; 'lob' = CLOB/BLOB)
pqora.py resize /u01/pqora/sales_manifest.json --field-size COMMENT=8000 --field-size user.name=200

# or take them from the failed load's logs (ORACLE_LOADER logs + pqora stream log)
pqora.py resize /u01/pqora/sales_manifest.json --auto-field-size-fix
```

`resize` updates the part files and `sales.sql`, and writes `sales_alter.sql`,
which recreates the external table and widens the target table; then rerun
the INSERT. `--field-size` and `--auto-field-size-fix` also work on `generate`.
Sizes above the `VARCHAR2`/`RAW` limit become `CLOB`/`BLOB`, and READSIZE grows
when needed. `--dry-run` shows the changes only.

### Large records (`--stage-large-records`)

A single CLOB column in the external table slows the whole load down. With
`--stage-large-records` the main external table `EXT_<table>` has no LOB
columns (would-be CLOB/BLOB columns become `VARCHAR2`/`RAW` at the limit), and
every record with a value larger than its column is written - instead of being
loaded - to `<table>_clob_NNNN.dat` in the log directory. A second external
table `EXT_<table>_CLOB` (all string columns CLOB, binary BLOB, no
preprocessor) reads those files into `<table>_CLOB`. `<table>.sql` contains
both; `EXT_<table>_CLOB` is complete only after `EXT_<table>` has been read in
full (interrupted reads keep the previous files).

### Reading

Each preprocessor process uses `--threads` pyarrow threads (default 8) to
decode a row group's columns in parallel. Row groups up to `--pre-buffer`
compressed bytes (default 128 MB) are fetched with large, coalesced, concurrent
reads (memory about one row group per process), which helps on NFS; larger
ones are streamed in 8 MB reads. `--pre-buffer 0` always streams. The stream
log records `mb_per_s` per part, to compare settings.

Oracle runs `pqora.sh <location file>` for each `.pqpart`; with one argument
that is a part file, `pqora.py` streams that part's rows to stdout. Everything
the stream needs (Parquet path and size, row groups, column specification,
READSIZE) is inside the part file, so the stream can't drift from the DDL.
Stream mode refuses to run if the Parquet file changed since `generate`.

## Stream format

`ORACLE_LOADER` with `RECORDS VARIABLE 10`, `CHARACTERSET AL32UTF8`,
`STRING SIZES ARE IN BYTES`:

| Data | Field | Notes |
|---|---|---|
| text, numbers | `VARCHARC(w,max)` | length prefix, no escaping of any byte |
| binary | `VARRAWC(w,max)` | raw bytes, no hex |
| timestamps | `CHAR(29) DATE_FORMAT TIMESTAMP MASK "YYYY-MM-DD HH24:MI:SS.FF9"` | UTC for tz-aware |
| dates | `CHAR(10) DATE_FORMAT DATE MASK "YYYY-MM-DD"` | |
| CLOB / BLOB | `CHAR(1)` indicator + `VARCHARC/VARRAWC(10,max) NULLIF ind='N'` | keeps NULL and empty LOBs apart |

Type mapping: integers → `NUMBER(p)`, decimals → `NUMBER(p,s)`, float/double →
`BINARY_FLOAT/BINARY_DOUBLE` (exact round trip), bool → `NUMBER(1)`, strings →
`VARCHAR2(n BYTE)` or `CLOB`, binary → `RAW(n)` or `BLOB`, structs flattened as
`PARENT#CHILD` columns, lists/maps → JSON text.

## Known limits (verified on Oracle 23ai)

- Oracle stores `-0.0` as `+0.0` (no loader can keep it).
- Oracle's calendar is Julian before 1582-10-15; dates keep their text, but
  1582-10-05..14 don't exist there, so pqora refuses them instead of letting
  Oracle shift them silently.
- `READSIZE` must exceed the largest record; pqora caps it at 128 MB by default
  (a 512 MB READSIZE hung on 23ai). Larger values fail with a clear error.
- A row group is the smallest parallel unit.
