# pqora User Guide

pqora loads Parquet files into Oracle through an `ORACLE_LOADER` external table
with a preprocessor. This guide covers installing, generating, loading,
verifying and fixing problems. For internals see [DEVELOPER.md](DEVELOPER.md).

- [1. Concepts](#1-concepts)
- [2. Requirements and setup](#2-requirements-and-setup)
- [3. Workflow](#3-workflow)
- [4. Commands](#4-commands)
- [5. Generated files](#5-generated-files)
- [6. Anatomy of `<table>.sql`](#6-anatomy-of-tablesql)
- [7. Type mapping and naming](#7-type-mapping-and-naming)
- [8. Column sizes](#8-column-sizes)
- [9. Dates and timestamps](#9-dates-and-timestamps)
- [10. Large values: `--stage-large-records`](#10-large-values---stage-large-records)
- [11. Performance and tuning](#11-performance-and-tuning)
- [12. Logs and monitoring](#12-logs-and-monitoring)
- [13. Troubleshooting](#13-troubleshooting)
- [14. Known limits](#14-known-limits)

---

## 1. Concepts

| Term | Meaning |
|---|---|
| **Row group** | Horizontal slice of a Parquet file; the smallest unit pqora can read independently. |
| **Part** (`.pqpart` file) | One parallel work unit: a small JSON file listing row groups (of one or more files) plus the full column specification. One part = one `LOCATION` entry = one Oracle parallel granule. |
| **Preprocessor** | A program Oracle runs for each location file; its stdout is the data. Oracle passes exactly one argument (the location file path) and an empty environment. pqora's wrapper is `pqora.sh`. |
| **Stream mode** | `pqora.py <file>.pqpart` - what the wrapper runs: reads the part's row groups and writes records to stdout. |
| **Manifest** | `<table>_manifest.json` - every decision `generate` made (settings, columns, parts, date check). Used by `resize` and by the verification queries. |
| **External table** | `EXT_<table>` - reads the parts through the preprocessor. Every query re-reads the Parquet files. |
| **Target table** | `<table>` - a normal heap table loaded from the external table. |

Parallelism: Oracle assigns location files (parts) to parallel execution
servers; each runs its own pqora process. With 64 parts and `PARALLEL 16`,
16 pqora processes run at a time until all 64 parts are done.

## 2. Requirements and setup

### Software on the database server
- Python 3.6 or newer, pyarrow 6.0.1 or newer, numpy (and `dataclasses`
  backport on 3.6 is *not* needed). Example: Oracle Linux 7 / ODA with
  Python 3.6.8, pyarrow 6.0.1, numpy 1.19.5.
- sqlplus (for running the generated SQL and the kit).
- The Python packages must be readable by the database OS user (`oracle`),
  e.g. a system install or a venv in a world-readable directory. A package
  installed with `pip --user` in another user's home is **not** visible to
  `oracle`.

Offline install on OL7 (all wheels in the current folder; wheels must be
`cp36` and `manylinux2014`/`manylinux_2_17`):
```bash
python3 -m venv /opt/pqora_venv
/opt/pqora_venv/bin/python -m pip install --no-index --find-links . --upgrade pip   # needs pip-21.3.1 wheel
/opt/pqora_venv/bin/pip install --no-index --find-links . ./*.whl
chmod -R o+rX /opt/pqora_venv
```
Run `generate` with that interpreter (`/opt/pqora_venv/bin/python pqora.py generate …`)
so the wrapper calls it; or pass `--python`.

### Files and permissions
The database OS user must be able to:
- read and execute the wrapper `pqora.sh` and read `pqora.py`,
- read every Parquet file (paths are recorded as seen by the server),
- read the part files and manifest,
- write the log directory (ORACLE_LOADER logs, pqora stream log, large-record files).

### Directory objects (DBA, once)
Default: one object for everything (the parts directory):
```sql
CREATE OR REPLACE DIRECTORY PQORA_PARTS AS '/u01/pqora';
GRANT READ, WRITE, EXECUTE ON DIRECTORY PQORA_PARTS TO <schema>;
```
Separate objects are possible (`--exec-dir-object/--exec-dir`,
`--log-dir-object/--log-dir`): exec needs READ, EXECUTE; parts READ; log
READ, WRITE. The header of every generated `.sql` lists the objects, paths and
privileges it needs.

Optional: `GRANT RESUMABLE TO <schema>` (the `.sql` starts with
`ALTER SESSION ENABLE RESUMABLE TIMEOUT 3600`).

## 3. Workflow

1. **Inspect** the files (read-only, metadata only):
   `python3 pqora.py /data/sales/`
2. **Generate** parts, DDL, manifest and wrapper:
   `python3 pqora.py generate /data/sales/ --table SALES --out-dir /u01/pqora --sample-size all`
   Read the output: `Dates`, `Retyped`, `Large`, `Resized` and `WARNING` lines
   point at anything unusual.
3. **Review** `/u01/pqora/sales.sql` (column comments explain every type and size).
4. **Load**: run `sales.sql` in sqlplus (or line by line).
5. **Verify**: the end of `sales.sql` compares loaded vs expected rows and
   shows the manifest as tables.
6. **If a value was too large**: `pqora.py resize /u01/pqora/sales_manifest.json --auto-field-size-fix`,
   run `sales_alter.sql`, rerun the INSERT.

Several tables: one `generate` per table (it treats all given files as one
table and requires one schema). Loop:
```bash
for f in /data/export/*.parquet; do            # one file per table, name from the file
  python3 pqora.py generate "$f" --out-dir /u01/pqora --sample-size all
done
for d in /data/export/*/; do                   # one directory per table
  python3 pqora.py generate "$d" --table "$(basename "$d")" --out-dir /u01/pqora --sample-size all
done
```
All tables can share the same directories; every file is prefixed with the table name.

If the Parquet files change, rerun `generate` (stream mode refuses a file whose
size or row groups differ from what was recorded).

## 4. Commands

The first argument selects the mode.

### 4.1 Inspect (default)
```
pqora.py PATHS... [--target-size SIZE] [--dop-list 2,4,8,16,32,64] [--list-rowgroups]
```
PATHS: files, globs or directories (`*.parquet`, natural sort order).
Reports: file count, rows, row groups, file size (byte length, not allocated
space - storage like Isilon reports more), compressed/uncompressed size,
format versions, codecs, writer; per column: physical type, logical type,
codec, sizes, ratio, dictionary pages, statistics, encodings; row group size
statistics and distribution; load balance per DOP for the parts `generate`
would create; notes (huge/tiny row groups, INT96, features the native Oracle
driver struggles with).

### 4.2 generate
```
pqora.py generate PATHS... --out-dir DIR [options]
```
| Option | Default | Meaning |
|---|---|---|
| `--out-dir DIR` | required | Where parts, `.sql`, manifest go (OS path of `--parts-dir-object`). |
| `--table NAME` | file name | Target table. |
| `--ext-table NAME` | `EXT_<table>` | External table. |
| `--parts-dir-object OBJ` | `PQORA_PARTS` | Directory object of `--out-dir`; exec and log objects default to it. |
| `--exec-dir-object OBJ` / `--exec-dir PATH` | parts object / out-dir | Where `pqora.sh` lives (path required if the object differs). |
| `--log-dir-object OBJ` / `--log-dir PATH` | parts object / out-dir | ORACLE_LOADER log/bad files, pqora stream log, large-record files. `--log-dir-object ''` = no log files. |
| `--ddl-dir PATH` | out-dir | Where `<table>.sql` goes. |
| `--wrapper-name NAME` | `pqora.sh` | Wrapper file name. |
| `--python PATH` | current interpreter | Interpreter the wrapper calls. |
| `--script PATH` | this `pqora.py` | Script the wrapper calls. |
| `--pythonpath PATH` | auto | `PYTHONPATH` set in the wrapper (auto-detected if pyarrow is not in the default site-packages). |
| `--target-size SIZE` | 0 | Group consecutive row groups into parts of about SIZE uncompressed (0 = one part per row group). |
| `--sample-size SIZE` | none | Scan this much data (e.g. `500MB`, or `all` for an exact scan) to size string/binary/JSON columns. |
| `--multi-threaded-sampling N` | 16 | Row groups sampled (and date-checked) at a time. |
| `--margin X` | 1.25 | Multiplier on sampled maximum lengths. |
| `--default-varchar N` | 4000 | VARCHAR2 size when not sampled. |
| `--max-string-size standard\|extended` | standard | Database `MAX_STRING_SIZE`: VARCHAR2/RAW limit 4000/2000 or 32767. |
| `--max-name-len N` | 128 | Oracle identifier length (30 for old databases). |
| `--max-readsize SIZE` | 128MB | Upper limit for READSIZE. |
| `--readsize SIZE` | computed | Force READSIZE. |
| `--dop N` | 8 | `PARALLEL` degree in the DDL and hints. |
| `--threads N` | 8 | pyarrow threads per preprocessor process. |
| `--pre-buffer SIZE` | 128MB | Row groups up to SIZE compressed are read with large coalesced concurrent requests; larger are streamed in 8 MB reads; 0 = always stream. |
| `--out-of-range-dates error\|null\|clamp` | error | What to do with dates Oracle can't store (section 9). |
| `--column-type COL=number\|text\|date` | - | Override how a date/timestamp column is loaded (section 9). Repeatable. |
| `--no-date-check` | off | Skip the date/timestamp check. |
| `--stage-large-records` | off | Divert records with large values to `<table>_CLOB` (section 10). |
| `--field-size COL=SIZE` | - | Set a column's size (section 8). Repeatable. |
| `--auto-field-size-fix` | off | Apply size errors found in this table's logs (section 8). |

SIZE values: `500`, `8K`, `64MB`, `2G` (binary units), `all` where allowed.

### 4.3 Stream (what Oracle runs)
```
pqora.py /path/to/<table>_0001.pqpart
```
Exactly one argument that is a part file. Writes records to stdout, errors to
stderr (shown by Oracle in `KUP-04095`), one start and one end line to the
stream log. Exit code 0 on success or when Oracle closes the pipe early
(e.g. `ROWNUM`), 1 on error.

### 4.4 verify
```
pqora.py verify PART [PART...]
```
Runs stream mode into a decoder (no Oracle) and checks the framing of every
record; prints rows, bytes, maximum record vs READSIZE and per column NULL
counts and maximum lengths (also for the large-record stream in staged mode).

### 4.5 resize
```
pqora.py resize <table>_manifest.json|<any part> [--field-size COL=SIZE ...] [--auto-field-size-fix] [--log-dir DIR] [--dry-run]
```
Changes column sizes of an already generated table without rescanning data;
updates every part file, `<table>.sql` and the manifest, and writes
`<table>_alter.sql`. Needs a manifest with a `settings` section (any manifest
written since the `resize` command exists; otherwise rerun `generate` once).

## 5. Generated files

| File | Where | Written by | Content |
|---|---|---|---|
| `<table>_NNNN.pqpart` | out-dir | generate, resize | One part: Parquet paths/sizes, row groups, column specs, READSIZE, options. |
| `<table>.sql` | ddl-dir | generate, resize | All DDL and load statements (section 6). |
| `<table>_alter.sql` | ddl-dir | resize | Recreates the external table and widens the target table. |
| `<table>_manifest.json` | out-dir | generate, resize | Settings, columns, parts (with source files and row groups), date check, inspect report. |
| `<table>_sample_cache.json` | out-dir | generate with `--sample-size` | Sampling results per sample size (section 8). Delete it to force sampling again. |
| `pqora.sh` | exec-dir | generate | `exec <python> <pqora.py> "$1"` (+ `PYTHONPATH`). Not rewritten if unchanged. |
| `<table>_stream.log` | log-dir | stream mode | One JSON line per part start/end (section 12). |
| `<ext>_<n>_<pid>.log` | log-dir | Oracle | ORACLE_LOADER log files. |
| `<ext>.<n>_<pid>.bad` | log-dir | Oracle | Rejected records (same format as the stream), read by `EXT_<table>_BAD`. The dot after the table name makes `<ext>.*.bad` match only this table. |
| `<table>_clob_NNNN.dat` | log-dir | stream mode (staged) | Large records, read by `EXT_<table>_CLOB`. |

Part files are named `<table>_<number>`; regenerating a table removes only its
own parts (never those of `<table>_EU` etc.).

## 6. Anatomy of `<table>.sql`

In order:
1. **Header comments**: source summary, directory objects with paths and privileges.
2. `ALTER SESSION ENABLE RESUMABLE TIMEOUT 3600 NAME 'pqora <table>'` - suspend instead of failing on space errors (needs the RESUMABLE privilege).
3. **One-line staging CTAS**: `CREATE TABLE <table>_STG COMPRESS FOR QUERY HIGH PARALLEL n AS SELECT * FROM EXT_<table>;` (run after the external table exists; Hybrid Columnar Compression needs supported storage). Staged mode adds `<table>_CLOB_STG`.
4. `CREATE TABLE EXT_<table> … ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER … PREPROCESSOR … LOCATION (parts, largest first)) REJECT LIMIT 0 PARALLEL n;` - every column has a comment explaining its source, type and size decision.
5. Staged mode: `CREATE TABLE EXT_<table>_CLOB …` (no preprocessor, reads the large-record files).
6. **Rejected rows**: a commented `ALTER TABLE EXT_<table> REJECT LIMIT UNLIMITED`
   (load past bad rows; they go to bad files), `CREATE TABLE EXT_<table>_BAD`
   (same columns, no preprocessor, reads `LOCATION ('<ext>.*.bad')` in the log
   directory, `REJECT LIMIT UNLIMITED`) and a commented
   `INSERT /*+ APPEND */ INTO <table> SELECT * FROM EXT_<table>_BAD` to
   reprocess them after fixing the cause (section 13). Delete the bad files
   afterwards so they aren't loaded twice.
7. `CREATE TABLE <table> (…)` (and `<table>_CLOB`).
8. `ALTER SESSION ENABLE PARALLEL DML; INSERT /*+ APPEND PARALLEL(n) */ … ; COMMIT;` (and the `_CLOB` INSERT).
9. Row count check: loaded vs expected (main + large in staged mode).
10. **Verification queries** reading `<table>_manifest.json` with `JSON_TABLE(TO_BLOB(BFILENAME(...)))`: overview (expected vs loaded rows, parts, columns, READSIZE, version), rows per source file, per part, column list. When `generate` sampled, also queries reading `<table>_sample_cache.json`: one row per cached sample size (when, row groups, rows, MiB, seconds, threads), the result per column and sample size (max bytes and the counts ≤ 4000 / ≤ 32767 / > 32767), and the per row group detail.
11. **Source file report** as comments (the full inspect output at generate time).

Running the whole file in sqlplus works; statements that need a privilege or
feature you don't have (RESUMABLE, HCC) fail without affecting the rest.

## 7. Type mapping and naming

| Parquet / Arrow | Oracle | Stream field |
|---|---|---|
| boolean | `NUMBER(1)` (1/0) | `VARCHARC(1,1)` |
| int8 / uint8 | `NUMBER(3)` | `VARCHARC(2,60)` |
| int16 / uint16 | `NUMBER(5)` | " |
| int32 / uint32 | `NUMBER(10)` | " |
| int64 | `NUMBER(19)` | " |
| uint64 | `NUMBER(20)` | " |
| float16 / float32 | `BINARY_FLOAT` | " (shortest round-trip text, `nan`/`inf`) |
| float64 | `BINARY_DOUBLE` | " |
| decimal(p,s), p ≤ 38 | `NUMBER(p,s)` | " (`unscaledE-s`) |
| decimal(p,s), p > 38 | `NUMBER` | " |
| date32 / date64 | `DATE` | `CHAR(10)` `YYYY-MM-DD` |
| timestamp(s/ms/us/ns) | `TIMESTAMP(0/3/6/9)` (tz-aware → UTC) | `CHAR(29)` `YYYY-MM-DD HH24:MI:SS.FF9` |
| time32/64 | `VARCHAR2(18 BYTE)` | `VARCHARC` |
| duration | `NUMBER(19)` (count of its unit) | `VARCHARC` |
| null | `VARCHAR2(1 BYTE)` | `VARCHARC` |
| string | `VARCHAR2(n BYTE)` or `CLOB` | `VARCHARC(w,n)` / indicator + `VARCHARC(10,max)` |
| binary / fixed_size_binary | `RAW(n)` or `BLOB` | `VARRAWC(w,n)` / indicator + `VARRAWC(10,max)` |
| struct | flattened: one column per leaf, `PARENT#CHILD` | per leaf |
| list (any) | JSON text, name suffix `_LST` | as string |
| map, union | JSON text | as string |
| dictionary / extension | as their value / storage type | |

Dates inside JSON columns are ISO text (any year, `Z` for tz-aware).

**Column names**: uppercased; characters other than `A-Z 0-9 _ $ #` become `_`;
nested parts joined with `#`; a name not starting with a letter gets `C_`;
names over `--max-name-len` are shortened with an 8-character hash; duplicates
get `_2`, `_3`; list columns end in `_LST` (kept when shortening). All names
are quoted in the DDL.

## 8. Column sizes

**Without sampling**: strings `VARCHAR2(4000)` (`--default-varchar`), binary
`RAW(2000)`; columns whose *average* size from metadata already exceeds the
limit become CLOB/BLOB.

**With `--sample-size`** (recommended: `all`): row groups are read evenly
(16 at a time) and the maximum byte length of every string/binary/JSON column
is measured. Size = maximum × `--margin`, rounded up to a round number
(10, 20, 50, 100, 150, 200, 250, 500, …), capped at the limit; above the limit
→ CLOB/BLOB. The column comment shows the result and the distribution over the
Oracle size boundaries:
```
"S_LONG" CLOB, -- s_long: string, sampled max 16,127 B; <=4000 B: 8,711, 4001-32767 B: 9,471, >32767 B: 0
```
(binary columns use RAW's 2000/32767). Nothing above 32767 means the column
could be a VARCHAR2 on a database with `MAX_STRING_SIZE=EXTENDED`.

**Sample cache**: sampling can take long, so its outcome is stored in
`<out-dir>/<table>_sample_cache.json`, one entry per sample size (`all`, or
the byte budget such as `524288000`). A later `generate` with the same
`--sample-size` uses the entry instead of reading the data; running first with
`500MB` and later with `all` keeps both, and each is reused afterwards. An
entry is used only if the input files and schema still match (paths, sizes,
rows per row group); otherwise that size is sampled again automatically. To
force a fresh sample, delete the cache file. Each entry records the sample
size, row groups sampled of total, rows and bytes read, threads, pre-buffer,
start time and seconds, one record per row group (rows, bytes, seconds, and
per column the maximum and size-band counts) and the combined result.
`generate` prints `Sampling : …` (sampled or cached), and the manifest's
`settings.sampling` records it. `--margin` is applied after the cache, so
changing it doesn't need a new sample.

**A value larger than its column** stops the load with a message naming every
oversize column of that part and its largest value:
```
KUP-04095: preprocessor command …/pqora.sh encountered error "pqora 1.4: sales_0006.pqpart:
column COMMENT: value of 9000 bytes exceeds the declared maximum of 4000 bytes; …"
```
Fix without rescanning:
```bash
pqora.py resize /u01/pqora/sales_manifest.json --auto-field-size-fix   # from the logs
pqora.py resize /u01/pqora/sales_manifest.json --field-size COMMENT=8000 --field-size user.name=200
pqora.py resize … --field-size NOTES=lob --dry-run
```
`--auto-field-size-fix` reads the table's ORACLE_LOADER logs (exact name
`<ext>_<n>_<pid>.log`) and stream log, applies every reported size (× margin)
and READSIZE growth; sizes only grow, so old log entries are harmless. Then run
`<table>_alter.sql` (drops/creates the external table, `ALTER TABLE … MODIFY`
for widened columns; a change to CLOB can't be done with MODIFY - recreate the
empty target table) and rerun the INSERT. `--field-size` and
`--auto-field-size-fix` also work on `generate`.

**READSIZE** (the ORACLE_LOADER read buffer) must hold the largest record. It
is computed from the column sizes (with LOBs: sampled sizes × 2, or the cap),
capped by `--max-readsize` (128 MB). A larger record fails with
`record of N bytes exceeds READSIZE R`; `--auto-field-size-fix` grows it.

## 9. Dates and timestamps

Oracle stores years 0001-9999 only, and its calendar is Julian before
1582-10-15, so 1582-10-05..1582-10-14 don't exist (Oracle would silently move
them). Parquet allows anything.

**Date check (generate, always on unless `--no-date-check`)**: every
date/timestamp column, every row group (not a sample). Uses the Parquet
min/max statistics; reads values only for row groups without usable statistics
(written without statistics, INT96 timestamps) or with a suspicious range.
Columns without statistics produce a warning:
```
WARNING: WAIT: no min/max statistics in 4 of 4 row group(s) - scanning the values
```
Results:
```
Dates    : VALID_TO: 3 value(s) Oracle can't store (e.g. 1582-10-10, 10000-01-01, 0000-12-29); --out-of-range-dates error: the load will FAIL
Dates    : 7 column(s) checked in every row group: all storable in Oracle
```
and in each column comment, e.g. `dates 2020-09-13 .. 2023-11-14`.

**`--out-of-range-dates`** (load time): `error` (default) stops with column,
count and examples; `null` loads those values as NULL; `clamp` loads
0001-01-01 00:00 / 9999-12-31 23:59:59.999999999 and moves gap dates to
1582-10-15 (same time of day). The stream log counts changed values per column.

**Interval detection**: a date/timestamp column whose whole range is in years
0000-0001 almost always holds durations (0000-01-01 + N days) or year-less
dates. It is loaded as `NUMBER` holding the raw Parquet value - days (date)
or the timestamp unit (timestamp) since 1970-01-01 - with a comment:
```
"WAIT" NUMBER(10), -- wait: date32[day], all values in years 0000-0001, looks like an interval
  (--column-type WAIT=date|text to change): loaded as NUMBER = days since 1970-01-01 as stored in
  Parquet (days since 0000-01-01 = value + 719528); values as dates 0000-01-01 .. 0001-11-30
```

**`--column-type COL=TYPE`** (date/timestamp columns): `number` (raw value as
above), `text` (VARCHAR2 ISO text exactly as stored, any year, e.g.
`0000-03-15`), `date` (normal DATE/TIMESTAMP; disables interval detection).
COL is the Oracle column name or the Parquet path (`event.valid_to`).

## 10. Large values: `--stage-large-records`

A CLOB column in the external table slows the whole load. With this option:
- `EXT_<table>` / `<table>` have **no LOB columns**: would-be CLOB/BLOB columns
  become `VARCHAR2`/`RAW` at the limit (4000/2000, or 32767 extended).
- While `EXT_<table>` is read, every record with a value larger than its
  column is **not** returned but written (all columns) to
  `<table>_clob_NNNN.dat` in the log directory, one file per part. Files are
  written under a temporary name and renamed when the part completes;
  interrupted reads keep the previous files.
- `EXT_<table>_CLOB` (no preprocessor; all string columns CLOB, binary BLOB)
  reads those files into `<table>_CLOB`.
- `<table>.sql` loads `<table>` first, then `<table>_CLOB`, then checks
  `<table>` + `<table>_CLOB` = expected rows.

`EXT_<table>_CLOB` is complete only after a full read of `EXT_<table>`. A
"large" record is one with any value above its main-table column size, so with
a partial `--sample-size` records that merely exceed the sampled size also go
there (no load failure, but check `<table>_CLOB`). `resize` caps sizes at the
limit instead of converting to LOB in staged mode.

## 11. Performance and tuning

| Lever | Effect |
|---|---|
| Row group size | The smallest parallel unit. ~128 MB uncompressed is a good size; multi-GB row groups cap parallelism and need that much memory per reader. |
| `--target-size` | Groups many small row groups into fewer parts (less per-process overhead). |
| `--dop` | Oracle parallel degree; useful up to the number of parts. The inspect report shows load balance per DOP. |
| `--threads` (8) | pyarrow threads inside each preprocessor process (decoding columns, concurrent reads). |
| `--pre-buffer` (128 MB) | Row groups up to this compressed size are read with large merged requests (good for NFS/Isilon); memory ≈ one row group per process. |
| `--stage-large-records` | Keeps LOB handling out of the bulk load. |
| `--multi-threaded-sampling` (16) | Speeds up sampling and the date check. |

On Linux the load average counts threads waiting on NFS (D state), so a high
load with few busy processes usually means storage waits. Useful while loading:
```bash
ps -eLo stat,comm | awk '$1 ~ /^D/' | sort | uniq -c      # threads waiting on I/O
nfsiostat 5                                               # NFS throughput / latency
```
and the `mb_per_s` of each part in the stream log.

## 12. Logs and monitoring

**Stream log** `<log-dir>/<table>_stream.log`, one JSON object per line:

| Field | When | Meaning |
|---|---|---|
| `event` | always | `start` or `end` |
| `ts`, `pid`, `host`, `table`, `part` | always | |
| `threads`, `pre_buffer` | start | settings used |
| `status` | end | `ok`, `error`, `reader closed pipe` (early stop, not an error) |
| `rows`, `bytes`, `seconds`, `mb_per_s` | end ok | rows/bytes sent to Oracle |
| `large_rows` | end ok, staged | records diverted to the large-record file |
| `dates_nulled` / `dates_clamped` | end ok | `{column: count}` changed by `--out-of-range-dates` |
| `error` | end error | message |
| `oversize`, `record_bytes` | end error | `{column: largest bytes}`, largest record (used by `--auto-field-size-fix`) |

**ORACLE_LOADER logs** `<ext>_<n>_<pid>.log` (and `.bad`): field definitions
and errors. Messages `KUP-05007`/`KUP-05009` ("intra source concurrency
disabled") are normal - parallelism comes from multiple parts.

**Errors in the client**: the preprocessor's message appears in
`KUP-04095: preprocessor command … encountered error "pqora 1.4: <part>: <message>"`
(Oracle may truncate it; the stream log has the full text).

## 13. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `KUP-04095 … value of N bytes exceeds the declared maximum` | Column too small | `resize --auto-field-size-fix`, run `<table>_alter.sql`, rerun INSERT |
| `KUP-04095 … record of N bytes exceeds READSIZE` | Very large row | `--auto-field-size-fix`, or `--max-readsize` |
| `KUP-04095 … has changed: size …` | Parquet file replaced since generate | rerun `generate` |
| `KUP-04095 … outside Oracle's range` / `1582-10-05` | Dates Oracle can't store | `--out-of-range-dates null\|clamp`, or `--column-type` |
| `KUP-04095 … ModuleNotFoundError: pyarrow` | `oracle` can't see the Python packages | install readable for `oracle`; `--python`/`--pythonpath` |
| `KUP-04095` with no pqora message / permission errors | wrapper/script/Parquet not readable or not executable by `oracle` | check permissions; directory EXECUTE grant |
| `ORA-29913 … KUP-04040 file not found` | directory object path wrong, part missing | compare header comments of `<table>.sql` with `ALL_DIRECTORIES` |
| `EXT_<table>_CLOB` empty or file not found | `EXT_<table>` not read completely yet | load `<table>` first |
| `ERROR: … pqora.sh exists with different content and is not writable` | wrapper owned by another user (e.g. `oracle`) | remove it as that user or use `--exec-dir` |
| `ERROR: schema of X differs from Y` | files of different tables in one `generate` | one `generate` per table |
| `OverflowError: date value out of range` (1.0, 1.1) | nested dates outside years 1-9999 | upgrade (fixed in 1.2) |
| `ORA-64307` on the `_STG` CTAS | storage doesn't support HCC | remove `COMPRESS FOR QUERY HIGH` |
| `ORA-01031` on `ENABLE RESUMABLE` | missing RESUMABLE privilege | `GRANT RESUMABLE TO <schema>` or ignore |
| Load slow, high load average | NFS waits | `--pre-buffer`, `--threads`, check mount options, `--stage-large-records` |
| Some rows rejected by Oracle (e.g. `ORA-12899`, conversion errors) and you want the rest loaded | `REJECT LIMIT 0` stops at the first one | `ALTER TABLE EXT_<table> REJECT LIMIT UNLIMITED`, load, fix the cause (e.g. `resize` + `<table>_alter.sql`, which also recreates `EXT_<table>_BAD`), `INSERT … SELECT * FROM EXT_<table>_BAD`, delete the `<ext>.*.bad` files |
| `KUP-04040 … <ext>.*.bad not found` / `KUP-05002` | no bad files (nothing rejected) | nothing to reprocess |

## 14. Known limits

- A row group is the smallest parallel unit.
- `-0.0` is stored as `0.0` by Oracle (no loader can keep it).
- Oracle dates: years 0001-9999, no 1582-10-05..14 (see section 9).
- Decimals with precision > 38 lose digits in `NUMBER`.
- READSIZE above ~128 MB is not used: 512 MB hung an Oracle 23ai session in testing.
- `resize` cannot change a VARCHAR2 target column to CLOB in place.
- Verification queries need `TO_BLOB(BFILENAME(...))` (verified on 23ai; the
  kit checks 19c).
