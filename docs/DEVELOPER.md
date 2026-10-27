# pqora Developer Guide

Internals of `pqora.py` and its test tooling. User-facing behaviour is in
[USER_GUIDE.md](USER_GUIDE.md); the reasons behind the design and the verified
Oracle/pyarrow behaviours are in [DESIGN_NOTES.md](DESIGN_NOTES.md).

- [1. Hard constraints](#1-hard-constraints)
- [2. Architecture](#2-architecture)
- [3. Code map of pqora.py](#3-code-map-of-pqorapy)
- [4. Data structures and file formats](#4-data-structures-and-file-formats)
- [5. The stream format (byte level)](#5-the-stream-format-byte-level)
- [6. Pipelines](#6-pipelines)
- [7. Testing](#7-testing)
- [8. Release](#8-release)
- [9. How to extend](#9-how-to-extend)

---

## 1. Hard constraints

These shape most of the code; keep them when changing anything.

1. **Python 3.6 and pyarrow 6.0.1** (Oracle Database Appliance, Oracle Linux 7).
   No f-string `=`, walrus, `dataclasses`, `from __future__ import annotations`,
   `subprocess.run(capture_output=)`, newer pyarrow compute functions or
   keyword forms. Check with `vermin -t=3.6-` and test with pyarrow 6.0.1
   (section 7). Notable pyarrow 6 gaps: no decimal→string cast, no
   tz-timestamp→string cast, no struct/map casts, `replace_substring` needs
   keyword arguments, `binary_join_element_wise` has no large_binary kernel.
2. **Single file**: `pqora.py` is copied to database servers by hand; it must
   have no dependencies beyond pyarrow and numpy.
3. **Preprocessor environment**: Oracle runs the wrapper as the database OS user
   with an **empty environment** (no `PATH`, no `LANG` → C locale on 3.6),
   one argument (the location file path), stdout = data. Therefore: absolute
   paths everywhere, all file I/O with explicit `encoding="utf-8"`, binary
   stdout (`sys.stdout.buffer`), ASCII-safe stderr.
4. **Vectorized encoding**: no per-row Python in the stream path except for
   JSON columns and decimals with precision > 18.
5. **Fail safe**: a value that would be stored differently from the source must
   stop the load (or follow an explicit user option), never silently change.

## 2. Architecture

### Components

| Component | Runs where / as | Role |
|---|---|---|
| `pqora.py` inspect | anywhere | Metadata report |
| `pqora.py generate` | DB server, any user | Plans parts, sizes columns, checks dates, writes parts, DDL, manifest, wrapper |
| `pqora.sh` | DB server, as `oracle`, by ORACLE_LOADER | `exec <python> <pqora.py> "$1"` |
| `pqora.py` stream | DB server, as `oracle`, one process per part | Reads a part's row groups, writes records to stdout |
| `pqora.py verify` | anywhere | Decodes the stream to check framing |
| `pqora.py resize` | DB server | Patches part files, DDL and manifest |
| Oracle ORACLE_LOADER | database | Parses records (`RECORDS VARIABLE 10` …) into rows |
| `pqora_selftest.py` | test | Test data + checksum comparison |
| `kit/run_kit.sh` | test, on a DB server | Runs every experiment, packs a report |

### Data flow (load)
```
generate:  Parquet footers ─┬─► plan parts (row groups → parts)
                            ├─► build column specs (types, names, sizes)  ◄── sampling (optional)
                            ├─► date check (statistics, scan if needed)
                            └─► write: parts (.pqpart JSON), <table>.sql, manifest, pqora.sh

load:      INSERT … SELECT FROM EXT_<t>
             Oracle QC ─► PX servers ─► for each assigned LOCATION file (part):
                 ORACLE_LOADER runs  pqora.sh /path/<t>_000k.pqpart
                     pqora.py stream: read_part → check_source → for each row group:
                         iter_batches → prepare (columns → text/binary arrays)
                                      → [staged: large_mask → split → assemble large → .dat file]
                                      → assemble (records) → stdout
                 ORACLE_LOADER parses records → rows → INSERT
```

### Sequence of one part (for a sequence diagram)
1. Oracle PX server opens location file `X.pqpart` → starts `pqora.sh X.pqpart`.
2. `main()` sees one argument that is a part file → `run_stream`.
3. `read_part` (JSON) → `log_line(start)` → (staged: open `<file>.tmp.<pid>`).
4. `stream_part`: set pyarrow thread pools (`--threads`); for each source file:
   `check_source` (size + row group row counts), then for each row group open
   the file with `pre_buffer` (≤ `--pre-buffer`) or 8 MB streaming,
   `iter_batches(row_groups=[rg])`.
5. Per batch: `prepare` → (staged: `large_mask`, `assemble` the large rows with
   the `_CLOB` specs → stage file) → `assemble` → `records_buffer` → write stdout.
6. Oversize values: stop writing, keep measuring the rest of the part, raise
   `Oversize` with every column's maximum.
7. Success: flush, rename stage file, `log_line(end, ok, rows, bytes, mb_per_s, …)`, exit 0.
   Broken pipe (Oracle stopped reading): discard stage temp file, log, exit 0.
   Error: discard, message to stderr (→ `KUP-04095`), log, exit 1.

## 3. Code map of pqora.py

Sections in file order (search for the `# ----` headers):

| Section | Key functions | Purpose |
|---|---|---|
| helpers | `parse_size`, `expand_inputs`, `natural_key` | CLI value parsing, input expansion |
| metadata | `RowGroup`, `FileInfo`, `read_file_info`, `load_files` | Footer reading; per-column aggregates |
| inspect | `print_inspect`, `lpt_efficiency`, `compact_logical` | The report (also embedded in `<table>.sql`) |
| arrow type helpers | `unwrap`, `is_text/binary/json_type/list_type`, `plain`, `struct_child`, `json_safe`, `to_json_array` | Type predicates; nested → JSON without Python datetimes |
| sampling | `measure`, `_len_stats`, `sample_row_group`, `sample` | Max byte lengths + size-band counts, thread pool over row groups |
| column specs | `Namer`, `scalar_spec`, `build_specs`, `nice_size`, `band_note`, `field_layout`, `field_max_bytes` | Schema → list of column spec dicts |
| stream mode: encoding | `to_text`, `fixed_text`, `fix_dates`, `date_ints`, `date_problems`, `prepare`, `large_mask`, `assemble`, `records_buffer`, `check_source`, `stream_part`, `run_stream`, `Oversize` | The preprocessor |
| verify mode | `Decoder`, `run_verify` | Incremental decoder of the record format |
| generate mode | `plan_parts`, `table_part_files`, `ddl_*`, `write_ddl`, `write_alter`, `wrapper_text`, `wrapper_state` | DDL text generation |
| column size changes | `parse_field_size`, `find_spec`, `scan_logs`, `apply_size_changes`, `grow_readsize` | `--field-size`, `--auto-field-size-fix` |
| generate / resize | `date_check`, `_scan_dates`, `parquet_per_day`, `date_note`, `apply_column_type`, `is_interval_like`, `stage_main_specs`, `stage_settings`, `run_generate`, `write_manifest`, `run_resize` | Orchestration |
| main | `main`, `is_part_file` | Mode dispatch and argparse |

Mode dispatch (`main`): exactly one non-option argument that is a part file
(`.pqpart` or JSON starting with `"pqora_part"`) → stream; `verify` /
`generate` / `resize` subcommands; anything else → inspect.

## 4. Data structures and file formats

### 4.1 Column spec (dict) - the central structure
Created by `build_specs`, possibly changed by `apply_column_type`,
`stage_main_specs`, `apply_size_changes`; stored in every part file and the
manifest; read by DDL generation, stream mode, verify and the self-test.

| Key | Present | Meaning |
|---|---|---|
| `name` | always | Oracle column name (also the ORACLE_LOADER field name) |
| `source` | always | Parquet path as a list (`["user", "name"]`) |
| `arrow` | always | Arrow type text (for comments) |
| `kind` | always | `int float decimal bool date timestamp time duration null text clob raw blob epoch` |
| `otype` | always | Oracle column type |
| `note` | always | Comment text (sizing, date check, retyping …) |
| `max` | text, raw | Declared maximum bytes (also the VARCHARC/VARRAWC max) |
| `json` | text/clob from nested | Value is JSON text of a list/map/union |
| `render` | text from date/timestamp | `date` / `timestamp`: ISO text rendering (`--column-type text`) |
| `from_kind` | retyped | Original kind before `--column-type` / interval detection |
| `unit`, `tz` | timestamp, duration | Arrow unit; tz name |
| `scale`, `precision` | decimal | |
| `date_check` | date/timestamp | Result of `date_check` for this column |

Kind → field encoding (`field_layout`): `timestamp` CHAR(29), `date` CHAR(10),
`clob`/`blob` indicator + 10-digit length, `text`/`raw` width = digits of
`max`, `bool`/`null` width 1, everything else (numbers, time, duration, epoch)
width 2 with max 60.

### 4.2 Part file (`<table>_NNNN.pqpart`, JSON)
```json
{
 "pqora_part": 1,                       // magic + format version (PART_FORMAT)
 "pqora_version": "1.3", "generated": "2026-10-27T…",
 "table": "SALES", "ext_table": "EXT_SALES",
 "part": 3, "parts": 64, "rows": 250000,
 "readsize": 1048576, "lob_max": 1048555,
 "threads": 8, "pre_buffer": 134217728, "out_of_range_dates": "error",
 "log_file": "/u01/pqora/sales_stream.log",
 "sources": [ { "path": "/data/sales/part-0001.parquet", "size": 123456789,
                "row_groups": [4, 5], "rg_rows": [125000, 125000],
                "rg_bytes": [40000000, 40100000],          // compressed, for --pre-buffer
                "batch_rows": 65536 } ],
 "columns": [ /* column specs */ ],
 "stage": { "table": "SALES_CLOB", "readsize": …, "lob_max": …,      // staged mode only
            "file": "/u01/pqora/sales_clob_0003.dat", "columns": [ /* all-LOB specs */ ] },
 "resized": "…"                          // set by resize
}
```
Stream mode validates `size` and every `rg_rows` against the file before reading.

### 4.3 Manifest (`<table>_manifest.json`)
Top level: `table`, `rows`, `readsize`, `pqora_version`, `generated`,
`settings`, `columns`, `parts`.
- `settings`: everything needed to regenerate the DDL (`table`, `ext_table`,
  directory objects and paths, `wrapper_name`, `dop`, `max_string_size`,
  `max_readsize`, `margin`, `readsize`, `lob_max`, `threads`, `pre_buffer`,
  `out_of_range_dates`, `files`, `total_rows`, `location` (LOCATION order),
  `inspect_report` (text), `stage` (staged mode: table names, readsize,
  columns, `location` of `.dat` files)).
- `parts[]`: `name`, `rows`, `compressed`, `uncompressed`,
  `sources[]` (`path`, `row_groups`, `rows`).
The verification queries in `<table>.sql` read `rows`, `parts`, `columns`
and the nested `sources` with `JSON_TABLE`.

### 4.4 Stream log line
See USER_GUIDE section 12. Written by `log_line` (append, one JSON object per
line, failures to write are ignored).

## 5. The stream format (byte level)

ORACLE_LOADER access parameters (from `ddl_external`):
```
RECORDS VARIABLE 10  CHARACTERSET AL32UTF8  STRING SIZES ARE IN BYTES
PREPROCESSOR <exec_obj>:'pqora.sh'  READSIZE <n>  BADFILE …  LOGFILE …
FIELDS ( … )
```
Record = `LLLLLLLLLL` (10 ASCII digits, byte length of the rest) + fields in
column order:

| Field kind | Bytes | DDL |
|---|---|---|
| text/number | `w` ASCII digits = byte length, then UTF-8 bytes; length 0 = NULL | `"C" VARCHARC(w,max)` |
| raw | `w` digits, raw bytes | `"C" VARRAWC(w,max)` |
| timestamp | 29 bytes `YYYY-MM-DD HH24:MI:SS.fffffffff`, all blanks = NULL | `"C" CHAR(29) DATE_FORMAT TIMESTAMP MASK "YYYY-MM-DD HH24:MI:SS.FF9" NULLIF "C" = BLANKS` |
| date | 10 bytes `YYYY-MM-DD`, blanks = NULL | `"C" CHAR(10) DATE_FORMAT DATE MASK "YYYY-MM-DD" NULLIF "C" = BLANKS` |
| clob/blob | `N`/`V` indicator byte, then 10 digits + bytes | `"PQORA$INDk" CHAR(1)`, `"C" VARCHARC(10,lob_max) NULLIF "PQORA$INDk" = 'N'` |

Number text: integers in decimal; floats shortest round-trip (`1e+300`,
`nan`, `inf`, `-inf`); decimals `unscaledE-scale` (precision ≤ 18, vectorized
from the 128-bit buffer) or Python `str(Decimal)`; bool `1`/`0`.
Example (`ID`=1, `NAME`='héllo', `TS` NULL):
`0000000042` `01` `1` `06` `héllo` `<29 blanks>` …

Records are built with `binary_join_element_wise` over whole columns; the
output buffer is the values buffer of the resulting binary array
(`records_buffer`), written with one `write` per batch.

## 6. Pipelines

### 6.1 generate (`run_generate`), in order
1. Resolve directories/objects (exec/log default to parts), check writability,
   compute wrapper text and decide whether it must be written (`wrapper_state`)
   - before touching any part file.
2. `load_files`: footers of all inputs; one schema required.
3. Table/ext names (`Namer`, `ident`).
4. Optional `sample` (thread pool) → `maxes`, size `bands`.
5. `build_specs` (types, names, sizes, notes).
6. `date_check` (statistics → flagged row groups → `_scan_dates` in threads),
   `--column-type` overrides and interval detection (`apply_column_type`),
   date notes.
7. Staged mode: `stage_main_specs` (no LOBs in the main table).
8. READSIZE from the specs (`field_max_bytes`, sampled LOB sizes).
9. `--field-size` / `--auto-field-size-fix` (`scan_logs`, `apply_size_changes`,
   `grow_readsize`).
10. Staged mode: `stage_settings` (all-LOB specs, READSIZE).
11. `plan_parts`; delete this table's old parts (`table_part_files`); write
    parts; LOCATION order = largest parts first.
12. `write_ddl`, wrapper (if needed), `write_manifest`; print summary.

### 6.2 resize (`run_resize`)
Load manifest + specs from the first part → `scan_logs` (optional) →
`apply_size_changes` (staged mode caps at the limit instead of LOB) →
`grow_readsize` → rewrite every part (columns, readsize, lob_max) →
`write_ddl`, `write_alter`, `write_manifest`.

### 6.3 Date check (`date_check`)
For each date/timestamp spec (Parquet path = `".".join(source)`), each row
group: statistics usable if `is_stats_set`, `has_min_max`, not INT96 and the
unit is known (`parquet_per_day` from the logical type JSON or legacy
converted type). Raw min/max → days (floor division). Clean if the range is
inside 0001-01-01..9999-12-31 and does not touch 1582-10-05..14; otherwise, or
without statistics, the row group is read (`_scan_dates`) and values are
counted (`date_problems`). Results feed the column note, `is_interval_like`
(min ≥ 0000-01-01 and max ≤ 0001-12-31), the generate output and the manifest.

### 6.4 Staged mode in stream (`stream_part`)
`large_mask`: OR over text/raw specs of `binary_length > max`. Large rows are
filtered out of every prepared column, assembled with the stage specs (all
LOB, same field order) and written to the stage file; the rest are assembled
with the main specs. Row accounting: `rows + large == part rows`.

## 7. Testing

### 7.1 Offline (no database)
```bash
python3 pqora_selftest.py make /tmp/t --big-rows 300000 --blob-mb 1,5
python3 pqora.py /tmp/t/types.parquet                       # inspect
python3 pqora.py generate /tmp/t/types.parquet --table T --out-dir /tmp/t/p --sample-size all
python3 pqora.py verify /tmp/t/p/t_*.pqpart                 # framing of every record
env -i /bin/sh /tmp/t/p/pqora.sh /tmp/t/p/t_0001.pqpart | wc -c   # as Oracle runs it
```
Test data (`pqora_selftest.py make`): `types.parquet` (every type and the nasty
cases: NaN/±Inf/-0.0/subnormals, decimal(38), all timestamp units and
time zones, Unicode/emoji/newlines/NUL bytes, NULL vs empty, nested
struct/list/map, Parquet format 2.6), `big.parquet` (CSV-like, throughput),
`blobs.parquet` (one row group per BLOB size), `gap.parquet` (dates Oracle
can't store), `interval.parquet` / `interval_nostats.parquet` (dates in years
0000-0001, with and without statistics).

### 7.2 Exact comparison with Oracle (`pqora_selftest.py`)
- `expected PART [--stage]` - per-column checksums computed with pyarrow.
- `sql PART TABLE [--dop n] [--stage]` - one query computing the same in Oracle
  (one scan, `UNPIVOT` to `CHK|column.check|value` lines). Checks per kind:
  counts, sums (numbers), MD5 sums of text/RAW (NULLs excluded explicitly -
  `STANDARD_HASH(NULL)` hashes the empty string), float bit patterns
  (`UTL_RAW.CAST_FROM_BINARY_*`, -0.0 normalized), CLOB/BLOB lengths and
  head/tail hashes, date/timestamp text hashes (not arithmetic: Julian
  calendar before 1582), maxima.
- `compare expected.json SPOOL` - PASS/FAIL per check (sums tolerate NUMBER's
  38-digit rounding).
- `stageview PART VIEW` - view `<t>` UNION ALL `<t>_CLOB` for staged checks.
- `parallel LOG TABLE [--since]` - concurrency from the stream log.

### 7.3 The kit (`kit/run_kit.sh`)
Unattended, on a database server; see `kit/README.txt`. Steps: environment,
test data, inspect, generate (incl. staged, null/clamp dates, interval
tables, a deliberately corrupted part), offline verify, empty-environment
wrapper run, directory object check, create + load of every table, exact
checks of external and loaded tables (and once with German NLS settings),
staged union check, parallelism and timings per DOP, ROWNUM early stop,
changed-file refusal, calendar-gap refusal, null/clamp exact values,
HCC CTAS probe, cleanup. Output: `work/pqora_report_<host>_<time>.tar.gz`
(`summary.txt` + every sqlplus output, logs, DDL; no password). Locally it
reports 39 checks.

Typical local run (database in a Docker container, `/opt/bigdata` mounted at
the same path inside):
```bash
sh kit/make_kit.sh
# unpack dist/pqora_kit_<v>.tar.gz somewhere readable by oracle, then as oracle:
WORK=<dir behind PQORA_* objects> PYTHON=<python with pyarrow 6> \
DB_CONNECT='<user>/<password>@//localhost:1521/<service>' ./run_kit.sh
```

### 7.4 Python 3.6 / pyarrow 6.0.1
- Static: `vermin -t=3.6- --no-tips --violations pqora.py pqora_selftest.py`.
- Runtime: a venv with Python 3.8 + `pyarrow==6.0.1 numpy==1.19.5` (uv can
  install 3.8; 3.6 is not available from uv), and for real 3.6 a throwaway
  container: `docker run --rm -v $PWD:/w python:3.6-slim bash -c 'pip install
  pyarrow==6.0.1 numpy==1.19.5 && cd /w && python pqora.py …'`.
- Lint: `pyflakes pqora.py pqora_selftest.py`.

## 8. Release

1. Change `VERSION` in `pqora.py` (appears in messages, logs, parts, manifest
   and the kit file name).
2. `sh kit/make_kit.sh` → `dist/pqora_kit_<VERSION>.tar.gz` (pqora.py,
   pqora_selftest.py, run_kit.sh, README.txt).
3. Run the kit locally; commit; ship the tarball (or just `pqora.py`) to the
   server. After upgrading, rerun `generate` so parts carry the new options.

## 9. How to extend

- **New Arrow type**: map it in `scalar_spec` (Oracle type, kind), produce
  text in `to_text` (vectorized), make sure `field_layout` gives a suitable
  field, add it to `make_types` and a check in `checks_for` / `expected_for`.
- **New DDL statement**: add a `ddl_*` function returning lines and include it
  in `write_ddl`; if the kit runs the whole `.sql`, make sure the statement
  works for a normal schema (or filter it in `run_kit.sh` like RESUMABLE/HCC).
- **New generate option that stream mode needs**: store it in the part dict
  (`run_generate`) and in `settings`; read it with `part.get(…)` and a default
  so old parts keep working.
- **New kit test**: add generation in step 4, SQL via `run_sql NAME <<<`,
  evaluate with `result PASS|FAIL|INFO`; add new tables to `TABLES` for cleanup.
