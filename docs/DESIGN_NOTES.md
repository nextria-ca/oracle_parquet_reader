# pqora Design Notes

Why pqora is built the way it is, and the Oracle / pyarrow behaviours it relies
on. Most points were verified by experiment on Oracle AI Database 26ai
(23.26) with pyarrow 6.0.1; the test kit re-verifies them on Oracle 19c
(Oracle Database Appliance, Oracle Linux 7). If you run the kit on another
platform, please attach the report to an issue (see CONTRIBUTING.md).

## Decisions

### Preprocessor instead of ORACLE_BIGDATA
- Oracle's native Parquet reader (`ORACLE_BIGDATA`) rejected some real-world
  files; rewriting them as Parquet format 1.0 helped only partly.
- It did not parallelize within a single Parquet file (one file = one parallel
  unit), which caps throughput for few large files.
- A preprocessor sees exactly what pyarrow can read (any format version,
  encoding, codec), and parallelism comes from many location files, each
  covering row groups.

### One location file per part, parts made of row groups
Oracle parallelizes an external table with a preprocessor only across
location files (a preprocessed file can't be split by byte ranges, and the
`RECORDS VARIABLE` format disables intra-file splitting anyway: `KUP-05007`,
`KUP-05009`). Row groups are the natural independently-readable unit of
Parquet. Part files carry everything stream mode needs, because the
preprocessor receives only the location file path - no parameters, no
environment.

### Length-prefixed format
Chosen for maximum data safety: no delimiters, no escaping, binary values
raw (no hex). `RECORDS VARIABLE 10` + `VARCHARC`/`VARRAWC` fields. Verified to
carry newlines, quotes, control characters, NUL bytes, Unicode/emoji, 100 MB
BLOBs.

### Dates/timestamps as fixed-width CHAR
`DATE_FORMAT … MASK` can't be combined with `VARCHARC` (`KUP-01005`), so
dates use `CHAR(10)` and timestamps `CHAR(29)` with an explicit mask and
`NULLIF … = BLANKS`. Independent of session NLS settings.

### Null indicator for LOBs
A zero-length `VARCHARC`/`VARRAWC` loads as an *empty* LOB, not NULL. Parquet
distinguishes the two, so every CLOB/BLOB field is preceded by a `CHAR(1)`
indicator field (`N`/`V`) and `NULLIF ind = 'N'`.

### Exactness over convenience
Values Oracle would store differently (out-of-range dates, the 1582 gap,
oversize strings) stop the load with a clear message unless the user chooses
otherwise (`--out-of-range-dates`, `--column-type`, `--stage-large-records`,
`resize`). The test kit compares checksums of every column.

### Staged large records
Any LOB column in the external table makes the whole load much slower
(observed on a 19c Oracle Database Appliance). `--stage-large-records` removes LOBs from
the bulk path and writes rare large records to side files read by a second
external table. Side files are written per part to a temp name and renamed on
success, because every read of the external table re-runs the preprocessor
(including interrupted ones like `ROWNUM` queries).

### Sample cache
Sampling large datasets (especially `--sample-size all` over NFS) can take a
long time and is repeated whenever a table is regenerated (new options, new
version). Its outcome is therefore cached per sample size next to the parts,
keyed by a fingerprint of the input files and schema so stale results are never
reused; deleting the file forces a new sample. `--margin` and sizing rules are
applied after the cache, so they can change without re-sampling.

### Vectorized encoding
Records are built with Arrow compute (`utf8_lpad`, `binary_join_element_wise`)
over whole columns, and the output is the Arrow values buffer - no per-row
Python. The stream path is `prepare` (convert each column once) + `assemble`
(build records), so staged mode can split rows without converting twice.

## Verified Oracle behaviours

| Behaviour | Consequence |
|---|---|
| Preprocessor stderr appears in the client error (`KUP-04095 … encountered error "…"`), possibly truncated | Messages are short and start with `pqora <version>: <part>:`; the stream log keeps the full text |
| Preprocessor runs as the DB OS user, empty environment | Wrapper with absolute paths; `PYTHONPATH` if needed |
| `READSIZE` must exceed the largest record; 128 MB worked, **512 MB hung** the session in "external table read" (23ai) | `--max-readsize` default 128 MB; stream mode refuses larger records |
| Number conversion in ORACLE_LOADER was not affected by session `NLS_NUMERIC_CHARACTERS=',.'` | Typed external columns are safe; the kit re-checks with German NLS |
| `nan`, `inf`, `-inf`, `1e+300`, `12345E-2` accepted for BINARY_DOUBLE / NUMBER | Arrow's float text and `unscaledE-scale` decimals work |
| Oracle stores `-0.0` as `+0.0` (even from the raw bit pattern) | Known limit |
| Oracle's calendar is Julian before 1582-10-15; `1582-10-10` becomes `1582-10-15` | Gap dates refused (or null/clamp); date checks compare text, not day arithmetic |
| Oracle has no year 0 | Year-0000 values are out of range; interval detection for 0000-0001 columns |
| `STANDARD_HASH(NULL)` returns the hash of the empty string | Checksums exclude NULLs explicitly |
| `COUNT(blob)` is not allowed (`ORA-22849`) | Checks count with `CASE WHEN … IS NOT NULL` |
| Sums of 38-digit decimals exceed NUMBER precision and round differently by order | Sum comparisons tolerate 1e-36 relative difference |
| `JSON_TABLE(TO_BLOB(BFILENAME(dir, file)) FORMAT JSON, …)` reads a JSON file | Manifest verification queries (23ai verified; 19c checked by the kit) |
| `COMPRESS FOR QUERY HIGH` → `ORA-64307` on non-HCC storage | Staging CTAS is a convenience line; kit probes it |
| `ALTER SESSION ENABLE RESUMABLE` needs the RESUMABLE privilege (`ORA-01031`) | Convenience line; kit skips it |
| SQL*Plus does not prompt for `&` inside standalone `--` comment lines | Inspect report and notes can be embedded as comments |
| ORACLE_LOADER log names: `<ext>_<3 digits>_<pid>.log` | Log scanning matches this exactly (so `EXT_SALES` doesn't read `EXT_SALES_EU`'s logs) |
| Rejected records are written to the bad file in the original record format (the preprocessor's output, LOB indicators included) | `EXT_<table>_BAD` reads them with the same field list; verified by rejecting 14,222 of 20,000 rows and reloading them exactly |
| `LOCATION` accepts wildcards (`'ext_sales.*.bad'`); no match → `KUP-04040` + `KUP-05002` | Bad file names `<ext>.%a_%p.bad`: the dot makes the pattern unique per table |

## Verified pyarrow 6.0.1 behaviours

| Behaviour | Handling |
|---|---|
| No `decimal → string` cast | Decimals p ≤ 18: unscaled int64 from the 128-bit buffer + `E-scale`; larger: Python `str(Decimal)` |
| No cast of tz-aware timestamps to string | Cast to naive (same instant, UTC) first |
| No struct→struct or map→map cast | `json_safe` rebuilds nested arrays with `Array.from_buffers` |
| Map key field is NOT NULL; mismatched child type aborts the process (C++ check) | Map entries built with the map type's own key/item fields |
| `to_pylist()` of dates outside years 1-9999 raises `OverflowError: date value out of range` | Never convert temporal values to Python objects; `json_safe` converts to ISO text in Arrow |
| Timestamps beyond Arrow's formatter print `<value out of range: N>` | Left as is in JSON text |
| `replace_substring(_regex)` needs keyword arguments | Used with `pattern=`, `replacement=` |
| `binary_join_element_wise`: no large_binary kernel, no mixed string/binary | Everything cast to `binary` first |
| `iter_batches` with `pre_buffer=True` pre-buffers *all* requested row groups | One `iter_batches` call per row group |
| Batch size does not bound memory without `buffer_size` (whole column chunk loaded) | `buffer_size=8 MB, pre_buffer=False` for streaming reads |
| Default CPU pool = all cores per process; `OMP_NUM_THREADS` also sizes it | `pa.set_cpu_count(--threads)` in stream mode; only BLAS variables are limited at import |
| numpy 1.19's OpenBLAS starts one idle thread per CPU | `OPENBLAS_NUM_THREADS=1`, `MKL_NUM_THREADS=1` before importing numpy |
| Parquet format 1.0 can't store nanosecond timestamps | Test data written as format 2.6 where needed |
| Statistics: `min_raw`/`max_raw` give physical values; `.min/.max` convert to Python (overflow risk) | Date check uses raw values + unit from the logical type JSON |

## Newer pyarrow versions

| Behaviour | Handling |
|---|---|
| pyarrow 21.0.0 **on Windows only**: `fill_null` / `coalesce` on integer arrays return wrong values near nulls (MSVC compiler bug, [apache/arrow#47862](https://github.com/apache/arrow/issues/47862), fixed in 22.0.0). On a Windows development machine this corrupts the length prefixes and `verify` fails with `invalid literal for int()` | Linux servers are unaffected. For development on Windows use pyarrow 20.0.0 or 22.0.0+ |
| pyarrow 6.0.1 and 26.0.0 on Linux (CI) and pyarrow 6.0.1 and 20.0.0 on Windows pass the offline self-test | Keep CI green on both ends before changing the stream path |

## Python observations

| Behaviour | Handling |
|---|---|
| JSON columns (lists, maps) are serialized per row in Python; Python threads can't run that in parallel (GIL): 16 sampling threads used 103% CPU | Sampling uses a process pool (`multiprocessing`, `spawn` context): 1091% CPU, 5x faster on a list column |
| Forking a process after pyarrow has started its thread pools is unsafe | `spawn` instead of the default `fork` (Python 3.6's `ProcessPoolExecutor` has no context argument, so `multiprocessing.get_context("spawn").Pool` is used) |
| Plain string/binary measurement runs in pyarrow/numpy and releases the GIL | Threads are enough for the date-check scan (numpy) |

## Performance observations

- Local, 2M CSV-like rows, DOP 1/4/8: 12.1 s / 3.2 s / 1.8 s scan (before
  `--threads`/`--pre-buffer`), 8.4 s / 2.4 s / 1.5 s after.
- One CLOB column with 0.1% large values: bulk INSERT 1.20 s with the CLOB vs
  0.70 s staged (+0.65 s for the large rows) - local; a 19c Oracle Database Appliance
  showed a much larger penalty.
- Date validation cost if values must be read: ~8 ns decode + ~11 ns check
  per value per core; with statistics usually zero reads.
- On NFS storage (Dell Isilon in testing), waits show up as threads in D state and a high
  load average; large coalesced reads (`--pre-buffer`) and fewer threads per
  process help.

## Open points

Verified on 23ai/26ai and covered by kit tests, but not yet confirmed on every
platform. Kit reports from other systems are welcome.

- `TO_BLOB(BFILENAME())` in the manifest verification queries on 19c.
- READSIZE 128 MB with a 100 MB BLOB on 19c.
- Hybrid Columnar Compression depends on the storage; the kit probes the
  staging CTAS and reports INFO either way.
- `--stage-large-records` and the `--out-of-range-dates` modes on 19c (kit
  tests exist).
- Oracle versions other than 19c and 23ai/26ai, and non-Linux platforms, are
  untested.

## Ideas not implemented

- `_MAP` suffix for map columns.
- Splitting very large row groups by row ranges (each part would decode the
  whole row group).
- `--column-type` for non-temporal columns (e.g. numbers as text).
