# pqora Design Notes

Why pqora is built the way it is, and the Oracle / pyarrow behaviours it relies
on. Most points were verified by experiment on Oracle AI Database 26ai
(23.26, local test system) with pyarrow 6.0.1; the test kit re-verifies them on
the target 19c system (Oracle Database Appliance).

## Decisions

### Preprocessor instead of ORACLE_BIGDATA
- Oracle's native Parquet reader (`ORACLE_BIGDATA`) rejected some production
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
(observed on the target system). `--stage-large-records` removes LOBs from
the bulk path and writes rare large records to side files read by a second
external table. Side files are written per part to a temp name and renamed on
success, because every read of the external table re-runs the preprocessor
(including interrupted ones like `ROWNUM` queries).

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

## Performance observations

- Local, 2M CSV-like rows, DOP 1/4/8: 12.1 s / 3.2 s / 1.8 s scan (before
  `--threads`/`--pre-buffer`), 8.4 s / 2.4 s / 1.5 s after.
- One CLOB column with 0.1% large values: bulk INSERT 1.20 s with the CLOB vs
  0.70 s staged (+0.65 s for the large rows) - local; the target system showed
  a much larger penalty.
- Date validation cost if values must be read: ~8 ns decode + ~11 ns check
  per value per core; with statistics usually zero reads.
- On NFS (Isilon) storage, waits show up as threads in D state and a high
  load average; large coalesced reads (`--pre-buffer`) and fewer threads per
  process help.

## Open questions / to be confirmed on 19c (kit report)

- `TO_BLOB(BFILENAME())` in the manifest verification queries.
- READSIZE 128 MB with a 100 MB BLOB on 19c.
- HCC availability on the ODA storage.
- Behaviour of `--stage-large-records` and the date modes on 19c (kit tests
  exist).

## Ideas not implemented

- `_MAP` suffix for map columns.
- Splitting very large row groups by row ranges (each part would decode the
  whole row group).
- `--column-type` for non-temporal columns (e.g. numbers as text).
