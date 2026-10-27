#!/usr/bin/env python3
"""
pqora.py - load Parquet files into Oracle in parallel through ORACLE_LOADER
external tables with a preprocessor.

Modes (selected by the first argument):

  pqora.py FILE.parquet|DIR [...]          inspect (default): metadata report
  pqora.py generate FILE.parquet|DIR [...]  write part files, DDL and wrapper
  pqora.py /path/to/NAME.pqpart             stream: write the part's rows to
                                            stdout in the length-prefixed
                                            format declared in the DDL.
                                            This is how Oracle calls it (the
                                            preprocessor gets one argument:
                                            the location file path).
  pqora.py verify NAME.pqpart [...]         run stream mode into a decoder and
                                            check the record framing

Each .pqpart file is a small JSON document that lists the Parquet row groups
of one parallel work unit and the full column specification, so stream mode
never has to re-derive the mapping and can't drift from the DDL.

Stream format (ORACLE_LOADER):
  RECORDS VARIABLE 10        - every record starts with its byte length as 10
                               ASCII digits
  VARCHARC(w,max)            - text fields: w ASCII digits of byte length, then
                               the UTF-8 bytes (zero length = NULL)
  VARRAWC(w,max)             - binary fields, same framing, raw bytes
  CHAR(29) / CHAR(10)        - timestamps / dates, fixed width, blank = NULL
  CHAR(1) indicator          - before every CLOB/BLOB field: 'N' = NULL,
                               'V' = value (so NULL and empty LOBs differ)

Compatible with Python 3.6+ and pyarrow 6.0.1+.
"""

import argparse
import base64
import concurrent.futures
import datetime
import glob
import hashlib
import heapq
import io
import json
import math
import multiprocessing
import os
import re
import socket
import sys
import time

# numpy's BLAS would otherwise start one idle thread per CPU in every process
for _v in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):   # not OMP_*: that sizes pyarrow's pool too
    os.environ.setdefault(_v, "1")

import numpy as np  # noqa: E402
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

VERSION = "1.4"
PART_MAGIC = "pqora_part"
PART_FORMAT = 1
REC_WIDTH = 10                 # RECORDS VARIABLE 10
LOB_WIDTH = 10                 # VARCHARC(10,..) / VARRAWC(10,..) for LOBs
NUM_MAX = 60                   # max text length of a number field
TS_WIDTH = 29                  # YYYY-MM-DD HH24:MI:SS.FF9
TS_MASK = "YYYY-MM-DD HH24:MI:SS.FF9"
DATE_WIDTH = 10
DATE_MASK = "YYYY-MM-DD"
MIB = 1024 * 1024
DEFAULT_MAX_READSIZE = 128 * MIB   # 512 MiB hung on 23ai in testing
BATCH_TARGET_BYTES = 64 * MIB
DEFAULT_DOPS = [2, 4, 8, 16, 32, 64]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def human(n):
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return "{:.0f} {}".format(n, unit) if unit == "B" else "{:.1f} {}".format(n, unit)
        n /= 1024


def parse_size(s):
    if s.strip().lower() == "all":
        return float("inf")
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?)(?:I?B)?\s*$", s.upper())
    if not m:
        raise argparse.ArgumentTypeError("invalid size: {!r} (examples: 500MB, 2G, all)".format(s))
    mult = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}[m.group(2)]
    return int(float(m.group(1)) * mult)


def hms(seconds):
    seconds = int(round(seconds))
    return "{}:{:02d}:{:02d}".format(seconds // 3600, seconds % 3600 // 60, seconds % 60)


def warn(msg):
    sys.stderr.write("WARNING: {}\n".format(msg))


def natural_key(s):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", s)]


def expand_inputs(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            found = sorted(glob.glob(os.path.join(p, "*.parquet")), key=natural_key)
            if not found:
                warn("no *.parquet files in directory {}".format(p))
            files.extend(found)
        else:
            files.extend(sorted(glob.glob(p), key=natural_key) or [p])
    return [os.path.abspath(f) for f in files]


def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * q
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def short(name, width):
    return name if len(name) <= width else "..." + name[-(width - 3):]


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------

class RowGroup(object):
    __slots__ = ("file", "index", "rows", "compressed", "uncompressed")

    def __init__(self, file, index, rows, compressed, uncompressed):
        self.file, self.index, self.rows = file, index, rows
        self.compressed, self.uncompressed = compressed, uncompressed


class FileInfo(object):
    def __init__(self, path, md):
        self.path = path
        self.disk_size = os.path.getsize(path)
        self.rows = md.num_rows
        self.format_version = md.format_version
        self.created_by = md.created_by or "?"
        self.row_groups = []
        self.codecs = set()
        self.int96 = False


def read_file_info(path, columns):
    """columns: dict path_in_schema -> per-column aggregate (filled in)."""
    md = pq.read_metadata(path)
    fi = FileInfo(path, md)
    schema = md.schema
    for j in range(len(schema)):
        col = schema.column(j)
        c = columns.setdefault(col.path, {
            "physical": col.physical_type,
            "logical": str(col.logical_type) if col.logical_type is not None else "",
            "converted": str(getattr(col, "converted_type", "") or ""),
            "max_def": col.max_definition_level, "max_rep": col.max_repetition_level,
            "codecs": set(), "encodings": set(), "compressed": 0, "uncompressed": 0,
            "dict_chunks": 0, "chunks": 0, "stats_chunks": 0, "nulls": 0, "nulls_known": True,
        })
        if col.physical_type == "INT96":
            fi.int96 = True
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        comp = unc = 0
        for j in range(rg.num_columns):
            cc = rg.column(j)
            comp += cc.total_compressed_size
            unc += cc.total_uncompressed_size
            fi.codecs.add(cc.compression)
            c = columns[cc.path_in_schema]
            c["codecs"].add(cc.compression)
            c["encodings"].update(cc.encodings)
            c["compressed"] += cc.total_compressed_size
            c["uncompressed"] += cc.total_uncompressed_size
            c["chunks"] += 1
            if cc.has_dictionary_page:
                c["dict_chunks"] += 1
            st = cc.statistics if cc.is_stats_set else None
            if st is not None:
                c["stats_chunks"] += 1
                if st.has_null_count:
                    c["nulls"] += st.null_count
                else:
                    c["nulls_known"] = False
            else:
                c["nulls_known"] = False
        fi.row_groups.append(RowGroup(path, i, rg.num_rows, comp, unc or rg.total_byte_size))
    return fi


def load_files(paths):
    columns, files = {}, []
    for p in paths:
        try:
            files.append(read_file_info(p, columns))
        except (OSError, pa.ArrowException) as e:
            sys.exit("ERROR: cannot read {}: {}".format(p, e))
    return files, columns


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------

def lpt_efficiency(sizes, dop):
    """Greedy longest-processing-time schedule; returns (efficiency, busiest)."""
    if not sizes:
        return 0.0, 0
    workers = [0] * dop
    for s in sorted(sizes, reverse=True):
        heapq.heapreplace(workers, workers[0] + s)
    busiest = max(workers)
    return sum(sizes) / float(dop * busiest), busiest


RG_BUCKETS = [(0, 16), (16, 64), (64, 128), (128, 256), (256, 512), (512, 1024), (1024, None)]

# things the native ORACLE_BIGDATA parquet reader is known/likely to struggle with
RISKY_ENCODINGS = {"DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
                   "BYTE_STREAM_SPLIT"}
RISKY_CODECS = {"ZSTD", "LZ4", "LZ4_RAW", "BROTLI", "LZO"}


UNIT_ABBR = {"milliseconds": "ms", "microseconds": "us", "nanoseconds": "ns"}
PHYS_ABBR = {"FIXED_LEN_BYTE_ARRAY": "FLBA", "BYTE_ARRAY": "BYTE_ARR"}


def compact_logical(s):
    """'Int(bitWidth=8, isSigned=true)' -> 'Int8', etc."""
    m = re.match(r"^Int\(bitWidth=(\d+), isSigned=(\w+)\)$", s)
    if m:
        return ("Int" if m.group(2) == "true" else "UInt") + m.group(1)
    m = re.match(r"^Decimal\(precision=(\d+), scale=(\d+)\)$", s)
    if m:
        return "Decimal({},{})".format(m.group(1), m.group(2))
    m = re.match(r"^(Timestamp|Time)\(isAdjustedToUTC=(\w+), timeUnit=(\w+)", s)
    if m:
        return "{}({}{})".format(m.group(1), UNIT_ABBR.get(m.group(3), m.group(3)),
                                 ",UTC" if m.group(2) == "true" else "")
    return s


def print_inspect(files, columns, dops, list_rowgroups, target_size, out):
    def p(*a):
        print(*a, file=out)
    rgs = [rg for f in files for rg in f.row_groups]
    total_rows = sum(f.rows for f in files)
    total_comp = sum(rg.compressed for rg in rgs)
    total_unc = sum(rg.uncompressed for rg in rgs)

    p("=" * 78)
    p("PARQUET METADATA SUMMARY")
    p("=" * 78)
    p("Files            : {}".format(len(files)))
    p("Rows             : {:,}".format(total_rows))
    p("Row groups       : {:,}".format(len(rgs)))
    p("File size        : {}".format(human(sum(f.disk_size for f in files))))
    p("Compressed data  : {}".format(human(total_comp)))
    p("Uncompressed data: {}".format(human(total_unc))
      + ("   (ratio {:.2f}x)".format(total_unc / float(total_comp)) if total_comp else ""))
    p("Format versions  : {}".format(", ".join(sorted({f.format_version for f in files}))))
    p("Codecs           : {}".format(", ".join(sorted({c for f in files for c in f.codecs})) or "-"))
    p("Created by       : {}".format(", ".join(sorted({f.created_by for f in files}))))

    if len(files) > 1:
        p("")
        p("{:<40} {:>14} {:>5} {:>7} {:>11} {:>12}".format(
            "File", "Rows", "RGs", "Format", "Compressed", "Uncompressed"))
        for f in files:
            p("{:<40} {:>14,} {:>5} {:>7} {:>11} {:>12}".format(
                short(os.path.basename(f.path), 40), f.rows, len(f.row_groups), f.format_version,
                human(sum(r.compressed for r in f.row_groups)),
                human(sum(r.uncompressed for r in f.row_groups))))

    p("")
    p("Columns (all files)")
    p("  {:<30} {:<10} {:<22} {:<8} {:>10} {:>11} {:>5} {:>4} {:>5}  {}".format(
        "column", "physical", "logical", "codec", "compressed", "uncompressed", "ratio",
        "dict", "stats", "encodings"))
    for path, c in columns.items():
        ratio = c["uncompressed"] / float(c["compressed"]) if c["compressed"] else 0
        logical = c["logical"] if c["logical"] not in ("None", "") else c["converted"]
        logical = "" if logical in ("NONE", "None") else compact_logical(logical)
        dict_s = "{}/{}".format(c["dict_chunks"], c["chunks"]) if c["chunks"] else "-"
        stats_s = "yes" if c["chunks"] and c["stats_chunks"] == c["chunks"] else (
            "part" if c["stats_chunks"] else "no")
        p("  {:<30} {:<10} {:<22} {:<8} {:>10} {:>11} {:>5.1f} {:>4} {:>5}  {}".format(
            short(path, 30), PHYS_ABBR.get(c["physical"], c["physical"]), short(logical, 22), ",".join(sorted(c["codecs"])),
            human(c["compressed"]), human(c["uncompressed"]), ratio, dict_s, stats_s,
            ",".join(sorted(c["encodings"]))))

    if not rgs:
        return

    unc = sorted(rg.uncompressed for rg in rgs)
    rows = sorted(rg.rows for rg in rgs)
    comp = sorted(rg.compressed for rg in rgs)
    p("")
    p("Row group statistics")
    p("  {:<14} {:>12} {:>12} {:>12} {:>12} {:>12}".format("", "min", "p50", "avg", "p95", "max"))
    for label, vals, fmt in (("uncompressed", unc, human), ("compressed", comp, human),
                             ("rows", rows, lambda v: "{:,.0f}".format(v))):
        p("  {:<14} {:>12} {:>12} {:>12} {:>12} {:>12}".format(
            label, fmt(vals[0]), fmt(percentile(vals, .5)), fmt(sum(vals) / float(len(vals))),
            fmt(percentile(vals, .95)), fmt(vals[-1])))

    p("")
    p("Row group size distribution (uncompressed)")
    for lo, hi in RG_BUCKETS:
        n = sum(1 for v in unc if v >= lo * MIB and (hi is None or v < hi * MIB))
        label = "{}-{} MiB".format(lo, hi) if hi else ">= {} MiB".format(lo)
        bar = "#" * max(1 if n else 0, int(round(40.0 * n / len(unc))))
        p("  {:<14} {:>7}  {}".format(label, n, bar))

    parts = plan_parts(files, target_size)
    part_sizes = [sum(rg.uncompressed for rg in pt) for pt in parts]
    p("")
    p("Parallelism (greedy load balance of uncompressed bytes)")
    p("  pqora parts with --target-size {}: {} part(s)".format(
        human(target_size) if target_size else "0 (one part per row group)", len(parts)))
    p("  {:>5} | {:>12} {:>15}".format("DOP", "efficiency", "busiest worker"))
    for dop in dops:
        e, b = lpt_efficiency(part_sizes, dop)
        note = "  (DOP > parts)" if dop > len(parts) else ""
        p("  {:>5} | {:>11.0f}% {:>15}{}".format(dop, 100 * e, human(b), note))

    p("")
    p("Notes")
    notes = []
    big = sum(1 for v in unc if v > 512 * MIB)
    if big:
        notes.append("{} row group(s) > 512 MiB: a row group is the smallest parallel unit; "
                     "these cap parallelism and each needs that much memory per reader.".format(big))
    small = sum(1 for v in unc if v < 16 * MIB)
    if small > len(unc) / 2 and not target_size:
        notes.append("{} row group(s) < 16 MiB: use --target-size to group them into "
                     "larger parts.".format(small))
    if any(f.int96 for f in files):
        notes.append("INT96 timestamps present (legacy Spark/Impala); pqora reads them as "
                     "nanosecond timestamps.")
    risky_enc = sorted({e for c in columns.values() for e in c["encodings"]} & RISKY_ENCODINGS)
    risky_codec = sorted({c for f in files for c in f.codecs} & RISKY_CODECS)
    v2 = sorted({f.format_version for f in files if not f.format_version.startswith("1")})
    if risky_enc or risky_codec or v2:
        notes.append("Possible ORACLE_BIGDATA incompatibilities (pqora reads these fine): "
                     + "; ".join(x for x in (
                         "format " + ",".join(v2) if v2 else "",
                         "encodings " + ",".join(risky_enc) if risky_enc else "",
                         "codecs " + ",".join(risky_codec) if risky_codec else "") if x))
    if not notes:
        notes.append("nothing unusual")
    for n in notes:
        p("  * " + n)

    if list_rowgroups:
        p("")
        p("{:<40} {:>5} {:>12} {:>12} {:>13}".format("file", "rg", "rows", "compressed", "uncompressed"))
        for rg in rgs:
            p("{:<40} {:>5} {:>12,} {:>12} {:>13}".format(
                short(os.path.basename(rg.file), 40), rg.index, rg.rows,
                human(rg.compressed), human(rg.uncompressed)))
    p("")


# ---------------------------------------------------------------------------
# arrow type helpers
# ---------------------------------------------------------------------------

def unwrap(t):
    """Strip dictionary / extension wrappers to get the value type."""
    while True:
        if pa.types.is_dictionary(t):
            t = t.value_type
        elif isinstance(t, pa.ExtensionType):
            t = t.storage_type
        else:
            return t


def _is(name, t):
    f = getattr(pa.types, name, None)
    return bool(f and f(t))


def is_text(t):
    return _is("is_string", t) or _is("is_large_string", t) or _is("is_string_view", t)


def is_binary(t):
    return _is("is_binary", t) or _is("is_large_binary", t) or _is("is_binary_view", t)


def is_json_type(t):
    """Types serialized to JSON text (not flattened)."""
    return (_is("is_list", t) or _is("is_large_list", t) or _is("is_fixed_size_list", t)
            or _is("is_map", t) or _is("is_list_view", t) or _is("is_large_list_view", t)
            or _is("is_union", t))


def is_list_type(t):
    return (_is("is_list", t) or _is("is_large_list", t) or _is("is_fixed_size_list", t)
            or _is("is_list_view", t) or _is("is_large_list_view", t))


def needs_sampling(t):
    t = unwrap(t)
    if pa.types.is_struct(t):
        return any(needs_sampling(t[i].type) for i in range(t.num_fields))
    return is_text(t) or is_binary(t) or is_json_type(t)


def plain(arr):
    """Decode dictionary / extension arrays."""
    if isinstance(arr, pa.ChunkedArray):
        arr = pa.concat_arrays(arr.chunks) if arr.num_chunks != 1 else arr.chunk(0)
    while True:
        if pa.types.is_dictionary(arr.type):
            arr = arr.dictionary_decode()
        elif isinstance(arr.type, pa.ExtensionType):
            arr = arr.storage
        else:
            return arr


def struct_child(arr, name):
    """Child of a struct array with the parent's nulls applied."""
    arr = plain(arr)
    idx = arr.type.get_field_index(name)
    return arr.flatten()[idx]


def _json_default(v):
    if isinstance(v, (bytes, bytearray)):
        return base64.b64encode(bytes(v)).decode("ascii")
    if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
        return v.isoformat()
    return str(v)


def json_safe(arr):
    """Rebuild a (nested) array with temporal leaves turned into ISO text in
    Arrow, so to_pylist() never builds Python datetimes: those only cover years
    1-9999 and raise 'date value out of range' for anything else."""
    arr = plain(arr)
    if arr.offset:
        arr = arr.take(pa.array(np.arange(len(arr))))       # compact: children start at 0
    t = arr.type
    T = pa.types
    if T.is_timestamp(t):
        tz = t.tz is not None
        if tz:
            arr = pc.cast(arr, pa.timestamp(t.unit))          # same instant as UTC wall time
        # 'YYYY-MM-DD hh:mm:ss.f' -> ISO 'YYYY-MM-DDThh:mm:ss.f[Z]'; Arrow's own
        # '<value out of range: N>' marker for absurd values is left as it is
        return pc.replace_substring_regex(pc.cast(arr, pa.string()),
                                          pattern=r"^(-?\d+-\d\d-\d\d) (.*)$",
                                          replacement=r"\1T\2" + ("Z" if tz else ""))
    if T.is_date(t) or T.is_time(t):
        if T.is_date64(t):
            arr = pc.cast(arr, pa.date32())
        return pc.cast(arr, pa.string())
    if T.is_duration(t):
        return pc.cast(arr, pa.int64())
    if T.is_struct(t):
        kids = [json_safe(arr.field(i)) for i in range(t.num_fields)]
        new_t = pa.struct([pa.field(t[i].name, k.type) for i, k in enumerate(kids)])
        return pa.Array.from_buffers(new_t, len(arr), arr.buffers()[:1], children=kids)
    if T.is_map(t):
        keys, items = json_safe(arr.keys), json_safe(arr.items)
        new_t = pa.map_(keys.type, items.type)
        # entries must use the map type's own fields (the key field is NOT NULL)
        entries = pa.Array.from_buffers(pa.struct([new_t.key_field, new_t.item_field]), len(keys),
                                        [None], children=[keys, items])
        return pa.Array.from_buffers(new_t, len(arr), arr.buffers()[:2], children=[entries])
    if T.is_list(t) or T.is_large_list(t):
        vals = json_safe(arr.values)
        new_t = pa.list_(vals.type) if T.is_list(t) else pa.large_list(vals.type)
        return pa.Array.from_buffers(new_t, len(arr), arr.buffers()[:2], children=[vals])
    if T.is_fixed_size_list(t):
        vals = json_safe(arr.values)
        return pa.Array.from_buffers(pa.list_(vals.type, t.list_size), len(arr), arr.buffers()[:1],
                                     children=[vals])
    return arr


def to_json_array(arr):
    vals = []
    for v in json_safe(arr).to_pylist():
        if v is None:
            vals.append(None)
        else:
            vals.append(json.dumps(v, default=_json_default, ensure_ascii=False,
                                   separators=(",", ":")))
    return pa.array(vals, pa.string())


# ---------------------------------------------------------------------------
# sampling
# ---------------------------------------------------------------------------

# Oracle size boundaries reported per sampled column: (standard limit, extended limit)
TEXT_BANDS = (4000, 32767)      # VARCHAR2 / extended VARCHAR2 / beyond: CLOB
RAW_BANDS = (2000, 32767)       # RAW / extended RAW / beyond: BLOB


def _len_stats(arr, bands):
    """(max byte length, [count <= bands[0], count <= bands[1], count above])
    over the non-null values."""
    v = pc.fill_null(pc.binary_length(arr), -1).to_numpy(zero_copy_only=False)
    v = v[v >= 0]
    if not len(v):
        return 0, [0, 0, 0]
    lo, hi = bands
    return int(v.max()), [int((v <= lo).sum()), int(((v > lo) & (v <= hi)).sum()), int((v > hi).sum())]


def _record(path, stats, maxes, bands):
    m, counts = stats
    maxes[path] = max(maxes.get(path, 0), m)
    acc = bands.setdefault(path, [0, 0, 0])
    for i, c in enumerate(counts):
        acc[i] += c


def measure(arr, path, maxes, bands):
    arr = plain(arr)
    t = arr.type
    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            measure(struct_child(arr, t[i].name), path + (t[i].name,), maxes, bands)
    elif is_json_type(t):
        _record(path, _len_stats(to_json_array(arr), TEXT_BANDS), maxes, bands)
    elif is_text(t) or is_binary(t):
        if _is("is_large_string", t):
            arr = pc.cast(arr, pa.string())
        elif _is("is_large_binary", t):
            arr = pc.cast(arr, pa.binary())
        _record(path, _len_stats(arr, RAW_BANDS if is_binary(t) else TEXT_BANDS), maxes, bands)


def sample_row_group(rg, cols, batch_rows, budget, pre_limit):
    """Measure one row group (up to `budget` bytes); own file handle, so it can
    run in a worker thread. Returns a detail record (JSON-serializable)."""
    t0 = time.time()
    maxes, bands, rows, read = {}, {}, 0, 0
    pre = bool(pre_limit and rg.compressed <= pre_limit)
    pf = pq.ParquetFile(rg.file, buffer_size=0 if pre else 8 * MIB, pre_buffer=pre)
    for batch in pf.iter_batches(batch_size=batch_rows, row_groups=[rg.index], columns=cols):
        for name in cols:
            measure(batch.column(batch.schema.get_field_index(name)), (name,), maxes, bands)
        rows += batch.num_rows
        read += batch.nbytes
        if read >= budget:
            break
    return {"file": rg.file, "row_group": rg.index, "rows": rows, "rows_in_row_group": rg.rows,
            "bytes": read, "seconds": round(time.time() - t0, 3), "pre_buffer": pre,
            "columns": [{"path": list(k), "max": maxes[k], "bands": bands.get(k, [0, 0, 0])}
                        for k in sorted(maxes)]}


def _sample_worker_init(threads):
    pa.set_cpu_count(threads)
    pa.set_io_thread_count(max(threads, 4))


def _sample_task(args):
    return sample_row_group(*args)


def combine_samples(details):
    """({path tuple: max}, {path tuple: band counts}, rows) from row group records."""
    maxes, bands, rows = {}, {}, 0
    for d in details:
        rows += d["rows"]
        for c in d["columns"]:
            k = tuple(c["path"])
            maxes[k] = max(maxes.get(k, 0), c["max"])
            acc = bands.setdefault(k, [0, 0, 0])
            for i, n in enumerate(c["bands"]):
                acc[i] += n
    return maxes, bands, rows


def sample(files, schema, budget, columns, workers=1, pre_limit=0):
    """Read up to `budget` bytes spread evenly across all row groups (only the
    columns that need size estimation), `workers` row groups at a time.
    Returns a sample entry (JSON-serializable): settings, timing, one record
    per row group and the combined result."""
    cols = [f.name for f in schema if needs_sampling(f.type)]
    rgs = [rg for f in files for rg in f.row_groups]
    entry = {"sample_size": "all" if budget == float("inf") else int(budget), "columns": cols,
             "created": datetime.datetime.now().isoformat(), "pqora_version": VERSION,
             "threads": workers, "pre_buffer": pre_limit, "row_groups_total": len(rgs),
             "row_groups_sampled": 0, "rows": 0, "bytes": 0, "seconds": 0.0, "row_groups": [],
             "result": []}
    if not cols or not rgs:
        return entry
    total = sum(rg.uncompressed for rg in rgs)
    if budget >= total:
        chosen = rgs
    else:
        n = max(1, min(len(rgs), int(math.ceil(budget / (total / float(len(rgs)))))))
        chosen = [rgs[int(k * len(rgs) / n)] for k in range(n)]
    per_rg = budget / float(len(chosen))
    total_rows = sum(f.rows for f in files) or 1
    sel_bytes = sum(c["uncompressed"] for p, c in columns.items() if p.split(".")[0] in cols)
    batch_rows = max(1, min(65536, int(BATCH_TARGET_BYTES / max(1.0, sel_bytes / float(total_rows)))))
    workers = max(1, min(workers, len(chosen)))
    entry["threads"] = workers
    sys.stderr.write("  {} row group(s), {} thread(s)\n".format(len(chosen), workers))
    details, rows, t0 = [], 0, time.time()
    # progress: ETA from the average speed so far, in (uncompressed) bytes of the
    # row groups to sample, since row groups can differ a lot in size
    todo = float(sum(min(rg.uncompressed, per_rg) for rg in chosen)) or 1.0
    done_bytes = 0.0
    size_of = {(rg.file, rg.index): rg.uncompressed for rg in chosen}
    tasks = [(rg, cols, batch_rows, per_rg, pre_limit) for rg in chosen]
    if workers > 1:
        # processes, not threads: JSON columns are measured in Python (GIL-bound).
        # 'spawn' because forking a process with pyarrow thread pools is unsafe.
        threads_each = max(1, (os.cpu_count() or workers) // workers)
        pool = multiprocessing.get_context("spawn").Pool(
            workers, initializer=_sample_worker_init, initargs=(threads_each,))
        results = pool.imap_unordered(_sample_task, tasks)
    else:
        pool, results = None, (sample_row_group(*t) for t in tasks)
    try:
        for d in results:
            details.append(d)
            rows += d["rows"]
            done_bytes += min(size_of[(d["file"], d["row_group"])], per_rg)
            done = len(details)
            if done == len(chosen) or done % max(1, len(chosen) // 20) == 0:
                elapsed = time.time() - t0
                left = elapsed * (todo - done_bytes) / done_bytes if done_bytes else 0
                sys.stderr.write("  sampled {}/{} row groups ({:,} rows, {:.0f}s, {:,.0f} rows/s{})\n".format(
                    done, len(chosen), rows, elapsed, rows / max(elapsed, 1e-6),
                    ", ETA {}".format(hms(left)) if done < len(chosen) else ", done"))
    finally:
        if pool is not None:
            pool.terminate()
            pool.join()
    details.sort(key=lambda d: (natural_key(d["file"]), d["row_group"]))
    maxes, bands, rows = combine_samples(details)
    entry.update(row_groups_sampled=len(details), rows=rows, bytes=sum(d["bytes"] for d in details),
                 seconds=round(time.time() - t0, 3), row_groups=details,
                 result=[{"path": list(k), "max": maxes[k], "bands": bands.get(k, [0, 0, 0])}
                         for k in sorted(maxes)])
    return entry


# ---------------------------------------------------------------------------
# sample cache: <out-dir>/<table>_sample_cache.json, one entry per sample size
# ---------------------------------------------------------------------------

SAMPLE_CACHE_FORMAT = 1


def sample_key(budget):
    return "all" if budget == float("inf") else str(int(budget))


def sample_fingerprint(files, schema):
    """What a cached sample depends on: the files (path, size, rows per row
    group) and the schema."""
    return {"schema": hashlib.sha1(str(schema).encode("utf-8")).hexdigest(),
            "files": [{"path": f.path, "size": f.disk_size,
                       "row_group_rows": [rg.rows for rg in f.row_groups]} for f in files]}


def read_sample_cache(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cache = json.load(fh)
    except (OSError, ValueError):
        return {}
    return cache if cache.get("pqora_sample_cache") == SAMPLE_CACHE_FORMAT else {}


def cached_sample(path, key, fingerprint):
    """The cached entry for this sample size, or None (with the reason on stderr)."""
    entry = read_sample_cache(path).get("entries", {}).get(key)
    if entry is None:
        return None
    if entry.get("fingerprint") != fingerprint:
        sys.stderr.write("  sample cache {}: entry '{}' was made from different files or schema; "
                         "sampling again\n".format(path, key))
        return None
    return entry


def write_sample_cache(path, key, entry):
    """Add/replace one entry, keeping the others; atomic replace."""
    cache = read_sample_cache(path) or {"pqora_sample_cache": SAMPLE_CACHE_FORMAT, "entries": {}}
    cache.setdefault("entries", {})[key] = entry
    cache["updated"] = datetime.datetime.now().isoformat()
    tmp = "{}.tmp.{}".format(path, os.getpid())
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(cache, fh, indent=1)
        os.rename(tmp, path)
    except OSError as e:
        warn("could not write the sample cache {}: {}".format(path, e))
        discard(None, tmp)


# ---------------------------------------------------------------------------
# column specs (shared by DDL generation and stream mode)
# ---------------------------------------------------------------------------

class Namer(object):
    def __init__(self, max_len):
        self.max_len = max_len
        self.used = set()

    def name(self, parts, suffix=""):
        """Oracle name for a column path; suffix (e.g. '_LST') is always kept."""
        stem = "#".join(re.sub(r"[^A-Za-z0-9_$#]", "_", p).upper() for p in parts)
        if not stem or not stem[0].isalpha():
            stem = "C_" + stem
        limit = self.max_len - len(suffix)
        if len(stem) > limit:
            h = hashlib.sha1("\x00".join(parts).encode("utf-8")).hexdigest()[:8].upper()
            stem = stem[:limit - 9] + "_" + h
        name, k = stem + suffix, 2
        while name in self.used:
            dup = "_{}".format(k)
            name = stem[:limit - len(dup)] + dup + suffix
            k += 1
        self.used.add(name)
        return name


NICE = [1, 2, 5, 10, 20, 30, 50, 100, 150, 200, 250, 500, 750, 1000, 1500, 2000,
        3000, 4000, 6000, 8000, 12000, 16000, 24000, 32767]


def nice_size(observed, margin, limit):
    want = max(1, int(math.ceil(observed * margin)))
    for b in NICE:
        if b >= want:
            return min(b, limit)
    return min(want, limit)


def scalar_spec(t, u):
    """Spec fields for fixed-size scalar types; None for text/binary."""
    T = pa.types
    if T.is_boolean(u):
        return {"kind": "bool", "otype": "NUMBER(1)"}
    for pred, digits in ((T.is_int8, 3), (T.is_uint8, 3), (T.is_int16, 5), (T.is_uint16, 5),
                         (T.is_int32, 10), (T.is_uint32, 10), (T.is_int64, 19), (T.is_uint64, 20)):
        if pred(u):
            return {"kind": "int", "otype": "NUMBER({})".format(digits)}
    if T.is_float16(u) or T.is_float32(u):
        return {"kind": "float", "otype": "BINARY_FLOAT"}
    if T.is_float64(u):
        return {"kind": "float", "otype": "BINARY_DOUBLE"}
    if T.is_decimal(u):
        otype = "NUMBER({},{})".format(u.precision, u.scale) if u.precision <= 38 else "NUMBER"
        return {"kind": "decimal", "otype": otype, "scale": u.scale, "precision": u.precision}
    if T.is_date(u):
        return {"kind": "date", "otype": "DATE"}
    if T.is_timestamp(u):
        prec = {"s": 0, "ms": 3, "us": 6, "ns": 9}[u.unit]
        return {"kind": "timestamp", "otype": "TIMESTAMP({})".format(prec), "unit": u.unit,
                "tz": u.tz}
    if T.is_time(u):
        return {"kind": "time", "otype": "VARCHAR2(18 BYTE)"}
    if T.is_duration(u):
        return {"kind": "duration", "otype": "NUMBER(19)", "unit": u.unit}
    if T.is_null(u):
        return {"kind": "null", "otype": "VARCHAR2(1 BYTE)"}
    return None


def band_note(counts, bands):
    """Counts per Oracle size band, leaving out empty bands."""
    lo, hi = bands
    labels = ("<={} B".format(lo), "{}-{} B".format(lo + 1, hi), ">{} B".format(hi))
    parts = ["{}: {:,}".format(l, c) for l, c in zip(labels, counts) if c]
    return "; " + ", ".join(parts) if parts else ""


def build_specs(schema, args, maxes, sampled, column_avg, bands=None):
    """Flatten the schema into a list of column specs (dicts)."""
    namer = Namer(args.max_name_len)
    var_limit = 32767 if args.max_string_size == "extended" else 4000
    raw_limit = 32767 if args.max_string_size == "extended" else 2000
    specs = []

    def sized(path, limit, default, band_def=TEXT_BANDS):
        """(size or None for LOB, note)"""
        key = ".".join(path)
        if sampled:
            obs = maxes.get(path, 0)
            note = "sampled max {:,} B".format(obs)
            if bands and path in bands:
                note += band_note(bands[path], band_def)
            if obs > limit:
                return None, note
            return nice_size(obs, args.margin, limit), note
        avg = column_avg.get(key)
        if avg and avg > limit:
            return None, "metadata avg {:,.0f} B".format(avg)
        return min(default, limit), "default size"

    def add(path, t, spec, note):
        spec["name"] = namer.name(path, "_LST" if is_list_type(unwrap(t)) else "")
        spec["source"] = list(path)
        spec["arrow"] = str(t)
        spec["note"] = note
        specs.append(spec)

    def walk(t, path):
        u = unwrap(t)
        if pa.types.is_struct(u):
            for i in range(u.num_fields):
                walk(u[i].type, path + (u[i].name,))
            return
        if is_json_type(u):
            n, note = sized(path, var_limit, var_limit)
            if n is None:
                add(path, t, {"kind": "clob", "otype": "CLOB", "json": True}, "JSON, " + note)
            else:
                add(path, t, {"kind": "text", "otype": "VARCHAR2({} BYTE)".format(n), "max": n,
                              "json": True}, "JSON, " + note)
            return
        if is_text(u):
            n, note = sized(path, var_limit, args.default_varchar)
            if n is None:
                add(path, t, {"kind": "clob", "otype": "CLOB"}, note)
            else:
                add(path, t, {"kind": "text", "otype": "VARCHAR2({} BYTE)".format(n), "max": n}, note)
            return
        if pa.types.is_fixed_size_binary(u):
            if u.byte_width <= raw_limit:
                add(path, t, {"kind": "raw", "otype": "RAW({})".format(u.byte_width),
                              "max": u.byte_width}, "fixed size")
            else:
                add(path, t, {"kind": "blob", "otype": "BLOB"}, "fixed size")
            return
        if is_binary(u):
            n, note = sized(path, raw_limit, raw_limit, RAW_BANDS)
            if n is None:
                add(path, t, {"kind": "blob", "otype": "BLOB"}, note)
            else:
                add(path, t, {"kind": "raw", "otype": "RAW({})".format(n), "max": n}, note)
            return
        s = scalar_spec(t, u)
        if s is None:
            warn("unsupported type {} for {}; exporting as JSON text".format(t, ".".join(path)))
            add(path, t, {"kind": "clob", "otype": "CLOB", "json": True}, "unsupported type")
            return
        add(path, t, s, "")

    for f in schema:
        walk(f.type, (f.name,))
    return specs


def field_layout(spec, lob_max):
    """(width, max) of the length prefix for VARCHARC/VARRAWC fields."""
    k = spec["kind"]
    if k in ("clob", "blob"):
        return LOB_WIDTH, lob_max
    if k in ("text", "raw"):
        return len(str(spec["max"])), spec["max"]
    if k == "bool":
        return 1, 1
    if k == "null":
        return 1, 1
    return 2, NUM_MAX       # numbers, time, duration


def field_max_bytes(spec, lob_est):
    k = spec["kind"]
    if k == "timestamp":
        return TS_WIDTH
    if k == "date":
        return DATE_WIDTH
    if k in ("clob", "blob"):
        return 1 + LOB_WIDTH + lob_est
    w, m = field_layout(spec, 0)
    return w + m


# ---------------------------------------------------------------------------
# stream mode: encoding
# ---------------------------------------------------------------------------

B_EMPTY = pa.scalar(b"", pa.binary())
B_N = pa.scalar(b"N", pa.binary())
B_V = pa.scalar(b"V", pa.binary())
TS_PAD = {"s": ".000000000", "ms": "000000", "us": "000", "ns": ""}
UNITS_PER_DAY = {"s": 86400, "ms": 86400 * 10 ** 3, "us": 86400 * 10 ** 6, "ns": 86400 * 10 ** 9}
# Oracle switches to the Julian calendar before 1582-10-15; the ten days
# 1582-10-05 .. 1582-10-14 don't exist there and would be silently shifted
GAP_FIRST = (datetime.date(1582, 10, 5) - datetime.date(1970, 1, 1)).days
GAP_LAST = (datetime.date(1582, 10, 14) - datetime.date(1970, 1, 1)).days
DATE_LO = (datetime.date(1, 1, 1) - datetime.date(1970, 1, 1)).days       # Oracle 4-digit years
DATE_HI = (datetime.date(9999, 12, 31) - datetime.date(1970, 1, 1)).days
YEAR0_FIRST = DATE_LO - 366                                               # 0000-01-01 (proleptic)
YEAR1_LAST = (datetime.date(1, 12, 31) - datetime.date(1970, 1, 1)).days  # 0001-12-31


class StreamError(Exception):
    pass


def as_binary(arr):
    return pc.cast(arr, pa.binary())


class Oversize(StreamError):
    """Values larger than declared. problems: {"columns": {name: (max seen, declared)},
    "record": (largest record, READSIZE) or None}"""

    def __init__(self, problems):
        self.problems = problems
        msgs = ["column {}: value of {} bytes exceeds the declared maximum of {} bytes".format(
            name, seen, declared) for name, (seen, declared) in sorted(problems["columns"].items())]
        if problems.get("record"):
            msgs.append("record of {} bytes exceeds READSIZE {}".format(*problems["record"]))
        StreamError.__init__(self, "; ".join(msgs) + " (fix with: pqora.py resize "
                                                      "<table>_manifest.json --auto-field-size-fix)")


def length_prefixed(arr, width, lens):
    """arr: string/binary array (may contain nulls), lens: its byte lengths with
    nulls as 0 -> binary array of '<len zero-padded to width><bytes>'."""
    prefix = as_binary(pc.utf8_lpad(pc.cast(lens, pa.string()), width=width, padding="0"))
    return pc.binary_join_element_wise(prefix, pc.fill_null(as_binary(arr), B_EMPTY), b"")


def to_text(arr, spec):
    """Convert a plain arrow array to a string array (nulls preserved)."""
    k = spec["kind"]
    t = arr.type
    if k == "int":
        return pc.cast(arr, pa.string())
    if k == "float":
        return pc.cast(arr, pa.string())        # shortest round-trip, nan/inf/-inf
    if k == "bool":
        return pc.cast(pc.cast(arr, pa.int8()), pa.string())
    if k == "decimal":
        scale = t.scale
        if t.precision <= 18 and len(arr) > 0:
            # low 64 bits of the little-endian 128-bit unscaled value
            words = np.frombuffer(arr.buffers()[1], dtype=np.int64)
            low = words[2 * arr.offset: 2 * (arr.offset + len(arr)): 2]
            mask = np.asarray(arr.is_null().to_numpy(zero_copy_only=False), dtype=bool)
            ints = pc.cast(pa.array(low, pa.int64(), mask=mask), pa.string())
            if scale == 0:
                return ints
            return pc.binary_join_element_wise(ints, pa.scalar("E{}".format(-scale)), "")
        return pa.array([None if v is None else str(v) for v in arr.to_pylist()], pa.string())
    if k == "time":
        return pc.cast(arr, pa.string())
    if k == "duration":
        return pc.cast(pc.cast(arr, pa.int64()), pa.string())
    if k == "null":
        return pa.nulls(len(arr), pa.string())
    if k == "epoch":
        # raw Parquet value: days (date) or units (timestamp) since 1970-01-01
        if pa.types.is_date64(arr.type):
            arr = pc.cast(arr, pa.date32())
        ints = pc.cast(arr, pa.int32()) if pa.types.is_date32(arr.type) else pc.cast(arr, pa.int64())
        return pc.cast(ints, pa.string())
    if spec.get("render") == "date":
        if pa.types.is_date64(arr.type):
            arr = pc.cast(arr, pa.date32())
        return pc.cast(arr, pa.string())
    if spec.get("render") == "timestamp":
        if arr.type.tz is not None:
            arr = pc.cast(arr, pa.timestamp(arr.type.unit))
        s = pc.cast(arr, pa.string())
        pad = TS_PAD[arr.type.unit]
        return pc.binary_join_element_wise(s, pa.scalar(pad), "") if pad else s
    if spec.get("json"):
        return to_json_array(arr)
    if k in ("text", "clob"):
        return pc.cast(arr, pa.string())
    if k in ("raw", "blob"):
        return pc.cast(arr, pa.binary())
    raise StreamError("unknown column kind " + k)


def date_ints(arr):
    """(int64 numpy values, bool numpy valid) of a date32/timestamp array."""
    ints = pc.cast(arr, pa.int32()) if pa.types.is_date32(arr.type) else pc.cast(arr, pa.int64())
    v = pc.fill_null(ints, 0).to_numpy(zero_copy_only=False).astype(np.int64)
    valid = ~np.asarray(arr.is_null().to_numpy(zero_copy_only=False), dtype=bool)
    return v, valid


def date_problems(v, valid, per_day):
    """Masks of values Oracle can't store: (too early, too late, in the 1582 gap)."""
    days = np.floor_divide(v, per_day)
    low = valid & (days < DATE_LO)
    high = valid & (days > DATE_HI)
    gap = valid & (days >= GAP_FIRST) & (days <= GAP_LAST)
    return days, low, high, gap


def date_examples(days, mask, n=3):
    """First n flagged values as dates (any year; time of day not shown)."""
    picked = days[np.nonzero(mask)[0][:n]]
    return pc.cast(pa.array(picked.astype(np.int32), pa.int32()).cast(pa.date32()), pa.string()).to_pylist()


def fix_dates(arr, spec, per_day, mode, fixes):
    """Apply --out-of-range-dates: 'error' raises, 'null' blanks the values,
    'clamp' moves them to the nearest date Oracle can store."""
    if len(arr) == 0 or arr.null_count == len(arr):
        return arr
    v, valid = date_ints(arr)
    days, low, high, gap = date_problems(v, valid, per_day)
    bad = low | high | gap
    n = int(bad.sum())
    if not n:
        return arr
    name = spec["name"]
    if mode == "error":
        if gap.any():
            raise StreamError("column {}: {} value(s) between 1582-10-05 and 1582-10-14; these dates "
                              "don't exist in Oracle's calendar and would be silently changed (use "
                              "--out-of-range-dates null or clamp)".format(name, int(gap.sum())))
        raise StreamError("column {}: {} value(s) outside Oracle's range 0001-01-01..9999-12-31, e.g. {} "
                          "(use --out-of-range-dates null or clamp)".format(
                              name, int((low | high).sum()), date_examples(days, low | high)))
    if mode == "null":
        valid = valid & ~bad
    else:
        v = v.copy()
        v[low] = DATE_LO * per_day
        v[high] = (DATE_HI + 1) * per_day - 1                     # last instant of 9999-12-31
        v[gap] = v[gap] + (GAP_LAST + 1 - days[gap]) * per_day    # same time on 1582-10-15
    fixes[name] = fixes.get(name, 0) + n
    if pa.types.is_date32(arr.type):
        return pc.cast(pa.array(v.astype(np.int32), pa.int32(), mask=~valid), pa.date32())
    return pc.cast(pa.array(v, pa.int64(), mask=~valid), arr.type)


def fixed_text(arr, spec, date_mode="error", fixes=None):
    """Timestamps / dates as fixed-width text, nulls as blanks."""
    fixes = {} if fixes is None else fixes
    if spec["kind"] == "timestamp":
        arr = fix_dates(arr, spec, UNITS_PER_DAY[arr.type.unit], date_mode, fixes)
        unit = arr.type.unit
        if arr.type.tz is not None:
            arr = pc.cast(arr, pa.timestamp(unit))       # same instant, as UTC wall time
        s = pc.cast(arr, pa.string())
        if TS_PAD[unit]:
            s = pc.binary_join_element_wise(s, pa.scalar(TS_PAD[unit]), "")
        width, what = TS_WIDTH, "timestamp"
    else:
        if pa.types.is_date64(arr.type):
            arr = pc.cast(arr, pa.date32())
        arr = fix_dates(arr, spec, 1, date_mode, fixes)
        s = pc.cast(arr, pa.string())
        width, what = DATE_WIDTH, "date"
    lens = pc.binary_length(s)
    if len(s) and (pc.min(lens).as_py() not in (None, width) or pc.max(lens).as_py() not in (None, width)):
        bad = [v for v in s.to_pylist() if v is not None and len(v) != width][:3]
        raise StreamError("{} {}: value(s) outside the Oracle-representable range: {}".format(
            what, spec["name"], bad))
    return pc.fill_null(as_binary(s), pa.scalar(b" " * width, pa.binary()))


def column_array(batch, spec):
    src = spec["source"]
    arr = batch.column(batch.schema.get_field_index(src[0]))
    for name in src[1:]:
        arr = struct_child(arr, name)
    arr = plain(arr)
    if _is("is_large_string", arr.type):
        arr = pc.cast(arr, pa.string())
    elif _is("is_large_binary", arr.type) or pa.types.is_fixed_size_binary(arr.type):
        arr = pc.cast(arr, pa.binary())
    return arr


def prepare(batch, specs, date_mode="error", fixes=None):
    """Convert every column once: fixed-width binary for dates/timestamps, text or
    binary arrays (nulls kept) for everything else. Aligned with specs."""
    prep = []
    for spec in specs:
        arr = column_array(batch, spec)
        if spec["kind"] in ("timestamp", "date"):
            prep.append(fixed_text(arr, spec, date_mode, fixes))
        else:
            prep.append(to_text(arr, spec))
    return prep


def large_mask(prep, specs):
    """Rows with a value larger than its (non-LOB) column allows, or None."""
    mask = None
    for spec, a in zip(specs, prep):
        if spec["kind"] in ("text", "raw") and "max" in spec:
            big = pc.greater(pc.fill_null(pc.binary_length(a), 0), spec["max"])
            mask = big if mask is None else pc.or_(mask, big)
    return mask


def assemble(prep, specs, readsize, lob_max, problems):
    """Records (binary array, one per row) from prepared columns, or None if a
    value or record is larger than declared (recorded in problems)."""
    fields = []
    for spec, a in zip(specs, prep):
        k = spec["kind"]
        if k in ("timestamp", "date"):
            fields.append(a)
            continue
        width, max_bytes = field_layout(spec, lob_max)
        lens = pc.fill_null(pc.binary_length(a), 0)
        m = pc.max(lens).as_py() or 0
        if m > max_bytes:
            seen = problems["columns"].get(spec["name"], (0, max_bytes))[0]
            problems["columns"][spec["name"]] = (max(seen, m), max_bytes)
        if problems["columns"]:
            continue        # only measuring from now on
        if k in ("clob", "blob"):
            fields.append(pc.if_else(pc.is_null(a), B_N, B_V))
        fields.append(length_prefixed(a, width, lens))
    if problems["columns"]:
        return None
    body = pc.binary_join_element_wise(*(fields + [b""]))
    rec_lens = pc.binary_length(body)
    m = pc.max(rec_lens).as_py() or 0
    if m + REC_WIDTH > readsize:
        seen = (problems.get("record") or (0, readsize))[0]
        problems["record"] = (max(seen, m + REC_WIDTH), readsize)
    if problems.get("record"):
        return None
    prefix = as_binary(pc.utf8_lpad(pc.cast(rec_lens, pa.string()), width=REC_WIDTH, padding="0"))
    return pc.binary_join_element_wise(prefix, body, b"")


def encode_batch(batch, specs, readsize, lob_max, problems):
    return assemble(prepare(batch, specs), specs, readsize, lob_max, problems)


def records_buffer(rec):
    """Contiguous bytes of all records of a (non-null) binary array."""
    bufs = rec.buffers()
    offsets = np.frombuffer(bufs[1], dtype=np.int32, count=len(rec) + 1, offset=rec.offset * 4)
    return memoryview(bufs[2])[int(offsets[0]):int(offsets[-1])]


def read_part(path):
    with open(path, "r", encoding="utf-8") as fh:
        part = json.load(fh)
    if part.get(PART_MAGIC) != PART_FORMAT:
        raise StreamError("{} is not a pqora part file of format {}".format(path, PART_FORMAT))
    return part


def check_source(src):
    """Make sure the Parquet file still matches what the part was generated from."""
    path = src["path"]
    try:
        size = os.path.getsize(path)
    except OSError as e:
        raise StreamError("cannot access {}: {}".format(path, e))
    if size != src["size"]:
        raise StreamError("{} has changed: size {} != {} recorded at generate time".format(
            path, size, src["size"]))
    md = pq.read_metadata(path)
    for rg, rows in zip(src["row_groups"], src["rg_rows"]):
        if rg >= md.num_row_groups or md.row_group(rg).num_rows != rows:
            raise StreamError("{} has changed: row group {} no longer has {} rows".format(
                path, rg, rows))


def stream_part(part, out, stage_out=None):
    """Write all records of a part to out (binary). With --stage-large-records,
    records with a value too large for the main table go to stage_out instead,
    in the _CLOB table's format. Returns (rows, bytes, large rows)."""
    stage = part.get("stage")
    stage_problems = {"columns": {}, "record": None}
    large = 0
    date_mode = part.get("out_of_range_dates") or "error"
    fixes = part["_date_fixes"] = {}
    specs = part["columns"]
    top = []
    for s in specs:
        if s["source"][0] not in top:
            top.append(s["source"][0])
    threads = part.get("threads") or 0
    if threads:
        pa.set_cpu_count(threads)            # decode threads per process
        pa.set_io_thread_count(threads)      # concurrent reads when pre-buffering
    pre_limit = part.get("pre_buffer") or 0
    rows = nbytes = 0
    # after the first oversize value nothing more is written, but the rest of
    # the part is still measured so the error reports every column's real maximum
    problems = {"columns": {}, "record": None}
    for src in part["sources"]:
        check_source(src)
        handles = {}
        rg_bytes = src.get("rg_bytes") or [None] * len(src["row_groups"])
        for rg, size in zip(src["row_groups"], rg_bytes):
            # small enough row groups: read whole column chunks with large coalesced
            # requests (memory ~ one row group); larger ones: stream in 8 MB reads
            pre = bool(pre_limit and size is not None and size <= pre_limit)
            if pre not in handles:
                handles[pre] = pq.ParquetFile(src["path"], buffer_size=0 if pre else 8 * MIB,
                                              pre_buffer=pre)
            for batch in handles[pre].iter_batches(batch_size=src["batch_rows"], row_groups=[rg],
                                                   columns=top, use_threads=threads != 1):
                if batch.num_rows == 0:
                    continue
                prep = prepare(batch, specs, date_mode, fixes)
                if stage:
                    mask = large_mask(prep, specs)
                    n_large = (pc.sum(mask).as_py() or 0) if mask is not None else 0
                    if n_large:
                        big = assemble([pc.filter(a, mask) for a in prep], stage["columns"],
                                       stage["readsize"], stage["lob_max"], stage_problems)
                        if stage_problems["columns"] or stage_problems["record"]:
                            raise StreamError("large record does not fit the {} table: {}".format(
                                stage["table"], Oversize(stage_problems).args[0].split(" (fix")[0]))
                        if stage_out is not None:
                            stage_out.write(records_buffer(big))
                        large += n_large
                        keep = pc.invert(mask)
                        prep = [pc.filter(a, keep) for a in prep]
                        if batch.num_rows == n_large:
                            continue
                rec = assemble(prep, specs, part["readsize"], part["lob_max"], problems)
                if rec is None or problems["columns"] or problems["record"]:
                    continue
                buf = records_buffer(rec)
                out.write(buf)
                rows += len(rec)
                nbytes += len(buf)
    if problems["columns"] or problems["record"]:
        raise Oversize(problems)
    if rows + large != part["rows"]:
        raise StreamError("wrote {} rows (+{} large) but the part expects {}".format(
            rows, large, part["rows"]))
    return rows, nbytes, large


def log_line(part, **kw):
    path = part.get("log_file")
    if not path:
        return
    kw.update(ts=datetime.datetime.now().isoformat(), pid=os.getpid(), host=socket.gethostname(),
              table=part.get("table"), part=part.get("part"))
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(kw) + "\n")
    except OSError:
        pass


def discard(fh, path):
    try:
        if fh:
            fh.close()
        if path:
            os.remove(path)
    except OSError:
        pass


def run_stream(path):
    t0 = time.time()
    part = None
    stage_fh = stage_tmp = None
    try:
        part = read_part(path)
        log_line(part, event="start", threads=part.get("threads"), pre_buffer=part.get("pre_buffer"))
        out = sys.stdout.buffer
        stage = part.get("stage")
        if stage:
            # written under a temporary name, renamed only when the part completed
            stage_tmp = "{}.tmp.{}".format(stage["file"], os.getpid())
            stage_fh = open(stage_tmp, "wb")
        rows, nbytes, large = stream_part(part, out, stage_fh)
        out.flush()
        if stage_fh:
            stage_fh.close()
            os.rename(stage_tmp, stage["file"])
            stage_fh = stage_tmp = None
        secs = time.time() - t0
        extra = {"large_rows": large} if stage else {}
        if part.get("_date_fixes"):
            extra["dates_" + ("nulled" if part.get("out_of_range_dates") == "null" else "clamped")] = \
                part["_date_fixes"]
        log_line(part, event="end", status="ok", rows=rows, bytes=nbytes, seconds=round(secs, 3),
                 mb_per_s=round(nbytes / MIB / max(secs, 1e-6), 1), **extra)
        return 0
    except BrokenPipeError:
        # Oracle stopped reading (e.g. ROWNUM limit) - not an error; the large
        # record file is incomplete, so the previous one is kept
        discard(stage_fh, stage_tmp)
        if part:
            log_line(part, event="end", status="reader closed pipe", seconds=round(time.time() - t0, 3))
        try:
            sys.stdout = None
        except Exception:
            pass
        return 0
    except Exception as e:
        discard(stage_fh, stage_tmp)
        msg = "pqora {}: {}: {}".format(VERSION, os.path.basename(path), e)
        if not isinstance(e, (StreamError, OSError)):
            msg = "pqora {}: {}: {}: {}".format(VERSION, os.path.basename(path), type(e).__name__, e)
        sys.stderr.write(msg.encode("ascii", "backslashreplace").decode("ascii") + "\n")
        if part:
            extra = {}
            if isinstance(e, Oversize):
                extra["oversize"] = {n: seen for n, (seen, _) in e.problems["columns"].items()}
                if e.problems.get("record"):
                    extra["record_bytes"] = e.problems["record"][0]
            log_line(part, event="end", status="error", error=str(e),
                     seconds=round(time.time() - t0, 3), **extra)
        return 1


# ---------------------------------------------------------------------------
# verify mode: decode a stream and check its framing
# ---------------------------------------------------------------------------

class Decoder(object):
    """Parses the stream incrementally and checks every record."""

    def __init__(self, part):
        self.part = part
        self.buf = bytearray()
        self.rows = 0
        self.bytes = 0
        self.max_record = 0
        self.nulls = [0] * len(part["columns"])
        self.max_len = [0] * len(part["columns"])
        self.layout = []
        for s in part["columns"]:
            if s["kind"] == "timestamp":
                self.layout.append(("fixed", TS_WIDTH))
            elif s["kind"] == "date":
                self.layout.append(("fixed", DATE_WIDTH))
            else:
                w, m = field_layout(s, part["lob_max"])
                self.layout.append(("lob" if s["kind"] in ("clob", "blob") else "var", w))

    def write(self, data):
        self.buf += data
        self.bytes += len(data)
        pos, n = 0, len(self.buf)
        while n - pos >= REC_WIDTH:
            rlen = int(self.buf[pos:pos + REC_WIDTH])
            if n - pos - REC_WIDTH < rlen:
                break
            self._record(memoryview(self.buf)[pos + REC_WIDTH:pos + REC_WIDTH + rlen])
            pos += REC_WIDTH + rlen
        del self.buf[:pos]

    def _record(self, rec):
        p = 0
        for i, (kind, w) in enumerate(self.layout):
            if kind == "fixed":
                v = bytes(rec[p:p + w])
                if v.strip() == b"":
                    self.nulls[i] += 1
                p += w
                continue
            if kind == "lob":
                ind = bytes(rec[p:p + 1])
                if ind not in (b"N", b"V"):
                    raise StreamError("row {}: bad LOB indicator {!r}".format(self.rows, ind))
                if ind == b"N":
                    self.nulls[i] += 1
                p += 1
            ln = int(bytes(rec[p:p + w]))
            p += w
            if kind == "var" and ln == 0:
                self.nulls[i] += 1
            self.max_len[i] = max(self.max_len[i], ln)
            p += ln
        if p != len(rec):
            raise StreamError("row {}: fields use {} bytes but the record has {}".format(
                self.rows, p, len(rec)))
        self.rows += 1
        self.max_record = max(self.max_record, len(rec) + REC_WIDTH)

    def close(self):
        if self.buf:
            raise StreamError("{} trailing bytes after the last record".format(len(self.buf)))


def run_verify(paths):
    rc = 0
    for path in paths:
        part = read_part(path)
        dec = Decoder(part)
        stage = part.get("stage")
        sdec = Decoder({"columns": stage["columns"], "lob_max": stage["lob_max"]}) if stage else None
        t0 = time.time()
        try:
            rows, _, large = stream_part(part, dec, sdec)
            dec.close()
            if sdec:
                sdec.close()
        except StreamError as e:
            print("{}: FAILED: {}".format(path, e))
            rc = 1
            continue
        print("{}: OK  rows={:,} bytes={:,} max_record={:,} (READSIZE {:,})  {:.1f}s".format(
            os.path.basename(path), dec.rows, dec.bytes, dec.max_record, part["readsize"],
            time.time() - t0))
        if sdec:
            print("    large records for {}: {:,} rows, {:,} bytes, max_record={:,} (READSIZE {:,})".format(
                stage["table"], sdec.rows, sdec.bytes, sdec.max_record, stage["readsize"]))
        for s, nn, ml in zip(part["columns"], dec.nulls, dec.max_len):
            print("    {:<32} {:<10} nulls={:<10,} max_len={:,}".format(
                short(s["name"], 32), s["kind"], nn, ml))
    return rc


# ---------------------------------------------------------------------------
# generate mode
# ---------------------------------------------------------------------------

def plan_parts(files, target_size):
    """Group consecutive row groups into parts of about target_size
    uncompressed bytes (0 = one part per row group)."""
    parts, cur, cur_size = [], [], 0
    for f in files:
        for rg in f.row_groups:
            if cur and (not target_size or cur_size + rg.uncompressed > target_size):
                parts.append(cur)
                cur, cur_size = [], 0
            cur.append(rg)
            cur_size += rg.uncompressed
    if cur:
        parts.append(cur)
    return parts


def table_part_files(directory, table):
    """Part files of exactly this table (<table>_<number>.pqpart), not of
    tables whose names merely start with it (SALES vs SALES_EU)."""
    pat = re.compile(r"^" + re.escape(table.lower()) + r"_\d+\.pqpart$")
    return sorted((os.path.join(directory, f) for f in os.listdir(directory) if pat.match(f)),
                  key=natural_key)


def pow2_ceil(n):
    return 1 << int(math.ceil(math.log(max(1, n), 2)))


def ident(name, what):
    if not re.match(r"^[A-Za-z][A-Za-z0-9_$#]*$", name):
        sys.exit("ERROR: {} {!r} must be a simple Oracle identifier".format(what, name))
    return name.upper()


def q(name):
    return '"' + name.replace('"', '""') + '"'


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def ddl_fields(specs, lob_max):
    lines = []
    for i, s in enumerate(specs):
        k = s["kind"]
        n = q(s["name"])
        if k == "timestamp":
            lines.append('{} CHAR({}) DATE_FORMAT TIMESTAMP MASK "{}" NULLIF {} = BLANKS'.format(
                n, TS_WIDTH, TS_MASK, n))
        elif k == "date":
            lines.append('{} CHAR({}) DATE_FORMAT DATE MASK "{}" NULLIF {} = BLANKS'.format(
                n, DATE_WIDTH, DATE_MASK, n))
        else:
            w, m = field_layout(s, lob_max)
            ftype = "VARRAWC" if k in ("raw", "blob") else "VARCHARC"
            if k in ("clob", "blob"):
                ind = q("PQORA$IND{}".format(i + 1))
                lines.append("{} CHAR(1)".format(ind))
                lines.append("{} {}({},{}) NULLIF {} = 'N'".format(n, ftype, w, m, ind))
            else:
                lines.append("{} {}({},{})".format(n, ftype, w, m))
    return lines


def column_lines(specs):
    width = min(40, max(len(q(s["name"])) for s in specs))
    out = []
    for i, s in enumerate(specs):
        sep = "," if i < len(specs) - 1 else " "
        note = ".".join(s["source"]) + ": " + s["arrow"] + (", " + s["note"] if s["note"] else "")
        out.append("  {:<{w}} {}{} -- {}".format(q(s["name"]), s["otype"], sep,
                                                  note.replace("\n", " "), w=width))
    return out


def dir_objects(st):
    """[(object, os path, privileges)] in DDL-comment order, merged when the
    same object is used for several purposes."""
    objs = []
    need = [(st["exec_dir_object"], st["exec_dir"], "EXECUTE", "pqora.sh"),
            (st["parts_dir_object"], st["out_dir"], "READ", ".pqpart files")]
    if st.get("log_dir_object"):
        need.append((st["log_dir_object"], st.get("log_dir") or "<log dir>", "WRITE",
                     "ORACLE_LOADER log/bad files"))
    for obj, path, priv, what in need:
        for o in objs:
            if o[0] == obj:
                o[2].add(priv)
                o[3].append(what)
                break
        else:
            objs.append([obj, path, {"READ", priv}, [what]])
    order = ["READ", "WRITE", "EXECUTE"]
    return [(o[0], o[1], ", ".join(p for p in order if p in o[2]), ", ".join(o[3])) for o in objs]


def ddl_header(st, title):
    L = ["-- {} (pqora.py {}, {})".format(title, VERSION, datetime.datetime.now().strftime("%Y-%m-%d %H:%M")),
         "-- Source: {} file(s), {:,} rows, {} part(s)".format(len(st["files"]), st["total_rows"],
                                                                len(st["location"])),
         "-- Directory objects (create as DBA, grant to the loading schema):"]
    for obj, path, privs, what in dir_objects(st):
        L.append("--   {:<14} -> {}   ({})  {}".format(obj, path, privs, what))
    return L


def bad_file_pattern(ext):
    """Wildcard matching exactly this external table's bad files."""
    return "{}.*.bad".format(ext.lower())


def ddl_external(st, specs, preprocessor=True, reject_limit="0"):
    ext = st["ext_table"]
    L = ["CREATE TABLE {} (".format(q(ext))]
    L.extend(column_lines(specs))
    L += [")",
          "ORGANIZATION EXTERNAL (",
          "  TYPE ORACLE_LOADER",
          "  DEFAULT DIRECTORY {}".format(st["parts_dir_object"]),
          "  ACCESS PARAMETERS (",
          "    RECORDS VARIABLE {}".format(REC_WIDTH),
          "    CHARACTERSET AL32UTF8",
          "    STRING SIZES ARE IN BYTES"]
    if preprocessor:
        L.append("    PREPROCESSOR {}:'{}'".format(st["exec_dir_object"], st["wrapper_name"]))
    L += ["    READSIZE {}".format(st["readsize"])]
    if st.get("log_dir_object"):
        L.append("    BADFILE {}:'{}.%a_%p.bad'".format(st["log_dir_object"], ext.lower()))
        L.append("    LOGFILE {}:'{}_%a_%p.log'".format(st["log_dir_object"], ext.lower()))
    else:
        L += ["    NOBADFILE", "    NOLOGFILE"]
    L.append("    FIELDS (")
    fl = ddl_fields(specs, st["lob_max"])
    for i, line in enumerate(fl):
        L.append("      " + line + ("," if i < len(fl) - 1 else ""))
    L += ["    )", "  )", "  LOCATION ("]
    for i, n in enumerate(st["location"]):
        L.append("    " + sql_str(n) + ("," if i < len(st["location"]) - 1 else ""))
    L += ["  )", ")", "REJECT LIMIT {}".format(reject_limit), "PARALLEL {};".format(st["dop"])]
    return L


def ddl_bad(st, specs):
    """External table over the main external table's bad files, and the
    (commented) statements to reprocess rejected rows."""
    if not st.get("log_dir_object"):
        return []
    ext, tgt = st["ext_table"], st["table"]
    bad_ext = ext + "_BAD"
    st2 = dict(st, ext_table=bad_ext, location=[bad_file_pattern(ext)],
               parts_dir_object=st["log_dir_object"])
    return ["-- " + "-" * 75,
            "-- Rejected rows. EXT_* tables are created with REJECT LIMIT 0 (the first bad row",
            "-- stops the load). To load past bad rows and collect them in bad files:",
            "--   ALTER TABLE {} REJECT LIMIT UNLIMITED;".format(q(ext)),
            "-- {} reads every {} file in {} (same record format,".format(
                q(bad_ext), bad_file_pattern(ext), st["log_dir_object"]),
            "-- no preprocessor; KUP-04040/KUP-05002 'no matching files' while there are none).",
            "-- After fixing the cause (e.g. pqora.py resize), load them with the INSERT below;",
            "-- then delete those bad files so they aren't loaded twice.",
            "-- " + "-" * 75] \
        + ddl_external(st2, specs, preprocessor=False, reject_limit="UNLIMITED") \
        + ["",
           "-- reprocess rejected rows (uncomment):",
           "-- INSERT /*+ APPEND */ INTO {} SELECT * FROM {};".format(q(tgt), q(bad_ext)),
           "-- COMMIT;"]


def ddl_target(st, specs):
    return ["CREATE TABLE {} (".format(q(st["table"]))] + column_lines(specs) + [");"]


def ddl_load(st):
    tgt, ext, dop = q(st["table"]), q(st["ext_table"]), st["dop"]
    L = ["ALTER SESSION ENABLE PARALLEL DML;",
         "",
         "INSERT /*+ APPEND PARALLEL({}) */ INTO {}".format(dop, tgt),
         "SELECT /*+ PARALLEL({}) */ * FROM {};".format(dop, ext),
         "",
         "COMMIT;",
         ""]
    stage = st.get("stage")
    if stage:
        L += ["-- large records, written while {} was read above".format(ext),
              "INSERT /*+ APPEND PARALLEL({}) */ INTO {}".format(dop, q(stage["table"])),
              "SELECT /*+ PARALLEL({}) */ * FROM {};".format(dop, q(stage["ext_table"])),
              "",
              "COMMIT;",
              "",
              "-- expected row count: {} ({} + {})".format(st["total_rows"], st["table"], stage["table"]),
              "SELECT (SELECT COUNT(*) FROM {}) AS loaded_rows, (SELECT COUNT(*) FROM {}) AS large_rows, "
              "{} AS expected_rows FROM dual;".format(tgt, q(stage["table"]), st["total_rows"])]
    else:
        L += ["-- expected row count: {}".format(st["total_rows"]),
              "SELECT COUNT(*) AS loaded_rows, {} AS expected_rows FROM {};".format(st["total_rows"], tgt)]
    return L


def write_lines(path, lines):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def ddl_path(st, suffix=""):
    return os.path.join(st["ddl_dir"], st["table"].lower() + suffix + ".sql")


def report_comment(st):
    """The inspect report of the source files as SQL comment lines."""
    text = st.get("inspect_report")
    if not text:
        return []
    L = ["", "", "-- " + "-" * 75,
         "-- Source file report at generate time (same as: pqora.py <files>)",
         "-- " + "-" * 75]
    L += [("-- " + line).rstrip() for line in text.rstrip("\n").split("\n")]
    return L


def stg_table(st):
    ext = st["ext_table"]
    base = ext[4:] if ext.upper().startswith("EXT_") and len(ext) > 4 else st["table"]
    return base + "_STG"


def ddl_ctas(st):
    """One-line compressed staging copy of the external table."""
    L = ["-- suspend instead of failing on space errors (needs the RESUMABLE privilege):",
         "ALTER SESSION ENABLE RESUMABLE TIMEOUT 3600 NAME {};".format(sql_str("pqora " + st["table"])),
         "",
         "-- staging copy; run after the external table below exists:",
         "CREATE TABLE {} COMPRESS FOR QUERY HIGH PARALLEL {} AS SELECT * FROM {};".format(
             q(stg_table(st)), st["dop"], q(st["ext_table"]))]
    stage = st.get("stage")
    if stage:
        L += ["-- large records; run after {} was read completely (e.g. by the line above):".format(
                  st["ext_table"]),
              "CREATE TABLE {} COMPRESS FOR QUERY HIGH PARALLEL {} AS SELECT * FROM {};".format(
                  q(stage["table"] + "_STG"), st["dop"], q(stage["ext_table"]))]
    return L


def ddl_stage_external(st):
    """External table over the large-record files (no preprocessor)."""
    stage = st["stage"]
    st2 = dict(st, ext_table=stage["ext_table"], readsize=stage["readsize"], lob_max=stage["lob_max"],
               location=stage["location"], parts_dir_object=st["log_dir_object"])
    return ["-- Records with a value too large for {} (written by its preprocessor while it is".format(
                st["ext_table"]),
            "-- read). Complete only after a full read of {}.".format(st["ext_table"])] \
        + ddl_external(st2, stage["columns"], preprocessor=False)


def ddl_manifest(st):
    """Queries reading <table>_manifest.json as tables, for verification."""
    name = os.path.basename(manifest_path(st))
    src = "TO_BLOB(BFILENAME({}, {})) FORMAT JSON".format(sql_str(st["parts_dir_object"]),
                                                         sql_str(name))
    tgt = q(st["table"])
    stage = st.get("stage")
    loaded = "(SELECT COUNT(*) FROM {})".format(tgt)
    if stage:
        loaded = "(SELECT COUNT(*) FROM {}) + (SELECT COUNT(*) FROM {})".format(tgt, q(stage["table"]))
    return [
        "-- " + "-" * 75,
        "-- Verification: {} (in {}) read as tables".format(name, st["parts_dir_object"]),
        "-- " + "-" * 75,
        "",
        "-- overview: rows expected by the manifest vs rows loaded",
        "SELECT m.tbl, m.expected_rows, {} AS loaded_rows, m.parts, m.cols, m.readsize,".format(loaded),
        "       m.generated, m.version",
        "FROM JSON_TABLE({}, '$' COLUMNS (".format(src),
        "       tbl VARCHAR2(128) PATH '$.table', expected_rows NUMBER PATH '$.rows',",
        "       parts NUMBER PATH '$.parts.size()', cols NUMBER PATH '$.columns.size()',",
        "       readsize NUMBER PATH '$.readsize', generated VARCHAR2(40) PATH '$.generated',",
        "       version VARCHAR2(20) PATH '$.pqora_version')) m;",
        "",
        "-- per source file: parts, row groups and rows (TOTAL = whole table)",
        "SELECT SUBSTR(NVL(f.file_path, 'TOTAL'), 1, 120) AS file_path, COUNT(DISTINCT f.part) AS parts,",
        "       SUM(f.row_groups) AS row_groups, SUM(f.file_rows) AS file_rows",
        "FROM JSON_TABLE({}, '$.parts[*]' COLUMNS (".format(src),
        "       part VARCHAR2(200) PATH '$.name',",
        "       NESTED PATH '$.sources[*]' COLUMNS (",
        "         file_path VARCHAR2(4000) PATH '$.path',",
        "         row_groups NUMBER PATH '$.row_groups.size()', file_rows NUMBER PATH '$.rows'))) f",
        "GROUP BY ROLLUP(f.file_path)",
        "ORDER BY f.file_path NULLS LAST;",
        "",
        "-- per part (one parallel work unit each)",
        "SELECT p.part, p.part_rows, ROUND(p.compressed / 1048576, 1) AS compressed_mib,",
        "       ROUND(p.uncompressed / 1048576, 1) AS uncompressed_mib, p.files",
        "FROM JSON_TABLE({}, '$.parts[*]' COLUMNS (".format(src),
        "       part VARCHAR2(200) PATH '$.name', part_rows NUMBER PATH '$.rows',",
        "       compressed NUMBER PATH '$.compressed', uncompressed NUMBER PATH '$.uncompressed',",
        "       files NUMBER PATH '$.sources.size()')) p",
        "ORDER BY p.part;",
        "",
        "-- columns",
        "SELECT c.column_name, c.kind, c.oracle_type, SUBSTR(c.note, 1, 120) AS note",
        "FROM JSON_TABLE({}, '$.columns[*]' COLUMNS (".format(src),
        "       column_name VARCHAR2(128) PATH '$.name', kind VARCHAR2(20) PATH '$.kind',",
        "       oracle_type VARCHAR2(60) PATH '$.otype', note VARCHAR2(4000) PATH '$.note')) c;",
    ] + ddl_sample_cache(st)


def ddl_sample_cache(st):
    """Queries reading <table>_sample_cache.json (only when generate sampled)."""
    if not st.get("sampling"):
        return []
    name = st["table"].lower() + "_sample_cache.json"
    src = "TO_BLOB(BFILENAME({}, {})) FORMAT JSON".format(sql_str(st["parts_dir_object"]),
                                                         sql_str(name))
    return [
        "",
        "-- " + "-" * 75,
        "-- Sampling: {} (in {}), one entry per --sample-size".format(name, st["parts_dir_object"]),
        "-- size bands: n_small <= 4000 B (RAW: 2000), n_extended <= 32767 B, n_lob > 32767 B",
        "-- " + "-" * 75,
        "",
        "-- sample runs",
        "SELECT e.sample_size, SUBSTR(e.created, 1, 19) AS created, e.row_groups_sampled,",
        "       e.row_groups_total, e.sampled_rows, ROUND(e.sampled_bytes / 1048576, 1) AS sampled_mib,",
        "       e.seconds, e.threads, e.version",
        "FROM JSON_TABLE({}, '$.entries.*' COLUMNS (".format(src),
        "       sample_size VARCHAR2(20) PATH '$.sample_size', created VARCHAR2(40) PATH '$.created',",
        "       row_groups_sampled NUMBER PATH '$.row_groups_sampled',",
        "       row_groups_total NUMBER PATH '$.row_groups_total', sampled_rows NUMBER PATH '$.rows',",
        "       sampled_bytes NUMBER PATH '$.bytes', seconds NUMBER PATH '$.seconds',",
        "       threads NUMBER PATH '$.threads', version VARCHAR2(20) PATH '$.pqora_version')) e",
        "ORDER BY e.created;",
        "",
        "-- result per column and sample size",
        "SELECT r.sample_size, SUBSTR(r.column_path, 1, 60) AS column_path, r.max_bytes,",
        "       r.n_small, r.n_extended, r.n_lob",
        "FROM JSON_TABLE({}, '$.entries.*' COLUMNS (".format(src),
        "       sample_size VARCHAR2(20) PATH '$.sample_size',",
        "       NESTED PATH '$.result[*]' COLUMNS (",
        "         column_path VARCHAR2(4000) FORMAT JSON PATH '$.path', max_bytes NUMBER PATH '$.max',",
        "         n_small NUMBER PATH '$.bands[0]', n_extended NUMBER PATH '$.bands[1]',",
        "         n_lob NUMBER PATH '$.bands[2]'))) r",
        "ORDER BY r.column_path, r.sample_size;",
        "",
        "-- per row group",
        "SELECT g.sample_size, SUBSTR(g.file_path, 1, 100) AS file_path, g.row_group, g.sampled_rows,",
        "       g.rows_in_row_group, ROUND(g.sampled_bytes / 1048576, 1) AS sampled_mib, g.seconds",
        "FROM JSON_TABLE({}, '$.entries.*' COLUMNS (".format(src),
        "       sample_size VARCHAR2(20) PATH '$.sample_size',",
        "       NESTED PATH '$.row_groups[*]' COLUMNS (",
        "         file_path VARCHAR2(4000) PATH '$.file', row_group NUMBER PATH '$.row_group',",
        "         sampled_rows NUMBER PATH '$.rows', rows_in_row_group NUMBER PATH '$.rows_in_row_group',",
        "         sampled_bytes NUMBER PATH '$.bytes', seconds NUMBER PATH '$.seconds'))) g",
        "ORDER BY g.sample_size, g.file_path, g.row_group;",
    ]


def write_ddl(st, specs):
    path = ddl_path(st)
    L = ddl_header(st, "Generated") + [""] + ddl_ctas(st) + [""] + ddl_external(st, specs) + [""]
    stage = st.get("stage")
    if stage:
        L += ddl_stage_external(st) + [""]
    bad = ddl_bad(st, specs)
    if bad:
        L += bad + [""]
    L += ddl_target(st, specs) + [""]
    if stage:
        L += ddl_target(dict(st, table=stage["table"]), stage["columns"]) + [""]
    write_lines(path, L + ddl_load(st) + ["", ""] + ddl_manifest(st) + report_comment(st))
    return path


def write_alter(st, specs, changed):
    """SQL that recreates the external table and widens the target table."""
    L = ddl_header(st, "Column size changes")
    L += ["-- Recreates the external table with the new sizes and widens the",
          "-- target table. Then rerun the INSERT from {}.".format(os.path.basename(ddl_path(st))),
          "",
          "DROP TABLE {} PURGE;".format(q(st["ext_table"])),
          ""]
    L += ddl_external(st, specs)
    L.append("")
    by_name = {s["name"]: s for s in specs}
    for name in changed:
        s = by_name[name]
        if s["kind"] in ("text", "raw"):
            L.append("ALTER TABLE {} MODIFY ({} {});".format(q(st["table"]), q(name), s["otype"]))
        else:
            L.append("-- {} is now {}: Oracle can't MODIFY a VARCHAR2/RAW column to a LOB.".format(
                q(name), s["otype"]))
            L.append("-- If {} is still empty, drop and recreate it with the CREATE TABLE in {}."
                     .format(q(st["table"]), os.path.basename(ddl_path(st))))
    path = ddl_path(st, "_alter")
    write_lines(path, L)
    return path


def wrapper_text(python, script, pythonpath):
    lines = ["#!/bin/sh",
             "# pqora preprocessor wrapper (generated by pqora.py).",
             "# Oracle runs this as the database OS user with an empty environment and",
             "# passes one argument: the full path of a .pqpart location file."]
    if pythonpath:
        lines.append("PYTHONPATH={}".format(pythonpath))
        lines.append("export PYTHONPATH")
    lines.append('exec {} {} "$1"'.format(python, script))
    return "\n".join(lines) + "\n"


def wrapper_state(path, text):
    """'same', 'write' or an error message."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                if fh.read() == text:
                    return "same"
        except OSError:
            pass
        if not os.access(path, os.W_OK):
            return "{} exists with different content and is not writable by this user".format(path)
        return "write"
    if not os.access(os.path.dirname(path), os.W_OK):
        return "cannot create {} (directory not writable)".format(path)
    return "write"


def detect_pythonpath():
    """If pyarrow is imported from a non-default location (user site,
    PYTHONPATH), return that directory so the wrapper can set PYTHONPATH."""
    import site
    loc = os.path.dirname(os.path.dirname(os.path.abspath(pa.__file__)))
    defaults = set(getattr(site, "getsitepackages", lambda: [])())
    if loc in defaults:
        return ""
    return loc


# ---------------------------------------------------------------------------
# column size changes (--field-size, --auto-field-size-fix)
# ---------------------------------------------------------------------------

def size_limits(max_string_size):
    """(VARCHAR2 limit, RAW limit)"""
    return (32767, 32767) if max_string_size == "extended" else (4000, 2000)


def parse_field_size(s):
    if "=" not in s:
        raise argparse.ArgumentTypeError("expected COLUMN=SIZE, got {!r}".format(s))
    col, size = s.rsplit("=", 1)
    size = size.strip()
    if size.lower() in ("lob", "clob", "blob"):
        return col.strip(), "lob"
    n = parse_size(size)
    if n == float("inf") or n <= 0:
        raise argparse.ArgumentTypeError("invalid size in {!r}".format(s))
    return col.strip(), int(n)


def find_spec(specs, key):
    for s in specs:
        if s["name"] == key.upper() or s["name"] == key or ".".join(s["source"]).lower() == key.lower():
            return s
    sys.exit("ERROR: no column {!r}; columns are: {}".format(key, ", ".join(s["name"] for s in specs)))


LOG_PATTERNS = [
    ("column", re.compile(r"column (\S+?): value of (\d+) bytes exceeds the declared maximum")),
    ("column", re.compile(r"ORA-12899: value too large for column (\S+) \(actual: (\d+), maximum: \d+\)")),
    ("record", re.compile(r"record of (\d+) bytes exceeds READSIZE")),
]


def scan_logs(log_dir, ext_table, table):
    """Size problems reported in this table's ORACLE_LOADER logs and pqora
    stream log: ({column: largest size seen}, largest record or 0, files)."""
    cols, record, files = {}, 0, []
    if not log_dir or not os.path.isdir(log_dir):
        return cols, record, files
    # exact ORACLE_LOADER log names (<ext>_%a_%p.log), so EXT_SALES doesn't pick up EXT_SALES_EU's logs
    pat = re.compile(r"^(" + re.escape(ext_table.lower()) + r"_\d+_\d+\.log|"
                     + re.escape(table.lower()) + r"_stream\.log)$")
    for f in sorted(os.listdir(log_dir)):
        if not pat.match(f):
            continue
        path = os.path.join(log_dir, f)
        files.append(path)
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                for kind, rx in LOG_PATTERNS:
                    for m in rx.finditer(line):
                        if kind == "record":
                            record = max(record, int(m.group(1)))
                        else:
                            name = m.group(1).split(".")[-1].strip('"')
                            cols[name] = max(cols.get(name, 0), int(m.group(2)))
    return cols, record, files


def to_lob(spec):
    spec["kind"] = "clob" if spec["kind"] == "text" else "blob"
    spec["otype"] = spec["kind"].upper()
    spec.pop("max", None)


def apply_size_changes(specs, manual, found, st):
    """Apply manual (column, size|'lob') and found {column: bytes seen} changes.
    Returns (descriptions, changed column names, {lob column: size hint})."""
    var_limit, raw_limit = size_limits(st["max_string_size"])
    margin = st.get("margin", 1.25)
    changes, changed, lob_hints = [], [], {}

    def resize(spec, size, how):
        k = spec["kind"]
        if k not in ("text", "raw", "clob", "blob"):
            return "{}: {} column ({}), size can't be changed - ignored".format(spec["name"], k, spec["otype"])
        limit = var_limit if k in ("text", "clob") else raw_limit
        old = spec["otype"]
        if k in ("clob", "blob"):
            if size != "lob":
                lob_hints[spec["name"]] = max(lob_hints.get(spec["name"], 0), size)
            return None
        if st.get("stage") and (size == "lob" or size > limit):
            if spec["max"] == limit:
                return None
            spec["max"] = limit             # larger values go to the _CLOB table
        elif size == "lob" or size > limit:
            to_lob(spec)
            if size != "lob":
                lob_hints[spec["name"]] = size
        elif how == "auto":
            new = nice_size(size, margin, limit)
            if new <= spec["max"]:
                return None
            spec["max"] = new
        else:
            spec["max"] = size
        if spec["kind"] == "text":
            spec["otype"] = "VARCHAR2({} BYTE)".format(spec["max"])
        elif spec["kind"] == "raw":
            spec["otype"] = "RAW({})".format(spec["max"])
        spec["note"] = "{} size{}".format("manual" if how == "manual" else "auto-fixed",
                                          "" if size == "lob" else ", seen/requested {} B".format(size))
        changed.append(spec["name"])
        shrink = spec["kind"] in ("text", "raw") and old != spec["otype"] and \
            spec["max"] < int(re.sub(r"\D", "", old) or 0)
        return "{}: {} -> {}{}".format(spec["name"], old, spec["otype"],
                                       "  (SMALLER - values above it will fail)" if shrink else "")

    for key, size in manual:
        d = resize(find_spec(specs, key), size, "manual")
        if d:
            changes.append(d)
    by_name = {s["name"]: s for s in specs}
    for name, seen in sorted(found.items()):
        if name not in by_name:
            changes.append("{}: reported in a log but not a column of this table - ignored".format(name))
            continue
        d = resize(by_name[name], seen, "auto")
        if d:
            changes.append(d)
    return changes, changed, lob_hints


def required_readsize(specs, lob_hints):
    need = REC_WIDTH
    for s in specs:
        if s["kind"] in ("clob", "blob"):
            need += 1 + LOB_WIDTH + lob_hints.get(s["name"], 0)
        else:
            need += field_max_bytes(s, 0)
    return need


def grow_readsize(st, specs, lob_hints, record_seen):
    """New READSIZE so the largest known record fits (never shrinks)."""
    cap = st["max_readsize"]
    exact = max(required_readsize(specs, lob_hints), record_seen)
    want = max(exact * 2 if lob_hints else exact, int(record_seen * st.get("margin", 1.25)))
    rs = max(st["readsize"], min(cap, max(MIB, pow2_ceil(want))))
    if exact > cap:
        warn("records of {:,} bytes need a READSIZE above the --max-readsize limit {:,}; "
             "such rows will still fail".format(exact, cap))
    return rs


# ---------------------------------------------------------------------------
# generate / resize
# ---------------------------------------------------------------------------

PARQUET_UNIT_PER_DAY = {"milliseconds": 86400 * 10 ** 3, "microseconds": 86400 * 10 ** 6,
                        "nanoseconds": 86400 * 10 ** 9}


def parquet_per_day(col):
    """Units per day of a Parquet date/timestamp column's raw statistics, or
    None when its statistics can't be used (INT96, unknown)."""
    if col.physical_type == "INT96":
        return None
    lt = col.logical_type
    info = json.loads(lt.to_json()) if lt is not None else {}
    if info.get("Type") == "Date":
        return 1
    if info.get("Type") == "Timestamp":
        return PARQUET_UNIT_PER_DAY.get(info.get("timeUnit"))
    conv = str(col.converted_type or "")
    return {"DATE": 1, "TIMESTAMP_MILLIS": 86400 * 10 ** 3,
            "TIMESTAMP_MICROS": 86400 * 10 ** 6}.get(conv)


def day_text(day):
    return pc.cast(pa.array([int(day)], pa.int32()).cast(pa.date32()), pa.string())[0].as_py()


def _scan_dates(path, rg_index, dspecs):
    """Read one row group's date/timestamp columns and count what Oracle can't store."""
    top = sorted({s["source"][0] for s in dspecs})
    table = pq.ParquetFile(path).read_row_group(rg_index, columns=top)
    out = {}
    for spec in dspecs:
        arr = column_array(table, spec)
        per_day = 1 if spec["kind"] == "date" else UNITS_PER_DAY[arr.type.unit]
        if pa.types.is_date64(arr.type):
            arr = pc.cast(arr, pa.date32())
        v, valid = date_ints(arr)
        days, low, high, gap = date_problems(v, valid, per_day)
        out[spec["name"]] = {
            "low": int(low.sum()), "high": int(high.sum()), "gap": int(gap.sum()),
            "min": int(days[valid].min()) if valid.any() else None,
            "max": int(days[valid].max()) if valid.any() else None,
            "examples": date_examples(days, low | high | gap)}
    return out


def date_check(files, specs, workers):
    """Check every row group of every date/timestamp column against Oracle's
    range (0001-01-01..9999-12-31, minus the 1582 gap). Uses the Parquet
    min/max statistics; reads values only for row groups that are flagged or
    have no usable statistics. Returns {column: result dict}."""
    dspecs = [s for s in specs if s["kind"] in ("date", "timestamp")]
    res = {s["name"]: {"row_groups": 0, "from_stats": 0, "read": 0, "no_stats": 0, "low": 0, "high": 0, "gap": 0,
                       "bad_row_groups": 0, "min": None, "max": None, "examples": []} for s in dspecs}
    if not dspecs:
        return res
    tasks = {}
    for f in files:
        md = pq.read_metadata(f.path)
        index = {md.schema.column(j).path: j for j in range(len(md.schema))}
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            for spec in dspecs:
                j = index.get(".".join(spec["source"]))
                if j is None:
                    continue
                r = res[spec["name"]]
                r["row_groups"] += 1
                cc = rg.column(j)
                st = cc.statistics if cc.is_stats_set else None
                per_day = parquet_per_day(md.schema.column(j))
                if st is not None and st.null_count == rg.num_rows:
                    r["from_stats"] += 1                  # all NULL
                    continue
                if not (st is not None and st.has_min_max and per_day):
                    r["no_stats"] += 1
                else:
                    lo, hi = int(st.min_raw) // per_day, int(st.max_raw) // per_day
                    r["min"] = lo if r["min"] is None else min(r["min"], lo)
                    r["max"] = hi if r["max"] is None else max(r["max"], hi)
                    if lo >= DATE_LO and hi <= DATE_HI and not (lo <= GAP_LAST and hi >= GAP_FIRST):
                        r["from_stats"] += 1
                        continue
                tasks.setdefault((f.path, i), []).append(spec)
    for name, r in res.items():
        if r["no_stats"]:
            warn("{}: no min/max statistics in {} of {} row group(s) - scanning the values".format(
                name, r["no_stats"], r["row_groups"]))
    if tasks:
        sys.stderr.write("Date check: reading {} row group(s) without usable statistics or with "
                         "suspicious ranges\n".format(len(tasks)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(workers, len(tasks)))) as pool:
            futs = {pool.submit(_scan_dates, path, i, ds): ds for (path, i), ds in tasks.items()}
            for fut in concurrent.futures.as_completed(futs):
                for name, c in fut.result().items():
                    r = res[name]
                    r["read"] += 1
                    for k in ("low", "high", "gap"):
                        r[k] += c[k]
                    if c["low"] or c["high"] or c["gap"]:
                        r["bad_row_groups"] += 1
                        r["examples"] = (r["examples"] + c["examples"])[:3]
                    for k, f in (("min", min), ("max", max)):
                        if c[k] is not None:
                            r[k] = c[k] if r[k] is None else f(r[k], c[k])
    return res


def date_note(r):
    if not r["row_groups"]:
        return ""
    rng = "{} .. {}".format(day_text(r["min"]), day_text(r["max"])) if r["min"] is not None else "all NULL"
    note = "; dates {}".format(rng)
    bad = r["low"] + r["high"] + r["gap"]
    if bad:
        note += "; NOT STORABLE: {:,} ({:,} before 0001, {:,} after 9999, {:,} in 1582 gap) in {} row group(s)".format(
            bad, r["low"], r["high"], r["gap"], r["bad_row_groups"])
    note += "; stats {}/{} row groups".format(r["from_stats"], r["row_groups"]) if r["read"] else ""
    return note


COLUMN_TYPES = ("number", "days", "text", "date", "timestamp")


def parse_column_type(s):
    if "=" not in s:
        raise argparse.ArgumentTypeError("expected COLUMN=TYPE, got {!r}".format(s))
    col, typ = s.rsplit("=", 1)
    typ = typ.strip().lower()
    if typ not in COLUMN_TYPES:
        raise argparse.ArgumentTypeError("TYPE must be one of {} (got {!r})".format(
            ", ".join(COLUMN_TYPES), typ))
    return col.strip(), typ


def is_interval_like(r):
    """All values in years 0000-0001: almost certainly durations or year-less
    dates stored on a dummy year, not real dates."""
    return bool(r) and r.get("min") is not None and r["min"] >= YEAR0_FIRST and r["max"] <= YEAR1_LAST


def apply_column_type(spec, typ, why):
    """Change how a date/timestamp column is loaded; returns a description."""
    k = spec["kind"]
    if typ in ("date", "timestamp"):
        return None
    unit = "day" if k == "date" else {"s": "second", "ms": "millisecond", "us": "microsecond",
                                       "ns": "nanosecond"}[spec["unit"]]
    if typ in ("number", "days"):
        spec.update(kind="epoch", otype="NUMBER(10)" if k == "date" else "NUMBER(19)", from_kind=k)
        how = "NUMBER = {}s since 1970-01-01 as stored in Parquet".format(unit)
        if k == "date":
            how += " (days since 0000-01-01 = value + {})".format(-YEAR0_FIRST)
    else:
        width = DATE_WIDTH if k == "date" else TS_WIDTH
        spec.update(kind="text", max=width, otype="VARCHAR2({} BYTE)".format(width), render=k, from_kind=k)
        how = "VARCHAR2 ISO text exactly as stored (any year)"
    append_note(spec, "; {}: loaded as {}".format(why, how))
    return "{}: {} -> {} ({})".format(spec["name"], k, spec["otype"], why)


def append_note(spec, text):
    """Add '; text'-style notes without a leading separator on an empty note."""
    base = spec.get("note") or ""
    spec["note"] = base + text if base else text.lstrip("; ")


def stage_main_specs(specs, max_string_size, clob_table):
    """--stage-large-records: no LOB columns in the main table; CLOB/BLOB columns
    become VARCHAR2/RAW at the limit and larger values go to the _CLOB table."""
    var_limit, raw_limit = size_limits(max_string_size)
    for s in specs:
        if s["kind"] == "clob":
            s.update(kind="text", max=var_limit, otype="VARCHAR2({} BYTE)".format(var_limit))
        elif s["kind"] == "blob":
            s.update(kind="raw", max=raw_limit, otype="RAW({})".format(raw_limit))
        else:
            continue
        append_note(s, "; larger values -> " + clob_table)


def stage_settings(st, specs, maxes, args):
    """Columns and READSIZE of the _CLOB table: every text column a CLOB and
    every binary column a BLOB, so any diverted record fits."""
    cols = []
    for s in specs:
        c = dict(s)
        c["note"] = (c.get("note") or "").replace("; larger values -> " + st["table"] + "_CLOB", "")
        if c["kind"] in ("text", "raw"):
            c.pop("max", None)
            c["kind"] = "clob" if c["kind"] == "text" else "blob"
            c["otype"] = c["kind"].upper()
        cols.append(c)
    cap = args.max_readsize
    if maxes is not None:
        need = REC_WIDTH + sum(field_max_bytes(c, 0) for c in cols if c["kind"] not in ("clob", "blob"))
        need += sum(1 + LOB_WIDTH + maxes.get(tuple(c["source"]), 0) for c in cols
                    if c["kind"] in ("clob", "blob"))
        readsize = min(cap, max(MIB, pow2_ceil(need * 2)))
    else:
        readsize = cap
    return {"table": st["table"] + "_CLOB", "ext_table": st["ext_table"] + "_CLOB",
            "readsize": readsize, "lob_max": readsize - REC_WIDTH - 1 - LOB_WIDTH, "columns": cols}


def run_generate(args):
    paths = expand_inputs(args.paths)
    if not paths:
        sys.exit("ERROR: no input files")

    # directories and directory objects: everything defaults to the parts directory
    out_dir = os.path.abspath(args.out_dir)
    parts_obj = ident(args.parts_dir_object, "--parts-dir-object")
    exec_obj = ident(args.exec_dir_object, "--exec-dir-object") if args.exec_dir_object else parts_obj
    if args.log_dir_object == "":
        log_obj = None
    else:
        log_obj = ident(args.log_dir_object, "--log-dir-object") if args.log_dir_object else parts_obj
    if args.exec_dir:
        exec_dir = os.path.abspath(args.exec_dir)
    elif exec_obj == parts_obj:
        exec_dir = out_dir
    else:
        sys.exit("ERROR: --exec-dir (OS path of {}) is required when it differs from "
                 "--parts-dir-object".format(exec_obj))
    if args.log_dir:
        log_dir = os.path.abspath(args.log_dir)
    elif log_obj == parts_obj:
        log_dir = out_dir
    else:
        log_dir = None
        warn("no --log-dir: the stream log is disabled and --auto-field-size-fix can't find the logs")
    ddl_dir = os.path.abspath(args.ddl_dir) if args.ddl_dir else out_dir
    for d in (out_dir, exec_dir, log_dir, ddl_dir):
        if d:
            os.makedirs(d, exist_ok=True)
            if not os.access(d, os.W_OK):
                sys.exit("ERROR: directory {} is not writable".format(d))
    script = os.path.abspath(args.script or __file__)
    python = os.path.abspath(args.python or sys.executable)
    wrapper = os.path.join(exec_dir, args.wrapper_name)
    wtext = wrapper_text(python, script, args.pythonpath if args.pythonpath is not None
                         else detect_pythonpath())
    wstate = wrapper_state(wrapper, wtext)
    if wstate not in ("same", "write"):
        sys.exit("ERROR: " + wstate)

    files, columns = load_files(paths)
    schema = pq.read_schema(files[0].path)
    for fi in files[1:]:
        if not pq.read_schema(fi.path).equals(schema, check_metadata=False):
            sys.exit("ERROR: schema of {} differs from {} (one generate run = one table)".format(
                fi.path, files[0].path))

    base = os.path.splitext(os.path.basename(files[0].path))[0]
    namer = Namer(args.max_name_len)
    table = ident(args.table or namer.name((base,)), "--table")
    ext_table = ident(args.ext_table or ("EXT_" + table)[:args.max_name_len], "--ext-table")

    maxes, bands, sampled, sampling = {}, {}, False, None
    if args.sample_size:
        what = "all data" if args.sample_size == float("inf") else human(args.sample_size)
        cache_file = os.path.join(out_dir, table.lower() + "_sample_cache.json")
        key = sample_key(args.sample_size)
        fingerprint = sample_fingerprint(files, schema)
        entry = cached_sample(cache_file, key, fingerprint)
        if entry is not None:
            sampling = "cached: {} sample from {}, {} of {} row groups, {:,} rows, took {:.1f}s ({})".format(
                what, entry["created"][:19], entry["row_groups_sampled"], entry["row_groups_total"],
                entry["rows"], entry["seconds"], cache_file)
            sys.stderr.write("Sampling {}: using the cache {}\n".format(what, cache_file))
        else:
            sys.stderr.write("Sampling up to {} ...\n".format(what))
            entry = sample(files, schema, args.sample_size, columns,
                           args.multi_threaded_sampling, args.pre_buffer)
            entry["fingerprint"] = fingerprint
            write_sample_cache(cache_file, key, entry)
            sampling = "{} sample, {} of {} row groups, {:,} rows, {} thread(s), {:.1f}s -> cached in {}".format(
                what, entry["row_groups_sampled"], entry["row_groups_total"], entry["rows"],
                entry["threads"], entry["seconds"], cache_file)
        maxes, bands, _ = combine_samples([{"rows": entry["rows"], "columns": entry["result"]}])
        sampled = True

    total_rows = sum(f.rows for f in files)
    column_avg = {}
    for path, c in columns.items():
        parts_ = path.split(".")
        for k in range(1, len(parts_) + 1):
            key = ".".join(parts_[:k])
            column_avg[key] = column_avg.get(key, 0) + c["uncompressed"] / float(max(1, total_rows))

    specs = build_specs(schema, args, maxes, sampled, column_avg, bands)
    dates = {}
    if not args.no_date_check:
        t0 = time.time()
        dates = date_check(files, specs, args.multi_threaded_sampling)
        for s in specs:
            if s["name"] in dates:
                s["date_check"] = dates[s["name"]]
        sys.stderr.write("Date check: {} column(s) in {:.1f}s\n".format(len(dates), time.time() - t0))
    overrides = {}
    for key, typ in args.column_type or []:
        s = find_spec(specs, key)
        if s["kind"] not in ("date", "timestamp"):
            sys.exit("ERROR: --column-type {}: only date/timestamp columns can be changed ({} is {})".format(
                key, s["name"], s["otype"]))
        overrides[s["name"]] = typ
    retyped = []
    for s in specs:
        if s["kind"] not in ("date", "timestamp"):
            continue
        if s["name"] in overrides:
            d = apply_column_type(s, overrides[s["name"]], "--column-type " + overrides[s["name"]])
        elif is_interval_like(dates.get(s["name"])):
            d = apply_column_type(s, "number", "all values in years 0000-0001, looks like an interval "
                                               "(--column-type {}=date|text to change)".format(s["name"]))
        else:
            continue
        if d:
            retyped.append(d)
    for s in specs:          # date notes, after retyping (retyped columns only show their range)
        r = dates.get(s["name"])
        if not r:
            continue
        if s["kind"] in ("date", "timestamp"):
            append_note(s, date_note(r))
        elif r.get("min") is not None:
            append_note(s, "; values as dates {} .. {}".format(day_text(r["min"]), day_text(r["max"])))
    if args.stage_large_records:
        if not log_dir or not log_obj:
            sys.exit("ERROR: --stage-large-records writes the large records into the log directory; "
                     "it needs --log-dir and --log-dir-object")
        stage_main_specs(specs, args.max_string_size, table + "_CLOB")

    # READSIZE: must hold the largest record (prefix included)
    has_lob = any(s["kind"] in ("clob", "blob") for s in specs)
    lob_paths = [tuple(s["source"]) for s in specs if s["kind"] in ("clob", "blob")]
    if has_lob and sampled:
        lob_est = sum(maxes.get(p, 0) for p in lob_paths)
        est = REC_WIDTH + sum(field_max_bytes(s, 0) for s in specs) + lob_est
        readsize = min(args.max_readsize, max(MIB, pow2_ceil(int(est * 2))))
    elif has_lob:
        readsize = args.max_readsize
        est = None
    else:
        est = REC_WIDTH + sum(field_max_bytes(s, 0) for s in specs)
        readsize = min(args.max_readsize, max(MIB, pow2_ceil(est)))
    if args.readsize:
        readsize = args.readsize

    st = {"table": table, "ext_table": ext_table,
          "parts_dir_object": parts_obj, "exec_dir_object": exec_obj, "log_dir_object": log_obj,
          "out_dir": out_dir, "exec_dir": exec_dir, "log_dir": log_dir, "ddl_dir": ddl_dir,
          "wrapper_name": args.wrapper_name, "dop": args.dop,
          "max_string_size": args.max_string_size, "max_readsize": args.max_readsize,
          "margin": args.margin, "readsize": readsize,
          "threads": args.threads, "pre_buffer": args.pre_buffer,
          "out_of_range_dates": args.out_of_range_dates,
          "sampling": sampling,
          "files": [f.path for f in files], "total_rows": total_rows}
    report = io.StringIO()
    print_inspect(files, columns, DEFAULT_DOPS, False, args.target_size, report)
    st["inspect_report"] = report.getvalue()

    # explicit and log-derived size changes
    found, record_seen = {}, 0
    if args.auto_field_size_fix:
        found, record_seen, logs = scan_logs(log_dir, ext_table, table)
        sys.stderr.write("Scanned {} log file(s) for size errors\n".format(len(logs)))
    changes, _, lob_hints = apply_size_changes(specs, args.field_size or [], found, st)
    if changes or record_seen:
        st["readsize"] = readsize = grow_readsize(st, specs, lob_hints, record_seen) \
            if not args.readsize else args.readsize
    lob_max = st["lob_max"] = readsize - REC_WIDTH - 1 - LOB_WIDTH
    if args.stage_large_records:
        st["stage"] = stage_settings(st, specs, maxes if sampled else None, args)
    if est and est > readsize:
        warn("estimated maximum record size {:,} exceeds READSIZE {:,}; rows that large will "
             "fail the load".format(est, readsize))

    parts = plan_parts(files, args.target_size)
    width = max(4, len(str(len(parts))))
    log_file = os.path.join(log_dir, table.lower() + "_stream.log") if log_dir else None

    for old in table_part_files(out_dir, table):
        os.remove(old)
    part_entries = []
    if st.get("stage"):
        st["stage"]["location"] = ["{}_clob_{:0{w}d}.dat".format(table.lower(), i, w=width)
                                   for i in range(1, len(parts) + 1)]
    for i, rgs in enumerate(parts, 1):
        name = "{}_{:0{w}d}.pqpart".format(table.lower(), i, w=width)
        sources = []
        for rg in rgs:
            if not sources or sources[-1]["path"] != rg.file:
                sources.append({"path": rg.file, "size": os.path.getsize(rg.file),
                                "row_groups": [], "rg_rows": [], "rg_bytes": [], "uncompressed": 0})
            src = sources[-1]
            src["row_groups"].append(rg.index)
            src["rg_rows"].append(rg.rows)
            src["rg_bytes"].append(rg.compressed)
            src["uncompressed"] += rg.uncompressed
        for src in sources:
            rows = sum(src["rg_rows"])
            bpr = src["uncompressed"] / float(max(1, rows))
            src["batch_rows"] = max(1, min(65536, int(BATCH_TARGET_BYTES / max(1.0, bpr))))
            del src["uncompressed"]
        rows = sum(rg.rows for rg in rgs)
        part = {
            PART_MAGIC: PART_FORMAT,
            "pqora_version": VERSION,
            "generated": datetime.datetime.now().isoformat(),
            "table": table, "ext_table": ext_table,
            "part": i, "parts": len(parts),
            "rows": rows,
            "readsize": readsize, "lob_max": lob_max,
            "threads": args.threads, "pre_buffer": args.pre_buffer,
            "out_of_range_dates": args.out_of_range_dates,
            "log_file": log_file,
            "sources": sources,
            "columns": specs,
        }
        if st.get("stage"):
            stg = st["stage"]
            part["stage"] = {"table": stg["table"], "readsize": stg["readsize"], "lob_max": stg["lob_max"],
                             "file": os.path.join(log_dir, stg["location"][i - 1]),
                             "columns": stg["columns"]}
        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as fh:
            json.dump(part, fh, indent=1)
        part_entries.append((name, rows, sum(rg.uncompressed for rg in rgs), {
            "name": name, "rows": rows,
            "compressed": sum(rg.compressed for rg in rgs),
            "uncompressed": sum(rg.uncompressed for rg in rgs),
            "sources": [{"path": src["path"], "row_groups": src["row_groups"], "rows": sum(src["rg_rows"])}
                        for src in sources]}))

    # biggest parts first in LOCATION so the long ones start early
    st["location"] = [e[0] for e in sorted(part_entries, key=lambda e: -e[2])]
    ddl_file = write_ddl(st, specs)
    if wstate == "write":
        with open(wrapper, "w", encoding="utf-8") as fh:
            fh.write(wtext)
        os.chmod(wrapper, 0o755)
    manifest = write_manifest(st, specs, [e[3] for e in part_entries])

    print("Parts    : {} x .pqpart in {}  ({:,} rows)".format(len(parts), out_dir, total_rows))
    print("DDL      : {}".format(ddl_file))
    print("Wrapper  : {}  -> {} {}{}".format(wrapper, python, script,
                                             "  (unchanged)" if wstate == "same" else ""))
    print("Manifest : {}".format(manifest))
    print("READSIZE : {:,}".format(readsize))
    if sampling:
        print("Sampling : " + sampling)
    all_rgs = [rg for f in files for rg in f.row_groups]
    n_pre = sum(1 for rg in all_rgs if args.pre_buffer and rg.compressed <= args.pre_buffer)
    print("Reading  : {} thread(s) per part; pre-buffer {} of {} row group(s) (<= {}), "
          "stream the rest".format(args.threads, n_pre, len(all_rgs),
                                   human(args.pre_buffer) if args.pre_buffer else "off"))
    print("Dir objs : " + "; ".join("{} -> {} ({})".format(o, p, pr) for o, p, pr, _ in dir_objects(st)))
    if log_file:
        print("Log file : {}".format(log_file))
    for c in changes:
        print("Resized  : " + c)
    for d in retyped:
        print("Retyped  : " + d)
    still_dates = {s["name"] for s in specs if s["kind"] in ("date", "timestamp")}
    dates = {n: r for n, r in dates.items() if n in still_dates}
    bad_dates = [(n, r) for n, r in dates.items() if r["low"] + r["high"] + r["gap"]]
    if dates and not bad_dates:
        print("Dates    : {} column(s) checked in every row group: all storable in Oracle".format(len(dates)))
    for n, r in bad_dates:
        effect = {"error": "the load will FAIL", "null": "they will be loaded as NULL",
                  "clamp": "they will be clamped"}[args.out_of_range_dates]
        print("Dates    : {}: {:,} value(s) Oracle can't store (e.g. {}); --out-of-range-dates {}: {}".format(
            n, r["low"] + r["high"] + r["gap"], ", ".join(r["examples"]), args.out_of_range_dates, effect))
    if st.get("stage"):
        print("Large    : records with a value above the main column sizes go to {} via {} "
              "(files {}_clob_*.dat in {}, READSIZE {:,})".format(
                  st["stage"]["table"], st["stage"]["ext_table"], table.lower(), log_dir,
                  st["stage"]["readsize"]))
    if not sampled and any(s["kind"] == "text" for s in specs):
        print("Note     : VARCHAR2 sizes are defaults; use --sample-size (or 'all') to size them "
              "from the data.")
    return 0


def manifest_path(st):
    return os.path.join(st["out_dir"], st["table"].lower() + "_manifest.json")


def write_manifest(st, specs, parts):
    path = manifest_path(st)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"table": st["table"], "rows": st["total_rows"], "readsize": st["readsize"],
                   "pqora_version": VERSION, "generated": datetime.datetime.now().isoformat(),
                   "settings": st, "columns": specs, "parts": parts}, fh, indent=1)
    return path


def run_resize(args):
    target = os.path.abspath(args.target)
    if target.endswith(".pqpart"):
        part = read_part(target)
        target = os.path.join(os.path.dirname(target), part["table"].lower() + "_manifest.json")
    try:
        with open(target, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    except (OSError, ValueError) as e:
        sys.exit("ERROR: cannot read manifest {}: {}".format(target, e))
    st = manifest.get("settings")
    if not st:
        sys.exit("ERROR: {} was written by an older pqora; rerun generate once".format(target))
    parts_dir = os.path.dirname(target)
    part_files = table_part_files(parts_dir, st["table"])
    if not part_files:
        sys.exit("ERROR: no part files of {} in {}".format(st["table"], parts_dir))
    specs = read_part(part_files[0])["columns"]

    found, record_seen, logs = {}, 0, []
    if args.auto_field_size_fix:
        log_dir = os.path.abspath(args.log_dir) if args.log_dir else st.get("log_dir")
        if not log_dir:
            sys.exit("ERROR: no log directory known for {}; pass --log-dir".format(st["table"]))
        found, record_seen, logs = scan_logs(log_dir, st["ext_table"], st["table"])
        print("Scanned {} log file(s) in {}".format(len(logs), log_dir))
        for name, seen in sorted(found.items()):
            print("  {:<32} largest value reported: {:,} bytes".format(name, seen))
        if record_seen:
            print("  {:<32} largest record reported: {:,} bytes".format("(READSIZE)", record_seen))
    if not args.field_size and not found and not record_seen:
        print("Nothing to change.")
        return 0

    old_rs = st["readsize"]
    changes, changed, lob_hints = apply_size_changes(specs, args.field_size or [], found, st)
    new_rs = grow_readsize(st, specs, lob_hints, record_seen)
    if new_rs != old_rs:
        changes.append("READSIZE: {:,} -> {:,}".format(old_rs, new_rs))
    for c in changes:
        print("Change   : " + c)
    if not changed and new_rs == old_rs:
        print("Nothing to change (sizes are already large enough).")
        return 0
    if args.dry_run:
        print("Dry run - nothing written.")
        return 0

    st["readsize"] = new_rs
    st["lob_max"] = new_rs - REC_WIDTH - 1 - LOB_WIDTH
    for f in part_files:
        p = read_part(f)
        p["columns"] = specs
        p["readsize"], p["lob_max"] = st["readsize"], st["lob_max"]
        p["resized"] = datetime.datetime.now().isoformat()
        with open(f, "w", encoding="utf-8") as fh:
            json.dump(p, fh, indent=1)
    ddl_file = write_ddl(st, specs)
    alter_file = write_alter(st, specs, changed)
    write_manifest(st, specs, manifest.get("parts", []))
    print("Updated  : {} part file(s), {}, {}".format(len(part_files), os.path.basename(ddl_file),
                                                      os.path.basename(manifest_path(st))))
    print("Next     : run {}".format(alter_file))
    print("           (recreates {}, widens {}), then rerun the INSERT".format(st["ext_table"], st["table"]))
    if logs:
        print("Tip      : old log files are re-read on every --auto-field-size-fix; sizes only grow, "
              "so this is harmless, but you can delete them after a successful load.")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def is_part_file(path):
    if path.endswith(".pqpart"):
        return True
    try:
        with open(path, "rb") as fh:
            head = fh.read(64)
        return head.lstrip().startswith(b'{') and PART_MAGIC.encode() in head
    except OSError:
        return False


def add_size_fix(ap):
    ap.add_argument("--field-size", type=parse_field_size, action="append", metavar="COLUMN=SIZE",
                    help="set a column's size in bytes (repeatable). COLUMN is the Oracle column "
                         "name or the Parquet path (a.b); SIZE like 500, 8K or 'lob'. Sizes above the "
                         "VARCHAR2/RAW limit become CLOB/BLOB")
    ap.add_argument("--auto-field-size-fix", action="store_true",
                    help="read this table's ORACLE_LOADER logs and pqora stream log and enlarge every "
                         "column (and READSIZE) that a load reported as too small")


def add_common(ap):
    ap.add_argument("--target-size", type=parse_size, default=0, metavar="SIZE",
                    help="group consecutive row groups into parts of about SIZE uncompressed "
                         "(default 0 = one part per row group)")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv

    # stream mode: exactly one argument that is a part file (how Oracle calls us)
    if len(argv) == 1 and not argv[0].startswith("-") and is_part_file(argv[0]):
        return run_stream(argv[0])

    if argv and argv[0] == "verify":
        ap = argparse.ArgumentParser(prog="pqora.py verify",
                                     description="Run stream mode into a decoder and check framing.")
        ap.add_argument("parts", nargs="+")
        a = ap.parse_args(argv[1:])
        return run_verify(a.parts)

    if argv and argv[0] == "generate":
        ap = argparse.ArgumentParser(
            prog="pqora.py generate",
            description="Write .pqpart control files, the ORACLE_LOADER DDL and the preprocessor wrapper.")
        ap.add_argument("paths", nargs="+", help="Parquet files, globs or directories")
        ap.add_argument("--out-dir", default=".",
                        help="where to write the .pqpart files (OS path of --parts-dir-object; "
                             "default: the current directory)")
        ap.add_argument("--table", help="target table name (default: first file name)")
        ap.add_argument("--ext-table", help="external table name (default: EXT_<table>)")
        ap.add_argument("--parts-dir-object", default="PQORA_PARTS",
                        help="directory object of --out-dir (default PQORA_PARTS); the exec and log "
                             "objects default to it, so one object with READ, WRITE, EXECUTE is enough")
        ap.add_argument("--exec-dir-object",
                        help="directory object holding pqora.sh (default: --parts-dir-object)")
        ap.add_argument("--log-dir-object",
                        help="directory object for ORACLE_LOADER log/bad files (default: "
                             "--parts-dir-object; '' = no log files)")
        ap.add_argument("--exec-dir", help="OS path of --exec-dir-object; pqora.sh is written there "
                                           "(default: --out-dir when the objects are the same)")
        ap.add_argument("--log-dir", help="OS path of --log-dir-object; stream mode appends one JSON "
                                          "line per part run to <table>_stream.log there (default: "
                                          "--out-dir when the objects are the same)")
        ap.add_argument("--ddl-dir", help="where to write <table>.sql (default: --out-dir)")
        ap.add_argument("--wrapper-name", default="pqora.sh")
        ap.add_argument("--python", help="python interpreter for the wrapper (default: this one)")
        ap.add_argument("--script", help="path of pqora.py for the wrapper (default: this file)")
        ap.add_argument("--pythonpath", help="PYTHONPATH for the wrapper (default: auto-detect)")
        add_common(ap)
        ap.add_argument("--sample-size", type=parse_size, metavar="SIZE",
                        help="sample this much data (e.g. 500MB, or 'all' for an exact scan) to size "
                             "VARCHAR2/RAW columns")
        ap.add_argument("--multi-threaded-sampling", type=int, default=16, metavar="N",
                        help="sample N row groups at a time (default 16, 1 = sequential)")
        ap.add_argument("--margin", type=float, default=1.25,
                        help="multiplier on sampled max lengths (default 1.25)")
        ap.add_argument("--default-varchar", type=int, default=4000,
                        help="VARCHAR2 size when not sampled (default 4000)")
        ap.add_argument("--max-string-size", choices=("standard", "extended"), default="standard",
                        help="database MAX_STRING_SIZE (VARCHAR2 4000 / 32767)")
        ap.add_argument("--max-name-len", type=int, default=128)
        ap.add_argument("--max-readsize", type=parse_size, default=DEFAULT_MAX_READSIZE,
                        help="upper limit for READSIZE (default 128MB); the largest record must fit")
        ap.add_argument("--readsize", type=parse_size, help="force READSIZE")
        ap.add_argument("--dop", type=int, default=8, help="PARALLEL degree in the DDL (default 8)")
        ap.add_argument("--out-of-range-dates", choices=("error", "null", "clamp"), default="error",
                        help="dates/timestamps Oracle can't store (before 0001-01-01, after 9999-12-31, "
                             "or 1582-10-05..14): error (default) fails the load, null loads NULL, clamp "
                             "moves them to 0001-01-01 / 9999-12-31 23:59:59.999999999 / 1582-10-15")
        ap.add_argument("--column-type", type=parse_column_type, action="append", metavar="COLUMN=TYPE",
                        help="load a date/timestamp column as number (raw Parquet value: days/units since "
                             "1970-01-01), text (ISO text, any year) or date (normal; disables the "
                             "automatic interval detection). Repeatable. Columns whose values are all in "
                             "years 0000-0001 are loaded as number automatically")
        ap.add_argument("--no-date-check", action="store_true",
                        help="skip the check of all date/timestamp values (Parquet statistics, plus "
                             "reading row groups without statistics or with suspicious ranges)")
        ap.add_argument("--stage-large-records", action="store_true",
                        help="keep LOBs out of the main external table: records with any value larger "
                             "than its VARCHAR2/RAW column are written to files in the log directory "
                             "instead and loaded through a second external table EXT_<table>_CLOB into "
                             "<table>_CLOB (CLOB/BLOB columns)")
        ap.add_argument("--threads", type=int, default=8,
                        help="pyarrow decode/read threads per preprocessor process (default 8; "
                             "Oracle already runs one process per part)")
        ap.add_argument("--pre-buffer", type=parse_size, default=128 * MIB, metavar="SIZE",
                        help="row groups up to SIZE compressed are read with large coalesced, "
                             "concurrent requests (memory ~ one row group per process); larger "
                             "ones are streamed in 8 MB reads (default 128MB, 0 = always stream)")
        add_size_fix(ap)
        a = ap.parse_args(argv[1:])
        return run_generate(a)

    if argv and argv[0] == "resize":
        ap = argparse.ArgumentParser(
            prog="pqora.py resize",
            description="Change column sizes of an already generated table without rescanning the "
                        "data: updates the part files, <table>.sql and writes <table>_alter.sql.")
        ap.add_argument("target", help="<table>_manifest.json or any .pqpart file of the table")
        add_size_fix(ap)
        ap.add_argument("--log-dir", help="where the logs are (default: the generate --log-dir)")
        ap.add_argument("--dry-run", action="store_true", help="only show the changes")
        a = ap.parse_args(argv[1:])
        if not a.field_size and not a.auto_field_size_fix:
            ap.error("give --field-size and/or --auto-field-size-fix")
        return run_resize(a)

    ap = argparse.ArgumentParser(
        description="pqora {}: Parquet -> Oracle external table preprocessor. Default mode "
                    "inspects Parquet files; see also 'generate', 'resize' and 'verify'.".format(VERSION))
    ap.add_argument("paths", nargs="+", help="Parquet files, globs or directories")
    ap.add_argument("--dop-list", default=",".join(str(d) for d in DEFAULT_DOPS))
    ap.add_argument("--list-rowgroups", action="store_true")
    add_common(ap)
    a = ap.parse_args(argv)
    paths = expand_inputs(a.paths)
    if not paths:
        ap.error("no input files")
    files, columns = load_files(paths)
    dops = [int(d) for d in a.dop_list.split(",") if d.strip()]
    print_inspect(files, columns, dops, a.list_rowgroups, a.target_size, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
