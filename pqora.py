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
import datetime
import glob
import hashlib
import heapq
import json
import math
import os
import re
import socket
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

VERSION = "1.0"
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


def to_json_array(arr):
    vals = []
    for v in plain(arr).to_pylist():
        if v is None:
            vals.append(None)
        else:
            vals.append(json.dumps(v, default=_json_default, ensure_ascii=False,
                                   separators=(",", ":")))
    return pa.array(vals, pa.string())


# ---------------------------------------------------------------------------
# sampling
# ---------------------------------------------------------------------------

def _max_len(arr):
    lens = pc.binary_length(arr)
    m = pc.max(lens).as_py()
    return m or 0


def measure(arr, path, maxes):
    arr = plain(arr)
    t = arr.type
    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            measure(struct_child(arr, t[i].name), path + (t[i].name,), maxes)
    elif is_json_type(t):
        maxes[path] = max(maxes.get(path, 0), _max_len(to_json_array(arr)))
    elif is_text(t) or is_binary(t):
        if _is("is_large_string", t):
            arr = pc.cast(arr, pa.string())
        elif _is("is_large_binary", t):
            arr = pc.cast(arr, pa.binary())
        maxes[path] = max(maxes.get(path, 0), _max_len(arr))


def sample(files, schema, budget, columns):
    """Read up to `budget` bytes spread evenly across all row groups (only the
    columns that need size estimation). Returns ({path: max bytes}, rows)."""
    cols = [f.name for f in schema if needs_sampling(f.type)]
    maxes = {}
    if not cols:
        return maxes, 0
    rgs = [rg for f in files for rg in f.row_groups]
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
    rows, handles = 0, {}
    for i, rg in enumerate(chosen, 1):
        sys.stderr.write("  sampling {}/{}: {} row group {}\n".format(
            i, len(chosen), os.path.basename(rg.file), rg.index))
        pf = handles.get(rg.file)
        if pf is None:
            pf = handles[rg.file] = pq.ParquetFile(rg.file, buffer_size=8 * MIB, pre_buffer=False)
        read = 0
        for batch in pf.iter_batches(batch_size=batch_rows, row_groups=[rg.index], columns=cols):
            for name in cols:
                measure(batch.column(batch.schema.get_field_index(name)), (name,), maxes)
            rows += batch.num_rows
            read += batch.nbytes
            if read >= per_rg:
                break
    return maxes, rows


# ---------------------------------------------------------------------------
# column specs (shared by DDL generation and stream mode)
# ---------------------------------------------------------------------------

class Namer(object):
    def __init__(self, max_len):
        self.max_len = max_len
        self.used = set()

    def name(self, parts):
        raw = "#".join(re.sub(r"[^A-Za-z0-9_$#]", "_", p).upper() for p in parts)
        if not raw or not raw[0].isalpha():
            raw = "C_" + raw
        name = raw
        if len(name) > self.max_len:
            h = hashlib.sha1("\x00".join(parts).encode("utf-8")).hexdigest()[:8].upper()
            name = name[:self.max_len - 9] + "_" + h
        base, k = name, 2
        while name in self.used:
            suffix = "_{}".format(k)
            name = base[:self.max_len - len(suffix)] + suffix
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


def build_specs(schema, args, maxes, sampled, column_avg):
    """Flatten the schema into a list of column specs (dicts)."""
    namer = Namer(args.max_name_len)
    var_limit = 32767 if args.max_string_size == "extended" else 4000
    raw_limit = 32767 if args.max_string_size == "extended" else 2000
    specs = []

    def sized(path, limit, default):
        """(size or None for LOB, note)"""
        key = ".".join(path)
        if sampled:
            obs = maxes.get(path, 0)
            if obs > limit:
                return None, "sampled max {} B".format(obs)
            return nice_size(obs, args.margin, limit), "sampled max {} B".format(obs)
        avg = column_avg.get(key)
        if avg and avg > limit:
            return None, "metadata avg {:,.0f} B".format(avg)
        return min(default, limit), "default size"

    def add(path, t, spec, note):
        spec["name"] = namer.name(path)
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
            n, note = sized(path, raw_limit, raw_limit)
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


class StreamError(Exception):
    pass


def as_binary(arr):
    return pc.cast(arr, pa.binary())


def length_prefixed(arr, width, max_bytes, what):
    """arr: string/binary array (may contain nulls) -> binary array of
    '<len zero-padded to width><bytes>', nulls become length 0."""
    lens = pc.fill_null(pc.binary_length(arr), 0)
    m = pc.max(lens).as_py() or 0
    if m > max_bytes:
        raise StreamError("{}: value of {} bytes exceeds the declared maximum of {} bytes "
                          "(regenerate the parts/DDL with a larger size)".format(what, m, max_bytes))
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
    if spec.get("json"):
        return to_json_array(arr)
    if k in ("text", "clob"):
        return pc.cast(arr, pa.string())
    if k in ("raw", "blob"):
        return pc.cast(arr, pa.binary())
    raise StreamError("unknown column kind " + k)


def check_calendar_gap(arr, per_day, spec):
    if len(arr) == 0 or arr.null_count == len(arr):
        return
    ints = pc.cast(arr, pa.int32()) if pa.types.is_date32(arr.type) else pc.cast(arr, pa.int64())
    v = pc.fill_null(ints, 0).to_numpy(zero_copy_only=False).astype(np.int64)
    days = np.floor_divide(v, per_day)
    bad = (days >= GAP_FIRST) & (days <= GAP_LAST) & ~np.asarray(
        arr.is_null().to_numpy(zero_copy_only=False), dtype=bool)
    if bad.any():
        raise StreamError("column {}: {} value(s) between 1582-10-05 and 1582-10-14; these dates "
                          "don't exist in Oracle's calendar and would be silently changed".format(
                              spec["name"], int(bad.sum())))


def fixed_text(arr, spec):
    """Timestamps / dates as fixed-width text, nulls as blanks."""
    if spec["kind"] == "timestamp":
        check_calendar_gap(arr, UNITS_PER_DAY[arr.type.unit], spec)
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
        check_calendar_gap(arr, 1, spec)
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


def encode_batch(batch, specs, readsize, lob_max):
    """Returns a binary array with one complete record per row."""
    fields = []
    for spec in specs:
        arr = column_array(batch, spec)
        k = spec["kind"]
        if k in ("timestamp", "date"):
            fields.append(fixed_text(arr, spec))
            continue
        width, max_bytes = field_layout(spec, lob_max)
        text = to_text(arr, spec)
        if k in ("clob", "blob"):
            fields.append(pc.if_else(pc.is_null(text), B_N, B_V))
        fields.append(length_prefixed(text, width, max_bytes, "column " + spec["name"]))
    body = pc.binary_join_element_wise(*(fields + [b""]))
    rec_lens = pc.binary_length(body)
    m = pc.max(rec_lens).as_py() or 0
    if m + REC_WIDTH > readsize:
        raise StreamError("record of {} bytes exceeds READSIZE {} (regenerate with a larger "
                          "--max-readsize, or the data has an unexpectedly large value)"
                          .format(m + REC_WIDTH, readsize))
    prefix = as_binary(pc.utf8_lpad(pc.cast(rec_lens, pa.string()), width=REC_WIDTH, padding="0"))
    return pc.binary_join_element_wise(prefix, body, b"")


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


def stream_part(part, out):
    """Write all records of a part to out (binary). Returns (rows, bytes)."""
    specs = part["columns"]
    top = []
    for s in specs:
        if s["source"][0] not in top:
            top.append(s["source"][0])
    rows = nbytes = 0
    for src in part["sources"]:
        check_source(src)
        pf = pq.ParquetFile(src["path"], buffer_size=8 * MIB, pre_buffer=False)
        for batch in pf.iter_batches(batch_size=src["batch_rows"], row_groups=src["row_groups"],
                                     columns=top, use_threads=True):
            if batch.num_rows == 0:
                continue
            rec = encode_batch(batch, specs, part["readsize"], part["lob_max"])
            buf = records_buffer(rec)
            out.write(buf)
            rows += batch.num_rows
            nbytes += len(buf)
    if rows != part["rows"]:
        raise StreamError("wrote {} rows but the part expects {}".format(rows, part["rows"]))
    return rows, nbytes


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


def run_stream(path):
    t0 = time.time()
    part = None
    try:
        part = read_part(path)
        log_line(part, event="start")
        out = sys.stdout.buffer
        rows, nbytes = stream_part(part, out)
        out.flush()
        log_line(part, event="end", status="ok", rows=rows, bytes=nbytes,
                 seconds=round(time.time() - t0, 3))
        return 0
    except BrokenPipeError:
        # Oracle stopped reading (e.g. ROWNUM limit) - not an error
        if part:
            log_line(part, event="end", status="reader closed pipe", seconds=round(time.time() - t0, 3))
        try:
            sys.stdout = None
        except Exception:
            pass
        return 0
    except Exception as e:
        msg = "pqora {}: {}: {}".format(VERSION, os.path.basename(path), e)
        if not isinstance(e, (StreamError, OSError)):
            msg = "pqora {}: {}: {}: {}".format(VERSION, os.path.basename(path), type(e).__name__, e)
        sys.stderr.write(msg.encode("ascii", "backslashreplace").decode("ascii") + "\n")
        if part:
            log_line(part, event="end", status="error", error=str(e), seconds=round(time.time() - t0, 3))
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
        t0 = time.time()
        try:
            rows, _ = stream_part(part, dec)
            dec.close()
        except StreamError as e:
            print("{}: FAILED: {}".format(path, e))
            rc = 1
            continue
        print("{}: OK  rows={:,} bytes={:,} max_record={:,} (READSIZE {:,})  {:.1f}s".format(
            os.path.basename(path), dec.rows, dec.bytes, dec.max_record, part["readsize"],
            time.time() - t0))
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


def write_ddl(path, args, specs, part_names, readsize, lob_max, total_rows, files):
    ext, tgt = args.ext_table, args.table
    L = []
    w = L.append
    w("-- Generated by pqora.py {} on {}".format(VERSION, datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
    w("-- Source: {} file(s), {:,} rows, {} part(s)".format(len(files), total_rows, len(part_names)))
    w("-- Directory objects (create as DBA):")
    w("--   {:<14} -> {}   (READ, EXECUTE)  holds pqora.sh".format(args.exec_dir_object, args.exec_dir or "<exec dir>"))
    w("--   {:<14} -> {}   (READ)           holds the .pqpart files".format(args.parts_dir_object, args.out_dir))
    if args.log_dir_object:
        w("--   {:<14} -> {}   (READ, WRITE)    ORACLE_LOADER log/bad files".format(
            args.log_dir_object, args.log_dir or "<log dir>"))
    w("")
    w("CREATE TABLE {} (".format(q(ext)))
    L.extend(column_lines(specs))
    w(")")
    w("ORGANIZATION EXTERNAL (")
    w("  TYPE ORACLE_LOADER")
    w("  DEFAULT DIRECTORY {}".format(args.parts_dir_object))
    w("  ACCESS PARAMETERS (")
    w("    RECORDS VARIABLE {}".format(REC_WIDTH))
    w("    CHARACTERSET AL32UTF8")
    w("    STRING SIZES ARE IN BYTES")
    w("    PREPROCESSOR {}:'{}'".format(args.exec_dir_object, args.wrapper_name))
    w("    READSIZE {}".format(readsize))
    if args.log_dir_object:
        w("    BADFILE {}:'{}_%a_%p.bad'".format(args.log_dir_object, ext.lower()))
        w("    LOGFILE {}:'{}_%a_%p.log'".format(args.log_dir_object, ext.lower()))
    else:
        w("    NOBADFILE")
        w("    NOLOGFILE")
    w("    FIELDS (")
    fl = ddl_fields(specs, lob_max)
    for i, line in enumerate(fl):
        w("      " + line + ("," if i < len(fl) - 1 else ""))
    w("    )")
    w("  )")
    w("  LOCATION (")
    for i, n in enumerate(part_names):
        w("    " + sql_str(n) + ("," if i < len(part_names) - 1 else ""))
    w("  )")
    w(")")
    w("REJECT LIMIT 0")
    w("PARALLEL {};".format(args.dop))
    w("")
    w("CREATE TABLE {} (".format(q(tgt)))
    L.extend(column_lines(specs))
    w(");")
    w("")
    w("ALTER SESSION ENABLE PARALLEL DML;")
    w("")
    w("INSERT /*+ APPEND PARALLEL({}) */ INTO {}".format(args.dop, q(tgt)))
    w("SELECT /*+ PARALLEL({}) */ * FROM {};".format(args.dop, q(ext)))
    w("")
    w("COMMIT;")
    w("")
    w("-- expected row count: {}".format(total_rows))
    w("SELECT COUNT(*) AS loaded_rows, {} AS expected_rows FROM {};".format(total_rows, q(tgt)))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


def write_wrapper(path, python, script, pythonpath):
    lines = ["#!/bin/sh",
             "# pqora preprocessor wrapper (generated by pqora.py {}).".format(VERSION),
             "# Oracle runs this as the database OS user with an empty environment and",
             "# passes one argument: the full path of a .pqpart location file."]
    if pythonpath:
        lines.append("PYTHONPATH={}".format(pythonpath))
        lines.append("export PYTHONPATH")
    lines.append('exec {} {} "$1"'.format(python, script))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    os.chmod(path, 0o755)


def detect_pythonpath():
    """If pyarrow is imported from a non-default location (user site,
    PYTHONPATH), return that directory so the wrapper can set PYTHONPATH."""
    import site
    loc = os.path.dirname(os.path.dirname(os.path.abspath(pa.__file__)))
    defaults = set(getattr(site, "getsitepackages", lambda: [])())
    if loc in defaults:
        return ""
    return loc


def run_generate(args):
    paths = expand_inputs(args.paths)
    if not paths:
        sys.exit("ERROR: no input files")
    files, columns = load_files(paths)
    schema = pq.read_schema(files[0].path)
    for fi in files[1:]:
        if not pq.read_schema(fi.path).equals(schema, check_metadata=False):
            sys.exit("ERROR: schema of {} differs from {}".format(fi.path, files[0].path))

    maxes, sampled = {}, False
    if args.sample_size:
        sys.stderr.write("Sampling up to {} ...\n".format(
            "all data" if args.sample_size == float("inf") else human(args.sample_size)))
        maxes, nrows = sample(files, schema, args.sample_size, columns)
        sampled = True
        sys.stderr.write("  sampled {:,} rows\n".format(nrows))

    total_rows = sum(f.rows for f in files)
    column_avg = {}
    for path, c in columns.items():
        parts_ = path.split(".")
        for k in range(1, len(parts_) + 1):
            key = ".".join(parts_[:k])
            column_avg[key] = column_avg.get(key, 0) + c["uncompressed"] / float(max(1, total_rows))

    base = os.path.splitext(os.path.basename(files[0].path))[0]
    namer = Namer(args.max_name_len)
    args.table = ident(args.table or namer.name((base,)), "--table")
    args.ext_table = ident(args.ext_table or (args.table + "_EXT")[:args.max_name_len], "--ext-table")
    for a in ("exec_dir_object", "parts_dir_object", "log_dir_object"):
        if getattr(args, a):
            setattr(args, a, ident(getattr(args, a), "--" + a.replace("_", "-")))

    specs = build_specs(schema, args, maxes, sampled, column_avg)

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
    lob_max = readsize - REC_WIDTH - 1 - LOB_WIDTH
    if est and est > readsize:
        warn("estimated maximum record size {:,} exceeds READSIZE {:,}; rows that large will "
             "fail the load".format(est, readsize))

    os.makedirs(args.out_dir, exist_ok=True)
    out_dir = os.path.abspath(args.out_dir)
    args.out_dir = out_dir
    parts = plan_parts(files, args.target_size)
    width = max(4, len(str(len(parts))))
    script = os.path.abspath(args.script or __file__)
    python = os.path.abspath(args.python or sys.executable)
    log_file = None
    if args.log_dir:
        os.makedirs(args.log_dir, exist_ok=True)
        log_file = os.path.join(os.path.abspath(args.log_dir), args.table.lower() + "_stream.log")

    for old in glob.glob(os.path.join(out_dir, args.table.lower() + "_*.pqpart")):
        os.remove(old)
    part_entries = []
    for i, rgs in enumerate(parts, 1):
        name = "{}_{:0{w}d}.pqpart".format(args.table.lower(), i, w=width)
        sources = []
        for rg in rgs:
            if not sources or sources[-1]["path"] != rg.file:
                sources.append({"path": rg.file, "size": os.path.getsize(rg.file),
                                "row_groups": [], "rg_rows": [], "uncompressed": 0})
            src = sources[-1]
            src["row_groups"].append(rg.index)
            src["rg_rows"].append(rg.rows)
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
            "table": args.table, "ext_table": args.ext_table,
            "part": i, "parts": len(parts),
            "rows": rows,
            "readsize": readsize, "lob_max": lob_max,
            "log_file": log_file,
            "sources": sources,
            "columns": specs,
        }
        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as fh:
            json.dump(part, fh, indent=1)
        part_entries.append((name, rows, sum(rg.uncompressed for rg in rgs)))

    # biggest parts first in LOCATION so the long ones start early
    location = [n for n, _, _ in sorted(part_entries, key=lambda e: -e[2])]
    ddl_path = os.path.join(args.ddl_dir or out_dir, args.table.lower() + ".sql")
    write_ddl(ddl_path, args, specs, location, readsize, lob_max, total_rows, files)

    wrapper_dir = args.exec_dir or out_dir
    os.makedirs(wrapper_dir, exist_ok=True)
    wrapper = os.path.join(wrapper_dir, args.wrapper_name)
    write_wrapper(wrapper, python, script, args.pythonpath if args.pythonpath is not None
                  else detect_pythonpath())

    manifest = os.path.join(out_dir, args.table.lower() + "_manifest.json")
    with open(manifest, "w", encoding="utf-8") as fh:
        json.dump({"table": args.table, "rows": total_rows, "readsize": readsize,
                   "parts": [{"name": n, "rows": r, "uncompressed": u} for n, r, u in part_entries]},
                  fh, indent=1)

    print("Parts    : {} x .pqpart in {}  ({:,} rows)".format(len(parts), out_dir, total_rows))
    print("DDL      : {}".format(ddl_path))
    print("Wrapper  : {}  -> {} {}".format(wrapper, python, script))
    print("Manifest : {}".format(manifest))
    print("READSIZE : {:,}".format(readsize))
    if log_file:
        print("Log file : {}".format(log_file))
    if not sampled and any(s["kind"] == "text" for s in specs):
        print("Note     : VARCHAR2 sizes are defaults; use --sample-size (or 'all') to size them "
              "from the data.")
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
        ap.add_argument("--out-dir", required=True,
                        help="where to write the .pqpart files (the PARTS directory object path)")
        ap.add_argument("--table", help="target table name (default: first file name)")
        ap.add_argument("--ext-table", help="external table name (default: <table>_EXT)")
        ap.add_argument("--exec-dir", help="OS path of the EXEC directory object; pqora.sh is written "
                                           "there (default: --out-dir)")
        ap.add_argument("--log-dir", help="OS path of the LOG directory object; stream mode appends "
                                          "one JSON line per part run to <table>_stream.log there")
        ap.add_argument("--exec-dir-object", default="PQORA_EXEC")
        ap.add_argument("--parts-dir-object", default="PQORA_PARTS")
        ap.add_argument("--log-dir-object", default="PQORA_LOG",
                        help="directory object for ORACLE_LOADER log/bad files ('' = none)")
        ap.add_argument("--ddl-dir", help="where to write <table>.sql (default: --out-dir)")
        ap.add_argument("--wrapper-name", default="pqora.sh")
        ap.add_argument("--python", help="python interpreter for the wrapper (default: this one)")
        ap.add_argument("--script", help="path of pqora.py for the wrapper (default: this file)")
        ap.add_argument("--pythonpath", help="PYTHONPATH for the wrapper (default: auto-detect)")
        add_common(ap)
        ap.add_argument("--sample-size", type=parse_size, metavar="SIZE",
                        help="sample this much data (e.g. 500MB, or 'all' for an exact scan) to size "
                             "VARCHAR2/RAW columns")
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
        a = ap.parse_args(argv[1:])
        if a.log_dir_object == "":
            a.log_dir_object = None
        return run_generate(a)

    ap = argparse.ArgumentParser(
        description="pqora {}: Parquet -> Oracle external table preprocessor. Default mode "
                    "inspects Parquet files; see also 'generate' and 'verify'.".format(VERSION))
    ap.add_argument("paths", nargs="+", help="Parquet files, globs or directories")
    ap.add_argument("--dop-list", default="2,4,8,16,32,64")
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
