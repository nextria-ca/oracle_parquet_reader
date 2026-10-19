#!/usr/bin/env python3
"""
parquet_inspect.py - assess Parquet files for parallel loading into Oracle and
generate ORACLE_BIGDATA external table DDL.

Produces:
  1. A metadata summary: sizes (compressed / uncompressed), rows, row groups,
     row group size distribution, largest columns, and a parallelism
     assessment (load balance for a range of DOPs, using row groups or whole
     files as the unit of work).
  2. DDL:
       - external table (TYPE ORACLE_BIGDATA) over the Parquet files.
         ORACLE_BIGDATA returns complex columns (struct / list / map) as JSON
         text, so these are declared as VARCHAR2 (or CLOB) columns.
       - target table where nested struct fields are flattened into
         prefixed columns, e.g. user.name -> "USER#NAME".
       - INSERT /*+ APPEND PARALLEL */ ... SELECT that extracts the nested
         fields with JSON_VALUE.

Optional --sample-size reads a sample of row groups (only the string, binary
and nested columns) to size VARCHAR2 / RAW columns from observed data instead
of using defaults.

Requires pyarrow.
"""

import argparse
import base64
import datetime
import decimal
import glob
import hashlib
import heapq
import json
import math
import os
import re
import sys
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def human(n):
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def parse_size(s):
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?)(?:I?B)?\s*", s.upper())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid size: {s!r} (examples: 500MB, 2G)")
    mult = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}[m.group(2)]
    return int(float(m.group(1)) * mult)


def warn(msg):
    print(f"WARNING: {msg}", file=sys.stderr)


def natural_key(s):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", s)]


def expand_inputs(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            found = sorted(glob.glob(os.path.join(p, "*.parquet")), key=natural_key)
            if not found:
                warn(f"no *.parquet files in directory {p}")
            files.extend(found)
        else:
            files.extend(sorted(glob.glob(p), key=natural_key) or [p])
    return files


def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


# ---------------------------------------------------------------------------
# metadata summary
# ---------------------------------------------------------------------------

@dataclass
class RowGroup:
    file: str
    index: int
    rows: int
    compressed: int
    uncompressed: int


@dataclass
class FileInfo:
    path: str
    disk_size: int
    rows: int
    row_groups: list
    created_by: str
    codecs: set
    has_int96: bool


def read_file_info(path, column_sizes):
    md = pq.read_metadata(path)
    rgs, codecs = [], set()
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        comp = unc = 0
        for j in range(rg.num_columns):
            c = rg.column(j)
            comp += c.total_compressed_size
            unc += c.total_uncompressed_size
            codecs.add(c.compression)
            acc = column_sizes.setdefault(c.path_in_schema, [0, 0])
            acc[0] += c.total_compressed_size
            acc[1] += c.total_uncompressed_size
        rgs.append(RowGroup(path, i, rg.num_rows, comp, unc or rg.total_byte_size))
    has_int96 = any(md.schema.column(j).physical_type == "INT96"
                    for j in range(len(md.schema)))
    return FileInfo(path, os.path.getsize(path), md.num_rows, rgs,
                    md.created_by or "?", codecs, has_int96)


def lpt_efficiency(sizes, dop):
    """Greedy longest-processing-time schedule of work units over dop workers.
    Returns (efficiency, busiest worker load). Efficiency 100% = perfect balance."""
    if not sizes:
        return 0.0, 0
    workers = [0] * dop
    for s in sorted(sizes, reverse=True):
        heapq.heapreplace(workers, workers[0] + s)
    busiest = max(workers)
    return sum(sizes) / (dop * busiest), busiest


RG_BUCKETS = [(0, 16), (16, 64), (64, 128), (128, 256), (256, 512), (512, 1024), (1024, None)]


def print_summary(files, column_sizes, dops, list_rowgroups, out):
    p = lambda *a: print(*a, file=out)
    rgs = [rg for f in files for rg in f.row_groups]
    total_rows = sum(f.rows for f in files)
    total_disk = sum(f.disk_size for f in files)
    total_comp = sum(rg.compressed for rg in rgs)
    total_unc = sum(rg.uncompressed for rg in rgs)

    p("=" * 78)
    p("PARQUET METADATA SUMMARY")
    p("=" * 78)
    p(f"Files            : {len(files)}")
    p(f"Rows             : {total_rows:,}")
    p(f"Row groups       : {len(rgs):,}")
    p(f"Size on disk     : {human(total_disk)}")
    p(f"Compressed data  : {human(total_comp)}")
    p(f"Uncompressed data: {human(total_unc)}"
      + (f"   (ratio {total_unc / total_comp:.2f}x)" if total_comp else ""))
    p(f"Codecs           : {', '.join(sorted({c for f in files for c in f.codecs})) or '-'}")
    p(f"Created by       : {', '.join(sorted({f.created_by for f in files}))}")

    if len(files) > 1:
        p("")
        p(f"{'File':<40} {'Rows':>14} {'RGs':>6} {'Compressed':>12} {'Uncompressed':>13}")
        for f in files:
            name = os.path.basename(f.path)
            name = name if len(name) <= 40 else "..." + name[-37:]
            p(f"{name:<40} {f.rows:>14,} {len(f.row_groups):>6} "
              f"{human(sum(r.compressed for r in f.row_groups)):>12} "
              f"{human(sum(r.uncompressed for r in f.row_groups)):>13}")

    if not rgs:
        return

    unc = sorted(rg.uncompressed for rg in rgs)
    rows = sorted(rg.rows for rg in rgs)
    comp = sorted(rg.compressed for rg in rgs)
    p("")
    p("Row group statistics")
    p(f"  {'':<14} {'min':>12} {'p50':>12} {'avg':>12} {'p95':>12} {'max':>12}")
    for label, vals, fmt in (("uncompressed", unc, human), ("compressed", comp, human),
                             ("rows", rows, lambda v: f"{v:,.0f}")):
        p(f"  {label:<14} {fmt(vals[0]):>12} {fmt(percentile(vals, .5)):>12} "
          f"{fmt(sum(vals) / len(vals)):>12} {fmt(percentile(vals, .95)):>12} {fmt(vals[-1]):>12}")

    p("")
    p("Row group size distribution (uncompressed; Oracle recommends ~128 MiB, 64-512 MiB OK)")
    mib = 1024 * 1024
    for lo, hi in RG_BUCKETS:
        n = sum(1 for v in unc if v >= lo * mib and (hi is None or v < hi * mib))
        label = f"{lo}-{hi} MiB" if hi else f">= {lo} MiB"
        bar = "#" * max(1 if n else 0, round(40 * n / len(unc)))
        p(f"  {label:<14} {n:>7}  {bar}")

    top = sorted(column_sizes.items(), key=lambda kv: kv[1][1], reverse=True)[:10]
    p("")
    p("Largest columns (all files)")
    p(f"  {'column':<46} {'compressed':>12} {'uncompressed':>13} {'share':>6}")
    for path, (c, u) in top:
        name = path if len(path) <= 46 else "..." + path[-43:]
        p(f"  {name:<46} {human(c):>12} {human(u):>13} {100 * u / total_unc:>5.1f}%")

    p("")
    p("Parallelism assessment (greedy load balance of uncompressed bytes)")
    p("  'row group' = one row group per parallel work unit (control-file / preprocessor")
    p("  design, or if the driver splits files by row group); 'file' = one file per unit.")
    p(f"  {'DOP':>5} | {'row group: efficiency':>22} {'busiest worker':>15} | "
      f"{'file: efficiency':>17} {'busiest worker':>15}")
    file_sizes = [sum(r.uncompressed for r in f.row_groups) for f in files]
    for dop in dops:
        e1, b1 = lpt_efficiency(unc, dop)
        e2, b2 = lpt_efficiency(file_sizes, dop)
        note = "  (DOP > row groups)" if dop > len(rgs) else ""
        p(f"  {dop:>5} | {100 * e1:>21.0f}% {human(b1):>15} | {100 * e2:>16.0f}% {human(b2):>15}{note}")

    p("")
    advice = []
    big = sum(1 for v in unc if v > 512 * mib)
    small = sum(1 for v in unc if v < 16 * mib)
    if big:
        advice.append(f"{big} row group(s) > 512 MiB: cannot be split further and may cause "
                      "PGA pressure; consider rewriting with smaller row groups.")
    if small > len(unc) / 2:
        advice.append(f"{small} row group(s) < 16 MiB: per-unit overhead will dominate; "
                      "consider grouping row groups per work unit or rewriting files.")
    usable = [d for d in dops if lpt_efficiency(unc, d)[0] >= 0.85]
    if usable:
        advice.append(f"Highest listed DOP with >= 85% balance at row group granularity: {max(usable)}.")
    else:
        advice.append("No listed DOP reaches 85% balance at row group granularity.")
    if any(f.has_int96 for f in files):
        advice.append("INT96 timestamps present (legacy Spark/Impala).")
    for a in advice:
        p(f"  * {a}")

    if list_rowgroups:
        p("")
        p(f"{'file':<40} {'rg':>5} {'rows':>12} {'compressed':>12} {'uncompressed':>13}")
        for rg in rgs:
            name = os.path.basename(rg.file)
            name = name if len(name) <= 40 else "..." + name[-37:]
            p(f"{name:<40} {rg.index:>5} {rg.rows:>12,} {human(rg.compressed):>12} "
              f"{human(rg.uncompressed):>13}")
    p("")


# ---------------------------------------------------------------------------
# sampling (observed max byte lengths)
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


def is_text(t):
    return (pa.types.is_string(t) or pa.types.is_large_string(t)
            or getattr(pa.types, "is_string_view", lambda _: False)(t))


def is_binary(t):
    return (pa.types.is_binary(t) or pa.types.is_large_binary(t)
            or getattr(pa.types, "is_binary_view", lambda _: False)(t))


def is_nested_json(t):
    """Types ORACLE_BIGDATA returns as JSON and we do not flatten."""
    return (pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t)
            or pa.types.is_map(t) or getattr(pa.types, "is_list_view", lambda _: False)(t)
            or getattr(pa.types, "is_large_list_view", lambda _: False)(t))


def needs_sampling(t):
    t = unwrap(t)
    return is_text(t) or is_binary(t) or pa.types.is_struct(t) or is_nested_json(t)


def _json_default(v):
    if isinstance(v, (bytes, bytearray)):
        return base64.b64encode(v).decode()
    if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
        return v.isoformat()
    if isinstance(v, (decimal.Decimal, datetime.timedelta)):
        return str(v)
    return str(v)


def json_max_len(arr):
    m = 0
    for v in arr.to_pylist():
        if v is not None:
            s = json.dumps(v, default=_json_default, ensure_ascii=False, separators=(",", ":"))
            m = max(m, len(s.encode("utf-8")))
    return m


def measure(arr, path, text_max, json_max):
    if isinstance(arr, pa.ChunkedArray):
        for chunk in arr.chunks:
            measure(chunk, path, text_max, json_max)
        return
    if pa.types.is_dictionary(arr.type):
        arr = arr.dictionary_decode()
    if isinstance(arr.type, pa.ExtensionType):
        arr = arr.storage
    t = arr.type
    if is_text(t) or is_binary(t):
        try:
            m = pc.max(pc.binary_length(arr)).as_py() or 0
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
            m = max((len(v.encode("utf-8") if isinstance(v, str) else v)
                     for v in arr.to_pylist() if v is not None), default=0)
        text_max[path] = max(text_max.get(path, 0), m)
    elif pa.types.is_struct(t):
        for i in range(t.num_fields):
            measure(arr.field(i), path + (t.field(i).name,), text_max, json_max)
        if len(path) == 1:  # top-level struct = one JSON column in the external table
            json_max[path] = max(json_max.get(path, 0), json_max_len(arr))
    elif is_nested_json(t):
        json_max[path] = max(json_max.get(path, 0), json_max_len(arr))


def sample(files, schema, budget, column_sizes):
    """Read up to `budget` bytes (Arrow in-memory, ~uncompressed) spread evenly
    across all row groups, only for columns that need size estimation."""
    cols = [f.name for f in schema if needs_sampling(f.type)]
    text_max, json_max = {}, {}
    if not cols:
        return text_max, json_max, 0
    # size batches to ~64 MiB so huge row groups / values don't blow up memory
    total_rows = sum(f.rows for f in files) or 1
    sel_bytes = sum(u for path, (_, u) in column_sizes.items() if path.split(".")[0] in cols)
    batch_rows = max(1, min(65536, int(64 * 1024 * 1024 / max(1, sel_bytes / total_rows))))
    rgs = [rg for f in files for rg in f.row_groups]
    total = sum(rg.uncompressed for rg in rgs)
    n = len(rgs) if budget >= total else max(1, min(len(rgs), math.ceil(budget / (total / len(rgs)))))
    chosen = [rgs[int(k * len(rgs) / n)] for k in range(n)]
    per_rg = budget / n
    sampled_rows, handles = 0, {}
    for i, rg in enumerate(chosen, 1):
        print(f"  sampling {i}/{n}: {os.path.basename(rg.file)} row group {rg.index}",
              file=sys.stderr)
        pf = handles.get(rg.file) or handles.setdefault(
            rg.file, pq.ParquetFile(rg.file, buffer_size=8 * 1024 * 1024, pre_buffer=False))
        read = 0
        for batch in pf.iter_batches(batch_size=batch_rows, row_groups=[rg.index], columns=cols):
            for name in cols:
                measure(batch.column(name), (name,), text_max, json_max)
            sampled_rows += batch.num_rows
            read += batch.nbytes
            if read >= per_rg:
                break
    return text_max, json_max, sampled_rows


# ---------------------------------------------------------------------------
# Oracle naming and types
# ---------------------------------------------------------------------------

class Namer:
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
            suffix = f"_{k}"
            name = base[:self.max_len - len(suffix)] + suffix
            k += 1
        self.used.add(name)
        return name


def q(ident):
    return '"' + ident.replace('"', '""') + '"'


NICE = [1, 2, 5, 10, 20, 30, 50, 100, 150, 200, 250, 500, 750, 1000, 1500, 2000,
        3000, 4000, 6000, 8000, 12000, 16000, 24000, 32767]


def size_from_sample(observed, default, limit, margin):
    """Returns a size, or None if the data needs a LOB."""
    if observed is None:
        return min(default, limit)
    if observed > limit:
        return None
    want = max(1, math.ceil(observed * margin))
    return min(next((b for b in NICE if b >= want), want), limit)


@dataclass
class OraType:
    sql: str            # column type
    note: str           # comment text
    kind: str = "scalar"  # scalar | bool | text | binary | json


class TypeMapper:
    def __init__(self, args, text_max, json_max, sampled, column_avg):
        self.a = args
        self.column_avg = column_avg  # "a.b" -> avg uncompressed bytes per row (metadata)
        self.text_max = text_max
        self.json_max = json_max
        self.sampled = sampled
        self.var_limit = 32767 if args.max_string_size == "extended" else 4000
        self.raw_limit = 32767 if args.max_string_size == "extended" else 2000

    def _obs(self, store, path, limit):
        """Observed max length; when not sampled, None (use defaults) unless the
        metadata average per value already exceeds the limit (forces a LOB)."""
        if self.sampled:
            return store.get(path, 0)
        avg = self.column_avg.get(".".join(path))
        return math.ceil(avg) if avg and avg > limit else None

    def _obs_note(self, store, path, limit):
        if self.sampled:
            return f", sampled max {store.get(path, 0)} B"
        avg = self.column_avg.get(".".join(path))
        return f", metadata avg {avg:,.0f} B > {limit}" if avg and avg > limit else ""

    def json(self, t, path):
        obs = self._obs(self.json_max, path, self.var_limit)
        n = size_from_sample(obs, self.var_limit, self.var_limit, self.a.margin)
        kind = "struct" if pa.types.is_struct(t) else "map" if pa.types.is_map(t) else "list"
        note = f"JSON ({kind})" + self._obs_note(self.json_max, path, self.var_limit)
        return OraType("CLOB" if n is None else f"VARCHAR2({n} BYTE)", note, "json")

    def scalar(self, t, path, in_json=False):
        """in_json: value extracted from a JSON column (binary arrives as text)."""
        u = unwrap(t)
        T = pa.types
        if T.is_boolean(u):
            return OraType("NUMBER(1)", "boolean", "bool")
        for pred, digits in ((T.is_int8, 3), (T.is_uint8, 3), (T.is_int16, 5), (T.is_uint16, 5),
                             (T.is_int32, 10), (T.is_uint32, 10), (T.is_int64, 19), (T.is_uint64, 20)):
            if pred(u):
                return OraType(f"NUMBER({digits})", str(u))
        if T.is_float16(u) or T.is_float32(u):
            return OraType("BINARY_FLOAT", str(u))
        if T.is_float64(u):
            return OraType("BINARY_DOUBLE", str(u))
        if T.is_decimal(u):
            if u.precision <= 38:
                return OraType(f"NUMBER({u.precision},{u.scale})", str(u))
            return OraType("NUMBER", f"{u} - precision > 38, may lose digits")
        if T.is_date(u):
            return OraType("DATE", str(u))
        if T.is_timestamp(u):
            prec = {"s": 0, "ms": 3, "us": 6, "ns": 9}[u.unit]
            note = str(u) + (" (UTC-adjusted)" if u.tz else "")
            return OraType(f"TIMESTAMP({prec})", note)
        if T.is_time(u):
            return OraType("VARCHAR2(18 BYTE)", f"{u} - time of day as text")
        if T.is_duration(u):
            return OraType("NUMBER(19)", f"{u} - count of {u.unit}")
        if T.is_null(u):
            return OraType("VARCHAR2(1 BYTE)", "all-null column")
        if is_text(u) or (in_json and is_binary(u)):
            obs = self._obs(self.text_max, path, self.var_limit)
            if in_json and is_binary(u) and obs is not None:
                obs = math.ceil(obs * 4 / 3) + 4  # base64-ish text in JSON
            n = size_from_sample(obs, self.a.default_varchar, self.var_limit, self.a.margin)
            note = str(u) + self._obs_note(self.text_max, path, self.var_limit)
            return OraType("CLOB" if n is None else f"VARCHAR2({n} BYTE)", note, "text")
        if T.is_fixed_size_binary(u):
            if u.byte_width <= self.raw_limit:
                return OraType(f"RAW({u.byte_width})", str(t), "binary")
            return OraType("BLOB", str(t), "binary")
        if is_binary(u):
            obs = self._obs(self.text_max, path, self.raw_limit)
            n = size_from_sample(obs, self.raw_limit, self.raw_limit, self.a.margin)
            note = str(u) + self._obs_note(self.text_max, path, self.raw_limit)
            return OraType("BLOB" if n is None else f"RAW({n})", note, "binary")
        warn(f"unmapped type {t} for {'.'.join(path)}; using VARCHAR2({self.var_limit})")
        return OraType(f"VARCHAR2({self.var_limit} BYTE)", f"unmapped {t}", "text")


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------

def json_path(parts):
    return "$" + "".join('."' + p.replace("\\", "\\\\").replace('"', '\\"') + '"' for p in parts)


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def column_lines(cols):
    """cols: list of (name, sql_type, comment). Comments after the comma."""
    width = min(40, max(len(q(n)) for n, _, _ in cols))
    out = []
    for i, (n, t, c) in enumerate(cols):
        sep = "," if i < len(cols) - 1 else " "
        out.append(f"  {q(n):<{width}} {t}{sep} -- {c}")
    return out


def generate_ddl(files, schema, mapper, args):
    ext_namer, tgt_namer = Namer(args.max_name_len), Namer(args.max_name_len)
    ext_cols, colmap, tgt_cols, select_exprs = [], [], [], []

    def flatten(field_type, parts, ext_name):
        """Add target columns + select expressions for a field inside a JSON
        column `ext_name` at JSON path `parts[1:]`."""
        u = unwrap(field_type)
        if pa.types.is_struct(u):
            for i in range(u.num_fields):
                f = u.field(i)
                flatten(f.type, parts + (f.name,), ext_name)
            return
        name = tgt_namer.name(parts)
        src = ".".join(parts)
        jp = sql_str(json_path(parts[1:]))
        col = f"e.{q(ext_name)}"
        if is_nested_json(u):
            ot = mapper.json(u, parts)
            expr = f"JSON_QUERY({col}, {jp} RETURNING {ot.sql.replace(' BYTE', '')} ERROR ON ERROR)"
        else:
            ot = mapper.scalar(field_type, parts, in_json=True)
            if ot.kind == "bool":
                expr = f"CASE JSON_VALUE({col}, {jp}) WHEN 'true' THEN 1 WHEN 'false' THEN 0 END"
            else:
                ret = re.sub(r"TIMESTAMP\(\d\)", "TIMESTAMP", ot.sql).replace(" BYTE", "")
                expr = f"JSON_VALUE({col}, {jp} RETURNING {ret} ERROR ON ERROR)"
        tgt_cols.append((name, ot.sql, f"{src}: {ot.note}"))
        select_exprs.append(expr)

    for f in schema:
        u = unwrap(f.type)
        ename = ext_namer.name((f.name,))
        if ename != f.name.upper() or args.colmap_all:
            colmap.append({"col": ename, "field": f.name})
        if pa.types.is_struct(u) or is_nested_json(u):
            ot = mapper.json(u, (f.name,))
            ext_cols.append((ename, ot.sql, f"{f.name}: {ot.note}"))
            if pa.types.is_struct(u):
                for i in range(u.num_fields):
                    c = u.field(i)
                    flatten(c.type, (f.name, c.name), ename)
                continue
        else:
            ot = mapper.scalar(f.type, (f.name,))
            ext_cols.append((ename, ot.sql, f"{f.name}: {ot.note}"))
        tname = tgt_namer.name((f.name,))
        tgt_cols.append((tname, ot.sql, f"{f.name}: {ot.note}"))
        select_exprs.append(f"e.{q(ename)}")

    dirs = {os.path.dirname(os.path.abspath(f.path)) for f in files}
    if len(dirs) > 1:
        warn("input files are in different directories; LOCATION lists base names only, "
             "so all files must be reachable through the one directory object")

    out = []
    w = out.append
    w("-- Generated by parquet_inspect.py on "
      + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
    w(f"-- Source: {len(files)} file(s), {sum(fi.rows for fi in files):,} rows, "
      f"{sum(len(fi.row_groups) for fi in files)} row groups")
    w("-- Sizes: " + ("from sampled data" if mapper.sampled
                      else f"defaults (VARCHAR2 {args.default_varchar}); use --sample-size to size from data"))
    w(f"-- Directory object {q(args.directory)} must point at: {', '.join(sorted(dirs))}")
    w("")
    w("-- 1. External table (ORACLE_BIGDATA returns struct/list/map columns as JSON text)")
    w(f"CREATE TABLE {q(args.ext_table)} (")
    out.extend(column_lines(ext_cols))
    w(")")
    w("ORGANIZATION EXTERNAL (")
    w("  TYPE ORACLE_BIGDATA")
    w(f"  DEFAULT DIRECTORY {q(args.directory)}")
    w("  ACCESS PARAMETERS (")
    w("    com.oracle.bigdata.fileformat = parquet")
    if any(fi.has_int96 for fi in files):
        w("    com.oracle.bigdata.prq.int96_as_timestamp = true")
    if colmap:
        w("    com.oracle.bigdata.colmap = " + json.dumps(colmap if len(colmap) > 1 else colmap[0], ensure_ascii=False))
    w("  )")
    w("  LOCATION (")
    names = [os.path.basename(fi.path) for fi in files]
    for i, n in enumerate(names):
        w(f"    {sql_str(n)}{',' if i < len(names) - 1 else ''}")
    w("  )")
    w(")")
    w("REJECT LIMIT 0")
    w(f"PARALLEL {args.dop};")
    w("")
    w("-- 2. Target table (nested struct fields flattened as PARENT#CHILD)")
    w(f"CREATE TABLE {q(args.table)} (")
    out.extend(column_lines(tgt_cols))
    w(");")
    w("")
    w("-- 3. Parallel direct-path load")
    w("ALTER SESSION ENABLE PARALLEL DML;")
    w("")
    w(f"INSERT /*+ APPEND PARALLEL({args.dop}) */ INTO {q(args.table)} (")
    tnames = [q(n) for n, _, _ in tgt_cols]
    for i, n in enumerate(tnames):
        w(f"  {n}{',' if i < len(tnames) - 1 else ''}")
    w(")")
    w(f"SELECT /*+ PARALLEL({args.dop}) */")
    for i, e in enumerate(select_exprs):
        w(f"  {e}{',' if i < len(select_exprs) - 1 else ''}")
    w(f"FROM {q(args.ext_table)} e;")
    w("")
    w("COMMIT;")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Summarize Parquet metadata for parallel Oracle loads and generate "
                    "ORACLE_BIGDATA external table DDL.")
    ap.add_argument("paths", nargs="+", help="Parquet files, globs, or directories (*.parquet)")
    ap.add_argument("--sample-size", type=parse_size, metavar="SIZE",
                    help="sample about this much data (e.g. 500MB, 2G) spread across row groups "
                         "to size VARCHAR2/RAW columns from observed max lengths")
    ap.add_argument("--margin", type=float, default=1.25,
                    help="multiplier applied to sampled max lengths (default 1.25)")
    ap.add_argument("--table", help="target table name (default: first file name)")
    ap.add_argument("--ext-table", help="external table name (default: <table>_EXT)")
    ap.add_argument("--directory", default="PARQUET_DIR",
                    help="Oracle directory object name (default PARQUET_DIR)")
    ap.add_argument("--dop", type=int, default=16,
                    help="degree of parallelism for the DDL/INSERT hints (default 16)")
    ap.add_argument("--dop-list", default="2,4,8,16,32,64,128",
                    help="DOPs evaluated in the parallelism assessment")
    ap.add_argument("--max-name-len", type=int, default=128,
                    help="max Oracle identifier length (128 for 12.2+, 30 for older)")
    ap.add_argument("--max-string-size", choices=("standard", "extended"), default="standard",
                    help="database MAX_STRING_SIZE: VARCHAR2 limit 4000 or 32767 (default standard)")
    ap.add_argument("--default-varchar", type=int, default=4000,
                    help="VARCHAR2 size for strings when not sampled (default 4000)")
    ap.add_argument("--colmap-all", action="store_true",
                    help="emit com.oracle.bigdata.colmap for every column, not only renamed ones")
    ap.add_argument("--list-rowgroups", action="store_true", help="list every row group")
    ap.add_argument("--ddl-out", metavar="FILE", help="write DDL to FILE instead of stdout")
    ap.add_argument("--no-summary", action="store_true", help="skip the metadata summary")
    ap.add_argument("--no-ddl", action="store_true", help="skip DDL generation")
    args = ap.parse_args()

    paths = expand_inputs(args.paths)
    if not paths:
        ap.error("no input files")

    column_sizes = {}
    files = []
    for p in paths:
        try:
            files.append(read_file_info(p, column_sizes))
        except (OSError, pa.ArrowException) as e:
            sys.exit(f"ERROR: cannot read {p}: {e}")

    if not args.no_summary:
        dops = [int(d) for d in args.dop_list.split(",") if d.strip()]
        print_summary(files, column_sizes, dops, args.list_rowgroups, sys.stdout)

    if args.no_ddl:
        return

    schema = pq.read_schema(files[0].path)
    for fi in files[1:]:
        s = pq.read_schema(fi.path)
        if not s.equals(schema, check_metadata=False):
            warn(f"schema of {fi.path} differs from {files[0].path}; DDL uses the first file")

    text_max, json_max, sampled = {}, {}, False
    if args.sample_size:
        print(f"Sampling up to {human(args.sample_size)} ...", file=sys.stderr)
        text_max, json_max, nrows = sample(files, schema, args.sample_size, column_sizes)
        sampled = True
        print(f"  sampled {nrows:,} rows", file=sys.stderr)

    base = os.path.splitext(os.path.basename(files[0].path))[0]
    namer = Namer(args.max_name_len)
    args.table = args.table or namer.name((base,))
    args.ext_table = args.ext_table or namer.name((args.table + "_EXT",))

    total_rows = sum(fi.rows for fi in files) or 1
    column_avg = {}  # every path prefix gets the sum of its leaves
    for path, (_, u) in column_sizes.items():
        parts = path.split(".")
        for k in range(1, len(parts) + 1):
            key = ".".join(parts[:k])
            column_avg[key] = column_avg.get(key, 0) + u / total_rows
    mapper = TypeMapper(args, text_max, json_max, sampled, column_avg)
    ddl = generate_ddl(files, schema, mapper, args)
    if args.ddl_out:
        with open(args.ddl_out, "w", encoding="utf-8") as fh:
            fh.write(ddl)
        print(f"DDL written to {args.ddl_out}", file=sys.stderr)
    else:
        print(ddl)


if __name__ == "__main__":
    main()
