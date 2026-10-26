#!/usr/bin/env python3
"""
pqora_selftest.py - test data and result checks for pqora.py.

  make DIR [--big-rows N] [--blob-mb 1,10,50,100]
        write types.parquet, big.parquet and blobs.parquet into DIR
  expected PART_FILE > expected.json
        compute per-column checksums from the Parquet files of a table
        (any one .pqpart of the table; all parts are read)
  sql PART_FILE TABLE > check.sql
        print a query computing the same checksums in Oracle (one scan);
        each output line is  CHK|<column>.<check>|<value>
  compare expected.json SPOOL [--label NAME]
        compare the query output with the expected values; exit 1 on mismatch
  parallel LOG_FILE TABLE [--since ISO]
        summarize pqora stream log lines: parts, processes, max concurrency

Compatible with Python 3.6+ and pyarrow 6.0.1+.
"""

import argparse
import datetime
import decimal
import hashlib
import json
import os
import struct
import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pqora  # noqa: E402

decimal.getcontext().prec = 80
GAP_FIRST = (datetime.date(1582, 10, 5) - datetime.date(1970, 1, 1)).days
GAP_LAST = (datetime.date(1582, 10, 14) - datetime.date(1970, 1, 1)).days


# ---------------------------------------------------------------------------
# test data
# ---------------------------------------------------------------------------

def make_types(path, n=20000, rg_rows=2500):
    rng = np.random.RandomState(42)
    idx = np.arange(n)

    def nulls(every):
        return (idx % every) == 3

    def arr(values, typ, every=None):
        mask = nulls(every) if every else None
        return pa.array(values, typ, mask=mask)

    i8 = rng.randint(-128, 128, n).astype(np.int8)
    i8[:2] = [-128, 127]
    i16 = rng.randint(-32768, 32768, n).astype(np.int16)
    i32 = rng.randint(-2 ** 31, 2 ** 31, n, dtype=np.int64).astype(np.int32)
    i32[:2] = [-2 ** 31, 2 ** 31 - 1]
    i64 = rng.randint(-2 ** 62, 2 ** 62, n, dtype=np.int64)
    i64[:2] = [-2 ** 63, 2 ** 63 - 1]
    u64 = rng.randint(0, 2 ** 62, n, dtype=np.int64).astype(np.uint64)
    u64[:2] = [0, 2 ** 64 - 1]

    f64 = rng.standard_normal(n) * 10.0 ** rng.randint(-30, 30, n)
    specials = [float("nan"), float("inf"), float("-inf"), -0.0, 0.0, 5e-324,
                1.7976931348623157e308, -1.7976931348623157e308, 0.1, 1 / 3.0, 2.0 ** -1074]
    f64[10:10 + len(specials)] = specials
    f32 = (rng.standard_normal(n) * 10.0 ** rng.randint(-10, 10, n)).astype(np.float32)
    s32 = [float("nan"), float("inf"), float("-inf"), -0.0, 3.4028235e38, 1.4e-45, 0.1]
    f32[10:10 + len(s32)] = s32

    def decs(prec, scale):
        out = []
        for i in range(n):
            digits = rng.randint(0, prec + 1)
            mag = int(rng.randint(0, 10 ** min(digits, 18), dtype=np.int64)) if digits else 0
            if prec > 18 and i % 3 == 0:
                mag = mag * 10 ** (prec - 18) + 7
            mag = min(mag, 10 ** prec - 1)
            sign = -1 if i % 2 else 1
            out.append(decimal.Decimal(sign * mag).scaleb(-scale))
        out[0] = decimal.Decimal(10 ** prec - 1).scaleb(-scale)
        out[1] = -out[0]
        return out

    days = rng.randint(-719162, 2932896, n)               # 0001-01-01 .. 9999-12-31
    days[:2] = [-719162, 2932896]
    gap = (days >= GAP_FIRST) & (days <= GAP_LAST)          # 1582-10-05..14 don't exist in Oracle
    days[gap] += 20
    secs = days.astype(np.int64) * 86400 + rng.randint(0, 86400, n)
    frac_ns = rng.randint(0, 10 ** 9, n, dtype=np.int64)
    ts_us = secs * 10 ** 6 + frac_ns // 1000
    ns_secs = rng.randint(-9 * 10 ** 9, 9 * 10 ** 9, n, dtype=np.int64)  # within ns range
    ts_ns = ns_secs * 10 ** 9 + frac_ns

    words = ["", " ", "N", "V", "a", "héllo", "日本語", "🎉 emoji", "quote \" ' `", "comma,semi;pipe|",
             "line1\nline2\r\nline3", "tab\there", "nul\x00byte", "ctrl\x1e\x1f\x01", "trailing   ",
             "0000000005abc", "​zero width", "ç" * 300, "x" * 3999]
    s_short = [words[i % len(words)] + ("" if i % 5 else str(i)) for i in range(n)]
    s_short = [s[:3999] for s in s_short]
    long_chunk = "Lorem ipsum é ü ß 日本 "
    s_long = [None if i % 11 == 3 else ("" if i % 11 == 4 else
              (long_chunk * (1 + (i * 37) % 600))[:100 + (i * 997) % 12000]) for i in range(n)]
    statuses = ["NEW", "OPEN", "CLOSED", "ÉTAT", ""]
    s_dict = pa.array([statuses[i % len(statuses)] for i in range(n)], mask=nulls(9)).dictionary_encode()
    bin_small = [None if i % 13 == 3 else (b"" if i % 13 == 4 else
                 bytes(rng.randint(0, 256, 1 + i % 100, dtype=np.int64).astype(np.uint8)))
                 for i in range(n)]
    bin_small[5] = b"\x00\x00\x00"
    bin_big = [None if i % 17 == 3 else (b"" if i % 17 == 4 or i % 4 else
               os.urandom(2000 + (i * 7919) % 40000)) for i in range(n)]
    uuid = [None if i % 19 == 3 else os.urandom(16) for i in range(n)]
    users = []
    for i in range(n):
        if i % 23 == 3:
            users.append(None)
        else:
            users.append({"name": None if i % 29 == 4 else "user {} ü".format(i),
                          "age": None if i % 31 == 5 else int(i % 100),
                          "addr": None if i % 37 == 6 else {"city": ["Toronto", "Montréal", ""][i % 3],
                                                             "zip": "M5V {}".format(i % 1000)}})
    user_t = pa.struct([("name", pa.string()), ("age", pa.int32()),
                        ("addr", pa.struct([("city", pa.string()), ("zip", pa.string())]))])
    tags = [None if i % 41 == 3 else ["t{}".format(j) for j in range(i % 5)] for i in range(n)]
    props = [None if i % 43 == 3 else [("k{}".format(j), j) for j in range(i % 3)] for i in range(n)]

    t = pa.table({
        "id": pa.array(idx, pa.int64()),
        "i8": arr(i8, pa.int8(), 7),
        "i16": arr(i16, pa.int16(), 7),
        "i32": arr(i32, pa.int32(), 7),
        "i64": arr(i64, pa.int64(), 7),
        "u64": arr(u64, pa.uint64(), 7),
        "f32": arr(f32, pa.float32(), 7),
        "f64": arr(f64, pa.float64(), 7),
        "dec9_2": arr(decs(9, 2), pa.decimal128(9, 2), 7),
        "dec18_4": arr(decs(18, 4), pa.decimal128(18, 4), 7),
        "dec38_10": arr(decs(38, 10), pa.decimal128(38, 10), 7),
        "flag": arr(idx % 3 == 0, pa.bool_(), 7),
        "d": arr(days.astype(np.int32), pa.date32(), 7),
        "ts_s": arr(secs, pa.timestamp("s"), 7),
        "ts_ms": arr(secs * 1000 + frac_ns // 10 ** 6, pa.timestamp("ms"), 7),
        "ts_us": arr(ts_us, pa.timestamp("us"), 7),
        "ts_ns": arr(ts_ns, pa.timestamp("ns"), 7),
        "ts_utc": arr(ts_us, pa.timestamp("us", tz="UTC"), 7),
        "ts_toronto": arr(ts_us, pa.timestamp("us", tz="America/Toronto"), 7),
        "t_us": arr(rng.randint(0, 86400 * 10 ** 6, n, dtype=np.int64), pa.time64("us"), 7),
        "s_short": arr(s_short, pa.string(), 7),
        "s_long": pa.array(s_long, pa.string()),
        "s_dict": s_dict,
        "bin_small": pa.array(bin_small, pa.binary()),
        "bin_big": pa.array(bin_big, pa.binary()),
        "uuid": pa.array(uuid, pa.binary(16)),
        "user": pa.array(users, user_t),
        "tags": pa.array(tags, pa.list_(pa.string())),
        "props": pa.array(props, pa.map_(pa.string(), pa.int32())),
        "weird name-ü": arr(idx * 2, pa.int64(), 7),
    })
    pq.write_table(t, path, row_group_size=rg_rows, version="2.6")
    return n


def make_big(path, n, rg_rows=250000):
    rng = np.random.RandomState(7)
    writer = None
    statuses = pa.array(["NEW", "OPEN", "CLOSED", "CANCELLED"])
    done = 0
    while done < n:
        m = min(rg_rows, n - done)
        ids = np.arange(done, done + m, dtype=np.int64)
        ids_s = pc.cast(pa.array(ids), pa.string())
        name = pc.binary_join_element_wise(pa.scalar("customer name "), ids_s, "")
        email = pc.binary_join_element_wise(pa.scalar("user"), ids_s, pa.scalar("@example.com"), "")
        comment_len = rng.randint(0, 200, m)
        comment = pa.array(["c" * k if k % 10 else None for k in comment_len], pa.string())
        t = pa.table({
            "id": pa.array(ids),
            "cust_id": pa.array(rng.randint(0, 10 ** 6, m).astype(np.int32)),
            "name": name,
            "email": email,
            "amount": pa.array([decimal.Decimal(int(v)).scaleb(-2) for v in rng.randint(-10 ** 9, 10 ** 9, m)],
                               pa.decimal128(12, 2)),
            "qty": pa.array(rng.randint(0, 1000, m).astype(np.int32)),
            "price": pa.array(rng.random_sample(m) * 1000),
            "created": pa.array(1600000000 * 10 ** 6 + rng.randint(0, 10 ** 14, m, dtype=np.int64),
                                pa.timestamp("us")),
            "status": statuses.take(pa.array(rng.randint(0, 4, m))),
            "comment": comment,
            "flag": pa.array(rng.randint(0, 2, m).astype(bool)),
        })
        if writer is None:
            writer = pq.ParquetWriter(path, t.schema)
        writer.write_table(t, row_group_size=rg_rows)
        done += m
    writer.close()
    return n


def make_gap(path):
    """One valid row and one date inside the 1582 calendar gap (must be refused)."""
    t = pa.table({"id": pa.array([1, 2], pa.int64()),
                  "d": pa.array([datetime.date(1582, 10, 15), datetime.date(1582, 10, 10)], pa.date32())})
    pq.write_table(t, path)
    return 2


def make_blobs(path, sizes_mb):
    sizes = [0, None] + [int(float(s) * 1024 * 1024) for s in sizes_mb]
    writer = None
    for i, s in enumerate(sizes):
        t = pa.table({"id": pa.array([i], pa.int64()),
                      "label": pa.array(["size {}".format(s)]),
                      "payload": pa.array([None if s is None else os.urandom(s)], pa.binary())})
        if writer is None:
            writer = pq.ParquetWriter(path, t.schema)
        writer.write_table(t)          # one row group per row
    writer.close()
    return len(sizes)


# ---------------------------------------------------------------------------
# checksums
# ---------------------------------------------------------------------------

def h12(b):
    return int(hashlib.md5(b).hexdigest()[:12], 16)


def utf16_units(s):
    return len(s.encode("utf-16-le")) // 2


def utf16_prefix(s, units):
    return s.encode("utf-16-le")[:units * 2].decode("utf-16-le", "replace")


def ts_text(epoch_ns):
    """Proleptic Gregorian 'YYYY-MM-DD HH:MM:SS.fffffffff' (zero-padded year)."""
    sec, ns = divmod(epoch_ns, 10 ** 9)
    d = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=sec)
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:09d}".format(
        d.year, d.month, d.day, d.hour, d.minute, d.second, ns)


def checks_for(spec):
    """[(check name, Oracle aggregate expression)] for one column."""
    n = '"' + spec["name"] + '"'
    k = spec["kind"]
    # STANDARD_HASH(NULL) hashes the empty string, so NULLs are skipped explicitly
    hexsum = ("SUM(CASE WHEN ({0}) IS NOT NULL THEN TO_NUMBER(SUBSTR(RAWTOHEX("
              "STANDARD_HASH({0}, 'MD5')), 1, 12), 'XXXXXXXXXXXX') END)")
    out = [("cnt", "COUNT(CASE WHEN {} IS NOT NULL THEN 1 END)".format(n))]
    if k in ("int", "decimal", "bool", "duration"):
        out.append(("sum", "SUM({})".format(n)))
    elif k == "float":
        cast = "UTL_RAW.CAST_FROM_BINARY_FLOAT" if spec["otype"] == "BINARY_FLOAT" \
            else "UTL_RAW.CAST_FROM_BINARY_DOUBLE"
        out.append(("nan", "SUM(CASE WHEN {} IS NAN THEN 1 ELSE 0 END)".format(n)))
        out.append(("inf", "SUM(CASE WHEN {} IS INFINITE THEN 1 ELSE 0 END)".format(n)))
        out.append(("bits", hexsum.format("CASE WHEN {0} IS NOT NAN THEN {1}({0}) END".format(n, cast))))
    elif k in ("text", "time"):
        out.append(("bytes", "SUM(LENGTHB({}))".format(n)))
        out.append(("hash", hexsum.format(n)))
    elif k == "raw":
        out.append(("bytes", "SUM(UTL_RAW.LENGTH({}))".format(n)))
        out.append(("hash", hexsum.format(n)))
    elif k == "clob":
        out.append(("empty", "SUM(CASE WHEN DBMS_LOB.GETLENGTH({}) = 0 THEN 1 ELSE 0 END)".format(n)))
        out.append(("chars", "SUM(DBMS_LOB.GETLENGTH({}))".format(n)))
        out.append(("head", hexsum.format("DBMS_LOB.SUBSTR({}, 1000, 1)".format(n))))
    elif k == "blob":
        out.append(("empty", "SUM(CASE WHEN DBMS_LOB.GETLENGTH({}) = 0 THEN 1 ELSE 0 END)".format(n)))
        out.append(("bytes", "SUM(DBMS_LOB.GETLENGTH({}))".format(n)))
        out.append(("head", hexsum.format("DBMS_LOB.SUBSTR({}, 2000, 1)".format(n))))
        out.append(("tail", hexsum.format(
            "DBMS_LOB.SUBSTR({0}, 2000, GREATEST(1, DBMS_LOB.GETLENGTH({0}) - 1999))".format(n))))
    elif k == "timestamp":
        # text, not arithmetic: Oracle uses the Julian calendar before 1582-10-15
        out.append(("text", hexsum.format("TO_CHAR({}, 'YYYY-MM-DD HH24:MI:SS.FF9')".format(n))))
        out.append(("max", "TO_CHAR(MAX({}), 'YYYY-MM-DD HH24:MI:SS.FF9')".format(n)))
    elif k == "date":
        out.append(("text", hexsum.format("TO_CHAR({}, 'YYYY-MM-DD HH24:MI:SS')".format(n))))
        out.append(("max", "TO_CHAR(MAX({}), 'YYYY-MM-DD')".format(n)))
    return out


def expected_for(spec, arr):
    """Python-side values for checks_for(spec), from a full column array."""
    k = spec["kind"]
    res = {}
    if k == "timestamp":
        unit = arr.type.unit
        mult = {"s": 10 ** 9, "ms": 10 ** 6, "us": 10 ** 3, "ns": 1}[unit]
        vals = [v for v in pc.cast(arr, pa.int64()).to_pylist() if v is not None]
        texts = [ts_text(v * mult) for v in vals]
        res["cnt"] = len(vals)
        res["text"] = sum(h12(t.encode()) for t in texts)
        if vals:
            res["max"] = ts_text(max(vals) * mult)
        return res
    if k == "date":
        vals = [v for v in pc.cast(arr, pa.int32()).to_pylist() if v is not None]
        res["cnt"] = len(vals)
        res["text"] = sum(h12(ts_text(v * 86400 * 10 ** 9)[:19].encode()) for v in vals)
        if vals:
            res["max"] = ts_text(max(vals) * 86400 * 10 ** 9)[:10]
        return res
    if k == "time" or spec.get("json"):
        vals = pqora.to_text(arr, spec).to_pylist()
    else:
        vals = arr.to_pylist()
    if k in ("int", "duration"):
        if k == "duration":
            vals = pc.cast(arr, pa.int64()).to_pylist()
        vals = [v for v in vals if v is not None]
        res.update(cnt=len(vals), sum=sum(vals))
    elif k == "bool":
        vals = [int(v) for v in vals if v is not None]
        res.update(cnt=len(vals), sum=sum(vals))
    elif k == "decimal":
        vals = [v for v in vals if v is not None]
        res.update(cnt=len(vals), sum=sum(vals, decimal.Decimal(0)))
    elif k == "float":
        fmt = ">f" if spec["otype"] == "BINARY_FLOAT" else ">d"
        vals = [v for v in vals if v is not None]
        res["cnt"] = len(vals)
        res["nan"] = sum(1 for v in vals if v != v)
        res["inf"] = sum(1 for v in vals if v in (float("inf"), float("-inf")))
        # Oracle always stores -0.0 as +0.0
        res["bits"] = sum(h12(struct.pack(fmt, 0.0 if v == 0 else v)) for v in vals if v == v)
    elif k in ("text", "time"):
        vals = [v.encode("utf-8") for v in vals if v]           # Oracle: '' is NULL
        res.update(cnt=len(vals), bytes=sum(len(v) for v in vals), hash=sum(h12(v) for v in vals))
    elif k == "raw":
        vals = [bytes(v) for v in vals if v]                    # Oracle: empty RAW is NULL
        res.update(cnt=len(vals), bytes=sum(len(v) for v in vals), hash=sum(h12(v) for v in vals))
    elif k == "clob":
        vals = [v for v in vals if v is not None]
        res.update(cnt=len(vals), empty=sum(1 for v in vals if v == ""),
                   chars=sum(utf16_units(v) for v in vals),
                   head=sum(h12(utf16_prefix(v, 1000).encode("utf-8")) for v in vals if v))
    elif k == "blob":
        vals = [bytes(v) for v in vals if v is not None]
        res.update(cnt=len(vals), empty=sum(1 for v in vals if not v),
                   bytes=sum(len(v) for v in vals),
                   head=sum(h12(v[:2000]) for v in vals if v),
                   tail=sum(h12(v[-2000:]) for v in vals if v))
    elif k == "null":
        res["cnt"] = 0
    return res


def table_sources(part_file):
    """All Parquet sources of the table a part file belongs to."""
    part = pqora.read_part(part_file)
    d = os.path.dirname(os.path.abspath(part_file))
    srcs = []
    for f in pqora.table_part_files(d, part["table"]):
        p = pqora.read_part(f)
        if p["table"] == part["table"]:
            srcs.extend(p["sources"])
    return part, srcs


def cmd_expected(a):
    part, srcs = table_sources(a.part)
    main_specs = part["columns"]
    # --stage: checksums of <table> UNION ALL <table>_CLOB, described by the _CLOB columns
    specs = part["stage"]["columns"] if a.stage else main_specs
    acc = {}
    for spec in specs:
        acc[spec["name"]] = {}
    for src in srcs:
        pf = pq.ParquetFile(src["path"])
        top = sorted({s["source"][0] for s in specs})
        for rg in src["row_groups"]:
            table = pf.read_row_group(rg, columns=top)
            if table.num_rows == 0:
                continue
            small = None
            if a.stage:
                mask = pqora.large_mask(pqora.prepare(table, main_specs), main_specs)
                small = pc.invert(mask) if mask is not None else None
            for spec, main_spec in zip(specs, main_specs):
                arr = pqora.column_array(table, spec)
                if small is not None and main_spec["kind"] in ("text", "raw") and not spec.get("json"):
                    # rows that stayed in the main table hold '' / empty RAW as NULL
                    empty = pc.and_(small, pc.equal(pc.binary_length(arr), 0))
                    arr = pc.if_else(empty, pa.scalar(None, arr.type), arr)
                for key, v in expected_for(spec, arr).items():
                    cur = acc[spec["name"]].get(key)
                    if key == "max":
                        acc[spec["name"]][key] = v if cur is None or v > cur else cur
                    else:
                        acc[spec["name"]][key] = (cur or 0) + v
    out = {"table": part["table"], "rows": part_rows(a.part), "checks": {}}
    for spec in specs:
        for key, v in acc[spec["name"]].items():
            out["checks"]["{}.{}".format(spec["name"], key)] = str(v)
    json.dump(out, sys.stdout, indent=1, ensure_ascii=False)
    print()


def part_rows(part_file):
    part, _ = table_sources(part_file)
    d = os.path.dirname(os.path.abspath(part_file))
    total = 0
    for f in pqora.table_part_files(d, part["table"]):
        total += pqora.read_part(f)["rows"]
    return total


def cmd_sql(a):
    part = pqora.read_part(a.part)
    cols = [("ROWS.cnt", "COUNT(*)")]
    for spec in (part["stage"]["columns"] if a.stage else part["columns"]):
        for key, expr in checks_for(spec):
            cols.append(("{}.{}".format(spec["name"], key), expr))
    print("SELECT 'CHK|' || k || '|' || v FROM (")
    print("  SELECT")
    for i, (key, expr) in enumerate(cols):
        print("    TO_CHAR({}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''') AS c{}{}".format(
            expr, i, "," if i < len(cols) - 1 else "")
              if not expr.startswith("TO_CHAR(") else
              "    {} AS c{}{}".format(expr, i, "," if i < len(cols) - 1 else ""))
    hint = "/*+ PARALLEL({}) */ ".format(a.dop) if a.dop else ""
    print("  FROM (SELECT {}* FROM {})".format(hint, '"' + a.table.upper() + '"'))
    print(") UNPIVOT INCLUDE NULLS (v FOR k IN (")
    for i, (key, _) in enumerate(cols):
        print("    c{} AS '{}'{}".format(i, key.replace("'", "''"), "," if i < len(cols) - 1 else ""))
    print("));")


def cmd_stageview(a):
    """CREATE VIEW over <table> UNION ALL <table>_CLOB with LOB columns throughout."""
    part = pqora.read_part(a.part)
    main, stage = part["columns"], part["stage"]
    sel_main, sel_clob = [], []
    for m, c in zip(main, stage["columns"]):
        n = '"' + m["name"] + '"'
        if m["kind"] == "text" and c["kind"] == "clob":
            sel_main.append("TO_CLOB({}) AS {}".format(n, n))
        elif m["kind"] == "raw" and c["kind"] == "blob":
            sel_main.append("TO_BLOB({}) AS {}".format(n, n))
        else:
            sel_main.append(n)
        sel_clob.append(n)
    print('CREATE OR REPLACE VIEW "{}" AS'.format(a.view.upper()))
    print('SELECT {} FROM "{}"'.format(", ".join(sel_main), part["table"]))
    print("UNION ALL")
    print('SELECT {} FROM "{}";'.format(", ".join(sel_clob), stage["table"]))


def parse_spool(path):
    got = {}
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            if line.startswith("CHK|"):
                _, k, v = line.split("|", 2)
                got[k] = v.strip()
    return got


def same(exp, got):
    if exp == got:
        return True
    if got in ("", None):
        return exp in ("0", "None", "")
    try:
        e, g = decimal.Decimal(exp), decimal.Decimal(got)
    except (decimal.InvalidOperation, ValueError):
        return False
    # Oracle NUMBER keeps 38 significant digits: sums of 38-digit decimals are
    # rounded there, differently depending on the order rows are added
    return e == g or abs(e - g) <= abs(e) * decimal.Decimal("1e-36")


def cmd_compare(a):
    with open(a.expected, "r", encoding="utf-8") as fh:
        exp = json.load(fh)
    got = parse_spool(a.spool)
    label = a.label or exp["table"]
    if not got:
        print("FAIL  {}: no CHK lines in {} (query failed?)".format(label, a.spool))
        return 1
    bad = 0
    checks = dict(exp["checks"])
    checks["ROWS.cnt"] = str(exp["rows"])
    for key in sorted(checks):
        e = checks[key]
        g = got.get(key)
        if g is None or not same(e, g):
            bad += 1
            print("FAIL  {}: {:<40} expected {}  got {}".format(label, key, e[:60], (g or "<missing>")[:60]))
    print("{}  {}: {} checks, {} mismatches".format("PASS" if not bad else "FAIL", label, len(checks), bad))
    return 1 if bad else 0


def cmd_parallel(a):
    starts, ends = {}, []
    since = a.since or ""
    with open(a.log, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("table", "").upper() != a.table.upper() or r.get("ts", "") < since:
                continue
            key = (r["pid"], r["part"])
            if r["event"] == "start":
                starts[key] = r["ts"]
            elif key in starts:
                ends.append((starts.pop(key), r["ts"], r))
    events = []
    for s, e, _ in ends:
        events.append((s, 1))
        events.append((e, -1))
    cur = peak = 0
    for _, d in sorted(events):
        cur += d
        peak = max(peak, cur)
    status = {}
    for _, _, r in ends:
        status[r.get("status")] = status.get(r.get("status"), 0) + 1
    rows = sum(r.get("rows", 0) for _, _, r in ends)
    print("parallel {}: part runs={} distinct processes={} max concurrent={} rows={:,} status={}".format(
        a.table, len(ends), len({r["pid"] for _, _, r in ends}), peak, rows, status))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    m = sub.add_parser("make")
    m.add_argument("dir")
    m.add_argument("--big-rows", type=int, default=2000000)
    m.add_argument("--blob-mb", default="1,10,50,100")
    e = sub.add_parser("expected")
    e.add_argument("part")
    e.add_argument("--stage", action="store_true", help="for <table> UNION ALL <table>_CLOB")
    s = sub.add_parser("sql")
    s.add_argument("part")
    s.add_argument("table")
    s.add_argument("--dop", type=int, default=0)
    s.add_argument("--stage", action="store_true", help="checks with the _CLOB table's columns")
    v = sub.add_parser("stageview")
    v.add_argument("part")
    v.add_argument("view")
    c = sub.add_parser("compare")
    c.add_argument("expected")
    c.add_argument("spool")
    c.add_argument("--label")
    p = sub.add_parser("parallel")
    p.add_argument("log")
    p.add_argument("table")
    p.add_argument("--since")
    a = ap.parse_args()
    if a.cmd == "make":
        os.makedirs(a.dir, exist_ok=True)
        print("types.parquet: {} rows".format(make_types(os.path.join(a.dir, "types.parquet"))))
        print("big.parquet  : {} rows".format(make_big(os.path.join(a.dir, "big.parquet"), a.big_rows)))
        print("gap.parquet  : {} rows".format(make_gap(os.path.join(a.dir, "gap.parquet"))))
        print("blobs.parquet: {} rows".format(make_blobs(os.path.join(a.dir, "blobs.parquet"),
                                                          [x for x in a.blob_mb.split(",") if x])))
        return 0
    if a.cmd == "expected":
        return cmd_expected(a) or 0
    if a.cmd == "sql":
        return cmd_sql(a) or 0
    if a.cmd == "stageview":
        return cmd_stageview(a) or 0
    if a.cmd == "compare":
        return cmd_compare(a)
    if a.cmd == "parallel":
        return cmd_parallel(a)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
