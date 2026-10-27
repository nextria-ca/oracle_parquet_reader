#!/bin/bash
# pqora test kit - runs every experiment unattended and packs one report.
#
#   DB_CONNECT='user/password@//host:1521/service' ./run_kit.sh
#
# Settings (environment variables):
#   DB_CONNECT    required; anything sqlplus CONNECT accepts ('/ as sysdba' too)
#   DB_PDB        optional; ALTER SESSION SET CONTAINER after connecting
#   PYTHON        interpreter with pyarrow + numpy (default: python3 on PATH)
#   WORK          work directory (default: <kit>/work); the database OS user
#                 must be able to read it
#   EXEC_OBJ PARTS_OBJ LOG_OBJ
#                 directory object names (default PQORA_EXEC PQORA_PARTS PQORA_LOG)
#                 pointing at $WORK/exec, $WORK/parts, $WORK/log
#   CREATE_DIRS=1 create the directory objects (needs CREATE ANY DIRECTORY)
#   BIG_ROWS      rows in the throughput/parallel test table (default 2000000)
#   BLOB_MB       BLOB sizes in MB for the large-value test (default 1,10,50,100)
#   DOPS          degrees of parallelism for the timing test (default "1 4 8")
#   SQL_TIMEOUT   seconds per sqlplus call (default 1800)
#   KEEP=1        keep the test tables
#
# Result: <WORK>/pqora_report_<host>_<time>.tar.gz  (send this file back)

KIT="$(cd "$(dirname "$0")" && pwd)"
WORK="${WORK:-$KIT/work}"
EXEC_OBJ="${EXEC_OBJ:-PQORA_EXEC}"
PARTS_OBJ="${PARTS_OBJ:-PQORA_PARTS}"
LOG_OBJ="${LOG_OBJ:-PQORA_LOG}"
BIG_ROWS="${BIG_ROWS:-2000000}"
BLOB_MB="${BLOB_MB:-1,10,50,100}"
DOPS="${DOPS:-1 4 8}"
SQL_TIMEOUT="${SQL_TIMEOUT:-1800}"
STAMP="$(date +%Y%m%d_%H%M%S)"
REP="$WORK/report"
SUMMARY="$REP/summary.txt"
TABLES="PQK_TYPES PQK_BIG PQK_BLOBS PQK_GAP PQK_GAPNULL PQK_GAPCLAMP PQK_ERR PQK_STAGE PQK_STAGE_CLOB PQK_INTERVAL PQK_INTNOSTATS PQK_INTTEXT"

mkdir -p "$WORK"/{data,parts,exec,log,sql} "$REP" || exit 1
chmod 755 "$WORK" "$WORK"/data "$WORK"/parts "$WORK"/exec
chmod 1777 "$WORK"/log
: > "$SUMMARY"

say()    { echo "$*" | tee -a "$SUMMARY"; }
result() { say "$(printf '%-5s %s' "$1" "$2")"; }
step()   { echo; echo "=== $*"; echo "=== $*" >> "$REP/steps.log"; }
now()    { date +%Y-%m-%dT%H:%M:%S; }
# real errors in a sqlplus output file (ignores password-expiry warnings at CONNECT)
errors() { grep -E 'ORA-|KUP-|SP2-' "$1" | grep -vE 'ORA-280(02|11|98)'; }

finish() {
    step "packing report"
    cp "$WORK"/log/*.log "$WORK"/log/*.bad "$REP"/ 2>/dev/null
    cp "$WORK"/parts/*.sql "$WORK"/parts/*_manifest.json "$REP"/ 2>/dev/null
    cp "$WORK"/sql/*.sql "$REP"/ 2>/dev/null
    rm -f "$REP"/_connect.sql
    cp "$WORK"/exec/pqora.sh "$REP"/ 2>/dev/null
    local tgz="$WORK/pqora_report_$(hostname -s)_$STAMP.tar.gz"
    tar czf "$tgz" -C "$WORK" report
    echo
    echo "============================================================"
    cat "$SUMMARY"
    echo "============================================================"
    echo "Report: $tgz"
    exit "${1:-0}"
}

# ---------------------------------------------------------------- 1. environment
step "environment"
PY="$(command -v "${PYTHON:-python3}")"
{
    echo "date      : $(date)"
    echo "host      : $(hostname)"
    echo "user      : $(id)"
    echo "os        : $(cat /etc/oracle-release 2>/dev/null || cat /etc/redhat-release 2>/dev/null)"
    echo "kernel    : $(uname -r)"
    echo "glibc     : $(ldd --version 2>&1 | head -1)"
    echo "python    : $PY"
    "$PY" -c 'import sys, pyarrow, numpy; print("versions  : python", sys.version.split()[0], "pyarrow", pyarrow.__version__, "numpy", numpy.__version__); print("pyarrow at:", pyarrow.__file__)' 2>&1
    echo "sqlplus   : $(command -v sqlplus) $(sqlplus -V 2>/dev/null | grep -i release)"
    echo "kit       : $KIT"
    echo "work      : $WORK"
    echo "settings  : BIG_ROWS=$BIG_ROWS BLOB_MB=$BLOB_MB DOPS='$DOPS' DB_PDB=${DB_PDB:-}"
} 2>&1 | tee "$REP/env.txt"

if [ -z "$PY" ] || ! "$PY" -c 'import pyarrow, numpy' 2>>"$REP/env.txt"; then
    result FAIL "python/pyarrow not usable (set PYTHON=...)"; finish 1
fi
if ! command -v sqlplus >/dev/null; then
    result FAIL "sqlplus not on PATH"; finish 1
fi
if [ -z "$DB_CONNECT" ]; then
    result FAIL "DB_CONNECT is not set"; finish 1
fi
result INFO "$(grep '^versions' "$REP/env.txt")"

# connect file (kept out of the report and out of ps output)
umask 077
{
    echo "WHENEVER SQLERROR EXIT FAILURE"
    echo "WHENEVER OSERROR EXIT FAILURE"
    echo "CONNECT $DB_CONNECT"
    [ -n "$DB_PDB" ] && echo "ALTER SESSION SET CONTAINER = $DB_PDB;"
    echo "WHENEVER SQLERROR CONTINUE"
    echo "WHENEVER OSERROR CONTINUE"
} > "$WORK/sql/_connect.sql"
umask 022

# run_sql NAME  (reads SQL from stdin, output to $REP/NAME.out)
run_sql() {
    local name="$1" f="$WORK/sql/$1.sql"
    {
        echo "@@_connect.sql"
        echo "SET ECHO ON FEEDBACK ON HEADING OFF PAGESIZE 0 LINESIZE 32767 LONG 2000000"
        echo "SET TRIMSPOOL ON TRIMOUT ON TAB OFF DEFINE OFF SQLBLANKLINES ON TIMING ON"
        cat
        echo "EXIT"
    } > "$f"
    ( cd "$WORK/sql" && timeout "$SQL_TIMEOUT" sqlplus -S -L /nolog @"$f" ) \
        > "$REP/$name.out" 2>&1 < /dev/null
    local rc=$?
    [ $rc -eq 124 ] && echo "*** TIMEOUT after ${SQL_TIMEOUT}s" >> "$REP/$name.out"
    return $rc
}
sqlplus_connect_check() {
    echo "SELECT 'PING|' || 1 FROM dual;" | run_sql connect && grep -q '^PING|1' "$REP/connect.out"
}

# ---------------------------------------------------------------- 2. test data
step "test data"
T0=$SECONDS
if "$PY" "$KIT/pqora_selftest.py" make "$WORK/data" --big-rows "$BIG_ROWS" --blob-mb "$BLOB_MB" \
        > "$REP/make_data.out" 2>&1; then
    chmod 644 "$WORK"/data/*.parquet
    result PASS "test data generated in $((SECONDS - T0))s"
else
    result FAIL "test data generation (see make_data.out)"; finish 1
fi

# ---------------------------------------------------------------- 3. inspect
step "inspect"
for t in types big blobs; do
    if "$PY" "$KIT/pqora.py" "$WORK/data/$t.parquet" > "$REP/inspect_$t.txt" 2>&1; then
        result PASS "inspect $t.parquet"
    else
        result FAIL "inspect $t.parquet (see inspect_$t.txt)"
    fi
done

# ---------------------------------------------------------------- 4. generate
step "generate"
rm -f "$WORK"/parts/* "$WORK"/log/*
gen() {   # gen TABLE FILE [extra args]
    local t="$1" f="$2"; shift 2
    "$PY" "$KIT/pqora.py" generate "$WORK/data/$f" --table "$t" --out-dir "$WORK/parts" \
        --exec-dir "$WORK/exec" --log-dir "$WORK/log" --exec-dir-object "$EXEC_OBJ" \
        --parts-dir-object "$PARTS_OBJ" --log-dir-object "$LOG_OBJ" --python "$PY" \
        --script "$KIT/pqora.py" --sample-size all "$@" >> "$REP/generate.out" 2>&1
}
for spec in "PQK_TYPES types.parquet" "PQK_BIG big.parquet" "PQK_BLOBS blobs.parquet" \
            "PQK_GAP gap.parquet" "PQK_ERR blobs.parquet"; do
    set -- $spec
    if gen "$1" "$2" --dop 8; then result PASS "generate $1"; else result FAIL "generate $1 (see generate.out)"; fi
done
for mode in null clamp; do
    t="PQK_GAP$(echo $mode | tr 'a-z' 'A-Z')"
    if gen "$t" gap.parquet --dop 2 --out-of-range-dates $mode; then result PASS "generate $t (--out-of-range-dates $mode)"
    else result FAIL "generate $t (see generate.out)"; fi
done
# date/timestamp columns holding intervals (years 0000-0001): auto NUMBER, with and without statistics,
# and --column-type text
gen PQK_INTERVAL interval.parquet --dop 2 && gen PQK_INTNOSTATS interval_nostats.parquet --dop 2 \
    && gen PQK_INTTEXT interval.parquet --dop 2 --column-type wait=text --column-type elapsed=text \
    && result PASS "generate PQK_INTERVAL / PQK_INTNOSTATS / PQK_INTTEXT" \
    || result FAIL "generate interval tables (see generate.out)"
grep -E "^Dates|^Retyped|WARNING: .*min/max" "$REP/generate.out" | sort -u | sed 's/^/INFO  /' | tee -a "$SUMMARY"
if gen PQK_STAGE types.parquet --dop 4 --stage-large-records; then
    result PASS "generate PQK_STAGE (--stage-large-records)"
else
    result FAIL "generate PQK_STAGE --stage-large-records (see generate.out)"
fi
# PQK_ERR: make every part claim a different file size -> stream mode must refuse
"$PY" - "$WORK/parts" <<'EOF' >> "$REP/generate.out" 2>&1
import glob, json, os, sys
for f in glob.glob(os.path.join(sys.argv[1], "pqk_err_*.pqpart")):
    p = json.load(open(f))
    for s in p["sources"]:
        s["size"] += 1
    json.dump(p, open(f, "w"), indent=1)
EOF
chmod 644 "$WORK"/parts/*
cat "$WORK/exec/pqora.sh" >> "$REP/generate.out"

# ---------------------------------------------------------------- 5. offline checks
step "offline verify"
if "$PY" "$KIT/pqora.py" verify "$WORK"/parts/pqk_types_*.pqpart "$WORK"/parts/pqk_big_*.pqpart \
        "$WORK"/parts/pqk_blobs_*.pqpart "$WORK"/parts/pqk_stage_*.pqpart > "$REP/verify.out" 2>&1; then
    result PASS "verify: framing of all parts ($(grep -c ': OK' "$REP/verify.out") parts)"
else
    result FAIL "verify (see verify.out)"
fi
# run exactly like Oracle would: empty environment, wrapper script, one argument
P1="$(ls "$WORK"/parts/pqk_types_*.pqpart | head -1)"
BYTES="$(env -i /bin/sh "$WORK/exec/pqora.sh" "$P1" 2>"$REP/wrapper_stderr.out" | wc -c)"
EXP="$(grep "$(basename "$P1")" "$REP/verify.out" | sed -n 's/.*bytes=\([0-9,]*\).*/\1/p' | tr -d ,)"
if [ -n "$BYTES" ] && [ "$BYTES" = "$EXP" ]; then
    result PASS "wrapper with empty environment produced $BYTES bytes (as expected)"
else
    result FAIL "wrapper with empty environment: got '$BYTES' bytes, expected '$EXP' (see wrapper_stderr.out)"
fi

# ---------------------------------------------------------------- 6. database setup
step "database"
if ! sqlplus_connect_check; then
    result FAIL "cannot connect (see connect.out)"; finish 1
fi
if [ "$CREATE_DIRS" = "1" ]; then
    run_sql create_dirs <<EOF
CREATE OR REPLACE DIRECTORY $EXEC_OBJ AS '$WORK/exec';
CREATE OR REPLACE DIRECTORY $PARTS_OBJ AS '$WORK/parts';
CREATE OR REPLACE DIRECTORY $LOG_OBJ AS '$WORK/log';
EOF
fi
run_sql db_info <<EOF
SELECT 'VERSION|' || banner_full FROM v\$version;
SELECT 'VERSION|' || banner FROM v\$version;
SELECT 'NLS|' || parameter || '=' || value FROM nls_database_parameters
 WHERE parameter IN ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET','NLS_NUMERIC_CHARACTERS','NLS_LENGTH_SEMANTICS');
SELECT 'PARAM|' || name || '=' || value FROM v\$parameter
 WHERE name IN ('max_string_size','parallel_max_servers','cpu_count','compatible','db_block_size');
SELECT 'USER|' || USER || ' CON=' || SYS_CONTEXT('USERENV','CON_NAME') FROM dual;
SELECT 'DIR|' || directory_name || '=' || directory_path FROM all_directories
 WHERE directory_name IN ('$EXEC_OBJ','$PARTS_OBJ','$LOG_OBJ');
EOF
grep -E '^(VERSION|NLS|PARAM|USER)\|' "$REP/db_info.out" | awk '!seen[$0]++' | sed 's/^/INFO  /' | tee -a "$SUMMARY"
DIRS_OK=1
for pair in "$EXEC_OBJ=$WORK/exec" "$PARTS_OBJ=$WORK/parts" "$LOG_OBJ=$WORK/log"; do
    if ! grep -qx "DIR|$pair" "$REP/db_info.out"; then DIRS_OK=0; fi
done
if [ $DIRS_OK = 1 ]; then
    result PASS "directory objects point at the work directories"
else
    result FAIL "directory objects missing or pointing elsewhere; as a DBA run:"
    {
        echo "  CREATE OR REPLACE DIRECTORY $EXEC_OBJ  AS '$WORK/exec';"
        echo "  CREATE OR REPLACE DIRECTORY $PARTS_OBJ AS '$WORK/parts';"
        echo "  CREATE OR REPLACE DIRECTORY $LOG_OBJ   AS '$WORK/log';"
        echo "  GRANT READ, EXECUTE ON DIRECTORY $EXEC_OBJ  TO <user>;"
        echo "  GRANT READ          ON DIRECTORY $PARTS_OBJ TO <user>;"
        echo "  GRANT READ, WRITE   ON DIRECTORY $LOG_OBJ   TO <user>;"
        echo "  (or rerun with CREATE_DIRS=1 as a user that may create directories)"
    } | tee -a "$SUMMARY"
    finish 1
fi

drop_tables() {
    for t in $TABLES; do
        echo "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"EXT_${t}\" PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        echo "/"
        echo "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"${t}\" PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        echo "/"
        echo "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"${t}_STG\" PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        echo "/"
    done
}
drop_tables | run_sql drop_before

# ---------------------------------------------------------------- 7. DDL + load
step "create and load"
for t in PQK_TYPES PQK_BIG PQK_BLOBS; do
    lt="$(echo "$t" | tr 'A-Z' 'a-z')"
    grep -v -e '_STG" COMPRESS FOR QUERY HIGH' -e 'ENABLE RESUMABLE' "$WORK/parts/$lt.sql" | run_sql "load_$lt"
    out="$REP/load_$lt.out"
    loaded="$(grep -E '^ *[0-9]+ +[0-9]+ *$' "$out" | tail -1 | awk '{print $1"/"$2}')"
    el="$(awk '/rows? created\./{f=1} f && /Elapsed/{print $2; exit}' "$out")"
    if errors "$out" >/dev/null; then
        result FAIL "create+load $t: $(errors "$out" | head -1)"
    elif [ -n "$loaded" ] && [ "${loaded%/*}" = "${loaded#*/}" ]; then
        result PASS "create+load $t: $loaded rows (insert elapsed ${el:-?})"
    else
        result FAIL "create+load $t: loaded/expected = ${loaded:-?} (see load_$lt.out)"
    fi
done

# --stage-large-records: main table without LOBs + large records via EXT_PQK_STAGE_CLOB
grep -v -e '_STG" COMPRESS FOR QUERY HIGH' -e 'ENABLE RESUMABLE' "$WORK/parts/pqk_stage.sql" | run_sql load_pqk_stage
out="$REP/load_pqk_stage.out"
counts="$(grep -E '^ *[0-9]+ +[0-9]+ +[0-9]+ *$' "$out" | tail -1)"
set -- $counts
if errors "$out" >/dev/null; then
    result FAIL "create+load PQK_STAGE: $(errors "$out" | head -1)"
elif [ -n "$counts" ] && [ $(( $1 + $2 )) = "$3" ] && [ "$2" -gt 0 ]; then
    result PASS "create+load PQK_STAGE: $1 rows + $2 large rows in PQK_STAGE_CLOB = $3"
else
    result FAIL "create+load PQK_STAGE: main/large/expected = '${counts:-?}' (see load_pqk_stage.out)"
fi

# the one-line compressed staging CTAS at the top of <table>.sql
grep '_STG" COMPRESS FOR QUERY HIGH' "$WORK/parts/pqk_big.sql" | run_sql ctas_stg
out="$REP/ctas_stg.out"
if grep -q 'Table created' "$out"; then
    result INFO "CTAS PQK_BIG_STG COMPRESS FOR QUERY HIGH: ok (elapsed $(grep Elapsed "$out" | tail -1 | awk '{print $2}'))"
else
    result INFO "CTAS PQK_BIG_STG COMPRESS FOR QUERY HIGH not possible here: $(errors "$out" | head -1)"
fi

for t in PQK_INTERVAL PQK_INTNOSTATS PQK_INTTEXT; do
    lt="$(echo "$t" | tr 'A-Z' 'a-z')"
    grep -v -e '_STG" COMPRESS FOR QUERY HIGH' -e 'ENABLE RESUMABLE' "$WORK/parts/$lt.sql" | run_sql "load_$lt"
    if errors "$REP/load_$lt.out" >/dev/null; then
        result FAIL "create+load $t: $(errors "$REP/load_$lt.out" | head -1)"
    else
        result PASS "create+load $t"
    fi
done

# error-test tables: the external tables must exist; their INSERT is expected to fail
for t in PQK_GAP PQK_ERR PQK_GAPNULL PQK_GAPCLAMP; do
    lt="$(echo "$t" | tr 'A-Z' 'a-z')"
    grep -v -e '_STG" COMPRESS FOR QUERY HIGH' -e 'ENABLE RESUMABLE' "$WORK/parts/$lt.sql" | run_sql "create_$lt"
done

# ---------------------------------------------------------------- 8. data checks
check() {   # check LABEL PART_TABLE ORACLE_TABLE DOP [session statements...]
    local label="$1" pt="$2" ot="$3" dop="$4"; shift 4
    local part
    part="$(ls "$WORK"/parts/"$(echo "$pt" | tr 'A-Z' 'a-z')"_*.pqpart | head -1)"
    [ -f "$REP/$pt.expected.json" ] || "$PY" "$KIT/pqora_selftest.py" expected "$part" \
        > "$REP/$pt.expected.json" 2>> "$REP/selftest.err"
    {
        for s in "$@"; do echo "$s"; done
        "$PY" "$KIT/pqora_selftest.py" sql "$part" "$ot" --dop "$dop"
    } | run_sql "check_$label"
    if "$PY" "$KIT/pqora_selftest.py" compare "$REP/$pt.expected.json" "$REP/check_$label.out" \
            --label "$label" > "$REP/compare_$label.txt" 2>&1; then
        result PASS "$(tail -1 "$REP/compare_$label.txt" | cut -c7-)"
    else
        result FAIL "$(tail -1 "$REP/compare_$label.txt" | cut -c7-)  (see compare_$label.txt)"
    fi
}
step "data checks"
SINCE="$(now)"
check types_ext PQK_TYPES EXT_PQK_TYPES 4
check types_loaded PQK_TYPES PQK_TYPES 4
check big_ext PQK_BIG EXT_PQK_BIG 8
check big_loaded PQK_BIG PQK_BIG 8
check blobs_ext PQK_BLOBS EXT_PQK_BLOBS 4
check blobs_loaded PQK_BLOBS PQK_BLOBS 4
check types_ext_nls PQK_TYPES EXT_PQK_TYPES 4 \
    "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ',.';" \
    "ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.RR';" \
    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD.MM.RR HH24.MI.SSXFF';" \
    "ALTER SESSION SET NLS_TERRITORY = 'GERMANY';"
check interval PQK_INTERVAL PQK_INTERVAL 2
check interval_nostats PQK_INTNOSTATS PQK_INTNOSTATS 2
check interval_text PQK_INTTEXT PQK_INTTEXT 2

# staged table: PQK_STAGE UNION ALL PQK_STAGE_CLOB must equal the source exactly
SP="$(ls "$WORK"/parts/pqk_stage_*.pqpart | head -1)"
"$PY" "$KIT/pqora_selftest.py" stageview "$SP" PQK_STAGE_ALL | run_sql stage_view
"$PY" "$KIT/pqora_selftest.py" expected --stage "$SP" > "$REP/PQK_STAGE.expected.json" 2>> "$REP/selftest.err"
"$PY" "$KIT/pqora_selftest.py" sql --stage "$SP" PQK_STAGE_ALL --dop 4 | run_sql check_stage_all
if "$PY" "$KIT/pqora_selftest.py" compare "$REP/PQK_STAGE.expected.json" "$REP/check_stage_all.out" \
        --label stage_main+clob > "$REP/compare_stage_all.txt" 2>&1; then
    result PASS "$(tail -1 "$REP/compare_stage_all.txt" | cut -c7-)"
else
    result FAIL "$(tail -1 "$REP/compare_stage_all.txt" | cut -c7-)  (see compare_stage_all.txt)"
fi

# ---------------------------------------------------------------- 9. parallelism
step "parallelism"
for t in PQK_TYPES PQK_BIG; do
    lt="$(echo "$t" | tr 'A-Z' 'a-z')"
    if [ -f "$WORK/log/${lt}_stream.log" ]; then
        result INFO "$("$PY" "$KIT/pqora_selftest.py" parallel "$WORK/log/${lt}_stream.log" "$t" --since "$SINCE")"
    else
        result FAIL "no stream log for $t (preprocessor could not write $WORK/log?)"
    fi
done
for d in $DOPS; do
    s="$(now)"
    run_sql "perf_dop$d" <<EOF
SELECT /*+ PARALLEL($d) */ 'PERF|' || COUNT(*) || '|' || SUM(LENGTHB("NAME")) FROM "EXT_PQK_BIG";
EOF
    el="$(grep 'Elapsed' "$REP/perf_dop$d.out" | tail -1 | awk '{print $2}')"
    conc="$("$PY" "$KIT/pqora_selftest.py" parallel "$WORK/log/pqk_big_stream.log" PQK_BIG --since "$s" | sed 's/.*max concurrent=\([0-9]*\).*/\1/')"
    if grep -q '^PERF|' "$REP/perf_dop$d.out"; then
        result INFO "scan EXT_PQK_BIG ($BIG_ROWS rows) at DOP $d: elapsed $el, max concurrent preprocessors $conc"
    else
        result FAIL "scan at DOP $d: $(errors "$REP/perf_dop$d.out" | head -1)"
    fi
done

# ---------------------------------------------------------------- 10. edge cases
step "edge cases"
s="$(now)"
run_sql rownum <<EOF
SELECT 'ROWNUM|' || COUNT(*) FROM (SELECT * FROM "EXT_PQK_BIG" WHERE ROWNUM <= 10);
EOF
el="$(grep 'Elapsed' "$REP/rownum.out" | tail -1 | awk '{print $2}')"
if grep -q '^ROWNUM|10' "$REP/rownum.out"; then
    result PASS "ROWNUM <= 10 stops early (elapsed $el): $("$PY" "$KIT/pqora_selftest.py" parallel "$WORK/log/pqk_big_stream.log" PQK_BIG --since "$s" | sed 's/.*status=//')"
else
    result FAIL "ROWNUM query: $(errors "$REP/rownum.out" | head -1)$(grep -m1 TIMEOUT "$REP/rownum.out")"
fi

run_sql error_changed_file <<EOF
SELECT 'ERRTEST|' || COUNT(*) FROM "EXT_PQK_ERR";
EOF
if grep -q 'has changed' "$REP/error_changed_file.out"; then
    result PASS "changed Parquet file is refused; Oracle shows: $(grep -m1 -o 'pqora[^"]*has changed[^"]*' "$REP/error_changed_file.out" | cut -c1-120)"
elif errors "$REP/error_changed_file.out" >/dev/null; then
    result FAIL "changed file: error raised but pqora message not visible: $(errors "$REP/error_changed_file.out" | head -1)"
else
    result FAIL "changed file was NOT refused (see error_changed_file.out)"
fi

run_sql error_calendar_gap <<EOF
SELECT 'GAPTEST|' || COUNT(*) FROM "EXT_PQK_GAP";
EOF
if grep -q '1582-10-05' "$REP/error_calendar_gap.out"; then
    result PASS "date in the 1582 calendar gap is refused"
else
    result FAIL "calendar gap date was not refused (see error_calendar_gap.out)"
fi

# --out-of-range-dates null / clamp: exact values (rows: valid, 1582 gap, year 10000, year 0, NULL)
exp_null="1582-10-15/1582-10-15 12:34:56.000000,NULL/NULL,NULL/NULL,NULL/NULL,NULL/NULL"
exp_clamp="1582-10-15/1582-10-15 12:34:56.000000,1582-10-15/1582-10-15 12:34:56.000000,9999-12-31/9999-12-31 23:59:59.999999,0001-01-01/0001-01-01 00:00:00.000000,NULL/NULL"
for mode in null clamp; do
    t="PQK_GAP$(echo $mode | tr 'a-z' 'A-Z')"
    printf '%s\n' "SELECT 'DATES|' || LISTAGG(NVL(TO_CHAR(\"D\", 'YYYY-MM-DD'), 'NULL') || '/' ||" \
        "  NVL(TO_CHAR(\"TS\", 'YYYY-MM-DD HH24:MI:SS.FF6'), 'NULL'), ',') WITHIN GROUP (ORDER BY \"ID\")" \
        "FROM \"EXT_$t\";" | run_sql "dates_$mode"
    got="$(sed -n 's/^DATES|//p' "$REP/dates_$mode.out" | tail -1)"
    if [ "$mode" = null ]; then exp="$exp_null"; else exp="$exp_clamp"; fi
    if [ "$got" = "$exp" ]; then
        result PASS "--out-of-range-dates $mode: values as expected"
    else
        result FAIL "--out-of-range-dates $mode: got '$got' expected '$exp' $(errors "$REP/dates_$mode.out" | head -1)"
    fi
done

# ---------------------------------------------------------------- 11. cleanup
if [ "$KEEP" != "1" ]; then
    { drop_tables; echo "BEGIN EXECUTE IMMEDIATE 'DROP VIEW \"PQK_STAGE_ALL\"'; EXCEPTION WHEN OTHERS THEN NULL; END;"; echo "/"; } \
        | run_sql drop_after
fi
FAILS=$(grep -c '^FAIL' "$SUMMARY")
say ""
say "$(grep -c '^PASS' "$SUMMARY") passed, $FAILS failed"
finish $([ "$FAILS" = 0 ] && echo 0 || echo 1)
