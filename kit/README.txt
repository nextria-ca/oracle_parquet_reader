pqora test kit
==============

Runs every pqora experiment against an Oracle database in one go and packs
the results into a single report file. Nothing needs to be edited.

Requirements on the database server
  - python3 with pyarrow (6.0.1+) and numpy; Python 3.6 is fine
  - sqlplus on PATH
  - run as an OS user whose files the database OS user can read
    (simplest: run as the database OS user, e.g. oracle)
  - about 2 GB free disk in the work directory (default BIG_ROWS/BLOB_MB)

1. Unpack

     tar xzf pqora_kit_*.tar.gz
     cd pqora_kit

2. Directory objects

   The kit uses three directory objects (default names PQORA_EXEC, PQORA_PARTS,
   PQORA_LOG) pointing at <kit>/work/exec, <kit>/work/parts and <kit>/work/log.
   Either let the kit create them (CREATE_DIRS=1, needs CREATE ANY DIRECTORY),
   or have a DBA run (replace <kit> and <user>):

     CREATE OR REPLACE DIRECTORY PQORA_EXEC  AS '<kit>/work/exec';
     CREATE OR REPLACE DIRECTORY PQORA_PARTS AS '<kit>/work/parts';
     CREATE OR REPLACE DIRECTORY PQORA_LOG   AS '<kit>/work/log';
     GRANT READ, EXECUTE ON DIRECTORY PQORA_EXEC  TO <user>;
     GRANT READ          ON DIRECTORY PQORA_PARTS TO <user>;
     GRANT READ, WRITE   ON DIRECTORY PQORA_LOG   TO <user>;

   If the directory objects are missing, the kit stops early and prints
   exactly these statements.

3. Run

     DB_CONNECT='user/password@//host:1521/service' ./run_kit.sh

   or, as the oracle OS user with a PDB:

     DB_CONNECT='/ as sysdba' DB_PDB=MYPDB CREATE_DIRS=1 ./run_kit.sh
     (tables are then created in the SYS schema of that PDB; a normal
      schema is preferable)

   Optional: PYTHON=/path/to/python3  WORK=/other/dir  BIG_ROWS=500000
             BLOB_MB=1,10  DOPS="1 4 8 16"  KEEP=1 (keep test tables)

4. Send back

     <kit>/work/pqora_report_<host>_<time>.tar.gz

   It contains the summary, all sqlplus output, ORACLE_LOADER logs, the
   preprocessor log and the generated DDL. It does not contain the password.

What it tests
  - test data with every supported type and the nasty cases (newlines,
    control characters, NUL bytes, emoji, NaN/Inf, nanosecond and
    time-zone timestamps, decimal(38), NULL vs empty LOBs, nested structs,
    lists/maps, dates 0001-01-01 .. 9999-12-31)
  - inspect, generate, offline framing verification, the wrapper with an
    empty environment (as Oracle runs it)
  - CREATE external table + parallel direct-path INSERT for every test table
  - per-column checksums (counts, sums, MD5 sums, bit patterns of floats)
    of the external tables and of the loaded tables, compared with values
    computed by pyarrow; once more with German NLS session settings
  - --stage-large-records: main table + <table>_CLOB, checked exactly
  - dates: the statistics-based check, --out-of-range-dates null and clamp
    (exact values), interval columns (years 0000-0001) loaded as numbers with
    and without Parquet statistics, --column-type text
  - parallel execution (how many preprocessors ran at once) and scan
    timings at several DOPs
  - large BLOBs (READSIZE), ROWNUM early stop, a changed Parquet file and a
    date in the 1582 calendar gap (both must fail with a clear message)
  - whether COMPRESS FOR QUERY HIGH works on this storage (INFO only)
  - the manifest and sampling-cache queries at the end of every <table>.sql

Locally (Oracle 23ai) the kit reports 39 passed checks.
