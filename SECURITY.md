# Security policy

## Scope

pqora runs on the database server as the Oracle preprocessor. The parts of it
that matter most for security are:

- **Stream mode** (`pqora.py <part>.pqpart`), which Oracle runs as the
  database OS user with an empty environment. It reads the Parquet files and
  the part file named in the location file and writes to stdout, the stream
  log and, in staged mode, the large-record files in the log directory.
- **The generated wrapper** `pqora.sh` and the generated SQL, which a DBA
  reviews and runs.
- **The test kit**, which connects to a database with credentials from the
  `DB_CONNECT` environment variable. The kit writes the connection string to a
  mode-600 file for SQL*Plus and excludes it from the report tarball.

pqora never opens network connections, never executes anything it reads from
a Parquet file, and never writes outside the directories named at `generate`
time.

## Supported versions

Security fixes are made on the latest release only. See
[CHANGELOG.md](CHANGELOG.md) for the current version.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately through GitHub's private vulnerability reporting on this
repository ("Security" tab, then "Report a vulnerability"). Include the pqora
version, the Oracle and pyarrow versions, and the smallest reproduction you
can share. A Parquet file that triggers the problem is ideal, provided it
contains no confidential data.

You will get an acknowledgement within five business days. We will work with
you on a fix and a coordinated disclosure date, and credit you in the release
notes unless you prefer otherwise.

## Hardening advice for operators

- Grant `EXECUTE` on the directory object that holds `pqora.sh` only to the
  schemas that load data. A preprocessor directory is effectively a way to run
  programs as the database OS user.
- Keep `pqora.py`, `pqora.sh` and the parts directory writable only by the
  account that runs `generate`, and readable by the database OS user.
- Review the generated `<table>.sql` before running it, as you would any DDL.
- Do not put credentials in scripts; the kit reads `DB_CONNECT` from the
  environment for this reason.
