# Contributing to pqora

Thank you for considering a contribution. pqora is small on purpose, runs in
an unusual place (inside Oracle's external table machinery, as the database
OS user, with an empty environment) and has to keep working on old enterprise
Linux. This page explains what that means for changes.

- [Ways to help](#ways-to-help)
- [Hard constraints](#hard-constraints)
- [Development setup](#development-setup)
- [Checks every change must pass](#checks-every-change-must-pass)
- [Testing against a database](#testing-against-a-database)
- [Pull requests](#pull-requests)
- [Versioning and releases](#versioning-and-releases)
- [License of contributions](#license-of-contributions)

## Ways to help

- **Run the kit on a platform we have not listed** (another Oracle version,
  storage, Linux distribution, pyarrow version) and attach the report
  tarball to an issue, whether it passed or not. This is the single most
  useful contribution.
- **Report Parquet files that do not load.** Describe the writer (Spark,
  pandas, DuckDB, ...), the schema and the error. The `inspect` output is
  usually enough; attach a file only if it holds no confidential data.
- **Documentation**: wrong, missing or unclear text in `README.md`,
  `docs/USER_GUIDE.md`, `docs/DEVELOPER.md` or `docs/DESIGN_NOTES.md`.
- **Code**: bug fixes, new Arrow types, new options. Please open an issue
  first for anything larger than a bug fix so the design can be discussed
  against the constraints below.

## Hard constraints

These are not style preferences. Changes that break them cannot be merged.

1. **Python 3.6 and pyarrow 6.0.1 must keep working.** The reference
   platform is Oracle Linux 7 on an Oracle Database Appliance with Python
   3.6.8, pyarrow 6.0.1 and numpy 1.19.5. No f-string `=`, walrus operator,
   `dataclasses`, `from __future__ import annotations`,
   `subprocess.run(capture_output=)` or newer pyarrow compute functions and
   keyword forms. `docs/DESIGN_NOTES.md` lists the pyarrow 6 gaps that have
   already been worked around.
2. **`pqora.py` stays a single file** with no dependencies beyond pyarrow and
   numpy. It is copied to database servers by hand.
3. **Stream mode runs with an empty environment**, one argument, binary
   stdout and ASCII-safe stderr. Absolute paths everywhere; every file open
   has an explicit encoding.
4. **No per-row Python in the stream path.** Records are built with Arrow
   compute over whole columns. The only exceptions are JSON columns and
   decimals with precision above 18.
5. **A value is never stored differently from the source without an explicit
   user option.** When in doubt, stop with a clear message.
6. **Generated SQL must run as a whole file in SQL\*Plus**: no blank lines
   inside statements, `--` comments only, no `&` outside comment lines.

`CLAUDE.md` repeats these constraints for AI coding assistants. If you use
one, point it there.

## Development setup

Any Python 3.6+ with pyarrow and numpy works for day-to-day development:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install pyarrow numpy pyflakes vermin
```

For the compatibility floor, a second environment with the oldest supported
libraries (Python 3.8 is the oldest that `uv` and most distributions still
offer; the kit and CI cover real 3.6):

```bash
python3.8 -m venv .venv36
.venv36/bin/pip install "pyarrow==6.0.1" "numpy==1.19.5"
```

On Windows, do not develop against pyarrow 21.0.0: its `fill_null` kernel is
broken on that platform (see `docs/DESIGN_NOTES.md`), so `verify` fails
although the code is correct. Use pyarrow 20.0.0 or 22.0.0 and later.

A throwaway container gives real Python 3.6:

```bash
docker run --rm -v "$PWD:/w" python:3.6-slim bash -c \
  'pip install -q pyarrow==6.0.1 numpy==1.19.5 && cd /w && python pqora_selftest.py make /tmp/t --big-rows 20000 --blob-mb 1 && python pqora.py generate /tmp/t/types.parquet --table T --out-dir /tmp/p --sample-size all && python pqora.py verify /tmp/p/t_*.pqpart'
```

## Checks every change must pass

Run these before opening a pull request. CI runs the same.

```bash
# 1. lint
pyflakes pqora.py pqora_selftest.py

# 2. Python 3.6 compatibility (static)
vermin -t=3.6- --no-tips --violations pqora.py pqora_selftest.py

# 3. offline self-test: test data, generate, framing check of every record
python3 pqora_selftest.py make /tmp/t --big-rows 300000 --blob-mb 1,5
python3 pqora.py /tmp/t/types.parquet
python3 pqora.py generate /tmp/t/types.parquet --table T --out-dir /tmp/t/p --sample-size all
python3 pqora.py verify /tmp/t/p/t_*.pqpart

# 4. as Oracle runs it: empty environment, wrapper, one argument
env -i /bin/sh /tmp/t/p/pqora.sh /tmp/t/p/t_0001.pqpart | wc -c
```

Run step 3 in both environments (current pyarrow and pyarrow 6.0.1). A
change that touches the stream path, the DDL or the type mapping also needs
the kit (next section).

Keep the documentation in sync in the same pull request:

| Change | Update |
|---|---|
| New or changed option | options table in `docs/USER_GUIDE.md` section 4.2, `--help` text |
| Column spec key, part file, manifest or cache format | `docs/DEVELOPER.md` section 4 |
| Behaviour of Oracle or pyarrow you relied on | the tables in `docs/DESIGN_NOTES.md` |
| New kit test | `kit/README.txt` ("What it tests") and the check count |
| Anything user-visible | `CHANGELOG.md` under *Unreleased* |

## Testing against a database

The kit (`kit/run_kit.sh`) is the real test. It needs an Oracle database
(19c or later) whose server can read a work directory, SQL\*Plus, and Python
with pyarrow on that server. A database in a container with the work
directory mounted at the same path inside and outside is the easiest local
setup.

```bash
sh kit/make_kit.sh                        # dist/pqora_kit_<version>.tar.gz
# unpack it where the database OS user can read it, then as that user:
DB_CONNECT='user/password@//host:1521/service' CREATE_DIRS=1 ./run_kit.sh
```

All checks must pass. The report tarball the kit writes is what to attach to
a pull request that changes the stream path, the DDL or the type mapping.
`kit/README.txt` has the full instructions and the list of what is tested.

## Pull requests

- One topic per pull request. Small is good.
- Describe **what** changed and **why**, and name the environments you
  tested in (Python, pyarrow, Oracle version, storage if relevant).
- Keep the existing style: plain functions, short docstrings that say what a
  function returns, comments that explain a non-obvious Oracle or pyarrow
  behaviour (and add that behaviour to `docs/DESIGN_NOTES.md`).
- Do not bump `VERSION`; maintainers do that at release time.
- Do not commit credentials, Parquet files, generated parts or kit reports.
  `.gitignore` already excludes the common ones.
- Pull requests are reviewed by Nextria maintainers. Expect questions about
  compatibility first; that is where most problems hide.

## Versioning and releases

Releases are made by maintainers:

1. Move the *Unreleased* section of `CHANGELOG.md` under the new version
   with today's date and set `VERSION` in `pqora.py`.
2. Run the checks above and the kit against at least one database.
3. Commit, tag `v<version>` and push the tag. The release workflow builds
   `pqora_kit_<version>.tar.gz` and attaches it to a GitHub Release.
4. After upgrading pqora on a server, rerun `generate` so parts carry the
   new version and options.

Version numbers follow Semantic Versioning as described at the top of
`CHANGELOG.md`.

## License of contributions

By contributing you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE), the same license as the project. There is no
separate contributor agreement. Keep the SPDX header at the top of every
source file:

```python
# Copyright 2026 Nextria
# SPDX-License-Identifier: Apache-2.0
```
