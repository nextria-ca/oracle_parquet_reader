# CLAUDE.md - orientation for AI assistants

## What this is
`pqora.py` loads Parquet into Oracle through an `ORACLE_LOADER` external table
whose preprocessor is `pqora.py` itself; one Parquet row group (or group of
them) = one `.pqpart` location file = one Oracle parallel work unit.
It is an open-source project of Nextria (Apache License 2.0).
Start with `README.md`, then:
- `docs/USER_GUIDE.md` - behaviour, options, generated files, troubleshooting
- `docs/DEVELOPER.md` - architecture, code map, data structures, stream format, testing, release
- `docs/DESIGN_NOTES.md` - decisions and verified Oracle/pyarrow behaviours (read before "fixing" something that looks odd)
- `CONTRIBUTING.md` - the checks every change must pass, release steps

## Non-negotiable constraints
- Must run on **Python 3.6 + pyarrow 6.0.1 + numpy 1.19** (Oracle Database
  Appliance, Oracle Linux 7). Check with `vermin -t=3.6- --violations` and test
  with pyarrow 6.0.1 (see DEVELOPER.md 7.4). Many newer pyarrow functions/casts
  don't exist there (list in DESIGN_NOTES.md).
- `pqora.py` stays a **single file** with no dependencies beyond pyarrow/numpy.
- Stream mode runs as the DB OS user with an **empty environment** and one
  argument; output is binary on stdout; errors go to stderr (shown by Oracle as
  `KUP-04095`).
- Never let a value be stored differently from the source without an explicit
  user option; fail with a clear message instead.
- No per-row Python in the stream path (vectorized Arrow compute).

## Files
| File | Role |
|---|---|
| `pqora.py` | the tool: inspect (default), `generate`, stream (`<part>.pqpart`), `verify`, `resize` |
| `pqora_selftest.py` | test data (`make`), pyarrow vs Oracle checksums (`expected`, `sql`, `compare`, `stageview`, `parallel`) |
| `kit/run_kit.sh` | unattended test kit for a DB server (all experiments → one report tarball) |
| `kit/make_kit.sh` | builds `dist/pqora_kit_<VERSION>.tar.gz` (includes LICENSE and NOTICE) |
| `kit/README.txt` | kit instructions (shipped in the tarball) |
| `parquet_inspect.py` | old prototype for the native ORACLE_BIGDATA driver; not maintained |
| `docs/images/*.svg` | hand-written diagrams used by README.md and docs/WHY_PQORA.md |
| `.github/workflows/ci.yml` | pyflakes, vermin, offline self-test on pyarrow 6.0.1 / current / real Python 3.6 |
| `.github/workflows/release.yml` | on tag `v<VERSION>`: builds the kit and publishes a GitHub Release |
| `LICENSE`, `NOTICE`, `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md` | open-source project files |

## Working on the code
- Version: `VERSION` in `pqora.py`; maintainers bump it at release time together
  with `CHANGELOG.md` (steps in DEVELOPER.md section 8).
- After changes: `pyflakes`, `vermin`, offline `generate` + `verify` on both the
  oldest (pyarrow 6.0.1) and a current pyarrow, and the full kit against a
  database (DEVELOPER.md section 7). The kit currently reports 42 checks; all
  must pass.
- Every source file starts with the SPDX header
  (`# Copyright 2026 Nextria` / `# SPDX-License-Identifier: Apache-2.0`).
- Record user-visible changes in `CHANGELOG.md` under *Unreleased*.
- Keep docs in sync: options table in USER_GUIDE.md section 4.2, column spec
  keys and file formats in DEVELOPER.md section 4, verified behaviours in
  DESIGN_NOTES.md.
- Generated SQL must stay runnable by SQL*Plus as a whole file (no blank lines
  inside statements, `--` comments only).
- Do not commit credentials, Parquet files, generated parts or kit reports;
  test connection details come from the local environment, not from this repo.
- Developing on Windows: pyarrow 21.0.0 has a broken `fill_null` on that
  platform (DESIGN_NOTES.md), so `verify` fails there. Use 20.0.0 or 22.0.0+.
- Public repository: write docs and comments for an outside reader; no
  internal host names, client names or paths.
- Commit messages: plain descriptive messages only. Do not add
  `Co-Authored-By`, `Claude-Session` or any other AI attribution trailers.

## Glossary
part = `.pqpart` JSON work unit · spec = column spec dict (DEVELOPER.md 4.1) ·
staged mode = `--stage-large-records` (`<table>_CLOB`) · manifest =
`<table>_manifest.json` · sample cache = `<table>_sample_cache.json` (one entry
per `--sample-size`; delete to re-sample) · stream log = `<table>_stream.log`.
