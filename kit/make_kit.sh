#!/bin/sh
# Copyright 2026 Nextria
# SPDX-License-Identifier: Apache-2.0
# Build dist/pqora_kit_<version>.tar.gz (one file to copy to the database server).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VER="$(sed -n 's/^VERSION = "\(.*\)"/\1/p' "$ROOT/pqora.py")"
DIST="$ROOT/dist"
STAGE="$DIST/pqora_kit"
rm -rf "$STAGE"
mkdir -p "$STAGE"
cp "$ROOT/pqora.py" "$ROOT/pqora_selftest.py" "$ROOT/kit/run_kit.sh" "$ROOT/kit/README.txt" "$ROOT/LICENSE" "$ROOT/NOTICE" "$STAGE/"
chmod 755 "$STAGE/run_kit.sh" "$STAGE/pqora.py" "$STAGE/pqora_selftest.py"
tar czf "$DIST/pqora_kit_$VER.tar.gz" -C "$DIST" pqora_kit
rm -rf "$STAGE"
echo "$DIST/pqora_kit_$VER.tar.gz"
