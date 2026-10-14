#!/bin/sh
# Every CANALIS_* variable read in src/ must appear in docs/configuration.md.
# CANALIS_LINE_CRASH_CHILD is a test-only hook (line.rs's crash test).
set -eu
cd "$(dirname "$0")/.."
missing=0
for var in $(grep -rhoE '"CANALIS_[A-Z0-9_]+"' src | tr -d '"' | sort -u); do
  [ "$var" = CANALIS_LINE_CRASH_CHILD ] && continue
  if ! grep -q "\`$var\`" docs/configuration.md; then
    echo "missing from docs/configuration.md: $var"
    missing=1
  fi
done
exit $missing
