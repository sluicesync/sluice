#!/usr/bin/env bash
# run-incremental.sh — orchestrate the combined-ALTER incremental comparison:
# v0.99.30 (per-index overlap) vs main-combined (overlap + combined-ALTER),
# interleaved, 2 clean runs each. Each run is bench-mysql.sh, which resets the
# target, runs the migrate detached+polled, and verifies zero-loss + indexes.
#
# A run that aborts on the Rancher Hyper-V "invalid connection" blip (rc!=0)
# is re-run ONCE with a 'b' suffix. Final block prints a consolidated table.
#
# Launched as a single background job by the main session so all 4 runs are
# orchestrated by one process (the prior subagent misfired by backgrounding a
# single run and ending its turn).
set -uo pipefail
export PATH="$PATH:/c/Program Files/Rancher Desktop/resources/resources/win32/bin"
export MSYS_NO_PATHCONV=1
export TESTCONTAINERS_RYUK_DISABLED=true

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
SUMMARY="$HERE/results/INCREMENTAL-SUMMARY.txt"
: > "$SUMMARY"

PERINDEX=sluice-bench-mysql:v0.99.30
COMBINED=sluice-bench-mysql:main-combined

# run_one <image> <label> -> appends a one-line digest to $SUMMARY.
run_one() {
  local img="$1" label="$2" out rc zl idx wall
  out="$(SLUICE_IMG="$img" bash bench-mysql.sh "$label" 2>&1)"
  echo "$out" > "$HERE/results/console-${label}.txt"
  rc="$(printf '%s\n' "$out"   | grep -oE 'rc=[0-9-]+'                | head -1 | cut -d= -f2)"
  wall="$(printf '%s\n' "$out" | grep -oE 'wall=[0-9.]+s'            | head -1)"
  zl="ZL-?";  printf '%s\n' "$out" | grep -q 'ZERO-LOSS-OK'   && zl="ZERO-LOSS-OK"
  printf '%s\n' "$out" | grep -q 'MISMATCH' && zl="ZL-MISMATCH"
  idx="IDX-?"; printf '%s\n' "$out" | grep -q 'ALL-INDEXES-OK' && idx="ALL-INDEXES-OK"
  printf '%s\n' "$out" | grep -q 'INDEX-CHECK-FAIL' && idx="INDEX-CHECK-FAIL"
  printf '%-26s | %-22s | rc=%-3s | %-12s | %-16s | %s\n' \
    "$img" "$label" "${rc:-?}" "$wall" "$zl" "$idx" >> "$SUMMARY"
  # rc!=0 is the Hyper-V flap signal — caller decides on re-run.
  [ "${rc:-1}" = "0" ]
}

# run_with_retry <image> <label>: one re-run on flap (rc!=0).
run_with_retry() {
  local img="$1" label="$2"
  if ! run_one "$img" "$label"; then
    echo ">>> $label flapped (rc!=0); re-running as ${label}b" >> "$SUMMARY"
    run_one "$img" "${label}b" || true
  fi
}

echo "=== combined-ALTER incremental bench: $(date -u +%FT%TZ) ===" >> "$SUMMARY"
run_with_retry "$PERINDEX" perindex-run1
run_with_retry "$COMBINED" combined-run1
run_with_retry "$PERINDEX" perindex-run2
run_with_retry "$COMBINED" combined-run2

echo "" >> "$SUMMARY"
echo "=== DONE: $(date -u +%FT%TZ) ===" >> "$SUMMARY"
cat "$SUMMARY"
