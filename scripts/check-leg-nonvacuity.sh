#!/usr/bin/env bash
# check-leg-nonvacuity.sh — run a build-tagged, -run-filtered integration leg
# with the vacuous-green guard the audit (H-6) found on only 2 of 8
# extended-suites legs.
#
# A `-run`-filtered `go test` can pass VACUOUSLY: `go test` exits 0 both when
# the -run regex matches NOTHING (a renamed test, a build-tag typo) and when
# every matched test t.Skip()s (a cluster-boot helper that skips on a
# docker-network-create failure — the audit's concrete reachable case). Either
# way the leg is GREEN having run zero real assertions. This wrapper makes that
# impossible with two exact counts around the run:
#
#   Phase 1  the -run filter must -list EXACTLY <selected> top-level tests
#            (BEFORE burning container time) — a vanished or newly-added test
#            fails here, forcing a deliberate count bump.
#   Phase 2  run them, propagating the REAL exit code through the tee.
#   Phase 3  EXACTLY <passed> top-level tests must report `--- PASS:`.
#
# With no failures (Phase 2 rc==0), selected == passed + skipped, so pinning
# both <selected> and <passed> pins the skip count too: a NEW skip (infra
# regression) or a quarantined test that starts passing both fail the leg
# loudly. <selected> == <passed> for a leg with no known skip; they differ only
# to encode a KNOWN, deliberate quarantine (e.g. chaos: 5 selected, 4 passed,
# 1 unconditional roadmap-tracked t.Skip).
#
# usage: check-leg-nonvacuity.sh <tags> <run-regex> <pkgs> <selected> <passed> [extra go test args...]
set -euo pipefail
cd "$(dirname "$0")/.."

tags=${1:?tags}; run=${2:?run-regex}; pkgs=${3:?pkgs}; selected=${4:?selected-count}; passed=${5:?passed-count}
shift 5

int_ge0() { case "$1" in ''|*[!0-9]*) return 1;; *) return 0;; esac; }
if ! int_ge0 "$selected" || ! int_ge0 "$passed"; then
	echo "check-leg-nonvacuity: <selected> and <passed> must be non-negative integers (got '$selected' / '$passed')"; exit 2
fi
# Anti-vacuity on the guard's OWN inputs: a leg that expects to select zero
# tests, or pass zero, is the vacuous shape this guard exists to reject.
if [ "$selected" -lt 1 ] || [ "$passed" -lt 1 ]; then
	echo "check-leg-nonvacuity: <selected> and <passed> must both be >= 1 (a leg selecting/passing zero tests is vacuous green)"; exit 2
fi
if [ "$passed" -gt "$selected" ]; then
	echo "check-leg-nonvacuity: <passed> ($passed) cannot exceed <selected> ($selected)"; exit 2
fi

# Phase 1 — the -run filter must select exactly <selected> top-level tests.
# shellcheck disable=SC2086 # $pkgs is a deliberate space-separated package list
matched=$(go test -tags="$tags" -list "$run" $pkgs | grep -c '^Test' || true)
if [ "$matched" -ne "$selected" ]; then
	echo "::error::check-leg-nonvacuity: -run '$run' on $pkgs selected $matched top-level tests, expected $selected. A test was renamed/added/removed or a build tag broke — bump the count deliberately if intended."
	# shellcheck disable=SC2086
	go test -tags="$tags" -list "$run" $pkgs || true
	exit 1
fi

# Phase 2 — run them, capturing the real exit code through the tee.
out=$(mktemp)
trap 'rm -f "$out"' EXIT
set +e
# shellcheck disable=SC2086
go test -tags="$tags" -count=1 -run "$run" -v "$@" $pkgs 2>&1 | tee "$out"
rc=${PIPESTATUS[0]}
set -e
if [ "$rc" -ne 0 ]; then
	echo "::error::check-leg-nonvacuity: go test exited $rc (a real failure) — see the run log."
	exit "$rc"
fi

# Phase 3 — exactly <passed> top-level tests reported PASS. Subtests are
# indented ("    --- PASS: Foo/sub"); the '^--- PASS: Test' anchor counts
# top-level only, matching the vstream/crossversion legs this generalizes.
got=$(grep -c '^--- PASS: Test' "$out" || true)
if [ "$got" -ne "$passed" ]; then
	echo "::error::check-leg-nonvacuity: $got top-level tests PASSed, expected $passed — a matched test was skipped or didn't run (vacuous green). selected=$selected, so $((selected - passed)) known skip(s) were expected."
	exit 1
fi

echo "check-leg-nonvacuity: OK — -run '$run' selected $selected top-level test(s), $passed PASSed ($((selected - passed)) known skip(s))."
