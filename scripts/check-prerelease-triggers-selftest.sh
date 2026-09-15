#!/bin/sh
# check-prerelease-triggers-selftest.sh — the unscheduled-pin category's own
# gate (audit 2026-09-15 T-1 / T-11).
#
# scripts/prerelease-triggers.sh DERIVES the set of build tags whose suites run
# in no scheduled workflow from scripts/check-run-filter-coverage.sh's
# EXEMPT_TAGS and MANIFEST, plus the workflows' own triggers. A derivation
# has two failure shapes a bare run cannot distinguish from "nothing to
# advise": the parse silently returning an empty universe, and a classifier
# that calls a suite scheduled when it is not. This feeds three synthetic
# deltas through the REAL script (via PRERELEASE_TRIGGERS_DELTA_FILE, the
# override that exists for exactly this) and asserts, for each, what fires:
#
#   1 sqlite engine file     -> [unscheduled-pin] names d1verify (EXEMPT_TAGS:
#                               no workflow at all) and lists its pins
#   2 translate package file -> [unscheduled-pin] names ddlfixture, whose
#                               extended-suites job is confined to
#                               workflow_dispatch by its `if:` — the job-level
#                               half of the classifier
#   3 docs-only delta        -> no [unscheduled-pin] block at all, so the
#                               category cannot be firing on everything
#
# Case 1 is the mutation target for the tag table (drop d1verify from
# EXEMPT_TAGS' parse and it fails); case 2 for the `if:` half (treat every
# scheduled workflow's jobs as scheduled and it fails); case 3 for the
# package-path match (match any prefix and it fails).
#
# Runs in ci.yml's Lint job and both pre-commit hooks. Hermetic in effect:
# it reads the tracked tree and the workflows, writes nothing, and the
# script under test is advisory (exit 0) either way, so every assertion here
# is on the OUTPUT.

set -eu
cd "$(dirname "$0")/.."

SCRIPT=scripts/prerelease-triggers.sh
[ -f "$SCRIPT" ] || {
	echo "check-prerelease-triggers-selftest: FAIL — $SCRIPT not found" >&2
	exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail=0

# run_case <name> <delta-lines...>: writes the delta, runs the script, leaves
# the combined output in $out.
out=""
run_case() {
	_name=$1
	shift
	printf '%s\n' "$@" >"$work/$_name.delta"
	if ! out=$(PRERELEASE_TRIGGERS_DELTA_FILE="$work/$_name.delta" bash "$SCRIPT" 2>&1); then
		echo "check-prerelease-triggers-selftest: FAIL [$_name] — the script exited non-zero on a synthetic delta (its derivation refused?):" >&2
		printf '%s\n' "$out" | sed 's/^/    | /' >&2
		fail=1
		return 1
	fi
	return 0
}

expect() {
	_name=$1
	_needle=$2
	if ! printf '%s\n' "$out" | grep -qF -- "$_needle"; then
		echo "check-prerelease-triggers-selftest: FAIL [$_name] — output missing: $_needle" >&2
		printf '%s\n' "$out" | sed 's/^/    | /' >&2
		fail=1
	fi
}

expect_absent() {
	_name=$1
	_needle=$2
	if printf '%s\n' "$out" | grep -qF -- "$_needle"; then
		echo "check-prerelease-triggers-selftest: FAIL [$_name] — output must NOT contain: $_needle" >&2
		printf '%s\n' "$out" | sed 's/^/    | /' >&2
		fail=1
	fi
}

# ---- case 1: the exempt tag (no workflow at all) ----
# The file need not exist: the category keys on the PACKAGE path, and the
# pins listed are the tagged test files that do exist there.
if run_case exempt-tag "internal/engines/sqlite/selftest_synthetic.go"; then
	expect exempt-tag "> [unscheduled-pin]"
	expect exempt-tag "tag 'd1verify' (no workflow at all"
	expect exempt-tag "covers internal/engines/sqlite/"
	expect exempt-tag "internal/engines/sqlite/d1_invalid_utf8_verify_test.go"
	echo "check-prerelease-triggers-selftest: ok [exempt-tag]"
fi

# ---- case 2: the dispatch-only job in a scheduled workflow ----
if run_case dispatch-only-job "internal/translate/selftest_synthetic.go"; then
	expect dispatch-only-job "> [unscheduled-pin]"
	expect dispatch-only-job "tag 'ddlfixture' (dispatch-only"
	expect dispatch-only-job "covers internal/translate/"
	# The classifier must not have dragged a genuinely scheduled or per-PR
	# axis in alongside: none of these has a leg under internal/translate.
	expect_absent dispatch-only-job "tag 'kmsverify'"
	expect_absent dispatch-only-job "tag 'postgis'"
	echo "check-prerelease-triggers-selftest: ok [dispatch-only-job]"
fi

# ---- case 3: docs-only delta fires nothing ----
if run_case docs-only "docs/testing.md" "README.md"; then
	expect_absent docs-only "> [unscheduled-pin]"
	expect docs-only "No specialist review triggered"
	echo "check-prerelease-triggers-selftest: ok [docs-only]"
fi

if [ "$fail" -ne 0 ]; then
	echo "check-prerelease-triggers-selftest: FAILED — the unscheduled-pin category does not behave as documented." >&2
	exit 1
fi
echo "check-prerelease-triggers-selftest: all 3 cases behave as documented (exempt tag, dispatch-only job, docs-only silence)."
