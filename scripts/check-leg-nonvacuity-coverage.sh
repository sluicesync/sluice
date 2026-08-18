#!/usr/bin/env bash
# check-leg-nonvacuity-coverage.sh — the meta-guard for the vacuous-green
# discipline (audit H-6). The per-leg guard (check-leg-nonvacuity.sh and the
# inline vstream/crossversion asserts) is only as good as its ADOPTION: the
# audit found it on 2 of 8 extended-suites legs precisely because nothing
# forced the other six to carry it. This guard closes that: across the
# scheduled correctness workflows, every job that runs a build-tagged,
# `-run`-filtered `go test` must ALSO carry a non-vacuity signal in the same
# job — either a `check-leg-nonvacuity.sh` call (exact two-count) or a
# `--- PASS:` assertion (the inline shape). A future bare leg fails HERE, in
# the required Lint check, instead of greening vacuously for weeks.
#
# ci.yml is in scope too (audit T-2): its REQUIRED `Integration (PostGIS)` /
# `Integration (vstream)` legs ran bare `go test` with no assertion, so a dead
# docker daemon would skip every geometry/vstream pin and green a REQUIRED
# check behind branch protection — worse than a scheduled vacuous-green. The
# 5-shard matrix leg is NOT flagged here because its `-run` lives in
# `matrix.shard.goflags` (no literal `-run` on the go-test line); its
# non-vacuity is `check-integration-skips.sh`, a different mechanism.
#
# Detection is deliberately coarse-at-block-granularity but continuation-aware:
# it collapses backslash-continued lines so `go test … -run …` reads as one
# line, then a job "runs a filtered integration leg" iff that joined line
# carries both `integration` (a build tag) and `-run`. Non-integration
# `-run` usages (unit shards, `-run NoMatch` typechecks) are not matched.
set -euo pipefail
cd "$(dirname "$0")/.."

FILES="
.github/workflows/extended-suites.yml
.github/workflows/vitess-version-matrix.yml
.github/workflows/pg-version-matrix.yml
.github/workflows/fuzz-roundtrip.yml
.github/workflows/ci.yml
"

# Anti-vacuity floor: the total detected legs (raw filtered `go test` + helper
# invocations) must not fall below this without a deliberate bump — otherwise a
# mass-removal of legs would leave nothing to check and the guard would pass on
# an empty universe. Measured universe at authoring: 5 raw + 6 helper = 11.
FLOOR=8

total_legs=0
helper_legs=0
fail=0

for f in $FILES; do
	[ -f "$f" ] || { echo "check-leg-nonvacuity-coverage: $f missing"; exit 1; }

	# Emit one record per job: "<name>\t<hasleg>\t<hasguard>\t<ishelper>".
	# awk collapses backslash continuations, splits jobs at 2-space-indented
	# keys AFTER the top-level `jobs:` line, and inspects each job's text.
	records=$(awk '
		function flush() {
			if (job == "") return
			# hasleg: a build-tagged, -run-filtered go test on one joined line,
			# in EITHER tag/-run order (audit T-4 — the old form required
			# `integration` to precede `-run`, so a reordered leg dropped out).
			hasleg  = ((blob ~ /go test[^\n]*integration[^\n]*-run/) || (blob ~ /go test[^\n]*-run[^\n]*integration/)) ? 1 : 0
			# hasguard: the real assertion, not a bare `--- PASS:` substring that
			# a copied yaml COMMENT would satisfy (audit T-4). A genuine
			# non-vacuity assert either greps for `--- PASS` or calls the helper.
			hasguard = ((blob ~ /grep[^\n]*--- PASS/) || (blob ~ /check-leg-nonvacuity\.sh/)) ? 1 : 0
			ishelper = (blob ~ /check-leg-nonvacuity\.sh/) ? 1 : 0
			printf "%s\t%d\t%d\t%d\n", job, hasleg, hasguard, ishelper
		}
		BEGIN { injobs=0; job=""; blob=""; pend="" }
		# Collapse a backslash-continued line into the next.
		{
			line=$0
			if (pend != "") { line = pend " " line; pend="" }
			if (line ~ /\\[[:space:]]*$/) { sub(/\\[[:space:]]*$/, "", line); pend=line; next }
		}
		/^jobs:[[:space:]]*$/ { injobs=1; next }
		injobs && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
			flush()
			job=$0; sub(/^[[:space:]]+/, "", job); sub(/:.*$/, "", job)
			blob=""
			next
		}
		injobs { blob = blob "\n" line }
		END { flush() }
	' "$f")

	while IFS=$'\t' read -r job hasleg hasguard ishelper; do
		[ -z "$job" ] && continue
		if [ "$ishelper" = "1" ]; then
			helper_legs=$((helper_legs + 1))
			total_legs=$((total_legs + 1))
		fi
		if [ "$hasleg" = "1" ]; then
			total_legs=$((total_legs + 1))
			if [ "$hasguard" != "1" ]; then
				echo "check-leg-nonvacuity-coverage: $f job '$job' runs a -run-filtered integration \`go test\` with NO non-vacuity signal."
				echo "  Fix: route it through scripts/check-leg-nonvacuity.sh, or add a '--- PASS:' count assertion in the same job."
				fail=1
			else
				echo "check-leg-nonvacuity-coverage: $f job '$job' — filtered leg guarded. OK."
			fi
		elif [ "$ishelper" = "1" ]; then
			echo "check-leg-nonvacuity-coverage: $f job '$job' — helper-wrapped leg. OK."
		fi
	done <<< "$records"
done

if [ "$total_legs" -lt "$FLOOR" ]; then
	echo "check-leg-nonvacuity-coverage: detected only $total_legs legs (floor $FLOOR) — detection likely broke or legs were mass-removed. Failing."
	exit 1
fi
if [ "$helper_legs" -lt 1 ]; then
	echo "check-leg-nonvacuity-coverage: ZERO check-leg-nonvacuity.sh invocations found — the helper wiring vanished. Failing."
	exit 1
fi

if [ "$fail" -ne 0 ]; then
	echo "check-leg-nonvacuity-coverage: FAILED — a filtered integration leg has no vacuous-green guard."
	exit 1
fi

echo "check-leg-nonvacuity-coverage: all $total_legs filtered integration legs carry a non-vacuity guard ($helper_legs via the helper)."
