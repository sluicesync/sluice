#!/bin/sh
# check-postgis-coverage.sh — fail when a postgis-tagged test would not
# actually RUN in the CI "Integration (PostGIS)" job.
#
# That job compiles with `-tags="integration postgis"` (build-tag
# additivity) but scopes EXECUTION with `-run 'PostGIS_'` over a
# hand-maintained package path list. Two ways a postgis test silently
# never runs despite compiling clean:
#   1. its name has no `PostGIS_` segment, so `-run 'PostGIS_'` skips it;
#   2. it lives in a package the job's path list doesn't include.
# Both bit us through v0.99.53: the narrower `-run TestMigrate_PostGIS_`
# filter (and a pipeline-only path list) silently excluded the 10
# TestMigrate_PG_PostGIS_* passthrough tests AND the engine-package
# TestPipelined_PostGIS_GeometryEquivalence pin — they compiled, vet-clean,
# and never executed. This guard makes the convention non-falsifiable, the
# same way scripts/check-shard-coverage.sh does for the integration shards.
#
# KEEP IN SYNC with the `integration-postgis` job in
# .github/workflows/ci.yml:
#   - RUN_PATTERN must equal that job's `-run` regex.
#   - COVERED_PREFIXES must equal that job's package path list (a `foo/...`
#     tree is treated here as the `foo` prefix).

set -eu
cd "$(dirname "$0")/.."

# The CI job's `-run` regex (a test name must contain this to execute).
RUN_PATTERN="PostGIS_"

# The CI job's package path list (./internal/pipeline/... +
# ./internal/engines/postgres/...).
COVERED_PREFIXES="
internal/pipeline
internal/engines/postgres
"

status=0

# Every file whose build expression includes the `postgis` tag.
# `git ls-files | xargs grep` rather than `git grep`: see the MSYS
# argument-mangling note in scripts/vet-tags.sh.
files=$(git ls-files -- '*_test.go' | xargs grep -l '^//go:build.*postgis' || true)

# Guard against vacuous success (empty discovery = broken discovery).
if [ -z "$files" ]; then
	echo "::error::check-postgis-coverage: discovery returned no postgis-tagged test files — discovery is broken, refusing to pass vacuously."
	exit 1
fi

for f in $files; do
	d=$(dirname "$f")

	# (1) package-path coverage
	covered=0
	for p in $COVERED_PREFIXES; do
		case "$d" in
		"$p" | "$p"/*) covered=1 ;;
		esac
	done
	if [ "$covered" -eq 0 ]; then
		echo "::error::$f is postgis-tagged but its package ($d) is not in the Integration (PostGIS) job's path list — add it to that job's \`go test\` package args AND to COVERED_PREFIXES in scripts/check-postgis-coverage.sh"
		status=1
	fi

	# (2) every test func name must match the job's -run pattern
	# (so it actually executes). Test helpers (non-Test funcs) are exempt.
	names=$(grep -oE '^func (Test[A-Za-z0-9_]+)' "$f" | awk '{print $2}' || true)
	for n in $names; do
		case "$n" in
		*"$RUN_PATTERN"*) ;;
		*)
			echo "::error::$f: test $n has no \"$RUN_PATTERN\" segment, so the Integration (PostGIS) job's \`-run '$RUN_PATTERN'\` filter would NEVER run it — rename it to include a ${RUN_PATTERN} segment (e.g. TestMigrate_PostGIS_Foo / TestPipelined_PostGIS_Foo)"
			status=1
			;;
		esac
	done
done

if [ "$status" -eq 0 ]; then
	echo "check-postgis-coverage: every postgis-tagged test is name-matched by -run '$RUN_PATTERN' and in a covered package."
fi
exit $status
