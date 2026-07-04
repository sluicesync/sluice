#!/bin/sh
# check-run-filter-coverage.sh — fail when a tag-gated test would COMPILE
# in its CI job but never RUN because the job's `-run` filter doesn't
# match its name, or its package is outside the job's path list.
#
# Every `-run`-filtered job is the same trap: build-tag additivity means
# the tagged file compiles (and vet-tags type-checks it), so nothing
# complains — the test just silently never executes anywhere. The postgis
# axis hit it first (11 tests hidden through v0.99.53, closed by the
# postgis-only predecessor of this script); the 2026-07-03 repo audit
# found the SAME class on the vstream axis: three vstream-tagged pins
# (the F7c schema-forward fix, the Bug-132 OLAP-scoping regression pin,
# the ADR-0073(a) self-hosted-defaults pin) escaped both vstream `-run`
# filters and ran in NO workflow. This guard covers every filtered axis
# so the next one can't rot silently, the same way
# scripts/check-shard-coverage.sh makes the shard package list
# non-falsifiable.
#
# MANIFEST — KEEP IN SYNC with the workflows. One line per (tag, job
# leg), fields separated by `;`:
#   tag[!excl] ; the leg's -run regex ; package scopes ; leg label
# A scope with a `/...` suffix covers the subtree (the job passes
# `./x/...`); a bare scope is that ONE package (the job passes `./x/`).
# A `!excl` suffix on the tag skips files that ALSO carry tag `excl` —
# needed when a broader tag's files are a superset of a more specific
# axis's: the chaos files carry `vitesscluster && chaos`, compile ONLY
# in the chaos leg (the matrix job's tag set lacks `chaos`), and are
# guarded by the chaos axis, so `vitesscluster!chaos` excludes them.
# Legs today:
#   - vstream        → ci.yml `integration-vstream` (mysql engine) +
#                      extended-suites.yml `vstream-pipeline` (pipeline pkg)
#   - postgis        → ci.yml `integration-postgis`
#   - chaos          → extended-suites.yml `chaos`
#   - vitessreshard  → extended-suites.yml `reshard`
#   - vitesscluster  → vitess-version-matrix.yml `cluster` (weekly; pins
#                      the SCHEDULED default filter — dispatch may
#                      override it per-run)
#   - ddlfixture     → extended-suites.yml `ddlfixture` (dispatch-only)
#
# Tags deliberately WITHOUT an axis here: `psverify` (cron off per
# operator), `kmsverify` (skip-scaffolding), `jsonbench`/`compressbench`
# (local/on-demand, no `-run` filter to escape). The ci.yml
# pipeline integration shards need no axis either: their -run/-skip
# regexes are a complete partition (the -skip shard catches every name
# the other two don't), so no name can escape.
MANIFEST='
vstream;TestVStream_;internal/engines/mysql/...;ci.yml integration-vstream
vstream;^(TestMigrate_VStream|TestStreamer_.*VStream|TestSpikeShapeA_);internal/pipeline;extended-suites.yml vstream-pipeline
postgis;PostGIS_;internal/pipeline/... internal/engines/postgres/...;ci.yml integration-postgis
chaos;^TestVitessChaos_;internal/engines/mysql/...;extended-suites.yml chaos
vitessreshard;^TestVitessReshard_;internal/engines/mysql/...;extended-suites.yml reshard
vitesscluster!chaos;TestVitessCluster;internal/engines/mysql/...;vitess-version-matrix.yml cluster (weekly default)
ddlfixture;^TestDDLFixture;internal/translate/...;extended-suites.yml ddlfixture
'

set -eu
cd "$(dirname "$0")/.."

# leg_for_dir TAG DIR — print "pattern;label" for the first manifest leg
# of TAG whose package scope contains DIR; return 1 if none does.
leg_for_dir() {
	_tag=$1
	_dir=$2
	while IFS=';' read -r mtag mpattern mscopes mlabel; do
		[ "$mtag" = "$_tag" ] || continue
		for scope in $mscopes; do
			case "$scope" in
			*/...)
				prefix=${scope%/...}
				case "$_dir" in
				"$prefix" | "$prefix"/*)
					printf '%s;%s\n' "$mpattern" "$mlabel"
					return 0
					;;
				esac
				;;
			*)
				if [ "$_dir" = "$scope" ]; then
					printf '%s;%s\n' "$mpattern" "$mlabel"
					return 0
				fi
				;;
			esac
		done
	done <<EOF
$MANIFEST
EOF
	return 1
}

status=0

for axis in $(printf '%s\n' "$MANIFEST" | sed '/^$/d' | cut -d';' -f1 | sort -u); do
	tag=${axis%%!*}
	excl=
	case "$axis" in
	*!*) excl=${axis#*!} ;;
	esac

	# Every test file whose build expression includes this tag (word-
	# bounded: `vitessreshard` must not match a hypothetical `reshard`).
	# `git ls-files | xargs grep` rather than `git grep`: see the MSYS
	# argument-mangling note in scripts/vet-tags.sh.
	files=$(git ls-files -- '*_test.go' | xargs grep -lE "^//go:build.*\b$tag\b" || true)

	# Drop files that also carry the axis's exclude tag (see the
	# manifest header for why).
	if [ -n "$excl" ]; then
		kept=
		for f in $files; do
			grep -qE "^//go:build.*\b$excl\b" "$f" || kept="$kept $f"
		done
		files=$kept
	fi

	# Guard against vacuous success (empty discovery = broken discovery).
	if [ -z "${files# }" ]; then
		echo "::error::check-run-filter-coverage: axis '$axis' discovered no tagged test files — discovery is broken, refusing to pass vacuously."
		status=1
		continue
	fi

	name_count=0
	for f in $files; do
		d=$(dirname "$f")

		# (1) package-path coverage: the file must sit inside some leg's
		# package scope, or no job even hands it to `go test`.
		if leg=$(leg_for_dir "$axis" "$d"); then
			pattern=${leg%%;*}
			label=${leg#*;}
		else
			echo "::error::$f is $tag-tagged but its package ($d) is outside every $axis leg's package scope — add the package to the job's \`go test\` args AND to the MANIFEST in scripts/check-run-filter-coverage.sh"
			status=1
			continue
		fi

		# (2) every test func name must match the covering leg's -run
		# regex (so it actually executes). Helpers (non-Test funcs) exempt.
		names=$(grep -oE '^func (Test[A-Za-z0-9_]+)' "$f" | awk '{print $2}' || true)
		for n in $names; do
			name_count=$((name_count + 1))
			if ! printf '%s\n' "$n" | grep -qE "$pattern"; then
				echo "::error::$f: test $n escapes the $label \`-run '$pattern'\` filter — it compiles but would NEVER run in any workflow; rename it to match, or widen that filter AND this manifest together"
				status=1
			fi
		done
	done

	# Vacuous-pass guard, name flavor: tagged files with zero discovered
	# test functions means the func discovery broke, not that all is well.
	if [ "$name_count" -eq 0 ]; then
		echo "::error::check-run-filter-coverage: axis '$axis' has tagged files but zero discovered test functions — discovery is broken, refusing to pass vacuously."
		status=1
	fi
done

if [ "$status" -eq 0 ]; then
	echo "check-run-filter-coverage: every filtered-axis test is name-matched by its job's -run filter and in a covered package."
fi
exit $status
