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
#   - kmsverify      → extended-suites.yml `kmsverify` (localstack KMS)
#
# The leg label's FIRST word must be the workflow filename: the
# manifest-drift cross-check below greps that file for the regex.
MANIFEST='
vstream;TestVStream_;internal/engines/mysql/...;ci.yml integration-vstream
vstream;^(TestMigrate_VStream|TestStreamer_.*VStream|TestSpikeShapeA_);internal/pipeline;extended-suites.yml vstream-pipeline
postgis;PostGIS_;internal/pipeline/... internal/engines/postgres/...;ci.yml integration-postgis
chaos;^TestVitessChaos_;internal/engines/mysql/...;extended-suites.yml chaos
vitessreshard;^TestVitessReshard_;internal/engines/mysql/...;extended-suites.yml reshard
vitesscluster!chaos;TestVitessCluster;internal/engines/mysql/...;vitess-version-matrix.yml cluster (weekly default)
ddlfixture;^TestDDLFixture;internal/translate/...;extended-suites.yml ddlfixture
kmsverify;^TestBackup_KMS;internal/pipeline;extended-suites.yml kmsverify
'

# Tags deliberately WITHOUT a manifest axis. Each entry needs a
# rationale here — an undocumented tag fails the axis auto-discovery
# below (audit N-17a: before that check, a brand-new `integration &&
# newtag` suite type-checked via vet-tags, passed shard coverage, and
# ran in NO workflow with zero guard firing — the pre-Bug-125
# "compiles but never runs" class one level up).
#   - psverify      → real-PlanetScale verification; its cron is OFF
#                     per operator decision (paid service), run manually.
#   - jsonbench     → local/on-demand serializer benchmark harness; no
#                     workflow, no `-run` filter to escape.
#   - compressbench → same, compression-algorithm harness.
# `integration` itself is structurally exempt (hardcoded below, not
# listed here): its packages are guarded by check-shard-coverage.sh and
# the ci.yml pipeline shards' -run/-skip regexes are a complete
# partition (the -skip shard catches every name the other two don't),
# so no bare-integration test can escape by name.
EXEMPT_TAGS='psverify jsonbench compressbench'

set -eu
cd "$(dirname "$0")/.."

# ---- axis auto-discovery (audit N-17a) -------------------------------
# Derive the set of build tags actually carried by *_test.go files, the
# same way vet-tags.sh discovers combos: grep the //go:build lines,
# strip GOOS/GOARCH/toolchain terms (selected by the toolchain, not
# -tags), split conjunctions/disjunctions into bare tags. Every
# discovered tag must be a MANIFEST axis or a documented exemption —
# otherwise a new tagged suite exists that no workflow runs and no
# guard watches.
#
# `git ls-files | xargs grep` rather than `git grep`: see the MSYS
# argument-mangling note in scripts/vet-tags.sh.
discovered_tags=$(git ls-files -- '*_test.go' | xargs grep -h '^//go:build ' | awk '
	BEGIN {
		split("aix android darwin dragonfly freebsd hurd illumos ios js linux nacl netbsd openbsd plan9 solaris wasip1 wasm windows zos 386 amd64 arm arm64 loong64 mips mips64 mips64le mipsle ppc64 ppc64le riscv64 s390x cgo gc gccgo unix boringcrypto", gl, " ")
		for (i in gl) goos[gl[i]] = 1
	}
	{
		sub(/^\/\/go:build /, "")
		gsub(/&&|\|\||[()]/, " ")
		n = split($0, terms, /[[:space:]]+/)
		for (i = 1; i <= n; i++) {
			t = terms[i]
			sub(/^!/, "", t)
			if (t == "" || t in goos) continue
			print t
		}
	}' | sort -u)

# Guard against vacuous success (this repo always has tagged test
# files, so empty discovery means discovery broke).
if [ -z "$discovered_tags" ]; then
	echo "::error::check-run-filter-coverage: tag auto-discovery returned no build tags from *_test.go files — discovery is broken, refusing to pass vacuously."
	exit 1
fi

axis_tags=$(printf '%s\n' "$MANIFEST" | sed '/^$/d' | cut -d';' -f1 | sed 's/!.*//' | sort -u)

status=0

for t in $discovered_tags; do
	[ "$t" = "integration" ] && continue # structurally exempt, see EXEMPT_TAGS note
	known=0
	for a in $axis_tags; do
		[ "$t" = "$a" ] && known=1
	done
	for e in $EXEMPT_TAGS; do
		[ "$t" = "$e" ] && known=1
	done
	if [ "$known" -eq 0 ]; then
		echo "::error::new build-tag axis '$t' discovered in *_test.go files but no workflow runs it and no guard watches it — either add a workflow leg with a -run filter AND a MANIFEST entry in scripts/check-run-filter-coverage.sh, or document the tag in EXEMPT_TAGS (with a rationale in the header)"
		status=1
	fi
done

# Symmetric staleness: an EXEMPT_TAGS entry whose tag no longer appears
# in any test file is residue from a suite removal/rename — drop it so
# the exemption list can't accumulate dead grants.
for e in $EXEMPT_TAGS; do
	found=0
	for t in $discovered_tags; do
		[ "$t" = "$e" ] && found=1
	done
	if [ "$found" -eq 0 ]; then
		echo "::error::EXEMPT_TAGS entry '$e' matches no build tag in any *_test.go file — the suite was removed or renamed; drop the stale exemption from scripts/check-run-filter-coverage.sh"
		status=1
	fi
done

# ---- manifest ↔ workflow drift cross-check (audit N-17a) -------------
# The MANIFEST is a hand-copied mirror of each workflow's -run filter;
# guards that validate hand-copied mirrors rot when only one side gets
# edited. Cheap tripwire: each manifest regex must still appear VERBATIM
# (fixed-string) in the workflow file its leg label names. This catches
# the common drift (someone edits the workflow filter and forgets the
# manifest — the stale manifest regex disappears from the file), though
# not the pathological case where the old regex survives in a comment;
# full YAML parsing isn't worth the dependency in a POSIX-sh guard.
while IFS=';' read -r mtag mpattern mscopes mlabel; do
	[ -n "$mtag" ] || continue
	wf=${mlabel%% *}
	wfpath=".github/workflows/$wf"
	if [ ! -f "$wfpath" ]; then
		echo "::error::MANIFEST leg '$mlabel' (axis $mtag) names workflow $wf but $wfpath does not exist — fix the label or restore the workflow"
		status=1
		continue
	fi
	if ! grep -qF -- "$mpattern" "$wfpath"; then
		echo "::error::MANIFEST regex '$mpattern' (axis $mtag) does not appear verbatim in $wfpath — the workflow's -run filter drifted from the manifest (or vice versa); update them TOGETHER"
		status=1
	fi
done <<EOF
$MANIFEST
EOF

# ---- per-axis escapee check ------------------------------------------

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
	echo "check-run-filter-coverage: every filtered-axis test is name-matched by its job's -run filter, every package covered, every discovered tag axis known, and no manifest/workflow drift."
fi
exit $status
