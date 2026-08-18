#!/bin/sh
# check-shard-coverage.sh — keep the CI integration-shard package list
# and the tree's integration-tagged packages in sync, BOTH ways:
#
#   forward — fail when a package with integration-tagged tests is not
#   covered by any CI integration shard;
#   reverse — fail when a COVERED_PACKAGES entry no longer matches any
#   integration-tagged package (a stale entry left behind by a package
#   removal/rename), unless it is a declared DEFENSIVE entry.
#
# The integration shard matrix in .github/workflows/ci.yml is a
# hand-maintained package list. Hand-maintained lists rot: the pgtrigger
# engine landed 2026-05-27 and sat outside every shard until 2026-06-10,
# so its integration suite — including the pin for a known silent-loss
# bug — never ran in CI while the shard comment claimed new packages
# were "defensively bundled". This guard makes the list non-falsifiable:
# adding integration-tagged tests in a package no shard covers fails the
# Lint job with a pointer here. The reverse direction (repo-audit
# 2026-07-03 item 0.4) closes the symmetric rot: removing/renaming a
# package used to leave its entry (and ci.yml shard entry) behind
# untouched, silently claiming coverage of nothing.
#
# COVERED_PACKAGES must be KEPT IN SYNC with the union of the
# `integration` matrix `packages:` entries in .github/workflows/ci.yml,
# INCLUDING each entry's recursion shape (audit N-17b). `go test` package
# arguments are NOT uniformly recursive: `./internal/pipeline/` tests
# exactly that one package, while `./internal/engines/mysql/...` tests
# the whole subtree. An earlier version of this guard treated every
# entry as a subtree prefix, so an integration test added under, say,
# internal/pipeline/migcore would have PASSED the guard (prefix match on
# internal/pipeline) yet run in NO shard (ci.yml lists ./internal/pipeline/
# non-recursively). The entries below therefore mirror ci.yml verbatim:
# a trailing `/...` covers the subtree; a bare entry covers ONLY that
# one package, and every shard-run subpackage needs its own entry.
# covers() is pinned by self_test below, which deliberately asserts the
# non-recursive failure case (migcore under a bare internal/pipeline).

set -eu
cd "$(dirname "$0")/.."

COVERED_PACKAGES="
internal/pipeline
internal/pipeline/backup
internal/engines/flatfile/...
internal/engines/mydumper/...
internal/engines/mysql/...
internal/rowpredicate
internal/engines/postgres/...
internal/engines/pgtrigger/...
internal/ir/...
internal/translate/...
internal/redact/...
internal/crypto/...
internal/config/...
internal/notify/...
"

# Entries that mirror ci.yml shard entries carrying NO integration-
# tagged tests today: the engines-postgres-and-rest shard deliberately
# bundles the small packages, and the pipeline shard lists
# internal/pipeline/backup explicitly, so future integration tests
# there run without a ci.yml edit. Declared BY NAME (spelled exactly as
# in COVERED_PACKAGES) so the reverse check stays strict for everything
# else — and self-tidying: an entry here that GAINS integration tests
# fails below until it is promoted out, and an entry whose directory
# disappears fails the tracked-Go-files check like any other entry.
DEFENSIVE_PACKAGES="
internal/pipeline/backup
internal/ir/...
internal/redact/...
internal/crypto/...
internal/config/...
"

# covers ENTRY DIR — does the ci.yml-shaped package entry cover DIR?
# `foo/...` covers foo and its whole subtree (the `./foo/...` go-test
# argument); a bare `foo` covers ONLY the package foo itself (the
# `./foo/` argument). This asymmetry is the entire point of N-17b —
# keep it in lockstep with how ci.yml's shards enumerate packages.
covers() {
	_entry=$1
	_dir=$2
	case "$_entry" in
	*/...)
		_base=${_entry%/...}
		case "$_dir" in
		"$_base" | "$_base"/*) return 0 ;;
		esac
		;;
	*)
		[ "$_dir" = "$_entry" ] && return 0
		;;
	esac
	return 1
}

# self_test — deliberate-failure pins for covers()'s matching semantics,
# run on every invocation (pure string matching, microseconds). The
# load-bearing case is the third one: a bare (non-recursive) entry must
# NOT cover its subpackages, because ci.yml's `./internal/pipeline/`
# argument doesn't run them. If someone "simplifies" covers() back to a
# prefix match, this fails the guard itself, loudly.
self_test() {
	_st_fail=0
	# expect ENTRY DIR WANT(yes|no)
	expect() {
		if covers "$1" "$2"; then _got=yes; else _got=no; fi
		if [ "$_got" != "$3" ]; then
			echo "::error::check-shard-coverage SELF-TEST: covers('$1', '$2') = $_got, want $3 — the matching semantics no longer mirror ci.yml's package listing; fix covers() before trusting this guard."
			_st_fail=1
		fi
	}
	expect "internal/pipeline" "internal/pipeline" yes
	expect "internal/pipeline/backup" "internal/pipeline/backup" yes
	expect "internal/pipeline" "internal/pipeline/migcore" no # N-17b: bare entries are non-recursive
	expect "internal/pipeline" "internal/pipeline/backup" no  # subpackages need their own entry
	expect "internal/engines/mysql/..." "internal/engines/mysql" yes
	expect "internal/engines/mysql/..." "internal/engines/mysql/sub" yes
	expect "internal/ir/..." "internal/irx" no # prefix must respect path boundaries
	return "$_st_fail"
}

if ! self_test; then
	exit 1
fi

status=0

# ---- COVERED_PACKAGES ↔ ci.yml cross-grep (audit 2026-07-23 TEST-3 /
# G-13). COVERED_PACKAGES is a hand-copied mirror of the integration
# matrix's `packages:` lines, and until this check the guard validated
# only the MIRROR — a shard rebalance that dropped a package from
# ci.yml passed the guard while the package's integration tests (e.g.
# the rowpredicate collation matrices, the 07-18 Critical's designated
# permanent gate) silently stopped running. This is the same tripwire
# check-run-filter-coverage.sh carries for its MANIFEST, ported here:
# parse the REAL ci.yml integration-job matrix and require set equality
# with COVERED_PACKAGES, both directions.
#
# Extraction scope: lines between the `  integration:` job key and the
# next top-level job key, keeping only quoted `packages: "..."` values
# (the job-level `packages: read` permission line has no quote). Tokens
# are normalized from go-test argument form (`./x/` or `./x/...`) to
# COVERED_PACKAGES form (`x` / `x/...`), preserving the recursion shape
# (N-17b) — the `/...` suffix is semantic and must match exactly.
ci_workflow=".github/workflows/ci.yml"
ci_packages=$(awk '
	/^  integration:/ { injob = 1; next }
	injob && /^  [A-Za-z0-9_-]+:/ { injob = 0 }
	injob && /packages: "/ {
		sub(/^[^"]*"/, ""); sub(/".*$/, "")
		n = split($0, toks, /[[:space:]]+/)
		for (i = 1; i <= n; i++) if (toks[i] != "") print toks[i]
	}
' "$ci_workflow" | sed -e 's|^\./||' -e 's|/$||' | sort -u)

# Vacuous-success guard: the matrix has 5 shards over >= 10 distinct
# packages; an extraction that finds almost nothing means the awk scope
# broke (job renamed, indentation changed), not that the list shrank.
if [ "$(printf '%s\n' "$ci_packages" | grep -c .)" -lt 5 ]; then
	echo "::error::check-shard-coverage: extracted fewer than 5 package entries from $ci_workflow's integration matrix — the extraction no longer matches the workflow's shape; fix the awk scope in scripts/check-shard-coverage.sh before trusting this guard."
	exit 1
fi

for p in $COVERED_PACKAGES; do
	if ! printf '%s\n' "$ci_packages" | grep -qxF "$p"; then
		echo "::error::COVERED_PACKAGES entry $p does not appear in $ci_workflow's integration matrix packages lines (recursion shape included) — the mirror drifted from the workflow it guards; update them TOGETHER"
		status=1
	fi
done
for e in $ci_packages; do
	found=0
	for p in $COVERED_PACKAGES; do
		[ "$p" = "$e" ] && found=1
	done
	if [ "$found" -eq 0 ]; then
		echo "::error::$ci_workflow integration matrix lists package $e, which is missing from COVERED_PACKAGES in scripts/check-shard-coverage.sh — the mirror drifted from the workflow it guards; update them TOGETHER"
		status=1
	fi
done
# Every directory containing a file whose build expression includes the
# `integration` tag (compound combos like `integration && vstream` are
# covered packages-wise here; whether their extra tag has a RUN entry
# is scripts/check-run-filter-coverage.sh's concern).
#
# `git ls-files | xargs grep` rather than `git grep`: see the MSYS
# argument-mangling note in scripts/vet-tags.sh.
dirs=$(git ls-files -- '*.go' | xargs grep -l '^//go:build integration' | xargs -n1 dirname | sort -u)

# Guard against vacuous success (empty discovery = broken discovery).
if [ -z "$dirs" ]; then
	echo "::error::check-shard-coverage: discovery returned no integration-tagged packages — discovery is broken, refusing to pass vacuously."
	exit 1
fi

for d in $dirs; do
	covered=0
	for p in $COVERED_PACKAGES; do
		if covers "$p" "$d"; then
			covered=1
		fi
	done
	if [ "$covered" -eq 0 ]; then
		echo "::error::package $d has integration-tagged tests but no ci.yml integration shard covers it (bare entries are NON-recursive, matching ci.yml's \`./pkg/\` arguments) — add it to the shard matrix AND to COVERED_PACKAGES in scripts/check-shard-coverage.sh"
		status=1
	fi
done

# ---- reverse direction: no stale entries (repo-audit 2026-07-03 0.4).
# Each COVERED_PACKAGES entry must still point at real tracked Go code,
# and (unless declared defensive above) at >=1 integration-tagged
# package — otherwise the entry is residue from a removal/rename and the
# shard matrix is claiming coverage of nothing.
for p in $COVERED_PACKAGES; do
	base=${p%/...}
	if ! git ls-files -- "$base" | grep -q '\.go$'; then
		echo "::error::COVERED_PACKAGES entry $p matches no tracked Go files — the package was removed or renamed; drop the stale entry from scripts/check-shard-coverage.sh AND its entry from the ci.yml integration shard matrix"
		status=1
		continue
	fi

	matched=0
	for d in $dirs; do
		if covers "$p" "$d"; then
			matched=1
		fi
	done

	defensive=0
	for dp in $DEFENSIVE_PACKAGES; do
		if [ "$p" = "$dp" ]; then
			defensive=1
		fi
	done

	if [ "$matched" -eq 0 ] && [ "$defensive" -eq 0 ]; then
		echo "::error::COVERED_PACKAGES entry $p matches no package with integration-tagged tests — if the tests moved or were removed, drop the entry (and the ci.yml shard entry); if the shard bundles it defensively, declare it in DEFENSIVE_PACKAGES in scripts/check-shard-coverage.sh"
		status=1
	elif [ "$matched" -eq 1 ] && [ "$defensive" -eq 1 ]; then
		echo "::error::DEFENSIVE_PACKAGES entry $p now HAS integration-tagged tests — promote it out of DEFENSIVE_PACKAGES in scripts/check-shard-coverage.sh (the strict reverse check covers it)"
		status=1
	fi
done


# ---------------------------------------------------------------------------
# Filtered-shard completeness (audit 2026-07-26 TEST-1).
#
# The pipeline shards partition the test-name space by regex:
#   -run ^TestMigrate_ | -run ^TestStreamer_ | -skip ^(TestMigrate_|TestStreamer_)
#
# That partition is complete only for a package listed in ALL of them. A
# package listed under the -skip shard ALONE has a hole exactly the size of the
# skipped names: such a test runs in no job, and NOTHING notices — "go test
# -skip" emits no RUN line and no --- SKIP marker and exits 0, so even the
# fail-on-skip belt is blind to it. internal/pipeline/backup sat in that hole,
# under a comment asserting the opposite.
#
# Rule: every package appearing in a -skip shard must also appear in every -run
# shard.
skip_pkgs=$(grep -B1 'goflags: "-skip' "$ci_workflow" | grep -oE '\./internal/[a-zA-Z0-9_/.-]+' | sort -u)
run_pkglists=$(grep -B1 'goflags: "-run' "$ci_workflow" | grep 'packages:')

# T-3 anti-vacuity floor (Tier-3 audit): the extraction above relies on
# `packages:` sitting one line before `goflags:` (grep -B1). A semantically-
# identical key reorder breaks that adjacency, `skip_pkgs` comes back empty,
# and the `[ -n "$skip_pkgs" ]` guard below then SILENTLY skips this whole
# sub-check — the exact hole it was built to close. Detect the -skip shard's
# existence INDEPENDENTLY (a bare grep, no adjacency), and if it exists but the
# extraction found nothing, fail loudly rather than pass vacuously. Same for the
# -run side.
if grep -q 'goflags: "-skip' "$ci_workflow" && [ -z "$skip_pkgs" ]; then
	echo "::error::check-shard-coverage TEST-1: the workflow HAS a -skip shard but skip_pkgs extraction returned EMPTY — the packages:/goflags: adjacency broke (a key reorder?). Fix the extraction; do not let this sub-check silently pass."
	status=1
fi
if grep -q 'goflags: "-run' "$ci_workflow" && [ -z "$run_pkglists" ]; then
	echo "::error::check-shard-coverage TEST-1: the workflow HAS a -run shard but run_pkglists extraction returned EMPTY — the packages:/goflags: adjacency broke. Fix the extraction."
	status=1
fi

if [ -n "$skip_pkgs" ] && [ -n "$run_pkglists" ]; then
	while IFS= read -r pkg; do
		[ -z "$pkg" ] && continue
		while IFS= read -r line; do
			[ -z "$line" ] && continue
			case "$line" in
			*"$pkg"*) ;;
			*)
				echo "::error::$pkg is listed in a -skip shard of $ci_workflow but is missing from a -run shard of the same partition: ${line# *}"
				echo "::error::A test there whose name the -skip regex matches would run in NO job. 'go test -skip' prints no RUN line and no SKIP marker and exits 0, so neither the fail-on-skip belt nor the coverage check above can see it. Add $pkg to every -run shard's packages list."
				status=1
				;;
			esac
		done <<-INNER
			$run_pkglists
		INNER
	done <<-OUTER
		$skip_pkgs
	OUTER
fi
if [ "$status" -eq 0 ]; then
	echo "check-shard-coverage: every integration-tagged package is covered by a CI shard (recursion semantics mirroring ci.yml), and no shard entry is stale."
fi
exit $status
