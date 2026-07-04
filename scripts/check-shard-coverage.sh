#!/bin/sh
# check-shard-coverage.sh — keep the CI integration-shard package list
# and the tree's integration-tagged packages in sync, BOTH ways:
#
#   forward — fail when a package with integration-tagged tests is not
#   covered by any CI integration shard;
#   reverse — fail when a COVERED_PREFIXES entry no longer matches any
#   integration-tagged package (a stale prefix left behind by a package
#   removal/rename), unless it is a declared DEFENSIVE prefix.
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
# package used to leave its prefix (and ci.yml shard entry) behind
# untouched, silently claiming coverage of nothing.
#
# COVERED_PREFIXES must be KEPT IN SYNC with the union of the
# `integration` matrix `packages:` entries in .github/workflows/ci.yml
# (a `foo/...` pattern covers the whole subtree; this script treats
# every entry as a prefix, which matches how the shards enumerate
# packages today — `./internal/pipeline/` plus `/...` trees).

set -eu
cd "$(dirname "$0")/.."

COVERED_PREFIXES="
internal/pipeline
internal/engines/mysql
internal/engines/postgres
internal/engines/pgtrigger
internal/ir
internal/translate
internal/redact
internal/crypto
internal/config
internal/notify
"

# Prefixes that mirror ci.yml shard entries carrying NO integration-
# tagged tests today: the engines-postgres-and-rest shard deliberately
# bundles these small packages so future integration tests there run
# without a ci.yml edit. Declared BY NAME so the reverse check stays
# strict for everything else — and self-tidying: an entry here that
# GAINS integration tests fails below until it is promoted out, and an
# entry whose directory disappears fails the tracked-Go-files check
# like any other prefix.
DEFENSIVE_PREFIXES="
internal/ir
internal/redact
internal/crypto
internal/config
"

status=0
# Every directory containing a file whose build expression includes the
# `integration` tag (compound combos like `integration && vstream` are
# covered packages-wise here; whether their extra tag has a RUN entry
# is a separate, scheduled-suite concern).
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
	for p in $COVERED_PREFIXES; do
		case "$d" in
		"$p" | "$p"/*) covered=1 ;;
		esac
	done
	if [ "$covered" -eq 0 ]; then
		echo "::error::package $d has integration-tagged tests but no ci.yml integration shard covers it — add it to the shard matrix AND to COVERED_PREFIXES in scripts/check-shard-coverage.sh"
		status=1
	fi
done

# ---- reverse direction: no stale prefixes (repo-audit 2026-07-03 0.4).
# Each COVERED_PREFIXES entry must still point at real tracked Go code,
# and (unless declared defensive above) at >=1 integration-tagged
# package — otherwise the entry is residue from a removal/rename and the
# shard matrix is claiming coverage of nothing.
for p in $COVERED_PREFIXES; do
	if ! git ls-files -- "$p" | grep -q '\.go$'; then
		echo "::error::COVERED_PREFIXES entry $p matches no tracked Go files — the package was removed or renamed; drop the stale prefix from scripts/check-shard-coverage.sh AND its entry from the ci.yml integration shard matrix"
		status=1
		continue
	fi

	matched=0
	for d in $dirs; do
		case "$d" in
		"$p" | "$p"/*) matched=1 ;;
		esac
	done

	defensive=0
	for dp in $DEFENSIVE_PREFIXES; do
		if [ "$p" = "$dp" ]; then
			defensive=1
		fi
	done

	if [ "$matched" -eq 0 ] && [ "$defensive" -eq 0 ]; then
		echo "::error::COVERED_PREFIXES entry $p matches no package with integration-tagged tests — if the tests moved or were removed, drop the prefix (and the ci.yml shard entry); if the shard bundles it defensively, declare it in DEFENSIVE_PREFIXES in scripts/check-shard-coverage.sh"
		status=1
	elif [ "$matched" -eq 1 ] && [ "$defensive" -eq 1 ]; then
		echo "::error::DEFENSIVE_PREFIXES entry $p now HAS integration-tagged tests — promote it out of DEFENSIVE_PREFIXES in scripts/check-shard-coverage.sh (the strict reverse check covers it)"
		status=1
	fi
done

if [ "$status" -eq 0 ]; then
	echo "check-shard-coverage: every integration-tagged package is covered by a CI shard, and no shard prefix is stale."
fi
exit $status
