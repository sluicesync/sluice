#!/bin/sh
# check-shard-coverage.sh — fail when a package with integration-tagged
# tests is not covered by any CI integration shard.
#
# The integration shard matrix in .github/workflows/ci.yml is a
# hand-maintained package list. Hand-maintained lists rot: the pgtrigger
# engine landed 2026-05-27 and sat outside every shard until 2026-06-10,
# so its integration suite — including the pin for a known silent-loss
# bug — never ran in CI while the shard comment claimed new packages
# were "defensively bundled". This guard makes the list non-falsifiable:
# adding integration-tagged tests in a package no shard covers fails the
# Lint job with a pointer here.
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

if [ "$status" -eq 0 ]; then
	echo "check-shard-coverage: every integration-tagged package is covered by a CI shard."
fi
exit $status
