#!/bin/sh
# crossversion-build.sh — build the TWO sluice binaries the cross-version
# backup-compatibility gate (extended-suites.yml `crossversion`) runs a
# single backup store through: the CURRENT working tree, and the newest
# released tag whose BackupFormatVersion is STRICTLY LOWER than it.
#
# WHY THIS EXISTS. A backup manifest records the format version its own
# chunks were sealed under, and every reader recomputes at the version the
# artifact records. That rule is per-link; its consequence is not — a
# restore walks the WHOLE chain, so a binary that refuses ANY one manifest
# restores NOTHING from that chain, including the segments it wrote itself.
# The moment a newer binary takes an incremental against an older chain,
# the chain's readability floor rises and every older binary loses all of
# it (roadmap item 90 / Bug 212, found by the v0.104.0 regression cycle —
# by a post-release cycle, not by CI, because EVERY test in this repo runs
# ONE binary against its own output and no single-binary test can observe
# this class at all).
#
# The old binary is BUILT FROM SOURCE at its tag rather than downloaded
# from the release page: no network dependency on GitHub release assets,
# and building the tag tests the actual source at that tag.
#
# TAG SELECTION IS DERIVED, NOT HARDCODED. Walking `git tag --sort=
# -v:refname` newest-first and stopping at the first tag whose
# BackupFormatVersion constant is lower than the working tree's picks the
# right "previous release" automatically on the next bump, and — the part
# that matters — makes a VACUOUS run impossible to reach by neglect: if
# no such tag exists the script refuses instead of building two copies of
# the same version and letting the gate report green against itself.
# (Needs tags in the checkout: the workflow uses fetch-depth: 0.)
#
# Usage:
#   scripts/crossversion-build.sh [OUT_DIR]
#
# THE SECOND AXIS: THE EPOCH-CROSSING BINARY (roadmap item 102). The
# derivation above is right for what it does, and it is untouched — but
# it can only ever select an IN-EPOCH pair. It walks on
# BackupFormatVersion, and BOTH schema-fingerprint boundaries the project
# has shipped (v0.100.0 and v0.103.0, from untagged field additions to
# ir.Index) sit INSIDE format version 8: the "strictly lower" walk steps
# straight over them. That is precisely why the fingerprint partition
# survived two releases unobserved. So a THIRD binary is built from a tag
# on the far side of a known fingerprint boundary, and the suite's epoch
# cell pins the current, accepted behaviour — restore REFUSES, with the
# non-accusatory schema-fingerprint refusal.
#
# Emits the gate's contract on stdout as KEY=VALUE lines, and appends the
# same lines to $GITHUB_ENV when running under Actions:
#   CROSSVER_OLD_BIN / CROSSVER_NEW_BIN   — the two binaries
#   CROSSVER_OLD_TAG                       — the tag the old one came from
#   CROSSVER_OLD_FORMAT / _NEW_FORMAT      — their BackupFormatVersion
#   CROSSVER_WORKTREE                      — the throwaway tag worktree
#   CROSSVER_EPOCH_BIN / _TAG / _FORMAT    — the epoch-crossing binary
#   CROSSVER_EPOCH_WORKTREE                — its throwaway tag worktree
#
# Environment overrides (all default OUTSIDE the repo, so nothing here
# needs a .gitignore entry and no sweep over the working tree ever walks a
# second copy of the module or a 40 MB binary):
#   CROSSVER_OUT_DIR    where the binaries land
#                        (default: ${RUNNER_TEMP:-/tmp}/sluice-crossversion-bin)
#   CROSSVER_WORKTREE   where the tag worktree is created
#                        (default: ${RUNNER_TEMP:-/tmp}/sluice-crossversion)
#   CROSSVER_EPOCH_TAG  overrides the hardcoded epoch-boundary tag below

set -eu
cd "$(dirname "$0")/.."

manifest_file=internal/ir/backup/manifest.go
out_dir=${1:-${CROSSVER_OUT_DIR:-${RUNNER_TEMP:-/tmp}/sluice-crossversion-bin}}

# read_format_version — print the BackupFormatVersion constant from a
# manifest.go on stdin. One definition so the working-tree read and the
# per-tag read can never disagree about what counts as the constant.
read_format_version() {
	sed -n 's/^const BackupFormatVersion = \([0-9][0-9]*\).*/\1/p' | head -1
}

new_version=$(read_format_version <"$manifest_file")
if [ -z "$new_version" ]; then
	echo "::error::crossversion-build: could not read BackupFormatVersion from $manifest_file — the constant was renamed or reformatted; update read_format_version in this script."
	exit 1
fi

old_tag=
old_version=
for t in $(git tag --list 'v*' --sort=-v:refname); do
	v=$(git show "$t:$manifest_file" 2>/dev/null | read_format_version || true)
	[ -n "$v" ] || continue
	if [ "$v" -lt "$new_version" ]; then
		old_tag=$t
		old_version=$v
		break
	fi
done

if [ -z "$old_tag" ]; then
	echo "::error::crossversion-build: no tag found whose BackupFormatVersion is below the working tree's ($new_version). Either the tags are missing from this checkout (the workflow needs fetch-depth: 0) or every release stamps the same version — in both cases the gate would run two identical binaries against each other and report a VACUOUS green, so refusing instead."
	exit 1
fi

# The tag worktree is created OUTSIDE the repo so a `go build ./...` /
# gofumpt / grep sweep over the working tree never walks a second copy of
# the module. Detached: no branch ref survives the run.
worktree=${CROSSVER_WORKTREE:-${RUNNER_TEMP:-/tmp}/sluice-crossversion}
if [ -e "$worktree" ]; then
	git worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
fi
git worktree prune
git worktree add --detach "$worktree" "$old_tag" >/dev/null

# Confirm the checked-out tree really is the version the tag advertised.
# `git show` reads the tag's blob; the worktree is what actually compiles,
# and those are only the same thing if the checkout did what it claimed.
wt_version=$(read_format_version <"$worktree/$manifest_file")
if [ "$wt_version" != "$old_version" ]; then
	echo "::error::crossversion-build: $old_tag advertises BackupFormatVersion=$old_version but its checked-out worktree reads $wt_version — the worktree is not the tag."
	exit 1
fi

# ---------------------------------------------------------------------
# The epoch-crossing tag. THE ONE HARDCODED TAG IN THIS SCRIPT.
#
# HOW TO UPDATE IT. Any tag from a schema-fingerprint epoch other than
# the working tree's works; v0.99.292 is the last release of epoch E1
# (the epoch table lives in docs/operator/error-codes.md, and roadmap
# item 102 carries the evidence). A NEW epoch does NOT require touching
# this — the cell asserts a CROSS-epoch pair, not one specific boundary.
# Change it only if this tag stops building.
#
# The tag NAME is never trusted, deliberately: the suite's epoch cell
# compares what the two binaries actually fingerprint one identical
# schema to, and FAILS if they agree. A wrong tag here is a red cell, not
# a vacuous green — the failure mode this repo has been burned by.
epoch_tag=${CROSSVER_EPOCH_TAG:-v0.99.292}

if ! git rev-parse -q --verify "refs/tags/$epoch_tag" >/dev/null; then
	echo "::error::crossversion-build: epoch tag $epoch_tag is not in this checkout (the workflow needs fetch-depth: 0). Refusing rather than skipping the epoch cell."
	exit 1
fi
epoch_version=$(git show "$epoch_tag:$manifest_file" | read_format_version)
if [ -z "$epoch_version" ]; then
	echo "::error::crossversion-build: could not read BackupFormatVersion from $epoch_tag."
	exit 1
fi
# The epoch cell asserts a SCHEMA-FINGERPRINT refusal. If the epoch tag
# wrote manifests the working tree cannot read at all, the format-version
# refusal would preempt it and the cell would assert the wrong contract.
if [ "$epoch_version" -gt "$new_version" ]; then
	echo "::error::crossversion-build: epoch tag $epoch_tag stamps BackupFormatVersion=$epoch_version, above the working tree's $new_version — the working tree would refuse it on the VERSION gate before ever reaching the fingerprint check, so the epoch cell would assert the wrong refusal. Pick an older epoch tag."
	exit 1
fi

epoch_worktree=${CROSSVER_EPOCH_WORKTREE:-${RUNNER_TEMP:-/tmp}/sluice-crossversion-epoch}
if [ -e "$epoch_worktree" ]; then
	git worktree remove --force "$epoch_worktree" >/dev/null 2>&1 || rm -rf "$epoch_worktree"
fi
git worktree prune
git worktree add --detach "$epoch_worktree" "$epoch_tag" >/dev/null

exe=
case "$(go env GOOS)" in
windows) exe=".exe" ;;
esac

mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)
old_bin="$out_dir/sluice-old$exe"
new_bin="$out_dir/sluice-new$exe"
epoch_bin="$out_dir/sluice-epoch$exe"

# The tag builds stamp their version the way a real release does
# (.goreleaser.yaml: `-X main.version={{.Version}}`, which is the tag
# WITHOUT its leading "v"). Without this every tag binary records
# sluice_version="dev" in the manifests it writes — and the refusal that
# names the WRITING RELEASE, which is the data-derived half of the
# fingerprint refusal, would have nothing to name.
echo "crossversion-build: building OLD $old_tag (BackupFormatVersion=$old_version) from $worktree"
(cd "$worktree" && go build -ldflags "-X main.version=${old_tag#v}" -o "$old_bin" ./cmd/sluice)
echo "crossversion-build: building EPOCH $epoch_tag (BackupFormatVersion=$epoch_version) from $epoch_worktree"
(cd "$epoch_worktree" && go build -ldflags "-X main.version=${epoch_tag#v}" -o "$epoch_bin" ./cmd/sluice)
echo "crossversion-build: building NEW working tree (BackupFormatVersion=$new_version)"
go build -o "$new_bin" ./cmd/sluice

emit() {
	printf '%s=%s\n' "$1" "$2"
	if [ -n "${GITHUB_ENV:-}" ]; then
		printf '%s=%s\n' "$1" "$2" >>"$GITHUB_ENV"
	fi
}

emit CROSSVER_OLD_BIN "$old_bin"
emit CROSSVER_NEW_BIN "$new_bin"
emit CROSSVER_OLD_TAG "$old_tag"
emit CROSSVER_OLD_FORMAT "$old_version"
emit CROSSVER_NEW_FORMAT "$new_version"
emit CROSSVER_WORKTREE "$worktree"
emit CROSSVER_EPOCH_BIN "$epoch_bin"
emit CROSSVER_EPOCH_TAG "$epoch_tag"
emit CROSSVER_EPOCH_FORMAT "$epoch_version"
emit CROSSVER_EPOCH_WORKTREE "$epoch_worktree"
