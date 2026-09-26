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
#   CROSSVER_PEER_BIN / _TAG / _FORMAT     — the SAME-format peer binary
#   CROSSVER_PEER_WORKTREE                 — its throwaway tag worktree
#   CROSSVER_BELOW_<NAME>_BIN / _TAG / _FORMAT / _TIER
#                                          — per shape-stamped tier suite,
#                                            the newest release below that
#                                            tier (see THE FOURTH AXIS)
#
# THE THIRD AXIS: THE SAME-FORMAT PEER (roadmap item 104 / Bug 216). Cells
# 1-5 all read OLD-written chains with a NEWER binary, or assert a refusal.
# None of them asks the question that actually caught the fourth epoch:
# does the chain THIS build writes still restore on the PREVIOUS release?
# That direction is where a schema-fingerprint addition bites, it is what
# a v0.104.3 operator hit, and it needs a binary that shares this build's
# BackupFormatVersion — otherwise the version gate refuses first and the
# fingerprint is never reached. Hence a FOURTH binary, derived as the
# newest tag at the same format version, minus the known orphan epochs —
# or, on a fresh proportional bump no release carries yet, the newest
# release below it (see the peer block for why that stays loud).
#
# Environment overrides (all default OUTSIDE the repo, so nothing here
# needs a .gitignore entry and no sweep over the working tree ever walks a
# second copy of the module or a 40 MB binary):
#   CROSSVER_OUT_DIR    where the binaries land
#                        (default: ${RUNNER_TEMP:-/tmp}/sluice-crossversion-bin)
#   CROSSVER_WORKTREE   where the tag worktree is created
#                        (default: ${RUNNER_TEMP:-/tmp}/sluice-crossversion)
#   CROSSVER_EPOCH_TAG  overrides the hardcoded epoch-boundary tag below
#   CROSSVER_PEER_TAG   overrides the derived same-format peer tag
#   CROSSVER_PEER_EXCLUDE
#                       space-separated tags the peer derivation skips —
#                       the orphan-epoch list (see below)

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

# ---------------------------------------------------------------------
# The SAME-FORMAT PEER tag (roadmap item 104 / Bug 216).
#
# Derived, like OLD: the newest tag whose BackupFormatVersion EQUALS the
# working tree's. Equality is the preference — the peer cell asserts that
# a chain THIS build writes still restores on the previous release, and a
# peer below a version that chain actually stamps would refuse on the
# version gate before the schema fingerprint is ever recomputed, turning
# the cell into a restatement of cell 2. That condition is checked per
# manifest inside cell 6, not assumed from the tag, so the fresh-bump
# fallback below cannot make the cell vacuous.
#
# THE EXCLUSION LIST IS THE ORPHAN EPOCHS. A release whose fingerprint
# nobody else reproduces cannot serve as the peer: it would fail the cell
# for a reason the cell is not about. v0.104.3 is the one such release —
# it hashed ir.Index.ConstraintNamed into the fingerprint (the omitempty
# that did not hold, Bug 216), so chains it wrote from a primary-keyed
# Postgres source are readable only by itself, and chains written by
# v0.104.4+ are unreadable by it. Add a tag here ONLY when it is a known
# orphan; every other same-format release must stay eligible, because the
# whole value of this cell is that the peer advances automatically.
peer_exclude=${CROSSVER_PEER_EXCLUDE:-v0.104.3}
peer_tag=${CROSSVER_PEER_TAG:-}
if [ -z "$peer_tag" ]; then
	for t in $(git tag --list 'v*' --sort=-v:refname); do
		skip=
		for x in $peer_exclude; do
			[ "$t" = "$x" ] && skip=1 && break
		done
		[ -n "$skip" ] && continue
		v=$(git show "$t:$manifest_file" 2>/dev/null | read_format_version || true)
		[ -n "$v" ] || continue
		if [ "$v" -eq "$new_version" ]; then
			peer_tag=$t
			break
		fi
	done
fi

# A FRESH, PROPORTIONAL FORMAT BUMP (the FormatVersion-11 case, audit
# 2026-09-15 F-2). Until a release ships at the working tree's format, no
# tag equals it. That used to be a refusal here — which also stopped OLD
# and NEW from being built, so NO cross-version cell could run in exactly
# the window before the bump's own tag, when the gate matters most. Since
# every bump from 10 up is proportional (only the manifests that carry
# the feature are stamped), the chain cell 6 writes is usually still at
# a version the newest release reads. So the peer falls back to the
# newest non-orphan release BELOW the working tree's format, and the
# question "can this peer reach the fingerprint check at all?" moves to
# where the evidence is: cell 6 reads every manifest its chain actually
# stamped and fails by name if one sits above the peer's ceiling. Loud,
# never vacuous, and it re-arms to a same-format peer the moment one ships.
if [ -z "$peer_tag" ]; then
	for t in $(git tag --list 'v*' --sort=-v:refname); do
		skip=
		for x in $peer_exclude; do
			[ "$t" = "$x" ] && skip=1 && break
		done
		[ -n "$skip" ] && continue
		v=$(git show "$t:$manifest_file" 2>/dev/null | read_format_version || true)
		[ -n "$v" ] || continue
		if [ "$v" -lt "$new_version" ]; then
			peer_tag=$t
			echo "crossversion-build: no released tag stamps BackupFormatVersion=$new_version yet (a fresh bump); the same-format peer falls back to $t (BackupFormatVersion=$v) and cell 6 checks every manifest it writes against that ceiling" >&2
			break
		fi
	done
fi

if [ -z "$peer_tag" ]; then
	echo "::error::crossversion-build: no released tag at or below BackupFormatVersion=$new_version (excluding the orphan epochs: $peer_exclude) — the peer cell has nothing to run against. The tags are probably missing from this checkout (the workflow needs fetch-depth: 0). There is no skip flag, on purpose."
	exit 1
fi

peer_version=$(git show "$peer_tag:$manifest_file" | read_format_version)
if [ -z "$peer_version" ] || [ "$peer_version" -gt "$new_version" ]; then
	echo "::error::crossversion-build: peer tag $peer_tag stamps BackupFormatVersion=${peer_version:-?}, above the working tree's $new_version — it is not a previous release of this format."
	exit 1
fi

peer_worktree=${CROSSVER_PEER_WORKTREE:-${RUNNER_TEMP:-/tmp}/sluice-crossversion-peer}
if [ -e "$peer_worktree" ]; then
	git worktree remove --force "$peer_worktree" >/dev/null 2>&1 || rm -rf "$peer_worktree"
fi
git worktree prune
git worktree add --detach "$peer_worktree" "$peer_tag" >/dev/null

# ---------------------------------------------------------------------
# THE FOURTH AXIS: ONE BELOW-TIER BINARY PER SHAPE-STAMPED TIER.
#
# A tier suite (the positionless-full suite for 11, the exact-numbers
# suite for 12) asserts that a release BELOW its tier refuses what the
# tier stamps. Through FormatVersion 11 that release was simply OLD — and
# the next bump broke it: OLD advances to the newest release below the
# NEW top tier, which is AT the older tier and reads it (the 12 bump,
# 2026-09-25, made OLD v0.156.3 = format 11, and the positionless suite
# would have Fatal'd on its own non-vacuity guard). So each tier suite
# gets its own binary, derived against its OWN tier's constant read from
# this tree — the newest tag strictly below it — and never moves again
# when the top tier does. When it coincides with OLD it reuses OLD's
# build rather than compiling the same tag twice.
#
# read_tier NAME — the value of a FormatVersion constant in manifest.go.
read_tier() {
	sed -n "s/^[[:space:]]*$1 *= *\([0-9][0-9]*\).*/\1/p" "$manifest_file" | head -1
}

# below_tag TIER — "TAG VERSION" of the newest tag stamping below TIER.
below_tag() {
	for t in $(git tag --list 'v*' --sort=-v:refname); do
		v=$(git show "$t:$manifest_file" 2>/dev/null | read_format_version || true)
		[ -n "$v" ] || continue
		if [ "$v" -lt "$1" ]; then
			echo "$t $v"
			return 0
		fi
	done
	return 1
}

tier_suites="POSITIONLESS:FormatVersionPositionlessFull EXACT_NUMBERS:FormatVersionExactNumbers"
tier_lines=
for entry in $tier_suites; do
	name=${entry%%:*}
	const=${entry#*:}
	tier=$(read_tier "$const")
	if [ -z "$tier" ]; then
		echo "::error::crossversion-build: could not read $const from $manifest_file — the tier suite $name has no tier to derive a binary below."
		exit 1
	fi
	pair=$(below_tag "$tier" || true)
	if [ -z "$pair" ]; then
		echo "::error::crossversion-build: no tag stamps below $const=$tier — the $name tier suite would assert a refusal no binary can make."
		exit 1
	fi
	tier_lines="$tier_lines $name:$tier:${pair% *}:${pair#* }"
done

exe=
case "$(go env GOOS)" in
windows) exe=".exe" ;;
esac

mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)
old_bin="$out_dir/sluice-old$exe"
new_bin="$out_dir/sluice-new$exe"
epoch_bin="$out_dir/sluice-epoch$exe"
peer_bin="$out_dir/sluice-peer$exe"

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
echo "crossversion-build: building PEER $peer_tag (BackupFormatVersion=$peer_version) from $peer_worktree"
(cd "$peer_worktree" && go build -ldflags "-X main.version=${peer_tag#v}" -o "$peer_bin" ./cmd/sluice)
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
emit CROSSVER_PEER_BIN "$peer_bin"
emit CROSSVER_PEER_TAG "$peer_tag"
emit CROSSVER_PEER_FORMAT "$peer_version"
emit CROSSVER_PEER_WORKTREE "$peer_worktree"

# The below-tier binaries (see THE FOURTH AXIS above). Emitted as
# CROSSVER_BELOW_<NAME>_{BIN,TAG,FORMAT,TIER}.
for line in $tier_lines; do
	name=${line%%:*}
	rest=${line#*:}
	tier=${rest%%:*}
	rest=${rest#*:}
	tag=${rest%%:*}
	version=${rest#*:}
	if [ "$tag" = "$old_tag" ]; then
		bin=$old_bin
		echo "crossversion-build: BELOW_$name (tier $tier) is OLD $old_tag — reusing its build"
	else
		lower=$(echo "$name" | tr 'A-Z_' 'a-z-')
		bin="$out_dir/sluice-below-$lower$exe"
		wt=${RUNNER_TEMP:-/tmp}/sluice-crossversion-below-$lower
		if [ -e "$wt" ]; then
			git worktree remove --force "$wt" >/dev/null 2>&1 || rm -rf "$wt"
		fi
		git worktree prune
		git worktree add --detach "$wt" "$tag" >/dev/null
		echo "crossversion-build: building BELOW_$name $tag (BackupFormatVersion=$version, below tier $tier) from $wt"
		(cd "$wt" && go build -ldflags "-X main.version=${tag#v}" -o "$bin" ./cmd/sluice)
		emit "CROSSVER_BELOW_${name}_WORKTREE" "$wt"
	fi
	emit "CROSSVER_BELOW_${name}_BIN" "$bin"
	emit "CROSSVER_BELOW_${name}_TAG" "$tag"
	emit "CROSSVER_BELOW_${name}_FORMAT" "$version"
	emit "CROSSVER_BELOW_${name}_TIER" "$tier"
done
