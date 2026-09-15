#!/bin/sh
# check-changelog-heading.sh — a release tag's CHANGELOG must carry that
# release's own heading, not leave its entries under "Unreleased".
#
# Why this exists (2026-09-09, v0.148.2). The release flow writes the
# CHANGELOG entries first, under `## [Unreleased]`, and the release commit
# is supposed to promote them to `## [X.Y.Z] - DATE`. v0.148.2 shipped
# without that promotion: the tag's tree describes the release's own three
# fixes as unreleased. Nothing caught it — the notes-claims gate grades the
# release-notes ARCHIVE, and every other publish check reads the notes or
# the assets, so the CHANGELOG at the tag was ungated. The published notes
# were correct and complete, which is exactly why nobody noticed: the only
# wrong artifact was the one no gate read.
#
# It also fails when entries are left BEHIND under Unreleased at a tag —
# an empty (or heading-only) Unreleased section is the correct state for a
# release commit, because anything still listed there is a claim that the
# release does not contain work it actually shipped.
#
# TWO FORMS, because the tag form can only ever fire AFTER the tag is
# pushed (audit 2026-09-15 T-3): by then the remedy is a force-move, and in
# the known case where ci.yml does not trigger on the tag push at all it
# never runs. The working-tree form needs no tag. It reads the INDEX — what
# the commit about to be made will contain — and asks the same question of
# the newest archived notes file: `docs/releases/release-notes-vX.Y.Z.md`
# is committed in the release commit (CLAUDE.md, since 2026-08-31), so the
# newest such file names the newest release, and the CHANGELOG in the same
# index must carry `## [X.Y.Z]`. At v0.148.2's release commit that would
# have failed before the tag existed. When the newest notes file is being
# ADDED by this very commit, this IS the release commit, and the Unreleased
# section must be empty too — between releases it legitimately fills.
#
# Usage: check-changelog-heading.sh <tag> [changelog-path]      (CI, on a tag)
#        check-changelog-heading.sh --working-tree [changelog-path]  (pre-commit)
set -eu

FILE="${2:-CHANGELOG.md}"

# grade_body LABEL VERSION BODY REQUIRE_EMPTY_UNRELEASED
grade_body() {
	_label=$1
	_version=$2
	_body=$3
	_require_empty=$4

	if ! printf '%s\n' "$_body" | grep -q "^## \[$_version\]"; then
		echo "check-changelog-heading: FAIL — $_label's $FILE has no '## [$_version]' heading." >&2
		echo "  The release commit must promote this release's entries out of '## [Unreleased]'" >&2
		echo "  into '## [$_version] - <date>'. Without it the CHANGELOG at the tag describes the" >&2
		echo "  release's own changes as unreleased, which is the one artifact no other publish" >&2
		echo "  check reads (v0.148.2 shipped exactly this way)." >&2
		return 1
	fi

	[ "$_require_empty" = yes ] || return 0

	# Anything still under Unreleased at a release tag is work the release
	# shipped but does not claim. Read from the Unreleased heading to the next
	# level-2 heading and require it to hold no content lines.
	_leftover="$(printf '%s\n' "$_body" |
		awk '/^## \[Unreleased\]/ {inblock=1; next} /^## / {inblock=0} inblock' |
		grep -v '^[[:space:]]*$' || true)"
	if [ -n "$_leftover" ]; then
		echo "check-changelog-heading: FAIL — $_label's $FILE still lists content under '## [Unreleased]':" >&2
		printf '%s\n' "$_leftover" | sed 's/^/    /' | head -10 >&2
		echo "  Entries left there at a tag are work this release shipped without claiming." >&2
		return 1
	fi
	return 0
}

case "${1:-}" in
"")
	echo "usage: check-changelog-heading.sh <tag> [changelog-path]" >&2
	echo "       check-changelog-heading.sh --working-tree [changelog-path]" >&2
	exit 2
	;;
--working-tree)
	# The newest archived notes file in the index names the newest release.
	# Pre-release archives (v1.2.3-rc1) are not graded by the tag form either.
	versions="$(git ls-files --cached -- 'docs/releases/release-notes-v*.md' |
		sed -n 's|^docs/releases/release-notes-v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)\.md$|\1|p' |
		sort -V)"
	nversions="$(printf '%s\n' "$versions" | grep -c . || true)"
	# Anti-vacuity: the archive holds hundreds of files; a listing that came
	# back nearly empty is a broken derivation, not a young repository.
	if [ "${nversions:-0}" -lt 20 ]; then
		echo "check-changelog-heading: FAIL (vacuous) — found only ${nversions:-0} archived release-notes files under docs/releases/;" >&2
		echo "  the archive carries hundreds, so the listing or the filename parse broke. Fix that rather than passing on nothing." >&2
		exit 2
	fi
	newest="$(printf '%s\n' "$versions" | tail -n 1)"
	BODY="$(git show ":$FILE" 2>/dev/null || true)"
	if [ -z "$BODY" ]; then
		echo "check-changelog-heading: $FILE is absent from the index" >&2
		exit 1
	fi
	require_empty=no
	if git diff --cached --name-only --diff-filter=A 2>/dev/null |
		grep -qx "docs/releases/release-notes-v$newest.md"; then
		require_empty=yes
	fi
	grade_body "the index (newest archived notes: v$newest)" "$newest" "$BODY" "$require_empty" || exit 1
	if [ "$require_empty" = yes ]; then
		echo "check-changelog-heading: OK — the staged release commit for v$newest carries '## [$newest]' and its Unreleased section is empty."
	else
		echo "check-changelog-heading: OK — the index carries '## [$newest]' for the newest archived notes (v$newest)."
	fi
	exit 0
	;;
esac

TAG="$1"
VERSION="${TAG#v}"

if ! git rev-parse -q --verify "$TAG^{commit}" >/dev/null 2>&1; then
	echo "check-changelog-heading: $TAG does not resolve to a commit" >&2
	exit 1
fi

BODY="$(git show "$TAG:$FILE" 2>/dev/null || true)"
if [ -z "$BODY" ]; then
	echo "check-changelog-heading: $FILE is absent from $TAG's tree" >&2
	exit 1
fi

grade_body "$TAG" "$VERSION" "$BODY" yes || exit 1

echo "check-changelog-heading: OK — $TAG carries '## [$VERSION]' and its Unreleased section is empty."
