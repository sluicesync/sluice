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
# Usage: check-changelog-heading.sh <tag> [changelog-path]
set -eu

TAG="${1:?usage: check-changelog-heading.sh <tag> [changelog-path]}"
FILE="${2:-CHANGELOG.md}"
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

if ! printf '%s\n' "$BODY" | grep -q "^## \[$VERSION\]"; then
	echo "check-changelog-heading: FAIL — $TAG's $FILE has no '## [$VERSION]' heading." >&2
	echo "  The release commit must promote this release's entries out of '## [Unreleased]'" >&2
	echo "  into '## [$VERSION] - <date>'. Without it the CHANGELOG at the tag describes the" >&2
	echo "  release's own changes as unreleased, which is the one artifact no other publish" >&2
	echo "  check reads." >&2
	exit 1
fi

# Anything still under Unreleased at a release tag is work the release
# shipped but does not claim. Read from the Unreleased heading to the next
# level-2 heading and require it to hold no content lines.
LEFTOVER="$(printf '%s\n' "$BODY" |
	awk '/^## \[Unreleased\]/ {inblock=1; next} /^## / {inblock=0} inblock' |
	grep -v '^[[:space:]]*$' || true)"
if [ -n "$LEFTOVER" ]; then
	echo "check-changelog-heading: FAIL — $TAG's $FILE still lists content under '## [Unreleased]':" >&2
	printf '%s\n' "$LEFTOVER" | sed 's/^/    /' | head -10 >&2
	echo "  Entries left there at a tag are work this release shipped without claiming." >&2
	exit 1
fi

echo "check-changelog-heading: OK — $TAG carries '## [$VERSION]' and its Unreleased section is empty."
