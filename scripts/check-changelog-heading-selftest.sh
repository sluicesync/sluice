#!/bin/sh
# check-changelog-heading-selftest.sh — the CHANGELOG-heading gate's own gate
# (audit 2026-09-15 T-3).
#
# scripts/check-changelog-heading.sh shipped with no self-test, wired only
# into the tag-triggered Lint step — the one place it can fire after the
# remedy has become a force-move. This proves BOTH of its forms in both
# directions, on a fixture that is CROSS-VERSION rather than self-referential:
# the shapes below are v0.148.2's real CHANGELOG (entries left under
# `## [Unreleased]`, no `## [0.148.2]` heading — the defect the gate was
# built for) and v0.148.1's (promoted heading, empty Unreleased), reduced to
# the lines the gate reads. A hermetic throwaway repo replays them as tags
# and as staged release commits:
#
#   tag form
#   1 promoted            -> exit 0 (v0.148.1's shape)
#   2 unpromoted          -> exit 1, "has no '## [9.9.2]' heading" (v0.148.2's shape)
#   3 leftover            -> exit 1, "still lists content under '## [Unreleased]'"
#
#   working-tree form (the pre-commit hook's)
#   4 release commit ok   -> exit 0, graded as the staged release commit
#   5 release commit bad  -> exit 1 (v0.148.2's shape at the commit that made
#                            it, BEFORE any tag exists — the whole point)
#   6 between releases    -> exit 0 with Unreleased non-empty: the newest
#                            notes file is not being added, so the fill is
#                            legitimate and must NOT be refused
#   7 vacuous             -> exit 2 when the archive listing is implausibly
#                            small (the floor)
#
# Then, when the real tags resolve (a full clone; CI's Lint checkout is
# shallow and tagless), the real pair is graded too: v0.148.2 must FAIL and
# v0.148.1 must pass. A shallow checkout prints a NOTE and relies on the
# hermetic cases, which carry the same shapes.
#
# Runs in ci.yml's Lint job and both pre-commit hooks. Hermetic: everything
# happens in a temp repo; no tag or commit is ever created in this one.

set -eu

# A pre-commit hook exports GIT_DIR / GIT_INDEX_FILE; without this the temp
# repo's commits would land in the REAL repository's index. Non-negotiable.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
	GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_COMMON_DIR GIT_PREFIX 2>/dev/null || true

here=$(cd "$(dirname "$0")/.." && pwd)
SCRIPT=$here/scripts/check-changelog-heading.sh
[ -f "$SCRIPT" ] || {
	echo "check-changelog-heading-selftest: FAIL — $SCRIPT not found" >&2
	exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
repo=$work/repo
mkdir -p "$repo/docs/releases"

git -C "$repo" init -q -b main
git -C "$repo" config user.email selftest@sluice.invalid
git -C "$repo" config user.name "changelog-heading selftest"
git -C "$repo" config commit.gpgsign false
git -C "$repo" config core.autocrlf false

# The archive floor is 20 files; seed 24 older releases so the working-tree
# form has a plausible archive and its newest file is the one under test.
i=1
while [ "$i" -le 24 ]; do
	printf '# sluice v9.8.%s\n' "$i" >"$repo/docs/releases/release-notes-v9.8.$i.md"
	i=$((i + 1))
done

# CHANGELOG shapes, reduced from the real files.
promoted='# Changelog

## [Unreleased]

## [%s] - 2026-09-09

### Fixed

**A fix the release claims.**

## [9.8.24] - 2026-09-01

Older.
'
unpromoted='# Changelog

## [Unreleased]

### Fixed

**A fix left under Unreleased at the tag (v0.148.2 shape).**

## [9.8.24] - 2026-09-01

Older.
'
leftover='# Changelog

## [Unreleased]

**Work this release shipped without claiming.**

## [%s] - 2026-09-09

### Fixed

**A fix the release claims.**
'

# shellcheck disable=SC2059  # the shapes carry a %s on purpose
write_changelog() { printf "$1" "${2:-}" >"$repo/CHANGELOG.md"; }

fail=0
out=""
# run_tag <name> <tag> <want-exit>
run_tag() {
	_name=$1
	_tag=$2
	_want=$3
	out=$( (cd "$repo" && sh "$SCRIPT" "$_tag" 2>&1) ) && _got=0 || _got=$?
	check_exit "$_name" "$_got" "$_want"
}
# run_tree <name> <want-exit>
run_tree() {
	_name=$1
	_want=$2
	out=$( (cd "$repo" && sh "$SCRIPT" --working-tree 2>&1) ) && _got=0 || _got=$?
	check_exit "$_name" "$_got" "$_want"
}
check_exit() {
	if [ "$2" -ne "$3" ]; then
		echo "check-changelog-heading-selftest: FAIL [$1] — exit $2, want $3" >&2
		printf '%s\n' "$out" | sed 's/^/    | /' >&2
		fail=1
		return 1
	fi
	echo "check-changelog-heading-selftest: ok [$1] exit $3"
	return 0
}
expect() {
	if ! printf '%s\n' "$out" | grep -qF -- "$2"; then
		echo "check-changelog-heading-selftest: FAIL [$1] — output missing: $2" >&2
		printf '%s\n' "$out" | sed 's/^/    | /' >&2
		fail=1
	fi
}

# ---- seed: an older release, committed and tagged ----
write_changelog "$promoted" 9.8.24
git -C "$repo" add -A
git -C "$repo" commit -q --no-verify -m "release: v9.8.24"
git -C "$repo" tag v9.8.24

# ---- case 1 / 4: a correctly promoted release commit ----
printf '# sluice v9.9.1\n' >"$repo/docs/releases/release-notes-v9.9.1.md"
write_changelog "$promoted" 9.9.1
git -C "$repo" add -A
run_tree release-commit-ok 0 && expect release-commit-ok "staged release commit for v9.9.1"
git -C "$repo" commit -q --no-verify -m "release: v9.9.1"
git -C "$repo" tag v9.9.1
run_tag promoted v9.9.1 0

# ---- case 5 / 2: v0.148.2's shape — notes archived, heading never promoted ----
printf '# sluice v9.9.2\n' >"$repo/docs/releases/release-notes-v9.9.2.md"
write_changelog "$unpromoted"
git -C "$repo" add -A
run_tree release-commit-unpromoted 1 && expect release-commit-unpromoted "has no '## [9.9.2]' heading"
git -C "$repo" commit -q --no-verify -m "release: v9.9.2 (unpromoted)"
git -C "$repo" tag v9.9.2
run_tag unpromoted v9.9.2 1 && expect unpromoted "has no '## [9.9.2]' heading"

# ---- case 3: heading promoted but content left under Unreleased ----
printf '# sluice v9.9.3\n' >"$repo/docs/releases/release-notes-v9.9.3.md"
write_changelog "$leftover" 9.9.3
git -C "$repo" add -A
run_tree release-commit-leftover 1 && expect release-commit-leftover "still lists content under '## [Unreleased]'"
git -C "$repo" commit -q --no-verify -m "release: v9.9.3 (leftover)"
git -C "$repo" tag v9.9.3
run_tag leftover v9.9.3 1 && expect leftover "still lists content under '## [Unreleased]'"

# ---- case 6: between releases, Unreleased fills legitimately ----
# Repair v9.9.3's CHANGELOG to the promoted shape first, then stage ordinary
# work under Unreleased with NO new notes file: not a release commit.
write_changelog "$promoted" 9.9.3
git -C "$repo" add -A
git -C "$repo" commit -q --no-verify -m "changelog: promote 9.9.3"
printf '# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n**In-progress work.**\n\n## [9.9.3] - 2026-09-09\n\nReleased.\n' >"$repo/CHANGELOG.md"
git -C "$repo" add -A
run_tree between-releases 0 && expect between-releases "the index carries '## [9.9.3]'"

# ---- case 7: the archive floor ----
# Remove the seeded archive from the index so the listing is implausibly
# small; the gate must refuse rather than grade whatever is left.
git -C "$repo" rm -q --cached 'docs/releases/release-notes-v9.8.*' >/dev/null
run_tree vacuous 2 && expect vacuous "FAIL (vacuous)"

# ---- the real cross-version pair, when the tags resolve ----
if git -C "$here" rev-parse -q --verify v0.148.2^{commit} >/dev/null 2>&1 &&
	git -C "$here" rev-parse -q --verify v0.148.1^{commit} >/dev/null 2>&1; then
	out=$( (cd "$here" && sh "$SCRIPT" v0.148.2 2>&1) ) && _got=0 || _got=$?
	check_exit real-v0.148.2-fails "$_got" 1 && expect real-v0.148.2-fails "has no '## [0.148.2]' heading"
	out=$( (cd "$here" && sh "$SCRIPT" v0.148.1 2>&1) ) && _got=0 || _got=$?
	check_exit real-v0.148.1-passes "$_got" 0
else
	echo "check-changelog-heading-selftest: NOTE — v0.148.2 / v0.148.1 do not resolve here (shallow or tagless checkout); the real cross-version pair was not graded, the hermetic replicas of both shapes were."
fi

if [ "$fail" -ne 0 ]; then
	echo "check-changelog-heading-selftest: FAILED — the changelog-heading gate does not behave as documented." >&2
	exit 1
fi
echo "check-changelog-heading-selftest: all cases behave as documented (both forms, both directions, the between-releases fill, the archive floor)."
