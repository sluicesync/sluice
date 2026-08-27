#!/bin/sh
# check-notes-claims.sh <tag> <notes-file> — the NOTES-CLAIMS-VS-TAG-TREE gate
# (audit backlog 2026-08-27, the v0.132.1 missed-landing incident's ratchet).
#
# The published v0.132.1 notes claimed code the tag did not carry: a landing
# session's cwd silently drifted into an agent worktree, so several commits
# landed on a worktree branch while main-tree greps and tests read mixed
# state, and nothing between "notes curated" and "publish" ever asked the TAG
# whether it contained what the notes said. The post-publish learnings sweep's
# manual `git show <tag>:<file>` ground-truthing caught it; this script is
# that check made mechanical, run as a pre-publish step (CLAUDE.md "Publish
# via Option B gate", sub-check 4b).
#
# What it does: extracts load-bearing IDENTIFIERS from the release notes —
# error codes (SLUICE-E-*), file names (*.go/*.md/*.log/*.sh), Go test names
# (TestXxx), and ALL-CAPS-HYPHENATED markers (e.g. DDL-DETECTION-ABSENT) —
# and asserts each one exists in the tag's tree: by CONTENT (`git grep`)
# first, then by FILE NAME (`git ls-tree`). The filename fallback is
# load-bearing, not belt: `git grep` matches content only, and a file like
# probe_timeout.go can exist at a tag while no tracked line mentions it
# (hand-verified on v0.132.2 — the prototype run's lesson).
#
# SCOPE, stated so the name cannot be read as broader than the truth: this
# gate catches a notes claim that NAMES an identifier absent from the tag. A
# purely behavioral claim about a pre-existing identifier — which is what
# most of v0.132.1's own missed prose was, since the missing commits' error
# codes already existed from v0.132.0 — is not detectable here. The gate
# NARROWS the missed-landing class (any notes naming a new file / test /
# code / marker now fails loudly); the cwd-anchoring landing rules in the
# audit backlog remain the primary defense for the rest.
#
# Identifiers that legitimately are NOT in the tag's tree (e.g. a removed
# file the notes mention by name) are exempted inline in the notes:
#
#   <!-- notes-claims-exempt: old_file.go SOME-RETIRED-MARKER -->
#
# (comma- or space-separated; multiple exemption lines allowed.)
#
# Anti-vacuity floor: fewer than 3 extracted identifiers fails the run as
# vacuous — curated sluice notes always name at least error codes or tests,
# so a near-empty extraction means the extraction broke (or the wrong file
# was passed), and a gate that silently checks nothing is worse than none.
#
# Usage: sh scripts/check-notes-claims.sh v0.132.2 docs/releases/release-notes-v0.132.2.md

set -eu

if [ $# -ne 2 ]; then
	echo "usage: $0 <tag> <notes-file>" >&2
	exit 2
fi
TAG="$1"
NOTES="$2"

if ! git rev-parse -q --verify "$TAG^{commit}" >/dev/null; then
	echo "check-notes-claims: FAIL — tag $TAG does not resolve to a commit" >&2
	exit 1
fi
if [ ! -f "$NOTES" ]; then
	echo "check-notes-claims: FAIL — notes file not found: $NOTES" >&2
	exit 1
fi

# ---- extraction ----
# Four identifier shapes, matched anywhere in the notes text (markdown
# backticks are the usual carrier but are not required — error codes and
# markers are load-bearing wherever they appear, and requiring backticks
# left the v0.132.2 self-test corpus below the anti-vacuity floor).
#   1. SLUICE-E error codes.
#   2. File names ending .go/.md/.log/.sh (dots allowed inside, so a
#      versioned name like release-notes-v0.132.1.md extracts whole).
#   3. Go test names — Test followed by an UPPERCASE letter, so prose
#      words ("Tests", "Testing") never extract.
#   4. ALL-CAPS-HYPHENATED markers (>=2 chars per segment, >=2 segments).
extracted=$(
	{
		grep -oE 'SLUICE-E-[A-Z][A-Z-]*[A-Z]' "$NOTES" || true
		grep -oE '[A-Za-z0-9_.-]*[A-Za-z0-9_-]\.(go|md|log|sh)' "$NOTES" || true
		grep -oE 'Test[A-Z][A-Za-z0-9_]+' "$NOTES" || true
		grep -oE '[A-Z][A-Z0-9]+(-[A-Z0-9]+)+' "$NOTES" || true
	} | sort -u
)

count=$(printf '%s\n' "$extracted" | grep -c . || true)
if [ "$count" -lt 3 ]; then
	echo "check-notes-claims: FAIL (vacuous) — only $count identifier(s) extracted from $NOTES; the floor is 3." >&2
	echo "Either the notes name nothing load-bearing (unlikely for curated notes) or the extraction broke — inspect before publishing." >&2
	exit 1
fi

# ---- inline exemptions ----
exempt=$(sed -n 's/.*<!-- notes-claims-exempt: *\(.*\) *-->.*/\1/p' "$NOTES" | tr ',' ' ')

is_exempt() {
	for e in $exempt; do
		[ "$e" = "$1" ] && return 0
	done
	return 1
}

# ---- per-identifier tag-tree check ----
missing=0
checked=0
tree_names=$(git ls-tree -r --name-only "$TAG")
for ident in $extracted; do
	checked=$((checked + 1))
	# Content match first (fixed-string: identifiers carry regex
	# metacharacters like dots and brackets).
	if git grep -q -F -e "$ident" "$TAG" -- 2>/dev/null; then
		continue
	fi
	# Filename fallback: git grep matches CONTENT, not names, and a pure
	# file-name claim (probe_timeout.go) can be real while un-mentioned.
	if printf '%s\n' "$tree_names" | grep -qF "$ident"; then
		continue
	fi
	if is_exempt "$ident"; then
		echo "check-notes-claims: exempt (declared in notes): $ident"
		continue
	fi
	echo "check-notes-claims: MISSING from $TAG: $ident" >&2
	missing=$((missing + 1))
done

if [ "$missing" -gt 0 ]; then
	echo "check-notes-claims: FAIL — $missing of $checked notes identifier(s) do not exist in $TAG's tree." >&2
	echo "The notes claim code the tag does not carry (the v0.132.1 missed-landing shape)." >&2
	echo "Either land the missing work and re-tag (draft-only), fix the notes, or add a" >&2
	echo "<!-- notes-claims-exempt: ... --> line for an identifier that is legitimately not in-tree." >&2
	exit 1
fi

echo "check-notes-claims: OK — $checked identifier(s) from $NOTES all present in $TAG"
