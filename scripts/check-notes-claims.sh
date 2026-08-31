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
# that check made mechanical, run on every release tag by ci.yml's Lint job
# and listed as pre-publish sub-check 4b in CLAUDE.md.
#
# What it does: extracts load-bearing IDENTIFIERS from the release notes —
# error codes (SLUICE-E-*), file names (*.go/*.md/*.log/*.sh), Go test names
# (TestXxx), ALL-CAPS-HYPHENATED markers (e.g. DDL-DETECTION-ABSENT), and
# backticked camelCase Go symbols (sessionTZSwapPair) — and asserts each one
# exists in the tag's tree: by CONTENT (`git grep`, scoped — see below)
# first, then by FILE NAME (`git ls-tree`). The filename fallback is
# load-bearing, not belt: `git grep` matches content only, and a file like
# probe_timeout.go can exist at a tag while no tracked line mentions it
# (hand-verified on v0.132.2, and again on v0.134.1's
# preflight_definer_search_path.go — the only corpus identifier that needs it).
#
# ---------------------------------------------------------------------------
# THE EVIDENCE SCOPE, and why it is an ALLOWLIST (audit 2026-08-31 T-1)
# ---------------------------------------------------------------------------
# The first cut of this script ran the content grep against the WHOLE tag
# tree. The release commit a tag points at is precisely the commit that adds
# `docs/releases/release-notes-<tag>.md` and the CHANGELOG entry — verified
# present at 25/25 of the last 25 tags — so every identifier the notes named
# matched its own source text and the gate structurally could not fail for
# the class it was built for. Two audit workers found it independently; one
# built a throwaway tag whose notes named four fabricated identifiers and
# watched the gate report OK.
#
# The fix is a positive ALLOWLIST, not a `:!docs/releases/` exclusion pair,
# because the notes file is not the only prose the release commit writes:
# `CHANGELOG.md`, `CLAUDE.md` and `docs/dev/audit-backlog.md` routinely land
# in the SAME commit describing the SAME change, and each is a self-satisfier
# of exactly the same shape (observed: at v0.133.0, `check-notes-claims.sh`
# resolved from CHANGELOG.md, CLAUDE.md, the audit backlog and the notes
# themselves). An exclusion list has to enumerate every prose home anyone
# might add next; an allowlist defaults to deny, so a new doc directory
# cannot quietly become evidence.
#
#   EVIDENCE_SCOPE = internal cmd scripts .github docs/adr
#
# `internal`/`cmd` are the implementation; `scripts` and `.github` are the
# gates and workflows the notes also legitimately name; `docs/adr` is there
# because an `ADR-####` claim is a claim that the ADR *artifact* exists, and
# the artifact — not the prose about it — is what satisfies it. `docs/`
# generally is NOT in scope: a notes claim satisfied only by the release's
# own documentation is the defect this gate exists to catch. Measured on the
# six delta releases v0.132.1 … v0.134.1: 34 identifiers, all resolved — 33
# on scoped content, one (v0.134.1's preflight_definer_search_path.go) on
# the filename leg below.
#
# The FILENAME fallback stays tree-wide (a file's existence anywhere is
# independent evidence regardless of directory) except for the notes file
# being graded, which exists by construction and can only satisfy itself.
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
# file the notes mention by name, or a foreign symbol quoted from another
# system) are exempted inline in the notes:
#
#   <!-- notes-claims-exempt: old_file.go SOME-RETIRED-MARKER -->
#
# (comma- or space-separated; multiple exemption lines allowed.)
#
# ANTI-VACUITY FLOOR. Two parts, because "3 identifiers of any shape" was
# satisfiable by tokens that cannot fail — `CHANGELOG.md` is in every tag by
# construction, and v0.132.2's real run met the old floor with two genuine
# claims plus that one (audit 2026-08-31 F-T5):
#   (a) at least 2 identifiers that are not ubiquitous-by-construction, and
#   (b) at least 1 identifier from the CODE-shaped classes (error code, test
#       name, *.go/*.sh, camelCase symbol, non-ADR marker).
# A release whose notes genuinely name no code — a docs-only cut — declares
# it, with a reason, rather than being silently waved through:
#
#   <!-- notes-claims-no-code-claims: docs-only release, no code changed -->
#
# The gate's own gate is scripts/check-notes-claims-selftest.sh, which runs
# in ci.yml's Lint job and both pre-commit hooks: it builds throwaway tags
# whose notes+CHANGELOG are COMMITTED IN THE TAG (the shape the real flow
# produces, and the shape the original hand-doctored negative fixture never
# reached) and asserts both directions.
#
# Usage: sh scripts/check-notes-claims.sh v0.134.1 docs/releases/release-notes-v0.134.1.md

set -eu

if [ $# -ne 2 ]; then
	echo "usage: $0 <tag> <notes-file>" >&2
	exit 2
fi
TAG="$1"
NOTES="$2"

# The content-evidence allowlist (see the header). Word-split deliberately
# into `git grep`'s pathspec arguments; no element contains a glob or space.
EVIDENCE_SCOPE="internal cmd scripts .github docs/adr"

if ! git rev-parse -q --verify "$TAG^{commit}" >/dev/null; then
	echo "check-notes-claims: FAIL — tag $TAG does not resolve to a commit" >&2
	exit 1
fi
if [ ! -f "$NOTES" ]; then
	echo "check-notes-claims: FAIL — notes file not found: $NOTES" >&2
	exit 1
fi

# ---- extraction ----
# Five identifier shapes. Shapes 1-4 are matched anywhere in the notes text:
# markdown backticks are the usual carrier but are not required, and an error
# code or marker is load-bearing wherever it appears.
#   1. SLUICE-E error codes.
#   2. File names ending .go/.md/.log/.sh (dots allowed inside, so a
#      versioned name like release-notes-v0.132.1.md extracts whole).
#   3. Go test names — Test followed by an UPPERCASE letter, so prose
#      words ("Tests", "Testing") never extract; word-anchored, so a
#      camelCase symbol like realSelfTestSymbol does not shed a phantom
#      `TestSymbol` claim that pads the floor.
#   4. ALL-CAPS-HYPHENATED markers (>=2 chars per segment, >=2 segments).
#   5. camelCase Go symbols — `sessionTZSwapPair`, the way these notes
#      usually name new code, and the shape the first cut was blind to
#      (audit 2026-08-31 D-2(b), mutation-proven: renaming
#      sessionTZSwapPair to a symbol existing nowhere still passed).
#      This one IS backtick-scoped, unlike 1-4, and deliberately: a bare
#      camelCase word in prose is ambiguous, and fenced code blocks — which
#      carry OTHER systems' output (D1 JSON keys, PG error text) — contain
#      no inline backticks, so scoping to inline code spans keeps foreign
#      camelCase out of the claim set. sluice's notes backtick their own
#      symbols, so nothing real is lost.
ids_codes=$(grep -oE 'SLUICE-E-[A-Z][A-Z-]*[A-Z]' "$NOTES" || true)
ids_files=$(grep -oE '[A-Za-z0-9_.-]*[A-Za-z0-9_-]\.(go|md|log|sh)' "$NOTES" || true)
ids_tests=$(grep -owE 'Test[A-Z][A-Za-z0-9_]+' "$NOTES" || true)
ids_markers=$(grep -oE '[A-Z][A-Z0-9]+(-[A-Z0-9]+)+' "$NOTES" || true)
ids_symbols=$(grep -oE '`[^`]*`' "$NOTES" | tr -d '`' | grep -owE '[a-z][A-Za-z0-9_]*[A-Z][A-Za-z0-9_]*' || true)

extracted=$(
	printf '%s\n%s\n%s\n%s\n%s\n' \
		"$ids_codes" "$ids_files" "$ids_tests" "$ids_markers" "$ids_symbols" |
		grep -v '^$' | sort -u || true
)

# The CODE-shaped subset, for floor part (b): everything except plain doc
# file names and bare ADR references, which name artifacts rather than code.
codeshaped=$(
	{
		printf '%s\n%s\n%s\n' "$ids_codes" "$ids_tests" "$ids_symbols"
		printf '%s\n' "$ids_files" | grep -E '\.(go|sh)$' || true
		printf '%s\n' "$ids_markers" | grep -vE '^ADR-[0-9]+$' || true
	} | grep -v '^$' | sort -u || true
)

# Tokens present in every tag of this repository by construction; they are
# still CHECKED, they just prove nothing, so they do not count toward the
# floor.
UBIQUITOUS="CHANGELOG.md README.md CLAUDE.md AGENTS.md CONTRIBUTING.md SECURITY.md CODE_OF_CONDUCT.md"

is_ubiquitous() {
	for u in $UBIQUITOUS; do
		[ "$u" = "$1" ] && return 0
	done
	return 1
}

distinct_count=0
for ident in $extracted; do
	is_ubiquitous "$ident" || distinct_count=$((distinct_count + 1))
done
code_count=$(printf '%s\n' "$codeshaped" | grep -c . || true)
total_count=$(printf '%s\n' "$extracted" | grep -c . || true)

if [ "$distinct_count" -lt 2 ]; then
	echo "check-notes-claims: FAIL (vacuous) — $total_count identifier(s) extracted from $NOTES, only $distinct_count of them non-ubiquitous; the floor is 2." >&2
	echo "Either the notes name nothing load-bearing (unlikely for curated notes) or the extraction broke — inspect before publishing." >&2
	echo "Ubiquitous-by-construction tokens that do not count: $UBIQUITOUS" >&2
	exit 1
fi

no_code_waiver=$(sed -n 's/.*<!-- notes-claims-no-code-claims: *\(.*[^ ]\) *-->.*/\1/p' "$NOTES" | head -1)
if [ "$code_count" -lt 1 ]; then
	if [ -n "$no_code_waiver" ]; then
		echo "check-notes-claims: no code-shaped claim, waived in the notes: $no_code_waiver"
	else
		echo "check-notes-claims: FAIL (vacuous) — $distinct_count non-ubiquitous identifier(s) but none is code-shaped (error code / test / *.go / *.sh / camelCase symbol / marker)." >&2
		echo "A release whose notes genuinely claim no code declares it: add" >&2
		echo "  <!-- notes-claims-no-code-claims: <reason> -->" >&2
		exit 1
	fi
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
# The filename leg drops the graded notes file itself: it is in the tag by
# construction, so it can only ever satisfy itself.
notes_base=$(basename "$NOTES")
tree_names=$(git ls-tree -r --name-only "$TAG" | grep -v "/${notes_base}\$" | grep -vxF "$notes_base" || true)

missing=0
checked=0
for ident in $extracted; do
	checked=$((checked + 1))
	# Content match first, scoped to the implementation allowlist
	# (fixed-string: identifiers carry regex metacharacters like dots).
	# shellcheck disable=SC2086 # EVIDENCE_SCOPE is a deliberate pathspec list
	if git grep -q -F -e "$ident" "$TAG" -- $EVIDENCE_SCOPE 2>/dev/null; then
		echo "check-notes-claims:   content   $ident"
		continue
	fi
	# Filename fallback: git grep matches CONTENT, not names, and a pure
	# file-name claim (probe_timeout.go) can be real while un-mentioned.
	if printf '%s\n' "$tree_names" | grep -qF "$ident"; then
		echo "check-notes-claims:   filename  $ident"
		continue
	fi
	if is_exempt "$ident"; then
		echo "check-notes-claims:   exempt    $ident (declared in notes)"
		continue
	fi
	echo "check-notes-claims: MISSING from $TAG: $ident" >&2
	missing=$((missing + 1))
done

if [ "$missing" -gt 0 ]; then
	echo "check-notes-claims: FAIL — $missing of $checked notes identifier(s) do not exist in $TAG's tree." >&2
	echo "Evidence scope (content): $EVIDENCE_SCOPE — prose homes written by the release commit itself (the notes, CHANGELOG.md, CLAUDE.md, docs/dev/) are deliberately NOT evidence." >&2
	echo "The notes claim code the tag does not carry (the v0.132.1 missed-landing shape)." >&2
	echo "Either land the missing work and re-tag (draft-only), fix the notes, or add a" >&2
	echo "<!-- notes-claims-exempt: ... --> line for an identifier that is legitimately not in-tree." >&2
	exit 1
fi

echo "check-notes-claims: OK — $checked identifier(s) from $NOTES all present in $TAG ($distinct_count non-ubiquitous, $code_count code-shaped; content scope: $EVIDENCE_SCOPE)"
