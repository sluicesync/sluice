#!/bin/sh
# check-notes-claims-selftest.sh — the gate's own gate (audit 2026-08-31 T-1).
#
# scripts/check-notes-claims.sh shipped without one, and that is precisely how
# it shipped self-referential: its recorded "mutation-run both directions"
# proof used a hand-doctored notes file that was NEVER COMMITTED, so the
# fabricated identifiers were genuinely absent from the tag. The real flow
# commits the notes (and a CHANGELOG entry repeating them) in the very commit
# the tag points at, and against that shape the gate reported OK on four
# fabricated identifiers. The negative leg had been measured under conditions
# the positive leg can never reach.
#
# So this self-test builds the SHAPE THE RELEASE FLOW ACTUALLY PRODUCES: a
# throwaway repo whose every case commits notes + CHANGELOG and then tags
# them. Seven cases, both directions on each mechanism:
#
#   1 honest            -> exit 0, and the run must show the camelCase shape
#                          and the filename fallback actually firing
#   2 fabricated        -> exit 1, all four fabricated identifiers reported
#                          MISSING (the pathspec-scope mutation target: revert
#                          the allowlist and this case passes)
#   3 vacuous           -> exit 1 (ubiquitous-only notes clear no floor)
#   4 no code claims    -> exit 1 (floor part (b))
#   5 no code + waiver  -> exit 0 (the declared docs-only escape)
#   6 fabricated+exempt -> exit 0 (the inline exemption, paired with case 2)
#   7 notes name self   -> exit 1 (the filename leg cannot self-satisfy either)
#
# Runs in ci.yml's Lint job and both pre-commit hooks. Hermetic: everything
# happens in a temp repo, no tag or commit is ever created in this one.

set -eu

# A pre-commit hook exports GIT_DIR / GIT_INDEX_FILE; without this the temp
# repo's commits would land in the REAL repository's index. Non-negotiable.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
	GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_COMMON_DIR GIT_PREFIX 2>/dev/null || true

SCRIPT=$(cd "$(dirname "$0")" && pwd)/check-notes-claims.sh
[ -f "$SCRIPT" ] || {
	echo "check-notes-claims-selftest: FAIL — $SCRIPT not found" >&2
	exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
repo=$work/repo
bodies=$work/bodies
mkdir -p "$repo" "$bodies"

git -C "$repo" init -q -b main
git -C "$repo" config user.email selftest@sluice.invalid
git -C "$repo" config user.name "notes-claims selftest"
git -C "$repo" config commit.gpgsign false
# Windows checkouts default to autocrlf=true, which floods the run with
# line-ending warnings and changes nothing this gate looks at.
git -C "$repo" config core.autocrlf false

mkdir -p "$repo/internal/engines/pgtrigger" "$repo/scripts" "$repo/docs/adr" "$repo/docs/releases"

# The implementation the notes may legitimately cite. Everything an honest
# notes file claims is grounded HERE, inside the evidence allowlist.
cat >"$repo/internal/engines/pgtrigger/reader.go" <<'EOF'
package pgtrigger

// SLUICE-E-SELFTEST-REAL is the refusal this fixture's honest notes claim.
// The REAL-SELFTEST-MARKER marker is emitted alongside it.
func realSelfTestSymbol() string { return "SLUICE-E-SELFTEST-REAL REAL-SELFTEST-MARKER" }

func TestlessHelper() {}
EOF

# Exists in the tree, mentioned by no tracked line: the filename fallback's
# only reason to exist (v0.134.1's preflight_definer_search_path.go shape).
cat >"$repo/internal/engines/pgtrigger/quiet_file.go" <<'EOF'
package pgtrigger
EOF

cat >"$repo/scripts/selftest-guard.sh" <<'EOF'
#!/bin/sh
echo TestSelfTestRealGate
EOF

cat >"$repo/docs/adr/adr-0999-selftest.md" <<'EOF'
# ADR-0999 — selftest fixture
EOF

# The ubiquitous-by-construction tokens, present exactly as they are in the
# real repo: outside the evidence allowlist, so they resolve only by FILE
# NAME — which is what makes case 3 a real mutation target for the floor.
cat >"$repo/README.md" <<'EOF'
See CHANGELOG.md.
EOF

cat >"$repo/CLAUDE.md" <<'EOF'
Working agreements.
EOF

git -C "$repo" add -A
git -C "$repo" commit -q --no-verify -m "fixture: the implementation"

fail=0

# tag_notes <tag> <notes-body-file> — reproduce the release-commit shape:
# archive the notes AND write a CHANGELOG entry repeating them, in one
# commit, then tag it. Both are self-satisfiers by construction; a gate that
# accepts them as evidence passes every case below.
tag_notes() {
	_tag=$1
	_body=$2
	cp "$_body" "$repo/docs/releases/release-notes-$_tag.md"
	{
		echo "## $_tag"
		cat "$_body"
	} >"$repo/CHANGELOG.md"
	git -C "$repo" add -A
	git -C "$repo" commit -q --no-verify -m "release: $_tag"
	git -C "$repo" tag "$_tag"
}

# run_case <name> <tag> <want-exit>; leaves the combined output in $out.
out=""
run_case() {
	_name=$1
	_tag=$2
	_want=$3
	out=$( (cd "$repo" && sh "$SCRIPT" "$_tag" "docs/releases/release-notes-$_tag.md" 2>&1) ) && _got=0 || _got=$?
	if [ "$_got" -ne "$_want" ]; then
		echo "check-notes-claims-selftest: FAIL [$_name] — exit $_got, want $_want" >&2
		printf '%s\n' "$out" | sed 's/^/    | /' >&2
		fail=1
		return 1
	fi
	echo "check-notes-claims-selftest: ok [$_name] exit $_want"
	return 0
}

expect() {
	_name=$1
	_needle=$2
	if ! printf '%s\n' "$out" | grep -qF -- "$_needle"; then
		echo "check-notes-claims-selftest: FAIL [$_name] — output missing: $_needle" >&2
		printf '%s\n' "$out" | sed 's/^/    | /' >&2
		fail=1
	fi
}

# ---- case 1: honest notes ----
cat >"$bodies/case1.md" <<'EOF'
# sluice v9.9.1

Fixed the refusal `SLUICE-E-SELFTEST-REAL`, whose REAL-SELFTEST-MARKER marker is
emitted by `realSelfTestSymbol`; the never-mentioned `quiet_file.go` carries it.
Pinned by `TestSelfTestRealGate` in `selftest-guard.sh`. See ADR-0999 and CHANGELOG.md.
EOF
tag_notes v9.9.1 "$bodies/case1.md"
if run_case honest v9.9.1 0; then
	expect honest "content   SLUICE-E-SELFTEST-REAL"
	# The camelCase shape (5) must actually be firing — otherwise case 2's
	# fabricated symbol would "pass" for the wrong reason.
	expect honest "content   realSelfTestSymbol"
	# The filename fallback must actually be reachable past the content leg.
	expect honest "filename  quiet_file.go"
	# Anti-vacuity floor on the SELF-TEST: a broken extraction that yields
	# almost nothing would make every case below meaningless.
	n=$(printf '%s\n' "$out" | sed -n 's/.*OK — \([0-9]*\) identifier.*/\1/p')
	if [ "${n:-0}" -lt 5 ]; then
		echo "check-notes-claims-selftest: FAIL [honest] — only ${n:-0} identifiers extracted; the fixture names at least 5." >&2
		fail=1
	fi
fi

# ---- case 2: the missed-landing shape (THE mutation target) ----
# Every fabricated identifier appears ONLY in the notes and the CHANGELOG,
# both committed in the tag. With the evidence allowlist reverted to a
# whole-tree grep, this case exits 0 and the self-test fails.
cat >"$bodies/case2.md" <<'EOF'
# sluice v9.9.2

Adds the `SLUICE-E-SELFTEST-FAKE` refusal in `phantom_file.go`, computed by
`fabricatedSelfTestSymbol` and pinned by `TestSelfTestNoSuchGate`.
Also fixes `SLUICE-E-SELFTEST-REAL`. See CHANGELOG.md.
EOF
tag_notes v9.9.2 "$bodies/case2.md"
if run_case fabricated v9.9.2 1; then
	expect fabricated "MISSING from v9.9.2: SLUICE-E-SELFTEST-FAKE"
	expect fabricated "MISSING from v9.9.2: phantom_file.go"
	expect fabricated "MISSING from v9.9.2: fabricatedSelfTestSymbol"
	expect fabricated "MISSING from v9.9.2: TestSelfTestNoSuchGate"
	# ... and the honest identifier in the same file still resolves, so the
	# failure is per-claim rather than a blanket refusal.
	expect fabricated "content   SLUICE-E-SELFTEST-REAL"
fi

# ---- case 3: vacuous (ubiquitous tokens only) ----
# THREE of them, deliberately: the old floor was "3 identifiers of any
# shape", which this clears while proving nothing. Drop the ubiquitous
# denylist and this case exits 0.
cat >"$bodies/case3.md" <<'EOF'
# sluice v9.9.3

Routine maintenance. See CHANGELOG.md, README.md and CLAUDE.md.
EOF
tag_notes v9.9.3 "$bodies/case3.md"
if run_case vacuous v9.9.3 1; then
	expect vacuous "FAIL (vacuous)"
	expect vacuous "the floor is 2"
fi

# ---- case 4: non-vacuous but no code-shaped claim ----
cat >"$bodies/case4.md" <<'EOF'
# sluice v9.9.4

Documentation only: ADR-0999 lands as `adr-0999-selftest.md`. See CHANGELOG.md.
EOF
tag_notes v9.9.4 "$bodies/case4.md"
if run_case no-code-claims v9.9.4 1; then
	expect no-code-claims "none is code-shaped"
fi

# ---- case 5: same notes, waiver declared ----
cat >"$bodies/case5.md" <<'EOF'
# sluice v9.9.5

Documentation only: ADR-0999 lands as `adr-0999-selftest.md`. See CHANGELOG.md.

<!-- notes-claims-no-code-claims: docs-only release, no code changed -->
EOF
tag_notes v9.9.5 "$bodies/case5.md"
if run_case no-code-waived v9.9.5 0; then
	expect no-code-waived "waived in the notes: docs-only release"
fi

# ---- case 6: case 2's fabrications, exempted inline ----
cat >"$bodies/case6.md" <<'EOF'
# sluice v9.9.6

Adds the `SLUICE-E-SELFTEST-FAKE` refusal in `phantom_file.go`, computed by
`fabricatedSelfTestSymbol` and pinned by `TestSelfTestNoSuchGate`.
Also fixes `SLUICE-E-SELFTEST-REAL`. See CHANGELOG.md.

<!-- notes-claims-exempt: SLUICE-E-SELFTEST-FAKE, phantom_file.go, fabricatedSelfTestSymbol, TestSelfTestNoSuchGate -->
EOF
tag_notes v9.9.6 "$bodies/case6.md"
if run_case fabricated-but-exempt v9.9.6 0; then
	expect fabricated-but-exempt "exempt    phantom_file.go"
fi

# ---- case 7: the notes file naming ITSELF ----
# The filename leg is tree-wide on purpose, so it needs its own exclusion:
# the graded notes file is in the tag by construction and can only satisfy
# itself. Drop that exclusion and this case exits 0.
cat >"$bodies/case7.md" <<'EOF'
# sluice v9.9.7

Fixes `SLUICE-E-SELFTEST-REAL`; the full write-up is in `release-notes-v9.9.7.md`.
EOF
tag_notes v9.9.7 "$bodies/case7.md"
if run_case notes-name-self v9.9.7 1; then
	expect notes-name-self "MISSING from v9.9.7: release-notes-v9.9.7.md"
fi

if [ "$fail" -ne 0 ]; then
	echo "check-notes-claims-selftest: FAILED — the notes-claims gate does not behave as documented." >&2
	exit 1
fi

echo "check-notes-claims-selftest: all 7 cases behave as documented (self-reference on both legs, camelCase shape, filename fallback, both floor legs, exemption)."
