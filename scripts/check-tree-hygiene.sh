#!/bin/sh
# check-tree-hygiene.sh — fail when a TRACKED file is scratch/editor
# detritus (`*.tmp`, `*.bak`, `*.orig`, `*.rej`, `*~`, `*.swp`,
# `.DS_Store`, `Thumbs.db`).
#
# Why this exists: a `.driftblock.tmp` fragment — a slice of Go source
# held aside during a multi-line edit — was swept in by a broad
# `git add` and stayed TRACKED for a day (untracked by 0bdf4c8f). It was
# found by a human noticing the filename in a directory listing; no
# check saw it. That gap is structural, not incidental: every existing
# local/CI gate validates the CODE's health (gofumpt, vet, lint, tests,
# the coverage guards) and NOTHING validates the TREE's shape, so a
# tracked scratch artifact passes all of them by construction.
#
# The check is deliberately narrow so it can never become the guard
# that cries wolf and gets disabled:
#
#   - TRACKED FILES ONLY (`git ls-files`), never the working tree. A
#     scratch file sitting untracked in your checkout is normal and
#     fine — the defect is *tracking* one. This is also why the check
#     is correct inside a pre-commit hook: `git ls-files` reads the
#     INDEX, so a freshly-`git add`ed `.tmp` is caught before the
#     commit that would have tracked it.
#   - BASENAME-shaped patterns only. A directory named `tmp/` or a doc
#     named `tmpfile-handling.md` is not detritus; only the terminal
#     `.tmp`/`~`/… shape is. The negative fixtures in self_test pin
#     that boundary (`foo.tmpl`, `mythumbs.db`, `orig-design.md`).
#   - `.orig` and `.rej` are merge/patch detritus — exactly the shape
#     that slipped through here, and the shape most likely to carry a
#     half-applied hunk of real source into the tree.
#
# The pattern set was verified against the tree at the time of writing:
# ZERO tracked files matched any of the eight, so no allowlist exists
# and none is needed. If a legitimate tracked file ever matches, prefer
# NARROWING the pattern; an allowlist is a last resort and every entry
# must carry a written justification here (an unexplained allowlist
# entry is how a guard rots into decoration). Siblings considered and
# deliberately left out to keep the initial set exactly the defensible
# one: `*.swo`/`*.swn` (vim overflow swaps), `.#*`/`*#` (emacs), and
# `desktop.ini` — add them when one actually happens, not before.

set -eu
cd "$(dirname "$0")/.."

# SCRATCH_RE — an ERE matched case-INSENSITIVELY against each tracked
# path. Case-folding covers `NOTES.BAK` / `THUMBS.DB` from Windows
# editors; no legitimate file is named `FOO.ORIG` either.
#
# Anatomy:
#   (^|/)(\.DS_Store|Thumbs\.db)$   whole-basename OS detritus (the
#                                   boundary is what stops `mythumbs.db`)
#   \.(tmp|bak|orig|rej|swp)$       terminal scratch extensions
#   ~$                              editor backup suffix
SCRATCH_RE='(^|/)(\.DS_Store|Thumbs\.db)$|\.(tmp|bak|orig|rej|swp)$|~$'

# scratch_hits — filter a newline-separated path list on stdin down to
# the offenders. The single chokepoint for the matching semantics: both
# the real check and self_test below go through it, so the self-test
# cannot drift into pinning a second, parallel implementation.
scratch_hits() {
	grep -Ei "$SCRATCH_RE" || true
}

# self_test — deliberate-fixture pins for scratch_hits(), run on every
# invocation (pure grep, milliseconds). Positives prove the guard has
# teeth for EVERY pattern, not just the representative that motivated
# it; negatives pin the false-fire boundary, which is the property that
# decides whether this guard survives contact with developers.
self_test() {
	_st_fail=0
	# expect PATH WANT(yes|no)
	expect() {
		if [ -n "$(printf '%s\n' "$1" | scratch_hits)" ]; then _got=yes; else _got=no; fi
		if [ "$_got" != "$2" ]; then
			echo "::error::check-tree-hygiene SELF-TEST: '$1' matched=$_got, want $2 — the scratch patterns no longer mean what this guard documents; fix SCRATCH_RE in scripts/check-tree-hygiene.sh before trusting it."
			_st_fail=1
		fi
	}
	# Positives — one per pattern, at the root and nested.
	expect ".driftblock.tmp" yes # the incident this guard exists for
	expect "internal/pipeline/migrate.go.bak" yes
	expect "internal/ir/schema.go.orig" yes
	expect "internal/ir/schema.go.rej" yes
	expect "docs/architecture.md~" yes
	expect ".migrate.go.swp" yes
	expect ".DS_Store" yes
	expect "docs/img/.DS_Store" yes
	expect "Thumbs.db" yes
	expect "docs/img/Thumbs.db" yes
	expect "NOTES.BAK" yes        # case-folded
	expect "docs/img/THUMBS.DB" yes
	# Negatives — the false-fire surface. Each of these exists (or
	# plausibly could) in a Go repo; a guard that fires on them gets
	# disabled, which is strictly worse than no guard.
	expect "internal/pipeline/migrate.go" no
	expect "tmp/notes.md" no            # a tmp DIRECTORY is not the defect
	expect "docs/dev/tmpfile-handling.md" no
	expect "scripts/query.sql.tmpl" no  # .tmpl must not read as .tmp
	expect "docs/orig-design.md" no
	expect "internal/pipeline/backup/manifest.go" no
	expect "mythumbs.db" no             # basename boundary
	expect "notes.DS_Store" no          # basename boundary
	expect "internal/ir/a~b.go" no      # tilde must be terminal
	return "$_st_fail"
}

if ! self_test; then
	exit 1
fi

tracked=$(git ls-files)
tracked_count=$(printf '%s\n' "$tracked" | grep -c . || true)

# Vacuous-success guard. A guard that stopped examining anything reads
# identically to a clean tree, and this repo has been burned by exactly
# that shape more than once. `git ls-files` returns nothing from the
# wrong working directory, outside a repo, or with a broken `cd` above;
# the tree carries ~2,800 tracked files, so 500 is a floor no legitimate
# state of this repo reaches.
MIN_TRACKED_FILES=500
if [ "$tracked_count" -lt "$MIN_TRACKED_FILES" ]; then
	echo "::error::check-tree-hygiene: git ls-files returned $tracked_count tracked files (floor $MIN_TRACKED_FILES) — the guard is not looking at this repo (wrong working directory, or not a git checkout); refusing to pass vacuously."
	exit 1
fi

offenders=$(printf '%s\n' "$tracked" | scratch_hits)

if [ -n "$offenders" ]; then
	echo "::error::check-tree-hygiene: scratch/editor-detritus file(s) are TRACKED in git:"
	printf '%s\n' "$offenders" | while IFS= read -r p; do
		[ -n "$p" ] || continue
		echo "::error::  tracked scratch file: $p"
	done
	echo ""
	echo "These are edit/merge leftovers (*.tmp *.bak *.orig *.rej *~ *.swp .DS_Store Thumbs.db),"
	echo "not source. A .driftblock.tmp fragment reached main this way and sat tracked for a day."
	echo ""
	echo "To fix, for each path above:"
	echo "  git rm --cached <path>     # untrack it (keeps your local copy)"
	echo "and check .gitignore covers the pattern so it cannot be re-added by a broad \`git add\`."
	echo ""
	echo "If the file is legitimate source that merely LOOKS like detritus, rename it —"
	echo "or, as a last resort, narrow SCRATCH_RE in scripts/check-tree-hygiene.sh with a"
	echo "written justification (see this script's header)."
	exit 1
fi

echo "check-tree-hygiene: $tracked_count tracked files, none matching the scratch/editor-detritus patterns."
