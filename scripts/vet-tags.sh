#!/bin/sh
# vet-tags.sh — type-check every build-tag combination in use.
#
# `go vet ./...` (no tags) skips every build-tagged file, and
# `go build -tags=...` skips _test.go files — so a package-level symbol
# rename that a tagged *test* file still references compiles clean in
# every default gate and only explodes when someone finally runs that
# suite (the v0.58.1 retag was exactly this class; the audit found five
# tagged suites no gate even type-checked).
#
# This script closes the gap structurally: the tag combinations are
# DISCOVERED from the tree (git grep), not hand-maintained, so a new
# tagged suite is gated here automatically the moment it lands.
#
# Each combination gets its own `go vet -tags=<combo> ./...` pass. A
# single all-tags superset pass would be cheaper but is incorrect:
# files from mutually-exclusive combos can declare the same symbol
# (e.g. `readerErr` exists under both `integration && vstream` and
# `integration && vitesscluster` in the mysql engine package) and only
# ever compile apart. Per-combo passes mirror how the suites actually
# build. `go vet` results are cached per package+config, so repeat
# local runs cost seconds, not minutes.
#
# Used by: the CI Lint job, .githooks/pre-commit, scripts/pre-commit.ps1
# (PowerShell mirror: scripts/vet-tags.ps1), and `make vet-tags`.

set -eu
cd "$(dirname "$0")/.."

# Discovery uses `git ls-files | xargs grep` rather than `git grep`:
# under Git Bash on Windows, MSYS argument conversion mangles a
# `^//go:build` pattern passed to the native git.exe (the `//` + `:`
# trips its path heuristics) and git grep silently matches nothing.
# Plain grep is an MSYS binary, so the pattern arrives intact; on
# Linux/macOS the two are equivalent.
exprs=$(git ls-files -- '*.go' | xargs grep -h '^//go:build ' | sort -u)

# GOOS/GOARCH constraints (e.g. `windows`, `!windows`, `linux`, `amd64`) are
# selected by the toolchain's GOOS/GOARCH, NOT passed via `-tags`, and the
# default `go vet ./...` already type-checks them for the runner's platform
# (plus the Windows CI matrix on tag pushes). Strip them — and their negations
# — from each expression before the conjunction parser below, so a
# platform-gated file like `//go:build !windows` (a negation) doesn't trip the
# guard or get mis-treated as a `-tags` value. A pure-GOOS expression collapses
# to empty and is dropped; a mixed one like `integration && !windows` reduces
# to its real tag set (`integration`).
exprs=$(printf '%s\n' "$exprs" | awk '
	BEGIN {
		split("aix android darwin dragonfly freebsd hurd illumos ios js linux nacl netbsd openbsd plan9 solaris wasip1 wasm windows zos 386 amd64 arm arm64 loong64 mips mips64 mips64le mipsle ppc64 ppc64le riscv64 s390x cgo gc gccgo unix boringcrypto", gl, " ")
		for (i in gl) goos[gl[i]] = 1
	}
	{
		sub(/^\/\/go:build /, "")
		n = split($0, terms, /[[:space:]]*&&[[:space:]]*/)
		out = ""
		for (i = 1; i <= n; i++) {
			bare = terms[i]; sub(/^!/, "", bare)
			if (bare in goos) continue
			out = (out == "" ? terms[i] : out " && " terms[i])
		}
		if (out != "") print "//go:build " out
	}' | sort -u)

# Guard against vacuous success: this repo always has tagged files, so
# empty discovery means the discovery itself broke — fail loudly rather
# than "pass" by checking nothing.
if [ -z "$exprs" ]; then
	echo "vet-tags: discovery returned no //go:build expressions — discovery is broken, refusing to pass vacuously." >&2
	exit 1
fi

# All expressions in this repo are simple conjunctions (`a && b`). The
# comma-join below is only valid for conjunctions, so refuse loudly if
# someone introduces negation/disjunction/grouping — extend this script
# (compute the satisfying tag sets) rather than silently skipping.
if printf '%s\n' "$exprs" | grep -q '[!|()]'; then
	echo "vet-tags: unsupported //go:build expression (negation/disjunction/grouping):" >&2
	printf '%s\n' "$exprs" | grep '[!|()]' >&2
	echo "vet-tags: extend scripts/vet-tags.sh (and vet-tags.ps1) to cover it." >&2
	exit 1
fi

combos=$(printf '%s\n' "$exprs" \
	| sed -e 's|^//go:build ||' -e 's/ *&& */,/g' \
	| sort -u)

status=0
for tags in $combos; do
	echo "vet-tags: go vet -tags=$tags ./..."
	if ! go vet -tags="$tags" ./...; then
		status=1
	fi
done

if [ "$status" -ne 0 ]; then
	echo "vet-tags: FAILED — one or more tag combinations do not type-check." >&2
fi
exit $status
