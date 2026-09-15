#!/bin/sh
# check-no-private-refs.sh — refuse a PUBLIC-repo reference to a PRIVATE tracker
# or to somebody's local filesystem.
#
# sluicesync/sluice is public. The issue tracker some of its findings were filed
# in is not, and an absolute path like C:\code\... is meaningless to every reader
# who is not the author. Commit 84918dc6 removed 26 such references by hand;
# a later commit the SAME DAY added one back, because nothing was watching. This
# is what watches.
#
# THE TWO CLASSES HAVE DIFFERENT SCOPES, and conflating them is how this gate
# spent its first days inert over the one file that accumulates the problem:
#
#   1. A PRIVATE-TRACKER reference (NEKI-018, neki-issues/…). Graded over the
#      WHOLE tracked tree — `git ls-files`, every path, every extension — with
#      no directory exemption. A reader cannot follow it, and its presence
#      advertises the private repo's shape — and that is true of a line in
#      docs/dev/ exactly as much as of a line in docs/adr/, because every
#      tracked file here is equally public. The first cut of this script
#      exempted docs/dev/ and docs/research/ from BOTH classes and told the
#      reader, in its own closing message, that docs/dev/audit-backlog.md was
#      a "legitimate home" for a private-tracker ID. It is not; it is a public
#      file. Four such references were sitting in it, unseen, while this gate
#      ran green in CI and in both pre-commit hooks (found 2026-09-13).
#
#      The SECOND cut said "whole tracked tree" in this paragraph and greped
#      four directories (audit 2026-09-15 T-2, mutation-confirmed): a private
#      ID planted in README.md or CHANGELOG.md — root-level files, .github/,
#      skills/, benchmarks/ — passed at exit 0, and so did a run whose scan
#      target had been renamed away, because every grep ended in `|| true`
#      and nothing counted what was scanned. The universe is now the index
#      itself, and the floors below refuse a scan that reached too little.
#
#   2. An ABSOLUTE LOCAL PATH (C:\code\…, /Users/<name>/…, /home/<name>/…).
#      Graded over the shipped and operator-facing surface only: internal/ and
#      cmd/ Go, scripts/, and the docs an operator reads (docs/*.md,
#      docs/operator/, docs/adr/). Engineering notes and measurement logs under
#      docs/dev/ and docs/research/ record WHERE work happened, including on
#      somebody's machine, and that is their job — a path there is provenance,
#      not a leak. This is the exemption that was sound; it was simply applied
#      to the other class as well.
#
# Also exempt from class 2, each with a reason:
#   - docs/releases/ — published notes, immutable once tagged.
#   - *_psverify_test.go and other machine-local harnesses — the literal path
#     is FUNCTIONAL there (the test reads that file to find a credential), so
#     removing it would break the harness rather than fix a leak. That these
#     name a credential file in a public repo is a real but separate question;
#     it is filed rather than silently swept in here.
#
# ANTI-VACUITY. Both classes count what they scanned and refuse to pass on a
# universe that is implausibly small: class 1 requires `git ls-files` to
# return at least CLASS1_FLOOR paths (the tree carries thousands), and class 2
# requires each of its four roots to EXIST and to hold at least CLASS2_FLOOR
# candidate files. A missing root, a renamed directory, or an index that came
# back empty is a broken scan, not a clean tree.
#
# Exit 1 with the offending lines on a hit; silent exit 0 otherwise.

set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

self='scripts/check-no-private-refs.sh'

# ---------------------------------------------------------------------------
# Class 1 — private-tracker references. Universe: every tracked path.
#
# [A-Z]{3,}-[0-9]{3} would catch SLUICE-E-… style codes, so the tracker prefix
# is named explicitly rather than matched by shape. `grep -I` skips binary
# files (images, fixtures) rather than matching inside them by accident.
# ---------------------------------------------------------------------------
tracker_pattern='neki-issues|NEKI-[0-9]{3}'

CLASS1_FLOOR=1000
tracked="$(git ls-files -z | tr '\0' '\n' | grep -v "^$self\$" || true)"
tracked_count=$(printf '%s\n' "$tracked" | grep -c . || true)
if [ "${tracked_count:-0}" -lt "$CLASS1_FLOOR" ]; then
  echo "check-no-private-refs: FAIL (vacuous) — git ls-files returned only ${tracked_count:-0} tracked paths;" >&2
  echo "  the tree carries thousands. The scan universe is broken, so a clean result would mean nothing." >&2
  exit 2
fi

tracker_hits=$(
  printf '%s\n' "$tracked" | tr '\n' '\0' \
    | xargs -0 grep -IlnE "$tracker_pattern" -- 2>/dev/null \
    || true
)
# `-l` above is for the xargs batching (one path per hit); re-grep the hit
# files for the offending LINES so the failure names them.
if [ -n "$tracker_hits" ]; then
  tracker_hits=$(printf '%s\n' "$tracker_hits" | tr '\n' '\0' | xargs -0 grep -HInE "$tracker_pattern" -- 2>/dev/null || true)
fi

# ---------------------------------------------------------------------------
# Class 2 — absolute local paths. Engineering-notes directories exempt.
#
# Deliberately narrow: a Windows drive-letter path, and a home directory with a
# username in it.
# ---------------------------------------------------------------------------
path_pattern='[A-Za-z]:\\+code\\+|/Users/[a-z][a-z0-9_-]+/|/home/[a-z][a-z0-9_-]+/'
class2_roots='internal cmd scripts docs'
includes="--include=*.go --include=*.md --include=*.sh --include=*.yml --include=*.yaml"

# Per-root floor: each root must exist and hold at least this many candidate
# files (the smallest root, scripts/, holds dozens). Refuses the T-2 mutant
# where docs/ was renamed away and the scan reported clean.
CLASS2_FLOOR=10
for r in $class2_roots; do
  if [ ! -d "$r" ]; then
    echo "check-no-private-refs: FAIL (vacuous) — class-2 scan root '$r/' does not exist;" >&2
    echo "  the local-path scan would silently cover less than it claims. Restore the root or update class2_roots." >&2
    exit 2
  fi
  n=$(find "$r" -type f \( -name '*.go' -o -name '*.md' -o -name '*.sh' -o -name '*.yml' -o -name '*.yaml' \) | wc -l | tr -d ' ')
  if [ "${n:-0}" -lt "$CLASS2_FLOOR" ]; then
    echo "check-no-private-refs: FAIL (vacuous) — class-2 scan root '$r/' holds only ${n:-0} candidate files (floor $CLASS2_FLOOR);" >&2
    echo "  the scan is reaching almost nothing there. Fix the discovery rather than trusting a clean result." >&2
    exit 2
  fi
done

# shellcheck disable=SC2086  # word-splitting of $includes and $class2_roots is intended
path_hits=$(
  grep -rInE "$path_pattern" $class2_roots $includes 2>/dev/null \
    | grep -v '^docs/dev/' \
    | grep -v '^docs/research/' \
    | grep -v '^docs/releases/' \
    | grep -v '_psverify_test\.go:' \
    | grep -v "^$self:" \
    || true
)

status=0

if [ -n "$tracker_hits" ]; then
  echo "check-no-private-refs: FAIL — the PUBLIC tree references a PRIVATE tracker:" >&2
  echo "$tracker_hits" >&2
  echo "" >&2
  echo "sluice is a public repository, and that includes docs/dev/, docs/research/, README.md," >&2
  echo "CHANGELOG.md and every other tracked file. A private-tracker ID is unfollowable by every" >&2
  echo "reader and advertises the private repo's contents. Describe the FINDING inline instead of" >&2
  echo "citing where it was filed; the engineering register is the right place for the finding," >&2
  echo "not for the ticket ID." >&2
  echo "" >&2
  status=1
fi

if [ -n "$path_hits" ]; then
  [ "$status" -eq 0 ] || echo "" >&2
  echo "check-no-private-refs: FAIL — the PUBLIC tree references a local filesystem path:" >&2
  echo "$path_hits" >&2
  echo "" >&2
  echo "An absolute local path is meaningless to every reader but its author. Engineering" >&2
  echo "notes under docs/dev/ and docs/research/ are exempt — a path is provenance there —" >&2
  echo "but the shipped and operator-facing surface is not." >&2
  echo "" >&2
  status=1
fi

if [ "$status" -eq 0 ]; then
  echo "check-no-private-refs: OK — class 1 scanned $tracked_count tracked paths; class 2 scanned $class2_roots (each root present, ≥$CLASS2_FLOOR candidates)."
fi
exit "$status"
