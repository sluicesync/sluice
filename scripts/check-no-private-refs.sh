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
#      WHOLE tracked tree, with no directory exemption. A reader cannot follow
#      it, and its presence advertises the private repo's shape — and that is
#      true of a line in docs/dev/ exactly as much as of a line in docs/adr/,
#      because every tracked file here is equally public. The first cut of this
#      script exempted docs/dev/ and docs/research/ from BOTH classes and told
#      the reader, in its own closing message, that docs/dev/audit-backlog.md
#      was a "legitimate home" for a private-tracker ID. It is not; it is a
#      public file. Four such references were sitting in it, unseen, while this
#      gate ran green in CI and in both pre-commit hooks (found 2026-09-13).
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
# Exit 1 with the offending lines on a hit; silent exit 0 otherwise.

set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

includes="--include=*.go --include=*.md --include=*.sh --include=*.yml --include=*.yaml"

# ---------------------------------------------------------------------------
# Class 1 — private-tracker references. No directory exemption.
#
# [A-Z]{3,}-[0-9]{3} would catch SLUICE-E-… style codes, so the tracker prefix
# is named explicitly rather than matched by shape.
# ---------------------------------------------------------------------------
tracker_pattern='neki-issues|NEKI-[0-9]{3}'

# shellcheck disable=SC2086  # word-splitting of $includes is intended
tracker_hits=$(
  grep -rInE "$tracker_pattern" internal cmd scripts docs $includes 2>/dev/null \
    | grep -v '^scripts/check-no-private-refs\.sh:' \
    || true
)

# ---------------------------------------------------------------------------
# Class 2 — absolute local paths. Engineering-notes directories exempt.
#
# Deliberately narrow: a Windows drive-letter path, and a home directory with a
# username in it.
# ---------------------------------------------------------------------------
path_pattern='[A-Za-z]:\\+code\\+|/Users/[a-z][a-z0-9_-]+/|/home/[a-z][a-z0-9_-]+/'

# shellcheck disable=SC2086  # word-splitting of $includes is intended
path_hits=$(
  grep -rInE "$path_pattern" internal cmd scripts docs $includes 2>/dev/null \
    | grep -v '^docs/dev/' \
    | grep -v '^docs/research/' \
    | grep -v '^docs/releases/' \
    | grep -v '_psverify_test\.go:' \
    | grep -v '^scripts/check-no-private-refs\.sh:' \
    || true
)

status=0

if [ -n "$tracker_hits" ]; then
  echo "check-no-private-refs: FAIL — the PUBLIC tree references a PRIVATE tracker:" >&2
  echo "$tracker_hits" >&2
  echo "" >&2
  echo "sluice is a public repository, and that includes docs/dev/ and docs/research/." >&2
  echo "A private-tracker ID is unfollowable by every reader and advertises the private" >&2
  echo "repo's contents. Describe the FINDING inline instead of citing where it was filed;" >&2
  echo "the engineering register is the right place for the finding, not for the ticket ID." >&2
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

exit "$status"
