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
# Both classes are cheap to detect and expensive to notice by eye in review:
#
#   1. A private-tracker ID (NEKI-018) or repo path (neki-issues/…). A reader
#      cannot follow it, and its presence advertises the private repo's shape.
#   2. An absolute local path (C:\code\…, /Users/<name>/…, /home/<name>/…).
#      The original scrub commit's own message calls this the worse half.
#
# WHAT THIS COVERS, stated narrowly because a gate read as broader than it is
# is worse than none. It grades the SHIPPED and OPERATOR-FACING surface:
# internal/ and cmd/ Go, scripts/, and the docs an operator reads
# (docs/*.md, docs/operator/, docs/adr/).
#
# WHAT IT DELIBERATELY DOES NOT COVER, each with a reason:
#   - docs/dev/ and docs/research/ — engineering notes and measurement logs.
#     They record where work happened, including on somebody's machine, and
#     that is their job. docs/dev/audit-backlog.md is the clearest case.
#   - docs/releases/ — published notes, immutable once tagged.
#   - *_psverify_test.go and other machine-local harnesses — the literal path
#     is FUNCTIONAL there (the test reads that file to find a credential), so
#     removing it would break the harness rather than fix a leak. That these
#     name a credential file in a public repo is a real but separate question;
#     it is filed rather than silently swept in here.
#
# Running it over the excluded paths today reports six pre-existing hits. They
# are not this script's business, and pretending otherwise by widening the
# scope would have meant either breaking a harness or disabling the gate.
#
# Exit 1 with the offending lines on a hit; silent exit 0 otherwise.

set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# Private-tracker references and absolute local paths.
#  - [A-Z]{3,}-[0-9]{3} would catch SLUICE-E-… style codes, so the tracker
#    prefix is named explicitly rather than matched by shape.
#  - The local-path patterns are deliberately narrow: a Windows drive-letter
#    path, and a home directory with a username in it.
pattern='neki-issues|NEKI-[0-9]{3}|[A-Za-z]:\\+code\\+|/Users/[a-z][a-z0-9_-]+/|/home/[a-z][a-z0-9_-]+/'

hits=$(
  grep -rInE "$pattern" internal cmd scripts docs \
    --include='*.go' --include='*.md' --include='*.sh' --include='*.yml' --include='*.yaml' \
    2>/dev/null \
    | grep -v '^docs/dev/' \
    | grep -v '^docs/research/' \
    | grep -v '^docs/releases/' \
    | grep -v '_psverify_test\.go:' \
    | grep -v '^scripts/check-no-private-refs\.sh:' \
    || true
)

if [ -n "$hits" ]; then
  echo "check-no-private-refs: FAIL — the PUBLIC tree references a private tracker or a local filesystem path:" >&2
  echo "$hits" >&2
  echo "" >&2
  echo "sluice is a public repository. A private-tracker ID is unfollowable by every reader and" >&2
  echo "advertises the private repo's contents; an absolute local path is meaningless to everyone" >&2
  echo "but its author. Describe the FINDING inline instead of citing where it was filed." >&2
  echo "" >&2
  echo "Legitimate homes for such a reference: docs/dev/audit-backlog.md (the engineering register)" >&2
  echo "and docs/releases/ (published, immutable)." >&2
  exit 1
fi
