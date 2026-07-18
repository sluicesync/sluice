#!/usr/bin/env bash
# scripts/prerelease-triggers.sh
#
# Delta-triggered pre-release QA advisor (Tier 2 of the audit-derived QA model;
# see CLAUDE.md "Pre-release QA triggers"). Given a release delta, prints which
# scoped specialist reviews (if any) the changed surface warrants -- so the
# expensive full blind audit (REPO_AUDIT_PROMPT.md) stays periodic while the
# cheap, targeted reviews run per-release ONLY when the risk surface is actually
# touched. Most releases touch no risk surface and trigger nothing.
#
# Usage:
#   scripts/prerelease-triggers.sh [BASE_REF]
#
# BASE_REF defaults to the most recent tag (git describe --tags --abbrev=0);
# the delta compared is BASE_REF..HEAD.
#
# Output is ADVISORY and deliberately errs toward suggesting: a hit means
# CONSIDER running the named agent against the delta. Zero hits = no specialist
# review triggered. Standard CI gates and the five-check publish gate always
# apply regardless.
set -euo pipefail

base="${1:-$(git describe --tags --abbrev=0 2>/dev/null || true)}"
if [ -z "$base" ]; then
  echo "prerelease-triggers: no BASE_REF given and no tag found; pass a base ref explicitly." >&2
  exit 2
fi

files="$(git diff --name-only "$base"..HEAD 2>/dev/null || true)"
if [ -z "$files" ]; then
  echo "prerelease-triggers: no changed files in $base..HEAD -- nothing to advise."
  exit 0
fi

# Category table: name | extended-regex over repo-relative paths | agent | why.
# Regexes are intentionally conservative (favor a false suggestion over a miss).
NAMES=(); REGEXES=(); AGENTS=(); WHYS=()
add() { NAMES+=("$1"); REGEXES+=("$2"); AGENTS+=("$3"); WHYS+=("$4"); }

add "value-fidelity" \
  '(internal/ir/(types|value|collation)|internal/engines/[^/]+/(collation|types|decode|encode|cdc_|normalize|schema_reader|row_reader|verifier)|internal/rowpredicate/|codec|decode|encode|normalize)' \
  "value-fidelity-reviewer" \
  "value/collation/type-codec surface touched -- re-derive the family x shape matrix; every family byte-exact or refuse loudly (Bug-74; the 07-18 PAD-SPACE Critical)"

add "persisted-state-codec" \
  '(migration_state|manifest|cursor|resume|progress|_state\.go|internal/ir/backup|/backup)' \
  "value-fidelity-reviewer + the CLAUDE.md new-surface codec checklist" \
  "a store round-trip is a codec -- apply the new-surface checklist (family matrix, independent reader, no skip-branch without proof)"

add "perf-parity" \
  '(internal/(pipeline|engines/[^/]+)/[^/]*(chunk|pool|parallel|batch|bulk|copy)|throughput)' \
  "perf-parity-checker" \
  "perf technique touched -- confirm it reached every engine x mode cell of docs/dev/perf-parity-matrix.md, not just one sibling"

add "docs-drift" \
  '(cmd/sluice/|capabilities|capabilities_assert|docs/operator/error-codes|docs/adr/adr-)' \
  "docs-drift-detector" \
  "CLI flags / capability declarations / error-codes / ADRs changed -- docs LAG code; check sluicesync.com + in-repo docs"

add "concurrency-race" \
  '(internal/(pipeline|engines/[^/]+)/[^/]*(streamer|broker|chain|rotation|fsm|cdc|concurrent|failpoint))' \
  "-race-before-tag (Integration + -race green BEFORE the tag)" \
  "concurrency-sensitive path touched -- do not cut the tag ahead of the first -race integration run (CLAUDE.md concurrency rule)"

nfiles="$(printf '%s\n' "$files" | grep -c . || true)"
echo "== pre-release QA triggers =="
echo "delta: $base..HEAD ($nfiles files changed)"
echo

hit=0
for i in "${!NAMES[@]}"; do
  matched="$(printf '%s\n' "$files" | grep -Ei "${REGEXES[$i]}" || true)"
  if [ -n "$matched" ]; then
    hit=1
    echo "> [${NAMES[$i]}] -> ${AGENTS[$i]}"
    echo "  why: ${WHYS[$i]}"
    echo "  triggering files:"
    printf '%s\n' "$matched" | sed 's/^/    - /'
    echo
  fi
done

if [ "$hit" -eq 0 ]; then
  echo "No specialist review triggered -- delta does not touch a known risk surface."
  echo "(Standard CI gates + the five-check publish gate still apply.)"
  echo
fi

echo "Advisory only. The full blind audit (REPO_AUDIT_PROMPT.md) stays periodic /"
echo "new-surface-triggered; this covers the cheap per-release targeted slice (Tier 2)."
