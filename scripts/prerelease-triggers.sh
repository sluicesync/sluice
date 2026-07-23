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
    # Perf-matrix path-coupling (audit 2026-07-23 DOC-2 / G-16): the
    # CLAUDE.md working agreement says a perf-technique change updates
    # docs/dev/perf-parity-matrix.md IN THE SAME delta -- unreached cells
    # are filed as explicit gaps, never implied. Flag the missing edit
    # here so the drift is caught before the tag, not by the next audit.
    if [ "${NAMES[$i]}" = "perf-parity" ] && ! printf '%s\n' "$files" | grep -qx 'docs/dev/perf-parity-matrix.md'; then
      echo "  !! perf-technique paths changed but docs/dev/perf-parity-matrix.md was NOT touched in this delta"
      echo "     -> update the matrix (or file the unreached cells as explicit gaps) in the same release; see CLAUDE.md 'Performance chunks must state their engine x mode coverage explicitly'"
    fi
    echo
  fi
done

# ---- Expiry-token gate (audit 2026-07-23 DOC-5 / G-18) --------------------
# Status prose like "unreleased at time of writing" / "pending review/
# release" EXPIRES the moment the work it describes ships -- and nothing
# used to re-visit it, so the roadmap accumulated markers for work released
# months earlier (mis-reporting shipped work as pending has repeatedly cost
# ground-truthing passes; see the CLAUDE.md verify-against-code agreement).
# Mechanical check: for each marker line in the roadmap + ADRs, blame the
# line and ask git whether the commit that last touched it is contained in
# a release tag. A marker written in a still-unreleased commit is ACCURATE
# and stays quiet; a marker whose surrounding commit has since shipped has
# outlived its truth window and gets flagged for the release's doc pass.
expiry_pattern='unreleased at time of writing|pending review/release|pending (review|release)[);.,]'
expiry_hits=0
while IFS=: read -r file line _; do
  [ -n "$file" ] || continue
  sha="$(git blame -L "$line,$line" --porcelain -- "$file" 2>/dev/null | head -1 | cut -d' ' -f1)"
  [ -n "$sha" ] || continue
  case "$sha" in 0000000000000000000000000000000000000000) continue ;; esac # uncommitted edit
  tags="$(git tag --contains "$sha" 2>/dev/null | head -1)"
  if [ -n "$tags" ]; then
    if [ "$expiry_hits" -eq 0 ]; then
      echo "> [expiry-tokens] -> doc pass before the tag"
      echo "  why: these release-state markers were written in commits that have since SHIPPED (git tag --contains), so the prose has outlived its truth window -- update it to name the release (audit 2026-07-23 DOC-5 / G-18)"
    fi
    expiry_hits=$((expiry_hits + 1))
    echo "    - $file:$line (marker committed in ${sha:0:8}, first shipped tag: $tags)"
  fi
done <<EOF
$(grep -nEi "$expiry_pattern" docs/dev/roadmap.md docs/adr/*.md 2>/dev/null || true)
EOF
if [ "$expiry_hits" -gt 0 ]; then
  hit=1
  echo
fi

if [ "$hit" -eq 0 ]; then
  echo "No specialist review triggered -- delta does not touch a known risk surface."
  echo "(Standard CI gates + the five-check publish gate still apply.)"
  echo
fi

echo "Advisory only. The full blind audit (REPO_AUDIT_PROMPT.md) stays periodic /"
echo "new-surface-triggered; this covers the cheap per-release targeted slice (Tier 2)."
