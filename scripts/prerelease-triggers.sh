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

# PRERELEASE_TRIGGERS_DELTA_FILE names a file holding one repo-relative path
# per line to grade INSTEAD of the git delta. It exists for
# scripts/check-prerelease-triggers-selftest.sh, which feeds synthetic deltas
# through the real category logic; nothing else should set it.
#
# It is checked BEFORE the base ref is resolved, and that order is
# load-bearing: CI's Lint job checks out shallow and tagless, so
# `git describe --tags` finds nothing there, and resolving the base first
# made every synthetic case exit 2 on the missing tag. The self-test passed
# on a developer machine (tags present) and failed on main's CI — the first
# run after the category landed (v0.153.3). The self-test now shims
# `git describe` to fail so a local run reproduces the tagless checkout.
if [ -n "${PRERELEASE_TRIGGERS_DELTA_FILE:-}" ]; then
  files="$(cat "$PRERELEASE_TRIGGERS_DELTA_FILE")"
  base="synthetic:$PRERELEASE_TRIGGERS_DELTA_FILE"
else
  base="${1:-$(git describe --tags --abbrev=0 2>/dev/null || true)}"
  if [ -z "$base" ]; then
    echo "prerelease-triggers: no BASE_REF given and no tag found; pass a base ref explicitly." >&2
    exit 2
  fi
  files="$(git diff --name-only "$base"..HEAD 2>/dev/null || true)"
fi
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

# ---- Unscheduled-pin gate (audit 2026-09-15 T-1 / T-11) --------------------
# Some surfaces have exactly one end-to-end pin, and that pin runs in NO
# scheduled workflow: the d1verify suite (live Cloudflare credentials are
# machine-local, so it has no workflow at all), psverify (dispatch-only), the
# ddlfixture leg (dispatch-only by its job's `if:`). Four D1 commits shipped in
# one window with the premise they rest on asserted only by a d1verify test
# nobody was asked to run -- the five categories above key on file NAMES and
# none of them names a suite.
#
# DERIVED, NOT LISTED. scripts/check-run-filter-coverage.sh already maps every
# tagged test axis to its workflow leg (MANIFEST) or to "no workflow at all"
# (EXEMPT_TAGS), and keeps both honest with its own symmetric staleness
# checks. This block reads those two tables, decides per tag whether ANY leg
# runs on a schedule (an active `schedule:` trigger in the workflow AND a job
# `if:` that does not demand workflow_dispatch), and for every tag with no
# scheduled leg maps the tag to the packages its tagged test files live in.
# A delta touching such a package fires: "run it, or record why not". A
# package under internal/pipeline is touched by most releases, so this will
# fire often for psverify -- that is the advisory posture of this script
# (favor a suggestion over a miss), and the hit lists the exact pins so the
# decision is one glance.
coverage_script="scripts/check-run-filter-coverage.sh"
if [ ! -f "$coverage_script" ]; then
  echo "prerelease-triggers: $coverage_script is missing -- the unscheduled-pin derivation has no source. Refusing to advise on a broken derivation." >&2
  exit 2
fi
exempt_tags="$(sed -n "s/^EXEMPT_TAGS='\(.*\)'$/\1/p" "$coverage_script")"
manifest_lines="$(awk -v q="'" '/^MANIFEST=/{on=1; next} on && $0 == q {exit} on' "$coverage_script" | sed '/^$/d')"
manifest_count="$(printf '%s\n' "$manifest_lines" | grep -c . || true)"
if [ -z "$exempt_tags" ] || [ "${manifest_count:-0}" -lt 5 ]; then
  echo "prerelease-triggers: could not read EXEMPT_TAGS / MANIFEST from $coverage_script (got exempt='$exempt_tags', $manifest_count manifest lines) -- the parse drifted; fix it rather than advising over an empty universe." >&2
  exit 2
fi

# leg_is_scheduled WORKFLOW JOB -> 0 when that leg runs WITHOUT being asked:
# the workflow fires on a schedule, on push, or on pull_request (ci.yml's
# postgis/vstream/mariadb legs run on every PR and are not dispatch-only),
# and the job's own `if:` does not confine it to workflow_dispatch.
leg_is_scheduled() {
  _wf=".github/workflows/$1"
  _job="$2"
  [ -f "$_wf" ] || return 1
  grep -qE '^[[:space:]]+(schedule|push|pull_request):' "$_wf" || return 1
  # The job's own `if:` can still confine it to dispatch (extended-suites'
  # ddlfixture). Read the job block and look for that condition.
  _jobif="$(awk -v job="  $_job:" 'index($0, job) == 1 {injob=1; next} injob && /^  [A-Za-z0-9_-]+:/ {injob=0} injob && /^    if:/' "$_wf")"
  case "$_jobif" in
  *"event_name == 'workflow_dispatch'"*) return 1 ;;
  esac
  return 0
}

unscheduled=""   # "tag|reason" per line
for t in $exempt_tags; do
  unscheduled="$unscheduled
$t|no workflow at all (EXEMPT_TAGS in $coverage_script)"
done
for t in $(printf '%s\n' "$manifest_lines" | cut -d';' -f1 | sed 's/!.*//' | sort -u); do
  scheduled=0
  legs=""
  while IFS=';' read -r mtag _ _ mlabel; do
    [ "${mtag%%!*}" = "$t" ] || continue
    wf="${mlabel%% *}"
    job="$(printf '%s' "$mlabel" | awk '{print $2}')"
    legs="$legs $wf/$job"
    if leg_is_scheduled "$wf" "$job"; then scheduled=1; fi
  done <<EOF
$manifest_lines
EOF
  if [ "$scheduled" -eq 0 ]; then
    unscheduled="$unscheduled
$t|dispatch-only (${legs# } has no scheduled trigger)"
  fi
done
unscheduled="$(printf '%s\n' "$unscheduled" | sed '/^$/d')"
if [ -z "$unscheduled" ]; then
  echo "prerelease-triggers: derived ZERO unscheduled tags, but d1verify is exempt by design -- the derivation broke. Refusing to advise over it." >&2
  exit 2
fi

unsched_hits=0
test_files="$(git ls-files -- '*_test.go')"
while IFS='|' read -r tag reason; do
  [ -n "$tag" ] || continue
  pins="$(printf '%s\n' "$test_files" | xargs grep -lE "^//go:build.*\b$tag\b" 2>/dev/null || true)"
  [ -n "$pins" ] || continue
  for d in $(printf '%s\n' "$pins" | xargs -n1 dirname | sort -u); do
    touched="$(printf '%s\n' "$files" | grep -E "^$d/[^/]+$" || true)"
    [ -n "$touched" ] || continue
    if [ "$unsched_hits" -eq 0 ]; then
      echo "> [unscheduled-pin] -> run the named suite by hand, or record in the release notes / backlog why not"
      echo "  why: this delta touches a surface whose only end-to-end pin runs in NO scheduled workflow -- a premise nobody is asked to re-measure rots silently (audit 2026-09-15 T-1)"
    fi
    unsched_hits=$((unsched_hits + 1))
    echo "  - tag '$tag' ($reason) covers $d/, touched by:"
    printf '%s\n' "$touched" | sed 's/^/      - /'
    echo "    pins that run nowhere on a schedule:"
    printf '%s\n' "$pins" | grep -E "^$d/[^/]+$" | sed 's/^/      - /'
  done
done <<EOF
$unscheduled
EOF
if [ "$unsched_hits" -gt 0 ]; then
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
