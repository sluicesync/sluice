#!/usr/bin/env bash
# check-local-gate-parity.sh — the local-gate<->CI parity guard (audit T-7).
#
# CI's Lint job runs a set of static-artifact guards (`bash scripts/check-*.sh`
# with no runtime argument). The local pre-commit hooks are supposed to run
# EVERYTHING CI runs, so a developer catches a violation before pushing — but
# the two drifted: check-skills-flags and, later, three newly-added guards ran
# only in CI. This guard closes the drift permanently: every bare check-*.sh
# invoked in ci.yml must also appear in BOTH local entry points
# (.githooks/pre-commit and scripts/pre-commit.ps1).
#
# "Bare" = invoked with no trailing argument. A guard invoked WITH an argument
# (check-integration-skips.sh takes a shard-output file) is a runtime guard
# that cannot run in a pre-commit context, so it is correctly excluded by the
# end-of-line anchor rather than by a hand-maintained exemption.
set -euo pipefail
cd "$(dirname "$0")/.."

CI=.github/workflows/ci.yml
HOOK_SH=.githooks/pre-commit
HOOK_PS1=scripts/pre-commit.ps1

for f in "$CI" "$HOOK_SH" "$HOOK_PS1"; do
	[ -f "$f" ] || { echo "check-local-gate-parity: $f missing"; exit 1; }
done

# Static Lint guards: bare `scripts/check-*.sh` invocations (nothing but
# whitespace after `.sh`).
ci_guards=$(grep -oE 'scripts/check-[a-z-]+\.sh[[:space:]]*$' "$CI" | grep -oE 'check-[a-z-]+\.sh' | sort -u)

# Anti-vacuity: the extraction must find the known guard set. Five ship today
# (skills-flags, schedule-consumers, leg-nonvacuity-coverage,
# dialect-translator-roster, shard-coverage, run-filter-coverage, this one);
# a floor of 4 fails loudly if the grep rots rather than passing on empty.
count=$(printf '%s\n' $ci_guards | sed '/^$/d' | wc -l | tr -d ' ')
if [ "$count" -lt 4 ]; then
	echo "check-local-gate-parity: extracted only $count bare Lint guard(s) from $CI — extraction likely broke. Failing."
	printf '  %s\n' $ci_guards
	exit 1
fi

fail=0
for g in $ci_guards; do
	# Match the base NAME (without .sh): the hooks reference some guards in a
	# `for g in ... ; do bash scripts/$g.sh` loop, so the literal `.sh` suffix
	# is not present — the base name is what actually appears in both styles.
	base=${g%.sh}
	in_sh=no; in_ps1=no
	# Comments and messages do not run guards. A reference counts only on
	# a non-comment line that either invokes the script by path
	# (`scripts/<guard>`) or lists it in the guard loop's header (`for g in
	# …` / `foreach ($g in @(…))`). Audit 2026-09-01 DDD-7: a hook that
	# merely MENTIONED a guard in a comment satisfied this check, and the
	# first fix (strip comments) was still satisfied by a Write-Host
	# message naming the guard — both mutation-proven.
	# Single-quoted pattern pieces: `$[` inside double quotes is bash
	# arithmetic expansion and blew up under set -u on the first cut.
	runs_guard() {
		grep -vE '^[[:space:]]*#' "$1" | grep -qE \
			'^[[:space:]]*(if +!? *)?(sh|bash|& +\$shExe) +"?scripts/'"$base"'(\.sh)?([^A-Za-z0-9_-]|$)|^[[:space:]]*for [A-Za-z_]+ in.*[^A-Za-z0-9_-]'"$base"'([^A-Za-z0-9_-]|$)|^[[:space:]]*foreach \(\$[A-Za-z_]+ in.*[^A-Za-z0-9_-]'"$base"'([^A-Za-z0-9_-]|$)'
	}
	runs_guard "$HOOK_SH" && in_sh=yes
	runs_guard "$HOOK_PS1" && in_ps1=yes
	if [ "$in_sh" = yes ] && [ "$in_ps1" = yes ]; then
		echo "check-local-gate-parity: $g — in ci.yml + both local hooks. OK."
	else
		echo "check-local-gate-parity: $g runs in ci.yml's Lint job but is MISSING from $([ "$in_sh" = no ] && echo "$HOOK_SH ")$([ "$in_ps1" = no ] && echo "$HOOK_PS1")."
		echo "  Fix: add it to the missing local entry point(s) so the pre-commit gate runs everything CI runs."
		fail=1
	fi
done

# ---- vet-tags.sh <-> vet-tags.ps1 property parity (audit 2026-09-15 X1) ----
# The guard loop above catches a guard MISSING from a hook. It cannot see a
# guard that exists in both entry points with different PROPERTIES — which is
# how vet-tags.sh gained a workspace/ exclusion (DDD-8) and a per-combo
# package-count floor (A0909-TCI-M-1) while vet-tags.ps1, the mirror
# scripts/pre-commit.ps1 actually runs on the primary development machine,
# got neither. Each property is asserted in BOTH files, with the .sh as the
# reference: the .sh losing one is a regression of the reference, not a
# licence for the mirror to drop it — so the check is red in both
# directions, not vacuous when the reference changes.
VET_SH=scripts/vet-tags.sh
VET_PS1=scripts/vet-tags.ps1
for f in "$VET_SH" "$VET_PS1"; do
	[ -f "$f" ] || { echo "check-local-gate-parity: $f missing"; exit 1; }
done
# name | regex that must match a NON-comment line of the .sh | same for the .ps1
vet_props='
workspace-exclusion|grep -v .\/workspace\/.|-notmatch .\/workspace\/.
package-count-floor|pkg_count.* -lt [0-9]+|\.Count -lt [0-9]+
'
printf '%s\n' "$vet_props" | sed '/^$/d' | while IFS='|' read -r pname sh_re ps1_re; do
	[ -n "$pname" ] || continue
	if ! grep -vE '^[[:space:]]*#' "$VET_SH" | grep -qE -- "$sh_re"; then
		echo "check-local-gate-parity: $VET_SH lost its '$pname' (no non-comment line matches /$sh_re/). That property is the reference the .ps1 mirror is held to; restore it."
		exit 1
	fi
	if ! grep -vE '^[[:space:]]*#' "$VET_PS1" | grep -qE -- "$ps1_re"; then
		echo "check-local-gate-parity: $VET_PS1 lacks the '$pname' that $VET_SH carries (no non-comment line matches /$ps1_re/)."
		echo "  The .ps1 is what scripts/pre-commit.ps1 runs; a property the .sh has and the .ps1 lacks is a gate that differs by machine."
		exit 1
	fi
	echo "check-local-gate-parity: vet-tags $pname — present in both $VET_SH and $VET_PS1. OK."
done || exit 1

if [ "$fail" -ne 0 ]; then
	echo "check-local-gate-parity: FAILED — a CI Lint guard is not enforced locally."
	exit 1
fi

echo "check-local-gate-parity: all $count CI Lint guards run in both local pre-commit entry points."
