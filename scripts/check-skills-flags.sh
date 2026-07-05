#!/usr/bin/env bash
# check-skills-flags.sh — the skills↔CLI doc-sync guard.
#
# Every `--flag` referenced in skills/**/*.md must resolve to a real CLI flag
# defined in cmd/sluice (a kong `name:"<flag>"` struct tag) OR be documented in
# AGENTS.md / docs/operator/error-codes.md. A skill that names a renamed or
# removed flag therefore fails CI here — not in a user's session. This is the
# in-repo advantage the skills pack was placed in this repo for (see
# docs/research/ai-skills-pack.md, "Repo location").
#
# Deliberately scoped to FLAGS (the most drift-prone + copy-paste-hazardous
# surface). Subcommand drift is caught less directly by the same authoring
# discipline; extend here if it becomes a problem.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d skills ]; then
	echo "check-skills-flags: no skills/ directory — nothing to check."
	exit 0
fi

# Extract every --flag token used anywhere in the skills markdown.
flags=$(grep -rhoE '\-\-[a-z][a-z0-9-]+' skills/ 2>/dev/null | sort -u || true)

# Vacuous-pass guard (the check-shard-coverage.sh lesson): if extraction yields
# nothing, the skills either vanished or the regex broke — fail loudly rather
# than green on an empty set.
if [ -z "$flags" ]; then
	echo "check-skills-flags: extracted ZERO --flags from skills/ — extraction broke or skills are gone. Failing."
	exit 1
fi

missing=0
for f in $flags; do
	name="${f#--}"
	# Authoritative: a kong flag is `name:"<flag>"` in cmd/sluice. Fall back to
	# a literal `--<flag>` mention in cmd/sluice, AGENTS.md, or the operator docs
	# (covers doc-only references and generated help text).
	if grep -rqE "name:\"${name}\"" cmd/sluice/ 2>/dev/null; then
		continue
	fi
	if grep -rqE "(^|[^A-Za-z0-9_-])--${name}([^A-Za-z0-9_-]|\$)" cmd/sluice/ AGENTS.md docs/operator/ docs/cookbook/ 2>/dev/null; then
		continue
	fi
	# kong AUTO-derives --foo-bar-baz from an untagged struct field FooBarBaz
	# (no explicit name: tag — e.g. verify.go's SampleSeed → --sample-seed).
	# Resolve the kebab flag to its CamelCase field and look for a struct-field
	# line of that name that carries a kong tag (a backtick).
	cc=$(printf '%s' "$name" | sed -E 's/(^|-)([a-z])/\U\2/g')
	if grep -rhE "^[[:space:]]+${cc}[[:space:]]" cmd/sluice/ 2>/dev/null | grep -q '`'; then
		continue
	fi
	echo "SKILLS-DRIFT: --${name} is referenced in skills/ but not defined as a kong flag in cmd/sluice/ nor documented in AGENTS.md / docs/{operator,cookbook}/"
	missing=1
done

if [ "$missing" != 0 ]; then
	echo ""
	echo "check-skills-flags FAILED: fix the skill to name the current flag, or add the flag. (skills are the in-repo playbooks under skills/.)"
	exit 1
fi
echo "check-skills-flags: all $(echo "$flags" | wc -l | tr -d ' ') distinct skill flags resolve to a real CLI flag."
