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
# Covers both --flags (verified against kong flag definitions) and the
# `sluice <subcommand>` paths skills reference (verified against the kong
# command tree). A renamed/removed flag OR subcommand fails CI here.
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

# --- Subcommands -----------------------------------------------------------
# Every command word a skill uses inside a backtick `sluice …` span (before any
# flag) must be a real kong command. The command-name set is the explicit
# cmd:"name" tags plus the kebab-cased field name of every cmd:"" (auto-named)
# subcommand field. Backtick-scoping avoids prose false positives, and
# `<placeholder>` spans are excluded (the [a-z -] class has no '<').
cmdset="$(mktemp)"
{
	grep -rhoE 'cmd:"[a-z][a-z-]+"' cmd/sluice/*.go 2>/dev/null | sed -E 's/cmd:"([a-z-]+)"/\1/'
	grep -rhE '`[^`]*cmd:""' cmd/sluice/*.go 2>/dev/null |
		grep -oE '^[[:space:]]+[A-Z][A-Za-z0-9]+' | sed 's/[[:space:]]//g' |
		sed -E 's/([a-z0-9])([A-Z])/\1-\2/g' | tr 'A-Z' 'a-z'
} | sort -u >"$cmdset"

if [ ! -s "$cmdset" ]; then
	echo "check-skills-flags: extracted ZERO kong commands from cmd/sluice — extraction broke. Failing."
	rm -f "$cmdset"
	exit 1
fi

cmd_words=$(grep -rhoE '`sluice [a-z][a-z -]*`' skills/ 2>/dev/null |
	sed 's/`//g; s/^sluice //; s/ --.*$//' |
	tr ' ' '\n' | grep -E '^[a-z][a-z-]+$' | sort -u || true)
for c in $cmd_words; do
	grep -qxF "$c" "$cmdset" && continue
	echo "SKILLS-DRIFT: subcommand '${c}' is referenced (\`sluice … ${c} …\`) in skills/ but is not a kong command in cmd/sluice/"
	missing=1
done
rm -f "$cmdset"

if [ "$missing" != 0 ]; then
	echo ""
	echo "check-skills-flags FAILED: a skill names a flag or subcommand that no longer exists in the CLI. Fix the skill, or add the flag/command. (skills are the in-repo playbooks under skills/.)"
	exit 1
fi
echo "check-skills-flags: all $(echo "$flags" | wc -l | tr -d ' ') skill flags and $(echo "$cmd_words" | wc -l | tr -d ' ') skill subcommands resolve to a real CLI surface."
