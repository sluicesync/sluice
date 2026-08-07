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

# --- (subcommand, flag) PAIRING -------------------------------------------
# The two passes above are both NAME-ONLY, and that is a hole with a name: a
# skill saying `sluice sync start --resume` passes both of them, because
# `sync`/`start` are real commands AND `--resume` is a real flag — on
# `migrate`. It is not a flag on `sync start` and never has been, so an agent
# following that skill gets `unknown flag --resume` and exit 80. Nine
# instances of exactly that string were found in sluice's own runtime
# messages (2026-08-06); this pass is the skills-side half of the fix, and
# internal/climsggate is the Go-string half.
#
# Ground truth is docs/dev/cli-flags.txt, which cmd/sluice's
# TestCLIFlagManifestIsCurrent generates from kong's model and holds current
# — so this cannot drift from the parser the binary uses.
manifest="docs/dev/cli-flags.txt"
if [ ! -f "$manifest" ]; then
	echo "check-skills-flags: $manifest is missing — regenerate it with 'SLUICE_UPDATE_CLI_MANIFEST=1 go test ./cmd/sluice/'. Failing."
	exit 1
fi

pairs="$(mktemp)"
paths="$(mktemp)"
grep -v '^#' "$manifest" | grep -e '--' >"$pairs"
# Every command path AND every prefix of one. The manifest only lists paths
# that own at least one flag, so a group command like `sync` (whose flags all
# live on its children) leaves no line of its own — without the prefixes the
# resolver would fail to walk `sync start` and grade every flag against the
# empty path.
sed -E 's/ ?--.*$//' "$pairs" | sort -u | grep -v '^$' |
	awk '{p=""; for (i=1; i<=NF; i++) { p = (p=="" ? $i : p" "$i); print p }}' |
	sort -u >"$paths"

if [ ! -s "$pairs" ] || [ ! -s "$paths" ]; then
	echo "check-skills-flags: parsed ZERO (command, flag) pairs from $manifest — the manifest format changed. Failing."
	rm -f "$pairs" "$paths"
	exit 1
fi

# pair_ok <command path> <flag name>: a flag declared on an ANCESTOR node is
# accepted on its descendants, because that is how kong resolves it.
pair_ok() {
	_p="$1"
	while :; do
		if [ -z "$_p" ]; then
			grep -qxF -- "--$2" "$pairs" && return 0
			return 1
		fi
		grep -qxF -- "$_p --$2" "$pairs" && return 0
		case "$_p" in
		*\ *) _p="${_p% *}" ;;
		*) _p="" ;;
		esac
	done
}

checked=0
while IFS= read -r span; do
	[ -z "$span" ] && continue
	path=""
	seen_flag=0
	for word in $span; do
		case "$word" in
		sluice) : ;;
		--) : ;;
		--*)
			seen_flag=1
			name="${word#--}"
			name="${name%%=*}"
			case "$name" in
			'' | *[!a-z0-9-]*) continue ;;
			esac
			checked=$((checked + 1))
			if ! pair_ok "$path" "$name"; then
				echo "SKILLS-DRIFT: \`sluice ${path} --${name}\` — '--${name}' is not a flag on '${path}' (it may well be a flag on a DIFFERENT command; that is the slip this pass exists for)"
				missing=1
			fi
			;;
		[a-z]*)
			[ "$seen_flag" = 1 ] && continue
			cand="${path:+$path }$word"
			grep -qxF -- "$cand" "$paths" && path="$cand"
			;;
		esac
	done
done <<EOF
$(grep -rhoE '`sluice [^\`]+`' skills/ 2>/dev/null | tr -d '\`' || true)
EOF

rm -f "$pairs" "$paths"

# Vacuous-pass guard: skills/ carries dozens of full invocations, so zero
# graded pairs means the span extraction broke, not that the skills are clean.
if [ "$checked" -lt 20 ]; then
	echo "check-skills-flags: graded only ${checked} (command, flag) pairs from skills/ — span extraction broke. Failing."
	exit 1
fi

if [ "$missing" != 0 ]; then
	echo ""
	echo "check-skills-flags FAILED: a skill names a flag or subcommand that no longer exists in the CLI, or pairs a real flag with the wrong command. Fix the skill, or add the flag/command. (skills are the in-repo playbooks under skills/.)"
	exit 1
fi
echo "check-skills-flags: all $(echo "$flags" | wc -l | tr -d ' ') skill flags, $(echo "$cmd_words" | wc -l | tr -d ' ') skill subcommands and ${checked} (command, flag) pairs resolve to a real CLI surface."
