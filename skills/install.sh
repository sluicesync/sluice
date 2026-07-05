#!/bin/sh
# install.sh — install the sluice agent-skills into whichever agents are present.
#
# POSIX sh, no bash-isms. Idempotent (re-running overwrites the installed copy
# with the repo copy), non-destructive (touches only the per-agent skills dirs,
# never deletes anything it didn't install), and prints exactly what it did.
#
# Usage:
#   ./skills/install.sh            # install into ~/.claude/skills (personal, all projects)
#   ./skills/install.sh --project  # install into ./.claude/skills (checked into this project)
#   ./skills/install.sh --help
#
# If this script is not executable, run it via `sh skills/install.sh`, or
# `chmod +x skills/install.sh` first.

set -eu

usage() {
	cat <<'EOF'
Install the sluice agent-skills.

  --project   Install into ./.claude/skills (project-local) instead of ~/.claude/skills
  --help      Show this help

Each skill is a self-contained SKILL.md directory; you can also copy the
directories by hand into any agent's skills location.
EOF
}

PROJECT_SCOPE=0
for arg in "$@"; do
	case "$arg" in
	--project) PROJECT_SCOPE=1 ;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "install.sh: unknown argument: $arg" >&2
		usage >&2
		exit 2
		;;
	esac
done

# Resolve the skills source dir = the directory this script lives in.
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

# Discover the skills to install: every immediate subdir containing a SKILL.md.
SKILLS=""
for d in "$SCRIPT_DIR"/*/; do
	[ -f "${d}SKILL.md" ] || continue
	name=$(basename "$d")
	SKILLS="$SKILLS $name"
done

if [ -z "$SKILLS" ]; then
	echo "install.sh: no skills (SKILL.md dirs) found next to $0" >&2
	exit 1
fi

# copy_into DEST_ROOT LABEL — copy every skill dir into DEST_ROOT/<name>/.
copy_into() {
	dest_root=$1
	label=$2
	echo "==> Installing into $label: $dest_root"
	mkdir -p "$dest_root"
	for name in $SKILLS; do
		mkdir -p "$dest_root/$name"
		cp "$SCRIPT_DIR/$name/SKILL.md" "$dest_root/$name/SKILL.md"
		echo "    installed $name"
	done
}

INSTALLED_ANY=0

# --- Claude Code ---------------------------------------------------------
if [ "$PROJECT_SCOPE" -eq 1 ]; then
	copy_into "./.claude/skills" "Claude Code (project)"
	INSTALLED_ANY=1
else
	copy_into "${HOME}/.claude/skills" "Claude Code (personal)"
	INSTALLED_ANY=1
fi

# --- Cursor --------------------------------------------------------------
# Cursor reads project rules from ./.cursor/rules. Only install there when a
# .cursor dir already exists (trivially known / opted-in), to avoid creating
# agent config the user didn't ask for.
if [ -d "./.cursor" ]; then
	copy_into "./.cursor/rules/sluice-skills" "Cursor (project rules)"
	INSTALLED_ANY=1
else
	echo "==> Cursor: no ./.cursor directory found — skipping."
	echo "    To use these with Cursor, copy the skill dirs under ./.cursor/rules/ by hand,"
	echo "    or point your agent's rules/skills path at $SCRIPT_DIR."
fi

echo ""
if [ "$INSTALLED_ANY" -eq 1 ]; then
	echo "Done. Skills installed:$SKILLS"
	echo "Describe a task in natural language and the matching skill loads by its description trigger."
else
	echo "Nothing installed."
fi
