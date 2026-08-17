#!/usr/bin/env bash
# check-dialect-translator-roster.sh — the sibling-roster guard for the
# cross-dialect CHECK-expression translator (audit M-5).
#
# An engine whose DDL emitter applies a cross-dialect CHECK-expression rewrite
# (a `func translateExprFor<Engine>`) MUST also expose that same rewrite via
# ir.CheckExprDialectTranslator — otherwise `schema diff` phantom-reports drift
# in that engine's TARGET direction on a foreign-source migrate, because the
# expected side reaches the diff untranslated. That is exactly what shipped:
# PostgreSQL exposed its translator, MySQL did not, and a PG->MySQL migrate
# phantom-reported drift (M-5). The translator EXISTED and was applied at emit
# — nothing forced its sibling exposure.
#
# This guard derives its universe from the tree (every engine package with a
# translator function) and requires each to carry the compile-time interface
# pin. A new translating engine that forgets the exposure fails HERE, in the
# required Lint check, not months later as phantom drift in that direction.
set -euo pipefail
cd "$(dirname "$0")/.."

ENG_DIR=internal/engines
# Match the actual blank-var PIN assignment, NOT a doc-comment mention: the
# line is `_ ir.CheckExprDialectTranslator = (*SchemaWriter)(nil)`. Anchoring
# on a leading `_` excludes `//`-comment lines (which this guard's own earlier
# cut matched — a commented-out pin or a doc reference satisfied it vacuously).
PIN_RE='^[[:space:]]*_[[:space:]]+ir\.CheckExprDialectTranslator[[:space:]]*='

# Discover engine packages that define a cross-dialect translator.
translators=""
for d in "$ENG_DIR"/*/; do
	pkg=$(basename "$d")
	# Only non-test source declares the emit-time translator.
	if grep -rql --include='*.go' --exclude='*_test.go' 'func translateExprFor' "$d" 2>/dev/null; then
		translators="$translators $pkg"
	fi
done

# Anti-vacuity: the discovery must find the known translators. Two ship today
# (mysql, postgres); a floor of 2 fails loudly if the grep pattern rots or the
# packages move, rather than passing on an empty universe.
count=$(printf '%s\n' $translators | sed '/^$/d' | wc -l | tr -d ' ')
if [ "$count" -lt 2 ]; then
	echo "check-dialect-translator-roster: discovered only $count translator package(s) ($translators) — expected >= 2 (mysql, postgres). Discovery likely broke. Failing."
	exit 1
fi

fail=0
for pkg in $translators; do
	if grep -rqlE --include="*.go" --exclude="*_test.go" "$PIN_RE" "$ENG_DIR/$pkg/" 2>/dev/null; then
		echo "check-dialect-translator-roster: $pkg has a translator AND exposes ir.CheckExprDialectTranslator. OK."
	else
		echo "check-dialect-translator-roster: $pkg defines a cross-dialect translator (translateExprFor...) but does NOT expose ir.CheckExprDialectTranslator."
		echo "  Fix: implement TranslateCheckExprFromDialect on its *SchemaWriter and pin it in $ENG_DIR/$pkg/capabilities_assert.go — else schema diff phantom-reports drift in the $pkg-target direction."
		fail=1
	fi
done

if [ "$fail" -ne 0 ]; then
	echo "check-dialect-translator-roster: FAILED — a translating engine does not expose its translator to schema diff."
	exit 1
fi

echo "check-dialect-translator-roster: every engine with a cross-dialect translator exposes it via ir.CheckExprDialectTranslator ($count checked)."
