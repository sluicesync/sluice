// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// Bug 60 (closed v0.58.1): the `mask:uuid` strategy produces output
// like `550eXXXX-XXXX-XXXX-XXXX-XXXXXXXX0000` — canonical UUID
// shape but with X characters in the masked positions. PG's native
// `uuid` column type rejects non-hex characters; when an operator
// configures `mask:uuid` on a UUID-typed column without an explicit
// `--type-override=col=text`, the migration fails mid-bulk-copy
// with an opaque pgx "cannot find encode plan" error after the
// target schema has already been created with a `uuid` column type.
//
// This preflight catches the misconfiguration at startup, before
// any data movement, and emits an operator-actionable error naming
// the column and suggesting the `--type-override=table.col=text`
// workaround. The check runs AFTER [translate.ApplyMappings] in
// the orchestrator, so operators who've already applied the type
// override see their column's type already re-mapped to text and
// the rule passes silently.
//
// Scope: `mask:uuid` is currently the only mask preset whose
// output shape has a known incompatibility with its target column
// type's strict typing. If/when other presets land with similar
// issues, this preflight expands to cover them (the structure is
// generic).

// errRedactTypeMismatch is the sentinel cause for a redaction-vs-
// target-type preflight refusal. Wrapped with per-column detail.
var errRedactTypeMismatch = errors.New("pipeline: redaction strategy incompatible with target column type")

// errRedactRandomizeNoPK is the sentinel for the second-tier preflight
// refusal added in v0.59.0: a randomize:* rule is registered against
// a table whose source schema has no primary key. Replay-stable
// randomization requires PK values for seed derivation, so the
// rule must refuse at startup rather than later when the strategy
// hits the no-PK case mid-row.
var errRedactRandomizeNoPK = errors.New("pipeline: randomize:* strategy requires a primary key on the source table")

// errRedactKeysetMissing is the sentinel for the PII Phase 4
// (ADR-0041 decision D2) loud refusal: a `hash:hmac-sha256` or
// `tokenize:dict` rule is registered but has no resolvable keyset
// key. The CLI/YAML parsers already refuse a keyless rule at
// construction; this preflight re-asserts it as defense-in-depth so
// a programmatically-built Registry can't slip a keyless rule into a
// data path.
var errRedactKeysetMissing = errors.New("pipeline: hash:hmac-sha256 / tokenize:dict rule requires --keyset-source")

// errRedactSelectorUnresolved is the sentinel for the Bug 99 silent-
// PII-loss refusal: a `--redact='TABLE.COLUMN=STRATEGY'` rule whose
// (Table, Column) doesn't resolve to a real column in the source
// schema. Pre-fix, the existing per-strategy checks silently `continue`
// on a missed lookup — a typo in the table or column name → the rule
// applies to nothing → plaintext PII lands at the destination. This
// is the silent-loss class the loud-failure tenet exists to catch.
// The fix is to validate selector resolution for every rule before
// the per-strategy type/PK/keyset checks run.
var errRedactSelectorUnresolved = errors.New("pipeline: redaction rule's TABLE.COLUMN selector does not resolve to any column in the source schema (typo class — would silently leak PII)")

// errRedactOnGeneratedColumn is the sentinel for the Bug 109 silent-
// PII-leak refusal (v0.92.2): a rule whose selector resolves to a
// GENERATED column (PG `GENERATED ALWAYS AS (...) STORED` or MySQL
// `GENERATED ALWAYS AS (...)` virtual/stored). Pre-fix the redact
// rule silently no-op'd at apply time — the target's generated
// column re-derives from the unredacted *source* columns the
// expression depends on, so the operator's intent to block PII
// propagation was silently nullified. The recovery is to redact the
// source columns the expression depends on. Same family as Bug 99.
var errRedactOnGeneratedColumn = errors.New("pipeline: redaction rule targets a GENERATED column whose value is re-derived from other columns at target write-time — rule silently no-ops and PII leaks via re-derivation. Redact the source columns the expression depends on instead")

// errRedactRandomizeRangeOverflow is the sentinel for the Bug 105
// silent-PII-loss refusal (v0.92.1): a `randomize:int:LO,HI` rule
// where LO or HI exceeds the target column's representable integer
// range. Pre-fix, the random int64 sluice generated was silently
// clamped at the DB layer (with the MySQL silent-mode bug — Bug
// 102/103 family) to the column's MAX, so every row got the same
// surrogate, defeating the randomization that PII compliance
// depends on. Refusing at preflight keeps the operator's compliance
// posture visible at startup, BEFORE any data moves.
var errRedactRandomizeRangeOverflow = errors.New("pipeline: randomize:int rule has Min/Max outside the target column's representable integer range (would silently clamp to MAX — defeats randomization)")

// preflightRedactTypes inspects every redaction rule in the
// registry against the (post-mappings) schema, refusing
// combinations whose strategy output won't satisfy the target
// column type's strict constraints OR whose strategy can't run
// at all (randomize:* on a no-PK table). nil/empty registry is a
// zero-cost no-op.
//
// Called AFTER [translate.ApplyMappings] so the column types
// reflect operator-supplied `--type-override` choices — an
// operator who passed `--type-override=users.id=text` to route
// around the issue sees their column already re-typed away from
// UUID and the rule passes.
//
// Checks:
//
//   - **Selector resolution (Bug 99 / v0.91.1).** Every rule's
//     (Table, Column) must resolve to a real column in the post-
//     mappings schema. A rule whose selector resolves to nothing
//     applies to nothing → silent PII leak. Refuses BEFORE the per-
//     strategy checks below so a typo'd `--redact` is caught even
//     when none of the type / PK / keyset checks would fire (e.g.,
//     `static` or `hash:sha256` strategies that have no preflight
//     of their own).
//   - `mask:uuid` on a column whose (post-mappings) type is still
//     [ir.UUID]. Refuses with a clear message naming the column
//     and pointing at the `--type-override=table.col=text`
//     workaround (Bug 60 / v0.58.1).
//   - `randomize:*` on a table whose source schema has no primary
//     key. Refuses with a clear message naming the table and
//     suggesting the operator either add a PK to the source or
//     pick a non-random strategy (hash:sha256 / mask:* / static:)
//     for that column (PII Phase 2.c / v0.59.0).
//
// Returns nil when every rule is compatible. Returns an error
// listing every offending rule when one or more fail; operators
// see the full set in a single run instead of fix-rerun-fix-rerun
// cycles.
func preflightRedactTypes(reg *redact.Registry, schema *ir.Schema) error {
	if reg == nil || reg.Empty() || schema == nil {
		return nil
	}
	var typeProblems []string
	var randomizeProblems []string
	var randomizeOverflowProblems []string
	var keysetProblems []string
	var selectorProblems []string
	var generatedColumnProblems []string
	for _, rule := range reg.Rules() {
		name := rule.Strategy.Name()
		qualified := rule.Column
		if rule.Table != "" {
			qualified = rule.Table + "." + rule.Column
		}
		if rule.Schema != "" {
			qualified = rule.Schema + "." + qualified
		}
		// Selector resolution check (Bug 99 / v0.91.1). Rules whose
		// (Table, Column) doesn't resolve are typo-class
		// misconfigurations that would silently leak PII; refuse loudly
		// before the per-strategy checks run. The check is gated on
		// rule.Table being non-empty — a future "redact every column
		// named X" wildcard rule (no Table) is not in v1's scope and
		// not handled here.
		if rule.Table != "" {
			col := findSchemaColumn(schema, rule.Table, rule.Column)
			if col == nil {
				selectorProblems = append(selectorProblems, fmt.Sprintf(
					"  - %s: rule's selector does not match any column in the source schema. "+
						"Check the table and column names against `sluice schema preview --source-driver=...`; "+
						"this almost always means a typo. If the rule was intentionally targeted at a "+
						"table excluded via --exclude-table, remove the rule (a rule that applies to nothing "+
						"is a silent PII-leak hazard, not a no-op). Strategy: %s.",
					qualified, name,
				))
				// Skip the per-strategy checks below — a rule that doesn't
				// resolve has nothing meaningful to validate against.
				continue
			}
			// Bug 109 (v0.92.2) — refuse rules targeting GENERATED
			// columns. The target's generated column re-derives from
			// the source columns the expression depends on; the
			// redact rule on the generated column itself silently
			// no-ops and PII leaks via the re-derivation. The recovery
			// hint names the dependency-tracing workflow.
			if col.IsGenerated() {
				generatedColumnProblems = append(generatedColumnProblems, fmt.Sprintf(
					"  - %s: column is GENERATED (expression: %q, dialect %q). The rule would "+
						"silently no-op at apply time — the target's generated column re-derives "+
						"from the source columns the expression depends on, so PII would still "+
						"appear on the target via the re-derivation. Strategy: %s. "+
						"Recovery: trace the columns the GENERATED expression depends on, and "+
						"redact those source columns instead. Example: if the GENERATED column "+
						"is `lower(email)`, redact `email` (not the generated column).",
					qualified, col.GeneratedExpr, col.GeneratedExprDialect, name,
				))
				// Skip the per-strategy checks below — the rule isn't
				// going to be applied anyway, so its strategy-level
				// constraints aren't meaningful to validate.
				continue
			}
		}
		if redact.StrategyNeedsKeyButMissing(rule.Strategy) {
			keysetProblems = append(keysetProblems, fmt.Sprintf(
				"  - %s: strategy %s requires --keyset-source; the built-in v0.61.0 key was removed in PII Phase 4 (ADR-0041). Supply --keyset-source=file:<path>|env:<var>|db:<dsn> and reference a key via the rule's 'key:' option (or rely on the keyset default / sole entry).",
				qualified, name,
			))
		}
		switch {
		case name == "mask:uuid":
			col := findSchemaColumn(schema, rule.Table, rule.Column)
			if col == nil {
				continue
			}
			if _, isUUID := col.Type.(ir.UUID); !isUUID {
				continue
			}
			typeProblems = append(typeProblems, fmt.Sprintf(
				"  - %s: mask:uuid output contains 'X' characters which are not valid hex; the target's UUID column type will refuse them mid-bulk-copy. Either switch to a different strategy (hash:sha256 / truncate:N) or override the target column type via --type-override=%s.%s=text (the latter re-types the destination column so the masked string lands cleanly).",
				qualified, rule.Table, rule.Column,
			))
		case strings.HasPrefix(name, "randomize:"):
			table := findSchemaTable(schema, rule.Table)
			if table == nil {
				continue // table not in scope; nothing to check
			}
			if table.PrimaryKey != nil && len(table.PrimaryKey.Columns) > 0 {
				// PK present — continue to the randomize:int range
				// check below; other randomize:* strategies have no
				// further check.
			} else {
				randomizeProblems = append(randomizeProblems, fmt.Sprintf(
					"  - %s: strategy %s requires a primary key on the source table (replay-stable randomization derives its seed from PK values; without a PK each row would draw an unrelated random value on every run, breaking idempotency). Either add a PRIMARY KEY to %s on the source, or pick a non-random strategy (hash:sha256, mask:*, static:) for this column.",
					qualified, name, rule.Table,
				))
				continue
			}
			// Bug 105 (v0.92.1) — randomize:int:LO,HI must fit the
			// target column's integer width. If LO or HI exceeds the
			// representable range, every row would silently clamp to
			// MAX (with sql_mode-strict on MySQL the clamp becomes
			// loud at apply time — Bugs 102+103+strict-mode patch in
			// this same release — but the preflight catches the
			// configuration error one round-trip earlier).
			if ri, ok := rule.Strategy.(redact.RandomizeInt); ok {
				col := findSchemaColumn(schema, rule.Table, rule.Column)
				if col == nil {
					continue // selector-unresolved already caught above
				}
				intT, isInt := col.Type.(ir.Integer)
				if !isInt {
					continue // non-integer column — let DB enforce type compatibility
				}
				lo, hi := integerColumnRange(intT)
				if ri.Min < lo || ri.Max > hi {
					randomizeOverflowProblems = append(randomizeOverflowProblems, fmt.Sprintf(
						"  - %s: strategy %s has Min=%d Max=%d outside the column's representable integer range [%d,%d] (Width=%d, Unsigned=%v). "+
							"Either narrow the Min,Max to fit the column, or widen the column type via --type-override. Pre-v0.92.1, an out-of-range randomize:int silently clamped to MAX every row "+
							"(defeating randomization → PII compliance failure).",
						qualified, name, ri.Min, ri.Max, lo, hi, intT.Width, intT.Unsigned,
					))
				}
			}
		}
	}
	if len(typeProblems) == 0 && len(randomizeProblems) == 0 && len(randomizeOverflowProblems) == 0 && len(keysetProblems) == 0 && len(selectorProblems) == 0 && len(generatedColumnProblems) == 0 {
		return nil
	}
	onlySelector := len(selectorProblems) > 0 && len(typeProblems) == 0 && len(randomizeProblems) == 0 && len(randomizeOverflowProblems) == 0 && len(keysetProblems) == 0 && len(generatedColumnProblems) == 0
	if onlySelector {
		return fmt.Errorf("%w (Bug 99 / v0.91.1 preflight):\n%s", errRedactSelectorUnresolved, strings.Join(selectorProblems, "\n"))
	}
	// Generated-column refusal — also fundamental (rule would silently
	// no-op). Surface on its own when it's the only failure so the
	// operator sees the actionable recovery (redact source columns).
	onlyGenerated := len(generatedColumnProblems) > 0 && len(selectorProblems) == 0 && len(typeProblems) == 0 && len(randomizeProblems) == 0 && len(randomizeOverflowProblems) == 0 && len(keysetProblems) == 0
	if onlyGenerated {
		return fmt.Errorf("%w (Bug 109 / v0.92.2 preflight):\n%s", errRedactOnGeneratedColumn, strings.Join(generatedColumnProblems, "\n"))
	}
	onlyKeyset := len(keysetProblems) > 0 && len(typeProblems) == 0 && len(randomizeProblems) == 0 && len(randomizeOverflowProblems) == 0 && len(selectorProblems) == 0 && len(generatedColumnProblems) == 0
	if onlyKeyset {
		return fmt.Errorf("%w (PII Phase 4 / ADR-0041 preflight):\n%s", errRedactKeysetMissing, strings.Join(keysetProblems, "\n"))
	}
	onlyType := len(typeProblems) > 0 && len(randomizeProblems) == 0 && len(randomizeOverflowProblems) == 0 && len(keysetProblems) == 0 && len(selectorProblems) == 0 && len(generatedColumnProblems) == 0
	if onlyType {
		return fmt.Errorf("%w (Bug 60 / v0.58.1 preflight):\n%s", errRedactTypeMismatch, strings.Join(typeProblems, "\n"))
	}
	onlyRandomizeNoPK := len(randomizeProblems) > 0 && len(randomizeOverflowProblems) == 0 && len(typeProblems) == 0 && len(keysetProblems) == 0 && len(selectorProblems) == 0 && len(generatedColumnProblems) == 0
	if onlyRandomizeNoPK {
		return fmt.Errorf("%w (PII Phase 2.c / v0.59.0 preflight):\n%s", errRedactRandomizeNoPK, strings.Join(randomizeProblems, "\n"))
	}
	onlyRandomizeOverflow := len(randomizeOverflowProblems) > 0 && len(randomizeProblems) == 0 && len(typeProblems) == 0 && len(keysetProblems) == 0 && len(selectorProblems) == 0 && len(generatedColumnProblems) == 0
	if onlyRandomizeOverflow {
		return fmt.Errorf("%w (Bug 105 / v0.92.1 preflight):\n%s", errRedactRandomizeRangeOverflow, strings.Join(randomizeOverflowProblems, "\n"))
	}
	// Multiple categories non-empty: surface all with a combined
	// header so the operator sees the full picture in one run.
	combined := append([]string{}, selectorProblems...)
	combined = append(combined, generatedColumnProblems...)
	combined = append(combined, keysetProblems...)
	combined = append(combined, typeProblems...)
	combined = append(combined, randomizeProblems...)
	combined = append(combined, randomizeOverflowProblems...)
	return fmt.Errorf("%w / %w / %w / %w / %w / %w (combined preflight):\n%s", errRedactSelectorUnresolved, errRedactOnGeneratedColumn, errRedactKeysetMissing, errRedactTypeMismatch, errRedactRandomizeNoPK, errRedactRandomizeRangeOverflow, strings.Join(combined, "\n"))
}

// integerColumnRange returns the inclusive [min, max] integer range
// for the given ir.Integer column. Handles signed widths 8/16/24/32/64
// and the unsigned [0, 2^width - 1] mirror. Width=24 is MySQL's
// MEDIUMINT (range [-2^23, 2^23-1] signed, [0, 2^24-1] unsigned).
//
// 64-bit unsigned overflows int64; sluice's randomize:int Min/Max are
// int64-typed, so an unsigned 64-bit column's effective upper bound
// for a randomize:int rule is math.MaxInt64 — Min/Max in the
// 2^63..2^64-1 range can't be expressed as int64 and the operator
// would need a different strategy (the configuration would also fail
// the int64 CLI parse).
func integerColumnRange(t ir.Integer) (lo, hi int64) {
	if t.Unsigned {
		switch t.Width {
		case 8:
			return 0, 255
		case 16:
			return 0, 65535
		case 24:
			return 0, 16777215
		case 32:
			return 0, 4294967295
		default: // 64-bit unsigned — clamp at int64 max for rule purposes
			return 0, math.MaxInt64
		}
	}
	switch t.Width {
	case 8:
		return -128, 127
	case 16:
		return -32768, 32767
	case 24:
		return -8388608, 8388607
	case 32:
		return -2147483648, 2147483647
	default: // 64-bit signed
		return math.MinInt64, math.MaxInt64
	}
}

// findSchemaTable returns the *ir.Table for name, or nil if not
// found in schema. Used by the randomize-no-PK preflight to look up
// the PK of the rule's table.
func findSchemaTable(schema *ir.Schema, name string) *ir.Table {
	for _, t := range schema.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// findSchemaColumn looks up a column in the schema by table + column
// name. Returns nil if not found.
func findSchemaColumn(schema *ir.Schema, table, column string) *ir.Column {
	for _, t := range schema.Tables {
		if t.Name != table {
			continue
		}
		for _, c := range t.Columns {
			if c.Name == column {
				return c
			}
		}
		return nil
	}
	return nil
}
