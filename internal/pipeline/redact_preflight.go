// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
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
	var keysetProblems []string
	for _, rule := range reg.Rules() {
		name := rule.Strategy.Name()
		if redact.StrategyNeedsKeyButMissing(rule.Strategy) {
			qualified := rule.Column
			if rule.Table != "" {
				qualified = rule.Table + "." + rule.Column
			}
			if rule.Schema != "" {
				qualified = rule.Schema + "." + qualified
			}
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
			qualified := rule.Column
			if rule.Table != "" {
				qualified = rule.Table + "." + rule.Column
			}
			if rule.Schema != "" {
				qualified = rule.Schema + "." + qualified
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
				continue
			}
			qualified := rule.Column
			if rule.Table != "" {
				qualified = rule.Table + "." + rule.Column
			}
			if rule.Schema != "" {
				qualified = rule.Schema + "." + qualified
			}
			randomizeProblems = append(randomizeProblems, fmt.Sprintf(
				"  - %s: strategy %s requires a primary key on the source table (replay-stable randomization derives its seed from PK values; without a PK each row would draw an unrelated random value on every run, breaking idempotency). Either add a PRIMARY KEY to %s on the source, or pick a non-random strategy (hash:sha256, mask:*, static:) for this column.",
				qualified, name, rule.Table,
			))
		}
	}
	if len(typeProblems) == 0 && len(randomizeProblems) == 0 && len(keysetProblems) == 0 {
		return nil
	}
	// Keyset-missing is the most fundamental misconfiguration (no key
	// material at all); surface it first and on its own when it's the
	// only failure so the operator gets the single actionable fix.
	if len(keysetProblems) > 0 && len(typeProblems) == 0 && len(randomizeProblems) == 0 {
		return fmt.Errorf("%w (PII Phase 4 / ADR-0041 preflight):\n%s", errRedactKeysetMissing, strings.Join(keysetProblems, "\n"))
	}
	if len(typeProblems) > 0 && len(randomizeProblems) == 0 && len(keysetProblems) == 0 {
		return fmt.Errorf("%w (Bug 60 / v0.58.1 preflight):\n%s", errRedactTypeMismatch, strings.Join(typeProblems, "\n"))
	}
	if len(randomizeProblems) > 0 && len(typeProblems) == 0 && len(keysetProblems) == 0 {
		return fmt.Errorf("%w (PII Phase 2.c / v0.59.0 preflight):\n%s", errRedactRandomizeNoPK, strings.Join(randomizeProblems, "\n"))
	}
	// Multiple categories non-empty: surface all with a combined
	// header so the operator sees the full picture in one run.
	combined := append([]string{}, keysetProblems...)
	combined = append(combined, typeProblems...)
	combined = append(combined, randomizeProblems...)
	return fmt.Errorf("%w / %w / %w (combined preflight):\n%s", errRedactKeysetMissing, errRedactTypeMismatch, errRedactRandomizeNoPK, strings.Join(combined, "\n"))
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
