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

// preflightRedactTypes inspects every redaction rule in the
// registry against the (post-mappings) schema, refusing
// combinations whose strategy output won't satisfy the target
// column type's strict constraints. nil/empty registry is a
// zero-cost no-op.
//
// Called AFTER [translate.ApplyMappings] so the column types
// reflect operator-supplied `--type-override` choices — an
// operator who passed `--type-override=users.id=text` to route
// around the issue sees their column already re-typed away from
// UUID and the rule passes.
//
// Currently checks:
//
//   - `mask:uuid` on a column whose (post-mappings) type is still
//     [ir.UUID]. Refuses with a clear message naming the column
//     and pointing at the `--type-override=table.col=text`
//     workaround.
//
// Returns nil when every rule is compatible. Returns an error
// listing every offending rule (one rule per column) when one or
// more fail; operators see the full set in a single run instead
// of fix-rerun-fix-rerun cycles.
func preflightRedactTypes(reg *redact.Registry, schema *ir.Schema) error {
	if reg == nil || reg.Empty() || schema == nil {
		return nil
	}
	var problems []string
	for _, rule := range reg.Rules() {
		// Only mask:uuid currently has a known type-shape conflict.
		if rule.Strategy.Name() != "mask:uuid" {
			continue
		}
		col := findSchemaColumn(schema, rule.Table, rule.Column)
		if col == nil {
			continue // column not in scope of this migration
		}
		if _, isUUID := col.Type.(ir.UUID); !isUUID {
			continue // operator has re-typed the column via --type-override
		}
		qualified := rule.Column
		if rule.Table != "" {
			qualified = rule.Table + "." + rule.Column
		}
		if rule.Schema != "" {
			qualified = rule.Schema + "." + qualified
		}
		problems = append(problems, fmt.Sprintf(
			"  - %s: mask:uuid output contains 'X' characters which are not valid hex; the target's UUID column type will refuse them mid-bulk-copy. Either switch to a different strategy (hash:sha256 / truncate:N) or override the target column type via --type-override=%s.%s=text (the latter re-types the destination column so the masked string lands cleanly).",
			qualified, rule.Table, rule.Column,
		))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w (Bug 60 / v0.58.1 preflight):\n%s", errRedactTypeMismatch, strings.Join(problems, "\n"))
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
