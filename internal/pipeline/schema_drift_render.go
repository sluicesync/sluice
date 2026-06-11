// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0060 — Operator-actionable rendering of [irdiff.SchemaDriftReport]
// for inclusion in CDC apply-side refuse-loudly messages.
//
// F11 (Reddit-research severity-A): when sluice refuses to forward a
// source DDL on the CDC stream, the refusal text must surface WHAT
// changed and WHAT THE OPERATOR SHOULD DO — not just the table name.
// This file owns the rendering pass; the underlying pure-function
// diff lives in [irdiff.TableDrift]. The split keeps the engine-neutral
// diff data structure in `internal/ir/` while the
// pipeline-specific operator-action wording (drained-model recovery
// hint, --forward-schema-add-column reference, etc.) stays here in
// the orchestrator package.
//
// Rendering policy:
//
//   - One line per change. Operators paste these into Slack/tickets
//     for incident triage, so per-line clarity beats compact prose.
//   - Every line is prefixed with the change category in brackets
//     (e.g. "[column-added]", "[column-dropped]") so the renderer's
//     output is greppable and the operator can filter on category.
//   - Each line ends with a per-change action hint where one
//     applies — generic recovery (drained model) is added once at
//     the end of the refusal message by [forwardRecoveryHint], not
//     repeated per-line.

import (
	"strings"

	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

// RenderSchemaDriftReport produces the multi-line operator-actionable
// rendering of an [irdiff.SchemaDriftReport] for inclusion in a
// refuse-loudly error message. Returns the empty string when the
// report carries no changes (callers should short-circuit on
// [irdiff.SchemaDriftReport.HasChanges] anyway, but the renderer is
// defensive).
//
// The output is a sequence of "\n" + "  " (two-space) indented lines,
// one per drift entry. Callers concatenate this directly to their
// error message body — e.g.
//
//	fmt.Errorf("schema change on %q: shape %s.%s", name, shape, render)
//
// Trailing newlines are NOT added; the final line ends without one,
// so the outer error message can append its recovery hint cleanly.
func RenderSchemaDriftReport(r irdiff.SchemaDriftReport) string {
	if !r.HasChanges() {
		return ""
	}
	var lines []string
	for _, c := range r.ColumnsAdded {
		lines = append(lines, renderColumnAddedLine(c))
	}
	for _, c := range r.ColumnsDropped {
		lines = append(lines, renderColumnDroppedLine(c))
	}
	for _, c := range r.ColumnsRenamed {
		lines = append(lines, renderColumnRenamedLine(c))
	}
	for _, c := range r.ColumnsAltered {
		lines = append(lines, renderColumnAlteredLine(c))
	}
	for _, c := range r.IndexesAdded {
		lines = append(lines, renderIndexAddedLine(c))
	}
	for _, c := range r.IndexesDropped {
		lines = append(lines, renderIndexDroppedLine(c))
	}
	for _, c := range r.ChecksAdded {
		lines = append(lines, renderCheckAddedLine(c))
	}
	for _, c := range r.ChecksDropped {
		lines = append(lines, renderCheckDroppedLine(c))
	}
	for _, c := range r.ChecksAltered {
		lines = append(lines, renderCheckAlteredLine(c))
	}
	for _, c := range r.ForeignKeysAdded {
		lines = append(lines, renderFKAddedLine(c))
	}
	for _, c := range r.ForeignKeysDropped {
		lines = append(lines, renderFKDroppedLine(c))
	}
	for _, c := range r.ForeignKeysAltered {
		lines = append(lines, renderFKAlteredLine(c))
	}
	if len(lines) == 0 {
		return ""
	}
	return "\n  " + strings.Join(lines, "\n  ")
}

// nullableLabel renders the Nullable flag as "NULL" / "NOT NULL".
func nullableLabel(n bool) string {
	if n {
		return "NULL"
	}
	return "NOT NULL"
}

// renderColumnAddedLine formats a single ColumnsAdded entry. Includes
// the type/nullable/default and the canonical operator-action hint:
// either run drained schema migrate, or restart with
// --forward-schema-add-column to opt into auto-forwarding.
func renderColumnAddedLine(c irdiff.ColumnDriftEntry) string {
	base := "[column-added] " + c.Name + " " + c.Type + " " + nullableLabel(c.Nullable)
	if c.Default != "<none>" {
		base += " DEFAULT " + c.Default
	}
	base += " — drained schema migrate to add this column on the target before resuming; " +
		"OR restart with --forward-schema-add-column to auto-forward future ADD COLUMN events (ADR-0058)"
	return base
}

// renderColumnDroppedLine formats a single ColumnsDropped entry. The
// operator-action wording differs from add — drop on the target is
// destructive, so the recovery is explicit DROP after a drained
// stop, not auto-forwarded.
func renderColumnDroppedLine(c irdiff.ColumnDriftEntry) string {
	return "[column-dropped] " + c.Name + " (was " + c.Type + " " + nullableLabel(c.Nullable) + ")" +
		" — drained schema migrate to drop this column on the target before resuming; " +
		"DROP COLUMN is destructive, no auto-forwarding"
}

// renderColumnRenamedLine formats a ColumnRenamed entry. The
// indistinguishable-from-drop-add-same-attributes edge is documented
// inline (ADR-0054 v0.78.0).
func renderColumnRenamedLine(c irdiff.ColumnRenameEntry) string {
	return "[column-renamed] " + c.OldName + " → " + c.NewName + " " + c.Type +
		" — drained schema migrate to RENAME on the target before resuming; " +
		"RENAME COLUMN is not auto-forwarded (ADR-0058 v1 scope)"
}

// renderColumnAlteredLine formats a ColumnAltered entry. Lists every
// AlterKind so the operator sees both type AND nullability changes
// when they fired together.
func renderColumnAlteredLine(c irdiff.ColumnAlterEntry) string {
	parts := make([]string, 0, len(c.AlterKinds))
	for _, k := range c.AlterKinds {
		switch k {
		case irdiff.ColumnAlterType:
			parts = append(parts, "type "+c.Before.Type+" → "+c.After.Type)
		case irdiff.ColumnAlterNullable:
			parts = append(parts, "nullability "+nullableLabel(c.Before.Nullable)+" → "+nullableLabel(c.After.Nullable))
		case irdiff.ColumnAlterDefault:
			parts = append(parts, "default "+c.Before.Default+" → "+c.After.Default)
		case irdiff.ColumnAlterGeneratedExpr:
			parts = append(parts, "generated-expr changed")
		}
	}
	return "[column-altered] " + c.Name + " (" + strings.Join(parts, ", ") + ")" +
		" — drained schema migrate to apply the change on the target before resuming; " +
		"ALTER COLUMN is not auto-forwarded"
}

// renderIndexAddedLine formats an IndexesAdded entry.
func renderIndexAddedLine(c irdiff.IndexDriftEntry) string {
	kind := "index"
	if c.Unique {
		kind = "unique index"
	}
	return "[index-added] " + kind + " " + c.Name + " on (" + c.Columns + ")" +
		" — drained schema migrate to add the index on the target before resuming; " +
		"CREATE INDEX is not auto-forwarded (concurrent rebuild needs operator scheduling)"
}

// renderIndexDroppedLine formats an IndexesDropped entry.
func renderIndexDroppedLine(c irdiff.IndexDriftEntry) string {
	kind := "index"
	if c.Unique {
		kind = "unique index"
	}
	return "[index-dropped] " + kind + " " + c.Name + " (was on " + c.Columns + ")" +
		" — drained schema migrate to drop the index on the target before resuming"
}

// renderCheckAddedLine formats a ChecksAdded entry.
func renderCheckAddedLine(c irdiff.CheckDriftEntry) string {
	return "[check-added] " + c.Name + " CHECK (" + c.Expr + ")" +
		" — drained schema migrate to add the constraint on the target before resuming; " +
		"existing target rows may violate the new constraint — validate before applying"
}

// renderCheckDroppedLine formats a ChecksDropped entry.
func renderCheckDroppedLine(c irdiff.CheckDriftEntry) string {
	return "[check-dropped] " + c.Name + " (was CHECK " + c.Expr + ")" +
		" — drained schema migrate to drop the constraint on the target before resuming"
}

// renderCheckAlteredLine formats a ChecksAltered entry.
func renderCheckAlteredLine(c irdiff.CheckAlterEntry) string {
	return "[check-altered] " + c.Name + " CHECK (" + c.BeforeExpr + ") → CHECK (" + c.AfterExpr + ")" +
		" — drained schema migrate to update the constraint on the target before resuming"
}

// renderFKAddedLine formats a ForeignKeysAdded entry.
func renderFKAddedLine(c irdiff.ForeignKeyDriftEntry) string {
	name := c.Name
	if name == "" {
		name = "<unnamed>"
	}
	return "[fk-added] " + name + " (" + c.Columns + ") → " + c.ReferencedTable + "(" + c.ReferencedColumns + ")" +
		" — drained schema migrate to add the foreign key on the target before resuming; " +
		"NOT VALID + VALIDATE is recommended for large tables"
}

// renderFKDroppedLine formats a ForeignKeysDropped entry.
func renderFKDroppedLine(c irdiff.ForeignKeyDriftEntry) string {
	name := c.Name
	if name == "" {
		name = "<unnamed>"
	}
	return "[fk-dropped] " + name + " (was " + c.Columns + " → " + c.ReferencedTable + "(" + c.ReferencedColumns + "))" +
		" — drained schema migrate to drop the foreign key on the target before resuming"
}

// renderFKAlteredLine formats a ForeignKeysAltered entry.
func renderFKAlteredLine(c irdiff.ForeignKeyAlterEntry) string {
	name := c.Name
	if name == "" {
		name = "<unnamed>"
	}
	return "[fk-altered] " + name + " (" + c.Before.Columns + " → " + c.Before.ReferencedTable +
		"(" + c.Before.ReferencedColumns + ")) → (" + c.After.Columns + " → " + c.After.ReferencedTable +
		"(" + c.After.ReferencedColumns + "))" +
		" — drained schema migrate to update the foreign key on the target before resuming"
}
