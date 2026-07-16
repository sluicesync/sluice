// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pre-create gate for existing target tables (ADR-0166, roadmap item
// 71b). Every engine's CreateTablesWithoutConstraints emits
// `CREATE TABLE IF NOT EXISTS`, so before this gate a pre-existing
// same-name table was silently tolerated WHATEVER its shape: a
// conflicting table surfaced only mid-copy as a confusing Error 1054
// that the shared drift classifier retries for the full ADR-0108
// 30-minute wall (v0.99.256 cycle observation), and on a PlanetScale
// safe-migrations branch a deploy-ddl-bootstrapped schema could not
// feed a fresh migrate at all (the CREATE is refused even when the
// table exists — item 71c's coded refusal).
//
// The gate reads the target's existing tables through the SAME
// SchemaReader surface `sluice schema diff` trusts, compares each
// pre-existing same-name table's COLUMN SHAPE (names, types,
// nullability — see irdiff.TableColumnShape for the deliberate
// exclusions) against the intended IR, and:
//
//   - absent        → create exactly as before;
//   - equal shape   → SKIP the CREATE with an INFO naming the table —
//     the deploy-ddl bootstrap now feeds a fresh migrate;
//   - differs       → the coded SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH
//     refusal, upfront, naming the first differing columns — never
//     the mid-copy retry wall;
//   - compare uncomputable (reader open/read failed) → WARN and fall
//     back to today's behavior (create everything, IF NOT EXISTS
//     tolerates) — the gate must never invent a new failure mode.
//
// The gate is skipped on --resume: the prior attempt already created
// (or validated) the tables, and re-running the idempotent CREATE is
// the long-standing resume contract — re-comparing would only add a
// round-trip-fidelity failure mode to a path that has none.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
	"sluicesync.dev/sluice/internal/translate"
)

// shapeMismatchColumnsShown caps how many differing columns one
// table's refusal message spells out; the remainder is summarized as a
// count so a wildly different table doesn't produce a page of error.
const shapeMismatchColumnsShown = 3

// phasePlanExistingTables partitions the schema's tables into
// create-vs-skip against the target's existing catalog and returns the
// schema subset the CREATE phase should apply. It refuses (coded) on
// any pre-existing same-name table whose column shape differs, and
// returns the input schema unchanged — with a WARN — when the target
// catalog cannot be read (never a new failure mode).
//
// The returned schema is a shallow clone when any table is skipped;
// schema-level objects (sequences, views) are carried through
// untouched — only the CREATE TABLE set shrinks. Every OTHER phase
// (bulk copy, indexes, constraints, views) keeps consuming the full
// schema: a pre-created table still receives its data, and the
// index/constraint phases are detect-then-skip idempotent, so a
// bootstrapped table that already carries them converges cleanly.
func (m *Migrator) phasePlanExistingTables(ctx context.Context, schema *ir.Schema) (*ir.Schema, error) {
	actual, ok := m.readTargetTablesForShapeGate(ctx)
	if !ok || len(actual) == 0 {
		return schema, nil
	}

	// Compare against the target engine's STORAGE shapes, mirroring the
	// schema-diff command: cross-engine pairs rewrite source-native IR
	// types to what the target writer would emit (PG uuid → CHAR(36),
	// …) so the IR comparison sees the shape the catalog will read
	// back. Same-engine pairs are identity. RetargetForEngine clones;
	// the writer keeps the untouched schema.
	expected := translate.RetargetForEngine(schema, m.Source.Name(), m.Target.Name())
	expTables := make(map[string]*ir.Table, len(expected.Tables))
	for _, t := range expected.Tables {
		expTables[t.Name] = t
	}

	skip := make(map[string]struct{})
	var refusals []string
	for _, t := range schema.Tables {
		act, exists := actual[t.Name]
		if !exists {
			continue
		}
		mismatches := irdiff.TableColumnShape(expTables[t.Name], act)
		if len(mismatches) == 0 {
			skip[t.Name] = struct{}{}
			slog.InfoContext(ctx,
				"migration: target table exists with matching column shape — skipping create",
				slog.String("table", t.Name))
			continue
		}
		refusals = append(refusals, renderShapeMismatch(t.Name, mismatches))
	}

	if len(refusals) > 0 {
		return nil, sluicecode.Wrap(
			sluicecode.CodeTargetTableShapeMismatch,
			"drop or rename the conflicting target table(s), exclude them with --exclude-table, or alter their shape to match `sluice schema preview`; --reset-target-data drops every in-scope target table first",
			fmt.Errorf(
				"pipeline: %d pre-existing target table(s) differ from the schema this migration would create — refusing before any data moves (proceeding would fail mid-copy or land rows in the wrong columns): %s",
				len(refusals), strings.Join(refusals, "; "),
			),
		)
	}
	if len(skip) == 0 {
		return schema, nil
	}
	createSchema := *schema
	createSchema.Tables = make([]*ir.Table, 0, len(schema.Tables)-len(skip))
	for _, t := range schema.Tables {
		if _, skipped := skip[t.Name]; !skipped {
			createSchema.Tables = append(createSchema.Tables, t)
		}
	}
	return &createSchema, nil
}

// readTargetTablesForShapeGate reads the target's existing tables via
// the target engine's own SchemaReader (the schema-diff surface),
// indexed by table name. ok=false means the compare is uncomputable —
// already WARNed — and the caller must fall back to today's behavior.
func (m *Migrator) readTargetTablesForShapeGate(ctx context.Context) (map[string]*ir.Table, bool) {
	warnFallback := func(step string, err error) {
		slog.WarnContext(ctx,
			"migration: cannot read the target's existing tables — skipping the pre-create shape compare (pre-existing same-name tables are tolerated as before)",
			slog.String("step", step), slog.String("err", err.Error()))
	}
	tr, err := m.Target.OpenSchemaReader(ctx, m.TargetDSN)
	if err != nil {
		warnFallback("open target schema reader", err)
		return nil, false
	}
	defer migcore.CloseIf(tr)
	migcore.ApplyTargetSchema(tr, m.TargetSchema)
	if err := applyEnabledPGExtensions(ctx, tr, m.EnabledPGExtensions); err != nil {
		warnFallback("enable PG extensions on target reader", err)
		return nil, false
	}
	actual, err := tr.ReadSchema(ctx)
	if err != nil {
		warnFallback("read target schema", err)
		return nil, false
	}
	if actual == nil {
		return nil, true
	}
	out := make(map[string]*ir.Table, len(actual.Tables))
	for _, t := range actual.Tables {
		if t != nil {
			out[t.Name] = t
		}
	}
	return out, true
}

// renderShapeMismatch renders one table's refusal fragment: the table
// name plus the first few differing columns (expected vs actual).
func renderShapeMismatch(table string, mismatches []irdiff.ColumnShapeMismatch) string {
	var b strings.Builder
	fmt.Fprintf(&b, "table %q (", table)
	for i, mm := range mismatches {
		if i == shapeMismatchColumnsShown {
			fmt.Fprintf(&b, ", and %d more column(s)", len(mismatches)-shapeMismatchColumnsShown)
			break
		}
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "column %q: want %s, target has %s", mm.Column, mm.Expected, mm.Actual)
	}
	b.WriteString(")")
	return b.String()
}
