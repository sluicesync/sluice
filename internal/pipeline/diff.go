// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Schema-diff orchestration for the `sluice schema diff` CLI
// (ADR-0029). Reads the source schema, applies the translation
// pipeline (filter + per-column type-mapping overrides) to produce
// the *expected* shape on the target, reads the *actual* shape from
// the target's SchemaReader, then runs an IR-level diff and renders
// the result — text (with copy-paste DDL suggestions) or JSON.
//
// Engine-neutral: every engine-specific operation goes through
// ir.Engine. Identifier-quoting differences are handled by an engine-
// name switch in the text renderer; this is intentional (the diff is
// a read-only inspection tool, not a migration writer) and avoids
// growing a new ir surface for the same job.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
)

// Differ runs a single schema-diff against the configured source/
// target pair. Same shape as [Previewer]: hold config, call Run.
type Differ struct {
	// Source / Target are the engines the source/target DSNs belong
	// to. Required.
	Source ir.Engine
	Target ir.Engine

	// SourceDSN is the source-engine-native DSN. ReadSchema is run
	// against it to derive the expected target shape (after
	// translation). Required.
	SourceDSN string

	// TargetDSN is the target-engine-native DSN. ReadSchema is run
	// against it to capture the actual on-target shape. Required.
	TargetDSN string

	// Mappings is the per-column type-override list, applied to the
	// source schema before computing the diff. Empty disables the
	// override step.
	Mappings []config.Mapping

	// ExpressionMappings is the per-column generated-expression
	// override list. Applied alongside Mappings so the diff compares
	// what migrate would actually emit (overridden bodies and all).
	ExpressionMappings []config.ExpressionMapping

	// Filter selects which source tables participate. Empty (zero
	// value) keeps every source table the reader returns.
	Filter TableFilter

	// ViewFilter selects which source views participate in the
	// diff. Empty keeps every view; SkipViews=true drops them all.
	ViewFilter ViewFilter

	// SkipViews drops every source view before computing the diff.
	// Useful when the operator manages views out-of-band and
	// considers any target-side view drift as not-sluice's-concern.
	SkipViews bool

	// Format is "text" (default) or "json". Empty defaults to "text".
	Format string

	// IgnoreCharsetCollation suppresses MySQL-specific charset/
	// collation diffs. Reserved for the v1.x extension when those
	// fields land in the IR; today's IR doesn't compare them at the
	// diff layer, so the flag is plumbed for forward compatibility
	// and surfaced in the text output's preamble.
	IgnoreCharsetCollation bool

	// IgnoreExtras suppresses "extra on target" diffs (tables and
	// columns/indexes present on actual but absent from expected).
	// Useful when the target hosts other applications' tables.
	IgnoreExtras bool

	// Out is the destination for the rendered diff. Required.
	Out io.Writer

	// TargetSchema is the per-source target-schema namespace
	// (ADR-0031). When set, the diff's target-side schema reader is
	// pinned to this schema, the missing-on-target DDL suggestions
	// render schema-qualified, and the comparison runs against the
	// target's per-source namespace rather than its DSN default.
	// PG-only.
	TargetSchema string

	// EnabledPGExtensions is the operator's `--enable-pg-extension`
	// allowlist (ADR-0032). PG → PG only. Threaded through both the
	// source and target SchemaReaders so the diff sees extension-
	// owned types as IR ExtensionType on both sides; the comparison
	// then matches target-side `vector(384)` against the expected
	// shape correctly. Empty preserves the pre-v0.26.0 behaviour.
	EnabledPGExtensions []string
}

// DiffJSON is the JSON-format diff output. The shape is stable for
// tooling consumers (CI gates, dashboards). Adding fields is
// backward-compatible; renaming or removing them is not.
type DiffJSON struct {
	SourceEngine string         `json:"source_engine"`
	TargetEngine string         `json:"target_engine"`
	Summary      DiffJSONCounts `json:"summary"`
	ir.SchemaDiff
}

// DiffJSONCounts is the high-level rollup the CI consumer looks at
// first. Per-table breakdowns live in the embedded SchemaDiff.
type DiffJSONCounts struct {
	TablesMissing     int `json:"tables_missing"`
	TablesExtra       int `json:"tables_extra"`
	TablesMismatched  int `json:"tables_mismatched"`
	ColumnsMissing    int `json:"columns_missing"`
	ColumnsExtra      int `json:"columns_extra"`
	ColumnsMismatched int `json:"columns_mismatched"`
	IndexesMissing    int `json:"indexes_missing"`
	IndexesExtra      int `json:"indexes_extra"`
	ChecksMissing     int `json:"checks_missing"`
	ChecksExtra       int `json:"checks_extra"`
	ChecksMismatched  int `json:"checks_mismatched"`
	ViewsMissing      int `json:"views_missing"`
	ViewsExtra        int `json:"views_extra"`
	ViewsMismatched   int `json:"views_mismatched"`
}

// Run executes the diff. Returns the computed diff plus an error.
// On success the diff is non-nil; on failure (couldn't read either
// schema, render error) the diff is nil and err describes the
// failure. The caller's CLI layer maps the (diff, err) tuple onto
// the ADR-0029 exit codes.
func (d *Differ) Run(ctx context.Context) (*ir.SchemaDiff, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	if err := validateTargetSchema(d.Target, d.TargetSchema); err != nil {
		return nil, err
	}
	if err := validateEnabledPGExtensions(d.Source, d.Target, d.EnabledPGExtensions); err != nil {
		return nil, err
	}

	// ---- 1. Read source schema ----
	sr, err := d.Source.OpenSchemaReader(ctx, d.SourceDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	if err := applyEnabledPGExtensions(ctx, sr, d.EnabledPGExtensions); err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: enable PG extensions on source: %w", err))
	}

	// Engine-default exclusions (Bug 22): same shape as Migrator and
	// Streamer — merge engine-supplied patterns (e.g. PlanetScale's
	// `_vt_*`) when the operator is in exclude-or-no-filter mode.
	// Computed before the read so the Bug-76 scope push-down (below)
	// matches the authoritative post-read prune.
	if eff, added := effectiveTableFilter(d.Filter, d.Source, d.SourceDSN); len(added) > 0 {
		slog.InfoContext(ctx, "applying engine-default table exclusions",
			slog.String("engine", d.Source.Name()),
			slog.Any("patterns", added),
		)
		d.Filter = eff
	}

	// catalog Bug 76: scope per-column type validation to the filtered
	// table set before the schema scan.
	applyTableScope(sr, d.Filter)

	srcSchema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: read source schema: %w", err))
	}
	if len(srcSchema.Tables) == 0 {
		return nil, errors.New("diff: source schema has no tables")
	}

	if err := applyTableFilter(ctx, srcSchema, d.Filter); err != nil {
		return nil, err
	}
	applyViewFilter(ctx, srcSchema, d.ViewFilter, d.SkipViews)

	expected, err := translate.ApplyMappings(srcSchema, d.Mappings)
	if err != nil {
		return nil, fmt.Errorf("diff: apply mappings: %w", err)
	}
	expected, err = translate.ApplyExpressionOverrides(expected, d.ExpressionMappings)
	if err != nil {
		return nil, fmt.Errorf("diff: apply expression overrides: %w", err)
	}

	// Cross-engine retarget: rewrite source-native IR types to their
	// target-engine emit equivalents (PG uuid → CHAR(36), inet →
	// VARCHAR(45), etc.) so the IR comparison below sees the actual
	// target storage shape, not the un-translated source type. Same-
	// engine pairs are identity. Mappings already ran above, so any
	// operator-supplied --type-override has already replaced the IR
	// type and the retarget pattern match doesn't fire.
	expected = translate.RetargetForEngine(expected, d.Source.Name(), d.Target.Name())

	// ---- 2. Read target's actual schema via the same SchemaReader
	// surface (ADR-0029). The reader doesn't care whether a DSN points
	// at a "source" or a "target".
	tr, err := d.Target.OpenSchemaReader(ctx, d.TargetDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: open target schema reader: %w", err))
	}
	applyTargetSchema(tr, d.TargetSchema)
	if err := applyEnabledPGExtensions(ctx, tr, d.EnabledPGExtensions); err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: enable PG extensions on target: %w", err))
	}
	defer closeIf(tr)

	actual, err := tr.ReadSchema(ctx)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: read target schema: %w", err))
	}

	// ---- 3. Compute the diff. ----
	diff := ir.DiffSchemas(expected, actual, ir.DiffOptions{
		IgnoreExtras:           d.IgnoreExtras,
		IgnoreCharsetCollation: d.IgnoreCharsetCollation,
	})

	// ---- 4. Resolve missing-table DDL via the target engine's
	// PreviewDDL surface so the text renderer can include real CREATE
	// TABLE suggestions (MySQL/PG syntax) rather than a generic
	// placeholder. PreviewDDL is optional; engines without it fall
	// through to a simple comment.
	missingDDL, missingColDDL, err := previewMissingDDL(ctx, d.Target, d.TargetDSN, d.TargetSchema, d.EnabledPGExtensions, expected, diff)
	if err != nil {
		return nil, err
	}

	// ---- 5. Render. ----
	switch strings.ToLower(strings.TrimSpace(d.Format)) {
	case "", "text":
		if err := renderDiffText(d.Out, diffBundle{
			srcEngine:     d.Source.Name(),
			tgtEngine:     d.Target.Name(),
			diff:          diff,
			missingDDL:    missingDDL,
			missingColDDL: missingColDDL,
			expected:      expected,
			actual:        actual,
			opts:          diffRenderOpts{IgnoreCharsetCollation: d.IgnoreCharsetCollation, IgnoreExtras: d.IgnoreExtras},
		}); err != nil {
			return nil, err
		}
	case "json":
		if err := renderDiffJSON(d.Out, d.Source.Name(), d.Target.Name(), diff); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("diff: unknown --format %q (recognised: text, json)", d.Format)
	}
	return &diff, nil
}

func (d *Differ) validate() error {
	switch {
	case d.Source == nil:
		return errors.New("diff: Source engine is nil")
	case d.Target == nil:
		return errors.New("diff: Target engine is nil")
	case d.SourceDSN == "":
		return errors.New("diff: SourceDSN is empty")
	case d.TargetDSN == "":
		return errors.New("diff: TargetDSN is empty")
	case d.Out == nil:
		return errors.New("diff: Out writer is nil")
	}
	return nil
}

// diffBundle gathers the data the text renderer consumes. Mirrors
// previewBundle's shape so the formatters read alike.
type diffBundle struct {
	srcEngine  string
	tgtEngine  string
	diff       ir.SchemaDiff
	missingDDL map[string][]ir.DDLStatement // table name -> CREATE TABLE / CREATE INDEX statements

	// missingColDDL maps "<table>.<column>" -> the target engine's
	// rendered column-def fragment (e.g. `"created_at" TIMESTAMP(6)
	// NOT NULL`) for use in the ALTER TABLE ADD COLUMN suggestion.
	// nil when no missing-on-target column was rendered (engine
	// didn't expose ir.ColumnDDLPreviewer or the emit failed for a
	// specific column — in that case the renderer falls back to the
	// `-- TYPE` placeholder).
	missingColDDL map[string]string

	expected *ir.Schema
	actual   *ir.Schema
	opts     diffRenderOpts
}

type diffRenderOpts struct {
	IgnoreCharsetCollation bool
	IgnoreExtras           bool
}

// previewMissingDDL opens the target engine's schema writer once and
// asks it for two flavours of "render the DDL you would emit"
// material: full CREATE TABLE statements for tables missing from the
// target, and per-column-def fragments for individual columns missing
// from a present-on-both-sides table. Returning both from a single
// helper keeps the connection lifecycle in one place — the writer
// (and its connection pool) is opened once and closed before this
// function returns regardless of which preview surface the engine
// implements.
//
// The returned maps may be nil when there's nothing to preview or the
// engine doesn't expose the relevant optional surface
// ([ir.DDLPreviewer] / [ir.ColumnDDLPreviewer]); the renderer falls
// back to placeholder output in those cases. Errors from the
// underlying preview calls are returned verbatim.
func previewMissingDDL(ctx context.Context, target ir.Engine, dsn, targetSchema string, enabledExtensions []string, expected *ir.Schema, diff ir.SchemaDiff) (tableDDL map[string][]ir.DDLStatement, columnDDL map[string]string, err error) {
	missingTables := diff.TablesMissing
	missingCols := collectMissingColumns(diff)
	if len(missingTables) == 0 && len(missingCols) == 0 {
		return nil, nil, nil
	}

	sw, openErr := target.OpenSchemaWriter(ctx, dsn)
	if openErr != nil {
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: open target schema writer: %w", openErr))
	}
	applyTargetSchema(sw, targetSchema)
	if err := applyEnabledPGExtensions(ctx, sw, enabledExtensions); err != nil {
		closeIf(sw)
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: enable PG extensions on target: %w", err))
	}
	defer closeIf(sw)

	tableDDL, err = previewDDLForTables(ctx, sw, expected, missingTables)
	if err != nil {
		return nil, nil, err
	}

	columnDDL, err = previewDDLForColumns(ctx, sw, expected, missingCols)
	if err != nil {
		return nil, nil, err
	}
	return tableDDL, columnDDL, nil
}

// collectMissingColumns returns the per-table list of columns absent
// from the target. Map key is table name, value is the slice of
// missing column names (in the same alphabetic order DiffSchemas
// returned them).
func collectMissingColumns(diff ir.SchemaDiff) map[string][]string {
	out := make(map[string][]string, len(diff.TablesMismatched))
	for _, td := range diff.TablesMismatched {
		if len(td.ColumnsMissing) == 0 {
			continue
		}
		out[td.Name] = td.ColumnsMissing
	}
	return out
}

// previewDDLForTables asks the target engine for the DDL it would
// emit for the listed tables. Used to render CREATE TABLE suggestions
// for "missing on target" entries. Returns an empty map (nil) when
// missing is empty or the target doesn't expose DDLPreviewer — the
// renderer falls back to a plain "-- CREATE TABLE x (missing)" hint.
func previewDDLForTables(ctx context.Context, sw ir.SchemaWriter, expected *ir.Schema, missing []string) (map[string][]ir.DDLStatement, error) {
	if len(missing) == 0 {
		return nil, nil
	}
	missingSet := make(map[string]struct{}, len(missing))
	for _, n := range missing {
		missingSet[n] = struct{}{}
	}
	subset := &ir.Schema{Tables: make([]*ir.Table, 0, len(missing))}
	for _, t := range expected.Tables {
		if _, ok := missingSet[t.Name]; ok {
			subset.Tables = append(subset.Tables, t)
		}
	}
	if len(subset.Tables) == 0 {
		return nil, nil
	}
	previewer, ok := sw.(ir.DDLPreviewer)
	if !ok {
		return nil, nil
	}
	stmts, err := previewer.PreviewDDL(ctx, subset)
	if err != nil {
		return nil, fmt.Errorf("diff: emit DDL for missing tables: %w", err)
	}
	out := make(map[string][]ir.DDLStatement, len(missing))
	for _, s := range stmts {
		if s.Table == "" {
			continue
		}
		out[s.Table] = append(out[s.Table], s)
	}
	return out, nil
}

// previewDDLForColumns asks the target engine for the column-def
// fragment of every (table, column) pair missing on the target.
// Returns nil when there's nothing to render or the engine doesn't
// expose ir.ColumnDDLPreviewer — the diff renderer falls back to the
// `-- TYPE` placeholder in either case.
//
// Per-column emit failures (e.g. PG GEOMETRY without PostGIS) are
// silently skipped — the renderer falls through to the placeholder
// for that column and the operator sees the same diagnostic loop the
// migration would surface. Aborting the whole diff over one column
// would be worse UX than partial rendering with a placeholder for the
// problem cases.
func previewDDLForColumns(ctx context.Context, sw ir.SchemaWriter, expected *ir.Schema, missing map[string][]string) (map[string]string, error) {
	if len(missing) == 0 {
		return nil, nil
	}
	previewer, ok := sw.(ir.ColumnDDLPreviewer)
	if !ok {
		return nil, nil
	}
	tablesByName := make(map[string]*ir.Table, len(expected.Tables))
	for _, t := range expected.Tables {
		tablesByName[t.Name] = t
	}
	out := make(map[string]string, totalColumns(missing))
	for tableName, cols := range missing {
		table, ok := tablesByName[tableName]
		if !ok {
			continue
		}
		colsByName := make(map[string]*ir.Column, len(table.Columns))
		for _, c := range table.Columns {
			colsByName[c.Name] = c
		}
		for _, colName := range cols {
			col, ok := colsByName[colName]
			if !ok {
				continue
			}
			frag, err := previewer.EmitColumnDef(ctx, table, col)
			if err != nil {
				// Skip; renderer falls back to placeholder. Column
				// emit errors are recoverable at the diff layer —
				// the renderer's job is to surface drift, not to
				// produce a fully-validated migration script.
				continue
			}
			out[tableName+"."+colName] = frag
		}
	}
	return out, nil
}

func totalColumns(m map[string][]string) int {
	n := 0
	for _, cols := range m {
		n += len(cols)
	}
	return n
}

// renderDiffText writes the human-readable diff to w. Format follows
// ADR-0029 §"Output format" — header summary, per-table sections with
// DDL suggestions for closing the diff.
func renderDiffText(w io.Writer, b diffBundle) error {
	var sb strings.Builder

	sb.WriteString("-- sluice schema diff\n")
	fmt.Fprintf(&sb, "-- source: %s (%d tables expected after translation)\n", b.srcEngine, countTables(b.expected))
	fmt.Fprintf(&sb, "-- target: %s (%d tables found)\n", b.tgtEngine, countTables(b.actual))
	fmt.Fprintf(&sb, "-- result: %s\n", b.diff.Summary())
	if b.opts.IgnoreCharsetCollation {
		sb.WriteString("-- (charset/collation comparisons suppressed via --ignore-charset-collation)\n")
	}
	if b.opts.IgnoreExtras {
		sb.WriteString("-- (extra-on-target entries suppressed via --ignore-extras)\n")
	}
	sb.WriteString("--\n")
	sb.WriteString("-- The ALTER/DROP statements below are starting points for manual\n")
	sb.WriteString("-- reconciliation. sluice does not execute them. Review carefully\n")
	sb.WriteString("-- before running them against the target.\n")
	sb.WriteByte('\n')

	if !b.diff.HasChanges() {
		sb.WriteString("-- No drift detected. Target schema matches the expected shape.\n")
		_, err := io.WriteString(w, sb.String())
		return err
	}

	quote := identifierQuoter(b.tgtEngine)

	// Tables missing on target — render the engine's CREATE TABLE
	// (and CREATE INDEX, FK) when available, otherwise a placeholder.
	for _, name := range b.diff.TablesMissing {
		fmt.Fprintf(&sb, "-- ──────────── %s (missing on target) ────────────\n", name)
		stmts := b.missingDDL[name]
		if len(stmts) == 0 {
			fmt.Fprintf(&sb, "-- target engine does not expose CREATE-DDL preview; manually create %s\n", quote(name))
		}
		for _, s := range stmts {
			sb.WriteString(s.SQL)
			sb.WriteString(";\n")
		}
		sb.WriteByte('\n')
	}

	// Tables extra on target.
	for _, name := range b.diff.TablesExtra {
		fmt.Fprintf(&sb, "-- ──────────── %s (extra on target) ────────────\n", name)
		fmt.Fprintf(&sb, "DROP TABLE %s;\n", quote(name))
		fmt.Fprintf(&sb, "-- ^ not in source schema; sluice would not create it\n\n")
	}

	// Views missing on target.
	for _, name := range b.diff.ViewsMissing {
		fmt.Fprintf(&sb, "-- ──────────── view %s (missing on target) ────────────\n", name)
		// Look up the expected definition so the operator gets a
		// copy-paste-ready CREATE VIEW. Materialized views emit
		// `CREATE MATERIALIZED VIEW ... WITH DATA`; regular views
		// emit `CREATE VIEW`. Cross-engine view-body translation is
		// Phase 3 — for now the body is verbatim.
		expView := lookupView(b.expected, name)
		if expView != nil {
			kw := "CREATE VIEW"
			suffix := ""
			if expView.Materialized {
				kw = "CREATE MATERIALIZED VIEW"
				suffix = " WITH DATA"
			}
			fmt.Fprintf(&sb, "%s %s AS %s%s;\n", kw, quote(name), expView.Definition, suffix)
		} else {
			fmt.Fprintf(&sb, "-- expected view definition not available; manually create %s\n", quote(name))
		}
		sb.WriteByte('\n')
	}

	// Views extra on target.
	for _, name := range b.diff.ViewsExtra {
		fmt.Fprintf(&sb, "-- ──────────── view %s (extra on target) ────────────\n", name)
		// Pick the right DROP keyword based on what the actual side
		// reports. Falls back to DROP VIEW (most common case).
		actView := lookupView(b.actual, name)
		if actView != nil && actView.Materialized {
			fmt.Fprintf(&sb, "DROP MATERIALIZED VIEW %s;\n", quote(name))
		} else {
			fmt.Fprintf(&sb, "DROP VIEW %s;\n", quote(name))
		}
		fmt.Fprintf(&sb, "-- ^ not in source schema; sluice would not create it\n\n")
	}

	// Views mismatched (definition drift).
	for _, vd := range b.diff.ViewsMismatched {
		fmt.Fprintf(&sb, "-- ──────────── view %s (mismatched) ────────────\n", vd.Name)
		if vd.ExpectedMaterialized != nil && vd.ActualMaterialized != nil &&
			*vd.ExpectedMaterialized != *vd.ActualMaterialized {
			fmt.Fprintf(&sb, "-- materialized flag differs: target=%v expected=%v\n",
				*vd.ActualMaterialized, *vd.ExpectedMaterialized)
		}
		if vd.ExpectedDefinition != "" || vd.ActualDefinition != "" {
			fmt.Fprintf(&sb, "-- (cross-engine view-definition comparison is high-noise; verify before applying)\n")
			fmt.Fprintf(&sb, "-- target  : %s\n", oneLine(vd.ActualDefinition))
			fmt.Fprintf(&sb, "-- expected: %s\n", oneLine(vd.ExpectedDefinition))
		}
		expView := lookupView(b.expected, vd.Name)
		if expView != nil {
			if expView.Materialized {
				// PG won't accept CREATE OR REPLACE on a matview;
				// the operator has to drop and recreate.
				fmt.Fprintf(&sb, "DROP MATERIALIZED VIEW %s;\n", quote(vd.Name))
				fmt.Fprintf(&sb, "CREATE MATERIALIZED VIEW %s AS %s WITH DATA;\n", quote(vd.Name), expView.Definition)
			} else {
				fmt.Fprintf(&sb, "CREATE OR REPLACE VIEW %s AS %s;\n", quote(vd.Name), expView.Definition)
			}
		}
		sb.WriteByte('\n')
	}

	// Per-table mismatched sections.
	for _, td := range b.diff.TablesMismatched {
		fmt.Fprintf(&sb, "-- ──────────── %s (mismatched) ────────────\n", td.Name)
		for _, col := range td.ColumnsMissing {
			renderMissingColumn(&sb, td.Name, col, quote, b.missingColDDL)
		}
		for _, col := range td.ColumnsExtra {
			fmt.Fprintf(&sb, "ALTER TABLE %s DROP COLUMN %s;\n", quote(td.Name), quote(col))
			fmt.Fprintln(&sb, "-- ^ column not in source schema; sluice would not create it")
		}
		for _, cd := range td.ColumnsMismatched {
			renderColumnMismatch(&sb, td.Name, cd, quote, b.tgtEngine)
		}
		for _, idx := range td.IndexesMissing {
			fmt.Fprintf(&sb, "-- index %s missing on target; CREATE INDEX %s ON %s (...);\n",
				quote(idx), quote(idx), quote(td.Name))
		}
		for _, idx := range td.IndexesExtra {
			fmt.Fprintf(&sb, "DROP INDEX %s; -- not in source schema\n", quote(idx))
		}
		for _, name := range td.ChecksMissing {
			renderMissingCheck(&sb, td.Name, name, quote, b.expected)
		}
		for _, name := range td.ChecksExtra {
			fmt.Fprintf(&sb, "ALTER TABLE %s DROP CONSTRAINT %s; -- CHECK not in source schema\n",
				quote(td.Name), quote(name))
		}
		for _, ck := range td.ChecksMismatched {
			fmt.Fprintf(&sb, "-- CHECK %s mismatched: target has %q; expected %q\n",
				quote(ck.Name), ck.ActualExpr, ck.ExpectedExpr)
			fmt.Fprintf(&sb, "ALTER TABLE %s DROP CONSTRAINT %s;\n", quote(td.Name), quote(ck.Name))
			fmt.Fprintf(&sb, "ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);\n",
				quote(td.Name), quote(ck.Name), ck.ExpectedExpr)
		}
		sb.WriteByte('\n')
	}

	_, err := io.WriteString(w, sb.String())
	return err
}

// renderMissingColumn writes the ALTER TABLE ADD COLUMN suggestion
// for a column missing on target. When the target engine emitted a
// concrete column-def fragment via ir.ColumnDDLPreviewer the renderer
// inlines it (operators get a copy-paste-ready ALTER); otherwise we
// fall back to the v0.7.0 `-- TYPE` placeholder shape.
func renderMissingColumn(sb *strings.Builder, table, col string, quote func(string) string, ddl map[string]string) {
	if frag, ok := ddl[table+"."+col]; ok && frag != "" {
		fmt.Fprintf(sb, "ALTER TABLE %s ADD COLUMN %s; -- column missing on target\n",
			quote(table), frag)
		return
	}
	fmt.Fprintf(sb, "ALTER TABLE %s ADD COLUMN %s; -- TYPE; column missing on target\n",
		quote(table), quote(col))
}

// renderMissingCheck writes the ADD CONSTRAINT ... CHECK suggestion
// for a CHECK constraint missing on target, looking up the expected
// expression in the expected-side schema. Surfaces a placeholder
// when the constraint name resolves but the schema is malformed (no
// expression text) — the operator can still see the name and chase
// it down by hand.
func renderMissingCheck(sb *strings.Builder, table, name string, quote func(string) string, expected *ir.Schema) {
	expr := lookupCheckExpr(expected, table, name)
	if expr == "" {
		fmt.Fprintf(sb, "-- CHECK %s missing on target; expression unavailable for ADD CONSTRAINT suggestion\n",
			quote(name))
		return
	}
	fmt.Fprintf(sb, "ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s); -- CHECK missing on target\n",
		quote(table), quote(name), expr)
}

// lookupCheckExpr returns the expression text for the named CHECK
// constraint on the named table within s, or "" when the schema is
// nil / table absent / constraint absent. Used by the missing-CHECK
// renderer; schemas should be populated with the constraint's text
// from the source-side reader.
func lookupCheckExpr(s *ir.Schema, tableName, checkName string) string {
	if s == nil {
		return ""
	}
	for _, t := range s.Tables {
		if t.Name != tableName {
			continue
		}
		for _, c := range t.CheckConstraints {
			if c != nil && c.Name == checkName {
				return c.Expr
			}
		}
	}
	return ""
}

// renderColumnMismatch emits one ALTER suggestion per column-level
// drift. The exact MODIFY syntax varies between MySQL (MODIFY COLUMN)
// and PG (ALTER COLUMN ... TYPE / SET NOT NULL); we write the MySQL
// form when targeting MySQL, the PG form otherwise. Operators copy-
// paste these as a starting point — they're not guaranteed verified
// migration scripts.
func renderColumnMismatch(sb *strings.Builder, table string, cd ir.ColumnDiff, quote func(string) string, engine string) {
	switch engine {
	case "mysql", "planetscale":
		if cd.ExpectedType != "" {
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedType, cd.ActualType)
		}
		if cd.ExpectedNullable != nil {
			null := "NOT NULL"
			if *cd.ExpectedNullable {
				null = "NULL"
			}
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... %s; -- nullable on target: %v -> expected: %v\n",
				quote(table), quote(cd.Name), null, *cd.ActualNullable, *cd.ExpectedNullable)
		}
		if cd.ExpectedDefault != "" || cd.ActualDefault != "" {
			renderDefaultMismatchMySQL(sb, table, cd, quote)
		}
		if cd.ExpectedGeneratedExpr != cd.ActualGeneratedExpr {
			renderGeneratedExprMismatch(sb, table, cd, quote)
		}
		renderCharsetCollationMismatch(sb, table, cd, quote, "mysql")
	default:
		if cd.ExpectedType != "" {
			fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s TYPE %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedType, cd.ActualType)
		}
		if cd.ExpectedNullable != nil {
			action := "SET NOT NULL"
			if *cd.ExpectedNullable {
				action = "DROP NOT NULL"
			}
			fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s %s; -- nullable on target: %v -> expected: %v\n",
				quote(table), quote(cd.Name), action, *cd.ActualNullable, *cd.ExpectedNullable)
		}
		if cd.ExpectedDefault != "" || cd.ActualDefault != "" {
			renderDefaultMismatchPG(sb, table, cd, quote)
		}
		if cd.ExpectedGeneratedExpr != cd.ActualGeneratedExpr {
			renderGeneratedExprMismatch(sb, table, cd, quote)
		}
		renderCharsetCollationMismatch(sb, table, cd, quote, "postgres")
	}
}

// renderCharsetCollationMismatch emits ALTER suggestions for charset
// or collation drift. Empty fields are skipped, so a ColumnDiff that
// passed `--ignore-charset-collation` (which clears these via
// stripCharsetCollation at compare time) renders nothing here.
//
// MySQL syntax uses `MODIFY COLUMN ... CHARACTER SET ... COLLATE ...`;
// PG uses `ALTER COLUMN ... TYPE ... COLLATE "..."`. Suggestions are
// hint comments — the precise type still needs filling in by the
// operator.
func renderCharsetCollationMismatch(sb *strings.Builder, table string, cd ir.ColumnDiff, quote func(string) string, engine string) {
	if cd.ExpectedCharset == "" && cd.ActualCharset == "" &&
		cd.ExpectedCollation == "" && cd.ActualCollation == "" {
		return
	}
	switch engine {
	case "mysql", "planetscale":
		switch {
		case cd.ExpectedCharset != cd.ActualCharset && cd.ExpectedCollation != cd.ActualCollation:
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... CHARACTER SET %s COLLATE %s; -- on target: charset=%s collation=%s\n",
				quote(table), quote(cd.Name), cd.ExpectedCharset, cd.ExpectedCollation, cd.ActualCharset, cd.ActualCollation)
		case cd.ExpectedCharset != cd.ActualCharset:
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... CHARACTER SET %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedCharset, cd.ActualCharset)
		case cd.ExpectedCollation != cd.ActualCollation:
			fmt.Fprintf(sb, "ALTER TABLE %s MODIFY COLUMN %s ... COLLATE %s; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedCollation, cd.ActualCollation)
		}
	default:
		// PG has no per-column charset; only collation surfaces here.
		if cd.ExpectedCollation != cd.ActualCollation {
			fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE ... COLLATE %q; -- on target: %s\n",
				quote(table), quote(cd.Name), cd.ExpectedCollation, cd.ActualCollation)
		}
	}
}

// renderDefaultMismatchPG renders an ALTER TABLE ... ALTER COLUMN ...
// SET DEFAULT / DROP DEFAULT suggestion for a PG-style target. When
// the diff carries DefaultLowConfidence=true the suggestion is
// preceded by a `-- (default may differ across engines)` hint so the
// operator knows to verify the rendering against the actual source-
// side spelling before applying.
func renderDefaultMismatchPG(sb *strings.Builder, table string, cd ir.ColumnDiff, quote func(string) string) {
	if cd.DefaultLowConfidence {
		fmt.Fprintf(sb, "-- (default on %s may differ across engines; verify before applying)\n",
			quote(cd.Name))
	}
	switch {
	case cd.ExpectedDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT; -- on target: %s -> expected: <none>\n",
			quote(table), quote(cd.Name), cd.ActualDefault)
	case cd.ActualDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: <none>\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault))
	default:
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: %s\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault), cd.ActualDefault)
	}
}

// renderDefaultMismatchMySQL renders the MySQL-style ALTER for a
// default-clause drift. MySQL uses MODIFY COLUMN ... DEFAULT (or
// ALTER COLUMN ... SET/DROP DEFAULT in 8.0+); we use the latter form
// because it's narrower (doesn't require the operator to retype the
// column type) and works on both 5.7+ and 8.0+.
func renderDefaultMismatchMySQL(sb *strings.Builder, table string, cd ir.ColumnDiff, quote func(string) string) {
	if cd.DefaultLowConfidence {
		fmt.Fprintf(sb, "-- (default on %s may differ across engines; verify before applying)\n",
			quote(cd.Name))
	}
	switch {
	case cd.ExpectedDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT; -- on target: %s -> expected: <none>\n",
			quote(table), quote(cd.Name), cd.ActualDefault)
	case cd.ActualDefault == "<none>":
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: <none>\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault))
	default:
		fmt.Fprintf(sb, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s; -- on target: %s\n",
			quote(table), quote(cd.Name), unwrapDefaultLiteral(cd.ExpectedDefault), cd.ActualDefault)
	}
}

// unwrapDefaultLiteral converts the diff's rendered-default string
// back into a SQL fragment suitable for inlining after `SET DEFAULT
// `. The diff renders literal defaults as `'value'` (with the
// surrounding quotes) and expression defaults verbatim; the SQL
// emitter wants both forms passed through unchanged. Today the two
// shapes happen to be identical at the surface, so this function is
// a no-op — it exists as a single point to evolve later if the IR's
// default rendering grows new shapes (e.g. typed literals).
func unwrapDefaultLiteral(rendered string) string {
	return rendered
}

// lookupView returns the named view from s, or nil when s is nil or
// the view is absent. Used by the diff renderer to fetch the
// expected definition for missing-on-target / mismatched-view
// suggestions.
func lookupView(s *ir.Schema, name string) *ir.View {
	if s == nil {
		return nil
	}
	for _, v := range s.Views {
		if v != nil && v.Name == name {
			return v
		}
	}
	return nil
}

// oneLine collapses internal whitespace runs in s to single spaces
// and trims leading/trailing whitespace, so a multi-line view
// definition fits on a single comment line in the diff output.
func oneLine(s string) string {
	s = strings.TrimSpace(s)
	var sb strings.Builder
	sb.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !prevSpace {
				sb.WriteByte(' ')
			}
			prevSpace = true
			continue
		}
		prevSpace = false
		sb.WriteRune(r)
	}
	return sb.String()
}

// renderGeneratedExprMismatch emits a comment describing the
// generated-column expression drift. We don't try to ALTER the
// expression: PG/MySQL both require dropping and re-adding the
// column to change a STORED generated expression, which is
// destructive enough that the operator should run the migration
// hand-edited rather than copy-pasting from a diff suggestion.
func renderGeneratedExprMismatch(sb *strings.Builder, table string, cd ir.ColumnDiff, quote func(string) string) {
	fmt.Fprintf(sb, "-- generated expression drift on %s.%s: target=%q expected=%q\n",
		quote(table), quote(cd.Name), cd.ActualGeneratedExpr, cd.ExpectedGeneratedExpr)
	fmt.Fprintln(sb, "-- ^ engines require DROP + ADD COLUMN to change a generated expression; review carefully")
}

// identifierQuoter returns a function that quotes a SQL identifier in
// the target engine's idiom — backticks for MySQL/PlanetScale, double
// quotes for everything else (PostgreSQL today, ANSI SQL idiom for
// future engines). The renderer is the only thing that cares about
// engine-specific identifier syntax in the diff path.
func identifierQuoter(engine string) func(string) string {
	switch engine {
	case "mysql", "planetscale":
		return func(s string) string { return "`" + s + "`" }
	default:
		return func(s string) string { return `"` + s + `"` }
	}
}

func countTables(s *ir.Schema) int {
	if s == nil {
		return 0
	}
	return len(s.Tables)
}

// summarise rolls per-table counts up into the header summary line.
func summarise(d ir.SchemaDiff) DiffJSONCounts {
	c := DiffJSONCounts{
		TablesMissing:    len(d.TablesMissing),
		TablesExtra:      len(d.TablesExtra),
		TablesMismatched: len(d.TablesMismatched),
		ViewsMissing:     len(d.ViewsMissing),
		ViewsExtra:       len(d.ViewsExtra),
		ViewsMismatched:  len(d.ViewsMismatched),
	}
	for _, td := range d.TablesMismatched {
		c.ColumnsMissing += len(td.ColumnsMissing)
		c.ColumnsExtra += len(td.ColumnsExtra)
		c.ColumnsMismatched += len(td.ColumnsMismatched)
		c.IndexesMissing += len(td.IndexesMissing)
		c.IndexesExtra += len(td.IndexesExtra)
		c.ChecksMissing += len(td.ChecksMissing)
		c.ChecksExtra += len(td.ChecksExtra)
		c.ChecksMismatched += len(td.ChecksMismatched)
	}
	return c
}

// renderDiffJSON writes the structured diff to w. The shape mirrors
// ir.SchemaDiff with a summary block prepended and the engine names
// recorded alongside.
func renderDiffJSON(w io.Writer, srcEngine, tgtEngine string, diff ir.SchemaDiff) error {
	out := DiffJSON{
		SourceEngine: srcEngine,
		TargetEngine: tgtEngine,
		Summary:      summarise(diff),
		SchemaDiff:   diff,
	}
	// Stable nested ordering: the fields inside SchemaDiff are already
	// sorted by DiffSchemas; this defensive sort is a no-op today but
	// keeps the JSON renderer's output deterministic if a future caller
	// constructs SchemaDiff some other way.
	sort.Strings(out.TablesMissing)
	sort.Strings(out.TablesExtra)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
