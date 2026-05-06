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

	// Filter selects which source tables participate. Empty (zero
	// value) keeps every source table the reader returns.
	Filter TableFilter

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

	// ---- 1. Read source schema ----
	sr, err := d.Source.OpenSchemaReader(ctx, d.SourceDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: open source schema reader: %w", err))
	}
	defer closeIf(sr)

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

	expected, err := translate.ApplyMappings(srcSchema, d.Mappings)
	if err != nil {
		return nil, fmt.Errorf("diff: apply mappings: %w", err)
	}

	// ---- 2. Read target's actual schema via the same SchemaReader
	// surface (ADR-0029). The reader doesn't care whether a DSN points
	// at a "source" or a "target".
	tr, err := d.Target.OpenSchemaReader(ctx, d.TargetDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: open target schema reader: %w", err))
	}
	defer closeIf(tr)

	actual, err := tr.ReadSchema(ctx)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: read target schema: %w", err))
	}

	// ---- 3. Compute the diff. ----
	diff := ir.DiffSchemas(expected, actual, ir.DiffOptions{IgnoreExtras: d.IgnoreExtras})

	// ---- 4. Resolve missing-table DDL via the target engine's
	// PreviewDDL surface so the text renderer can include real CREATE
	// TABLE suggestions (MySQL/PG syntax) rather than a generic
	// placeholder. PreviewDDL is optional; engines without it fall
	// through to a simple comment.
	missingDDL, err := previewDDLForTables(ctx, d.Target, d.TargetDSN, expected, diff.TablesMissing)
	if err != nil {
		return nil, err
	}

	// ---- 5. Render. ----
	switch strings.ToLower(strings.TrimSpace(d.Format)) {
	case "", "text":
		if err := renderDiffText(d.Out, diffBundle{
			srcEngine:  d.Source.Name(),
			tgtEngine:  d.Target.Name(),
			diff:       diff,
			missingDDL: missingDDL,
			expected:   expected,
			actual:     actual,
			opts:       diffRenderOpts{IgnoreCharsetCollation: d.IgnoreCharsetCollation, IgnoreExtras: d.IgnoreExtras},
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
	expected   *ir.Schema
	actual     *ir.Schema
	opts       diffRenderOpts
}

type diffRenderOpts struct {
	IgnoreCharsetCollation bool
	IgnoreExtras           bool
}

// previewDDLForTables asks the target engine for the DDL it would
// emit for the listed tables. Used to render CREATE TABLE suggestions
// for "missing on target" entries. Returns an empty map (nil) when
// missing is empty or the target doesn't expose DDLPreviewer — the
// renderer falls back to a plain "-- CREATE TABLE x (missing)" hint.
func previewDDLForTables(ctx context.Context, target ir.Engine, dsn string, expected *ir.Schema, missing []string) (map[string][]ir.DDLStatement, error) {
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

	sw, err := target.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("diff: open target schema writer: %w", err))
	}
	defer closeIf(sw)
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

	// Per-table mismatched sections.
	for _, td := range b.diff.TablesMismatched {
		fmt.Fprintf(&sb, "-- ──────────── %s (mismatched) ────────────\n", td.Name)
		for _, col := range td.ColumnsMissing {
			fmt.Fprintf(&sb, "ALTER TABLE %s ADD COLUMN %s; -- TYPE; column missing on target\n",
				quote(td.Name), quote(col))
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
		sb.WriteByte('\n')
	}

	_, err := io.WriteString(w, sb.String())
	return err
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
	}
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
	}
	for _, td := range d.TablesMismatched {
		c.ColumnsMissing += len(td.ColumnsMissing)
		c.ColumnsExtra += len(td.ColumnsExtra)
		c.ColumnsMismatched += len(td.ColumnsMismatched)
		c.IndexesMissing += len(td.IndexesMissing)
		c.IndexesExtra += len(td.IndexesExtra)
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
