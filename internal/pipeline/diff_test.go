// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

func diffSchemaPG() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
				{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: false},
				{Name: "created_at", Type: ir.Timestamp{Precision: 6}, Nullable: false},
			},
			Indexes: []*ir.Index{
				{Name: "users_email_idx", Columns: []ir.IndexColumn{{Column: "email"}}, Unique: true},
			},
		},
	}}
}

// diffSchemaPGDrift returns a copy of diffSchemaPG with deliberate
// drift baked in: a column missing, a column type narrowed, and an
// extra table.
func diffSchemaPGDrift() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
				{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false}, // narrowed
				// created_at missing
			},
			Indexes: []*ir.Index{
				{Name: "users_email_idx", Columns: []ir.IndexColumn{{Column: "email"}}, Unique: true},
				{Name: "legacy_idx", Columns: []ir.IndexColumn{{Column: "id"}}}, // extra
			},
		},
		{
			Name:    "deprecated_log",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		},
	}}
}

func TestDiffer_Run_NoDrift(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}
	tgt := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}

	var buf bytes.Buffer
	d := &Differ{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Out:       &buf,
	}
	diff, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if diff == nil {
		t.Fatal("expected non-nil diff")
	}
	if diff.HasChanges() {
		t.Errorf("expected no drift; got %+v", diff)
	}
	out := buf.String()
	if !strings.Contains(out, "No drift detected") {
		t.Errorf("expected no-drift preamble in output:\n%s", out)
	}
}

func TestDiffer_Run_DriftDetected_TextOutput(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}
	tgt := &previewStubEngine{name: "postgres", schema: diffSchemaPGDrift()}

	var buf bytes.Buffer
	d := &Differ{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "text",
		Out:       &buf,
	}
	diff, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if diff == nil || !diff.HasChanges() {
		t.Fatalf("expected drift; got %+v", diff)
	}
	out := buf.String()

	// Header.
	if !strings.Contains(out, "-- sluice schema diff") {
		t.Errorf("missing diff header:\n%s", out)
	}
	// Summary line should mention the missing column and type mismatch.
	if !strings.Contains(out, "missing column") {
		t.Errorf("expected 'missing column' in summary:\n%s", out)
	}
	if !strings.Contains(out, "extra table") {
		t.Errorf("expected 'extra table' in summary:\n%s", out)
	}
	// Per-table sections.
	if !strings.Contains(out, "users (mismatched)") {
		t.Errorf("expected users mismatched section:\n%s", out)
	}
	if !strings.Contains(out, "deprecated_log (extra on target)") {
		t.Errorf("expected deprecated_log extra section:\n%s", out)
	}
	// DROP TABLE suggestion uses PG-style double quotes.
	if !strings.Contains(out, `DROP TABLE "deprecated_log";`) {
		t.Errorf("expected DROP TABLE suggestion:\n%s", out)
	}
	// Pre-amble warning that suggestions are not verified.
	if !strings.Contains(out, "starting points for manual") {
		t.Errorf("expected pre-amble safety note:\n%s", out)
	}
}

func TestDiffer_Run_JSONOutput(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}
	tgt := &previewStubEngine{name: "postgres", schema: diffSchemaPGDrift()}

	var buf bytes.Buffer
	d := &Differ{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "json",
		Out:       &buf,
	}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got DiffJSON
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v\noutput:\n%s", err, buf.String())
	}
	if got.SourceEngine != "postgres" || got.TargetEngine != "postgres" {
		t.Errorf("engines = (%q, %q); want (postgres, postgres)", got.SourceEngine, got.TargetEngine)
	}
	if got.Summary.TablesExtra != 1 {
		t.Errorf("summary.tables_extra = %d; want 1", got.Summary.TablesExtra)
	}
	if got.Summary.ColumnsMismatched != 1 {
		t.Errorf("summary.columns_mismatched = %d; want 1", got.Summary.ColumnsMismatched)
	}
	if got.Summary.ColumnsMissing != 1 {
		t.Errorf("summary.columns_missing = %d; want 1", got.Summary.ColumnsMissing)
	}
	if len(got.TablesMismatched) != 1 || got.TablesMismatched[0].Name != "users" {
		t.Errorf("tables_mismatched = %+v; want [users]", got.TablesMismatched)
	}
}

func TestDiffer_Run_IgnoreExtras(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}
	tgt := &previewStubEngine{name: "postgres", schema: diffSchemaPGDrift()}

	var buf bytes.Buffer
	d := &Differ{
		Source:       src,
		Target:       tgt,
		SourceDSN:    "src",
		TargetDSN:    "tgt",
		IgnoreExtras: true,
		Format:       "json",
		Out:          &buf,
	}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got DiffJSON
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if got.Summary.TablesExtra != 0 {
		t.Errorf("expected no extras under IgnoreExtras; got %d", got.Summary.TablesExtra)
	}
	// Drift on `users` (missing column + type mismatch) should still surface.
	if got.Summary.ColumnsMissing != 1 {
		t.Errorf("expected 1 missing column; got %d", got.Summary.ColumnsMissing)
	}
	if got.Summary.ColumnsMismatched != 1 {
		t.Errorf("expected 1 mismatched column; got %d", got.Summary.ColumnsMismatched)
	}
}

func TestDiffer_Run_EmptySource(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: &ir.Schema{}}
	tgt := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}
	_, err := d.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for empty source schema")
	}
}

func TestDiffer_Run_UnknownFormat(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}
	tgt := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "yaml", Out: &buf}
	_, err := d.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
	if !strings.Contains(err.Error(), "unknown --format") {
		t.Errorf("error %v should mention unknown format", err)
	}
}

func TestDiffer_Validate(t *testing.T) {
	cases := []struct {
		name string
		d    *Differ
		want string
	}{
		{"nil source", &Differ{Target: &previewStubEngine{}, SourceDSN: "x", TargetDSN: "y", Out: &bytes.Buffer{}}, "Source"},
		{"nil target", &Differ{Source: &previewStubEngine{}, SourceDSN: "x", TargetDSN: "y", Out: &bytes.Buffer{}}, "Target"},
		{"empty source DSN", &Differ{Source: &previewStubEngine{}, Target: &previewStubEngine{}, TargetDSN: "y", Out: &bytes.Buffer{}}, "SourceDSN"},
		{"empty target DSN", &Differ{Source: &previewStubEngine{}, Target: &previewStubEngine{}, SourceDSN: "x", Out: &bytes.Buffer{}}, "TargetDSN"},
		{"nil out", &Differ{Source: &previewStubEngine{}, Target: &previewStubEngine{}, SourceDSN: "x", TargetDSN: "y"}, "Out"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.d.validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %q; want substring %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDiffer_Run_MissingTableUsesEnginePreviewDDL(t *testing.T) {
	// The expected schema has a `users` table; the actual (target)
	// schema is empty. The Differ should ask the target engine for
	// the CREATE TABLE DDL it would emit to bring the target into
	// shape and include it in the rendered diff output.
	src := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}
	tgt := &previewStubEngine{
		name:   "postgres",
		schema: &ir.Schema{},
		stmts: []ir.DDLStatement{
			{Table: "users", Kind: "CREATE TABLE", SQL: `CREATE TABLE "users" ("id" BIGINT NOT NULL)`},
		},
	}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "users (missing on target)") {
		t.Errorf("expected missing-on-target section:\n%s", out)
	}
	if !strings.Contains(out, `CREATE TABLE "users"`) {
		t.Errorf("expected target-engine CREATE TABLE DDL:\n%s", out)
	}
}

func TestDiffer_Run_TargetReadFailureSurfaces(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: diffSchemaPG()}
	tgt := &previewStubFailingTargetEngine{name: "postgres", err: errors.New("synthetic target read failure")}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}
	_, err := d.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when target schema read fails")
	}
	if !strings.Contains(err.Error(), "synthetic target read failure") {
		t.Errorf("err should wrap target failure: %v", err)
	}
}

func TestDiffer_Run_DefaultMismatch_RendersSetDefault(t *testing.T) {
	srcSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "status", Type: ir.Varchar{Length: 16}, Default: ir.DefaultLiteral{Value: "active"}},
		},
	}}}
	tgtSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "status", Type: ir.Varchar{Length: 16}, Default: ir.DefaultLiteral{Value: "pending"}},
		},
	}}}
	src := &previewStubEngine{name: "postgres", schema: srcSchema}
	tgt := &previewStubEngine{name: "postgres", schema: tgtSchema}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "text", Out: &buf}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `ALTER COLUMN "status" SET DEFAULT 'active'`) {
		t.Errorf("expected SET DEFAULT 'active' suggestion:\n%s", out)
	}
	if strings.Contains(out, "may differ across engines") {
		t.Errorf("literal-vs-literal mismatch should be high confidence; got:\n%s", out)
	}
}

func TestDiffer_Run_DefaultExpressionLowConfidenceComment(t *testing.T) {
	srcSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "ts", Type: ir.Timestamp{Precision: 6}, Default: ir.DefaultExpression{Expr: "now() AT TIME ZONE 'UTC'"}},
		},
	}}}
	tgtSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "ts", Type: ir.Timestamp{Precision: 6}, Default: ir.DefaultExpression{Expr: "statement_timestamp()"}},
		},
	}}}
	src := &previewStubEngine{name: "postgres", schema: srcSchema}
	tgt := &previewStubEngine{name: "postgres", schema: tgtSchema}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "text", Out: &buf}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "may differ across engines") {
		t.Errorf("expected low-confidence hint for expr-vs-expr default mismatch:\n%s", out)
	}
}

func TestDiffer_Run_NowVsCurrentTimestampSuppressed(t *testing.T) {
	srcSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "ts", Type: ir.Timestamp{Precision: 6}, Default: ir.DefaultExpression{Expr: "now()"}},
		},
	}}}
	tgtSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "ts", Type: ir.Timestamp{Precision: 6}, Default: ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(6)"}},
		},
	}}}
	src := &previewStubEngine{name: "postgres", schema: srcSchema}
	tgt := &previewStubEngine{name: "mysql", schema: tgtSchema}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "text", Out: &buf}
	diff, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if diff.HasChanges() {
		t.Errorf("expected no drift for now() vs CURRENT_TIMESTAMP(6); got %+v", diff)
	}
}

func TestDiffer_Run_GeneratedExprMismatchSurfaced(t *testing.T) {
	srcSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "price", Type: ir.Decimal{Precision: 10, Scale: 2}},
			{
				Name: "price_with_tax", Type: ir.Decimal{Precision: 10, Scale: 2},
				GeneratedExpr: "(price * 1.1)", GeneratedStored: true,
			},
		},
	}}}
	tgtSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "price", Type: ir.Decimal{Precision: 10, Scale: 2}},
			{
				Name: "price_with_tax", Type: ir.Decimal{Precision: 10, Scale: 2},
				GeneratedExpr: "(price * 1.2)", GeneratedStored: true,
			},
		},
	}}}
	src := &previewStubEngine{name: "postgres", schema: srcSchema}
	tgt := &previewStubEngine{name: "postgres", schema: tgtSchema}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "text", Out: &buf}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "generated expression drift") {
		t.Errorf("expected generated-expression drift comment:\n%s", out)
	}
	if !strings.Contains(out, "DROP + ADD COLUMN") {
		t.Errorf("expected DROP + ADD reconciliation hint:\n%s", out)
	}
}

func TestDiffer_Run_CheckConstraintMissingExtraMismatched(t *testing.T) {
	srcSchema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "orders",
		Columns: []*ir.Column{{Name: "qty", Type: ir.Integer{Width: 32}}},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "qty_nonneg", Expr: "qty >= 0"},
			{Name: "qty_range", Expr: "qty < 1000"},
		},
	}}}
	tgtSchema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "orders",
		Columns: []*ir.Column{{Name: "qty", Type: ir.Integer{Width: 32}}},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "qty_range", Expr: "qty < 500"},
			{Name: "legacy_check", Expr: "qty != 7"},
		},
	}}}
	src := &previewStubEngine{name: "postgres", schema: srcSchema}
	tgt := &previewStubEngine{name: "postgres", schema: tgtSchema}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "text", Out: &buf}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	// Missing CHECK rendered with ADD CONSTRAINT.
	if !strings.Contains(out, `ADD CONSTRAINT "qty_nonneg" CHECK (qty >= 0)`) {
		t.Errorf("expected ADD CONSTRAINT for missing CHECK:\n%s", out)
	}
	// Extra CHECK rendered with DROP CONSTRAINT.
	if !strings.Contains(out, `DROP CONSTRAINT "legacy_check"`) {
		t.Errorf("expected DROP CONSTRAINT for extra CHECK:\n%s", out)
	}
	// Mismatched CHECK rendered as drop+re-add.
	if !strings.Contains(out, `DROP CONSTRAINT "qty_range"`) {
		t.Errorf("expected DROP for mismatched CHECK:\n%s", out)
	}
	if !strings.Contains(out, `ADD CONSTRAINT "qty_range" CHECK (qty < 1000)`) {
		t.Errorf("expected ADD with expected expr for mismatched CHECK:\n%s", out)
	}
}

func TestDiffer_Run_CheckConstraintsInJSON(t *testing.T) {
	srcSchema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "orders",
		Columns: []*ir.Column{{Name: "qty", Type: ir.Integer{Width: 32}}},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "qty_nonneg", Expr: "qty >= 0"},
		},
	}}}
	tgtSchema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "orders",
		Columns: []*ir.Column{{Name: "qty", Type: ir.Integer{Width: 32}}},
	}}}
	src := &previewStubEngine{name: "postgres", schema: srcSchema}
	tgt := &previewStubEngine{name: "postgres", schema: tgtSchema}

	var buf bytes.Buffer
	d := &Differ{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "json", Out: &buf}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got DiffJSON
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if got.Summary.ChecksMissing != 1 {
		t.Errorf("summary.checks_missing = %d; want 1", got.Summary.ChecksMissing)
	}
	if len(got.TablesMismatched) != 1 || len(got.TablesMismatched[0].ChecksMissing) != 1 {
		t.Errorf("expected one missing CHECK in tables_mismatched; got %+v", got.TablesMismatched)
	}
}

// previewStubFailingTargetEngine fails ReadSchema on the target side.
// Used to verify the Differ surfaces target-side read errors with the
// same wrapping shape as the source-side failure path.
type previewStubFailingTargetEngine struct {
	name string
	err  error
}

func (e *previewStubFailingTargetEngine) Name() string                  { return e.name }
func (e *previewStubFailingTargetEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *previewStubFailingTargetEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return &failingReader{err: e.err}, nil
}

func (e *previewStubFailingTargetEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return &previewStubWriter{}, nil
}

func (e *previewStubFailingTargetEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	panic("not used")
}

func (e *previewStubFailingTargetEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	panic("not used")
}

func (e *previewStubFailingTargetEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	panic("not used")
}

func (e *previewStubFailingTargetEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	panic("not used")
}

func (e *previewStubFailingTargetEngine) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	panic("not used")
}

type failingReader struct{ err error }

func (r *failingReader) ReadSchema(_ context.Context) (*ir.Schema, error) { return nil, r.err }

// TestRenderColumnMismatch_DialectDispatch pins the M2.5 capability
// conversion of the diff renderer: the ALTER-suggestion shape and
// identifier quoting follow the target's declared
// [ir.Capabilities.DDLDialect], not its engine name. Both dialect
// families are exercised (the family matrix is two-wide here, so this
// IS the full pin): MySQL-style MODIFY COLUMN + backticks, ANSI/PG
// ALTER COLUMN ... TYPE + double quotes. Note the vitess flavor
// inherits DDLDialectMySQL via planetscale's capabilities — under the
// old name-switch it silently fell into the PG-style default.
func TestRenderColumnMismatch_DialectDispatch(t *testing.T) {
	cd := irdiff.ColumnDiff{Name: "title", ExpectedType: "VARCHAR(64)", ActualType: "TEXT"}

	var mysqlOut strings.Builder
	renderColumnMismatch(&mysqlOut, "books", cd, identifierQuoter(ir.DDLDialectMySQL), ir.DDLDialectMySQL)
	if want := "ALTER TABLE `books` MODIFY COLUMN `title` VARCHAR(64);"; !strings.Contains(mysqlOut.String(), want) {
		t.Errorf("MySQL dialect rendering missing %q; got %q", want, mysqlOut.String())
	}

	var ansiOut strings.Builder
	renderColumnMismatch(&ansiOut, "books", cd, identifierQuoter(ir.DDLDialectANSI), ir.DDLDialectANSI)
	if want := `ALTER TABLE "books" ALTER COLUMN "title" TYPE VARCHAR(64);`; !strings.Contains(ansiOut.String(), want) {
		t.Errorf("ANSI dialect rendering missing %q; got %q", want, ansiOut.String())
	}
}
