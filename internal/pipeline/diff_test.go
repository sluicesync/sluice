package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
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
