package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
)

// previewStubEngine is a minimal ir.Engine implementation for the
// preview test. ReadSchema returns the configured schema; the schema
// writer satisfies ir.DDLPreviewer with a fixed list of statements
// derived from the schema. Other methods panic — the preview path
// shouldn't reach them.
type previewStubEngine struct {
	name     string
	schema   *ir.Schema
	stmts    []ir.DDLStatement
	emitErr  error
	noWriter bool // when true, OpenSchemaWriter returns a writer without DDLPreviewer
}

func (e *previewStubEngine) Name() string                  { return e.name }
func (e *previewStubEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (e *previewStubEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return &previewStubReader{schema: e.schema}, nil
}

func (e *previewStubEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	if e.noWriter {
		return &previewStubBareWriter{}, nil
	}
	return &previewStubWriter{stmts: e.stmts, err: e.emitErr}, nil
}

func (e *previewStubEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	panic("preview should not open row reader")
}

func (e *previewStubEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	panic("preview should not open row writer")
}

func (e *previewStubEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	panic("preview should not open CDC reader")
}

func (e *previewStubEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	panic("preview should not open change applier")
}

func (e *previewStubEngine) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	panic("preview should not open snapshot stream")
}

type previewStubReader struct{ schema *ir.Schema }

func (r *previewStubReader) ReadSchema(_ context.Context) (*ir.Schema, error) {
	return r.schema, nil
}

type previewStubWriter struct {
	stmts []ir.DDLStatement
	err   error
}

func (w *previewStubWriter) CreateTablesWithoutConstraints(_ context.Context, _ *ir.Schema) error {
	panic("preview should not call CreateTables")
}

func (w *previewStubWriter) CreateIndexes(_ context.Context, _ *ir.Schema) error {
	panic("preview should not call CreateIndexes")
}

func (w *previewStubWriter) CreateConstraints(_ context.Context, _ *ir.Schema) error {
	panic("preview should not call CreateConstraints")
}

func (w *previewStubWriter) SyncIdentitySequences(_ context.Context, _ *ir.Schema) error {
	return nil
}

func (w *previewStubWriter) PreviewDDL(_ context.Context, _ *ir.Schema) ([]ir.DDLStatement, error) {
	if w.err != nil {
		return nil, w.err
	}
	return w.stmts, nil
}

// previewStubBareWriter satisfies ir.SchemaWriter but not
// ir.DDLPreviewer. Used to verify the orchestrator surfaces a clear
// error when the target engine doesn't expose the preview surface.
type previewStubBareWriter struct{}

func (w *previewStubBareWriter) CreateTablesWithoutConstraints(_ context.Context, _ *ir.Schema) error {
	return nil
}

func (w *previewStubBareWriter) CreateIndexes(_ context.Context, _ *ir.Schema) error { return nil }

func (w *previewStubBareWriter) CreateConstraints(_ context.Context, _ *ir.Schema) error {
	return nil
}

func (w *previewStubBareWriter) SyncIdentitySequences(_ context.Context, _ *ir.Schema) error {
	return nil
}

// previewFixtureSchema returns a small PG-shaped schema with columns
// chosen to exercise both notes and hints when migrated to MySQL.
func previewFixtureSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.UUID{}},
				{Name: "email", Type: ir.Varchar{Length: 255}},
				{Name: "bio", Type: ir.Text{Size: ir.TextLong}},
			},
		},
	}}
}

func previewMySQLStmts() []ir.DDLStatement {
	return []ir.DDLStatement{
		{
			Table: "users",
			Kind:  "CREATE TABLE",
			SQL:   "CREATE TABLE `users` (\n  `id` CHAR(36) NOT NULL,\n  `email` VARCHAR(255) NOT NULL,\n  `bio` LONGTEXT NOT NULL\n) ENGINE=InnoDB",
		},
	}
}

func TestPreviewer_Run_TextOutput(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewFixtureSchema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewFixtureSchema(), stmts: previewMySQLStmts()}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "text",
		Out:       &buf,
	}
	if err := prev.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	out := buf.String()

	// Header lines.
	if !strings.Contains(out, "-- sluice schema preview") {
		t.Errorf("missing preview header in:\n%s", out)
	}
	if !strings.Contains(out, "-- source: postgres") {
		t.Errorf("missing source line; got:\n%s", out)
	}
	if !strings.Contains(out, "-- target: mysql") {
		t.Errorf("missing target line; got:\n%s", out)
	}
	if !strings.Contains(out, "-- advisory hints: 2") {
		// uuid hint + longtext hint
		t.Errorf("expected advisory hints: 2 (uuid + longtext); got:\n%s", out)
	}

	// Per-table section.
	if !strings.Contains(out, "──────────── users ────────────") {
		t.Errorf("missing table separator; got:\n%s", out)
	}

	// Hints. Both binary_uuid and mediumtext should fire.
	if !strings.Contains(out, "--type-override users.id=binary_uuid") {
		t.Errorf("missing uuid override hint; got:\n%s", out)
	}
	if !strings.Contains(out, "--type-override users.bio=mediumtext") {
		t.Errorf("missing text override hint; got:\n%s", out)
	}

	// DDL passthrough.
	if !strings.Contains(out, "CREATE TABLE `users`") {
		t.Errorf("missing CREATE TABLE in output; got:\n%s", out)
	}

	// Inline column annotation. The uuid column should pick up a
	// "source: uuid -> target: char(36)" comment.
	if !strings.Contains(out, "source: uuid -> target: char(36)") {
		t.Errorf("missing inline note on id column; got:\n%s", out)
	}
}

func TestPreviewer_Run_JSONOutput(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewFixtureSchema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewFixtureSchema(), stmts: previewMySQLStmts()}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "json",
		Out:       &buf,
	}
	if err := prev.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var got PreviewJSON
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v\noutput:\n%s", err, buf.String())
	}

	if got.SourceEngine != "postgres" {
		t.Errorf("source = %q; want postgres", got.SourceEngine)
	}
	if got.TargetEngine != "mysql" {
		t.Errorf("target = %q; want mysql", got.TargetEngine)
	}
	if len(got.Tables) != 1 {
		t.Fatalf("tables = %d; want 1", len(got.Tables))
	}
	if got.Tables[0].Name != "users" {
		t.Errorf("table[0].name = %q; want users", got.Tables[0].Name)
	}
	if len(got.Tables[0].DDL) != 1 {
		t.Errorf("ddl statements = %d; want 1", len(got.Tables[0].DDL))
	}
	if !strings.HasSuffix(got.Tables[0].DDL[0], ";") {
		t.Errorf("DDL statement should end with semicolon; got %q", got.Tables[0].DDL[0])
	}
	if len(got.Tables[0].Hints) != 2 {
		t.Errorf("hints = %d; want 2 (uuid + longtext)", len(got.Tables[0].Hints))
	}
	// Find the UUID hint.
	var uuidHint *PreviewJSONHint
	for i := range got.Tables[0].Hints {
		if got.Tables[0].Hints[i].Column == "id" {
			uuidHint = &got.Tables[0].Hints[i]
			break
		}
	}
	if uuidHint == nil {
		t.Fatal("expected hint for id column")
	}
	if uuidHint.SuggestedOverride != "--type-override users.id=binary_uuid" {
		t.Errorf("override = %q; want binary_uuid", uuidHint.SuggestedOverride)
	}
}

func TestPreviewer_Run_AppliedOverrideSuppressesHint(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewFixtureSchema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewFixtureSchema(), stmts: previewMySQLStmts()}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "text",
		Out:       &buf,
		Mappings: []config.Mapping{
			// The ApplyMappings step requires a real fixture column;
			// override id to binary_uuid (the canonical resolution
			// for this hint). The orchestrator should suppress the
			// uuid hint after this override is applied.
			{Table: "users", Column: "id", TargetType: "binary_uuid"},
		},
	}
	if err := prev.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "--type-override users.id=binary_uuid") {
		t.Errorf("uuid hint should be suppressed once operator applied the override; got:\n%s", out)
	}
	// The bio hint should still fire.
	if !strings.Contains(out, "--type-override users.bio=mediumtext") {
		t.Errorf("bio hint should still fire; got:\n%s", out)
	}
	if !strings.Contains(out, "-- mappings applied: 1") {
		t.Errorf("expected mapping count of 1 in header; got:\n%s", out)
	}
}

func TestPreviewer_Run_NoPreviewerSurface(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewFixtureSchema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewFixtureSchema(), noWriter: true}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Out:       &buf,
	}
	err := prev.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for engine without DDLPreviewer; got nil")
	}
	if !strings.Contains(err.Error(), "DDLPreviewer") {
		t.Errorf("error %v should mention DDLPreviewer surface", err)
	}
}

func TestPreviewer_Run_PreviewDDLError(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewFixtureSchema()}
	tgt := &previewStubEngine{
		name:    "mysql",
		schema:  previewFixtureSchema(),
		emitErr: errors.New("synthetic emit failure"),
	}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Out:       &buf,
	}
	err := prev.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when PreviewDDL fails; got nil")
	}
	if !strings.Contains(err.Error(), "synthetic emit failure") {
		t.Errorf("error should wrap engine error: %v", err)
	}
}

func TestPreviewer_Run_SameEngine_NoNotesNoHints(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewFixtureSchema()}
	tgt := &previewStubEngine{
		name:   "postgres",
		schema: previewFixtureSchema(),
		stmts: []ir.DDLStatement{
			{Table: "users", Kind: "CREATE TABLE", SQL: `CREATE TABLE "users" ("id" UUID NOT NULL)`},
		},
	}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Out:       &buf,
	}
	if err := prev.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "-- hint:") {
		t.Errorf("same-engine preview should have no hints; got:\n%s", out)
	}
	if !strings.Contains(out, "-- advisory hints: 0") {
		t.Errorf("expected hints: 0; got:\n%s", out)
	}
}

func TestPreviewer_Run_EmptySchema(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: &ir.Schema{}}
	tgt := &previewStubEngine{name: "mysql", schema: &ir.Schema{}}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Out:       &buf,
	}
	err := prev.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for empty source schema")
	}
}

func TestPreviewer_Run_UnknownFormat(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewFixtureSchema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewFixtureSchema(), stmts: previewMySQLStmts()}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "yaml",
		Out:       &buf,
	}
	err := prev.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
	if !strings.Contains(err.Error(), "unknown --format") {
		t.Errorf("error should mention unknown format: %v", err)
	}
}

func TestPreviewer_Validate(t *testing.T) {
	cases := []struct {
		name string
		p    *Previewer
		want string
	}{
		{"nil source", &Previewer{Target: &previewStubEngine{}, SourceDSN: "x", TargetDSN: "y", Out: &bytes.Buffer{}}, "Source"},
		{"nil target", &Previewer{Source: &previewStubEngine{}, SourceDSN: "x", TargetDSN: "y", Out: &bytes.Buffer{}}, "Target"},
		{"empty source DSN", &Previewer{Source: &previewStubEngine{}, Target: &previewStubEngine{}, TargetDSN: "y", Out: &bytes.Buffer{}}, "SourceDSN"},
		{"empty target DSN", &Previewer{Source: &previewStubEngine{}, Target: &previewStubEngine{}, SourceDSN: "x", Out: &bytes.Buffer{}}, "TargetDSN"},
		{"nil out", &Previewer{Source: &previewStubEngine{}, Target: &previewStubEngine{}, SourceDSN: "x", TargetDSN: "y"}, "Out"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %q; want substring %q", err.Error(), tc.want)
			}
		})
	}
}
