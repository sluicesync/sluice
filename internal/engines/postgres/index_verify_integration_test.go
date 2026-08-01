//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// verifySchema is the fixture both VerifyIndexes integration tests build
// on: one table with a non-unique and a UNIQUE secondary index, plus a
// constraint-backed unique index the index phase deliberately skips.
func verifySchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "widgets",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "sku", Type: ir.Varchar{Length: 64}},
				{Name: "name", Type: ir.Varchar{Length: 255}},
			},
			PrimaryKey: &ir.Index{Name: "widgets_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{
				{Name: "widgets_sku_unique", Unique: true, Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "sku"}}},
				{Name: "widgets_name_idx", Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "name"}}},
			},
		},
	}}
}

// TestVerifyIndexes_GreenAfterARealBuild is the non-vacuity half: the net
// must be SILENT over a schema whose index phase actually ran. A verifier
// that refuses a healthy migration is worse than none.
func TestVerifyIndexes_GreenAfterARealBuild(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	schema := verifySchema()
	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}

	verifier, ok := swHandle.(ir.IndexVerifier)
	if !ok {
		t.Fatal("the Postgres SchemaWriter must implement ir.IndexVerifier — " +
			"it is the engine whose index build no-ops SILENTLY (CREATE INDEX IF NOT EXISTS)")
	}
	if err := verifier.VerifyIndexes(ctx, schema); err != nil {
		t.Fatalf("VerifyIndexes over a completed index phase must pass, got: %v", err)
	}
}

// TestVerifyIndexes_RefusesAMissingIndex drops one built index behind the
// verifier's back — the observable end state of a build that silently
// no-op'd — and requires a coded refusal naming it.
func TestVerifyIndexes_RefusesAMissingIndex(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	schema := verifySchema()
	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}

	sw := swHandle.(*SchemaWriter)
	if _, err := sw.db.ExecContext(ctx, `DROP INDEX "widgets_name_idx"`); err != nil {
		t.Fatalf("drop index: %v", err)
	}

	err = sw.VerifyIndexes(ctx, schema)
	if err == nil {
		t.Fatal("VerifyIndexes must refuse when an expected index is absent, got nil")
	}
	if !strings.Contains(err.Error(), "widgets.widgets_name_idx") {
		t.Errorf("refusal must name the missing table.index; got %q", err.Error())
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeIndexMissing {
		t.Errorf("refusal must carry %s; got %+v (ok=%v)", sluicecode.CodeIndexMissing, ce, ok)
	}
}

// TestVerifyIndexes_RefusesAUniqueIndexDemotedToNonUnique reproduces the
// EXACT residue a colliding index build leaves behind, on a real server:
// a NON-unique index already occupies the name a UNIQUE build wants, so
// `CREATE UNIQUE INDEX IF NOT EXISTS` succeeds while creating nothing and
// the target keeps accepting duplicate rows the source rejects.
//
// The ground truth this test asserts is checked directly: after the index
// phase reports success, the target ACCEPTS a duplicate `sku` pair that
// the source's UNIQUE index forbids — and VerifyIndexes is what turns that
// into a loud, coded refusal instead of exit 0.
func TestVerifyIndexes_RefusesAUniqueIndexDemotedToNonUnique(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	schema := verifySchema()
	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	sw := swHandle.(*SchemaWriter)
	// Squat on the UNIQUE index's name with a NON-unique index, exactly as
	// an alphabetically-earlier colliding source index would have.
	if _, err := sw.db.ExecContext(ctx,
		`CREATE INDEX "widgets_sku_unique" ON "public"."widgets" ("sku")`); err != nil {
		t.Fatalf("pre-create the squatting non-unique index: %v", err)
	}

	// The index phase reports SUCCESS — this is the silent half.
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}
	// Ground truth: the uniqueness the source declared is simply gone.
	if _, err := sw.db.ExecContext(ctx,
		`INSERT INTO "public"."widgets" ("sku","name") VALUES ('dup','a'),('dup','b')`); err != nil {
		t.Fatalf("setup: the target should (wrongly) accept the duplicate sku here: %v", err)
	}

	err = sw.VerifyIndexes(ctx, schema)
	if err == nil {
		t.Fatal("VerifyIndexes must refuse: a UNIQUE index was requested and a NON-unique one of that " +
			"name exists, so the target admits duplicates the source rejects")
	}
	if !strings.Contains(err.Error(), "widgets.widgets_sku_unique") {
		t.Errorf("refusal must name the demoted table.index; got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "NOT UNIQUE") {
		t.Errorf("refusal must say WHICH failure this is (not-unique vs absent); got %q", err.Error())
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeIndexMissing {
		t.Errorf("refusal must carry %s; got %+v (ok=%v)", sluicecode.CodeIndexMissing, ce, ok)
	}
}

// TestCreateTables_RefusesCollidingIndexNames is the defect-1 end-to-end
// pin against a real server: the MySQL-shaped source below (index names
// are table-scoped there, and MySQL auto-names a single-column index after
// its column) carries two indexes that collapse to `t_a_idx`. Pre-fix, the
// build created `t_a_idx` non-unique from `a_idx`, then silently discarded
// the UNIQUE `t_a_idx` — leaving a target that accepts duplicate (a,b)
// pairs forever, exit 0. It must now refuse BEFORE any table is created.
func TestCreateTables_RefusesCollidingIndexNames(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Timestamp{}},
		},
		PrimaryKey: &ir.Index{Name: "t_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "a_idx", Columns: []ir.IndexColumn{{Column: "a"}}},
			{Name: "t_a_idx", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
		},
	}}}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)

	err = swHandle.CreateTablesWithoutConstraints(ctx, schema)
	if err == nil {
		t.Fatal("CreateTablesWithoutConstraints must refuse a source whose index names collapse to one " +
			"Postgres identifier — the second CREATE INDEX IF NOT EXISTS silently no-ops")
	}
	for _, want := range []string{"a_idx", "t_a_idx"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal must name source index %q; got %q", want, err.Error())
		}
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeSchemaIndexNameCollision {
		t.Errorf("refusal must carry %s; got %+v (ok=%v)", sluicecode.CodeSchemaIndexNameCollision, ce, ok)
	}

	// Nothing was created: the refusal fires before any DDL runs, so a
	// retry after renaming the source index starts from a clean target.
	sw := swHandle.(*SchemaWriter)
	var n int
	if err := sw.db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 't'`).Scan(&n); err != nil {
		t.Fatalf("count target tables: %v", err)
	}
	if n != 0 {
		t.Errorf("the collision refusal must fire before any table is created; found %d", n)
	}
}
