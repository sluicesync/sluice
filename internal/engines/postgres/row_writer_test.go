//go:build integration

// Integration tests for the Postgres RowWriter. Exercises the full
// SchemaWriter → RowWriter → RowReader round-trip end-to-end against a
// real Postgres container.

package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestRowWriter_RoundTrip(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Build the schema in memory and apply via SchemaWriter.
	createdAt := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "samples",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "active", Type: ir.Boolean{}},
			{Name: "name", Type: ir.Varchar{Length: 64}},
			{Name: "score", Type: ir.Decimal{Precision: 8, Scale: 2}},
			{Name: "tags", Type: ir.Array{Element: ir.Integer{Width: 32}}},
			{Name: "external_id", Type: ir.UUID{}, Nullable: true},
			{Name: "created_at", Type: ir.Timestamp{Precision: 0, WithTimeZone: true}},
		},
		PrimaryKey: &ir.Index{
			Name:    "samples_pkey",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}}}

	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(sw)
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}

	// Re-read the schema from the database so the table object we
	// pass to the RowWriter matches what the SchemaReader produces.
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	readBack, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(readBack, "samples")
	if table == nil {
		t.Fatalf("samples table not found; have %v", tableNames(readBack))
	}

	// Build the input rows and write them.
	wantRows := []ir.Row{
		{
			"id":          int64(1),
			"active":      true,
			"name":        "Alice",
			"score":       "19.95",
			"tags":        []any{int64(10), int64(20), int64(30)},
			"external_id": "00112233-4455-6677-8899-aabbccddeeff",
			"created_at":  createdAt,
		},
		{
			"id":          int64(2),
			"active":      false,
			"name":        "Bob",
			"score":       "0.00",
			"tags":        []any{},
			"external_id": nil,
			"created_at":  createdAt,
		},
	}

	rw, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rw)

	in := make(chan ir.Row, len(wantRows))
	for _, r := range wantRows {
		in <- r
	}
	close(in)

	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	// Read back and assert each row matches.
	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)

	out, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var got []ir.Row
	for row := range out {
		got = append(got, row)
	}

	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows; want %d", len(got), len(wantRows))
	}

	// Rows are returned in PK order.
	for i, w := range wantRows {
		g := got[i]
		for col, wantVal := range w {
			gotVal := g[col]
			if !rowValueEqual(gotVal, wantVal) {
				t.Errorf("row[%d].%s = %#v (%T); want %#v (%T)",
					i, col, gotVal, gotVal, wantVal, wantVal)
			}
		}
	}
}

// rowValueEqual compares two IR Row values, with a small accommodation
// for time.Time (compared by .Equal which is location-agnostic).
func rowValueEqual(got, want any) bool {
	if gt, ok := got.(time.Time); ok {
		if wt, ok := want.(time.Time); ok {
			return gt.Equal(wt)
		}
		return false
	}
	return reflect.DeepEqual(got, want)
}
