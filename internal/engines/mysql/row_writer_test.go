//go:build integration

// Integration tests for the MySQL RowWriter. Exercises the full
// SchemaWriter → RowWriter → RowReader round-trip end-to-end against a
// real MySQL container.

package mysql

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestRowWriter_RoundTrip writes a small batch of rows representative
// of the canonical IR-typed value contract, then reads them back via
// the RowReader and asserts equality field-by-field. This is the most
// load-bearing integration test in the engine — failures here usually
// indicate a value-encoding bug somewhere in the data path.
func TestRowWriter_RoundTrip(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const ddl = `
		CREATE TABLE samples (
			id          BIGINT UNSIGNED NOT NULL,
			active      TINYINT(1)      NOT NULL,
			name        VARCHAR(64)     NOT NULL,
			price       DECIMAL(10,2)   NOT NULL,
			role        ENUM('admin','user','guest') NOT NULL,
			tags        SET('go','sql','mysql','postgres') NOT NULL,
			payload     JSON            NULL,
			data        BLOB            NULL,
			created_at  TIMESTAMP(0)    NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	// Read the schema so we have an IR Table to drive the writer.
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "samples")
	if table == nil {
		t.Fatalf("samples table not found; have %v", tableNames(schema))
	}

	// Build the input rows. Values follow the canonical IR-typed
	// value contract (see docs/value-types.md).
	createdAt := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)
	wantRows := []ir.Row{
		{
			"id":         uint64(1),
			"active":     true,
			"name":       "Alice",
			"price":      "19.95",
			"role":       "admin",
			"tags":       []string{"go", "sql"},
			"payload":    []byte(`{"plan": "free"}`),
			"data":       []byte{0xde, 0xad, 0xbe, 0xef},
			"created_at": createdAt,
		},
		{
			"id":         uint64(2),
			"active":     false,
			"name":       "Bob",
			"price":      "0.00",
			"role":       "user",
			"tags":       []string{},
			"payload":    nil,
			"data":       nil,
			"created_at": createdAt,
		},
	}

	// Open the writer and stream the rows in.
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

	// Read back and assert each row matches what we wrote.
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
	if rrConcrete, ok := rr.(*RowReader); ok {
		if err := rrConcrete.Err(); err != nil {
			t.Fatalf("Err after streaming: %v", err)
		}
	}

	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows; want %d", len(got), len(wantRows))
	}

	// Rows are returned in PK order (we set id=1 then id=2).
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
// for time.Time (whose location may differ even when the instant is
// equal — we compare by .Equal which is location-agnostic).
func rowValueEqual(got, want any) bool {
	if gt, ok := got.(time.Time); ok {
		if wt, ok := want.(time.Time); ok {
			return gt.Equal(wt)
		}
		return false
	}
	return reflect.DeepEqual(got, want)
}

// TestRowWriter_LargeBatch exercises the multi-batch path: insert
// more rows than fit in a single INSERT batch and assert all land
// correctly. Catches off-by-one and final-flush bugs in the
// streaming loop.
func TestRowWriter_LargeBatch(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const ddl = `
		CREATE TABLE counts (
			n  INT NOT NULL,
			s  VARCHAR(32) NOT NULL,
			PRIMARY KEY (n)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "counts")

	// Use a small batch limit so a few hundred rows produce multiple
	// flushes, exercising the loop path that matters.
	rwGeneric, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	rw := rwGeneric.(*RowWriter)
	rw.maxRowsPerBatch = 100

	const total = 1234 // not a multiple of the batch size, on purpose

	in := make(chan ir.Row, 64)
	go func() {
		defer close(in)
		for i := 0; i < total; i++ {
			in <- ir.Row{"n": int64(i), "s": "row"}
		}
	}()

	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	// Verify count via the reader.
	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	out, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	count := 0
	for range out {
		count++
	}
	if count != total {
		t.Errorf("read back %d rows; want %d", count, total)
	}
}
