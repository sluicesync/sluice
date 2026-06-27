// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/ir"
)

// writeRows is a tiny helper: feed the rows over a channel into a RowWriter.
func writeRows(t *testing.T, ctx context.Context, eng Engine, dsn string, table *ir.Table, rows []ir.Row) error {
	t.Helper()
	rw, err := eng.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() { _ = rw.(*RowWriter).Close() }()
	ch := make(chan ir.Row, len(rows))
	for _, r := range rows {
		ch <- r
	}
	close(ch)
	return rw.WriteRows(ctx, table, ch)
}

// readBack reads every row of table back through the SQLite reader.
func readBack(t *testing.T, ctx context.Context, eng Engine, dsn string, table *ir.Table) []ir.Row {
	t.Helper()
	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(*RowReader).Close() }()
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("reader Err: %v", err)
	}
	return out
}

// TestWriterInProcessRoundTrip is the headline within-engine pin: a schema
// covering every IR value family is created + loaded via the SQLite WRITER,
// then read back through the SQLite READER, and every value must match. This
// proves the writer is the faithful inverse of the reader (the same
// guarantee the cross-engine SQLite→X→SQLite integration test proves
// end-to-end). Pure-Go driver — no Docker.
func TestWriterInProcessRoundTrip(t *testing.T) {
	ctx := context.Background()
	eng := Engine{}
	dsn := filepath.Join(t.TempDir(), "out.db")

	tbl := &ir.Table{
		Name: "v",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "big", Type: ir.Integer{Width: 64}}, // > 2^53 — must stay exact
			{Name: "f", Type: ir.Float{Precision: ir.FloatDouble}},
			{Name: "txt", Type: ir.Text{Size: ir.TextLong}, Nullable: true},
			{Name: "blb", Type: ir.Blob{Size: ir.BlobLong}, Nullable: true},
			{Name: "flag", Type: ir.Boolean{}},
			{Name: "amt", Type: ir.Decimal{Unconstrained: true}, Nullable: true},
			{Name: "d", Type: ir.Date{}, Nullable: true},
			{Name: "ts", Type: ir.Timestamp{}, Nullable: true},
			{Name: "tm", Type: ir.Time{}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}

	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{tbl}}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTables: %v", err)
	}

	d := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	in := []ir.Row{
		{
			"id": int64(1), "big": int64(9007199254740993), "f": 3.5,
			"txt": "héllo", "blb": []byte{0xca, 0xfe}, "flag": true,
			"amt": "123.45", "d": d, "ts": ts, "tm": "03:04:05",
		},
		{
			// All-NULL nullable cols survive as NULL.
			"id": int64(2), "big": int64(-1), "f": 1.25, "flag": false,
			"txt": nil, "blb": nil, "amt": nil, "d": nil, "ts": nil, "tm": nil,
		},
	}
	if err := writeRows(t, ctx, eng, dsn, tbl, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
		t.Fatalf("SyncIdentitySequences: %v", err)
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateConstraints: %v", err)
	}
	_ = sw.(*SchemaWriter).Close()

	// Read back through the reader the way the engine actually does — via the
	// resolved schema (the decimal column was emitted TEXT-affinity per Bug
	// 162, so it resolves to ir.Text and its value comes back as the exact
	// decimal string `123.45`). Every value family must round-trip.
	srdr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	rschema, err := srdr.ReadSchema(ctx)
	_ = srdr.(*SchemaReader).Close()
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	got := readBack(t, ctx, eng, dsn, tableByName(rschema, "v"))
	if len(got) != 2 {
		t.Fatalf("rows = %d; want 2", len(got))
	}
	r0 := got[0]
	if r0["big"].(int64) != 9007199254740993 {
		t.Errorf("big = %#v; want 9007199254740993 (exact > 2^53)", r0["big"])
	}
	if r0["f"].(float64) != 3.5 {
		t.Errorf("f = %#v; want 3.5", r0["f"])
	}
	if r0["txt"].(string) != "héllo" {
		t.Errorf("txt = %#v; want héllo", r0["txt"])
	}
	if b, ok := r0["blb"].([]byte); !ok || string(b) != "\xca\xfe" {
		t.Errorf("blb = %#v; want bytes ca fe", r0["blb"])
	}
	if r0["flag"].(bool) != true {
		t.Errorf("flag = %#v; want true", r0["flag"])
	}
	if r0["amt"].(string) != "123.45" {
		t.Errorf("amt = %#v; want 123.45", r0["amt"])
	}
	if r0["d"].(time.Time).Format("2006-01-02") != "2024-01-02" {
		t.Errorf("d = %#v; want 2024-01-02", r0["d"])
	}
	if r0["ts"].(time.Time).UTC().Format("2006-01-02 15:04:05") != "2024-01-02 03:04:05" {
		t.Errorf("ts = %#v; want 2024-01-02 03:04:05", r0["ts"])
	}
	if !strings.Contains(r0["tm"].(string), "03:04:05") {
		t.Errorf("tm = %#v; want 03:04:05", r0["tm"])
	}
	// Row 2: NULLs survived.
	for _, c := range []string{"txt", "blb", "amt", "d", "ts", "tm"} {
		if got[1][c] != nil {
			t.Errorf("row2[%s] = %#v; want nil", c, got[1][c])
		}
	}

	// The produced file is a valid SQLite database (open + query it).
	assertValidSQLite(t, dsn, "v", 2)
}

// TestWriterFKOffBulkLoadThenCheck pins the ADR-0134 §3 FK story: a child
// row loads BEFORE its parent (FK enforcement off during copy), the file is
// created with inline FKs, and CreateConstraints' foreign_key_check passes
// once both rows are present.
func TestWriterFKOffBulkLoadThenCheck(t *testing.T) {
	ctx := context.Background()
	eng := Engine{}
	dsn := filepath.Join(t.TempDir(), "fk.db")

	users := &ir.Table{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	posts := &ir.Table{
		Name: "posts",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "user_id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
		ForeignKeys: []*ir.ForeignKey{{
			Name: "fk_u", Columns: []string{"user_id"},
			ReferencedTable: "users", ReferencedColumns: []string{"id"},
		}},
	}
	schema := &ir.Schema{Tables: []*ir.Table{users, posts}}

	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTables: %v", err)
	}

	// Load the CHILD first — would fail if FK enforcement were on.
	if err := writeRows(t, ctx, eng, dsn, posts, []ir.Row{{"id": int64(1), "user_id": int64(10)}}); err != nil {
		t.Fatalf("WriteRows posts (child-first): %v", err)
	}
	if err := writeRows(t, ctx, eng, dsn, users, []ir.Row{{"id": int64(10)}}); err != nil {
		t.Fatalf("WriteRows users: %v", err)
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateConstraints (clean FK) should pass: %v", err)
	}
	_ = sw.(*SchemaWriter).Close()
}

// TestWriterFKViolationIsLoud pins the loud-failure surface: a dangling FK
// reference left after the copy makes CreateConstraints' foreign_key_check
// FAIL, naming the violation — never a silent accept.
func TestWriterFKViolationIsLoud(t *testing.T) {
	ctx := context.Background()
	eng := Engine{}
	dsn := filepath.Join(t.TempDir(), "fkbad.db")

	users := &ir.Table{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	posts := &ir.Table{
		Name: "posts",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "user_id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
		ForeignKeys: []*ir.ForeignKey{{
			Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"},
		}},
	}
	schema := &ir.Schema{Tables: []*ir.Table{users, posts}}

	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTables: %v", err)
	}
	// Insert a post pointing at a non-existent user; never insert the user.
	if err := writeRows(t, ctx, eng, dsn, posts, []ir.Row{{"id": int64(1), "user_id": int64(999)}}); err != nil {
		t.Fatalf("WriteRows posts: %v", err)
	}
	err = sw.CreateConstraints(ctx, schema)
	if err == nil {
		t.Fatal("CreateConstraints succeeded with a dangling FK; want a LOUD foreign_key_check failure")
	}
	if !strings.Contains(err.Error(), "foreign-key violation") {
		t.Errorf("error = %v; want it to name the FK violation", err)
	}
	_ = sw.(*SchemaWriter).Close()
}

// TestWriterMaterializedViewRefused pins the loud refusal: SQLite has no
// materialized views, so CreateViews refuses one rather than silently
// degrading it to a plain view.
func TestWriterMaterializedViewRefused(t *testing.T) {
	ctx := context.Background()
	eng := Engine{}
	dsn := filepath.Join(t.TempDir(), "mv.db")
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()
	schema := &ir.Schema{Views: []*ir.View{{Name: "mv", Definition: "SELECT 1", Materialized: true}}}
	err = sw.CreateViews(ctx, schema)
	if err == nil || !strings.Contains(err.Error(), "materialized") {
		t.Fatalf("CreateViews(materialized) err = %v; want a loud refusal naming materialized", err)
	}
}

// decimalColTable is the helper table for the decimal write-path pins: a PK
// plus a single ir.Decimal column, emitted with TEXT affinity on the SQLite
// target (Bug 162) so the value is stored verbatim.
func decimalColTable() *ir.Table {
	return &ir.Table{
		Name:       "m",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "amt", Type: ir.Decimal{Unconstrained: true}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// TestWriterDecimalTextExact pins the Bug 162 fix through the real
// WRITER→DB→READER path: an ir.Decimal column is stored with TEXT affinity,
// so EVERY decimal round-trips BYTE-EXACT — ordinary money (the `19.99` →
// `19.989999999999998` corruption), preserved scale / trailing zeros, big
// and small magnitudes (no scientific-notation normalization), and values
// far beyond float64's precision (TEXT has no precision limit, so the
// previously-refused over-precision cases now carry exactly). This is the
// exact path the v0.99.146 value-fidelity probe MISSED: it must read back
// through the real reader and compare the TEXT byte-for-byte, never
// numeric-tolerantly (a numeric compare would have hidden the corruption).
func TestWriterDecimalTextExact(t *testing.T) {
	ctx := context.Background()
	eng := Engine{}
	dsn := filepath.Join(t.TempDir(), "decok.db")
	tbl := decimalColTable()

	sw, _ := eng.OpenSchemaWriter(ctx, dsn)
	if err := sw.CreateTablesWithoutConstraints(ctx, &ir.Schema{Tables: []*ir.Table{tbl}}); err != nil {
		t.Fatalf("CreateTables: %v", err)
	}
	_ = sw.(*SchemaWriter).Close()

	srcs := []string{
		"19.99", "5.10", "0.1", "0.30", "100.00", "-19.99", // money + preserved scale/trailing-zeros
		"1234567.89", "0.00001", // big/small magnitude — NO scientific-notation normalization
		"99999999999999.9",                // old 15-sig boundary
		"123456789012345.6",               // 16 sig — was REFUSED, now exact
		"12345678901234567890.1234567890", // 30 sig (DECIMAL(38,10)-class) — exact
		"0",
	}
	rows := make([]ir.Row, len(srcs))
	for i, s := range srcs {
		rows[i] = ir.Row{"id": int64(i + 1), "amt": s}
	}
	if err := writeRows(t, ctx, eng, dsn, tbl, rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	// Read back the way the engine actually does: resolve the schema first.
	// The decimal column was emitted with TEXT affinity, so resolveColumnType
	// maps it to ir.Text (the documented type downgrade) and the values come
	// back as their exact decimal strings — never a coerced binary float.
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	_ = sr.(*SchemaReader).Close()
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	resolved := tableByName(schema, "m")
	if resolved == nil {
		t.Fatal("table m missing from resolved schema")
	}
	for _, c := range resolved.Columns {
		if c.Name == "amt" {
			if _, ok := c.Type.(ir.Text); !ok {
				t.Errorf("decimal column read back as %T; want ir.Text (Bug 162 TEXT-affinity downgrade)", c.Type)
			}
		}
	}
	got := readBack(t, ctx, eng, dsn, resolved)
	if len(got) != len(srcs) {
		t.Fatalf("rows = %d; want %d", len(got), len(srcs))
	}
	byID := map[int64]string{}
	for _, r := range got {
		s, ok := r["amt"].(string)
		if !ok {
			t.Fatalf("id %v: amt read back as %T; want string (TEXT affinity)", r["id"], r["amt"])
		}
		byID[r["id"].(int64)] = s
	}
	for i, src := range srcs {
		if rb := byID[int64(i+1)]; rb != src {
			t.Errorf("decimal %q read back as %q; want BYTE-EXACT (Bug 162: TEXT affinity must not coerce to a binary float)", src, rb)
		}
	}
}

// assertValidSQLite opens the produced file directly with the driver and
// confirms it is a real SQLite database with the expected row count.
func assertValidSQLite(t *testing.T, path, table string, wantRows int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open produced db: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM "+quoteIdent(table)).Scan(&n); err != nil {
		t.Fatalf("count rows in produced db: %v", err)
	}
	if n != wantRows {
		t.Errorf("produced db row count = %d; want %d", n, wantRows)
	}
}
