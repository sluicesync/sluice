// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Audit 2026-09-01 LA-1 (HIGH, observed on real D1): a PK-less table whose user
// column is named `rowid` was keyset-paginated on THAT column — SQLite binds a
// declared `rowid`/`oid`/`_rowid_` column in preference to the implicit rowid —
// and every row sharing a page-boundary value with the previous page's last
// row was skipped: 2,500 source rows → 2,000 copied, `bulk copy complete`,
// exit 0, on both the direct reader and `--stage-local`.
//
// The pins below run the reader's REAL SQL against a real (modernc) SQLite
// through the exec-backed mock, so the shadowing rule under test is SQLite's
// own name resolution, not a canned answer. Family = which implicit-rowid
// name(s) the table shadows (each of the three, the case-folded spelling, a
// generated column, and all three at once); shape = the direct [D1RowReader]
// stream and the [stageD1Table] materializer, which share one paginator.

// rowidShadowCell is one family cell: the DDL for a PK-less table whose
// duplicate-valued TEXT column(s) shadow some rowid name(s), and the implicit-
// rowid name the planner must therefore key on ("" = every name is shadowed,
// so the table is refused).
type rowidShadowCell struct {
	name    string
	ddl     string
	wantKey string
}

// rowidShadowMatrix is the LA-1 family. The shadowing column is declared TEXT
// and duplicate-valued in every cell, so the buggy keyset — `<shadow> > ?` —
// is what a regression would run.
var rowidShadowMatrix = []rowidShadowCell{
	{"shadow_rowid", `CREATE TABLE t (rowid TEXT, v INTEGER)`, "_rowid_"},
	{"shadow_oid", `CREATE TABLE t (oid TEXT, v INTEGER)`, "rowid"},
	{"shadow__rowid_", `CREATE TABLE t (_rowid_ TEXT, v INTEGER)`, "rowid"},
	{"shadow_ROWID_uppercase", `CREATE TABLE t (ROWID TEXT, v INTEGER)`, "_rowid_"},
	{"shadow_rowid_and__rowid_", `CREATE TABLE t (rowid TEXT, _rowid_ TEXT, v INTEGER)`, "oid"},
	{"shadow_generated_rowid", `CREATE TABLE t (v INTEGER, k TEXT, rowid TEXT GENERATED ALWAYS AS (k) VIRTUAL)`, "_rowid_"},
	{"shadow_all_three", `CREATE TABLE t (rowid TEXT, oid TEXT, _rowid_ TEXT, v INTEGER)`, ""},
}

// rowidShadowSeed inserts seven rows whose shadowing column(s) carry duplicate
// values straddling every page-2 boundary (a,a,a,b,b,a,b at pageSize 2), so a
// keyset on that column skips rows on today's-bug code and a full read is only
// possible on the implicit rowid.
func rowidShadowSeed(t *testing.T, db *sql.DB, ddl string) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Every declared TEXT column gets the same duplicate-valued series; the
	// generated cell's `k` is its source column.
	textCols := declaredTextColumns(t, db)
	if len(textCols) == 0 {
		t.Fatal("test bug: the cell declares no TEXT column to shadow with")
	}
	for i, val := range []string{"a", "a", "a", "b", "b", "a", "b"} {
		set := make([]string, len(textCols))
		for j := range textCols {
			set[j] = "'" + val + "'"
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO t (v, `+strings.Join(textCols, ", ")+`) VALUES (`+
			strconv.Itoa(i+1)+`, `+strings.Join(set, ", ")+`)`); err != nil {
			t.Fatalf("seed row %d: %v", i+1, err)
		}
	}
}

// declaredTextColumns lists the table's declared (non-generated) TEXT columns,
// quoted for SQL.
func declaredTextColumns(t *testing.T, db *sql.DB) []string {
	t.Helper()
	cols, err := db.QueryContext(context.Background(), `SELECT name FROM pragma_table_info('t') WHERE type = 'TEXT'`)
	if err != nil {
		t.Fatalf("pragma_table_info: %v", err)
	}
	defer func() { _ = cols.Close() }()
	var textCols []string
	for cols.Next() {
		var name string
		if err := cols.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		textCols = append(textCols, quoteIdent(name))
	}
	if err := cols.Err(); err != nil {
		t.Fatalf("pragma_table_info rows: %v", err)
	}
	return textCols
}

// recordingExec wraps the exec-backed mock so a test can assert which SQL the
// planner actually ran (the key name it chose, and that a refused table never
// saw a data page).
func recordingExec(db *sql.DB, sqls *[]string, mu *sync.Mutex) d1Handler {
	inner := execD1Handler(db)
	return func(sqlStr string, params []string) (int, []byte) {
		mu.Lock()
		*sqls = append(*sqls, sqlStr)
		mu.Unlock()
		return inner(sqlStr, params)
	}
}

// d1TableFromMock reads the IR table through the real D1SchemaReader over the
// mock, so the planner sees exactly what a live `d1` source would hand it.
func d1TableFromMock(t *testing.T, client *d1Client, name string) *ir.Table {
	t.Helper()
	schema, err := (&D1SchemaReader{client: client}).ReadSchema(context.Background())
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	for _, tb := range schema.Tables {
		if tb.Name == name {
			return tb
		}
	}
	t.Fatalf("table %q not in schema", name)
	return nil
}

// TestD1RowReader_RowidShadowMatrix is the direct-reader shape: every family
// cell reads all seven rows in rowid order, keyed on the expected unshadowed
// name; the all-shadowed cell is refused with the coded error before any data
// page is requested.
func TestD1RowReader_RowidShadowMatrix(t *testing.T) {
	for _, cell := range rowidShadowMatrix {
		t.Run(cell.name, func(t *testing.T) {
			src, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "src.db"))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })
			rowidShadowSeed(t, src, cell.ddl)

			var (
				mu   sync.Mutex
				sqls []string
			)
			client := startMockD1(t, recordingExec(src, &sqls, &mu))
			table := d1TableFromMock(t, client, "t")
			r := &D1RowReader{client: client, pageSize: 2}

			ch, err := r.ReadRows(context.Background(), table)
			if cell.wantKey == "" {
				assertNoPaginationKeyRefusal(t, err, &sqls, &mu)
				return
			}
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			got := drain(ch)
			if err := r.Err(); err != nil {
				t.Fatalf("Err: %v", err)
			}
			if len(got) != 7 {
				t.Fatalf("delivered %d rows; want 7 — the keyset paginated on the shadowing column and dropped rows (LA-1)", len(got))
			}
			for i, row := range got {
				if row["v"] != int64(i+1) {
					t.Errorf("row %d: v = %#v; want %d (rowid order)", i, row["v"], i+1)
				}
			}
			assertKeyedOn(t, cell.wantKey, &sqls, &mu)
		})
	}
}

// TestStageD1Table_RowidShadowMatrix is the `--stage-local` shape over the same
// family: the staged file must be an exact replica (quote()-dump equality with
// the source, the TestStageD1_ByteFaithful discipline), and the all-shadowed
// cell is refused — never a silently short staged file.
func TestStageD1Table_RowidShadowMatrix(t *testing.T) {
	for _, cell := range rowidShadowMatrix {
		t.Run(cell.name, func(t *testing.T) {
			src, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "src.db"))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })
			rowidShadowSeed(t, src, cell.ddl)

			var (
				mu   sync.Mutex
				sqls []string
			)
			client := startMockD1(t, recordingExec(src, &sqls, &mu))
			table := d1TableFromMock(t, client, "t")
			dst := openStageDest(t, cell.ddl)

			rr := &D1RowReader{client: client, pageSize: 2}
			total, err := stageD1Table(context.Background(), rr, dst, table, slog.Default())
			if cell.wantKey == "" {
				assertNoPaginationKeyRefusal(t, err, &sqls, &mu)
				return
			}
			if err != nil {
				t.Fatalf("stageD1Table: %v", err)
			}
			if total != 7 {
				t.Fatalf("staged %d rows; want 7", total)
			}
			want := dumpTableQuoted(t, src, "t", "v")
			got := dumpTableQuoted(t, dst, "t", "v")
			if len(want) != 7 || strings.Join(want, "\n") != strings.Join(got, "\n") {
				t.Errorf("staged copy differs from source:\n src=%v\n dst=%v", want, got)
			}
			assertKeyedOn(t, cell.wantKey, &sqls, &mu)
		})
	}
}

// assertKeyedOn asserts the page queries ordered on the expected implicit-rowid
// name and never on a user column of the same spelling.
func assertKeyedOn(t *testing.T, key string, sqls *[]string, mu *sync.Mutex) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	want := `ORDER BY "t".` + quoteIdent(key)
	pages := 0
	for _, s := range *sqls {
		if strings.Contains(s, `ORDER BY "t".`) {
			pages++
			if !strings.Contains(s, want) {
				t.Errorf("page query keyed on the wrong name; want %s in:\n%s", want, s)
			}
		}
	}
	if pages < 2 {
		t.Errorf("saw %d page queries; want >= 2 so a page-boundary skip could have shown (test bug)", pages)
	}
}

// assertNoPaginationKeyRefusal asserts the coded refusal for a table with no
// reachable rowid, and that no data page was ever requested.
func assertNoPaginationKeyRefusal(t *testing.T, err error, sqls *[]string, mu *sync.Mutex) {
	t.Helper()
	if err == nil {
		t.Fatal("a table shadowing every implicit-rowid name must be refused, not paginated on a user column")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBulkCopyNoPaginationKey {
		t.Errorf("refusal must carry %s; got %v", sluicecode.CodeBulkCopyNoPaginationKey, err)
	}
	if !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Errorf("refusal must name the remedy; got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, s := range *sqls {
		if strings.Contains(s, `ORDER BY "t".`) {
			t.Errorf("a data page was requested despite the refusal:\n%s", s)
		}
	}
}

// ---- LA-3: the COUNT(*) bracket ---------------------------------------------

// shadowFreeTable is a PK-less table for the canned-page count pins.
func shadowFreeTable() *ir.Table {
	return &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}},
	}
}

// TestD1RowReader_CountBracketRefusesQuiescentMismatch pins the independent
// expected value: pages deliver 2 rows while the server counts 3 before and
// after the read → SLUICE-E-BULKCOPY-ROW-COUNT-MISMATCH naming both numbers,
// on the direct reader AND the staging materializer. This is the class gate
// for LA-1 — it fails a short read whatever the pagination defect's cause.
func TestD1RowReader_CountBracketRefusesQuiescentMismatch(t *testing.T) {
	table := shadowFreeTable()
	page := []map[string]any{
		withRowid(table, dataRow(table, map[string]cell{"v": tval("a")}), "1"),
		withRowid(table, dataRow(table, map[string]cell{"v": tval("b")}), "2"),
	}
	// pageSize 4: the single 2-row page is short, so it is the final page.
	newClient := func() *d1Client {
		return startMockD1(t, withCount(3, withRowidCatalog(table, fixedRows(page))))
	}

	t.Run("direct", func(t *testing.T) {
		r := &D1RowReader{client: newClient(), pageSize: 4}
		ch, err := r.ReadRows(context.Background(), table)
		if err != nil {
			t.Fatalf("ReadRows: %v", err)
		}
		_ = drain(ch)
		assertRowCountMismatch(t, r.Err())
	})
	t.Run("stage", func(t *testing.T) {
		dst := openStageDest(t, `CREATE TABLE t (v TEXT)`)
		rr := &D1RowReader{client: newClient(), pageSize: 4}
		_, err := stageD1Table(context.Background(), rr, dst, table, slog.Default())
		assertRowCountMismatch(t, err)
	})
}

func assertRowCountMismatch(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("2 rows delivered against a stable COUNT(*) of 3 must be refused — a nil error is the LA-1 exit-0 short copy")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBulkCopyRowCountMismatch {
		t.Errorf("refusal must carry %s; got %v", sluicecode.CodeBulkCopyRowCountMismatch, err)
	}
	for _, want := range []string{`"t"`, "delivered 2 rows", "COUNT(*) was 3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal must name %q; got %v", want, err)
		}
	}
}

// TestD1RowReader_CountBracketWarnsOnConcurrentWrites pins the other verdict:
// the two counts disagree with EACH OTHER (writes landed mid-read), so the
// delivered total is not comparable and the reader WARNs — naming all three
// numbers — instead of refusing.
func TestD1RowReader_CountBracketWarnsOnConcurrentWrites(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	table := shadowFreeTable()
	page := []map[string]any{
		withRowid(table, dataRow(table, map[string]cell{"v": tval("a")}), "1"),
		withRowid(table, dataRow(table, map[string]cell{"v": tval("b")}), "2"),
	}
	counts := []string{"3", "4"} // before, after
	var calls int
	client := startMockD1(t, withRowidCatalog(table, func(sql string, params []string) (int, []byte) {
		if isD1CountQuery(sql) {
			n := counts[calls]
			calls++
			return http.StatusOK, d1OK([]map[string]any{{"n": n}})
		}
		return fixedRows(page)(sql, params)
	}))
	r := &D1RowReader{client: client, pageSize: 4}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("a moving source must WARN, not refuse; got %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("delivered %d rows; want 2", len(got))
	}
	if calls != 2 {
		t.Fatalf("COUNT(*) issued %d times; want 2 (before and after the read)", calls)
	}
	for _, want := range []string{"not a point-in-time snapshot", "count_before=3", "count_after=4", "rows_delivered=2"} {
		if !strings.Contains(logBuf.String(), want) {
			t.Errorf("WARN must carry %q; got:\n%s", want, logBuf.String())
		}
	}
}

// TestD1RowReader_CountBracketFailureIsLoud pins that a failed COUNT(*) is a
// loud read error, never a skipped check.
func TestD1RowReader_CountBracketFailureIsLoud(t *testing.T) {
	table := shadowFreeTable()
	client := startMockD1(t, withRowidCatalog(table, func(sql string, _ []string) (int, []byte) {
		if isD1CountQuery(sql) {
			return http.StatusOK, d1Err(7500, "simulated COUNT failure")
		}
		return http.StatusOK, d1OK(nil)
	}))
	r := &D1RowReader{client: client}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	_ = drain(ch)
	if err := r.Err(); err == nil || !strings.Contains(err.Error(), "count rows before the read") ||
		!strings.Contains(err.Error(), "simulated COUNT failure") {
		t.Errorf("want the loud count failure; got %v", err)
	}
}

// TestD1SchemaReader_ExactRowCount pins the operator-invoked form of the same
// count (`sluice verify --depth count` against a `d1` source, LA-3) over a real
// SQLite through the exec-backed mock — an exact int64 out of the TEXT
// projection.
func TestD1SchemaReader_ExactRowCount(t *testing.T) {
	src, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "src.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	rowidShadowSeed(t, src, `CREATE TABLE t (rowid TEXT, v INTEGER)`)

	client := startMockD1(t, execD1Handler(src))
	sr := &D1SchemaReader{client: client}
	n, err := sr.ExactRowCount(context.Background(), &ir.Table{Name: "t"})
	if err != nil {
		t.Fatalf("ExactRowCount: %v", err)
	}
	if n != 7 {
		t.Errorf("ExactRowCount = %d; want 7", n)
	}
	if _, err := sr.ExactRowCount(context.Background(), nil); err == nil {
		t.Error("nil table must be refused")
	}
}
