// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ---- mock D1 server -------------------------------------------------------

// d1Handler answers one D1 query: it receives the SQL and positional params and
// returns the HTTP status + raw response body. Tests dispatch on the SQL.
type d1Handler func(sql string, params []string) (status int, body []byte)

// startMockD1 boots an httptest server speaking the D1 query API and returns a
// d1Client pointed at it (endpoint base injected — ADR-0132). Credentials are
// dummies; the mock does not check the Authorization header except where a test
// asks it to.
func startMockD1(t *testing.T, h d1Handler) *d1Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req d1RequestBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		status, body := h(req.SQL, req.Params)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return &d1Client{
		httpClient:   srv.Client(),
		endpointBase: srv.URL,
		accountID:    "acct",
		databaseID:   "db",
		token:        "tok",
	}
}

// d1OK builds a success envelope carrying one statement's result rows.
func d1OK(results []map[string]any) []byte {
	env := map[string]any{
		"result": []any{
			map[string]any{"results": results, "success": true, "meta": map[string]any{}},
		},
		"errors":   []any{},
		"messages": []any{},
		"success":  true,
	}
	b, _ := json.Marshal(env)
	return b
}

// d1Err builds a failed envelope (success:false + an errors[] entry).
func d1Err(code int, msg string) []byte {
	env := map[string]any{
		"result":   []any{},
		"errors":   []any{map[string]any{"code": code, "message": msg}},
		"messages": []any{},
		"success":  false,
	}
	b, _ := json.Marshal(env)
	return b
}

// dataRow builds one D1 data-result object for table: each column's exact-text
// value under its real name, and its typeof under the collision-free alias the
// reader will look up. cells maps column name → (typeof, jsonValue).
func dataRow(table *ir.Table, cells map[string]cell) map[string]any {
	prefix := typeofPrefix(table.Columns)
	row := map[string]any{}
	for i, c := range table.Columns {
		cl := cells[c.Name]
		row[c.Name] = cl.value
		row[typeofAlias(prefix, i)] = cl.typeOf
	}
	return row
}

// cell is a (typeof, exact-text/hex value) pair as D1 returns it. value is a
// Go string for a CAST/hex value (→ JSON string) or nil for a NULL value.
type cell struct {
	typeOf string
	value  any
}

func tval(s string) cell { return cell{typeOf: "text", value: s} }
func ival(s string) cell { return cell{typeOf: "integer", value: s} }
func rval(s string) cell { return cell{typeOf: "real", value: s} }

// withRowid stamps the implicit-rowid alias onto a data-result row, for tables
// the reader paginates by rowid (no PK, or a BLOB PK falling back to rowid).
func withRowid(table *ir.Table, row map[string]any, rowid string) map[string]any {
	row[typeofPrefix(table.Columns)+"rowid"] = rowid
	return row
}

// drain reads a row channel to completion and returns the rows.
func drain(ch <-chan ir.Row) []ir.Row {
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	return out
}

// ---- HEADLINE: integers > 2^53 round-trip EXACTLY -------------------------

// TestD1RowReader_BigIntExact is the reason this engine exists: an INTEGER value
// larger than 2^53 (a snowflake ID) and max int64 must decode to the EXACT
// int64, NOT the rounded double the bare-JSON / export paths return. End-to-end
// through the full reader (projection → HTTP → typeof/CAST decode).
func TestD1RowReader_BigIntExact(t *testing.T) {
	const (
		twoPow53Plus1 = int64(9007199254740993)    // 2^53+1 — the canonical rounding tripwire
		maxInt64      = int64(9223372036854775807) // off by 1,193 on the lossy paths
	)
	table := &ir.Table{
		Name:       "snowflakes",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	rows := []map[string]any{
		dataRow(table, map[string]cell{"id": ival("9007199254740993")}),
		dataRow(table, map[string]cell{"id": ival("9223372036854775807")}),
	}
	client := startMockD1(t, func(_ string, _ []string) (int, []byte) {
		return http.StatusOK, d1OK(rows)
	})
	r := &D1RowReader{client: client}

	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err after drain: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows; want 2", len(got))
	}
	if got[0]["id"] != twoPow53Plus1 {
		t.Errorf("2^53+1: got %#v (%T); want int64 %d — a ROUNDED value here is the silent-loss bug this engine exists to prevent", got[0]["id"], got[0]["id"], twoPow53Plus1)
	}
	if got[1]["id"] != maxInt64 {
		t.Errorf("max int64: got %#v (%T); want int64 %d", got[1]["id"], got[1]["id"], maxInt64)
	}
}

// TestD1StorageValue_BigIntExact pins the (typeof,text)→int64 reconstruction in
// isolation — the exact-text parse that defeats the JS-52-bit ceiling.
func TestD1StorageValue_BigIntExact(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int64
	}{
		{"9007199254740993", 9007199254740993},
		{"9223372036854775807", 9223372036854775807},
		{"-9223372036854775808", -9223372036854775808},
		{"0", 0},
	} {
		got, err := d1StorageValue("integer", json.RawMessage(`"`+tc.text+`"`))
		if err != nil {
			t.Fatalf("d1StorageValue integer %q: %v", tc.text, err)
		}
		if got != tc.want {
			t.Errorf("integer %q → %#v; want int64 %d", tc.text, got, tc.want)
		}
	}
}

// ---- typeof × shape decode matrix -----------------------------------------

// TestD1StorageValue_TypeofMatrix pins the per-typeof reconstruction: INTEGER vs
// REAL distinguished by typeof (so `1` and `1.0` don't collapse), TEXT carried
// verbatim, BLOB decoded from hex, NULL → nil, and an unrecognised/invalid
// payload refused loudly. (The raw→IR half is pinned by TestDecodeCell; this is
// the new D1 half.)
func TestD1StorageValue_TypeofMatrix(t *testing.T) {
	rawStr := func(s string) json.RawMessage { return json.RawMessage(`"` + s + `"`) }

	// integer 1 vs real 1.0 — same JSON-looking text, different storage class.
	if got, err := d1StorageValue("integer", rawStr("1")); err != nil || got != int64(1) {
		t.Errorf("integer 1 → %#v, err=%v; want int64(1)", got, err)
	}
	if got, err := d1StorageValue("real", rawStr("1.0")); err != nil || got != float64(1) {
		t.Errorf("real 1.0 → %#v, err=%v; want float64(1)", got, err)
	}
	if got, err := d1StorageValue("real", rawStr("1.5")); err != nil || got != float64(1.5) {
		t.Errorf("real 1.5 → %#v, err=%v; want float64(1.5)", got, err)
	}
	if got, err := d1StorageValue("text", rawStr("hello")); err != nil || got != "hello" {
		t.Errorf("text → %#v, err=%v; want \"hello\"", got, err)
	}
	// blob: 'cafe00ff' hex → exact bytes.
	got, err := d1StorageValue("blob", rawStr("cafe00ff"))
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	if b, ok := got.([]byte); !ok || len(b) != 4 || b[0] != 0xca || b[1] != 0xfe || b[2] != 0x00 || b[3] != 0xff {
		t.Errorf("blob 'cafe00ff' → %#v; want []byte{ca fe 00 ff}", got)
	}
	// null → nil (both the typeof-null and JSON-null forms).
	if got, err := d1StorageValue("null", json.RawMessage(`null`)); err != nil || got != nil {
		t.Errorf("null → %#v, err=%v; want nil", got, err)
	}
	if got, err := d1StorageValue("integer", json.RawMessage(`null`)); err != nil || got != nil {
		t.Errorf("integer+JSON-null → %#v, err=%v; want nil", got, err)
	}
	// Loud refusals.
	if _, err := d1StorageValue("integer", rawStr("not-a-number")); err == nil {
		t.Error("integer non-numeric text: want loud error")
	}
	if _, err := d1StorageValue("blob", rawStr("zzzz")); err == nil {
		t.Error("blob non-hex text: want loud error")
	}
	if _, err := d1StorageValue("clob", rawStr("x")); err == nil {
		t.Error("unrecognised typeof: want loud error")
	}
	// A bare JSON number (the lossy default path) must be refused, not silently
	// accepted — it means the CAST projection was bypassed.
	if _, err := d1StorageValue("integer", json.RawMessage(`9007199254740993`)); err == nil {
		t.Error("bare JSON number value: want loud refusal (projection not applied)")
	}
}

// TestD1RowReader_StorageClassFidelity pins the end-to-end refusal: a REAL value
// landing in an INTEGER-affinity column is refused loudly via the shared
// decodeCell mismatch path (storage-class fidelity), not silently coerced.
func TestD1RowReader_StorageClassFidelity(t *testing.T) {
	table := &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "n", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "n"}}, Unique: true},
	}
	rows := []map[string]any{dataRow(table, map[string]cell{"n": rval("1.5")})}
	client := startMockD1(t, func(string, []string) (int, []byte) { return http.StatusOK, d1OK(rows) })
	r := &D1RowReader{client: client}

	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	_ = drain(ch)
	if r.Err() == nil {
		t.Fatal("want a loud storage-class refusal for REAL in an INTEGER column; got none (silent coercion is the failure mode)")
	}
	if !strings.Contains(r.Err().Error(), "mismatch") || !strings.Contains(r.Err().Error(), "REAL") {
		t.Errorf("refusal %q must name the mismatch + the REAL storage class", r.Err())
	}
}

// ---- date / bool over the exact text --------------------------------------

// TestD1RowReader_DateBool pins the ADR-0129 date/bool policy over the D1 text:
// an ISO date/datetime column, a unixepoch integer column, and a 0/1 boolean.
func TestD1RowReader_DateBool(t *testing.T) {
	// ISO encoding (default): DATETIME text, DATE text, BOOLEAN 0/1.
	isoTable := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "at", Type: ir.Timestamp{}},
			{Name: "day", Type: ir.Date{}},
			{Name: "ok", Type: ir.Boolean{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	isoRows := []map[string]any{dataRow(isoTable, map[string]cell{
		"id":  ival("1"),
		"at":  tval("2024-01-02 03:04:05"),
		"day": tval("2024-01-02"),
		"ok":  ival("1"),
	})}
	client := startMockD1(t, func(string, []string) (int, []byte) { return http.StatusOK, d1OK(isoRows) })
	r := &D1RowReader{client: client} // dateEnc inherit → iso default
	ch, err := r.ReadRows(context.Background(), isoTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows; want 1", len(got))
	}
	at, ok := got[0]["at"].(time.Time)
	if !ok || !at.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("DATETIME → %#v; want 2024-01-02 03:04:05 UTC", got[0]["at"])
	}
	day, ok := got[0]["day"].(time.Time)
	if !ok || !day.Equal(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("DATE → %#v; want 2024-01-02 UTC", got[0]["day"])
	}
	if got[0]["ok"] != true {
		t.Errorf("BOOLEAN 1 → %#v; want true", got[0]["ok"])
	}

	// unixepoch encoding: a DATETIME column stored as an INTEGER unix-seconds.
	uxTable := &ir.Table{
		Name: "logs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "at", Type: ir.Timestamp{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	uxRows := []map[string]any{dataRow(uxTable, map[string]cell{
		"id": ival("1"),
		"at": ival("1704164645"), // 2024-01-02 03:04:05 UTC
	})}
	client2 := startMockD1(t, func(string, []string) (int, []byte) { return http.StatusOK, d1OK(uxRows) })
	r2 := &D1RowReader{client: client2, dateEnc: dateEncodingUnixEpoch}
	ch2, err := r2.ReadRows(context.Background(), uxTable)
	if err != nil {
		t.Fatalf("ReadRows ux: %v", err)
	}
	got2 := drain(ch2)
	if err := r2.Err(); err != nil {
		t.Fatalf("Err ux: %v", err)
	}
	at2, ok := got2[0]["at"].(time.Time)
	if !ok || !at2.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("unixepoch DATETIME → %#v; want 2024-01-02 03:04:05 UTC", got2[0]["at"])
	}
}

// ---- pagination -----------------------------------------------------------

// TestD1RowReader_Pagination pins keyset stitching across two pages in PK order
// with no dup/gap, and that the second page's request carries the previous
// page's last PK as the (string) keyset bound.
func TestD1RowReader_Pagination(t *testing.T) {
	table := &ir.Table{
		Name: "items",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "label", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	page1 := []map[string]any{
		dataRow(table, map[string]cell{"id": ival("1"), "label": tval("a")}),
		dataRow(table, map[string]cell{"id": ival("2"), "label": tval("b")}),
	}
	page2 := []map[string]any{
		dataRow(table, map[string]cell{"id": ival("3"), "label": tval("c")}),
	}
	var sawBound []string
	client := startMockD1(t, func(sql string, params []string) (int, []byte) {
		if !strings.Contains(sql, "WHERE") {
			return http.StatusOK, d1OK(page1) // first page, no bound
		}
		sawBound = params
		return http.StatusOK, d1OK(page2)
	})
	r := &D1RowReader{client: client, pageSize: 2} // page size 2 → page1 is full, triggers page2

	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	wantLabels := []string{"a", "b", "c"}
	if len(got) != 3 {
		t.Fatalf("got %d rows; want 3", len(got))
	}
	for i, w := range wantLabels {
		if got[i]["label"] != w {
			t.Errorf("row %d label = %#v; want %q (order/dup/gap bug)", i, got[i]["label"], w)
		}
	}
	if len(sawBound) != 1 || sawBound[0] != "2" {
		t.Errorf("page-2 keyset bound = %#v; want [\"2\"] (the last PK of page 1, as a string)", sawBound)
	}
}

// TestBuildD1PageQuery_Shape pins the projection + keyset SQL shape: typeof +
// CAST/hex per column, the keyset predicate and ORDER BY TABLE-QUALIFIED (so the
// typed column — not the CAST-text alias — orders the page), and the string
// bound param.
func TestBuildD1PageQuery_Shape(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	plan := pagePlan{typeofPrefix: typeofPrefix(table.Columns), orderCols: []string{"id"}}
	proj := buildD1Projection(table, plan)
	for _, want := range []string{
		`CAST("id" AS TEXT)`,
		`typeof("id")`,
		`WHEN 'blob' THEN hex("v")`,
	} {
		if !strings.Contains(proj, want) {
			t.Errorf("projection missing %q:\n%s", want, proj)
		}
	}

	// Subsequent page: keyset predicate + ORDER BY, table-qualified.
	query, params := buildD1PageQuery(table, plan, proj, []string{"42"}, 0, d1PageSize)
	if !strings.Contains(query, `WHERE "t"."id" > ?`) {
		t.Errorf("keyset predicate not table-qualified:\n%s", query)
	}
	if !strings.Contains(query, `ORDER BY "t"."id"`) {
		t.Errorf("ORDER BY not table-qualified (would sort the CAST-text alias lexically):\n%s", query)
	}
	if len(params) != 1 || params[0] != "42" {
		t.Errorf("bound params = %#v; want [\"42\"]", params)
	}

	// Composite key → row-value comparison.
	plan2 := pagePlan{typeofPrefix: typeofPrefix(table.Columns), orderCols: []string{"id", "v"}}
	sql2, _ := buildD1PageQuery(table, plan2, proj, []string{"1", "a"}, 0, d1PageSize)
	if !strings.Contains(sql2, `("t"."id", "t"."v") > (?, ?)`) {
		t.Errorf("composite keyset not a row-value comparison:\n%s", sql2)
	}
}

// ---- schema read ----------------------------------------------------------

// TestD1SchemaReader_ReadSchema pins schema extraction over HTTP from canned
// PRAGMA JSON: tables/columns/types/PK/FK, and that the table-list query carries
// the sqlite_* + _cf_* exclusion (the skip is server-side SQL).
func TestD1SchemaReader_ReadSchema(t *testing.T) {
	var tableListSQL string
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		switch {
		case strings.Contains(sql, "FROM sqlite_master"):
			tableListSQL = sql
			return http.StatusOK, d1OK([]map[string]any{
				{"name": "orders"}, {"name": "users"},
			})
		case strings.Contains(sql, "table_info('users')"):
			return http.StatusOK, d1OK([]map[string]any{
				{"cid": 0, "name": "id", "type": "INTEGER", "notnull": 1, "dflt_value": nil, "pk": 1},
				{"cid": 1, "name": "name", "type": "TEXT", "notnull": 0, "dflt_value": nil, "pk": 0},
				{"cid": 2, "name": "created_at", "type": "DATETIME", "notnull": 0, "dflt_value": nil, "pk": 0},
				{"cid": 3, "name": "active", "type": "BOOLEAN", "notnull": 0, "dflt_value": "1", "pk": 0},
			})
		case strings.Contains(sql, "table_info('orders')"):
			return http.StatusOK, d1OK([]map[string]any{
				{"cid": 0, "name": "id", "type": "INTEGER", "notnull": 1, "dflt_value": nil, "pk": 1},
				{"cid": 1, "name": "user_id", "type": "INTEGER", "notnull": 1, "dflt_value": nil, "pk": 0},
				{"cid": 2, "name": "total", "type": "REAL", "notnull": 0, "dflt_value": nil, "pk": 0},
			})
		case strings.Contains(sql, "foreign_key_list('orders')"):
			return http.StatusOK, d1OK([]map[string]any{
				{"id": 0, "seq": 0, "table": "users", "from": "user_id", "to": "id", "on_update": "NO ACTION", "on_delete": "CASCADE", "match": "NONE"},
			})
		case strings.Contains(sql, "foreign_key_list"):
			return http.StatusOK, d1OK(nil)
		case strings.Contains(sql, "index_list"):
			return http.StatusOK, d1OK(nil)
		default:
			return http.StatusOK, d1OK(nil)
		}
	})
	r := &D1SchemaReader{client: client}

	sch, err := r.ReadSchema(context.Background())
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if !strings.Contains(tableListSQL, `NOT LIKE 'sqlite_%'`) || !strings.Contains(tableListSQL, `\_cf\_%`) {
		t.Errorf("table-list query must exclude sqlite_* and _cf_*:\n%s", tableListSQL)
	}
	if len(sch.Tables) != 2 {
		t.Fatalf("got %d tables; want 2", len(sch.Tables))
	}
	users := sch.Tables[1] // ordered by name: orders, users
	if users.Name != "users" {
		t.Fatalf("table[1] = %q; want users", users.Name)
	}
	if users.PrimaryKey == nil || len(users.PrimaryKey.Columns) != 1 || users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %#v; want single-column id", users.PrimaryKey)
	}
	// Column types resolved through the shared resolveColumnType.
	wantTypes := map[string]string{
		"id": "Integer", "name": "Text", "created_at": "Timestamp", "active": "Boolean",
	}
	for _, c := range users.Columns {
		if got := typeKind(c.Type); got != wantTypes[c.Name] {
			t.Errorf("column %q type = %s; want %s", c.Name, got, wantTypes[c.Name])
		}
	}
	// id is a single-column INTEGER PK → rowid-alias auto-increment.
	for _, c := range users.Columns {
		if c.Name == "id" {
			iv, ok := c.Type.(ir.Integer)
			if !ok || !iv.AutoIncrement {
				t.Errorf("users.id should be auto-increment Integer; got %#v", c.Type)
			}
		}
	}
	orders := sch.Tables[0]
	if len(orders.ForeignKeys) != 1 {
		t.Fatalf("orders FKs = %d; want 1", len(orders.ForeignKeys))
	}
	fk := orders.ForeignKeys[0]
	if fk.ReferencedTable != "users" || len(fk.Columns) != 1 || fk.Columns[0] != "user_id" ||
		len(fk.ReferencedColumns) != 1 || fk.ReferencedColumns[0] != "id" || fk.OnDelete != ir.FKActionCascade {
		t.Errorf("orders FK = %#v; want user_id → users.id ON DELETE CASCADE", fk)
	}
}

// typeKind names an IR type for the schema assertions.
func typeKind(t ir.Type) string {
	switch t.(type) {
	case ir.Integer:
		return "Integer"
	case ir.Text:
		return "Text"
	case ir.Timestamp:
		return "Timestamp"
	case ir.Boolean:
		return "Boolean"
	case ir.Float:
		return "Float"
	case ir.Decimal:
		return "Decimal"
	default:
		return "other"
	}
}

// ---- transport errors -----------------------------------------------------

// TestD1Client_TransportErrors pins the loud-failure surface: success:false /
// errors[] / non-2xx all return an error naming the cause; the D1 error text is
// surfaced.
func TestD1Client_TransportErrors(t *testing.T) {
	t.Run("success_false", func(t *testing.T) {
		client := startMockD1(t, func(string, []string) (int, []byte) {
			return http.StatusOK, d1Err(7400, "no such table: gone")
		})
		_, err := client.queryRows(context.Background(), "SELECT 1")
		if err == nil || !strings.Contains(err.Error(), "no such table: gone") {
			t.Errorf("want loud error naming the D1 message; got %v", err)
		}
	})
	t.Run("non_2xx", func(t *testing.T) {
		client := startMockD1(t, func(string, []string) (int, []byte) {
			return http.StatusUnauthorized, []byte(`{"success":false,"errors":[{"code":10000,"message":"Authentication error"}]}`)
		})
		_, err := client.queryRows(context.Background(), "SELECT 1")
		if err == nil || !strings.Contains(err.Error(), "401") {
			t.Errorf("want loud HTTP-status error; got %v", err)
		}
	})
	t.Run("empty_result_block", func(t *testing.T) {
		client := startMockD1(t, func(string, []string) (int, []byte) {
			return http.StatusOK, []byte(`{"success":true,"errors":[],"result":[]}`)
		})
		_, err := client.queryRows(context.Background(), "SELECT 1")
		if err == nil || !strings.Contains(err.Error(), "no result block") {
			t.Errorf("want loud no-result error; got %v", err)
		}
	})
}

// ---- DSN + secrets --------------------------------------------------------

// TestParseD1DSN pins both DSN forms, the date-encoding param, and loud refusals.
func TestParseD1DSN(t *testing.T) {
	cases := []struct {
		in          string
		wantAccount string
		wantDB      string
		wantEnc     dateEncoding
		wantErr     bool
	}{
		{"d1://acct123/db456", "acct123", "db456", dateEncodingInherit, false},
		{"d1://db456", "", "db456", dateEncodingInherit, false},
		{"d1://acct123/db456?sqlite_date_encoding=unixepoch", "acct123", "db456", dateEncodingUnixEpoch, false},
		{"", "", "", dateEncodingInherit, true},
		{"d1://", "", "", dateEncodingInherit, true},
		{"postgres://x", "", "", dateEncodingInherit, true},
		{"d1://acct/db?sqlite_date_encoding=bogus", "", "", dateEncodingInherit, true},
	}
	for _, c := range cases {
		acct, db, enc, err := parseD1DSN(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseD1DSN(%q): want error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseD1DSN(%q): %v", c.in, err)
			continue
		}
		if acct != c.wantAccount || db != c.wantDB || enc != c.wantEnc {
			t.Errorf("parseD1DSN(%q) = (%q,%q,%v); want (%q,%q,%v)", c.in, acct, db, enc, c.wantAccount, c.wantDB, c.wantEnc)
		}
	}
}

// TestOpenD1Client_Secrets pins the env-first secrets posture: a missing token
// or account is refused loudly; the account comes from the env when the DSN
// omits it; the DSN account wins over the env.
func TestOpenD1Client_Secrets(t *testing.T) {
	t.Run("missing_token", func(t *testing.T) {
		t.Setenv(envD1Token, "")
		t.Setenv(envD1Account, "acctenv")
		_, err := openD1Client("d1://acct/db")
		if err == nil || !strings.Contains(err.Error(), envD1Token) {
			t.Errorf("want loud missing-token error naming %s; got %v", envD1Token, err)
		}
	})
	t.Run("missing_account", func(t *testing.T) {
		t.Setenv(envD1Token, "tok")
		t.Setenv(envD1Account, "")
		_, err := openD1Client("d1://db") // no account in DSN, none in env
		if err == nil || !strings.Contains(err.Error(), envD1Account) {
			t.Errorf("want loud missing-account error naming %s; got %v", envD1Account, err)
		}
	})
	t.Run("account_from_env", func(t *testing.T) {
		t.Setenv(envD1Token, "tok")
		t.Setenv(envD1Account, "acctenv")
		c, err := openD1Client("d1://dbonly")
		if err != nil {
			t.Fatalf("openD1Client: %v", err)
		}
		if c.accountID != "acctenv" || c.databaseID != "dbonly" {
			t.Errorf("client = (%q,%q); want (acctenv,dbonly)", c.accountID, c.databaseID)
		}
	})
	t.Run("dsn_account_wins", func(t *testing.T) {
		t.Setenv(envD1Token, "tok")
		t.Setenv(envD1Account, "acctenv")
		c, err := openD1Client("d1://acctdsn/db")
		if err != nil {
			t.Fatalf("openD1Client: %v", err)
		}
		if c.accountID != "acctdsn" {
			t.Errorf("account = %q; want acctdsn (DSN wins over env)", c.accountID)
		}
	})
}

// ---- engine registration + not-implemented --------------------------------

// TestD1EngineRegistered confirms the d1 engine self-registered alongside sqlite.
func TestD1EngineRegistered(t *testing.T) {
	e, ok := engines.Get("d1")
	if !ok {
		t.Fatal("d1 engine not registered")
	}
	if e.Name() != "d1" {
		t.Errorf("Name() = %q; want d1", e.Name())
	}
	if e.Capabilities().CDC != ir.CDCNone {
		t.Errorf("CDC = %v; want CDCNone", e.Capabilities().CDC)
	}
}

// TestD1WriteSideNotImplemented confirms the write/CDC/snapshot Open* return the
// wrapped ErrD1NotImplemented — D1 is a migrate source only.
func TestD1WriteSideNotImplemented(t *testing.T) {
	e := d1Engine{}
	ctx := context.Background()
	const dsn = "d1://acct/db"
	if _, err := e.OpenSchemaWriter(ctx, dsn); !errors.Is(err, ErrD1NotImplemented) {
		t.Errorf("OpenSchemaWriter err = %v; want ErrD1NotImplemented", err)
	}
	if _, err := e.OpenRowWriter(ctx, dsn); !errors.Is(err, ErrD1NotImplemented) {
		t.Errorf("OpenRowWriter err = %v; want ErrD1NotImplemented", err)
	}
	if _, err := e.OpenCDCReader(ctx, dsn); !errors.Is(err, ErrD1NotImplemented) {
		t.Errorf("OpenCDCReader err = %v; want ErrD1NotImplemented", err)
	}
	if _, err := e.OpenChangeApplier(ctx, dsn); !errors.Is(err, ErrD1NotImplemented) {
		t.Errorf("OpenChangeApplier err = %v; want ErrD1NotImplemented", err)
	}
	if _, err := e.OpenSnapshotStream(ctx, dsn); !errors.Is(err, ErrD1NotImplemented) {
		t.Errorf("OpenSnapshotStream err = %v; want ErrD1NotImplemented", err)
	}
}

// TestD1Engine_OpenMissingToken confirms a row/schema open refuses loudly before
// any request when the env token is absent.
func TestD1Engine_OpenMissingToken(t *testing.T) {
	t.Setenv(envD1Token, "")
	t.Setenv(envD1Account, "acct")
	e := d1Engine{}
	if _, err := e.OpenRowReader(context.Background(), "d1://acct/db"); err == nil ||
		!strings.Contains(err.Error(), envD1Token) {
		t.Errorf("OpenRowReader want loud missing-token error; got %v", err)
	}
	if _, err := e.OpenSchemaReader(context.Background(), "d1://acct/db"); err == nil ||
		!strings.Contains(err.Error(), envD1Token) {
		t.Errorf("OpenSchemaReader want loud missing-token error; got %v", err)
	}
}

// ---- BLOCK 1: REAL precision (%.17g round-trip) ---------------------------

// TestD1RealPrecision_Pure pins that a REAL rendered at 17 significant digits
// (the IEEE-754 round-trip guarantee, which the projection now uses instead of
// CAST-AS-TEXT) decodes back to the EXACT float64 — for ir.Float and for
// ir.Decimal's real branch. Without %.17g a low-digit render would silently lose
// the low bits.
func TestD1RealPrecision_Pure(t *testing.T) {
	vals := []float64{math.Pi, 0.1, 1.0 / 3.0, 1.0, 1234567890123456.7, math.SmallestNonzeroFloat64, math.MaxFloat64}
	for _, want := range vals {
		text := fmt.Sprintf("%.17g", want) // mirrors SQLite format('%.17g', x)
		raw := json.RawMessage(`"` + text + `"`)

		got, err := d1StorageValue("real", raw)
		if err != nil {
			t.Fatalf("d1StorageValue real %q: %v", text, err)
		}
		f, err := decodeCell(got, ir.Float{Precision: ir.FloatDouble}, dateEncodingISO)
		if err != nil {
			t.Fatalf("decodeCell Float %q: %v", text, err)
		}
		if f != want {
			t.Errorf("real %.17g → Float %v; want EXACT %v (precision silently lost)", want, f, want)
		}
		// ir.Decimal real branch: shortest round-trippable decimal of the same float.
		d, err := decodeCell(got, ir.Decimal{Unconstrained: true}, dateEncodingISO)
		if err != nil {
			t.Fatalf("decodeCell Decimal %q: %v", text, err)
		}
		if d != strconv.FormatFloat(want, 'g', -1, 64) {
			t.Errorf("real %.17g → Decimal %q; want %q", want, d, strconv.FormatFloat(want, 'g', -1, 64))
		}
	}
}

// TestD1Format17g_RealDriver is the SQLite ground truth for BLOCK 1: it confirms
// that the real engine D1 runs (modernc = the same SQLite) actually supports
// `format('%.17g', x)` AND that its output round-trips a double exactly — so the
// projection's dependence shifts from D1's default formatter to the IEEE-754
// guarantee. It runs the reader's EXACT generated projection over a real table
// and decodes via the production d1StorageValue.
func TestD1Format17g_RealDriver(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, f REAL, big INTEGER, b BLOB)`,
		`INSERT INTO t (id, f, big, b) VALUES (9007199254740993, 3.141592653589793, 9223372036854775807, x'cafe00ff')`,
	)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "f", Type: ir.Float{Precision: ir.FloatDouble}},
			{Name: "big", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Blob{Size: ir.BlobLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	plan := pagePlan{typeofPrefix: typeofPrefix(table.Columns), orderCols: []string{"id"}}
	proj := buildD1Projection(table, plan)
	query, _ := buildD1PageQuery(table, plan, proj, nil, 0, d1PageSize)

	rows, err := db.QueryContext(context.Background(), query) //nolint:rowserrcheck // single-row read below, err checked
	if err != nil {
		t.Fatalf("run projection %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("no rows from projection")
	}
	colNames, _ := rows.Columns()
	cells := make([]sql.NullString, len(colNames))
	ptrs := make([]any, len(colNames))
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("scan: %v", err)
	}
	byName := map[string]sql.NullString{}
	for i, n := range colNames {
		byName[n] = cells[i]
	}

	decode := func(i int, col *ir.Column) any {
		t.Helper()
		typeofText := byName[typeofAlias(plan.typeofPrefix, i)].String
		var raw json.RawMessage
		if v := byName[col.Name]; v.Valid {
			b, _ := json.Marshal(v.String)
			raw = b
		} else {
			raw = json.RawMessage(`null`)
		}
		storage, err := d1StorageValue(typeofText, raw)
		if err != nil {
			t.Fatalf("d1StorageValue %s: %v", col.Name, err)
		}
		out, err := decodeCell(storage, col.Type, dateEncodingISO)
		if err != nil {
			t.Fatalf("decodeCell %s: %v", col.Name, err)
		}
		return out
	}

	if got := decode(1, table.Columns[1]); got != 3.141592653589793 {
		t.Errorf("REAL via real driver = %v; want exact math.Pi-ish 3.141592653589793", got)
	}
	if got := decode(2, table.Columns[2]); got != int64(9223372036854775807) {
		t.Errorf("big INTEGER via real driver = %v; want exact max int64", got)
	}
	if got, ok := decode(3, table.Columns[3]).([]byte); !ok || len(got) != 4 || got[0] != 0xca || got[3] != 0xff {
		t.Errorf("BLOB via real driver = %#v; want ca fe 00 ff", got)
	}
}

// TestD1Julian_RealText pins a julian-day timestamp delivered as a %.17g REAL:
// it decodes to the right instant (the REAL precision fix matters for the
// julian/unix-REAL temporal encodings, not just ir.Float).
func TestD1Julian_RealText(t *testing.T) {
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	const unixEpochJulianDay = 2440587.5
	jd := unixEpochJulianDay + float64(want.Unix())/86400.0
	raw := json.RawMessage(`"` + fmt.Sprintf("%.17g", jd) + `"`)

	storage, err := d1StorageValue("real", raw)
	if err != nil {
		t.Fatalf("d1StorageValue real: %v", err)
	}
	got, err := decodeCell(storage, ir.Timestamp{}, dateEncodingJulian)
	if err != nil {
		t.Fatalf("decodeCell julian: %v", err)
	}
	gotT, ok := got.(time.Time)
	if !ok || gotT.Sub(want).Abs() > time.Millisecond {
		t.Errorf("julian REAL → %v; want ~%v", got, want)
	}
}

// ---- BLOCK 2: BLOB-PK keyset safety ---------------------------------------

// TestD1RowReader_BlobPKUsesRowid pins that a table with a BLOB primary key
// paginates by the integer rowid (NOT the blob, which would loop forever because
// SQLite ranks BLOB above every TEXT param) — it terminates on a short page and
// the generated page query orders by rowid.
func TestD1RowReader_BlobPKUsesRowid(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "k", Type: ir.Blob{Size: ir.BlobLong}},
			{Name: "v", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}, Unique: true},
	}
	var dataSQL string
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		switch {
		case strings.Contains(sql, "SELECT rowid FROM"):
			return http.StatusOK, d1OK(nil) // rowid exists (probe succeeds)
		default:
			dataSQL = sql
			return http.StatusOK, d1OK([]map[string]any{
				withRowid(table, dataRow(table, map[string]cell{"k": bval("cafe"), "v": tval("x")}), "1"),
			})
		}
	})
	r := &D1RowReader{client: client, pageSize: 2}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows; want 1 (a blob-keyset loop would never terminate / would dupe)", len(got))
	}
	if !strings.Contains(dataSQL, `ORDER BY "t"."rowid"`) {
		t.Errorf("blob-PK table must paginate by rowid, not the blob key:\n%s", dataSQL)
	}
}

// bval builds a blob cell (hex value).
func bval(hx string) cell { return cell{typeOf: "blob", value: hx} }

// TestD1RowReader_BlobPKNoRowidRefused pins the loud refusal when a WITHOUT ROWID
// table is keyed only by a BLOB column: no safe keyset and no rowid fallback, so
// the reader refuses rather than looping forever.
func TestD1RowReader_BlobPKNoRowidRefused(t *testing.T) {
	table := &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "k", Type: ir.Blob{Size: ir.BlobLong}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}, Unique: true},
	}
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		if strings.Contains(sql, "SELECT rowid FROM") {
			return http.StatusOK, d1Err(1, "no such column: rowid") // WITHOUT ROWID
		}
		return http.StatusOK, d1OK(nil)
	})
	r := &D1RowReader{client: client}
	_, err := r.ReadRows(context.Background(), table)
	if err == nil || !strings.Contains(err.Error(), "BLOB") || !strings.Contains(err.Error(), "t") {
		t.Errorf("want loud BLOB-key refusal naming the table; got %v", err)
	}
}

// TestD1RowReader_RowidFallbackStitch pins keyset-by-rowid for a table with NO
// explicit primary key: two pages stitch in rowid order with no dup/gap, and the
// page-2 bound is the last rowid.
func TestD1RowReader_RowidFallbackStitch(t *testing.T) {
	table := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}},
	}
	page1 := []map[string]any{
		withRowid(table, dataRow(table, map[string]cell{"v": tval("a")}), "1"),
		withRowid(table, dataRow(table, map[string]cell{"v": tval("b")}), "2"),
	}
	page2 := []map[string]any{
		withRowid(table, dataRow(table, map[string]cell{"v": tval("c")}), "3"),
	}
	var bound []string
	client := startMockD1(t, func(sql string, params []string) (int, []byte) {
		switch {
		case strings.Contains(sql, "SELECT rowid FROM"):
			return http.StatusOK, d1OK(nil)
		case strings.Contains(sql, "WHERE"):
			bound = params
			return http.StatusOK, d1OK(page2)
		default:
			return http.StatusOK, d1OK(page1)
		}
	})
	r := &D1RowReader{client: client, pageSize: 2}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	want := []string{"a", "b", "c"}
	if len(got) != 3 {
		t.Fatalf("got %d rows; want 3", len(got))
	}
	for i, w := range want {
		if got[i]["v"] != w {
			t.Errorf("row %d = %#v; want %q", i, got[i]["v"], w)
		}
	}
	if len(bound) != 1 || bound[0] != "2" {
		t.Errorf("rowid keyset bound = %#v; want [\"2\"]", bound)
	}
}

// TestD1RowReader_CompositePKStitch pins an END-TO-END composite-PK keyset across
// a page boundary: the row-value comparison (a,b) > (?,?) advances correctly with
// no dup/gap, and the page-2 bound is the last (a,b).
func TestD1RowReader_CompositePKStitch(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}, Unique: true},
	}
	page1 := []map[string]any{
		dataRow(table, map[string]cell{"a": ival("1"), "b": ival("1"), "v": tval("a")}),
		dataRow(table, map[string]cell{"a": ival("1"), "b": ival("2"), "v": tval("b")}),
	}
	page2 := []map[string]any{
		dataRow(table, map[string]cell{"a": ival("2"), "b": ival("1"), "v": tval("c")}),
	}
	var bound []string
	var page2SQL string
	client := startMockD1(t, func(sql string, params []string) (int, []byte) {
		if strings.Contains(sql, "WHERE") {
			bound = params
			page2SQL = sql
			return http.StatusOK, d1OK(page2)
		}
		return http.StatusOK, d1OK(page1)
	})
	r := &D1RowReader{client: client, pageSize: 2}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	want := []string{"a", "b", "c"}
	if len(got) != 3 {
		t.Fatalf("got %d rows; want 3 (composite keyset dup/gap)", len(got))
	}
	for i, w := range want {
		if got[i]["v"] != w {
			t.Errorf("row %d = %#v; want %q", i, got[i]["v"], w)
		}
	}
	if len(bound) != 2 || bound[0] != "1" || bound[1] != "2" {
		t.Errorf("composite bound = %#v; want [\"1\",\"2\"]", bound)
	}
	if !strings.Contains(page2SQL, `("t"."a", "t"."b") > (?, ?)`) {
		t.Errorf("page-2 SQL must use the row-value comparison:\n%s", page2SQL)
	}
}

// TestD1RowReader_NullKeysetRefused pins the loud refusal when a keyset (PK)
// column value is NULL — a NULL bound would make pagination skip or loop.
func TestD1RowReader_NullKeysetRefused(t *testing.T) {
	table := &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "k", Type: ir.Text{Size: ir.TextLong}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}, Unique: true},
	}
	rows := []map[string]any{dataRow(table, map[string]cell{"k": nullCell()})}
	client := startMockD1(t, func(string, []string) (int, []byte) { return http.StatusOK, d1OK(rows) })
	r := &D1RowReader{client: client}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	_ = drain(ch)
	if r.Err() == nil || !strings.Contains(r.Err().Error(), "primary-key column") {
		t.Errorf("want loud NULL-keyset refusal; got %v", r.Err())
	}
}

// nullCell is a NULL cell (typeof null, JSON null value).
func nullCell() cell { return cell{typeOf: "null", value: nil} }

// TestD1RowReader_OffsetFallbackStitch pins the LIMIT/OFFSET fallback for a table
// with neither a PK nor a rowid: pages stitch by OFFSET and the documented
// not-safe-under-concurrent-writes caveat is logged.
func TestD1RowReader_OffsetFallbackStitch(t *testing.T) {
	// Capture the WARN the fallback emits.
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	table := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}},
	}
	page1 := []map[string]any{
		dataRow(table, map[string]cell{"v": tval("a")}),
		dataRow(table, map[string]cell{"v": tval("b")}),
	}
	page2 := []map[string]any{dataRow(table, map[string]cell{"v": tval("c")})}
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		switch {
		case strings.Contains(sql, "SELECT rowid FROM"):
			return http.StatusOK, d1Err(1, "no such column: rowid") // no rowid → OFFSET fallback
		case strings.Contains(sql, "OFFSET 2"):
			return http.StatusOK, d1OK(page2)
		default:
			return http.StatusOK, d1OK(page1)
		}
	})
	r := &D1RowReader{client: client, pageSize: 2}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	want := []string{"a", "b", "c"}
	if len(got) != 3 {
		t.Fatalf("got %d rows; want 3 (OFFSET stitch)", len(got))
	}
	for i, w := range want {
		if got[i]["v"] != w {
			t.Errorf("row %d = %#v; want %q", i, got[i]["v"], w)
		}
	}
	if !strings.Contains(logBuf.String(), "LIMIT/OFFSET") {
		t.Errorf("expected the LIMIT/OFFSET not-safe caveat to be logged; got:\n%s", logBuf.String())
	}
}

// TestD1RowReader_NumericDecimal pins an ir.Decimal (NUMERIC-affinity) column
// end-to-end for BOTH an integer and a real stored value — each carried as an
// exact decimal string through the shared decodeCell.
func TestD1RowReader_NumericDecimal(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "n", Type: ir.Decimal{Unconstrained: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	rows := []map[string]any{
		dataRow(table, map[string]cell{"id": ival("1"), "n": ival("42")}),
		dataRow(table, map[string]cell{"id": ival("2"), "n": rval(fmt.Sprintf("%.17g", 1.5))}),
	}
	client := startMockD1(t, func(string, []string) (int, []byte) { return http.StatusOK, d1OK(rows) })
	r := &D1RowReader{client: client}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows; want 2", len(got))
	}
	if got[0]["n"] != "42" {
		t.Errorf("NUMERIC integer → %#v; want \"42\"", got[0]["n"])
	}
	if got[1]["n"] != "1.5" {
		t.Errorf("NUMERIC real → %#v; want \"1.5\"", got[1]["n"])
	}
}

// TestD1RowReader_TextVerbatim pins that a TEXT column carries an embedded NUL
// byte and multibyte unicode VERBATIM (the reader is faithful; any NUL refusal
// is a downstream writer concern, not the reader's to silently strip).
func TestD1RowReader_TextVerbatim(t *testing.T) {
	const withNUL = "a\x00b"
	const unicode = "héllo · 世界"
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "s", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	rows := []map[string]any{
		dataRow(table, map[string]cell{"id": ival("1"), "s": tval(withNUL)}),
		dataRow(table, map[string]cell{"id": ival("2"), "s": tval(unicode)}),
	}
	client := startMockD1(t, func(string, []string) (int, []byte) { return http.StatusOK, d1OK(rows) })
	r := &D1RowReader{client: client}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if got[0]["s"] != withNUL {
		t.Errorf("embedded-NUL text = %q; want %q (verbatim)", got[0]["s"], withNUL)
	}
	if got[1]["s"] != unicode {
		t.Errorf("unicode text = %q; want %q (verbatim)", got[1]["s"], unicode)
	}
}
