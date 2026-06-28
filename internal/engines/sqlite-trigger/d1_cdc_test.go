// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// These httptest-backed tests pin the Phase-2 D1 transport substitution
// (ADR-0136) WITHOUT a live D1: a mock /query server lets us (a) capture the
// setup DDL request bodies, (b) prove the change-log poll is the right SELECT
// with the watermark bound as a STRING param and that a faithful ir.Change is
// reconstructed from a canned (typeof, text/hex) response — incl. an integer
// > 2^53 EXACT, a REAL, a BLOB-from-hex, and NULL — (c) prove the watermark
// resumes/advances, (d) prove the schema-drift fingerprint refusal fires, and
// (e) prove the token/DSN refusals fire loudly at open. The decode path itself
// is the SHARED sqlite decoder (already pinned by the local DB tests +
// sqlite/d1 tests); these tests pin the HTTP transport + executor parsing.

// --- mock D1 /query server --------------------------------------------------

// mockD1 answers the D1 query API over httptest, dispatching on the SQL and
// recording the requests so tests can assert request shapes.
type mockD1 struct {
	mu sync.Mutex

	exists        bool        // change-log table present?
	fingerprints  [][2]string // (tbl, columns) rows for the drift check
	maxID         string      // CAST(MAX(id) AS TEXT); "" → NULL (empty log)
	rows          []clRow     // the canned change-log rows (filtered by id>since)
	pollHTTPError bool        // when set, the poll returns HTTP 500 (transport error)

	ddl      []string // captured non-SELECT statements (setup/teardown DDL)
	pollSQL  []string // captured poll SELECTs
	pollArgs []string // the watermark param of each poll
	bodies   []string // raw request bodies (to assert string-param binding)
}

// clRow is one canned change-log row.
type clRow struct {
	id         int64
	op         string
	before     map[string]any // nil → JSON null image
	after      map[string]any // nil → JSON null image
	capturedAt string
}

func (m *mockD1) handle(t *testing.T, raw []byte, sql string, params []string) (status int, body []byte) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bodies = append(m.bodies, string(raw))
	// A non-SELECT statement is setup/teardown DDL — record it. (Checked first so
	// a CREATE TABLE / INSERT against the columns table isn't mis-routed to the
	// fingerprint-SELECT branch below.)
	if !strings.HasPrefix(strings.TrimSpace(sql), "SELECT") {
		m.ddl = append(m.ddl, sql)
		return http.StatusOK, d1OK(nil)
	}
	switch {
	case sql == "SELECT 1":
		return http.StatusOK, d1OK(nil)
	case strings.Contains(sql, "sluice_change_log_columns"):
		rows := make([]map[string]any, 0, len(m.fingerprints))
		for _, fp := range m.fingerprints {
			rows = append(rows, map[string]any{"tbl": fp[0], "columns": fp[1]})
		}
		return http.StatusOK, d1OK(rows)
	case strings.Contains(sql, "MAX(id)"):
		v := any(nil)
		if m.maxID != "" {
			v = m.maxID
		}
		return http.StatusOK, d1OK([]map[string]any{{"m": v}})
	case strings.Contains(sql, "WHERE id > ?"):
		since := ""
		if len(params) > 0 {
			since = params[0]
		}
		m.pollSQL = append(m.pollSQL, sql)
		m.pollArgs = append(m.pollArgs, since)
		if m.pollHTTPError {
			// A transport failure mid-poll MUST surface via Err() (loud), never a
			// silent empty batch falsely read as "no changes".
			return http.StatusInternalServerError, []byte("d1 internal error")
		}
		return http.StatusOK, d1OK(m.pollResultsLocked(t, since))
	case strings.Contains(sql, "type = 'table'"):
		if m.exists {
			return http.StatusOK, d1OK([]map[string]any{{"name": sqlite.ChangeLogTable}})
		}
		return http.StatusOK, d1OK(nil)
	case strings.Contains(sql, "type = 'trigger'"):
		return http.StatusOK, d1OK(nil) // teardown discovery; unused here
	default:
		t.Fatalf("mock D1: unexpected SELECT: %q", sql)
		return http.StatusInternalServerError, nil
	}
}

// pollResultsLocked returns the canned rows with id > since, shaped as D1 result
// rows (id as exact TEXT per the CAST(id AS TEXT) projection; before/after as the
// captured JSON image strings; NULL images as JSON null).
func (m *mockD1) pollResultsLocked(t *testing.T, since string) []map[string]any {
	t.Helper()
	sinceID := int64(0)
	if since != "" {
		n, err := strconv.ParseInt(since, 10, 64)
		if err != nil {
			t.Fatalf("poll since param %q is not an int (should be a string-encoded int): %v", since, err)
		}
		sinceID = n
	}
	var out []map[string]any
	for _, r := range m.rows {
		if r.id <= sinceID {
			continue
		}
		out = append(out, map[string]any{
			"id":          strconv.FormatInt(r.id, 10),
			"op":          r.op,
			"tbl":         "t",
			"before":      imageOrNull(t, r.before),
			"after":       imageOrNull(t, r.after),
			"captured_at": r.capturedAt,
		})
	}
	return out
}

// startMockD1 boots the httptest server + returns the mock and a D1Conn (via the
// exported test seam) pointed at it.
func startMockD1(t *testing.T, m *mockD1) *sqlite.D1Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req struct {
			SQL    string   `json:"sql"`
			Params []string `json:"params"`
		}
		_ = json.Unmarshal(raw, &req)
		status, body := m.handle(t, raw, req.SQL, req.Params)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return sqlite.D1ConnForTest(srv.URL, "acct", "db", "tok")
}

// d1OK builds a D1 success envelope carrying one statement's result rows.
func d1OK(results []map[string]any) []byte {
	env := map[string]any{
		"result":   []any{map[string]any{"results": results, "success": true}},
		"errors":   []any{},
		"messages": []any{},
		"success":  true,
	}
	b, _ := json.Marshal(env)
	return b
}

// cell builds one captured (typeof, value) pair; pass v=nil for a NULL cell.
func cell(typeOf string, v any) map[string]any { return map[string]any{"t": typeOf, "v": v} }

// imageOrNull renders a captured before/after image map as its JSON text, or a
// JSON null (Go nil) when the image is absent (e.g. the before of an INSERT).
func imageOrNull(t *testing.T, cells map[string]any) any {
	t.Helper()
	if cells == nil {
		return nil
	}
	b, err := json.Marshal(cells)
	if err != nil {
		t.Fatalf("marshal image: %v", err)
	}
	return string(b)
}

// --- stub cold-start engine -------------------------------------------------

// stubColdStart stands in for the `d1` cold-start engine: its schema reader
// returns a fixed schema so the executor/decode path can be exercised without a
// live D1 catalog read (the real D1 schema reader is pinned in the sqlite
// package's d1 tests). Only OpenSchemaReader is used by these tests.
type stubColdStart struct{ schema *ir.Schema }

func (stubColdStart) Name() string                  { return "stub-d1" }
func (stubColdStart) Capabilities() ir.Capabilities { return ir.Capabilities{} }

// OpenSchemaReader returns the stub itself (it also implements ir.SchemaReader
// via ReadSchema), so the canned schema flows to resolveTables / loadColumnTypes.
func (e stubColdStart) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return e, nil
}

func (e stubColdStart) ReadSchema(context.Context) (*ir.Schema, error) { return e.schema, nil }

func (stubColdStart) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errUnsupportedStub
}

func (stubColdStart) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errUnsupportedStub
}

func (stubColdStart) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errUnsupportedStub
}

func (stubColdStart) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errUnsupportedStub
}

func (stubColdStart) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errUnsupportedStub
}

func (stubColdStart) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errUnsupportedStub
}

var errUnsupportedStub = stubErr("stub-d1: unsupported in this test")

type stubErr string

func (e stubErr) Error() string { return string(e) }

// fidelityTable mirrors the local DB fidelity matrix's table (every storage
// class + the IR families that consume them).
func fidelityTable() *ir.Table {
	return &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "big", Type: ir.Integer{}},
			{Name: "flt", Type: ir.Float{}},
			{Name: "txt", Type: ir.Text{}},
			{Name: "blb", Type: ir.Blob{}},
			{Name: "num", Type: ir.Decimal{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// d1TestBackend builds a backend whose executor + decoder run over the mock
// (conn) and whose schema comes from the stub. It is the white-box seam these
// tests drive the shared logic through.
func d1TestBackend(conn *sqlite.D1Conn, schema *ir.Schema) backend {
	return backend{
		driver:    EngineNameD1,
		dsn:       "d1://acct/db",
		coldStart: stubColdStart{schema},
		newDecoder: func() (*sqlite.CapturedCellDecoder, error) {
			return conn.CellDecoder(), nil
		},
		openExec: func(context.Context, bool) (executor, error) {
			return &d1Executor{conn: conn}, nil
		},
	}
}

// --- tests ------------------------------------------------------------------

// TestD1Setup_IssuesExpectedDDL pins (a): trigger setup over the D1 API issues
// the SAME change-log + meta + fingerprint DDL and per-table capture triggers as
// the local engine, captured as request bodies on the mock /query endpoint.
func TestD1Setup_IssuesExpectedDDL(t *testing.T) {
	m := &mockD1{}
	conn := startMockD1(t, m)
	tbl := fidelityTable()
	b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})

	if _, err := setup(bg(), b, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("setup over D1: %v", err)
	}

	all := strings.Join(m.ddl, "\n")
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "sluice_change_log"`,
		"id           INTEGER PRIMARY KEY AUTOINCREMENT",
		`CREATE TABLE IF NOT EXISTS "sluice_change_log_meta"`,
		`CREATE TABLE IF NOT EXISTS "sluice_change_log_columns"`,
		`INSERT INTO "sluice_change_log_meta" (singleton_pk, schema_version) VALUES (1, 1)`,
		`CREATE TRIGGER "sluice_capture_t_ins" AFTER INSERT ON "t"`,
		`CREATE TRIGGER "sluice_capture_t_upd" AFTER UPDATE ON "t"`,
		`CREATE TRIGGER "sluice_capture_t_del" AFTER DELETE ON "t"`,
		// Faithful per-column capture body (the §crux), byte-identical to the
		// local engine — the blob column must use hex(), never a lossy JSON number.
		`json_object('t', typeof(NEW."big"), 'v', CASE typeof(NEW."big") WHEN 'blob' THEN hex(NEW."big")`,
		// The captured-column fingerprint records the non-generated set.
		`INSERT INTO "sluice_change_log_columns" (tbl, columns) VALUES ('t', '["id","big","flt","txt","blb","num"]')`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("setup DDL over D1 missing fragment:\n  %s\n--- all ---\n%s", want, all)
		}
	}
}

// TestD1Capture_FidelityMatrixOverHTTP pins (b): a canned (typeof, text/hex)
// change-log response decodes to EXACT values across every storage class — the
// Bug-74-class pin carried over HTTP. A bare JSON number would corrupt the big
// integers; the CAST/typeof + hex contract keeps them exact.
func TestD1Capture_FidelityMatrixOverHTTP(t *testing.T) {
	tbl := fidelityTable()
	m := &mockD1{
		exists:       true,
		fingerprints: [][2]string{{"t", columnFingerprint(nonGeneratedColumnNames(tbl))}},
		rows: []clRow{
			{
				id: 1, op: "I", capturedAt: "2023-01-15 12:30:45.000",
				after: map[string]any{
					"id":  cell("integer", "1"),
					"big": cell("integer", "9007199254740993"), // 2^53 + 1, EXACT
					"flt": cell("real", "0.1"),
					"txt": cell("text", "héllo→世界"),
					"blb": cell("blob", "DEADBEEF"),
					"num": cell("integer", "9007199254740993"), // NUMERIC, integer storage
				},
			},
			{
				id: 2, op: "I", capturedAt: "2023-01-15 12:30:46.000",
				after: map[string]any{
					"id":  cell("integer", "2"),
					"big": cell("integer", "9223372036854775807"), // max int64
					"flt": cell("real", "-2.5"),
					"txt": cell("text", ""),
					"blb": cell("blob", "000102"),
					"num": cell("real", "123.456"), // NUMERIC, real storage → decimal string
				},
			},
			{
				id: 3, op: "I", capturedAt: "2023-01-15 12:30:47.000",
				after: map[string]any{
					"id":  cell("integer", "3"),
					"big": cell("null", nil),
					"flt": cell("null", nil),
					"txt": cell("null", nil),
					"blb": cell("null", nil),
					"num": cell("null", nil),
				},
			},
		},
	}
	conn := startMockD1(t, m)
	b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})

	r, err := openCDCReaderBackend(bg(), b)
	if err != nil {
		t.Fatalf("openCDCReaderBackend: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	changes := collect(t, r, pos0(t), 3)
	if len(changes) != 3 {
		t.Fatalf("got %d changes; want 3", len(changes))
	}

	row1 := mustInsert(t, changes[0]).Row
	assertEq(t, "id1.big", row1["big"], bigBeyond2p53)
	assertEq(t, "id1.flt", row1["flt"], 0.1)
	assertEq(t, "id1.txt", row1["txt"], "héllo→世界")
	assertBytes(t, "id1.blb", row1["blb"], []byte{0xde, 0xad, 0xbe, 0xef})
	assertEq(t, "id1.num", row1["num"], "9007199254740993") // exact decimal string

	row2 := mustInsert(t, changes[1]).Row
	assertEq(t, "id2.big", row2["big"], maxInt64)
	assertEq(t, "id2.flt", row2["flt"], -2.5)
	assertEq(t, "id2.txt", row2["txt"], "")
	assertBytes(t, "id2.blb", row2["blb"], []byte{0x00, 0x01, 0x02})
	assertEq(t, "id2.num", row2["num"], "123.456")

	row3 := mustInsert(t, changes[2]).Row
	for _, c := range []string{"big", "flt", "txt", "blb", "num"} {
		if v := row3[c]; v != nil {
			t.Errorf("id3.%s = %#v; want nil (NULL faithful)", c, v)
		}
	}
}

// TestD1Capture_WatermarkBoundAsString pins (b)+(c): the resume watermark is
// sent as a STRING param (so a > 2^53 bound is never JS-rounded), the poll SQL
// is the CAST(id AS TEXT) SELECT, and resuming from last_id=2 emits ONLY ids 3,4.
func TestD1Capture_WatermarkBoundAsString(t *testing.T) {
	tbl := &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{}}, {Name: "n", Type: ir.Integer{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	var rows []clRow
	for i := int64(1); i <= 4; i++ {
		rows = append(rows, clRow{
			id: i, op: "I", capturedAt: "2023-01-15 12:30:45.000",
			after: map[string]any{"id": cell("integer", strconv.FormatInt(i, 10)), "n": cell("integer", "0")},
		})
	}
	m := &mockD1{
		exists:       true,
		fingerprints: [][2]string{{"t", columnFingerprint(nonGeneratedColumnNames(tbl))}},
		rows:         rows,
	}
	conn := startMockD1(t, m)
	b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})

	r, err := openCDCReaderBackend(bg(), b)
	if err != nil {
		t.Fatalf("openCDCReaderBackend: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	resume, err := encodePos(sqliteTriggerPos{LastID: 2})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}
	changes := collect(t, r, resume, 2)
	if len(changes) != 2 {
		t.Fatalf("got %d changes; want 2 (ids 3,4 only)", len(changes))
	}
	for i, ch := range changes {
		assertEq(t, "resume.id", mustInsert(t, ch).Row["id"], int64(i+3))
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pollSQL) == 0 {
		t.Fatal("no poll request recorded")
	}
	if !strings.Contains(m.pollSQL[0], `CAST(id AS TEXT) AS id`) ||
		!strings.Contains(m.pollSQL[0], `WHERE id > ?`) ||
		!strings.Contains(m.pollSQL[0], "LIMIT ") {
		t.Errorf("poll SQL not the expected CAST/keyset SELECT: %q", m.pollSQL[0])
	}
	if m.pollArgs[0] != "2" {
		t.Errorf("first poll watermark param = %q; want \"2\" (string-bound)", m.pollArgs[0])
	}
	// The raw body must carry the watermark as a JSON STRING in params, never a
	// number (the ADR-0132 discipline that survives a > 2^53 bound).
	foundStringParam := false
	for _, body := range m.bodies {
		if strings.Contains(body, `"params":["2"]`) {
			foundStringParam = true
		}
	}
	if !foundStringParam {
		t.Errorf("no poll request bound the watermark as a string param (\"params\":[\"2\"]); bodies=%v", m.bodies)
	}
}

// TestD1Capture_RefusesSchemaDrift pins (d): when the live schema's column set
// differs from the captured fingerprint (an un-re-setup ADD COLUMN, whose new
// values a stale trigger would SILENTLY drop), the reader refuses loudly at open.
func TestD1Capture_RefusesSchemaDrift(t *testing.T) {
	live := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "v", Type: ir.Text{}},
			{Name: "extra", Type: ir.Text{}}, // added after setup
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	// The fingerprint recorded at setup was the OLD column set {id, v}.
	captured := columnFingerprint([]string{"id", "v"})
	m := &mockD1{exists: true, fingerprints: [][2]string{{"t", captured}}}
	conn := startMockD1(t, m)
	b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{live}})

	_, err := openCDCReaderBackend(bg(), b)
	if err == nil {
		t.Fatal("openCDCReaderBackend should refuse on schema drift (an ADD COLUMN would silently drop)")
	}
	if !strings.Contains(err.Error(), "drift") || !strings.Contains(err.Error(), "trigger setup") {
		t.Errorf("drift refusal should name the drift + recovery action; got: %v", err)
	}
}

// TestD1Capture_RefusesAbsentChangeLog pins the "you forgot `trigger setup`"
// refusal, naming the d1-trigger driver in the recovery command.
func TestD1Capture_RefusesAbsentChangeLog(t *testing.T) {
	tbl := fidelityTable()
	m := &mockD1{exists: false}
	conn := startMockD1(t, m)
	b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})

	_, err := openCDCReaderBackend(bg(), b)
	if err == nil {
		t.Fatal("openCDCReaderBackend should refuse when the change-log is absent")
	}
	if !strings.Contains(err.Error(), "--source-driver d1-trigger") {
		t.Errorf("absent-change-log refusal should name the d1-trigger recovery command; got: %v", err)
	}
}

// TestD1Open_RefusesMissingToken pins (e): the env-only token / account refusals
// fire loudly at open (before any HTTP request), through the real entry points.
func TestD1Open_RefusesMissingToken(t *testing.T) {
	t.Setenv("CLOUDFLARE_API_TOKEN", "")  // unset → refuse
	t.Setenv("CLOUDFLARE_ACCOUNT_ID", "") // account from the DSN below

	if _, err := OpenD1CDCReader(bg(), "d1://acct/db"); err == nil ||
		!strings.Contains(err.Error(), "CLOUDFLARE_API_TOKEN") {
		t.Errorf("missing token should refuse loudly naming CLOUDFLARE_API_TOKEN; got: %v", err)
	}

	// With a token but no account (neither DSN nor env) → account refusal.
	t.Setenv("CLOUDFLARE_API_TOKEN", "tok")
	if _, err := SetupD1(bg(), "d1://justdb", SetupOptions{Tables: []string{"t"}}); err == nil ||
		!strings.Contains(err.Error(), "CLOUDFLARE_ACCOUNT_ID") {
		t.Errorf("missing account should refuse loudly naming CLOUDFLARE_ACCOUNT_ID; got: %v", err)
	}
}

// idNTable is a tiny {id, n} table for the I/U/D + transport-error tests.
func idNTable() *ir.Table {
	return &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{}}, {Name: "n", Type: ir.Integer{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// TestD1Capture_UpdateDeleteOverHTTP pins the I/U/D dispatch + before/after image
// extraction over the D1 transport — the UPDATE (before AND after present) and the
// DELETE (non-null before) shapes the fidelity matrix's all-INSERT rows don't
// exercise. The dispatch is shared/transport-neutral, but the before-image-present
// case is pinned over HTTP here.
func TestD1Capture_UpdateDeleteOverHTTP(t *testing.T) {
	tbl := idNTable()
	m := &mockD1{
		exists:       true,
		fingerprints: [][2]string{{"t", columnFingerprint(nonGeneratedColumnNames(tbl))}},
		rows: []clRow{
			{
				id: 1, op: "I", capturedAt: "2023-01-15 12:30:45.000",
				after: map[string]any{"id": cell("integer", "1"), "n": cell("integer", "10")},
			},
			{
				id: 2, op: "U", capturedAt: "2023-01-15 12:30:46.000",
				before: map[string]any{"id": cell("integer", "1"), "n": cell("integer", "10")},
				after:  map[string]any{"id": cell("integer", "1"), "n": cell("integer", "99")},
			},
			{
				id: 3, op: "D", capturedAt: "2023-01-15 12:30:47.000",
				before: map[string]any{"id": cell("integer", "1"), "n": cell("integer", "99")},
			},
		},
	}
	conn := startMockD1(t, m)
	b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})

	r, err := openCDCReaderBackend(bg(), b)
	if err != nil {
		t.Fatalf("openCDCReaderBackend: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	changes := collect(t, r, pos0(t), 3)
	if len(changes) != 3 {
		t.Fatalf("got %d changes; want 3 (I, U, D)", len(changes))
	}
	mustInsert(t, changes[0])

	upd, ok := changes[1].(ir.Update)
	if !ok {
		t.Fatalf("change[1] is %T; want ir.Update", changes[1])
	}
	assertEq(t, "upd.Before.n", upd.Before["n"], int64(10))
	assertEq(t, "upd.After.n", upd.After["n"], int64(99))

	del, ok := changes[2].(ir.Delete)
	if !ok {
		t.Fatalf("change[2] is %T; want ir.Delete", changes[2])
	}
	assertEq(t, "del.Before.id", del.Before["id"], int64(1))
	assertEq(t, "del.Before.n", del.Before["n"], int64(99))
}

// TestD1Capture_PollTransportErrorIsLoud pins the loud-failure contract over the
// D1 transport: a transport failure mid-poll (HTTP 500) surfaces via Err() with
// the channel closing — NOT a silent empty batch that the streamer would read as
// "no changes". (The local path's poll error is the same shape; this pins the
// HTTP transport's error propagation.)
func TestD1Capture_PollTransportErrorIsLoud(t *testing.T) {
	tbl := idNTable()
	m := &mockD1{
		exists:        true,
		fingerprints:  [][2]string{{"t", columnFingerprint(nonGeneratedColumnNames(tbl))}},
		pollHTTPError: true,
	}
	conn := startMockD1(t, m)
	b := d1TestBackend(conn, &ir.Schema{Tables: []*ir.Table{tbl}})

	r, err := openCDCReaderBackend(bg(), b)
	if err != nil {
		t.Fatalf("openCDCReaderBackend: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	ch, err := r.StreamChanges(bg(), pos0(t))
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	n := 0
	for range ch { // a failed poll emits NO events and closes the channel
		n++
	}
	if n != 0 {
		t.Errorf("got %d events on a failed poll; want 0 (loud, not a silent batch)", n)
	}
	errer, ok := r.(interface{ Err() error })
	if !ok {
		t.Fatal("reader does not expose Err()")
	}
	gotErr := errer.Err()
	if gotErr == nil || !strings.Contains(gotErr.Error(), "poll") {
		t.Errorf("transport error mid-poll must surface via Err() naming the poll; got: %v", gotErr)
	}
}
