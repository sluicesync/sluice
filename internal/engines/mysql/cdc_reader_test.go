// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCDCReader_DatabaseInScope pins the ADR-0074 Phase 1b reader-scope
// class (part A): the single drop-decision point the dispatch paths
// consult. In single-database mode (no scope set) it must reduce
// EXACTLY to `database == r.schema` — byte-identical back-compat. In
// multi-database mode it must delegate to the selected-set predicate
// (wider allow-set, same drop mechanism).
func TestCDCReader_DatabaseInScope(t *testing.T) {
	t.Run("single-database mode is byte-identical to database==schema", func(t *testing.T) {
		r := &CDCReader{schema: "app"}
		if !r.databaseInScope("app") {
			t.Error("bound database must be in scope")
		}
		for _, other := range []string{"other", "", "App", "mysql"} {
			if r.databaseInScope(other) {
				t.Errorf("database %q must be dropped in single-database mode (only %q in scope)", other, "app")
			}
		}
	})

	t.Run("multi-database mode delegates to the selected-set predicate", func(t *testing.T) {
		selected := map[string]struct{}{"app_db": {}, "shared_db": {}}
		r := &CDCReader{schema: "app_db"}
		r.SetCDCDatabaseScope(func(db string) bool {
			_, ok := selected[db]
			return ok
		})
		for _, in := range []string{"app_db", "shared_db"} {
			if !r.databaseInScope(in) {
				t.Errorf("selected database %q must be in scope", in)
			}
		}
		// A database OUTSIDE the selected set is dropped — even though the
		// server-wide binlog carries its events.
		for _, out := range []string{"other_db", "mysql", ""} {
			if r.databaseInScope(out) {
				t.Errorf("out-of-scope database %q must be dropped", out)
			}
		}
	})

	t.Run("nil predicate is a no-op (single-database mode preserved)", func(t *testing.T) {
		r := &CDCReader{schema: "app"}
		r.SetCDCDatabaseScope(nil)
		if r.cdcDBInScope != nil {
			t.Fatal("nil predicate must not engage multi-database mode")
		}
		if r.databaseInScope("other") {
			t.Error("after a nil SetCDCDatabaseScope the reader must stay single-database")
		}
	})
}

func TestEncodeDecodeBinlogPos(t *testing.T) {
	cases := []struct {
		name string
		pos  binlogPos
	}{
		{
			"file_pos",
			binlogPos{Mode: positionModeFilePos, File: "mysql-bin.000123", Pos: 4567},
		},
		{
			"gtid",
			binlogPos{Mode: positionModeGTID, GTIDSet: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000"},
		},
		{
			"file_pos zero offset",
			binlogPos{Mode: positionModeFilePos, File: "binlog.000001", Pos: 0},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			encoded, err := encodeBinlogPos(c.pos)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if encoded.Engine != engineNameMySQL {
				t.Errorf("Engine = %q; want %q", encoded.Engine, engineNameMySQL)
			}
			got, ok, err := decodeBinlogPos(encoded)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !ok {
				t.Fatalf("decode: ok=false; expected a valid position")
			}
			if !reflect.DeepEqual(got, c.pos) {
				t.Errorf("round-trip\n got = %#v\nwant = %#v", got, c.pos)
			}
		})
	}
}

func TestEncodeBinlogPosRejectsInvalidMode(t *testing.T) {
	_, err := encodeBinlogPos(binlogPos{Mode: "lsn", File: "x", Pos: 1})
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

// TestEncodeDecodeBinlogPosServerUUID pins that the Track-1c
// node-replace floor's instance binding survives the position
// encode/decode round-trip — if ServerUUID were dropped on the wire
// the resume-time identity check would always see an empty persisted
// uuid and silently degrade to the old filename-only behaviour.
//
// **ADR-0049 DP-2 invariant (Chunk E regression-pin):** ServerUUID
// is the position-token field the cdc_reader GTID-position-loss /
// node-replace loud-refuse hinges on; if a future schema-history
// change altered the ir.Position serialization in a way that
// dropped this field, the resolve() floor would silently degrade.
// Keeping this round-trip pin green guards the position-loss
// loud-floor that ADR-0049's compaction-floor refuse composes with.
func TestEncodeDecodeBinlogPosServerUUID(t *testing.T) {
	in := binlogPos{
		Mode:       positionModeFilePos,
		File:       "mysql-bin.000003",
		Pos:        3389,
		ServerUUID: "31f7e90d-5234-11f1-8bd3-c65cb9b6c94f",
	}
	enc, err := encodeBinlogPos(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, ok, err := decodeBinlogPos(enc)
	if err != nil || !ok {
		t.Fatalf("decode: ok=%v err=%v", ok, err)
	}
	if got.ServerUUID != in.ServerUUID {
		t.Errorf("ServerUUID round-trip: got %q want %q", got.ServerUUID, in.ServerUUID)
	}
}

// TestVerifySourceInstanceIdentity is the unit-level pin for the
// Track-1c node-replace loud-failure floor. The integration test
// (TestStreamer_MySQL_FreshInstanceNodeReplaceFallsThroughToColdStart)
// proves it end-to-end against two real instances; this pins the
// decision table cheaply so a regression is caught without Docker.
//
// **ADR-0049 DP-2 invariant (Chunk E regression-pin):** the
// schema-history compaction floor must COMPOSE with, not bypass,
// this pre-existing loud-refuse on identity-changed sources.
// Resume from a position whose @@server_uuid differs from the
// current source is ir.ErrPositionInvalid here BEFORE ADR-0049
// resolveSchemaVersion is consulted — a new instance carries no
// historical schema versions for our anchors, so the schema-history
// floor would also refuse, but this floor's specificity ("instance
// replaced, not just compacted past") gives the operator the
// correct ADR-0022 cold-start signal.
func TestVerifySourceInstanceIdentity(t *testing.T) {
	cases := []struct {
		name             string
		persisted, cur   string
		wantPositionLoss bool
	}{
		{"same instance — resumable", "uuid-A", "uuid-A", false},
		{"instance replaced — loud refuse", "uuid-A", "uuid-B", true},
		{"persisted empty (pre-field / degraded) — skip check", "", "uuid-B", false},
		{"current empty (lookup failed now) — degrade not refuse", "uuid-A", "", false},
		{"both empty — skip check", "", "", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := verifySourceInstanceIdentity(context.Background(), c.persisted, c.cur)
			if c.wantPositionLoss {
				if err == nil {
					t.Fatal("expected an error (loud refuse) on instance mismatch")
				}
				if !errors.Is(err, ir.ErrPositionInvalid) {
					t.Errorf("error must wrap ir.ErrPositionInvalid (the streamer's "+
						"ADR-0022 fall-through trigger); got %v", err)
				}
			} else if err != nil {
				t.Errorf("expected nil (resumable / degraded-skip); got %v", err)
			}
		})
	}
}

func TestDecodeBinlogPosFromNowSentinel(t *testing.T) {
	_, ok, err := decodeBinlogPos(ir.Position{})
	if err != nil {
		t.Fatalf("zero position should not error: %v", err)
	}
	if ok {
		t.Errorf("zero position should report ok=false (from-now sentinel)")
	}
}

func TestDecodeBinlogPosErrors(t *testing.T) {
	cases := []struct {
		name string
		in   ir.Position
	}{
		{"wrong engine", ir.Position{Engine: "postgres", Token: `{"mode":"file_pos"}`}},
		{"empty token with non-empty engine", ir.Position{Engine: "mysql", Token: ""}},
		{"malformed json", ir.Position{Engine: "mysql", Token: "not json"}},
		{"unknown mode", ir.Position{Engine: "mysql", Token: `{"mode":"lsn"}`}},
		{"gtid mode missing set", ir.Position{Engine: "mysql", Token: `{"mode":"gtid"}`}},
		{"file_pos mode missing file", ir.Position{Engine: "mysql", Token: `{"mode":"file_pos","pos":42}`}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, _, err := decodeBinlogPos(c.in)
			if err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

func TestFormatSIDAsUUID(t *testing.T) {
	cases := []struct {
		name string
		sid  []byte
		want string
		err  bool
	}{
		{
			"canonical sid",
			[]byte{
				0x3e, 0x11, 0xfa, 0x47,
				0x71, 0xca,
				0x11, 0xe1,
				0x9e, 0x33,
				0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62,
			},
			"3e11fa47-71ca-11e1-9e33-c80aa9429562",
			false,
		},
		{
			"wrong length",
			[]byte{0x01, 0x02, 0x03},
			"",
			true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := formatSIDAsUUID(c.sid)
			if c.err {
				if err == nil {
					t.Errorf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}

func TestHostPortFromAddr(t *testing.T) {
	cases := []struct {
		in      string
		host    string
		port    uint16
		wantErr bool
	}{
		{"127.0.0.1:3306", "127.0.0.1", 3306, false},
		{"[::1]:33060", "::1", 33060, false},
		{"db.example.com:3307", "db.example.com", 3307, false},
		{"no-port", "", 0, true},
		{"127.0.0.1:notnum", "", 0, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			host, port, err := hostPortFromAddr(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", c.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != c.host || port != c.port {
				t.Errorf("got (%q, %d); want (%q, %d)", host, port, c.host, c.port)
			}
		})
	}
}

func TestSplitQualified(t *testing.T) {
	cases := []struct {
		in           string
		schema, name string
	}{
		{"db.users", "db", "users"},
		{"public.posts", "public", "posts"},
		{"users", "", "users"},
		{"", "", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			s, n := splitQualified(c.in)
			if s != c.schema || n != c.name {
				t.Errorf("got (%q, %q); want (%q, %q)", s, n, c.schema, c.name)
			}
		})
	}
}

func TestDecodeBinlogRow(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}},
		{Name: "active", Type: ir.Boolean{}},
	}
	raw := []any{int64(7), []byte("alice@example.com"), int64(1)}

	row, err := decodeBinlogRow(raw, cols, "users", nil)
	if err != nil {
		t.Fatalf("decodeBinlogRow: %v", err)
	}
	if got := row["id"]; got != int64(7) {
		t.Errorf("id = %#v; want int64(7)", got)
	}
	if got := row["email"]; got != "alice@example.com" {
		t.Errorf("email = %#v; want alice@example.com", got)
	}
	if got := row["active"]; got != true {
		t.Errorf("active = %#v; want true", got)
	}
}

// TestDecodeBinlogRow_TinyInt1OutOfRangeWarns pins the Vector D CDC-tail
// wiring: a TINYINT(1)/ir.Boolean column carrying a value outside {0,1} on
// the binlog path is still decoded to a bool (per convention) but emits the
// one-time-per-column WARN naming the column + the --type-override remedy.
func TestDecodeBinlogRow_TinyInt1OutOfRangeWarns(t *testing.T) {
	buf := captureSlog(t)
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "active", Type: ir.Boolean{}},
	}
	warner := newBoolRangeWarner()
	// active=2 (out of range) -> still decodes to true, but warns.
	row, err := decodeBinlogRow([]any{int64(1), int64(2)}, cols, "users", warner)
	if err != nil {
		t.Fatalf("decodeBinlogRow: %v", err)
	}
	if row["active"] != true {
		t.Errorf("active = %#v; want true (convention: non-zero -> true)", row["active"])
	}
	// A second out-of-range row must NOT warn again (once per column).
	if _, err := decodeBinlogRow([]any{int64(2), int64(127)}, cols, "users", warner); err != nil {
		t.Fatalf("decodeBinlogRow (2nd): %v", err)
	}
	out := buf.String()
	if got := strings.Count(out, "column=users.active"); got != 1 {
		t.Errorf("users.active warned %d times; want exactly 1\n%s", got, out)
	}
	if !strings.Contains(out, "--type-override users.active=smallint") {
		t.Errorf("WARN missing the --type-override hint:\n%s", out)
	}
	// An in-range bool column never warns.
	buf.Reset()
	if _, err := decodeBinlogRow([]any{int64(3), int64(1)}, cols, "users", newBoolRangeWarner()); err != nil {
		t.Fatalf("decodeBinlogRow (in-range): %v", err)
	}
	if strings.Contains(buf.String(), "users.active") {
		t.Errorf("in-range value warned:\n%s", buf.String())
	}
}

func TestDecodeBinlogRowColumnCountMismatch(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}
	if _, err := decodeBinlogRow([]any{int64(1), int64(2)}, cols, "t", nil); err == nil {
		t.Error("expected error for column count mismatch")
	}
}

func TestGenerateServerIDIsNonZero(t *testing.T) {
	// Cheap sanity: the binlog protocol rejects ID=0, so the helper
	// must never return it.
	for i := 0; i < 50; i++ {
		if id := generateServerID(); id == 0 {
			t.Fatal("generateServerID returned 0")
		}
	}
}

// TestParseTruncateTable exercises the narrow string-prefix parser
// the CDC reader uses to recognise TRUNCATE inside binlog
// QUERY_EVENTs. The parser is deliberately not a SQL parser —
// anything outside the recognised shapes returns ok=false and the
// caller falls through to generic DDL handling (cache invalidation).
func TestParseTruncateTable(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantSchema string
		wantTable  string
		wantOK     bool
	}{
		// Recognised forms
		{"basic TRUNCATE TABLE", "TRUNCATE TABLE foo", "", "foo", true},
		{"lowercase", "truncate table foo", "", "foo", true},
		{"mixed case", "Truncate Table foo", "", "foo", true},
		{"optional TABLE", "TRUNCATE foo", "", "foo", true},
		{"optional TABLE lowercase", "truncate foo", "", "foo", true},
		{"surrounding whitespace", "  TRUNCATE TABLE foo  ", "", "foo", true},
		{"backticks", "TRUNCATE TABLE `foo`", "", "foo", true},
		{"schema.table", "TRUNCATE TABLE db.foo", "db", "foo", true},
		{"backticked schema.table", "TRUNCATE TABLE `db`.`foo`", "db", "foo", true},
		{"backticks one side", "TRUNCATE TABLE db.`foo`", "db", "foo", true},
		{"tab whitespace", "TRUNCATE\tTABLE\tfoo", "", "foo", true},

		// Not TRUNCATE — fall through to generic DDL handling
		{"CREATE", "CREATE TABLE foo", "", "", false},
		{"DROP", "DROP TABLE foo", "", "", false},
		{"ALTER", "ALTER TABLE foo ADD COLUMN bar INT", "", "", false},
		{"BEGIN", "BEGIN", "", "", false},
		{"COMMIT", "COMMIT", "", "", false},
		{"empty", "", "", "", false},
		{"TRUNCATE prefix only", "TRUNCATE", "", "", false},
		{"TRUNCATE TABLE no name", "TRUNCATE TABLE", "", "", false},
		{"TRUNCATE TABLE trailing space", "TRUNCATE TABLE ", "", "", false},

		// Out-of-shape — punt to generic DDL
		{"multi-table", "TRUNCATE foo, bar", "", "", false},
		{"trailing semicolon", "TRUNCATE foo;", "", "", false},
		{"with parens", "TRUNCATE TABLE foo()", "", "", false},
		// "TRUNCATE TABLEFOO" is the optional-TABLE form with TABLEFOO
		// as the table name (a legal MySQL identifier). The parser
		// must NOT mistakenly strip "TABLE" as a keyword without a
		// whitespace separator.
		{"TABLEFOO is a valid table name", "TRUNCATE TABLEFOO", "", "TABLEFOO", true},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotSchema, gotTable, gotOK := parseTruncateTable(c.in)
			if gotOK != c.wantOK {
				t.Errorf("ok = %v; want %v", gotOK, c.wantOK)
			}
			if gotSchema != c.wantSchema {
				t.Errorf("schema = %q; want %q", gotSchema, c.wantSchema)
			}
			if gotTable != c.wantTable {
				t.Errorf("table = %q; want %q", gotTable, c.wantTable)
			}
		})
	}
}

// TestFilterDeleteBefore pins the Bug 88 fix at unit level: the CDC
// reader narrows the Before-image of a DELETE rows-event to its PK
// columns before emit, so the applier's [buildWhereClause] never
// gets to emit `non_pk_col IS NULL` predicates that fail to match
// real target rows whose non-PK columns hold non-null values.
//
// The matrix integration test (cdc_delete_matrix_mysql_integration_test.go)
// pins the end-to-end behaviour; this unit test pins the narrowing
// helper itself so a regression that drops the filter or alters the
// PK lookup shape fails cheaply, without spinning up a testcontainer.
//
// Same name and shape as the PG-side test pinning the equivalent
// helper — the family-dispatched fix-locus matches between engines
// per ADR-0057's Bug-74 family-pin discipline.
func TestFilterDeleteBefore(t *testing.T) {
	cases := []struct {
		name string
		tbl  *tableSchema
		in   ir.Row
		want ir.Row
	}{
		{
			name: "MINIMAL plain-delete narrows nil non-PK columns away",
			tbl: &tableSchema{
				Schema:     "source_db",
				Name:       "widgets",
				PrimaryKey: []string{"id"},
			},
			// What MySQL emits under MINIMAL: PK carries its value,
			// every non-PK column is nil. Without the filter, the
			// applier builds "WHERE `id`=? AND `name` IS NULL AND
			// `payload` IS NULL AND `created_at` IS NULL", which
			// fails to match real rows.
			in: ir.Row{
				"id":         int64(42),
				"name":       nil,
				"payload":    nil,
				"created_at": nil,
			},
			want: ir.Row{"id": int64(42)},
		},
		{
			name: "FULL plain-delete narrows non-PK columns away (correct, just shorter)",
			tbl: &tableSchema{
				Schema:     "source_db",
				Name:       "widgets",
				PrimaryKey: []string{"id"},
			},
			// Under FULL every column is present with its real
			// value. The filter still narrows to PK — same target
			// row, shorter WHERE clause, identical effect.
			in: ir.Row{
				"id":      int64(42),
				"name":    "alice",
				"payload": "hello",
			},
			want: ir.Row{"id": int64(42)},
		},
		{
			name: "NOBLOB with TOAST'd BLOB narrows nil name + absent payload away",
			tbl: &tableSchema{
				Schema:     "source_db",
				Name:       "widgets",
				PrimaryKey: []string{"id"},
			},
			// Under NOBLOB the BLOB column is absent from the map
			// entirely; the non-BLOB non-PK column (name) is present
			// as nil. The filter narrows both away.
			in: ir.Row{
				"id":   int64(42),
				"name": nil,
				// payload absent (NOBLOB drops it from the map).
			},
			want: ir.Row{"id": int64(42)},
		},
		{
			name: "composite PK keeps both PK columns",
			tbl: &tableSchema{
				Schema:     "source_db",
				Name:       "memberships",
				PrimaryKey: []string{"org_id", "user_id"},
			},
			in: ir.Row{
				"org_id":  int64(1),
				"user_id": int64(7),
				"role":    nil, // nil under MINIMAL
			},
			want: ir.Row{"org_id": int64(1), "user_id": int64(7)},
		},
		{
			name: "PK-less table falls back to the full image (no shorter identity exists)",
			tbl: &tableSchema{
				Schema:     "source_db",
				Name:       "events",
				PrimaryKey: nil,
			},
			// With no PK, there's no narrowing possible — return the
			// full Before-image verbatim. Same fallback as PG's
			// filterDeleteBefore on a PK-less REPLICA IDENTITY FULL
			// relation.
			in: ir.Row{
				"event_id": int64(99),
				"payload":  "x",
			},
			want: ir.Row{"event_id": int64(99), "payload": "x"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := filterDeleteBefore(c.tbl, c.in)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("filterDeleteBefore(...) = %#v; want %#v", got, c.want)
			}
		})
	}
}
