//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) review F2/F3: the VStream COPY phase of non-UTF-8 columns.
//
// vttablet's rowstreamer (COPY) reads under `set names binary`, so a COPY
// cell carries the STORED bytes in the column's charset — for CHAR/VARCHAR/
// TEXT and, unlike CDC, for ENUM/SET too (MEASURED: latin1 ENUM 'é' arrived
// as 0xE9 in COPY and as UTF-8 C3A9 in CDC, with identical FIELD events).
// Before the fix the COPY ENUM/SET cell was taken as UTF-8: 'é' arrived as
// "\xe9" (loud at a target) and 'Ã©' as 'é' — a DIFFERENT, valid member,
// silently.
//
// # The independent expected value
//
// The source server's own conversion, `HEX(CONVERT(col USING utf8mb4))`,
// per row and column — for ENUM/SET that is the label text — compared with
// what the COPY emitted and then what a CDC insert emitted. Run with a
// single COPY stream and with four concurrent ones (the concurrent path has
// its own pump and decode call).

package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// vstreamCopyCharsetTables is one table per charset family; ENUM/SET labels
// are chosen so neither reading of a cell is ambiguous (see the ambiguity
// test below for the one that is).
var vstreamCopyCharsetTables = []struct{ table, cs, v, label string }{
	{"cp_latin1", "latin1", "E9", "é"},
	{"cp_cp1251", "cp1251", "C0", "А"},
	{"cp_sjis", "sjis", "82A0", "あ"},
	{"cp_utf16", "utf16", "00E9", "é"},
}

// serverRowHexes returns, per id, the server's UTF-8 hex of v, x, e, s.
func serverRowHexes(t *testing.T, db *sql.DB, table string) map[string][4]string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT id, HEX(CONVERT(v USING utf8mb4)), HEX(CONVERT(x USING utf8mb4)),
		HEX(CONVERT(e USING utf8mb4)), HEX(CONVERT(s USING utf8mb4)) FROM %s`, table))
	if err != nil {
		t.Fatalf("%s: server read: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string][4]string{}
	for rows.Next() {
		var id string
		var v, x, e, s sql.NullString
		if err := rows.Scan(&id, &v, &x, &e, &s); err != nil {
			t.Fatal(err)
		}
		out[id] = [4]string{v.String, x.String, e.String, s.String}
	}
	return out
}

// emittedHexes renders an emitted row's v, x, e, s the way serverRowHexes
// does (SET elements comma-joined).
func emittedHexes(r ir.Row) [4]string {
	h := func(v any) string {
		switch x := v.(type) {
		case string:
			return strings.ToUpper(hex.EncodeToString([]byte(x)))
		case []string:
			return strings.ToUpper(hex.EncodeToString([]byte(strings.Join(x, ","))))
		case nil:
			return ""
		}
		return fmt.Sprintf("<%T>", v)
	}
	return [4]string{h(r["v"]), h(r["x"]), h(r["e"]), h(r["s"])}
}

func vstreamCopyCharsetLane(t *testing.T, parallelism int) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var names []string
	for _, c := range vstreamCopyCharsetTables {
		label := strings.ReplaceAll(c.label, "'", "''")
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (id INT NOT NULL PRIMARY KEY,
			v VARCHAR(16) CHARACTER SET %[2]s NULL, x TEXT CHARACTER SET %[2]s NULL,
			e ENUM('%[3]s','b') CHARACTER SET %[2]s NULL, s SET('%[3]s','b') CHARACTER SET %[2]s NULL)`, c.table, c.cs, label))
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`INSERT INTO %s VALUES
			(1, _%[2]s X'%[3]s', CONCAT(_%[2]s X'%[3]s', _%[2]s X'%[3]s'), 1, 3), (2, NULL, NULL, NULL, NULL)`, c.table, c.cs, c.v))
		names = append(names, c.table)
	}
	time.Sleep(3 * time.Second)

	dsn := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_copy_table_parallelism=%d",
		mysqlDSN, grpcEndpoint, parallelism)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	stream, err := Engine{Flavor: FlavorPlanetScale}.OpenSnapshotStreamForTables(ctx, dsn, names)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables: %v", err)
	}
	defer func() { _ = stream.Close() }()

	for _, c := range vstreamCopyCharsetTables {
		tbl := &ir.Table{Name: c.table, Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "v", Type: ir.Varchar{Length: 16}},
			{Name: "x", Type: ir.Text{}},
			{Name: "e", Type: ir.Text{}},
			{Name: "s", Type: ir.Text{}},
		}, PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}}
		ch, err := stream.Rows.ReadRows(ctx, tbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", c.table, err)
		}
		want := serverRowHexes(t, db, c.table)
		got := 0
		for r := range ch {
			got++
			id := fmt.Sprint(r["id"])
			if emittedHexes(r) != want[id] {
				t.Errorf("%s (%s) COPY row %s = %v; server %v (v, x, e, s as UTF-8 hex)", c.table, c.cs, id, emittedHexes(r), want[id])
			}
		}
		if err := stream.Rows.Err(); err != nil {
			t.Fatalf("%s COPY: %v", c.table, err)
		}
		if got != len(want) {
			t.Fatalf("%s: COPY emitted %d rows; the source holds %d", c.table, got, len(want))
		}
	}
	if err := stream.WaitCopyComplete(ctx); err != nil {
		t.Fatalf("WaitCopyComplete: %v", err)
	}

	// The same shapes through CDC, where ENUM/SET arrive as UTF-8 labels.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(time.Second)
	for _, c := range vstreamCopyCharsetTables {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`INSERT INTO %s VALUES (3, _%[2]s X'%[3]s', _%[2]s X'%[3]s', 1, 1)`, c.table, c.cs, c.v))
		want := serverRowHexes(t, db, c.table)["3"]
		got := drainVTTestChanges(t, ctx, changes, 1, 45*time.Second)
		if len(got) != 1 {
			t.Fatalf("%s: CDC insert did not stream (stream error: %v)", c.table, stream.Changes.(interface{ Err() error }).Err())
		}
		ins, _ := got[0].(ir.Insert)
		if emittedHexes(ins.Row) != want {
			t.Errorf("%s (%s) CDC row 3 = %v; server %v", c.table, c.cs, emittedHexes(ins.Row), want)
		}
	}
}

func TestVStream_CopyCharsetDecode_SingleStream(t *testing.T) { vstreamCopyCharsetLane(t, 1) }

func TestVStream_CopyCharsetDecode_FourStreams(t *testing.T) { vstreamCopyCharsetLane(t, 4) }

// TestVStream_CopyCharsetDecode_AmbiguousLabelsRefuse: a latin1 ENUM whose
// labels are each other's mis-encoding ('é' and 'Ã©'). A COPY cell holding
// 'Ã©' is the bytes C3A9 — a member read as latin1 ('Ã©') AND read as UTF-8
// ('é'). Before the fix it silently became 'é'; it must now refuse. The
// unambiguous 'é' (stored E9: not valid UTF-8) copies exactly.
func TestVStream_CopyCharsetDecode_AmbiguousLabelsRefuse(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE amb (id INT NOT NULL PRIMARY KEY, e ENUM('é','Ã©','b') CHARACTER SET latin1 NULL)`)
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO amb VALUES (1, 'é'), (2, 'Ã©')`)
	time.Sleep(3 * time.Second)
	dsn := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stream, err := Engine{Flavor: FlavorPlanetScale}.OpenSnapshotStreamForTables(ctx, dsn, []string{"amb"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	ch, err := stream.Rows.ReadRows(ctx, &ir.Table{Name: "amb", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "e", Type: ir.Text{}},
	}, PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}})
	if err != nil {
		t.Fatal(err)
	}
	for r := range ch {
		if fmt.Sprint(r["id"]) == "2" {
			t.Fatalf("the ambiguous row was emitted as e=%q; want a refusal", r["e"])
		}
		if r["e"] != "é" {
			t.Errorf("row %v e = %q; want é", r["id"], r["e"])
		}
	}
	if err := stream.Rows.Err(); !errors.Is(err, errCharsetNotDecodable) || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("COPY error = %v; want %s naming the ambiguity", err, charsetNotDecodableMarker)
	}
}

// TestVStream_CopyCharsetDecode_EnumIndexZero: MySQL's ENUM index 0 — the
// empty string an invalid value is stored as under a non-strict insert — is
// a legal value of every ENUM, listed or not. A latin1 ENUM holding it must
// COPY and stream through CDC as "", never refuse as "not a member". The
// expected value is the server's own read of the cell (checked below: ” at
// index 0).
func TestVStream_CopyCharsetDecode_EnumIndexZero(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE ez (id INT NOT NULL PRIMARY KEY, e ENUM('é','b') CHARACTER SET latin1 NULL)`)
	applyVTTestSQL(t, mysqlDSN, `INSERT IGNORE INTO ez VALUES (1, 'not-a-label')`)
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO ez VALUES (2, 'é')`)
	var idx int
	var val string
	if err := db.QueryRow(`SELECT e+0, e FROM ez WHERE id = 1`).Scan(&idx, &val); err != nil || idx != 0 || val != "" {
		t.Fatalf("setup: row 1 is (index %d, %q), err %v; want the index-0 empty string", idx, val, err)
	}
	time.Sleep(3 * time.Second)
	dsn := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stream, err := Engine{Flavor: FlavorPlanetScale}.OpenSnapshotStreamForTables(ctx, dsn, []string{"ez"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	ch, err := stream.Rows.ReadRows(ctx, &ir.Table{Name: "ez", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "e", Type: ir.Text{}},
	}, PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"1": "", "2": "é"}
	seen := 0
	for r := range ch {
		seen++
		if id := fmt.Sprint(r["id"]); r["e"] != want[id] {
			t.Errorf("COPY row %s e = %#v; want %q", id, r["e"], want[id])
		}
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("COPY of an ENUM holding index 0: %v; want it copied as \"\"", err)
	}
	if seen != 2 {
		t.Fatalf("COPY emitted %d rows; want 2", seen)
	}
	if err := stream.WaitCopyComplete(ctx); err != nil {
		t.Fatalf("WaitCopyComplete: %v", err)
	}

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(time.Second)
	applyVTTestSQL(t, mysqlDSN, `INSERT IGNORE INTO ez VALUES (3, 'not-a-label')`)
	got := drainVTTestChanges(t, ctx, changes, 1, 45*time.Second)
	if len(got) != 1 {
		t.Fatalf("CDC insert of index 0 did not stream (stream error: %v)", stream.Changes.(interface{ Err() error }).Err())
	}
	if ins, _ := got[0].(ir.Insert); ins.Row["e"] != "" {
		t.Errorf("CDC row 3 e = %#v; want \"\"", ins.Row["e"])
	}
}
