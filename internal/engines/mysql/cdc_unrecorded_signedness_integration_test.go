//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MARIADB-NOLOG-UNSIGNED against real servers: every numeric family the
// binlog carries, at the values that tell signed from unsigned, decoded by
// the binlog reader under each binlog_row_metadata a server can write.
//
// The independent expected value is the literal each statement wrote, as
// the value contract renders it (docs/value-types.md) — never a decode by
// sluice. The PRIMARY KEY is itself a BIGINT UNSIGNED above the signed
// maximum, so the UPDATE before-image and the DELETE key (which the applier
// matches the target row by) are graded as well as the values.
//
// Two reader positions: AT the head (the stream is open before the rows are
// written, a live sync keeping up) and BEHIND it (the rows are written first
// and the stream starts from a position before them — a lagging or resumed
// sync, a backup incremental window, the CDC phase after a cold copy). The
// pipeline lanes are pinned end to end in internal/pipeline.

package mysql

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// signColumn is one column of the signedness matrix and the literal each of
// the three rows writes into it.
type signColumn struct {
	def  string
	lits [3]string
	want [3]any
}

// signRowIDs are the three rows' keys: the unsigned maximum, the signed
// maximum plus one, and one.
var signRowIDs = [3]string{"18446744073709551615", "9223372036854775808", "1"}

// signednessColumns is the family matrix. YEAR leads, ahead of every
// integer, so a signedness bitmap that counted a column the server does not
// (or skipped one it does) would shift every integer behind it.
//
// Row 0 holds each unsigned type's maximum, row 1 its signed maximum plus
// one (the smallest value a signed decode turns negative), row 2 its signed
// maximum (the largest a signed decode keeps). The signed controls hold
// their minimum, -1 and their maximum.
func signednessColumns() []signColumn {
	ones := strings.Repeat("1", 64)
	u := func(def string, max, half uint64) signColumn {
		return signColumn{
			def:  def,
			lits: [3]string{fmt.Sprint(max), fmt.Sprint(half), fmt.Sprint(half - 1)},
			want: [3]any{max, half, half - 1},
		}
	}
	s := func(def string, minV, maxV int64) signColumn {
		return signColumn{
			def:  def,
			lits: [3]string{fmt.Sprint(minV), "-1", fmt.Sprint(maxV)},
			want: [3]any{minV, int64(-1), maxV},
		}
	}
	return []signColumn{
		{def: "c_year YEAR", lits: [3]string{"2155", "1901", "2000"}, want: [3]any{int64(2155), int64(1901), int64(2000)}},
		u("c_tinyint_u TINYINT UNSIGNED", math.MaxUint8, 1<<7),
		s("c_tinyint TINYINT", math.MinInt8, math.MaxInt8),
		u("c_tinyint1_u TINYINT(1) UNSIGNED", math.MaxUint8, 1<<7),
		u("c_smallint_u SMALLINT UNSIGNED", math.MaxUint16, 1<<15),
		s("c_smallint SMALLINT", math.MinInt16, math.MaxInt16),
		u("c_mediumint_u MEDIUMINT UNSIGNED", 1<<24-1, 1<<23),
		s("c_mediumint MEDIUMINT", -1<<23, 1<<23-1),
		u("c_int_u INT UNSIGNED", math.MaxUint32, 1<<31),
		s("c_int INT", math.MinInt32, math.MaxInt32),
		u("c_bigint_u BIGINT UNSIGNED", math.MaxUint64, 1<<63),
		s("c_bigint BIGINT", math.MinInt64, math.MaxInt64),
		{
			def:  "c_decimal_u DECIMAL(20,0) UNSIGNED",
			lits: [3]string{"99999999999999999999", "18446744073709551616", "0"},
			want: [3]any{"99999999999999999999", "18446744073709551616", "0"},
		},
		{
			def:  "c_decimal_fu DECIMAL(30,10) UNSIGNED",
			lits: [3]string{"99999999999999999999.9999999999", "0.0000000001", "1.5000000000"},
			want: [3]any{"99999999999999999999.9999999999", "0.0000000001", "1.5000000000"},
		},
		{def: "c_float_u FLOAT UNSIGNED", lits: [3]string{"1.5", "0", "16777216"}, want: [3]any{1.5, 0.0, 16777216.0}},
		{def: "c_double_u DOUBLE UNSIGNED", lits: [3]string{"1.7976931348623157e308", "0", "0.5"}, want: [3]any{math.MaxFloat64, 0.0, 0.5}},
		{def: "c_bit64 BIT(64)", lits: [3]string{"b'" + ones + "'", "b'1" + strings.Repeat("0", 63) + "'", "b'0'"}, want: [3]any{ones, "1" + strings.Repeat("0", 63), strings.Repeat("0", 64)}},
		{def: "c_bit8 BIT(8)", lits: [3]string{"b'11111111'", "b'10000000'", "b'01111111'"}, want: [3]any{"11111111", "10000000", "01111111"}},
		u("c_int_u_tail INT UNSIGNED", math.MaxUint32, 1<<31),
	}
}

// signednessMatrix is one server and the binlog_row_metadata values to run
// on it.
type signednessServer struct {
	name     string
	flavor   Flavor
	boot     func(t *testing.T) (string, func())
	metadata []string
}

func signednessServers() []signednessServer {
	mysqlBoot := func(image string) func(t *testing.T) (string, func()) {
		return func(t *testing.T) (string, func()) {
			return startMySQLM2PreflightImage(t, image, "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
		}
	}
	mariaBoot := func(image string) func(t *testing.T) (string, func()) {
		return func(t *testing.T) (string, func()) { return newMariaDBDedicatedForCDC(t, image) }
	}
	return []signednessServer{
		{"mariadb-11.4", FlavorMariaDB, mariaBoot(mariadb114Image), []string{"NO_LOG", "MINIMAL", "FULL"}},
		{"mariadb-10.11", FlavorMariaDB, mariaBoot(mariadb1011Image), []string{"NO_LOG", "MINIMAL", "FULL"}},
		{"mysql-8.0", FlavorVanilla, mysqlBoot(sharedMySQLImage), []string{"MINIMAL", "FULL"}},
		{"mysql-8.4", FlavorVanilla, mysqlBoot("mysql:8.4"), []string{"MINIMAL", "FULL"}},
	}
}

// TestCDCSignedness_EveryNumericFamily is the matrix: server ×
// binlog_row_metadata × reader position, every family × the three
// discriminating values, through INSERT, UPDATE (before-image key and
// after-image) and DELETE (key).
func TestCDCSignedness_EveryNumericFamily(t *testing.T) {
	cols := signednessColumns()
	for _, srv := range signednessServers() {
		t.Run(srv.name, func(t *testing.T) {
			base, cleanup := srv.boot(t)
			defer cleanup()
			logServerVersion(t, base)
			for _, metadata := range srv.metadata {
				setRowMetadata(t, base, metadata)
				for _, behind := range []bool{false, true} {
					where := "at-head"
					if behind {
						where = "behind"
					}
					t.Run(metadata+"/"+where, func(t *testing.T) {
						db := strings.ToLower("sg_" + metadata + "_" + where)
						db = strings.ReplaceAll(db, "-", "_")
						runSignednessCase(t, Engine{Flavor: srv.flavor}, shapeCaseDB(t, base, db), cols, behind, metadata != "NO_LOG")
					})
				}
			}
		})
	}
}

// runSignednessCase writes the three rows, updates row 0 and deletes row 1,
// and grades every change the reader emits for them against the literals.
//
// recordsSignedness is the premise the fix's scope rests on — that this
// server's binlog_row_metadata writes the signedness field — and it is
// checked, not assumed: the reader reports whether it took any column's
// signedness from the shape, which it must do exactly when the server
// records none.
func runSignednessCase(t *testing.T, eng Engine, dsn string, cols []signColumn, behind, recordsSignedness bool) {
	t.Helper()
	names := make([]string, len(cols))
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i], names[i] = c.def, strings.Fields(c.def)[0]
	}
	applyMySQL(t, dsn, "CREATE TABLE sg (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "+strings.Join(defs, ", ")+");")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	from := stream.Position
	_ = stream.Close()

	var history strings.Builder
	for r := range 3 {
		vals := make([]string, len(cols))
		for i, c := range cols {
			vals[i] = c.lits[r]
		}
		fmt.Fprintf(&history, "INSERT INTO sg (id, %s) VALUES (%s, %s);\n", strings.Join(names, ", "), signRowIDs[r], strings.Join(vals, ", "))
	}
	fmt.Fprintf(&history, "UPDATE sg SET c_int = 7 WHERE id = %s;\n", signRowIDs[0])
	fmt.Fprintf(&history, "DELETE FROM sg WHERE id = %s;\n", signRowIDs[1])

	write := func() { applyMySQL(t, dsn, history.String()) }
	if behind {
		write()
		write = func() {}
	}
	changes, fromShape, streamErr := streamSignedness(ctx, t, eng, dsn, from, 5, write)
	if streamErr != nil {
		t.Fatalf("stream error: %v (after %d changes)", streamErr, len(changes))
	}
	if fromShape == recordsSignedness {
		t.Errorf("the reader took signedness from the shape = %v, but this server's binlog_row_metadata records it = %v",
			fromShape, recordsSignedness)
	}
	if len(changes) != 5 {
		t.Fatalf("got %d changes, want 5 (3 inserts, 1 update, 1 delete): %v", len(changes), changes)
	}

	wantRow := func(r int) ir.Row {
		row := ir.Row{"id": mustUint64(signRowIDs[r])}
		for i, c := range cols {
			row[names[i]] = c.want[r]
		}
		return row
	}
	for r := range 3 {
		ins, ok := changes[r].(ir.Insert)
		if !ok {
			t.Fatalf("change %d is %T; want an insert", r, changes[r])
		}
		gradeSignedRow(t, fmt.Sprintf("insert %d", r), ins.Row, wantRow(r))
	}
	upd, ok := changes[3].(ir.Update)
	if !ok {
		t.Fatalf("change 3 is %T; want an update", changes[3])
	}
	if got := upd.Before["id"]; got != mustUint64(signRowIDs[0]) {
		t.Errorf("update before-image key = %#v (%T); written as %s — the applier matches the target row by it", got, got, signRowIDs[0])
	}
	after := wantRow(0)
	after["c_int"] = int64(7)
	gradeSignedRow(t, "update after", upd.After, after)
	del, ok := changes[4].(ir.Delete)
	if !ok {
		t.Fatalf("change 4 is %T; want a delete", changes[4])
	}
	if got := del.Before["id"]; got != mustUint64(signRowIDs[1]) {
		t.Errorf("delete key = %#v (%T); written as %s — the applier matches the target row by it", got, got, signRowIDs[1])
	}
}

// gradeSignedRow compares every column of got with want, naming each
// disagreement.
func gradeSignedRow(t *testing.T, what string, got, want ir.Row) {
	t.Helper()
	for k, w := range want {
		if g := got[k]; !reflect.DeepEqual(g, w) {
			t.Errorf("%s: %s = %#v (%T); want %#v (%T)", what, k, g, g, w, w)
		}
	}
}

func mustUint64(s string) uint64 {
	var v uint64
	if _, err := fmt.Sscan(s, &v); err != nil {
		panic(err)
	}
	return v
}

// streamSignedness opens a reader at from, runs write once the stream is
// open, and returns sg's first want row changes (fewer if the stream ended),
// whether the reader took any of sg's signedness from the shape, and the
// reader's error.
func streamSignedness(ctx context.Context, t *testing.T, eng Engine, dsn string, from ir.Position, want int, write func()) (got []ir.Change, fromShape bool, err error) {
	t.Helper()
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc := rdr.(*CDCReader)
	changes, err := cdc.StreamChanges(ctx, from)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	write()
	deadline := time.After(30 * time.Second)
collect:
	for len(got) < want {
		select {
		case ch, ok := <-changes:
			if !ok {
				break collect
			}
			var table string
			switch v := ch.(type) {
			case ir.Insert:
				table = v.Table
			case ir.Update:
				table = v.Table
			case ir.Delete:
				table = v.Table
			}
			if table == "sg" {
				got = append(got, ch)
			}
		case <-deadline:
			break collect
		}
	}
	err = cdc.Err()
	// Close joins the pump, so the shape cache is read after it stops.
	_ = cdc.Close()
	for _, tbl := range cdc.schemaCache {
		if tbl.Name == "sg" && tbl.signednessFromShape {
			fromShape = true
		}
	}
	return got, fromShape, err
}

// TestCDCSignedness_RetypedAhead_Refuses is the other direction: a column
// whose signedness an in-place DDL changed AHEAD of the reader. The shape the
// reader holds is then not the one the event was written under, so on a
// source that records no signedness it must refuse rather than decode by
// the shape — in both directions, before any row of the table is emitted.
// Under MINIMAL the event's own record disagrees with the shape, and that
// refuses too.
func TestCDCSignedness_RetypedAhead_Refuses(t *testing.T) {
	cases := []struct {
		name, create, history string
	}{
		{
			// The row written UNSIGNED at the maximum: decoded by the
			// now-signed shape it would read -1.
			name:    "unsigned-to-signed",
			create:  "CREATE TABLE sg (id INT NOT NULL PRIMARY KEY, u INT UNSIGNED);",
			history: "INSERT INTO sg VALUES (1, 4294967295); DELETE FROM sg WHERE id = 1; ALTER TABLE sg MODIFY u INT;",
		},
		{
			// The row written SIGNED at -1: decoded by the now-unsigned
			// shape it would read 4294967295.
			name:    "signed-to-unsigned",
			create:  "CREATE TABLE sg (id INT NOT NULL PRIMARY KEY, u INT);",
			history: "INSERT INTO sg VALUES (1, -1); DELETE FROM sg WHERE id = 1; ALTER TABLE sg MODIFY u INT UNSIGNED;",
		},
	}
	servers := []struct {
		name     string
		flavor   Flavor
		boot     func(t *testing.T) (string, func())
		metadata string
	}{
		{"mariadb-11.4", FlavorMariaDB, func(t *testing.T) (string, func()) { return newMariaDBDedicatedForCDC(t, mariadb114Image) }, "NO_LOG"},
		{"mysql-8.0", FlavorVanilla, func(t *testing.T) (string, func()) {
			return startMySQLM2PreflightImage(t, sharedMySQLImage, "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
		}, "MINIMAL"},
	}
	for _, srv := range servers {
		t.Run(srv.name+"/"+srv.metadata, func(t *testing.T) {
			base, cleanup := srv.boot(t)
			defer cleanup()
			setRowMetadata(t, base, srv.metadata)
			eng := Engine{Flavor: srv.flavor}
			for i, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					dsn := shapeCaseDB(t, base, fmt.Sprintf("sg_retyped_%d", i))
					applyMySQL(t, dsn, c.create)
					ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
					defer cancel()
					stream, err := eng.OpenSnapshotStream(ctx, dsn)
					if err != nil {
						t.Fatalf("OpenSnapshotStream: %v", err)
					}
					from := stream.Position
					_ = stream.Close()
					applyMySQL(t, dsn, c.history)
					got, _, streamErr := streamSignedness(ctx, t, eng, dsn, from, 5, func() {})
					if len(got) != 0 {
						t.Errorf("emitted %d change(s) before refusing: %v", len(got), got)
					}
					if ce, ok := sluicecode.FromError(streamErr); !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
						t.Fatalf("stream error = %v; want %s", streamErr, sluicecode.CodeCDCSchemaReplayMismatch)
					}
				})
			}
		})
	}
}

// TestCDCSignedness_AtHeadFlip_MariaDBNoLog: a live reader at the head on
// MariaDB NO_LOG crosses two in-place signedness changes — SIGNED to UNSIGNED,
// then back, the second issued from a foreign default database — and every
// row decodes by the signedness it was written under. Each schema change
// clears the shape cache, so the definition the reader takes signedness from
// must be the one re-read after it, never the one before. The statements are
// spaced so the reader decodes each at the head, where the shape is proven.
func TestCDCSignedness_AtHeadFlip_MariaDBNoLog(t *testing.T) {
	base, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	setRowMetadata(t, base, "NO_LOG")
	eng := Engine{Flavor: FlavorMariaDB}
	dsn := shapeCaseDB(t, base, "sg_flip")
	_ = shapeCaseDB(t, base, "sg_other")
	applyMySQL(t, dsn, "CREATE TABLE sg (id INT NOT NULL PRIMARY KEY, u INT, m MEDIUMINT);")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	from := stream.Position
	_ = stream.Close()

	write := func() {
		for _, s := range []string{
			"INSERT INTO sg VALUES (1, -1, -8388608);",
			"DELETE FROM sg WHERE id = 1;",
			"ALTER TABLE sg MODIFY u INT UNSIGNED, MODIFY m MEDIUMINT UNSIGNED;",
			"INSERT INTO sg VALUES (2, 4294967295, 16777215);",
			"UPDATE sg SET u = 2147483648, m = 8388608 WHERE id = 2;",
			"DELETE FROM sg WHERE id = 2;",
			"USE sg_other; ALTER TABLE sg_flip.sg MODIFY u INT, MODIFY m MEDIUMINT;",
			"INSERT INTO sg_flip.sg VALUES (3, -5, -1);",
		} {
			applyMySQL(t, dsn, s)
			time.Sleep(700 * time.Millisecond)
		}
	}
	got, _, streamErr := streamSignedness(ctx, t, eng, dsn, from, 6, write)
	if streamErr != nil {
		t.Fatalf("stream error: %v (after %d changes)", streamErr, len(got))
	}
	if len(got) != 6 {
		t.Fatalf("got %d changes, want 6: %v", len(got), got)
	}
	// The literals written; a DELETE's before-image is narrowed to the key.
	want := []struct {
		op  string
		row ir.Row
	}{
		{"insert", ir.Row{"id": int64(1), "u": int64(-1), "m": int64(-8388608)}},
		{"delete", ir.Row{"id": int64(1)}},
		{"insert", ir.Row{"id": int64(2), "u": uint64(4294967295), "m": uint64(16777215)}},
		{"update", ir.Row{"id": int64(2), "u": uint64(2147483648), "m": uint64(8388608)}},
		{"delete", ir.Row{"id": int64(2)}},
		{"insert", ir.Row{"id": int64(3), "u": int64(-5), "m": int64(-1)}},
	}
	for i, w := range want {
		var op string
		var row ir.Row
		switch v := got[i].(type) {
		case ir.Insert:
			op, row = "insert", v.Row
		case ir.Update:
			op, row = "update", v.After
		case ir.Delete:
			op, row = "delete", v.Before
		}
		if op != w.op || !reflect.DeepEqual(row, w.row) {
			t.Errorf("change %d: %s %#v; want %s %#v", i, op, row, w.op, w.row)
		}
	}
}
