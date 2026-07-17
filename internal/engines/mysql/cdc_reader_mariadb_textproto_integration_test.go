//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MariaDB native uuid/inet6/inet4 TEXT-PROTOCOL cold-start convergence pin
// (audit 2026-07-17 A2 — the Bug-74 reviewer-corollary).
//
// The value-fidelity pins in cdc_reader_mariadb_integration_test.go prove
// the CDC binlog decode converges with a driver SELECT — but BOTH oracles
// there read via `SELECT ... WHERE id=?` (a prepared statement = MySQL
// BINARY protocol). The REAL cold-start bulk copy does NOT: the RowReader's
// [buildSelect] issues an arg-less `QueryContext`, which go-sql-driver
// sends as COM_QUERY (the TEXT protocol). Those are two distinct wire paths
// and the netip-divergent inet6 shapes (::1.2.3.4 / ::0.1.0.0 / ::100 /
// ::ffff) are exactly where a one-byte protocol difference would land a
// different cold-start string than the CDC tail — a perpetual spurious
// diff at exit 0 as the CDC UPDATE overwrites the cold-start value on the
// first change. This test carries every family × the divergent shapes
// through the ACTUAL text-protocol RowReader and asserts all three wire
// paths (text cold-start, CDC binlog decode, binary SELECT) render
// BYTE-IDENTICAL, on both LTS lines.
package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestMariaDB_RowReader_TextProtocol_NativeConvergence(t *testing.T) {
	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "mdb_textproto_native")
			execSQLScript(t, dsn, `
				CREATE TABLE nat (
					id  INT PRIMARY KEY,
					u   UUID,
					ip6 INET6,
					ip4 INET4
				) ENGINE=InnoDB;`)

			eng := Engine{Flavor: FlavorMariaDB}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			// Open the CDC stream BEFORE inserting so the same INSERTs are
			// captured on the binlog decode path.
			rdr, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			defer func() {
				if c, ok := rdr.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			changes, err := rdr.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			time.Sleep(300 * time.Millisecond)

			// The family × divergent-shape matrix. Rows 6-9 are the four
			// shapes where mariadbInet6Text DELIBERATELY diverges from Go's
			// net/netip; row 4's uuid carries trailing zeros (binlog-stripped)
			// and row 4's ip6 is trailing-zero (2001:db8::). Rows 2 carries the
			// all-zero shapes (empty-in-binlog). Every shape must render the
			// same on the text-protocol cold read as on the binary/CDC paths.
			applyMySQL(t, dsn, `
				INSERT INTO nat (id, u, ip6, ip4) VALUES
					(1, '01234567-89ab-cdef-8123-456789abcdef', '2001:db8::1',    '192.168.1.10'),
					(2, '00000000-0000-0000-0000-000000000000', '::',            '0.0.0.0'),
					(3, 'ffffffff-ffff-ffff-ffff-ffffffffffff', '::ffff:1.2.3.4', '255.255.255.255'),
					(4, '01234567-89ab-cdef-8100-000000000000', '2001:db8::',     '10.0.0.0'),
					(5, NULL, NULL, NULL),
					(6, '10000000-0000-4000-8000-000000000006', '::1.2.3.4',      '1.2.3.4'),
					(7, '20000000-0000-4000-8000-000000000007', '::0.1.0.0',      '5.6.7.8'),
					(8, '30000000-0000-4000-8000-000000000008', '::100',          '9.10.11.12'),
					(9, '40000000-0000-4000-8000-000000000009', '::ffff',         '13.14.15.16');`)

			// ---- CDC binlog decode path (native decoder) ----
			cdcByID := map[int64]ir.Row{}
			for _, ch := range drainChanges(t, ctx, changes, 9, 30*time.Second) {
				ins, ok := ch.(ir.Insert)
				if !ok {
					t.Fatalf("change = %T; want ir.Insert", ch)
				}
				id, _ := ins.Row["id"].(int64)
				cdcByID[id] = ins.Row
			}
			if len(cdcByID) != 9 {
				t.Fatalf("CDC: captured %d rows; want 9", len(cdcByID))
			}

			// ---- TEXT-protocol cold-start path (the arg-less RowReader) ----
			textByID := readTableTextProtocol(t, ctx, eng, dsn, "nat")
			if len(textByID) != 9 {
				t.Fatalf("text cold-start: read %d rows; want 9", len(textByID))
			}

			// ---- Binary-protocol oracle (SELECT ... WHERE id=?) ----
			binByID := readNativeBinaryProtocol(t, dsn, 9)

			// Expected canonical text for the netip-divergent inet6 shapes —
			// the codec's target. If the text cold-start (or any path) renders
			// one of these even one byte differently, it is a REAL silent-loss
			// finding: the cold-start would perpetually diff the CDC tail.
			divergentIP6 := map[int64]string{
				6: "::1.2.3.4",
				7: "::0.1.0.0",
				8: "::100",
				9: "::ffff",
			}

			for id := int64(1); id <= 9; id++ {
				text, bin := textByID[id], binByID[id]
				cdc := cdcByID[id]
				for _, col := range []string{"u", "ip6", "ip4"} {
					tv := nativeString(t, "text", id, col, text[col])
					cv := nativeString(t, "cdc", id, col, cdc[col])
					bv := bin[col] // sql.NullString rendered to (string, isNull)

					// NULL row (id=5): every path must agree it is NULL.
					if id == 5 {
						if text[col] != nil || cdc[col] != nil || bv.Valid {
							t.Errorf("id=5 %s: text=%#v cdc=%#v bin=%v; want NULL on all paths", col, text[col], cdc[col], bv)
						}
						continue
					}
					if !bv.Valid {
						t.Errorf("id=%d %s: binary oracle NULL but text/cdc non-null", id, col)
						continue
					}
					// The load-bearing assertion: the TEXT-protocol cold-start
					// read must be byte-identical to the binary and CDC reads.
					if tv != bv.String {
						t.Errorf("id=%d %s: TEXT-protocol cold-start = %q; binary SELECT = %q — "+
							"the cold-start and CDC/verify paths DIVERGE by wire protocol (Bug-74 reviewer-corollary)", id, col, tv, bv.String)
					}
					if tv != cv {
						t.Errorf("id=%d %s: TEXT-protocol cold-start = %q; CDC binlog decode = %q — "+
							"cold-start would perpetually diff the CDC tail", id, col, tv, cv)
					}
				}
				if want, ok := divergentIP6[id]; ok {
					if got := textByID[id]["ip6"].(string); got != want {
						t.Errorf("id=%d ip6 (netip-divergent shape): TEXT-protocol cold-start = %q; canonical target = %q — "+
							"MariaDB's text protocol renders this shape differently; a REAL finding", id, got, want)
					}
				}
			}
		})
	}
}

// readTableTextProtocol reads table via the engine's real RowReader (the
// arg-less QueryContext = TEXT/COM_QUERY protocol, exactly the cold-start
// bulk-copy path) and returns the rows keyed by their int id.
func readTableTextProtocol(t *testing.T, ctx context.Context, eng Engine, dsn, tableName string) map[int64]ir.Row {
	t.Helper()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, tableName)
	if table == nil {
		t.Fatalf("table %q not found in schema", tableName)
	}

	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(interface{ Close() error }).Close() }()
	rowCh, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	out := map[int64]ir.Row{}
	for row := range rowCh {
		id, _ := row["id"].(int64)
		out[id] = row
	}
	if r, ok := rr.(*RowReader); ok {
		if err := r.Err(); err != nil {
			t.Fatalf("RowReader.Err: %v", err)
		}
	}
	return out
}

// readNativeBinaryProtocol reads the native columns of rows 1..n via a
// prepared-statement SELECT (the BINARY protocol) — the oracle the text
// cold-start read is compared against.
func readNativeBinaryProtocol(t *testing.T, dsn string, n int) map[int64]map[string]sql.NullString {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	out := map[int64]map[string]sql.NullString{}
	for id := int64(1); id <= int64(n); id++ {
		var u, ip6, ip4 sql.NullString
		if err := db.QueryRowContext(ctx, "SELECT u, ip6, ip4 FROM nat WHERE id = ?", id).Scan(&u, &ip6, &ip4); err != nil {
			t.Fatalf("binary read id=%d: %v", id, err)
		}
		out[id] = map[string]sql.NullString{"u": u, "ip6": ip6, "ip4": ip4}
	}
	return out
}

// nativeString renders a decoded native cell to its string form for
// comparison. A non-nil native uuid/inet value must always decode to a Go
// string on every wire path; anything else is a decode-shape bug.
func nativeString(t *testing.T, path string, id int64, col string, v any) string {
	t.Helper()
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		t.Errorf("id=%d %s (%s path): value = %#v (%T); want a decoded string", id, col, path, v, v)
		return ""
	}
	return s
}
