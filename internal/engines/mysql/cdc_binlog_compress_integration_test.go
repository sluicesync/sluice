//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 G8 capture pin: MariaDB log_bin_compress=ON row events are fully
// CAPTURED, family × verb, against a REAL compressing MariaDB.
//
// The Bug-74 matrix this file holds: value families {string (TEXT +
// VARCHAR, multibyte), numeric (BIGINT edge + DECIMAL + DOUBLE),
// temporal (DATETIME(6) + DATE + TIME(3) + TIMESTAMP(6)), binary (BLOB
// + VARBINARY, non-UTF-8 bytes)} × DML verbs {INSERT, UPDATE
// (after-image), DELETE (before-image identity)} × {a ≥min_len payload
// per family where the type can physically carry one (string, binary),
// a pad-forced compressed event for the families that cannot (numeric,
// temporal — a row of numbers can never reach 256 B alone), a
// NULL-per-family row, and a sub-min_len row that pins the size-
// conditional INTERLEAVE (compressed and uncompressed events on the
// same table in one stream)}. Expected values are written literally in
// the test — the independent expected value is the SQL literal the test
// itself inserted, decoded to the exact Go shapes the UNCOMPRESSED
// binlog path is already pinned to produce package-wide.
//
// Anti-vacuity: the test reads SHOW BINLOG EVENTS off the real server
// and REQUIRES all three *_rows_compressed_v1 event types (plus an
// uncompressed write) to actually be present — so a server/tuning
// change that stopped producing compressed events fails the pin rather
// than silently degrading it to a plain-event test.
//
// Version reach, stated: pinned on mariadb:11.4 (the G8 ground-truth
// line). The compressed-event wire format is decoded by go-mysql's
// version-independent event-type dispatch (the same vocabulary on every
// MariaDB line that has log_bin_compress, 10.2+); the per-line LTS
// spread for MariaDB CDC value decode lives in the uncompressed pins
// (flavor_mariadb_integration_test.go, cdc_reader_mariadb_*), which
// share every column-decode path with this one past decompression.
// Uses a DEDICATED container: the test depends on global binlog state
// (--log-bin-compress=ON from boot).

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMariaDB_CDCReader_LogBinCompress_FamilyMatrix is the G8 capture
// pin. See the file comment for the matrix it holds.
func TestMariaDB_CDCReader_LogBinCompress_FamilyMatrix(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, "--log-bin-compress=ON")
	defer cleanup()
	execSQLScript(t, dsn, `
		CREATE TABLE fam (
			id INT           NOT NULL,
			s  TEXT          NULL,
			vc VARCHAR(800)  NULL,
			n  BIGINT        NULL,
			dc DECIMAL(20,6) NULL,
			f  DOUBLE        NULL,
			dt DATETIME(6)   NULL,
			d  DATE          NULL,
			tm TIME(3)       NULL,
			ts TIMESTAMP(6)  NULL DEFAULT NULL,
			b  BLOB          NULL,
			vb VARBINARY(800) NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	eng := Engine{Flavor: FlavorMariaDB}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

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
		t.Fatalf("StreamChanges with @@GLOBAL.log_bin_compress=ON: %v (a compressing source is a "+
			"supported configuration)", err)
	}
	time.Sleep(300 * time.Millisecond)

	// bigS carries a multibyte tail so the compressed decode is pinned
	// through utf8mb4, not just ASCII.
	bigS := strings.Repeat("abc", 200) + "Ωé漢"
	bigB := bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF}, 100) // 600 B, non-UTF-8, embedded NUL
	bigVB := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 40)      // 400 B
	pad := strings.Repeat("p", 300)
	padQ := strings.Repeat("q", 300)

	// INSERT arm — one statement (= one autocommit transaction = one
	// rows event) per row, so each row's event size is its own: rows
	// 1-3 and 5 each exceed log_bin_compress_min_len (256 B default)
	// and MUST arrive via Write_rows_compressed_v1 (asserted below via
	// SHOW BINLOG EVENTS); row 4 stays under it and pins the
	// uncompressed interleave on the same table in the same stream.
	applyMySQL(t, dsn, `
		INSERT INTO fam (id, s, vc, n, dc, f, dt, d, tm, ts, b, vb) VALUES
			(1, '`+bigS+`', 'vc-one', 42, '1.500000', 2.25,
			 '2024-03-01 12:34:56.789012', '2024-02-29', '12:34:56.789', '2024-03-01 12:34:56.123456',
			 x'DEADBEEF', x'0102');
		INSERT INTO fam (id, s, vc, n, dc, f, dt, d, tm, ts, b, vb) VALUES
			(2, 'small-2', 'vc-two', 7, '2.000000', 0.5,
			 '2020-01-02 03:04:05.000006', '2020-01-02', '03:04:05.000', '2020-01-02 03:04:05.000006',
			 UNHEX(REPEAT('DEADBEEF00FF', 100)), UNHEX(REPEAT('0102030405060708090A', 40)));
		INSERT INTO fam (id, s, vc, n, dc, f, dt, d, tm, ts, b, vb) VALUES
			(3, 'small-3', '`+pad+`', 9223372036854775807, '-12345678901234.567890', -1.75,
			 '1999-12-31 23:59:59.999999', '1999-12-31', '23:59:59.999', '2038-01-19 03:14:07.000000',
			 x'00', x'FF');
		INSERT INTO fam (id, s, vc, n, dc, f, dt, d, tm, ts, b, vb) VALUES
			(4, 'tiny', 'vc-four', -1, '0.000001', 1.0,
			 '2024-01-01 00:00:00.000000', '2024-01-01', '00:00:01.000', NULL,
			 x'AB', x'CD');
		INSERT INTO fam (id, s, vc, n, dc, f, dt, d, tm, ts, b, vb) VALUES
			(5, NULL, '`+padQ+`', NULL, NULL, NULL,
			 NULL, NULL, NULL, NULL,
			 NULL, NULL);`)

	inserts := drainChanges(t, ctx, changes, 5, 45*time.Second)
	if len(inserts) != 5 {
		t.Fatalf("got %d inserts; want 5 (stream error: %v)", len(inserts), rdr.(*CDCReader).Err())
	}
	wantRows := map[int64]map[string]any{
		1: {
			"s": bigS, "vc": "vc-one", "n": int64(42), "dc": "1.500000", "f": 2.25,
			"dt": time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC),
			"d":  time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			"tm": "12:34:56.789",
			"ts": time.Date(2024, 3, 1, 12, 34, 56, 123456000, time.UTC),
			"b":  []byte{0xDE, 0xAD, 0xBE, 0xEF}, "vb": []byte{0x01, 0x02},
		},
		2: {
			"s": "small-2", "vc": "vc-two", "n": int64(7), "dc": "2.000000", "f": 0.5,
			"dt": time.Date(2020, 1, 2, 3, 4, 5, 6000, time.UTC),
			"d":  time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
			// TIME2's decoder drops an all-zero fractional part.
			"tm": "03:04:05",
			"ts": time.Date(2020, 1, 2, 3, 4, 5, 6000, time.UTC),
			"b":  bigB, "vb": bigVB,
		},
		3: {
			"s": "small-3", "vc": pad, "n": int64(9223372036854775807), "dc": "-12345678901234.567890", "f": -1.75,
			"dt": time.Date(1999, 12, 31, 23, 59, 59, 999999000, time.UTC),
			"d":  time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC),
			"tm": "23:59:59.999",
			"ts": time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC),
			"b":  []byte{0x00}, "vb": []byte{0xFF},
		},
		4: {
			"s": "tiny", "vc": "vc-four", "n": int64(-1), "dc": "0.000001", "f": 1.0,
			"dt": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"d":  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"tm": "00:00:01",
			"ts": nil,
			"b":  []byte{0xAB}, "vb": []byte{0xCD},
		},
		5: {
			"s": nil, "vc": padQ, "n": nil, "dc": nil, "f": nil,
			"dt": nil, "d": nil, "tm": nil, "ts": nil, "b": nil, "vb": nil,
		},
	}
	seen := map[int64]bool{}
	for _, ch := range inserts {
		ins, ok := ch.(ir.Insert)
		if !ok {
			t.Fatalf("change = %T; want ir.Insert", ch)
		}
		id, _ := ins.Row["id"].(int64)
		want, known := wantRows[id]
		if !known {
			t.Fatalf("insert for unexpected id %d", id)
		}
		seen[id] = true
		assertFamRow(t, "insert", id, ins.Row, want)
	}
	if len(seen) != 5 {
		t.Fatalf("captured %d distinct rows; want 5: %v", len(seen), seen)
	}

	// UPDATE arm: rewrite one value from EVERY family on row 1 — the
	// 600-char before-image and the ≥min_len after-image both compress,
	// so this is the Update_rows_compressed_v1 decode of all families
	// at once. The after-image must carry the new values exactly.
	newS := strings.Repeat("upd", 150) + "→ok"
	applyMySQL(t, dsn, `UPDATE fam SET s = '`+newS+`', n = 777, dc = '7.250000',
		dt = '2025-06-15 01:02:03.000004', tm = '01:02:03.004', b = UNHEX(REPEAT('AB', 300)) WHERE id = 1`)
	upd := drainChanges(t, ctx, changes, 1, 45*time.Second)
	if len(upd) != 1 {
		t.Fatalf("got %d updates; want 1 (stream error: %v)", len(upd), rdr.(*CDCReader).Err())
	}
	u, ok := upd[0].(ir.Update)
	if !ok {
		t.Fatalf("change = %T; want ir.Update", upd[0])
	}
	wantAfter := map[string]any{
		"s": newS, "vc": "vc-one", "n": int64(777), "dc": "7.250000", "f": 2.25,
		"dt": time.Date(2025, 6, 15, 1, 2, 3, 4000, time.UTC),
		"d":  time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
		"tm": "01:02:03.004",
		"ts": time.Date(2024, 3, 1, 12, 34, 56, 123456000, time.UTC),
		"b":  bytes.Repeat([]byte{0xAB}, 300), "vb": []byte{0x01, 0x02},
	}
	assertFamRow(t, "update-after", 1, u.After, wantAfter)
	// The before-image is PK-narrowed before emit (Bug 88 discipline —
	// same as the uncompressed arm); the identity must survive the
	// compressed decode.
	if id, _ := u.Before["id"].(int64); id != 1 {
		t.Errorf("update before-image id = %#v; want 1", u.Before["id"])
	}

	// DELETE arm: rows 2 (binary-bulk) and 3 (padded numeric/temporal)
	// both carry ≥min_len before-images → Delete_rows_compressed_v1.
	// The PK identity must survive the compressed before-image decode.
	applyMySQL(t, dsn, "DELETE FROM fam WHERE id IN (2, 3)")
	dels := drainChanges(t, ctx, changes, 2, 45*time.Second)
	if len(dels) != 2 {
		t.Fatalf("got %d deletes; want 2 (stream error: %v)", len(dels), rdr.(*CDCReader).Err())
	}
	delSeen := map[int64]bool{}
	for _, ch := range dels {
		d, ok := ch.(ir.Delete)
		if !ok {
			t.Fatalf("change = %T; want ir.Delete", ch)
		}
		id, _ := d.Before["id"].(int64)
		delSeen[id] = true
	}
	if !delSeen[2] || !delSeen[3] {
		t.Fatalf("delete before-images carried ids %v; want {2,3}", delSeen)
	}

	// Anti-vacuity: the server must actually have WRITTEN compressed
	// events for all three verbs (and an uncompressed write for the
	// sub-min_len row) — otherwise everything above degraded to a plain
	// uncompressed test without failing.
	counts := countBinlogEventTypes(t, dsn)
	for _, want := range []string{"Write_rows_compressed_v1", "Update_rows_compressed_v1", "Delete_rows_compressed_v1", "Write_rows_v1"} {
		if counts[want] == 0 {
			t.Errorf("SHOW BINLOG EVENTS carries no %s event (counts: %v) — the matrix did not exercise "+
				"the compressed path it exists to pin", want, counts)
		}
	}
}

// assertFamRow compares a CDC-decoded row against the literal expected
// values, per family shape: []byte via bytes.Equal, time.Time via
// Equal, everything else via direct comparison. A nil want asserts SQL
// NULL decoded to nil.
func assertFamRow(t *testing.T, arm string, id int64, got ir.Row, want map[string]any) {
	t.Helper()
	for col, w := range want {
		g, present := got[col]
		if !present {
			t.Errorf("%s id=%d: column %s missing from decoded row", arm, id, col)
			continue
		}
		switch wv := w.(type) {
		case nil:
			if g != nil {
				t.Errorf("%s id=%d %s: got %#v; want nil (SQL NULL)", arm, id, col, g)
			}
		case []byte:
			gb, ok := g.([]byte)
			if !ok || !bytes.Equal(gb, wv) {
				t.Errorf("%s id=%d %s: got %#v (%T); want bytes %#v", arm, id, col, g, g, wv)
			}
		case time.Time:
			gt, ok := g.(time.Time)
			if !ok || !gt.Equal(wv) {
				t.Errorf("%s id=%d %s: got %#v (%T); want time %v", arm, id, col, g, g, wv)
			}
		default:
			if g != w {
				t.Errorf("%s id=%d %s: got %#v (%T); want %#v (%T)", arm, id, col, g, g, w, w)
			}
		}
	}
}

// countBinlogEventTypes tallies Event_type over every binlog file on
// the server — the independent evidence that compressed events were
// actually produced.
func countBinlogEventTypes(t *testing.T, dsn string) map[string]int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logRows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	var files []string
	for logRows.Next() {
		var name string
		var size int64
		if err := logRows.Scan(&name, &size); err != nil {
			_ = logRows.Close()
			t.Fatalf("scan binary logs: %v", err)
		}
		files = append(files, name)
	}
	if err := logRows.Err(); err != nil {
		_ = logRows.Close()
		t.Fatalf("binary logs rows: %v", err)
	}
	_ = logRows.Close()

	counts := map[string]int{}
	for _, f := range files {
		evRows, err := db.QueryContext(ctx, "SHOW BINLOG EVENTS IN '"+f+"'")
		if err != nil {
			t.Fatalf("SHOW BINLOG EVENTS IN %q: %v", f, err)
		}
		for evRows.Next() {
			var logName, eventType, info string
			var pos, endPos int64
			var serverID int64
			if err := evRows.Scan(&logName, &pos, &eventType, &serverID, &endPos, &info); err != nil {
				_ = evRows.Close()
				t.Fatalf("scan binlog event: %v", err)
			}
			counts[eventType]++
		}
		if err := evRows.Err(); err != nil {
			_ = evRows.Close()
			t.Fatalf("binlog events rows: %v", err)
		}
		_ = evRows.Close()
	}
	return counts
}
