//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) on the VStream change stream (vttestserver). vttablet sends a
// character cell in the column's OWN charset, tagged with its collation ID
// — MEASURED before the fix: latin1 'é' arrived as the byte 0xE9 on every
// charset — and sends collation 0 for gbk, big5, tis620 and gb18030, whose
// collations its environment does not list. The binlog lane's matrix runs
// here for every charset vttablet names; the four it does not are pinned to
// stream pure ASCII exactly and REFUSE anything else, never raw bytes.
// Expected values are the source server's own conversion, as in the binlog
// pin.

package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// vstreamUnnamedCharsets are the charsets vttablet tags with collation 0.
var vstreamUnnamedCharsets = map[string]bool{"gbk": true, "big5": true, "tis620": true, "gb18030": true}

func vstreamStart(t *testing.T, mysqlDSN, grpcEndpoint string) func(ctx context.Context) (func(int) []ir.Change, func() error) {
	return func(ctx context.Context) (func(int) []ir.Change, func() error) {
		dsn := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint)
		rdr, err := Engine{Flavor: FlavorPlanetScale}.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		t.Cleanup(func() {
			if c, ok := rdr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		})
		changes, err := rdr.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		time.Sleep(2 * time.Second)
		return func(n int) []ir.Change { return drainVTTestChanges(t, ctx, changes, n, 45*time.Second) },
			func() error { return rdr.(*vstreamCDCReader).Err() }
	}
}

func TestVStream_CDCCharsetDecode(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	cdcCharsetLane(t, mysqlDSN, vstreamUnnamedCharsets, vstreamStart(t, mysqlDSN, grpcEndpoint))
}

// TestVStream_CDCCharsetDDLReplay_GuardRefuses is the VStream twin of the
// binlog replay pin (charset_decode_replay_integration_test.go): a value
// recorded in one charset, the column converted, then a resume from before
// the value.
//
// MEASURED on vttestserver: vttablet's FIELD event for the replayed row
// carries the column's CURRENT collation, so without a guard the value
// written as utf8mb4 'é' was decoded as latin1 'Ã©' at exit 0. VStream
// carries no statement of the written charset, so the charset-DDL guard must
// refuse (SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH) when the replay reaches the
// DDL — for every direction. Rows before the DDL are emitted first; that is
// the documented window the re-snapshot remedy repairs.
func TestVStream_CDCCharsetDDLReplay_GuardRefuses(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	dsn := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint)
	for i, c := range charsetReplayCases {
		table := fmt.Sprintf("rep%d", i)
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf("CREATE TABLE %s (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET %s NULL)", table, c.from))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

		drain, _ := vstreamStart(t, mysqlDSN, grpcEndpoint)(ctx)
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf("INSERT INTO %s VALUES (1, 'anchor')", table))
		anchor := drain(1)
		if len(anchor) != 1 {
			cancel()
			t.Fatalf("%s: the anchor row did not stream", c.name)
		}
		resumeFrom := anchor[0].Pos()

		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf("INSERT INTO %s VALUES (2, _%s X'%s')", table, c.from, c.valueHex))
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf("ALTER TABLE %s MODIFY v VARCHAR(16) CHARACTER SET %s NULL", table, c.to))

		var buf logcapture.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		rdr, err := Engine{Flavor: FlavorPlanetScale}.OpenCDCReader(ctx, dsn)
		if err != nil {
			cancel()
			t.Fatalf("OpenCDCReader: %v", err)
		}
		changes, err := rdr.StreamChanges(ctx, resumeFrom)
		if err != nil {
			cancel()
			t.Fatalf("StreamChanges: %v", err)
		}
		if c.to == "utf8mb4" {
			// Third review item 1a: a replay INTO a UTF-8 charset is carried
			// as v0.156.2 carried it — the stored byte 0xE9 passed through,
			// invalid UTF-8 and loud downstream — and not refused. The anchor
			// may replay too; look for row 2.
			got := drainVTTestChanges(t, ctx, changes, 3, 45*time.Second)
			slog.SetDefault(prev)
			if err := rdr.(*vstreamCDCReader).Err(); err != nil {
				t.Errorf("%s: err = %v; a replay into a UTF-8 charset is not refused", c.name, err)
			}
			found := false
			for _, ch := range got {
				if ins, ok := ch.(ir.Insert); ok && fmt.Sprint(ins.Row["id"]) == "2" {
					found = true
					if ins.Row["v"] != "\xe9" {
						t.Errorf("%s: replayed v = %q; want the stored byte 0xE9 carried as-is (the v0.156.2 behaviour)", c.name, ins.Row["v"])
					}
				}
			}
			if !found {
				t.Errorf("%s: row 2 never replayed", c.name)
			}
			_ = rdr.(interface{ Close() error }).Close()
			cancel()
			continue
		}
		// Drain until the stream ends at the refusal (or times out).
		_ = drainVTTestChanges(t, ctx, changes, 10, 45*time.Second)
		slog.SetDefault(prev)
		assertCharsetDDLGuardRefusal(t, c.name, rdr.(*vstreamCDCReader).Err())
		if warned := strings.Contains(buf.String(), charsetHistoryUnrecordedMarker); warned != (c.to != "utf8mb4") {
			t.Errorf("%s: %s logged = %v; want %v (current charset %s)", c.name, charsetHistoryUnrecordedMarker, warned, c.to != "utf8mb4", c.to)
		}
		_ = rdr.(interface{ Close() error }).Close()
		cancel()
	}
}

// TestVStream_CDCCharsetDDLLive_NoGuardRefusal: the guard's other direction.
// A stream LIVE across the DDL has cached the pre-DDL collation and must not
// refuse; rows on both sides decode by the charset they were written in.
func TestVStream_CDCCharsetDDLLive_NoGuardRefusal(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE live (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET utf8mb4 NULL)`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	drain, streamErr := vstreamStart(t, mysqlDSN, grpcEndpoint)(ctx)
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO live VALUES (1, _utf8mb4 X'C3A9')`)
	before := drain(1)
	applyVTTestSQL(t, mysqlDSN, `ALTER TABLE live MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL`)
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO live VALUES (2, _latin1 X'E9')`)
	after := drain(1)
	if err := streamErr(); err != nil {
		t.Fatalf("live stream across a charset DDL: err = %v; want no refusal", err)
	}
	for _, g := range [][]ir.Change{before, after} {
		if len(g) != 1 {
			t.Fatalf("got %d changes around the DDL; want 1 each side", len(g))
		}
		if ins, _ := g[0].(ir.Insert); ins.Row["v"] != "é" {
			t.Errorf("live row v = %q; want %q", ins.Row["v"], "é")
		}
	}
}

// TestVStream_CDCCharsetDDLLive_RoutineDDLNoRefusal: the routine DDLs the
// third review MEASURED refusing a LIVE VStream (charset names compared,
// collations not, UTF-8 targets guarded) — a collation-only CONVERT on a
// pure-utf8mb4 table, a restated utf8mb4 MODIFY, and latin1 collation-only
// changes — must pass, each with a row streamed just before it so the guard
// compares against a live FIELD shape.
func TestVStream_CDCCharsetDDLLive_RoutineDDLNoRefusal(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE u (id INT NOT NULL PRIMARY KEY, v VARCHAR(64) CHARACTER SET utf8mb4 NULL)`)
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE l1 (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET latin1 NULL)`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	drain, streamErr := vstreamStart(t, mysqlDSN, grpcEndpoint)(ctx)
	id := 0
	for _, step := range []struct{ table, value, ddl string }{
		{"u", "_utf8mb4 X'C3A9'", "ALTER TABLE u CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"},
		{"u", "_utf8mb4 X'C3A9'", "ALTER TABLE u MODIFY v VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"},
		{"l1", "_latin1 X'E9'", "ALTER TABLE l1 MODIFY v VARCHAR(16) COLLATE latin1_bin"},
		{"l1", "_latin1 X'E9'", "ALTER TABLE l1 MODIFY v VARCHAR(16) CHARACTER SET latin1 COLLATE latin1_general_ci"},
		{"l1", "_latin1 X'E9'", "ALTER TABLE l1 CONVERT TO CHARACTER SET latin1 COLLATE latin1_swedish_ci"},
		{"u", "_utf8mb4 X'C3A9'", ""},
		{"l1", "_latin1 X'E9'", ""},
	} {
		id++
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf("INSERT INTO %s VALUES (%d, %s)", step.table, id, step.value))
		got := drain(1)
		if err := streamErr(); err != nil {
			t.Fatalf("live stream: err = %v before row %d; want no refusal", err, id)
		}
		if len(got) != 1 {
			t.Fatalf("row %d of %s did not stream", id, step.table)
		}
		if ins, _ := got[0].(ir.Insert); ins.Row["v"] != "é" {
			t.Errorf("live row %d of %s: v = %q; want %q", id, step.table, ins.Row["v"], "é")
		}
		if step.ddl != "" {
			applyVTTestSQL(t, mysqlDSN, step.ddl)
		}
	}
}

// TestVStream_CharsetUnrecordedShapes pins the VStream reader's side of the
// backup lanes' window check (fourth review, item 2): after a row streams,
// the reader reports the shape it decoded the table by — charset and
// collation from the FIELD event — and stops reporting the table once the
// stream crosses an ALTER on it. (The binlog reader's side is pinned end to
// end by the pipeline's MariaDB NO_LOG backup lanes.)
func TestVStream_CharsetUnrecordedShapes(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE sh (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET latin1 NULL)`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dsn := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint)
	rdr, err := Engine{Flavor: FlavorPlanetScale}.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdr.(interface{ Close() error }).Close() }()
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	reporter := rdr.(interface {
		CharsetUnrecordedShapes() map[string]map[string][2]string
	})
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO sh VALUES (1, 'plain')`)
	if got := drainVTTestChanges(t, ctx, changes, 1, 45*time.Second); len(got) != 1 {
		t.Fatal("the row did not stream")
	}
	if got := reporter.CharsetUnrecordedShapes()["sh"]["v"]; got != [2]string{"latin1", "latin1_swedish_ci"} {
		t.Fatalf("reported shape sh.v = %v; want latin1/latin1_swedish_ci", got)
	}
	applyVTTestSQL(t, mysqlDSN, `ALTER TABLE sh MODIFY v VARCHAR(16) CHARACTER SET cp1251 NULL`)
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO sh VALUES (2, _cp1251 X'C0')`)
	if got := drainVTTestChanges(t, ctx, changes, 1, 45*time.Second); len(got) != 1 {
		t.Fatalf("the row after the ALTER did not stream (stream error: %v)", rdr.(*vstreamCDCReader).Err())
	}
	if _, ok := reporter.CharsetUnrecordedShapes()["sh"]; ok {
		t.Error("sh is still reported after the stream crossed an ALTER on it")
	}
}

func TestVStream_CDCCharsetDecode_UnnamedCharsetsRefuse(t *testing.T) {
	samples := map[string]string{"gbk": "D6D0", "big5": "A4A4", "tis620": "A1", "gb18030": "81308130"}
	for cs := range vstreamUnnamedCharsets {
		t.Run(cs, func(t *testing.T) {
			mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
			defer cleanup()
			cdcCharsetRefusalLane(t, mysqlDSN, cs, samples[cs], vstreamStart(t, mysqlDSN, grpcEndpoint))
		})
	}
}
