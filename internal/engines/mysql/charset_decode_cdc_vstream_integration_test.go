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
