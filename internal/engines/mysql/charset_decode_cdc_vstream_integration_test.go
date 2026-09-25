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
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
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

// TestVStream_CDCCharsetDecode_ReplayAcrossCharsetDDLLimitation is the
// VStream twin of the binlog replay pin (charset_decode_replay_integration_
// test.go): events recorded in utf8mb4, the column converted to latin1, then
// a resume from before them.
//
// MEASURED on vttestserver: vttablet's FIELD event for the replayed row
// carries the column's CURRENT collation (latin1), so the value written as
// utf8mb4 'é' is decoded as latin1 'Ã©', silently. Unlike the binlog's
// TABLE_MAP, the VStream row event carries no independent statement of the
// charset it was written in, so sluice cannot detect this. That is the
// documented LIMITATION (charset_decode.go, migrating-legacy-mysql.md:
// re-snapshot a table after a charset DDL on a VStream source whenever the
// stream was behind it). This pin asserts the known-wrong value on purpose:
// if vttablet starts reporting the historical collation (schema tracking),
// it fails, and the limitation must be revisited. Whether PlanetScale's
// vttablets run with schema tracking — which might change this — is an
// UNVERIFIED PREMISE.
func TestVStream_CDCCharsetDecode_ReplayAcrossCharsetDDLLimitation(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE rep (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET utf8mb4 NULL)`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	drain, _ := vstreamStart(t, mysqlDSN, grpcEndpoint)(ctx)
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO rep VALUES (1, 'anchor')`)
	anchor := drain(1)
	if len(anchor) != 1 {
		t.Fatal("the anchor row did not stream")
	}
	resumeFrom := anchor[0].Pos()

	applyVTTestSQL(t, mysqlDSN, `INSERT INTO rep VALUES (2, _utf8mb4 X'C3A9')`)
	applyVTTestSQL(t, mysqlDSN, `ALTER TABLE rep MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL`)

	dsn := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint)
	rdr, err := Engine{Flavor: FlavorPlanetScale}.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() { _ = rdr.(interface{ Close() error }).Close() }()
	changes, err := rdr.StreamChanges(ctx, resumeFrom)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	// A VStream position is "before this transaction", so the anchor may
	// replay too; look for row 2 among what arrives.
	for _, c := range drainVTTestChanges(t, ctx, changes, 3, 45*time.Second) {
		ins, ok := c.(ir.Insert)
		if !ok || fmt.Sprint(ins.Row["id"]) != "2" {
			continue
		}
		if ins.Row["v"] != "Ã©" {
			t.Fatalf("replayed row 2 v = %q; the documented limitation says %q (utf8mb4 é decoded by the post-DDL latin1) — "+
				"if this changed, revisit the VStream limitation in charset_decode.go and migrating-legacy-mysql.md", ins.Row["v"], "Ã©")
		}
		return
	}
	t.Fatalf("row 2 never replayed (stream error: %v)", rdr.(*vstreamCDCReader).Err())
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
