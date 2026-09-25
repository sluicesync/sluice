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

func TestCDCCharsetDecode_VStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	cdcCharsetLane(t, mysqlDSN, vstreamUnnamedCharsets, vstreamStart(t, mysqlDSN, grpcEndpoint))
}

func TestCDCCharsetDecode_VStream_UnnamedCharsetsRefuse(t *testing.T) {
	samples := map[string]string{"gbk": "D6D0", "big5": "A4A4", "tis620": "A1", "gb18030": "81308130"}
	for cs := range vstreamUnnamedCharsets {
		t.Run(cs, func(t *testing.T) {
			mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
			defer cleanup()
			cdcCharsetRefusalLane(t, mysqlDSN, cs, samples[cs], vstreamStart(t, mysqlDSN, grpcEndpoint))
		})
	}
}
