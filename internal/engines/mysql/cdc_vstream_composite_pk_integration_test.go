//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Composite-PK regression test for the VStream (FlavorPlanetScale)
// CDC reader.
//
// Bug 8 (silent DELETE loss on composite-PK tables under PG REPLICA
// IDENTITY DEFAULT) reached real-world soak because no CDC
// integration test exercised composite-PK tables. The v0.3.0 fix
// closed the PG→PG path; a follow-up commit added regression
// coverage on the MySQL binlog reader
// (cdc_reader_composite_pk_integration_test.go) and the MySQL→PG
// cross-engine streamer path. This file is the punted VStream
// (FlavorPlanetScale) leg of the same regression-prevention surface:
// the Vitess binlogdata.RowChange protocol is structurally similar
// to the binlog row event, but it's a different parser exercising a
// different proto, so a Bug-8-class slip in the Vitess path
// wouldn't be caught by the binlog reader's test.
//
// Vitess emits Before/After row images via binlogdata.RowChange.
// On vanilla Vitess (vttestserver, the test target here) every
// column shows up in both halves with real values, so the same
// invariants the binlog reader test pins down apply here:
// Insert.Row, Update.Before, Update.After, and Delete.Before all
// carry both PK columns. If a future change to the VStream reader
// (or to vitess.io/vitess's row decoding) ever started dropping a
// PK column from Delete.Before, this test fails loudly instead of
// becoming a silent-data-loss field bug.

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestVStream_VTTestServer_CompositePK exercises a table with a
// two-column primary key over the VStream path and asserts every CDC
// event carries both PK columns in the right place — Insert.Row
// contains both, Update.Before and Update.After both contain both,
// Delete.Before contains both.
//
// Schema mirrors TestCDCReader_CompositePK (the binlog reader's
// equivalent test): order_items with composite PK on
// (order_id, line_no) and non-null non-PK columns so the
// "WHERE non_pk IS NULL" failure mode that hit Bug 8 cannot be
// masked.
//
// Type contract differs from the binlog reader's test in one place:
// VStream's decoder collapses INT16/INT24/INT32/INT64 into Go int64,
// so line_no (an INT column) surfaces as int64 here rather than
// int32 as on the binlog path. Both shapes round-trip cleanly to
// Postgres' INTEGER per docs/value-types.md.
func TestVStream_VTTestServer_CompositePK(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE order_items (
			order_id   BIGINT       NOT NULL,
			line_no    INT          NOT NULL,
			product_id BIGINT       NOT NULL,
			qty        INT          NOT NULL,
			PRIMARY KEY (order_id, line_no)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
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

	// Settle window: vtgate's stream takes a moment to register at
	// "current"; matches the pattern in the basic-change-stream
	// test for the same reason.
	time.Sleep(2 * time.Second)

	const dml = `
		INSERT INTO order_items (order_id, line_no, product_id, qty) VALUES
			(100, 1, 5001, 3),
			(100, 2, 5002, 1);
		UPDATE order_items SET qty = 7 WHERE order_id = 100 AND line_no = 1;
		DELETE FROM order_items WHERE order_id = 100 AND line_no = 2;
	`
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", dml)

	got := drainVTTestChanges(t, ctx, changes, 4, 60*time.Second)
	if len(got) != 4 {
		if cdcRdr, ok := rdr.(*vstreamCDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 4 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 4 (Insert, Insert, Update, Delete)", len(got))
	}

	// change[0]: INSERT (100, 1, 5001, 3)
	ins1, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if ins1.Table != "order_items" {
		t.Errorf("change[0].Table = %q; want order_items", ins1.Table)
	}
	if ord, _ := ins1.Row["order_id"].(int64); ord != 100 {
		t.Errorf("change[0].Row[order_id] = %#v; want int64(100)", ins1.Row["order_id"])
	}
	if ln, _ := ins1.Row["line_no"].(int64); ln != 1 {
		t.Errorf("change[0].Row[line_no] = %#v; want int64(1)", ins1.Row["line_no"])
	}

	// change[2]: UPDATE composite-PK row — Before and After must
	// both carry both PK columns. After also carries the new qty.
	//
	// VStream's RowChange carries the full before-image on UPDATE
	// for vanilla Vitess (vttestserver's default binlog_row_image).
	// If a future Vitess flavor ever started elide the before-image
	// for unchanged-PK UPDATEs, the right move is a flavor-specific
	// branch here, not relaxing the assertion globally — Delete's
	// before-image is the load-bearing one (below).
	upd, ok := got[2].(ir.Update)
	if !ok {
		t.Fatalf("change[2] = %T; want ir.Update", got[2])
	}
	if upd.Before == nil {
		t.Fatal("update.Before is nil; expected populated under vttestserver's default row-image")
	}
	if ord, _ := upd.Before["order_id"].(int64); ord != 100 {
		t.Errorf("update.Before[order_id] = %#v; want int64(100)", upd.Before["order_id"])
	}
	if ln, _ := upd.Before["line_no"].(int64); ln != 1 {
		t.Errorf("update.Before[line_no] = %#v; want int64(1)", upd.Before["line_no"])
	}
	if ord, _ := upd.After["order_id"].(int64); ord != 100 {
		t.Errorf("update.After[order_id] = %#v; want int64(100)", upd.After["order_id"])
	}
	if ln, _ := upd.After["line_no"].(int64); ln != 1 {
		t.Errorf("update.After[line_no] = %#v; want int64(1)", upd.After["line_no"])
	}
	if q, _ := upd.After["qty"].(int64); q != 7 {
		t.Errorf("update.After[qty] = %#v; want int64(7)", upd.After["qty"])
	}

	// change[3]: DELETE — Before must carry both PK columns. This
	// is the load-bearing assertion for the Bug 8 regression-
	// prevention on the VStream path. If a future change ever
	// started dropping line_no (or any PK column) from
	// Delete.Before, the test fails loudly here instead of
	// becoming a silent-data-loss field bug.
	del, ok := got[3].(ir.Delete)
	if !ok {
		t.Fatalf("change[3] = %T; want ir.Delete", got[3])
	}
	if del.Before == nil {
		t.Fatal("delete.Before is nil; expected populated under vttestserver's default row-image")
	}
	if ord, _ := del.Before["order_id"].(int64); ord != 100 {
		t.Errorf("delete.Before[order_id] = %#v; want int64(100)", del.Before["order_id"])
	}
	if ln, _ := del.Before["line_no"].(int64); ln != 2 {
		t.Errorf("delete.Before[line_no] = %#v; want int64(2) — composite-PK second column dropped?", del.Before["line_no"])
	}
}
