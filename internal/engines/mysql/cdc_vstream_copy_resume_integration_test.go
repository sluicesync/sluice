//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0072 resumable cold-start COPY — integration coverage against
// vitess/vttestserver. The unit tests pin the position round-trip
// (Phase A), the checkpoint cadence (Phase B), and the in-place
// reconnect plumbing (Phase C) with fakes; this file grounds the
// load-bearing claim that only a REAL vtgate can confirm: a mid-COPY
// checkpoint position carries Vitess's per-shard TablePKs cursor, and
// resuming from it makes vtgate continue the COPY scan from the
// last-copied PK rather than restarting the whole table from row 0
// (zero loss, no full re-copy).
//
// Shares the harness in cdc_vstream_integration_test.go
// (startVTTestServer, applyVTTestSQL, drainVTTestChanges).
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_CopyResume' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_CopyResume_MidCopyCheckpointResumesFromCursor seeds a
// table large enough that the COPY phase emits several LASTPK/VGTID
// batches, captures a mid-COPY checkpoint via the Phase B sink, then
// resumes a fresh stream from that checkpoint and asserts:
//
//   - the checkpoint position carries a non-empty TablePKs cursor
//     (Phase A round-trips the real vtgate cursor), and
//   - the resumed stream's COPY rows all have PK strictly greater than
//     the checkpoint cursor's lastpk (vtgate resumed from the cursor —
//     it did NOT restart from row 0), and
//   - the union of "rows at/below the cursor" + "rows from the resume"
//     equals the full source set: zero loss.
func TestVStream_CopyResume_MidCopyCheckpointResumesFromCursor(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const totalRows = 3000
	const seedDDL = `
		CREATE TABLE widgets (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			name VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	// Seed in batches so the single statement stays under the wire limit.
	var b strings.Builder
	for i := 1; i <= totalRows; i++ {
		if b.Len() == 0 {
			b.WriteString("INSERT INTO widgets (name) VALUES ")
		} else {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "('w%d')", i)
		if i%500 == 0 {
			applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
			b.Reset()
		}
	}
	if b.Len() > 0 {
		applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
	}

	// Let vttestserver's async schema tracker see the table.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Tighten the checkpoint cadence and install a capturing sink so we
	// observe mid-COPY checkpoints (in-package access to the engine
	// internals — this is a white-box integration test).
	rows := stream.Rows.(*vstreamSnapshotRows)
	rows.snap.checkpointRows = 200
	rows.snap.checkpointInterval = 50 * time.Millisecond

	var (
		captured   []ir.Position
		capturedCh = make(chan ir.Position, 64)
	)
	rows.SetCopyCheckpoint(func(_ context.Context, pos ir.Position) error {
		select {
		case capturedCh <- pos:
		default:
		}
		return nil
	})

	widgetsTable := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "name", Type: ir.Varchar{Length: 64}},
		},
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, widgetsTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	// Drain the full COPY (the source is static, so the COPY captures all
	// 3000 rows), collecting checkpoints as they arrive.
	seen := 0
	for range rowsCh {
		seen++
	}
	close(capturedCh)
	for pos := range capturedCh {
		captured = append(captured, pos)
	}
	if seen != totalRows {
		t.Fatalf("drained %d COPY rows; want %d", seen, totalRows)
	}

	// Pick a mid-COPY checkpoint that carries a TablePKs cursor (Phase A
	// against real vtgate). The last few checkpoints may be empty if the
	// per-table COPY already completed, so scan from the front for the
	// first one with a cursor.
	var (
		checkpoint ir.Position
		cursorLast int64 = -1
		haveCursor bool
	)
	for _, pos := range captured {
		decoded, ok, derr := decodeVStreamPos(pos)
		if derr != nil || !ok {
			t.Fatalf("checkpoint position failed to decode: ok=%v err=%v", ok, derr)
		}
		last, found := widgetsLastPK(t, decoded)
		if found {
			checkpoint = pos
			cursorLast = last
			haveCursor = true
			break
		}
	}
	if !haveCursor {
		t.Fatalf("no mid-COPY checkpoint carried a TablePKs cursor across %d checkpoints — Phase A did not capture the cursor against real vtgate", len(captured))
	}
	t.Logf("resuming from mid-COPY checkpoint: widgets lastpk id=%d (of %d rows)", cursorLast, totalRows)
	if cursorLast <= 0 || cursorLast >= totalRows {
		t.Fatalf("captured cursor lastpk id=%d is not strictly mid-COPY (want 0 < id < %d)", cursorLast, totalRows)
	}

	// Close the snapshot stream, then resume a FRESH standalone reader
	// from the mid-COPY checkpoint. vtgate must continue the COPY from the
	// cursor: the resumed COPY rows arrive as ir.Insert events whose ids
	// are all strictly greater than cursorLast (NO restart from row 0).
	_ = stream.Close()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader (resume): %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	resumeCtx, resumeCancel := context.WithTimeout(ctx, 90*time.Second)
	defer resumeCancel()
	changes, err := rdr.StreamChanges(resumeCtx, checkpoint)
	if err != nil {
		t.Fatalf("StreamChanges (resume from checkpoint): %v", err)
	}

	// The resume must yield exactly the rows with id > cursorLast as COPY
	// inserts. We expect totalRows-cursorLast of them; collect until we
	// have them (or the deadline trips).
	wantResume := totalRows - int(cursorLast)
	resumedIDs := make(map[int64]bool, wantResume)
	minResumed := int64(1<<62 - 1)
	deadline := time.After(80 * time.Second)
collect:
	for len(resumedIDs) < wantResume {
		select {
		case ev, ok := <-changes:
			if !ok {
				break collect
			}
			ins, isIns := ev.(ir.Insert)
			if !isIns {
				continue
			}
			id, _ := ins.Row["id"].(int64)
			if id < minResumed {
				minResumed = id
			}
			resumedIDs[id] = true
		case <-deadline:
			break collect
		}
	}

	if minResumed <= cursorLast {
		t.Errorf("resume re-emitted a row with id=%d <= cursor lastpk %d — vtgate restarted the COPY from row 0 instead of resuming from the cursor",
			minResumed, cursorLast)
	}
	if len(resumedIDs) < wantResume {
		t.Errorf("resume yielded %d distinct rows; want %d (rows with id > %d) — possible loss",
			len(resumedIDs), wantResume, cursorLast)
	}
	// Zero-loss union check: every id in (cursorLast, totalRows] must be
	// present in the resumed set.
	for id := cursorLast + 1; id <= int64(totalRows); id++ {
		if !resumedIDs[id] {
			t.Fatalf("row id=%d missing from the resumed COPY — silent loss across the resume seam", id)
		}
	}
}

// widgetsLastPK extracts the integer lastpk for the "widgets" table from
// a decoded position's per-shard TablePKs cursor, if present. Returns
// (value, true) when the cursor exists; (_, false) when no widgets
// cursor is present (e.g. a post-COPY-completion checkpoint).
func widgetsLastPK(t *testing.T, shards []shardGtid) (int64, bool) {
	t.Helper()
	for _, sg := range shards {
		protoPKs, err := decodeTablePKs(sg.TablePKs)
		if err != nil {
			t.Fatalf("decodeTablePKs: %v", err)
		}
		for _, pk := range protoPKs {
			if pk.GetTableName() != "widgets" {
				continue
			}
			qr := pk.GetLastpk()
			if qr == nil || len(qr.GetRows()) == 0 {
				continue
			}
			// The lastpk QueryResult has one row whose first column is the
			// PK value, wire-encoded as text bytes.
			row := qr.GetRows()[0]
			if len(row.GetLengths()) == 0 || row.GetLengths()[0] < 0 {
				continue
			}
			raw := row.GetValues()[:row.GetLengths()[0]]
			var v int64
			if _, err := fmt.Sscanf(string(raw), "%d", &v); err != nil {
				continue
			}
			return v, true
		}
	}
	return 0, false
}
