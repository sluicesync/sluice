//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Auto-shard-aware VStream cold-copy RESUME (ADR-0098) — the regression pin
// for the live HIGH bug (BUG-CATALOG Bug 156). ADR-0095 auto-sharded the
// cold-copy by table on a FRESH start, but a --resume of a multi-table
// keyspace fell back to the legacy single keyspace-wide interleaved stream
// and crash-looped on the ADR-0071 buffer cap (`table "X" would buffer …
// exceeding the --max-buffer-bytes cap … while table "Y" is being copied`).
// ADR-0098 makes the resume path auto-shard-aware too.
//
// This test cold-starts a 3-table auto-shard copy under a TINY byte cap
// (Σ(tables) ≫ cap), captures a mid-COPY checkpoint while the MIDDLE table is
// in flight (≥1 table already complete), then RESUMES from that checkpoint
// via OpenSnapshotStreamFromPosition with the full multi-table list. It
// asserts the resume:
//
//   - does NOT hit the ADR-0071 multi-table-interleave buffer-cap error
//     (Rows.Err() stays nil under the same tiny cap), and
//   - completes every table (no gap), with the in-progress table resumed
//     from its cursor and the others re-copied/fresh, and
//   - the handoff Position (stitched per-shard min) is non-empty so the CDC
//     tail can resume.
//
// Shares the harness in cdc_vstream_integration_test.go (startVTTestServer,
// applyVTTestSQL) and the cursor helpers in
// cdc_vstream_copy_resume_integration_test.go (tableLastPK).
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_AutoShardResume' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStream_AutoShardResume_MultiTableBoundedMemoryNoInterleaveCap(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// Three tables, each wide enough that interleaving any two behind a tiny
	// cap would trip the ADR-0071 refusal. The MIDDLE table (orders) is sized
	// large enough that its COPY spans several VStream packets so the bounded
	// checkpoint cadence captures a genuine MID-COPY cursor for it.
	tables := []string{"alpha", "orders", "omega"}
	const pad = "0123456789012345678901234567890123456789" // 40 bytes
	for _, tbl := range tables {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (
			id   BIGINT        NOT NULL AUTO_INCREMENT,
			name VARCHAR(255)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tbl))
	}

	seed := func(tbl string, n int) {
		var b strings.Builder
		for i := 1; i <= n; i++ {
			if b.Len() == 0 {
				fmt.Fprintf(&b, "INSERT INTO %s (name) VALUES ", tbl)
			} else {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "('%s-%d-%s')", tbl, i, pad)
			if i%500 == 0 {
				applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
				b.Reset()
			}
		}
		if b.Len() > 0 {
			applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
		}
	}
	const alphaRows = 1000
	const ordersRows = 20000 // big enough to span packets → a mid-COPY cursor
	const omegaRows = 1000
	seed("alpha", alphaRows)
	seed("orders", ordersRows)
	seed("omega", omegaRows)

	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	mkTable := func(name string) *ir.Table {
		return &ir.Table{
			Name: name,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "name", Type: ir.Varchar{Length: 255}},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		}
	}

	// ---- Phase 1: fresh auto-shard cold-start under a tiny cap. Drain alpha
	// FULLY (it completes), then drain orders, capturing checkpoints, until we
	// hold a mid-COPY cursor for orders (≥1 table done, another in flight).
	stream, err := eng.OpenSnapshotStreamForTables(ctx, sluiceDSN, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables: %v", err)
	}
	rows := stream.Rows.(*vstreamSnapshotRows)
	// Tighten checkpoint cadence + tiny cap (the cap proves auto-shard: under
	// the legacy interleave path orders would trip the ADR-0071 refusal the
	// instant it buffered behind alpha).
	rows.snap.mu.Lock()
	rows.snap.checkpointRows = 200
	rows.snap.checkpointInterval = 50 * time.Millisecond
	rows.snap.maxBufferBytes = 65536
	rows.snap.mu.Unlock()

	capturedCh := make(chan ir.Position, 256)
	rows.SetCopyCheckpoint(func(_ context.Context, pos ir.Position) error {
		select {
		case capturedCh <- pos:
		default:
		}
		return nil
	})

	// Drain alpha fully → it completes (a per-table COPY_COMPLETED).
	alphaCh, err := stream.Rows.ReadRows(ctx, mkTable("alpha"))
	if err != nil {
		t.Fatalf("ReadRows(alpha): %v", err)
	}
	alphaSeen := 0
	for range alphaCh {
		alphaSeen++
		rows.AdvanceDurableRows(1)
	}
	if err := stream.Rows.Err(); err != nil {
		_ = stream.Close()
		t.Fatalf("Rows.Err after alpha (auto-shard must NOT loud-refuse): %v", err)
	}
	if alphaSeen != alphaRows {
		_ = stream.Close()
		t.Fatalf("alpha drained %d; want %d", alphaSeen, alphaRows)
	}

	// Drain orders fully (static source) so the cadence captures genuine
	// mid-COPY cursors; the interrupt is modeled by RESUMING from an early
	// orders checkpoint.
	ordersCh, err := stream.Rows.ReadRows(ctx, mkTable("orders"))
	if err != nil {
		t.Fatalf("ReadRows(orders): %v", err)
	}
	ordersSeen := 0
	for range ordersCh {
		ordersSeen++
		rows.AdvanceDurableRows(1)
	}
	if err := stream.Rows.Err(); err != nil {
		_ = stream.Close()
		t.Fatalf("Rows.Err after orders: %v", err)
	}
	if ordersSeen != ordersRows {
		_ = stream.Close()
		t.Fatalf("orders drained %d; want %d", ordersSeen, ordersRows)
	}

	// Collect checkpoints; find the first carrying a mid-COPY orders cursor.
	close(capturedCh)
	var captured []ir.Position
	for pos := range capturedCh {
		captured = append(captured, pos)
	}
	var (
		checkpoint ir.Position
		cursorLast int64 = -1
		haveCursor bool
	)
	for _, pos := range captured {
		decoded, ok, derr := decodeVStreamPos(pos)
		if derr != nil || !ok {
			continue
		}
		if last, found := tableLastPK(t, decoded, "orders"); found {
			checkpoint = pos
			cursorLast = last
			haveCursor = true
			break
		}
	}
	if !haveCursor || cursorLast <= 0 || cursorLast >= ordersRows {
		_ = stream.Close()
		t.Fatalf("did not capture a strictly mid-COPY orders cursor (have=%v lastpk=%d of %d) — enlarge orders / tighten cadence",
			haveCursor, cursorLast, ordersRows)
	}
	t.Logf("auto-shard resume: interrupting with orders mid-COPY cursor lastpk id=%d (of %d); alpha already complete", cursorLast, ordersRows)

	// FULL TEARDOWN — the process-restart boundary.
	_ = stream.Close()

	// The persisted checkpoint must route to the bulk resume path AND name
	// exactly the one in-progress table (orders).
	if !eng.PositionCarriesCopyCursor(checkpoint) {
		t.Fatal("PositionCarriesCopyCursor=false for the mid-COPY checkpoint — would mis-route to plain CDC")
	}

	// ---- Phase 2: RESUME via OpenSnapshotStreamFromPosition with the FULL
	// multi-table list (>1 → ADR-0098 auto-shard-aware resume). The same tiny
	// cap MUST hold: the resume copies one table at a time (alpha re-copied,
	// orders resumed from cursor, omega fresh) — NO interleave, so the
	// ADR-0071 buffer-cap error must NOT fire.
	resumeCtx, resumeCancel := context.WithTimeout(ctx, 8*time.Minute)
	defer resumeCancel()

	resumed, err := eng.OpenSnapshotStreamFromPosition(resumeCtx, sluiceDSN, checkpoint, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamFromPosition (auto-shard resume): %v", err)
	}
	defer func() { _ = resumed.Close() }()

	rrows := resumed.Rows.(*vstreamSnapshotRows)
	rrows.snap.mu.Lock()
	rrows.snap.maxBufferBytes = 65536 // SAME tiny cap — the whole point
	rrows.snap.mu.Unlock()

	// Drain every table on the resume. The headline assertion: NO buffer-cap
	// error on ANY table (auto-shard resume keeps one table in flight), and
	// every table fully delivered.
	wantPerTable := map[string]int{"alpha": alphaRows, "omega": omegaRows}
	for _, name := range tables {
		ch, err := resumed.Rows.ReadRows(resumeCtx, mkTable(name))
		if err != nil {
			t.Fatalf("resumed ReadRows(%s): %v", name, err)
		}
		seen := 0
		minID := int64(1<<62 - 1)
		maxID := int64(0)
		for row := range ch {
			seen++
			if id, _ := row["id"].(int64); id > 0 {
				if id < minID {
					minID = id
				}
				if id > maxID {
					maxID = id
				}
			}
		}
		if err := resumed.Rows.Err(); err != nil {
			t.Fatalf("resumed Rows.Err after %s — a multi-table-interleave buffer-cap error here is the EXACT Bug-156 crash-loop the fix prevents: %v", name, err)
		}
		switch name {
		case "orders":
			// The in-progress table resumes from its cursor: vtgate replays
			// rows id > cursorLast. Assert no row-0 restart and full tail.
			if seen == 0 || minID <= cursorLast {
				t.Errorf("orders resume: minID=%d (want > cursor %d), seen=%d — vtgate restarted from row 0 or lost the tail", minID, cursorLast, seen)
			}
			wantTail := ordersRows - int(cursorLast)
			if seen != wantTail {
				t.Errorf("orders resume yielded %d rows; want %d (id > %d) — gap/dup across the resume seam", seen, wantTail, cursorLast)
			}
		default:
			// Re-copied (alpha) / fresh (omega) tables deliver in full.
			if seen != wantPerTable[name] {
				t.Errorf("%s resume yielded %d rows; want %d (full table)", name, seen, wantPerTable[name])
			}
		}
	}

	// Handoff position present so the CDC tail can resume from the stitched
	// per-shard minimum.
	if resumed.Position.Engine == "" || resumed.Position.Token == "" {
		t.Fatalf("resumed handoff Position is empty after auto-shard resume: %+v", resumed.Position)
	}
	if _, err := resumed.Changes.StreamChanges(resumeCtx, resumed.Position); err != nil {
		t.Fatalf("StreamChanges from resumed stitched position: %v", err)
	}
}
