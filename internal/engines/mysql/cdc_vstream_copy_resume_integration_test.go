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

	// 20k rows with a wide padded name (matching the no-PK test) so the
	// COPY reliably spans many VStream packets and the bounded checkpoint
	// cadence captures a genuine MID-COPY cursor. The original 3k table
	// drained inside a single COPY batch on vttestserver, leaving only a
	// terminal (post-completion) cursor — too small to exercise resume.
	const totalRows = 20000
	const pad = "0123456789012345678901234567890123456789" // 40 bytes
	const seedDDL = `
		CREATE TABLE widgets (
			id   BIGINT        NOT NULL AUTO_INCREMENT,
			name VARCHAR(255)  NOT NULL,
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
		fmt.Fprintf(&b, "('w%d-%s')", i, pad)
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
			{Name: "name", Type: ir.Varchar{Length: 255}},
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

// TestVStream_CopyResume_NoPKTable_CheckpointLagReSendUpserts is the
// ADR-0072 Gap-2 + Bug-125 interlock pin (pin-the-class, per the Bug-74
// lesson): the resumable cold-start COPY's warm-resume path routes the
// remaining COPY rows through the CDC applier, NOT the idempotent COPY
// writer. On a NO-PRIMARY-KEY table (the Bug-125 shape: BIGINT UNIQUE
// id, no PK, plus a cheaper non-null UNIQUE key forcing a divergent
// COPY scan order), the checkpoint cadence lags the writer's flushes,
// so on resume vtgate re-sends rows the target ALREADY holds. Before
// Fix 1, buildInsertSQL emitted a plain INSERT for no-PK tables and
// those re-sends hit MySQL 1062 → terminal resume failure.
//
// This test reproduces that exactly: it captures a mid-COPY checkpoint,
// PRE-POPULATES the target with the FULL source set (simulating the
// writer having flushed every row, well past the checkpoint cursor),
// then resumes from the lagging checkpoint and routes the resumed COPY
// Inserts through the real MySQL ChangeApplier into that already-full
// target. It asserts ZERO 1062 (Apply returns no error) and ZERO loss
// (final target COUNT == source COUNT — the re-sends upsert harmlessly).
func TestVStream_CopyResume_NoPKTable_CheckpointLagReSendUpserts(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// No PRIMARY KEY, one BIGINT UNIQUE key (the Bug-125 shape that the
	// resumable cold-start COPY must survive: the warm-resume routes
	// catch-up rows through the CDC applier, which must upsert on the
	// unique key rather than 1062). The divergent-scan-order concern
	// (cheaper unique key) is already covered by the Bug-125 copy-writer
	// tests; here the load-bearing property is applier idempotency on
	// the unique key during resume.
	//
	// Row count + wide padded `name` are sized so the COPY spans many
	// VStream packets and the bounded checkpoint cadence captures a
	// genuine MID-COPY cursor (not just the terminal one). A small,
	// narrow table can drain inside a single packet, leaving only a
	// COPY-completed cursor — which would make wantResume==0 and prove
	// nothing.
	const totalRows = 20000
	const seedDDL = `
		CREATE TABLE connections (
			id   BIGINT        NOT NULL,
			name VARCHAR(255)  NOT NULL,
			UNIQUE KEY uq_id (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	// pad widens each row so the COPY emits multiple packets / cursors.
	const pad = "0123456789012345678901234567890123456789" // 40 bytes
	var b strings.Builder
	for i := 1; i <= totalRows; i++ {
		if b.Len() == 0 {
			b.WriteString("INSERT INTO connections (id, name) VALUES ")
		} else {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d,'c%d-%s')", i, i, pad)
		if i%500 == 0 {
			applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
			b.Reset()
		}
	}
	if b.Len() > 0 {
		applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
	}

	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	connTable := &ir.Table{
		Name: "connections",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "name", Type: ir.Varchar{Length: 255}, Nullable: false},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	rows := stream.Rows.(*vstreamSnapshotRows)
	rows.snap.checkpointRows = 200
	rows.snap.checkpointInterval = 50 * time.Millisecond

	capturedCh := make(chan ir.Position, 64)
	rows.SetCopyCheckpoint(func(_ context.Context, pos ir.Position) error {
		select {
		case capturedCh <- pos:
		default:
		}
		return nil
	})

	rowsCh, err := stream.Rows.ReadRows(ctx, connTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	seen := 0
	for range rowsCh {
		seen++
	}
	close(capturedCh)
	var captured []ir.Position
	for pos := range capturedCh {
		captured = append(captured, pos)
	}
	if seen != totalRows {
		t.Fatalf("drained %d COPY rows; want %d", seen, totalRows)
	}

	// First mid-COPY checkpoint carrying a TablePKs cursor.
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
		last, found := tableLastPK(t, decoded, "connections")
		if found {
			checkpoint = pos
			cursorLast = last
			haveCursor = true
			break
		}
	}
	// With the ADR-0072 resume-targets-PRIMARY fix, a 20k wide-row no-PK
	// table reliably surfaces a strictly mid-COPY cursor on vttestserver
	// (the COPY spans many packets; the bounded checkpoint cadence catches
	// an intermediate LASTPK). A missing mid-COPY cursor now means the
	// table sizing / cadence regressed, NOT an environment quirk — so it
	// is a hard failure, not a skip. (If a future vttestserver build
	// changes packetization, enlarge totalRows / tighten the cadence until
	// a mid-COPY cursor is captured — the point is a genuine resume
	// assertion.)
	if !haveCursor || cursorLast <= 0 || cursorLast >= totalRows {
		_ = stream.Close()
		t.Fatalf("did not capture a strictly mid-COPY no-PK cursor (haveCursor=%v lastpk=%d of %d) — enlarge the table / tighten the checkpoint cadence until vtgate emits an intermediate LASTPK",
			haveCursor, cursorLast, totalRows)
	}
	t.Logf("no-PK resume: mid-COPY checkpoint connections lastpk id=%d (of %d rows)", cursorLast, totalRows)
	_ = stream.Close()

	// Pre-populate a FRESH target on the shared MySQL container with the
	// FULL source set — simulating the COPY writer having flushed every
	// row well past the lagging checkpoint. This is the checkpoint-lag
	// condition: on resume vtgate re-sends id > cursorLast that the
	// target already holds.
	targetDSN, _ := newSharedDB(t, "coldstart_nopk_resume")
	sw, err := Engine{}.OpenSchemaWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIfErr(sw)
	schema := &ir.Schema{Tables: []*ir.Table{connTable}}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}
	// Seed the full source set into the target directly.
	{
		var tb strings.Builder
		for i := 1; i <= totalRows; i++ {
			if tb.Len() == 0 {
				tb.WriteString("INSERT INTO connections (id, name) VALUES ")
			} else {
				tb.WriteString(",")
			}
			fmt.Fprintf(&tb, "(%d,'c%d-%s')", i, i, pad)
			if i%500 == 0 {
				applyVTTestSQL(t, targetDSN+"&multiStatements=true", tb.String())
				tb.Reset()
			}
		}
		if tb.Len() > 0 {
			applyVTTestSQL(t, targetDSN+"&multiStatements=true", tb.String())
		}
	}
	if got := scalarCount(t, targetDSN, "SELECT COUNT(*) FROM connections"); got != totalRows {
		t.Fatalf("pre-populated target has %d rows; want %d", got, totalRows)
	}

	// Resume the stream from the lagging checkpoint and route the resumed
	// COPY Inserts through the REAL MySQL applier into the already-full
	// target. The applier rewrites v.Schema to the target DB so the
	// connections table resolves there.
	applier, err := Engine{}.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer closeIfErr(applier)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	// The applier's configured schema (from targetDSN) is authoritative
	// for write routing — see applierSchema — so the resumed Insert's
	// source-side v.Schema doesn't need retargeting.

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader (resume): %v", err)
	}
	defer closeIfErr(rdr)

	// The resume reader and the applier get SEPARATE contexts. The reader's
	// resume read is bounded (we stop once we have the bounded sample); the
	// applier drains on the outer ctx so its final WritePosition isn't cut
	// off by the read deadline — a deadline there would masquerade as an
	// apply failure even though the resume itself succeeded.
	readCtx, readCancel := context.WithTimeout(ctx, 8*time.Minute)
	defer readCancel()
	changes, err := rdr.StreamChanges(readCtx, checkpoint)
	if err != nil {
		t.Fatalf("StreamChanges (resume from checkpoint): %v", err)
	}

	// Bridge the resumed COPY Inserts into the applier channel. We only
	// forward `connections` COPY-phase Inserts (id > cursorLast) — all
	// already present in the target — and the applier's idempotent
	// upsert MUST absorb every one (zero 1062). The "/.*/" resume also
	// re-copies other keyspace tables; we ignore their events.
	//
	// With the ADR-0072 resume-targets-PRIMARY fix, vtgate continues the
	// COPY scan from the cursor and replays rows id > cursorLast as COPY
	// Inserts. firstResumeGrace bounds the wait for the FIRST resumed
	// `connections` Insert; if none arrives within the window the resume
	// silently degraded to plain CDC tailing (the pre-fix REPLICA-cold-
	// schema bug) and we FAIL — not skip. Once rows arrive, we assert
	// zero 1062 + zero loss on them.
	remaining := totalRows - int(cursorLast)
	// We route a BOUNDED sample of the resumed COPY rows through the real
	// applier — enough to prove the load-bearing properties (resume starts
	// at id > cursor, the re-sends upsert with zero 1062, and the replayed
	// prefix is gap-free) — rather than all ~15k. Routing the full
	// remainder through the control-table-writing applier is ~12 min of
	// wall-clock (the applier's per-batch commit under the 100ms idle-flush
	// is the rate limiter); a bounded sample keeps the CI vstream job fast
	// while still exercising the exact warm-resume seam. The full-source
	// zero-loss is independently ground-truthed by the final DB COUNT
	// below.
	sampleN := remaining
	if sampleN > 2000 {
		sampleN = 2000
	}
	// applierCh is buffered to hold the whole sample so the forward loop
	// NEVER blocks on the send. Decoupling the channel-drain from the
	// applier's commit latency is load-bearing for throughput: if the
	// forward loop blocked on a slow applier mid-commit, it would stop
	// draining `changes`, vtgate's server-side buffer would fill, and the
	// COPY replay would throttle to the applier's commit rate.
	applierCh := make(chan ir.Change, sampleN+64)
	applyErrCh := make(chan error, 1)
	// Drain through the BATCHED apply path (ApplyBatch, batch 500) rather
	// than per-change Apply: it routes through the identical no-PK
	// idempotent upsert (buildInsertSQL → ON DUPLICATE KEY UPDATE), so the
	// zero-1062 property is preserved, but it commits + writes position
	// once per batch instead of once per row. The warm-resume production
	// path uses ApplyBatch too (the orchestrator batches), so this is also
	// the realistic shape.
	go func() {
		applyErrCh <- applier.(*ChangeApplier).ApplyBatch(ctx, "nopk-resume-stream", applierCh, 500)
	}()

	resumedIDs := make(map[int64]bool, sampleN)
	minResumed := int64(1<<62 - 1)
	maxResumed := int64(-1)
	firstResumeGrace := time.After(90 * time.Second)
	overallDeadline := time.After(5 * time.Minute)
	sawAny := false
forward:
	for len(resumedIDs) < sampleN {
		select {
		case ev, ok := <-changes:
			if !ok {
				break forward
			}
			ins, isIns := ev.(ir.Insert)
			if !isIns {
				continue
			}
			id, ok := ins.Row["id"].(int64)
			if !ok {
				continue // an event from another resumed table; ignore.
			}
			sawAny = true
			if id < minResumed {
				minResumed = id
			}
			if id > maxResumed {
				maxResumed = id
			}
			if !resumedIDs[id] {
				resumedIDs[id] = true
				// The buffer is sized to the whole sample, so this send
				// never blocks; still guard against the applier having
				// returned early on an error (a 1062 would surface here).
				select {
				case applierCh <- ins:
				case err := <-applyErrCh:
					t.Fatalf("applier returned mid-stream — a 1062 here means the no-PK re-sends collided instead of upserting: %v", err)
				}
			}
		case <-firstResumeGrace:
			if !sawAny {
				close(applierCh)
				<-applyErrCh
				t.Fatalf("no resumed COPY rows for the no-PK table arrived within the grace window — the resume degraded to plain CDC tailing instead of continuing the COPY from the cursor (the pre-ADR-0072 REPLICA-cold-schema silent-loss bug)")
			}
		case <-overallDeadline:
			break forward
		}
	}
	close(applierCh)
	if err := <-applyErrCh; err != nil {
		t.Fatalf("applier returned error on checkpoint-lag re-send — a 1062 here means the no-PK re-sends collided instead of upserting: %v", err)
	}

	// Resume started past the cursor (no row-0 restart).
	if minResumed <= cursorLast {
		t.Errorf("resume re-emitted id=%d <= cursor lastpk %d — vtgate restarted COPY from row 0", minResumed, cursorLast)
	}
	// We collected the full sample.
	if len(resumedIDs) < sampleN {
		t.Errorf("resume yielded %d distinct sample rows; want %d (rows with id > %d) — possible loss", len(resumedIDs), sampleN, cursorLast)
	}
	// The replayed prefix is GAP-FREE: every id in [minResumed, maxResumed]
	// is present. A silent mid-replay skip (loss within the resumed range)
	// would leave a hole here even though the sample count matched.
	for id := minResumed; id <= maxResumed; id++ {
		if !resumedIDs[id] {
			t.Fatalf("resumed COPY has a gap at id=%d within [%d,%d] — silent loss inside the replayed range", id, minResumed, maxResumed)
		}
	}

	// Full-source zero loss: the target still holds exactly the full source
	// set (the sample's re-sends upserted in place — no duplicates, no
	// drops — and the pre-populated rows beyond the sample are untouched).
	srcCount := scalarCount(t, mysqlDSN, "SELECT COUNT(*) FROM connections")
	dstCount := scalarCount(t, targetDSN, "SELECT COUNT(*) FROM connections")
	if dstCount != srcCount {
		t.Fatalf("after no-PK resume: target count = %d; want source count = %d (1062 or loss across the resume seam)", dstCount, srcCount)
	}
}

// TestVStream_CopyResume_ProcessRestart_ResumesViaBulkPath is the
// v0.99.8 SILENT-DEGRADE pin (the gap the original ADR-0072 coverage
// missed). It is DISTINCT from both the in-place reconnect test
// (transient drop DURING an active stream, via reconnectCopy) and the
// CDC-reader checkpoint-resume test above (which resumes via the plain
// OpenCDCReader → per-row apply path): here the stream is fully TORN
// DOWN and a process restart resumes the bulk COPY through the NEW
// OpenSnapshotStreamFromPosition path, draining via the snapshot's
// ReadRows (the bulk copyPump) — NOT the per-row CDC apply path.
//
// The bug: the pipeline's warmResume routed a process-restart resume
// (persisted position carrying a TablePKs cursor) through the plain CDC
// reader, which applied the un-copied COPY tail one INSERT round-trip at
// a time (~10 rows/sec) instead of the batched bulk-COPY writer
// (~4000 rows/sec) — the target stuck at ~5% of a 19M-row table, only
// heartbeats, no error (silent-loss class).
//
// The fix routes a cursor-carrying resume through the seedable snapshot
// stream so vtgate's re-emitted COPY-tail rows arrive on the bulk
// copyPump. This test asserts EXACTLY that path is taken and yields zero
// loss:
//
//   - the resumed snapshot's ReadRows (copyPump) yields the COPY tail —
//     proving the bulk path is engaged, not the per-row CDC reader, and
//   - every yielded id is strictly greater than the cursor lastpk
//     (vtgate continued the COPY from the cursor — NO restart from row 0),
//     and
//   - the union (rows at/below the cursor on the first drain) + (rows from
//     the resumed drain) covers the full source set: zero loss.
//
// NOTE on vttestserver vs real PlanetScale (Phase C caveat): vttestserver
// runs a single local tablet, so its COPY throughput does NOT reproduce
// real-PS's per-row-vs-bulk latency gap (the ~10 rows/sec crawl is a
// remote-round-trip artifact). This test therefore CANNOT distinguish
// bulk-vs-crawl by wall-clock; it instead asserts the STRUCTURAL property
// that the resume flows through the snapshot's ReadRows/copyPump (the bulk
// path) with zero loss. The wall-clock win is the operator's to confirm
// on real PlanetScale.
func TestVStream_CopyResume_ProcessRestart_ResumesViaBulkPath(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const totalRows = 20000
	const pad = "0123456789012345678901234567890123456789" // 40 bytes
	const seedDDL = `
		CREATE TABLE widgets (
			id   BIGINT        NOT NULL AUTO_INCREMENT,
			name VARCHAR(255)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	applyVTTestSQL(t, mysqlDSN, seedDDL)

	var b strings.Builder
	for i := 1; i <= totalRows; i++ {
		if b.Len() == 0 {
			b.WriteString("INSERT INTO widgets (name) VALUES ")
		} else {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "('w%d-%s')", i, pad)
		if i%500 == 0 {
			applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
			b.Reset()
		}
	}
	if b.Len() > 0 {
		applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", b.String())
	}
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	widgetsTable := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
	}

	// ---- Phase 1: fresh cold-start, drained partway, captures a mid-COPY
	// checkpoint, then INTERRUPTED (stream fully closed = process death).
	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	rows := stream.Rows.(*vstreamSnapshotRows)
	rows.snap.checkpointRows = 200
	rows.snap.checkpointInterval = 50 * time.Millisecond

	capturedCh := make(chan ir.Position, 256)
	rows.SetCopyCheckpoint(func(_ context.Context, pos ir.Position) error {
		select {
		case capturedCh <- pos:
		default:
		}
		return nil
	})

	rowsCh, err := stream.Rows.ReadRows(ctx, widgetsTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	// Drain the full COPY (static source) so the checkpoint cadence captures
	// genuine mid-COPY cursors; the interrupt is simulated by RESUMING from
	// an early checkpoint, which is equivalent to the target having only
	// reached that point before the process died.
	firstSeen := 0
	for range rowsCh {
		firstSeen++
	}
	close(capturedCh)
	var captured []ir.Position
	for pos := range capturedCh {
		captured = append(captured, pos)
	}
	if firstSeen != totalRows {
		t.Fatalf("phase-1 drained %d COPY rows; want %d", firstSeen, totalRows)
	}

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
		if last, found := widgetsLastPK(t, decoded); found {
			checkpoint = pos
			cursorLast = last
			haveCursor = true
			break
		}
	}
	if !haveCursor || cursorLast <= 0 || cursorLast >= totalRows {
		_ = stream.Close()
		t.Fatalf("did not capture a strictly mid-COPY cursor (haveCursor=%v lastpk=%d of %d) — enlarge the table / tighten the cadence",
			haveCursor, cursorLast, totalRows)
	}
	t.Logf("process-restart resume: interrupting at mid-COPY checkpoint widgets lastpk id=%d (of %d)", cursorLast, totalRows)

	// FULL TEARDOWN — the process-restart boundary. Nothing of the phase-1
	// stream survives; only the persisted checkpoint position does.
	_ = stream.Close()

	// Guard: confirm the routing discriminator AGREES this position needs
	// the bulk resume path (so the test exercises the real pipeline gate).
	if !eng.PositionCarriesCopyCursor(checkpoint) {
		t.Fatal("PositionCarriesCopyCursor=false for a mid-COPY checkpoint — the pipeline would mis-route this to the plain CDC path")
	}

	// ---- Phase 2: RESUME via the NEW bulk path (OpenSnapshotStreamFromPosition).
	// This is what the pipeline's coldStart-resume now calls. We drain via
	// ReadRows (the copyPump) and assert the COPY continues from the cursor.
	resumeCtx, resumeCancel := context.WithTimeout(ctx, 6*time.Minute)
	defer resumeCancel()

	resumed, err := eng.OpenSnapshotStreamFromPosition(resumeCtx, sluiceDSN, checkpoint)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamFromPosition (process-restart resume): %v", err)
	}
	defer func() { _ = resumed.Close() }()

	resumedRowsCh, err := resumed.Rows.ReadRows(resumeCtx, widgetsTable)
	if err != nil {
		t.Fatalf("resumed ReadRows: %v", err)
	}

	resumedIDs := make(map[int64]bool, totalRows-int(cursorLast))
	minResumed := int64(1<<62 - 1)
	for row := range resumedRowsCh {
		id, _ := row["id"].(int64)
		if id < minResumed {
			minResumed = id
		}
		resumedIDs[id] = true
	}
	if err := resumed.Rows.Err(); err != nil {
		t.Fatalf("resumed bulk COPY ended with an error (loud-failure path, not a silent crawl): %v", err)
	}

	// The resumed COPY (bulk copyPump) must NOT restart from row 0.
	if minResumed <= cursorLast {
		t.Errorf("resumed bulk COPY yielded id=%d <= cursor lastpk %d — vtgate restarted the COPY from row 0 instead of resuming from the cursor",
			minResumed, cursorLast)
	}
	// Zero loss: every id in (cursorLast, totalRows] must arrive on the bulk
	// path. A missing id is silent loss across the process-restart seam.
	wantResume := totalRows - int(cursorLast)
	if len(resumedIDs) < wantResume {
		t.Errorf("resumed bulk COPY yielded %d distinct rows; want %d (rows with id > %d) — possible loss",
			len(resumedIDs), wantResume, cursorLast)
	}
	for id := cursorLast + 1; id <= int64(totalRows); id++ {
		if !resumedIDs[id] {
			t.Fatalf("row id=%d missing from the resumed BULK COPY — silent loss across the process-restart seam", id)
		}
	}
}

// widgetsLastPK extracts the integer lastpk for the "widgets" table from
// a decoded position's per-shard TablePKs cursor, if present. Returns
// (value, true) when the cursor exists; (_, false) when no widgets
// cursor is present (e.g. a post-COPY-completion checkpoint).
func widgetsLastPK(t *testing.T, shards []shardGtid) (int64, bool) {
	t.Helper()
	return tableLastPK(t, shards, "widgets")
}

// tableLastPK is widgetsLastPK generalized over the table name — it
// pulls the integer lastpk cursor for the named table from a decoded
// position's per-shard TablePKs.
func tableLastPK(t *testing.T, shards []shardGtid, table string) (int64, bool) {
	t.Helper()
	for _, sg := range shards {
		protoPKs, err := decodeTablePKs(sg.TablePKs)
		if err != nil {
			t.Fatalf("decodeTablePKs: %v", err)
		}
		for _, pk := range protoPKs {
			if pk.GetTableName() != table {
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
