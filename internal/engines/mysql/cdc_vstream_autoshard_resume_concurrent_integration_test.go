//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Auto-shard-aware VStream cold-copy RESUME composed with cross-table WRITE
// concurrency (ADR-0098 × ADR-0100) — the resume+concurrency surface the
// ADR-0100 value-fidelity review flagged as the one integration gap.
//
// ADR-0098 makes a --resume of a multi-table keyspace auto-shard-aware (one
// single-table COPY at a time, bounded memory, the in-progress table resumed
// past its cursor). ADR-0100 makes the WRITE side concurrent: with K > 1 the
// engine partitions the in-scope tables into K disjoint groups and surfaces
// them via ir.ConcurrentCopyPartitioner.ConcurrentCopyGroups(), so the
// pipeline runs W = K consumer pipelines (one per group) concurrently.
//
// The review's structural argument (ADR-0100 §7): resume exactly-once under
// W > 1 is INHERITED, not a new invariant — concurrentGroups is a
// deterministic pure function of (sorted tables, K), surfaced identically for
// cold-start and resume, and the only resume-specific logic is producer-side
// seeding (already covered by the ADR-0098/0099 resume tests). The W-goroutine
// consumer is resume-agnostic. This test pins that end-to-end: a resume with
// K = 2 (a) ENGAGES the concurrent-write path (ConcurrentCopyGroups() returns
// ≥2 groups — so the pipeline's W-goroutine consumer would drain ≥2 groups),
// and (b) lands every table EXACTLY ONCE (the in-progress table resumed past
// its cursor, the rest re-copied/fresh — no gap, no dup), with (c) a clean
// stitched-min CDC handoff.
//
// COVERAGE BOUNDARY — read before "but it doesn't OS-kill mid-write":
//   - The process-restart boundary is modeled by a full stream.Close() + a
//     fresh OpenSnapshotStreamFromPosition, NOT an OS-level kill of an
//     in-flight write goroutine. The resume exactly-once this pins is the
//     STRUCTURALLY-INHERITED one (deterministic partition + producer-side
//     seeding), which is the exact surface the review flagged — an OS kill
//     exercises the same producer-side seed + the same resume-agnostic
//     consumer, so it adds no consumer invariant this doesn't already cover.
//   - The seed cursor is captured on a K = 1 (sequential) cold-start, because
//     the K > 1 concurrent pump deliberately records NO mid-COPY breadcrumb
//     (ADR-0100 §6 / ADR-0099: the concurrent frontiers aren't order-
//     equivalent, so a mid-COPY checkpoint there would be silent-loss-on-
//     resume). The persisted cursor is K-AGNOSTIC by shape — it only names the
//     single in-progress table (resolveResumeAutoShard) — so resuming it under
//     K = 2 is exactly what a real operator does: the cold-start dies with a
//     mid-COPY cursor, the resume re-derives the K = 2 partition purely from
//     (tables, K) and places the seed into whichever group holds it. The
//     RESUME — the path under test — runs at K = 2 throughout.
//
// Shares the harness in cdc_vstream_integration_test.go (startVTTestServer,
// applyVTTestSQL, seedAutoShardWide) and the cursor helpers in
// cdc_vstream_copy_resume_integration_test.go (tableLastPK).
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_AutoShardResume_ConcurrentWrite' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStream_AutoShardResume_ConcurrentWrite_AllTablesExactlyOnce(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// Four tables so a K = 2 partition yields TWO non-trivial groups (≥2 tables
	// each by round-robin over the sorted list), proving the W-goroutine
	// consumer drains ≥2 groups concurrently. Distinct per-table row counts so a
	// mis-routed / dropped / double-copied table surfaces as a COUNT mismatch,
	// not a coincidental match. The MIDDLE-sorted table (cw_orders) is sized big
	// enough that its COPY spans several VStream packets, so the sequential
	// seed-capture cold-start records a genuine MID-COPY cursor for it.
	tables := []string{"cw_alpha", "cw_orders", "cw_yankee", "cw_zulu"}
	rowsPerTable := map[string]int{
		"cw_alpha":  120,
		"cw_orders": 20000, // spans packets → a real mid-COPY cursor
		"cw_yankee": 80,
		"cw_zulu":   160,
	}
	for _, tbl := range tables {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (
			id   BIGINT        NOT NULL,
			blob VARCHAR(4096) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tbl))
		seedAutoShardWide(t, mysqlDSN, tbl, rowsPerTable[tbl])
	}

	time.Sleep(3 * time.Second)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 13*time.Minute)
	defer cancel()

	mkTable := func(name string) *ir.Table {
		return &ir.Table{
			Name: name,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
				{Name: "blob", Type: ir.Varchar{Length: 4096}, Nullable: false},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		}
	}

	// ---- Phase 1: capture a mid-COPY cursor on a SEQUENTIAL (K=1) cold-start.
	// The K>1 concurrent pump records no breadcrumb by design (ADR-0100 §6), so
	// the seed must come from the sequential auto-shard path. The cursor is
	// K-agnostic — it names only the in-progress table (cw_orders) — so it is a
	// valid seed for the K=2 resume below.
	seedDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	stream, err := eng.OpenSnapshotStreamForTables(ctx, seedDSN, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables (seed cold-start): %v", err)
	}
	rows := stream.Rows.(*vstreamSnapshotRows)
	// Sanity: K=1 cold-start surfaces NO concurrent groups (the serial loop).
	if g := rows.ConcurrentCopyGroups(); len(g) > 1 {
		_ = stream.Close()
		t.Fatalf("seed cold-start (K=1) surfaced %d concurrent groups; want ≤1 (serial path)", len(g))
	}
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

	// Drain cw_alpha fully (it completes → a per-table COPY_COMPLETED), then
	// drain cw_orders fully so the cadence captures genuine mid-COPY cursors.
	// The interrupt is modeled by RESUMING from an early cw_orders checkpoint
	// (≥1 table already complete, cw_orders mid-flight).
	drainFull := func(name string, want int) {
		ch, derr := stream.Rows.ReadRows(ctx, mkTable(name))
		if derr != nil {
			_ = stream.Close()
			t.Fatalf("seed ReadRows(%s): %v", name, derr)
		}
		seen := 0
		for range ch {
			seen++
			rows.AdvanceDurableRows(1)
		}
		if e := stream.Rows.Err(); e != nil {
			_ = stream.Close()
			t.Fatalf("seed Rows.Err after %s: %v", name, e)
		}
		if seen != want {
			_ = stream.Close()
			t.Fatalf("seed %s drained %d; want %d", name, seen, want)
		}
	}
	drainFull("cw_alpha", rowsPerTable["cw_alpha"])
	drainFull("cw_orders", rowsPerTable["cw_orders"])

	close(capturedCh)
	var (
		checkpoint ir.Position
		cursorLast int64 = -1
		haveCursor bool
	)
	for pos := range capturedCh {
		decoded, ok, derr := decodeVStreamPos(pos)
		if derr != nil || !ok {
			continue
		}
		if last, found := tableLastPK(t, decoded, "cw_orders"); found {
			checkpoint = pos
			cursorLast = last
			haveCursor = true
			break
		}
	}
	if !haveCursor || cursorLast <= 0 || cursorLast >= int64(rowsPerTable["cw_orders"]) {
		_ = stream.Close()
		t.Fatalf("did not capture a strictly mid-COPY cw_orders cursor (have=%v lastpk=%d of %d) — enlarge cw_orders / tighten cadence",
			haveCursor, cursorLast, rowsPerTable["cw_orders"])
	}
	t.Logf("resume×concurrent: interrupting with cw_orders mid-COPY cursor lastpk id=%d (of %d); cw_alpha already complete",
		cursorLast, rowsPerTable["cw_orders"])

	// FULL TEARDOWN — the process-restart boundary.
	_ = stream.Close()

	if !eng.PositionCarriesCopyCursor(checkpoint) {
		t.Fatal("PositionCarriesCopyCursor=false for the mid-COPY checkpoint — would mis-route to plain CDC")
	}

	// ---- Phase 2: RESUME with K = 2 (vstream_copy_table_parallelism=2) over
	// the FULL table list. This is the path under test: the engine re-derives
	// the K=2 partition purely from (tables, K), places the cw_orders seed into
	// whichever group holds it, and runs W = 2 consumer pipelines. The headline
	// assertions: (a) the concurrent-write path ENGAGES on resume
	// (ConcurrentCopyGroups() ≥ 2 groups), and (b) every table lands EXACTLY
	// ONCE with no gap/dup.
	resumeCtx, resumeCancel := context.WithTimeout(ctx, 9*time.Minute)
	defer resumeCancel()

	resumeDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_copy_table_parallelism=2",
		mysqlDSN, grpcEndpoint,
	)
	resumed, err := eng.OpenSnapshotStreamFromPosition(resumeCtx, resumeDSN, checkpoint, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamFromPosition (K=2 concurrent resume): %v", err)
	}
	defer func() { _ = resumed.Close() }()

	rrows := resumed.Rows.(*vstreamSnapshotRows)

	// (a) The concurrent-write path is ENGAGED on the resume — the pipeline's
	// W-goroutine consumer would drive ≥2 groups (the exact ADR-0100 surface).
	// This is the load-bearing resume×concurrency assertion: it proves the
	// concurrent consumer partition is surfaced on the RESUME path, not just
	// cold-start, and that it covers EXACTLY the in-scope tables (no table
	// dropped from / duplicated across the partition).
	groups := rrows.ConcurrentCopyGroups()
	if len(groups) < 2 {
		t.Fatalf("K=2 resume surfaced %d concurrent groups; want ≥2 (the W-goroutine consumer must drain ≥2 groups on resume)", len(groups))
	}
	seenInGroups := map[string]int{}
	for _, g := range groups {
		for _, tbl := range g {
			seenInGroups[tbl]++
		}
	}
	for _, tbl := range tables {
		if seenInGroups[tbl] != 1 {
			t.Fatalf("table %q appears in %d resume groups; want exactly 1 (coverage+disjointness — a 0 = silently un-copied, a 2 = double-copied)",
				tbl, seenInGroups[tbl])
		}
	}
	if len(seenInGroups) != len(tables) {
		t.Fatalf("resume partition covers %d distinct tables; want %d (the full in-scope set)", len(seenInGroups), len(tables))
	}

	// A tight per-stream cap: K=2 streams × one-table each must stay bounded.
	rrows.snap.mu.Lock()
	rrows.snap.maxBufferBytes = 65536
	rrows.snap.mu.Unlock()

	// (b) Drain every table on the resume. Each table lands EXACTLY ONCE: the
	// in-progress table (cw_orders) resumes past its cursor (vtgate replays
	// id > cursorLast, no row-0 restart), the others re-copy (cw_alpha,
	// completed before the interrupt) / fresh (cw_yankee, cw_zulu). No
	// buffer-cap error on ANY table (concurrent resume keeps one table per
	// stream in flight).
	for _, name := range tables {
		ch, rerr := resumed.Rows.ReadRows(resumeCtx, mkTable(name))
		if rerr != nil {
			t.Fatalf("resumed ReadRows(%s): %v", name, rerr)
		}
		seen := 0
		minID := int64(1<<62 - 1)
		for row := range ch {
			seen++
			if id, _ := row["id"].(int64); id > 0 && id < minID {
				minID = id
			}
		}
		if e := resumed.Rows.Err(); e != nil {
			t.Fatalf("resumed Rows.Err after %s — a buffer-cap refusal here means a stream interleaved (concurrent resume must keep one table per stream): %v", name, e)
		}
		switch name {
		case "cw_orders":
			if seen == 0 || minID <= cursorLast {
				t.Errorf("cw_orders resume: minID=%d (want > cursor %d), seen=%d — vtgate restarted from row 0 or lost the tail",
					minID, cursorLast, seen)
			}
			wantTail := rowsPerTable["cw_orders"] - int(cursorLast)
			if seen != wantTail {
				t.Errorf("cw_orders resume yielded %d rows; want %d (id > %d) — gap/dup across the resume seam", seen, wantTail, cursorLast)
			}
		default:
			if seen != rowsPerTable[name] {
				t.Errorf("%s resume yielded %d rows; want %d (full table, exactly once)", name, seen, rowsPerTable[name])
			}
		}
	}

	// (c) Clean CDC handoff from the stitched per-shard minimum across all K
	// streams' per-table snapshots. Join the COPY-completion barrier BEFORE
	// reading Position: the concurrent path closes each table's Rows channel on
	// a PER-TABLE signal, so the last ReadRows above can return before the
	// producer stitches and writes the stitched-min Position (the #243 race).
	// Mirror the production cold-start handoff (coldStartBeginCDC) so the read
	// is ordered after the write.
	if err := resumed.WaitCopyComplete(resumeCtx); err != nil {
		t.Fatalf("WaitCopyComplete after K=2 concurrent resume: %v", err)
	}
	if resumed.Position.Engine == "" || resumed.Position.Token == "" {
		t.Fatalf("resumed handoff Position empty after K=2 concurrent resume: %+v", resumed.Position)
	}
	if _, err := resumed.Changes.StreamChanges(resumeCtx, resumed.Position); err != nil {
		t.Fatalf("StreamChanges from resumed stitched position: %v", err)
	}
}
