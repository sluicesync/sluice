//go:build integration && vitesscluster

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0174 Piece 2 — continuous filtered sync (`sync --where`) on the
// VStream (PlanetScale / Vitess) path — DURABLE end-to-end coverage against a
// REAL multi-process Vitess cluster.
//
// The in-repo unit pins cover the pieces in isolation:
//
//   - vstreamCopyFilterRules emits the `select * from <t> where (<pred>)`
//     rule (cdc_vstream_snapshot_test.go), and
//   - the pipeline's row-move truth table converts a move-IN UPDATE →
//     target INSERT and a move-OUT UPDATE → target DELETE
//     (internal/pipeline/where_cdc_filter_test.go).
//
// What NO unit pin can prove is the load-bearing Vitess wire behavior the
// design rests on (ADR-0174 §Context, ground-truthed against vendored vitess
// v0.24.2 processRowEvent lines 1224-1234): for a NON-vindex `--where` filter,
// vtgate evaluates the predicate on BOTH the before- and after-image and, if
// EITHER passes, forces both OK and emits the RowChange with BOTH images. So a
// move-IN (before out-of-scope, after in-scope) and a move-OUT (before
// in-scope, after out-of-scope) each arrive as a full UPDATE carrying both
// images — never silently dropped, never reshaped. THAT is what makes the
// pipeline's client-side row-move table produce the target INSERT / DELETE.
//
// This suite boots the real cluster and asserts, against the SOURCE as ground
// truth:
//
//   - the FILTERED cold-start COPY lands ONLY in-scope rows (the server-side
//     `where` on the copy phase);
//   - a move-IN UPDATE arrives as an ir.Update whose Before is out-of-scope
//     and After is in-scope (→ the pipeline INSERTs the after-image);
//   - a move-OUT UPDATE arrives as an ir.Update whose Before is in-scope and
//     After is out-of-scope (→ the pipeline DELETEs by key) — the cell that
//     WOULD leak an out-of-scope row on a naive per-event filter;
//   - an out-of-scope INSERT never reaches the stream at all.
//
// The "→ target INSERT / DELETE" half of each claim is the pipeline's
// unit-pinned route(); this suite proves the engine delivers the exact raw
// material (both images) route() needs. If a cluster run shows a move-OUT does
// NOT arrive with both images, the ADR premise is wrong and the design must
// change — so these assertions are the independent gate.
//
// Run (heavy — own build tag, NOT in the per-PR gate):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
//	  -run 'TestVitessClusterFilteredSync' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// filteredRegionsDDL seeds a table with a low-cardinality scope column
// (`region`) — the region/tenant/country shape the `--where` use case
// targets — plus a payload so an out-of-scope UPDATE has something to change.
const filteredRegionsDDL = `
	CREATE TABLE regions (
		id      BIGINT       NOT NULL AUTO_INCREMENT,
		region  VARCHAR(8)   NOT NULL,
		payload VARCHAR(64)  NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// filteredRegionsTable is the ir.Table describing `regions` for the
// cold-start COPY drain.
func filteredRegionsTable() *ir.Table {
	return &ir.Table{
		Name: "regions",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "region", Type: ir.Varchar{Length: 8}},
			{Name: "payload", Type: ir.Varchar{Length: 64}},
		},
	}
}

// TestVitessClusterFilteredSync is the ADR-0174 Piece 2 correctness gate.
func TestVitessClusterFilteredSync(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessCluster(t)
	defer cleanup()

	applyClusterSQL(t, mysqlDSN, filteredRegionsDDL)
	// Let the tablet's schema engine register the table before the VStream
	// FieldEvent (column-type metadata) is needed.
	time.Sleep(3 * time.Second)

	// Seed BEFORE the snapshot: 2 in-scope (EU) + 1 out-of-scope (US). The
	// filtered COPY must land ONLY the EU rows.
	applyClusterSQL(t, mysqlDSN+"&multiStatements=true", `
		INSERT INTO regions (id, region, payload) VALUES
			(1, 'EU', 'seed-eu-1'),
			(2, 'US', 'seed-us-2'),
			(3, 'EU', 'seed-eu-3')`)
	time.Sleep(2 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// The FILTERED open — the ADR-0174 Piece 2 entry point. The predicate is
	// pushed into the VStream COPY filter rules at open (server-side).
	filters := map[string]string{"regions": "region = 'EU'"}
	fo, ok := any(eng).(ir.FilteredSnapshotOpener)
	if !ok {
		t.Fatal("Engine{Flavor: FlavorPlanetScale} must implement ir.FilteredSnapshotOpener")
	}
	stream, err := fo.OpenSnapshotStreamForTablesFiltered(ctx, sluiceDSN, []string{"regions"}, filters)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTablesFiltered: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// (1) Filtered COPY drain — ONLY in-scope (EU) rows must land. The
	// out-of-scope US row (id=2) must never appear: the server-side `where`
	// scoped the copy scan, not a client-side skip.
	rowsCh, err := stream.Rows.ReadRows(ctx, filteredRegionsTable())
	if err != nil {
		t.Fatalf("ReadRows(regions): %v", err)
	}
	copied := map[int64]string{}
	for row := range rowsCh {
		id, ok := asInt64Val(row["id"])
		if !ok {
			t.Fatalf("COPY row has non-integer id: %#v", row["id"])
		}
		copied[id] = asStringVal(row["region"])
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("snapshot COPY error after drain: %v", err)
	}
	if len(copied) != 2 || copied[1] != "EU" || copied[3] != "EU" {
		t.Fatalf("filtered COPY delivered %v; want exactly {1:EU, 3:EU} (out-of-scope id=2 US must be excluded server-side)", copied)
	}
	if _, leaked := copied[2]; leaked {
		t.Fatalf("filtered COPY leaked out-of-scope row id=2 (US): the server-side WHERE did not scope the copy")
	}
	t.Log("filtered COPY PASS: only in-scope EU rows {1,3} landed; out-of-scope US row 2 excluded")

	// (2) Resume CDC from the COPY_COMPLETED position on the same stream.
	catchup, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("Changes.StreamChanges: %v", err)
	}
	// Settle before post-COPY DML so it lands inside the CDC window.
	time.Sleep(3 * time.Second)

	// Drive the four cells (source is ground truth for each):
	//   (A) in-scope INSERT      : id=10 EU        -> arrives as ir.Insert
	//   (B) out-of-scope INSERT  : id=11 US        -> NEVER arrives (vtgate drops)
	//   (C) move-IN UPDATE       : id=11 US -> EU   -> ir.Update, before US / after EU
	//   (D) move-OUT UPDATE      : id=10 EU -> US   -> ir.Update, before EU / after US
	applyClusterSQL(t, mysqlDSN, "INSERT INTO regions (id, region, payload) VALUES (10, 'EU', 'live-eu-10')")
	applyClusterSQL(t, mysqlDSN, "INSERT INTO regions (id, region, payload) VALUES (11, 'US', 'live-us-11')")
	applyClusterSQL(t, mysqlDSN, "UPDATE regions SET region = 'EU', payload = 'moved-in-11' WHERE id = 11")
	applyClusterSQL(t, mysqlDSN, "UPDATE regions SET region = 'US', payload = 'moved-out-10' WHERE id = 10")

	// Collect the changes the filtered stream delivers. We expect exactly
	// three row-bearing changes on `regions` (the in-scope INSERT, the move-IN
	// UPDATE, the move-OUT UPDATE); the out-of-scope INSERT (id=11 US) is
	// dropped server-side and never counts toward the total.
	got := drainFilteredRegionChanges(t, ctx, catchup, 3, 90*time.Second)

	// (A) in-scope INSERT (id=10) arrived with its in-scope region.
	sawInsert10 := false
	for _, ch := range got {
		if ins, ok := ch.(ir.Insert); ok && ins.Table == "regions" {
			if id, _ := asInt64Val(ins.Row["id"]); id == 10 {
				sawInsert10 = true
				if r := asStringVal(ins.Row["region"]); r != "EU" {
					t.Errorf("in-scope INSERT id=10 region = %q; want EU", r)
				}
			}
		}
	}
	if !sawInsert10 {
		t.Errorf("in-scope INSERT id=10 never arrived among %d changes (%s)", len(got), changeKinds(got))
	}

	// (B) out-of-scope INSERT (id=11 US) must NEVER appear as its own INSERT —
	// vtgate's server-side filter drops it. The only id=11 event is the move-IN
	// UPDATE below.
	for _, ch := range got {
		if ins, ok := ch.(ir.Insert); ok && ins.Table == "regions" {
			if id, _ := asInt64Val(ins.Row["id"]); id == 11 {
				t.Errorf("out-of-scope INSERT id=11 (US) leaked into the filtered stream: %#v", ins.Row)
			}
		}
	}

	// (C) move-IN (id=11 US->EU): the LOAD-BEARING anchor — must arrive as an
	// ir.Update carrying BOTH images (before out-of-scope US, after in-scope
	// EU). The pipeline's unit-pinned route() turns this into a target INSERT
	// of the after-image; if the before-image were absent the classification
	// could not run.
	moveIn := findRegionUpdate(t, got, 11)
	if before := asStringVal(moveIn.Before["region"]); before != "US" {
		t.Errorf("move-IN id=11 before.region = %q; want US (VStream must deliver the OLD out-of-scope image)", before)
	}
	if after := asStringVal(moveIn.After["region"]); after != "EU" {
		t.Errorf("move-IN id=11 after.region = %q; want EU", after)
	}

	// (D) move-OUT (id=10 EU->US): the cell a naive per-event filter would DROP
	// (leaking the now-out-of-scope row). VStream must deliver the full UPDATE
	// with BOTH images (before in-scope EU, after out-of-scope US); the
	// pipeline's route() turns before=in/after=out into a target DELETE by key.
	moveOut := findRegionUpdate(t, got, 10)
	if before := asStringVal(moveOut.Before["region"]); before != "EU" {
		t.Errorf("move-OUT id=10 before.region = %q; want EU (the in-scope OLD image the DELETE-by-key needs)", before)
	}
	if after := asStringVal(moveOut.After["region"]); after != "US" {
		t.Errorf("move-OUT id=10 after.region = %q; want US", after)
	}
	// The before-image must carry the filtered column (region) so route() can
	// classify the move-OUT — the exact partial-before-image hazard the
	// pipeline's SLUICE-E-WHERE-CDC-BEFORE-IMAGE guard refuses when it is absent.
	if _, ok := moveOut.Before["region"]; !ok {
		t.Errorf("move-OUT id=10 before-image omits the filtered column `region` — a partial before-image would mis-classify the move-OUT as a drop (leak)")
	}

	if err := stream.Changes.(interface{ Err() error }).Err(); err != nil {
		t.Fatalf("filtered CDC stream errored: %v", err)
	}
	t.Log("filtered CDC PASS: in-scope INSERT flowed; out-of-scope INSERT dropped server-side; move-IN and move-OUT each delivered as a full UPDATE with BOTH images (→ pipeline INSERT / DELETE)")
}

// TestVitessClusterFilteredSyncWarmResume is the ADR-0174 Piece 2 / audit F-P1
// WARM-RESUME correctness+efficiency gate. Cold start already pushes the
// `--where` predicate into the VStream server-side (proven by
// TestVitessClusterFilteredSync); F-P1 was that a WARM RESUME of the STANDALONE
// CDC reader (openVStreamReader → buildVStreamRequest) streamed the ENTIRE
// keyspace unfiltered and discarded ~99% client-side. This test proves the fix:
// a standalone filtered CDC reader, resumed from a persisted position with the
// row filters threaded via [ir.ServerSideCDCFilterSetter], reduces the stream
// SERVER-SIDE on resume.
//
// It asserts, against the SOURCE as ground truth:
//   - (a) the RESUMED stream delivers the in-scope INSERT and the move-OUT
//     UPDATE (before in-scope / after out-of-scope — the raw material the
//     pipeline's unit-pinned route() turns into a target INSERT / DELETE-by-
//     key, so the target ends correct); and
//   - (b) a purely-out-of-scope INSERT is dropped SERVER-SIDE on resume — it
//     never reaches the reader at all (the reader is BELOW the pipeline's
//     client-side route(), so a change arriving here proves it was streamed;
//     its ABSENCE proves vtgate filtered it at the source on the resumed
//     stream, which is exactly what F-P1 was missing).
func TestVitessClusterFilteredSyncWarmResume(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVitessCluster(t)
	defer cleanup()

	applyClusterSQL(t, mysqlDSN, filteredRegionsDDL)
	// Let the tablet's schema engine register the table before the VStream
	// FieldEvent (column-type metadata) is needed.
	time.Sleep(3 * time.Second)
	// Seed one in-scope row so the table exists with data before streaming.
	applyClusterSQL(t, mysqlDSN, "INSERT INTO regions (id, region, payload) VALUES (1, 'EU', 'seed-eu-1')")
	time.Sleep(2 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_tablet_type=primary",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	filters := map[string]string{"regions": "region = 'EU'"}

	// setFilters threads the --where predicates onto a standalone CDC reader
	// exactly as the pipeline's warmResume does (ir.ServerSideCDCFilterSetter).
	setFilters := func(r ir.CDCReader) {
		t.Helper()
		setter, ok := r.(ir.ServerSideCDCFilterSetter)
		if !ok {
			t.Fatal("*vstreamCDCReader must implement ir.ServerSideCDCFilterSetter (audit F-P1)")
		}
		setter.SetServerSideRowFilters(filters)
	}

	// --- Stream #1: run a filtered CDC tail briefly to capture a persisted
	// resume position (the "before the crash/restart" bookmark). ---
	r1, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader #1: %v", err)
	}
	setFilters(r1)
	ch1, err := r1.StreamChanges(ctx, ir.Position{}) // "current"
	if err != nil {
		closeReader(r1)
		t.Fatalf("StreamChanges #1: %v", err)
	}
	time.Sleep(3 * time.Second)
	applyClusterSQL(t, mysqlDSN, "INSERT INTO regions (id, region, payload) VALUES (5, 'EU', 'anchor-eu-5')")
	got1 := drainFilteredRegionChanges(t, ctx, ch1, 1, 60*time.Second)
	resumePos := got1[len(got1)-1].Pos()
	if resumePos.Token == "" {
		closeReader(r1)
		t.Fatalf("captured resume position is empty: %+v", resumePos)
	}
	closeReader(r1)
	t.Logf("captured resume position: %s", resumePos.Token)

	// --- Stream #2: the WARM RESUME — a fresh standalone reader, filters set,
	// StreamChanges from the persisted position. This is the F-P1 code path. ---
	r2, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader #2 (warm resume): %v", err)
	}
	setFilters(r2)
	ch2, err := r2.StreamChanges(ctx, resumePos)
	if err != nil {
		closeReader(r2)
		t.Fatalf("StreamChanges #2 (warm resume from %s): %v", resumePos.Token, err)
	}
	defer func() { closeReader(r2) }()
	// Settle so the post-resume DML lands inside the resumed CDC window.
	time.Sleep(3 * time.Second)

	// Post-resume mutations (source is ground truth):
	//   in-scope INSERT   : id=20 EU        -> arrives as ir.Insert
	//   out-of-scope INS  : id=21 US        -> NEVER arrives (server-side dropped on RESUME)
	//   move-OUT UPDATE   : id=20 EU -> US   -> ir.Update, before EU / after US
	applyClusterSQL(t, mysqlDSN, "INSERT INTO regions (id, region, payload) VALUES (20, 'EU', 'resume-eu-20')")
	applyClusterSQL(t, mysqlDSN, "INSERT INTO regions (id, region, payload) VALUES (21, 'US', 'resume-us-21')")
	applyClusterSQL(t, mysqlDSN, "UPDATE regions SET region = 'US', payload = 'moved-out-20' WHERE id = 20")

	// Collect row-bearing changes on the RESUMED stream up to and INCLUDING the
	// move-OUT UPDATE (id=20). Draining to the move-OUT (rather than a fixed
	// count) is robust to the resume legitimately REPLAYING an earlier in-scope
	// INSERT (VStream resume can re-deliver a change at/near the persisted
	// position) — an extra in-scope insert must not hide the move-OUT. If the
	// resume streamed the whole keyspace unfiltered (the pre-F-P1 behavior), the
	// out-of-scope INSERT id=21 (US) commits BEFORE the id=20 UPDATE, so it
	// would appear in this collection and the (b) assertion below would catch it.
	got := collectResumedRegionChanges(t, ctx, ch2, 20, 90*time.Second)

	// (b) SERVER-SIDE proof: the out-of-scope INSERT id=21 (US) must NEVER
	// appear on the resumed stream (it committed before the move-OUT we drained
	// to, so an unfiltered resume would have surfaced it here).
	for _, ch := range got {
		if ins, ok := ch.(ir.Insert); ok && ins.Table == "regions" {
			if id, _ := asInt64Val(ins.Row["id"]); id == 21 {
				t.Errorf("F-P1 REGRESSION: out-of-scope INSERT id=21 (US) reached the RESUMED reader — "+
					"the warm-resume stream was NOT filtered server-side: %#v", ins.Row)
			}
		}
	}

	// (a1) in-scope INSERT id=20 arrived with its in-scope region.
	sawInsert20 := false
	for _, ch := range got {
		if ins, ok := ch.(ir.Insert); ok && ins.Table == "regions" {
			if id, _ := asInt64Val(ins.Row["id"]); id == 20 {
				sawInsert20 = true
				if r := asStringVal(ins.Row["region"]); r != "EU" {
					t.Errorf("in-scope INSERT id=20 region = %q; want EU", r)
				}
			}
		}
	}
	if !sawInsert20 {
		t.Errorf("in-scope INSERT id=20 never arrived on the resumed stream among %d changes (%s)", len(got), changeKinds(got))
	}

	// (a2) move-OUT id=20 (EU->US): must arrive as an ir.Update carrying BOTH
	// images (before in-scope EU, after out-of-scope US) — the raw material
	// route() turns into a target DELETE-by-key. The before-image must carry
	// the filtered column so route() can classify the move-OUT.
	moveOut := findRegionUpdate(t, got, 20)
	if before := asStringVal(moveOut.Before["region"]); before != "EU" {
		t.Errorf("move-OUT id=20 before.region = %q; want EU (the in-scope OLD image the DELETE-by-key needs)", before)
	}
	if after := asStringVal(moveOut.After["region"]); after != "US" {
		t.Errorf("move-OUT id=20 after.region = %q; want US", after)
	}
	if _, ok := moveOut.Before["region"]; !ok {
		t.Errorf("move-OUT id=20 before-image omits the filtered column `region` — route() could not classify the move-OUT (leak)")
	}

	if err := r2.(interface{ Err() error }).Err(); err != nil {
		t.Fatalf("resumed filtered CDC stream errored: %v", err)
	}
	t.Log("warm-resume filtered CDC PASS: resumed from a persisted position; in-scope INSERT + move-OUT UPDATE (both images) delivered; out-of-scope INSERT dropped SERVER-SIDE on resume (F-P1 fixed)")
}

// collectResumedRegionChanges drains row-bearing changes on `regions` until it
// observes an ir.Update whose Before or After id is untilUpdateID (the move-OUT
// we drive last), returning EVERYTHING collected up to and including it. Unlike
// the fixed-count drain, it tolerates the warm resume replaying an extra
// in-scope INSERT near the persisted position without hiding the move-OUT — and
// because the out-of-scope INSERT commits before that move-OUT, an unfiltered
// resume would necessarily surface it in the returned slice.
func collectResumedRegionChanges(t *testing.T, ctx context.Context, changes <-chan ir.Change, untilUpdateID int64, timeout time.Duration) []ir.Change {
	t.Helper()
	var got []ir.Change
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case ch, ok := <-changes:
			if !ok {
				t.Fatalf("resumed stream closed before the move-OUT update id=%d (%s)", untilUpdateID, changeKinds(got))
			}
			switch e := ch.(type) {
			case ir.Insert, ir.Delete:
				got = append(got, ch)
			case ir.Update:
				got = append(got, ch)
				bid, _ := asInt64Val(e.Before["id"])
				aid, _ := asInt64Val(e.After["id"])
				if bid == untilUpdateID || aid == untilUpdateID {
					return got
				}
			default:
				// Tx boundaries / schema snapshots — ignored.
			}
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for the move-OUT update id=%d (%s)", timeout, untilUpdateID, changeKinds(got))
		case <-ctx.Done():
			t.Fatalf("context done waiting for move-OUT id=%d: %v", untilUpdateID, ctx.Err())
		}
	}
}

// closeReader closes a standalone CDC reader (Close is on the concrete
// *vstreamCDCReader, not the ir.CDCReader interface).
func closeReader(r ir.CDCReader) {
	if c, ok := r.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// drainFilteredRegionChanges collects row-bearing changes on `regions` until
// it has `want` of them or the timeout / context fires. Unlike the NOBLOB
// belt drain (which waits for the stream to CLOSE on a refusal), a healthy
// filtered stream stays open and keeps tailing, so we stop at the expected
// count. Non-row events (Tx boundaries, heartbeats surfaced as nothing) are
// ignored.
func drainFilteredRegionChanges(t *testing.T, ctx context.Context, changes <-chan ir.Change, want int, timeout time.Duration) []ir.Change {
	t.Helper()
	var got []ir.Change
	rowBearing := 0
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for rowBearing < want {
		select {
		case ch, ok := <-changes:
			if !ok {
				t.Fatalf("filtered stream closed early after %d row-bearing changes; want %d (%s)", rowBearing, want, changeKinds(got))
			}
			switch ch.(type) {
			case ir.Insert, ir.Update, ir.Delete:
				got = append(got, ch)
				rowBearing++
			default:
				// Tx boundaries / schema snapshots — not part of the count.
			}
		case <-deadline.C:
			t.Fatalf("timed out after %v with %d/%d row-bearing changes (%s)", timeout, rowBearing, want, changeKinds(got))
		case <-ctx.Done():
			t.Fatalf("context done draining changes (%d/%d): %v", rowBearing, want, ctx.Err())
		}
	}
	return got
}

// findRegionUpdate returns the ir.Update on `regions` for the given id, or
// fails. It is the move-IN / move-OUT locator (both arrive as UPDATEs with
// both images under a non-vindex filter).
func findRegionUpdate(t *testing.T, got []ir.Change, id int64) ir.Update {
	t.Helper()
	for _, ch := range got {
		if upd, ok := ch.(ir.Update); ok && upd.Table == "regions" {
			if bid, _ := asInt64Val(upd.Before["id"]); bid == id {
				return upd
			}
			if aid, _ := asInt64Val(upd.After["id"]); aid == id {
				return upd
			}
		}
	}
	t.Fatalf("no ir.Update on regions for id=%d among %d changes (%s)", id, len(got), changeKinds(got))
	return ir.Update{}
}
