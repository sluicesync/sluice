//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item 114 — the segmented, replayable LOAD DATA cold-copy path, against a
// real MySQL. Four things need a real server and cannot be faked:
//
//  1. THE PREMISE. The whole exactly-once argument rests on an
//     environmental fact — LOAD DATA *LOCAL* downgrades a duplicate-key
//     error to a warning and SKIPS the row. If that ever stops being true,
//     the replay stops converging, so the fact gets its own test rather
//     than a comment (CLAUDE.md's premise-naming step).
//  2. THE GATE. A copy interrupted mid-table resumes from its SEGMENT
//     boundary and lands the table exactly once.
//  3. THE CONTROL. An uninterrupted segmented copy is identical to the
//     pre-item-114 single-statement copy — for every value family, and with
//     a segment boundary between every pair of rows.
//  4. THE COST. What the segmentation does to throughput, measured.
//
// To run:
//   go test -tags=integration ./internal/engines/mysql/ -run LoadData

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// ---------------------------------------------------------------------
// 1. The premise
// ---------------------------------------------------------------------

// TestLoadDataLocal_DuplicateKeyIsAWarningAndSkipsTheRow pins the
// environmental fact item 114's replay safety is built on. Named in
// [RowWriter.writeLoadData]'s doc; if MySQL ever made this an error (or, far
// worse, made it overwrite), the replay would stop being exactly-once and
// this test is what says so.
//
// The oracle is the SERVER's own accounting — affected-rows, the warning
// list, and a post-load COUNT(*) — never the writer's bookkeeping.
func TestLoadDataLocal_DuplicateKeyIsAWarningAndSkipsTheRow(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applyDDL(t, dsn, loadDataPinDDL("dup_premise"))
	db := openTestDB(t, dsn)
	defer db.Close()

	first := rawLoadData(ctx, t, db, "dup_premise", "1\ta\n2\tb\n3\tc\n")
	if first.err != nil {
		t.Fatalf("first load: %v", first.err)
	}
	if first.affected != 3 || first.visibleWarnings != 0 {
		t.Fatalf("first load: affected=%d warnings=%d; want 3, 0", first.affected, first.visibleWarnings)
	}

	// The committed-but-unacked shape: the identical bytes, again.
	replay := rawLoadData(ctx, t, db, "dup_premise", "1\ta\n2\tb\n3\tc\n")
	if replay.err != nil {
		t.Fatalf("PREMISE BROKEN: a byte-identical LOAD DATA LOCAL replay must not ERROR on the duplicate "+
			"keys — item 114's retry depends on it being a warning: %v", replay.err)
	}
	if replay.affected != 0 {
		t.Errorf("replay inserted %d rows; want 0 (every row is a duplicate and must be skipped)", replay.affected)
	}
	if replay.visibleWarnings != 3 || replay.nonDupWarnings != 0 {
		t.Errorf("replay warnings = %d (%d non-duplicate); want 3 duplicate-key warnings",
			replay.visibleWarnings, replay.nonDupWarnings)
	}

	// Partial prior commit: rows 2,3 already there, 4,5 are new.
	partial := rawLoadData(ctx, t, db, "dup_premise", "2\tb\n3\tc\n4\td\n5\te\n")
	if partial.err != nil {
		t.Fatalf("partial-overlap load: %v", partial.err)
	}
	if partial.affected != 2 {
		t.Errorf("partial-overlap load inserted %d rows; want 2 (only the missing ones)", partial.affected)
	}

	// The property that matters: convergence. Five distinct rows, one copy each.
	if n := scalarInt(ctx, t, db, "SELECT COUNT(*) FROM dup_premise"); n != 5 {
		t.Errorf("COUNT(*) = %d; want 5 — replays must converge, never duplicate", n)
	}
	if s := scalarInt(ctx, t, db, "SELECT SUM(id) FROM dup_premise"); s != 15 {
		t.Errorf("SUM(id) = %d; want 15", s)
	}
}

// TestLoadDataLocal_TrueWarningCountSurvivesShowWarnings pins the ordering
// contract [readTrueWarningCount] documents: SHOW WARNINGS consumes the
// detail list WITHOUT clearing @@warning_count, so the uncapped total is
// still readable afterwards. Reverse the two and the accounting silently
// reads zero — which would make every replay look clean.
func TestLoadDataLocal_TrueWarningCountSurvivesShowWarnings(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applyDDL(t, dsn, loadDataPinDDL("count_premise"))
	db := openTestDB(t, dsn)
	defer db.Close()

	payload := numberedTSV(1, 3000)
	if r := rawLoadData(ctx, t, db, "count_premise", payload); r.err != nil {
		t.Fatalf("seed load: %v", r.err)
	}
	replay := rawLoadData(ctx, t, db, "count_premise", payload)
	if replay.err != nil {
		t.Fatalf("replay load: %v", replay.err)
	}
	if replay.trueWarnings != 3000 {
		t.Errorf("@@warning_count after SHOW WARNINGS = %d; want 3000 (the UNCAPPED total)", replay.trueWarnings)
	}
	if replay.visibleWarnings >= 3000 {
		t.Errorf("SHOW WARNINGS returned %d rows; expected it to be CAPPED at @@max_error_count — "+
			"if the server stopped capping, the accounting is still correct but its motivating case is gone",
			replay.visibleWarnings)
	}
}

// TestLoadDataLocal_TruncationHidesBehindDuplicateWarnings is why
// [readTrueWarningCount] exists at all. A replay of 3000 already-committed
// rows plus ONE genuinely truncating row produces 3001 warnings, of which
// the server shows only the first @@max_error_count — all of them 1062. The
// sampled list therefore says "nothing but duplicates" while a value has
// been silently coerced; only the count arithmetic catches it.
func TestLoadDataLocal_TruncationHidesBehindDuplicateWarnings(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applyDDL(t, dsn, loadDataPinDDL("hidden_trunc"))
	db := openTestDB(t, dsn)
	defer db.Close()

	seed := numberedTSV(1, 3000)
	if r := rawLoadData(ctx, t, db, "hidden_trunc", seed); r.err != nil {
		t.Fatalf("seed load: %v", r.err)
	}
	// v is VARCHAR(16); 200 x's truncate.
	mixed := seed + "99999\t" + strings.Repeat("x", 200) + "\n"
	replay := rawLoadData(ctx, t, db, "hidden_trunc", mixed)
	if replay.err != nil {
		t.Fatalf("mixed replay: %v", replay.err)
	}

	if replay.nonDupWarnings != 0 {
		t.Fatalf("this test's premise is that the truncation is INVISIBLE in the sampled list; "+
			"it showed %d non-duplicate warnings, so the hazard shape has changed", replay.nonDupWarnings)
	}
	// The decision the writer would reach on this exact input.
	if replayWarningsAreOnlyDuplicates(3001, replay.affected, replay.trueWarnings, replay.nonDupWarnings) {
		t.Errorf("a segment whose replay silently truncated a value was classified as harmless duplicates "+
			"(segRows=3001 affected=%d totalWarnings=%d visibleNonDup=%d) — this is the silent-coercion path "+
			"the accounting exists to close", replay.affected, replay.trueWarnings, replay.nonDupWarnings)
	}
}

// ---------------------------------------------------------------------
// 2. The gate
// ---------------------------------------------------------------------

// TestLoadDataSegments_InterruptedCopyResumesFromItsSegmentBoundary is item
// 114's reason for existing. A cold copy is interrupted part-way through a
// table by a classified transient injected AFTER the segment's statement has
// already committed — the committed-but-unacked shape, the one a killed
// connection cannot reproduce on demand and the one a naive replay would
// duplicate.
//
// Three independent oracles, none of them the writer's own return value:
//
//   - the target's contents (every source row present exactly once);
//   - Com_load, the server's own count of LOAD DATA statements executed,
//     which shows the replay re-sent ONE segment and not the table;
//   - the per-flush hook, which shows how many logical segments there were.
func TestLoadDataSegments_InterruptedCopyResumesFromItsSegmentBoundary(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, loadDataPinDDL("interrupted"))
	table := readPinTable(ctx, t, dsn, "interrupted")

	const rowCount = 400
	// ~14 bytes/row ⇒ 50 rows per segment ⇒ 8 segments.
	withSmallLoadDataSegments(t, 700)
	withFastReparentBackoff(t, 12)

	var attempts, replays int
	loadDataSegmentFailHookForTest = func(segment int, replay bool, phase loadDataSegmentPhase) error {
		if phase != loadDataAfterExec {
			return nil
		}
		attempts++
		if replay {
			replays++
			return nil
		}
		// Interrupt the 3rd segment, AFTER its rows are committed.
		if segment == 3 {
			return vttabletUnavailable()
		}
		return nil
	}
	defer func() { loadDataSegmentFailHookForTest = nil }()

	var flushes []int
	bulkFlushHookForTest = func(rows int, _ int64) { flushes = append(flushes, rows) }
	defer func() { bulkFlushHookForTest = nil }()

	db := openTestDB(t, dsn)
	defer db.Close()
	comLoadBefore := scalarInt(ctx, t, db, "SHOW GLOBAL STATUS LIKE 'Com_load'")

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	mustBeLoadData(t, rw)

	if err := rw.WriteRows(ctx, table, numberedRows(rowCount)); err != nil {
		t.Fatalf("an interrupted copy must ride the transient and finish; got: %v", err)
	}

	if replays != 1 {
		t.Errorf("replayed segment attempts = %d; want exactly 1", replays)
	}
	// Oracle 1 — the data. Exactly-once, not at-least-once.
	if n := scalarInt(ctx, t, db, "SELECT COUNT(*) FROM interrupted"); n != rowCount {
		t.Errorf("COUNT(*) = %d; want %d — a replay that duplicated rows would read HIGHER, "+
			"one that lost its segment LOWER", n, rowCount)
	}
	wantSum := int64(rowCount) * int64(rowCount-1) / 2
	if s := scalarInt(ctx, t, db, "SELECT SUM(id) FROM interrupted"); s != wantSum {
		t.Errorf("SUM(id) = %d; want %d — the row SET must be exactly the source's", s, wantSum)
	}
	if n := scalarInt(ctx, t, db, "SELECT COUNT(*) FROM interrupted WHERE v <> CONCAT('v', id)"); n != 0 {
		t.Errorf("%d rows carry the wrong value; the replay must not have shifted the stream", n)
	}

	// Oracle 2 — the server's statement counter. 8 segments + 1 replay.
	comLoadAfter := scalarInt(ctx, t, db, "SHOW GLOBAL STATUS LIKE 'Com_load'")
	if got, want := comLoadAfter-comLoadBefore, int64(len(flushes)+1); got != want {
		t.Errorf("server executed %d LOAD DATA statements; want %d (%d segments + 1 replayed segment). "+
			"A whole-table restart would be far more, and no resume at all would be fewer",
			got, want, len(flushes))
	}
	// Oracle 3 — the copy really was segmented, not one statement (which is
	// what makes the replay cheap). Anti-vacuity: a single-segment copy
	// would satisfy the counters above trivially.
	if len(flushes) < 4 {
		t.Errorf("logical segments = %d; want >= 4 — with one segment this test proves nothing", len(flushes))
	}
	total := 0
	for _, n := range flushes {
		total += n
	}
	if total != rowCount {
		t.Errorf("segments carried %d rows in total; want %d", total, rowCount)
	}
}

// TestLoadDataSegments_RolledBackAttemptIsReplayedInFull is the other half
// of the transient's outcome space, and the half that proves the replay
// carries the DATA. The failpoint fires BEFORE the statement runs, so
// segment 2 never lands on the first attempt — its rows exist only in the
// replay buffer. If the replay re-drove an empty or stale stream, those rows
// would be missing here, whereas the committed-but-unacked test above would
// not notice (its rows are already on the server).
func TestLoadDataSegments_RolledBackAttemptIsReplayedInFull(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, loadDataPinDDL("rolled_back"))
	table := readPinTable(ctx, t, dsn, "rolled_back")

	const rowCount = 200
	withSmallLoadDataSegments(t, 700) // ~50 rows per segment
	withFastReparentBackoff(t, 12)

	loadDataSegmentFailHookForTest = func(segment int, replay bool, phase loadDataSegmentPhase) error {
		if segment == 2 && !replay && phase == loadDataBeforeExec {
			return &driver.MySQLError{Number: 2013, Message: "invalid connection (errno 2013)"}
		}
		return nil
	}
	defer func() { loadDataSegmentFailHookForTest = nil }()

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	if err := rw.WriteRows(ctx, table, numberedRows(rowCount)); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	db := openTestDB(t, dsn)
	defer db.Close()
	if n := scalarInt(ctx, t, db, "SELECT COUNT(*) FROM rolled_back"); n != rowCount {
		t.Errorf("COUNT(*) = %d; want %d — the never-executed segment's rows come ONLY from the replay buffer", n, rowCount)
	}
	wantSum := int64(rowCount) * int64(rowCount-1) / 2
	if s := scalarInt(ctx, t, db, "SELECT SUM(id) FROM rolled_back"); s != wantSum {
		t.Errorf("SUM(id) = %d; want %d", s, wantSum)
	}
	if n := scalarInt(ctx, t, db, "SELECT COUNT(*) FROM rolled_back WHERE v <> CONCAT('v', id)"); n != 0 {
		t.Errorf("%d rows carry the wrong value — the replayed bytes must be the segment's own", n)
	}
}

// TestLoadDataSegments_ReplayRefusesWhenACoercionHidesAmongTheDuplicates is
// the silent-corruption gate on the replay tolerance, driven END TO END
// through the writer rather than through the classifier function.
//
// This test exists because the pure-function matrix did NOT catch a mutation
// that made the writer stop consulting the classifier at all: both the unit
// matrix and the max_error_count pin call replayWarningsAreOnlyDuplicates
// directly, so `if false {` at the call site passed them both. That is the
// "gate narrower than its name" shape — the pins covered the DECISION and
// nothing covered the WIRING.
//
// The segment carries more rows than @@max_error_count so every warning the
// server will SHOW is a duplicate-key one, and the single truncation sits
// outside that window. Only the @@warning_count arithmetic can see it.
func TestLoadDataSegments_ReplayRefusesWhenACoercionHidesAmongTheDuplicates(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE hidden_replay (
			id BIGINT     NOT NULL,
			v  VARCHAR(8) NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	table := readPinTable(ctx, t, dsn, "hidden_replay")

	db := openTestDB(t, dsn)
	defer db.Close()
	const rowCount = 2100
	if cap := scalarInt(ctx, t, db, "SELECT @@max_error_count"); cap >= rowCount {
		t.Skipf("@@max_error_count is %d; this test needs it BELOW the segment's row count (%d) so the "+
			"truncation is outside the visible warning window", cap, rowCount)
	}

	// One segment holding every row, interrupted AFTER it committed.
	withSmallLoadDataSegments(t, 1<<20)
	withFastReparentBackoff(t, 12)
	loadDataSegmentFailHookForTest = func(_ int, replay bool, phase loadDataSegmentPhase) error {
		if !replay && phase == loadDataAfterExec {
			return vttabletUnavailable()
		}
		return nil
	}
	defer func() { loadDataSegmentFailHookForTest = nil }()

	rows := make(chan ir.Row, rowCount)
	for i := 0; i < rowCount; i++ {
		v := "ok"
		if i == rowCount-1 {
			v = strings.Repeat("x", 50) // VARCHAR(8) — a silent truncation
		}
		rows <- ir.Row{"id": int64(i), "v": v}
	}
	close(rows)

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	err := rw.WriteRows(ctx, table, rows)
	if err == nil {
		t.Fatal("the replayed segment silently coerced a value and the copy exited 0 — the duplicate-key " +
			"tolerance must not swallow a warning the duplicates cannot account for")
	}
	if !strings.Contains(err.Error(), "warning") {
		t.Errorf("refusal should name the warnings (Bugs 102/103 wording); got: %v", err)
	}
}

// TestLoadDataSegments_KeylessInterruptedCopyRefusesLoudly is the keyless
// carve-out at the real-server level. LOAD DATA LOCAL's duplicate-key skip
// is exactly what makes the keyed replay convergent — a table with no key
// has nothing to collide on, so its replay would DOUBLE the segment. The
// refusal is inherited from flushWithReparentRetry; this proves the
// inheritance against a real server, and that no rows were doubled.
func TestLoadDataSegments_KeylessInterruptedCopyRefusesLoudly(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE keyless_copy (
			id BIGINT      NOT NULL,
			v  VARCHAR(16) NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	table := readPinTable(ctx, t, dsn, "keyless_copy")

	withSmallLoadDataSegments(t, 700)
	withFastReparentBackoff(t, 12)
	loadDataSegmentFailHookForTest = func(segment int, replay bool, phase loadDataSegmentPhase) error {
		if segment == 2 && !replay && phase == loadDataAfterExec {
			return vttabletUnavailable()
		}
		return nil
	}
	defer func() { loadDataSegmentFailHookForTest = nil }()

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	err := rw.WriteRows(ctx, table, numberedRows(200))
	if err == nil {
		t.Fatal("a keyless table's interrupted copy must REFUSE rather than replay a segment it cannot dedupe")
	}
	if !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Errorf("refusal must name the remedy; got: %v", err)
	}

	db := openTestDB(t, dsn)
	defer db.Close()
	// Segment 1 landed and segment 2 landed once (the injection is post-exec)
	// — the point is that NOTHING was doubled.
	dupes := scalarInt(ctx, t, db, "SELECT COUNT(*) FROM (SELECT id FROM keyless_copy GROUP BY id HAVING COUNT(*) > 1) d")
	if dupes != 0 {
		t.Errorf("%d ids appear more than once — the refusal exists precisely to prevent this", dupes)
	}
}

// ---------------------------------------------------------------------
// 3. The control
// ---------------------------------------------------------------------

// TestLoadDataSegments_SegmentedCopyMatchesTheSingleStatementCopy is the
// no-regression floor: for a corpus covering every value family the TSV
// encoder dispatches on, a segmented copy and the pre-item-114
// one-statement-per-table copy produce IDENTICAL tables.
//
// Bug-74 discipline. [encodeRowsTSV] dispatches on the value's type family,
// and item 114 changed its loop and introduced a new shape variant — the
// segment boundary. So the matrix is every family × {mid-segment, first row
// of a segment, last row of a segment}, and the cheapest way to cover the
// boundary column exhaustively is a 1-byte budget: EVERY row is then both
// the first and the last row of its own segment.
//
// The comparison oracle is the server's own CHECKSUM TABLE plus a full
// column-by-column readback — not a re-encode by the writer under test.
func TestLoadDataSegments_SegmentedCopyMatchesTheSingleStatementCopy(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	variants := []struct {
		table  string
		budget int64
		what   string
	}{
		{"fam_single", 1 << 40, "one statement for the whole table (the pre-item-114 shape)"},
		{"fam_perrow", 1, "one row per segment — every row sits on a boundary"},
		{"fam_mixed", 400, "a few rows per segment"},
	}
	for _, v := range variants {
		applyDDL(t, dsn, familyCorpusDDL(v.table))
	}

	for _, v := range variants {
		table := readPinTable(ctx, t, dsn, v.table)
		withSmallLoadDataSegments(t, v.budget)
		rw := openRowWriter(t, ctx, dsn)
		mustBeLoadData(t, rw)
		if err := rw.WriteRows(ctx, table, familyCorpusRows()); err != nil {
			closeIf(rw)
			t.Fatalf("%s (%s): WriteRows: %v", v.table, v.what, err)
		}
		closeIf(rw)
	}

	db := openTestDB(t, dsn)
	defer db.Close()

	want := readRowsRaw(t, dsn, "SELECT * FROM fam_single ORDER BY id")
	if len(want) != len(familyCorpusValues()) {
		t.Fatalf("control table has %d rows; want %d", len(want), len(familyCorpusValues()))
	}
	baseCk := scalarInt(ctx, t, db, "CHECKSUM TABLE fam_single")
	for _, v := range variants[1:] {
		if ck := scalarInt(ctx, t, db, "CHECKSUM TABLE "+v.table); ck != baseCk {
			t.Errorf("%s (%s): CHECKSUM TABLE = %d; want %d (the single-statement copy) — "+
				"segmentation must not change a single byte", v.table, v.what, ck, baseCk)
		}
		got := readRowsRaw(t, dsn, "SELECT * FROM "+v.table+" ORDER BY id")
		if len(got) != len(want) {
			t.Errorf("%s: %d rows; want %d", v.table, len(got), len(want))
			continue
		}
		for i := range want {
			for col, wv := range want[i] {
				if !valEqual(got[i][col], wv) {
					t.Errorf("%s row[%d] col %q = %#v; want %#v (%s)", v.table, i, col, got[i][col], wv, v.what)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------
// 4. The cost
// ---------------------------------------------------------------------

// TestLoadDataSegments_ThroughputCost measures what item 114 costs, and
// pins the MODEL of that cost rather than an absolute ratio.
//
// The model, established by the floor measurement below: splitting a table
// into N segments adds exactly N−1 extra TRANSACTION COMMITS on the target
// and nothing else. Everything else is unchanged — same rows, same bytes,
// same 16 KiB driver packets, and the added per-segment SHOW WARNINGS probe
// is a sub-millisecond round trip.
//
// Measured on this development box (Docker-on-Windows, MySQL 8.0,
// innodb_flush_log_at_trx_commit=1, log_bin=1 — a pathologically slow fsync
// environment):
//
//	1-row LOAD DATA   53.4 ms   \_ identical, so the cost is the COMMIT,
//	1-row INSERT      52.4 ms   /  not anything LOAD DATA does
//	SELECT 1           0.6 ms      (pure round trip)
//	SHOW WARNINGS      0.35 ms     (what item 114 adds per segment)
//
// and end-to-end over a 7.2 MiB corpus, best of 3 interleaved rounds:
// 8 MiB budget 0.99x, 4 MiB 1.09x, 2 MiB 1.19x, 1 MiB 1.46x, 256 KiB 2.59x
// — i.e. linear in the segment COUNT, exactly as the model says, and within
// noise at the shipping budget. For scale: the batched INSERT core this same
// writer falls back to commits every ~1 MiB, so a 16 MiB-segmented LOAD DATA
// still commits 16x LESS often than the alternative write path.
//
// An absolute-ratio assertion was tried first and rejected: successive
// tens-of-MiB loads hit a progressively dirtier InnoDB buffer pool, so the
// SAME single-statement form measured 2.59 s in load position 1 and 6.07 s
// in position 4 — a straight A-then-B comparison reported a fictitious 2.2x
// regression that was pure ordering. The model-based bound below is
// independent of how fast the box is.
func TestLoadDataSegments_ThroughputCost(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput measurement; skipped under -short")
	}
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// (a) The per-statement floor, measured with the LOAD DATA path OUT of
	// the picture: a one-row INSERT. This is the independent expected value
	// the segmented copy's overhead is compared against below.
	commitFloor, loadFloor := measureStatementFloors(ctx, t, dsn)
	t.Logf("per-statement floor: 1-row INSERT %v | 1-row LOAD DATA %v", commitFloor, loadFloor)
	if loadFloor > 2*commitFloor {
		t.Errorf("a one-row LOAD DATA costs %v against %v for a one-row INSERT. The segmentation model "+
			"assumes an extra segment costs one ordinary commit; if LOAD DATA carries a per-statement "+
			"cost of its own, the segment budget needs re-deriving", loadFloor, commitFloor)
	}

	// (b) End-to-end. The budget is deliberately far below shipping so the
	// per-segment cost is maximally visible on a small, fast corpus.
	const (
		rowCount    = 30000 // ~7.2 MiB at this width
		width       = 240
		smallBudget = 1 << 20
		rounds      = 3
	)
	corpusBytes := float64(rowCount) * float64(width+12)
	segments := corpusBytes / float64(smallBudget)

	var single, segmented time.Duration
	for r := 0; r < rounds; r++ {
		applyDDL(t, dsn, loadDataPerfDDL(fmt.Sprintf("perf_single_%d", r)))
		applyDDL(t, dsn, loadDataPerfDDL(fmt.Sprintf("perf_seg_%d", r)))
		s := timeLoadDataCopy(ctx, t, dsn, fmt.Sprintf("perf_single_%d", r), 1<<40, rowCount, width)
		g := timeLoadDataCopy(ctx, t, dsn, fmt.Sprintf("perf_seg_%d", r), smallBudget, rowCount, width)
		t.Logf("round %d: single %v | %d MiB segments %v", r, s.Round(time.Millisecond),
			smallBudget>>20, g.Round(time.Millisecond))
		if single == 0 || s < single {
			single = s
		}
		if segmented == 0 || g < segmented {
			segmented = g
		}
	}
	overhead := segmented - single
	t.Logf("item 114 throughput (best of %d), %.1f MiB corpus: single statement %v | ~%.0f segments %v "+
		"| overhead %v = %v per segment (one-row-INSERT floor is %v) | ratio %.2fx",
		rounds, corpusBytes/(1<<20), single.Round(time.Millisecond), segments,
		segmented.Round(time.Millisecond), overhead.Round(time.Millisecond),
		(overhead / time.Duration(segments)).Round(time.Millisecond), commitFloor, segmented.Seconds()/single.Seconds())

	// The bound is the model, not a wall-clock number: N extra segments may
	// cost N extra commits (×2 slack for the noise a shared runner adds).
	// Anything beyond that means segmentation is paying for something it was
	// not supposed to — a per-row round trip, a lost buffer, a re-encode.
	if budget := 2 * time.Duration(segments) * commitFloor; overhead > budget {
		t.Errorf("segmenting the copy into ~%.0f segments cost %v over the single-statement form; the model "+
			"says it should cost about %.0f extra commits (~%v, doubled to %v for noise). Segmentation is "+
			"paying for something other than the extra commits",
			segments, overhead, segments, time.Duration(segments)*commitFloor, budget)
	}
}

// measureStatementFloors times a one-row INSERT and a one-row LOAD DATA on a
// pinned connection — the per-statement cost of each on THIS target,
// independent of any corpus.
func measureStatementFloors(ctx context.Context, t *testing.T, dsn string) (insert, load time.Duration) {
	t.Helper()
	applyDDL(t, dsn, loadDataPerfDDL("floor_insert"))
	applyDDL(t, dsn, loadDataPerfDDL("floor_load"))
	db := openTestDB(t, dsn)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	defer conn.Close()

	const n = 100
	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := conn.ExecContext(ctx, "INSERT INTO floor_insert (id, v) VALUES (?, ?)", i, "v"); err != nil {
			t.Fatalf("floor INSERT %d: %v", i, err)
		}
	}
	insert = time.Since(start) / n

	name, err := mintReaderName()
	if err != nil {
		t.Fatalf("mintReaderName: %v", err)
	}
	var payload []byte
	driver.RegisterReaderHandler(name, func() io.Reader { return bytes.NewReader(payload) })
	defer driver.DeregisterReaderHandler(name)
	stmt := buildLoadDataStmt("", "floor_load", []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "v", Type: ir.Varchar{Length: 255}},
	}, name)
	start = time.Now()
	for i := 0; i < n; i++ {
		payload = []byte(strconv.Itoa(i) + "\tv\n")
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("floor LOAD DATA %d: %v", i, err)
		}
	}
	load = time.Since(start) / n
	return insert, load
}

func timeLoadDataCopy(ctx context.Context, t *testing.T, dsn, tableName string, budget int64, rows, width int) time.Duration {
	t.Helper()
	table := readPinTable(ctx, t, dsn, tableName)
	withSmallLoadDataSegments(t, budget)
	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	mustBeLoadData(t, rw)
	start := time.Now()
	if err := rw.WriteRows(ctx, table, wideRows(rows, width)); err != nil {
		t.Fatalf("%s: WriteRows: %v", tableName, err)
	}
	return time.Since(start)
}

func loadDataPerfDDL(name string) string {
	return fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT       NOT NULL,
			v  VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, name)
}

func wideRows(n, width int) <-chan ir.Row {
	pad := strings.Repeat("x", width)
	ch := make(chan ir.Row, 1024)
	go func() {
		defer close(ch)
		for i := 0; i < n; i++ {
			ch <- ir.Row{"id": int64(i), "v": pad}
		}
	}()
	return ch
}

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

func loadDataPinDDL(name string) string {
	return fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT      NOT NULL,
			v  VARCHAR(16) NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, name)
}

// familyCorpusDDL covers every value family the TSV encoder dispatches on.
func familyCorpusDDL(name string) string {
	return fmt.Sprintf(`
		CREATE TABLE %s (
			id       BIGINT           NOT NULL,
			i_sig    INT              NULL,
			i_uns    BIGINT UNSIGNED  NULL,
			f_dbl    DOUBLE           NULL,
			f_flt    FLOAT            NULL,
			d_dec    DECIMAL(20,6)    NULL,
			b_bool   TINYINT(1)       NULL,
			s_var    VARCHAR(255)     NULL,
			s_txt    TEXT             NULL,
			bin_var  VARBINARY(255)   NULL,
			bin_blob BLOB             NULL,
			j_doc    JSON             NULL,
			e_enum   ENUM('a','b','c') NULL,
			s_set    SET('go','sql','mysql') NULL,
			bt       BIT(8)           NULL,
			d_date   DATE             NULL,
			d_dt     DATETIME(6)      NULL,
			d_ts     TIMESTAMP(6)     NULL,
			d_time   TIME             NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, name)
}

// familyCorpusValues is the row set: one row per interesting shape within
// each family, so that with a 1-byte segment budget every one of them sits
// on a segment boundary.
func familyCorpusValues() []ir.Row {
	ts := time.Date(2026, 8, 5, 13, 14, 15, 123456000, time.UTC)
	base := func(id int64) ir.Row {
		return ir.Row{
			"id": id, "i_sig": int64(-42), "i_uns": uint64(18446744073709551615),
			"f_dbl": 1.7976931348623157e308, "f_flt": float32(3.5), "d_dec": "12345678901234.567890",
			"b_bool": true, "s_var": "plain", "s_txt": "text", "bin_var": []byte{0x00, 0xff},
			"bin_blob": []byte("blob"), "j_doc": []byte(`{"k":"v"}`), "e_enum": "b",
			"s_set": []string{"go", "sql"}, "bt": "10101010", "d_date": "2026-08-05",
			"d_dt": ts, "d_ts": ts, "d_time": "13:14:15",
		}
	}
	rows := []ir.Row{base(1)}

	// NULL in every nullable family.
	nulls := base(2)
	for k := range nulls {
		if k != "id" {
			nulls[k] = nil
		}
	}
	rows = append(rows, nulls)

	// Escape-bearing strings and bytes (the TSV framing bytes themselves).
	esc := base(3)
	esc["s_var"] = "tab\there\nnewline\rcr\\backslash"
	esc["s_txt"] = "\x00nul\x00"
	esc["bin_var"] = []byte{'\t', '\n', '\r', '\\', 0x00, 0xde, 0xad}
	esc["bin_blob"] = []byte{}
	rows = append(rows, esc)

	// Boundary numerics and empty strings.
	edge := base(4)
	edge["i_sig"] = int64(-2147483648)
	edge["i_uns"] = uint64(0)
	edge["f_dbl"] = float64(-0)
	edge["f_flt"] = float32(0)
	edge["d_dec"] = "-99999999999999.999999"
	edge["b_bool"] = false
	edge["s_var"] = ""
	edge["s_txt"] = ""
	edge["bt"] = "00000000"
	edge["s_set"] = []string{}
	rows = append(rows, edge)

	// Unicode + a large JSON document + a wide value that dwarfs any budget.
	wide := base(5)
	wide["s_var"] = "héllo — ünïcode ✅"
	wide["s_txt"] = strings.Repeat("wide ", 2000)
	wide["j_doc"] = []byte(`{"nested":{"a":[1,2,3],"b":null},"s":"ünïcode"}`)
	rows = append(rows, wide)

	// A second dense row so the mixed-budget variant has >1 row per segment.
	rows = append(rows, base(6), base(7), base(8))
	return rows
}

func familyCorpusRows() <-chan ir.Row {
	vals := familyCorpusValues()
	ch := make(chan ir.Row, len(vals))
	for _, r := range vals {
		ch <- r
	}
	close(ch)
	return ch
}

func numberedRows(n int) <-chan ir.Row {
	ch := make(chan ir.Row, 512)
	go func() {
		defer close(ch)
		for i := 0; i < n; i++ {
			ch <- ir.Row{"id": int64(i), "v": "v" + strconv.Itoa(i)}
		}
	}()
	return ch
}

func numberedTSV(from, count int) string {
	var b strings.Builder
	for i := from; i < from+count; i++ {
		b.WriteString(strconv.Itoa(i))
		b.WriteString("\tv\n")
	}
	return b.String()
}

// rawLoadDataResult is what one hand-built LOAD DATA statement reported.
type rawLoadDataResult struct {
	err             error
	affected        int64
	visibleWarnings int
	nonDupWarnings  int
	trueWarnings    int64
}

// rawLoadData runs one LOAD DATA LOCAL statement with the writer's own
// statement form and reports the server's accounting. Used by the premise
// pins, which must observe MySQL directly rather than through the writer.
func rawLoadData(ctx context.Context, t *testing.T, db *sql.DB, table, payload string) rawLoadDataResult {
	t.Helper()
	name, err := mintReaderName()
	if err != nil {
		t.Fatalf("mintReaderName: %v", err)
	}
	driver.RegisterReaderHandler(name, func() io.Reader { return bytes.NewReader([]byte(payload)) })
	defer driver.DeregisterReaderHandler(name)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	defer conn.Close()

	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "v", Type: ir.Varchar{Length: 16}},
	}
	res, execErr := conn.ExecContext(ctx, buildLoadDataStmt("", table, cols, name))
	out := rawLoadDataResult{err: execErr}
	if execErr != nil {
		return out
	}
	out.affected, _ = res.RowsAffected()
	sw, err := readShowWarnings(ctx, conn, table)
	if err != nil {
		t.Fatalf("readShowWarnings: %v", err)
	}
	out.visibleWarnings, out.nonDupWarnings = sw.Visible, sw.NonDup
	if out.visibleWarnings > 0 {
		if out.trueWarnings, err = readTrueWarningCount(ctx, conn); err != nil {
			t.Fatalf("readTrueWarningCount: %v", err)
		}
	}
	return out
}

func openTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// scalarInt runs a query and returns its LAST column as an int64 — which
// makes it serve both `SELECT COUNT(*)` and the two-column
// `SHOW GLOBAL STATUS LIKE ...` / `CHECKSUM TABLE` shapes.
func scalarInt(ctx context.Context, t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("%s: columns: %v", query, err)
	}
	if !rows.Next() {
		t.Fatalf("%s: no rows", query)
	}
	dest := make([]any, len(cols))
	raw := make([]sql.NullString, len(cols))
	for i := range dest {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		t.Fatalf("%s: scan: %v", query, err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	n, err := strconv.ParseInt(raw[len(raw)-1].String, 10, 64)
	if err != nil {
		t.Fatalf("%s: value %q is not an integer: %v", query, raw[len(raw)-1].String, err)
	}
	return n
}

// readPinTable reads one table's IR shape through the real schema reader.
func readPinTable(ctx context.Context, t *testing.T, dsn, name string) *ir.Table {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, name)
	if table == nil {
		t.Fatalf("table %q not found", name)
	}
	return table
}
