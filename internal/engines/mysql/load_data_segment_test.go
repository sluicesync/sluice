// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Item 114 unit pins for the segmented LOAD DATA core. Three layers:
//
//  1. the encoder's byte budget (pure);
//  2. the replay-warning accounting (pure — the decision that keeps a real
//     coercion from hiding behind a replay's duplicate-key warnings);
//  3. the retry behaviour itself, through the scriptable fake driver, whose
//     oracle is the number of statements the DRIVER saw, not the writer's
//     own return value.
//
// The end-to-end property — a segment replayed against a real MySQL lands
// the table's rows exactly once — needs a real server and lives in
// row_writer_loaddata_resume_integration_test.go.

func segTestColumns() []*ir.Column {
	return []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "v", Type: ir.Varchar{Length: 64}},
	}
}

// feedNumberedRows produces n rows of the segTestColumns shape.
func feedNumberedRows(n int, value string) <-chan ir.Row {
	ch := make(chan ir.Row, n)
	for i := 0; i < n; i++ {
		ch <- ir.Row{"id": int64(i), "v": value}
	}
	close(ch)
	return ch
}

// TestEncodeRowsTSV_SegmentBudgetSplitsOnRowBoundaries is the segmentation
// crux: the budget closes a segment WITHOUT splitting a row, and only a
// closed CHANNEL reports drained. A segment that ended mid-row would not be
// independently replayable — the whole design rests on this.
func TestEncodeRowsTSV_SegmentBudgetSplitsOnRowBoundaries(t *testing.T) {
	cols := segTestColumns()
	// Each encoded row is "N\tvvvvvvvv\n" — 11 bytes for a 1-digit id.
	rows := feedNumberedRows(9, "vvvvvvvv")

	var all bytes.Buffer
	segments := 0
	for {
		var buf bytes.Buffer
		n, drained, err := encodeRowsTSV(context.Background(), &buf, cols, rows, 30)
		if err != nil {
			t.Fatalf("encodeRowsTSV: %v", err)
		}
		if n > 0 {
			segments++
			if !strings.HasSuffix(buf.String(), "\n") {
				t.Fatalf("segment %d does not end on a row boundary: %q", segments, buf.String())
			}
			if got := strings.Count(buf.String(), "\n"); got != n {
				t.Errorf("segment %d reported %d rows but carries %d lines", segments, n, got)
			}
			all.Write(buf.Bytes())
		}
		if drained {
			break
		}
	}
	// 11 bytes/row, 30-byte budget ⇒ 3 rows per segment ⇒ 3 segments.
	if segments != 3 {
		t.Errorf("segments = %d; want 3 (9 rows × 11 bytes at a 30-byte budget)", segments)
	}
	if got := strings.Count(all.String(), "\n"); got != 9 {
		t.Errorf("total rows across segments = %d; want 9 — segmentation must not lose or duplicate a row", got)
	}
	if !strings.HasPrefix(all.String(), "0\tvvvvvvvv\n") || !strings.HasSuffix(all.String(), "8\tvvvvvvvv\n") {
		t.Errorf("segment concatenation is not the original stream: %q", all.String())
	}
}

// TestEncodeRowsTSV_RowLargerThanBudgetShipsAlone pins the soft-budget
// wording: the budget bounds ACCUMULATION, never an individual row, so an
// over-budget row is a one-row segment rather than a refusal or a split.
func TestEncodeRowsTSV_RowLargerThanBudgetShipsAlone(t *testing.T) {
	cols := segTestColumns()
	rows := make(chan ir.Row, 2)
	rows <- ir.Row{"id": int64(1), "v": strings.Repeat("x", 500)}
	rows <- ir.Row{"id": int64(2), "v": "small"}
	close(rows)

	var buf bytes.Buffer
	n, drained, err := encodeRowsTSV(context.Background(), &buf, cols, rows, 16)
	if err != nil {
		t.Fatalf("encodeRowsTSV: %v", err)
	}
	if n != 1 || drained {
		t.Fatalf("n=%d drained=%v; want 1 row, not drained (the oversized row closes the segment alone)", n, drained)
	}
	if !strings.Contains(buf.String(), strings.Repeat("x", 500)) {
		t.Error("the oversized row must ship whole, not truncated")
	}
}

// TestEncodeRowsTSV_ZeroBudgetDrainsTheWholeChannel pins that the
// pre-item-114 behaviour is still exactly what a non-positive budget means
// (the encoder's own unit tests, and the throughput comparison, rely on it).
func TestEncodeRowsTSV_ZeroBudgetDrainsTheWholeChannel(t *testing.T) {
	var buf bytes.Buffer
	n, drained, err := encodeRowsTSV(context.Background(), &buf, segTestColumns(), feedNumberedRows(100, "v"), 0)
	if err != nil {
		t.Fatalf("encodeRowsTSV: %v", err)
	}
	if n != 100 || !drained {
		t.Errorf("n=%d drained=%v; want 100, true", n, drained)
	}
}

// TestReplayWarningsAreOnlyDuplicates is the matrix behind the ONE decision
// that could turn item 114's retry into a silent-corruption path: whether a
// replayed segment's warnings are fully explained by duplicate-key skips.
//
// The conservative direction is always "refuse" — a false refusal costs a
// restart, a false tolerance costs a silently coerced value.
func TestReplayWarningsAreOnlyDuplicates(t *testing.T) {
	cases := []struct {
		name          string
		segRows       int
		inserted      int64
		totalWarnings int64
		visibleNonDup int
		want          bool
	}{
		{"full replay of an already-committed segment", 5000, 0, 5000, 0, true},
		{"partial prior commit, remainder inserted", 5000, 1200, 3800, 0, true},
		{"fewer warnings than skips still fully explained", 100, 0, 90, 0, true},
		// The crux (real-server shape pinned in the integration test): a
		// truncation warning sits beyond @@max_error_count so every VISIBLE
		// warning is a 1062, and only the count arithmetic catches it.
		{"one truncation hidden behind 5000 duplicates", 5001, 1, 5001, 0, false},
		{"a visible non-duplicate warning", 100, 0, 100, 1, false},
		{"nothing was skipped, so nothing is explained", 100, 100, 1, 0, false},
		{"affected-rows unknown", 100, -1, 5, 0, false},
		{"server inserted more rows than we sent", 100, 101, 1, 0, false},
	}
	sawTrue, sawFalse := false, false
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := replayWarningsAreOnlyDuplicates(tc.segRows, tc.inserted, tc.totalWarnings, tc.visibleNonDup)
			if got != tc.want {
				t.Errorf("replayWarningsAreOnlyDuplicates(%d, %d, %d, %d) = %v; want %v",
					tc.segRows, tc.inserted, tc.totalWarnings, tc.visibleNonDup, got, tc.want)
			}
			if got {
				sawTrue = true
			} else {
				sawFalse = true
			}
		})
	}
	// Anti-vacuity floor: a matrix that only ever produced one answer would
	// pass while proving nothing about the decision.
	if !sawTrue || !sawFalse {
		t.Errorf("matrix produced only one verdict (sawTrue=%v sawFalse=%v)", sawTrue, sawFalse)
	}
}

// TestLoadDataSegmentTarget_MaxBufferBytesOnlyLowers pins the zero-value-safe
// direction of the operator knob: an unset --max-buffer-bytes leaves the
// package default in charge, and a value ABOVE it does not raise the heap
// footprint of the replay buffer.
func TestLoadDataSegmentTarget_MaxBufferBytesOnlyLowers(t *testing.T) {
	cases := []struct {
		name  string
		field int64
		want  int64
	}{
		{"unset (the zero value every non-CLI construction gets)", 0, defaultLoadDataSegmentBytes},
		{"negative", -1, defaultLoadDataSegmentBytes},
		{"below the default lowers it", 1 << 20, 1 << 20},
		{"above the default does not raise it", 1 << 30, defaultLoadDataSegmentBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &RowWriter{maxBufferBytes: tc.field}
			if got := w.loadDataSegmentTarget(); got != tc.want {
				t.Errorf("loadDataSegmentTarget() = %d; want %d", got, tc.want)
			}
		})
	}
}

// withSmallLoadDataSegments shrinks the segment budget for one test so a
// handful of rows produces several segments.
func withSmallLoadDataSegments(t *testing.T, budget int64) {
	t.Helper()
	orig := defaultLoadDataSegmentBytes
	defaultLoadDataSegmentBytes = budget
	t.Cleanup(func() { defaultLoadDataSegmentBytes = orig })
}

// TestLoadDataSegments_OneStatementPerSegment is the shape pin: the table is
// no longer one statement. The oracle is the DRIVER's exec count.
func TestLoadDataSegments_OneStatementPerSegment(t *testing.T) {
	withSmallLoadDataSegments(t, 30) // ~3 rows per segment at 11 bytes/row
	script := &flushScript{}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadLoadDataInfile}

	if err := w.WriteRows(context.Background(), segPinTable(), feedNumberedRows(9, "vvvvvvvv")); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if got := script.execCalls.Load(); got != 3 {
		t.Errorf("LOAD DATA exec calls = %d; want 3 (9 rows at a 3-row budget) — "+
			"one statement per table is the item-114 defect", got)
	}
}

// TestLoadDataSegments_RidesATransientByReplayingOneSegment is the item-114
// crux at unit level: a classified transient on a segment is RETRIED rather
// than ending the table's copy, and the retry re-drives that segment only.
//
// Before item 114 this returned a terminal "CANNOT be resumed" error after
// exactly one statement.
func TestLoadDataSegments_RidesATransientByReplayingOneSegment(t *testing.T) {
	withFastReparentBackoff(t, 12)
	withSmallLoadDataSegments(t, 30)
	script := &flushScript{execErrs: []error{
		nil,                   // segment 1
		vttabletUnavailable(), // segment 2 — transient
		nil,                   // segment 2 replay
		nil,                   // segment 3
	}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadLoadDataInfile}

	if err := w.WriteRows(context.Background(), segPinTable(), feedNumberedRows(9, "vvvvvvvv")); err != nil {
		t.Fatalf("a transient on one segment must be ridden, not fatal; got: %v", err)
	}
	if got := script.execCalls.Load(); got != 4 {
		t.Errorf("LOAD DATA exec calls = %d; want 4 (3 segments + 1 replay)", got)
	}
	// The retry must have re-acquired a FRESH connection (ADR-0108): the
	// pinned one is dead after a reparent.
	if got := script.opens.Load(); got < 2 {
		t.Errorf("driver.Open calls = %d; want >= 2 — the replay must not reuse the dead conn", got)
	}
}

// TestLoadDataSegments_KeylessRefusesRatherThanReplay is the sibling of
// TestColdCopyReparentRetry_KeylessRefusesRatherThanResend for the LOAD DATA
// core. It matters MORE here than on the batched cores: LOAD DATA LOCAL
// downgrades a duplicate key to a warning and skips the row, which is
// exactly what makes a keyed replay convergent — and exactly what a keyless
// table cannot do, so a replay would double the segment with no signal at
// all. The gate is inherited from flushWithReparentRetry; this proves the
// inheritance rather than assuming it.
func TestLoadDataSegments_KeylessRefusesRatherThanReplay(t *testing.T) {
	withFastReparentBackoff(t, 12)
	withSmallLoadDataSegments(t, 30)
	script := &flushScript{execErrs: []error{vttabletUnavailable()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadLoadDataInfile}

	err := w.WriteRows(context.Background(), keylessPinTable(), feedNumberedRows(9, "vvvvvvvv"))
	if err == nil {
		t.Fatal("a keyless table's segment must not be replayed; want a refusal, got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
		t.Errorf("refusal code = %v (coded=%v); want %s", ce, ok, sluicecode.CodeCopyRetryAmbiguousKeyless)
	}
	if got := script.execCalls.Load(); got != 1 {
		t.Errorf("LOAD DATA exec calls = %d; want 1 — the ambiguous segment must not be re-sent", got)
	}
}

// TestLoadDataSegments_TerminalErrorStillFailsLoudly is the false-retry
// floor: a non-transient statement error is returned unchanged, naming the
// table, exactly as before item 114.
func TestLoadDataSegments_TerminalErrorStillFailsLoudly(t *testing.T) {
	withFastReparentBackoff(t, 12)
	withSmallLoadDataSegments(t, 30)
	script := &flushScript{execErrs: []error{errTerminalLoadData()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadLoadDataInfile}

	err := w.WriteRows(context.Background(), segPinTable(), feedNumberedRows(9, "vvvvvvvv"))
	if err == nil {
		t.Fatal("a terminal statement error must still fail the copy")
	}
	if !containsAll(err.Error(), "LOAD DATA", "t_seg") {
		t.Errorf("terminal error must name the path and the table; got: %v", err)
	}
	if got := script.execCalls.Load(); got != 1 {
		t.Errorf("LOAD DATA exec calls = %d; want 1 (terminal, no retry)", got)
	}
}

// TestLoadDataSegments_EmptyTableIssuesNoStatement pins the one deliberate
// behaviour change at the boundary: an empty row channel emits no LOAD DATA
// at all, where the pre-item-114 shape sent one empty-stream statement.
// Nothing observable depends on the empty statement, and skipping it keeps
// the loop's "a segment with rows is a statement" rule total.
func TestLoadDataSegments_EmptyTableIssuesNoStatement(t *testing.T) {
	script := &flushScript{}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadLoadDataInfile}

	empty := make(chan ir.Row)
	close(empty)
	if err := w.WriteRows(context.Background(), segPinTable(), empty); err != nil {
		t.Fatalf("WriteRows on an empty table: %v", err)
	}
	if got := script.execCalls.Load(); got != 0 {
		t.Errorf("LOAD DATA exec calls = %d; want 0", got)
	}
}

// errTerminalLoadData is a NON-transient statement failure this path can
// really produce: Error 1083, the "field separator argument is not what is
// expected" abort that Bug 178's sql_mode interaction used to trigger before
// the hex-literal framing. Deliberately not 1054/1146 — the classifier
// treats those as retriable schema drift by design.
func errTerminalLoadData() error {
	return &gomysql.MySQLError{Number: 1083, Message: "Field separator argument is not what is expected; check the manual"}
}

// segPinTable is the keyed table the segment pins load into.
func segPinTable() *ir.Table {
	return &ir.Table{
		Name: "t_seg",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "v", Type: ir.Varchar{Length: 64}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}
