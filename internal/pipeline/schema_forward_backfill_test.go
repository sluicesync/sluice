// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// staticBackfillReader adapts an already-open reader to the backfill's
// reader func — the unit-test shape of [lazyBackfillReader.reader].
func staticBackfillReader(br ir.BatchedRowReader) func(context.Context) (ir.BatchedRowReader, error) {
	return func(context.Context) (ir.BatchedRowReader, error) { return br, nil }
}

// pagedBackfillReader serves rows in pages of the requested limit, in PK
// order, from the cursor on — and can be told to fail a page the way the
// real readers do: close the page's channel early and report the cause
// only through Err().
type pagedBackfillReader struct {
	rows []ir.Row

	// failPage, when > 0, is the 1-based page that stops after failAfter
	// rows and reports failErr through Err().
	failPage  int
	failAfter int
	failErr   error

	page int
	err  error

	// read is the table the last page was asked for.
	read *ir.Table
}

func (p *pagedBackfillReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return nil, errors.New("not used")
}

func (p *pagedBackfillReader) ReadRowsBatch(_ context.Context, table *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	p.page++
	p.read = table
	p.err = nil
	start := 0
	for len(after) == 1 && start < len(p.rows) && p.rows[start]["id"].(int64) <= after[0].(int64) {
		start++
	}
	end := min(start+limit, len(p.rows))
	out := make(chan ir.Row, limit)
	for i := start; i < end; i++ {
		if p.page == p.failPage && i-start == p.failAfter {
			p.err = p.failErr
			break
		}
		out <- p.rows[i]
	}
	close(out)
	return out, nil
}

func (p *pagedBackfillReader) Err() error { return p.err }

func backfillRows(n int) []ir.Row {
	rows := make([]ir.Row, n)
	for i := range rows {
		rows[i] = ir.Row{"id": int64(i + 1), "flag": "v"}
	}
	return rows
}

func backfillTestSnap() ir.SchemaSnapshot {
	return addColForwardSnap(addColForwardTable("dj", &ir.Column{Name: "flag", Type: ir.Text{}, Nullable: true}))
}

func drainUpdates(t *testing.T, out <-chan ir.Change) []ir.Update {
	t.Helper()
	var got []ir.Update
	for c := range out {
		u, ok := c.(ir.Update)
		if !ok {
			t.Fatalf("backfill emitted %T; want only ir.Update", c)
		}
		got = append(got, u)
	}
	return got
}

// TestRunBackfill_PagesTheWholeTable pins the page walk: every row across
// several full pages and a short final page gets exactly one Update.
func TestRunBackfill_PagesTheWholeTable(t *testing.T) {
	reader := &pagedBackfillReader{rows: backfillRows(7)}
	bf := &schemaForwardBackfill{reader: staticBackfillReader(reader), streamID: "s", batchSize: 3}
	snap := backfillTestSnap()
	out := make(chan ir.Change, 16)
	if err := runBackfillForAddedColumn(context.Background(), bf, snap, []*ir.Column{{Name: "flag", Type: ir.Text{}}}, out); err != nil {
		t.Fatalf("runBackfillForAddedColumn: %v", err)
	}
	close(out)
	got := drainUpdates(t, out)
	if len(got) != 7 {
		t.Fatalf("backfilled %d rows; want 7 (pages 3+3+1)", len(got))
	}
	for i, u := range got {
		if u.Before["id"] != int64(i+1) || u.After["flag"] != "v" {
			t.Errorf("update %d = before %v after %v; want id=%d flag=v", i, u.Before, u.After, i+1)
		}
	}
}

// TestRunBackfill_ReadsOnlyTheKeyAndTheAddedColumns pins the read's
// narrowing (perf-parity gap 35): the post-ALTER table carries a large
// column the backfill never writes, and the reader must not be asked for it.
func TestRunBackfill_ReadsOnlyTheKeyAndTheAddedColumns(t *testing.T) {
	reader := &pagedBackfillReader{rows: backfillRows(2)}
	bf := &schemaForwardBackfill{reader: staticBackfillReader(reader), streamID: "s", batchSize: 3}
	snap := addColForwardSnap(addColForwardTable(
		"dj",
		&ir.Column{Name: "payload", Type: ir.Text{}, Nullable: true},
		&ir.Column{Name: "flag", Type: ir.Text{}, Nullable: true},
	))
	out := make(chan ir.Change, 4)
	if err := runBackfillForAddedColumn(context.Background(), bf, snap, []*ir.Column{{Name: "flag", Type: ir.Text{}}}, out); err != nil {
		t.Fatalf("runBackfillForAddedColumn: %v", err)
	}
	if reader.read == nil {
		t.Fatal("the reader was never asked for a page")
	}
	got := make([]string, 0, len(reader.read.Columns))
	for _, c := range reader.read.Columns {
		got = append(got, c.Name)
	}
	if !slices.Equal(got, []string{"id", "flag"}) {
		t.Fatalf("the backfill read columns %v; want only the key and the added column [id flag] — every other column is read and discarded", got)
	}
	if reader.read.PrimaryKey == nil || len(reader.read.PrimaryKey.Columns) != 1 || reader.read.PrimaryKey.Columns[0].Column != "id" {
		t.Fatalf("the narrowed read lost its primary key (%v); the page cursor needs it", reader.read.PrimaryKey)
	}
}

// TestRunBackfill_PageErrorReportedThroughErrIsRefused pins the silent-loss
// fix on the backfill loop itself. The real readers report a mid-page
// failure by closing the page's channel early and setting Err(); the loop
// never consulted Err(), so the short page read as the END of the table and
// the backfill logged "complete" with every later row still holding the
// target's fill. It must refuse instead.
func TestRunBackfill_PageErrorReportedThroughErrIsRefused(t *testing.T) {
	cause := errors.New("connection reset mid-page")
	reader := &pagedBackfillReader{rows: backfillRows(7), failPage: 2, failAfter: 1, failErr: cause}
	bf := &schemaForwardBackfill{reader: staticBackfillReader(reader), streamID: "s", batchSize: 3}
	out := make(chan ir.Change, 16)
	err := runBackfillForAddedColumn(context.Background(), bf, backfillTestSnap(), []*ir.Column{{Name: "flag", Type: ir.Text{}}}, out)
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v; want the page's Err() cause surfaced (a short page is not the end of the table)", err)
	}
}

// TestBoundaryBackfill_FailureNamesTheSourceRepair pins Bug 290: a failed
// backfill's own error must not end with the generic forward hint ("apply
// the schema change yourself, then resume") — the ALTER already landed, the
// attempt ends as ADD-COLUMN-BACKFILL-INCOMPLETE, and that hint leaves the
// pre-existing rows wrong. Same repair text as the startup door and the
// fleet log (Bug 289), so the three cannot disagree.
func TestBoundaryBackfill_FailureNamesTheSourceRepair(t *testing.T) {
	cause := errors.New("connection reset mid-page")
	reader := &pagedBackfillReader{rows: backfillRows(7), failPage: 2, failAfter: 1, failErr: cause}
	b := &boundaryBackfill{
		bf:        &schemaForwardBackfill{reader: staticBackfillReader(reader), streamID: "s", batchSize: 3},
		tableName: "public.t",
		snap:      backfillTestSnap(),
		added:     []*ir.Column{{Name: "flag", Type: ir.Text{}}},
	}
	err := b.run(context.Background(), make(chan ir.Change, 16))
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v; want the page cause surfaced", err)
	}
	msg := err.Error()
	for _, want := range []string{addColumnBackfillIncompleteMarker, backfillIncompleteRepair, unforwardedRefusalAckFlag} {
		if !strings.Contains(msg, want) {
			t.Errorf("backfill failure missing %q: %v", want, msg)
		}
	}
	if strings.Contains(msg, forwardRecoveryHint("public.t")) {
		t.Errorf("backfill failure carries the generic forward hint, which leaves the rows wrong: %v", msg)
	}
}

// TestRunBackfill_MissingAddedColumnIsRefused: a source row without the
// added column is a reader that did not project it; writing NULL for it is
// the loss the backfill exists to prevent.
func TestRunBackfill_MissingAddedColumnIsRefused(t *testing.T) {
	reader := &pagedBackfillReader{rows: []ir.Row{{"id": int64(1)}}}
	bf := &schemaForwardBackfill{reader: staticBackfillReader(reader), streamID: "s", batchSize: 3}
	out := make(chan ir.Change, 4)
	err := runBackfillForAddedColumn(context.Background(), bf, backfillTestSnap(), []*ir.Column{{Name: "flag", Type: ir.Text{}}}, out)
	if err == nil || !strings.Contains(err.Error(), `added column "flag"`) {
		t.Fatalf("err = %v; want a refusal naming the missing added column", err)
	}
}

// TestRunBackfill_GeneratedAddedColumnIsNotCarried: the target computes a
// generated column itself, so the backfill neither reads nor sets it.
func TestRunBackfill_GeneratedAddedColumnIsNotCarried(t *testing.T) {
	gen := &ir.Column{Name: "g", Type: ir.Integer{Width: 32}, GeneratedExpr: "id * 2", GeneratedStored: true}
	snap := addColForwardSnap(addColForwardTable("dj", gen))
	opened := false
	bf := &schemaForwardBackfill{
		reader: func(context.Context) (ir.BatchedRowReader, error) {
			opened = true
			return &pagedBackfillReader{}, nil
		},
		batchSize: 3,
	}
	out := make(chan ir.Change, 1)
	if err := runBackfillForAddedColumn(context.Background(), bf, snap, []*ir.Column{gen}, out); err != nil {
		t.Fatalf("runBackfillForAddedColumn: %v", err)
	}
	if opened {
		t.Error("the source reader was opened for a generated-only ADD COLUMN; nothing needs reading")
	}
}

// TestRunBackfill_PrimaryKeyFromTheCatalog: a boundary whose projection
// carries no primary key (the VStream FIELD event) resolves it from the
// source catalog; with no resolver, or a key naming a column the projection
// lacks, it refuses rather than guessing a cursor.
func TestRunBackfill_PrimaryKeyFromTheCatalog(t *testing.T) {
	keyless := backfillTestSnap()
	tbl := *keyless.IR
	tbl.PrimaryKey = nil
	keyless.IR = &tbl
	added := []*ir.Column{{Name: "flag", Type: ir.Text{}}}
	resolveTo := func(col string) func(context.Context, string, string) (*ir.Index, error) {
		return func(context.Context, string, string) (*ir.Index, error) {
			return &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: col}}}, nil
		}
	}

	bf := &schemaForwardBackfill{reader: staticBackfillReader(&pagedBackfillReader{rows: backfillRows(2)}), batchSize: 10, primaryKey: resolveTo("id")}
	out := make(chan ir.Change, 4)
	if err := runBackfillForAddedColumn(context.Background(), bf, keyless, added, out); err != nil {
		t.Fatalf("catalog-resolved key: %v", err)
	}
	close(out)
	if got := drainUpdates(t, out); len(got) != 2 {
		t.Fatalf("backfilled %d rows with a catalog-resolved key; want 2", len(got))
	}

	for name, resolver := range map[string]func(context.Context, string, string) (*ir.Index, error){
		"no resolver":             nil,
		"key names a lost column": resolveTo("not_projected"),
	} {
		bf := &schemaForwardBackfill{reader: staticBackfillReader(&pagedBackfillReader{rows: backfillRows(2)}), batchSize: 10, primaryKey: resolver}
		if err := runBackfillForAddedColumn(context.Background(), bf, keyless, added, make(chan ir.Change, 4)); err == nil {
			t.Errorf("%s: backfill ran without a usable primary key; want a refusal", name)
		}
	}
}

// TestRunBackfill_ReplicaIdentityFullKeyIsNotTheKey pins the key choice
// under a Postgres REPLICA IDENTITY FULL table, whose projection flags every
// column — the added one included — as a key column. Keyed on that, the
// UPDATE's WHERE demanded the value being filled and matched no target row,
// silently. The catalog's key must win; and a key naming the added column
// must refuse even when nothing better is available.
func TestRunBackfill_ReplicaIdentityFullKeyIsNotTheKey(t *testing.T) {
	full := backfillTestSnap()
	tbl := *full.IR
	tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}, {Column: "flag"}}}
	full.IR = &tbl
	added := []*ir.Column{{Name: "flag", Type: ir.Text{}}}

	bf := &schemaForwardBackfill{
		reader:    staticBackfillReader(&pagedBackfillReader{rows: backfillRows(2)}),
		batchSize: 10,
		primaryKey: func(context.Context, string, string) (*ir.Index, error) {
			return &ir.Index{Name: "w_pkey", Columns: []ir.IndexColumn{{Column: "id"}}}, nil
		},
	}
	out := make(chan ir.Change, 4)
	if err := runBackfillForAddedColumn(context.Background(), bf, full, added, out); err != nil {
		t.Fatalf("catalog key over a FULL-identity projection: %v", err)
	}
	close(out)
	for _, u := range drainUpdates(t, out) {
		if _, keyed := u.Before["flag"]; keyed || len(u.Before) != 1 {
			t.Fatalf("the backfill's WHERE is %v; want only the catalog key id — a WHERE on the column being filled matches no target row", u.Before)
		}
	}

	bf = &schemaForwardBackfill{reader: staticBackfillReader(&pagedBackfillReader{rows: backfillRows(2)}), batchSize: 10}
	err := runBackfillForAddedColumn(context.Background(), bf, full, added, make(chan ir.Change, 4))
	if err == nil || !strings.Contains(err.Error(), `includes the added column "flag"`) {
		t.Fatalf("err = %v; want a refusal naming the added column in the key", err)
	}
}

// TestAddedColumnBackfill_DefaultOnZeroValue is the v0.99.51 pin: the zero
// Streamer — every construction that is not the CLI — gets the backfill,
// and only the opt-out removes it.
func TestAddedColumnBackfill_DefaultOnZeroValue(t *testing.T) {
	if (&Streamer{}).addedColumnBackfill("s", nil) == nil {
		t.Fatal("zero-value Streamer has no added-column backfill; it must be default-on (opt-out semantics)")
	}
	if (&Streamer{SuppressAddedColumnBackfill: true}).addedColumnBackfill("s", nil) != nil {
		t.Fatal("SuppressAddedColumnBackfill did not remove the backfill")
	}
	// The deprecated opt-in changes nothing.
	if (&Streamer{BackfillAddedColumn: true, SuppressAddedColumnBackfill: true}).addedColumnBackfill("s", nil) != nil {
		t.Fatal("the deprecated BackfillAddedColumn overrode the opt-out")
	}
}

// notBatchedReader is a RowReader with no PK-cursor surface.
type notBatchedReader struct{ closed bool }

func (*notBatchedReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return nil, errors.New("not used")
}
func (*notBatchedReader) Err() error     { return nil }
func (r *notBatchedReader) Close() error { r.closed = true; return nil }

// TestLazyBackfillReader_OpensOnceAndRefusesUnbatched: the reader opens on
// first use only (a stream that never forwards an ADD COLUMN never opens
// one), and a source reader that cannot paginate is refused at the boundary
// with the opt-out named — not silently skipped.
func TestLazyBackfillReader_OpensOnceAndRefusesUnbatched(t *testing.T) {
	opens := 0
	rr := &notBatchedReader{}
	l := &lazyBackfillReader{open: func(context.Context) (ir.RowReader, error) {
		opens++
		return rr, nil
	}}
	for range 2 {
		if _, err := l.reader(context.Background()); !errors.Is(err, errBackfillSourceNotBatched) {
			t.Fatalf("err = %v; want errBackfillSourceNotBatched", err)
		}
	}
	if opens != 1 {
		t.Errorf("opened %d times; want exactly 1", opens)
	}
	if !strings.Contains(errBackfillSourceNotBatched.Error(), "--no-backfill-added-column") {
		t.Error("the refusal does not name the opt-out")
	}
	l.Close()
	if !rr.closed {
		t.Error("Close did not release the opened reader")
	}
	// After its attempt closed it, a straggling caller cannot reopen one
	// that nothing would close.
	if _, err := l.reader(context.Background()); err == nil || opens != 1 {
		t.Errorf("reader after Close = err %v, %d opens; want a refusal and no reopen", err, opens)
	}
}

// TestBackfillAddedColumns_OptOutWarns: with the backfill suppressed, what
// it leaves behind is said at WARN for every forwarded ADD COLUMN.
func TestBackfillAddedColumns_OptOutWarns(t *testing.T) {
	var buf logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)
	out := make(chan ir.Change, 1)
	if err := backfillAddedColumns(context.Background(), nil, "public.dj", backfillTestSnap(), []*ir.Column{{Name: "flag"}}, out); err != nil {
		t.Fatalf("backfillAddedColumns: %v", err)
	}
	if !strings.Contains(buf.String(), "backfill suppressed") || !strings.Contains(buf.String(), "flag") {
		t.Fatalf("no opt-out WARN naming the column; log = %q", buf.String())
	}
}

// assertSnapshotThenBackfill checks the load-bearing order on an intercept's
// output: the ADD COLUMN boundary's snapshot reaches the applier BEFORE the
// first backfilled row (the applier refreshes its view of the table on the
// snapshot; a value ahead of it is encoded against the pre-ALTER table), and
// every source row is backfilled exactly once.
func assertSnapshotThenBackfill(t *testing.T, got []ir.Change, postToken string, wantUpdates int) {
	t.Helper()
	snapAt, firstUpdate, updates := -1, -1, 0
	for i, c := range got {
		switch v := c.(type) {
		case ir.SchemaSnapshot:
			if v.Position.Token == postToken {
				snapAt = i
			}
		case ir.Update:
			if firstUpdate < 0 {
				firstUpdate = i
			}
			updates++
		}
	}
	if snapAt < 0 {
		t.Fatalf("the ADD COLUMN boundary's snapshot was never forwarded; got %#v", got)
	}
	if updates != wantUpdates {
		t.Fatalf("backfilled %d rows; want %d", updates, wantUpdates)
	}
	if firstUpdate < snapAt {
		t.Fatalf("a backfilled row (index %d) reached the applier before the boundary's snapshot (index %d)", firstUpdate, snapAt)
	}
}

// TestForwardAddColumn_BackfillFollowsTheSnapshot pins the order on the
// single-stream intercept.
func TestForwardAddColumn_BackfillFollowsTheSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pre := addColForwardTable("dj")
	post := addColForwardTable("dj", &ir.Column{Name: "flag", Type: ir.Text{}, Nullable: true})
	postSnap := addColForwardSnap(post)
	postSnap.Position.Token = "lsn/2"
	in := make(chan ir.Change, 2)
	in <- addColForwardSnap(pre)
	in <- postSnap
	close(in)
	var errStore atomic.Pointer[error]
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          &fakeShapeApplier{},
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
		backfill: &schemaForwardBackfill{
			reader: staticBackfillReader(&pagedBackfillReader{rows: backfillRows(3)}), batchSize: 10,
		},
	}, &errStore)
	got := drainChannel(t, out, 2*time.Second)
	if e := errStore.Load(); e != nil {
		t.Fatalf("intercept refused: %v", *e)
	}
	assertSnapshotThenBackfill(t, got, "lsn/2", 3)
}

// TestShapeAIntercept_BackfillFollowsTheSnapshot pins the same order, and
// that the backfill runs at all, on the Shape A boundary intercept.
func TestShapeAIntercept_BackfillFollowsTheSnapshot(t *testing.T) {
	clock := newMockClock(testClockNow())
	mgr := newTestLeaseManager(t, newFakeLeaseStore(clock.Now), "stream-a",
		LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 5 * time.Minute}, clock)
	router, err := NewBoundaryRouter(mgr, &fakeShapeApplier{}, &fakeProber{}, "postgres", "postgres", sourceDefaultReaders{})
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}
	pre := addColForwardTable("dj")
	post := addColForwardTable("dj", &ir.Column{Name: "flag", Type: ir.Text{}, Nullable: true})
	in := make(chan ir.Change, 2)
	in <- ir.SchemaSnapshot{Schema: "public", Table: "dj", Position: ir.Position{Token: "p1"}, IR: pre}
	in <- ir.SchemaSnapshot{Schema: "public", Table: "dj", Position: ir.Position{Token: "p2"}, IR: post}
	close(in)
	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bf := &schemaForwardBackfill{reader: staticBackfillReader(&pagedBackfillReader{rows: backfillRows(3)}), batchSize: 10}
	out := interceptSchemaSnapshotsForCoordination(ctx, in, nil, router, nil, bf, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	if e := errStore.Load(); e != nil {
		t.Fatalf("intercept refused: %v", *e)
	}
	assertSnapshotThenBackfill(t, got, "p2", 3)
}

// Compile-time: the paged fake satisfies the surface the backfill needs.
var _ ir.BatchedRowReader = (*pagedBackfillReader)(nil)
