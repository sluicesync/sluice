// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestResolveIndexBuildWorkers pins the MySQL-specific worker-sizing policy
// (ADR-0080): min(default-N, jobCount), clamped to [floor, ceil]. MySQL has
// no connection-slot prober, so the budget (always 0) is NOT an input — this
// fixed-N policy is the only sizing lever.
func TestResolveIndexBuildWorkers(t *testing.T) {
	w := &SchemaWriter{}
	cases := []struct {
		jobCount int
		want     int
	}{
		{jobCount: 0, want: 1},   // floored to 1 even with no jobs
		{jobCount: 1, want: 1},   // one job → one worker
		{jobCount: 3, want: 3},   // fewer jobs than default → jobCount
		{jobCount: 4, want: 4},   // exactly default
		{jobCount: 7, want: 4},   // capped at default N=4
		{jobCount: 100, want: 4}, // capped at default N=4
	}
	for _, c := range cases {
		if got := w.resolveIndexBuildWorkers(c.jobCount); got != c.want {
			t.Errorf("resolveIndexBuildWorkers(%d) = %d; want %d", c.jobCount, got, c.want)
		}
	}
}

// TestResolveIndexBuildWorkers_ClampInvariant pins the [floor, ceil] clamp
// holds regardless of the default policy — a guard so a future bump of
// indexBuildWorkerDefault above the ceil still clamps.
func TestResolveIndexBuildWorkers_ClampInvariant(t *testing.T) {
	w := &SchemaWriter{}
	for jobs := 0; jobs <= 50; jobs++ {
		got := w.resolveIndexBuildWorkers(jobs)
		if got < indexBuildWorkerFloor || got > indexBuildWorkerCeil {
			t.Errorf("resolveIndexBuildWorkers(%d) = %d; outside [%d,%d]",
				jobs, got, indexBuildWorkerFloor, indexBuildWorkerCeil)
		}
	}
}

// TestIndexBuildJobsForTables_Parity pins that indexBuildJobsForTables
// produces exactly the (table, index) work-list the prior CreateIndexes loop
// did: inline-skip indexes (the AUTO_INCREMENT supporting key) are dropped,
// surviving indexes are sorted alphabetically within each table, and PRIMARY
// is never a job. Same SQL on both the whole-schema and overlap paths.
func TestIndexBuildJobsForTables_Parity(t *testing.T) {
	// Table with an AUTO_INCREMENT column `seq` that is NOT the leading PK
	// column, plus an operator index `seq_idx` on it → inlineAutoIncrementIndex
	// returns seq_idx, so inlineSkipIndexNames includes it and the job list
	// must drop it. The two other secondary indexes survive, sorted.
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "seq", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "z_idx", Columns: []ir.IndexColumn{{Column: "b"}}},
			{Name: "seq_idx", Unique: true, Columns: []ir.IndexColumn{{Column: "seq"}}},
			{Name: "a_idx", Columns: []ir.IndexColumn{{Column: "a"}}},
		},
	}

	// Precondition: the inline-skip set actually contains seq_idx, else the
	// parity assertion below proves nothing.
	if _, skipped := inlineSkipIndexNames(table)["seq_idx"]; !skipped {
		t.Fatalf("test setup: expected seq_idx in inlineSkipIndexNames, got %v",
			inlineSkipIndexNames(table))
	}

	w := &SchemaWriter{}
	jobs := w.indexBuildJobsForTables([]*ir.Table{table})

	// One job per table now (combined-ALTER model): the single job carries
	// the table's full, sorted, skip-filtered index set.
	if len(jobs) != 1 {
		t.Fatalf("indexBuildJobsForTables = %d jobs; want 1 (one per table)", len(jobs))
	}
	if jobs[0].tableName != "t" {
		t.Errorf("job for unexpected table %q", jobs[0].tableName)
	}
	gotNames := make([]string, 0, len(jobs[0].idxs))
	for _, idx := range jobs[0].idxs {
		gotNames = append(gotNames, idx.Name)
	}
	want := []string{"a_idx", "z_idx"} // sorted, seq_idx dropped
	if !reflect.DeepEqual(gotNames, want) {
		t.Errorf("indexBuildJobsForTables names = %v; want %v", gotNames, want)
	}

	// Cross-check against the reference: the same surviving set the prior
	// CreateIndexes loop computed by hand (sorted, inline-skip applied).
	ref := referenceCreateIndexNames(table)
	if !reflect.DeepEqual(gotNames, ref) {
		t.Errorf("indexBuildJobsForTables names = %v; reference loop = %v", gotNames, ref)
	}
}

// referenceCreateIndexNames reproduces the pre-ADR-0080 CreateIndexes
// per-table loop body (skip-inline + sort) independently, so the parity test
// compares two implementations rather than the helper against itself.
func referenceCreateIndexNames(table *ir.Table) []string {
	skip := inlineSkipIndexNames(table)
	indexes := append([]*ir.Index(nil), table.Indexes...)
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	var out []string
	for _, idx := range indexes {
		if _, s := skip[idx.Name]; s {
			continue
		}
		out = append(out, idx.Name)
	}
	return out
}

// TestBuildTableIndexesFromChannel_VStreamBuildsSerially is the regression
// lock for the CRITICAL silent-index-loss bug: a PlanetScale/Vitess writer
// (flavor.usesVStream()) reads the completed-tables channel and BUILDS each
// received table's secondary indexes SERIALLY — it is NOT a no-op. The prior
// gate drained into a pure no-op and relied on a post-copy CreateIndexes that
// never ran on the overlapped path, so a MySQL VStream target silently created
// NO secondary indexes at all.
//
// Pins:
//   - every fed table gets an ALTER … ADD INDEX emitted (recorded by the fake
//     driver) — the build genuinely happens;
//   - the per-table IndexesBuilt callback fires for every table, and fires
//     ONLY AFTER that table's ALTER was emitted (build-then-mark);
//   - both usesVStream flavors are exercised (pin the class: PlanetScale AND
//     Vitess share the code path but are asserted independently).
func TestBuildTableIndexesFromChannel_VStreamBuildsSerially(t *testing.T) {
	for _, flavor := range []Flavor{FlavorPlanetScale, FlavorVitess} {
		t.Run(flavor.String(), func(t *testing.T) {
			rec := &indexRecorder{exists: false} // indexes don't exist yet → build them
			db := newIndexFakeDB(t, rec)
			w := &SchemaWriter{db: db, schema: "testdb", flavor: flavor}

			var mu sync.Mutex
			var fired []string
			var markedBeforeBuild []string // tables whose callback fired with NO ALTER recorded yet
			w.SetTableIndexedCallback(func(table *ir.Table) {
				mu.Lock()
				fired = append(fired, table.Name)
				if rec.alterCountFor(table.Name) == 0 {
					markedBeforeBuild = append(markedBeforeBuild, table.Name)
				}
				mu.Unlock()
			})

			tables := []*ir.Table{indexedTable("t0"), indexedTable("t1"), indexedTable("t2")}
			schema := &ir.Schema{Tables: tables}
			ch := make(chan *ir.Table, len(tables))
			for _, tbl := range tables {
				ch <- tbl
			}
			close(ch)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := w.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
				t.Fatalf("BuildTableIndexesFromChannel (vstream serial build): %v", err)
			}

			// Every table's index was actually built (not silently skipped).
			for _, name := range []string{"t0", "t1", "t2"} {
				if rec.alterCountFor(name) == 0 {
					t.Errorf("no ALTER … ADD INDEX emitted for %q — the vstream path silently skipped it (the bug)", name)
				}
			}
			// And every emitted statement is an ADD INDEX (proves it's index DDL).
			for _, stmt := range rec.snapshot() {
				if !strings.Contains(stmt, "ADD INDEX") {
					t.Errorf("recorded statement is not an ADD INDEX: %q", stmt)
				}
			}

			mu.Lock()
			sort.Strings(fired)
			gotMarkedEarly := append([]string(nil), markedBeforeBuild...)
			mu.Unlock()
			if want := []string{"t0", "t1", "t2"}; !reflect.DeepEqual(fired, want) {
				t.Errorf("callback fired for %v; want %v", fired, want)
			}
			if len(gotMarkedEarly) != 0 {
				t.Errorf("build-then-mark violated: IndexesBuilt fired before the ALTER for %v", gotMarkedEarly)
			}
		})
	}
}

// TestBuildTableIndexesFromChannel_VStreamBuildErrorLeavesUnmarked pins the
// build-then-mark failure contract: when a table's index build FAILS mid-way,
// BuildTableIndexesFromChannel returns the error loudly and does NOT fire the
// IndexesBuilt callback for that table (nor for any not-yet-built table), so a
// --resume rebuilds them rather than stranding them marked-done.
func TestBuildTableIndexesFromChannel_VStreamBuildErrorLeavesUnmarked(t *testing.T) {
	buildErr := errors.New("vtgate: ADD INDEX rejected")
	rec := &indexRecorder{
		exists:   false,
		failTbls: map[string]error{"t0": buildErr}, // t0's ALTER fails
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}

	var mu sync.Mutex
	var fired []string
	w.SetTableIndexedCallback(func(table *ir.Table) {
		mu.Lock()
		fired = append(fired, table.Name)
		mu.Unlock()
	})

	// t0 is drained first (FIFO) and its build fails → t1 never reached.
	tables := []*ir.Table{indexedTable("t0"), indexedTable("t1")}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	for _, tbl := range tables {
		ch <- tbl
	}
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := w.BuildTableIndexesFromChannel(ctx, schema, ch)
	if err == nil {
		t.Fatal("a mid-build failure must fail the phase LOUDLY; got nil")
	}
	if !errors.Is(err, buildErr) {
		t.Errorf("returned error should wrap the underlying build error; got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 0 {
		t.Errorf("IndexesBuilt fired for %v after a build failure; build-then-mark requires no table be marked when its (or an earlier) build failed", fired)
	}
}

// TestBuildTableIndexesFromChannel_VStreamBuildsAsReceived pins the OVERLAP
// property the build-as-copied rework added: the vstream serial builder builds
// each table's indexes the MOMENT that table is received — it does NOT wait for
// the channel to close. It feeds one table at a time over an UNBUFFERED channel
// and asserts that table's IndexesBuilt callback fires (its ALTER emitted)
// before the next table is ever sent, and long before close. The prior
// drain-all-then-build implementation would block until close and never fire a
// callback here, so this test times out (fails) against the old behaviour —
// exactly the regression guard we want. It also pins per-received-table build
// ORDER (t0 then t1), the serial one-ALTER-at-a-time contract.
func TestBuildTableIndexesFromChannel_VStreamBuildsAsReceived(t *testing.T) {
	rec := &indexRecorder{exists: false} // indexes don't exist yet → build them
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}

	// Each table's build fires exactly one callback; buffered so the builder
	// goroutine never blocks pushing the signal.
	fired := make(chan string, 4)
	w.SetTableIndexedCallback(func(table *ir.Table) { fired <- table.Name })

	// UNBUFFERED: a send blocks until the builder's loop receives it, so we
	// drive the receive→build→fire sequence one table at a time and observe
	// each build land before we release the next table.
	ch := make(chan *ir.Table)
	schema := &ir.Schema{Tables: []*ir.Table{indexedTable("t0"), indexedTable("t1")}}

	errCh := make(chan error, 1)
	go func() { errCh <- w.BuildTableIndexesFromChannel(context.Background(), schema, ch) }()

	// awaitFire returns the next table whose callback fired, failing the test
	// if none fires within the timeout (the old drain-all path would hang here).
	awaitFire := func(want string) {
		t.Helper()
		select {
		case got := <-fired:
			if got != want {
				t.Fatalf("IndexesBuilt fired for %q; want %q (receive order)", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %q to build — the builder waited for channel close instead of building as-received", want)
		}
	}

	ch <- schema.Tables[0] // release t0
	awaitFire("t0")        // t0 built + marked before t1 is ever sent…
	if n := rec.alterCountFor("t1"); n != 0 {
		t.Fatalf("t1 built (%d ALTERs) before it was sent — builds must be per-received-table, serial", n)
	}

	ch <- schema.Tables[1] // release t1
	awaitFire("t1")        // …and t1 built only after it was received

	close(ch) // signal end; the builder returns nil
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("BuildTableIndexesFromChannel (build-as-received): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("BuildTableIndexesFromChannel did not return after channel close")
	}
}

// TestBuildTableIndexesFromChannel_NoIndexesDrains pins that a vanilla writer
// fed a schema with NO secondary indexes drains the channel and fires the
// per-table callback for every table without touching the database (no jobs
// to build). nil db again makes the no-build invariant observable.
func TestBuildTableIndexesFromChannel_NoIndexesDrains(t *testing.T) {
	var mu sync.Mutex
	var fired []string
	w := &SchemaWriter{db: nil, flavor: FlavorVanilla}
	w.SetTableIndexedCallback(func(table *ir.Table) {
		mu.Lock()
		fired = append(fired, table.Name)
		mu.Unlock()
	})

	tables := []*ir.Table{
		{Name: "p0", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, PrimaryKey: pk()},
		{Name: "p1", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, PrimaryKey: pk()},
	}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	for _, tbl := range tables {
		ch <- tbl
	}
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
		t.Fatalf("BuildTableIndexesFromChannel (no indexes): %v", err)
	}

	mu.Lock()
	sort.Strings(fired)
	mu.Unlock()
	want := []string{"p0", "p1"}
	if !reflect.DeepEqual(fired, want) {
		t.Errorf("callback fired for %v; want %v", fired, want)
	}
}

// indexedTable returns a PK table carrying one secondary index, so the
// build path (if wrongly entered) would have work to do.
func indexedTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk(),
		Indexes:    []*ir.Index{{Name: name + "_v_idx", Columns: []ir.IndexColumn{{Column: "v"}}}},
	}
}

func pk() *ir.Index {
	return &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
}

// --- fake database/sql driver for the index-build + verification pins ---
//
// indexFakeDriver answers the indexExists EXISTS probe from a configurable
// existence flag and records every ALTER … ADD INDEX statement, so a unit
// test can assert the vstream serial builder actually emits ADD INDEX (not a
// silent no-op) and that VerifyIndexes flags a missing index — no
// testcontainers. Mirrors the scriptDriver pattern in
// row_writer_reparent_retry_test.go.

// indexRecorder holds the fake driver's scripted behaviour + instrumentation.
// It is populated before the DB is opened and, on the serial vstream build
// path, only ever touched from the single build goroutine; the mutex guards
// the callback's concurrent snapshot reads regardless.
type indexRecorder struct {
	mu       sync.Mutex
	execs    []string         // recorded ALTER statements, in order
	exists   bool             // EXISTS-probe result (false ⇒ build the index)
	failTbls map[string]error // tableName ⇒ error returned when its ALTER runs
}

func (r *indexRecorder) record(q string) {
	r.mu.Lock()
	r.execs = append(r.execs, q)
	r.mu.Unlock()
}

func (r *indexRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.execs...)
}

// alterCountFor counts recorded ALTERs naming the quoted table.
func (r *indexRecorder) alterCountFor(table string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, q := range r.execs {
		if strings.Contains(q, "`"+table+"`") {
			n++
		}
	}
	return n
}

type indexFakeDriver struct{ rec *indexRecorder }

// identical single-field shape; staticcheck S1016 prefers the conversion.
func (d indexFakeDriver) Open(string) (driver.Conn, error) { return indexFakeConn(d), nil }

type indexFakeConn struct{ rec *indexRecorder }

func (indexFakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (indexFakeConn) Close() error                        { return nil }
func (indexFakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c indexFakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "ALTER") {
		c.rec.record(query)
		for tbl, err := range c.rec.failTbls {
			if err != nil && strings.Contains(query, "`"+tbl+"`") {
				return nil, err
			}
		}
	}
	return driver.RowsAffected(0), nil
}

// QueryContext serves the indexExists EXISTS probe (the only query these
// paths issue) as a single bool row driven by rec.exists.
func (c indexFakeConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	v := int64(0)
	if c.rec.exists {
		v = 1
	}
	return &boolRow{val: v}, nil
}

// boolRow is a one-column, one-row result carrying an int64 (0/1) the
// database/sql layer converts into the bool indexExists scans.
type boolRow struct {
	val  int64
	done bool
}

func (*boolRow) Columns() []string { return []string{"exists"} }
func (*boolRow) Close() error      { return nil }

func (r *boolRow) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.val
	return nil
}

// newIndexFakeDB registers a driver bound to rec and returns a *sql.DB over
// it. sql.Register is global and panics on a duplicate name; t.Name() is
// unique per test within a run, so the name is safe.
func newIndexFakeDB(t *testing.T, rec *indexRecorder) *sql.DB {
	t.Helper()
	name := "sluice-index-fake-" + t.Name()
	sql.Register(name, indexFakeDriver{rec: rec})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open index fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
