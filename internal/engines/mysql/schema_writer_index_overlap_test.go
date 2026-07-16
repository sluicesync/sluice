// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
// indexFakeDriver answers the existence probes — the batched
// [probeCatalogPairs] chunk query the bulk phases use (audit V-1) and the
// legacy single-object EXISTS probe the shape appliers kept — from a
// configurable existence flag, and records every ALTER … ADD INDEX statement,
// so a unit test can assert the vstream serial builder actually emits ADD
// INDEX (not a silent no-op) and that VerifyIndexes flags a missing index —
// no testcontainers. Mirrors the scriptDriver pattern in
// row_writer_reparent_retry_test.go.

// indexRecorder holds the fake driver's scripted behaviour + instrumentation.
// It is populated before the DB is opened and, on the serial vstream build
// path, only ever touched from the single build goroutine; the mutex guards
// the callback's concurrent snapshot reads regardless.
type indexRecorder struct {
	mu       sync.Mutex
	execs    []string         // recorded ALTER statements, in order
	queries  []recordedQuery  // recorded catalog probes, in order (the V-1 shape pins)
	exists   bool             // EXISTS-probe result (false ⇒ build the index)
	failTbls map[string]error // tableName ⇒ error returned when its ALTER runs

	// answerUppercase makes the batched probe return its (table, name) rows
	// UPPERCASED — modelling a target whose catalog reports a different
	// identifier case than the IR carries (lower_case_table_names variance).
	// The foldCatalogPair pin asserts the set compare still matches.
	answerUppercase bool

	// transientFailsLeft scripts the reparent-retry pins (audit N-15b):
	// the table's first N ALTERs return a CLASSIFIED transient
	// (errSimulatedReparent) AFTER being recorded — modelling an ALTER the
	// reparent killed (or that committed server-side but died unacked).
	transientFailsLeft map[string]int
	// probeBuilt flips the EXISTS probe from the static `exists` flag to
	// "answer from recorded ALTERs": an index whose ALTER was already
	// emitted probes as existing — the committed-but-unacked shape the
	// detect-then-skip idempotency (no-double-create) pin needs.
	probeBuilt bool

	// driftDefs scripts, per index NAME, the definition the MED-D0-8
	// drift probe serves for an existing index. Absent names get the
	// default single-column non-unique BTREE over `v` — matching the
	// unit schemas' intended definitions, so no drift WARN fires in the
	// pre-existing pins.
	driftDefs map[string]fakeIndexDef
}

// fakeIndexDef is one scripted catalog definition for the drift probe.
type fakeIndexDef struct {
	unique    bool
	columns   []string
	indexType string // "" serves BTREE
}

// errSimulatedReparent classifies retriable via classifyApplierError's
// ADR-0108 "reparent" text fallback — the exact production shape an
// un-framed PlanetScale/vtgate reparent surfaces.
var errSimulatedReparent = errors.New("vttablet: operation interrupted by emergency reparent (simulated)")

// recordedQuery is one catalog probe the fake served: its SQL text and
// parameter count, enough for the V-1 batched-shape pins to assert "one
// query, everything parameterized".
type recordedQuery struct {
	query string
	args  int
}

func (r *indexRecorder) record(q string) {
	r.mu.Lock()
	r.execs = append(r.execs, q)
	r.mu.Unlock()
}

func (r *indexRecorder) recordQuery(q string, args int) {
	r.mu.Lock()
	r.queries = append(r.queries, recordedQuery{query: q, args: args})
	r.mu.Unlock()
}

// querySnapshot returns the recorded catalog probes, in order.
func (r *indexRecorder) querySnapshot() []recordedQuery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedQuery(nil), r.queries...)
}

// takeTransientFail reports whether the table's ALTER should fail with the
// simulated reparent transient this time, consuming one scripted failure.
func (r *indexRecorder) takeTransientFail(q string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for tbl, left := range r.transientFailsLeft {
		if left > 0 && strings.Contains(q, "`"+tbl+"`") {
			r.transientFailsLeft[tbl] = left - 1
			return true
		}
	}
	return false
}

// indexProbeAnswer answers the existence probes — per NAME, for both the
// batched [probeCatalogPairs] chunk and the legacy single EXISTS probe:
// probeBuilt mode answers from the recorded ALTERs (emitted ⇒ landed), else
// the static flag.
func (r *indexRecorder) indexProbeAnswer(indexName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.probeBuilt {
		return r.exists
	}
	for _, q := range r.execs {
		if strings.Contains(q, "`"+indexName+"`") {
			return true
		}
	}
	return false
}

func (r *indexRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.execs...)
}

// driftDefFor answers the drift probe's served definition for one index
// name: scripted, or the default matching the unit schemas' intent.
func (r *indexRecorder) driftDefFor(name string) fakeIndexDef {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.driftDefs[name]; ok {
		return d
	}
	return fakeIndexDef{columns: []string{"v"}}
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
		if c.rec.takeTransientFail(query) {
			return nil, errSimulatedReparent
		}
		for tbl, err := range c.rec.failTbls {
			if err != nil && strings.Contains(query, "`"+tbl+"`") {
				return nil, err
			}
		}
	}
	return driver.RowsAffected(0), nil
}

// QueryContext serves the two existence-probe shapes these paths issue.
//
// The batched [probeCatalogPairs] chunk (audit V-1) — recognisable by its two
// IN groups, args = (schema, tables…, names…) — answers with one (table,
// name) row for every tables×names combination whose NAME probes true via
// [indexRecorder.indexProbeAnswer] (probeBuilt answers from recorded ALTERs,
// else the static exists flag). The cross-product overshoot models "this
// name exists on every probed table", which the production set-compare
// tolerates by contract (callers only test wanted-pair membership) and the
// unit schemas never alias (index names are per-table unique).
//
// Anything else is the legacy single-object EXISTS probe (args carry
// (schema, table, name)), answered as a single bool row exactly as before.
func (c indexFakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.rec.recordQuery(query, len(args))
	// The MED-D0-8 definition-drift probe — recognisable by its
	// seq_in_index ORDER BY; args = (schema, table, names…). One row per
	// (existing index × scripted column), from driftDefs or the default.
	if strings.Contains(query, "seq_in_index") {
		var rows indexDefRows
		for i := 2; i < len(args); i++ {
			name, _ := args[i].Value.(string)
			if !c.rec.indexProbeAnswer(name) {
				continue
			}
			def := c.rec.driftDefFor(name)
			outName := name
			if c.rec.answerUppercase {
				outName = strings.ToUpper(name)
			}
			nonUnique := int64(1)
			if def.unique {
				nonUnique = 0
			}
			indexType := def.indexType
			if indexType == "" {
				indexType = "BTREE"
			}
			for _, col := range def.columns {
				rows.rows = append(rows.rows, [6]driver.Value{outName, nonUnique, col, nil, "A", indexType})
			}
		}
		return &rows, nil
	}
	if nTables, nNames, ok := batchedProbeShape(query); ok {
		if len(args) != 1+nTables+nNames {
			return nil, fmt.Errorf("batched probe args = %d; want %d (schema + %d tables + %d names): %s",
				len(args), 1+nTables+nNames, nTables, nNames, query)
		}
		argStr := func(i int) string {
			s, _ := args[i].Value.(string)
			return s
		}
		var pairs [][2]string
		for t := 0; t < nTables; t++ {
			for n := 0; n < nNames; n++ {
				table, name := argStr(1+t), argStr(1+nTables+n)
				if !c.rec.indexProbeAnswer(name) {
					continue
				}
				if c.rec.answerUppercase {
					table, name = strings.ToUpper(table), strings.ToUpper(name)
				}
				pairs = append(pairs, [2]string{table, name})
			}
		}
		return &pairRows{pairs: pairs}, nil
	}
	indexName := ""
	if len(args) >= 3 {
		if s, ok := args[2].Value.(string); ok {
			indexName = s
		}
	}
	v := int64(0)
	if c.rec.indexProbeAnswer(indexName) {
		v = 1
	}
	return &boolRow{val: v}, nil
}

// batchedProbeShape recognises a [probeCatalogPairs] chunk query and returns
// its two IN groups' placeholder counts. ok=false for any other query.
func batchedProbeShape(query string) (nTables, nNames int, ok bool) {
	segs := strings.Split(query, " IN (")
	if len(segs) != 3 {
		return 0, 0, false
	}
	count := func(seg string) int {
		group, _, found := strings.Cut(seg, ")")
		if !found {
			return 0
		}
		return strings.Count(group, "?")
	}
	return count(segs[1]), count(segs[2]), true
}

// indexDefRows is a six-column driver result streaming
// (index_name, non_unique, column_name, sub_part, collation, index_type)
// rows — the MED-D0-8 definition probe's wire shape.
type indexDefRows struct {
	rows [][6]driver.Value
	next int
}

func (*indexDefRows) Columns() []string {
	return []string{"index_name", "non_unique", "column_name", "sub_part", "collation", "index_type"}
}
func (*indexDefRows) Close() error { return nil }

func (r *indexDefRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next][:])
	r.next++
	return nil
}

// pairRows is a two-column driver result streaming (table, name) rows —
// the batched probe's wire shape.
type pairRows struct {
	pairs [][2]string
	next  int
}

func (*pairRows) Columns() []string { return []string{"table_name", "name"} }
func (*pairRows) Close() error      { return nil }

func (r *pairRows) Next(dest []driver.Value) error {
	if r.next >= len(r.pairs) {
		return io.EOF
	}
	dest[0] = r.pairs[r.next][0]
	dest[1] = r.pairs[r.next][1]
	r.next++
	return nil
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

// indexFakeSeq disambiguates driver names when one test opens several fake
// DBs (e.g. the chunking pin's present/absent recorders) — sql.Register is
// global and panics on a duplicate name, and t.Name() alone is only unique
// per TEST, not per open.
var indexFakeSeq atomic.Int64

// newIndexFakeDB registers a driver bound to rec and returns a *sql.DB over
// it. The name combines t.Name() (readable) with a process-wide sequence
// number (unique across multiple opens within one test).
func newIndexFakeDB(t *testing.T, rec *indexRecorder) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("sluice-index-fake-%s-%d", t.Name(), indexFakeSeq.Add(1))
	sql.Register(name, indexFakeDriver{rec: rec})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open index fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
