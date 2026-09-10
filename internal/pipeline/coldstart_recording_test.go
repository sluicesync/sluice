// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"go/ast"
	"reflect"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Audit A0909-P2b: the SERIAL cold start records what the ADR-0079
// parallel lane records.
//
// v0.148.0's cold-start visibility was built inside runColdStartParallel,
// whose gate admits only a Postgres source that exports a shareable
// snapshot. Every other cold start — MySQL/MariaDB binlog, VStream
// (PlanetScale/Vitess), pgtrigger, sqlite-trigger, d1-trigger,
// --schema-already-applied, an ADR-0072 resumable copy, the A0 client-copy
// fallback, and the whole multi-database fan-out — reaches
// runBulkCopyWithOpts instead and recorded nothing at all. These cells pin
// the recording, its inertness on every non-recording caller, and the one
// deliberate asymmetry (no snapshot anchor).

// phaseLoggingStateStore is a fakeStateStore that also keeps the ORDER of
// the phases written to the header, so a test can assert the ladder rather
// than only its last rung.
type phaseLoggingStateStore struct {
	*fakeStateStore

	mu     sync.Mutex
	phases []ir.MigrationPhase
}

func newPhaseLoggingStateStore() *phaseLoggingStateStore {
	return &phaseLoggingStateStore{fakeStateStore: newFakeStateStore()}
}

func (s *phaseLoggingStateStore) Write(ctx context.Context, st ir.MigrationState) error {
	s.mu.Lock()
	s.phases = append(s.phases, st.Phase)
	s.mu.Unlock()
	return s.fakeStateStore.Write(ctx, st)
}

func (s *phaseLoggingStateStore) phaseLog() []ir.MigrationPhase {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ir.MigrationPhase(nil), s.phases...)
}

// panicOnUseStateStore fails LOUDLY on any store call. It is how the
// zero-value-inertness cell proves the gate is [resumeContext.writes] and
// not "the store happened to be nil": a context that is not recording must
// not touch a store it nonetheless carries.
type panicOnUseStateStore struct{ t *testing.T }

func (p panicOnUseStateStore) EnsureControlTable(context.Context) error {
	p.t.Fatal("EnsureControlTable called on a non-recording context")
	return nil
}

func (p panicOnUseStateStore) Read(context.Context, string) (ir.MigrationState, bool, error) {
	p.t.Fatal("Read called on a non-recording context")
	return ir.MigrationState{}, false, nil
}

func (p panicOnUseStateStore) Write(context.Context, ir.MigrationState) error {
	p.t.Fatal("Write called on a non-recording context — `migrate` and every zero-options caller must be untouched")
	return nil
}

func (p panicOnUseStateStore) WriteTableProgress(context.Context, string, string, ir.TableProgress) error {
	p.t.Fatal("WriteTableProgress called on a non-recording context")
	return nil
}

func (p panicOnUseStateStore) ClearMigration(context.Context, string) error {
	p.t.Fatal("ClearMigration called on a non-recording context")
	return nil
}

func (panicOnUseStateStore) Close() error { return nil }

// alwaysFailingStateStore errors on every write. Recording is
// observability: a store that cannot be written must cost the status
// surface, never the copy.
type alwaysFailingStateStore struct{ *fakeStateStore }

func (alwaysFailingStateStore) Write(context.Context, ir.MigrationState) error {
	return errors.New("state store is down")
}

func (alwaysFailingStateStore) WriteTableProgress(context.Context, string, string, ir.TableProgress) error {
	return errors.New("state store is down")
}

// rowEmittingReader emits rowsPerTable rows for every table, so the
// recorded RowsCopied is a number a test can hold to something.
type rowEmittingReader struct{ rowsPerTable int }

func (r *rowEmittingReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	go func() {
		defer close(ch)
		for i := 0; i < r.rowsPerTable; i++ {
			select {
			case <-ctx.Done():
				return
			case ch <- ir.Row{"id": int64(i), "t": table.Name}:
			}
		}
	}()
	return ch, nil
}

func (*rowEmittingReader) Err() error { return nil }

// drainingRowWriter consumes the row channel (so the reader's rows are
// actually counted) and logs the table.
type drainingRowWriter struct {
	mu       sync.Mutex
	phaseLog *[]string
}

func (w *drainingRowWriter) WriteRows(_ context.Context, table *ir.Table, rows <-chan ir.Row) error {
	for range rows { //nolint:revive // draining is the point
	}
	w.mu.Lock()
	*w.phaseLog = append(*w.phaseLog, "WriteRows:"+table.Name)
	w.mu.Unlock()
	return nil
}

func twoTableRecordingSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
}

// runRecordedSerialCopy drives the REAL serial cold-start entry point
// (runBulkCopyWithOpts — the function streamer_coldstart.go and
// streamer_multidb.go call) with a recording context, and returns the
// store it recorded into plus the copy's own phase log.
func runRecordedSerialCopy(t *testing.T, store ir.MigrationStateStore, migrationID, namespace string, rowsPerTable int) []string {
	t.Helper()
	var phaseLog []string
	sw := &recordingSchemaWriter{phaseLog: &phaseLog}
	rw := &drainingRowWriter{phaseLog: &phaseLog}
	if err := runBulkCopyWithOpts(context.Background(), twoTableRecordingSchema(), &rowEmittingReader{rowsPerTable: rowsPerTable}, sw, rw, bulkCopyOpts{
		Recording:         resumeContext{store: store, migrationID: migrationID, enabled: true, noResume: true},
		ProgressNamespace: namespace,
	}); err != nil {
		t.Fatalf("runBulkCopyWithOpts: %v", err)
	}
	return phaseLog
}

// The headline cell: a serial cold start leaves a readable phase ladder
// and one complete per-table row per table, carrying real row counts.
func TestSerialColdStartRecordsPhasesAndPerTableProgress(t *testing.T) {
	t.Parallel()
	store := newPhaseLoggingStateStore()
	runRecordedSerialCopy(t, store, "sync-prod-cutover", "", 7)

	wantPhases := []ir.MigrationPhase{
		ir.MigrationPhaseTables,
		ir.MigrationPhaseBulkCopy,
		ir.MigrationPhaseIdentitySync,
		ir.MigrationPhaseIndexes,
		ir.MigrationPhaseConstraints,
		ir.MigrationPhaseViews,
	}
	if got := store.phaseLog(); !reflect.DeepEqual(got, wantPhases) {
		t.Errorf("the serial cold start recorded phases %v, want %v — `sync status` reads the header phase, so a "+
			"missing rung is a phase an operator cannot see the run is in", got, wantPhases)
	}

	state, ok := store.get("sync-prod-cutover")
	if !ok {
		t.Fatal("the serial cold start wrote no header row at all; `sync status` would report the stream as absent " +
			"for the whole copy — the exact blackout this feature exists to end")
	}
	for _, table := range []string{"users", "orders"} {
		entry, present := state.TableProgress[table]
		if !present {
			t.Errorf("table %q has no recorded progress row", table)
			continue
		}
		if entry.State != ir.TableProgressComplete {
			t.Errorf("table %q recorded state %q, want %q", table, entry.State, ir.TableProgressComplete)
		}
		if entry.RowsCopied != 7 {
			t.Errorf("table %q recorded %d rows copied, want 7 — a complete row carrying zero rows is a number "+
				"nothing measured", table, entry.RowsCopied)
		}
	}
}

// The v0.99.51 zero-value rule, pinned at the struct: an omitted
// Recording is INERT. `migrate`, every test and every future caller that
// does not know the field exists must write nothing — and the gate must
// be `enabled`, not "the store is nil", which is why the store here fails
// the test if it is touched at all.
func TestBulkCopyRecordingZeroValueTouchesNoStore(t *testing.T) {
	t.Parallel()
	var phaseLog []string
	sw := &recordingSchemaWriter{phaseLog: &phaseLog}
	rw := &drainingRowWriter{phaseLog: &phaseLog}
	// A store is present but the context is not recording: every writer
	// must short-circuit before reaching it.
	opts := bulkCopyOpts{Recording: resumeContext{store: panicOnUseStateStore{t: t}, migrationID: "m-inert"}}
	if err := runBulkCopyWithOpts(context.Background(), twoTableRecordingSchema(), &rowEmittingReader{rowsPerTable: 3}, sw, rw, opts); err != nil {
		t.Fatalf("runBulkCopyWithOpts: %v", err)
	}
	if rec := newTableProgressRecorder(opts.Recording, ""); rec != nil {
		t.Error("newTableProgressRecorder built a recorder for a non-recording context")
	}

	// And the copy itself is unchanged: the same phase log a zero-value
	// bulkCopyOpts produces.
	var bareLog []string
	bareSW := &recordingSchemaWriter{phaseLog: &bareLog}
	bareRW := &drainingRowWriter{phaseLog: &bareLog}
	if err := runBulkCopyWithOpts(context.Background(), twoTableRecordingSchema(), &rowEmittingReader{rowsPerTable: 3}, bareSW, bareRW, bulkCopyOpts{}); err != nil {
		t.Fatalf("runBulkCopyWithOpts (bare): %v", err)
	}
	if !reflect.DeepEqual(phaseLog, bareLog) {
		t.Errorf("threading a non-recording context changed the copy's phase order:\n with = %v\n bare = %v", phaseLog, bareLog)
	}
}

// Recording is OBSERVABILITY. A store that refuses every write costs the
// status surface and nothing else — failing a working migration because a
// progress row could not be written would be strictly worse than the
// blackout it replaces.
func TestSerialColdStartRecordingFailureNeverFailsTheCopy(t *testing.T) {
	t.Parallel()
	var phaseLog []string
	sw := &recordingSchemaWriter{phaseLog: &phaseLog}
	rw := &drainingRowWriter{phaseLog: &phaseLog}
	err := runBulkCopyWithOpts(context.Background(), twoTableRecordingSchema(), &rowEmittingReader{rowsPerTable: 2}, sw, rw, bulkCopyOpts{
		Recording: resumeContext{
			store:       alwaysFailingStateStore{fakeStateStore: newFakeStateStore()},
			migrationID: "sync-x",
			enabled:     true,
			noResume:    true,
		},
	})
	if err != nil {
		t.Fatalf("a failing progress store failed the COPY: %v", err)
	}
	if len(phaseLog) == 0 {
		t.Fatal("the copy did not run at all")
	}
}

// The multi-database fan-out copies N namespaces under ONE stream id, so
// two databases holding a same-named table must not upsert each other's
// progress row. Without the qualifier the second database's copy silently
// overwrites the first's, and the recorded table count is short by every
// collision — invisible, because nothing reads these rows back on that
// path today.
func TestMultiDatabaseColdStartQualifiesProgressKeys(t *testing.T) {
	t.Parallel()
	store := newPhaseLoggingStateStore()
	// The fan-out's shape: one recording context, one call per database.
	runRecordedSerialCopy(t, store, "sync-fanout", "shopdb", 4)
	runRecordedSerialCopy(t, store, "sync-fanout", "analyticsdb", 9)

	state, ok := store.get("sync-fanout")
	if !ok {
		t.Fatal("no recorded header for the fan-out")
	}
	want := map[string]int64{
		"shopdb.users":       4,
		"shopdb.orders":      4,
		"analyticsdb.users":  9,
		"analyticsdb.orders": 9,
	}
	if len(state.TableProgress) != len(want) {
		t.Errorf("the fan-out recorded %d progress rows, want %d — a collision means one database's copy was "+
			"attributed to another: %v", len(state.TableProgress), len(want), state.TableProgress)
	}
	for key, rows := range want {
		entry, present := state.TableProgress[key]
		if !present {
			t.Errorf("no progress row under %q", key)
			continue
		}
		if entry.RowsCopied != rows {
			t.Errorf("%q recorded %d rows, want %d", key, entry.RowsCopied, rows)
		}
	}
}

// A table copied as M work-stealing chunks is complete when its LAST
// chunk lands, not its first. Marking it complete early records a
// finished copy for a table still being read — and "every in-scope table
// recorded complete" is exactly the evidence the stopped-cold-start
// resume gate stands on.
func TestTableProgressRecorderCompletesOnTheLastChunk(t *testing.T) {
	t.Parallel()
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "sync-chunked", enabled: true, noResume: true}
	rec := newTableProgressRecorder(rc, "")
	table := &ir.Table{Name: "big"}
	rec.expectItems(table, 3)

	ctx := context.Background()
	rec.started(ctx, table)
	for i, wantRows := range []int64{10, 30, 60} {
		rec.completed(ctx, table, 10+int64(i)*10)
		state, _ := store.get("sync-chunked")
		entry := state.TableProgress["big"]
		if entry.RowsCopied != wantRows {
			t.Errorf("after chunk %d: recorded %d rows, want %d (the per-chunk counts must SUM)", i+1, entry.RowsCopied, wantRows)
		}
		wantState := ir.TableProgressInProgress
		if i == 2 {
			wantState = ir.TableProgressComplete
		}
		if entry.State != wantState {
			t.Errorf("after chunk %d of 3: state %q, want %q", i+1, entry.State, wantState)
		}
	}
}

// A repeated `started` — one per work-stealing chunk of the same table —
// must not zero the running count a peer chunk has been accumulating.
func TestTableProgressRecorderStartedDoesNotZeroAPeersCount(t *testing.T) {
	t.Parallel()
	store := newFakeStateStore()
	rec := newTableProgressRecorder(resumeContext{store: store, migrationID: "sync-x", enabled: true, noResume: true}, "")
	table := &ir.Table{Name: "big"}
	rec.expectItems(table, 2)
	ctx := context.Background()
	rec.started(ctx, table)
	rec.completed(ctx, table, 100)
	rec.started(ctx, table) // the second chunk's pipeline arrives
	state, _ := store.get("sync-x")
	if got := state.TableProgress["big"].RowsCopied; got != 100 {
		t.Errorf("a peer chunk's started() reset the running count to %d, want 100", got)
	}
}

// The deliberate asymmetry, stated at the code and pinned here: a SERIAL
// cold start records progress but NO snapshot anchor, so
// [Streamer.resumeStoppedColdStart] keeps declining it exactly as it did
// before this lane recorded anything.
//
// Scope of this gate, stated so its name cannot read broader than the
// truth: it grades the two functions that carry the promise
// ([beginRecordedColdStart] refuses to write an empty anchor,
// [gradeRecordedColdStartHeader] refuses an anchorless header). The
// companion roster below is what holds the CALL SITES to it.
func TestSerialColdStartRecordsNoSnapshotAnchor(t *testing.T) {
	t.Parallel()
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "sync-serial", enabled: true, noResume: true}

	// What coldStartRunCopy's serial branch passes.
	beginRecordedColdStart(context.Background(), rc, ir.SnapshotAnchorRecord{})
	runRecordedSerialCopy(t, store, "sync-serial", "", 5)

	state, ok := store.get("sync-serial")
	if !ok {
		t.Fatal("no recorded header")
	}
	if state.SnapshotAnchor != "" {
		t.Fatalf("the serial lane recorded a snapshot anchor (%q); resumeStoppedColdStart would then SKIP a copy "+
			"on a lane its ladder was never designed for (--schema-already-applied re-runs DDL the operator "+
			"forbade; a mid-COPY resume and the A0 client-copy fallback have never been exercised through it)",
			state.SnapshotAnchor)
	}
	if why := gradeRecordedColdStartHeader(state, true); why == "" {
		t.Fatal("the stopped-cold-start resume gate ACCEPTED a serial lane's recorded header; the serial lanes' " +
			"stop behaviour has silently changed")
	}
}

// The call-site roster for the asymmetry above: exactly one place in the
// package may build an ir.SnapshotAnchorRecord carrying an Anchor, and it
// is the fast branch of coldStartRunCopy. A new lane that starts
// recording an anchor fails HERE rather than in a regression cycle.
//
// Reach, stated: this walks the internal/pipeline package's non-test
// files only. An engine package writing an anchor through the store's own
// API is out of its scope — that is the store's contract, pinned by the
// per-engine anchor integration tests.
func TestSnapshotAnchorRecordSitesRoster(t *testing.T) {
	t.Parallel()
	// file → the function that may build an anchor-bearing record there.
	allowed := map[string]string{
		"streamer_coldstart.go": "coldStartRunCopy",
	}
	_, files := parsePipelineFiles(t)
	found := map[string]string{}
	for name, f := range files {
		var fn string
		ast.Inspect(f, func(n ast.Node) bool {
			if d, ok := n.(*ast.FuncDecl); ok {
				fn = d.Name.Name
				return true
			}
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			sel, ok := lit.Type.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "SnapshotAnchorRecord" {
				return true
			}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Anchor" {
					found[name] = fn
				}
			}
			return true
		})
	}
	if len(found) == 0 {
		t.Fatal("no anchor-bearing ir.SnapshotAnchorRecord literal found anywhere in the package — the walk is " +
			"not seeing what it grades (anti-vacuity floor), or the fast lane stopped recording its anchor")
	}
	if !reflect.DeepEqual(found, allowed) {
		t.Errorf("the anchor-recording sites are %v, want %v.\n"+
			"An anchor licenses resumeStoppedColdStart to SKIP a copy. Adding one on a new lane means that "+
			"ladder now runs against a shape it was never measured on — widen the ladder first, then this list.",
			found, allowed)
	}
}
