//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// errStoppedAtTheAnchor is the panic a test's anchor hook raises to stop a
// cold start the way a killed process stops: no deferred record write runs
// with a result to record.
var errStoppedAtTheAnchor = errors.New("stopped right after the cold start's anchor write")

// stopAfterTheAnchor runs s to a cold-start anchor write — the cold start's,
// the stopped-cold-start resume's or the multi-database fan-out's — and stops
// it there, as a kill landing in that window does.
func stopAfterTheAnchor(t *testing.T, s *Streamer) {
	t.Helper()
	coldStartAnchorWrittenForTest = func() { panic(errStoppedAtTheAnchor) }
	defer func() { coldStartAnchorWrittenForTest = nil }()
	var runErr error
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		runErr = s.Run(context.Background())
	}()
	select {
	case got := <-done:
		if got != errStoppedAtTheAnchor { //nolint:errorlint // the recovered panic value itself
			t.Fatalf("the run did not stop at a cold-start anchor: recovered %v, run returned %v", got, runErr)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the run never reached a cold-start anchor")
	}
}

// anchorRig drives the k* / kd stream these pins share.
type anchorRig struct {
	t        *testing.T
	engine   string
	eng      ir.Engine
	src, tgt string
	applySrc func(string)
}

func newAnchorRig(t *testing.T, engine, src, tgt string, applySrc func(string)) anchorRig {
	t.Helper()
	eng, ok := engines.Get(engine)
	if !ok {
		t.Fatalf("%s engine not registered", engine)
	}
	return anchorRig{t: t, engine: engine, eng: eng, src: src, tgt: tgt, applySrc: applySrc}
}

func (r anchorRig) run(include string) *Streamer {
	f, err := migcore.NewTableFilter([]string{include}, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	s := &Streamer{Source: r.eng, Target: r.eng, SourceDSN: r.src, TargetDSN: r.tgt, StreamID: "rs", Filter: f}
	if r.engine == "postgres" {
		s.SlotName = "sluice_rs"
	}
	return s
}

// record reads the stream's record.
func (r anchorRig) record() ir.EstablishedRecord {
	r.t.Helper()
	app, err := r.eng.OpenChangeApplier(context.Background(), r.tgt)
	if err != nil {
		r.t.Fatal(err)
	}
	defer migcore.CloseIf(app)
	rec, err := app.(ir.EstablishedTablesStore).ReadEstablishedRecord(context.Background(), "rs")
	if err != nil {
		r.t.Fatal(err)
	}
	return rec
}

// requireTheCopyWithIDs requires the record the anchor wrote: the copy, and
// each table's engine id (R9c — a warm resume after the stop never reaches
// the full record write, so the anchor's record is all the identity check
// has).
func (r anchorRig) requireTheCopyWithIDs(want []string) {
	r.t.Helper()
	rec := r.record()
	if !rec.Recorded || !reflect.DeepEqual(rec.Established, want) {
		r.t.Fatalf("after a stop at the anchor the record reads %+v; want the copy, established %q", rec, want)
	}
	for _, k := range want {
		if rec.IDs[k] == "" {
			r.t.Fatalf("after a stop at the anchor the record carries no id for %q: %+v", k, rec)
		}
	}
}

// narrowThenRewidenRefuses runs the narrowed (kd) restart — a warm resume
// from the anchor that narrows kx to dropped — then requires the k*
// re-widen to refuse.
func (r anchorRig) narrowThenRewidenRefuses() {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.run("kd").Run(ctx) }()
	r.applySrc(`INSERT INTO kd VALUES (2, 'b')`)
	deadline := time.Now().Add(2 * time.Minute)
	for anchorTestRows(r.t, r.engine, r.tgt, "kd") != 2 && time.Now().Before(deadline) {
		select {
		case err := <-done:
			cancel()
			r.t.Fatalf("the narrowed restart exited: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	r.applySrc(`UPDATE kx SET e = 'Y' WHERE id = 2`)
	time.Sleep(2 * time.Second)
	cancel()
	<-done

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel3()
	err := r.run("k*").Run(ctx3)
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || !strings.Contains(err.Error(), "dropped record") {
		r.t.Fatalf("the re-widen after a stop at the anchor and a narrowed restart: %v; want INCLUDE-TABLE-SCOPE-DRIFT, kx dropped", err)
	}
}

// stopInIndexBuildAfterCopies stops s, a Postgres fast-path cold start, in
// its index build once all n table copies have landed (an index build
// overlaps the other tables' copies, and a stop that cut one leaves nothing
// the stopped-cold-start resume can skip), and requires it stopped before
// its anchor.
func (r anchorRig) stopInIndexBuildAfterCopies(s *Streamer, n int) {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	copied := 0
	allCopied := make(chan struct{})
	prevCopied := onTableCopiedObserver
	onTableCopiedObserver = func(string) {
		mu.Lock()
		defer mu.Unlock()
		if copied++; copied == n {
			close(allCopied)
		}
	}
	var once sync.Once
	restore := postgres.SetIndexBuildStartObserverForTest(func(string) {
		once.Do(func() {
			select {
			case <-allCopied:
			case <-time.After(2 * time.Minute):
			}
			cancel()
		})
	})
	err := s.Run(ctx)
	restore()
	onTableCopiedObserver = prevCopied
	if err == nil {
		r.t.Fatal("the cold start was not stopped in its index build")
	}
	if rec := r.record(); rec.Recorded {
		r.t.Fatalf("a cold start stopped before its anchor has a record already: %+v", rec)
	}
}

// anchorTestRows is a table's row count, -1 when it cannot be read.
func anchorTestRows(t *testing.T, engine, dsn, table string) int {
	t.Helper()
	if engine == "mysql" {
		return pollRowCountMySQL(dsn, table)
	}
	return pollRowCount(dsn, table)
}

const anchorFixture = `CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a');
	CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); CREATE INDEX kx_e ON kx (e);
	INSERT INTO kx VALUES (1, 'x'), (2, 'y');`

func pgAnchorRig(t *testing.T) anchorRig {
	t.Helper()
	src, tgt, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	applyDDL(t, src, anchorFixture)
	return newAnchorRig(t, "postgres", src, tgt, func(q string) { applyDDL(t, src, q) })
}

// TestIncludeTableScopeDrift_PgoutputStopAtTheAnchorRefuses is Bug 321 R9b,
// the CI failure of TestIncludeTableScopeDrift_GlobRewidenRefuses made
// deterministic. A stream's FIRST cold start has no control row until its
// CDC anchor, and its record was written only after the anchor; a stop
// between the two (a slow -race run's stop landed there) left a row with a
// position and no record — judged like a stream an older release
// cold-started, on which a glob scope convicts nothing. The narrowed restart
// then recorded no narrowing, and the re-widen streamed kx onto a copy
// missing the narrowed window's changes, at exit 0. The anchor now records
// the copy it describes, with its ids.
func TestIncludeTableScopeDrift_PgoutputStopAtTheAnchorRefuses(t *testing.T) {
	r := pgAnchorRig(t)
	stopAfterTheAnchor(t, r.run("k*"))
	r.requireTheCopyWithIDs([]string{"kd", "kx"})
	r.narrowThenRewidenRefuses()
}

// TestIncludeTableScopeDrift_BinlogStopAtTheAnchorRefuses is the MySQL half.
func TestIncludeTableScopeDrift_BinlogStopAtTheAnchorRefuses(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	applyDDLMySQL(t, src, anchorFixture)
	r := newAnchorRig(t, "mysql", src, tgt, func(q string) { applyDDLMySQL(t, src, q) })
	stopAfterTheAnchor(t, r.run("k*"))
	r.requireTheCopyWithIDs([]string{"kd", "kx"})
	r.narrowThenRewidenRefuses()
}

// TestIncludeTableScopeDrift_BinlogResetStopAtTheAnchorRecordsTheCopy is
// R9e (f): --reset-target-data deletes the control row, its in-progress mark
// with it, so a stop at its anchor must leave the copy on record — like a
// first cold start's — not an in-progress mark or no record. The narrowed
// reset scope (kd) is what is recorded, not the earlier k* copy.
func TestIncludeTableScopeDrift_BinlogResetStopAtTheAnchorRecordsTheCopy(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	applyDDLMySQL(t, src, anchorFixture)
	r := newAnchorRig(t, "mysql", src, tgt, func(q string) { applyDDLMySQL(t, src, q) })
	stopAfterTheAnchor(t, r.run("k*"))
	r.requireTheCopyWithIDs([]string{"kd", "kx"})
	reset := r.run("kd")
	reset.ResetTargetData = true
	stopAfterTheAnchor(t, reset)
	if rec := r.record(); rec.ColdStartInProgress {
		t.Fatalf("a --reset-target-data cold start stopped at its anchor left the in-progress mark: %+v", rec)
	}
	r.requireTheCopyWithIDs([]string{"kd"})
}

// TestIncludeTableScopeDrift_PgoutputStopAtTheAnchorThenRecreateRefuses is
// R9c: without the ids on the anchor's record, a table dropped and
// re-created under the same name while the stream was stopped passed the
// identity check (nothing to compare) instead of refusing. The re-created
// table goes back into the publication, as an operator re-adding it would
// (or a FOR ALL TABLES / FOR TABLES IN SCHEMA publication does by itself),
// so the "no longer in publication" conviction cannot catch it first: only
// the identity check can.
func TestIncludeTableScopeDrift_PgoutputStopAtTheAnchorThenRecreateRefuses(t *testing.T) {
	r := pgAnchorRig(t)
	stopAfterTheAnchor(t, r.run("k*"))
	r.applySrc(`DROP TABLE kx; CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (9, 'new');
		ALTER PUBLICATION sluice_pub ADD TABLE kx`)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := r.run("k*").Run(ctx)
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") {
		t.Fatalf("a restart after a stop at the anchor and a drop+recreate of kx: %v; want TABLE-IDENTITY-CHANGED", err)
	}
}

// TestIncludeTableScopeDrift_PgoutputStopAtTheResumeAnchorRefuses is R9c on
// the stopped-cold-start resume's anchor (the PG fast path): a k* cold start
// stopped in its index build, then the resume stopped right after ITS
// anchor. The resume's anchor records the copy with its ids, and the
// narrowed restart and re-widen behave as after a plain cold start.
func TestIncludeTableScopeDrift_PgoutputStopAtTheResumeAnchorRefuses(t *testing.T) {
	r := pgAnchorRig(t)
	r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
	logs := captureSlog(t)
	stopAfterTheAnchor(t, r.run("k*"))
	if !strings.Contains(logs.String(), coldStartResumedMarker) {
		t.Fatalf("the second run did not take the stopped-cold-start resume path:\n%s", logs.String())
	}
	r.requireTheCopyWithIDs([]string{"kd", "kx"})
	r.narrowThenRewidenRefuses()
}

// TestIncludeTableScopeDrift_MultiDatabaseStopAtTheAnchorRecords is R9c on
// the multi-database fan-out's anchor: the record it writes carries the
// NUL-namespaced keys and their ids.
func TestIncludeTableScopeDrift_MultiDatabaseStopAtTheAnchorRecords(t *testing.T) {
	srcServer, _, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	_, tgtDSN, tgtCleanup := startMySQLBinlog(t) // the fan-out writes each database by its own name
	t.Cleanup(tgtCleanup)
	applyDDLMySQL(t, srcServer, `CREATE TABLE w (id INT PRIMARY KEY); INSERT INTO w VALUES (1)`)
	shopDSN := mustCreateMySQLDatabase(t, srcServer, "shop_db")
	applyDDLMySQL(t, shopDSN, `CREATE TABLE w (id INT PRIMARY KEY); INSERT INTO w VALUES (2)`)
	eng, _ := engines.Get("mysql")
	stopAfterTheAnchor(t, &Streamer{
		Source: eng, Target: eng, SourceDSN: serverDSN(t, srcServer), TargetDSN: tgtDSN, StreamID: "rs",
		DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
	})
	r := newAnchorRig(t, "mysql", srcServer, tgtDSN, nil)
	r.requireTheCopyWithIDs([]string{"shop_db\x00w", "source_db\x00w"})
}
