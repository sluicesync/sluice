// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pins for the VStream copy-completion door (VSTREAM-COPY-COMPLETION-UNPROVEN,
// Bug 327). Every fixture is a control-row shape an OLDER binary actually
// wrote, never one derived from this change:
//
//   - v0.148-era finished stream: a cursor-less position, established_tables
//     NULL (no record), and no `sync-<id>` header in sluice_migrate_state
//     (the serial lanes recorded none before v0.149.0).
//   - v0.150-era finished stream: the same position and no record (the record
//     arrived in v0.163.0), a `sync-<id>` header at phase complete, and a
//     position row written AFTER it (the anchor, then CDC).
//   - v0.163.1 stopped mid-copy: the frozen cursor-less checkpoint (Bug 325),
//     no record (the anchor that writes it never ran), and a header at phase
//     bulk_copy (written at the copy's start, never flipped).
//   - ≤v0.163.1 stopped between complete and the anchor (F2): a complete
//     header and a position row written BEFORE it.
//   - v0.149–v0.162 re-copy stopped after the target drop and before the
//     header clear (F1): a complete header recording copied rows, and the
//     target table empty.
//   - v0.163.0+ finished stream: a record written by the anchor.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// cursorlessVStreamToken is the 93-byte shape Bug 327's cells persisted.
const cursorlessVStreamToken = `[{"keyspace":"ks1","shard":"0","gtid":"MySQL56/abcd:1-1500"}]`

// headerTime is when every fixture header went to its phase; a position row
// is written before or after it.
var headerTime = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// proofTarget is a target engine whose migrate-state store is store (nil:
// the engine keeps none), or whose open fails with openErr. Its row writer
// answers emptiness from empty (a missing name holds rows), fails with
// probeErr, or, with noProbe, has no emptiness probe.
type proofTarget struct {
	ir.Engine
	store    ir.MigrationStateStore
	openErr  error
	opens    atomic.Int32
	empty    map[string]bool
	probeErr error
	noProbe  bool
}

func (e *proofTarget) OpenMigrationStateStore(context.Context, string) (ir.MigrationStateStore, error) {
	e.opens.Add(1)
	if e.openErr != nil {
		return nil, e.openErr
	}
	return e.store, nil
}

func (e *proofTarget) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	if e.noProbe {
		return struct{ ir.RowWriter }{}, nil
	}
	return &emptyProbeWriter{target: e}, nil
}

type emptyProbeWriter struct {
	ir.RowWriter
	target *proofTarget
}

func (w *emptyProbeWriter) IsTableEmpty(_ context.Context, t *ir.Table) (bool, error) {
	if w.target.probeErr != nil {
		return false, w.target.probeErr
	}
	return w.target.empty[t.Name], nil
}

func (w *emptyProbeWriter) Close() error { return nil }

// failingReadStateStore fails every Read, as a store on an unreachable
// target does; ensureErr makes its EnsureControlTable fail too.
type failingReadStateStore struct {
	*fakeStateStore
	ensureErr error
}

func (failingReadStateStore) Read(context.Context, string) (ir.MigrationState, bool, error) {
	return ir.MigrationState{}, false, errors.New("connection refused")
}

func (f failingReadStateStore) EnsureControlTable(context.Context) error { return f.ensureErr }

// header is a store holding this stream's header at phase, written at
// headerTime, recording copied rows per table.
func header(phase ir.MigrationPhase, copied ...string) *fakeStateStore {
	st := newFakeStateStore()
	progress := map[string]ir.TableProgress{}
	for _, t := range copied {
		progress[t] = ir.TableProgress{State: ir.TableProgressComplete, RowsCopied: 1000}
	}
	st.rows[syncMigrationID("st")] = ir.MigrationState{
		MigrationID: syncMigrationID("st"), Phase: phase, UpdatedAt: headerTime, TableProgress: progress,
	}
	return st
}

// clockedApplier gives scopeDriftApplier the stream's position row (written
// at rowAt; zero = no row) and the control-timestamp clock surface.
type clockedApplier struct {
	*scopeDriftApplier
	rowAt     time.Time
	sameClock bool
	clockErr  error
}

func (a *clockedApplier) ControlTimestampsComparable(context.Context) (bool, error) {
	return a.sameClock, a.clockErr
}

func (a *clockedApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	if a.rowAt.IsZero() {
		return nil, nil
	}
	return []ir.StreamStatus{{StreamID: "st", UpdatedAt: a.rowAt}}, nil
}

// anchoredAfter is a v0.150-era finished stream's applier: no record, its
// position row written after the header, one clock.
func anchoredAfter() *clockedApplier {
	return &clockedApplier{scopeDriftApplier: &scopeDriftApplier{}, rowAt: headerTime.Add(time.Hour), sameClock: true}
}

// vstreamDoorStreamer is a VStream (planetscale) source holding two tables,
// against target.
func vstreamDoorStreamer(t *testing.T, target ir.Engine) *Streamer {
	t.Helper()
	src := &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "planetscale", caps: ir.Capabilities{CDC: ir.CDCVStream}},
		schemas:              map[string]*ir.Schema{"src-dsn": driftSchema("t1_small", "t2_big")},
	}
	return driftStreamer(t, src, target)
}

// proofTargetWith is a target holding targetTables (both source tables when
// none are named) with migrate-state store.
func proofTargetWith(store ir.MigrationStateStore, targetTables ...string) *proofTarget {
	if len(targetTables) == 0 {
		targetTables = []string{"t1_small", "t2_big"}
	}
	return &proofTarget{
		Engine: &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "postgres"},
			schemas:              map[string]*ir.Schema{"tgt-dsn": driftSchema(targetTables...)},
		},
		store: store,
	}
}

var cursorless = ir.Position{Engine: "planetscale", Token: cursorlessVStreamToken}

func requireUnproven(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), vstreamCopyUnprovenMarker) {
		t.Fatalf("want the %s refusal, got %v", vstreamCopyUnprovenMarker, err)
	}
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || refusalARestartRepeats(err) == nil {
		t.Errorf("the refusal must carry the supervisor's no-restart sentinel: %v", err)
	}
	for _, w := range []string{"--restart-from-scratch", "RESTART-FROM-SCRATCH-TARGET-NOT-CLEARED", "--inject-shard-column"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("the refusal does not name %q: %v", w, err)
		}
	}
}

// requireContradicted is the refusal of --assert-copy-finished over positive
// evidence that the copy did not finish.
func requireContradicted(t *testing.T, err error, reason string) {
	t.Helper()
	requireUnproven(t, err)
	if !strings.Contains(err.Error(), "--assert-copy-finished cannot be accepted") || !strings.Contains(err.Error(), reason) {
		t.Fatalf("want --assert-copy-finished refused naming %q, got %v", reason, err)
	}
}

// TestVStreamCopyDoor_EraFixtures: each control-row shape an older binary
// left, judged by the door alone.
func TestVStreamCopyDoor_EraFixtures(t *testing.T) {
	ctx := context.Background()
	before := func() *clockedApplier {
		a := anchoredAfter()
		a.rowAt = headerTime.Add(-time.Hour)
		return a
	}
	cases := []struct {
		name    string
		app     ir.ChangeApplier
		store   ir.MigrationStateStore
		empty   map[string]bool
		refused bool
	}{
		{"v0.163.1 stopped mid-copy: header at bulk_copy", anchoredAfter(), header(ir.MigrationPhaseBulkCopy), nil, true},
		{"v0.163.1 stopped mid-copy: header in tables phase", anchoredAfter(), header(ir.MigrationPhaseTables), nil, true},
		{"v0.163.1 stopped mid-copy: header failed", anchoredAfter(), header(ir.MigrationPhaseFailed), nil, true},
		{"v0.148-era finished: no record, no header", anchoredAfter(), newFakeStateStore(), nil, true},
		{"v0.150-era finished: complete header, row after, copy on target", anchoredAfter(), header(ir.MigrationPhaseComplete, "t1_small", "t2_big"), nil, false},
		{"F1: complete header, copied table emptied on the target", anchoredAfter(), header(ir.MigrationPhaseComplete, "t1_small", "t2_big"), map[string]bool{"t2_big": true}, true},
		{"F2: complete header, row written before it", before(), header(ir.MigrationPhaseComplete), nil, true},
		{"complete header, row in the same instant", &clockedApplier{scopeDriftApplier: &scopeDriftApplier{}, rowAt: headerTime, sameClock: true}, header(ir.MigrationPhaseComplete), nil, true},
		{"complete header, clocks not comparable", &clockedApplier{scopeDriftApplier: &scopeDriftApplier{}, rowAt: headerTime.Add(time.Hour)}, header(ir.MigrationPhaseComplete), nil, true},
		{"complete header, no clock surface", &scopeDriftApplier{}, header(ir.MigrationPhaseComplete), nil, true},
		{"complete header, no position row", &clockedApplier{scopeDriftApplier: &scopeDriftApplier{}, sameClock: true}, header(ir.MigrationPhaseComplete), nil, true},
		{"v0.163.0+ finished: record present, no header", recorded([]string{"t1_small", "t2_big"}), newFakeStateStore(), nil, false},
		{"record present (empty set), header mid-copy", recorded(nil), header(ir.MigrationPhaseBulkCopy), nil, false},
		{"record reads cold start in progress", &scopeDriftApplier{rec: ir.EstablishedRecord{Recorded: true, ColdStartInProgress: true}}, newFakeStateStore(), nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tgt := proofTargetWith(tc.store)
			tgt.empty = tc.empty
			s := vstreamDoorStreamer(t, tgt)
			assertRecord, err := s.phaseRefuseUnprovenVStreamCopy(ctx, tc.app, "st", cursorless)
			if tc.refused {
				requireUnproven(t, err)
			} else if err != nil {
				t.Fatalf("want a pass, got %v", err)
			}
			if assertRecord {
				t.Error("assertRecord without --assert-copy-finished")
			}
		})
	}
}

// TestVStreamCopyDoor_HeaderKeyedByStream: a complete header under another
// stream's id is not this stream's evidence.
func TestVStreamCopyDoor_HeaderKeyedByStream(t *testing.T) {
	st := newFakeStateStore()
	st.rows[syncMigrationID("other")] = ir.MigrationState{MigrationID: syncMigrationID("other"), Phase: ir.MigrationPhaseComplete, UpdatedAt: headerTime}
	st.rows["st"] = ir.MigrationState{MigrationID: "st", Phase: ir.MigrationPhaseComplete, UpdatedAt: headerTime} // unprefixed: a migrate row
	s := vstreamDoorStreamer(t, proofTargetWith(st))
	_, err := s.phaseRefuseUnprovenVStreamCopy(context.Background(), anchoredAfter(), "st", cursorless)
	requireUnproven(t, err)
}

// TestVStreamCopyDoor_FailsClosed: every read the door makes that cannot
// answer refuses, and so does a target that keeps neither marker.
func TestVStreamCopyDoor_FailsClosed(t *testing.T) {
	ctx := context.Background()
	wantErr := func(t *testing.T, err error, sub string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), sub) {
			t.Fatalf("want a refusal carrying %q, got %v", sub, err)
		}
	}
	t.Run("established read error", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete)))
		app := anchoredAfter()
		app.recErr = errors.New("connection reset")
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, app, "st", cursorless)
		wantErr(t, err, "connection reset")
	})
	t.Run("established record undecodable", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete)))
		app := anchoredAfter()
		app.recErr = &ir.UndecodableTableSetError{Column: "established_tables", RawBytes: 3, Recovery: "re-copy", Err: errors.New("bad")}
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, app, "st", cursorless)
		if !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("want the terminal undecodable refusal, got %v", err)
		}
	})
	t.Run("migrate-state store open error", func(t *testing.T) {
		tgt := proofTargetWith(nil)
		tgt.openErr = errors.New("dial tcp: refused")
		_, err := vstreamDoorStreamer(t, tgt).phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		wantErr(t, err, "dial tcp: refused")
	})
	t.Run("migrate-state read error", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(failingReadStateStore{fakeStateStore: newFakeStateStore()}))
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		wantErr(t, err, "connection refused")
	})
	t.Run("target keeps no migrate-state store", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(nil).Engine) // no opener at all
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		requireUnproven(t, err)
	})
	t.Run("applier keeps no established record, header mid-copy", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseBulkCopy)))
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &resumeDispatchApplier{}, "st", cursorless)
		requireUnproven(t, err)
	})
	t.Run("target emptiness probe fails", func(t *testing.T) {
		tgt := proofTargetWith(header(ir.MigrationPhaseComplete, "t2_big"))
		tgt.probeErr = errors.New("lock timeout")
		_, err := vstreamDoorStreamer(t, tgt).phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		wantErr(t, err, "lock timeout")
	})
	t.Run("target has no emptiness probe", func(t *testing.T) {
		tgt := proofTargetWith(header(ir.MigrationPhaseComplete, "t2_big"))
		tgt.noProbe = true
		_, err := vstreamDoorStreamer(t, tgt).phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		wantErr(t, err, "cannot report whether a table holds rows")
	})
	t.Run("clock read fails", func(t *testing.T) {
		app := anchoredAfter()
		app.clockErr = errors.New("server gone")
		_, err := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete))).phaseRefuseUnprovenVStreamCopy(ctx, app, "st", cursorless)
		wantErr(t, err, "server gone")
	})
}

// TestVStreamCopyDoor_ControlTableDDLRefusal (F6): a progress table this role
// cannot bring up to date refuses terminally — under the no-restart sentinel,
// naming `sluice control-tables ddl` — with or without the flag; a restart
// would meet the same refusal.
func TestVStreamCopyDoor_ControlTableDDLRefusal(t *testing.T) {
	ctx := context.Background()
	ddlErr := ir.WithMarker(errors.New("permission denied for schema public"), ir.ErrControlTableDDLRequired)
	for _, assert := range []bool{false, true} {
		s := vstreamDoorStreamer(t, proofTargetWith(failingReadStateStore{fakeStateStore: newFakeStateStore(), ensureErr: ddlErr}))
		s.AssertCopyFinished = assert
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		if err == nil || !errors.Is(err, ir.ErrIncludeTableScopeDrift) || refusalARestartRepeats(err) == nil ||
			!strings.Contains(err.Error(), "sluice control-tables ddl") || !strings.Contains(err.Error(), "sluice deploy-ddl") {
			t.Fatalf("assert=%v: want the terminal control-tables refusal, got %v", assert, err)
		}
	}
}

// TestVStreamCopyDoor_AssertCopyFinished: the flag passes the door where
// nothing contradicts it, records only in-scope tables that hold rows on the
// target, and the next resume passes without it; it is refused over positive
// evidence that the copy did not finish (F3); a record that cannot be written
// refuses.
func TestVStreamCopyDoor_AssertCopyFinished(t *testing.T) {
	ctx := context.Background()
	app := anchoredAfter()
	tgt := proofTargetWith(newFakeStateStore()) // v0.148-era: no header
	s := vstreamDoorStreamer(t, tgt)
	s.AssertCopyFinished = true
	assertRecord, err := s.phaseRefuseUnprovenVStreamCopy(ctx, app, "st", cursorless)
	if err != nil || !assertRecord {
		t.Fatalf("with --assert-copy-finished: assertRecord=%v err=%v; want a pass that asks for the record", assertRecord, err)
	}
	if err := s.recordAssertedCopy(ctx, app, "st"); err != nil {
		t.Fatalf("recordAssertedCopy: %v", err)
	}
	if !app.rec.Recorded || !sameSet(app.rec.Established, []string{"t1_small", "t2_big"}) {
		t.Fatalf("record = %+v; want both in-scope tables established", app.rec)
	}
	again := vstreamDoorStreamer(t, tgt) // the next resume, without the flag
	if _, err := again.phaseRefuseUnprovenVStreamCopy(ctx, app, "st", cursorless); err != nil {
		t.Fatalf("the resume after an asserted one must pass on the record: %v", err)
	}

	t.Run("F4: an in-scope table empty or absent on the target is not recorded", func(t *testing.T) {
		for name, tgt := range map[string]*proofTarget{
			"empty": func() *proofTarget {
				p := proofTargetWith(newFakeStateStore())
				p.empty = map[string]bool{"t2_big": true}
				return p
			}(),
			"absent": proofTargetWith(newFakeStateStore(), "t1_small"),
		} {
			app := anchoredAfter()
			s := vstreamDoorStreamer(t, tgt)
			s.AssertCopyFinished = true
			if err := s.recordAssertedCopy(ctx, app, "st"); err != nil {
				t.Fatal(err)
			}
			if !sameSet(app.rec.Established, []string{"t1_small"}) {
				t.Errorf("%s: established = %v; want only t1_small", name, app.rec.Established)
			}
		}
	})
	t.Run("F3: refused over a header short of complete", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseBulkCopy)))
		s.AssertCopyFinished = true
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		requireContradicted(t, err, `reads phase "bulk_copy"`)
	})
	t.Run("F3: refused over a complete header whose copy left the target", func(t *testing.T) {
		tgt := proofTargetWith(header(ir.MigrationPhaseComplete, "t2_big"))
		tgt.empty = map[string]bool{"t2_big": true}
		s := vstreamDoorStreamer(t, tgt)
		s.AssertCopyFinished = true
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, anchoredAfter(), "st", cursorless)
		requireContradicted(t, err, "empty or absent")
	})
	t.Run("F3: refused over a position row older than the complete header", func(t *testing.T) {
		app := anchoredAfter()
		app.rowAt = headerTime.Add(-time.Minute)
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete)))
		s.AssertCopyFinished = true
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, app, "st", cursorless)
		requireContradicted(t, err, "checkpoint from during the copy")
	})
	t.Run("accepted where the clocks cannot be compared (no contradiction)", func(t *testing.T) {
		app := anchoredAfter()
		app.sameClock = false
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete)))
		s.AssertCopyFinished = true
		if rec, err := s.phaseRefuseUnprovenVStreamCopy(ctx, app, "st", cursorless); err != nil || !rec {
			t.Fatalf("rec=%v err=%v; want the flag accepted", rec, err)
		}
	})
	t.Run("record write fails", func(t *testing.T) {
		failing := anchoredAfter()
		failing.recordErr = errors.New("permission denied for table sluice_cdc_state")
		s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore()))
		s.AssertCopyFinished = true
		err := s.recordAssertedCopy(ctx, failing, "st")
		if err == nil || !strings.Contains(err.Error(), "permission denied") || !strings.Contains(err.Error(), "--assert-copy-finished") {
			t.Fatalf("want a refusal naming the failed record write, got %v", err)
		}
	})
	t.Run("F6: record write needs DDL this role cannot run", func(t *testing.T) {
		failing := anchoredAfter()
		failing.recordErr = ir.WithMarker(errors.New("permission denied for table sluice_cdc_state"), ir.ErrControlTableDDLRequired)
		s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore()))
		err := s.recordAssertedCopy(ctx, failing, "st")
		if err == nil || refusalARestartRepeats(err) == nil || !strings.Contains(err.Error(), "sluice control-tables ddl") {
			t.Fatalf("want the terminal control-tables refusal, got %v", err)
		}
	})
	t.Run("the scope a live-added table widens is recorded", func(t *testing.T) {
		src := &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "planetscale", caps: ir.Capabilities{CDC: ir.CDCVStream}},
			schemas:              map[string]*ir.Schema{"src-dsn": driftSchema("t1_small", "t2_big", "added")},
		}
		s := driftStreamer(t, src, proofTargetWith(newFakeStateStore(), "t1_small", "t2_big", "added"), "t1_small", "t2_big")
		app := anchoredAfter()
		app.liveAdded = []string{"added"}
		if err := s.recordAssertedCopy(ctx, app, "st"); err != nil {
			t.Fatal(err)
		}
		if !sameSet(app.rec.Established, []string{"t1_small", "t2_big", "added"}) {
			t.Fatalf("established = %v", app.rec.Established)
		}
	})
}

// TestVStreamCopyDoor_OtherSourcesUnaffected: the same control-row shape on a
// source that never persisted a position mid-copy is not judged, nor is a
// position read from a backup chain.
func TestVStreamCopyDoor_OtherSourcesUnaffected(t *testing.T) {
	ctx := context.Background()
	for _, cdc := range []ir.CDCMethod{ir.CDCBinlog, ir.CDCLogicalReplication, ir.CDCTriggers} {
		src := &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "src", caps: ir.Capabilities{CDC: cdc}},
			schemas:              map[string]*ir.Schema{"src-dsn": driftSchema("t1_small")},
		}
		tgt := proofTargetWith(newFakeStateStore())
		s := driftStreamer(t, src, tgt)
		if _, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &scopeDriftApplier{}, "st", ir.Position{Engine: "src", Token: "x"}); err != nil {
			t.Errorf("CDC %v: the door judged a non-VStream source: %v", cdc, err)
		}
		if n := tgt.opens.Load(); n != 0 {
			t.Errorf("CDC %v: the door opened the migrate-state store %d times", cdc, n)
		}
	}
	s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore()))
	s.PositionFromManifestStore = struct{ irbackup.Store }{}
	if _, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &scopeDriftApplier{}, "st", cursorless); err != nil {
		t.Errorf("--position-from-manifest: the door judged a chain's position: %v", err)
	}
}

// proofApplier is resumeDispatchApplier with an established-table record,
// every record write logged, and a position row written at rowAt on one
// clock — the Run-level stand-in for a target control row.
type proofApplier struct {
	*resumeDispatchApplier
	rec    ir.EstablishedRecord
	writes []string
	rowAt  time.Time
}

func (a *proofApplier) ReadEstablishedRecord(context.Context, string) (ir.EstablishedRecord, error) {
	return a.rec, nil
}

func (a *proofApplier) BeginColdStartRecord(context.Context, string) error {
	a.writes = append(a.writes, "begin")
	return nil
}

func (a *proofApplier) RecordEstablishedTables(_ context.Context, _ string, tables []string, _ map[string]string) error {
	a.writes = append(a.writes, "established="+strings.Join(tables, ","))
	a.rec = ir.EstablishedRecord{Recorded: true, Established: tables}
	return nil
}

func (a *proofApplier) RecordDroppedTables(_ context.Context, _ string, tables []string) error {
	a.writes = append(a.writes, "dropped="+strings.Join(tables, ","))
	return nil
}

func (a *proofApplier) NarrowEstablishedRecord(context.Context, string, ir.RecordNarrowing) ([]string, error) {
	return nil, nil
}

func (a *proofApplier) ControlTimestampsComparable(context.Context) (bool, error) { return true, nil }

func (a *proofApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	if a.rowAt.IsZero() {
		return nil, nil
	}
	return []ir.StreamStatus{{StreamID: "st", UpdatedAt: a.rowAt}}, nil
}

// runVStreamResume runs a Streamer over a VStream source whose persisted
// position is the applier's, until the plain CDC reader opens (returned true) or the
// run returns.
func runVStreamResume(t *testing.T, carriesCursor bool, applier ir.ChangeApplier, target ir.Engine, assert bool) (cdcOpened bool, resumeOpens int32, err error) {
	t.Helper()
	cdc := &capturingCDCReader{captured: make(chan struct{})}
	source := &copyResumeEngine{
		name:          "planetscale",
		caps:          ir.Capabilities{CDC: ir.CDCVStream},
		cdcReader:     cdc,
		carriesCursor: carriesCursor,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := &Streamer{
		Source:             source,
		Target:             target,
		SourceDSN:          "src",
		TargetDSN:          "tgt",
		StreamID:           "st",
		SchemaChanges:      "refuse",
		Applier:            applier,
		AssertCopyFinished: assert,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	select {
	case <-cdc.captured:
		cancel()
		<-runErr
		return true, source.resumeOpenCalls.Load(), nil
	case err = <-runErr:
		return cdc.once || source.cdcOpenCalls.Load() != 0, source.resumeOpenCalls.Load(), err
	}
}

// TestStreamer_VStreamCursorlessResume_Dispatch: the door at the real
// dispatch — a v0.163.1 mid-copy row refuses before the reader opens and
// before any record write; proven rows resume; a cursor-bearing position
// keeps VSTREAM-COPY-RESUME-WITHDRAWN's precedence; the flag resumes and
// records.
func TestStreamer_VStreamCursorlessResume_Dispatch(t *testing.T) {
	stored := ir.Position{Engine: "postgres", Token: cursorlessVStreamToken}
	newTarget := func(store ir.MigrationStateStore) ir.Engine {
		return &proofTarget{Engine: &copyResumeEngine{name: "postgres"}, store: store}
	}
	newApp := func() *proofApplier {
		return &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{stored: stored, found: true}}
	}

	t.Run("v0.163.1 mid-copy: refused, nothing opened or written", func(t *testing.T) {
		app := newApp()
		app.rowAt = headerTime.Add(time.Hour)
		opened, _, err := runVStreamResume(t, false, app, newTarget(header(ir.MigrationPhaseBulkCopy)), false)
		requireUnproven(t, err)
		if opened {
			t.Error("the plain CDC reader opened: the refused resume reached warmResume")
		}
		if len(app.writes) != 0 {
			t.Errorf("record writes %v on a refused resume", app.writes)
		}
	})
	t.Run("applier with no established record, target with no store: refused", func(t *testing.T) {
		app := &resumeDispatchApplier{stored: stored, found: true}
		opened, _, err := runVStreamResume(t, false, app, &copyResumeEngine{name: "postgres"}, false)
		requireUnproven(t, err)
		if opened {
			t.Error("the plain CDC reader opened")
		}
	})
	t.Run("v0.163+ finished (record): resumes", func(t *testing.T) {
		app := newApp()
		app.rec = ir.EstablishedRecord{Recorded: true}
		opened, _, err := runVStreamResume(t, false, app, newTarget(newFakeStateStore()), false)
		if !opened {
			t.Fatalf("a recorded stream did not warm-resume: %v", err)
		}
	})
	t.Run("v0.150-era finished (complete header, row after): resumes", func(t *testing.T) {
		app := newApp()
		app.rowAt = headerTime.Add(time.Hour)
		opened, _, err := runVStreamResume(t, false, app, newTarget(header(ir.MigrationPhaseComplete)), false)
		if !opened {
			t.Fatalf("a stream with a complete header and a later position did not warm-resume: %v", err)
		}
	})
	t.Run("F2 (complete header, row before): refused", func(t *testing.T) {
		app := newApp()
		app.rowAt = headerTime.Add(-time.Hour)
		opened, _, err := runVStreamResume(t, false, app, newTarget(header(ir.MigrationPhaseComplete)), false)
		requireUnproven(t, err)
		if opened {
			t.Error("the plain CDC reader opened")
		}
	})
	t.Run("cursor-bearing: VSTREAM-COPY-RESUME-WITHDRAWN keeps precedence", func(t *testing.T) {
		app := newApp()
		opened, resumes, err := runVStreamResume(t, true, app, newTarget(newFakeStateStore()), true)
		if err == nil || !strings.Contains(err.Error(), copyResumeWithdrawnMarker) || strings.Contains(err.Error(), vstreamCopyUnprovenMarker) {
			t.Fatalf("want %s (and not %s, nor a pass on --assert-copy-finished), got %v", copyResumeWithdrawnMarker, vstreamCopyUnprovenMarker, err)
		}
		if opened || resumes != 0 || len(app.writes) != 0 {
			t.Errorf("opened=%v resumes=%d writes=%v; want nothing", opened, resumes, app.writes)
		}
	})
	t.Run("--assert-copy-finished: resumes and records; the next run needs no flag", func(t *testing.T) {
		app := newApp()
		tgt := newTarget(newFakeStateStore())
		opened, _, err := runVStreamResume(t, false, app, tgt, true)
		if !opened {
			t.Fatalf("--assert-copy-finished did not resume: %v", err)
		}
		if !app.rec.Recorded {
			t.Fatalf("--assert-copy-finished resumed without recording (writes %v)", app.writes)
		}
		app.writes = nil
		opened, _, err = runVStreamResume(t, false, app, tgt, false)
		if !opened {
			t.Fatalf("the resume after an asserted one did not pass without the flag: %v", err)
		}
		if len(app.writes) != 0 {
			t.Errorf("the second resume wrote %v", app.writes)
		}
	})
	t.Run("--assert-copy-finished over a bulk_copy header: refused, nothing written", func(t *testing.T) {
		app := newApp()
		opened, _, err := runVStreamResume(t, false, app, newTarget(header(ir.MigrationPhaseBulkCopy)), true)
		requireContradicted(t, err, `reads phase "bulk_copy"`)
		if opened || len(app.writes) != 0 {
			t.Errorf("opened=%v writes=%v; want nothing", opened, app.writes)
		}
	})
	t.Run("non-VStream source with the same row shape: resumes", func(t *testing.T) {
		cdc := &capturingCDCReader{captured: make(chan struct{})}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s := &Streamer{
			Source:        &copyResumeEngine{name: "mysql", caps: ir.Capabilities{CDC: ir.CDCBinlog}, cdcReader: cdc},
			Target:        newTarget(newFakeStateStore()),
			SourceDSN:     "src",
			TargetDSN:     "tgt",
			StreamID:      "st",
			SchemaChanges: "refuse",
			Applier:       newApp(),
		}
		runErr := make(chan error, 1)
		go func() { runErr <- s.Run(ctx) }()
		select {
		case <-cdc.captured:
			cancel()
			<-runErr
		case err := <-runErr:
			t.Fatalf("a binlog source with no record and no header was refused: %v", err)
		}
	})
}
