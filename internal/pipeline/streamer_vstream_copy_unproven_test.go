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
//     arrived in v0.163.0), but a `sync-<id>` header at phase complete.
//   - v0.163.1 stopped mid-copy: the frozen cursor-less checkpoint (Bug 325),
//     no record (the anchor that writes it never ran), and a header at phase
//     bulk_copy (written at the copy's start, never flipped).
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

// proofTarget is a target engine whose migrate-state store is store (nil:
// the engine keeps none), or whose open fails with openErr.
type proofTarget struct {
	ir.Engine
	store   ir.MigrationStateStore
	openErr error
	opens   atomic.Int32
}

func (e *proofTarget) OpenMigrationStateStore(context.Context, string) (ir.MigrationStateStore, error) {
	e.opens.Add(1)
	if e.openErr != nil {
		return nil, e.openErr
	}
	return e.store, nil
}

// failingReadStateStore fails every Read, as a store on an unreachable
// target does.
type failingReadStateStore struct{ *fakeStateStore }

func (failingReadStateStore) Read(context.Context, string) (ir.MigrationState, bool, error) {
	return ir.MigrationState{}, false, errors.New("connection refused")
}

func header(phase ir.MigrationPhase) *fakeStateStore {
	st := newFakeStateStore()
	st.rows[syncMigrationID("st")] = ir.MigrationState{MigrationID: syncMigrationID("st"), Phase: phase}
	return st
}

// vstreamDoorStreamer is a VStream (planetscale) source holding two tables,
// against target. Its target engine serves the same tables.
func vstreamDoorStreamer(t *testing.T, target ir.Engine) *Streamer {
	t.Helper()
	src := &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "planetscale", caps: ir.Capabilities{CDC: ir.CDCVStream}},
		schemas:              map[string]*ir.Schema{"src-dsn": driftSchema("t1_small", "t2_big")},
	}
	return driftStreamer(t, src, target)
}

func proofTargetWith(store ir.MigrationStateStore) *proofTarget {
	return &proofTarget{
		Engine: &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "postgres"},
			schemas:              map[string]*ir.Schema{"tgt-dsn": driftSchema("t1_small", "t2_big")},
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
	for _, w := range []string{"--restart-from-scratch", "--assert-copy-finished", "v0.149.0", "v0.163.1"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("the refusal does not name %q: %v", w, err)
		}
	}
}

// TestVStreamCopyDoor_EraFixtures: each control-row shape an older binary
// left, judged by the door alone.
func TestVStreamCopyDoor_EraFixtures(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		app     *scopeDriftApplier
		store   ir.MigrationStateStore
		refused bool
	}{
		{"v0.163.1 stopped mid-copy: no record, header at bulk_copy", &scopeDriftApplier{}, header(ir.MigrationPhaseBulkCopy), true},
		{"v0.163.1 stopped mid-copy: no record, header in tables phase", &scopeDriftApplier{}, header(ir.MigrationPhaseTables), true},
		{"v0.163.1 stopped mid-copy: no record, header failed", &scopeDriftApplier{}, header(ir.MigrationPhaseFailed), true},
		{"v0.148-era finished: no record, no header", &scopeDriftApplier{}, newFakeStateStore(), true},
		{"v0.150-era finished: no record, header complete", &scopeDriftApplier{}, header(ir.MigrationPhaseComplete), false},
		{"v0.163.0+ finished: record present, no header", recorded([]string{"t1_small", "t2_big"}), newFakeStateStore(), false},
		{"record present (empty set), header mid-copy", recorded(nil), header(ir.MigrationPhaseBulkCopy), false},
		{"record reads cold start in progress", &scopeDriftApplier{rec: ir.EstablishedRecord{Recorded: true, ColdStartInProgress: true}}, newFakeStateStore(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := vstreamDoorStreamer(t, proofTargetWith(tc.store))
			assertRecord, err := s.phaseRefuseUnprovenVStreamCopy(ctx, tc.app, "st", cursorless)
			if tc.refused {
				requireUnproven(t, err)
			} else if err != nil {
				t.Fatalf("want a pass, got %v", err)
			}
			if assertRecord {
				t.Error("assertRecord without --assert-copy-finished")
			}
			if len(tc.app.writes) != 0 {
				t.Errorf("the door wrote %v; it only reads", tc.app.writes)
			}
		})
	}
}

// TestVStreamCopyDoor_HeaderKeyedByStream: a complete header under another
// stream's id is not this stream's evidence.
func TestVStreamCopyDoor_HeaderKeyedByStream(t *testing.T) {
	st := newFakeStateStore()
	st.rows[syncMigrationID("other")] = ir.MigrationState{MigrationID: syncMigrationID("other"), Phase: ir.MigrationPhaseComplete}
	st.rows["st"] = ir.MigrationState{MigrationID: "st", Phase: ir.MigrationPhaseComplete} // unprefixed: a migrate row
	s := vstreamDoorStreamer(t, proofTargetWith(st))
	_, err := s.phaseRefuseUnprovenVStreamCopy(context.Background(), &scopeDriftApplier{}, "st", cursorless)
	requireUnproven(t, err)
}

// TestVStreamCopyDoor_FailsClosed: every read the door makes that cannot
// answer refuses, and so does a target that keeps neither marker.
func TestVStreamCopyDoor_FailsClosed(t *testing.T) {
	ctx := context.Background()
	t.Run("established read error", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete)))
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &scopeDriftApplier{recErr: errors.New("connection reset")}, "st", cursorless)
		if err == nil || !strings.Contains(err.Error(), "connection reset") {
			t.Fatalf("want the read failure, got %v", err)
		}
	})
	t.Run("established record undecodable", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete)))
		bad := &ir.UndecodableTableSetError{Column: "established_tables", RawBytes: 3, Recovery: "re-copy", Err: errors.New("bad")}
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &scopeDriftApplier{recErr: bad}, "st", cursorless)
		if !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("want the terminal undecodable refusal, got %v", err)
		}
	})
	t.Run("migrate-state store open error", func(t *testing.T) {
		tgt := proofTargetWith(nil)
		tgt.openErr = errors.New("dial tcp: refused")
		s := vstreamDoorStreamer(t, tgt)
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &scopeDriftApplier{}, "st", cursorless)
		if err == nil || !strings.Contains(err.Error(), "dial tcp: refused") {
			t.Fatalf("want the open failure, got %v", err)
		}
	})
	t.Run("migrate-state read error", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(failingReadStateStore{newFakeStateStore()}))
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &scopeDriftApplier{}, "st", cursorless)
		if err == nil || !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("want the read failure, got %v", err)
		}
	})
	t.Run("target keeps no migrate-state store", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(nil).Engine) // no opener at all
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &scopeDriftApplier{}, "st", cursorless)
		requireUnproven(t, err)
	})
	t.Run("applier keeps no established record, header mid-copy", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseBulkCopy)))
		_, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &resumeDispatchApplier{}, "st", cursorless)
		requireUnproven(t, err)
	})
	t.Run("applier keeps no established record, header complete", func(t *testing.T) {
		s := vstreamDoorStreamer(t, proofTargetWith(header(ir.MigrationPhaseComplete)))
		if _, err := s.phaseRefuseUnprovenVStreamCopy(ctx, &resumeDispatchApplier{}, "st", cursorless); err != nil {
			t.Fatalf("a complete header is proof on its own: %v", err)
		}
	})
}

// TestVStreamCopyDoor_AssertCopyFinished: the flag passes the door, the
// record is written from the current scope, and the next resume passes
// without the flag; a record that cannot be written refuses.
func TestVStreamCopyDoor_AssertCopyFinished(t *testing.T) {
	ctx := context.Background()
	app := &scopeDriftApplier{}
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

	t.Run("record write fails", func(t *testing.T) {
		failing := &scopeDriftApplier{recordErr: errors.New("permission denied for table sluice_cdc_state")}
		s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore()))
		s.AssertCopyFinished = true
		if _, err := s.phaseRefuseUnprovenVStreamCopy(ctx, failing, "st", cursorless); err != nil {
			t.Fatalf("door: %v", err)
		}
		err := s.recordAssertedCopy(ctx, failing, "st")
		if err == nil || !strings.Contains(err.Error(), "permission denied") || !strings.Contains(err.Error(), "--assert-copy-finished") {
			t.Fatalf("want a refusal naming the failed record write, got %v", err)
		}
	})
	t.Run("the scope a live-added table widens is recorded", func(t *testing.T) {
		src := &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "planetscale", caps: ir.Capabilities{CDC: ir.CDCVStream}},
			schemas:              map[string]*ir.Schema{"src-dsn": driftSchema("t1_small", "t2_big", "added")},
		}
		s := driftStreamer(t, src, proofTargetWith(newFakeStateStore()), "t1_small", "t2_big")
		s.AssertCopyFinished = true
		app := &scopeDriftApplier{liveAdded: []string{"added"}}
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

// proofApplier is resumeDispatchApplier with an established-table record
// and every record write logged — the Run-level stand-in for a v0.163+
// target control row.
type proofApplier struct {
	*resumeDispatchApplier
	rec    ir.EstablishedRecord
	writes []string
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

	t.Run("v0.163.1 mid-copy: refused, nothing opened or written", func(t *testing.T) {
		app := &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{stored: stored, found: true}}
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
		app := &proofApplier{
			resumeDispatchApplier: &resumeDispatchApplier{stored: stored, found: true},
			rec:                   ir.EstablishedRecord{Recorded: true},
		}
		opened, _, err := runVStreamResume(t, false, app, newTarget(newFakeStateStore()), false)
		if !opened {
			t.Fatalf("a recorded stream did not warm-resume: %v", err)
		}
	})
	t.Run("v0.150-era finished (complete header): resumes", func(t *testing.T) {
		app := &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{stored: stored, found: true}}
		opened, _, err := runVStreamResume(t, false, app, newTarget(header(ir.MigrationPhaseComplete)), false)
		if !opened {
			t.Fatalf("a stream with a complete header did not warm-resume: %v", err)
		}
	})
	t.Run("cursor-bearing: VSTREAM-COPY-RESUME-WITHDRAWN keeps precedence", func(t *testing.T) {
		app := &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{stored: stored, found: true}}
		opened, resumes, err := runVStreamResume(t, true, app, newTarget(newFakeStateStore()), true)
		if err == nil || !strings.Contains(err.Error(), copyResumeWithdrawnMarker) || strings.Contains(err.Error(), vstreamCopyUnprovenMarker) {
			t.Fatalf("want %s (and not %s, nor a pass on --assert-copy-finished), got %v", copyResumeWithdrawnMarker, vstreamCopyUnprovenMarker, err)
		}
		if opened || resumes != 0 || len(app.writes) != 0 {
			t.Errorf("opened=%v resumes=%d writes=%v; want nothing", opened, resumes, app.writes)
		}
	})
	t.Run("--assert-copy-finished: resumes and records; the next run needs no flag", func(t *testing.T) {
		app := &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{stored: stored, found: true}}
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
			Applier:       &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{stored: stored, found: true}},
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
