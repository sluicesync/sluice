// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pins for the --position-from-manifest handoff's record
// (recordManifestHandoff): a PlanetScale/Vitess handoff that starts records
// the chain's position and the restored tables — the stream's scope
// intersected with the chain's table set, never the scope alone — so the
// next plain restart passes VSTREAM-COPY-COMPLETION-UNPROVEN on the record;
// a table in scope the chain never held stays not established; a chain whose
// table set cannot be read, or a record write that fails, refuses
// MANIFEST-HANDOFF-UNRECORDED; every other source records nothing, as
// before. The fixture target is the shape a handoff meets: no control row
// for the stream (found=false), no record.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// handoffApplier adds the cold-start anchor surface to proofApplier: the
// write creates the row (position) and records copied on a row with none.
type handoffApplier struct {
	*proofApplier
	anchorErr error
	anchors   []string

	// applying is closed when the apply loop starts: the stream's open
	// (and the handoff record that runs inside it) has returned.
	applying     chan struct{}
	applyingOnce sync.Once
}

func (a *handoffApplier) Apply(ctx context.Context, streamID string, changes <-chan ir.Change) error {
	a.applyingOnce.Do(func() { close(a.applying) })
	return a.resumeDispatchApplier.Apply(ctx, streamID, changes)
}

func (a *handoffApplier) WriteColdStartAnchor(_ context.Context, _ string, pos ir.Position, copied []string, _ map[string]string) error {
	if a.anchorErr != nil {
		return a.anchorErr
	}
	a.anchors = append(a.anchors, pos.Token+" copied="+strings.Join(copied, ","))
	a.stored, a.found = pos, true
	if !a.rec.Recorded {
		a.rec = ir.EstablishedRecord{Recorded: true, Established: copied}
	}
	return nil
}

// anchorDriftApplier adds the same surface to scopeDriftApplier, so the
// record a handoff writes can then be judged by the scope door.
type anchorDriftApplier struct {
	*scopeDriftApplier
	anchors []string
}

func (a *anchorDriftApplier) WriteColdStartAnchor(_ context.Context, _ string, _ ir.Position, copied []string, _ map[string]string) error {
	a.anchors = append(a.anchors, "copied="+strings.Join(copied, ","))
	if !a.rec.Recorded {
		a.rec = ir.EstablishedRecord{Recorded: true, Established: copied}
	}
	return nil
}

// vstreamChainStore is a local backup chain whose terminal full ends at a
// cursor-less VStream position and holds tables (schema nil when noSchema).
func vstreamChainStore(t *testing.T, noSchema bool, tables ...string) irbackup.Store {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "planetscale",
		Schema:        driftSchema(tables...),
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "planetscale", Token: cursorlessVStreamToken},
		PartialState:  irbackup.BackupStateComplete,
	}
	if noSchema {
		full.Schema = nil
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return store
}

// runHandoff runs a Streamer whose source (one table, widgets) streams by
// cdc, until its apply loop starts — after the stream's open, handoff
// record included, has returned — or, with waitExit, until it returns.
func runHandoff(t *testing.T, cdcMethod ir.CDCMethod, applier *handoffApplier, chain irbackup.Store, waitExit bool) (opened bool, err error) {
	t.Helper()
	cdc := &capturingCDCReader{captured: make(chan struct{})}
	source := &copyResumeEngine{name: "planetscale", caps: ir.Capabilities{CDC: cdcMethod}, cdcReader: cdc, schemaOneTable: true}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := &Streamer{
		Source:        source,
		Target:        &proofTarget{Engine: &copyResumeEngine{name: "postgres", schemaOneTable: true}, store: newFakeStateStore()},
		SourceDSN:     "src",
		TargetDSN:     "tgt",
		StreamID:      "st",
		SchemaChanges: "refuse",
		Applier:       applier,
	}
	if chain != nil {
		s.PositionFromManifestStore = chain
	}
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	if waitExit {
		// The reader opens before the record is written, so a refusal
		// arrives after it: wait for the run's own exit.
		err = <-runErr
		return cdc.once, err
	}
	select {
	case <-applier.applying:
		cancel()
		<-runErr
		return true, nil
	case err = <-runErr:
		return false, err
	}
}

func newHandoffApplier() *handoffApplier {
	return &handoffApplier{
		proofApplier: &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{}},
		applying:     make(chan struct{}),
	}
}

// TestStreamer_ManifestHandoff_RecordsTheRestoredCopy: the handoff records,
// and the next plain restart passes the VStream copy door on that record.
func TestStreamer_ManifestHandoff_RecordsTheRestoredCopy(t *testing.T) {
	app := newHandoffApplier()
	opened, err := runHandoff(t, ir.CDCVStream, app, vstreamChainStore(t, false, "widgets"), false)
	if !opened {
		t.Fatalf("the handoff did not start: %v", err)
	}
	if len(app.anchors) != 1 || !strings.Contains(app.anchors[0], cursorlessVStreamToken) || !strings.HasSuffix(app.anchors[0], "copied=widgets") {
		t.Fatalf("anchors = %v; want one anchor at the chain's position recording widgets", app.anchors)
	}

	app.anchors = nil
	opened, err = runHandoff(t, ir.CDCVStream, app, nil, false) // the next plain restart
	if !opened {
		t.Fatalf("the plain restart after a recorded handoff did not resume: %v", err)
	}
	if len(app.anchors) != 0 {
		t.Errorf("a plain restart wrote anchors %v", app.anchors)
	}
}

// TestStreamer_ManifestHandoff_RecordFailureRefuses: a record write that
// fails refuses the handoff rather than leave a stream that refuses later.
func TestStreamer_ManifestHandoff_RecordFailureRefuses(t *testing.T) {
	app := newHandoffApplier()
	app.anchorErr = errors.New("permission denied for table sluice_cdc_state")
	_, err := runHandoff(t, ir.CDCVStream, app, vstreamChainStore(t, false, "widgets"), true)
	if err == nil || !strings.Contains(err.Error(), manifestHandoffUnrecordedMarker) || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("want the %s refusal naming the write failure, got %v", manifestHandoffUnrecordedMarker, err)
	}
	if app.found || app.rec.Recorded {
		t.Errorf("a refused handoff left a row (found=%v) or a record (%+v)", app.found, app.rec)
	}
}

// TestStreamer_ManifestHandoff_NonVStreamRecordsNothing: on any other source
// the handoff behaves as before — it starts and writes no anchor or record.
func TestStreamer_ManifestHandoff_NonVStreamRecordsNothing(t *testing.T) {
	for _, cdc := range []ir.CDCMethod{ir.CDCBinlog, ir.CDCLogicalReplication} {
		app := newHandoffApplier()
		opened, err := runHandoff(t, cdc, app, vstreamChainStore(t, false, "widgets"), false)
		if !opened {
			t.Fatalf("CDC %v: the handoff did not start: %v", cdc, err)
		}
		if len(app.anchors) != 0 || app.rec.Recorded {
			t.Errorf("CDC %v: a non-VStream handoff wrote anchors %v / record %+v", cdc, app.anchors, app.rec)
		}
	}
}

// TestRecordManifestHandoff_ScopeIntersectsTheChain: a table in scope that
// the chain never held is not recorded, and the scope door then refuses it
// as never copied (its target table exists, so its changes would apply).
func TestRecordManifestHandoff_ScopeIntersectsTheChain(t *testing.T) {
	ctx := context.Background()
	s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore())) // scope: t1_small, t2_big; both on the target
	s.PositionFromManifestStore = vstreamChainStore(t, false, "t1_small")
	app := &anchorDriftApplier{scopeDriftApplier: &scopeDriftApplier{}}
	if err := s.recordManifestHandoff(ctx, app, "st", cursorless); err != nil {
		t.Fatal(err)
	}
	if len(app.anchors) != 1 || app.anchors[0] != "copied=t1_small" {
		t.Fatalf("anchors = %v; want only the table the chain holds (t1_small) recorded", app.anchors)
	}
	next := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore())) // the next plain start
	requireDrift(t, next.phaseRefuseIncludeTableScopeDrift(ctx, app, "st"), "t2_big", reasonTargetPresent)
}

// TestRecordManifestHandoff_TargetIntersection (F5): a chain table the target
// does not hold (a restore filtered by `restore --include-table`) is not
// recorded either.
func TestRecordManifestHandoff_TargetIntersection(t *testing.T) {
	s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore(), "t1_small"))
	s.PositionFromManifestStore = vstreamChainStore(t, false, "t1_small", "t2_big")
	app := &anchorDriftApplier{scopeDriftApplier: &scopeDriftApplier{}}
	if err := s.recordManifestHandoff(context.Background(), app, "st", cursorless); err != nil {
		t.Fatal(err)
	}
	if len(app.anchors) != 1 || app.anchors[0] != "copied=t1_small" {
		t.Fatalf("anchors = %v; want only the table the target holds (t1_small) recorded", app.anchors)
	}
}

// silentAnchorApplier writes the anchor and records nothing, returning nil:
// what a control table with no record columns this role cannot ALTER does
// (COLD-START-ANCHOR-UNRECORDED).
type silentAnchorApplier struct {
	*scopeDriftApplier
	anchors int
}

func (a *silentAnchorApplier) WriteColdStartAnchor(context.Context, string, ir.Position, []string, map[string]string) error {
	a.anchors++
	return nil
}

// TestRecordManifestHandoff_AnchorUnrecordedRefuses (F6): an anchor write
// that silently recorded nothing refuses, under the no-restart sentinel,
// naming the control-tables remedy — never a false "recorded" INFO.
func TestRecordManifestHandoff_AnchorUnrecordedRefuses(t *testing.T) {
	s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore()))
	s.PositionFromManifestStore = vstreamChainStore(t, false, "t1_small", "t2_big")
	app := &silentAnchorApplier{scopeDriftApplier: &scopeDriftApplier{}}
	err := s.recordManifestHandoff(context.Background(), app, "st", cursorless)
	if app.anchors != 1 {
		t.Fatalf("RIG: anchor writes = %d", app.anchors)
	}
	if err == nil || !strings.Contains(err.Error(), manifestHandoffUnrecordedMarker) || refusalARestartRepeats(err) == nil ||
		!strings.Contains(err.Error(), "sluice control-tables ddl") {
		t.Fatalf("want the terminal %s refusal naming control-tables ddl, got %v", manifestHandoffUnrecordedMarker, err)
	}
}

// TestRecordManifestHandoff_Arms: the helper's other arms.
func TestRecordManifestHandoff_Arms(t *testing.T) {
	ctx := context.Background()
	chain := func() irbackup.Store { return vstreamChainStore(t, false, "t1_small", "t2_big") }
	streamer := func(store irbackup.Store) *Streamer {
		s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore()))
		s.PositionFromManifestStore = store
		return s
	}

	app := newHandoffApplier()
	if err := streamer(nil).recordManifestHandoff(ctx, app, "st", cursorless); err != nil || len(app.anchors) != 0 {
		t.Fatalf("off the manifest path: err=%v anchors=%v; want a no-op", err, app.anchors)
	}

	app = newHandoffApplier()
	app.rec = ir.EstablishedRecord{Recorded: true, Established: []string{"t1_small"}}
	if err := streamer(chain()).recordManifestHandoff(ctx, app, "st", cursorless); err != nil || len(app.anchors) != 0 {
		t.Fatalf("recorded stream: err=%v anchors=%v; want it left alone", err, app.anchors)
	}

	app = newHandoffApplier()
	if err := streamer(chain()).recordManifestHandoff(ctx, app, "st", cursorless); err != nil {
		t.Fatal(err)
	}
	if len(app.anchors) != 1 || !strings.HasSuffix(app.anchors[0], "copied=t1_small,t2_big") {
		t.Fatalf("anchors = %v; want both restored tables recorded", app.anchors)
	}

	for name, store := range map[string]irbackup.Store{
		"chain with no manifests":          mustLocalStore(t),
		"terminal manifest with no schema": vstreamChainStore(t, true),
	} {
		app = newHandoffApplier()
		err := streamer(store).recordManifestHandoff(ctx, app, "st", cursorless)
		if err == nil || !strings.Contains(err.Error(), manifestHandoffUnrecordedMarker) {
			t.Errorf("%s: got %v; want the %s refusal", name, err, manifestHandoffUnrecordedMarker)
		}
		if len(app.anchors) != 0 {
			t.Errorf("%s: anchors %v written; an unreadable table set must never fall back to the scope", name, app.anchors)
		}
	}

	reading := &scopeDriftApplier{recErr: errors.New("connection reset")}
	if err := streamer(chain()).recordManifestHandoff(ctx, reading, "st", cursorless); err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("record read error: got %v; want a refusal", err)
	}

	if err := streamer(chain()).recordManifestHandoff(ctx, &scopeDriftApplier{}, "st", cursorless); err != nil {
		t.Fatalf("an applier without the anchor surface must WARN, not refuse: %v", err)
	}
}

func mustLocalStore(t *testing.T) irbackup.Store {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}
