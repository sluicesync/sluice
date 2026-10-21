// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pins for the --position-from-manifest handoff's record
// (recordManifestHandoff): a handoff that starts records the chain's
// position and the stream's scope as established, so the next plain restart
// of a PlanetScale/Vitess stream passes VSTREAM-COPY-COMPLETION-UNPROVEN on
// the record; a record write that fails refuses MANIFEST-HANDOFF-UNRECORDED.
// The fixture target is the shape a handoff meets: no control row for the
// stream (found=false), no record.

import (
	"context"
	"errors"
	"strings"
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

// vstreamChainStore is a local backup chain whose terminal full ends at a
// cursor-less VStream position.
func vstreamChainStore(t *testing.T) irbackup.Store {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "planetscale",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "planetscale", Token: cursorlessVStreamToken},
		PartialState:  irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return store
}

// runHandoff runs a VStream-source Streamer (one table, widgets) until its
// plain CDC reader opens, or until it returns.
func runHandoff(t *testing.T, applier ir.ChangeApplier, chain irbackup.Store, waitExit bool) (opened bool, err error) {
	t.Helper()
	cdc := &capturingCDCReader{captured: make(chan struct{})}
	source := &copyResumeEngine{name: "planetscale", caps: ir.Capabilities{CDC: ir.CDCVStream}, cdcReader: cdc, schemaOneTable: true}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := &Streamer{
		Source:        source,
		Target:        &proofTarget{Engine: &copyResumeEngine{name: "postgres"}, store: newFakeStateStore()},
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
	case <-cdc.captured:
		cancel()
		<-runErr
		return true, nil
	case err = <-runErr:
		return false, err
	}
}

// TestStreamer_ManifestHandoff_RecordsTheRestoredCopy: the handoff records,
// and the next plain restart passes the VStream copy door on that record.
func TestStreamer_ManifestHandoff_RecordsTheRestoredCopy(t *testing.T) {
	app := &handoffApplier{proofApplier: &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{}}}
	opened, err := runHandoff(t, app, vstreamChainStore(t), false)
	if !opened {
		t.Fatalf("the handoff did not start: %v", err)
	}
	if len(app.anchors) != 1 || !strings.Contains(app.anchors[0], cursorlessVStreamToken) || !strings.HasSuffix(app.anchors[0], "copied=widgets") {
		t.Fatalf("anchors = %v; want one anchor at the chain's position recording widgets", app.anchors)
	}
	if !app.rec.Recorded {
		t.Fatal("the handoff left no established-table record")
	}

	app.anchors = nil
	opened, err = runHandoff(t, app, nil, false) // the next plain restart
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
	app := &handoffApplier{
		proofApplier: &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{}},
		anchorErr:    errors.New("permission denied for table sluice_cdc_state"),
	}
	_, err := runHandoff(t, app, vstreamChainStore(t), true)
	if err == nil || !strings.Contains(err.Error(), manifestHandoffUnrecordedMarker) || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("want the %s refusal naming the write failure, got %v", manifestHandoffUnrecordedMarker, err)
	}
	if app.found || app.rec.Recorded {
		t.Errorf("a refused handoff left a row (found=%v) or a record (%+v)", app.found, app.rec)
	}
}

// TestRecordManifestHandoff_Arms: the helper alone — no-op off the manifest
// path, a record already present is left alone, a record read error refuses,
// and an applier without the anchor surface WARNs and passes.
func TestRecordManifestHandoff_Arms(t *testing.T) {
	ctx := context.Background()
	pos := ir.Position{Engine: "planetscale", Token: cursorlessVStreamToken}
	newApp := func() *handoffApplier {
		return &handoffApplier{proofApplier: &proofApplier{resumeDispatchApplier: &resumeDispatchApplier{}}}
	}
	s := vstreamDoorStreamer(t, proofTargetWith(newFakeStateStore()))

	app := newApp()
	if err := s.recordManifestHandoff(ctx, app, "st", pos); err != nil || len(app.anchors) != 0 {
		t.Fatalf("off the manifest path: err=%v anchors=%v; want a no-op", err, app.anchors)
	}

	s.PositionFromManifestStore = struct{ irbackup.Store }{}
	app = newApp()
	app.rec = ir.EstablishedRecord{Recorded: true, Established: []string{"t1_small"}}
	if err := s.recordManifestHandoff(ctx, app, "st", pos); err != nil || len(app.anchors) != 0 {
		t.Fatalf("recorded stream: err=%v anchors=%v; want it left alone", err, app.anchors)
	}

	app = newApp()
	if err := s.recordManifestHandoff(ctx, app, "st", pos); err != nil {
		t.Fatal(err)
	}
	if len(app.anchors) != 1 || !strings.HasSuffix(app.anchors[0], "copied=t1_small,t2_big") {
		t.Fatalf("anchors = %v; want the whole scope recorded", app.anchors)
	}

	reading := &scopeDriftApplier{recErr: errors.New("connection reset")}
	if err := s.recordManifestHandoff(ctx, reading, "st", pos); err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("record read error: got %v; want a refusal", err)
	}

	if err := s.recordManifestHandoff(ctx, &scopeDriftApplier{}, "st", pos); err != nil {
		t.Fatalf("an applier without the anchor surface must WARN, not refuse: %v", err)
	}
}
