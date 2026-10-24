// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestAddTableValidate enforces the required-fields contract before
// any I/O happens. Caller bugs surface deterministically.
func TestAddTableValidate(t *testing.T) {
	cases := []struct {
		name string
		a    *AddTable
		want string
	}{
		{
			"nil source",
			&AddTable{Target: stubEngine{}, SourceDSN: "x", TargetDSN: "y", StreamID: "s", TableName: "t"},
			"Source engine is nil",
		},
		{
			"nil target",
			&AddTable{Source: stubEngine{}, SourceDSN: "x", TargetDSN: "y", StreamID: "s", TableName: "t"},
			"Target engine is nil",
		},
		{
			"empty source DSN",
			&AddTable{Source: stubEngine{}, Target: stubEngine{}, TargetDSN: "y", StreamID: "s", TableName: "t"},
			"SourceDSN is empty",
		},
		{
			"empty target DSN",
			&AddTable{Source: stubEngine{}, Target: stubEngine{}, SourceDSN: "x", StreamID: "s", TableName: "t"},
			"TargetDSN is empty",
		},
		{
			"empty stream id",
			&AddTable{Source: stubEngine{}, Target: stubEngine{}, SourceDSN: "x", TargetDSN: "y", TableName: "t"},
			"StreamID is empty",
		},
		{
			"empty table name",
			&AddTable{Source: stubEngine{}, Target: stubEngine{}, SourceDSN: "x", TargetDSN: "y", StreamID: "s"},
			"TableName is empty",
		},
		{
			"whitespace-only table name",
			&AddTable{Source: stubEngine{}, Target: stubEngine{}, SourceDSN: "x", TargetDSN: "y", StreamID: "s", TableName: "   "},
			"TableName is empty",
		},
		{
			"source with CDCNone",
			&AddTable{Source: cdcNoneEngine{}, Target: stubEngine{}, SourceDSN: "x", TargetDSN: "y", StreamID: "s", TableName: "t"},
			"declares CDC=None",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.a.Run(context.Background())
			if err == nil {
				t.Fatalf("expected error containing %q; got nil", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want contains %q", err, c.want)
			}
		})
	}
}

// TestAddTableRefusesUnknownStream confirms add-table refuses cleanly
// when the supplied stream-id has no row in the target's cdc-state.
// The most common operator failure mode is a typo or pointing at the
// wrong target.
func TestAddTableRefusesUnknownStream(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("target")
	// No streams configured on the applier.

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "missing-stream", TableName: "new_table",
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("expected an error; got nil")
	}
	if !strings.Contains(err.Error(), `no stream "missing-stream"`) {
		t.Errorf("err = %v; want a no-such-stream message", err)
	}
}

// TestAddTableRefusesRunningStream is DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED:
// a drained add-table refuses a stream whose liveness stamp is fresh (and
// one whose liveness cannot be read or was never recorded) before it
// changes anything — on --dry-run too, which reports what a real run would
// do — and passes only a recorded stop or no row: a stamp of any age refuses.
func TestAddTableRefusesRunningStream(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name    string
		l       ir.StreamLiveness
		err     error
		rowGone bool
		refuse  bool
	}{
		{name: "fresh stamp", l: ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now.Add(-2 * time.Second), Now: now}, refuse: true},
		{name: "unreadable", err: errors.New("connection reset by peer"), refuse: true},
		{name: "never recorded", l: ir.StreamLiveness{Found: true, Now: now}, refuse: true},
		{name: "recorded stop", l: ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now}},
		{name: "hour-old stamp (a crash, or an idle stream)", l: ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now.Add(-time.Hour), Now: now}, refuse: true},
		{name: "no row (it vanished after the stream was listed)", rowGone: true, refuse: true},
	}
	for _, tc := range cases {
		for _, dry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/dry-run=%v", tc.name, dry), func(t *testing.T) {
				src := newAddTableSourceEngine("source")
				src.schema = &ir.Schema{Tables: []*ir.Table{
					{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
				}}
				tgt := newAddTableTargetEngine("target")
				tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}
				tgt.applier.liveness, tgt.applier.livenessErr, tgt.applier.livenessRowGone = tc.l, tc.err, tc.rowGone
				err := (&AddTable{
					Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt",
					StreamID: "live", TableName: "new_table", DryRun: dry,
				}).Run(context.Background())
				if !tc.refuse {
					if err != nil {
						t.Fatalf("want the add to run, got %v", err)
					}
					return
				}
				if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "sluice sync stop --wait --stream-id live") {
					t.Fatalf("want the %s refusal naming the stop, got %v", DrainedAddTableStreamNotStoppedMarker, err)
				}
				if len(tgt.phaseLog) != 0 || src.snapshotCalls != 0 || tgt.openRowWriterCalls != 0 {
					t.Fatalf("the refused add changed something: phases %v, snapshots %d, row writers %d",
						tgt.phaseLog, src.snapshotCalls, tgt.openRowWriterCalls)
				}
			})
		}
	}
	t.Run("a stream that starts after the first read is caught by the recheck", func(t *testing.T) {
		src := newAddTableSourceEngine("source")
		src.schema = &ir.Schema{Tables: []*ir.Table{
			{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		}}
		tgt := newAddTableTargetEngine("target")
		tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}
		tgt.applier.liveness = ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now}
		tgt.applier.livenessAfter = &ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now, Now: now}
		err := (&AddTable{
			Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt",
			StreamID: "live", TableName: "new_table",
		}).Run(context.Background())
		if !errors.Is(err, errStreamNotStopped) {
			t.Fatalf("want the %s refusal from the recheck, got %v", DrainedAddTableStreamNotStoppedMarker, err)
		}
		if tgt.applier.livenessReads != 2 || len(tgt.phaseLog) != 0 || src.snapshotCalls != 0 {
			t.Fatalf("reads %d (want 2: the preflight and the recheck), phases %v, snapshots %d",
				tgt.applier.livenessReads, tgt.phaseLog, src.snapshotCalls)
		}
	})
	// --no-drain refuses on every source (Bug 332) before the door is
	// reached, so it never reads the record; a recorded stop does not let
	// it through.
	t.Run("--no-drain refuses before it is consulted", func(t *testing.T) {
		src := newAddTableSourceEngine("source")
		src.schema = &ir.Schema{Tables: []*ir.Table{
			{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		}}
		tgt := newAddTableTargetEngine("target")
		tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}
		tgt.applier.liveness = ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now}
		err := (&AddTable{
			Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt",
			StreamID: "live", TableName: "new_table", LiveMode: true,
		}).Run(context.Background())
		if err == nil || errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "--no-drain") {
			t.Fatalf("want the --no-drain refusal, got %v", err)
		}
		if tgt.applier.livenessReads != 0 || len(tgt.phaseLog) != 0 || src.snapshotCalls != 0 {
			t.Fatalf("reads %d, phases %v, snapshots %d; want none", tgt.applier.livenessReads, tgt.phaseLog, src.snapshotCalls)
		}
	})
}

// TestAddTableRefusesActiveStream confirms add-table refuses when
// the stream's stop_requested_at is set — operator should run `sync
// stop --wait` and let it complete (which clears the flag) before
// invoking add-table. (Despite its name, a stream that is merely
// running is TestAddTableRefusesRunningStream's case.)
func TestAddTableRefusesActiveStream(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}
	tgt.applier.stopRequested = true

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("expected an error; got nil")
	}
	if !strings.Contains(err.Error(), "in-flight stop request") {
		t.Errorf("err = %v; want an in-flight-stop message", err)
	}
}

// TestAddTableRefusesMissingTableOnSource confirms add-table surfaces
// a clear message when the operator runs `add-table` before the
// CREATE TABLE has actually landed on the source.
func TestAddTableRefusesMissingTableOnSource(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "existing", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "not_yet_created",
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("expected an error; got nil")
	}
	if !strings.Contains(err.Error(), `not_yet_created`) || !strings.Contains(err.Error(), "not found on source") {
		t.Errorf("err = %v; want a not-found-on-source message", err)
	}
}

// TestAddTableRefusesPopulatedTarget confirms the per-table preflight
// fires when the target table already has rows. Same shape as the
// cold-start preflight.
func TestAddTableRefusesPopulatedTarget(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}
	tgt.rowWriter.empty = false

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("expected an error; got nil")
	}
	if !strings.Contains(err.Error(), "already exists with rows") {
		t.Errorf("err = %v; want an already-exists-with-rows message", err)
	}
}

// TestAddTableHappyPath wires the orchestrator end-to-end against
// recording engines and asserts the full phase sequence: preflight
// → publication-add (no-op for the recording engine, which does not
// implement publicationAdder) → snapshot open → bulk-copy → close.
func TestAddTableHappyPath(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	logs := captureSlog(t)
	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Snapshot stream was opened.
	if src.snapshotCalls != 1 {
		t.Errorf("snapshot opens = %d; want 1", src.snapshotCalls)
	}
	// Bulk-copy phases ran on the target. ADR-0036 Phase B: the
	// orchestrator pre-creates the target table BEFORE publication-add
	// (step 3a in AddTable.Run) so events on the new table delivered
	// to the active stream's applier between publication-add and
	// bulk-copy don't hit the applier's errUnknownTable silent-drop
	// branch. The CreateTablesWithoutConstraints inside
	// runBulkCopyForAddTable is therefore idempotent (CREATE TABLE IF
	// NOT EXISTS on both engines); it appears in the phase log twice
	// because the recording stub doesn't distinguish second-call
	// no-op from a first-time create. The recordingRowWriterEmpty
	// stub doesn't implement ir.IdempotentRowWriter, so the bulk-copy
	// falls back to plain WriteRows — exactly what the phase log
	// records.
	wantPhases := []string{
		"CreateTablesWithoutConstraints",
		"CreateTablesWithoutConstraints",
		"WriteRows:new_table",
		"SyncIdentitySequences",
		"CreateIndexes",
		"CreateConstraints",
	}
	if len(tgt.phaseLog) != len(wantPhases) {
		t.Fatalf("got %d phases (%v); want %d", len(tgt.phaseLog), tgt.phaseLog, len(wantPhases))
	}
	for i, want := range wantPhases {
		if tgt.phaseLog[i] != want {
			t.Errorf("phase[%d] = %q; want %q", i, tgt.phaseLog[i], want)
		}
	}
	if !strings.Contains(logs.String(), "add-table: complete") {
		t.Errorf("expected completion log; got %q", logs.String())
	}
}

// TestAddTableDryRun confirms the dry-run path does not open writers
// or touch the snapshot stream.
func TestAddTableDryRun(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	logs := captureSlog(t)
	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		DryRun: true,
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if src.snapshotCalls != 0 {
		t.Errorf("snapshot opens during dry-run = %d; want 0", src.snapshotCalls)
	}
	if tgt.openSchemaWriterCalls != 0 || tgt.openRowWriterCalls != 0 {
		t.Errorf("dry-run opened writers (sw=%d, rw=%d); want 0/0",
			tgt.openSchemaWriterCalls, tgt.openRowWriterCalls)
	}
	if !strings.Contains(logs.String(), "dry run: add-table") {
		t.Errorf("expected dry-run log; got %q", logs.String())
	}
}

// TestAddTablePublicationAddCalled confirms an engine that implements
// publicationAdder receives the new table's name on the add path.
func TestAddTablePublicationAddCalled(t *testing.T) {
	src := newAddTableSourceEngineWithPub("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := src.addedTables; len(got) != 1 || got[0] != "new_table" {
		t.Errorf("publication-add tables = %v; want [new_table]", got)
	}
}

// TestAddTable_LiveMode_FieldRoundTrip pins the LiveMode field's
// presence on the orchestrator struct. A typo or rename here would
// silently disable Phase 2 because the CLI passes the bool through.
func TestAddTable_LiveMode_FieldRoundTrip(t *testing.T) {
	a := &AddTable{LiveMode: true}
	if !a.LiveMode {
		t.Errorf("LiveMode round-trip = %v; want true", a.LiveMode)
	}
}

// TestAddTable_UsesRecordedPublication pins the ADR-0176-prerequisite
// inheritance: when the active stream's StreamStatus carries a
// recorded publication_name, preflightStream scopes the source engine
// to it (with the recorded slot as the guard's own-slot exclusion) so
// the publication-extend lands on the publication the stream actually
// reads through — not the shared default, which would leave the new
// table silently outside the stream's scope after bulk-copy.
func TestAddTable_UsesRecordedPublication(t *testing.T) {
	src := &addTableSourceEnginePubScoped{addTableSourceEngineLive: newAddTableSourceEngineLive("source")}
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}

	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{
		{StreamID: "live", SlotName: "sluice_shard_a", PublicationName: "sluice_wave_x"},
	}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run errored: %v", err)
	}
	if src.scopeCalls == 0 {
		t.Fatal("WithPublicationScope was never called despite a recorded publication_name")
	}
	if src.scopedPublication != "sluice_wave_x" {
		t.Errorf("scoped publication = %q; want the recorded %q", src.scopedPublication, "sluice_wave_x")
	}
	if src.scopedOwnSlot != "sluice_shard_a" {
		t.Errorf("scoped own-slot = %q; want the recorded %q", src.scopedOwnSlot, "sluice_shard_a")
	}
}

// TestAddTable_LegacyEmptyPublicationDoesNotScope pins the zero-value
// floor: a legacy row (no recorded publication) must leave the source
// engine untouched — the engine default, byte-identical to before the
// ADR-0176 prerequisite chunk.
func TestAddTable_LegacyEmptyPublicationDoesNotScope(t *testing.T) {
	src := &addTableSourceEnginePubScoped{addTableSourceEngineLive: newAddTableSourceEngineLive("source")}
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}

	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run errored: %v", err)
	}
	if src.scopeCalls != 0 {
		t.Errorf("WithPublicationScope called %d times for a legacy (empty) record; want 0", src.scopeCalls)
	}
}

// TestAddTable_LiveMode_RefusesSourceWithoutPublication confirms --no-drain
// refuses, with the generic message and the drained flow, on a source that
// neither extends a publication nor is a binlog or VStream source (the stub
// declares logical replication without the publication surface).
func TestAddTable_LiveMode_RefusesSourceWithoutPublication(t *testing.T) {
	src := newAddTableSourceEngine("unsupported-source") // no publicationAdder
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("unsupported-target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("expected error on live mode with a source without a publication; got nil")
	}
	for _, want := range []string{"withdrawn on every source", "drained add-table flow", "sluice sync stop --wait"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v; want it to contain %q", err, want)
		}
	}
	if src.snapshotCalls != 0 {
		t.Errorf("snapshot opens = %d before refusing; want 0", src.snapshotCalls)
	}
}

// TestAddTable_LiveMode_BinlogSourceRefusesBeforeAnyWrite pins
// NO-DRAIN-BINLOG-SOURCE at the stub level: a binlog source writing to a
// MySQL-family target whose applier records live adds — the pair that took
// ADR-0034's filter-flip — refuses before any snapshot, writer or
// live-added record.
func TestAddTable_LiveMode_BinlogSourceRefusesBeforeAnyWrite(t *testing.T) {
	src := newAddTableSourceEngine("binlog-source") // no publicationAdder
	src.cdc = ir.CDCBinlog
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngineWithLiveAdd("mysql")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	err := (&AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), noDrainBinlogSourceMarker) {
		t.Fatalf("Run = %v; want the %s refusal", err, noDrainBinlogSourceMarker)
	}
	if src.snapshotCalls != 0 || len(tgt.applier.recordedLiveAdds) != 0 || tgt.openSchemaWriterCalls != 0 || tgt.openRowWriterCalls != 0 {
		t.Errorf("wrote before refusing: snapshots %d, live adds %v, schema writers %d, row writers %d",
			src.snapshotCalls, tgt.applier.recordedLiveAdds, tgt.openSchemaWriterCalls, tgt.openRowWriterCalls)
	}
}

// TestAddTable_LiveMode_PublicationSourceRefusesBeforeAnyWrite pins
// NO-DRAIN-POSTGRES-SOURCE (Bug 332) at the stub level: a publication
// source — the one source that still had a live path — refuses --no-drain
// with the marker and the drained flow, whether or not a stop is in flight
// and on a dry run too, before any publication add, snapshot, live-added
// record or target writer.
func TestAddTable_LiveMode_PublicationSourceRefusesBeforeAnyWrite(t *testing.T) {
	for _, stopRequested := range []bool{false, true} {
		for _, dryRun := range []bool{false, true} {
			src := newAddTableSourceEngineLive("pg-source")
			src.schema = &ir.Schema{Tables: []*ir.Table{
				{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			}}
			tgt := newAddTableTargetEngineWithLiveAdd("postgres")
			tgt.applier.streams = []ir.StreamStatus{{StreamID: "live", SlotName: "sluice_slot", PublicationName: "sluice_pub"}}
			tgt.applier.stopRequested = stopRequested
			err := (&AddTable{
				Source: src, Target: tgt,
				SourceDSN: "src", TargetDSN: "tgt",
				StreamID: "live", TableName: "new_table",
				LiveMode: true, DryRun: dryRun,
			}).Run(context.Background())
			if err == nil {
				t.Fatalf("stop=%v dry-run=%v: --no-drain on a publication source succeeded; want the %s refusal", stopRequested, dryRun, noDrainPostgresSourceMarker)
			}
			for _, want := range []string{noDrainPostgresSourceMarker, "sluice sync stop --wait", "without --no-drain", "Keep the stream stopped"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("stop=%v dry-run=%v: err = %v; want it to contain %q", stopRequested, dryRun, err, want)
				}
			}
			if len(src.addedTables) != 0 || src.snapshotCalls != 0 || len(tgt.applier.recordedLiveAdds) != 0 ||
				tgt.openSchemaWriterCalls != 0 || tgt.openRowWriterCalls != 0 {
				t.Errorf("stop=%v dry-run=%v: wrote before refusing: publication adds %v, snapshots %d, live adds %v, schema writers %d, row writers %d",
					stopRequested, dryRun, src.addedTables, src.snapshotCalls, tgt.applier.recordedLiveAdds, tgt.openSchemaWriterCalls, tgt.openRowWriterCalls)
			}
		}
	}
}

// TestActiveSlotName pins the slot-name lookup behavior (ADR-0030; now
// the recorded publication's own-slot exclusion, ADR-0176). The cdc-state row's slot_name column is the
// authoritative source; an empty value falls back to the engine
// default `sluice_slot` to cover both legacy rows that pre-date
// the column and streamers that ran with the default slot name.
// Without the fallback, every live-add against a default-named
// stream would refuse on a NULL slot_name lookup.
func TestActiveSlotName(t *testing.T) {
	cases := []struct {
		name string
		in   ir.StreamStatus
		want string
	}{
		{
			name: "recorded custom slot",
			in:   ir.StreamStatus{StreamID: "s", SlotName: "sluice_shard_a"},
			want: "sluice_shard_a",
		},
		{
			name: "empty slot falls back to default",
			in:   ir.StreamStatus{StreamID: "s", SlotName: ""},
			want: "sluice_slot",
		},
		{
			name: "legacy row (zero-value StreamStatus) falls back to default",
			in:   ir.StreamStatus{StreamID: "s"},
			want: "sluice_slot",
		},
		{
			name: "default-named stream passes through",
			in:   ir.StreamStatus{StreamID: "s", SlotName: "sluice_slot"},
			want: "sluice_slot",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := activeSlotName(c.in); got != c.want {
				t.Errorf("activeSlotName(%+v) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

// TestTempSlotName pins the temp-slot naming convention so changes
// here become visible in code review.
func TestTempSlotName(t *testing.T) {
	cases := []struct {
		table    string
		operator string
		want     string
	}{
		{"new_orders", "", "sluice_addtable_new_orders"},
		{"NEW_Orders", "", "sluice_addtable_new_orders"},
		{"weird-name!", "", "sluice_addtable_weird_name_"},
		{"x", "shard_a", "sluice_shard_a"},
		{"x", "sluice_already", "sluice_already"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.table, func(t *testing.T) {
			a := &AddTable{TableName: c.table, SlotName: c.operator}
			got := a.tempSlotName()
			if got != c.want {
				t.Errorf("tempSlotName() = %q; want %q", got, c.want)
			}
		})
	}
}

// ---- mocks ----

// addTableSourceEngine wraps recordingEngine with snapshot-stream
// support and CDC=LogicalReplication (or cdc, when set) so
// AddTable.validate accepts it.
type addTableSourceEngine struct {
	*recordingEngine
	snapshotCalls int
	cdc           ir.CDCMethod
}

func newAddTableSourceEngine(name string) *addTableSourceEngine {
	return &addTableSourceEngine{recordingEngine: newRecordingEngine(name)}
}

func (e *addTableSourceEngine) Capabilities() ir.Capabilities {
	if e.cdc != ir.CDCNone {
		return ir.Capabilities{CDC: e.cdc}
	}
	return ir.Capabilities{CDC: ir.CDCLogicalReplication}
}

func (e *addTableSourceEngine) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	e.snapshotCalls++
	return &ir.SnapshotStream{
		Position: ir.Position{Engine: e.name, Token: "snapshot-token"},
		Rows:     &recordingRowReader{},
		Changes:  &noopCDCReader{},
		CloseFn:  func() error { return nil },
	}, nil
}

// addTableSourceEngineWithPub also implements publicationAdder.
type addTableSourceEngineWithPub struct {
	*addTableSourceEngine
	addedTables []string
}

func newAddTableSourceEngineWithPub(name string) *addTableSourceEngineWithPub {
	return &addTableSourceEngineWithPub{addTableSourceEngine: newAddTableSourceEngine(name)}
}

func (e *addTableSourceEngineWithPub) AddPublicationTables(_ context.Context, _ string, tables []string) error {
	e.addedTables = append(e.addedTables, tables...)
	return nil
}

// addTableSourceEnginePubScoped wraps the live stub with
// ir.PublicationScoper so the ADR-0176-prerequisite recorded-
// publication inheritance is pinnable. WithPublicationScope records
// its arguments and returns the SAME stub, so the test keeps its
// handle on the recorded fields after preflightStream reassigns
// a.Source.
type addTableSourceEnginePubScoped struct {
	*addTableSourceEngineLive

	scopedPublication string
	scopedOwnSlot     string
	scopeCalls        int
}

func (e *addTableSourceEnginePubScoped) WithPublicationScope(publication, ownSlot string) ir.Engine {
	e.scopeCalls++
	e.scopedPublication = publication
	e.scopedOwnSlot = ownSlot
	return e
}

// addTableSourceEngineLive is the publication-source (Postgres-shaped)
// test stand-in: it implements publicationAdder and records the tables the
// add extends the publication with. (Named for the --no-drain live path it
// once stood in for; Bug 332 withdrew that path.)
type addTableSourceEngineLive struct {
	*addTableSourceEngine
	addedTables []string
}

func newAddTableSourceEngineLive(name string) *addTableSourceEngineLive {
	return &addTableSourceEngineLive{addTableSourceEngine: newAddTableSourceEngine(name)}
}

func (e *addTableSourceEngineLive) AddPublicationTables(_ context.Context, _ string, tables []string) error {
	e.addedTables = append(e.addedTables, tables...)
	return nil
}

// addTableTargetEngine bundles a recordingEngine with a configurable
// applier so the orchestrator's stream-existence check has something
// to read.
type addTableTargetEngine struct {
	*recordingEngine
	applier   *fakeApplier
	rowWriter *recordingRowWriterEmpty
}

func newAddTableTargetEngine(name string) *addTableTargetEngine {
	rw := &recordingRowWriterEmpty{empty: true}
	return &addTableTargetEngine{
		recordingEngine: newRecordingEngine(name),
		applier:         &fakeApplier{},
		rowWriter:       rw,
	}
}

func (e *addTableTargetEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	return e.applier, nil
}

func (e *addTableTargetEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	e.openRowWriterCalls++
	e.rowWriter.phaseLog = &e.phaseLog
	return e.rowWriter, nil
}

// fakeApplier is the minimal ChangeApplier shape AddTable.preflight
// needs: ListStreams + ReadStopRequested. Other methods panic so an
// unexpected call stands out in test output.
type fakeApplier struct {
	streams       []ir.StreamStatus
	stopRequested bool

	// liveness / livenessErr / livenessAfter drive ReadStreamLiveness
	// (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED); livenessReads counts
	// the reads.
	liveness      ir.StreamLiveness
	livenessErr   error
	livenessAfter *ir.StreamLiveness
	livenessReads int
	// livenessRowGone makes the zero liveness a missing row rather than
	// the default recorded stop.
	livenessRowGone bool

	// targetSchemaCalls captures the value the orchestrator passed to
	// SetSchema (Bug 46) so tests assert the resolved namespace
	// reached the applier post-resolution.
	targetSchemaCalls string

	// recordedLiveAdds captures the tables passed to
	// RecordLiveAddedTable (ADR-0034) so tests assert the binlog
	// filter-flip path completed end-to-end. Empty unless the test
	// uses a wrapper that exposes the surface.
	recordedLiveAdds []string
	recordedStream   string
}

// SetSchema implements [ir.SchemaSetter] so add-table's
// migcore.ApplyTargetSchema(applier, ...) call lands a non-empty
// targetSchemaCalls value when the orchestrator threads through a
// resolved namespace (Bug 46).
func (f *fakeApplier) SetSchema(name string) {
	f.targetSchemaCalls = name
}

func (f *fakeApplier) EnsureControlTable(context.Context) error { return nil }
func (f *fakeApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (f *fakeApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return f.streams, nil
}

func (f *fakeApplier) Apply(context.Context, string, <-chan ir.Change) error {
	panic("fakeApplier.Apply called — add-table should not stream changes")
}

func (f *fakeApplier) RequestStop(context.Context, string) error {
	panic("fakeApplier.RequestStop called — add-table is a read-only check")
}

func (f *fakeApplier) ReadStopRequested(_ context.Context, _ string) (bool, error) {
	return f.stopRequested, nil
}

func (f *fakeApplier) ClearStopRequested(context.Context, string) error {
	panic("fakeApplier.ClearStopRequested called — add-table should not clear flags")
}

// ReadStreamLiveness implements [ir.StreamLivenessStore] from the
// liveness / livenessErr fields; the zero value is a recorded stop (the
// stream was stopped with `sync stop --wait`), which a drained add passes,
// and livenessRowGone a missing row. livenessAfter, when set, is returned from
// the second read on (the recheck after the pending entry is written).
func (f *fakeApplier) ReadStreamLiveness(context.Context, string) (ir.StreamLiveness, error) {
	f.livenessReads++
	if f.livenessAfter != nil && f.livenessReads > 1 {
		return *f.livenessAfter, nil
	}
	if f.liveness == (ir.StreamLiveness{}) && !f.livenessRowGone {
		return ir.StreamLiveness{Found: true, Recorded: true, Stopped: true}, f.livenessErr
	}
	return f.liveness, f.livenessErr
}

func (f *fakeApplier) StampStreamAlive(context.Context, string) (ir.StreamStamp, error) {
	panic("fakeApplier.StampStreamAlive called — add-table must not stamp a stream's liveness")
}

func (f *fakeApplier) RecordStreamStopped(context.Context, string) error {
	panic("fakeApplier.RecordStreamStopped called — add-table must not stamp a stream's liveness")
}

func (f *fakeApplier) RestoreStreamStopped(context.Context, string, ir.StreamStamp) (bool, error) {
	panic("fakeApplier.RestoreStreamStopped called — add-table must not stamp a stream's liveness")
}

var _ ir.StreamLivenessStore = (*fakeApplier)(nil)

// recordingRowWriterEmpty extends recordingRowWriter with a
// configurable IsTableEmpty so the per-table preflight can be
// exercised in either direction.
type recordingRowWriterEmpty struct {
	phaseLog *[]string
	empty    bool
}

func (w *recordingRowWriterEmpty) WriteRows(_ context.Context, table *ir.Table, rows <-chan ir.Row) error {
	drainRowsLikeAWriter(rows)
	*w.phaseLog = append(*w.phaseLog, "WriteRows:"+table.Name)
	return nil
}

func (w *recordingRowWriterEmpty) IsTableEmpty(_ context.Context, _ *ir.Table) (bool, error) {
	return w.empty, nil
}

// noopCDCReader is a placeholder for SnapshotStream.Changes that
// add-table doesn't actually consume — it captures the snapshot,
// bulk-copies, then exits. Calling StreamChanges in the test path
// would indicate a regression.
type noopCDCReader struct{}

func (noopCDCReader) StreamChanges(context.Context, ir.Position) (<-chan ir.Change, error) {
	return nil, errors.New("noopCDCReader.StreamChanges called — add-table should not start CDC")
}

// cdcNoneEngine is an engine that declares CDC=None, used to test
// the validate-time refusal.
type cdcNoneEngine struct{}

func (cdcNoneEngine) Name() string                  { return "cdc-none" }
func (cdcNoneEngine) Capabilities() ir.Capabilities { return ir.Capabilities{CDC: ir.CDCNone} }
func (cdcNoneEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	panic("not used")
}

func (cdcNoneEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	panic("not used")
}

func (cdcNoneEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) { panic("not used") }

func (cdcNoneEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) { panic("not used") }

func (cdcNoneEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) { panic("not used") }

func (cdcNoneEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	panic("not used")
}

func (cdcNoneEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	panic("not used")
}

// fakeApplierWithLiveAdd implements liveAddedTablesWriter on top of
// fakeApplier so tests for the ADR-0034 binlog filter-flip path can
// assert the orchestrator recorded the table on the cdc-state column.
type fakeApplierWithLiveAdd struct {
	*fakeApplier
}

func (f *fakeApplierWithLiveAdd) RecordLiveAddedTable(_ context.Context, streamID, tableName string) error {
	f.recordedStream = streamID
	f.recordedLiveAdds = append(f.recordedLiveAdds, tableName)
	return nil
}

// addTableTargetEngineWithLiveAdd is an addTableTargetEngine variant
// whose applier implements liveAddedTablesWriter. Used to exercise the
// ADR-0034 binlog filter-flip path in the unit tests without booting a
// real MySQL container.
type addTableTargetEngineWithLiveAdd struct {
	*recordingEngine
	applier   *fakeApplierWithLiveAdd
	rowWriter *recordingRowWriterEmpty
}

func newAddTableTargetEngineWithLiveAdd(name string) *addTableTargetEngineWithLiveAdd {
	rw := &recordingRowWriterEmpty{empty: true}
	base := &fakeApplier{}
	return &addTableTargetEngineWithLiveAdd{
		recordingEngine: newRecordingEngine(name),
		applier:         &fakeApplierWithLiveAdd{fakeApplier: base},
		rowWriter:       rw,
	}
}

func (e *addTableTargetEngineWithLiveAdd) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	return e.applier, nil
}

func (e *addTableTargetEngineWithLiveAdd) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	e.openRowWriterCalls++
	e.rowWriter.phaseLog = &e.phaseLog
	return e.rowWriter, nil
}

// addTableNamespacedTargetEngine wraps an addTableTargetEngine with
// SchemaScope=Namespaced so migcore.ValidateTargetSchema accepts the
// --target-schema flag in Bug 46's resolution tests. The default
// recordingEngine declares Flat (zero-value Capabilities), which the
// validate-time gate refuses upstream.
type addTableNamespacedTargetEngine struct {
	*addTableTargetEngine
}

func newAddTableNamespacedTargetEngine(name string) *addTableNamespacedTargetEngine {
	return &addTableNamespacedTargetEngine{addTableTargetEngine: newAddTableTargetEngine(name)}
}

func (e *addTableNamespacedTargetEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{SchemaScope: ir.SchemaScopeNamespaced}
}

// ---- Bug 46 (v0.25.1) target-schema resolution tests ----

// TestAddTable_TargetSchema_FieldRoundTrip pins the field's presence
// on the orchestrator struct. A typo or rename here would silently
// disable Bug 46's resolve-and-refuse path because the CLI passes
// the value through.
func TestAddTable_TargetSchema_FieldRoundTrip(t *testing.T) {
	a := &AddTable{TargetSchema: "customer_svc"}
	if a.TargetSchema != "customer_svc" {
		t.Errorf("TargetSchema round-trip = %q; want customer_svc", a.TargetSchema)
	}
}

// TestResolveAddTableTargetSchema covers the resolution table from
// resolveAddTableTargetSchema's doc comment: empty/empty, empty/X,
// X/empty, X/X, X/Y. The Y mismatch is the load-bearing refusal —
// it closes the v0.25.0 silent-event-drop failure mode.
func TestResolveAddTableTargetSchema(t *testing.T) {
	cases := []struct {
		name        string
		operator    string
		recorded    string
		want        string
		wantErr     bool
		errContains string
	}{
		{"both empty (DSN default)", "", "", "", false, ""},
		{"inherit recorded", "", "customer_svc", "customer_svc", false, ""},
		{"operator override on legacy/empty recorded", "customer_svc", "", "customer_svc", false, ""},
		{"agreement on non-empty", "customer_svc", "customer_svc", "customer_svc", false, ""},
		{"mismatch refuses", "customer_svc", "billing_svc", "", true, "does not match the active stream's recorded target_schema"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveAddTableTargetSchema("live", c.operator, c.recorded)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q; got nil", c.errContains)
				}
				if !strings.Contains(err.Error(), c.errContains) {
					t.Errorf("err = %v; want contains %q", err, c.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got = %q; want %q", got, c.want)
			}
		})
	}
}

// TestAddTable_TargetSchemaInheritsRecorded confirms add-table with
// no operator-supplied --target-schema flag inherits the recorded
// value from cdc-state and threads it into the schema/row writer.
// Closes Bug 46's primary failure mode: operator forgets the flag,
// table lands in `public`, CDC events drop silently.
func TestAddTable_TargetSchemaInheritsRecorded(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableNamespacedTargetEngine("target")
	// Active stream recorded target_schema=customer_svc.
	tgt.applier.streams = []ir.StreamStatus{
		{StreamID: "live", TargetSchema: "customer_svc"},
	}
	tgt.rowWriter.empty = true

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		// TargetSchema flag deliberately empty → inherit.
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The schema-setter on each writer should have been called with
	// the inherited "customer_svc". The recordingSchemaWriter and
	// recordingRowWriterEmpty don't implement SchemaSetter (no need
	// for them), so the type-assert in migcore.ApplyTargetSchema is a no-op
	// here. The fakeApplier's TargetSchemaCalls counter is the
	// asserting surface: it implements SchemaSetter so we can confirm
	// the inherited value reached the applier.
	if got := tgt.applier.targetSchemaCalls; got != "customer_svc" {
		t.Errorf("applier.SetSchema called with %q; want customer_svc (inherited from cdc-state)", got)
	}
}

// TestAddTable_TargetSchemaMismatchRefuses confirms add-table refuses
// loudly when the operator passes --target-schema=X but the active
// stream's recorded target_schema=Y. ADR-0031 mid-flight namespace
// change detection (now CLOSED via Bug 46's fix).
func TestAddTable_TargetSchemaMismatchRefuses(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableNamespacedTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{
		{StreamID: "live", TargetSchema: "customer_svc"},
	}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		TargetSchema: "billing_svc", // differs from recorded
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatal("expected mismatch refusal; got nil")
	}
	if !strings.Contains(err.Error(), `--target-schema="billing_svc" does not match`) {
		t.Errorf("err = %v; want a target-schema-mismatch message", err)
	}
	// No I/O should have been attempted post-refusal.
	if src.snapshotCalls != 0 {
		t.Errorf("snapshot opens after refusal = %d; want 0", src.snapshotCalls)
	}
}

// TestAddTable_TargetSchemaAgreementProceeds confirms that operator
// flag + recorded value matching is not a refusal, just a regular
// proceed-with-resolved-namespace.
func TestAddTable_TargetSchemaAgreementProceeds(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableNamespacedTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{
		{StreamID: "live", TargetSchema: "customer_svc"},
	}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		TargetSchema: "customer_svc", // matches recorded
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := tgt.applier.targetSchemaCalls; got != "customer_svc" {
		t.Errorf("applier.SetSchema called with %q; want customer_svc", got)
	}
}

// TestAddTable_TargetSchemaLegacyRecordedAcceptsOperatorOverride
// covers the migration scenario: a stream started under v0.25.0
// (before Bug 46's fix) has NULL target_schema. An operator running
// add-table on v0.25.1+ with --target-schema=NAME should be allowed
// to back-fill the namespace rather than refused.
func TestAddTable_TargetSchemaLegacyRecordedAcceptsOperatorOverride(t *testing.T) {
	src := newAddTableSourceEngine("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableNamespacedTargetEngine("target")
	// Legacy row: TargetSchema empty (pre-Bug-46 stream).
	tgt.applier.streams = []ir.StreamStatus{
		{StreamID: "live", TargetSchema: ""},
	}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		TargetSchema: "customer_svc",
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := tgt.applier.targetSchemaCalls; got != "customer_svc" {
		t.Errorf("applier.SetSchema called with %q; want customer_svc (operator-supplied override)", got)
	}
}
