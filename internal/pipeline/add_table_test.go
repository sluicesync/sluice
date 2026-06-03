// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

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

// TestAddTableRefusesActiveStream confirms add-table refuses when
// the stream's stop_requested_at is set — operator should run `sync
// stop --wait` and let it complete (which clears the flag) before
// invoking add-table.
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

// TestAddTable_LiveMode_SkipsActiveStreamRefusal confirms the live-
// mode path does NOT trip the Phase 1 stop_requested_at refusal.
// The stream's row exists, the stop flag is set (would refuse in
// Phase 1), and the live-mode engine's slot-position read succeeds —
// the orchestrator proceeds to publication-add and snapshot.
func TestAddTable_LiveMode_SkipsActiveStreamRefusal(t *testing.T) {
	src := newAddTableSourceEngineLive("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	src.slotLSN = "0/1000"
	src.snapshotLSN = "0/2000"

	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}
	// Phase 1 would refuse on this; live mode must skip the check.
	tgt.applier.stopRequested = true

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run with LiveMode=true unexpectedly errored: %v", err)
	}
	if src.snapshotCalls != 1 {
		t.Errorf("live-mode snapshot opens = %d; want 1", src.snapshotCalls)
	}
	if got := src.addedTables; len(got) != 1 || got[0] != "new_table" {
		t.Errorf("live-mode publication-add tables = %v; want [new_table]", got)
	}
}

// TestAddTable_LiveMode_UsesRecordedSlotName pins the slot-name
// plumbing through cdc-state (ADR-0030 follow-up): when the active
// stream's StreamStatus carries a non-empty SlotName, preflightLive
// queries that slot's position rather than the engine default. This
// is the load-bearing surface for operators running multiple
// concurrent streams against the same source via custom --slot-name.
func TestAddTable_LiveMode_UsesRecordedSlotName(t *testing.T) {
	src := newAddTableSourceEngineLive("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	src.slotLSN = "0/1000"
	src.snapshotLSN = "0/2000"

	tgt := newAddTableTargetEngine("target")
	// StreamStatus carries the operator's custom slot name (recorded
	// by the streamer's SetSlotName plumbing on every position-write).
	tgt.applier.streams = []ir.StreamStatus{
		{StreamID: "live", SlotName: "sluice_shard_a"},
	}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run errored: %v", err)
	}
	if !src.slotReadCalled {
		t.Fatal("ReadSlotPosition was never called")
	}
	if src.slotNameAsked != "sluice_shard_a" {
		t.Errorf("ReadSlotPosition queried slot %q; want %q (from recorded StreamStatus.SlotName)",
			src.slotNameAsked, "sluice_shard_a")
	}
}

// TestAddTable_LiveMode_FallsBackToDefaultSlotName pins the legacy-
// row / default-named-stream fallback: when StreamStatus.SlotName is
// empty (NULL slot_name in cdc-state, e.g. a row that pre-dates the
// column or a stream that ran with the default slot), preflightLive
// queries `sluice_slot`. Without this fallback the live-add would
// refuse when the recorded value happens to be empty.
func TestAddTable_LiveMode_FallsBackToDefaultSlotName(t *testing.T) {
	src := newAddTableSourceEngineLive("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	src.slotLSN = "0/1000"
	src.snapshotLSN = "0/2000"

	tgt := newAddTableTargetEngine("target")
	// SlotName left empty — covers both legacy rows + default streams.
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run errored: %v", err)
	}
	if src.slotNameAsked != "sluice_slot" {
		t.Errorf("ReadSlotPosition queried slot %q; want %q (default fallback)",
			src.slotNameAsked, "sluice_slot")
	}
}

// TestAddTable_LiveMode_RefusesEngineWithoutEitherSurface confirms
// live mode refuses (with a clear engine-pair message) when neither
// the source implements publicationAdder (PG path) NOR the target
// applier implements liveAddedTablesWriter (MySQL filter-flip path).
// Pre-ADR-0034 this was "MySQL refuses live mode" outright; post-
// ADR-0034 MySQL succeeds via the filter-flip path, so the refusal
// only fires when both engines lack the necessary surfaces.
func TestAddTable_LiveMode_RefusesEngineWithoutEitherSurface(t *testing.T) {
	src := newAddTableSourceEngine("unsupported-source") // no publicationAdder
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngine("unsupported-target")
	// fakeApplier does NOT implement liveAddedTablesWriter — this
	// covers the "neither surface available" refusal path.
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("expected error on live mode with neither-surface engine pair; got nil")
	}
	if !strings.Contains(err.Error(), "publication-bearing source engine") {
		t.Errorf("err = %v; want a publication-bearing-engine refusal", err)
	}
	if !strings.Contains(err.Error(), "RecordLiveAddedTable") {
		t.Errorf("err = %v; want a RecordLiveAddedTable refusal mention", err)
	}
	if !strings.Contains(err.Error(), "drained add-table flow") {
		t.Errorf("err = %v; want recovery hint pointing at the drained flow", err)
	}
}

// TestAddTable_LiveMode_BinlogPath_SucceedsWithFilterFlipWriter
// confirms the ADR-0034 filter-flip path runs end-to-end when the
// target applier implements liveAddedTablesWriter — even though the
// source engine does NOT implement publicationAdder. MySQL is the
// canonical engine pair this covers.
func TestAddTable_LiveMode_BinlogPath_SucceedsWithFilterFlipWriter(t *testing.T) {
	src := newAddTableSourceEngine("binlog-source") // no publicationAdder
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngineWithLiveAdd("filter-flip-target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("AddTable (binlog filter-flip path): %v", err)
	}
	// The orchestrator MUST have recorded the new table on the
	// target's cdc-state column.
	if got := tgt.applier.recordedLiveAdds; len(got) != 1 || got[0] != "new_table" {
		t.Errorf("recorded live-adds = %v; want [new_table]", got)
	}
	// Snapshot was opened (bulk-copy path ran).
	if src.snapshotCalls != 1 {
		t.Errorf("snapshot opens = %d; want 1", src.snapshotCalls)
	}
}

// TestAddTable_LiveMode_BinlogPath_RecordsBeforeReturning is the
// regression pin: a successful binlog filter-flip live add MUST have
// recorded the new table on the cdc-state column before Run returns —
// otherwise the running streamer's poll never observes the addition.
func TestAddTable_LiveMode_BinlogPath_RecordsBeforeReturning(t *testing.T) {
	src := newAddTableSourceEngine("binlog-source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "events", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	tgt := newAddTableTargetEngineWithLiveAdd("filter-flip-target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "stream-a"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "stream-a", TableName: "events",
		LiveMode: true,
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(tgt.applier.recordedLiveAdds) == 0 {
		t.Fatal("orchestrator returned without recording the live-added table on cdc-state — streamer poll would never see it")
	}
	if tgt.applier.recordedStream != "stream-a" {
		t.Errorf("recorded against streamID %q; want %q", tgt.applier.recordedStream, "stream-a")
	}
}

// TestAddTable_LiveMode_InvariantFires confirms the snapshot-LSN ≥
// slot-LSN invariant trips loudly when a stub engine reports a
// regressed snapshot LSN. The standard ordering can't actually
// produce this in practice, but the check pins the invariant
// against a future regression in the flow's ordering.
func TestAddTable_LiveMode_InvariantFires(t *testing.T) {
	src := newAddTableSourceEngineLive("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	src.slotLSN = "0/2000"     // active stream at 0/2000
	src.snapshotLSN = "0/1000" // snapshot somehow regressed to 0/1000

	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("expected invariant-fired error; got nil")
	}
	if !strings.Contains(err.Error(), "snapshot LSN") || !strings.Contains(err.Error(), "behind") {
		t.Errorf("err = %v; want snapshot-LSN-behind-slot-LSN refusal", err)
	}
}

// TestAddTable_LiveMode_EmptySlotLSNSkipsInvariant confirms that
// when the active slot's confirmed_flush_lsn is empty (fresh slot,
// no consumer progress yet), the invariant check is skipped and
// the orchestrator proceeds. This mirrors PG's behaviour right
// after a slot is created but before any commits have flowed.
func TestAddTable_LiveMode_EmptySlotLSNSkipsInvariant(t *testing.T) {
	src := newAddTableSourceEngineLive("source")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	src.slotLSN = ""           // empty floor → skip
	src.snapshotLSN = "0/1000" // any value is fine

	tgt := newAddTableTargetEngine("target")
	tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}

	a := &AddTable{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		StreamID: "live", TableName: "new_table",
		LiveMode: true,
	}
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run with empty slot LSN unexpectedly errored: %v", err)
	}
}

// TestActiveSlotName pins the slot-name lookup behavior for live
// mode (ADR-0030). The cdc-state row's slot_name column is the
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

// TestSlotNameSource pins the diagnostic tag so log inspection has
// a stable contract. Operators tracing a live-add that picked the
// wrong slot read this tag to confirm whether it came from the
// recorded row or the default fallback.
func TestSlotNameSource(t *testing.T) {
	if got := slotNameSource(ir.StreamStatus{SlotName: "sluice_shard_a"}); got != "recorded" {
		t.Errorf("slotNameSource(recorded) = %q; want %q", got, "recorded")
	}
	if got := slotNameSource(ir.StreamStatus{}); got != "default-fallback" {
		t.Errorf("slotNameSource(empty) = %q; want %q", got, "default-fallback")
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
// support and CDC=LogicalReplication so AddTable.validate accepts
// it.
type addTableSourceEngine struct {
	*recordingEngine
	snapshotCalls int
}

func newAddTableSourceEngine(name string) *addTableSourceEngine {
	return &addTableSourceEngine{recordingEngine: newRecordingEngine(name)}
}

func (e *addTableSourceEngine) Capabilities() ir.Capabilities {
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

// addTableSourceEngineLive is the live-mode (Phase 2) test stand-in.
// Implements every optional surface the live-mode preflight + LSN
// invariant check needs:
//   - publicationAdder (gates the live-mode refusal)
//   - slotPositionReader (returns the configured slotLSN)
//   - snapshotLSNExtractor + lsnComparer (drive the invariant check;
//     the comparer here is a string compare which works for the
//     monotonically-increasing test fixtures)
//
// The snapshot it returns advertises snapshotLSN as its position
// token via a JSON envelope ExtractSnapshotLSN can decode.
type addTableSourceEngineLive struct {
	*addTableSourceEngine
	addedTables    []string
	slotLSN        string
	snapshotLSN    string
	slotNameAsked  string // captured by ReadSlotPosition for assertion
	slotReadCalled bool
}

func newAddTableSourceEngineLive(name string) *addTableSourceEngineLive {
	return &addTableSourceEngineLive{addTableSourceEngine: newAddTableSourceEngine(name)}
}

func (e *addTableSourceEngineLive) AddPublicationTables(_ context.Context, _ string, tables []string) error {
	e.addedTables = append(e.addedTables, tables...)
	return nil
}

func (e *addTableSourceEngineLive) ReadSlotPosition(_ context.Context, _, slotName string) (string, error) {
	e.slotReadCalled = true
	e.slotNameAsked = slotName
	return e.slotLSN, nil
}

// OpenSnapshotStream overrides the embedded base to advertise the
// configured snapshotLSN in the position token. The token is the
// LSN itself (no JSON envelope) — the test extractor / comparer
// below handle this format directly.
func (e *addTableSourceEngineLive) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	e.snapshotCalls++
	return &ir.SnapshotStream{
		Position: ir.Position{Engine: e.name, Token: e.snapshotLSN},
		Rows:     &recordingRowReader{},
		Changes:  &noopCDCReader{},
		CloseFn:  func() error { return nil },
	}, nil
}

// ExtractSnapshotLSN treats the position token as the LSN string
// directly (test-only shape; the real PG engine decodes a JSON
// envelope).
func (e *addTableSourceEngineLive) ExtractSnapshotLSN(pos ir.Position) (lsn string, ok bool, err error) {
	if pos.Engine == "" && pos.Token == "" {
		return "", false, nil
	}
	return pos.Token, true, nil
}

// CompareLSN does a lexicographic compare on the LSN strings —
// adequate for the monotonically-increasing fixtures the live-
// mode unit tests use ("0/1000", "0/2000", ...). Real engines do
// numeric comparison after parsing.
func (e *addTableSourceEngineLive) CompareLSN(a, b string) (int, error) {
	switch {
	case a < b:
		return -1, nil
	case a > b:
		return 1, nil
	}
	return 0, nil
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
// applyTargetSchema(applier, ...) call lands a non-empty
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

// recordingRowWriterEmpty extends recordingRowWriter with a
// configurable IsTableEmpty so the per-table preflight can be
// exercised in either direction.
type recordingRowWriterEmpty struct {
	phaseLog *[]string
	empty    bool
}

func (w *recordingRowWriterEmpty) WriteRows(_ context.Context, table *ir.Table, _ <-chan ir.Row) error {
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
// SchemaScope=Namespaced so validateTargetSchema accepts the
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
	// for them), so the type-assert in applyTargetSchema is a no-op
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
