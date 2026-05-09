// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
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
	// Bulk-copy phases ran on the target.
	wantPhases := []string{
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

// TestAddTable_LiveMode_RefusesEngineWithoutPublication confirms
// live mode refuses (with a clear PG-only message) on an engine
// that doesn't implement publicationAdder. MySQL is the canonical
// case; the recordingEngine-without-pub stand-in here exercises the
// same path.
func TestAddTable_LiveMode_RefusesEngineWithoutPublication(t *testing.T) {
	src := newAddTableSourceEngine("mysql-like") // no publicationAdder
	src.schema = &ir.Schema{Tables: []*ir.Table{
		{Name: "new_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
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
		t.Fatalf("expected error on live mode with non-publication engine; got nil")
	}
	if !strings.Contains(err.Error(), "publication-bearing source engine") {
		t.Errorf("err = %v; want a publication-bearing-engine refusal", err)
	}
	if !strings.Contains(err.Error(), "drained add-table flow") {
		t.Errorf("err = %v; want recovery hint pointing at the drained flow", err)
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
	addedTables []string
	slotLSN     string
	snapshotLSN string
}

func newAddTableSourceEngineLive(name string) *addTableSourceEngineLive {
	return &addTableSourceEngineLive{addTableSourceEngine: newAddTableSourceEngine(name)}
}

func (e *addTableSourceEngineLive) AddPublicationTables(_ context.Context, _ string, tables []string) error {
	e.addedTables = append(e.addedTables, tables...)
	return nil
}

func (e *addTableSourceEngineLive) ReadSlotPosition(_ context.Context, _, _ string) (string, error) {
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
