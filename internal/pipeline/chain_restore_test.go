// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// makeManifest returns a manifest with deterministic CreatedAt and
// position for chain-walk test fixtures.
func makeManifest(t *testing.T, kind string, parent *ir.Manifest, lsn string) *ir.Manifest {
	t.Helper()
	m := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}},
		Kind:          kind,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"` + lsn + `"}`},
	}
	if parent != nil {
		m.ParentBackupID = manifestBackupID(parent)
		m.StartPosition = parent.EndPosition
	}
	m.BackupID = ir.ComputeBackupID(m)
	return m
}

func TestBuildChain_SingleFull(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write: %v", err)
	}
	chain, err := buildChain(context.Background(), store)
	if err != nil {
		t.Fatalf("buildChain: %v", err)
	}
	if len(chain) != 1 {
		t.Errorf("chain len = %d; want 1", len(chain))
	}
}

func TestBuildChain_LinearChainOK(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	incr1 := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incr2 := makeManifest(t, ir.BackupKindIncremental, incr1, "0/300")

	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0001-aa.json", incr1); err != nil {
		t.Fatalf("write incr1: %v", err)
	}
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0002-bb.json", incr2); err != nil {
		t.Fatalf("write incr2: %v", err)
	}

	chain, err := buildChain(context.Background(), store)
	if err != nil {
		t.Fatalf("buildChain: %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("chain len = %d; want 3", len(chain))
	}
	if chain[0].manifest.Kind != ir.BackupKindFull {
		t.Errorf("chain[0] kind = %q; want full", chain[0].manifest.Kind)
	}
	if manifestBackupID(chain[1].manifest) != manifestBackupID(incr1) {
		t.Errorf("chain[1] is not incr1")
	}
	if manifestBackupID(chain[2].manifest) != manifestBackupID(incr2) {
		t.Errorf("chain[2] is not incr2")
	}
}

func TestBuildChain_BranchingRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	incrA := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incrB := makeManifest(t, ir.BackupKindIncremental, full, "0/250") // also chains off full → branch

	_ = writeManifestAt(context.Background(), store, ManifestFileName, full)
	_ = writeManifestAt(context.Background(), store, "manifests/incr-0001-a.json", incrA)
	_ = writeManifestAt(context.Background(), store, "manifests/incr-0001-b.json", incrB)

	_, err := buildChain(context.Background(), store)
	if err == nil || !strings.Contains(err.Error(), "branches") {
		t.Errorf("err = %v; want branching refusal", err)
	}
}

func TestBuildChain_OrphanRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	incr1 := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	// orphan: parent BackupID points at something that doesn't exist
	orphan := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: "0000000000000000",
		StartPosition:  ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"9/9"}`},
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"9/A"}`},
	}
	orphan.BackupID = ir.ComputeBackupID(orphan)

	_ = writeManifestAt(context.Background(), store, ManifestFileName, full)
	_ = writeManifestAt(context.Background(), store, "manifests/incr-0001.json", incr1)
	_ = writeManifestAt(context.Background(), store, "manifests/incr-9999.json", orphan)

	_, err := buildChain(context.Background(), store)
	if err == nil || !strings.Contains(err.Error(), "orphan") {
		t.Errorf("err = %v; want orphan refusal", err)
	}
}

func TestBuildChain_MultipleFullsRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full1 := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	full2 := makeManifest(t, ir.BackupKindFull, nil, "0/200")
	_ = writeManifestAt(context.Background(), store, ManifestFileName, full1)
	// A second "full" lurking in manifests/ — fabricated test scenario.
	_ = writeManifestAt(context.Background(), store, "manifests/incr-rogue.json", full2)
	_, err := buildChain(context.Background(), store)
	if err == nil || !strings.Contains(err.Error(), "full manifests") {
		t.Errorf("err = %v; want multiple-full refusal", err)
	}
}

func TestBuildChain_StartPositionMismatchRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	// Tampered: manually point incremental to a different StartPosition.
	tampered := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	tampered.StartPosition = ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"WRONG"}`}
	tampered.BackupID = ir.ComputeBackupID(tampered)

	_ = writeManifestAt(context.Background(), store, ManifestFileName, full)
	_ = writeManifestAt(context.Background(), store, "manifests/incr-0001.json", tampered)

	_, err := buildChain(context.Background(), store)
	if err == nil || !strings.Contains(err.Error(), "chain link mismatch") {
		t.Errorf("err = %v; want chain-link mismatch refusal", err)
	}
}

func TestDetectAmbiguousDeltas_RenameRefuses(t *testing.T) {
	deltas := []*ir.SchemaDeltaEntry{
		{
			Kind:  ir.SchemaDeltaAlterTable,
			Table: "users",
			Before: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "username", Type: ir.Varchar{Length: 50}},
			}},
			After: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "login", Type: ir.Varchar{Length: 50}}, // rename!
			}},
		},
	}
	if err := detectAmbiguousDeltas(deltas); err == nil {
		t.Errorf("detectAmbiguousDeltas: nil; want refusal on rename ambiguity")
	}
}

func TestDetectAmbiguousDeltas_AddOnlyOK(t *testing.T) {
	deltas := []*ir.SchemaDeltaEntry{
		{
			Kind:  ir.SchemaDeltaAlterTable,
			Table: "users",
			Before: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
			}},
			After: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "email", Type: ir.Varchar{Length: 255}},
			}},
		},
	}
	if err := detectAmbiguousDeltas(deltas); err != nil {
		t.Errorf("detectAmbiguousDeltas: %v; want clean", err)
	}
}

// chainRestoreRecorderEngine: a target engine that records every
// schema-write phase and every applier event so chain-restore tests
// can assert ordering and content.
type chainRestoreRecorderEngine struct {
	*restoreRecorderEngine
	mu       sync.Mutex
	applied  []ir.Change
	applierC *chainRestoreRecordingApplier
}

func (e *chainRestoreRecorderEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	if e.applierC == nil {
		e.applierC = &chainRestoreRecordingApplier{owner: e}
	}
	return e.applierC, nil
}

type chainRestoreRecordingApplier struct {
	owner *chainRestoreRecorderEngine
}

func (a *chainRestoreRecordingApplier) EnsureControlTable(_ context.Context) error { return nil }
func (a *chainRestoreRecordingApplier) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *chainRestoreRecordingApplier) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (a *chainRestoreRecordingApplier) Apply(_ context.Context, _ string, changes <-chan ir.Change) error {
	for c := range changes {
		a.owner.mu.Lock()
		a.owner.applied = append(a.owner.applied, c)
		a.owner.mu.Unlock()
	}
	return nil
}

func (a *chainRestoreRecordingApplier) RequestStop(context.Context, string) error        { return nil }
func (a *chainRestoreRecordingApplier) ClearStopRequested(context.Context, string) error { return nil }
func (a *chainRestoreRecordingApplier) Close() error                                     { return nil }

// TestChainRestore_FullPlusOneIncremental_RoundTrip is the load-bearing
// end-to-end test for Phase 3.2 acceptance criterion 2: write a full
// + one incremental, restore the chain, and verify the applied
// change events match what was streamed into the incremental.
func TestChainRestore_FullPlusOneIncremental_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	// Full backup via the existing Backup pipeline.
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
	})
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	// Patch the full's manifest with an EndPosition + BackupID so the
	// incremental can chain off it. (The full backup pipeline
	// doesn't yet record EndPosition for fulls — Phase 3.3 work.)
	full, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("read full manifest: %v", err)
	}
	full.Kind = ir.BackupKindFull
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}

	// Incremental: feed a recorded set of changes through the
	// IncrementalBackup orchestrator.
	cdc := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema, schema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}},
			ir.Insert{
				Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`},
				Table:    "users",
				Row:      ir.Row{"id": int64(3)},
			},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
	}
	ib := &IncrementalBackup{
		Source:    cdc,
		SourceDSN: "src",
		Store:     store,
		ParentRef: full.BackupID,
		Window:    5 * time.Minute,
	}
	if err := ib.Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// Restore via the chain path. Override Restore.Run's chain
	// dispatch by calling ChainRestore directly to keep the test
	// independent of the dispatcher.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &ChainRestore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
	}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}

	// Phase ordering: the full's CreateTablesWithoutConstraints must
	// have run before any change-apply.
	phases, _ := tgt.snapshot()
	if len(phases) == 0 || phases[0] != "CreateTablesWithoutConstraints" {
		t.Errorf("phase[0] = %v; want CreateTablesWithoutConstraints first", phases)
	}

	// Applier received the 3 incremental changes (tx_begin + insert
	// + tx_commit).
	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("applied changes = %d; want 3 (got %v)", len(got), got)
	}
	if _, ok := got[0].(ir.TxBegin); !ok {
		t.Errorf("got[0] = %T; want TxBegin", got[0])
	}
	ins, ok := got[1].(ir.Insert)
	if !ok {
		t.Errorf("got[1] = %T; want Insert", got[1])
	} else if ins.Table != "users" || !valuesEquivalent(ins.Row["id"], int64(3)) {
		t.Errorf("got[1] = %+v; want users.id=3", ins)
	}
	if _, ok := got[2].(ir.TxCommit); !ok {
		t.Errorf("got[2] = %T; want TxCommit", got[2])
	}
}

// TestChainRestore_CrossEngineWithIncrementalsSucceeds verifies that
// Phase 5's cross-engine routing lifts the v0.17.0–v0.20.x refusal:
// a chain whose source engine differs from the target engine now
// runs through the restore + applier pipeline. Same shape, different
// outcome: the test that used to assert refusal now asserts the
// no-incremental-data path completes cleanly. Cross-engine acceptance
// criteria 1–4 are validated end-to-end by the integration tests in
// chain_restore_cross_integration_test.go.
func TestChainRestore_CrossEngineWithIncrementalsSucceeds(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	// Full manifest: source_engine=postgres.
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	full.SourceEngine = "postgres"
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	incr := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incr.SourceEngine = "postgres"
	incr.BackupID = ir.ComputeBackupID(incr)
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}

	// Target engine = mysql, distinct from the chain's source. The
	// recorder engine handles every phase as a no-op + record; the
	// incremental has no change chunks so the applier is exercised
	// only via OpenChangeApplier + EnsureControlTable.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	chain := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("chain restore: %v", err)
	}
	phases, _ := tgt.snapshot()
	if len(phases) == 0 || phases[0] != "CreateTablesWithoutConstraints" {
		t.Errorf("phase[0] = %v; want CreateTablesWithoutConstraints first", phases)
	}
}

// TestChainRestore_CrossEnginePostGISRefuses asserts the loud-refusal
// path for unsupportable cross-engine types. Phase 5 acceptance
// criterion 4: a PG chain whose schema includes a PostGIS geometry
// column refuses with operator-actionable guidance when the target is
// MySQL — the offending table is named so `--exclude-table=<name>`
// gives the operator a clean recovery path.
func TestChainRestore_CrossEnginePostGISRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	// Full manifest with a PostGIS geometry column.
	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          ir.BackupKindFull,
		Schema: &ir.Schema{Tables: []*ir.Table{{
			Name: "places",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
			},
		}}},
		EndPosition: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	chain := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	err := chain.Run(context.Background())
	if err == nil {
		t.Fatal("err = nil; want PostGIS refusal")
	}
	if !strings.Contains(err.Error(), "PostGIS") {
		t.Errorf("err = %v; want PostGIS refusal", err)
	}
	if !strings.Contains(err.Error(), "places") {
		t.Errorf("err = %v; want refusal to name table 'places'", err)
	}
	if !strings.Contains(err.Error(), "--exclude-table") {
		t.Errorf("err = %v; want recovery hint with --exclude-table", err)
	}
}

// TestChainRestore_DispatchFromRestore_Run verifies the existing
// Restore.Run delegates to ChainRestore when incrementals are
// present.
func TestChainRestore_DispatchFromRestore_Run(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{"users": {{"id": int64(1)}}})
	_ = (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background())

	// Patch the full with an EndPosition.
	full, _ := readManifest(context.Background(), store)
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"0/100"}`}
	full.BackupID = ir.ComputeBackupID(full)
	_ = writeManifestAt(context.Background(), store, ManifestFileName, full)

	// Add an empty incremental so the dispatch fires.
	cdc := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema, schema},
		cdcChanges:     []ir.Change{},
	}
	if err := (&IncrementalBackup{
		Source: cdc, SourceDSN: "src", Store: store,
		ParentRef: full.BackupID,
		Window:    1 * time.Millisecond, // close immediately
	}).Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup: %v", err)
	}

	// Now Restore.Run with the standard recorder should detect the
	// incremental and dispatch.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	r := &Restore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	// Successful chain restore implies the dispatch fired (otherwise
	// the recorder's applier would never have been called and we'd
	// surface a different error).
	phases, _ := tgt.snapshot()
	if len(phases) == 0 {
		t.Errorf("no phases recorded; restore must have dispatched to chain")
	}
}
