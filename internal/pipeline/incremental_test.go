// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// fakeCDCEngine is a backup-recorder analogue for incremental tests:
// it serves a configured schema, and its CDC reader plays a
// pre-recorded sequence of changes onto the channel.
type fakeCDCEngine struct {
	name              string
	schemaSequence    []*ir.Schema // returned in order; last value is sticky
	schemaReadCalls   int
	cdcChanges        []ir.Change
	cdcStartErr       error // returned from StreamChanges when non-nil
	cdcExpectedFromOK bool  // when true, refuse a "from now" empty position
	cdcSeenFrom       ir.Position
}

func (e *fakeCDCEngine) Name() string { return e.name }
func (e *fakeCDCEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDC: ir.CDCLogicalReplication}
}

func (e *fakeCDCEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	idx := e.schemaReadCalls
	if idx >= len(e.schemaSequence) {
		idx = len(e.schemaSequence) - 1
	}
	if idx < 0 {
		return nil, errors.New("fakeCDCEngine: no schema configured")
	}
	e.schemaReadCalls++
	return &recordingSchemaReader{schema: e.schemaSequence[idx]}, nil
}

func (*fakeCDCEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return nil, errors.New("not used")
}

func (*fakeCDCEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return nil, errors.New("not used")
}

func (*fakeCDCEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return nil, errors.New("not used")
}

func (e *fakeCDCEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	return &fakeCDCReader{engine: e}, nil
}

func (*fakeCDCEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not used")
}

func (*fakeCDCEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not used")
}

type fakeCDCReader struct {
	engine *fakeCDCEngine
}

func (r *fakeCDCReader) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	r.engine.cdcSeenFrom = from
	if r.engine.cdcExpectedFromOK && (from.Engine == "" && from.Token == "") {
		return nil, errors.New("fakeCDCReader: expected non-empty position")
	}
	if r.engine.cdcStartErr != nil {
		return nil, r.engine.cdcStartErr
	}
	out := make(chan ir.Change, len(r.engine.cdcChanges)+1)
	go func() {
		defer close(out)
		for _, c := range r.engine.cdcChanges {
			select {
			case <-ctx.Done():
				return
			case out <- c:
			}
		}
	}()
	return out, nil
}

func (r *fakeCDCReader) Close() error { return nil }

// helper: write a fake "parent full" manifest into the store so the
// incremental orchestrator has something to chain off.
func writeParentFullManifest(t *testing.T, store *LocalStore, parent *ir.Manifest) {
	t.Helper()
	if err := writeManifestAt(context.Background(), store, ManifestFileName, parent); err != nil {
		t.Fatalf("write parent: %v", err)
	}
}

func TestIncrementalBackup_Validate(t *testing.T) {
	cases := []struct {
		name string
		b    *IncrementalBackup
		want string
	}{
		{"nil source", &IncrementalBackup{SourceDSN: "x", Store: &LocalStore{}}, "Source engine is nil"},
		{"empty DSN", &IncrementalBackup{Source: &fakeCDCEngine{name: "postgres"}, Store: &LocalStore{}}, "SourceDSN is empty"},
		{"nil store", &IncrementalBackup{Source: &fakeCDCEngine{name: "postgres"}, SourceDSN: "x"}, "Store is nil"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.b.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want contains %q", err, c.want)
			}
		})
	}
}

func TestIncrementalBackup_Validate_NoCDC(t *testing.T) {
	src := &fakeCDCEngine{name: "postgres"}
	src.schemaSequence = []*ir.Schema{{}}
	// Override capabilities by wrapping.
	wrapped := &noCDCCapEngine{src: src}
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	b := &IncrementalBackup{Source: wrapped, SourceDSN: "x", Store: store, ParentRef: ""}
	err := b.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not declare CDC support") {
		t.Errorf("err = %v; want CDC capability refusal", err)
	}
}

// noCDCCapEngine wraps fakeCDCEngine but reports CDCNone.
type noCDCCapEngine struct {
	src *fakeCDCEngine
}

func (e *noCDCCapEngine) Name() string                  { return e.src.Name() }
func (e *noCDCCapEngine) Capabilities() ir.Capabilities { return ir.Capabilities{CDC: ir.CDCNone} }
func (e *noCDCCapEngine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	return e.src.OpenSchemaReader(ctx, dsn)
}

func (e *noCDCCapEngine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	return e.src.OpenSchemaWriter(ctx, dsn)
}

func (e *noCDCCapEngine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	return e.src.OpenRowReader(ctx, dsn)
}

func (e *noCDCCapEngine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	return e.src.OpenRowWriter(ctx, dsn)
}

func (e *noCDCCapEngine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return e.src.OpenCDCReader(ctx, dsn)
}

func (e *noCDCCapEngine) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	return e.src.OpenChangeApplier(ctx, dsn)
}

func (e *noCDCCapEngine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	return e.src.OpenSnapshotStream(ctx, dsn)
}

// TestIncrementalBackup_RoundTrip runs an incremental backup against
// a fake source with a recorded change stream and validates the
// resulting manifest + chunks shape. This is the load-bearing test
// for Phase 3.1 acceptance criterion 1.
func TestIncrementalBackup_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	// Parent full: written as a Phase-3 manifest with a recorded
	// EndPosition so the incremental opens at the right LSN.
	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  ir.BackupStateComplete,
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	// CDC stream: a Begin + Insert + Commit.
	parentEndPos := parent.EndPosition
	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema}, // after-shape; before comes from parent manifest
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}},
			ir.Insert{
				Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`},
				Schema:   "",
				Table:    "users",
				Row:      ir.Row{"id": int64(42), "name": "Zaphod"},
			},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
		cdcExpectedFromOK: true,
	}

	now := time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)
	// Pin the clock so the deadline-driven loop closes promptly. The
	// loop closes on channel-close rather than timer expiry in this
	// test (the fake stream emits 3 changes then closes), so the
	// clock is informational.
	b := &IncrementalBackup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return now },
		clockNow:      func() time.Time { return now },
	}

	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// CDC reader saw the parent's EndPosition.
	if src.cdcSeenFrom != parentEndPos {
		t.Errorf("CDC stream from = %+v; want %+v", src.cdcSeenFrom, parentEndPos)
	}

	// Find the new manifest in the store.
	records, err := listAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("listAllManifestsViaWalk: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("manifests in store = %d; want 2 (full + 1 incremental)", len(records))
	}
	var incr *ir.Manifest
	var incrPath string
	for _, r := range records {
		if r.manifest.Kind == ir.BackupKindIncremental {
			incr = r.manifest
			incrPath = r.path
		}
	}
	if incr == nil {
		t.Fatalf("no incremental manifest written; paths = %v", records)
	}

	// Verify the manifest's chain link.
	if incr.ParentBackupID != parent.BackupID {
		t.Errorf("ParentBackupID = %q; want %q", incr.ParentBackupID, parent.BackupID)
	}
	if incr.Kind != ir.BackupKindIncremental {
		t.Errorf("Kind = %q; want incremental", incr.Kind)
	}
	if incr.PartialState != ir.BackupStateComplete {
		t.Errorf("PartialState = %q; want complete", incr.PartialState)
	}
	// StartPosition = parent's EndPosition.
	if incr.StartPosition != parent.EndPosition {
		t.Errorf("StartPosition = %+v; want %+v", incr.StartPosition, parent.EndPosition)
	}
	// EndPosition = position of the last applied change (Commit at 0/130).
	wantEnd := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}
	if incr.EndPosition != wantEnd {
		t.Errorf("EndPosition = %+v; want %+v", incr.EndPosition, wantEnd)
	}
	// SchemaHash non-empty.
	if incr.SchemaHash == "" {
		t.Errorf("SchemaHash empty")
	}
	// SchemaDelta empty (before == after).
	if len(incr.SchemaDelta) != 0 {
		for _, d := range incr.SchemaDelta {
			t.Logf("unexpected delta: kind=%q table=%q before=%+v after=%+v", d.Kind, d.Table, d.Before, d.After)
		}
		t.Errorf("SchemaDelta len = %d; want empty", len(incr.SchemaDelta))
	}
	// One chunk written, with 3 changes.
	if len(incr.ChangeChunks) != 1 {
		t.Fatalf("ChangeChunks len = %d; want 1", len(incr.ChangeChunks))
	}
	if incr.ChangeChunks[0].RowCount != 3 {
		t.Errorf("ChangeChunks[0].RowCount = %d; want 3", incr.ChangeChunks[0].RowCount)
	}
	// BackupID populated and stable.
	if incr.BackupID == "" {
		t.Errorf("BackupID empty")
	}
	// Manifest path matches the convention.
	if !strings.HasPrefix(incrPath, "manifests/incr-") || !strings.HasSuffix(incrPath, ".json") {
		t.Errorf("manifest path = %q; want manifests/incr-<...>.json", incrPath)
	}
}

// TestIncrementalBackup_PositionInvalid_LoudFailure verifies the
// "parent's WAL has been pruned" surface produces a clear error
// rather than a silent success.
func TestIncrementalBackup_PositionInvalid_LoudFailure(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{{}},
		cdcStartErr:    ir.ErrPositionInvalid,
	}
	b := &IncrementalBackup{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
		ParentRef: parent.BackupID,
	}
	err := b.Run(context.Background())
	if err == nil {
		t.Fatal("err = nil; want loud failure on pruned WAL")
	}
	if !strings.Contains(err.Error(), "fresh full") {
		t.Errorf("err = %v; want clear 'take a fresh full' guidance", err)
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Errorf("err = %v; want errors.Is ErrPositionInvalid", err)
	}
}

// TestIncrementalBackup_SchemaDelta_AddColumn verifies that an ALTER
// TABLE on the source between the start- and end-of-window schema
// reads surfaces as a SchemaDelta entry on the manifest.
func TestIncrementalBackup_SchemaDelta_AddColumn(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	beforeSchema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	afterSchema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
		},
	}}}

	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        beforeSchema,
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	// The source schema reader is called once at window-end (the
	// "after" snapshot); the "before" baseline comes from the parent
	// manifest's Schema (= beforeSchema, written via
	// writeParentFullManifest above). So schemaSequence here only
	// needs to return the after-shape.
	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{afterSchema},
		cdcChanges: []ir.Change{
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
	}

	b := &IncrementalBackup{
		Source: src, SourceDSN: "src", Store: store,
		ParentRef: parent.BackupID,
		Window:    5 * time.Minute,
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	records, _ := listAllManifestsViaWalk(context.Background(), store)
	var incr *ir.Manifest
	for _, r := range records {
		if r.manifest.Kind == ir.BackupKindIncremental {
			incr = r.manifest
		}
	}
	if incr == nil {
		t.Fatal("no incremental manifest")
	}
	if len(incr.SchemaDelta) != 1 {
		t.Fatalf("SchemaDelta len = %d; want 1", len(incr.SchemaDelta))
	}
	d := incr.SchemaDelta[0]
	if d.Kind != ir.SchemaDeltaAlterTable || d.Table != "users" {
		t.Errorf("SchemaDelta = %+v; want alter_table on users", d)
	}
	if d.Before == nil || d.After == nil {
		t.Errorf("SchemaDelta missing before/after: %+v", d)
	}
	if len(d.After.Columns) != 2 {
		t.Errorf("After.Columns = %d; want 2", len(d.After.Columns))
	}
}

// TestDiffSchemas_AddDropAlter pins the diff helper's behaviour.
func TestDiffSchemas_AddDropAlter(t *testing.T) {
	before := &ir.Schema{Tables: []*ir.Table{
		{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		{Name: "to_drop", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	after := &ir.Schema{Tables: []*ir.Table{
		{Name: "users", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
		}},
		{Name: "fresh_table", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}

	deltas := diffSchemas(before, after)
	// Expect: drop to_drop, add fresh_table, alter users.
	kinds := map[string]int{}
	for _, d := range deltas {
		kinds[d.Kind+":"+d.Table]++
	}
	want := map[string]int{
		ir.SchemaDeltaDropTable + ":to_drop":    1,
		ir.SchemaDeltaAddTable + ":fresh_table": 1,
		ir.SchemaDeltaAlterTable + ":users":     1,
	}
	for k, v := range want {
		if kinds[k] != v {
			t.Errorf("missing/incorrect delta %s: got %d want %d (all=%v)", k, kinds[k], v, kinds)
		}
	}
}

// TestDiffSchemas_NoChange returns empty.
func TestDiffSchemas_NoChange(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	if got := diffSchemas(s, s); len(got) != 0 {
		t.Errorf("no-change diff = %+v; want empty", got)
	}
}

// TestIncrementalBackup_NoParent loud failure when the store has no
// manifests.
func TestIncrementalBackup_NoParent(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	src := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}}
	b := &IncrementalBackup{Source: src, SourceDSN: "x", Store: store}
	err := b.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no parent manifest") {
		t.Errorf("err = %v; want 'no parent manifest'", err)
	}
}

// TestIncrementalBackup_UnknownParentRef loud failure when the
// supplied ParentRef doesn't match anything.
func TestIncrementalBackup_UnknownParentRef(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          ir.BackupKindFull,
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}}
	b := &IncrementalBackup{Source: src, SourceDSN: "x", Store: store, ParentRef: "doesnotexist"}
	err := b.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "doesnotexist") {
		t.Errorf("err = %v; want clear unknown-parent error", err)
	}
}

// TestIncrementalBackup_TwoIncrementals_NoChunkCollision is the
// regression for Bug 35 from the v0.17.0 cycle. Pre-fix, both
// incrementals' change chunks landed at the same path
// (`chunks/_changes/changes-0.jsonl.gz`) — the second overwrote the
// first on disk while the manifests still referenced the original
// SHA-256, so chain restore + `backup verify` both failed with
// `chunk SHA-256 mismatch`.
//
// The fix namespaces chunk paths under a per-incremental segment so
// the two incrementals' chunks coexist on disk. This test pins:
//
//   - the two incrementals' recorded chunk paths are distinct;
//   - both files exist on the store after both runs;
//   - each manifest's recorded SHA-256 matches the bytes on disk
//     (the chain-restore failure mode pre-fix was exactly the SHA-256
//     mismatch that this assertion guards against).
func TestIncrementalBackup_TwoIncrementals_NoChunkCollision(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  ir.BackupStateComplete,
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	// Helper: drive one incremental against `store`, parented on
	// `parentID`, with a single insert at the given LSN and a pinned
	// CreatedAt so the run namespace is deterministic per-call.
	runIncr := func(t *testing.T, parentID, lsn string, createdAt time.Time) *ir.Manifest {
		t.Helper()
		src := &fakeCDCEngine{
			name:           "postgres",
			schemaSequence: []*ir.Schema{schema},
			cdcChanges: []ir.Change{
				ir.Insert{
					Position: ir.Position{Engine: "postgres", Token: lsn},
					Table:    "users",
					Row:      ir.Row{"id": int64(1)},
				},
			},
		}
		b := &IncrementalBackup{
			Source:        src,
			SourceDSN:     "src",
			Store:         store,
			ParentRef:     parentID,
			Window:        5 * time.Minute,
			ChunkChanges:  10,
			SluiceVersion: "test",
			Now:           func() time.Time { return createdAt },
			clockNow:      func() time.Time { return createdAt },
		}
		if err := b.Run(context.Background()); err != nil {
			t.Fatalf("incremental Run: %v", err)
		}
		records, err := listAllManifestsViaWalk(context.Background(), store)
		if err != nil {
			t.Fatalf("listAllManifestsViaWalk: %v", err)
		}
		// Pick the most recent incremental.
		var newest *ir.Manifest
		for _, r := range records {
			if r.manifest.Kind != ir.BackupKindIncremental {
				continue
			}
			if newest == nil || r.manifest.CreatedAt.After(newest.CreatedAt) {
				newest = r.manifest
			}
		}
		if newest == nil {
			t.Fatal("no incremental manifest written")
		}
		return newest
	}

	// First incremental: chained off the full.
	incr1 := runIncr(t, parent.BackupID, `{"slot":"sluice_slot","lsn":"0/110"}`,
		time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC))
	if len(incr1.ChangeChunks) != 1 {
		t.Fatalf("incr1 ChangeChunks = %d; want 1", len(incr1.ChangeChunks))
	}
	// Second incremental: chained off the first (so both manifests are
	// linked into a single chain, mirroring the bug repro).
	incr2 := runIncr(t, incr1.BackupID, `{"slot":"sluice_slot","lsn":"0/120"}`,
		time.Date(2026, 5, 8, 11, 0, 1, 0, time.UTC)) // 1 second later → distinct UnixMilli
	if len(incr2.ChangeChunks) != 1 {
		t.Fatalf("incr2 ChangeChunks = %d; want 1", len(incr2.ChangeChunks))
	}

	path1 := incr1.ChangeChunks[0].File
	path2 := incr2.ChangeChunks[0].File
	if path1 == path2 {
		t.Fatalf("change-chunk path collision: incr1 and incr2 both reference %q (Bug 35 regression)", path1)
	}
	if !strings.HasPrefix(path1, changeChunksPrefix) || !strings.HasPrefix(path2, changeChunksPrefix) {
		t.Errorf("paths not under change-chunks prefix: %q, %q", path1, path2)
	}

	// Both files exist on the store and their recorded SHA-256 matches
	// the bytes on disk. Pre-fix, path1 == path2 and the file's content
	// was incr2's, so incr1's recorded SHA-256 mismatched what was on disk.
	for label, info := range map[string]*ir.ChunkInfo{"incr1": incr1.ChangeChunks[0], "incr2": incr2.ChangeChunks[0]} {
		exists, err := store.Exists(context.Background(), info.File)
		if err != nil {
			t.Fatalf("%s: store.Exists(%q): %v", label, info.File, err)
		}
		if !exists {
			t.Fatalf("%s: chunk file %q missing on disk", label, info.File)
		}
		rc, err := store.Get(context.Background(), info.File)
		if err != nil {
			t.Fatalf("%s: store.Get(%q): %v", label, info.File, err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, rc); err != nil {
			_ = rc.Close()
			t.Fatalf("%s: read %q: %v", label, info.File, err)
		}
		_ = rc.Close()
		got := hex.EncodeToString(h.Sum(nil))
		if got != info.SHA256 {
			t.Errorf("%s: chunk %q SHA-256 mismatch: manifest=%s, on-disk=%s",
				label, info.File, info.SHA256, got)
		}
	}
}

// TestChangeChunkPath_RunNamespaceShape pins the per-run namespace
// shape so a future refactor that drops it accidentally re-opens
// Bug 35.
func TestChangeChunkPath_RunNamespaceShape(t *testing.T) {
	m := &ir.Manifest{CreatedAt: time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)}
	ns := changeChunkRunNamespace(m)
	got := changeChunkPath(ns, 0)
	want := "chunks/_changes/" + ns + "/changes-0.jsonl.gz"
	if got != want {
		t.Errorf("changeChunkPath = %q; want %q", got, want)
	}
	// Two manifests with distinct CreatedAt produce distinct namespaces.
	m2 := &ir.Manifest{CreatedAt: m.CreatedAt.Add(1 * time.Millisecond)}
	if changeChunkRunNamespace(m2) == ns {
		t.Errorf("manifests one millisecond apart collide on namespace %q", ns)
	}
}
