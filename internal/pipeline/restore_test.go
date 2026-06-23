// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// restoreRecorderEngine is a fake [ir.Engine] for restore tests: a
// schema writer that records phase calls and a row writer that
// captures all written rows by table.
type restoreRecorderEngine struct {
	name string
	mu   sync.Mutex

	// Schema-write calls in order — for asserting phase ordering.
	phases []string
	// Per-table rows recorded by the row writer.
	rows map[string][]ir.Row
	// growGateSets counts SetGrowGate calls with a NON-nil gate across all
	// row writers this engine handed out — pins the ADR-0110 grow-gate
	// wiring into the restore path (the Track-C silent-under-copy fix).
	growGateSets int
}

func newRestoreRecorderEngine(name string) *restoreRecorderEngine {
	return &restoreRecorderEngine{name: name, rows: map[string][]ir.Row{}}
}

func (e *restoreRecorderEngine) Name() string                  { return e.name }
func (e *restoreRecorderEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *restoreRecorderEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return nil, errors.New("restoreRecorderEngine: read side not used")
}

func (e *restoreRecorderEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return &restoreRecordingSchemaWriter{engine: e}, nil
}

func (e *restoreRecorderEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return nil, errors.New("restoreRecorderEngine: read side not used")
}

func (e *restoreRecorderEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return &restoreRecordingRowWriter{engine: e}, nil
}

func (*restoreRecorderEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (*restoreRecorderEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (*restoreRecorderEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

func (e *restoreRecorderEngine) recordPhase(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.phases = append(e.phases, name)
}

func (e *restoreRecorderEngine) recordRow(table string, row ir.Row) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rows[table] = append(e.rows[table], row)
}

func (e *restoreRecorderEngine) snapshot() (phases []string, rows map[string][]ir.Row) {
	e.mu.Lock()
	defer e.mu.Unlock()
	phases = append(phases, e.phases...)
	rows = make(map[string][]ir.Row, len(e.rows))
	for k, v := range e.rows {
		rows[k] = append(rows[k], v...)
	}
	return phases, rows
}

type restoreRecordingSchemaWriter struct {
	engine *restoreRecorderEngine
}

func (w *restoreRecordingSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateTablesWithoutConstraints")
	return nil
}

func (w *restoreRecordingSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateIndexes")
	return nil
}

func (w *restoreRecordingSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateConstraints")
	return nil
}

func (w *restoreRecordingSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	w.engine.recordPhase("SyncIdentitySequences")
	return nil
}

func (w *restoreRecordingSchemaWriter) CreateViews(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateViews")
	return nil
}

type restoreRecordingRowWriter struct {
	engine *restoreRecorderEngine
}

func (w *restoreRecordingRowWriter) WriteRows(_ context.Context, table *ir.Table, rows <-chan ir.Row) error {
	for r := range rows {
		w.engine.recordRow(table.Name, r)
	}
	w.engine.recordPhase("WriteRows:" + table.Name)
	return nil
}

// SetGrowGate implements [ir.GrowGateSetter] so the recorder can pin that
// restore wires the ADR-0110 coordinated grow-gate onto every writer it
// opens. A non-nil gate increments the engine's counter.
func (w *restoreRecordingRowWriter) SetGrowGate(gate ir.GrowGate) {
	if gate == nil {
		return
	}
	w.engine.mu.Lock()
	defer w.engine.mu.Unlock()
	w.engine.growGateSets++
}

func TestRestore_Validate(t *testing.T) {
	cases := []struct {
		name string
		r    *Restore
		want string
	}{
		{"nil target", &Restore{TargetDSN: "x", Store: &LocalStore{}}, "Target engine is nil"},
		{"empty DSN", &Restore{Target: stubEngine{}, Store: &LocalStore{}}, "TargetDSN is empty"},
		{"nil store", &Restore{Target: stubEngine{}, TargetDSN: "x"}, "Store is nil"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.r.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want containing %q", err, c.want)
			}
		})
	}
}

// TestBackupRestore_FullRoundTrip is the load-bearing end-to-end
// test for Phase 1: backup a populated source schema, restore into
// a recording target, and verify (a) phase ordering and (b) every
// row arrives at the target with correct values.
func TestBackupRestore_FullRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
					{Name: "name", Type: ir.Varchar{Length: 100}},
					{Name: "active", Type: ir.Boolean{}},
				},
			},
			{
				Name: "events",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "payload", Type: ir.JSON{Binary: true}},
				},
			},
		},
	}
	rows := map[string][]ir.Row{
		"users": {
			{"id": int64(1), "name": "Alice", "active": true},
			{"id": int64(2), "name": "Bob", "active": false},
		},
		"events": {
			{"id": int64(101), "payload": `{"type":"signup"}`},
		},
	}

	// Backup phase.
	src := newBackupRecorderEngine("postgres", schema, rows)
	b := &Backup{Source: src, SourceDSN: "src", Store: store, ChunkRows: 10}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// Restore phase.
	tgt := newRestoreRecorderEngine("postgres") // same engine; cross-engine covered by separate test
	r := &Restore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	phases, gotRows := tgt.snapshot()

	// Phase ordering: CreateTablesWithoutConstraints must come before
	// any WriteRows; SyncIdentitySequences / CreateIndexes /
	// CreateConstraints must come after.
	wantOrder := []string{
		"CreateTablesWithoutConstraints",
		// WriteRows in some order interleave here.
		"SyncIdentitySequences",
		"CreateIndexes",
		"CreateConstraints",
	}
	// Find positions of each guard phase and assert ordering.
	pos := make(map[string]int, len(wantOrder))
	for i, p := range phases {
		pos[p] = i
	}
	for i := 0; i < len(wantOrder)-1; i++ {
		if pos[wantOrder[i]] >= pos[wantOrder[i+1]] {
			t.Errorf("phase %q at %d should precede %q at %d (phases=%v)",
				wantOrder[i], pos[wantOrder[i]], wantOrder[i+1], pos[wantOrder[i+1]], phases)
		}
	}

	// Verify rows arrived. Compare via valuesEquivalent so int64/int
	// kind drift after JSON round-trip doesn't fail spuriously.
	if len(gotRows["users"]) != 2 {
		t.Fatalf("users rows: got %d want 2", len(gotRows["users"]))
	}
	if len(gotRows["events"]) != 1 {
		t.Fatalf("events rows: got %d want 1", len(gotRows["events"]))
	}
	for i, want := range rows["users"] {
		got := gotRows["users"][i]
		for k, wantV := range want {
			if !valuesEquivalent(got[k], wantV) {
				t.Errorf("users[%d].%s: got %v want %v", i, k, got[k], wantV)
			}
		}
	}
}

// TestRestore_WiresGrowGate pins the ADR-0110 grow-gate wiring into the
// restore path (the Track-C silent-under-copy fix): every writer the
// restore opens must receive a NON-nil coordinated grow-gate so concurrent
// restore workers quiesce together through a storage-grow reparent instead
// of independently hammering the target and outrunning its replication.
// The bug was that restore — unlike the migrate cold-copy — never wired the
// gate, so SetGrowGate was never called.
func TestRestore_WiresGrowGate(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	rows := map[string][]ir.Row{"t": {{"id": int64(1)}, {"id": int64(2)}}}

	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store, ChunkRows: 10}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	tgt := newRestoreRecorderEngine("postgres")
	if err := (&Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	tgt.mu.Lock()
	n := tgt.growGateSets
	tgt.mu.Unlock()
	if n < 1 {
		t.Fatalf("restore wired the grow-gate to %d writers; want >=1 (gate never reached the writers — the Track-C silent-loss regression)", n)
	}
}

// TestRestore_HashMismatch_FailsLoudly is the load-bearing
// integrity check: corrupt a chunk file after backup and verify the
// restore phase surfaces ErrChunkHashMismatch (loud-failure tenet).
func TestRestore_HashMismatch_FailsLoudly(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
			},
		}},
	}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
	}
	src := newBackupRecorderEngine("mysql", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// Corrupt the chunk file. Read the chunk's bytes via Get, close
	// the handle (Windows holds an exclusive lock until close), then
	// rewrite via Put.
	manifest, _ := readManifest(context.Background(), store)
	chunkPath := manifest.Tables[0].Chunks[0].File
	original := bytes.Buffer{}
	rc, err := store.Get(context.Background(), chunkPath)
	if err != nil {
		t.Fatalf("Get chunk: %v", err)
	}
	if _, err := original.ReadFrom(rc); err != nil {
		_ = rc.Close()
		t.Fatalf("read chunk: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("close chunk handle: %v", err)
	}
	corrupted := append([]byte{}, original.Bytes()...)
	// Flip a byte in the compressed body region (after the codec
	// header). Codec is DefaultCodec (zstd, v0.67.0) via Backup.Run.
	if len(corrupted) > 30 {
		corrupted[len(corrupted)/2] ^= 0xff
	}
	if err := store.Put(context.Background(), chunkPath, bytes.NewReader(corrupted)); err != nil {
		t.Fatalf("Put corrupted: %v", err)
	}

	// VerifyBackup should report the mismatch.
	total, mismatches, err := VerifyBackup(context.Background(), store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches == 0 {
		t.Errorf("VerifyBackup total=%d mismatches=%d; want at least one mismatch", total, mismatches)
	}

	// Restore should also fail loudly. Wrap target in recorder so
	// Run can attempt the restore.
	tgt := newRestoreRecorderEngine("mysql")
	err = (&Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background())
	if err == nil {
		t.Fatal("Restore.Run on corrupted chunk: nil err; want ErrChunkHashMismatch")
	}
	// Either the codec-decode check or the SHA-256 check should fire,
	// depending on which byte was flipped (zstd magic/frame, gzip
	// header, decode garbage tripping the size guard, or the hash).
	// All acceptable — the load-bearing invariant (already enforced by
	// the nil-check above) is that restore refused to proceed silently;
	// this only classifies the error as corruption-shaped, codec-neutral.
	es := err.Error()
	corruptionShaped := errors.Is(err, ErrChunkHashMismatch) ||
		strings.Contains(es, "gzip") || strings.Contains(es, "zstd") ||
		strings.Contains(es, "magic") || strings.Contains(es, "decode") ||
		strings.Contains(es, "exceeded") || strings.Contains(es, "corrupt") ||
		strings.Contains(es, "checksum") || strings.Contains(es, "row")
	if !corruptionShaped {
		t.Errorf("err = %v; want ErrChunkHashMismatch or a clear corruption error", err)
	}
}

// TestRestore_CrossEngine_RetargetsTypes verifies the load-bearing
// claim that a PG-source backup can restore into a MySQL target with
// PG-native types (UUID, Inet, Array) rewritten to their MySQL-storage
// equivalents via translate.RetargetForEngine. The recording target
// captures the schema-write phase's input; we assert the IR types
// arrive in their retargeted shape.
func TestRestore_CrossEngine_RetargetsTypes(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.UUID{}},
				{Name: "ip", Type: ir.Inet{}},
				{Name: "tags", Type: ir.Array{Element: ir.Varchar{Length: 50}}},
			},
		}},
	}
	rows := map[string][]ir.Row{
		"users": {{
			"id":   "11111111-2222-3333-4444-555555555555",
			"ip":   "127.0.0.1",
			"tags": []string{"a", "b"},
		}},
	}
	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Capture the schema the writer sees by wrapping the recorder.
	tgt := &capturingTargetEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	if err := (&Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if tgt.lastSchema == nil {
		t.Fatal("schema writer never received a schema")
	}
	cols := tgt.lastSchema.Tables[0].Columns
	wantTypes := map[string]string{
		"id":   "Char(36)",     // UUID → Char(36) per RetargetForEngine PG→MySQL
		"ip":   "Varchar(45)",  // Inet → Varchar(45)
		"tags": "JSON[binary]", // Array → JSON binary
	}
	for _, c := range cols {
		want := wantTypes[c.Name]
		if c.Type.String() != want {
			t.Errorf("col %q type = %q; want %q (cross-engine retarget)", c.Name, c.Type.String(), want)
		}
	}
}

// TestRestore_CrossEngine_SingleManifest_RefusesUnsupportable pins
// the Bug 134 fix: the SINGLE-MANIFEST restore branch (Restore.Run,
// no incrementals) must run the same checkCrossEngineSupportable gate
// the chain path has had since Phase 5. Pre-fix, a full-only PG
// backup carrying an EXCLUDE constraint restored to a MySQL-family
// target with exit 0 and the constraint silently downgraded to a
// plain non-unique KEY — found by the v0.99.32 regression cycle, the
// instance one branch over from the v0.99.32 chain-path fix.
//
// Matrix: {mysql, vitess, planetscale} targets refuse identically
// (the family is "MySQL-family target", per the isMySQLFamilyEngine
// set the vitess gap was about); PG→PG passes (same-engine); a clean
// schema passes the gate cross-engine. The per-CONSTRUCT family
// coverage (opclasses, GIN, PostGIS, …) lives on the gate's own
// tests — this pins the CALL, which dispatches uniformly.
func TestRestore_CrossEngine_SingleManifest_RefusesUnsupportable(t *testing.T) {
	excludeSchema := &ir.Schema{
		Tables: []*ir.Table{{
			Name: "bookings",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
			},
			ExcludeConstraints: []*ir.ExcludeConstraint{{
				Name:       "bookings_id_excl",
				Definition: "EXCLUDE USING btree (id WITH =)",
			}},
		}},
	}
	rows := map[string][]ir.Row{"bookings": {{"id": int64(1)}}}

	for _, target := range []string{"mysql", "vitess", "planetscale"} {
		t.Run("refuses_"+target, func(t *testing.T) {
			dir := t.TempDir()
			store, _ := NewLocalStore(dir)
			src := newBackupRecorderEngine("postgres", excludeSchema, rows)
			if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
				t.Fatalf("Backup: %v", err)
			}
			tgt := newRestoreRecorderEngine(target)
			err := (&Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background())
			if err == nil {
				t.Fatalf("restore to %s with EXCLUDE constraint succeeded; want loud refusal (Bug 134)", target)
			}
			if !strings.Contains(err.Error(), "bookings_id_excl") {
				t.Errorf("refusal should name the constraint; got: %v", err)
			}
			phases, _ := tgt.snapshot()
			if len(phases) != 0 {
				t.Errorf("gate must fire before any target write; schema-write phases ran: %v", phases)
			}
		})
	}

	t.Run("pg_to_pg_passes", func(t *testing.T) {
		dir := t.TempDir()
		store, _ := NewLocalStore(dir)
		src := newBackupRecorderEngine("postgres", excludeSchema, rows)
		if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
			t.Fatalf("Backup: %v", err)
		}
		tgt := newRestoreRecorderEngine("postgres")
		if err := (&Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background()); err != nil {
			t.Fatalf("same-engine restore must pass the gate: %v", err)
		}
	})

	t.Run("clean_schema_passes_cross_engine", func(t *testing.T) {
		dir := t.TempDir()
		store, _ := NewLocalStore(dir)
		clean := &ir.Schema{Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}}}
		src := newBackupRecorderEngine("postgres", clean, map[string][]ir.Row{"users": {{"id": int64(1)}}})
		if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
			t.Fatalf("Backup: %v", err)
		}
		tgt := newRestoreRecorderEngine("mysql")
		if err := (&Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background()); err != nil {
			t.Fatalf("clean cross-engine restore must pass the gate: %v", err)
		}
	})
}

// capturingTargetEngine wraps restoreRecorderEngine to capture the
// schema sent through CreateTablesWithoutConstraints — needed to
// assert the cross-engine type retarget reached the writer.
type capturingTargetEngine struct {
	*restoreRecorderEngine
	lastSchema *ir.Schema
}

func (e *capturingTargetEngine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	inner, err := e.restoreRecorderEngine.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &capturingSchemaWriter{inner: inner, engine: e}, nil
}

type capturingSchemaWriter struct {
	inner  ir.SchemaWriter
	engine *capturingTargetEngine
}

func (w *capturingSchemaWriter) CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error {
	w.engine.lastSchema = s
	return w.inner.CreateTablesWithoutConstraints(ctx, s)
}

func (w *capturingSchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	return w.inner.CreateIndexes(ctx, s)
}

func (w *capturingSchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	return w.inner.CreateConstraints(ctx, s)
}

func (w *capturingSchemaWriter) SyncIdentitySequences(ctx context.Context, s *ir.Schema) error {
	return w.inner.SyncIdentitySequences(ctx, s)
}

func (w *capturingSchemaWriter) CreateViews(ctx context.Context, s *ir.Schema) error {
	return w.inner.CreateViews(ctx, s)
}

// TestVerifyBackup_DetectsMissingChunk also exercises VerifyBackup's
// loud-failure path.
func TestVerifyBackup_DetectsMissingChunk(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "x", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}},
	}
	rows := map[string][]ir.Row{"x": {{"id": int64(1)}}}
	src := newBackupRecorderEngine("mysql", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	manifest, _ := readManifest(context.Background(), store)
	if err := store.Delete(context.Background(), manifest.Tables[0].Chunks[0].File); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	total, mismatches, err := VerifyBackup(context.Background(), store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if total != 1 || mismatches != 1 {
		t.Errorf("VerifyBackup total/mismatches = %d/%d; want 1/1", total, mismatches)
	}
}
