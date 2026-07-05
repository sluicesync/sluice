// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the ADR-0086 progress-sidecar checkpoint layout (task #54):
// per-chunk / per-table backup checkpoints append O(1) delta lines to
// `manifest.progress.jsonl` instead of rewriting the whole manifest —
// the #38 scale probe measured the rewrite path at ≈ 2.77e-5·N²
// seconds over N tables (~78 h at 100k). The pins cover:
//
//   - the committer's store-call pattern (deltas appended, exactly two
//     manifest Puts per run) and the legacy fallback on stores without
//     the append capability (byte-identical to the pre-ADR behaviour),
//   - the O(1)-per-event claim at 10k tables via Put/Append BYTE
//     accounting (never wall time),
//   - the crash → reconstruct → resume end-to-end shape on a real
//     LocalStore, including the on-disk base manifest's loud
//     FormatVersion=3 stamp (what makes an OLDER binary refuse the
//     layout instead of resuming off an under-reporting base),
//   - cross-version compatibility both directions: a new binary
//     resumes an old-format in-progress manifest; any binary refuses a
//     manifest stamped newer than it understands.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// accountingPutStore wraps memStore and accounts Put calls + bytes per
// path. It deliberately does NOT implement [irbackup.Appender] — it is
// the legacy-mode store double.
type accountingPutStore struct {
	*memStore

	putCalls map[string]int
	putBytes map[string]int64
}

func newAccountingPutStore() *accountingPutStore {
	return &accountingPutStore{
		memStore: newMemStore(),
		putCalls: map[string]int{},
		putBytes: map[string]int64{},
	}
}

func (s *accountingPutStore) Put(ctx context.Context, path string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.putCalls[path]++
	s.putBytes[path] += int64(len(b))
	return s.memStore.Put(ctx, path, bytes.NewReader(b))
}

// accountingAppendStore adds the [irbackup.Appender] capability with
// its own call/byte accounting — the sidecar-mode store double.
type accountingAppendStore struct {
	*accountingPutStore

	appendCalls    int
	appendBytes    int64
	maxAppendBytes int64
}

func newAccountingAppendStore() *accountingAppendStore {
	return &accountingAppendStore{accountingPutStore: newAccountingPutStore()}
}

func (s *accountingAppendStore) Append(_ context.Context, path string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.appendCalls++
	s.appendBytes += int64(len(b))
	if int64(len(b)) > s.maxAppendBytes {
		s.maxAppendBytes = int64(len(b))
	}
	s.data[path] = append(s.data[path], b...)
	return nil
}

// sidecarTestManifest builds an in-progress manifest over n staged
// tables (schema attached — the heavy immutable part whose repeated
// re-marshal was the O(N²) term).
func sidecarTestManifest(n int) (*irbackup.Manifest, []*irbackup.TableManifest) {
	schema := &ir.Schema{Tables: make([]*ir.Table, 0, n)}
	entries := make([]*irbackup.TableManifest, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("t%05d", i)
		schema.Tables = append(schema.Tables, &ir.Table{
			Name: name,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "payload", Type: ir.Varchar{Length: 255}, Nullable: true},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		})
		entries = append(entries, &irbackup.TableManifest{Name: name, Partial: true})
	}
	m := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionFor(schema),
		SourceEngine:  "postgres",
		Schema:        schema,
		PartialState:  irbackup.BackupStateInProgress,
		Kind:          irbackup.BackupKindFull,
		Tables:        make([]*irbackup.TableManifest, 0, n),
	}
	return m, entries
}

func TestManifestCommitter_SidecarMode_CheckpointsAppendDeltas(t *testing.T) {
	ctx := context.Background()
	store := newAccountingAppendStore()
	// Pre-seed a stale sidecar (a previous attempt's debris) so the
	// commitBase reset is observable.
	store.data[ManifestProgressFileName] = []byte(`{"attempt_id":"stale","event":"table_complete","table":"users","row_count":9}` + "\n")

	m, entries := sidecarTestManifest(2)
	committer, err := newManifestCommitter(store, m)
	if err != nil {
		t.Fatalf("newManifestCommitter: %v", err)
	}
	// The constructor switches the manifest into the sidecar layout:
	// the loud version stamp is what makes an older binary refuse it.
	if m.FormatVersion != irbackup.FormatVersionProgressSidecar {
		t.Errorf("in-progress FormatVersion = %d; want %d (older binaries must refuse the sidecar layout)", m.FormatVersion, irbackup.FormatVersionProgressSidecar)
	}
	if m.ProgressSidecar == nil || m.ProgressSidecar.File != ManifestProgressFileName || m.ProgressSidecar.AttemptID == "" {
		t.Fatalf("ProgressSidecar = %+v; want a populated reference with an attempt id", m.ProgressSidecar)
	}

	for _, e := range entries {
		committer.stageTable(e)
	}
	if err := committer.commitBase(ctx); err != nil {
		t.Fatalf("commitBase: %v", err)
	}
	if got := store.putCalls[lineage.ManifestFileName]; got != 1 {
		t.Errorf("manifest Puts after commitBase = %d; want 1", got)
	}
	if _, ok := store.data[ManifestProgressFileName]; ok {
		t.Error("stale sidecar survived commitBase; want it reset (deleted)")
	}

	// Checkpoints: chunk + completion per table — appended deltas, ZERO
	// additional manifest Puts.
	for i, e := range entries {
		ci := &irbackup.ChunkInfo{File: fmt.Sprintf("chunks/%s/%s-0.jsonl.gz", e.Name, e.Name), RowCount: 1, SHA256: "ab"}
		if err := committer.appendChunk(ctx, e, ci); err != nil {
			t.Fatalf("appendChunk[%d]: %v", i, err)
		}
		if err := committer.finishTable(ctx, e, 1); err != nil {
			t.Fatalf("finishTable[%d]: %v", i, err)
		}
	}
	if got := store.putCalls[lineage.ManifestFileName]; got != 1 {
		t.Errorf("manifest Puts after checkpoints = %d; want still 1 (checkpoints must append, not rewrite)", got)
	}
	if store.appendCalls != 4 {
		t.Errorf("sidecar appends = %d; want 4 (2 chunks + 2 completions)", store.appendCalls)
	}

	// Finalize: one more manifest Put, schema-appropriate version
	// restored, reference cleared, sidecar deleted.
	m.PartialState = irbackup.BackupStateComplete
	if err := committer.finalize(ctx); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if got := store.putCalls[lineage.ManifestFileName]; got != 2 {
		t.Errorf("manifest Puts after finalize = %d; want 2 (base + final)", got)
	}
	if m.FormatVersion != irbackup.FormatVersionFor(m.Schema) {
		t.Errorf("final FormatVersion = %d; want the schema's own %d (finalized manifests stay readable by older binaries)", m.FormatVersion, irbackup.FormatVersionFor(m.Schema))
	}
	if m.ProgressSidecar != nil {
		t.Errorf("final ProgressSidecar = %+v; want nil", m.ProgressSidecar)
	}
	if _, ok := store.data[ManifestProgressFileName]; ok {
		t.Error("sidecar survived finalize; want it deleted")
	}
	// The replay path itself must agree the events landed: decode the
	// final on-disk manifest and check both tables are complete.
	final, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	for _, e := range final.Tables {
		if e.Partial || len(e.Chunks) != 1 {
			t.Errorf("final entry %q = %+v; want complete with 1 chunk", e.Name, e)
		}
	}
}

func TestManifestCommitter_LegacyMode_KeepsFullRewriteCheckpoints(t *testing.T) {
	ctx := context.Background()
	store := newAccountingPutStore() // no Appender — the BlobStore shape

	m, entries := sidecarTestManifest(2)
	wantVersion := m.FormatVersion
	committer, err := newManifestCommitter(store, m)
	if err != nil {
		t.Fatalf("newManifestCommitter: %v", err)
	}
	// Legacy mode leaves the manifest untouched: no sidecar reference,
	// no version juggling — byte-identical to the pre-ADR-0086 layout.
	if m.FormatVersion != wantVersion || m.ProgressSidecar != nil {
		t.Fatalf("legacy mode mutated the manifest: version=%d sidecar=%+v", m.FormatVersion, m.ProgressSidecar)
	}

	for _, e := range entries {
		committer.stageTable(e)
	}
	if err := committer.commitBase(ctx); err != nil {
		t.Fatalf("commitBase: %v", err)
	}
	for i, e := range entries {
		ci := &irbackup.ChunkInfo{File: fmt.Sprintf("chunks/%s/%s-0.jsonl.gz", e.Name, e.Name), RowCount: 1, SHA256: "ab"}
		if err := committer.appendChunk(ctx, e, ci); err != nil {
			t.Fatalf("appendChunk[%d]: %v", i, err)
		}
		if err := committer.finishTable(ctx, e, 1); err != nil {
			t.Fatalf("finishTable[%d]: %v", i, err)
		}
	}
	m.PartialState = irbackup.BackupStateComplete
	if err := committer.finalize(ctx); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	// base + 4 checkpoints + final = 6 full-manifest Puts — the legacy
	// per-checkpoint rewrite, preserved exactly.
	if got := store.putCalls[lineage.ManifestFileName]; got != 6 {
		t.Errorf("legacy manifest Puts = %d; want 6 (per-checkpoint full rewrites preserved on append-less stores)", got)
	}
	if _, ok := store.data[ManifestProgressFileName]; ok {
		t.Error("legacy mode wrote a sidecar; it must not")
	}
}

// TestManifestCommitter_SidecarCheckpointCost_10kTables is the task-#54
// scale demonstration: at 10k tables (20k checkpoint events) the
// manifest is marshaled exactly twice (base + final) and every
// checkpoint appends one bounded delta line — asserted via Put/Append
// BYTE accounting, never wall time. Under the pre-fix layout the same
// run would Put the full schema-bearing manifest 20k times (the
// measured ≈ 2.77e-5·N² wall at the probe's scale).
func TestManifestCommitter_SidecarCheckpointCost_10kTables(t *testing.T) {
	const n = 10_000
	ctx := context.Background()
	store := newAccountingAppendStore()

	m, entries := sidecarTestManifest(n)
	committer, err := newManifestCommitter(store, m)
	if err != nil {
		t.Fatalf("newManifestCommitter: %v", err)
	}
	for _, e := range entries {
		committer.stageTable(e)
	}
	if err := committer.commitBase(ctx); err != nil {
		t.Fatalf("commitBase: %v", err)
	}
	baseManifestBytes := store.putBytes[lineage.ManifestFileName]

	for i, e := range entries {
		ci := &irbackup.ChunkInfo{File: fmt.Sprintf("chunks/%s/%s-0.jsonl.gz", e.Name, e.Name), RowCount: 100, SHA256: strings.Repeat("a", 64)}
		if err := committer.appendChunk(ctx, e, ci); err != nil {
			t.Fatalf("appendChunk[%d]: %v", i, err)
		}
		if err := committer.finishTable(ctx, e, 100); err != nil {
			t.Fatalf("finishTable[%d]: %v", i, err)
		}
	}
	if got := store.putCalls[lineage.ManifestFileName]; got != 1 {
		t.Fatalf("manifest Puts during 20k checkpoints = %d; want 1 — a value > 1 means a checkpoint re-marshaled the manifest (the O(N²) regression)", got)
	}
	if store.putBytes[lineage.ManifestFileName] != baseManifestBytes {
		t.Fatalf("manifest byte volume grew during checkpoints (%d → %d bytes); checkpoints must not rewrite it", baseManifestBytes, store.putBytes[lineage.ManifestFileName])
	}
	if store.appendCalls != 2*n {
		t.Fatalf("appends = %d; want %d (one per event)", store.appendCalls, 2*n)
	}
	// O(1)-per-event bound: every delta line is a few hundred bytes —
	// independent of N. 512 B is generous headroom over the observed
	// ~230 B chunk event; a delta that scales with N would blow through
	// it immediately (the base manifest alone is megabytes here).
	const maxEventBytes = 512
	if store.maxAppendBytes > maxEventBytes {
		t.Errorf("largest checkpoint delta = %d bytes; want ≤ %d (per-event cost must not scale with table count)", store.maxAppendBytes, maxEventBytes)
	}

	m.PartialState = irbackup.BackupStateComplete
	if err := committer.finalize(ctx); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if got := store.putCalls[lineage.ManifestFileName]; got != 2 {
		t.Errorf("total manifest Puts = %d; want exactly 2 (base + final)", got)
	}

	// The reconstructed final manifest must carry all 10k tables
	// complete — O(1) checkpoints must not trade away correctness.
	final, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	for _, e := range final.Tables {
		if e.Partial || len(e.Chunks) != 1 || e.RowCount != 100 {
			t.Fatalf("final entry %q = %+v; want complete with 1 chunk / 100 rows", e.Name, e)
		}
	}
}

// TestManifestCommitter_SidecarMode_ConcurrentCheckpoints is the
// sidecar-mode twin of
// [TestManifestCommitter_ConcurrentCheckpointsKeepSchemaOrder]: pool
// workers checkpoint concurrently through the committer's mutex, every
// event lands as a whole sidecar line, and the replay reconstructs
// every table complete with all its chunks in per-table write order.
// -race in CI is the load-bearing leg of this pin (the append path is
// new shared-state-adjacent code).
func TestManifestCommitter_SidecarMode_ConcurrentCheckpoints(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	const nTables, nChunks = 8, 5
	m, _ := sidecarTestManifest(0)
	committer, err := newManifestCommitter(store, m)
	if err != nil {
		t.Fatalf("newManifestCommitter: %v", err)
	}
	entries := make([]*irbackup.TableManifest, nTables)
	for i := range entries {
		entries[i] = &irbackup.TableManifest{Name: fmt.Sprintf("table-%02d", i), Partial: true}
		committer.stageTable(entries[i])
	}
	if err := committer.commitBase(ctx); err != nil {
		t.Fatalf("commitBase: %v", err)
	}

	var wg sync.WaitGroup
	for _, entry := range entries {
		wg.Add(1)
		go func(entry *irbackup.TableManifest) {
			defer wg.Done()
			for c := 0; c < nChunks; c++ {
				ci := &irbackup.ChunkInfo{
					File:     fmt.Sprintf("chunks/%s/%s-%d.jsonl.gz", entry.Name, entry.Name, c),
					RowCount: 1,
					SHA256:   "deadbeef",
				}
				if err := committer.appendChunk(ctx, entry, ci); err != nil {
					t.Errorf("appendChunk(%s, %d): %v", entry.Name, c, err)
					return
				}
			}
			if err := committer.finishTable(ctx, entry, nChunks); err != nil {
				t.Errorf("finishTable(%s): %v", entry.Name, err)
			}
		}(entry)
	}
	wg.Wait()

	// Reconstruct via the production read path: every table complete,
	// chunks in per-table write order, staged (schema) order preserved.
	got, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if len(got.Tables) != nTables {
		t.Fatalf("reconstructed tables = %d; want %d", len(got.Tables), nTables)
	}
	for i, e := range got.Tables {
		if want := fmt.Sprintf("table-%02d", i); e.Name != want {
			t.Errorf("reconstructed table[%d] = %q; want %q (staged order must survive concurrent completion order)", i, e.Name, want)
		}
		if e.Partial || e.RowCount != nChunks || len(e.Chunks) != nChunks {
			t.Errorf("reconstructed entry %q = %+v; want complete with %d chunks", e.Name, e, nChunks)
			continue
		}
		for c, ci := range e.Chunks {
			if want := fmt.Sprintf("chunks/%s/%s-%d.jsonl.gz", e.Name, e.Name, c); ci.File != want {
				t.Errorf("entry %q chunk[%d] = %q; want %q (per-table write order)", e.Name, c, ci.File, want)
			}
		}
	}
}

// TestBackup_SidecarCrashResume_EndToEnd pins the crash → reconstruct
// → resume shape on a real LocalStore: the RAW on-disk base manifest
// under-reports progress by design (and is stamped FormatVersion=3 so
// an older binary refuses it loudly instead of resuming off it);
// lineage.ReadManifest reconstructs the truth from the sidecar; the resume
// keeps the completed table and finishes; the finalized layout is the
// pre-ADR-0086 contract (schema-appropriate version, no sidecar).
func TestBackup_SidecarCrashResume_EndToEnd(t *testing.T) {
	ctx := context.Background()
	inner, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Name: "posts", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
		"posts": {{"id": int64(10)}},
	}

	// Crash on the posts chunk Put: users completed (its checkpoints
	// live ONLY in the sidecar), posts never started.
	b1 := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src", Store: newFailOnNthPutStore(inner, 3), ChunkRows: 100,
	}
	if err := b1.Run(ctx); err == nil {
		t.Fatal("first Run: expected injected failure; got nil")
	}

	// RAW base manifest (decoded without replay): the sidecar layout's
	// on-disk truth split.
	raw := func() *irbackup.Manifest {
		rc, err := inner.Get(ctx, lineage.ManifestFileName)
		if err != nil {
			t.Fatalf("Get raw manifest: %v", err)
		}
		defer func() { _ = rc.Close() }()
		body, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("read raw manifest: %v", err)
		}
		var m irbackup.Manifest
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatalf("decode raw manifest: %v", err)
		}
		return &m
	}()
	if raw.FormatVersion != irbackup.FormatVersionProgressSidecar {
		t.Errorf("raw base FormatVersion = %d; want %d — without this stamp an older binary would resume off a base that under-reports progress", raw.FormatVersion, irbackup.FormatVersionProgressSidecar)
	}
	if raw.ProgressSidecar == nil || raw.ProgressSidecar.File != ManifestProgressFileName {
		t.Fatalf("raw base ProgressSidecar = %+v; want a reference to %q", raw.ProgressSidecar, ManifestProgressFileName)
	}
	if !raw.Tables[0].Partial || len(raw.Tables[0].Chunks) != 0 {
		t.Errorf("raw base users entry = %+v; want the pre-staged Partial=true zero-chunk shape (progress lives in the sidecar)", raw.Tables[0])
	}
	if exists, _ := inner.Exists(ctx, ManifestProgressFileName); !exists {
		t.Fatal("progress sidecar missing after crash; the users checkpoints would be lost")
	}

	// lineage.ReadManifest reconstructs: users complete, posts still staged.
	m1, err := lineage.ReadManifest(ctx, inner)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if m1.Tables[0].Partial || len(m1.Tables[0].Chunks) != 1 || m1.Tables[0].RowCount != 2 {
		t.Errorf("reconstructed users entry = %+v; want complete with 1 chunk / 2 rows", m1.Tables[0])
	}
	if !m1.Tables[1].Partial {
		t.Errorf("reconstructed posts entry = %+v; want still partial", m1.Tables[1])
	}
	// The decoded view is normalized: replay is not idempotent, so the
	// in-memory manifest must never reference the sidecar it absorbed.
	if m1.ProgressSidecar != nil || m1.FormatVersion != irbackup.FormatVersionFor(m1.Schema) {
		t.Errorf("decoded manifest not normalized: version=%d sidecar=%+v", m1.FormatVersion, m1.ProgressSidecar)
	}

	// Resume: keeps users (no re-stream Put), finishes posts.
	b2 := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src", Store: inner, ChunkRows: 100,
	}
	if err := b2.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	final, err := lineage.ReadManifest(ctx, inner)
	if err != nil {
		t.Fatalf("lineage.ReadManifest final: %v", err)
	}
	if final.PartialState != irbackup.BackupStateComplete {
		t.Errorf("final PartialState = %q; want complete", final.PartialState)
	}
	if final.FormatVersion != irbackup.FormatVersionLegacy {
		t.Errorf("final FormatVersion = %d; want %d (innocent schema — finalized backups stay readable by older binaries)", final.FormatVersion, irbackup.FormatVersionLegacy)
	}
	if final.ProgressSidecar != nil {
		t.Errorf("final ProgressSidecar = %+v; want nil", final.ProgressSidecar)
	}
	if exists, _ := inner.Exists(ctx, ManifestProgressFileName); exists {
		t.Error("progress sidecar survived the final manifest write; want it deleted")
	}
	if m1.Tables[0].Chunks[0].SHA256 != final.Tables[0].Chunks[0].SHA256 {
		t.Errorf("users chunk SHA changed across resume: %s → %s (the kept table must ride the crashed run's chunk)", m1.Tables[0].Chunks[0].SHA256, final.Tables[0].Chunks[0].SHA256)
	}
	total, mismatches, err := VerifyBackup(ctx, inner)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 || total != 2 {
		t.Errorf("VerifyBackup = %d mismatches over %d chunks; want 0 over 2", mismatches, total)
	}
}

// TestBackup_ResumesOldFormatInProgressManifest pins new-reads-old: an
// in-progress manifest written by a pre-ADR-0086 binary (legacy
// version, no sidecar reference) resumes exactly as before.
func TestBackup_ResumesOldFormatInProgressManifest(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}},
	}
	prior := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionLegacy,
		SourceEngine:  "postgres",
		Schema:        schema,
		PartialState:  irbackup.BackupStateInProgress,
		Tables:        []*irbackup.TableManifest{{Name: "t", Partial: true}},
	}
	if err := lineage.WriteManifest(ctx, store, prior); err != nil {
		t.Fatalf("lineage.WriteManifest: %v", err)
	}

	b := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{"t": {{"id": int64(1)}}}),
		SourceDSN: "src", Store: store,
	}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("Run over old-format in-progress manifest: %v", err)
	}
	final, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if final.PartialState != irbackup.BackupStateComplete || len(final.Tables) != 1 || final.Tables[0].RowCount != 1 {
		t.Errorf("final manifest = %+v; want one completed table with 1 row", final)
	}
}

// TestReadManifest_RefusesNewerFormatVersion pins the loud downgrade
// sentinel an OLDER binary relies on: a manifest stamped newer than
// the build's ceiling is refused before any of its content is acted
// on. This is exactly how a pre-ADR-0086 binary reacts to the
// FormatVersion=3 in-progress base (its ceiling is 2).
func TestReadManifest_RefusesNewerFormatVersion(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	body, err := json.Marshal(&irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion + 1,
		SourceEngine:  "postgres",
		PartialState:  irbackup.BackupStateInProgress,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.Put(ctx, lineage.ManifestFileName, bytes.NewReader(body)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, err = lineage.ReadManifest(ctx, store)
	if err == nil {
		t.Fatal("lineage.ReadManifest accepted a newer-format manifest; want loud refusal")
	}
	if !strings.Contains(err.Error(), "newer than this build supports") || !strings.Contains(err.Error(), "upgrade sluice") {
		t.Errorf("refusal %q should name the version gap and the remedy", err)
	}
}
