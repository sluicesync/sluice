// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for resume anchor adoption (task #42, ADR-0085) — the
// silent-chain-gap class: a resumed `backup full` keeps the prior
// attempt's completed tables (exact as-of the PRIOR anchor) but used to
// record the NEW snapshot's anchor as EndPosition, so writes to kept
// tables between the two anchors were in neither the row chunks nor the
// next incremental's window. The pins:
//
//   - the in-progress manifest carries the anchor from its first write,
//   - a resumed run ADOPTS the prior anchor (never the new snapshot's),
//   - a pre-fix prior (no anchor) re-streams everything under a
//     snapshot-anchored run,
//   - an anchored resume refuses keyless re-streams and schema drift,
//   - --chain-slot resume preflights + opens the snapshot WITHOUT
//     PersistChainSlot (adoption, not creation),
//   - --force-overwrite discards an in-progress prior,
//   - resolveParent refuses an in-progress parent.
//
// The end-to-end chain-gap shape (gap writes present exactly once after
// full → incremental → chain restore) is pinned against real Postgres
// in backup_resume_anchor_pg_integration_test.go.

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// keyedTable builds a one-PK-column table descriptor; anchored resumes
// require re-streamed tables to be replay-idempotent, so most tests
// here need keyed tables.
func keyedTable(name string) *ir.Table {
	return &ir.Table{
		Name:       name,
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// keylessTable builds a table with no PK and no unique index — the
// replay-idempotency refusal shape.
func keylessTable(name string) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "v", Type: ir.Integer{Width: 64}, Nullable: true}},
	}
}

// newAnchorTestEngine builds a snapshot-opening engine over two keyed
// tables (t_first swept before t_second; schema order is sweep order on
// the serial pool) whose snapshot reports pos.
func newAnchorTestEngine(schema *ir.Schema, rows map[string][]ir.Row, pos ir.Position) *snapshotOpeningEngine {
	return &snapshotOpeningEngine{
		capturingBackupEngine: &capturingBackupEngine{
			backupRecorderEngine: newBackupRecorderEngine("postgres", schema, rows),
			cdc:                  ir.CDCLogicalReplication,
			reader:               &capturingSchemaReader{schema: schema},
		},
		snapshotPos: pos,
	}
}

var (
	anchorPosA1 = ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/A1"}`}
	anchorPosA2 = ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/A2"}`}
)

// crashAfterFirstTable runs a backup over schema that fails uploading
// the SECOND table's first chunk — the first table completes, the rest
// never start. Put order on the snapshot path (serial sweep; the
// per-chunk / per-table checkpoints are sidecar APPENDS under
// ADR-0086, not Puts):
//
//	1: pre-sweep in-progress base manifest (anchor-stamped)
//	2: first table chunk 0 (+ appended chunk + table-complete events)
//	3: second table chunk 0  ← injected failure
func crashAfterFirstTable(t *testing.T, store *blobcodec.LocalStore, src *snapshotOpeningEngine, chainSlot bool) {
	t.Helper()
	b := &Backup{
		Source: src, SourceDSN: "src", Store: newFailOnNthPutStore(store, 3),
		ChunkRows: 100, ChainSlot: chainSlot,
	}
	if err := b.Run(context.Background()); err == nil {
		t.Fatal("first Run: expected injected failure; got nil")
	}
}

// TestBackup_InProgressManifestCarriesAnchor pins fix step 1: the
// manifest a crashed run leaves behind already carries the snapshot
// anchor, so a resume can adopt it.
func TestBackup_InProgressManifestCarriesAnchor(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), false)

	m, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if m.PartialState != irbackup.BackupStateInProgress {
		t.Fatalf("PartialState = %q; want in_progress", m.PartialState)
	}
	if m.EndPosition != anchorPosA1 {
		t.Errorf("in-progress EndPosition = %+v; want the snapshot anchor %+v (crashed manifests must carry the anchor)", m.EndPosition, anchorPosA1)
	}
}

// TestBackup_ResumeAdoptsPriorAnchor is the core adoption pin: the
// resumed run records the FIRST attempt's anchor — not its own fresh
// snapshot position — and WARN-logs the adoption with both tokens.
func TestBackup_ResumeAdoptsPriorAnchor(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var logBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), false)
	usersSHA := func() string {
		m, err := lineage.ReadManifest(context.Background(), store)
		if err != nil {
			t.Fatalf("lineage.ReadManifest: %v", err)
		}
		return m.Tables[0].Chunks[0].SHA256
	}()

	// Resume with a LATER snapshot anchor — the pre-fix bug recorded
	// this one, gapping kept tables' writes in (A1, A2].
	b2 := &Backup{
		Source:    newAnchorTestEngine(schema, rows, anchorPosA2),
		SourceDSN: "src", Store: store, ChunkRows: 100,
	}
	if err := b2.Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	final, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest final: %v", err)
	}
	if final.PartialState != irbackup.BackupStateComplete {
		t.Errorf("PartialState = %q; want complete", final.PartialState)
	}
	if final.EndPosition != anchorPosA1 {
		t.Errorf("final EndPosition = %+v; want the ADOPTED prior anchor %+v (recording the new snapshot's %+v is the silent chain gap)",
			final.EndPosition, anchorPosA1, anchorPosA2)
	}
	if len(final.Tables) != 2 {
		t.Fatalf("final tables = %d; want 2", len(final.Tables))
	}
	// The kept table's chunk is the first attempt's, untouched.
	if final.Tables[0].Chunks[0].SHA256 != usersSHA {
		t.Errorf("kept table chunk SHA changed across resume: %s → %s", usersSHA, final.Tables[0].Chunks[0].SHA256)
	}
	if final.BackupID == "" {
		t.Error("final BackupID empty; want computed from the adopted anchor")
	}

	logged := logBuf.String()
	if !strings.Contains(logged, "adopted") || !strings.Contains(logged, "0/A1") || !strings.Contains(logged, "0/A2") {
		t.Errorf("adoption WARN must name both position tokens; log=%q", logged)
	}
}

// countingRowReader wraps a RowReader and records which tables were
// actually streamed — distinguishing "kept verbatim" from "re-streamed"
// without relying on Put counts (the content-addressed upload skip
// hides identical re-streams from Put counting).
type countingRowReader struct {
	inner ir.RowReader
	reads map[string]int
}

func (r *countingRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if r.reads == nil {
		r.reads = map[string]int{}
	}
	r.reads[table.Name]++
	return r.inner.ReadRows(ctx, table)
}

func (r *countingRowReader) Err() error { return r.inner.Err() }

// TestBackup_ResumeWithoutPriorAnchorRestreamsEverything pins the
// option-(c) fallback: a pre-fix in-progress manifest (no recorded
// anchor) under a snapshot-anchored run cannot keep ANY table — kept
// chunks would be as-of an unknown earlier anchor while the manifest
// records this run's. Everything re-streams, with a WARN.
func TestBackup_ResumeWithoutPriorAnchorRestreamsEverything(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var logBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), false)

	// Forge the pre-fix shape: strip the anchor from the crashed
	// manifest (pre-fix binaries never stamped it while in progress).
	m, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	m.EndPosition = ir.Position{}
	if err := lineage.WriteManifest(context.Background(), store, m); err != nil {
		t.Fatalf("lineage.WriteManifest (anchor stripped): %v", err)
	}

	src2 := newAnchorTestEngine(schema, rows, anchorPosA2)
	counting := &countingRowReader{inner: &fakeRowReader{rows: rows}}
	src2.useSnapshotRows = true
	src2.snapshotRowsHook = func() ir.RowReader { return counting }
	b2 := &Backup{Source: src2, SourceDSN: "src", Store: store, ChunkRows: 100}
	if err := b2.Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// BOTH tables re-streamed — the completed users table was not kept.
	if counting.reads["users"] != 1 || counting.reads["posts"] != 1 {
		t.Errorf("re-stream reads = %v; want users and posts both read once (no kept tables without a prior anchor)", counting.reads)
	}
	final, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest final: %v", err)
	}
	// Nothing to adopt → this run's anchor is recorded.
	if final.EndPosition != anchorPosA2 {
		t.Errorf("final EndPosition = %+v; want this run's anchor %+v", final.EndPosition, anchorPosA2)
	}
	if !strings.Contains(logBuf.String(), "carries no anchor") {
		t.Errorf("missing the no-anchor re-stream WARN; log=%q", logBuf.String())
	}
}

// TestBackup_AnchoredResumeRefusesKeylessRestream pins the keyless
// guard: a truly keyless table that must be (re-)streamed on an
// anchored resume is refused loudly (its chunks would overlap the
// chain's replay window, and keyless replay is plain INSERT —
// duplicates). A KEPT keyless table is fine: exact at the anchor.
func TestBackup_AnchoredResumeRefusesKeylessRestream(t *testing.T) {
	t.Run("keyless table to re-stream: refused", func(t *testing.T) {
		store, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		// kept_t sweeps first (completes), keyless_t never starts.
		schema := &ir.Schema{Tables: []*ir.Table{keyedTable("kept_t"), keylessTable("keyless_t")}}
		rows := map[string][]ir.Row{
			"kept_t":    {{"id": int64(1)}},
			"keyless_t": {{"v": int64(2)}},
		}
		crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), false)

		b2 := &Backup{
			Source:    newAnchorTestEngine(schema, rows, anchorPosA2),
			SourceDSN: "src", Store: store, ChunkRows: 100,
		}
		err = b2.Run(context.Background())
		if err == nil {
			t.Fatal("resume Run succeeded; want loud keyless-re-stream refusal")
		}
		for _, want := range []string{"keyless_t", "PRIMARY KEY", "--force-overwrite"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q missing %q", err.Error(), want)
			}
		}
	})

	t.Run("keyless table kept verbatim: allowed", func(t *testing.T) {
		store, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		// keyless_t sweeps first and COMPLETES; keyed_t re-streams.
		schema := &ir.Schema{Tables: []*ir.Table{keylessTable("keyless_t"), keyedTable("keyed_t")}}
		rows := map[string][]ir.Row{
			"keyless_t": {{"v": int64(2)}},
			"keyed_t":   {{"id": int64(1)}},
		}
		crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), false)

		b2 := &Backup{
			Source:    newAnchorTestEngine(schema, rows, anchorPosA2),
			SourceDSN: "src", Store: store, ChunkRows: 100,
		}
		if err := b2.Run(context.Background()); err != nil {
			t.Fatalf("resume Run: %v (kept keyless tables are exact at the anchor — must not refuse)", err)
		}
		final, err := lineage.ReadManifest(context.Background(), store)
		if err != nil {
			t.Fatalf("lineage.ReadManifest: %v", err)
		}
		if final.EndPosition != anchorPosA1 {
			t.Errorf("final EndPosition = %+v; want adopted %+v", final.EndPosition, anchorPosA1)
		}
	})
}

// TestBackup_AnchoredResumeRefusesSchemaDrift pins the schema-stability
// guard: DDL between the interrupted attempt and the resume breaks the
// replay claim — refuse loudly rather than pair old-anchor chunks with
// a schema they were not read under.
func TestBackup_AnchoredResumeRefusesSchemaDrift(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), false)

	// The source grew a column between the attempts.
	drifted := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	drifted.Tables[0].Columns = append(drifted.Tables[0].Columns, &ir.Column{Name: "added", Type: ir.Varchar{Length: 10}, Nullable: true})

	b2 := &Backup{
		Source:    newAnchorTestEngine(drifted, rows, anchorPosA2),
		SourceDSN: "src", Store: store, ChunkRows: 100,
	}
	err = b2.Run(context.Background())
	if err == nil {
		t.Fatal("resume Run succeeded under schema drift; want loud refusal")
	}
	for _, want := range []string{"schema changed", "--force-overwrite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
}

// TestBackup_ForceOverwriteDiscardsInProgressPrior pins the escape
// hatch every resume guard names: --force-overwrite discards the
// in-progress prior and starts fresh (recording THIS run's anchor,
// re-streaming everything).
func TestBackup_ForceOverwriteDiscardsInProgressPrior(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), false)

	src2 := newAnchorTestEngine(schema, rows, anchorPosA2)
	counting := &countingRowReader{inner: &fakeRowReader{rows: rows}}
	src2.useSnapshotRows = true
	src2.snapshotRowsHook = func() ir.RowReader { return counting }
	b2 := &Backup{
		Source: src2, SourceDSN: "src", Store: store,
		ChunkRows: 100, ForceOverwrite: true,
	}
	if err := b2.Run(context.Background()); err != nil {
		t.Fatalf("force-overwrite Run: %v", err)
	}
	if counting.reads["users"] != 1 || counting.reads["posts"] != 1 {
		t.Errorf("re-stream reads = %v; want both tables read (fresh start)", counting.reads)
	}
	final, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if final.EndPosition != anchorPosA2 {
		t.Errorf("final EndPosition = %+v; want this fresh run's anchor %+v (nothing adopted)", final.EndPosition, anchorPosA2)
	}
}

// chainResumeBackupEngine adds [irbackup.ChainResumePreflighter] on top of
// the snapshot-opening stub so the --chain-slot resume-adoption
// dispatch is observable.
type chainResumeBackupEngine struct {
	*snapshotOpeningEngine
	preflightErr     error
	preflightCalls   int
	gotPreflightFrom ir.Position
}

func (e *chainResumeBackupEngine) PreflightChainResume(_ context.Context, _ string, from ir.Position) error {
	e.preflightCalls++
	e.gotPreflightFrom = from
	return e.preflightErr
}

// TestBackup_ChainSlotResumeAdoptsSlot pins fix step 6: a --chain-slot
// RESUME is an adoption, not a creation — the orchestrator preflights
// the adopted anchor against the standing chain slot, then opens this
// run's snapshot with PersistChainSlot=FALSE (temporary anchor; the
// adopted slot is never re-created, never committed, and never dropped
// by this run), and records the adopted anchor.
func TestBackup_ChainSlotResumeAdoptsSlot(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	src1 := newAnchorTestEngine(schema, rows, anchorPosA1)
	crashAfterFirstTable(t, store, src1, true)
	if !src1.gotPersistChainSlot {
		t.Fatal("first run opts.PersistChainSlot = false; want true (fresh --chain-slot run creates the slot)")
	}
	// The crashed run had durably written the anchor-stamped manifest,
	// so its snapshot was committed — the slot survives for adoption.
	if src1.commitCalls != 1 {
		t.Fatalf("first run CommitFn calls = %d; want 1 (slot must survive a resumable failure)", src1.commitCalls)
	}

	src2 := &chainResumeBackupEngine{snapshotOpeningEngine: newAnchorTestEngine(schema, rows, anchorPosA2)}
	b2 := &Backup{
		Source:    src2,
		SourceDSN: "src", Store: store, ChunkRows: 100, ChainSlot: true,
	}
	if err := b2.Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	if src2.preflightCalls != 1 {
		t.Errorf("PreflightChainResume calls = %d; want 1 (the adopted slot must be verified)", src2.preflightCalls)
	}
	if src2.gotPreflightFrom != anchorPosA1 {
		t.Errorf("preflight position = %+v; want the adopted anchor %+v", src2.gotPreflightFrom, anchorPosA1)
	}
	if src2.gotPersistChainSlot {
		t.Error("resume opts.PersistChainSlot = true; want false (adoption opens a temporary anchor — the chain slot already exists)")
	}
	if src2.commitCalls != 0 {
		t.Errorf("resume CommitFn calls = %d; want 0 (nothing to persist on the temporary-anchor shape)", src2.commitCalls)
	}
	final, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if final.EndPosition != anchorPosA1 {
		t.Errorf("final EndPosition = %+v; want adopted %+v", final.EndPosition, anchorPosA1)
	}
}

// TestBackup_ChainSlotResumePreflightRefusalStopsRun pins the loud path
// when the adopted slot cannot serve the anchor (dropped, or advanced
// by another consumer): refuse BEFORE opening any snapshot, naming
// --force-overwrite.
func TestBackup_ChainSlotResumePreflightRefusalStopsRun(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	crashAfterFirstTable(t, store, newAnchorTestEngine(schema, rows, anchorPosA1), true)

	src2 := &chainResumeBackupEngine{
		snapshotOpeningEngine: newAnchorTestEngine(schema, rows, anchorPosA2),
		preflightErr:          errors.New(`slot "sluice_slot" does not exist on the source`),
	}
	b2 := &Backup{
		Source:    src2,
		SourceDSN: "src", Store: store, ChunkRows: 100, ChainSlot: true,
	}
	err = b2.Run(context.Background())
	if err == nil {
		t.Fatal("resume Run succeeded with a failing preflight; want loud refusal")
	}
	for _, want := range []string{"does not exist", "--force-overwrite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
	if src2.snapshotCalls != 0 {
		t.Errorf("OpenBackupSnapshot calls = %d; want 0 (refusal must fire before any source resource opens)", src2.snapshotCalls)
	}
}

// TestIncremental_RefusesInProgressParent pins fix step 8: now that
// in-progress full manifests carry an anchor, an incremental must not
// silently chain off a crashed full (its row chunks are incomplete —
// restore would be missing tables, exit 0).
func TestIncremental_RefusesInProgressParent(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{keyedTable("users"), keyedTable("posts")}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}},
		"posts": {{"id": int64(10)}},
	}
	src := newAnchorTestEngine(schema, rows, anchorPosA1)
	crashAfterFirstTable(t, store, src, false)

	incr := &IncrementalBackup{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
	}
	err = incr.Run(context.Background())
	if err == nil {
		t.Fatal("IncrementalBackup.Run succeeded off an in-progress parent; want loud refusal")
	}
	for _, want := range []string{"in_progress", "incomplete link"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
}
