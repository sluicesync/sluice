// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the cross-table backup pool (ADR-0084): the
// capability gate, the free-reader claim/release accounting, the
// serial collapse, first-error peer cancellation, and the
// manifestCommitter's concurrency contract. CI runs these under
// -race; locally (CGO=0 Windows) they pin shape only.

package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// importerStubEngine is a stubEngine that additionally satisfies
// [ir.SnapshotImporterOpener], so the gate's interface predicate can
// be exercised both ways. The opener itself is never called by the
// gate (presence-only check).
type importerStubEngine struct{ stubEngine }

func (importerStubEngine) OpenSnapshotImporter(context.Context, string) (ir.SnapshotImporter, error) {
	panic("importerStubEngine.OpenSnapshotImporter called — gate tests are presence-only")
}

func TestBackupParallelEligible(t *testing.T) {
	withName := func(name string) *irbackup.Snapshot { return &irbackup.Snapshot{SnapshotName: name} }
	withExtras := func(n int) *irbackup.Snapshot {
		extras := make([]ir.RowReader, n)
		for i := range extras {
			extras[i] = &backupPoolFakeReader{}
		}
		return &irbackup.Snapshot{ExtraReaders: extras}
	}
	cases := []struct {
		name             string
		snap             *irbackup.Snapshot
		source           ir.Engine
		tableParallelism int
		wantOK           bool
		wantReason       string
	}{
		{
			name:             "no shareable snapshot (MySQL serial / v0.17.x fallback)",
			snap:             withName(""),
			source:           importerStubEngine{},
			tableParallelism: 4,
			wantReason:       "not shareable",
		},
		{
			name:             "no snapshot importer",
			snap:             withName("00000003-0000001B-1"),
			source:           stubEngine{},
			tableParallelism: 4,
			wantReason:       "no snapshot importer",
		},
		{
			name:             "operator requested serial",
			snap:             withName("00000003-0000001B-1"),
			source:           importerStubEngine{},
			tableParallelism: 1,
			wantReason:       "--table-parallelism=1",
		},
		{
			name:             "eligible via lazy exported snapshot (PG)",
			snap:             withName("00000003-0000001B-1"),
			source:           importerStubEngine{},
			tableParallelism: 4,
			wantOK:           true,
		},
		{
			// ADR-0088: eager coordinated readers qualify on a source with
			// NO snapshot importer and NO shareable name — presence of
			// ExtraReaders is the EAGER eligibility signal (MySQL FTWRL).
			name:             "eligible via eager coordinated readers (MySQL)",
			snap:             withExtras(3),
			source:           stubEngine{},
			tableParallelism: 4,
			wantOK:           true,
		},
		{
			// Eager readers present but operator asked for serial: still
			// not eligible (the request gate is checked first).
			name:             "eager readers but serial requested",
			snap:             withExtras(3),
			source:           stubEngine{},
			tableParallelism: 1,
			wantReason:       "--table-parallelism=1",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			ok, reason := backupParallelEligible(c.snap, c.source, c.tableParallelism)
			if ok != c.wantOK {
				t.Errorf("ok = %v; want %v (reason %q)", ok, c.wantOK, reason)
			}
			if !strings.Contains(reason, c.wantReason) {
				t.Errorf("reason = %q; want contains %q", reason, c.wantReason)
			}
		})
	}
}

// TestResolveBackupTableParallelism_TaskCountClamp pins that the
// fan-out never exceeds the number of tables to sweep — a one-table
// backup stays serial (and never opens an importer) even when the gate
// would otherwise engage.
func TestResolveBackupTableParallelism_TaskCountClamp(t *testing.T) {
	b := &Backup{Source: importerStubEngine{}, SourceDSN: "dsn", Store: &LocalStore{}}
	got, _, err := b.resolveBackupTableParallelism(context.Background(), &irbackup.Snapshot{SnapshotName: "snap-1"}, 1)
	if err != nil {
		t.Fatalf("resolveBackupTableParallelism: %v", err)
	}
	if got != 1 {
		t.Errorf("tableParallelism = %d; want 1 (clamped to task count)", got)
	}
}

// TestResolveBackupTableParallelism_AutoDefault pins the 0 = auto = 4
// resolution on an eligible source without a connection-budget prober
// (no measured ceiling → the requested value stands).
func TestResolveBackupTableParallelism_AutoDefault(t *testing.T) {
	b := &Backup{Source: importerStubEngine{}, SourceDSN: "dsn", Store: &LocalStore{}}
	got, _, err := b.resolveBackupTableParallelism(context.Background(), &irbackup.Snapshot{SnapshotName: "snap-1"}, 10)
	if err != nil {
		t.Fatalf("resolveBackupTableParallelism: %v", err)
	}
	if got != defaultTableParallelism {
		t.Errorf("tableParallelism = %d; want auto default %d", got, defaultTableParallelism)
	}
}

// backupPoolFakeReader is a per-test [ir.RowReader] whose behaviour is keyed
// on the table name:
//
//   - "bad"   → ReadRows returns an immediate error.
//   - "block" → ReadRows returns a channel that never delivers a row;
//     the streaming goroutine exits (closing nothing) only when ctx is
//     cancelled, so the consuming backupTable unwinds via ctx.Done.
//   - else    → streams the configured rows then closes (natural EOF).
//
// Close increments closes so dedicated-reader accounting is pinnable.
type backupPoolFakeReader struct {
	rows   map[string][]ir.Row
	closes *atomic.Int64
}

func (r *backupPoolFakeReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	switch table.Name {
	case "bad":
		return nil, errors.New("injected read failure")
	case "block":
		out := make(chan ir.Row)
		go func() {
			<-ctx.Done()
			close(out)
		}()
		return out, nil
	}
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for _, row := range r.rows[table.Name] {
			select {
			case <-ctx.Done():
				return
			case out <- row:
			}
		}
	}()
	return out, nil
}

func (*backupPoolFakeReader) Err() error { return nil }

func (r *backupPoolFakeReader) Close() error {
	if r.closes != nil {
		r.closes.Add(1)
	}
	return nil
}

// poolTestFixture builds a Backup + committer + tasks over nTables
// simple one-column tables, each with one row.
func poolTestFixture(t *testing.T, nTables int) (*Backup, *manifestCommitter, []backupTableTask, map[string][]ir.Row) {
	t.Helper()
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	b := &Backup{Source: stubEngine{}, SourceDSN: "dsn", Store: store}
	manifest := &irbackup.Manifest{PartialState: irbackup.BackupStateInProgress, Kind: irbackup.BackupKindFull}
	committer := &manifestCommitter{store: store, manifest: manifest}
	rows := map[string][]ir.Row{}
	tasks := make([]backupTableTask, 0, nTables)
	for i := 0; i < nTables; i++ {
		name := fmt.Sprintf("t%02d", i)
		rows[name] = []ir.Row{{"id": int64(i)}}
		table := &ir.Table{
			Name:    name,
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}
		entry := &irbackup.TableManifest{Name: name, Partial: true}
		committer.stageTable(entry)
		tasks = append(tasks, backupTableTask{table: table, entry: entry})
	}
	return b, committer, tasks, rows
}

// TestBackupTablePool_SerialCollapseNeverMintsReaders pins that
// tableParallelism=1 runs every table through the free (primary)
// reader and the dedicated-reader factory is never invoked — the
// one-code-path serial collapse.
func TestBackupTablePool_SerialCollapseNeverMintsReaders(t *testing.T) {
	b, committer, tasks, rows := poolTestFixture(t, 4)
	primary := &backupPoolFakeReader{rows: rows}
	var factoryCalls atomic.Int64
	factory := func(context.Context) (ir.RowReader, error) {
		factoryCalls.Add(1)
		return nil, errors.New("factory must not be called on the serial path")
	}
	if err := b.runBackupTablePool(context.Background(), tasks, primary, nil, factory, 1, 100, committer, nil, nil); err != nil {
		t.Fatalf("runBackupTablePool: %v", err)
	}
	if n := factoryCalls.Load(); n != 0 {
		t.Errorf("factory called %d times on the serial path; want 0", n)
	}
	for _, task := range tasks {
		if task.entry.Partial {
			t.Errorf("table %s still Partial after serial pool", task.table.Name)
		}
	}
}

// TestBackupTablePool_DedicatedReadersClosed pins the free-reader
// claim/release accounting: with N tables and full parallelism, every
// reader the factory minted is closed exactly once by the release
// path, and the primary (free) reader is never closed by the pool.
func TestBackupTablePool_DedicatedReadersClosed(t *testing.T) {
	b, committer, tasks, rows := poolTestFixture(t, 6)
	var primaryCloses, dedicatedCloses atomic.Int64
	primary := &backupPoolFakeReader{rows: rows, closes: &primaryCloses}
	var minted atomic.Int64
	factory := func(context.Context) (ir.RowReader, error) {
		minted.Add(1)
		return &backupPoolFakeReader{rows: rows, closes: &dedicatedCloses}, nil
	}
	if err := b.runBackupTablePool(context.Background(), tasks, primary, nil, factory, 4, 100, committer, nil, nil); err != nil {
		t.Fatalf("runBackupTablePool: %v", err)
	}
	if primaryCloses.Load() != 0 {
		t.Errorf("primary reader closed %d times by the pool; want 0 (caller owns it)", primaryCloses.Load())
	}
	if m, c := minted.Load(), dedicatedCloses.Load(); m != c {
		t.Errorf("minted %d dedicated readers but closed %d; want equal", m, c)
	}
}

// TestBackupTablePool_EagerPeersReusedNeverMinted pins the ADR-0088
// eager path: the pre-opened coordinated peers seed the reusable
// free-reader pool, so with more tables than (primary + peers) the
// factory is NEVER called (there is nothing to mint — MySQL can't open
// fresh coincident readers after the FTWRL window) and no peer is closed
// by the pool (the snapshot's CloseFn owns every reader). Sweeping 8
// tables through a pool of 1 primary + 3 peers at parallelism 4 proves
// the pool reuses peers across tables.
func TestBackupTablePool_EagerPeersReusedNeverMinted(t *testing.T) {
	b, committer, tasks, rows := poolTestFixture(t, 8)
	var primaryCloses, peerCloses atomic.Int64
	primary := &backupPoolFakeReader{rows: rows, closes: &primaryCloses}
	peers := make([]ir.RowReader, 3)
	for i := range peers {
		peers[i] = &backupPoolFakeReader{rows: rows, closes: &peerCloses}
	}
	var factoryCalls atomic.Int64
	factory := func(context.Context) (ir.RowReader, error) {
		factoryCalls.Add(1)
		return nil, errors.New("factory must not be called on the eager path")
	}
	if err := b.runBackupTablePool(context.Background(), tasks, primary, peers, factory, 4, 100, committer, nil, nil); err != nil {
		t.Fatalf("runBackupTablePool: %v", err)
	}
	if n := factoryCalls.Load(); n != 0 {
		t.Errorf("factory called %d times on the eager path; want 0 (peers seed the pool)", n)
	}
	if c := primaryCloses.Load() + peerCloses.Load(); c != 0 {
		t.Errorf("pool closed %d pooled readers; want 0 (snapshot CloseFn owns them)", c)
	}
	for _, task := range tasks {
		if task.entry.Partial {
			t.Errorf("table %s still Partial after eager pool", task.table.Name)
		}
	}
}

// TestBackupTablePool_FirstErrorCancelsPeers pins the errgroup
// contract: one table's read failure cancels the derived ctx so peer
// tables blocked mid-stream unwind, and the pool returns the original
// error naming the failed table.
func TestBackupTablePool_FirstErrorCancelsPeers(t *testing.T) {
	b, committer, _, rows := poolTestFixture(t, 0)
	// Three blocking tables + one failing table, parallelism 4 so all
	// are in flight together.
	tasks := make([]backupTableTask, 0, 4)
	for _, name := range []string{"block", "block2", "block3", "bad"} {
		base := name
		if strings.HasPrefix(name, "block") {
			base = "block" // backupPoolFakeReader keys behaviour on "block" exactly
		}
		table := &ir.Table{
			Name:    base,
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}
		entry := &irbackup.TableManifest{Name: name, Partial: true}
		committer.stageTable(entry)
		tasks = append(tasks, backupTableTask{table: table, entry: entry})
	}
	primary := &backupPoolFakeReader{rows: rows}
	factory := func(context.Context) (ir.RowReader, error) {
		return &backupPoolFakeReader{rows: rows}, nil
	}

	done := make(chan error, 1)
	go func() {
		done <- b.runBackupTablePool(context.Background(), tasks, primary, nil, factory, 4, 100, committer, nil, nil)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), `table "bad"`) || !strings.Contains(err.Error(), "injected read failure") {
			t.Fatalf("pool error = %v; want the injected failure naming table \"bad\"", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pool did not return after the failing table; peer cancellation broken")
	}
}

// TestBackupTablePool_NilFactoryDedicatedBranchIsLoud pins the
// programming-error guard: reaching the dedicated-reader branch with a
// nil factory surfaces errBackupPoolNoFactory rather than a nil deref.
func TestBackupTablePool_NilFactoryDedicatedBranchIsLoud(t *testing.T) {
	free := make(chan ir.RowReader, 1) // empty: free reader held by a peer
	_, release, err := acquireBackupReader(context.Background(), free, nil)
	release()
	if !errors.Is(err, errBackupPoolNoFactory) {
		t.Fatalf("err = %v; want errBackupPoolNoFactory", err)
	}
}

// TestManifestCommitter_ConcurrentCheckpointsKeepSchemaOrder drives N
// worker goroutines concurrently appending chunks + checkpointing
// against one committer and verifies (a) every intermediate and final
// manifest on the store is well-formed JSON, and (b) the final table
// order equals the staged (schema) order regardless of completion
// order. -race in CI is the load-bearing leg of this pin.
func TestManifestCommitter_ConcurrentCheckpointsKeepSchemaOrder(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	const nTables, nChunks = 8, 5
	manifest := &irbackup.Manifest{PartialState: irbackup.BackupStateInProgress}
	committer := &manifestCommitter{store: store, manifest: manifest}

	entries := make([]*irbackup.TableManifest, nTables)
	wantOrder := make([]string, nTables)
	for i := range entries {
		name := fmt.Sprintf("table-%02d", i)
		entries[i] = &irbackup.TableManifest{Name: name, Partial: true}
		wantOrder[i] = name
		committer.stageTable(entries[i])
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
				if err := committer.appendChunk(context.Background(), entry, ci); err != nil {
					t.Errorf("appendChunk(%s, %d): %v", entry.Name, c, err)
					return
				}
			}
			if err := committer.finishTable(context.Background(), entry, nChunks); err != nil {
				t.Errorf("finishTable(%s): %v", entry.Name, err)
			}
		}(entry)
	}
	wg.Wait()

	rc, err := store.Get(context.Background(), ManifestFileName)
	if err != nil {
		t.Fatalf("Get manifest: %v", err)
	}
	raw, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var got irbackup.Manifest
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("final manifest is not well-formed JSON: %v", err)
	}
	if len(got.Tables) != nTables {
		t.Fatalf("manifest tables = %d; want %d", len(got.Tables), nTables)
	}
	for i, table := range got.Tables {
		if table.Name != wantOrder[i] {
			t.Errorf("manifest.Tables[%d] = %s; want %s (schema order must survive concurrent completion)", i, table.Name, wantOrder[i])
		}
		if table.Partial || len(table.Chunks) != nChunks || table.RowCount != nChunks {
			t.Errorf("table %s = partial=%v chunks=%d rows=%d; want complete with %d chunks", table.Name, table.Partial, len(table.Chunks), table.RowCount, nChunks)
		}
	}
}
