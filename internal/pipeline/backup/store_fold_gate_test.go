// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The Bug 248 pins: the store-fold gate refuses `backup full` /
// `export-as-parquet` BEFORE any write when the destination store
// measures case-folding and table-derived paths would collide. The
// fake stores make the platform-independent both-direction pins; the
// real-LocalStore test measures the host filesystem with a direct
// os.WriteFile oracle and asserts the probe AGREES with it — so the
// same test is meaningful on Windows/macOS (folds) and Linux (does
// not) without asserting either platform's answer a priori.

// foldingStore wraps the in-memory store with lower-cased keys — a
// case-folding namespace like NTFS. putCount proves the gate's
// before-any-write claim.
type foldingStore struct {
	*memStore
	putCount int
}

func (f *foldingStore) Put(ctx context.Context, path string, r io.Reader) error {
	f.putCount++
	return f.memStore.Put(ctx, strings.ToLower(path), r)
}

func (f *foldingStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	return f.memStore.Get(ctx, strings.ToLower(path))
}

func (f *foldingStore) Delete(ctx context.Context, path string) error {
	return f.memStore.Delete(ctx, strings.ToLower(path))
}

func TestCaseFoldCollisions(t *testing.T) {
	got := caseFoldCollisions([]string{"Orders", "orders", "users", "AUDIT", "audit", "Audit"})
	if len(got) != 2 {
		t.Fatalf("groups = %v; want 2", got)
	}
	if strings.Join(got[0], ",") != "AUDIT,Audit,audit" || strings.Join(got[1], ",") != "Orders,orders" {
		t.Errorf("groups = %v; want sorted [AUDIT/Audit/audit] and [Orders/orders]", got)
	}
	if c := caseFoldCollisions([]string{"orders", "users", "orders"}); len(c) != 0 {
		t.Errorf("duplicate identical names are not a case collision: %v", c)
	}
}

// TestStoreFoldsCase_FakeStores pins both probe directions and the
// probe's cleanup contract.
func TestStoreFoldsCase_FakeStores(t *testing.T) {
	ctx := context.Background()

	folding := &foldingStore{memStore: newMemStore()}
	folds, err := storeFoldsCase(ctx, folding)
	if err != nil || !folds {
		t.Fatalf("folding store measured (%v, %v); want (true, nil)", folds, err)
	}
	if paths, _ := folding.List(ctx, ""); len(paths) != 0 {
		t.Errorf("probe objects not cleaned up: %v", paths)
	}

	sensitive := newMemStore()
	folds, err = storeFoldsCase(ctx, sensitive)
	if err != nil || folds {
		t.Fatalf("case-sensitive store measured (%v, %v); want (false, nil)", folds, err)
	}
	if paths, _ := sensitive.List(ctx, ""); len(paths) != 0 {
		t.Errorf("probe objects not cleaned up: %v", paths)
	}
}

// TestStoreFoldsCase_RealLocalStore measures the host filesystem two
// ways — a direct os.WriteFile case-twin pair (the independent oracle)
// and the gate's own probe through LocalStore — and requires them to
// AGREE. On this project's Windows dev machine the oracle says folding
// (the Bug 248 repro environment); on CI Linux it says case-sensitive;
// either way a disagreement is a probe bug.
func TestStoreFoldsCase_RealLocalStore(t *testing.T) {
	dir := t.TempDir()

	// Oracle: write two case-twin files directly and see whether the
	// second clobbered the first.
	if err := os.WriteFile(filepath.Join(dir, "probe-oracle"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "PROBE-ORACLE"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
	back, err := os.ReadFile(filepath.Join(dir, "probe-oracle"))
	if err != nil {
		t.Fatal(err)
	}
	oracleFolds := string(back) == "b"

	store, err := blobcodec.NewLocalStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	probeFolds, err := storeFoldsCase(context.Background(), store)
	if err != nil {
		t.Fatalf("probe errored on a real LocalStore: %v", err)
	}
	if probeFolds != oracleFolds {
		t.Fatalf("probe says folds=%v but the filesystem oracle says folds=%v — the probe mismeasures the very premise the Bug 248 gate rests on", probeFolds, oracleFolds)
	}
}

func bug248Schema() *ir.Schema {
	col := func() []*ir.Column { return []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}} }
	return &ir.Schema{Tables: []*ir.Table{
		{Name: "Orders", Columns: col()},
		{Name: "orders", Columns: col()},
		{Name: "users", Columns: col()},
	}}
}

// TestBackupRun_StoreFoldCollision_RefusesBeforeAnyWrite is the e2e
// door pin on the `backup full` path: a folding store + case-colliding
// tables refuses with the code and ZERO objects written (the
// before-any-write claim, proven by the store's put counter); the same
// schema on a case-sensitive store proceeds; excluding one colliding
// twin releases the gate (the remedy pin) — the filtered set has no
// collision, so the store is not even probed.
func TestBackupRun_StoreFoldCollision_RefusesBeforeAnyWrite(t *testing.T) {
	ctx := context.Background()

	rows := map[string][]ir.Row{}
	folding := &foldingStore{memStore: newMemStore()}
	b := &Backup{Source: newBackupRecorderEngine("mysql", bug248Schema(), rows), SourceDSN: "src", Store: folding}
	err := b.Run(ctx)
	if err == nil {
		t.Fatal("backup to a folding store with Orders+orders succeeded — the Bug 248 silent self-clobber shape")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupStoreNameCollision {
		t.Fatalf("want %s, got %v", sluicecode.CodeBackupStoreNameCollision, err)
	}
	for _, want := range []string{"Orders / orders", "--exclude-table", "case-sensitive store"} {
		if !strings.Contains(err.Error()+" "+ce.Hint, want) {
			t.Errorf("refusal does not carry %q:\n%v\nhint: %s", want, err, ce.Hint)
		}
	}
	// The probe's own two puts are the ONLY writes; both were deleted.
	if paths, _ := folding.List(ctx, ""); len(paths) != 0 {
		t.Errorf("refusal left store objects behind: %v", paths)
	}
	if folding.putCount != 2 {
		t.Errorf("putCount = %d; want exactly the 2 probe objects (nothing of the backup itself)", folding.putCount)
	}

	// Case-sensitive store: same schema proceeds end-to-end.
	sensitive := newMemStore()
	b = &Backup{Source: newBackupRecorderEngine("mysql", bug248Schema(), rows), SourceDSN: "src", Store: sensitive}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("case-sensitive store must accept Orders+orders: %v", err)
	}

	// The remedy: exclude one twin — released without probing.
	excl, ferr := migcore.NewTableFilter(nil, []string{"Orders"})
	if ferr != nil {
		t.Fatal(ferr)
	}
	probeless := &foldingStore{memStore: newMemStore()}
	b = &Backup{
		Source: newBackupRecorderEngine("mysql", bug248Schema(), rows), SourceDSN: "src", Store: probeless,
		Filter: excl,
	}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("--exclude-table=Orders must release the gate: %v", err)
	}
	// The no-probe-on-collision-free-set property is pinned at the
	// function level by TestRefuseStoreFoldCollision_NoCollisionNeverProbes;
	// this run proves only that the remedy releases the door end-to-end.
	_ = probeless
}

// TestRefuseStoreFoldCollision_NoCollisionNeverProbes pins the cheap
// path: a collision-free name set must not touch the store at all (the
// probe would cost two PUTs per backup on cloud stores otherwise).
func TestRefuseStoreFoldCollision_NoCollisionNeverProbes(t *testing.T) {
	counting := &foldingStore{memStore: newMemStore()}
	if err := refuseStoreFoldCollision(context.Background(), counting, []string{"orders", "users"}, "backup"); err != nil {
		t.Fatalf("collision-free set refused: %v", err)
	}
	if counting.putCount != 0 {
		t.Errorf("putCount = %d; a collision-free set must never probe", counting.putCount)
	}
}

// TestParquetExportFileNames_FoldGateSeesTheRealKeys pins that the
// export door scans parquetFileName output (schema-qualified keys), so
// a PG `s1.Orders` / `s1.orders` pair collides while `s1.t` / `s2.t`
// does not.
func TestParquetExportFileNames_FoldGateSeesTheRealKeys(t *testing.T) {
	c := caseFoldCollisions([]string{
		parquetFileName("s1", "Orders"),
		parquetFileName("s1", "orders"),
		parquetFileName("s2", "t"),
		parquetFileName("s1", "t"),
	})
	if len(c) != 1 || strings.Join(c[0], ",") != "s1.Orders.parquet,s1.orders.parquet" {
		t.Fatalf("parquet key collisions = %v; want exactly the s1 Orders pair", c)
	}
}

var _ irbackup.Store = (*foldingStore)(nil)

// TestParquetExportRun_StoreFoldCollision_Refuses is the e2e pin on
// the EXPORT door (the wiring in ParquetExport.Run, not just the key
// math above): a backup taken on a case-SENSITIVE store from a
// case-colliding source exports fine to another case-sensitive store,
// and refuses with the code toward a folding one — before any parquet
// file is written.
func TestParquetExportRun_StoreFoldCollision_Refuses(t *testing.T) {
	ctx := context.Background()

	src := newMemStore()
	b := &Backup{Source: newBackupRecorderEngine("mysql", bug248Schema(), map[string][]ir.Row{}), SourceDSN: "src", Store: src}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("seed backup: %v", err)
	}

	folding := &foldingStore{memStore: newMemStore()}
	err := (&ParquetExport{Store: src, Output: folding, SluiceVersion: "test"}).Run(ctx)
	if err == nil {
		t.Fatal("export to a folding store with Orders+orders succeeded — the later table's parquet silently overwrites the earlier's")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupStoreNameCollision {
		t.Fatalf("want %s, got %v", sluicecode.CodeBackupStoreNameCollision, err)
	}
	for _, p := range mustListStore(t, folding.memStore) {
		if strings.HasSuffix(p, ".parquet") {
			t.Errorf("refusal left a parquet file behind: %s", p)
		}
	}

	sensitive := newMemStore()
	if err := (&ParquetExport{Store: src, Output: sensitive, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("case-sensitive output must accept Orders+orders: %v", err)
	}
}

func mustListStore(t *testing.T, s *memStore) []string {
	t.Helper()
	paths, err := s.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	return paths
}
