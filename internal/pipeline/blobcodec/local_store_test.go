// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestLocalStore_PutGet(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	want := []byte("hello, sluice backup")
	if err := s.Put(context.Background(), "manifest.json", bytes.NewReader(want)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := s.Get(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %q; want %q", got, want)
	}

	// Verify atomic write: there's no leftover .tmp.
	if _, err := os.Stat(filepath.Join(dir, "manifest.json.tmp")); !os.IsNotExist(err) {
		t.Errorf("leftover .tmp file: err=%v", err)
	}
}

// TestLocalStore_OwnerOnlyPermissions pins the 0600/0700 contract:
// backup chunks contain full row data and --encrypt is opt-in, so a
// world-readable backup dir hands any local user the dataset. Skipped
// on Windows, where Go approximates Unix permission bits.
func TestLocalStore_OwnerOnlyPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are approximated on Windows")
	}
	dir := t.TempDir()
	s, err := NewLocalStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := s.Put(context.Background(), "chunks/users/users-0.jsonl.gz", bytes.NewReader([]byte("row data"))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	wantPerms := map[string]os.FileMode{
		filepath.Join(dir, "store"):                                        0o700,
		filepath.Join(dir, "store", "chunks", "users"):                     0o700,
		filepath.Join(dir, "store", "chunks", "users", "users-0.jsonl.gz"): 0o600,
	}
	for path, want := range wantPerms {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s: perm = %o; want %o", path, got, want)
		}
	}
}

func TestLocalStore_PutNestedDirectories(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	if err := s.Put(context.Background(), "chunks/users/users-0.jsonl.gz", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("Put nested: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "chunks", "users", "users-0.jsonl.gz"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "data" {
		t.Errorf("got %q", got)
	}
}

func TestLocalStore_List(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	files := []string{
		"manifest.json",
		"chunks/users/users-0.jsonl.gz",
		"chunks/users/users-1.jsonl.gz",
		"chunks/orders/orders-0.jsonl.gz",
	}
	for _, f := range files {
		if err := s.Put(context.Background(), f, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put %s: %v", f, err)
		}
	}

	got, err := s.List(context.Background(), "chunks/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(got)
	want := []string{
		"chunks/orders/orders-0.jsonl.gz",
		"chunks/users/users-0.jsonl.gz",
		"chunks/users/users-1.jsonl.gz",
	}
	if !equalStrSlices(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}

	// Empty prefix returns everything.
	all, _ := s.List(context.Background(), "")
	if len(all) != len(files) {
		t.Errorf("List(\"\") returned %d; want %d", len(all), len(files))
	}
}

// TestLocalStore_CrashOrphanedTemps pins the crash-orphan `.tmp.*`
// handling (two audits filed the accumulation): an orphaned Put temp is
// invisible to List, is swept by a LATER store instance's first write into
// its directory once STALE, and a FRESH temp — a live concurrent writer's
// — is never swept (the age guard). isPutTempName's shape-tightness is
// pinned too, so a legitimate object that merely contains ".tmp." cannot
// be swallowed by the filter.
//
// The store-instance boundary is the audit-P-1 shape and is deliberate:
// orphans are a PREVIOUS process's residue, so the sweep runs once per
// (store, directory) rather than once per write — see the sweep's doc for
// the quadratic cost that made per-write sweeping untenable. The
// same-instance-does-not-re-sweep cell below pins that tradeoff rather
// than leaving it implied.
func TestLocalStore_CrashOrphanedTemps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// newStore stands in for a fresh PROCESS: the sweep memo is per store
	// instance, so each re-open gets one sweep per directory it writes to.
	newStore := func() *LocalStore {
		t.Helper()
		s, err := NewLocalStore(dir)
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		return s
	}
	old := time.Now().Add(-2 * localStoreTempSweepAge)
	plantOrphan := func(name string, stale bool) string {
		t.Helper()
		p := filepath.Join(dir, "chain", name)
		if err := os.WriteFile(p, []byte("torn"), 0o600); err != nil {
			t.Fatalf("plant orphan %s: %v", name, err)
		}
		if stale {
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatalf("age orphan %s: %v", name, err)
			}
		}
		return p
	}

	s := newStore()
	if err := s.Put(ctx, "chain/lineage.json", bytes.NewReader([]byte("v1"))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A crash orphan: exactly os.CreateTemp's naming, written directly (no
	// Put ever renames it in).
	orphan := plantOrphan("lineage.json.tmp.123456789", false)

	// Invisible to List — a temp is never a legitimate object.
	got, err := s.List(ctx, "chain/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !equalStrSlices(got, []string{"chain/lineage.json"}) {
		t.Errorf("List = %v; want the object only (the orphan must be filtered)", got)
	}

	// FRESH orphan (mtime = now): a new store's first write into the
	// directory must NOT sweep it — it could be a live concurrent writer's
	// in-flight temp.
	if err := newStore().Put(ctx, "chain/lineage.json", bytes.NewReader([]byte("v2"))); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("fresh temp swept by Put (age guard broken): %v", err)
	}

	// STALE orphan (mtime aged past the sweep window): a new store's first
	// write into the directory reclaims it.
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatalf("age orphan: %v", err)
	}
	s3 := newStore()
	if err := s3.Put(ctx, "chain/lineage.json", bytes.NewReader([]byte("v3"))); err != nil {
		t.Fatalf("Put v3: %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("stale orphan survived the first-write sweep: err=%v", err)
	}

	// The memo, named: an orphan appearing AFTER this store already swept
	// the directory is not reclaimed by that same store — that is the cost
	// tradeoff, and it is harmless because a live process leaves no
	// orphans. The next process reclaims it.
	late := plantOrphan("lineage.json.tmp.777", true)
	if err := s3.Put(ctx, "chain/lineage.json", bytes.NewReader([]byte("v4"))); err != nil {
		t.Fatalf("Put v4: %v", err)
	}
	if _, err := os.Stat(late); err != nil {
		t.Fatalf("the same store re-swept an already-swept directory — the per-Put directory read is exactly the P-1 regression: %v", err)
	}

	// A-6: the orphan of a key that is NEVER WRITTEN AGAIN is still
	// reclaimed, because the sweep's scope is the DIRECTORY. Backup chunk
	// keys are written exactly once, so a per-key sweep could never fire
	// for the dominant object class.
	neverAgain := plantOrphan("chunk-000123.jsonl.gz.tmp.9", true)
	if err := newStore().Put(ctx, "chain/some-other-key.json", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put other key: %v", err)
	}
	for _, p := range []string{late, neverAgain} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("stale orphan %s survived a fresh store's first write into its directory: err=%v", filepath.Base(p), err)
		}
	}

	// PutIfAbsent sweeps too (a crashed Put into this directory may have
	// left an orphan, and this may be the store's first write there).
	orphan2 := plantOrphan("claim.json.tmp.42", true)
	if err := newStore().PutIfAbsent(ctx, "chain/claim.json", bytes.NewReader([]byte("claimed"))); err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if _, err := os.Stat(orphan2); !os.IsNotExist(err) {
		t.Errorf("stale orphan survived the PutIfAbsent sweep: err=%v", err)
	}

	// The name filter is tight to CreateTemp's pattern: these are
	// legitimate objects and MUST list. (A trailing-dot name like
	// "archive.tmp." — empty suffix, also legit per the filter — cannot be
	// pinned portably: Windows strips trailing dots at the filesystem.)
	last := newStore()
	for _, legit := range []string{"chain/report.tmp.notes", "chain/data.tmp.1x", "chain/backup.tmp.9a"} {
		if err := last.Put(ctx, legit, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put %s: %v", legit, err)
		}
	}
	all, err := last.List(ctx, "chain/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(all)
	want := []string{
		"chain/backup.tmp.9a", "chain/claim.json", "chain/data.tmp.1x",
		"chain/lineage.json", "chain/report.tmp.notes", "chain/some-other-key.json",
	}
	if !equalStrSlices(all, want) {
		t.Errorf("List = %v; want %v (a legitimate name containing .tmp. must not be swallowed)", all, want)
	}
}

// TestLocalStorePut_CostIsIndependentOfDirectorySize is the audit
// 2026-08-31 P-1 gate: a Put's cost must not grow with the number of files
// already in its directory. Before the fix every Put ran
// `filepath.Glob(<key>.tmp.*)`, which — the directory component carrying
// no metacharacter — reads and sorts the ENTIRE containing directory. All
// of one table's backup chunks land in one directory, so chunk N paid an
// O(N) scan and the table cost O(N²).
//
// METHOD, and why it is not the ThroughputCost flake class: the gate
// counts directory READS (LocalStore.sweepDirScans / .sweepEntriesRead),
// never wall-clock. The quantity that regressed is exactly "how many
// directory entries did the write path read", so counting it is both the
// direct measurement and a deterministic one — identical on a loaded CI
// runner and an idle laptop, with no margin to tune.
//
// SCOPE: LocalStore only. The cloud BlobStore streams straight to an
// object-store writer and has no temp-file sweep to be quadratic in — the
// sibling is exempt because the mechanism does not exist there, not
// because it was checked and found fine.
func TestLocalStorePut_CostIsIndependentOfDirectorySize(t *testing.T) {
	ctx := context.Background()
	const putsPerDir = 20

	for _, siblings := range []int{100, 10_000} {
		t.Run(fmt.Sprintf("%d siblings", siblings), func(t *testing.T) {
			root := t.TempDir()
			chunkDir := filepath.Join(root, "chunks")
			if err := os.MkdirAll(chunkDir, 0o700); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			for i := range siblings {
				name := filepath.Join(chunkDir, fmt.Sprintf("existing-%06d.jsonl.gz", i))
				if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
					t.Fatalf("populate: %v", err)
				}
			}
			s, err := NewLocalStore(root)
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}

			put := func(i int) {
				t.Helper()
				key := fmt.Sprintf("chunks/new-%06d.jsonl.gz", i)
				if err := s.Put(ctx, key, bytes.NewReader([]byte("row"))); err != nil {
					t.Fatalf("Put %s: %v", key, err)
				}
			}

			put(0)
			firstScans, firstEntries := s.sweepDirScans.Load(), s.sweepEntriesRead.Load()
			for i := 1; i < putsPerDir; i++ {
				put(i)
			}
			restScans := s.sweepDirScans.Load() - firstScans
			restEntries := s.sweepEntriesRead.Load() - firstEntries

			// Anti-vacuity floor: the counters must be REAL. The first Put
			// does exactly one pass over the pre-existing siblings — a gate
			// measuring a sweep that never ran would pass every assertion
			// below while proving nothing.
			if firstScans != 1 {
				t.Fatalf("first Put ran %d directory scan(s); want exactly 1 (the once-per-directory sweep)", firstScans)
			}
			if firstEntries != uint64(siblings) {
				t.Fatalf("first Put read %d directory entries; want %d (the counter must reflect the real ReadDir)", firstEntries, siblings)
			}

			// The load-bearing assertion: after the directory has been
			// swept once, no later Put reads it at all. This is what makes
			// the per-Put cost independent of directory size — under the
			// pre-fix per-Put sweep these are putsPerDir-1 scans and
			// ~(putsPerDir-1)×siblings entries, which for the 10,000-file
			// directory is ~190,000 entry comparisons for 19 writes.
			if restScans != 0 || restEntries != 0 {
				t.Fatalf("%d Put(s) after the first read the directory %d time(s) / %d entries; want 0/0 — a per-Put directory scan is the O(N²) chunk-writer regression",
					putsPerDir-1, restScans, restEntries)
			}
			if total := s.sweepEntriesRead.Load(); total > uint64(siblings+putsPerDir) {
				t.Errorf("%d Put(s) read %d directory entries total; want at most one pass (%d)", putsPerDir, total, siblings+putsPerDir)
			}
		})
	}
}

func TestLocalStore_DeleteIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	_ = s.Put(context.Background(), "x", bytes.NewReader([]byte("y")))
	if err := s.Delete(context.Background(), "x"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Second delete is a no-op.
	if err := s.Delete(context.Background(), "x"); err != nil {
		t.Errorf("second Delete: %v; want nil (idempotent)", err)
	}
}

func TestLocalStore_RejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	cases := []string{
		"../escape",
		"safe/../escape",
		"../../../etc/passwd",
	}
	for _, p := range cases {
		err := s.Put(context.Background(), p, bytes.NewReader([]byte("evil")))
		if err == nil {
			t.Errorf("Put(%q) succeeded; expected path-traversal rejection", p)
			continue
		}
		if !strings.Contains(err.Error(), "path traversal") && !strings.Contains(err.Error(), "escapes root") {
			t.Errorf("Put(%q) error = %v; want path-traversal message", p, err)
		}
	}
}

func TestLocalStore_Exists(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)

	// Absent path.
	exists, err := s.Exists(context.Background(), "absent.txt")
	if err != nil {
		t.Fatalf("Exists(absent): %v", err)
	}
	if exists {
		t.Errorf("Exists(absent) = true; want false")
	}

	// After Put.
	if err := s.Put(context.Background(), "present.txt", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	exists, err = s.Exists(context.Background(), "present.txt")
	if err != nil {
		t.Fatalf("Exists(present): %v", err)
	}
	if !exists {
		t.Errorf("Exists(present) = false; want true")
	}

	// A directory is not a "blob" — Exists returns false.
	if err := os.MkdirAll(filepath.Join(dir, "somedir"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	exists, err = s.Exists(context.Background(), "somedir")
	if err != nil {
		t.Fatalf("Exists(dir): %v", err)
	}
	if exists {
		t.Errorf("Exists(dir) = true; want false (directories are not blobs)")
	}
}

func TestLocalStore_GetMissing(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	if _, err := s.Get(context.Background(), "nope"); err == nil {
		t.Errorf("Get(missing) returned nil err; want a clear error")
	}
}

func TestLocalStore_NewWithEmptyRoot(t *testing.T) {
	if _, err := NewLocalStore(""); err == nil {
		t.Errorf("NewLocalStore(\"\") = nil; want error")
	}
}

func TestLocalStore_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Put(ctx, "x", bytes.NewReader([]byte("y")))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Put with cancelled ctx: err = %v; want context.Canceled", err)
	}
}

func equalStrSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
