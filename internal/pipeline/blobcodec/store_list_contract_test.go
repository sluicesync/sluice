// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Store-conformance pins for the [irbackup.Store.List] contract (audit
// 2026-08-31 A-6): the shared assertions both implementations must
// satisfy, the ONE place they deliberately diverge, and the third-party
// premise that divergence rests on.

package blobcodec

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gocloud.dev/blob"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// listedNames returns store's full listing, sorted.
func listedNames(t *testing.T, store irbackup.Store) []string {
	t.Helper()
	got, err := store.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	slices.Sort(got)
	return got
}

// TestStoreListConformance is the store-conformance table the A-6 finding
// asked for, with the finding's own question already answered: the
// `.tmp.<digits>` exclusion is DOCUMENTED as part of the
// [irbackup.Store.List] contract rather than made symmetric, because it
// belongs to the tmp+rename WRITE STRATEGY and only LocalStore has one.
// Mirroring it onto BlobStore would hide nothing of ours and could only
// swallow a legitimate operator object.
//
// So this pins three things, and the third is the load-bearing one:
//
//  1. the assertions BOTH stores must satisfy (ordinary keys list; a
//     legitimate name that merely contains ".tmp." lists) — the shared
//     contract and this table's anti-vacuity floor;
//  2. the divergence itself, in both directions, so neither store can
//     drift onto the other's behaviour unnoticed;
//  3. the environmental premise the divergence rests on — that gocloud's
//     fileblob driver stages its in-flight temp OUTSIDE the bucket. That
//     is a fact about a third-party dependency, not about sluice, so it
//     is ground-truthed against the real driver (premise-naming rule). A
//     gocloud release that staged temps in-place would make BlobStore's
//     exemption false, and this is what would say so.
func TestStoreListConformance(t *testing.T) {
	ctx := context.Background()

	openLocal := func(t *testing.T) irbackup.Store {
		t.Helper()
		s, err := NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		return s
	}
	openBlob := func(t *testing.T) irbackup.Store {
		t.Helper()
		s, err := OpenBlobStore(ctx, fileBlobURL(t, t.TempDir()), BlobStoreOptions{})
		if err != nil {
			t.Fatalf("OpenBlobStore: %v", err)
		}
		return s
	}
	stores := []struct {
		name string
		open func(*testing.T) irbackup.Store
	}{
		{"LocalStore", openLocal},
		{"BlobStore(file://)", openBlob},
	}

	// (1) The shared contract. Both stores list every object written
	// through them, INCLUDING a legitimate operator name that merely
	// contains ".tmp." — the filter is tight to os.CreateTemp's
	// `<base>.tmp.<all-digits>` shape, so these must survive on both.
	t.Run("both stores list every written object", func(t *testing.T) {
		want := []string{
			"chunks/t/000001.jsonl.gz",
			"chunks/t/report.tmp.notes",
			"chunks/t/data.tmp.1x",
			"manifest.json",
		}
		for _, sc := range stores {
			t.Run(sc.name, func(t *testing.T) {
				store := sc.open(t)
				for _, key := range want {
					if err := store.Put(ctx, key, bytes.NewReader([]byte("x"))); err != nil {
						t.Fatalf("Put %q: %v", key, err)
					}
				}
				got := listedNames(t, store)
				sorted := slices.Clone(want)
				slices.Sort(sorted)
				if !slices.Equal(got, sorted) {
					t.Errorf("List = %v; want %v", got, sorted)
				}
			})
		}
	})

	// (2a) LocalStore hides its own Put temp shape.
	t.Run("LocalStore hides its own Put temp shape", func(t *testing.T) {
		root := t.TempDir()
		store, err := NewLocalStore(root)
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		if err := store.Put(ctx, "chunks/t/000001.jsonl.gz", bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put: %v", err)
		}
		orphan := filepath.Join(root, "chunks", "t", "000001.jsonl.gz.tmp.123456789")
		if err := os.WriteFile(orphan, []byte("torn"), 0o600); err != nil {
			t.Fatalf("plant orphan: %v", err)
		}
		if got := listedNames(t, store); !slices.Equal(got, []string{"chunks/t/000001.jsonl.gz"}) {
			t.Errorf("List = %v; want the object only — a crash-orphaned Put temp is never a caller's object", got)
		}
	})

	// (2b) BlobStore does NOT hide it — the deliberate half of the
	// divergence. An object at that name in a cloud bucket was put there
	// by someone, since this store never writes one.
	t.Run("BlobStore does not hide the same shape", func(t *testing.T) {
		store := openBlob(t)
		const operatorKey = "chunks/t/000001.jsonl.gz.tmp.123456789"
		if err := store.Put(ctx, operatorKey, bytes.NewReader([]byte("operator's own file"))); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if got := listedNames(t, store); !slices.Equal(got, []string{operatorKey}) {
			t.Errorf("List = %v; want %v — BlobStore writes no temps, so filtering that shape could only swallow a legitimate operator object", got, []string{operatorKey})
		}
	})

	// (3) The premise. BlobStore's exemption is only true because its
	// in-flight write is not visible IN THE BUCKET. For s3/gs/azblob that
	// is the object model; for `file://` it is a gocloud implementation
	// choice (fileblob stages `<name>.<hex-nanos>.tmp` in os.TempDir()),
	// which is exactly the kind of fact that can change under a
	// dependency bump. Assert it against the real driver, both from the
	// store's own List and from the raw directory.
	t.Run("premise: gocloud fileblob stages its in-flight temp outside the bucket", func(t *testing.T) {
		dir := t.TempDir()
		store, err := OpenBlobStore(ctx, fileBlobURL(t, dir), BlobStoreOptions{})
		if err != nil {
			t.Fatalf("OpenBlobStore: %v", err)
		}
		// ContentType is set deliberately: gocloud's blob.Writer buffers
		// the first 512 bytes to sniff a content type and does not build
		// the DRIVER writer until it has them, so a small write with a nil
		// options struct reaches fileblob not at all — which made the
		// first cut of this subtest pass vacuously (caught by its own
		// mutation run). With a content type the driver writer is
		// constructed on the spot.
		w, err := store.bucket.NewWriter(ctx, "chunks/t/000001.jsonl.gz", &blob.WriterOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if _, err := w.Write(bytes.Repeat([]byte("in flight "), 200)); err != nil {
			_ = w.Close()
			t.Fatalf("Write: %v", err)
		}
		// Anti-vacuity for the whole subtest: fileblob's NewTypedWriter
		// MkdirAll's the key's parent before creating its temp, so the
		// directory's existence is the proof that the driver is genuinely
		// mid-write and the two assertions below are looking at a real
		// crash window rather than at nothing having happened.
		if fi, err := os.Stat(filepath.Join(dir, "chunks", "t")); err != nil || !fi.IsDir() {
			_ = w.Close()
			t.Fatalf("fileblob did not create the key's parent directory, so no write is actually in flight — this subtest would be vacuous: %v", err)
		}
		// The write is open and uncommitted — the crash window.
		for _, name := range listedNames(t, store) {
			if strings.HasSuffix(name, ".tmp") {
				_ = w.Close()
				t.Fatalf("fileblob exposed an in-flight temp %q in List — the premise BlobStore's List exemption rests on no longer holds; either filter the driver's shape or re-open the A-6 decision", name)
			}
		}
		var inBucket []string
		if err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			if strings.HasSuffix(d.Name(), ".tmp") {
				inBucket = append(inBucket, p)
			}
			return nil
		}); err != nil {
			_ = w.Close()
			t.Fatalf("walk bucket dir: %v", err)
		}
		if len(inBucket) != 0 {
			_ = w.Close()
			t.Fatalf("fileblob staged its in-flight temp INSIDE the bucket (%v) — a crashed `file://` write would now leave a visible orphan sluice does not sweep; re-open the A-6 decision", inBucket)
		}
		// Anti-vacuity: the writer must genuinely commit, or the two
		// assertions above are satisfied by nothing having happened.
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if got := listedNames(t, store); !slices.Equal(got, []string{"chunks/t/000001.jsonl.gz"}) {
			t.Fatalf("List after commit = %v; want the committed object (the in-flight assertions above were vacuous otherwise)", got)
		}
	})
}
