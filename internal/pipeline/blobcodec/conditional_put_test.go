// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// Pins for the [irbackup.ConditionalPutter] capability (ADR-0161): the
// create-only conditional write both store backends expose so the chain
// concurrent-writer guard can claim write-generations. Exactly-one-
// winner semantics + the [irbackup.ErrPathExists] error contract are
// pinned per backend — LocalStore's O_EXCL and BlobStore's gocloud
// IfNotExist→gcerrors.FailedPrecondition mapping are DIFFERENT wire
// paths even though the caller code is identical (pin the class, not
// the representative). The real S3 conditional PUT is pinned against
// MinIO in internal/pipeline/blob_store_integration_test.go.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// conditionalPutContract exercises the shared PutIfAbsent contract
// against any capable store: first create wins and lands its content,
// the second fails with ErrPathExists and leaves the winner untouched,
// and an unrelated key is unaffected.
func conditionalPutContract(t *testing.T, store irbackup.Store) {
	t.Helper()
	cp, ok := store.(irbackup.ConditionalPutter)
	if !ok {
		t.Fatalf("%T does not implement irbackup.ConditionalPutter", store)
	}
	ctx := context.Background()

	first := []byte(`{"claimed_at":"first"}`)
	if err := cp.PutIfAbsent(ctx, "lineage.gen/g-00000000000000000001", bytes.NewReader(first)); err != nil {
		t.Fatalf("first PutIfAbsent: %v", err)
	}
	err := cp.PutIfAbsent(ctx, "lineage.gen/g-00000000000000000001", bytes.NewReader([]byte(`{"claimed_at":"second"}`)))
	if err == nil {
		t.Fatal("second PutIfAbsent = nil; want ErrPathExists (the slot was taken)")
	}
	if !errors.Is(err, irbackup.ErrPathExists) {
		t.Fatalf("second PutIfAbsent = %v; want an error wrapping irbackup.ErrPathExists", err)
	}

	// The winner's content is untouched by the losing attempt.
	rc, err := store.Get(ctx, "lineage.gen/g-00000000000000000001")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, first) {
		t.Errorf("content = %q; want the first writer's %q", got, first)
	}

	// A different key is a fresh slot.
	if err := cp.PutIfAbsent(ctx, "lineage.gen/g-00000000000000000002", bytes.NewReader([]byte(`{}`))); err != nil {
		t.Errorf("PutIfAbsent on a fresh key: %v", err)
	}
}

// TestLocalStore_PutIfAbsent pins the O_EXCL-based local-FS leg.
func TestLocalStore_PutIfAbsent(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	conditionalPutContract(t, store)
}

// TestLocalStore_PutIfAbsent_RejectsTraversal keeps the path-traversal
// posture consistent with Put.
func TestLocalStore_PutIfAbsent_RejectsTraversal(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := store.PutIfAbsent(context.Background(), "../escape", bytes.NewReader(nil)); err == nil {
		t.Error("PutIfAbsent(../escape) = nil; want a traversal rejection")
	}
}

// TestBlobStore_PutIfAbsent_FileBlob pins the gocloud leg through the
// REAL fileblob driver (WriterOptions.IfNotExist → the driver's own
// exclusive create → gcerrors.FailedPrecondition → ErrPathExists). The
// s3blob leg of the same mapping is pinned against a real MinIO in the
// integration suite.
func TestBlobStore_PutIfAbsent_FileBlob(t *testing.T) {
	store, err := OpenBlobStore(context.Background(), fileBlobURL(t, t.TempDir()), BlobStoreOptions{})
	if err != nil {
		t.Fatalf("OpenBlobStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	conditionalPutContract(t, store)
}

// TestBlobStore_PutIfAbsent_MemBlob pins the same contract through the
// memblob driver — a second, independent gocloud codepath for the
// IfNotExist option (per-driver preconditions differ even when the
// sluice-side code is byte-identical).
func TestBlobStore_PutIfAbsent_MemBlob(t *testing.T) {
	store, err := OpenBlobStore(context.Background(), "mem://", BlobStoreOptions{})
	if err != nil {
		t.Fatalf("OpenBlobStore(mem://): %v", err)
	}
	defer func() { _ = store.Close() }()
	conditionalPutContract(t, store)
}
