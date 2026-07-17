// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// Pins for the [irbackup.ConditionalPutter] capability (ADR-0160): the
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
	"fmt"
	"io"
	"testing"

	smithy "github.com/aws/smithy-go"

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

// s3ConflictErr fabricates the wire shape a truly-simultaneous S3
// conditional-PUT loser surfaces: a smithy APIError with code
// ConditionalRequestConflict, wrapped the way gocloud propagates driver
// errors (unmapped — NOT FailedPrecondition).
func s3ConflictErr() error {
	return fmt.Errorf("blob (key \"lineage.gen/g-1\") (code=Unknown): %w",
		&smithy.GenericAPIError{Code: "ConditionalRequestConflict", Message: "A conflicting conditional operation is currently in progress against this resource."})
}

// s3PreconditionErr fabricates the wire shape an S3 conditional-PUT
// loser surfaces when the key already exists: a smithy APIError with code
// PreconditionFailed (HTTP 412), wrapped the way gocloud propagates
// driver errors — and crucially with a RequestID containing the substring
// "301" (the exact shape observed on the v0.99.268 tag CI, RequestID
// 18C30130E2747EAB) that trips gocloud v0.46.0's s3blob ErrorCode
// "301"-substring hack into misclassifying the 412 as NoSuchBucket →
// gcerrors.NotFound.
func s3PreconditionErr() error {
	return fmt.Errorf("operation error S3: PutObject, https response error StatusCode: 412, RequestID: 18C30130E2747EAB: %w",
		&smithy.GenericAPIError{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold"})
}

// TestIsPreconditionFailed pins the create-only CAS-loser classification
// against gocloud v0.46.0's s3blob "301"-substring misfire. The 412
// PreconditionFailed must be recognised via the AUTHORITATIVE smithy API
// error code, not gocloud's derived gcerrors code — which this shape
// (RequestID containing "301") misclassifies as NotFound ~2% of the time,
// silently turning the chain-guard conflict into a confusing "not found".
// The gcerrors.FailedPrecondition fallback (fileblob/memblob, no smithy
// error) is covered end-to-end by the driver contract tests above.
func TestIsPreconditionFailed(t *testing.T) {
	if !isPreconditionFailed(s3PreconditionErr()) {
		t.Error("wrapped 412 PreconditionFailed (RequestID with \"301\") not recognised as the CAS loser")
	}
	// The 409 simultaneous-writer conflict is a DIFFERENT shape, handled
	// by the retry path — not the 412→ErrPathExists mapping.
	if isPreconditionFailed(s3ConflictErr()) {
		t.Error("409 ConditionalRequestConflict misclassified as the 412 loser")
	}
	// Unrelated errors are not the loser.
	if isPreconditionFailed(&smithy.GenericAPIError{Code: "NoSuchBucket"}) {
		t.Error("NoSuchBucket misclassified as the 412 loser")
	}
	if isPreconditionFailed(errors.New("PreconditionFailed")) {
		t.Error("plain-string match must not classify (smithy APIError or gcerrors only)")
	}
}

// TestConditionalPutConflictRetry pins the audit 2026-07-16 S3-probe
// follow-up: S3's 409 ConditionalRequestConflict routes to the
// CONFLICT outcome (ErrPathExists → the chain guard's coded refusal),
// never the degrade-WARN path — with one retry, because a 409 writes
// nothing and the racer that caused it may itself have failed.
func TestConditionalPutConflictRetry(t *testing.T) {
	t.Run("409 twice → ErrPathExists (conflict, not degrade)", func(t *testing.T) {
		calls := 0
		err := conditionalPutWithConflictRetry("p", func() error { calls++; return s3ConflictErr() })
		if !errors.Is(err, irbackup.ErrPathExists) {
			t.Fatalf("err = %v; want an ErrPathExists-wrapping conflict", err)
		}
		if calls != 2 {
			t.Errorf("attempts = %d; want exactly 2 (one retry)", calls)
		}
	})

	t.Run("409 then clean → won on retry", func(t *testing.T) {
		calls := 0
		err := conditionalPutWithConflictRetry("p", func() error {
			calls++
			if calls == 1 {
				return s3ConflictErr()
			}
			return nil
		})
		if err != nil || calls != 2 {
			t.Fatalf("err = %v calls = %d; want nil after the single retry", err, calls)
		}
	})

	t.Run("409 then 412-mapped → ErrPathExists (the racer landed)", func(t *testing.T) {
		calls := 0
		err := conditionalPutWithConflictRetry("p", func() error {
			calls++
			if calls == 1 {
				return s3ConflictErr()
			}
			return fmt.Errorf("blob store: %q: %w", "p", irbackup.ErrPathExists)
		})
		if !errors.Is(err, irbackup.ErrPathExists) {
			t.Fatalf("err = %v; want ErrPathExists", err)
		}
	})

	t.Run("non-conflict errors pass through with NO retry (the degrade class)", func(t *testing.T) {
		calls := 0
		sentinel := errors.New("NotImplemented: If-None-Match not supported")
		err := conditionalPutWithConflictRetry("p", func() error { calls++; return sentinel })
		if !errors.Is(err, sentinel) || errors.Is(err, irbackup.ErrPathExists) {
			t.Fatalf("err = %v; want the raw error (degrade stays available for genuinely unsupporting stores)", err)
		}
		if calls != 1 {
			t.Errorf("attempts = %d; want 1 (no retry for non-409 failures)", calls)
		}
	})

	t.Run("classifier matches the code through wrapping, nothing else", func(t *testing.T) {
		if !isConditionalRequestConflict(s3ConflictErr()) {
			t.Error("wrapped ConditionalRequestConflict not recognised")
		}
		if isConditionalRequestConflict(&smithy.GenericAPIError{Code: "PreconditionFailed"}) {
			t.Error("PreconditionFailed misclassified as the 409 conflict")
		}
		if isConditionalRequestConflict(errors.New("ConditionalRequestConflict")) {
			t.Error("plain-string match must not classify (smithy APIError only)")
		}
	})
}
