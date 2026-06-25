// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// retryFakeStore is a minimal irbackup.Store whose Get returns a scripted
// sequence of bodies — modelling a flaky object store that truncates a read
// before serving the full object. Only Get is exercised by
// fetchChunkVerified; the rest satisfy the interface.
type retryFakeStore struct {
	bodies [][]byte // one per Get call, in order; last reused if calls exceed len
	getErr []error  // optional per-call error (nil = serve bodies[i])
	calls  int
}

func (s *retryFakeStore) Get(_ context.Context, _ string) (io.ReadCloser, error) {
	i := s.calls
	s.calls++
	if i < len(s.getErr) && s.getErr[i] != nil {
		return nil, s.getErr[i]
	}
	idx := i
	if idx >= len(s.bodies) {
		idx = len(s.bodies) - 1
	}
	return io.NopCloser(bytes.NewReader(s.bodies[idx])), nil
}

func (s *retryFakeStore) Put(context.Context, string, io.Reader) error { return nil }
func (s *retryFakeStore) List(context.Context, string) ([]string, error) {
	return nil, nil
}
func (s *retryFakeStore) Delete(context.Context, string) error         { return nil }
func (s *retryFakeStore) Exists(context.Context, string) (bool, error) { return true, nil }

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h[:])
}

func drain(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read returned reader: %v", err)
	}
	_ = rc.Close()
	return b
}

// TestFetchChunkVerified_RetriesTruncatedRead is the core pin: a store that
// serves a SHORT (truncated) body first and the full body on the second call
// must be retried, returning the complete, SHA-verified bytes. This is the
// live Track-C failure shape — a flaky S3 body that the prior no-retry path
// turned into a whole-restore abort.
func TestFetchChunkVerified_RetriesTruncatedRead(t *testing.T) {
	full := []byte("the full chunk body, gzip plaintext or ciphertext, byte-exact")
	truncated := full[:20]
	want := sha256Hex(full)

	store := &retryFakeStore{bodies: [][]byte{truncated, full}}
	rc, err := fetchChunkVerified(context.Background(), store, "chunks/t/t-0.jsonl.gz", want)
	if err != nil {
		t.Fatalf("fetchChunkVerified: %v", err)
	}
	got := drain(t, rc)
	if !bytes.Equal(got, full) {
		t.Errorf("returned bytes = %q; want the full body", got)
	}
	if store.calls != 2 {
		t.Errorf("store.Get calls = %d; want 2 (one truncated + one good)", store.calls)
	}
}

// TestFetchChunkVerified_HappyPathNoRetry pins that a complete first read
// returns immediately with exactly one Get — the retry must not add latency
// to the overwhelmingly common healthy case.
func TestFetchChunkVerified_HappyPathNoRetry(t *testing.T) {
	full := []byte("complete on first read")
	store := &retryFakeStore{bodies: [][]byte{full}}
	rc, err := fetchChunkVerified(context.Background(), store, "c", sha256Hex(full))
	if err != nil {
		t.Fatalf("fetchChunkVerified: %v", err)
	}
	_ = drain(t, rc)
	if store.calls != 1 {
		t.Errorf("store.Get calls = %d; want 1 (no retry on a healthy read)", store.calls)
	}
}

// TestFetchChunkVerified_PersistentMismatchSurfacesLoudly pins that a chunk
// that is genuinely corrupt at rest — every fetch returns the same wrong
// bytes — is NOT retried away: after the bounded attempts it surfaces a loud
// ErrChunkHashMismatch (never silently accepted). Distinguishes "transient
// truncation" (retry helps) from "real corruption" (must stay loud).
func TestFetchChunkVerified_PersistentMismatchSurfacesLoudly(t *testing.T) {
	corrupt := []byte("wrong bytes at rest")
	wantSHA := sha256Hex([]byte("the bytes the manifest expected"))

	store := &retryFakeStore{bodies: [][]byte{corrupt}}
	_, err := fetchChunkVerified(context.Background(), store, "c", wantSHA)
	if err == nil {
		t.Fatal("expected an error on persistent corruption; got nil")
	}
	if !errors.Is(err, ErrChunkHashMismatch) {
		t.Errorf("error = %v; want wrapped ErrChunkHashMismatch", err)
	}
	if store.calls != chunkFetchMaxAttempts {
		t.Errorf("store.Get calls = %d; want %d (all attempts exhausted)", store.calls, chunkFetchMaxAttempts)
	}
}

// TestFetchChunkVerified_TransientGetErrorRetried pins that a transient error
// from the store's Get (open failure, not just a short body) is retried, then
// succeeds when the object becomes readable.
func TestFetchChunkVerified_TransientGetErrorRetried(t *testing.T) {
	full := []byte("served after a transient open error")
	store := &retryFakeStore{
		bodies: [][]byte{nil, full},
		getErr: []error{errors.New("connection reset by peer"), nil},
	}
	rc, err := fetchChunkVerified(context.Background(), store, "c", sha256Hex(full))
	if err != nil {
		t.Fatalf("fetchChunkVerified: %v", err)
	}
	_ = drain(t, rc)
	if store.calls != 2 {
		t.Errorf("store.Get calls = %d; want 2 (one errored + one good)", store.calls)
	}
}

// TestFetchChunkVerified_CtxCancelStopsRetry pins that a cancelled context is
// honored promptly: with every read truncated, a cancel returns ctx.Err()
// rather than burning all attempts.
func TestFetchChunkVerified_CtxCancelStopsRetry(t *testing.T) {
	full := []byte("never fully served before cancel")
	store := &retryFakeStore{bodies: [][]byte{full[:5]}} // always truncated
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call

	_, err := fetchChunkVerified(ctx, store, "c", sha256Hex(full))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v; want context.Canceled", err)
	}
}

var _ irbackup.Store = (*retryFakeStore)(nil)
