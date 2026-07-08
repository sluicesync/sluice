// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// failingExistsStore simulates a store whose read path is transiently
// broken at the Exists probe (auth outage, network partition).
type failingExistsStore struct {
	*memStore
	err error
}

func (s *failingExistsStore) Exists(context.Context, string) (bool, error) {
	return false, s.err
}

// failingGetStore simulates a store that claims the root manifest
// exists but fails to deliver its bytes.
type failingGetStore struct {
	*memStore
	err error
}

func (s *failingGetStore) Exists(context.Context, string) (bool, error) { return true, nil }

func (s *failingGetStore) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, s.err
}

// TestChainRootEncryption_ReadFailureIsAnError pins the audit N-6 fix:
// a store read failure while resolving the chain root's encryption
// state must surface as an ERROR, never as the nil that consumers
// interpret as "parent chain is plaintext" — pre-fix that conflation
// let a transient outage silently extend an encrypted chain with
// plaintext chunks (exit 0), or wrongly refuse --encrypt.
func TestChainRootEncryption_ReadFailureIsAnError(t *testing.T) {
	ctx := context.Background()
	// A parent that is itself an incremental (no ChainEncryption), so
	// ChainRootEncryption must consult the store.
	incParent := &irbackup.Manifest{Kind: "incremental"}

	t.Run("failing Exists probe → error", func(t *testing.T) {
		store := &failingExistsStore{memStore: newMemStore(), err: errors.New("transient store outage")}
		enc, err := ChainRootEncryption(ctx, store, incParent)
		if err == nil {
			t.Fatalf("want error on failing store read; got enc=%v err=nil (the N-6 swallow is back)", enc)
		}
		if !strings.Contains(err.Error(), "read chain root manifest") {
			t.Errorf("error %q missing the 'read chain root manifest' context", err.Error())
		}
	})

	t.Run("failing Get → error", func(t *testing.T) {
		store := &failingGetStore{memStore: newMemStore(), err: errors.New("connection reset")}
		if _, err := ChainRootEncryption(ctx, store, incParent); err == nil {
			t.Fatal("want error on failing manifest Get; got nil")
		}
	})

	t.Run("parent carries ChainEncryption → no store read, no error", func(t *testing.T) {
		// The store is broken, but the parent already answers the
		// question — the fast path must not touch the store.
		store := &failingExistsStore{memStore: newMemStore(), err: errors.New("must not be reached")}
		want := &irbackup.ChainEncryption{Algorithm: "AES-256-GCM", KEKMode: "passphrase-argon2id"}
		enc, err := ChainRootEncryption(ctx, store, &irbackup.Manifest{ChainEncryption: want})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if enc != want {
			t.Errorf("enc = %v; want the parent's ChainEncryption", enc)
		}
	})

	t.Run("absent root manifest → (nil, nil) = genuinely plaintext", func(t *testing.T) {
		enc, err := ChainRootEncryption(ctx, newMemStore(), incParent)
		if err != nil {
			t.Fatalf("absent root manifest must stay the plaintext shape, not an error: %v", err)
		}
		if enc != nil {
			t.Errorf("enc = %v; want nil for an absent root manifest", enc)
		}
	})

	t.Run("present plaintext root manifest → (nil, nil)", func(t *testing.T) {
		store := newMemStore()
		root := &irbackup.Manifest{Kind: "full"}
		if err := WriteManifest(ctx, store, root); err != nil {
			t.Fatalf("WriteManifest: %v", err)
		}
		enc, err := ChainRootEncryption(ctx, store, incParent)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if enc != nil {
			t.Errorf("enc = %v; want nil for a plaintext root", enc)
		}
	})
}
