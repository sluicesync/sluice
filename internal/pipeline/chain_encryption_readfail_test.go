// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// readFailStore simulates a transiently unreadable backup store: every
// read-path probe fails. Writes are irrelevant — alignEncryption must
// abort before any chunk is written.
type readFailStore struct {
	*memStore
	err error
}

func (s *readFailStore) Exists(context.Context, string) (bool, error) {
	return false, s.err
}

func cheapTestEnvelope(t *testing.T) crypto.EnvelopeEncryption {
	t.Helper()
	env, err := crypto.NewPassphraseEnvelope("pass", crypto.Argon2idParams{
		Salt:        []byte("abcdefghijklmnop"),
		Memory:      1024,
		Iterations:  1,
		Parallelism: 1,
		KeyLen:      crypto.KEKLen,
	})
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	return env
}

// TestAlignEncryption_RootManifestReadFailureAborts pins the audit N-6
// consumer contract for BOTH extending writers (incremental + stream)
// × BOTH operator intents (--encrypt and plaintext): when the chain
// root manifest cannot be read, alignEncryption must abort loudly
// instead of branching on "parent chain is plaintext". Pre-fix the
// plaintext-intent path silently extended an encrypted chain with
// plaintext chunks (exit 0), and the --encrypt path emitted a wrong
// refusal steering the operator to DROP --encrypt.
func TestAlignEncryption_RootManifestReadFailureAborts(t *testing.T) {
	ctx := context.Background()
	// A parent that is itself an incremental (no ChainEncryption), so
	// the alignment must consult the chain root on the store.
	incParent := &irbackup.Manifest{Kind: "incremental"}
	failing := func() *readFailStore {
		return &readFailStore{memStore: newMemStore(), err: errors.New("transient store outage")}
	}

	align := map[string]func(enc *lineage.BackupEncryption, store irbackup.Store) ([]byte, error){
		"incremental": func(enc *lineage.BackupEncryption, store irbackup.Store) ([]byte, error) {
			b := &IncrementalBackup{segStore: store, Encryption: enc}
			return b.alignEncryption(ctx, incParent)
		},
		"stream": func(enc *lineage.BackupEncryption, store irbackup.Store) ([]byte, error) {
			b := &BackupStream{segStore: store, Encryption: enc}
			return b.alignEncryption(ctx, incParent)
		},
	}

	for name, alignFn := range align {
		alignFn := alignFn
		t.Run(name, func(t *testing.T) {
			t.Run("no --encrypt + failing read → abort (NOT silent plaintext extension)", func(t *testing.T) {
				_, err := alignFn(nil, failing())
				if err == nil {
					t.Fatal("alignEncryption returned nil on a failed root-manifest read; an encrypted chain would be silently extended with plaintext chunks")
				}
				if !strings.Contains(err.Error(), "cannot determine parent chain encryption state") {
					t.Errorf("error %q missing the 'cannot determine parent chain encryption state' context", err.Error())
				}
			})

			t.Run("--encrypt + failing read → abort (NOT the misleading plaintext refusal)", func(t *testing.T) {
				enc := &lineage.BackupEncryption{Envelope: cheapTestEnvelope(t)}
				_, err := alignFn(enc, failing())
				if err == nil {
					t.Fatal("alignEncryption returned nil on a failed root-manifest read with --encrypt")
				}
				if strings.Contains(err.Error(), "parent chain is plaintext") {
					t.Errorf("error %q is the misleading plaintext refusal — it steers the operator to drop --encrypt on a chain whose state is UNKNOWN", err.Error())
				}
				if !strings.Contains(err.Error(), "cannot determine parent chain encryption state") {
					t.Errorf("error %q missing the 'cannot determine parent chain encryption state' context", err.Error())
				}
			})

			t.Run("absent root manifest still behaves as plaintext", func(t *testing.T) {
				// Genuinely-absent root (ReadManifestIfPresent's
				// not-found → nil, nil): the pre-existing plaintext
				// semantics must be preserved.
				cek, err := alignFn(nil, newMemStore())
				if err != nil {
					t.Fatalf("plaintext chain + no encryption config must align cleanly: %v", err)
				}
				if cek != nil {
					t.Errorf("cek = %v; want nil for a plaintext chain", cek)
				}

				enc := &lineage.BackupEncryption{Envelope: cheapTestEnvelope(t)}
				_, err = alignFn(enc, newMemStore())
				if err == nil || !strings.Contains(err.Error(), "parent chain is plaintext") {
					t.Errorf("want the plaintext-chain --encrypt refusal for a genuinely absent root; got %v", err)
				}
			})
		})
	}
}
