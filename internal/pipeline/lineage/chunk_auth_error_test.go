// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"errors"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestCodeChunkAuthError pins the SEC-1 mapping: a chunk auth failure
// (crypto.ErrChunkAuthFailed anywhere in the chain) becomes the coded
// SLUICE-E-BACKUP-CHUNK-AUTH-FAILED Refusal (exit 3), while nil and
// non-auth errors pass through byte-identical so a wrong key, SHA
// mismatch, or I/O error keeps its own shape and exit.
func TestCodeChunkAuthError(t *testing.T) {
	t.Run("nil passes through", func(t *testing.T) {
		if got := CodeChunkAuthError(nil); got != nil {
			t.Errorf("nil should map to nil; got %v", got)
		}
	})

	t.Run("auth failure maps to the coded refusal", func(t *testing.T) {
		// Wrapped a couple layers deep, as the restore layer hands it over.
		in := fmt.Errorf("open chunk reader: %w", fmt.Errorf("blobcodec: %w", crypto.ErrChunkAuthFailed))
		out := CodeChunkAuthError(in)
		ce, ok := sluicecode.FromError(out)
		if !ok {
			t.Fatalf("mapped error is not coded: %v", out)
		}
		if ce.Code != sluicecode.CodeBackupChunkAuthFailed {
			t.Errorf("code = %s; want %s", ce.Code, sluicecode.CodeBackupChunkAuthFailed)
		}
		if ce.ExitCode() != sluicecode.ExitRefusal {
			t.Errorf("exit = %d; want %d (ExitRefusal)", ce.ExitCode(), sluicecode.ExitRefusal)
		}
		// The original chain stays traversable.
		if !errors.Is(out, crypto.ErrChunkAuthFailed) {
			t.Errorf("coded error dropped the ErrChunkAuthFailed chain")
		}
	})

	t.Run("non-auth error passes through unchanged", func(t *testing.T) {
		in := errors.New("resolve chunk cek: wrong passphrase")
		out := CodeChunkAuthError(in)
		if !errors.Is(out, in) {
			t.Errorf("non-auth error should pass through identically; got %v", out)
		}
		if _, ok := sluicecode.FromError(out); ok {
			t.Errorf("non-auth error must NOT gain a code")
		}
	})

	t.Run("a real wrong-key CEK-unwrap error is NOT coded as chunk tamper", func(t *testing.T) {
		// The REALISTIC wrong-passphrase shape (F-1): a CEK-unwrap failure
		// wraps crypto.ErrCEKUnwrapFailed, which is disjoint from
		// ErrChunkAuthFailed — so even if such an error reached the mapper it
		// must pass through uncoded, never relabeled as "spliced/reordered
		// store" tamper. (The earlier fixture used a plain string that did not
		// wrap any sentinel, giving false confidence; this exercises the exact
		// error content the wrong-key path now produces.)
		in := fmt.Errorf("resolve chunk cek: %w", crypto.ErrCEKUnwrapFailed)
		out := CodeChunkAuthError(in)
		if !errors.Is(out, in) {
			t.Errorf("CEK-unwrap error should pass through identically; got %v", out)
		}
		if errors.Is(out, crypto.ErrChunkAuthFailed) {
			t.Errorf("a CEK-unwrap error must not carry ErrChunkAuthFailed")
		}
		if _, ok := sluicecode.FromError(out); ok {
			t.Errorf("a wrong-key CEK-unwrap error must NOT gain the chunk-auth code")
		}
	})
}
