// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// BackupEncryption is the chunk-writer-side encryption configuration
// shared by [Backup], [IncrementalBackup], and [BackupStream]. Nil
// means plaintext (the v0.16.x..v0.21.x shape, preserved for backward
// compatibility); non-nil means every chunk written by this run is
// encrypted under the supplied envelope.
//
// The orchestrator generates the per-chain CEK on first use (per-chain
// mode; the default), wraps it via the envelope, and records the
// wrapped CEK + Argon2id params (passphrase mode) in the chain
// manifest's [irbackup.ChainEncryption] field. Per-chunk mode generates a
// fresh CEK + wrap per chunk; the wrapped CEK lands in
// [irbackup.ChunkEncryption.WrappedCEK].
type BackupEncryption struct {
	// Envelope is the [crypto.EnvelopeEncryption] implementation used
	// to wrap CEKs. Phase 6.1: a *crypto.PassphraseEnvelope. Required
	// when the parent struct's encryption is enabled.
	//
	// Cold-start path: the orchestrator uses Envelope as-is to wrap a
	// fresh chain CEK and stamps the envelope's params on the chain
	// root's [irbackup.ChainEncryption].
	//
	// Chain-extension path: when the orchestrator detects an existing
	// chain root (or in-progress full's prior manifest) carrying
	// recorded [irbackup.Argon2idParams], it rebuilds the envelope via
	// [BackupEncryption.RebuildForChain] (when supplied) so the KEK
	// derives against the chain's salt rather than a freshly-minted
	// one. Without RebuildForChain, the orchestrator uses Envelope
	// as-is — correct for tests that build envelopes with a known
	// salt, broken for production CLI calls that mint fresh salts.
	// Bug 43 (v0.22.1): closes the gap by routing CLI passphrase
	// envelopes through RebuildForChain.
	Envelope crypto.EnvelopeEncryption

	// RebuildForChain, when non-nil, is called by the orchestrator
	// when extending an existing encrypted chain (incremental / stream
	// against a chain with recorded Argon2id params, or backup-full
	// resume against an in-progress encrypted manifest). The supplied
	// params are the chain root's recorded [irbackup.Argon2idParams] (the
	// salt that was used to derive the chain's KEK). Implementations
	// should rebuild a [crypto.EnvelopeEncryption] tied to that salt
	// + the operator's passphrase / KMS key.
	//
	// Returning a non-nil error aborts the orchestrator's startup
	// loudly (e.g. wrong passphrase shape).
	//
	// Phase 6.1: passphrase mode populates this with a closure over
	// the operator's passphrase. KMS modes (Phase 6.2/6.3) leave it
	// nil — KMS unwrap doesn't depend on a chain-recorded salt.
	RebuildForChain func(parentParams *irbackup.Argon2idParams) (crypto.EnvelopeEncryption, error)

	// Mode is "per-chain" (default) or "per-chunk". See
	// `docs/dev/design/logical-backups-phase-6.md` for the trade-off.
	Mode string

	// KEKRef is the operator-visible reference recorded in
	// [irbackup.ChainEncryption.KEKRef]. Empty for passphrase mode (the
	// salt + Argon2id params are the reference); KMS modes record the
	// key ARN / resource name.
	KEKRef string
}

// RebindForChain rebuilds the encryption envelope against the parent
// chain's recorded Argon2id params and swaps it onto the receiver. A
// no-op when params are nil or RebuildForChain is unset; callers fall
// through to the cold-start envelope in that case.
//
// Bug 43 fix: the write-side previously built the envelope with a
// fresh-minted Argon2id salt, so unwrapping the parent chain's
// WrappedCEK (which was sealed under the parent's salt) failed with
// `aes-gcm open: cipher: message authentication failed`. This helper
// is the load-bearing mirror of the read-side
// [EncryptionFlags.buildReadEnvelope] pattern: detect chain extension
// via recorded Argon2id params, rebuild the envelope tied to those
// params before any CEK unwrap.
func (e *BackupEncryption) RebindForChain(parentParams *irbackup.Argon2idParams) error {
	if e == nil || parentParams == nil || e.RebuildForChain == nil {
		return nil
	}
	env, err := e.RebuildForChain(parentParams)
	if err != nil {
		return err
	}
	e.Envelope = env
	return nil
}

// ChainRootEncryption returns the chain-root's [irbackup.ChainEncryption]
// when an extending writer (incremental / stream) needs to align its
// envelope. parent's ChainEncryption is returned directly when set
// (the common case: parent is a full carrying the chain header).
// When parent is itself an incremental (no ChainEncryption), the
// chain root manifest is read from store and its ChainEncryption is
// returned.
//
// Read errors are swallowed (returns nil) — the alignment logic
// already handles a nil ChainEncryption shape gracefully and a noisy
// store read at this point would mask the simpler "parent is
// plaintext" path.
func ChainRootEncryption(ctx context.Context, store irbackup.Store, parent *irbackup.Manifest) *irbackup.ChainEncryption {
	if parent != nil && parent.ChainEncryption != nil {
		return parent.ChainEncryption
	}
	root, err := ReadManifestIfPresent(ctx, store)
	if err != nil || root == nil {
		return nil
	}
	return root.ChainEncryption
}

// ProbeChunkDecrypt attempts to unwrap a per-chunk WrappedCEK using the
// supplied envelope. No-op when the envelope is nil (no decrypt probe
// requested), the chunk is plaintext (no Encryption metadata), or the
// chunk is per-chain-mode (empty WrappedCEK; the chain root's probe
// already covered it). Returns a wrapping error on unwrap failure so
// the caller can surface "wrong passphrase for THIS chunk" — the
// Bug 117 signal.
func ProbeChunkDecrypt(env crypto.EnvelopeEncryption, chunk *irbackup.ChunkInfo) error {
	if env == nil || chunk == nil || chunk.Encryption == nil {
		return nil
	}
	if len(chunk.Encryption.WrappedCEK) == 0 {
		return nil
	}
	if _, err := env.UnwrapCEK(chunk.Encryption.WrappedCEK); err != nil {
		return fmt.Errorf("unwrap chunk cek (passphrase rotated mid-chain?): %w", err)
	}
	return nil
}
