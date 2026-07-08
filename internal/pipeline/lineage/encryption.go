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
// — plus the manifest CARRYING it — when an extending writer
// (incremental / stream) needs to align its envelope. parent's
// ChainEncryption is returned directly when set (the common case:
// parent is a full carrying the chain header). When parent is itself
// an incremental (no ChainEncryption), the chain root manifest is read
// from store and its ChainEncryption is returned.
//
// The carrying manifest is what [UnwrapChainCEK] needs: its RECORDED
// FormatVersion decides whether the chain CEK's wrap is identity-bound
// (ADR-0152), and its identity is the binding.
//
// A (nil, nil, nil) return means the chain is GENUINELY plaintext: the
// root manifest exists and carries no ChainEncryption, or no root
// manifest exists at all ([ReadManifestIfPresent]'s not-found shape).
// A store read/parse failure is an error, never nil — callers branch
// nil = "parent chain is plaintext", so swallowing a transient read
// error here (the pre-audit-N-6 behaviour) either wrongly refused an
// operator's --encrypt with "parent chain is plaintext…" (steering them
// to drop the flag) or, with no --encrypt, silently extended an
// ENCRYPTED chain with plaintext chunks at exit 0.
func ChainRootEncryption(ctx context.Context, store irbackup.Store, parent *irbackup.Manifest) (*irbackup.Manifest, *irbackup.ChainEncryption, error) {
	if parent != nil && parent.ChainEncryption != nil {
		return parent, parent.ChainEncryption, nil
	}
	root, err := ReadManifestIfPresent(ctx, store)
	if err != nil {
		return nil, nil, fmt.Errorf("read chain root manifest: %w", err)
	}
	if root == nil {
		return nil, nil, nil
	}
	return root, root.ChainEncryption, nil
}

// WrapChainCEK wraps cek for the chain whose header manifest `owner`
// records the wrap (the segment full; per-chain mode). The single
// write-side chokepoint for the ADR-0152 CEK identity binding: when
// owner is stamped [irbackup.FormatVersionEncryptedChunkBinding] and
// the envelope supports bound wraps ([crypto.BoundEnvelope]), the wrap
// is bound to owner's identity ([irbackup.CEKBinding]); otherwise it
// is the legacy unbound wrap. Deterministic from (owner.FormatVersion,
// envelope type) on both sides — [UnwrapChainCEK] makes the mirrored
// decision, so the two can never disagree.
//
// Envelopes without the bound surface (Azure Key Vault — WrapKey has
// no context/AAD parameter) wrap unbound even at v5; their chains'
// splice resistance rests entirely on the chunk-level GCM AAD, and
// Azure's rotation hazard is covered by the versioned-KEKRef pin. See
// ADR-0152 for that documented limitation.
func WrapChainCEK(env crypto.EnvelopeEncryption, cek []byte, owner *irbackup.Manifest) ([]byte, error) {
	if bound, ok := env.(crypto.BoundEnvelope); ok {
		if binding := irbackup.CEKBinding(owner); binding != "" {
			return bound.WrapCEKBound(cek, binding)
		}
	}
	return env.WrapCEK(cek)
}

// UnwrapChainCEK is the read-side mirror of [WrapChainCEK] and the
// single chokepoint every chain-CEK unwrap routes through (restore /
// chain-restore / broker / verify preflights, backup-full resume,
// incremental / stream chain extension). Besides the binding decision
// it performs the Azure version retarget: when owner records a KEKRef
// and the envelope implements [crypto.ChainKEKRebinder], the envelope
// is pointed at the recorded (wrap-time) key version BEFORE the unwrap
// — without it, Azure unwraps target the vault's LATEST key version
// and break on rotated keys (audit N-9).
func UnwrapChainCEK(env crypto.EnvelopeEncryption, wrapped []byte, owner *irbackup.Manifest) ([]byte, error) {
	RebindEnvelopeKEK(env, owner)
	if bound, ok := env.(crypto.BoundEnvelope); ok {
		if binding := irbackup.CEKBinding(owner); binding != "" {
			return bound.UnwrapCEKBound(wrapped, binding)
		}
	}
	return env.UnwrapCEK(wrapped)
}

// RebindEnvelopeKEK points env at the exact KEK reference owner's
// ChainEncryption records, for envelopes that need telling
// ([crypto.ChainKEKRebinder]; today Azure Key Vault). No-op for every
// other envelope and for manifests without encryption metadata. Safe
// to call more than once. Exposed separately from [UnwrapChainCEK] for
// the per-chunk-mode paths, which have no chain-level CEK to unwrap
// but still probe/unwrap per-chunk wraps against the envelope.
func RebindEnvelopeKEK(env crypto.EnvelopeEncryption, owner *irbackup.Manifest) {
	if env == nil || owner == nil || owner.ChainEncryption == nil {
		return
	}
	if rb, ok := env.(crypto.ChainKEKRebinder); ok {
		rb.RebindChainKEKRef(owner.ChainEncryption.KEKRef)
	}
}

// ResolvedKEKRef returns the more-precise KEK reference env reports
// after wrapping ([crypto.ResolvedKEKReferencer] — Azure's versioned
// key URL), or fallback when the envelope has none. The orchestrator
// records the result in [irbackup.ChainEncryption.KEKRef].
func ResolvedKEKRef(env crypto.EnvelopeEncryption, fallback string) string {
	if r, ok := env.(crypto.ResolvedKEKReferencer); ok {
		if ref := r.ResolvedKEKRef(); ref != "" {
			return ref
		}
	}
	return fallback
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
