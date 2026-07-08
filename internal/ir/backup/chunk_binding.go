// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"strconv"
	"time"
)

// Chunk-position and CEK identity bindings for encrypted backups
// (ADR-0152, audit N-8/N-9).
//
// Per-chain encryption shares ONE CEK across every chunk in a chain,
// so before FormatVersion 5 any valid ciphertext chunk decrypted
// cleanly wherever a rewritten manifest pointed it — splice, replay,
// and reorder on a shared store were undetectable. FormatVersion-5
// manifests close that: each chunk's AES-GCM AAD binds it to the
// identity of the manifest that records it PLUS its recorded path, and
// the chain CEK's wrap is bound to the manifest identity (KMS
// EncryptionContext / GCP AAD / passphrase-wrap AAD).
//
// The identity is the (CreatedAt, SourceEngine, Kind) prefix of
// [ComputeBackupID] — deliberately NOT the BackupID itself: BackupID
// hashes EndPosition, which for incrementals is only known when the
// window CLOSES, after every chunk has been written. The prefix is
// fixed at manifest construction, preserved verbatim by every rewrite
// path (full-backup resume adopts the prior CreatedAt; compaction
// copies manifests with their identity intact and re-stitches only
// ParentBackupID, which the binding excludes for exactly that reason),
// and unique per backup within a store in practice (the change-chunk
// run namespace already keys on CreatedAt — Bug 35).
//
// Honesty note: the binding authenticates chunks AGAINST the manifest
// that names them. The manifest itself is unsigned, so presenting a
// complete older (manifest + chunks) pair — a whole-backup rollback —
// remains possible; ADR-0152 documents that boundary.

// chunkAADPrefix / cekBindingPrefix version the binding derivations
// independently of the manifest FormatVersion. Part of the on-disk
// contract for FormatVersion-5+ chunks; changing either strands every
// chunk written since (a change requires a new FormatVersion with its
// own derivation).
const (
	chunkAADPrefix   = "sluice-chunk-aad/v1\n"
	cekBindingPrefix = "sluice-cek-binding/v1\n"
)

// timeRFC3339Nano aliases the stdlib layout so the derivation above
// reads as the contract it is (and stays greppable next to
// [ComputeBackupID]'s use of the same layout).
const timeRFC3339Nano = time.RFC3339Nano

// bindingIdentity renders the manifest-identity lines shared by
// [ChunkAAD] and [CEKBinding]. Field order is part of the on-disk
// contract; do not reorder. Mirrors [ComputeBackupID]'s rendering of
// the same fields so the two identities can never disagree in format.
func bindingIdentity(m *Manifest) string {
	return "created_at=" + m.CreatedAt.UTC().Format(timeRFC3339Nano) +
		"\nsource_engine=" + m.SourceEngine +
		"\nkind=" + canonicalKind(m.Kind)
}

// ChunkAAD returns the AES-GCM additional-authenticated-data a chunk
// at the given manifest-recorded path carries — or nil when the
// manifest predates [FormatVersionEncryptedChunkBinding], whose chunks
// were written unbound and MUST decrypt via the legacy nil-AAD path.
// This function is the single version gate for both sides: writers
// call it with the freshly-stamped manifest (v5 when encryption is
// on), readers with the manifest as read from the store, so the shape
// is always derived from the RECORDED version and never guessed.
//
// file is the chunk's manifest-recorded path ([ChunkInfo.File]) —
// segment-relative, exactly as stored, which is what keeps the binding
// stable when compaction relocates a whole segment directory.
func ChunkAAD(m *Manifest, file string) []byte {
	if m == nil || m.FormatVersion < FormatVersionEncryptedChunkBinding {
		return nil
	}
	return []byte(chunkAADPrefix + bindingIdentity(m) + "\nfile=" + file)
}

// ChunkAADFor is the READ-side form of [ChunkAAD]: it additionally
// gates on the chunk's own recorded encryption metadata, because only
// encrypted chunks carry a GCM binding — a plaintext chunk under a
// v5+ manifest (hand-assembled today; possible for real once a future
// FormatVersion covers a plaintext feature) has no ciphertext to bind.
// Writers use [ChunkAAD] directly (they know they are encrypting);
// readers use this so the shape is derived from what the manifest
// RECORDS about each chunk. Stripping the Encryption field off a
// bound chunk's manifest entry doesn't downgrade anything silently:
// the reader then treats ciphertext as a plaintext codec stream and
// fails loudly at the codec header.
func ChunkAADFor(m *Manifest, c *ChunkInfo) []byte {
	if c == nil || c.Encryption == nil {
		return nil
	}
	return ChunkAAD(m, c.File)
}

// ChangeChunkAAD is [ChunkAAD] for CHANGE chunks, which additionally
// bind their ordinal in [Manifest.ChangeChunks]. Row chunks don't:
// a table's rows are a set (chunk order is not semantic, and the
// ADR-0149 range workers legitimately append entries out of index
// order), so the path binding suffices there. Change-chunk REPLAY
// ORDER is semantic — swapping two entries (each with its intact
// File+SHA) would replay events out of source order, a silent-
// corruption class the path binding alone cannot catch because each
// ciphertext still sits at its own recorded path. The ordinal is the
// list position, which for change chunks always equals the writer's
// chunk index (single-threaded appends; compaction rewrites in place,
// preserving both paths and order).
func ChangeChunkAAD(m *Manifest, file string, index int) []byte {
	base := ChunkAAD(m, file)
	if base == nil {
		return nil
	}
	return append(base, []byte("\nindex="+strconv.Itoa(index))...)
}

// ChangeChunkAADFor is the READ-side form of [ChangeChunkAAD], gated
// on the chunk's recorded encryption metadata like [ChunkAADFor].
func ChangeChunkAADFor(m *Manifest, c *ChunkInfo, index int) []byte {
	if c == nil || c.Encryption == nil {
		return nil
	}
	return ChangeChunkAAD(m, c.File, index)
}

// CEKBinding returns the identity string a FormatVersion-5+ manifest's
// chain-CEK wrap is bound to ([crypto.BoundEnvelope]), or "" for older
// manifests whose CEKs were wrapped unbound. Same single-gate contract
// as [ChunkAAD]: the manifest that RECORDS the wrap (the segment full
// for per-chain mode) decides the shape on both wrap and unwrap.
func CEKBinding(m *Manifest) string {
	if m == nil || m.FormatVersion < FormatVersionEncryptedChunkBinding {
		return ""
	}
	return cekBindingPrefix + bindingIdentity(m)
}
