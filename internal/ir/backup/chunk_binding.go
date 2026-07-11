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
// that names them. The manifest itself is unsigned, so a store adversary
// who edits it self-consistently is not caught by the chunk bindings —
// ADR-0152 documents this boundary, of which there are three named shapes:
//
//   - Whole-backup rollback: presenting a complete older (manifest +
//     chunks) pair. Recoverable (older-but-coherent state).
//   - Partial change-chunk tail-truncation: deleting the tail entries of
//     an unsigned incremental's ChangeChunks list. Survivors keep ordinals
//     0..k so every GCM AAD still validates, and restore/broker would
//     return exit 0 with fewer events while the intact EndPosition
//     overstates the data — poisoning a resumed CDC stream (a resume STARTS
//     past the lost events, unrecoverable). The restore/broker replay path
//     now backstops this by asserting the applied tail REACHES EndPosition
//     (SLUICE-E-BACKUP-INCOMPLETE), converting the unrecoverable variant
//     into a loud refusal; a fully-coherent edit that ALSO lowers
//     EndPosition stays the recoverable rollback above.
//   - Coherent two-sided position rewrite: moving a link's EndPosition
//     forward AND the next link's StartPosition to match. All chunks still
//     apply (positions don't gate data), but the forged EndPosition is the
//     CDC resume anchor, so a post-restore position-from-manifest resume
//     skips past the gap. Not caught by the tail-reach backstop (the tail
//     DOES reach the forged EndPosition).
//
// Signing (ADR-0154) folds the change-chunk ordinal + count + positions
// into the signed canon and closes all three; these are the unsigned
// residual, and --require-signature is the operator's opt-in to refuse an
// unsigned chain outright.

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

// ChunkAAD returns the BASE AES-GCM additional-authenticated-data a chunk
// at the given manifest-recorded path carries — manifest identity + path —
// or nil when the manifest predates [FormatVersionEncryptedChunkBinding],
// whose chunks were written unbound and MUST decrypt via the legacy
// nil-AAD path. This function is the single VERSION gate for both sides:
// writers call it with the freshly-stamped manifest, readers with the
// manifest as read from the store, so the shape is always derived from the
// RECORDED version and never guessed.
//
// It is the shared base for BOTH chunk kinds and deliberately carries NO
// parent-table field: change chunks ([ChangeChunkAAD]) are manifest-scoped
// (they bind a replay ordinal, not a table), and row chunks fold their
// parent table on top via [ChunkAADFor] / [ChunkAADForWrite] (SEC-F1,
// gated at [FormatVersionChunkTableBinding]). Row-chunk callers MUST use
// those wrappers, never this base directly, or a chunk reassigned between
// two same-column-set tables would decrypt into the wrong one.
//
// It does NOT gate on encryption — an incremental's manifest records no
// ChainEncryption of its own (the chain CEK lives on the segment full),
// so an encryption gate here would strip the binding off encrypted
// change chunks. The ENCRYPTION gate is the caller's: writers pass this
// AAD only when they hold a CEK ([ChunkAADForWrite]); readers gate on
// the chunk's recorded [ChunkInfo.Encryption] ([ChunkAADFor]). Since
// ADR-0154 Phase 2 a plaintext v6 (Ed25519-signed) backup has v6 but no
// CEK, so those caller-side gates — not the version — are what keep a
// plaintext chunk unbound (its signature covers SHA + position instead).
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

// rowChunkTableBinding is the parent-table suffix a ROW chunk's AAD carries
// at [FormatVersionChunkTableBinding]+ (SEC-F1): the (schema, table) that
// lists the chunk, so a chunk reassigned between two same-column-set tables
// on a shared store fails to decrypt (its GCM tag was computed against the
// ORIGINAL table's AAD). Empty for pre-v7 manifests, so their row-chunk
// AAD stays byte-identical to what shipped — the on-disk contract that
// keeps every v5/v6 chain decryptable. Uses the same `\nkey=value` framing
// as [bindingIdentity] and [ChangeChunkAAD]'s `\nindex=`.
func rowChunkTableBinding(m *Manifest, schema, table string) string {
	if m == nil || m.FormatVersion < FormatVersionChunkTableBinding {
		return ""
	}
	return "\nschema=" + schema + "\ntable=" + table
}

// ChunkAADForWrite is the WRITE-side gate for a ROW chunk's AAD: it returns
// the position binding (manifest identity + path, plus the parent table at
// v7+) only when the chunk is actually being encrypted (a non-nil CEK). A
// plaintext chunk — including one under a v6 (Ed25519-signed) manifest —
// has no ciphertext to bind, so it must be written with a nil AAD (the
// chunk writer refuses an AAD without a CEK). The manifest signature covers
// a plaintext chunk's SHA + position + parent table (canon v4).
func ChunkAADForWrite(m *Manifest, file, schema, table string, cek []byte) []byte {
	if len(cek) == 0 {
		return nil
	}
	base := ChunkAAD(m, file)
	if base == nil {
		return nil
	}
	return append(base, rowChunkTableBinding(m, schema, table)...)
}

// ChangeChunkAADForWrite is [ChunkAADForWrite] for change chunks (adds the
// replay ordinal). nil for a plaintext (no-CEK) change chunk.
func ChangeChunkAADForWrite(m *Manifest, file string, index int, cek []byte) []byte {
	if len(cek) == 0 {
		return nil
	}
	return ChangeChunkAAD(m, file, index)
}

// ChunkAADFor is the READ-side form of a ROW chunk's AAD: it additionally
// gates on the chunk's own recorded encryption metadata, because only
// encrypted chunks carry a GCM binding — a plaintext chunk under a
// v5+ manifest (hand-assembled today; possible for real once a future
// FormatVersion covers a plaintext feature) has no ciphertext to bind.
// schema/table are the chunk's PARENT table (folded at v7+, SEC-F1); the
// caller supplies them from the table whose [TableManifest.Chunks] the
// chunk belongs to — the reader iterates tables → chunks, so the parent is
// always in hand. Writers use [ChunkAADForWrite]; readers use this so the
// shape is derived from what the manifest RECORDS about each chunk.
// Stripping the Encryption field off a bound chunk's manifest entry doesn't
// downgrade anything silently: the reader then treats ciphertext as a
// plaintext codec stream and fails loudly at the codec header.
func ChunkAADFor(m *Manifest, c *ChunkInfo, schema, table string) []byte {
	if c == nil || c.Encryption == nil {
		return nil
	}
	base := ChunkAAD(m, c.File)
	if base == nil {
		return nil
	}
	return append(base, rowChunkTableBinding(m, schema, table)...)
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
