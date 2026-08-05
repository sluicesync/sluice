// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// The `backup verify` depth that PARSES (roadmap item 129).
//
// # Why a second depth exists at all
//
// `backup verify`'s historical work is to re-hash every chunk's bytes and
// compare against the SHA-256 the manifest records. That check's evidence is
// the same artifact it is checking: it proves the bytes have not rotted and
// says nothing about whether they can be READ BACK. Bug 226 is what that costs
// in practice —
//
//	backup full    rc=0
//	backup verify  rc=0        <-- reports the backup fine
//	restore        rc=1, 0 rows   "chunk reader: scan: token too long"
//
// — an operator taking a backup, verifying it, and discovering at the moment
// they need it that it was never restorable. Item 128 stopped NEW backups from
// containing a row the reader could not scan, on all three write cores. It did
// nothing for artifacts already written, and the reason those were possible —
// that verify never parses a row — was still live.
//
// The independent evidence was cheaply available and unused: whether the
// chunk's OWN reader can read it. [VerifyDepthRead] is that evidence. It
// streams every chunk through the real [blobcodec.ChunkReader] /
// [blobcodec.ChangeChunkReader], with the exact (CEK, AAD, codec) triple
// `restore` resolves, and discards the rows.
//
// # What this depth covers, and what it does NOT
//
// Stated explicitly because the point of item 129 is not the >64 MiB row; it
// is that the whole verify surface's evidence was the artifact itself, and a
// parse must not be sold as making verify complete.
//
// COVERED (each of these passes the hash-only depth and fails here):
//
//   - A single row/change line longer than [blobcodec.MaxChunkLineBytes] —
//     Bug 226's shape, on artifacts written by a sluice at or below v0.111.1.
//   - A truncated or otherwise mangled codec stream whose SHA was re-stamped
//     to match (a store-side rewrite; bit-rot alone is already caught by SHA).
//   - A segment whose RECORDED codec is not the codec its chunks were written
//     with — the record is DR data and is never sniffed at read time, so a
//     wrong one is invisible until something decompresses.
//   - A row whose tagged-value envelope this build's decoder rejects (an
//     unknown `_t` tag from a newer writer, a malformed payload).
//   - On an encrypted chain verified WITH key material: a wrong CEK or a
//     wrong AAD binding, because the reader performs the same authenticated
//     open. (The hash-only depth also covers this when `--encrypt` is
//     supplied — see [verifyChunkAuthenticated]; here it is subsumed.)
//
// NOT COVERED, and no depth of `backup verify` covers them:
//
//   - Whether the VALUES are correct. A parse proves the chunk decodes, not
//     that what it decodes is what the source held. Nothing in a backup is
//     independent evidence of that; only a comparison against the source is,
//     which is `sluice verify`'s job, not this command's.
//   - Whether the rows will APPLY to a target — type-retarget failures,
//     constraint violations, and every other restore-time semantic. Those
//     need a target database, which `backup verify` deliberately does not
//     have.
//   - A CHANGE chunk whose decoded event count disagrees with the manifest's
//     recorded RowCount. Row chunks get that cross-check here because
//     [Restore.streamChunkRows] refuses on it, so verify matching restore is
//     free; the chain-restore path does NOT check it for change chunks, and
//     verify must never refuse a chain `restore` would accept. Filed as a
//     residual rather than silently skipped.
//   - A coherent whole-manifest rollback, which is ADR-0154's signing
//     territory, not readability.
//   - Anything at all on an ENCRYPTED chain without key material. Parsing is
//     the whole of what this depth does, so unlike the hash depth it cannot
//     degrade gracefully; it refuses with
//     [sluicecode.CodeBackupEncryptionMismatch] rather than reporting a
//     healthy chain unreadable ([chunkReadTarget.requireKey]).
//   - A chunk whose HEADER column list disagrees with the manifest schema.
//     [Restore.streamChunkRows] refuses on that, so it is a genuine
//     verify-green/restore-refuses shape and it is still open — a
//     schema-pairing check rather than a readability one, left out to keep
//     this depth to one class. Named here rather than left implied.
//
// # Cost, and why this is not the default
//
// One full read + decompress + decrypt + decode of every chunk. Today's
// default is deliberately hash-only so a cron probe can run against object
// storage cheaply; this depth is the one you run before you are relying on
// the backup, or on a schedule you can afford.

import (
	"context"
	"errors"
	"fmt"
	"io"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// VerifyDepth selects how much of each chunk `sluice backup verify` reads.
//
// The ZERO VALUE is [VerifyDepthHash], and that is load-bearing rather than
// incidental: the CLI sets this field, and every other construction — the
// tests, any future caller — gets the Go zero value. A depth field whose zero
// value meant "read" would quietly make every non-CLI verify pay a full read;
// one whose zero value means "hash" preserves the historical behaviour for
// every caller that does not ask (the v0.99.51 zero-value trap, inverted).
type VerifyDepth string

const (
	// VerifyDepthHash is the historical depth: re-hash every chunk's stored
	// bytes against the manifest's SHA-256, plus — when key material is
	// supplied — the authenticated open of every encrypted chunk. It never
	// parses a row, so it cannot distinguish a readable chunk from an
	// unreadable one whose bytes are intact.
	VerifyDepthHash VerifyDepth = "hash"

	// VerifyDepthRead additionally streams every chunk through its own real
	// reader and discards the rows. See this file's header for exactly what
	// that does and does not prove.
	VerifyDepthRead VerifyDepth = "read"
)

// readsRows reports whether d streams chunks through their real readers.
func (d VerifyDepth) readsRows() bool { return d == VerifyDepthRead }

// ParseVerifyDepth maps the `--depth` flag value onto a [VerifyDepth]. The
// empty string is the hash-only depth, matching the zero value, so a caller
// that never set the flag and a caller that passed the default agree.
func ParseVerifyDepth(s string) (VerifyDepth, error) {
	switch VerifyDepth(s) {
	case "", VerifyDepthHash:
		return VerifyDepthHash, nil
	case VerifyDepthRead:
		return VerifyDepthRead, nil
	default:
		return "", fmt.Errorf("unknown backup verify depth %q (want %q or %q)",
			s, VerifyDepthHash, VerifyDepthRead)
	}
}

// chunkUnreadableHint is the operator remedy for a byte-intact chunk its own
// reader cannot decode. Kept next to the refusal because the useful advice is
// specific: the artifact is already written, so the remedy is a fresh backup
// taken by a binary that refuses the offending row at WRITE time.
const chunkUnreadableHint = "this backup cannot be restored as it stands — take a fresh backup with a current sluice, whose write cores refuse an over-long row up front (a chain written by v0.111.1 or earlier had no such refusal); if this chain is the only copy, the named chunk is the loss and `restore --tables` can still recover the rest"

// chunkReadTarget is one chunk to stream, carrying everything the real reader
// needs — the same (CEK, AAD, codec, store) quadruple the restore path
// resolves for it, so a chunk that reads here reads there.
type chunkReadTarget struct {
	store irbackup.Store
	chunk *irbackup.ChunkInfo
	codec blobcodec.Codec
	cek   []byte
	aad   []byte

	// tableKey names the owning table in refusals ("" for change chunks).
	tableKey string
}

// chunkReadResult is what a read-through learned about the chunk.
type chunkReadResult struct {
	// opened reports that the chunk's CIPHERTEXT was actually AES-GCM-opened,
	// so verify's `decrypted=N` coverage signal (Bug 215) stays honest at this
	// depth too.
	opened bool

	// records is how many rows / change events actually decoded — the number
	// the manifest's RowCount is checked against.
	records int64
}

// readRows streams a DATA chunk through [blobcodec.ChunkReader].
func (t chunkReadTarget) readRows(ctx context.Context) (chunkReadResult, error) {
	if err := t.requireKey(); err != nil {
		return chunkReadResult{}, err
	}
	src, err := t.fetch(ctx)
	if err != nil {
		return chunkReadResult{}, err
	}
	cr, err := blobcodec.NewChunkReader(src, t.chunk.SHA256, t.cek, t.codec, t.aad)
	if err != nil {
		// NewChunkReader owns src's Close on every failure path.
		return chunkReadResult{}, t.classify(fmt.Errorf("open chunk reader: %w", err))
	}
	res := chunkReadResult{opened: t.cek != nil}
	for {
		_, rerr := cr.ReadRow()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			_ = cr.Close()
			return res, t.classify(rerr)
		}
		res.records++
	}
	if cerr := cr.Close(); cerr != nil {
		return res, t.classify(cerr)
	}
	return res, nil
}

// readChanges streams a CHANGE chunk through [blobcodec.ChangeChunkReader].
// The incremental/CDC half of the same check — item 128's first cut closed two
// of three write cores and the third was found a day later, so the read side
// covers both chunk kinds from the outset.
func (t chunkReadTarget) readChanges(ctx context.Context) (chunkReadResult, error) {
	if err := t.requireKey(); err != nil {
		return chunkReadResult{}, err
	}
	src, err := t.fetch(ctx)
	if err != nil {
		return chunkReadResult{}, err
	}
	cr, err := blobcodec.NewChangeChunkReader(src, t.chunk.SHA256, t.cek, t.codec, t.aad)
	if err != nil {
		return chunkReadResult{}, t.classify(fmt.Errorf("open change chunk reader: %w", err))
	}
	res := chunkReadResult{opened: t.cek != nil}
	for {
		_, rerr := cr.ReadChange()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			_ = cr.Close()
			return res, t.classify(rerr)
		}
		res.records++
	}
	if cerr := cr.Close(); cerr != nil {
		return res, t.classify(cerr)
	}
	return res, nil
}

// requireKey refuses an ENCRYPTED chunk that arrived here with no CEK, before
// the read attempt rather than after it.
//
// Without this the read depth would hand ciphertext to the gzip reader and
// report every chunk of an unkeyed encrypted chain as
// SLUICE-E-BACKUP-CHUNK-UNREADABLE — a true statement about this run and a
// badly wrong diagnosis, since the artifact is fine and the operator simply
// did not pass `--encrypt`. The hash depth degrades gracefully there (it WARNs
// and does the sha256-only check); the read depth CANNOT, because parsing is
// the whole of what it does, so it says which of the two things is missing.
func (t chunkReadTarget) requireKey() error {
	if t.chunk.Encryption == nil || t.cek != nil {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeBackupEncryptionMismatch,
		"re-run with --encrypt plus the chain's passphrase / KMS reference, or drop --depth read to get the sha256-only check that needs no key",
		fmt.Errorf("chunk %q is encrypted and no key material was supplied — `backup verify --depth read` "+
			"must decrypt a chunk before it can parse it", t.chunk.File))
}

// fetch resolves the chunk bytes through the same retrying, SHA-verifying
// helper restore uses (ADR-0117), so a transient short read is a retry here
// rather than a false "unreadable".
func (t chunkReadTarget) fetch(ctx context.Context) (io.ReadCloser, error) {
	src, err := blobcodec.FetchChunkVerified(ctx, t.store, t.chunk.File, t.chunk.SHA256)
	if err != nil {
		return nil, lineage.CodeChunkHashError(err)
	}
	return src, nil
}

// classify maps a read-path failure onto the right coded refusal. The two
// pre-existing chunk codes keep their meanings — a SHA mismatch is at-rest
// corruption, a GCM failure is tamper/splice — and everything else is the new
// class this depth exists to surface: bytes that are intact and still cannot
// be read.
func (t chunkReadTarget) classify(err error) error {
	switch {
	case errors.Is(err, blobcodec.ErrChunkHashMismatch):
		return lineage.CodeChunkHashError(err)
	case errors.Is(err, crypto.ErrChunkAuthFailed):
		return lineage.CodeChunkAuthError(err)
	}
	where := t.chunk.File
	if t.tableKey != "" {
		where = fmt.Sprintf("%s (table %s)", t.chunk.File, t.tableKey)
	}
	return sluicecode.Wrap(sluicecode.CodeBackupChunkUnreadable, chunkUnreadableHint,
		fmt.Errorf("chunk %s hashes correctly and its own reader still cannot read it — `restore` fails on this chunk: %w",
			where, err))
}

// verifyRewrittenChangeChunk re-reads a change chunk that smart compaction
// has just rewritten, THROUGH THE STORE, with the real reader (audit 2026-08-04
// C-4).
//
// The shape it closes is the same evidence-sharing one item 129 exists for, on
// the single maintenance path that rewrites restore-critical bytes:
// [applySmartCompactionToIncremental] stamps `ch.SHA256 = cw.Hash()` and
// `ch.RowCount = cw.ChangeCount()` from ITS OWN writer's in-memory accounting
// and never reads the object back. Everything downstream — restore, broker
// replay, `backup verify` at any depth — then checks the store's bytes against
// a number the writer asserted rather than one anything observed, so a Put that
// truncated, a store that returned success without durably holding the object,
// or a same-path upload skip that kept the OLD object all produce a chain whose
// manifest is internally consistent and whose bytes are not what it says.
//
// The re-read is one extra read of a chunk this path has already read once, and
// it is INDEPENDENT evidence in the precise sense the project's rule asks for:
// the expected SHA comes from the writer, the actual bytes come from the store,
// and the expected record count comes from the writer while the actual one
// comes from decoding what the store returned.
//
// SIBLING SWEEP, because "the maintenance paths" is exactly the phrasing that
// hides one: smart compaction is the ONLY chain-maintenance path that
// re-encodes chunk bytes and re-stamps their SHA. The non-smart compaction path
// moves chunks verbatim through [copyFile] and carries the ORIGINAL manifest
// SHA with them — its evidence is a hash neither the copier nor its writer
// produced on this run, so a truncated copy fails the existing check. `backup
// prune` deletes chunks and rewrites no bytes at all.
func verifyRewrittenChangeChunk(
	ctx context.Context,
	store irbackup.Store,
	chunk *irbackup.ChunkInfo,
	cek []byte,
	codec blobcodec.Codec,
	aad []byte,
) error {
	target := chunkReadTarget{store: store, chunk: chunk, codec: codec, cek: cek, aad: aad}
	res, err := target.readChanges(ctx)
	if err != nil {
		return err
	}
	if res.records != chunk.RowCount {
		return sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
			"re-run `backup compact`; if it refuses again the destination store is not durably holding what it accepts",
			fmt.Errorf("rewritten chunk %q decodes %d change(s) but the compactor recorded %d — "+
				"the store does not hold what compaction believes it wrote",
				chunk.File, res.records, chunk.RowCount))
	}
	return nil
}

// checkChunkRowCount is the layer-2 row-count predicate, shared by
// [Restore.streamChunkRows] and the read depth so the two cannot disagree
// about which chunks are acceptable.
//
// Reaches ROW chunks only. Change chunks are deliberately outside it: the
// chain-restore path does not compare a change chunk's decoded count against
// its recorded RowCount, and verify refusing what restore accepts is the one
// direction this command must never fail in.
func checkChunkRowCount(chunk *irbackup.ChunkInfo, decoded int64) error {
	switch {
	case chunk.RowCount > 0 && decoded != chunk.RowCount:
		return sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
			"restore from an untampered copy, or sign the chain so a truncated/edited chunk is caught at verify time",
			fmt.Errorf("layer-2 chunk row-count mismatch on %s: manifest says %d, decoded %d",
				chunk.File, chunk.RowCount, decoded))
	case chunk.RowCount == 0 && decoded > 0:
		return sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
			"restore from an untampered copy, or sign the chain so a zeroed chunk row-count is caught at verify time",
			fmt.Errorf("layer-2 chunk row-count anomaly on %s: manifest records 0 rows but decoded %d (zeroed chunk RowCount)",
				chunk.File, decoded))
	}
	return nil
}
