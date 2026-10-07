// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The broker's mid-incremental frontier (ADR-0191 §3.2).
//
// Before ADR-0191 every change of an incremental carried the PARENT token
// (BRK-1), so a run interrupted partway through an incremental re-applied the
// whole incremental. The position now stands INSIDE the incremental: each
// change carries a token naming how far into incremental X its source
// transaction starts, and the applier — which already persists only at a
// source-transaction boundary (the serial loop's CheckpointOnlyAtTxBoundary,
// the lane frontier's recorded boundaries, amendment E's barrier fold) —
// persists "X applied through event e" in the same target transaction as the
// work. A restart re-reads X, drops events 0..e and resumes at e+1: only the
// source transaction that was in flight is re-delivered, which is exactly the
// window ADR-0190's apply marks were built for.
//
// No new target write: the frontier tokens REPLACE the parent tokens those
// writes carried. The producer pays an ordinal counter and one token encode per
// boundary.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// BackupBrokerPositionEngineV2 is the sentinel every position this binary
// writes carries (ADR-0191 §6, Q3). It is NEW so a downgrade is loud: v0.156.12
// and older recognise only [BackupBrokerPositionEngine] and refuse a v2 token
// as "owned by a non-broker writer" — rather than read its
// last_applied_backup_id, re-apply the in-flight incremental with no marks,
// and (before v0.156.11) duplicate keyless rows silently. Written always, not
// only while an incremental is in progress, so the refusal does not depend on
// where the broker stopped. Held by TestBrokerToken_DowngradeIsLoud against a
// FROZEN copy of the v0.156.11 decoder.
const BackupBrokerPositionEngineV2 = "backup-broker-v2"

// brokerInProgress is the token's frontier inside incremental BackupID: the
// changes of its first Through+1 decoded events (TxBegin and TxCommit
// included, over the RAW stream) are durable on the target.
type brokerInProgress struct {
	// BackupID names the incremental the frontier stands inside.
	BackupID string `json:"backup_id"`
	// Chunks is [chunkListDigest] of that incremental's manifest when the
	// frontier was written: the independent evidence that the stream a
	// restart re-reads is the stream the ordinal counts over.
	Chunks string `json:"chunks"`
	// Through is the ordinal of the last event whose effects are durable —
	// always a boundary: a TxCommit, or a change outside a source
	// transaction.
	Through int64 `json:"through"`
}

// validate refuses a decoded frontier no writer produces, rather than
// resuming from a guess.
func (p *brokerInProgress) validate() error {
	switch {
	case p.BackupID == "":
		return errors.New("broker: position token's in_progress names no incremental")
	case len(p.Chunks) != sha256.Size*2:
		return fmt.Errorf("broker: position token's in_progress chunk digest %q is not a SHA-256", p.Chunks)
	case p.Through < 0:
		return fmt.Errorf("broker: position token's in_progress ordinal %d is negative", p.Through)
	}
	return nil
}

// chunkListDigest is the SHA-256 of an incremental's ordered change-chunk
// SHA-256 list. The decoded stream of an incremental is a pure function of
// that list (every chunk is fetched against its recorded SHA, decrypted and
// decompressed deterministically, read in manifest order — ADR-0191 §1.2),
// so equal digests mean an equal stream and an ordinal into one is an ordinal
// into the other. Chunk FILE names are deliberately not hashed: naive
// compaction moves the same bytes under the same names, and smart compaction
// rewrites different bytes under the SAME names, so names prove nothing.
func chunkListDigest(m *irbackup.Manifest) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%d\n", len(m.ChangeChunks))
	for _, c := range m.ChangeChunks {
		sha := ""
		if c != nil {
			sha = c.SHA256
		}
		_, _ = h.Write([]byte(sha))
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// BrokerIncrementalRewrittenMarker is the grep-stable token of the refusal a
// resume raises when the incremental it stands inside is no longer the stream
// it counted over.
const BrokerIncrementalRewrittenMarker = "BROKER-INCREMENTAL-REWRITTEN"

// errBrokerIncrementalRewritten refuses to skip over a rewritten stream. It
// never skips by ordinal into a stream that changed: smart compaction collapses
// a key's changes ACROSS transactions (an INSERT in one merged with an UPDATE
// in a later one), so "through e" in the old stream is not a prefix of the
// new one, and a skip would drop the later change silently.
func errBrokerIncrementalRewritten(backupID, why string) error {
	return sluicecode.Wrap(sluicecode.CodeBrokerIncrementalRewritten,
		"recover with `sluice sync from-backup run --reset-target-data` (drops the chain's tables and restores the chain whole); "+
			"to keep this from recurring, do not smart-compact (`backup compact --smart-compaction`) a segment a broker is still replaying",
		fmt.Errorf("broker: %s: incremental %s was rewritten after this broker applied part of it — %s. The persisted position "+
			"stands inside that incremental, and resuming would skip by event ordinal into a stream that is no longer the one the "+
			"ordinal counted (smart compaction collapses a key's changes across transactions), which could drop changes silently. "+
			"Nothing of it was applied by this run",
			BrokerIncrementalRewrittenMarker, backupID, why))
}

// brokerFrontier stamps each event of incremental X with the token naming the
// start of the source transaction it belongs to (ADR-0191 §3.2). It is confined
// to the producer goroutine, like the F1 backstop's lastApplied.
//
//   - a TxCommit at ordinal e, and a change OUTSIDE a source transaction at
//     ordinal e (a MySQL TRUNCATE; every change of a marker-less trigger
//     chunk): "X through e";
//   - a TxBegin and every change inside a transaction: the boundary before the
//     transaction — the transaction's START. A row's token is therefore never
//     a mid-transaction resume point, the rule CheckpointOnlyAtTxBoundary and
//     the lane frontier already enforce on the applier side; even if one were
//     persisted it would be safe.
//
// The start of X's FIRST transaction is the parent token (no in_progress):
// nothing of X is durable there, and it is the classic resume point.
// A TxBegin while a transaction is already open (a MySQL ROLLBACK group, an
// interleaved VStream group the reader merged) keeps the earlier start —
// re-delivering more, never less.
type brokerFrontier struct {
	chainURL     string
	parentID     string
	backupID     string
	chunks       string
	next         int64 // ordinal of the next event
	lastBoundary int64 // -1 = the parent
	open         bool

	// skipThrough drops events at or below this ordinal (-1 = none): the
	// resume of a persisted frontier.
	skipThrough int64

	// cached is the last token encoded, for lastBoundary == cachedFor; rows of
	// one transaction share it.
	cached    ir.Position
	cachedFor int64
	hasCached bool
}

// newBrokerFrontier starts a frontier over incremental m (whose parent is
// parentID), skipping events at or below skipThrough.
func newBrokerFrontier(chainURL, parentID string, m *irbackup.Manifest, skipThrough int64) *brokerFrontier {
	return &brokerFrontier{
		chainURL:     chainURL,
		parentID:     parentID,
		backupID:     lineage.ManifestBackupID(m),
		chunks:       chunkListDigest(m),
		lastBoundary: -1,
		skipThrough:  skipThrough,
	}
}

// stamp assigns the next ordinal to c and returns the token c must carry and
// whether c is emitted (false: it is at or below the resumed frontier).
func (f *brokerFrontier) stamp(c ir.Change) (pos ir.Position, emit bool) {
	e := f.next
	f.next++
	switch c.(type) {
	case ir.TxBegin:
		f.open = true
	case ir.TxCommit:
		f.open = false
		f.lastBoundary = e
	default:
		if !f.open {
			f.lastBoundary = e
		}
	}
	return f.token(f.lastBoundary), e > f.skipThrough
}

// events is how many events the frontier has counted.
func (f *brokerFrontier) events() int64 { return f.next }

// token renders the position for "applied through boundary".
func (f *brokerFrontier) token(boundary int64) ir.Position {
	if f.hasCached && f.cachedFor == boundary {
		return f.cached
	}
	var p ir.Position
	if boundary < 0 {
		p = encodeBrokerPosition(f.chainURL, f.parentID)
	} else {
		p = encodeBrokerFrontier(f.chainURL, f.parentID, &brokerInProgress{BackupID: f.backupID, Chunks: f.chunks, Through: boundary})
	}
	f.cached, f.cachedFor, f.hasCached = p, boundary, true
	return p
}

// encodeBrokerFrontier is [encodeBrokerPosition] standing inside an
// incremental.
func encodeBrokerFrontier(chainURL, lastApplied string, in *brokerInProgress) ir.Position {
	tok := brokerPositionToken{
		Engine:              BackupBrokerPositionEngineV2,
		ChainURL:            chainURL,
		LastAppliedBackupID: lastApplied,
		InProgress:          in,
	}
	body, _ := json.Marshal(tok)
	return ir.Position{Engine: BackupBrokerPositionEngineV2, Token: string(body)}
}

// resumeSkip derives, from the position the TARGET holds, how many leading
// events of incremental m this run must drop (ADR-0191 §3.2 Resume; §13 R10):
// -1 when nothing of m is durable. It is the ONLY justification for skipping
// committed work, and it is read back from the target before every
// incremental rather than carried in memory.
//
// The independent expected values: the chunk-list digest the frontier
// recorded when the work was done, compared against m as the chain holds it
// now, and (after the stream) the event count — neither derives from the
// events being skipped.
func resumeSkip(persisted ir.Position, found bool, m *irbackup.Manifest, parentID string) (int64, error) {
	if !found {
		return -1, nil
	}
	tok, err := decodeBrokerPosition(persisted)
	if err != nil {
		return 0, err
	}
	id := lineage.ManifestBackupID(m)
	if tok.LastAppliedBackupID != "" && parentID != "" && tok.LastAppliedBackupID != parentID {
		return 0, fmt.Errorf("broker: about to apply incremental %s after %s, but the target's position says the last fully "+
			"applied incremental is %s; another writer moved this stream's position — refusing rather than guess which is right",
			id, parentID, tok.LastAppliedBackupID)
	}
	in := tok.InProgress
	if in == nil {
		return -1, nil
	}
	if in.BackupID != id {
		return 0, errBrokerIncrementalRewritten(in.BackupID, fmt.Sprintf(
			"the position stands inside incremental %s, but the incremental that now follows %s in the chain is %s",
			in.BackupID, parentID, id,
		))
	}
	if now := chunkListDigest(m); now != in.Chunks {
		return 0, errBrokerIncrementalRewritten(id, fmt.Sprintf(
			"its change-chunk list now digests to %s, and the position was written against %s", now, in.Chunks,
		))
	}
	return in.Through, nil
}

// errBrokerResumeBeyondStream refuses a stream shorter than the frontier said
// was already applied — the same rewrite, seen from the count.
func errBrokerResumeBeyondStream(backupID string, through, events int64) error {
	return errBrokerIncrementalRewritten(backupID, "the position says its events 0.."+strconv.FormatInt(through, 10)+
		" are applied, but it now decodes to only "+strconv.FormatInt(events, 10)+" events")
}
