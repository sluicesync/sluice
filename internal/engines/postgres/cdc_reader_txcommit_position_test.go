// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// beginWireBytes renders a pgoutput Begin message: final_lsn (int64),
// commit_ts (int64, microseconds since 2000-01-01), xid (int32).
func beginWireBytes(finalLSN pglogrepl.LSN, xid uint32) []byte {
	b := make([]byte, 21)
	b[0] = byte(pglogrepl.MessageTypeBegin)
	binary.BigEndian.PutUint64(b[1:9], uint64(finalLSN))
	binary.BigEndian.PutUint64(b[9:17], 0)
	binary.BigEndian.PutUint32(b[17:21], xid)
	return b
}

// commitWireBytes renders a pgoutput Commit message: flags (int8),
// commit_lsn (int64), end_lsn (int64), commit_ts (int64).
func commitWireBytes(commitLSN, endLSN pglogrepl.LSN) []byte {
	b := make([]byte, 26)
	b[0] = byte(pglogrepl.MessageTypeCommit)
	b[1] = 0
	binary.BigEndian.PutUint64(b[2:10], uint64(commitLSN))
	binary.BigEndian.PutUint64(b[10:18], uint64(endLSN))
	binary.BigEndian.PutUint64(b[18:26], 0)
	return b
}

// dispatchOne drives one pgoutput payload through dispatchWAL with
// fresh pump bookkeeping and returns whatever the arm emitted.
func dispatchOne(t *testing.T, r *CDCReader, payload []byte) (ir.Change, error) {
	t.Helper()
	var (
		relations            = map[uint32]*relationCacheEntry{}
		snapshotSig          = map[uint32]ir.SchemaSignature{}
		currentTxnLSN        pglogrepl.LSN
		currentTxnStartLSN   pglogrepl.LSN
		currentTxnCommitTime time.Time
		streamedLSN          pglogrepl.LSN
		inStream             bool
		firstSeenRelLSN      = map[uint32]pglogrepl.LSN{}
	)
	out := make(chan ir.Change, 1)
	err := r.dispatchWAL(
		context.Background(),
		pglogrepl.XLogData{WALStart: 0x50, ServerWALEnd: 0x200, ServerTime: time.Now(), WALData: payload},
		relations, snapshotSig,
		&currentTxnLSN, &currentTxnStartLSN, &currentTxnCommitTime, &streamedLSN, &inStream,
		firstSeenRelLSN, out,
	)
	select {
	case c := <-out:
		return c, err
	default:
		return nil, err
	}
}

// positionLSN decodes the LSN a reader-emitted position carries.
func positionLSN(t *testing.T, p ir.Position) pglogrepl.LSN {
	t.Helper()
	decoded, ok, err := decodePGPos(p)
	if err != nil || !ok {
		t.Fatalf("decodePGPos(%+v) = ok=%v err=%v", p, ok, err)
	}
	lsn, err := pglogrepl.ParseLSN(decoded.LSN)
	if err != nil {
		t.Fatalf("ParseLSN(%q): %v", decoded.LSN, err)
	}
	return lsn
}

// TestDispatchWAL_TxCommitCarriesTransactionEndLSN pins the position
// convention at the dispatch arm (audit 2026-09-01 A2-1, the Postgres
// sibling of item 132): TxBegin carries the transaction's CommitLSN
// (the pre-transaction point) and TxCommit carries the
// TransactionEndLSN (the post-commit point), so a resume from a clean
// boundary starts at the NEXT transaction while a resume from anything
// inside the transaction re-delivers it whole. The real-server pin of
// the resume behaviour is TestPGCDC_TxCommitPositionIsPostCommit.
func TestDispatchWAL_TxCommitCarriesTransactionEndLSN(t *testing.T) {
	r := &CDCReader{slotName: "sluice_slot", publication: "sluice_pub", protoVersion: 2}
	const (
		commitLSN = pglogrepl.LSN(0x1000)
		endLSN    = pglogrepl.LSN(0x1038)
	)

	c, err := dispatchOne(t, r, beginWireBytes(commitLSN, 42))
	if err != nil {
		t.Fatalf("dispatch Begin: %v", err)
	}
	begin, ok := c.(ir.TxBegin)
	if !ok {
		t.Fatalf("Begin emitted %T; want ir.TxBegin", c)
	}
	if got := positionLSN(t, begin.Position); got != commitLSN {
		t.Errorf("TxBegin position LSN = %s; want the transaction's CommitLSN %s (the pre-transaction point)", got, commitLSN)
	}

	c, err = dispatchOne(t, r, commitWireBytes(commitLSN, endLSN))
	if err != nil {
		t.Fatalf("dispatch Commit: %v", err)
	}
	commit, ok := c.(ir.TxCommit)
	if !ok {
		t.Fatalf("Commit emitted %T; want ir.TxCommit", c)
	}
	if got := positionLSN(t, commit.Position); got != endLSN {
		t.Errorf("TxCommit position LSN = %s; want TransactionEndLSN %s (the post-commit point) — a TxCommit carrying CommitLSN %s is re-delivered by logical decoding on resume, which is the A2-1 replay", got, endLSN, commitLSN)
	}
}

// TestDispatchWAL_TxCommitRefusesEndLSNNotPastCommit pins the wart's
// loud arm: a commit whose end LSN does not follow its commit record
// cannot yield a post-commit resume point, and the reader says so
// instead of persisting a position that would re-deliver or skip.
func TestDispatchWAL_TxCommitRefusesEndLSNNotPastCommit(t *testing.T) {
	r := &CDCReader{slotName: "sluice_slot", publication: "sluice_pub", protoVersion: 2}
	for _, tc := range []struct {
		name   string
		endLSN pglogrepl.LSN
	}{
		{"end == commit", 0x1000},
		{"end < commit", 0x0800},
		{"end zero", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := dispatchOne(t, r, commitWireBytes(0x1000, tc.endLSN))
			if err == nil {
				t.Fatalf("dispatch Commit(end=%s) = nil error, emitted %T; want the loud refusal", tc.endLSN, c)
			}
			if !strings.Contains(err.Error(), "does not follow the commit record") {
				t.Errorf("refusal text = %q; want it to name the malformed end LSN", err)
			}
			if c != nil {
				t.Errorf("a refused commit emitted %T; want nothing", c)
			}
		})
	}
}
