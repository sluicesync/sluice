// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pin for ADR-0055 (research finding F1): pgoutput
// StreamAbortMessageV2 must refuse loudly through dispatchWAL.
//
// sluice's START_REPLICATION passes proto_version=2 without the
// streaming='on' plugin argument, so PG SHOULD NEVER emit a
// StreamAbort message on a sluice-driven stream. If one ever does
// fire (operator-side config drift or a future sluice change that
// enabled streaming without wiring StreamAbort rollback into the
// IR), silently skipping it would leave already-committed pre-abort
// chunks on the target after the source rolled the transaction back
// — silent unrecoverable divergence. The loud refusal closes that
// class.
//
// This test exercises the dispatch path with a synthetic wire-format
// StreamAbort message and asserts the returned error names the
// message type, the xid, and the recovery hint. It does NOT use
// streaming because sluice does not enable it; the synthetic shape
// is the only way to exercise the loud-refusal arm under the unit
// build tag.

package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// streamAbortWireBytes builds the pgoutput StreamAbort message body.
//
// Wire format (per pglogrepl messageV2.go::StreamAbortMessageV2.DecodeV2
// and the PG documentation for logical streaming replication
// protocol):
//
//	byte 0      : 'A' (MessageTypeStreamAbort)
//	bytes 1..4  : xid    (uint32, big-endian)
//	bytes 5..8  : sub_xid (uint32, big-endian)
//
// ParseV2 strips the leading type byte before calling DecodeV2 on
// the rest of the body.
func streamAbortWireBytes(xid, subXid uint32) []byte {
	b := make([]byte, 9)
	b[0] = byte(pglogrepl.MessageTypeStreamAbort)
	binary.BigEndian.PutUint32(b[1:5], xid)
	binary.BigEndian.PutUint32(b[5:9], subXid)
	return b
}

// TestStreamAbortMessageV2_RefusesLoudly is the F1 unit pin.
//
// Drives a synthetic StreamAbortMessageV2 wire payload through
// dispatchWAL and asserts:
//  1. The dispatcher returns a non-nil error (the silent-skip
//     pre-fix returned nil — this is the load-bearing assertion).
//  2. The error message names "StreamAbortMessageV2" so an operator
//     reading the log can identify the offending wire message.
//  3. The error includes the xid and sub_xid values so the operator
//     can correlate against pg_stat_activity / WAL records.
//  4. The error includes the recovery hint ("drop the slot, re-
//     snapshot") so the operator has an actionable path forward.
//  5. The error references ADR-0055 so the operator can find the
//     full audit context.
//  6. No ir.Change is emitted before the refusal (the silent-skip
//     pre-fix returned nil with no events, but a buggy refusal might
//     emit a TxCommit or similar; the assertion locks the shape).
//
// The test does NOT exercise the integration receiver — that lives
// in cdc_reader_streaming_protocol_integration_test.go, which
// confirms via observation that sluice's plugin args do not enable
// streaming end-to-end against a real PG.
func TestStreamAbortMessageV2_RefusesLoudly(t *testing.T) {
	const (
		testXid    uint32 = 12345
		testSubXid uint32 = 12346
	)

	r := &CDCReader{
		slotName:     "sluice_slot",
		publication:  "sluice_pub",
		protoVersion: 2,
	}

	// dispatchWAL parses the WAL payload itself. Construct an
	// XLogData payload whose body is the StreamAbort wire format.
	// The xld.WALStart / ServerWALEnd values are not load-bearing
	// for this dispatch arm — the refusal happens before any LSN-
	// derived position is constructed.
	xld := pglogrepl.XLogData{
		WALStart:     pglogrepl.LSN(0x100),
		ServerWALEnd: pglogrepl.LSN(0x200),
		ServerTime:   time.Now(),
		WALData:      streamAbortWireBytes(testXid, testSubXid),
	}

	// Mutable bookkeeping state mirroring what pump owns. ParseV2
	// expects inStream=true on a StreamAbort (it's a streaming-
	// in-progress message) but the dispatch path under audit
	// must refuse BEFORE the inStream gating matters. Pre-set the
	// flag so the parse succeeds and the dispatch arm is reached.
	var (
		relations          = map[uint32]*relationCacheEntry{}
		snapshotSig        = map[uint32]ir.SchemaSignature{}
		currentTxnLSN      = pglogrepl.LSN(0)
		currentTxnStartLSN = pglogrepl.LSN(0)
		streamedLSN        = pglogrepl.LSN(0)
		inStream           = true
		firstSeenRelLSN    = map[uint32]pglogrepl.LSN{}
	)
	out := make(chan ir.Change, 1)

	err := r.dispatchWAL(
		context.Background(),
		xld,
		relations,
		snapshotSig,
		&currentTxnLSN,
		&currentTxnStartLSN,
		&streamedLSN,
		&inStream,
		firstSeenRelLSN,
		out,
	)

	// Load-bearing assertion: must NOT be nil. Pre-fix this
	// dispatch path returned nil (silent skip).
	if err == nil {
		t.Fatal("dispatchWAL(StreamAbortMessageV2) = nil; want non-nil error (F1/ADR-0055)")
	}

	msg := err.Error()
	requiredFragments := []string{
		"StreamAbortMessageV2",
		"streaming",
		"drop the slot",
		"re-snapshot",
		"ADR-0055",
	}
	for _, fragment := range requiredFragments {
		if !strings.Contains(msg, fragment) {
			t.Errorf("error message missing required fragment %q\nfull message: %s", fragment, msg)
		}
	}

	// xid + sub_xid must surface — the operator needs them to
	// correlate against the source's WAL records and pg_stat_activity.
	for _, want := range []string{"12345", "12346"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing xid/sub_xid value %q\nfull message: %s", want, msg)
		}
	}

	// No change events should be emitted before the refusal.
	select {
	case c := <-out:
		t.Errorf("unexpected ir.Change emitted before refusal: %T %+v", c, c)
	default:
	}
}

// TestStreamAbortMessageV2_ErrorPropagatesAsUnclassified pins the
// expected propagation shape: the loud refusal returns a plain error
// (no sentinel wrap). The streamer's error-classification path
// treats unclassified errors as fatal, which is what we want — the
// stream tears down and the operator sees the message.
//
// If a future change introduces a sentinel (e.g. ir.ErrPositionInvalid)
// for retry / fall-through semantics, this pin will force a deliberate
// revision to the wrap shape, not a silent change in behaviour.
func TestStreamAbortMessageV2_ErrorPropagatesAsUnclassified(t *testing.T) {
	r := &CDCReader{
		slotName:     "sluice_slot",
		publication:  "sluice_pub",
		protoVersion: 2,
	}

	xld := pglogrepl.XLogData{
		WALData: streamAbortWireBytes(1, 1),
	}
	var (
		relations          = map[uint32]*relationCacheEntry{}
		snapshotSig        = map[uint32]ir.SchemaSignature{}
		currentTxnLSN      = pglogrepl.LSN(0)
		currentTxnStartLSN = pglogrepl.LSN(0)
		streamedLSN        = pglogrepl.LSN(0)
		inStream           = true
		firstSeenRelLSN    = map[uint32]pglogrepl.LSN{}
	)
	out := make(chan ir.Change, 1)

	err := r.dispatchWAL(
		context.Background(),
		xld,
		relations,
		snapshotSig,
		&currentTxnLSN,
		&currentTxnStartLSN,
		&streamedLSN,
		&inStream,
		firstSeenRelLSN,
		out,
	)
	if err == nil {
		t.Fatal("dispatchWAL(StreamAbortMessageV2) = nil; want non-nil error")
	}

	// Must NOT wrap ir.ErrPositionInvalid (that sentinel routes
	// through the ADR-0022 cold-start fall-through, which is the
	// wrong recovery semantics for a streaming-divergence refusal —
	// the operator must drop+resnapshot, not silently re-cold-start).
	if errors.Is(err, ir.ErrPositionInvalid) {
		t.Errorf("StreamAbort refusal must not wrap ir.ErrPositionInvalid (ADR-0022 fall-through is wrong recovery): %v", err)
	}
}
