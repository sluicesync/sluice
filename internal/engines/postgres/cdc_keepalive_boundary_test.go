// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// GC-41 (j) unit pins for the keepalive boundary. The real-server pin —
// a Postgres 15+ source whose tables are idle while WAL flows elsewhere,
// on every apply path and both target families, with PG 14 as the control —
// is TestStreamer_PostgresIdleSource_SlotFollowsKeepaliveBoundary in
// internal/pipeline.

// keepaliveHarness drives the pump's keepalive arm and dispatchWAL against
// one shared set of pump bookkeeping, the way the pump does.
type keepaliveHarness struct {
	t           *testing.T
	r           *CDCReader
	kb          *keepaliveBoundary
	streamedLSN pglogrepl.LSN
	inStream    bool
	out         chan ir.Change

	currentTxnLSN, currentTxnStartLSN pglogrepl.LSN
	currentTxnCommitTime              time.Time
}

func newKeepaliveHarness(t *testing.T, start pglogrepl.LSN) *keepaliveHarness {
	t.Helper()
	return &keepaliveHarness{
		t:           t,
		r:           &CDCReader{slotName: "sluice_slot", publication: "sluice_pub", protoVersion: 2, systemID: "7000000000000000001", timeline: 1},
		kb:          newKeepaliveBoundary(start, time.Now().Add(-time.Hour)),
		streamedLSN: start,
		out:         make(chan ir.Change, 16),
	}
}

// wal dispatches one pgoutput payload.
func (h *keepaliveHarness) wal(payload []byte) {
	h.t.Helper()
	if err := h.r.dispatchWAL(
		context.Background(),
		pglogrepl.XLogData{WALStart: 0x50, ServerWALEnd: 0x200, ServerTime: time.Now(), WALData: payload},
		map[uint32]*relationCacheEntry{}, map[uint32]ir.SchemaSignature{},
		&h.currentTxnLSN, &h.currentTxnStartLSN, &h.currentTxnCommitTime, &h.streamedLSN, &h.inStream, h.kb,
		map[uint32]pglogrepl.LSN{}, h.out,
	); err != nil {
		h.t.Fatalf("dispatchWAL: %v", err)
	}
}

// keepalive runs the pump's keepalive arm for a keepalive reporting walEnd,
// with the boundary interval already elapsed.
func (h *keepaliveHarness) keepalive(walEnd pglogrepl.LSN) {
	h.t.Helper()
	h.kb.lastAt = time.Now().Add(-2 * keepaliveBoundaryInterval)
	if err := h.r.standKeepaliveIn(context.Background(), walEnd, h.inStream, h.kb, &h.streamedLSN, h.out); err != nil {
		h.t.Fatalf("standKeepaliveIn: %v", err)
	}
}

// drain returns everything emitted since the last drain.
func (h *keepaliveHarness) drain() []ir.Change {
	var got []ir.Change
	for {
		select {
		case c := <-h.out:
			got = append(got, c)
		default:
			return got
		}
	}
}

// TestStandKeepaliveIn_EmitsABoundaryOnlyTransaction pins the boundary's
// shape: a TxBegin and a TxCommit, nothing between them, both at the
// keepalive's ServerWALEnd, no source commit time, no ADR-0190 identity
// opened — and the streamed LSN moved to it, which is what lets the ack
// follow once the target holds it.
func TestStandKeepaliveIn_EmitsABoundaryOnlyTransaction(t *testing.T) {
	const (
		start  = pglogrepl.LSN(0x1000)
		walEnd = pglogrepl.LSN(0x5000)
	)
	h := newKeepaliveHarness(t, start)
	h.keepalive(walEnd)

	got := h.drain()
	if len(got) != 2 {
		t.Fatalf("a due keepalive emitted %d changes (%v); want exactly a TxBegin and a TxCommit", len(got), got)
	}
	begin, ok := got[0].(ir.TxBegin)
	if !ok {
		t.Fatalf("first change is %T; want ir.TxBegin", got[0])
	}
	commit, ok := got[1].(ir.TxCommit)
	if !ok {
		t.Fatalf("second change is %T; want ir.TxCommit", got[1])
	}
	for name, p := range map[string]ir.Position{"TxBegin": begin.Position, "TxCommit": commit.Position} {
		if lsn := positionLSN(t, p); lsn != walEnd {
			t.Errorf("%s position LSN = %s; want the keepalive's ServerWALEnd %s", name, lsn, walEnd)
		}
	}
	if !begin.CommitTime.IsZero() || !commit.CommitTime.IsZero() {
		t.Errorf("boundary carries commit times %v / %v; want zero — no source transaction committed there, and a "+
			"non-zero time would feed the sync-lag tracker a fabricated reading", begin.CommitTime, commit.CommitTime)
	}
	if h.streamedLSN != walEnd {
		t.Errorf("streamedLSN = %s; want %s — the ack can only follow a boundary the pump has streamed", h.streamedLSN, walEnd)
	}
	// ADR-0190: a row emitted after the boundary, outside any transaction,
	// must carry no identity — the boundary opened none.
	if id := h.r.applyIDs.Next("public.t"); !id.IsZero() {
		t.Errorf("after a keepalive boundary the sequencer stamps %+v; want no identity — a boundary names no source "+
			"transaction, and one keyed at its LSN could collide with a real commit's", id)
	}
}

// TestStandKeepaliveIn_NeverInsideATransaction pins the open-transaction
// gate across the pump's own arms: between a BeginMessage and its
// CommitMessage no keepalive is stood in, however far ServerWALEnd has run —
// persisting it would put the resume point after rows still in flight.
func TestStandKeepaliveIn_NeverInsideATransaction(t *testing.T) {
	const (
		start     = pglogrepl.LSN(0x1000)
		commitLSN = pglogrepl.LSN(0x3000)
		endLSN    = pglogrepl.LSN(0x3038)
	)
	h := newKeepaliveHarness(t, start)
	h.wal(beginWireBytes(commitLSN, 7))
	h.drain()

	h.keepalive(0x9000)
	if got := h.drain(); len(got) != 0 {
		t.Fatalf("a keepalive inside an open transaction emitted %v; want nothing", got)
	}

	h.wal(commitWireBytes(commitLSN, endLSN))
	h.drain()
	h.keepalive(0x9000)
	if got := h.drain(); len(got) != 2 {
		t.Fatalf("after the commit a due keepalive emitted %v; want a boundary", got)
	}

	h.inStream = true
	h.keepalive(0xA000)
	if got := h.drain(); len(got) != 0 {
		t.Fatalf("a keepalive inside a pgoutput streamed chunk emitted %v; want nothing", got)
	}
}

// TestStandKeepaliveIn_StrictlyPastTheLastBoundary pins monotonicity: a
// keepalive at or below the last boundary emitted — a real commit's end or
// an earlier keepalive's — emits nothing, so the persisted position never
// moves backward (the SLOT-ACKED-PAST-TARGET-POSITION door's precondition,
// cdc_resume_ack.go).
func TestStandKeepaliveIn_StrictlyPastTheLastBoundary(t *testing.T) {
	const (
		start     = pglogrepl.LSN(0x1000)
		commitLSN = pglogrepl.LSN(0x3000)
		endLSN    = pglogrepl.LSN(0x3038)
	)
	h := newKeepaliveHarness(t, start)

	for _, walEnd := range []pglogrepl.LSN{start - 1, start} {
		h.keepalive(walEnd)
		if got := h.drain(); len(got) != 0 {
			t.Errorf("keepalive at %s, not past the start position %s, emitted %v", walEnd, start, got)
		}
	}

	h.wal(beginWireBytes(commitLSN, 7))
	h.wal(commitWireBytes(commitLSN, endLSN))
	h.drain()
	for _, walEnd := range []pglogrepl.LSN{commitLSN, endLSN} {
		h.keepalive(walEnd)
		if got := h.drain(); len(got) != 0 {
			t.Errorf("keepalive at %s, not past the last commit's end %s, emitted %v", walEnd, endLSN, got)
		}
	}

	h.keepalive(0x4000)
	if got := h.drain(); len(got) != 2 {
		t.Fatalf("keepalive past the last commit emitted %v; want a boundary", got)
	}
	h.keepalive(0x4000)
	if got := h.drain(); len(got) != 0 {
		t.Errorf("a second keepalive at the same LSN emitted %v; want nothing", got)
	}
}

// TestStandKeepaliveIn_Throttled pins the cadence: no boundary until
// keepaliveBoundaryInterval has passed since the last boundary of either
// kind.
func TestStandKeepaliveIn_Throttled(t *testing.T) {
	kb := newKeepaliveBoundary(0x1000, time.Now())
	if kb.due(0x2000, false, time.Now()) {
		t.Error("due immediately after the start; want the interval to elapse first")
	}
	if !kb.due(0x2000, false, time.Now().Add(keepaliveBoundaryInterval)) {
		t.Error("not due once the interval elapsed")
	}
	kb.commit(0x2000, 0x2038, time.Now())
	if kb.due(0x3000, false, time.Now()) {
		t.Error("due right after a real commit; a stream whose own commits keep coming needs no keepalive boundary")
	}
}

// TestNoteCommit_KeepaliveBoundaryPassedCommit pins the runtime check on the
// walsender premise: a delivered commit below a keepalive boundary already
// emitted logs the marker at ERROR and stops further keepalive boundaries on
// the connection, and a commit at or above it does neither.
func TestNoteCommit_KeepaliveBoundaryPassedCommit(t *testing.T) {
	logs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	h := newKeepaliveHarness(t, 0x1000)
	h.keepalive(0x5000)
	h.drain()

	// A commit record starting exactly at the boundary is the walsender's
	// legal worst case: a resume from 0x5000 re-delivers it.
	h.wal(beginWireBytes(0x5000, 8))
	h.wal(commitWireBytes(0x5000, 0x5038))
	h.drain()
	if strings.Contains(logs.String(), KeepaliveBoundaryPassedCommitMarker) {
		t.Fatalf("a commit at the boundary logged %s; want silence:\n%s", KeepaliveBoundaryPassedCommitMarker, logs)
	}

	h.keepalive(0x8000)
	h.drain()
	h.wal(beginWireBytes(0x7000, 9))
	h.wal(commitWireBytes(0x7000, 0x7038))
	h.drain()
	if !strings.Contains(logs.String(), KeepaliveBoundaryPassedCommitMarker) || !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("a commit below a keepalive boundary did not log %s at ERROR:\n%s", KeepaliveBoundaryPassedCommitMarker, logs)
	}
	h.keepalive(0x9000)
	if got := h.drain(); len(got) != 0 {
		t.Errorf("after the premise was seen broken a keepalive emitted %v; want keepalive boundaries off", got)
	}
}
