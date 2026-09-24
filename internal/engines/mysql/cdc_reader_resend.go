// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// # The binlog re-send, and why the reader drops it
//
// go-mysql re-dials the source INSIDE its syncer when the replication
// connection breaks (retrySync), invisibly to sluice. The commonest cause
// is a held consumer: while sluice's pipeline is busy — a forwarded ADD
// COLUMN's backfill, a slow or lock-blocked target — every buffer between
// the dump thread and this reader fills, and after net_write_timeout the
// server drops the dump thread. Where the re-dial resumes depends on the
// position mode:
//
//   - file/pos: from the end of the last event go-mysql received. Nothing
//     is delivered twice.
//   - GTID (MySQL and MariaDB): from the executed set EXCLUDING the newest
//     GTID it has seen (go-mysql's prevGset). The newest transaction is
//     therefore re-sent from its first event — the in-flight one if the
//     connection broke inside it, the last completed one if it broke
//     between transactions.
//
// Delivering those events again is not harmless. Measured end to end
// (streamer_mysql_binlog_resend_integration_test.go): a re-sent prefix
// re-applies rows on top of the rows that followed them, so an INSERT that
// reuses a unique value the same transaction later freed collides with the
// row that now holds it, and the stream stops — then crash-loops, because
// every restart re-delivers the transaction onto the same target. And the
// re-sent GTID event re-stages the GTID it is re-sending, which folds the
// still-pending copy into the executed set early: every re-sent row then
// carries a position claiming the transaction had committed, the exact
// shape item 132 removed.
//
// So the reader recognises a re-sent transaction by its GTID and drops what
// it already delivered. Recognition is by EXACT equality with the GTID the
// reader itself staged or folded — never by set containment, which MariaDB's
// per-domain sets do not support soundly — so a transaction the reader has
// not seen can never match:
//
//   - the in-flight GTID (still pending): the re-sent GTID event and every
//     re-sent event at or before the last one already dispatched are
//     dropped; the first event past it resumes normal dispatch.
//   - the last committed GTID: the whole re-sent group is dropped, up to
//     the next group's GTID event.
//
// Alignment within the in-flight transaction is by (binlog file, end
// LogPos): the re-sent events are the same bytes of the same file, so they
// carry the same positions. A re-send that does not line up — another file,
// or no event positions to compare — is refused rather than guessed at.

// binlogEventPos is an event's (file, end LogPos) — its identity within the
// source's binlog.
type binlogEventPos struct {
	file string
	pos  uint32
}

// atOrBefore reports whether p is at or before q in the same file.
func (p binlogEventPos) atOrBefore(q binlogEventPos) bool {
	return p.file == q.file && p.pos <= q.pos
}

// binlogResendGuard is the reader's re-send state (see the file comment).
// Owned by the pump goroutine.
type binlogResendGuard struct {
	// lastFolded is the GTID of the last transaction the reader folded into
	// its executed set — the one a between-transactions re-dial re-sends.
	lastFolded string
	// lastDispatched is the last top-level event dispatched with a real
	// position (FORMAT_DESCRIPTION and the artificial ROTATE a new dump
	// connection opens with carry LogPos 0 and are not tracked).
	lastDispatched binlogEventPos

	// skipThrough is set while re-sent events of the in-flight transaction
	// are being dropped: every event at or before it has been delivered.
	skipThrough *binlogEventPos
	// skipGroup is set while a re-sent COMPLETED transaction is being
	// dropped, until the next group's GTID event.
	skipGroup bool
	// resentGTID names the transaction being dropped, for the log line.
	resentGTID string
	// dropped counts the events dropped for the current re-send.
	dropped int
}

// deliver is the pump's per-event step: drop a re-sent event this stream
// already delivered, otherwise record its position and dispatch it.
func (r *CDCReader) deliver(ctx context.Context, ev *replication.BinlogEvent, out chan<- ir.Change) error {
	drop, err := r.dropResent(ctx, ev)
	if err != nil || drop {
		return err
	}
	r.noteDispatched(ev)
	return r.dispatch(ctx, ev, out)
}

// eventGTID returns the GTID a group-opening event names, and whether ev is
// one.
func eventGTID(ev *replication.BinlogEvent) (gtid string, opensGroup bool, err error) {
	switch e := ev.Event.(type) {
	case *replication.GTIDEvent:
		u, err := formatSIDAsUUID(e.SID)
		if err != nil {
			return "", true, fmt.Errorf("mysql: cdc: gtid sid: %w", err)
		}
		return fmt.Sprintf("%s:%d", u, e.GNO), true, nil
	case *replication.MariadbGTIDEvent:
		return e.GTID.String(), true, nil
	}
	return "", false, nil
}

// dropResent reports whether ev is a re-sent event the reader already
// delivered and must not dispatch again. It runs before dispatch, so a
// re-sent GTID event never reaches stageGTID.
func (r *CDCReader) dropResent(ctx context.Context, ev *replication.BinlogEvent) (bool, error) {
	g := &r.resend
	gtid, opensGroup, err := eventGTID(ev)
	if err != nil {
		return false, err
	}
	if opensGroup {
		r.endResend(ctx)
		switch {
		case gtid != "" && gtid == r.pendingGTID:
			if g.lastDispatched.pos == 0 {
				return false, fmt.Errorf("mysql: cdc: the source re-sent in-flight transaction %s after a reconnect, "+
					"and the reader has no event position to align the re-send with; refusing to guess which events were delivered", gtid)
			}
			through := g.lastDispatched
			g.skipThrough, g.resentGTID = &through, gtid
			g.dropped = 1
			return true, nil
		case gtid != "" && gtid == g.lastFolded && r.pendingGTID == "":
			g.skipGroup, g.resentGTID = true, gtid
			g.dropped = 1
			return true, nil
		}
		return false, nil
	}
	switch {
	case g.skipGroup:
		if !alignsResend(ev) {
			return false, nil
		}
		g.dropped++
		return true, nil
	case g.skipThrough != nil:
		if !alignsResend(ev) {
			// Bookkeeping — the artificial ROTATE / FORMAT_DESCRIPTION a
			// new dump connection opens with, a heartbeat: dispatched as
			// usual, and it neither is dropped nor ends the window.
			return false, nil
		}
		at := binlogEventPos{file: r.currentFile, pos: ev.Header.LogPos}
		if at.file != g.skipThrough.file {
			return false, fmt.Errorf("mysql: cdc: the source re-sent in-flight transaction %s after a reconnect from binlog %s, "+
				"but it was being read from %s; refusing to guess which events were delivered", g.resentGTID, at.file, g.skipThrough.file)
		}
		if at.atOrBefore(*g.skipThrough) {
			g.dropped++
			return true, nil
		}
		r.endResend(ctx)
	}
	return false, nil
}

// noteDispatched records a dispatched top-level event's position.
func (r *CDCReader) noteDispatched(ev *replication.BinlogEvent) {
	if _, opensGroup, _ := eventGTID(ev); !opensGroup && !alignsResend(ev) {
		return
	}
	if ev.Header == nil || ev.Header.LogPos == 0 {
		return
	}
	r.resend.lastDispatched = binlogEventPos{file: r.currentFile, pos: ev.Header.LogPos}
}

// alignsResend reports whether ev is part of a transaction's body — the
// events a re-send repeats byte for byte, at the same positions. Only these
// are compared against, dropped, or end a re-send window; a heartbeat or
// the artificial events that open a dump connection are none of the three.
func alignsResend(ev *replication.BinlogEvent) bool {
	if ev.Header == nil || ev.Header.LogPos == 0 {
		return false
	}
	switch ev.Event.(type) {
	case *replication.QueryEvent, *replication.TableMapEvent, *replication.RowsEvent,
		*replication.XIDEvent, *replication.TransactionPayloadEvent, *replication.RowsQueryEvent,
		*replication.IntVarEvent, *replication.ExecuteLoadQueryEvent, *replication.BeginLoadQueryEvent:
		return true
	}
	return false
}

// endResend closes a re-send window, logging what it dropped.
func (r *CDCReader) endResend(ctx context.Context) {
	g := &r.resend
	if g.resentGTID == "" {
		return
	}
	shape := "the rest of an in-flight transaction"
	if g.skipGroup {
		shape = "an already-delivered transaction"
	}
	slog.InfoContext(ctx, "mysql: cdc: the binlog connection was re-established and the source re-sent "+shape+"; "+
		"dropped the events this stream had already delivered",
		slog.String("gtid", g.resentGTID), slog.Int("events_dropped", g.dropped))
	g.skipThrough, g.skipGroup, g.resentGTID, g.dropped = nil, false, "", 0
}
