// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// The reader-side pin for the binlog re-send (cdc_reader_resend.go). Each
// case feeds the stream a GTID-mode re-dial produces — the events delivered
// on the first connection, the artificial events a new dump connection
// opens with, then the re-sent transaction from its first event — through
// the pump's own per-event step ([CDCReader.deliver]), and grades what the
// reader emitted against the source's transactions written out ONCE, as
// literals. The real-server counterpart is
// TestStreamer_MySQLBinlogResend_* in internal/pipeline.

const resendFile = "binlog.000007"

// at stamps an event with its end position in the binlog.
func at(ev *replication.BinlogEvent, pos uint32) *replication.BinlogEvent {
	ev.Header.LogPos = pos
	return ev
}

// reconnect is what a new dump connection opens with: an artificial ROTATE
// naming the file it resumes in, and that file's FORMAT_DESCRIPTION, both at
// LogPos 0.
func reconnect(file string) []*replication.BinlogEvent {
	return []*replication.BinlogEvent{
		{Header: hdr(replication.ROTATE_EVENT), Event: &replication.RotateEvent{NextLogName: []byte(file), Position: 4}},
		{Header: hdr(replication.FORMAT_DESCRIPTION_EVENT), Event: &replication.FormatDescriptionEvent{}},
	}
}

func heartbeat(pos uint32) *replication.BinlogEvent {
	return at(&replication.BinlogEvent{Header: hdr(replication.HEARTBEAT_EVENT), Event: &replication.GenericEvent{}}, pos)
}

// deliverAll runs the events through the pump's per-event step.
func deliverAll(t *testing.T, r *CDCReader, evs ...*replication.BinlogEvent) ([]ir.Change, error) {
	t.Helper()
	out := make(chan ir.Change, 256)
	ctx := context.Background()
	for _, ev := range evs {
		if err := r.deliver(ctx, ev, out); err != nil {
			close(out)
			return drain(out), err
		}
	}
	close(out)
	return drain(out), nil
}

func drain(out chan ir.Change) []ir.Change {
	var got []ir.Change
	for c := range out {
		got = append(got, c)
	}
	return got
}

// shape renders the emitted stream as the source's transactions would read.
func shape(got []ir.Change) string {
	parts := make([]string, 0, len(got))
	for _, c := range got {
		switch c := c.(type) {
		case ir.TxBegin:
			parts = append(parts, "BEGIN")
		case ir.TxCommit:
			parts = append(parts, "COMMIT")
		case ir.Insert:
			parts = append(parts, fmt.Sprintf("I%v", c.Row["id"]))
		default:
			parts = append(parts, fmt.Sprintf("%T", c))
		}
	}
	return strings.Join(parts, " ")
}

// tx6 is G6 → BEGIN → I1 → I2 → I3 → XID, with its real end positions.
func tx6(t *testing.T) []*replication.BinlogEvent {
	return []*replication.BinlogEvent{
		at(mysqlGTIDEvent(t, 6), 100), at(queryEvent("BEGIN"), 150),
		at(insertRowEvent(1), 200), at(insertRowEvent(2), 250), at(insertRowEvent(3), 300),
		at(xidEvent(), 350),
	}
}

func tx7(t *testing.T) []*replication.BinlogEvent {
	return []*replication.BinlogEvent{
		at(mysqlGTIDEvent(t, 7), 400), at(queryEvent("BEGIN"), 450), at(insertRowEvent(4), 500), at(xidEvent(), 550),
	}
}

func concat(parts ...[]*replication.BinlogEvent) []*replication.BinlogEvent {
	var all []*replication.BinlogEvent
	for _, p := range parts {
		all = append(all, p...)
	}
	return all
}

func newResendReader(t *testing.T) *CDCReader {
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	r.currentFile = resendFile
	return r
}

const onceShape = "BEGIN I1 I2 I3 COMMIT BEGIN I4 COMMIT"

// The connection breaks after I2: the first delivery is G6 BEGIN I1 I2, and
// the re-dial re-sends G6 from its GTID event. The reader must emit each
// change of the source once, and the transaction's rows must still exclude
// its own GTID (a re-staged GTID used to fold the pending copy early).
func TestBinlogResend_InFlightTransactionIsDeliveredOnce(t *testing.T) {
	r := newResendReader(t)
	first := tx6(t)[:4]
	got, err := deliverAll(t, r, concat(first, reconnect(resendFile), []*replication.BinlogEvent{heartbeat(250)}, tx6(t), tx7(t))...)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if s := shape(got); s != onceShape {
		t.Fatalf("the reader emitted %q across a mid-transaction re-send, want the source's transactions once: %q", s, onceShape)
	}
	const g6 = stagingUUID + ":6"
	for i, what := range []string{"TxBegin", "I1", "I2", "I3"} {
		assertExcludes(t, FlavorVanilla, got[i], g6, what+" (after the re-send)")
	}
	assertIncludes(t, FlavorVanilla, got[4], g6, "TxCommit")
}

// The connection breaks between transactions (after G6's XID): go-mysql's
// re-dial set excludes the newest GTID it saw, so the whole of G6 is re-sent.
func TestBinlogResend_CompletedTransactionIsNotReplayed(t *testing.T) {
	r := newResendReader(t)
	got, err := deliverAll(t, r, concat(tx6(t), reconnect(resendFile), tx6(t), tx7(t))...)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if s := shape(got); s != onceShape {
		t.Fatalf("the reader emitted %q across a between-transactions re-send, want %q", s, onceShape)
	}
}

// A break right after the GTID event: nothing but the GTID was delivered, so
// the stream's shape is right even with no guard — what a re-staged GTID
// breaks here is the positions (it folds the pending copy early).
func TestBinlogResend_BreakAfterTheGTIDEvent(t *testing.T) {
	r := newResendReader(t)
	got, err := deliverAll(t, r, concat(tx6(t)[:1], reconnect(resendFile), tx6(t), tx7(t))...)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if s := shape(got); s != onceShape {
		t.Fatalf("emitted %q, want %q", s, onceShape)
	}
	const g6 = stagingUUID + ":6"
	for i, what := range []string{"TxBegin", "I1", "I2", "I3"} {
		assertExcludes(t, FlavorVanilla, got[i], g6, what+" (after the re-send)")
	}
	assertIncludes(t, FlavorVanilla, got[4], g6, "TxCommit")
}

// MariaDB opens a transaction with its GTID event (no BEGIN), so the re-sent
// GTID event is also the one that would emit a second TxBegin.
func TestBinlogResend_MariaDBInFlightTransaction(t *testing.T) {
	r := newStagingReader(t, FlavorMariaDB, "0-1-5")
	r.currentFile = resendFile
	tx := func() []*replication.BinlogEvent {
		return []*replication.BinlogEvent{
			at(mariadbGTIDEvent(0, 6, false), 100),
			at(insertRowEvent(1), 200), at(insertRowEvent(2), 250), at(insertRowEvent(3), 300),
			at(xidEvent(), 350),
		}
	}
	// Only the FORMAT_DESCRIPTION half of the reconnect: MariaDB's ROTATE
	// arm re-anchors the lineage with a live query this dispatch-only
	// reader has no connection for.
	got, err := deliverAll(t, r, concat(tx()[:3], reconnect(resendFile)[1:], tx())...)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if s, want := shape(got), "BEGIN I1 I2 I3 COMMIT"; s != want {
		t.Fatalf("emitted %q, want %q", s, want)
	}
	for i, what := range []string{"TxBegin", "I1", "I2", "I3"} {
		assertExcludes(t, FlavorMariaDB, got[i], "0-1-6", what)
	}
	assertIncludes(t, FlavorMariaDB, got[4], "0-1-6", "TxCommit")
}

// A re-send that does not line up with what was delivered is refused, not
// guessed at.
func TestBinlogResend_MisalignedResendRefuses(t *testing.T) {
	r := newResendReader(t)
	_, err := deliverAll(t, r, concat(tx6(t)[:4], reconnect("binlog.000008"), tx6(t))...)
	if err == nil || !strings.Contains(err.Error(), "refusing to guess") {
		t.Fatalf("a re-send from another binlog file returned %v, want a refusal", err)
	}
}

// A GTID the reader has not seen is never treated as a re-send, even when it
// follows a reconnect.
func TestBinlogResend_NewTransactionAfterReconnectIsDelivered(t *testing.T) {
	r := newResendReader(t)
	got, err := deliverAll(t, r, concat(tx6(t), reconnect(resendFile), tx7(t))...)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if s := shape(got); s != onceShape {
		t.Fatalf("emitted %q, want %q", s, onceShape)
	}
}

// File/pos mode stages no GTID, so the guard is inert there (go-mysql's
// file/pos re-dial resumes exactly after the last event it received).
func TestBinlogResend_FilePosModeIsUntouched(t *testing.T) {
	r := newResendReader(t)
	r.posMode, r.gtidSet = positionModeFilePos, nil
	got, err := deliverAll(t, r, concat(tx6(t), tx7(t))...)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if s := shape(got); s != onceShape {
		t.Fatalf("emitted %q, want %q", s, onceShape)
	}
}
