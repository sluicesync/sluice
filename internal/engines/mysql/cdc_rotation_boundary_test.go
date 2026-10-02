// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"math"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// The GC-43 (r) reader-side pins: a ROTATE read from the binlog, with no
// transaction open, emits a boundary-only transaction naming the resume point
// past the file it ends; every guard skips it instead. The real-server
// counterparts — an idle stream rotated twice, its old files purged, then
// restarted — are TestCDCReader_RotationBoundary_* (engines/mysql) and
// TestStreamer_IdleBinlogRotation_* (pipeline).
//
// The expected positions are literals (the next file's name and offset 4) and
// go-mysql's own GTID set algebra, never a second call into the reader.

const rotationNextFile = "mysql-bin.000006"

// realRotate is a ROTATE as the server writes it into the binlog: a real
// header timestamp, no ARTIFICIAL flag.
func realRotate(next string, pos uint64) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT, Timestamp: 1_790_000_000, LogPos: 900},
		Event:  &replication.RotateEvent{NextLogName: []byte(next), Position: pos},
	}
}

// artificialRotate is the rotate a dump connection opens with, in the two
// spellings the exclusion recognises.
func artificialRotate(next string, timestamp uint32, flags uint16) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT, Timestamp: timestamp, Flags: flags},
		Event:  &replication.RotateEvent{NextLogName: []byte(next), Position: 4},
	}
}

func newFilePosRotationReader() *CDCReader {
	return &CDCReader{
		schema:      "app",
		flavor:      FlavorVanilla,
		posMode:     positionModeFilePos,
		currentFile: "mysql-bin.000005",
		serverUUID:  stagingUUID,
		tableMap:    map[uint64]string{},
		schemaCache: map[string]*tableSchema{},
	}
}

// assertRotationBoundary checks got is exactly the boundary pair: TxBegin then
// TxCommit at one position, no ADR-0190 identity, no commit time. It returns
// the decoded position.
func assertRotationBoundary(t *testing.T, got []ir.Change) binlogPos {
	t.Helper()
	if len(got) != 2 {
		t.Fatalf("emitted %d changes, want the boundary pair (TxBegin, TxCommit): %#v", len(got), got)
	}
	begin, ok := got[0].(ir.TxBegin)
	if !ok {
		t.Fatalf("first change is %T, want ir.TxBegin", got[0])
	}
	commit, ok := got[1].(ir.TxCommit)
	if !ok {
		t.Fatalf("second change is %T, want ir.TxCommit", got[1])
	}
	if begin.Position.Token != commit.Position.Token {
		t.Errorf("boundary TxBegin %q and TxCommit %q differ; a row-less transaction's points coincide",
			begin.Position.Token, commit.Position.Token)
	}
	if !begin.CommitTime.IsZero() || !commit.CommitTime.IsZero() {
		t.Errorf("boundary carries a commit time (%v / %v); no source transaction committed there, and a "+
			"non-zero time would feed sync lag and the --apply-delay hold", begin.CommitTime, commit.CommitTime)
	}
	for _, c := range got {
		if id := ir.ApplyIDOf(c); !id.IsZero() {
			t.Errorf("boundary %T carries ADR-0190 identity %+v; it names no source transaction", c, id)
		}
	}
	decoded, ok, err := decodeBinlogPos(commit.Position)
	if err != nil || !ok {
		t.Fatalf("decode boundary position: ok=%v err=%v", ok, err)
	}
	return decoded
}

// TestRotationBoundary_FilePos: the boundary names (next file, 4) and keeps
// the file/pos identity stamp.
func TestRotationBoundary_FilePos(t *testing.T) {
	r := newFilePosRotationReader()
	got := dispatchAll(t, r, realRotate(rotationNextFile, 4))
	p := assertRotationBoundary(t, got)
	if p.Mode != positionModeFilePos || p.File != rotationNextFile || p.Pos != 4 {
		t.Errorf("boundary position = %+v, want file/pos %s:4 (the first event of the next file)", p, rotationNextFile)
	}
	if p.ServerUUID != stagingUUID {
		t.Errorf("boundary position ServerUUID = %q, want %q: the file/pos lineage stamp must ride every "+
			"persisted position", p.ServerUUID, stagingUUID)
	}
	if r.currentFile != rotationNextFile {
		t.Errorf("currentFile = %q after the rotate, want %q", r.currentFile, rotationNextFile)
	}
}

// TestRotationBoundary_GTIDCarriesFoldedStandaloneGroups is harm (2): the
// last groups before the rotate are standalone (a DDL and a GRANT-shaped
// statement, folded with no TxCommit). The boundary set must contain them,
// or gtid_purged outruns the persisted set the moment their file is purged.
func TestRotationBoundary_GTIDCarriesFoldedStandaloneGroups(t *testing.T) {
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 6),
		queryEvent("CREATE TABLE other_db.x (id INT)"),
		mysqlGTIDEvent(t, 7),
		queryEvent("OPTIMIZE TABLE users"), // in scope: leaves pendingDDLActive set
		realRotate(rotationNextFile, 4),
	)
	p := assertRotationBoundary(t, got)
	if p.Mode != positionModeGTID {
		t.Fatalf("boundary mode = %q, want gtid", p.Mode)
	}
	for _, g := range []string{stagingUUID + ":6", stagingUUID + ":7"} {
		if !containsGTID(t, FlavorVanilla, p.GTIDSet, g) {
			t.Errorf("boundary set %q omits standalone group %s", p.GTIDSet, g)
		}
	}
	if !r.pendingDDLActive {
		t.Fatal("precondition: the in-scope OPTIMIZE should have left pendingDDLActive set")
	}
}

// TestRotationBoundary_MariaDBCarriesLineage: the boundary carries the
// standalone group's GTID and the lineage the rotate arm re-anchored. The
// re-anchor itself needs a server (BINLOG_GTID_POS), so this drives the
// emitter with the anchor already moved; TestCDCReader_RotationBoundary_MariaDB
// covers the arm end to end.
func TestRotationBoundary_MariaDBCarriesLineage(t *testing.T) {
	r := newStagingReader(t, FlavorMariaDB, "0-1-5")
	dispatchAll(t, r, mariadbGTIDEvent(0, 6, true), queryEvent("CREATE USER u"))
	r.lineageFile, r.lineagePos, r.lineageSet = "mysqld-bin.000006", 4, "0-1-6"

	rot := realRotate("mysqld-bin.000006", 4)
	out := make(chan ir.Change, 4)
	if err := r.emitRotationBoundary(context.Background(), rot.Event.(*replication.RotateEvent), out); err != nil {
		t.Fatalf("emitRotationBoundary: %v", err)
	}
	close(out)
	var got []ir.Change
	for c := range out {
		got = append(got, c)
	}
	p := assertRotationBoundary(t, got)
	if !containsGTID(t, FlavorMariaDB, p.GTIDSet, "0-1-6") {
		t.Errorf("boundary set %q omits the standalone group 0-1-6", p.GTIDSet)
	}
	if p.LineageFile != "mysqld-bin.000006" || p.LineagePos != 4 || p.LineageSet != "0-1-6" {
		t.Errorf("boundary lineage = %s:%d %q, want the re-anchored mysqld-bin.000006:4 \"0-1-6\"",
			p.LineageFile, p.LineagePos, p.LineageSet)
	}
}

// TestRotationBoundary_ArtificialRotateKeepsMariaDBAnchor: the rotate a dump
// connection opens with names the file the stream is already in, so it must
// not move the capture door's anchor to that file's start. (The reader has no
// database: a re-anchor attempt would fail the test by panicking on it.)
func TestRotationBoundary_ArtificialRotateKeepsMariaDBAnchor(t *testing.T) {
	r := newStagingReader(t, FlavorMariaDB, "0-1-5")
	r.lineageFile, r.lineagePos, r.lineageSet = "mysqld-bin.000001", 1234, "0-1-5"
	for _, rot := range []*replication.BinlogEvent{
		artificialRotate("mysqld-bin.000001", 0, replication.LOG_EVENT_ARTIFICIAL_F),
		artificialRotate("mysqld-bin.000001", 0, 0),
	} {
		if got := dispatchAll(t, r, rot); len(got) != 0 {
			t.Errorf("artificial rotate emitted %d changes, want none", len(got))
		}
	}
	if r.lineageFile != "mysqld-bin.000001" || r.lineagePos != 1234 || r.lineageSet != "0-1-5" {
		t.Errorf("artificial rotate moved the anchor to %s:%d %q; want the capture anchor mysqld-bin.000001:1234 \"0-1-5\"",
			r.lineageFile, r.lineagePos, r.lineageSet)
	}
}

// TestRotationBoundary_GuardsSkip: every guard skips the boundary — the
// pre-fix behaviour — rather than persisting a position that is not a
// boundary.
func TestRotationBoundary_GuardsSkip(t *testing.T) {
	cases := []struct {
		name string
		gtid bool
		pre  func(t *testing.T) []*replication.BinlogEvent
		rot  *replication.BinlogEvent
	}{
		{
			name: "artificial: zero timestamp",
			rot:  artificialRotate(rotationNextFile, 0, 0),
		},
		{
			name: "artificial: LOG_EVENT_ARTIFICIAL_F",
			rot:  artificialRotate(rotationNextFile, 1_790_000_000, replication.LOG_EVENT_ARTIFICIAL_F),
		},
		{
			name: "offset below the first event",
			rot:  realRotate(rotationNextFile, 0),
		},
		{
			name: "offset past uint32",
			rot:  realRotate(rotationNextFile, math.MaxUint32+1),
		},
		{
			name: "transaction open (file/pos)",
			pre: func(*testing.T) []*replication.BinlogEvent {
				return []*replication.BinlogEvent{queryEvent("BEGIN")}
			},
			rot: realRotate(rotationNextFile, 4),
		},
		{
			name: "transaction open (gtid)",
			gtid: true,
			pre: func(t *testing.T) []*replication.BinlogEvent {
				return []*replication.BinlogEvent{mysqlGTIDEvent(t, 6), queryEvent("BEGIN"), insertRowEvent(1)}
			},
			rot: realRotate(rotationNextFile, 4),
		},
		{
			name: "inside XA START … XA END",
			pre: func(*testing.T) []*replication.BinlogEvent {
				return []*replication.BinlogEvent{queryEvent("XA START 'x1'")}
			},
			rot: realRotate(rotationNextFile, 4),
		},
		{
			name: "GTID staged by an unmodelled terminator (XA PREPARE)",
			gtid: true,
			pre: func(t *testing.T) []*replication.BinlogEvent {
				return []*replication.BinlogEvent{
					mysqlGTIDEvent(t, 6), queryEvent("XA START 'x1'"), queryEvent("XA END 'x1'"),
				}
			},
			rot: realRotate(rotationNextFile, 4),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r *CDCReader
			if tc.gtid {
				r = newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
			} else {
				r = newFilePosRotationReader()
			}
			var pre []*replication.BinlogEvent
			if tc.pre != nil {
				pre = tc.pre(t)
			}
			dispatchAll(t, r, pre...)
			if got := dispatchAll(t, r, tc.rot); len(got) != 0 {
				t.Errorf("rotate emitted %d changes, want none: %#v", len(got), got)
			}
			if r.currentFile != rotationNextFile {
				t.Errorf("currentFile = %q, want %q: a skipped boundary still follows the rotate",
					r.currentFile, rotationNextFile)
			}
		})
	}
}
