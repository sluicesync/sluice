//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The sync snapshot openers' resume arm, against real servers (audit
// 2026-09-01 SLM-4).
//
// Both `sync` cold-start openers stamped a file/pos handoff position
// regardless of gtid_mode while the backup doors and the from-now open
// picked the GTID arm on a gtid_mode=ON source. Ground truth before the
// fix, on mysql:8.0 booted --gtid-mode=ON: OpenSnapshotStreamForTables
// (serial and N-way alike) returned
// {"mode":"file_pos","file":"mysql-bin.000001","pos":…,"server_uuid":<A>}.
// A failover to a promoted replica then hit the file/pos identity
// refusal instead of the GTID lineage check that was built to accept it.
//
// The matrix is every opener shape × both modes, because the two openers
// capture on different code paths (one pinned conn vs. FTWRL-coordinated
// N conns) and the serial opener has a third path — the no-RELOAD
// lock-free fallback — that reads its cut BEFORE the snapshot:
//
//	gtid_mode=ON   serial (FTWRL) · serial (lock-free) · concurrent   → GTID set
//	gtid_mode=OFF  serial · concurrent                                → file/pos + @@server_uuid
//	MariaDB        serial · concurrent                                → GTID set + lineage anchor
//
// Each cell names an independent expected value: the set the server
// itself reports (@@global.gtid_executed / @@gtid_binlog_pos) at a moment
// with no writes in between, the @@server_uuid read directly, and — for
// the failover cell — a row written on the promoted replica AFTER the
// resume arriving on the stream. The pipeline-level counterpart (the
// real Streamer, the persisted token, the target-table witness) is
// internal/pipeline/streamer_mysql_gtid_handoff_integration_test.go.

package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// slm4Opener names one opener shape and how to reach it through the
// public surface. The serial path is selected by a one-table scope, the
// N-way path by a multi-table scope under the default parallelism (auto,
// clamped to the table count — the pipeline's own selection).
type slm4Opener struct {
	name   string
	tables []string
	// wantRows pins WHICH opener actually ran, so a cell cannot pass on the
	// wrong path (the N-way opener falls back to serial when FTWRL fails).
	wantRows string
}

var slm4Openers = []slm4Opener{
	{name: "serial", tables: []string{"t1"}, wantRows: "*mysql.RowReader"},
	{name: "concurrent", tables: []string{"t1", "t2"}, wantRows: "*mysql.concurrentBinlogRows"},
}

func openSyncSnapshotFor(t *testing.T, ctx context.Context, e Engine, dsn string, o slm4Opener) ir.Position {
	t.Helper()
	stream, err := e.OpenSnapshotStreamForTables(ctx, dsn, o.tables)
	if err != nil {
		t.Fatalf("%s opener: OpenSnapshotStreamForTables: %v", o.name, err)
	}
	defer func() { _ = stream.Close() }()
	if got := fmt.Sprintf("%T", stream.Rows); got != o.wantRows {
		t.Fatalf("%s opener: the snapshot's Rows is %s, want %s — the cell ran on the wrong opener", o.name, got, o.wantRows)
	}
	return stream.Position
}

func decodeSyncPosition(t *testing.T, cell string, p ir.Position) binlogPos {
	t.Helper()
	var decoded binlogPos
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		t.Fatalf("%s: token %q is not a binlogPos: %v", cell, p.Token, err)
	}
	return decoded
}

// deliversAfterResume resumes pos on dsn and requires a write made on that
// server AFTER the resume to arrive — the independent expected value for
// "accepted", since an accepted-but-dead stream would otherwise pass.
func deliversAfterResume(t *testing.T, ctx context.Context, e Engine, dsn, cell, write string, pos ir.Position) {
	t.Helper()
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("%s: OpenCDCReader: %v", cell, err)
	}
	defer closeLineageReader(reader)
	ch, err := reader.StreamChanges(ctx, pos)
	if err != nil {
		t.Fatalf("%s: the resume was REFUSED: %v", cell, err)
	}
	execSQL(t, ctx, dsn, write)
	deadline := time.After(45 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				t.Fatalf("%s: stream closed: %v", cell, reader.(*CDCReader).Err())
			}
			t.Logf("%s: accepted and delivered the post-resume write", cell)
			return
		case <-deadline:
			t.Fatalf("%s: accepted but delivered nothing within 45s", cell)
		}
	}
}

func TestSyncSnapshotOpenersStampGTIDPositionOnGTIDModeSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	dsnA, cleanupA := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "slm4", mysqlFilePosEnvs("slm4"), mysqlGTIDBootCmd,
	)
	defer cleanupA()
	assertServerVar(t, ctx, dsnA, "@@global.gtid_mode", "ON")
	uuidA := serverUUIDOf(t, ctx, dsnA)
	execSQL(t, ctx, dsnA, `CREATE TABLE slm4.t1 (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsnA, `CREATE TABLE slm4.t2 (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsnA, `INSERT INTO slm4.t1 VALUES (1,'a'),(2,'b'),(3,'c')`)
	execSQL(t, ctx, dsnA, `INSERT INTO slm4.t2 VALUES (1,'x')`)

	// A user that can read rows and the binlog but cannot FLUSH TABLES
	// WITH READ LOCK, so the serial opener takes its lock-free path.
	execSQL(t, ctx, dsnA, `CREATE USER 'slm4_lockfree'@'%' IDENTIFIED BY 'lockfree-pw'`)
	execSQL(t, ctx, dsnA, `GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'slm4_lockfree'@'%'`)
	dsnLockFree := strings.Replace(dsnA, "root:rootpw@", "slm4_lockfree:lockfree-pw@", 1)

	e := Engine{Flavor: FlavorVanilla}
	// No writes happen between here and the last capture, so the server's
	// own executed set is the value every opener must have stamped.
	wantSet := globalVar(t, ctx, dsnA, "gtid_executed")
	if !strings.Contains(wantSet, uuidA) {
		t.Fatalf("premise gone: @@gtid_executed %q does not carry the source uuid %q", wantSet, uuidA)
	}

	positions := map[string]ir.Position{}
	for _, o := range slm4Openers {
		pos := openSyncSnapshotFor(t, ctx, e, dsnA, o)
		decoded := decodeSyncPosition(t, o.name, pos)
		if decoded.Mode != positionModeGTID {
			t.Fatalf("%s opener on a gtid_mode=ON source stamped mode %q (token %s); want %q — the SLM-4 defect: "+
				"a failover to a promoted replica becomes a file/pos identity refusal and a target re-copy",
				o.name, decoded.Mode, pos.Token, positionModeGTID)
		}
		if decoded.GTIDSet != wantSet {
			t.Fatalf("%s opener stamped set %q; the server reports %q", o.name, decoded.GTIDSet, wantSet)
		}
		if decoded.File != "" || decoded.Pos != 0 || decoded.ServerUUID != "" {
			t.Fatalf("%s opener: a GTID position must not carry file/pos or a server_uuid: %+v", o.name, decoded)
		}
		t.Logf("%s opener: %s", o.name, pos.Token)
		positions[o.name] = pos
	}

	// The serial opener's third path: the no-RELOAD lock-free fallback reads
	// its cut BEFORE the snapshot, on a different branch of the same
	// function.
	posLockFree := openSyncSnapshotFor(t, ctx, e, dsnLockFree, slm4Opener{name: "serial-lock-free", tables: []string{"t1"}, wantRows: "*mysql.RowReader"})
	if d := decodeSyncPosition(t, "serial-lock-free", posLockFree); d.Mode != positionModeGTID || d.GTIDSet != wantSet {
		t.Fatalf("serial lock-free opener stamped %+v; want a GTID position at %q", d, wantSet)
	}
	t.Logf("serial-lock-free opener: %s", posLockFree.Token)
	positions["serial-lock-free"] = posLockFree

	// The failover cell: a promoted replica — an instance whose
	// gtid_executed CONTAINS A's set under a DIFFERENT @@server_uuid — must
	// accept each opener's position and deliver the next write. Seeded the
	// way TestGTIDResumeBindsLineageAcrossInstances builds it.
	dsnC, cleanupC := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "slm4", mysqlFilePosEnvs("slm4"), mysqlGTIDBootCmd,
	)
	defer cleanupC()
	execSQL(t, ctx, dsnC, `CREATE TABLE slm4.t1 (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsnC, `CREATE TABLE slm4.t2 (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsnC, "RESET MASTER")
	execSQL(t, ctx, dsnC, "SET @@GLOBAL.gtid_purged = '"+wantSet+"'")
	uuidC := serverUUIDOf(t, ctx, dsnC)
	if uuidC == uuidA {
		t.Fatal("the promoted replica must have a different @@server_uuid")
	}
	if got := globalVar(t, ctx, dsnC, "gtid_executed"); !strings.Contains(got, uuidA) {
		t.Fatalf("seeding C's lineage failed: gtid_executed %q does not carry A's uuid %q", got, uuidA)
	}
	next := 100
	for name, pos := range positions {
		next++
		deliversAfterResume(t, ctx, e, dsnC, "failover/"+name,
			"INSERT INTO slm4.t1 VALUES ("+strconv.Itoa(next)+",'after-failover')", pos)
	}
}

// TestSyncSnapshotOpenersStampFilePosOnFilePosSource is the other half of
// the arm: gtid_mode=OFF (MySQL 8's default) still gets a file/pos
// position bound to the instance's @@server_uuid, on both openers.
func TestSyncSnapshotOpenersStampFilePosOnFilePosSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	dsn, cleanup := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "slm4", mysqlFilePosEnvs("slm4"), mysqlFilePosBootCmd,
	)
	defer cleanup()
	assertServerVar(t, ctx, dsn, "@@global.gtid_mode", "OFF")
	wantUUID := serverUUIDOf(t, ctx, dsn)
	execSQL(t, ctx, dsn, `CREATE TABLE slm4.t1 (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsn, `CREATE TABLE slm4.t2 (id INT PRIMARY KEY, v TEXT)`)
	wantFile := currentBinlogFile(t, ctx, dsn)

	e := Engine{Flavor: FlavorVanilla}
	for _, o := range slm4Openers {
		pos := openSyncSnapshotFor(t, ctx, e, dsn, o)
		decoded := decodeSyncPosition(t, o.name, pos)
		if decoded.Mode != positionModeFilePos {
			t.Fatalf("%s opener on a gtid_mode=OFF source stamped mode %q; want %q", o.name, decoded.Mode, positionModeFilePos)
		}
		if decoded.File != wantFile || decoded.Pos == 0 {
			t.Fatalf("%s opener stamped %s:%d; the server's current binlog is %s", o.name, decoded.File, decoded.Pos, wantFile)
		}
		if decoded.ServerUUID != wantUUID {
			t.Fatalf("%s opener: ServerUUID = %q, want the source's @@server_uuid %q", o.name, decoded.ServerUUID, wantUUID)
		}
		if decoded.GTIDSet != "" {
			t.Fatalf("%s opener: a file/pos position must not carry a GTID set: %+v", o.name, decoded)
		}
		t.Logf("%s opener: %s", o.name, pos.Token)
	}
}

// TestSyncSnapshotOpenersMariaDBAnchorGTID: MariaDB has no gtid_mode and
// every capture door takes the GTID arm; the sync openers used to be the
// exception (file/pos plus the lineage anchor). Both now stamp the
// domain-GTID set with the anchor, the same shape as a MariaDB backup;
// the resume accepts it and delivers; and the v0.138.0-and-earlier
// file/pos+anchor shape is still accepted, so an existing MariaDB sync
// stream is not forced onto a re-copy by this change.
func TestSyncSnapshotOpenersMariaDBAnchorGTID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dsn, cleanup := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	defer cleanup()
	execSQL(t, ctx, dsn, `CREATE TABLE cdc_src.t1 (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsn, `CREATE TABLE cdc_src.t2 (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsn, `INSERT INTO cdc_src.t1 VALUES (1,'a'),(2,'b')`)

	e := Engine{Flavor: FlavorMariaDB}
	next := 500
	for _, o := range slm4Openers {
		// Read per cell: the previous cell's continuation writes advance it.
		wantSet := globalVar(t, ctx, dsn, "gtid_binlog_pos")
		if wantSet == "" {
			t.Fatal("premise gone: @@gtid_binlog_pos is empty after writes")
		}
		pos := openSyncSnapshotFor(t, ctx, e, dsn, o)
		d := decodeSyncPosition(t, o.name, pos)
		if d.Mode != positionModeGTID || d.GTIDSet != wantSet {
			t.Fatalf("MariaDB %s opener stamped %+v; want a GTID position at %q", o.name, d, wantSet)
		}
		if d.LineageFile == "" || d.LineagePos == 0 || d.LineageSet != d.GTIDSet {
			t.Fatalf("MariaDB %s opener: lineage anchor missing or inconsistent: %+v", o.name, d)
		}
		t.Logf("MariaDB %s opener: %s", o.name, pos.Token)

		next++
		deliversAfterResume(t, ctx, e, dsn, "mariadb/"+o.name,
			"INSERT INTO cdc_src.t1 VALUES ("+strconv.Itoa(next)+",'continuation')", pos)

		// The legacy shape a v0.138.0 sync cold start persisted: file/pos
		// at the anchor's byte, plus the anchor.
		legacy := binlogPos{
			Mode: positionModeFilePos, File: d.LineageFile, Pos: d.LineagePos,
			LineageFile: d.LineageFile, LineagePos: d.LineagePos, LineageSet: d.LineageSet,
		}
		legacyPos, err := encodeBinlogPos(legacy)
		if err != nil {
			t.Fatalf("encode legacy position: %v", err)
		}
		next++
		deliversAfterResume(t, ctx, e, dsn, "mariadb/"+o.name+"/legacy-file-pos",
			"INSERT INTO cdc_src.t1 VALUES ("+strconv.Itoa(next)+",'legacy-continuation')", legacyPos)
	}
}
