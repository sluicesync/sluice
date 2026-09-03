//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The lineage binding on a GTID-mode resume position, against real
// servers (audit 2026-09-01, SLM-2).
//
// The file/pos arm binds a position to an instance with @@server_uuid
// (v0.137.2). The GTID arm was documented as not needing that — "GTID
// UUIDs are themselves instance-bound, so verifyGTIDSetReachable already
// catches the node-replace case" — and v0.137.2's notes repeated it. It
// was false: verifyGTIDSetReachable ran only GTID_SUBSET(@@gtid_purged,
// resume), which asks whether the source has PURGED anything the position
// needs, and a fresh instance has purged nothing. Ground truth, 2026-09-01,
// two independent MySQL 8.0 instances with gtid_mode=ON: a `backup full`
// position from A resumed as `backup incremental` against B was ACCEPTED,
// B streamed every transaction it had ever executed, and the chain
// recorded them as A's delta at exit 0 with an end_position that was the
// union of two lineages.
//
// The fix is the other direction, verifyGTIDLineageContinuity:
// GTID_SUBSET(resume, @@gtid_executed) — the source must have EXECUTED
// everything the position consumed.
//
// Three directions are asserted, because each alone can pass for the
// wrong reason: (1) the foreign instance REFUSES; (2) the same instance
// ACCEPTS (a check that refuses every resume would pass 1); (3) an
// instance whose gtid_executed CONTAINS the position's set — the shape of
// a promoted replica or a `--set-gtid-purged=ON` restore — ACCEPTS (a
// check that binds the @@server_uuid instead of the lineage would fail
// this, and would refuse every legitimate failover).
//
// SCOPE. This pins the vanilla-MySQL GTID arm. The MariaDB arm has no
// GTID_SUBSET and defers to the stream's reactive error; the MariaDB cell
// below is the MEASUREMENT of whether that reactive refusal actually
// fires for a foreign position on a fresh instance — it asserts loud
// refusal at StreamChanges or on the first event, so a silent acceptance
// fails the test rather than being assumed away. VStream positions ride a
// different arm and are not covered here (stated, not implied).

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// mysqlGTIDBootCmd boots a vanilla MySQL with binary logging and GTID mode
// ON, which routes both capture doors into the GTID arm.
var mysqlGTIDBootCmd = []string{
	"mysqld", "--server-id=1", "--log-bin=mysql-bin",
	"--binlog-format=ROW", "--binlog-row-image=FULL",
	"--gtid-mode=ON", "--enforce-gtid-consistency=ON",
}

func TestGTIDResumeBindsLineageAcrossInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	dsnA, cleanupA := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "lineage", mysqlFilePosEnvs("lineage"), mysqlGTIDBootCmd,
	)
	defer cleanupA()
	dsnB, cleanupB := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "lineage", mysqlFilePosEnvs("lineage"), mysqlGTIDBootCmd,
	)
	defer cleanupB()

	uuidA := serverUUIDOf(t, ctx, dsnA)
	uuidB := serverUUIDOf(t, ctx, dsnB)
	if uuidA == uuidB || uuidA == "" || uuidB == "" {
		t.Fatalf("the two instances must have distinct non-empty identities; got A=%q B=%q", uuidA, uuidB)
	}

	// Give A a lineage worth resuming from: a few committed transactions,
	// so its position carries a non-empty GTID set under A's UUID.
	execSQL(t, ctx, dsnA, `CREATE TABLE lineage.t (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsnA, `INSERT INTO lineage.t VALUES (1,'a'),(2,'b'),(3,'c')`)

	// The defect's precondition, asserted rather than assumed: B is a fresh
	// instance with NOTHING purged, so the purged-subset check alone passes
	// any position whatsoever.
	if purged := globalVar(t, ctx, dsnB, "gtid_purged"); purged != "" {
		t.Fatalf("premise gone: fresh instance B has a non-empty gtid_purged %q; the vacuous-subset shape "+
			"this test reproduces needs an empty one", purged)
	}

	e := Engine{Flavor: FlavorVanilla}

	snap, err := e.OpenBackupSnapshot(ctx, dsnA, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot(A): %v", err)
	}
	capturedOnA := snap.Position
	_ = snap.Close()
	setA := assertGTIDPositionUnder(t, "OpenBackupSnapshot(A)", capturedOnA, uuidA)

	// Direction 1 — the regression: resuming A's GTID position on the
	// unrelated fresh instance B must refuse with ir.ErrPositionInvalid.
	readerB, err := e.OpenCDCReader(ctx, dsnB)
	if err != nil {
		t.Fatalf("OpenCDCReader(B): %v", err)
	}
	defer closeLineageReader(readerB)
	_, err = readerB.StreamChanges(ctx, capturedOnA)
	if err == nil {
		t.Fatal("resuming instance A's GTID position against fresh instance B was ACCEPTED; " +
			"this is the SLM-2 defect — B streams its entire history as if it were A's delta")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("cross-instance GTID resume failed, but not with ir.ErrPositionInvalid "+
			"(so the streamer will not route it to a cold-start re-snapshot): %v", err)
	}
	if !strings.Contains(err.Error(), "gtid_executed") {
		t.Fatalf("the refusal must name the lineage check (gtid_executed), got: %v", err)
	}

	// Direction 2 — the control: the SAME position on the instance that
	// produced it must be accepted.
	readerA, err := e.OpenCDCReader(ctx, dsnA)
	if err != nil {
		t.Fatalf("OpenCDCReader(A): %v", err)
	}
	defer closeLineageReader(readerA)
	if _, err := readerA.StreamChanges(ctx, capturedOnA); err != nil {
		t.Fatalf("same-instance resume of a GTID position was REFUSED: %v "+
			"(the lineage check must bind the lineage, not block every resume)", err)
	}

	// Direction 3 — the OTHER control, and the one that separates "binds
	// the lineage" from "binds the instance": a THIRD instance whose
	// gtid_executed contains A's set — seeded through gtid_purged, which
	// is exactly what a `--set-gtid-purged=ON` restore does and what a
	// promoted replica's executed set looks like — must ACCEPT, even though
	// its @@server_uuid differs from A's.
	dsnC, cleanupC := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "lineage", mysqlFilePosEnvs("lineage"), mysqlGTIDBootCmd,
	)
	defer cleanupC()
	execSQL(t, ctx, dsnC, "RESET MASTER")
	execSQL(t, ctx, dsnC, "SET @@GLOBAL.gtid_purged = '"+setA+"'")
	if got := globalVar(t, ctx, dsnC, "gtid_executed"); !strings.Contains(got, uuidA) {
		t.Fatalf("seeding C's lineage failed: gtid_executed %q does not carry A's uuid %q", got, uuidA)
	}
	readerC, err := e.OpenCDCReader(ctx, dsnC)
	if err != nil {
		t.Fatalf("OpenCDCReader(C): %v", err)
	}
	defer closeLineageReader(readerC)
	if _, err := readerC.StreamChanges(ctx, capturedOnA); err != nil {
		t.Fatalf("resume on an instance whose gtid_executed CONTAINS the position's set was REFUSED: %v "+
			"(a promoted replica or a --set-gtid-purged=ON restore must resume; the check binds lineage, "+
			"not @@server_uuid)", err)
	}
}

// TestGTIDResumeMariaDBBindsLineage is the MariaDB family, every cell on
// a real server, because the first cut of this test measured ONE cell
// (different server_id — which the server itself refuses) and declared
// MariaDB safe; the pre-tag review then measured the two cells the
// server does NOT refuse. The matrix:
//
//   - different server_id, same domain — the server refuses (1236 "not
//     in the master's binlog"); sluice must route it to cold-start.
//   - different gtid_domain_id — the server ACCEPTS and streams its whole
//     history; sluice's domain door and anchor must refuse.
//   - rebuilt: same server_id, same domain, a history that reads the SAME
//     GTIDs — the server accepts; only the BINLOG_GTID_POS anchor can
//     tell them apart, and must.
//   - the same instance — must ACCEPT (a check that refuses everything
//     would pass the three above).
//   - an anchorless legacy position on a rebuilt instance — must ACCEPT
//     with the UNVERIFIED-INSTANCE-IDENTITY warning (the documented
//     degraded posture), never refuse.
//
// Every cell asserts an independent expected value: the refusal wraps
// ir.ErrPositionInvalid (so the streamer cold-starts), and an accepted
// stream must deliver a write made on the resumed instance.
func TestGTIDResumeMariaDBBindsLineage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	dsnA, cleanupA := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	defer cleanupA()
	execSQL(t, ctx, dsnA, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsnA, `INSERT INTO cdc_src.t VALUES (1,'a'),(2,'b'),(3,'c')`)

	e := Engine{Flavor: FlavorMariaDB}
	snap, err := e.OpenBackupSnapshot(ctx, dsnA, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot(A): %v", err)
	}
	capturedOnA := snap.Position
	_ = snap.Close()
	var decoded binlogPos
	if err := json.Unmarshal([]byte(capturedOnA.Token), &decoded); err != nil || decoded.Mode != positionModeGTID {
		t.Fatalf("MariaDB backup position is not a GTID position: token=%q err=%v", capturedOnA.Token, err)
	}
	if decoded.LineageFile == "" || decoded.LineageSet == "" {
		t.Fatalf("MariaDB backup position carries no lineage anchor (token %q); the capture door did not stamp it", capturedOnA.Token)
	}
	if decoded.LineageSet != decoded.GTIDSet {
		t.Fatalf("anchor set %q != captured set %q — BINLOG_GTID_POS at the captured byte should equal the captured state", decoded.LineageSet, decoded.GTIDSet)
	}
	t.Logf("A: set=%s anchor=%s:%d", decoded.GTIDSet, decoded.LineageFile, decoded.LineagePos)

	// A legacy position: the same set with the anchor stripped, as a
	// pre-v0.138.0 binary would have written it.
	legacy := decoded
	legacy.LineageFile, legacy.LineagePos, legacy.LineageSet = "", 0, ""
	legacyPos, err := encodeBinlogPos(legacy)
	if err != nil {
		t.Fatalf("encode legacy position: %v", err)
	}

	mustRefuse := func(t *testing.T, dsn, cell string, pos ir.Position) {
		t.Helper()
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("%s: OpenCDCReader: %v", cell, err)
		}
		defer closeLineageReader(reader)
		ch, err := reader.StreamChanges(ctx, pos)
		if err != nil {
			if !errors.Is(err, ir.ErrPositionInvalid) {
				t.Fatalf("%s: refused, but not with ir.ErrPositionInvalid (no cold-start route): %v", cell, err)
			}
			t.Logf("%s: refused at open: %v", cell, err)
			return
		}
		// Accepted at open: only a reactive server refusal may follow.
		// Write on the resumed instance so silent acceptance has
		// something to deliver, then require the channel to close with
		// the reader's Err set (the reader contract).
		execSQL(t, ctx, dsn, `INSERT INTO cdc_src.t VALUES (900,'foreign')`)
		deadline := time.After(45 * time.Second)
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					serr := reader.(*CDCReader).Err()
					if serr == nil {
						t.Fatalf("%s: stream closed with NO error after accepting a foreign position — silent acceptance", cell)
					}
					if !errors.Is(serr, ir.ErrPositionInvalid) {
						t.Fatalf("%s: stream refused, but not with ir.ErrPositionInvalid: %v", cell, serr)
					}
					t.Logf("%s: refused reactively on the stream: %v", cell, serr)
					return
				}
				t.Fatalf("%s: a foreign position was ACCEPTED and the resumed instance's write was DELIVERED as the position's continuation (%+v) — the SLM-2 shape", cell, ev)
			case <-deadline:
				t.Fatalf("%s: neither a refusal nor a change arrived within 45s; unmeasured, not safe", cell)
			}
		}
	}
	mustAccept := func(t *testing.T, dsn, cell string, pos ir.Position) {
		t.Helper()
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("%s: OpenCDCReader: %v", cell, err)
		}
		defer closeLineageReader(reader)
		ch, err := reader.StreamChanges(ctx, pos)
		if err != nil {
			t.Fatalf("%s: a legitimate resume was REFUSED: %v", cell, err)
		}
		execSQL(t, ctx, dsn, `INSERT INTO cdc_src.t VALUES (901,'continuation')`)
		deadline := time.After(45 * time.Second)
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					t.Fatalf("%s: stream closed: %v", cell, reader.(*CDCReader).Err())
				}
				// The only write after the resume is the one above, so any
				// delivered change is the continuation.
				t.Logf("%s: accepted and delivered the continuation write", cell)
				return
			case <-deadline:
				t.Fatalf("%s: accepted but delivered nothing within 45s", cell)
			}
		}
	}

	t.Run("different server_id: server-refused, routed to cold-start", func(t *testing.T) {
		dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4", "--server-id=2")
		defer cleanupB()
		execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		mustRefuse(t, dsnB, "different-server-id", capturedOnA)
	})

	t.Run("different gtid_domain_id: server accepts, sluice refuses", func(t *testing.T) {
		dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4", "--server-id=4", "--gtid-domain-id=7")
		defer cleanupB()
		execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		execSQL(t, ctx, dsnB, `INSERT INTO cdc_src.t VALUES (10,'x'),(11,'y')`)
		mustRefuse(t, dsnB, "different-domain", capturedOnA)
		// The domain door alone must also catch the anchorless legacy
		// shape here — the server would accept it.
		mustRefuse(t, dsnB, "different-domain/legacy-position", legacyPos)
	})

	t.Run("rebuilt: same server_id, colliding GTIDs — only the anchor can tell", func(t *testing.T) {
		dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
		defer cleanupB()
		// Three transactions, like A (the container helper's own CREATE
		// DATABASE is the first): the rebuilt instance's own history then
		// reads the same "0-1-3" A's position names, with different data.
		execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		execSQL(t, ctx, dsnB, `INSERT INTO cdc_src.t VALUES (10,'x'),(11,'y')`)
		state := globalVar(t, ctx, dsnB, "gtid_binlog_pos")
		if state != decoded.GTIDSet {
			t.Fatalf("premise gone: the rebuilt instance's state is %q, A's position is %q — the collision this cell reproduces did not happen", state, decoded.GTIDSet)
		}
		mustRefuse(t, dsnB, "rebuilt-colliding", capturedOnA)
		// The anchorless legacy position on this instance is the documented
		// degraded posture: accepted, with the WARN — refusing would force a
		// full re-copy on every pre-v0.138.0 chain.
		mustAccept(t, dsnB, "rebuilt-colliding/legacy-position", legacyPos)
	})

	t.Run("same instance: accepted", func(t *testing.T) {
		mustAccept(t, dsnA, "same-instance", capturedOnA)
	})

	t.Run("rebuilt colliding instance whose numbering never reached the anchor's file: refused", func(t *testing.T) {
		// A different instance can be absent the anchor's file for the
		// other reason — it never rotated that far. The instance must
		// COLLIDE (same server_id, same domain, the same "0-1-3") so the
		// server itself accepts the position and ONLY sluice's purge
		// disambiguation stands between the resume and a whole-history
		// replay: a cell the server catches first would pass with the
		// disambiguation mutated to "always purged" (it did, in the first
		// cut of this cell). Synthesise a high-numbered anchor file: the
		// absence must be read as "different instance", not as a purge.
		dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
		defer cleanupB()
		execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		execSQL(t, ctx, dsnB, `INSERT INTO cdc_src.t VALUES (10,'x'),(11,'y')`)
		if state := globalVar(t, ctx, dsnB, "gtid_binlog_pos"); state != decoded.GTIDSet {
			t.Fatalf("premise gone: the rebuilt instance's state is %q, A's position is %q", state, decoded.GTIDSet)
		}
		high := decoded
		high.LineageFile = "mysqld-bin.000077"
		highPos, err := encodeBinlogPos(high)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		mustRefuse(t, dsnB, "rebuilt-colliding/high-anchor", highPos)
	})

	t.Run("anchor purged by retention on the SAME lineage: accepted", func(t *testing.T) {
		// The reviewer's scenario: a start-of-stream anchor carried
		// forever would refuse here, once per retention window, forever.
		// Small binlogs so 60 writes rotate many times; then purge past
		// the anchor's file while the GTID resume point stays retained
		// (the oldest retained file's start state covers it).
		dsnP, cleanupP := newMariaDBDedicatedForCDC(t, "mariadb:11.4", "--server-id=3", "--max-binlog-size=4096")
		defer cleanupP()
		execSQL(t, ctx, dsnP, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v VARCHAR(200))`)
		snapP, err := e.OpenBackupSnapshot(ctx, dsnP, irbackup.SnapshotOptions{})
		if err != nil {
			t.Fatalf("OpenBackupSnapshot(P): %v", err)
		}
		capturedOnP := snapP.Position
		_ = snapP.Close()
		var dp binlogPos
		if err := json.Unmarshal([]byte(capturedOnP.Token), &dp); err != nil || dp.LineageFile == "" {
			t.Fatalf("P's position has no anchor: %q %v", capturedOnP.Token, err)
		}
		// Rotate right after the capture, so the NEXT file starts at exactly
		// the resume set — that file is what keeps the GTID resume point
		// retained while the anchor's own file is purged (a stopped stream
		// whose last persisted position sits at the end of a file that
		// retention later removes). Then enough writes to rotate many times.
		execSQL(t, ctx, dsnP, `FLUSH LOGS`)
		for i := 100; i < 160; i++ {
			execSQL(t, ctx, dsnP, fmt.Sprintf(`INSERT INTO cdc_src.t VALUES (%d, REPEAT('x', 150))`, i))
		}
		anchorNo, _ := binlogFileNumber(dp.LineageFile)
		db, err := sql.Open("mysql", dsnP)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		names, err := binaryLogNames(ctx, db)
		_ = db.Close()
		if err != nil || len(names) < 3 {
			t.Fatalf("list binary logs: %v (%v)", err, names)
		}
		// Purge exactly through the anchor's file: the file after it (which
		// starts at the resume set) must survive.
		keepFrom := ""
		for _, n := range names {
			if no, ok := binlogFileNumber(n); ok && no == anchorNo+1 {
				keepFrom = n
			}
		}
		if keepFrom == "" {
			t.Fatalf("no binlog numbered %d+1 among %v", anchorNo, names)
		}
		execSQL(t, ctx, dsnP, `PURGE BINARY LOGS TO '`+keepFrom+`'`)
		// Premise: the anchor's file really is gone now.
		dbP, err := sql.Open("mysql", dsnP)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		names, err = binaryLogNames(ctx, dbP)
		_ = dbP.Close()
		if err != nil {
			t.Fatalf("binary logs: %v", err)
		}
		for _, n := range names {
			if n == dp.LineageFile {
				t.Fatalf("premise gone: the anchor file %s survived the purge (retained: %v)", dp.LineageFile, names)
			}
		}
		// The GTID resume point is retained through the newest file's
		// start state, so this is a legitimate resume; MariaDB streams
		// the 60 rows plus the continuation write. Drain and require
		// the stream to be open and delivering.
		//
		// And it must SAY the lineage was not verified (Bug 261): the
		// evidence this branch has — the oldest retained file above the
		// anchor starts at a state covering the anchor's set — is exactly
		// what a rebuilt colliding instance reproduces at a file boundary,
		// and MariaDB has no second witness. v0.138.0 logged INFO
		// "lineage confirmed" here and the regression cycle recorded a
		// foreign instance's rows as a chain delta at exit 0 under it.
		// Capturing WARN+ and requiring the marker is the pin: a revert
		// to the INFO wording, or to a silent accept, fails this cell.
		var warnLog bytes.Buffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&warnLog, &slog.HandlerOptions{Level: slog.LevelWarn})))
		mustAccept(t, dsnP, "same-lineage/anchor-purged", capturedOnP)
		slog.SetDefault(prevLogger)
		if !strings.Contains(warnLog.String(), unverifiedInstanceIdentityMarker) {
			t.Fatalf("anchor-purged resume proceeded WITHOUT the %s WARN — the branch is claiming a lineage it cannot verify (Bug 261). WARN+ log:\n%s",
				unverifiedInstanceIdentityMarker, warnLog.String())
		}
		if strings.Contains(warnLog.String(), "lineage confirmed") {
			t.Fatalf("anchor-purged resume still claims 'lineage confirmed' (Bug 261). WARN+ log:\n%s", warnLog.String())
		}
	})

	t.Run("rotation while streaming moves the anchor to the new file", func(t *testing.T) {
		// A FRESH position at A's current tip, so the stream has no backlog
		// from earlier cells and the first delivered change is the one
		// written after the rotation below.
		snapNow, err := e.OpenBackupSnapshot(ctx, dsnA, irbackup.SnapshotOptions{})
		if err != nil {
			t.Fatalf("OpenBackupSnapshot(A, now): %v", err)
		}
		fromNow := snapNow.Position
		_ = snapNow.Close()
		reader, err := e.OpenCDCReader(ctx, dsnA)
		if err != nil {
			t.Fatalf("OpenCDCReader(A): %v", err)
		}
		defer closeLineageReader(reader)
		ch, err := reader.StreamChanges(ctx, fromNow)
		if err != nil {
			t.Fatalf("StreamChanges(A): %v", err)
		}
		execSQL(t, ctx, dsnA, `FLUSH LOGS`)
		execSQL(t, ctx, dsnA, `INSERT INTO cdc_src.t VALUES (7000,'after-rotate')`)
		dbA, err := sql.Open("mysql", dsnA)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		names, err := binaryLogNames(ctx, dbA)
		_ = dbA.Close()
		if err != nil || len(names) == 0 {
			t.Fatalf("binary logs: %v", err)
		}
		newest := names[len(names)-1]
		deadline := time.After(45 * time.Second)
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					t.Fatalf("stream closed: %v", reader.(*CDCReader).Err())
				}
				var got binlogPos
				if err := json.Unmarshal([]byte(ev.Pos().Token), &got); err != nil {
					t.Fatalf("decode emitted position: %v", err)
				}
				if got.LineageFile != newest {
					t.Fatalf("the position emitted after a rotation carries anchor %s:%d; want the new file %s (the anchor must follow the stream or retention purges it)", got.LineageFile, got.LineagePos, newest)
				}
				if got.LineagePos != 4 || got.LineageSet == "" {
					t.Fatalf("re-anchored position is malformed: %+v", got)
				}
				t.Logf("re-anchored at %s:%d set=%s", got.LineageFile, got.LineagePos, got.LineageSet)
				return
			case <-deadline:
				t.Fatal("no change delivered within 45s after the rotation")
			}
		}
	})
}

func assertGTIDPositionUnder(t *testing.T, door string, p ir.Position, wantUUID string) string {
	t.Helper()
	var decoded binlogPos
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		t.Fatalf("%s: token %q is not a binlogPos: %v", door, p.Token, err)
	}
	if decoded.Mode != positionModeGTID {
		t.Fatalf("%s: expected the GTID arm (gtid_mode is ON), got mode %q", door, decoded.Mode)
	}
	if !strings.Contains(decoded.GTIDSet, wantUUID) {
		t.Fatalf("%s: GTID set %q does not carry the source's uuid %q", door, decoded.GTIDSet, wantUUID)
	}
	return decoded.GTIDSet
}

func closeLineageReader(r ir.CDCReader) {
	if c, ok := r.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

func execSQL(t *testing.T, ctx context.Context, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func globalVar(t *testing.T, ctx context.Context, dsn, name string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var v string
	if err := db.QueryRowContext(ctx, "SELECT @@global."+name).Scan(&v); err != nil {
		t.Fatalf("read @@global.%s: %v", name, err)
	}
	return v
}
