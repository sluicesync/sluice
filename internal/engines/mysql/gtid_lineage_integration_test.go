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
	"os"
	"strconv"
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
		t, ctx, lineageMySQLImage(), "lineage", mysqlFilePosEnvs("lineage"), mysqlGTIDBootCmd,
	)
	defer cleanupA()
	dsnB, cleanupB := startMySQLFamilyContainer(
		t, ctx, lineageMySQLImage(), "lineage", mysqlFilePosEnvs("lineage"), mysqlGTIDBootCmd,
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
	// Audit 2026-09-09 A0909-MYSQL-HIGH-1 flipped this expectation. B has a
	// NON-EMPTY gtid_executed sharing no UUID with A's position: a
	// different lineage. Routing that into the cold-start re-snapshot is
	// exactly the destructive path the worker measured (target dropped
	// and refilled from B). It must be TERMINAL — the foreign sentinel,
	// never the invalid-position one.
	if !errors.Is(err, ir.ErrPositionForeignLineage) {
		t.Fatalf("cross-instance GTID resume failed, but not with ir.ErrPositionForeignLineage: %v", err)
	}
	if errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("cross-instance GTID resume wraps ir.ErrPositionInvalid — the streamer would drop the target and "+
			"re-copy from the wrong database (A0909-MYSQL-HIGH-1): %v", err)
	}
	if !strings.Contains(err.Error(), "gtid_executed") {
		t.Fatalf("the refusal must name the lineage check (gtid_executed), got: %v", err)
	}

	// Direction 1b — a REBUILT instance after RESET MASTER: an EMPTY
	// gtid_executed on a server whose @@server_uuid A's position has
	// never seen. Until audit 2026-09-15 A0915-MYSQL-HIGH-2 this cell was
	// titled "the SAME instance after RESET MASTER" and expected
	// ir.ErrPositionInvalid — while booting a DIFFERENT container. It was
	// measuring the rebuilt-node shape and pinning the destructive route
	// for it: measured end to end, the automatic re-copy reduced the
	// target to the rebuild's stale rows at exit 0. The real same-server
	// cells are at the end of this test, on A itself.
	dsnD, cleanupD := startMySQLFamilyContainer(
		t, ctx, lineageMySQLImage(), "lineage", mysqlFilePosEnvs("lineage"), mysqlGTIDBootCmd,
	)
	defer cleanupD()
	uuidD := serverUUIDOf(t, ctx, dsnD)
	if uuidD == uuidA || strings.Contains(setA, uuidD) {
		t.Fatalf("premise gone: the rebuilt instance's uuid %q is named by A's position %q", uuidD, setA)
	}
	resetBinaryLogs(t, ctx, dsnD)
	if got := globalVar(t, ctx, dsnD, "gtid_executed"); strings.TrimSpace(got) != "" {
		t.Fatalf("premise gone: RESET MASTER left gtid_executed %q; want empty", got)
	}
	mustBeForeign := func(t *testing.T, cell, dsn string) {
		t.Helper()
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("%s: OpenCDCReader: %v", cell, err)
		}
		defer closeLineageReader(reader)
		_, err = reader.StreamChanges(ctx, capturedOnA)
		if err == nil {
			t.Fatalf("%s: resuming A's position was ACCEPTED", cell)
		}
		if !errors.Is(err, ir.ErrPositionForeignLineage) {
			t.Fatalf("%s: refused, but not as a foreign lineage: %v", cell, err)
		}
		if errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("%s: the refusal also reads as an invalid position — the streamer would drop the target and "+
				"re-copy from the rebuilt instance's stale rows (A0915-MYSQL-HIGH-2): %v", cell, err)
		}
		if !strings.Contains(err.Error(), uuidD) {
			t.Fatalf("%s: the refusal must name the witness (@@server_uuid %s): %v", cell, uuidD, err)
		}
		t.Logf("%s: refused: %v", cell, err)
	}
	mustBeInvalid := func(t *testing.T, cell, dsn string) {
		t.Helper()
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("%s: OpenCDCReader: %v", cell, err)
		}
		defer closeLineageReader(reader)
		_, err = reader.StreamChanges(ctx, capturedOnA)
		if err == nil {
			t.Fatalf("%s: resuming A's position was ACCEPTED", cell)
		}
		if !errors.Is(err, ir.ErrPositionInvalid) || errors.Is(err, ir.ErrPositionForeignLineage) {
			t.Fatalf("%s: must keep the automatic re-copy route (ErrPositionInvalid, not foreign): %v", cell, err)
		}
		t.Logf("%s: refused, re-copy route kept: %v", cell, err)
	}
	// mustBeRolledBack grades the BEHIND-under-its-own-uuid arm: terminal
	// like a foreign lineage, with the rolled-back diagnosis naming the
	// server's own uuid and the target being AHEAD.
	mustBeRolledBack := func(t *testing.T, cell, dsn, ownUUID string) {
		t.Helper()
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("%s: OpenCDCReader: %v", cell, err)
		}
		defer closeLineageReader(reader)
		_, err = reader.StreamChanges(ctx, capturedOnA)
		if err == nil {
			t.Fatalf("%s: resuming A's position was ACCEPTED", cell)
		}
		if !errors.Is(err, ir.ErrPositionForeignLineage) || errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("%s: must be terminal (ErrPositionForeignLineage, not the ErrPositionInvalid re-copy route — "+
				"the target is AHEAD of a rolled-back source): %v", cell, err)
		}
		for _, phrase := range []string{"under this server's own @@server_uuid " + ownUUID, "AHEAD", "--restart-from-scratch"} {
			if !strings.Contains(err.Error(), phrase) {
				t.Fatalf("%s: the refusal is missing %q: %v", cell, phrase, err)
			}
		}
		t.Logf("%s: refused terminally: %v", cell, err)
	}
	mustBeForeign(t, "rebuilt instance, EMPTY executed set", dsnD)

	// Direction 1c — the sibling the refuter measured (S1): the same
	// rebuilt instance restored from an OLDER backup of A, which is what
	// `mysqldump --set-gtid-purged=ON` / the xtrabackup restore step
	// produce: gtid_purged seeded under A's uuid, short of the position.
	// The BEHIND arm; the uuid witness must make it terminal too.
	resetBinaryLogs(t, ctx, dsnD)
	execSQL(t, ctx, dsnD, "SET @@GLOBAL.gtid_purged = '"+behindGTIDSet(t, setA)+"'")
	if got := globalVar(t, ctx, dsnD, "gtid_executed"); !strings.Contains(got, uuidA) {
		t.Fatalf("premise gone: seeding D behind A's position failed: gtid_executed %q", got)
	}
	mustBeForeign(t, "rebuilt instance, BEHIND under the old uuid", dsnD)

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
		t, ctx, lineageMySQLImage(), "lineage", mysqlFilePosEnvs("lineage"), mysqlGTIDBootCmd,
	)
	defer cleanupC()
	resetBinaryLogs(t, ctx, dsnC)
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

	// Direction 4 — the SAME instance, for real this time (A, whose uuid
	// the position names), on both arms, and they now route differently.
	// After RESET MASTER (empty) the automatic re-copy stays — the lineage
	// is this server's own (the v0.148.2 decision, scoped to the instance
	// it was made for). Restored BEHIND its own position (gtid_purged under
	// its own uuid, the in-place restore / rollback shape) is TERMINAL by
	// operator decision 2026-09-15: the target is ahead of such a source,
	// and until the decision this cell pinned the re-copy that reduced it.
	// Last, because RESET MASTER on A ends its usefulness for the accept
	// cells above.
	resetBinaryLogs(t, ctx, dsnA)
	if got := globalVar(t, ctx, dsnA, "gtid_executed"); strings.TrimSpace(got) != "" {
		t.Fatalf("premise gone: RESET MASTER left A's gtid_executed %q; want empty", got)
	}
	mustBeInvalid(t, "same instance, EMPTY executed set (RESET MASTER)", dsnA)
	execSQL(t, ctx, dsnA, "SET @@GLOBAL.gtid_purged = '"+behindGTIDSet(t, setA)+"'")
	if got := globalVar(t, ctx, dsnA, "gtid_executed"); !strings.Contains(strings.ToLower(got), strings.ToLower(uuidA)) {
		t.Fatalf("premise gone: seeding A behind its own position failed: gtid_executed %q", got)
	}
	mustBeRolledBack(t, "same instance, BEHIND its own position", dsnA, uuidA)
}

// lineageMySQLImage is the stock image the MySQL lineage cells boot:
// SLUICE_TEST_MYSQL_IMAGE when set (the version matrix, or a local
// mysql:8.4 run), mysql:8.0 otherwise. Never the pre-baked default of
// that variable: every cell here boots its own gtid_mode=ON command line.
func lineageMySQLImage() string {
	if img := os.Getenv(sharedMySQLImageEnv); img != "" {
		return img
	}
	return "mysql:8.0"
}

// resetBinaryLogs empties a MySQL server's binary logs and GTID state.
// MySQL 8.4 removed RESET MASTER in favour of RESET BINARY LOGS AND GTIDS
// (8.2+); older servers know only the first spelling.
func resetBinaryLogs(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "RESET BINARY LOGS AND GTIDS"); err == nil {
		return
	}
	if _, err := db.ExecContext(ctx, "RESET MASTER"); err != nil {
		t.Fatalf("neither RESET BINARY LOGS AND GTIDS nor RESET MASTER ran: %v", err)
	}
}

// behindGTIDSet returns set with its last interval's upper bound reduced
// by one — a set the same lineage would have had one transaction earlier,
// the shape of a restore from an older backup. A single-uuid, single-
// interval set is what instance A's short history produces; anything
// else fails the premise rather than being guessed at.
func behindGTIDSet(t *testing.T, set string) string {
	t.Helper()
	uuid, interval, ok := strings.Cut(set, ":")
	if !ok || strings.Contains(interval, ",") || strings.Contains(interval, ":") {
		t.Fatalf("premise gone: GTID set %q is not a single uuid:lo-hi interval", set)
	}
	lo, hi, ok := strings.Cut(interval, "-")
	if !ok {
		t.Fatalf("premise gone: GTID set %q has a single-transaction interval; a BEHIND set needs at least two", set)
	}
	n, err := strconv.Atoi(hi)
	if err != nil || n < 2 {
		t.Fatalf("premise gone: GTID set %q upper bound %q", set, hi)
	}
	return uuid + ":" + lo + "-" + strconv.Itoa(n-1)
}

// TestGTIDResumeMariaDBBindsLineage is the MariaDB family, every cell on
// a real server, because the first cut of this test measured ONE cell
// (different server_id — which the server itself refuses) and declared
// MariaDB safe; the pre-tag review then measured the two cells the
// server does NOT refuse. The matrix:
//
//   - different server_id, same domain — the server would refuse (1236
//     "not in the master's binlog"); sluice's anchor door refuses it
//     first, as a foreign lineage.
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
//   - an EMPTY gtid_binlog_state, on a rebuilt instance AND on the same
//     instance after RESET MASTER — both must refuse terminally (operator
//     decision 2026-09-15: MariaDB cannot tell them apart), with a message
//     naming both possibilities.
//
// Every cell asserts an independent expected value: the refusal carries
// the sentinel its routing requires (ir.ErrPositionForeignLineage for a
// terminal verdict, and never the ir.ErrPositionInvalid re-copy route
// alongside it), and an accepted stream must deliver a write made on the
// resumed instance.
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

	// want is the sentinel the refusal must carry — and the OTHER one it
	// must not (audit 2026-09-09 A0909-MYSQL-HIGH-1): a foreign lineage is
	// terminal (ir.ErrPositionForeignLineage), a server-side 1236 that the
	// reactive classifier cannot tell from a reset keeps the automatic
	// re-copy (ir.ErrPositionInvalid).
	// It returns the refusal's text, for the cells that also grade the
	// diagnosis.
	mustRefuse := func(t *testing.T, dsn, cell string, pos ir.Position, want error) string {
		t.Helper()
		other := ir.ErrPositionInvalid
		if errors.Is(want, ir.ErrPositionInvalid) {
			other = ir.ErrPositionForeignLineage
		}
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("%s: OpenCDCReader: %v", cell, err)
		}
		defer closeLineageReader(reader)
		ch, err := reader.StreamChanges(ctx, pos)
		if err != nil {
			if !errors.Is(err, want) || errors.Is(err, other) {
				t.Fatalf("%s: refused, but with the wrong routing (want %v, not %v): %v", cell, want, other, err)
			}
			t.Logf("%s: refused at open: %v", cell, err)
			return err.Error()
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
					if !errors.Is(serr, want) || errors.Is(serr, other) {
						t.Fatalf("%s: stream refused, but with the wrong routing (want %v, not %v): %v", cell, want, other, serr)
					}
					t.Logf("%s: refused reactively on the stream: %v", cell, serr)
					return serr.Error()
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

	t.Run("different server_id: sluice's anchor door refuses it at open as a foreign lineage", func(t *testing.T) {
		// Before A0909-MYSQL-HIGH-1 this cell was named "server-refused,
		// routed to cold-start": the server's 1236 arrived reactively and
		// was classified ErrPositionInvalid. Measured on 2026-09-09, the
		// anchor door catches it first (no binlog event at the anchor on a
		// different instance), and a different instance IS a foreign
		// lineage — terminal, never the automatic re-copy.
		dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4", "--server-id=2")
		defer cleanupB()
		execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		mustRefuse(t, dsnB, "different-server-id", capturedOnA, ir.ErrPositionForeignLineage)
	})

	t.Run("different gtid_domain_id: server accepts, sluice refuses", func(t *testing.T) {
		dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4", "--server-id=4", "--gtid-domain-id=7")
		defer cleanupB()
		execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		execSQL(t, ctx, dsnB, `INSERT INTO cdc_src.t VALUES (10,'x'),(11,'y')`)
		mustRefuse(t, dsnB, "different-domain", capturedOnA, ir.ErrPositionForeignLineage)
		// The domain door alone must also catch the anchorless legacy
		// shape here — the server would accept it.
		mustRefuse(t, dsnB, "different-domain/legacy-position", legacyPos, ir.ErrPositionForeignLineage)
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
		mustRefuse(t, dsnB, "rebuilt-colliding", capturedOnA, ir.ErrPositionForeignLineage)
		// The anchorless legacy position on this instance is the documented
		// degraded posture: accepted, with the WARN — refusing would force a
		// full re-copy on every pre-v0.138.0 chain.
		mustAccept(t, dsnB, "rebuilt-colliding/legacy-position", legacyPos)
	})

	t.Run("same instance: accepted", func(t *testing.T) {
		mustAccept(t, dsnA, "same-instance", capturedOnA)
	})

	// measureVerdict drives a resume and reports what happened without
	// grading it: "invalid", "foreign", "accepted" (the resumed instance's
	// own write was delivered as the position's continuation), or the
	// error text for anything else. The KNOWN-GAP cell below uses it so the
	// gap is measured on a real server on every run and stated, rather
	// than asserted away or left as a permanent red.
	measureVerdict := func(t *testing.T, dsn string, pos ir.Position) string {
		t.Helper()
		reader, err := e.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		defer closeLineageReader(reader)
		classify := func(err error) string {
			switch {
			case errors.Is(err, ir.ErrPositionForeignLineage):
				return "foreign"
			case errors.Is(err, ir.ErrPositionInvalid):
				return "invalid"
			}
			return "other: " + err.Error()
		}
		ch, err := reader.StreamChanges(ctx, pos)
		if err != nil {
			return classify(err)
		}
		execSQL(t, ctx, dsn, `INSERT INTO cdc_src.t VALUES (950,'rebuilt-instance-write')`)
		deadline := time.After(45 * time.Second)
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					if serr := reader.(*CDCReader).Err(); serr != nil {
						return classify(serr)
					}
					return "other: stream closed with no error"
				}
				return "accepted"
			case <-deadline:
				return "other: neither a refusal nor a change within 45s"
			}
		}
	}

	// wantEmptyStateDiagnosis grades the empty-state refusal's text: it must
	// name BOTH possibilities, because sluice cannot say which it is, and
	// the remedy for each.
	wantEmptyStateDiagnosis := func(t *testing.T, cell, refusal string) {
		t.Helper()
		for _, phrase := range []string{
			"@@gtid_binlog_state is EMPTY",
			"RESET MASTER on this same server with its data intact",
			"REBUILT",
			"--restart-from-scratch",
			"point the sync at the instance that holds the position",
		} {
			if !strings.Contains(refusal, phrase) {
				t.Fatalf("%s: the empty-state refusal is missing %q: %s", cell, phrase, refusal)
			}
		}
	}

	t.Run("rebuilt instance with an EMPTY gtid_binlog_state (a restore, then RESET MASTER): refused terminally", func(t *testing.T) {
		// Audit 2026-09-15 A0915-MYSQL-HIGH-2, MariaDB arm — measured end to
		// end before the decision: a rebuilt mariadb:11.4 at the same address
		// with stale rows and an empty state took the automatic re-copy and
		// the target was reduced to the stale rows at exit 0. MariaDB has no
		// instance identity (no @@server_uuid; GTIDs carry domain-server-seq
		// only), so this shape and a same-server RESET MASTER (the next cell)
		// are indistinguishable from the server's answers; the operator
		// decision (2026-09-15) is that both refuse. Until the decision this
		// cell was a measured KNOWN GAP asserting the 'invalid' verdict.
		dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
		defer cleanupB()
		execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		execSQL(t, ctx, dsnB, `INSERT INTO cdc_src.t VALUES (10,'stale'),(11,'stale')`)
		execSQL(t, ctx, dsnB, `RESET MASTER`)
		if got := globalVar(t, ctx, dsnB, "gtid_binlog_state"); strings.TrimSpace(got) != "" {
			t.Fatalf("premise gone: RESET MASTER left gtid_binlog_state %q; want empty", got)
		}
		wantEmptyStateDiagnosis(t, "rebuilt-empty-state",
			mustRefuse(t, dsnB, "rebuilt-empty-state", capturedOnA, ir.ErrPositionForeignLineage))
	})

	t.Run("same server after RESET MASTER, data intact: refused terminally too", func(t *testing.T) {
		// The cost side of the decision, measured rather than assumed: the
		// SAME instance that issued the position, reset with every row still
		// in place, refuses the same way — MariaDB cannot tell it from the
		// rebuilt node above, so this reset now takes one deliberate
		// --restart-from-scratch. A dedicated instance, because RESET MASTER
		// on A would end its usefulness for the cells below.
		dsnS, cleanupS := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
		defer cleanupS()
		execSQL(t, ctx, dsnS, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
		execSQL(t, ctx, dsnS, `INSERT INTO cdc_src.t VALUES (1,'a'),(2,'b'),(3,'c')`)
		snapS, err := e.OpenBackupSnapshot(ctx, dsnS, irbackup.SnapshotOptions{})
		if err != nil {
			t.Fatalf("OpenBackupSnapshot(S): %v", err)
		}
		capturedOnS := snapS.Position
		_ = snapS.Close()
		var dS binlogPos
		if err := json.Unmarshal([]byte(capturedOnS.Token), &dS); err != nil || dS.Mode != positionModeGTID || dS.GTIDSet == "" {
			t.Fatalf("premise gone: S's position is not a non-empty GTID position: %q %v", capturedOnS.Token, err)
		}
		execSQL(t, ctx, dsnS, `RESET MASTER`)
		if got := globalVar(t, ctx, dsnS, "gtid_binlog_state"); strings.TrimSpace(got) != "" {
			t.Fatalf("premise gone: RESET MASTER left gtid_binlog_state %q; want empty", got)
		}
		dbS, err := sql.Open("mysql", dsnS)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		var rows int
		err = dbS.QueryRowContext(ctx, `SELECT COUNT(*) FROM cdc_src.t`).Scan(&rows)
		_ = dbS.Close()
		if err != nil || rows != 3 {
			t.Fatalf("premise gone: the reset server must still hold its 3 rows (got %d, %v)", rows, err)
		}
		wantEmptyStateDiagnosis(t, "same-server-reset",
			mustRefuse(t, dsnS, "same-server-reset", capturedOnS, ir.ErrPositionForeignLineage))
	})

	t.Run("KNOWN GAP S2: a byte-identical rebuild collides with the lineage anchor and passes both doors", func(t *testing.T) {
		// The refuter's S2 (audit 2026-09-15): a stock rebuilt container
		// reproduces the ORIGINAL's GTID state at the same binlog byte,
		// because the same image, --server-id and statements produce the
		// same events at the same offsets. Then the domain door passes
		// (domain 0 present), the anchor door passes (BINLOG_GTID_POS at
		// the anchor reads exactly the recorded set), the server accepts
		// the position, and the rebuilt instance's history streams as the
		// original's continuation with NO lineage WARN. The existing
		// "rebuilt: same server_id, colliding GTIDs" cell above catches its
		// rebuild only because its data differ in LENGTH from A's, so the
		// anchor offset is not an event boundary there — orchestrated
		// rebuilds are byte-identical. mariadb_lineage.go names this
		// residual as an accidental collision; on containerised MariaDB it
		// is systematic. No second witness exists today (candidate: the
		// anchor file's Format_description timestamp); behaviour is
		// deliberately UNCHANGED, the cell measures and states the gap.
		//
		// NOT reached by the 2026-09-15 empty-state refusal, and that is
		// why this is the one MariaDB known-gap cell left: the rebuild's
		// @@gtid_binlog_state is NOT empty — it carries domain 0 at the
		// colliding sequence — so the domain door passes on presence before
		// the empty-state branch is ever consulted, and the anchor door
		// passes on the collision.
		dsnA2, cleanupA2 := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
		defer cleanupA2()
		dsnB2, cleanupB2 := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
		defer cleanupB2()
		for _, dsn := range []string{dsnA2, dsnB2} {
			execSQL(t, ctx, dsn, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
			execSQL(t, ctx, dsn, `INSERT INTO cdc_src.t VALUES (1,'a'),(2,'b'),(3,'c')`)
			execSQL(t, ctx, dsn, `FLUSH LOGS`)
		}
		snapA2, err := e.OpenBackupSnapshot(ctx, dsnA2, irbackup.SnapshotOptions{})
		if err != nil {
			t.Fatalf("OpenBackupSnapshot(A2): %v", err)
		}
		capturedOnA2 := snapA2.Position
		_ = snapA2.Close()
		var dA2 binlogPos
		if err := json.Unmarshal([]byte(capturedOnA2.Token), &dA2); err != nil || dA2.LineageFile == "" {
			t.Fatalf("A2's position has no anchor: %q %v", capturedOnA2.Token, err)
		}
		// The premise, asserted on B2: the same file, offset and state as
		// A2's anchor — i.e. both doors are inert by construction.
		dbB2, err := sql.Open("mysql", dsnB2)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		setAtAnchor, ok, err := mariadbLineageSetAt(ctx, dbB2, dA2.LineageFile, dA2.LineagePos)
		_ = dbB2.Close()
		if err != nil {
			t.Fatalf("BINLOG_GTID_POS on B2: %v", err)
		}
		if !ok || setAtAnchor != dA2.LineageSet {
			t.Fatalf("premise gone: B2 reads %q (ok=%v) at A2's anchor %s:%d, A2 recorded %q — the byte-identical "+
				"collision this cell measures did not happen", setAtAnchor, ok, dA2.LineageFile, dA2.LineagePos, dA2.LineageSet)
		}
		switch verdict := measureVerdict(t, dsnB2, capturedOnA2); verdict {
		case "accepted":
			t.Logf("KNOWN GAP S2 (A0915-MYSQL-HIGH-2, MariaDB), measured and unchanged by design: a byte-identical rebuilt instance passed the domain "+
				"door AND the anchor door (anchor %s:%d = %q on both), the server accepted the position, and the "+
				"rebuild's own write was delivered as the original's continuation with no lineage WARN. No second "+
				"witness exists on MariaDB; policy undecided", dA2.LineageFile, dA2.LineagePos, dA2.LineageSet)
		case "foreign":
			t.Fatalf("the MariaDB S2 gap has been CLOSED (verdict foreign): promote this cell to " +
				"mustRefuse(..., ir.ErrPositionForeignLineage)")
		default:
			t.Fatalf("byte-identical rebuilt MariaDB: unexpected verdict %q (want the known-gap 'accepted', or "+
				"'foreign' once the gap is closed)", verdict)
		}
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
		mustRefuse(t, dsnB, "rebuilt-colliding/high-anchor", highPos, ir.ErrPositionForeignLineage)
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
