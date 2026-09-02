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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	defer closeReader(readerB)
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
	defer closeReader(readerA)
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
	defer closeReader(readerC)
	if _, err := readerC.StreamChanges(ctx, capturedOnA); err != nil {
		t.Fatalf("resume on an instance whose gtid_executed CONTAINS the position's set was REFUSED: %v "+
			"(a promoted replica or a --set-gtid-purged=ON restore must resume; the check binds lineage, "+
			"not @@server_uuid)", err)
	}
}

// TestGTIDResumeMariaDBForeignInstanceIsLoud is the MariaDB sibling as a
// MEASUREMENT: the MariaDB arm has no pre-check and relies on the stream
// refusing a position the server cannot serve. A foreign domain-server
// GTID on a fresh instance must be refused loudly — at StreamChanges or on
// the first event — never streamed from wherever the server feels like.
func TestGTIDResumeMariaDBForeignInstanceIsLoud(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	dsnA, cleanupA := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	defer cleanupA()
	dsnB, cleanupB := newMariaDBDedicatedForCDC(t, "mariadb:11.4", "--server-id=2")
	defer cleanupB()

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

	readerB, err := e.OpenCDCReader(ctx, dsnB)
	if err != nil {
		t.Fatalf("OpenCDCReader(B): %v", err)
	}
	defer closeReader(readerB)
	ch, err := readerB.StreamChanges(ctx, capturedOnA)
	if err != nil {
		if !errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("MariaDB foreign-instance resume refused, but not with ir.ErrPositionInvalid: %v", err)
		}
		t.Logf("MariaDB: refused at StreamChanges (pre-check or immediate stream error): %v", err)
		return
	}
	// Accepted at open: the reactive refusal must arrive on the stream
	// itself. Write on B so a silently-accepted stream would have
	// something to deliver, then require an error (not a change) first.
	execSQL(t, ctx, dsnB, `CREATE TABLE cdc_src.t (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, dsnB, `INSERT INTO cdc_src.t VALUES (500,'foreign')`)
	// Errors surface as the channel closing with the reader's Err() set —
	// the reader contract, not a per-change field.
	deadline := time.After(45 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				serr := readerB.(*CDCReader).Err()
				if serr == nil {
					t.Fatal("MariaDB: stream closed with NO error after accepting a foreign GTID position — silent acceptance")
				}
				if !errors.Is(serr, ir.ErrPositionInvalid) {
					t.Fatalf("MariaDB: stream refused, but not with ir.ErrPositionInvalid: %v", serr)
				}
				t.Logf("MariaDB: refused reactively on the stream: %v", serr)
				return
			}
			t.Fatalf("MariaDB: a foreign-instance GTID position was ACCEPTED and B's write was DELIVERED as if it were A's delta (%+v) — silent lineage confusion, the SLM-2 shape on the MariaDB arm", ev)
		case <-deadline:
			t.Fatal("MariaDB: neither a refusal nor a change arrived within 45s after resuming a foreign GTID position; treat as unmeasured, not as safe")
		}
	}
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

func closeReader(r ir.CDCReader) {
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
