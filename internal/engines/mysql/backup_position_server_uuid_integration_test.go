//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The instance-identity binding on a BACKUP-captured file/pos cursor,
// against real servers.
//
// [verifySourceInstanceIdentity] is the loud-failure floor for the "node
// replaced / restored from backup" position-loss class, and its own doc
// names "restored from backup" as the case it exists for. It never
// reached that case: it skips when the persisted uuid is empty, and
// neither backup capturer stamped one, so every manifest EndPosition in
// file/pos mode was undefendable. `backup incremental` and `sync start
// --position-from-manifest` both resume from exactly that value.
//
// Ground truth, 2026-09-01, two independent MySQL 8.0.46 instances with
// gtid_mode=OFF (MySQL 8's DEFAULT — this is not a contrived flag) whose
// binlogs both carried mysql-bin.000001: sluice accepted the
// cross-instance resume, logged `begin to sync binlog from position
// (mysql-bin.000001, 513414)` against the wrong lineage, and finished at
// exit 0 having never applied three rows that existed on the source. No
// WARN, no error, no mention of identity anywhere in the log.
//
// SCOPE. This pins the vanilla-MySQL file/pos arm, which is the only arm
// that can reach the defect: MariaDB is forced onto the GTID arm by
// [gtidModeOnFor] (unconditionally true for that flavor) and has no
// @@server_uuid at all, and the VStream flavors reach a capture door only
// on the degraded fallback. Both capture doors are covered — the
// SchemaReader one and the conn-scoped snapshot sibling — because both
// persist into Manifest.EndPosition and fixing one would have left the
// other, which is the sibling shape this project keeps paying for.
//
// Both DIRECTIONS are asserted. A refusal test that only shows the
// mismatch case cannot distinguish "binds the instance" from "refuses
// every resume", so the same-instance control is load-bearing, not
// decoration.

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// mysqlFilePosBootCmd boots a vanilla MySQL with binary logging ON and
// GTID mode OFF — the state that routes both capturers into the file/pos
// arm. gtid_mode=OFF is MySQL 8's default; it is passed explicitly so the
// test states its own premise rather than inheriting it.
var mysqlFilePosBootCmd = []string{
	"mysqld", "--server-id=1", "--log-bin=mysql-bin",
	"--binlog-format=ROW", "--binlog-row-image=FULL",
	"--gtid-mode=OFF", "--enforce-gtid-consistency=OFF",
}

func mysqlFilePosEnvs(db string) map[string]string {
	return map[string]string{"MYSQL_ROOT_PASSWORD": "rootpw", "MYSQL_DATABASE": db}
}

// TestBackupPositionStampsServerUUID pins that BOTH backup capture doors
// record the source instance identity on a file/pos cursor, and that the
// recorded value is the server's real @@server_uuid rather than any
// non-empty placeholder.
func TestBackupPositionStampsServerUUID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	dsn, cleanup := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "uuidpin", mysqlFilePosEnvs("uuidpin"), mysqlFilePosBootCmd,
	)
	defer cleanup()

	// Premise, read from the server rather than trusted from the flags:
	// binary logging on, GTID off. If either drifts, the capturers take a
	// different arm and this test would silently stop testing anything.
	assertServerVar(t, ctx, dsn, "@@GLOBAL.log_bin", "1")
	assertServerVar(t, ctx, dsn, "@@GLOBAL.gtid_mode", "OFF")

	wantUUID := serverUUIDOf(t, ctx, dsn)
	if wantUUID == "" {
		t.Fatal("server reported an empty @@server_uuid; the premise of this pin is gone")
	}

	e := Engine{Flavor: FlavorVanilla}

	// Door 1: the SchemaReader capturer (irbackup.PositionCapturer) —
	// the fallback path, and the only door a VStream flavor can reach.
	sr, err := e.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	capturer, ok := sr.(interface {
		CaptureBackupPosition(context.Context, string) (ir.Position, error)
	})
	if !ok {
		t.Fatal("SchemaReader does not implement CaptureBackupPosition")
	}
	got, err := capturer.CaptureBackupPosition(ctx, "")
	if err != nil {
		t.Fatalf("CaptureBackupPosition: %v", err)
	}
	assertFilePosCarriesUUID(t, "SchemaReader.CaptureBackupPosition", got, wantUUID)

	// Door 2: the conn-scoped sibling, reached through the real backup
	// snapshot opener — which is the door an actual `backup full` takes.
	// Exercising it via OpenBackupSnapshot rather than calling
	// captureBackupPosition directly is what makes this cover the value
	// that genuinely lands in Manifest.EndPosition.
	snap, err := e.OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}
	defer func() { _ = snap.Close() }()
	assertFilePosCarriesUUID(t, "OpenBackupSnapshot", snap.Position, wantUUID)
}

// TestBackupChainResumeRefusesAcrossInstances is the end-to-end pin: a
// cursor captured by a BACKUP on instance A must not resume against a
// different instance B that reuses the same binlog filename, and must
// still resume cleanly against A.
func TestBackupChainResumeRefusesAcrossInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	dsnA, cleanupA := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "lineage", mysqlFilePosEnvs("lineage"), mysqlFilePosBootCmd,
	)
	defer cleanupA()
	dsnB, cleanupB := startMySQLFamilyContainer(
		t, ctx, "mysql:8.0", "lineage", mysqlFilePosEnvs("lineage"), mysqlFilePosBootCmd,
	)
	defer cleanupB()

	uuidA := serverUUIDOf(t, ctx, dsnA)
	uuidB := serverUUIDOf(t, ctx, dsnB)
	if uuidA == uuidB || uuidA == "" || uuidB == "" {
		t.Fatalf("the two instances must have distinct non-empty identities; got A=%q B=%q", uuidA, uuidB)
	}

	// The defect's precondition, asserted rather than assumed: the two
	// unrelated instances really do reuse the same binlog FILENAME. If
	// they ever stopped doing so, verifyBinlogFilePresent alone would
	// catch the cross-instance resume and this test would be passing for
	// a reason that has nothing to do with the identity check.
	fileA := currentBinlogFile(t, ctx, dsnA)
	fileB := currentBinlogFile(t, ctx, dsnB)
	if fileA != fileB {
		t.Fatalf("premise gone: instances are on different binlog filenames (A=%q B=%q); "+
			"the filename-reuse false-positive this guard exists for is not reproduced", fileA, fileB)
	}

	e := Engine{Flavor: FlavorVanilla}

	// Capture on A through the real backup snapshot door.
	snap, err := e.OpenBackupSnapshot(ctx, dsnA, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot(A): %v", err)
	}
	capturedOnA := snap.Position
	_ = snap.Close()
	assertFilePosCarriesUUID(t, "OpenBackupSnapshot(A)", capturedOnA, uuidA)

	// Direction 1 — the regression: resuming A's cursor on B must refuse
	// with ir.ErrPositionInvalid.
	readerB, err := e.OpenCDCReader(ctx, dsnB)
	if err != nil {
		t.Fatalf("OpenCDCReader(B): %v", err)
	}
	defer func() {
		if c, ok := readerB.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	_, err = readerB.StreamChanges(ctx, capturedOnA)
	if err == nil {
		t.Fatal("resuming instance A's backup cursor against instance B was ACCEPTED; " +
			"this is the silent-gap defect — the syncer streams from a byte offset in an unrelated binlog")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("cross-instance resume failed, but not with ir.ErrPositionInvalid "+
			"(so the streamer will not route it to a cold-start re-snapshot): %v", err)
	}

	// Direction 2 — the control that stops direction 1 from passing for
	// the wrong reason: the SAME cursor on the instance that produced it
	// must be accepted.
	readerA, err := e.OpenCDCReader(ctx, dsnA)
	if err != nil {
		t.Fatalf("OpenCDCReader(A): %v", err)
	}
	defer func() {
		if c, ok := readerA.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if _, err := readerA.StreamChanges(ctx, capturedOnA); err != nil {
		t.Fatalf("same-instance resume of a backup cursor was REFUSED: %v "+
			"(the identity check must bind the instance, not block every resume)", err)
	}
}

func assertFilePosCarriesUUID(t *testing.T, door string, p ir.Position, wantUUID string) {
	t.Helper()
	var decoded binlogPos
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		t.Fatalf("%s: token %q is not a binlogPos: %v", door, p.Token, err)
	}
	if decoded.Mode != positionModeFilePos {
		t.Fatalf("%s: expected the file/pos arm (gtid_mode is OFF), got mode %q", door, decoded.Mode)
	}
	if decoded.ServerUUID == "" {
		t.Fatalf("%s: recorded a file/pos cursor with NO ServerUUID (token %q). "+
			"verifySourceInstanceIdentity skips on an empty persisted uuid, so this cursor cannot "+
			"be defended against a replaced/restored source instance.", door, p.Token)
	}
	if decoded.ServerUUID != wantUUID {
		t.Fatalf("%s: ServerUUID = %q, want the source's @@server_uuid %q",
			door, decoded.ServerUUID, wantUUID)
	}
}

func serverUUIDOf(t *testing.T, ctx context.Context, dsn string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var uuid string
	if err := db.QueryRowContext(ctx, "SELECT @@global.server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("read @@server_uuid: %v", err)
	}
	return uuid
}

func currentBinlogFile(t *testing.T, ctx context.Context, dsn string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var last string
	for rows.Next() {
		dest := make([]any, len(cols))
		holders := make([]any, len(cols))
		for i := range dest {
			holders[i] = &dest[i]
		}
		if err := rows.Scan(holders...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name, ok := scanString(dest[0]); ok {
			last = name
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return last
}
