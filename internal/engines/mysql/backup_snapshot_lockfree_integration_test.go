//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit B-4b: the ORDER of the unlocked BACKUP snapshot capture,
// ground-truthed against the server's own general log.
//
// The AST roster next door proves no capture site in this package is
// written in the losing order. It reads source token positions, so it
// cannot prove that the source order is the EXECUTION order once the
// statements are spread across helpers, a conditional coordinated open, and
// a fallback. That evidence has to come from the server.
//
// The independent expected value is `mysql.general_log`: MySQL's own record
// of what it was asked to run, in the order it was asked. Nothing sluice
// reports about itself is consulted. This is the backup-lane twin of
// TestLockFreeSnapshotCapturesPositionBeforeSnapshot — deliberately a
// SEPARATE test rather than a shared one, because "both lanes" meaning
// "both branches of one function" is how B-4b survived B-4.

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestBackupSnapshotCapturesPositionBeforeSnapshot forces the serial floor
// with a least-privilege user (no RELOAD — the RDS shape, reproduced by
// grant rather than by mocking the failure) and asserts, from the server's
// general log, that the chain-anchor read precedes the consistent-snapshot
// transaction.
func TestBackupSnapshotCapturesPositionBeforeSnapshot(t *testing.T) {
	host, port, rootUser, rootPass := ensureSharedMySQL(t)
	resetSharedDB(t, "bkuplockfree_db")
	rootDSN := sharedDSN(host, port, rootUser, rootPass, "bkuplockfree_db")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	root, err := sql.Open("mysql", rootDSN+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer func() { _ = root.Close() }()

	const probeUser = "bkuplockfree_probe"
	const probePass = "probe-pw-2"
	lockfreeExec(t, ctx, root, `DROP USER IF EXISTS '`+probeUser+`'@'%'`)
	lockfreeExec(t, ctx, root, `CREATE USER '`+probeUser+`'@'%' IDENTIFIED BY '`+probePass+`'`)
	lockfreeExec(t, ctx, root, `GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '`+probeUser+`'@'%'`)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = root.ExecContext(cctx, `DROP USER IF EXISTS '`+probeUser+`'@'%'`)
	})

	lockfreeExec(t, ctx, root, `CREATE TABLE widgets (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB`)
	lockfreeExec(t, ctx, root, `INSERT INTO widgets (v) VALUES ('a'), ('b')`)

	lockfreeExec(t, ctx, root, `SET GLOBAL log_output = 'TABLE'`)
	lockfreeExec(t, ctx, root, `TRUNCATE TABLE mysql.general_log`)
	lockfreeExec(t, ctx, root, `SET GLOBAL general_log = ON`)
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = root.ExecContext(cctx, `SET GLOBAL general_log = OFF`)
	}()

	// ReaderParallelism > 1 so the COORDINATED open is attempted first and
	// denied (no RELOAD) — the exact sequence an RDS backup runs, rather
	// than skipping straight to the floor.
	probeDSN := sharedDSN(host, port, probeUser, probePass, "bkuplockfree_db")
	snap, err := (Engine{Flavor: FlavorVanilla}).OpenBackupSnapshot(ctx, probeDSN, irbackup.SnapshotOptions{ReaderParallelism: 4})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot as a no-RELOAD user: %v", err)
	}
	if len(snap.ExtraReaders) != 0 {
		t.Fatalf("coordinated open engaged for a user without RELOAD (%d extra readers); this test must exercise the serial floor",
			len(snap.ExtraReaders))
	}
	if snap.Position.Token == "" {
		t.Error("serial backup snapshot recorded an empty chain anchor")
	}
	if err := snap.CloseFn(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}

	lockfreeExec(t, ctx, root, `SET GLOBAL general_log = OFF`)

	rows, err := root.QueryContext(ctx,
		`SELECT CONVERT(argument USING utf8mb4)
		   FROM mysql.general_log
		  WHERE user_host LIKE ?
		    AND command_type = 'Query'`, probeUser+"%")
	if err != nil {
		t.Fatalf("read general_log: %v", err)
	}
	defer func() { _ = rows.Close() }()

	// Two markers, because the probe and the anchor BOTH read the binlog tip
	// and the log cannot tell those two statements apart. The probe's near
	// edge precedes the snapshot by construction, so asserting on "the first
	// tip read" would pass even with the anchor moved after the snapshot —
	// verified: that mutation passed the first draft of this test. The
	// `gtid_mode` lookup is [captureBackupPosition]'s own opening statement
	// and is emitted nowhere else on this path, so it locates the ANCHOR.
	var (
		stmts        []string
		anchorIdx    = -1
		snapIdx      = -1
		tipsBefore   int
		tipsAfter    int
		sawFTWRLTry  bool
		upperContain = func(s, sub string) bool { return strings.Contains(strings.ToUpper(s), sub) }
	)
	for rows.Next() {
		var arg string
		if err := rows.Scan(&arg); err != nil {
			t.Fatalf("scan general_log: %v", err)
		}
		i := len(stmts)
		stmts = append(stmts, arg)
		switch {
		case upperContain(arg, "FLUSH TABLES WITH READ LOCK"):
			sawFTWRLTry = true
		case upperContain(arg, "START TRANSACTION WITH CONSISTENT SNAPSHOT"):
			if snapIdx < 0 {
				snapIdx = i
			}
		case upperContain(arg, "GTID_MODE"):
			if anchorIdx < 0 {
				anchorIdx = i
			}
		case upperContain(arg, "BINARY LOG STATUS"), upperContain(arg, "MASTER STATUS"),
			upperContain(arg, "BINLOG STATUS"), upperContain(arg, "GTID_EXECUTED"),
			upperContain(arg, "GTID_BINLOG_POS"):
			if snapIdx < 0 {
				tipsBefore++
			} else {
				tipsAfter++
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate general_log: %v", err)
	}

	// Anti-vacuity: an ordering assertion over an empty statement list
	// passes while proving nothing.
	if len(stmts) == 0 {
		t.Fatal("general_log captured no statements for the probe user — the log is not recording, so this gate is vacuous")
	}
	if !sawFTWRLTry {
		t.Error("expected the coordinated open to ATTEMPT FLUSH TABLES WITH READ LOCK before falling back to the serial floor")
	}
	if anchorIdx < 0 || snapIdx < 0 {
		t.Fatalf("did not observe both statements (anchor idx=%d, snapshot idx=%d) in:\n%s",
			anchorIdx, snapIdx, strings.Join(stmts, "\n"))
	}
	if anchorIdx > snapIdx {
		t.Errorf("the chain anchor was captured AFTER the consistent snapshot opened (anchor at %d, snapshot "+
			"at %d). That is the ordering where a commit inside the capture window is above the backup's read "+
			"view and below the recorded EndPosition, so it is in NEITHER the full nor any incremental chained "+
			"off it — audit B-4b's silent-loss case. Statements:\n%s",
			anchorIdx, snapIdx, strings.Join(stmts, "\n"))
	}
	// The window probe brackets the snapshot: near edge before, far edge
	// after. Without both edges [reportBackupCaptureWindow] has nothing to
	// compare and can only ever report UNKNOWN.
	if tipsBefore < 2 || tipsAfter < 1 {
		t.Errorf("binlog-tip reads around the snapshot = %d before / %d after; want at least 2 before (the "+
			"probe's near edge AND the anchor's own tip read) and 1 after (the far edge). Statements:\n%s",
			tipsBefore, tipsAfter, strings.Join(stmts, "\n"))
	}
}
