//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit B-4: the ORDER of the lock-free snapshot capture, ground-truthed
// against the server's own general log.
//
// The unit pin next door proves the RESOLVER always returns the earlier
// tip. It cannot prove the thing that actually moved the residual from
// loss to duplication, which is that the position statement now executes
// BEFORE `START TRANSACTION WITH CONSISTENT SNAPSHOT` on the lock-free
// path. That is statement sequence inside a function that talks to a live
// server, so the evidence has to come from the server.
//
// The independent expected value here is `mysql.general_log`: MySQL's own
// record of what it was asked to run, in the order it was asked. Nothing
// sluice reports about itself is consulted.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestLockFreeSnapshotCapturesPositionBeforeSnapshot forces the no-FTWRL
// fallback with a least-privilege user (no RELOAD) and asserts, from the
// server's general log, that the binlog-position read precedes the
// consistent-snapshot transaction.
func TestLockFreeSnapshotCapturesPositionBeforeSnapshot(t *testing.T) {
	host, port, rootUser, rootPass := ensureSharedMySQL(t)
	resetSharedDB(t, "lockfree_db")
	rootDSN := sharedDSN(host, port, rootUser, rootPass, "lockfree_db")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	root, err := sql.Open("mysql", rootDSN+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer func() { _ = root.Close() }()

	// A user that can read rows and the binlog but CANNOT run FLUSH
	// TABLES WITH READ LOCK — the RDS shape, reproduced by grant rather
	// than by mocking the failure.
	const probeUser = "lockfree_probe"
	const probePass = "probe-pw-1"
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

	// Turn the server's own statement log on for the window of the
	// capture, and off again afterwards — this is a globally-scoped
	// setting on a shared container.
	lockfreeExec(t, ctx, root, `SET GLOBAL log_output = 'TABLE'`)
	lockfreeExec(t, ctx, root, `TRUNCATE TABLE mysql.general_log`)
	lockfreeExec(t, ctx, root, `SET GLOBAL general_log = ON`)
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = root.ExecContext(cctx, `SET GLOBAL general_log = OFF`)
	}()

	probeDSN := sharedDSN(host, port, probeUser, probePass, "lockfree_db")
	stream, err := (Engine{Flavor: FlavorVanilla}).OpenSnapshotStream(ctx, probeDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream as a no-RELOAD user: %v", err)
	}
	_ = stream.Close()

	lockfreeExec(t, ctx, root, `SET GLOBAL general_log = OFF`)

	// Read back the probe user's statements, in the order the server
	// logged them.
	rows, err := root.QueryContext(ctx,
		`SELECT CONVERT(argument USING utf8mb4)
		   FROM mysql.general_log
		  WHERE user_host LIKE ?
		    AND command_type = 'Query'`, probeUser+"%")
	if err != nil {
		t.Fatalf("read general_log: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var (
		stmts        []string
		posIdx       = -1
		snapIdx      = -1
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
		case upperContain(arg, "BINARY LOG STATUS"), upperContain(arg, "MASTER STATUS"), upperContain(arg, "BINLOG STATUS"):
			if posIdx < 0 {
				posIdx = i
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate general_log: %v", err)
	}

	// Anti-vacuity: if the log captured nothing, an ordering assertion
	// over an empty list would pass while proving nothing.
	if len(stmts) == 0 {
		t.Fatal("general_log captured no statements for the probe user — the log is not recording, so this gate is vacuous")
	}
	if !sawFTWRLTry {
		t.Error("expected the capture to ATTEMPT FLUSH TABLES WITH READ LOCK before falling back")
	}
	if posIdx < 0 || snapIdx < 0 {
		t.Fatalf("did not observe both statements (position idx=%d, snapshot idx=%d) in:\n%s",
			posIdx, snapIdx, strings.Join(stmts, "\n"))
	}
	if posIdx > snapIdx {
		t.Errorf("the binlog position was captured AFTER the consistent snapshot opened (position at %d, "+
			"snapshot at %d). That is the ordering where a commit inside the capture window lands in NEITHER "+
			"the snapshot nor the CDC tail — audit B-4's silent-loss case. Statements:\n%s",
			posIdx, snapIdx, strings.Join(stmts, "\n"))
	}
}

// lockfreeExec is a small fatal-on-error statement helper for this file.
func lockfreeExec(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec %s: %v", lockfreeTruncate(stmt), err)
	}
}

func lockfreeTruncate(s string) string {
	if len(s) <= 80 {
		return s
	}
	return fmt.Sprintf("%s…", s[:80])
}
