//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-43 (r) F1: a rotation boundary must not persist a position past an
// in-scope DDL whose schema forward is still owed.
//
// The binlog reader emits a DDL's schema boundary (the ir.SchemaSnapshot that
// drives the ADR-0091 forward) lazily, ahead of the table's next row, and a
// restart forgets it is owed. A rotation boundary on an idle stream is a
// persisted position with no row in it, so before the fix an ALTER, two
// rotations and a restart lost the forward: the target kept the pre-ALTER
// shape and the next row landed in it — microseconds rounded into DATETIME(0),
// a new column's value dropped — at exit 0. The boundary now settles every
// owed table first, in every position mode
// (engines/mysql/cdc_owed_schema_boundary.go). In GTID mode the settled ADD
// COLUMN's backfill cannot be proven durable by a boundary whose set equals the
// DDL's, so the stop refuses ADD-COLUMN-BACKFILL-INCOMPLETE — the documented,
// loud residual — and the restart acknowledges it as an operator would.
//
// The independent expected values are the target's own catalog
// (information_schema.columns) and the row read back from it.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func pgOwedDDLScalar(t *testing.T, dsn, query string, args ...any) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var v sql.NullString
	if err := db.QueryRowContext(ctx, query, args...).Scan(&v); err != nil {
		return "<" + err.Error() + ">"
	}
	return v.String
}

// runIdleRotationOwedDDL: two tables, one gets an fsp widen and the other an
// ADD COLUMN, each a separate shape so the forward is unambiguous; then two
// rotations with no row in either table, a restart, and one row in each.
func runIdleRotationOwedDDL(t *testing.T, engine, sourceDSN string, backfillProvable bool, caughtUp func(t *testing.T, p idleRotationToken) (bool, string)) {
	t.Helper()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE fsp (id BIGINT NOT NULL PRIMARY KEY, ts DATETIME NOT NULL) ENGINE=InnoDB;
		CREATE TABLE addc (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(16) NOT NULL) ENGINE=InnoDB;
		INSERT INTO fsp VALUES (1, '2026-10-02 10:00:00');
		INSERT INTO addc VALUES (1, 'seed');
	`)
	srcEng, ok := engines.Get(engine)
	if !ok {
		t.Fatalf("%s engine not registered", engine)
	}
	pgEng, _ := engines.Get("postgres")
	streamID := "gc43r-f1-" + engine
	newStreamer := func(ack string) *Streamer {
		return &Streamer{
			Source: srcEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: pgDSN, StreamID: streamID,
			AcceptUnforwardedSchemaChange: ack,
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- newStreamer("").Run(ctx1) }()
	if !waitForRowCount(t, pgDSN, "fsp", 1, 120*time.Second) || !waitForRowCount(t, pgDSN, "addc", 1, 60*time.Second) {
		t.Fatal("the cold copy never delivered the seed rows")
	}
	// Prime fsp past its FIRST post-cold-start boundary: the pipeline skips a
	// mutating shape there (classified against the cold-start seed, ADR-0091
	// §3 — logged "skipping a destructive/mutating shape at the first
	// post-cold-start boundary"), which would mask what this test measures.
	applyDDLMySQL(t, sourceDSN, "ALTER TABLE fsp ADD COLUMN prime INT NULL")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO fsp VALUES (2, '2026-10-02 10:00:01', NULL)")
	if !waitForRowPresent(t, pgDSN, "fsp", 2, true, 60*time.Second) {
		t.Fatal("the CDC change never reached the target")
	}

	// The DDLs, then rotations with no row in either table.
	applyDDLMySQL(t, sourceDSN, "ALTER TABLE fsp MODIFY ts DATETIME(6) NOT NULL")
	applyDDLMySQL(t, sourceDSN, "ALTER TABLE addc ADD COLUMN extra VARCHAR(16) NULL")
	applyDDLMySQL(t, sourceDSN, "FLUSH BINARY LOGS")
	applyDDLMySQL(t, sourceDSN, "FLUSH BINARY LOGS")
	caught := false
	var why string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if tok, raw := readIdleRotationToken(t, pgDSN, streamID); raw != "" {
			if caught, why = caughtUp(t, tok); caught {
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	_, persisted := readIdleRotationToken(t, pgDSN, streamID)
	t.Logf("persisted after the DDLs and rotations (caught up: %v — %s): %s", caught, why, persisted)
	cancel1()
	ack := ""
	select {
	case err := <-runErr1:
		switch {
		case err == nil && backfillProvable:
		case err != nil && !backfillProvable && strings.Contains(err.Error(), "ADD-COLUMN-BACKFILL-INCOMPLETE"):
			// The accepted GTID residual: the boundary's set equals the
			// ADD COLUMN's anchor, so on an idle source nothing proves the
			// settled backfill durable before the stop. Loud; acknowledged
			// as an operator would (engines/mysql/cdc_owed_schema_boundary.go).
			recorded := pgOwedDDLScalar(t, pgDSN, `SELECT unforwarded_refusal FROM sluice_cdc_state WHERE stream_id = $1`, streamID)
			ack = unforwardedRefusalFingerprint(recorded)
			t.Logf("GTID stop refused as documented; acknowledging %s", ack)
		default:
			// Not fatal: the target assertions below are the verdict.
			t.Errorf("Streamer.Run returned %v on cancel (backfill provable in this mode: %v)", err, backfillProvable)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}
	// The rotation boundary settles the owed forwards and THEN persists, in
	// every position mode (engines/mysql/cdc_owed_schema_boundary.go).
	if !caught {
		t.Errorf("the persisted position did not reach the tip after the DDLs (%s): the rotation boundary "+
			"did not settle the owed schema boundaries", why)
	}

	// Restart; the first rows after the DDLs.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- newStreamer(ack).Run(ctx2) }()
	applyDDLMySQL(t, sourceDSN, "INSERT INTO fsp VALUES (3, '2026-10-02 10:11:12.345678', NULL)")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO addc VALUES (3, 'after', 'x')")
	gotFsp := waitForRowPresent(t, pgDSN, "fsp", 3, true, 90*time.Second)
	gotAddc := waitForRowPresent(t, pgDSN, "addc", 3, true, 30*time.Second)
	cancel2()
	var runErr error
	select {
	case runErr = <-runErr2:
	case <-time.After(30 * time.Second):
		t.Error("Streamer.Run did not return after cancel")
	}

	precision := pgOwedDDLScalar(t, pgDSN,
		`SELECT datetime_precision::text FROM information_schema.columns WHERE table_name = 'fsp' AND column_name = 'ts'`)
	if precision != "6" {
		t.Errorf("GC-43 (r) F1: target fsp.ts has datetime_precision %q, want 6 — the ALTER's forward was lost "+
			"across the restart. Persisted: %s", precision, persisted)
	}
	if !gotFsp {
		t.Errorf("the post-restart fsp row never arrived (stream: %v)", runErr)
	} else if ts := pgOwedDDLScalar(t, pgDSN, `SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS.US') FROM fsp WHERE id = 3`); ts != "2026-10-02 10:11:12.345678" {
		t.Errorf("GC-43 (r) F1: target fsp row 3 ts = %q, want 2026-10-02 10:11:12.345678", ts)
	}
	if col := pgOwedDDLScalar(t, pgDSN,
		`SELECT data_type FROM information_schema.columns WHERE table_name = 'addc' AND column_name = 'extra'`); col == "" || strings.HasPrefix(col, "<") {
		t.Errorf("GC-43 (r) F1: target addc has no column extra (%q) — the ADD COLUMN forward was lost across "+
			"the restart. Persisted: %s", col, persisted)
	}
	if !gotAddc {
		t.Errorf("the post-restart addc row never arrived (stream: %v)", runErr)
	} else if v := pgOwedDDLScalar(t, pgDSN, `SELECT extra FROM addc WHERE id = 3`); v != "x" {
		t.Errorf("GC-43 (r) F1: target addc row 3 extra = %q, want x", v)
	}
}

func TestStreamer_IdleBinlogRotationOwedDDL_MySQLFilePos(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runIdleRotationOwedDDL(t, "mysql", sourceDSN, true, func(t *testing.T, p idleRotationToken) (bool, string) {
		return latestBinlogCaughtUp(t, sourceDSN, p.File)
	})
}

func TestStreamer_IdleBinlogRotationOwedDDL_MySQLGTID(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	runIdleRotationOwedDDL(t, "mysql", sourceDSN, false, func(t *testing.T, p idleRotationToken) (bool, string) {
		sub := sourceScalar(t, sourceDSN, "SELECT GTID_SUBSET(@@GLOBAL.gtid_executed, ?)", p.GTIDSet)
		return sub == "1", "persisted " + p.GTIDSet
	})
}

func TestStreamer_IdleBinlogRotationOwedDDL_MariaDB(t *testing.T) {
	sourceDSN, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	runIdleRotationOwedDDL(t, "mariadb", sourceDSN, false, func(t *testing.T, p idleRotationToken) (bool, string) {
		pos := sourceScalar(t, sourceDSN, "SELECT @@GLOBAL.gtid_binlog_pos")
		return p.GTIDSet == pos, "persisted " + p.GTIDSet + ", binlog pos " + pos
	})
}
