//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-43 (r): an IDLE binlog stream must persist its position across binlog
// rotations, so a restart after the old files are purged warm-resumes
// instead of dropping and re-copying the target.
//
// Before the fix, only a transaction's commit persisted a position. A
// ROTATE, a heartbeat and a standalone GTID group (DDL, CREATE USER,
// OPTIMIZE) persisted nothing, so an idle stream's persisted position stayed
// in a file the source then rotated past and purged. The restart failed the
// resume check as "purged", and the ADR-0093 auto-resnapshot DROPPED the
// target tables and re-copied them — though nothing had been missed. The
// reader now emits a boundary-only transaction at each rotation
// (engines/mysql/cdc_rotation_boundary.go).
//
// Each test runs the measured shape end to end on a real source with a
// Postgres target (a separate server, so the target's own control-table
// writes cannot put traffic into the source's binlog and mask the idle
// case): cold start, one CDC change, a target-ONLY sentinel row, an idle
// window of rotations (plus, where the harm needs them, standalone GTID
// groups), stop, PURGE BINARY LOGS, restart.
//
// The independent expected value is the SENTINEL: a row that exists only on
// the target. A warm resume leaves it; the auto-resnapshot drops the table
// and re-copies from the source, which never had it. The log markers and the
// persisted position are corroboration, not the verdict.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// idleRotationCase is one source shape for runIdleBinlogRotation.
type idleRotationCase struct {
	// engine is the registered source engine name.
	engine string
	// sourceDSN is the source database (source_db).
	sourceDSN string
	// idleStatements run in the idle window before the rotations: the
	// standalone GTID groups a harm needs, or none for a fully idle source.
	idleStatements []string
	// caughtUp reports whether the persisted position has reached the
	// source's current binlog tip, with a description for the log.
	caughtUp func(t *testing.T, persisted idleRotationToken) (bool, string)
	// noUnverifiedIdentity additionally requires the restart not to log
	// UNVERIFIED-INSTANCE-IDENTITY (the MariaDB lineage anchor harm).
	noUnverifiedIdentity bool
	// checkPersisted runs on the position persisted after the first CDC
	// change, before any rotation.
	checkPersisted func(t *testing.T, persisted idleRotationToken)
}

// idleRotationToken is the binlog position token as the target stores it —
// decoded here from its JSON, independently of the reader's codec.
type idleRotationToken struct {
	Mode        string `json:"mode"`
	GTIDSet     string `json:"gtid_set"`
	File        string `json:"file"`
	Pos         uint32 `json:"pos"`
	LineageFile string `json:"lineage_file"`
	LineagePos  uint32 `json:"lineage_pos"`
	LineageSet  string `json:"lineage_set"`
}

func readIdleRotationToken(t *testing.T, pgDSN, streamID string) (idleRotationToken, string) {
	t.Helper()
	raw := readPersistedPositionTolerant(pgDSN, streamID)
	var tok idleRotationToken
	if raw == "" {
		return tok, raw
	}
	if err := json.Unmarshal([]byte(raw), &tok); err != nil {
		t.Fatalf("persisted position %q is not a binlog position token: %v", raw, err)
	}
	return tok, raw
}

// sourceBinlogFiles lists the source's retained binlogs, oldest first.
func sourceBinlogFiles(t *testing.T, dsn string) []string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var names []string
	for rows.Next() {
		dest := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, string(dest[0]))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return names
}

func sourceScalar(t *testing.T, dsn, query string, args ...any) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var v sql.NullString
	if err := db.QueryRowContext(ctx, query, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return v.String
}

// idleRotationResnapshotMarkers are the log lines of the two re-snapshot
// routes: the proactive warm-resume fall-through and the reactive ADR-0093
// recovery (a purged position the server itself refuses, e.g. MariaDB 1236).
var idleRotationResnapshotMarkers = []string{
	"persisted position is no longer valid",
	"auto re-snapshotting",
}

const (
	idleRotationSentinelID = 1_000_000
	idleRotationAfterID    = 500
)

func runIdleBinlogRotation(t *testing.T, c idleRotationCase) {
	t.Helper()
	logBuf := &logcapture.Buffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prevDefault)

	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyDDLMySQL(t, c.sourceDSN, `
		CREATE TABLE rot (
			id      BIGINT       NOT NULL,
			payload VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO rot (id, payload) VALUES (1, 'seed-1'), (2, 'seed-2'), (3, 'seed-3');
	`)

	srcEng, ok := engines.Get(c.engine)
	if !ok {
		t.Fatalf("%s engine not registered", c.engine)
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamID := "gc43r-idle-rotation-" + c.engine
	newStreamer := func() *Streamer {
		return &Streamer{Source: srcEng, Target: pgEng, SourceDSN: c.sourceDSN, TargetDSN: pgDSN, StreamID: streamID}
	}
	stopStreamer := func(cancel context.CancelFunc, runErr <-chan error, phase string) {
		t.Helper()
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Fatalf("%s: Streamer.Run returned %v on cancel", phase, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("%s: Streamer.Run did not return after cancel", phase)
		}
	}

	// ---- Phase 1: cold start, one CDC change. ----
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- newStreamer().Run(ctx1) }()
	if !waitForRowCount(t, pgDSN, "rot", 3, 120*time.Second) {
		t.Fatal("phase 1: the cold copy never delivered the seed rows")
	}
	applyDDLMySQL(t, c.sourceDSN, "INSERT INTO rot (id, payload) VALUES (4, 'cdc-1')")
	if !waitForRowPresent(t, pgDSN, "rot", 4, true, 60*time.Second) {
		t.Fatal("phase 1: the CDC change never reached the target")
	}
	// The position the CDC change persisted (the apply loop persists after
	// the row lands; give it a moment to be the CDC commit, not the handoff).
	var before idleRotationToken
	var beforeRaw string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		before, beforeRaw = readIdleRotationToken(t, pgDSN, streamID)
		if beforeRaw != "" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if beforeRaw == "" {
		t.Fatal("phase 1: no persisted position after the CDC change")
	}
	t.Logf("persisted before the idle window: %s", beforeRaw)
	if c.checkPersisted != nil {
		time.Sleep(2 * time.Second)
		before, beforeRaw = readIdleRotationToken(t, pgDSN, streamID)
		c.checkPersisted(t, before)
	}

	// The sentinel: on the target only.
	applyPGDDL(t, pgDSN, "INSERT INTO rot (id, payload) VALUES (1000000, 'target-only-sentinel')")

	// ---- Phase 2: the idle window. ----
	for _, stmt := range c.idleStatements {
		applyDDLMySQL(t, c.sourceDSN, stmt)
	}
	applyDDLMySQL(t, c.sourceDSN, "FLUSH BINARY LOGS")
	applyDDLMySQL(t, c.sourceDSN, "FLUSH BINARY LOGS")
	// Wait for the persisted position to reach the tip. Not asserted here:
	// the verdict is the restart below, which is what an operator sees, and
	// a mutant that never catches up must reach it to show the harm.
	caught := false
	var why string
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		tok, raw := readIdleRotationToken(t, pgDSN, streamID)
		if raw != "" {
			if caught, why = c.caughtUp(t, tok); caught {
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	_, afterRaw := readIdleRotationToken(t, pgDSN, streamID)
	t.Logf("persisted after the idle window (caught up: %v — %s): %s", caught, why, afterRaw)
	stopStreamer(cancel1, runErr1, "phase 2")

	// ---- Phase 3: retention purges everything the stream has passed. ----
	purgeAllButLatestBinlog(t, c.sourceDSN)
	t.Logf("retained after purge: %v", sourceBinlogFiles(t, c.sourceDSN))

	// ---- Phase 4: restart; it must warm-resume. ----
	restartLogFrom := logBuf.Len()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- newStreamer().Run(ctx2) }()
	applyDDLMySQL(t, c.sourceDSN, "INSERT INTO rot (id, payload) VALUES (500, 'after-restart')")
	if !waitForRowPresent(t, pgDSN, "rot", idleRotationAfterID, true, 120*time.Second) {
		select {
		case err := <-runErr2:
			t.Fatalf("phase 4: the post-restart change never reached the target; Run returned %v", err)
		default:
			t.Fatal("phase 4: the post-restart change never reached the target")
		}
	}
	restartLog := logBuf.String()[restartLogFrom:]
	if !pgRowExists(t, pgDSN, "rot", idleRotationSentinelID) {
		t.Errorf("GC-43 (r): the target-only sentinel row is GONE after the restart — the restart re-snapshotted "+
			"(dropped and re-copied the target) instead of warm-resuming, though the stream was connected through "+
			"every rotation and missed nothing. Persisted before purge: %s", afterRaw)
	}
	for _, m := range idleRotationResnapshotMarkers {
		if strings.Contains(restartLog, m) {
			t.Errorf("GC-43 (r): the restart logged %q — the persisted position did not survive the purge", m)
		}
	}
	if c.noUnverifiedIdentity && strings.Contains(restartLog, "UNVERIFIED-INSTANCE-IDENTITY") {
		t.Errorf("GC-43 (r): the restart logged UNVERIFIED-INSTANCE-IDENTITY — the lineage anchor the target held "+
			"was in a purged file, so the lineage door could not check this source. Persisted: %s", afterRaw)
	}
	if t.Failed() {
		t.Logf("restart log:\n%s", restartLog)
	}
	if n := countRows(t, pgDSN, "rot"); n != 6 {
		t.Errorf("target holds %d rows, want 6 (3 seeds, cdc-1, the sentinel, after-restart)", n)
	}
	stopStreamer(cancel2, runErr2, "phase 4")
}

// latestBinlogCaughtUp is the file/pos and MariaDB-anchor half of caughtUp:
// the persisted token names the newest retained binlog.
func latestBinlogCaughtUp(t *testing.T, dsn, persistedFile string) (bool, string) {
	files := sourceBinlogFiles(t, dsn)
	newest := files[len(files)-1]
	return persistedFile == newest, "persisted " + persistedFile + ", newest " + newest
}

// TestStreamer_IdleBinlogRotation_MySQLFilePos is harm (1): a fully idle
// file/pos stream, two rotations, the persisted file purged.
func TestStreamer_IdleBinlogRotation_MySQLFilePos(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runIdleBinlogRotation(t, idleRotationCase{
		engine:    "mysql",
		sourceDSN: sourceDSN,
		caughtUp: func(t *testing.T, p idleRotationToken) (bool, string) {
			if p.Mode != "file_pos" {
				t.Fatalf("persisted mode %q, want file_pos (gtid_mode is OFF on this source)", p.Mode)
			}
			ok, why := latestBinlogCaughtUp(t, sourceDSN, p.File)
			return ok && p.Pos == 4, why
		},
	})
}

// TestStreamer_IdleBinlogRotation_MySQLGTIDStandaloneGroups is harm (2): the
// last GTIDs before the purge are standalone groups — out-of-scope DDL and an
// in-scope OPTIMIZE, which also leaves the reader's pending-DDL anchor set —
// so gtid_purged outruns a persisted set that never folded them in.
func TestStreamer_IdleBinlogRotation_MySQLGTIDStandaloneGroups(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	runIdleBinlogRotation(t, idleRotationCase{
		engine:    "mysql",
		sourceDSN: sourceDSN,
		idleStatements: []string{
			"CREATE DATABASE other_db",
			"CREATE TABLE other_db.x (id INT PRIMARY KEY)",
			"CREATE TABLE other_db.y (id INT PRIMARY KEY)",
			"OPTIMIZE TABLE rot",
		},
		caughtUp: func(t *testing.T, p idleRotationToken) (bool, string) {
			if p.Mode != "gtid" {
				t.Fatalf("persisted mode %q, want gtid", p.Mode)
			}
			executed := sourceScalar(t, sourceDSN, "SELECT @@GLOBAL.gtid_executed")
			sub := sourceScalar(t, sourceDSN, "SELECT GTID_SUBSET(@@GLOBAL.gtid_executed, ?)", p.GTIDSet)
			return sub == "1", "persisted " + p.GTIDSet + ", executed " + executed
		},
	})
}

// TestStreamer_IdleBinlogRotation_MariaDBStandaloneGroups is harm (3a): the
// MariaDB GTID shape of (2), refused by the server itself (error 1236).
func TestStreamer_IdleBinlogRotation_MariaDBStandaloneGroups(t *testing.T) {
	sourceDSN, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	runIdleBinlogRotation(t, idleRotationCase{
		engine:    "mariadb",
		sourceDSN: sourceDSN,
		idleStatements: []string{
			"CREATE DATABASE other_db",
			"CREATE TABLE other_db.x (id INT PRIMARY KEY)",
			"CREATE TABLE other_db.y (id INT PRIMARY KEY)",
		},
		caughtUp: func(t *testing.T, p idleRotationToken) (bool, string) {
			pos := sourceScalar(t, sourceDSN, "SELECT @@GLOBAL.gtid_binlog_pos")
			ok, why := latestBinlogCaughtUp(t, sourceDSN, p.LineageFile)
			return ok && p.GTIDSet == pos, why + "; persisted set " + p.GTIDSet + ", binlog pos " + pos
		},
		noUnverifiedIdentity: true,
	})
}

// TestStreamer_IdleBinlogRotation_MariaDBLineageAnchor is harm (3b): a fully
// idle MariaDB stream. The GTID resume point survives the purge (no GTIDs
// were written), but the lineage anchor never persisted past the purged
// file, so the restart could only proceed under UNVERIFIED-INSTANCE-IDENTITY.
//
// It also pins the connect-time anchor: before any rotation, the persisted
// anchor must still be the capture door's — not moved to offset 4 of the
// stream's first file by the dump connection's artificial rotate (on a
// server's first binlog that anchor is (file, 4, "") with the empty set
// omitted, which every fresh instance reproduces).
func TestStreamer_IdleBinlogRotation_MariaDBLineageAnchor(t *testing.T) {
	sourceDSN, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	runIdleBinlogRotation(t, idleRotationCase{
		engine:    "mariadb",
		sourceDSN: sourceDSN,
		checkPersisted: func(t *testing.T, p idleRotationToken) {
			if p.LineageFile == "" {
				t.Fatalf("persisted position carries no lineage anchor: %+v", p)
			}
			if p.LineageSet == "" || p.LineagePos == 4 {
				t.Errorf("before any rotation the persisted anchor is %s:%d %q — moved to the start of the file by "+
					"the connection's artificial rotate; want the capture door's mid-file anchor with the GTID "+
					"state at it", p.LineageFile, p.LineagePos, p.LineageSet)
			}
		},
		caughtUp: func(t *testing.T, p idleRotationToken) (bool, string) {
			ok, why := latestBinlogCaughtUp(t, sourceDSN, p.LineageFile)
			return ok && p.LineagePos == 4 && p.LineageSet != "", why
		},
		noUnverifiedIdentity: true,
	})
}
