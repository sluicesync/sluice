//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (i) end to end: `sync` from MySQL of an ENUM/SET column whose
// label holds a character outside the Basic Multilingual Plane. Every
// catalog surface writes that character as '?', and the binlog carries only
// the label's index, so before the fix a row inserted after cutover landed
// on the target as the catalog's '?b' at exit 0 (MEASURED: MySQL → PG `?b`
// / `{?,y}`; MySQL → MySQL 3F62 / 3F2C79). The run must now END with
// ENUM-LABEL-NOT-RECOVERABLE and land nothing for that row, while a
// BMP-only control table — non-ASCII 'é' included — copies and streams
// exactly, and the lossy table's rows that use only labels the catalog kept
// copy and stream too.
//
// The independent expected value is the label text the test inserted,
// compared as bytes on the TARGET (a hex of the stored value), never a
// catalog read.

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

const enumLabelLossSourceDDL = `
	CREATE TABLE ctl (
		id INT NOT NULL PRIMARY KEY,
		e  ENUM('x','é') CHARACTER SET utf8mb4 NULL,
		s  SET('y','é')  CHARACTER SET utf8mb4 NULL
	) ENGINE=InnoDB;
	CREATE TABLE lossy (
		id INT NOT NULL PRIMARY KEY,
		e  ENUM('😀b','x','é') CHARACTER SET utf8mb4 NULL,
		s  SET('😀','y','é')   CHARACTER SET utf8mb4 NULL
	) ENGINE=InnoDB;
	INSERT INTO ctl VALUES (1, 'é', 'y,é');
	INSERT INTO lossy VALUES (1, 'x', 'y');`

// enumLabelLossTarget reads one row's e and s back as hex, per dialect.
type enumLabelLossTarget struct {
	driver string
	dsn    string
	query  string // selects hex(e), hex(s) for a table name and id
}

func (tg enumLabelLossTarget) rowHex(t *testing.T, table string, id int) (e, s string, found bool) {
	t.Helper()
	db, err := sql.Open(tg.driver, tg.dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = db.QueryRowContext(ctx, strings.ReplaceAll(tg.query, "{t}", table), id).Scan(&e, &s)
	if err == sql.ErrNoRows {
		return "", "", false
	}
	if err != nil {
		t.Fatalf("read %s id=%d: %v", table, id, err)
	}
	return strings.ToUpper(e), strings.ToUpper(s), true
}

func enumLabelLossLane(t *testing.T, sourceDSN, targetEngine string, tg enumLabelLossTarget, waitRows func(table string, n int) bool) {
	t.Helper()
	applyDDLMySQL(t, sourceDSN, enumLabelLossSourceDDL)

	src, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	dst, ok := engines.Get(targetEngine)
	if !ok {
		t.Fatalf("%s engine not registered", targetEngine)
	}
	streamer := &Streamer{
		Source: src, Target: dst,
		SourceDSN: sourceDSN, TargetDSN: tg.dsn,
		StreamID: "test-enum-label-loss-" + targetEngine,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	if !waitRows("ctl", 1) || !waitRows("lossy", 1) {
		t.Fatalf("cold start never landed the seed rows")
	}
	// Cold start: the control row and the lossy table's kept-label row.
	if e, s, _ := tg.rowHex(t, "ctl", 1); e != "C3A9" || s != "792CC3A9" {
		t.Errorf("ctl id=1 copied as e=%s s=%s; want C3A9 / 792CC3A9 ('é' / 'y,é')", e, s)
	}
	if e, s, _ := tg.rowHex(t, "lossy", 1); e != "78" || s != "79" {
		t.Errorf("lossy id=1 copied as e=%s s=%s; want 78 / 79 ('x' / 'y')", e, s)
	}

	// CDC: the control streams exactly.
	applyDDLMySQL(t, sourceDSN, `INSERT INTO ctl VALUES (2, 'x', 'é')`)
	if !waitRows("ctl", 2) {
		t.Fatalf("the control row never streamed")
	}
	if e, s, _ := tg.rowHex(t, "ctl", 2); e != "78" || s != "C3A9" {
		t.Errorf("ctl id=2 streamed as e=%s s=%s; want 78 / C3A9 ('x' / 'é')", e, s)
	}

	// CDC: the lost label ends the run, loudly, and lands nothing.
	applyDDLMySQL(t, sourceDSN, `INSERT INTO lossy VALUES (2, '😀b', '😀,y')`)
	select {
	case err := <-runErr:
		if err == nil || !strings.Contains(err.Error(), "ENUM-LABEL-NOT-RECOVERABLE") {
			t.Fatalf("sync ended with %v; want ENUM-LABEL-NOT-RECOVERABLE", err)
		}
	case <-time.After(90 * time.Second):
		e, s, found := tg.rowHex(t, "lossy", 2)
		t.Fatalf("sync did not stop on the lost label (target lossy id=2 found=%v e=%s s=%s — before the fix it "+
			"landed as 3F62 / 3F2C79, the catalog's '?b' / '?,y')", found, e, s)
	}
	if e, s, found := tg.rowHex(t, "lossy", 2); found {
		t.Errorf("target holds lossy id=2 as e=%s s=%s; want no row (the source holds F09F988062 / F09F98802C79)", e, s)
	}
}

func TestSync_EnumLabelLoss_MySQLToPostgres(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	tg := enumLabelLossTarget{
		driver: "pgx", dsn: pgDSN,
		query: `SELECT encode(convert_to(e::text, 'UTF8'), 'hex'),
			encode(convert_to(array_to_string(s, ','), 'UTF8'), 'hex') FROM {t} WHERE id = $1`,
	}
	enumLabelLossLane(t, mysqlDSN, "postgres", tg, func(table string, n int) bool {
		return waitForPGRowCount(t, pgDSN, table, n, 60*time.Second)
	})
}

func TestSync_EnumLabelLoss_MySQLToMySQL(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	tg := enumLabelLossTarget{
		driver: "mysql", dsn: tgtDSN,
		query: `SELECT HEX(e), HEX(s) FROM {t} WHERE id = ?`,
	}
	enumLabelLossLane(t, srcDSN, "mysql", tg, func(table string, n int) bool {
		return waitForRowCountMySQL(t, tgtDSN, table, n, 60*time.Second)
	})
}
