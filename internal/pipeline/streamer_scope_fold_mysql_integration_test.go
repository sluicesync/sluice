//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 RC-1b, end to end: a sync whose SOURCE DSN spells the
// database in a case the folding server never stores must still deliver
// post-copy writes. The reader-level pin (engines/mysql
// TestCDCReader_ScopeFollowsTheServersFold) proves the stream emits the
// insert; this one proves the TARGET receives it, which is the only
// number an operator sees — and the one that was 2 while the source held
// 4 in the RC-1 shape. If the pipeline had a second byte-exact compare
// on the change's schema downstream of the reader, this cell would fail
// where the reader cell passes.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// startMySQLBinlogFolding boots a binlog-enabled MySQL INITIALISED at
// lower_case_table_names=1 — a sibling of startMySQLBinlogCaseSensitive
// (which pins 0), a separate helper because the flag is the claim. The
// upstream image pays a cold init: the pre-baked one is initialised at 0
// and MySQL 8 refuses to boot it under 1.
func startMySQLBinlogFolding(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()

	container := runMySQLImageWithRetry(
		t,
		"mysql:8.0",
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld",
					"--lower-case-table-names=1",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=ROW",
					"--binlog-row-image=FULL",
					"--net-write-timeout=600",
					"--net-read-timeout=600",
				},
			},
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var lct int
	if err := db.QueryRowContext(ctx, "SELECT @@global.lower_case_table_names").Scan(&lct); err != nil || lct != 1 {
		terminate()
		t.Fatalf("server is not folding (lower_case_table_names=%d, err=%v); the cell below would measure the wrong regime", lct, err)
	}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := buildMySQLDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return srcConn, tgtConn, terminate
}

func TestStreamer_MySQLFoldingSource_UppercaseDSNDeliversPostCopyWrites(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlogFolding(t)
	defer cleanup()

	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('seed@example.com');
	`)

	// The operator's spelling: the server resolves it for every query,
	// so nothing before CDC can tell it apart from the stored name.
	upperDSN := strings.Replace(srcDSN, "/source_db?", "/SOURCE_DB?", 1)
	if upperDSN == srcDSN {
		t.Fatalf("could not respell the DSN database: %q", srcDSN)
	}

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: upperDSN,
		TargetDSN: tgtDSN,
		StreamID:  "test-scope-fold-upper-dsn",
		Filter:    filter,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, tgtDSN, "users", 1, 60*time.Second) {
		t.Fatalf("cold copy did not deliver the seed row through the uppercase DSN")
	}
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('after-copy@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 2, 60*time.Second) {
		t.Fatalf("the post-copy insert never reached the target: the copy worked through the operator's spelling and CDC silently dropped every write (RC-1b)")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(30 * time.Second):
		t.Fatal("streamer did not stop after cancel")
	}
}
