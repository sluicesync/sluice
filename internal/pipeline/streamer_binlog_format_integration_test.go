//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end pins for the binlog_format=ROW gate (roadmap item 68e) on
// real servers, covering the format family × flavor matrix the
// preflight dispatches over:
//
//   - mysql:8.0 STATEMENT — the Phase-A ground-truth shape (2026-07-23):
//     pre-fix, the cold copy landed and then every live DML was
//     SILENTLY LOST (target frozen, position frozen, stream green,
//     exit 0). Now: coded refusal BEFORE the bulk copy.
//   - mysql:8.0 MIXED — the family sibling (deterministic writes are
//     statement-logged, so most DML is still lost).
//   - mariadb STATEMENT — the flavor sibling; the variable + refusal
//     behave identically on the MariaDB binlog path (whose platform
//     DEFAULT is the also-refused MIXED).
//
// Each cell asserts the refusal fires UPFRONT: Run returns the coded
// error and the target never receives the table (no partial cold copy
// ahead of a dead CDC tail).

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// startMySQLBinlogWithFormat mirrors startMySQLBinlog with the
// binlog-format flag parameterised (the startMySQLBinlogWithRowImage
// shape, one flag over).
func startMySQLBinlogWithFormat(t *testing.T, format string) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=" + format,
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

// assertBinlogFormatRefusal runs a same-engine sync against the given
// DSN pair and asserts the coded 68e refusal fired before any target
// DDL/data.
func assertBinlogFormatRefusal(t *testing.T, engineName, src, tgt, streamID string) {
	t.Helper()
	eng, ok := engines.Get(engineName)
	if !ok {
		t.Fatalf("%s engine not registered", engineName)
	}
	streamer := &Streamer{
		Source:    eng,
		Target:    eng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  streamID,
	}
	err := streamer.Run(context.Background())
	if err == nil {
		t.Fatal("expected the binlog-format refusal; Run returned nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCBinlogFormatNotRow {
		t.Fatalf("expected coded %s; got %v (err=%v)", sluicecode.CodeCDCBinlogFormatNotRow, ce, err)
	}
	// Upfront means UPFRONT: the refusal precedes the bulk copy, so the
	// target must not have the table at all.
	db, dbErr := sql.Open("mysql", tgt)
	if dbErr != nil {
		t.Fatalf("open target: %v", dbErr)
	}
	defer func() { _ = db.Close() }()
	var n int
	if scanErr := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'fmt_probe'`,
	).Scan(&n); scanErr != nil {
		t.Fatalf("probe target catalog: %v", scanErr)
	}
	if n != 0 {
		t.Fatalf("target has the fmt_probe table after the refusal — the gate fired AFTER target DDL, not before")
	}
}

func TestStreamer_BinlogFormatGate_MySQLStatement(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlogWithFormat(t, "STATEMENT")
	defer cleanup()
	applyMySQLDDL(t, src, `CREATE TABLE fmt_probe (id BIGINT PRIMARY KEY, v INT);
		INSERT INTO fmt_probe (id, v) VALUES (1, 1);`)
	assertBinlogFormatRefusal(t, "mysql", src, tgt, "fmt-gate-statement")
}

func TestStreamer_BinlogFormatGate_MySQLMixed(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlogWithFormat(t, "MIXED")
	defer cleanup()
	applyMySQLDDL(t, src, `CREATE TABLE fmt_probe (id BIGINT PRIMARY KEY, v INT);
		INSERT INTO fmt_probe (id, v) VALUES (1, 1);`)
	assertBinlogFormatRefusal(t, "mysql", src, tgt, "fmt-gate-mixed")
}

// startMariaDBBinlogStatement boots a mariadb source with
// binlog_format=STATEMENT (the startMariaDBBinlog shape, format flag
// flipped) plus a target_db on the same server for a same-flavor sync.
func startMariaDBBinlogStatement(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	req := testcontainers.ContainerRequest{
		Image: "mariadb:11.4",
		Env: map[string]string{
			"MARIADB_ROOT_PASSWORD": "rootpw",
			"MARIADB_DATABASE":      "source_db",
		},
		Cmd: []string{
			"--server-id=1",
			"--log-bin=mysqld-bin",
			"--binlog-format=STATEMENT",
		},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
			return fmt.Sprintf("root:rootpw@tcp(%s:%s)/source_db", host, port.Port())
		}).WithStartupTimeout(4 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("boot mariadb:11.4: %v", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	cleanup = func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	srcDSN := fmt.Sprintf("root:rootpw@tcp(%s:%s)/source_db?parseTime=true", host, port.Port())
	db, err := sql.Open("mysql", srcDSN)
	if err != nil {
		cleanup()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		cleanup()
		t.Fatalf("create target_db: %v", err)
	}
	tgtDSN := fmt.Sprintf("root:rootpw@tcp(%s:%s)/target_db?parseTime=true", host, port.Port())
	return srcDSN, tgtDSN, cleanup
}

// TestStreamer_BinlogFormatGate_MariaDBStatement pins the flavor
// sibling: the MariaDB binlog path shares the preflight chokepoints and
// the variable, so a STATEMENT-format MariaDB source refuses with the
// same coded error. (MariaDB's un-tuned DEFAULT is MIXED — also
// refused — which is why the flavor pin earns its container boot.)
func TestStreamer_BinlogFormatGate_MariaDBStatement(t *testing.T) {
	src, tgt, cleanup := startMariaDBBinlogStatement(t)
	defer cleanup()
	applyMariaDBSQL(t, src, `CREATE TABLE fmt_probe (id BIGINT PRIMARY KEY, v INT);
		INSERT INTO fmt_probe (id, v) VALUES (1, 1);`)
	assertBinlogFormatRefusal(t, "mariadb", src, tgt, "fmt-gate-mariadb")
}
