//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 A0909-MYSQL-HIGH-1, end to end on one real MySQL: a
// sync whose source becomes a DIFFERENT lineage at the same DSN must
// REFUSE, and the target must keep every row it had — while the same
// source after a plain RESET MASTER (empty executed set) must still take
// the automatic re-snapshot, because there the re-copy is the right
// recovery.
//
// The worker reproduced the loss by replacing the container behind one
// host:port. testcontainers cannot re-bind a port to a new instance, so
// the foreign lineage is produced the way the server itself produces it
// on a restore-from-elsewhere: RESET MASTER, then SET GLOBAL gtid_purged
// to a set under a UUID this server never had. @@gtid_executed is then
// non-empty and shares no UUID with the persisted position — byte for
// byte the state the door grades — and the source's rows are gone, which
// is what makes an automatic re-copy destructive: the target would be
// refilled with NOTHING.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestStreamer_MySQLForeignLineage_RefusesTheAutomaticRecopy(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLGTID(t)
	defer cleanup()

	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('a@example.com'), ('b@example.com'), ('c@example.com');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	newStream := func() *Streamer {
		return &Streamer{
			Source:    mysqlEng,
			Target:    mysqlEng,
			SourceDSN: srcDSN,
			TargetDSN: tgtDSN,
			StreamID:  "test-foreign-lineage",
			Filter:    filter,
		}
	}

	// Cold start, one live change, clean stop — a persisted GTID position
	// under this server's UUID and four rows on the target.
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- newStream().Run(ctx) }()
	if !waitForRowCountMySQL(t, tgtDSN, "users", 3, 60*time.Second) {
		t.Fatal("cold copy did not deliver the seed rows")
	}
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('d@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 4, 60*time.Second) {
		t.Fatal("CDC did not deliver the live insert")
	}
	cancel()
	select {
	case <-runErr:
	case <-time.After(30 * time.Second):
		t.Fatal("streamer did not stop after cancel")
	}

	// Become a different lineage at the same DSN: non-empty executed set,
	// no UUID in common, and the rows gone. The replacement's own table
	// is written with sql_log_bin=0 so it adds no GTID under THIS server's
	// UUID — a server keeps its server_uuid across RESET MASTER, and a
	// logged write here would make the executed set SHARE a UUID with the
	// position, which is the "behind" verdict (correctly still
	// auto-recopied), not the foreign one this cell grades. The first cut
	// of this test made exactly that mistake and measured the wrong arm.
	const foreignUUID = "ffffffff-1111-2222-3333-444444444444"
	applyDDLMySQL(t, srcDSN, "DROP TABLE users; RESET MASTER; SET GLOBAL gtid_purged = '"+foreignUUID+":1-5'; "+
		"SET SESSION sql_log_bin = 0; "+
		"CREATE TABLE users (id BIGINT NOT NULL, email VARCHAR(255) NOT NULL, PRIMARY KEY (id)); "+
		"INSERT INTO users VALUES (77, 'REPLACED-HOST'); "+
		"SET SESSION sql_log_bin = 1;")
	if got := strings.TrimSpace(mysqlGlobal(t, srcDSN, "gtid_executed")); got != foreignUUID+":1-5" {
		t.Fatalf("premise gone: gtid_executed = %q; want exactly the foreign set (a GTID under this server's own "+
			"UUID would turn the verdict into 'behind')", got)
	}

	t.Run("foreign lineage: refuses, target untouched", func(t *testing.T) {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer rcancel()
		err := newStream().Run(rctx)
		if err == nil {
			t.Fatal("sync start against a different lineage returned nil")
		}
		if !errors.Is(err, ir.ErrPositionForeignLineage) || !strings.Contains(err.Error(), foreignLineageMarker) {
			t.Fatalf("want the %s refusal carrying ir.ErrPositionForeignLineage, got: %v", foreignLineageMarker, err)
		}
		if errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("the refusal also reads as an invalid position, which is the auto-recopy route: %v", err)
		}
		// The independent expected value: the target still holds the
		// four rows the OLD source produced, not the replacement's one.
		if got := pollRowCountMySQL(tgtDSN, "users"); got != 4 {
			t.Fatalf("target holds %d rows after the refusal; want the original 4 — the recovery this door "+
				"exists to stop replaces them with the replacement's single row (A0909-MYSQL-HIGH-1)", got)
		}
	})

	t.Run("same server after RESET MASTER: the automatic re-snapshot still runs", func(t *testing.T) {
		// Empty executed set: no other lineage here, the re-copy is right.
		applyDDLMySQL(t, srcDSN, "RESET MASTER;")
		if got := mysqlGlobal(t, srcDSN, "gtid_executed"); strings.TrimSpace(got) != "" {
			t.Fatalf("premise gone: gtid_executed = %q after RESET MASTER; want empty", got)
		}
		rctx, rcancel := context.WithCancel(context.Background())
		defer rcancel()
		errCh := make(chan error, 1)
		go func() { errCh <- newStream().Run(rctx) }()
		// The re-copy lands the replacement's one row (that is what the
		// source now holds); a refusal here would strand a legitimate
		// reset behind --restart-from-scratch for nothing.
		if !waitForExactRowCountMySQLRows(t, tgtDSN, "users", 1, 2*time.Minute) {
			select {
			case err := <-errCh:
				t.Fatalf("re-snapshot after a plain RESET MASTER did not run: %v", err)
			default:
				t.Fatalf("re-snapshot after RESET MASTER never delivered; target holds %d rows", pollRowCountMySQL(tgtDSN, "users"))
			}
		}
		rcancel()
		select {
		case <-errCh:
		case <-time.After(30 * time.Second):
			t.Fatal("streamer did not stop after cancel")
		}
	})
}

func mysqlGlobal(t *testing.T, dsn, name string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v string
	if err := db.QueryRow("SELECT @@global." + name).Scan(&v); err != nil {
		t.Fatalf("read @@global.%s: %v", name, err)
	}
	return v
}

func waitForExactRowCountMySQLRows(t *testing.T, dsn, table string, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCountMySQL(dsn, table) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
