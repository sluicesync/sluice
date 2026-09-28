//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ADR-0190 §1 premise, pinned: a GTID transaction re-delivered from a
// REPLICA — the failover shape — numbers its changes exactly as the primary's
// delivery did.
//
// The apply marks' skip rule trusts an identity (the transaction's GTID, and a
// per-table ROW ordinal) across re-deliveries. After a failover the re-delivery
// comes from a different server whose binlog holds the same transaction as
// ITS OWN rows events — and a replica is free to split them differently (the
// row-event size is a per-server setting). The rule survives that only if the
// replica emits the same rows in the same order. The replica here is booted
// with the smallest --binlog-row-event-max-size, so one multi-row statement
// the primary writes as a single rows event is re-encoded as several: the
// cell grades the identity through a genuinely different event layout.
//
// The independent expected value is the primary's own delivery; the
// replica's must equal it change for change.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
)

// startMySQLGTIDOnNetwork boots an upstream mysql:8.0 (not the pre-baked
// image: its baked data directory would give the primary and the replica the
// same server_uuid, which replication refuses) in gtid_mode=ON on nw.
func startMySQLGTIDOnNetwork(t *testing.T, ctx context.Context, nw *testcontainers.DockerNetwork, alias string, extra ...string) (dsn string, cleanup func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:          "mysql:8.0",
		Networks:       []string{nw.Name},
		NetworkAliases: map[string][]string{nw.Name: {alias}},
		ExposedPorts:   []string{"3306/tcp"},
		Env:            map[string]string{"MYSQL_ROOT_PASSWORD": "rootpw", "MYSQL_DATABASE": "source_db"},
		Cmd: append([]string{
			"--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL",
			"--gtid-mode=ON", "--enforce-gtid-consistency=ON",
		}, extra...),
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(5 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start %s: %v", alias, err)
	}
	cleanup = func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(shutdown)
	}
	host, err := c.Host(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("%s host: %v", alias, err)
	}
	port, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		cleanup()
		t.Fatalf("%s port: %v", alias, err)
	}
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/source_db?parseTime=true", host, port.Port()), cleanup
}

// writeRowsEvents counts the Write_rows events in the server's current
// binlog file.
func writeRowsEvents(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	file := queryString(t, dsn, "SELECT SUBSTRING_INDEX(@@global.log_bin_basename, '/', -1)")
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	var logs []string
	for rows.Next() {
		cols, _ := rows.Columns()
		vals := make([]any, len(cols))
		var name string
		vals[0] = &name
		for i := 1; i < len(cols); i++ {
			vals[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(vals...); err != nil {
			t.Fatalf("scan binary logs: %v", err)
		}
		logs = append(logs, name)
	}
	_ = rows.Close()
	n := 0
	for _, l := range logs {
		if !strings.HasPrefix(l, file) {
			continue
		}
		ev, err := db.Query("SHOW BINLOG EVENTS IN '" + l + "'")
		if err != nil {
			t.Fatalf("SHOW BINLOG EVENTS IN %s: %v", l, err)
		}
		for ev.Next() {
			cols, _ := ev.Columns()
			vals := make([]any, len(cols))
			for i := range vals {
				vals[i] = new(sql.RawBytes)
			}
			if err := ev.Scan(vals...); err != nil {
				t.Fatalf("scan binlog event: %v", err)
			}
			if string(*vals[2].(*sql.RawBytes)) == "Write_rows" {
				n++
			}
		}
		_ = ev.Close()
	}
	return n
}

func TestCDCReader_ApplyIdentity_GTID_StableFromReplica(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	nw, err := network.New(ctx)
	if err != nil {
		t.Skipf("create docker network (provider likely unavailable): %v", err)
	}
	defer func() {
		rm, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = nw.Remove(rm)
	}()

	primary, pCleanup := startMySQLGTIDOnNetwork(t, ctx, nw, "mysqlprimary", "--server-id=1")
	defer pCleanup()
	replica, rCleanup := startMySQLGTIDOnNetwork(t, ctx, nw, "mysqlreplica", "--server-id=2",
		"--log-replica-updates=ON", "--binlog-row-event-max-size=256")
	defer rCleanup()

	// Replicate only what happens from here on: both servers start from an
	// empty binlog and executed set (the image's own initialisation writes
	// are not this test's subject), then the replica follows by
	// auto-position.
	applyMySQL(t, primary, `RESET MASTER;`)
	applyMySQL(t, replica, `
		RESET MASTER;
		CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysqlprimary', SOURCE_PORT=3306, SOURCE_USER='root',
		  SOURCE_PASSWORD='rootpw', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1;
		START REPLICA;`)

	applyMySQL(t, primary, `
		CREATE TABLE ta (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) NOT NULL, pad VARCHAR(400) NOT NULL) ENGINE=InnoDB;
		CREATE TABLE tb (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) NOT NULL) ENGINE=InnoDB;`)

	eng := Engine{Flavor: FlavorVanilla}
	rdr1, err := eng.OpenCDCReader(ctx, primary)
	if err != nil {
		t.Fatalf("OpenCDCReader(primary): %v", err)
	}
	ch1, err := rdr1.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges(primary): %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	applyMySQL(t, primary, `INSERT INTO ta (id, v, pad) VALUES (100, 'w', 'w');`)
	warm := drainChangesWithBoundaries(t, ctx, ch1, 3, 60*time.Second)
	commitW, ok := warm[len(warm)-1].(ir.TxCommit)
	if len(warm) != 3 || !ok {
		t.Fatalf("warm-up transaction delivered %#v; want TxBegin, Insert, TxCommit", warm)
	}
	// One five-row statement: a single rows event on the primary, several on
	// the replica (each ~300-byte row exceeds the replica's 256-byte cap).
	pad := strings.Repeat("p", 300)
	applyMySQL(t, primary, fmt.Sprintf(`
		START TRANSACTION;
		INSERT INTO ta (id, v, pad) VALUES (1, 'a', '%[1]s'), (2, 'b', '%[1]s'), (3, 'c', '%[1]s'), (4, 'd', '%[1]s'), (5, 'e', '%[1]s');
		INSERT INTO tb (id, v) VALUES (10, 'x');
		UPDATE ta SET v = 'a2' WHERE id IN (1, 2);
		DELETE FROM tb WHERE id = 10;
		COMMIT;`, pad))
	fromPrimary := identitySteps(drainChangesWithBoundaries(t, ctx, ch1, 11, 60*time.Second))
	if c, ok := rdr1.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	if len(fromPrimary) != 9 {
		t.Fatalf("the primary delivered %d row changes; want 9: %+v", len(fromPrimary), fromPrimary)
	}

	// Wait for the replica to hold the transaction, then re-deliver it from
	// the replica, resumed at the warm-up's commit.
	want := gtidExecuted(t, primary)
	deadline := time.Now().Add(2 * time.Minute)
	for !serverGTIDSubset(t, replica, want, gtidExecuted(t, replica)) {
		if time.Now().After(deadline) {
			t.Fatalf("the replica never caught up to %q (at %q)", want, gtidExecuted(t, replica))
		}
		time.Sleep(time.Second)
	}
	rdr2, err := eng.OpenCDCReader(ctx, replica)
	if err != nil {
		t.Fatalf("OpenCDCReader(replica): %v", err)
	}
	ch2, err := rdr2.StreamChanges(ctx, commitW.Position)
	if err != nil {
		t.Fatalf("StreamChanges(replica, warm-up commit): %v", err)
	}
	fromReplica := identitySteps(drainChangesWithBoundaries(t, ctx, ch2, 11, 60*time.Second))
	if c, ok := rdr2.(interface{ Close() error }); ok {
		_ = c.Close()
	}

	// Anti-vacuity: the replica must really have re-encoded the rows into
	// more events than the primary wrote, or this cell graded two identical
	// event layouts.
	if p, r := writeRowsEvents(t, primary), writeRowsEvents(t, replica); r <= p {
		t.Fatalf("the replica's binlog holds %d Write_rows events and the primary's %d: the replica did not split the "+
			"transaction's rows differently, so the cell proves nothing about re-encoding", r, p)
	}
	if fmt.Sprint(fromReplica) != fmt.Sprint(fromPrimary) {
		t.Errorf("the replica numbered the transaction differently from the primary — after a failover an apply "+
			"mark would skip the wrong change (ADR-0190 §1 premise):\n  primary %+v\n  replica %+v", fromPrimary, fromReplica)
	}
	for i, s := range fromPrimary {
		if s.id.IsZero() {
			t.Errorf("change %d carries no identity", i)
		}
	}
}
