//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-08-27 A1 — the G5 door against a real MariaDB with a
// NAMED multi-source connection. `CHANGE MASTER 'name' TO …` creates a
// connection the bare SHOW REPLICA STATUS does NOT list (zero rows —
// asserted below as the anti-vacuity floor, since a door test that
// rides the bare probe would prove nothing about the A1 shape); only
// the MariaDB-only SHOW ALL REPLICAS/SLAVES STATUS spellings see it.
// MariaDB defaults log_slave_updates=0, so before the ALL-spelling
// probe existed this was exactly the G5 silent-loss conjunction
// passing the preflight.
//
// Two dedicated containers (log_slave_updates is read-only at
// runtime): the default-OFF one walks no-connection → named-connection
// (refuse) → reset (release); the --log-slave-updates=ON one proves a
// named connection with log updates ON stays a legitimate chained
// source. Like TestCDCReader_ReplicaSourcePreflight, this pin holds
// the DOOR on a configured connection (threads never started — a
// channel's presence is the operator's stated intent); the blind-
// replica server mechanism itself is the capture-completeness matrix's
// recorded ground truth.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// countStatusRows returns the number of rows stmt yields on dsn.
func countStatusRows(t *testing.T, dsn, stmt string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s rows: %v", stmt, err)
	}
	return n
}

// TestMariaDB_CDCReader_ReplicaSourcePreflight_NamedConnection is the
// A1 refusal pin on a real mariadb:11.4 with the default
// log_slave_updates=0.
func TestMariaDB_CDCReader_ReplicaSourcePreflight_NamedConnection(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()

	applyMySQL(t, dsn, "CREATE TABLE orders (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	applyMySQL(t, dsn, "INSERT INTO orders (id) VALUES (1)")

	eng := Engine{Flavor: FlavorMariaDB}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	openStream := func(t *testing.T) error {
		t.Helper()
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		t.Cleanup(func() { _ = rdr.(*CDCReader).Close() })
		_, err = rdr.(*CDCReader).StreamChanges(ctx, ir.Position{})
		return err
	}

	t.Run("no_connection_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges on a non-replica MariaDB = %v; want nil", err)
		}
	})

	t.Run("named_connection_refuses", func(t *testing.T) {
		applyMySQL(t, dsn, `CHANGE MASTER 'conn1' TO MASTER_HOST='192.0.2.10', MASTER_PORT=3306,
			MASTER_USER='repl', MASTER_PASSWORD='replpw'`)
		defer applyMySQL(t, dsn, "RESET REPLICA 'conn1' ALL")

		// Anti-vacuity floor: this scenario must BE the bare-blind shape —
		// the bare spelling empty, the ALL spelling listing the named
		// connection. If MariaDB ever starts listing named connections in
		// the bare form, this pin stops testing what it claims to.
		if n := countStatusRows(t, dsn, "SHOW REPLICA STATUS"); n != 0 {
			t.Fatalf("bare SHOW REPLICA STATUS = %d rows for a named connection; the A1 bare-blind premise no longer holds on this MariaDB", n)
		}
		if n := countStatusRows(t, dsn, "SHOW ALL REPLICAS STATUS"); n != 1 {
			t.Fatalf("SHOW ALL REPLICAS STATUS = %d rows; want 1 (the named connection)", n)
		}

		wantCodedRefusal(t, openStream(t), sluicecode.CodeCDCReplicaNoLogUpdates, "StreamChanges")

		if snap, err := eng.OpenSnapshotStream(ctx, dsn); err == nil {
			_ = snap.Close()
			t.Fatal("OpenSnapshotStream: accepted a named-connection blind replica; want the coded refusal before any copy")
		} else {
			wantCodedRefusal(t, err, sluicecode.CodeCDCReplicaNoLogUpdates, "OpenSnapshotStream")
		}
	})

	t.Run("after_reset_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges after RESET REPLICA 'conn1' ALL = %v; want nil", err)
		}
	})
}

// TestMariaDB_CDCReader_ReplicaSourcePreflight_NamedConnectionLogUpdatesOn
// is the pass direction: a named connection on a server with
// log_slave_updates=ON is a legitimate chained-replication source and
// must keep streaming.
func TestMariaDB_CDCReader_ReplicaSourcePreflight_NamedConnectionLogUpdatesOn(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, "--log-slave-updates=ON")
	defer cleanup()

	applyMySQL(t, dsn, `CHANGE MASTER 'conn1' TO MASTER_HOST='192.0.2.10', MASTER_PORT=3306,
		MASTER_USER='repl', MASTER_PASSWORD='replpw'`)
	defer applyMySQL(t, dsn, "RESET REPLICA 'conn1' ALL")

	eng := Engine{Flavor: FlavorMariaDB}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() { _ = rdr.(*CDCReader).Close() }()
	if _, err := rdr.(*CDCReader).StreamChanges(ctx, ir.Position{}); err != nil {
		t.Fatalf("StreamChanges on a named-connection replica WITH log_slave_updates=ON = %v; want nil (legitimate chained source)", err)
	}
}
