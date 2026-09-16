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
// runtime): the default-OFF one walks no-connection → named connection
// RUNNING (refuse) → named connection STOPPED (accept, with the INFO;
// operator decision 2026-09-15, audit 2026-09-15 A0915-CLI-MEDIUM-1) →
// reset (release); the --log-slave-updates=ON one proves a named
// connection with log updates ON stays a legitimate chained source. The
// thread state each cell claims is read back from SHOW ALL REPLICAS
// STATUS before the door is graded. The blind-replica server mechanism
// itself is the capture-completeness matrix's recorded ground truth.

package mysql

import (
	"context"
	"database/sql"
	"strings"
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
// A1 pin on a real MariaDB with the default log_slave_updates=0, both
// thread-state arms of the stopped-channel acceptance.
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
	configure := func(t *testing.T) {
		t.Helper()
		// Self-healing precondition: if a previous subtest's teardown did not
		// run to completion, conn1 still exists and this cell would assert on
		// inherited state. Clear it first — that cascade is how one slow
		// teardown became TWO failures on CI (run 35035194041, 2026-09-15:
		// the deferred STOP blew its deadline here, and the next subtest then
		// failed its own "want 1 connection" precondition).
		if countStatusRows(t, dsn, "SHOW ALL REPLICAS STATUS") > 0 {
			applyMySQL(t, dsn, "STOP REPLICA 'conn1'; RESET REPLICA 'conn1' ALL")
		}
		// MASTER_HOST is an address that REFUSES a connection immediately, not
		// a black-holed one, and that difference is load-bearing for teardown.
		// MEASURED on mariadb:11.4 (2026-09-15): pointed at 192.0.2.10
		// (TEST-NET-1, packets dropped) the IO thread sits in "Connecting" and
		// `STOP REPLICA 'conn1'` blocks until the connect attempt gives up —
		// 18s locally, and past applyMySQL's 30s deadline on the CI runner,
		// which failed the deferred teardown; MASTER_CONNECT_RETRY=1 with
		// slave_net_timeout=10 only narrowed it to 7s. Pointed at 127.0.0.1:1
		// the connect is refused at once, so the IO thread is between retries
		// and STOP returns in 0–1s (three repeats), while the states both arms
		// assert are unchanged: IO "Connecting" with SQL "Yes" while started,
		// No/No after STOP. The MySQL sibling is unaffected either way — it
		// stops in ~2s against the same black-holed address — so this is a
		// MariaDB-specific teardown property, not a shared one.
		applyMySQL(t, dsn, `CHANGE MASTER 'conn1' TO MASTER_HOST='127.0.0.1', MASTER_PORT=1,
			MASTER_USER='repl', MASTER_PASSWORD='replpw'`)
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
	}

	t.Run("no_connection_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges on a non-replica MariaDB = %v; want nil", err)
		}
	})

	t.Run("named_connection_running_refuses", func(t *testing.T) {
		configure(t)
		applyMySQL(t, dsn, "START REPLICA 'conn1'")
		defer applyMySQL(t, dsn, "RESET REPLICA 'conn1' ALL")
		defer applyMySQL(t, dsn, "STOP REPLICA 'conn1'")
		if io, sqlThread := replicaThreadState(t, dsn, "SHOW ALL REPLICAS STATUS", "conn1"); sqlThread != "Yes" || io == "No" {
			t.Fatalf("Slave_IO_Running=%q Slave_SQL_Running=%q after START REPLICA 'conn1'; want a running SQL "+
				"thread and a not-stopped IO thread", io, sqlThread)
		}

		err := openStream(t)
		wantCodedRefusal(t, err, sluicecode.CodeCDCReplicaNoLogUpdates, "StreamChanges")
		if !strings.Contains(err.Error(), "conn1 [") {
			t.Errorf("the refusal does not name the running named connection: %v", err)
		}

		if snap, err := eng.OpenSnapshotStream(ctx, dsn); err == nil {
			_ = snap.Close()
			t.Fatal("OpenSnapshotStream: accepted a named-connection blind replica; want the coded refusal before any copy")
		} else {
			wantCodedRefusal(t, err, sluicecode.CodeCDCReplicaNoLogUpdates, "OpenSnapshotStream")
		}
	})

	t.Run("named_connection_stopped_is_accepted", func(t *testing.T) {
		configure(t)
		applyMySQL(t, dsn, "START REPLICA 'conn1'")
		applyMySQL(t, dsn, "STOP REPLICA 'conn1'")
		defer applyMySQL(t, dsn, "RESET REPLICA 'conn1' ALL")
		if io, sqlThread := replicaThreadState(t, dsn, "SHOW ALL REPLICAS STATUS", "conn1"); io != "No" || sqlThread != "No" {
			t.Fatalf("Slave_IO_Running=%q Slave_SQL_Running=%q after STOP REPLICA 'conn1'; want No/No", io, sqlThread)
		}

		var err error
		logged := captureInfoLog(func() { err = openStream(t) })
		if err != nil {
			t.Fatalf("StreamChanges with a stopped named connection = %v; want the reader to open", err)
		}
		for _, want := range []string{replicaChannelsStoppedMarker, "conn1 [IO=No SQL=No]", "RESET REPLICA 'connection_name' ALL"} {
			if !strings.Contains(logged, want) {
				t.Errorf("the acceptance INFO is missing %q:\n%s", want, logged)
			}
		}
		snap, err := eng.OpenSnapshotStream(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenSnapshotStream with a stopped named connection = %v; want it to open", err)
		}
		_ = snap.Close()
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

	// Same refusing address as the sibling above, for one convention in this
	// file: this cell never STARTs the connection, so its teardown never
	// blocked — but a future cell that does start one would hit the 18s stop
	// described there.
	applyMySQL(t, dsn, `CHANGE MASTER 'conn1' TO MASTER_HOST='127.0.0.1', MASTER_PORT=1,
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
