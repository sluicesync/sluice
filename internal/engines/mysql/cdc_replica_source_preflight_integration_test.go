//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 G5 — the replica-source preflight against a real mysqld, both
// directions on one container. The container boots with
// --log-replica-updates=OFF (mysql:8.0's default is ON, so the flag is
// the whole scenario), then walks the conjunction:
//
//	not a replica + OFF        → PASS   (the variable alone must not refuse)
//	channel configured + OFF   → REFUSE (both CDC-open chokepoint families)
//	channel STOPPED + OFF      → REFUSE (the promoted-primary shape — a stated
//	                                     decision, and the refusal must name
//	                                     RESET REPLICA ALL; audit 2026-09-15 A0915-CLI-MEDIUM-1)
//	RESET REPLICA ALL + OFF    → PASS   (the door releases when the replica
//	                                     config does)
//
// The replica-with-log-updates-ON pass direction is unit-pinned
// (TestPreflightReplicaSource) — it needs no second container because
// the variable read is the same code path either way. The full
// two-container blind-replica repro (replicated writes absent from the
// binlog) is the m2 sweep's recorded ground truth; this pin holds the
// DOOR, not the server mechanism.

package mysql

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// startMySQLM2Preflight boots a dedicated MySQL container with the
// standard binlog posture (ROW + FULL) plus the caller's extra mysqld
// args — the M2 preflight scenarios (replica config, binlog filters)
// are startup-option-shaped, which is why the shared TestMain container
// can't host them. Boot retry schedule mirrors startMySQLRowImageForCDC.
func startMySQLM2Preflight(t *testing.T, extraArgs ...string) (dsn string, cleanup func()) {
	t.Helper()
	return startMySQLM2PreflightImage(t, sharedMySQLImage, extraArgs...)
}

// startMySQLM2PreflightImage is startMySQLM2Preflight on a caller-chosen
// image. The pre-baked image ships a data directory already initialised
// at lower_case_table_names=0, and MySQL 8 refuses to boot it under any
// other value (MY-011087) — so a cell that needs a FOLDING server must
// pay the upstream image's cold init. Say which in the caller's name.
func startMySQLM2PreflightImage(t *testing.T, image string, extraArgs ...string) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	cmd := append([]string{
		"mysqld",
		"--server-id=1",
		"--log-bin=mysql-bin",
		"--binlog-format=ROW",
		"--binlog-row-image=FULL",
	}, extraArgs...)

	var (
		container *mysqltc.MySQLContainer
		lastErr   error
	)
	for attempt := 1; attempt <= sharedMySQLBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
		c, err := mysqltc.Run(
			ctx,
			image,
			mysqltc.WithDatabase("source_db"),
			mysqltc.WithUsername("root"),
			mysqltc.WithPassword("rootpw"),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{Cmd: cmd},
			}),
			testcontainers.WithWaitStrategyAndDeadline(
				sharedMySQLBootTimeout,
				wait.ForLog("port: 3306  MySQL Community Server").
					WithStartupTimeout(sharedMySQLBootTimeout),
			),
		)
		cancel()
		if err == nil {
			container = c
			if attempt > 1 {
				log.Printf("startMySQLM2Preflight boot attempt %d/%d succeeded", attempt, sharedMySQLBootAttempts)
			}
			break
		}
		if c != nil {
			_ = c.Terminate(context.Background())
		}
		lastErr = err
		if attempt < sharedMySQLBootAttempts {
			backoff := sharedMySQLBootBackoff(attempt)
			log.Printf("startMySQLM2Preflight boot attempt %d/%d failed: %v; retrying in %s",
				attempt, sharedMySQLBootAttempts, err, backoff)
			time.Sleep(backoff)
			continue
		}
		log.Printf("startMySQLM2Preflight boot attempt %d/%d failed: %v; giving up",
			attempt, sharedMySQLBootAttempts, err)
	}
	if container == nil {
		t.Fatalf("start container: %d attempts exhausted: %v", sharedMySQLBootAttempts, lastErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

// wantCodedRefusal asserts err carries the given sluice code.
func wantCodedRefusal(t *testing.T, err error, code sluicecode.Code, site string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: passed; want the %s refusal", site, code)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != code {
		t.Fatalf("%s: want %s; got %T: %v", site, code, err, err)
	}
}

// TestCDCReader_ReplicaSourcePreflight is the G5 door pin on a real
// mysqld.
func TestCDCReader_ReplicaSourcePreflight(t *testing.T) {
	dsn, cleanup := startMySQLM2Preflight(t, "--log-replica-updates=OFF")
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE orders (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		INSERT INTO orders (id) VALUES (1);
	`)

	eng := Engine{Flavor: FlavorVanilla}
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

	// --- Conjunction, first half: log_replica_updates=OFF alone (the
	// MariaDB default posture) must NOT refuse — this is not a replica.
	t.Run("off_but_not_a_replica_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges on a non-replica with log_replica_updates=OFF = %v; want nil", err)
		}
	})

	// --- Configure a replication channel (threads never started — the
	// channel's presence is the operator's stated intent, and blindness
	// begins the moment it starts). Both chokepoint families refuse.
	t.Run("configured_replica_refuses", func(t *testing.T) {
		applyMySQL(t, dsn, `CHANGE REPLICATION SOURCE TO SOURCE_HOST='192.0.2.10', SOURCE_PORT=3306,
			SOURCE_USER='repl', SOURCE_PASSWORD='replpw';`)
		defer applyMySQL(t, dsn, "RESET REPLICA ALL;")

		wantCodedRefusal(t, openStream(t), sluicecode.CodeCDCReplicaNoLogUpdates, "StreamChanges")

		if snap, err := eng.OpenSnapshotStream(ctx, dsn); err == nil {
			_ = snap.Close()
			t.Fatal("OpenSnapshotStream: accepted a blind replica source; want the coded refusal before any copy")
		} else {
			wantCodedRefusal(t, err, sluicecode.CodeCDCReplicaNoLogUpdates, "OpenSnapshotStream")
		}
	})

	// --- The promoted-primary shape (audit 2026-09-15 A0915-CLI-MEDIUM-1), as a
	// STATED decision with a measurement rather than a posture note: the
	// channel was started and then STOPPED — the ordinary state of a
	// primary promoted after a failover that never ran RESET REPLICA ALL.
	// Its threads are down, every direct write it takes is binlogged, and
	// it still REFUSES, because the door keys on the channel record. That
	// is today's verdict; whether such a server should be ACCEPTED is a
	// pending policy decision, and this cell is what a change to it must
	// flip. What the cell holds firm either way: the refusal names the
	// one remedy that can be run on this server — RESET REPLICA ALL — in
	// both the message and the hint, and running it releases the door
	// (the cell below).
	t.Run("stopped_channel_still_refuses", func(t *testing.T) {
		applyMySQL(t, dsn, `CHANGE REPLICATION SOURCE TO SOURCE_HOST='192.0.2.10', SOURCE_PORT=3306,
			SOURCE_USER='repl', SOURCE_PASSWORD='replpw';
			START REPLICA;
			STOP REPLICA;`)
		defer applyMySQL(t, dsn, "RESET REPLICA ALL;")
		if io, sqlThread := replicaThreadState(t, dsn); io != "No" || sqlThread != "No" {
			t.Fatalf("Replica_IO_Running=%q Replica_SQL_Running=%q after STOP REPLICA; want No/No — the cell "+
				"must measure a STOPPED channel, not a running one", io, sqlThread)
		}

		err := openStream(t)
		wantCodedRefusal(t, err, sluicecode.CodeCDCReplicaNoLogUpdates, "StreamChanges (stopped channel)")
		ce, _ := sluicecode.FromError(err)
		for _, home := range []struct{ name, text string }{{"message", err.Error()}, {"hint", ce.Hint}} {
			if !strings.Contains(home.text, "RESET REPLICA ALL") {
				t.Errorf("the refusal's %s does not name RESET REPLICA ALL — the only remedy that runs on a "+
					"promoted primary (\"point at the primary\": this IS the primary; \"restart mysqld\": a "+
					"production primary, to clear bookkeeping):\n%s", home.name, home.text)
			}
		}
	})

	// --- RESET REPLICA ALL (ran in the sub-tests' defers) releases the
	// door: the server is no longer a configured replica.
	t.Run("after_reset_replica_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges after RESET REPLICA ALL = %v; want nil", err)
		}
	})
}

// replicaThreadState reads Replica_IO_Running / Replica_SQL_Running from
// the default channel's SHOW REPLICA STATUS row, by column name — the row
// is wide and its column order is a server detail.
func replicaThreadState(t *testing.T, dsn string) (ioRunning, sqlRunning string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		t.Fatalf("SHOW REPLICA STATUS: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("SHOW REPLICA STATUS returned no row; the channel is not configured (%v)", rows.Err())
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("scan: %v", err)
	}
	for i, c := range cols {
		switch c {
		case "Replica_IO_Running", "Slave_IO_Running":
			ioRunning = vals[i].String
		case "Replica_SQL_Running", "Slave_SQL_Running":
			sqlRunning = vals[i].String
		}
	}
	return ioRunning, sqlRunning
}
