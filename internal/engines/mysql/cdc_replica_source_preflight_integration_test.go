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
	"log"
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
			sharedMySQLImage,
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

	// --- RESET REPLICA ALL (ran in the sub-test's defer) releases the
	// door: the server is no longer a configured replica.
	t.Run("after_reset_replica_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges after RESET REPLICA ALL = %v; want nil", err)
		}
	})
}
