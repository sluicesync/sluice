//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 G5 — the replica-source preflight against a real mysqld, both
// directions on one container. The container boots with
// --log-replica-updates=OFF (mysql:8.0's default is ON, so the flag is
// the whole scenario), then walks the conjunction:
//
//	not a replica + OFF              → PASS   (the variable alone must not refuse)
//	channel RUNNING (connecting) + OFF → REFUSE (both CDC-open chokepoint families)
//	channel never started + OFF      → PASS   (both threads read No)
//	channel started, then STOPPED + OFF → PASS, with the REPLICA-CHANNELS-STOPPED
//	                                   INFO (the promoted-primary shape; operator
//	                                   decision 2026-09-15, audit 2026-09-15
//	                                   A0915-CLI-MEDIUM-1)
//	RESET REPLICA ALL + OFF          → PASS   (no channel at all)
//
// The replica-with-log-updates-ON pass direction is unit-pinned
// (TestPreflightReplicaSource) — it needs no second container because
// the variable read is the same code path either way. The full
// two-container blind-replica repro (replicated writes absent from the
// binlog) is the m2 sweep's recorded ground truth; this pin holds the
// DOOR, not the server mechanism. Every thread-state cell asserts the
// state it claims from SHOW REPLICA STATUS before grading the door, so a
// server that reported a different state than the cell assumes fails the
// premise rather than the verdict.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"log"
	"log/slog"
	"strings"
	"sync"
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

// captureInfoLog routes the default slog handler into a buffer at INFO
// for the duration of fn and returns what was written.
//
// The writer is mutex-guarded, and that is load-bearing rather than
// defensive tidiness. This comment used to say "the callers are not
// parallel" — true, and irrelevant: `slog.SetDefault` installs a
// PROCESS-WIDE logger, so every live goroutine writes through it, not
// just the caller. CI's `-race` shard caught the consequence on run
// 35043003665: a go-mysql `BinlogSyncer` started by an EARLIER subtest
// was still emitting INFO from `handleEventAndACK` while this helper
// read `buf.String()` for a later one. A syncer keeps logging after
// `Close()` returns, and a test cannot join a goroutine it does not
// own — so guarding the buffer is the only fix available here, and it
// holds for any stray logger rather than for the one we happened to
// find. The class (a capture-the-default-logger helper over an
// unguarded buffer, in a package that starts goroutines) is filed as
// A0915-LOGCAPTURE-1; it is NOT swept repo-wide here, because ~90 test
// files share the shape and most are never at risk.
func captureInfoLog(fn func()) string {
	buf := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

// lockedBuffer serialises writes against reads so a logger still running
// in another goroutine cannot race the reader. See captureInfoLog.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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
	const configure = `CHANGE REPLICATION SOURCE TO SOURCE_HOST='192.0.2.10', SOURCE_PORT=3306,
		SOURCE_USER='repl', SOURCE_PASSWORD='replpw';`

	// --- Conjunction, first half: log_replica_updates=OFF alone (the
	// MariaDB default posture) must NOT refuse — this is not a replica.
	t.Run("off_but_not_a_replica_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges on a non-replica with log_replica_updates=OFF = %v; want nil", err)
		}
	})

	// --- A channel whose threads RUN: START REPLICA against an unroutable
	// source leaves the SQL thread running and the IO thread Connecting —
	// replicated writes could arrive at any moment, and would never reach
	// this server's binlog. Both chokepoint families refuse.
	t.Run("running_channel_refuses", func(t *testing.T) {
		applyMySQL(t, dsn, configure+" START REPLICA;")
		defer applyMySQL(t, dsn, "STOP REPLICA; RESET REPLICA ALL;")
		if io, sqlThread := replicaThreadState(t, dsn, "SHOW REPLICA STATUS", ""); sqlThread != "Yes" || io == "No" {
			t.Fatalf("Replica_IO_Running=%q Replica_SQL_Running=%q after START REPLICA; want a running SQL thread "+
				"and a not-stopped IO thread — the cell must measure a RUNNING channel", io, sqlThread)
		}

		err := openStream(t)
		wantCodedRefusal(t, err, sluicecode.CodeCDCReplicaNoLogUpdates, "StreamChanges")
		ce, _ := sluicecode.FromError(err)
		for _, home := range []struct{ name, text string }{{"message", err.Error()}, {"hint", ce.Hint}} {
			if !strings.Contains(home.text, "RESET REPLICA ALL") || !strings.Contains(home.text, "STOP REPLICA") {
				t.Errorf("the refusal's %s does not name both STOP REPLICA and RESET REPLICA ALL — the remedies "+
					"that run on a promoted primary:\n%s", home.name, home.text)
			}
		}
		if !strings.Contains(err.Error(), "SQL=Yes") {
			t.Errorf("the refusal does not name the running channel's thread state: %v", err)
		}

		if snap, err := eng.OpenSnapshotStream(ctx, dsn); err == nil {
			_ = snap.Close()
			t.Fatal("OpenSnapshotStream: accepted a replica with a running channel; want the coded refusal before any copy")
		} else {
			wantCodedRefusal(t, err, sluicecode.CodeCDCReplicaNoLogUpdates, "OpenSnapshotStream")
		}
	})

	// --- A channel configured and never started: both threads read No,
	// nothing can be replicated in. Accepted (until 2026-09-15 this
	// refused on the channel record alone).
	t.Run("never_started_channel_is_accepted", func(t *testing.T) {
		applyMySQL(t, dsn, configure)
		defer applyMySQL(t, dsn, "RESET REPLICA ALL;")
		if io, sqlThread := replicaThreadState(t, dsn, "SHOW REPLICA STATUS", ""); io != "No" || sqlThread != "No" {
			t.Fatalf("Replica_IO_Running=%q Replica_SQL_Running=%q on a never-started channel; want No/No", io, sqlThread)
		}
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges with a never-started channel = %v; want nil", err)
		}
	})

	// --- The promoted-primary shape (audit 2026-09-15 A0915-CLI-MEDIUM-1): the
	// channel was started and then STOPPED — the ordinary state of a
	// primary promoted after a failover that never ran RESET REPLICA ALL.
	// Its threads are down and every direct write it takes is binlogged.
	// ACCEPTED by operator decision (2026-09-15), and not silently: the
	// INFO names the channel and the bookkeeping clear. Both chokepoint
	// families open.
	t.Run("stopped_channel_is_accepted", func(t *testing.T) {
		applyMySQL(t, dsn, configure+" START REPLICA; STOP REPLICA;")
		defer applyMySQL(t, dsn, "RESET REPLICA ALL;")
		if io, sqlThread := replicaThreadState(t, dsn, "SHOW REPLICA STATUS", ""); io != "No" || sqlThread != "No" {
			t.Fatalf("Replica_IO_Running=%q Replica_SQL_Running=%q after STOP REPLICA; want No/No — the cell "+
				"must measure a STOPPED channel, not a running one", io, sqlThread)
		}

		var err error
		logged := captureInfoLog(func() { err = openStream(t) })
		if err != nil {
			t.Fatalf("StreamChanges on a promoted primary with a stopped channel = %v; want the reader to open", err)
		}
		for _, want := range []string{replicaChannelsStoppedMarker, "(default) [IO=No SQL=No]", "RESET REPLICA ALL"} {
			if !strings.Contains(logged, want) {
				t.Errorf("the acceptance INFO is missing %q:\n%s", want, logged)
			}
		}

		snap, err := eng.OpenSnapshotStream(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenSnapshotStream on a promoted primary with a stopped channel = %v; want it to open", err)
		}
		_ = snap.Close()
	})

	// --- RESET REPLICA ALL (ran in the sub-tests' defers) leaves no
	// channel at all.
	t.Run("after_reset_replica_passes", func(t *testing.T) {
		if err := openStream(t); err != nil {
			t.Fatalf("StreamChanges after RESET REPLICA ALL = %v; want nil", err)
		}
	})
}

// replicaThreadState reads the IO / SQL thread state of one channel from
// stmt's rows — by column name, because the row is wide, its column order
// is a server detail, and the columns are spelled Replica_* on MySQL
// 8.0.22+ and Slave_* on MariaDB. channel selects the row by
// Channel_Name / Connection_name ("" is the default channel).
func replicaThreadState(t *testing.T, dsn, stmt, channel string) (ioRunning, sqlRunning string) {
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
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		var name, io, sqlThread string
		for i, c := range cols {
			switch strings.ToLower(c) {
			case "channel_name", "connection_name":
				name = vals[i].String
			case "replica_io_running", "slave_io_running":
				io = vals[i].String
			case "replica_sql_running", "slave_sql_running":
				sqlThread = vals[i].String
			}
		}
		if strings.EqualFold(name, channel) {
			return io, sqlThread
		}
	}
	t.Fatalf("%s returned no row for channel %q; the channel is not configured (%v)", stmt, channel, rows.Err())
	return "", ""
}
