//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Chaos-lite container-restart gate for the CDC retry arc (audit
// 2026-07-23 TEST-1 / gate G-11, milestone M1-2).
//
// The transient-classification/retry class escaped to live soaks FOUR
// times inside one delta (the 2026-07-22 d1-trigger soak, the
// scale-soak connect-phase incident, Bug 199, Bug 200) because all of
// its coverage was unit pins against hand-transcribed strings: a driver
// upgrade that rewords a dial error passes every pin while the real
// classifier stops matching — and the next 30s blip kills a multi-day
// sync. This suite ground-truths the whole v0.99.286→290 arc against
// REAL driver errors by restarting the actual containers mid-CDC, per
// engine leg:
//
//   - TARGET restart with pending source writes → the apply path's
//     classified transients + the connect-phase retry (Bugs 199a/200)
//     + the D0-4 ReadPosition-failure latch fix ride the outage in the
//     SAME process (Run never returns), then warm-resume — never a
//     restart-from-scratch (witness per leg below) — and the outage-
//     window source DELETE is REPLAYED (an idempotent re-snapshot at
//     NOW would leave the deleted row as a silent orphan: the exact
//     D0-4 divergence).
//   - SOURCE restart → the CDC read error classifies transient, the
//     re-establish (control-table preamble, ARCH-4 + CDC reopen) rides
//     the outage, and post-restart writes converge.
//
// Restart-from-scratch witnesses (why they differ per leg): a forced
// fresh cold start behaves differently per source class. With a
// non-idempotent source (MySQL binlog → PG), it DROPS + recreates the
// in-scope target tables — pinned by the PG table's OID staying
// constant. With an idempotent source (PG → MySQL), it re-copies with
// UPSERT (tables survive) but recreates the slot at NOW, so the
// outage-window DELETE is never replayed — pinned by the
// deleted-row-absent convergence itself; the MySQL CREATE_TIME check is
// a belt. PG exposes no slot-creation identity in its catalogs, so slot
// sameness is asserted indirectly through that DELETE replay.
//
// Containers are stopped + started IN PLACE (docker stop/start; only
// Terminate removes the container), the startShardedVTTestServer
// restartSource pattern — with one addition: the DB port is bound to an
// explicitly-reserved FIXED host port at create time, because Docker
// may re-allocate an ephemeral (empty-HostPort) binding on restart
// (observed under Rancher Desktop: the restarted target came back on a
// new host port and the streamer's DSN dialed the dead one forever).
//
// Test names ride the per-PR pipeline-rest-streamer shard
// (`-run ^TestStreamer_` over ./internal/pipeline/) — no filter or
// manifest change needed.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// chaosFixedPortModifier reserves a free host port and returns a
// HostConfigModifier binding containerPort (e.g. "5432/tcp") to it,
// plus the reserved port. A fixed binding survives docker stop/start —
// an ephemeral one may be re-allocated by the daemon on restart, which
// would strand the streamer's DSN on a dead port (see file header).
// The tiny reserve-then-bind race is acceptable: tests in this package
// run sequentially, and a collision fails loudly at container create.
func chaosFixedPortModifier(t *testing.T, containerPort string) (func(*mobycontainer.HostConfig), string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve host port: %v", err)
	}
	hostPort := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()
	return func(hc *mobycontainer.HostConfig) {
		if hc.PortBindings == nil {
			hc.PortBindings = mobynetwork.PortMap{}
		}
		hc.PortBindings[mobynetwork.MustParsePort(containerPort)] = []mobynetwork.PortBinding{
			{HostIP: netip.IPv4Unspecified(), HostPort: hostPort},
		}
	}, hostPort
}

// chaosRestartFn stops the SAME container in place, runs the optional
// whileDown callback INSIDE the guaranteed-down window (nil to skip),
// re-starts it, and waits until the database answers queries again
// before returning. whileDown is how the target-restart phase commits
// its pending source writes while the target is provably unreachable.
type chaosRestartFn func(t *testing.T, whileDown func())

// chaosRestarter builds the stop+whileDown+start+ready closure shared
// by both engines' boot helpers. The DSN stays valid across the restart
// (port bindings survive a Docker stop/start; only Terminate removes
// the container).
func chaosRestarter(container testcontainers.Container, driver, dsn string) chaosRestartFn {
	return func(t *testing.T, whileDown func()) {
		t.Helper()
		rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer rcancel()
		stopTimeout := 30 * time.Second
		if err := container.Stop(rctx, &stopTimeout); err != nil {
			t.Fatalf("chaos restart: stop container: %v", err)
		}
		if whileDown != nil {
			whileDown()
		}
		// A short down-window so the in-flight streamer demonstrably
		// observes the outage (real dial-refused / severed-conn errors)
		// rather than racing a near-instant restart.
		time.Sleep(2 * time.Second)
		if err := container.Start(rctx); err != nil {
			t.Fatalf("chaos restart: start container: %v", err)
		}
		waitChaosDBReady(t, driver, dsn, 3*time.Minute)
	}
}

// waitChaosDBReady polls SELECT 1 until the restarted database serves
// queries again (InnoDB recovery / PG crash recovery included).
func waitChaosDBReady(t *testing.T, driver, dsn string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if chaosDBAnswers(driver, dsn) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("chaos restart: database (%s) not ready again within %s", driver, timeout)
}

func chaosDBAnswers(driver, dsn string) bool {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var one int
	return db.QueryRowContext(ctx, "SELECT 1").Scan(&one) == nil
}

// startMySQLBinlogChaos boots a binlog-enabled MySQL source (source_db)
// and returns its DSN plus a restart closure. Mirrors startMySQLBinlog,
// which cannot be reused directly because it does not expose the
// container handle the restart needs.
func startMySQLBinlogChaos(t *testing.T) (dsn string, restart chaosRestartFn, cleanup func()) {
	t.Helper()
	fixedPort, _ := chaosFixedPortModifier(t, "3306/tcp")
	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				HostConfigModifier: fixedPort,
				Cmd: []string{
					"mysqld",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=ROW",
					"--binlog-row-image=FULL",
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
	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, chaosRestarter(container, "mysql", conn), terminate
}

// startMySQLTargetChaos boots a plain MySQL target (no binlog needed)
// and returns a target_db DSN plus a restart closure. Like startMySQL,
// it boots the prebaked image's source_db and CREATEs target_db beside
// it (the pre-baked image has source_db baked in, so a different
// MYSQL_DATABASE would be silently ignored).
func startMySQLTargetChaos(t *testing.T) (dsn string, restart chaosRestartFn, cleanup func()) {
	t.Helper()
	fixedPort, _ := chaosFixedPortModifier(t, "3306/tcp")
	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{HostConfigModifier: fixedPort},
		}),
	)
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
	db, err := sql.Open("mysql", conn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}
	tgtConn, err := buildMySQLDSN(conn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return tgtConn, chaosRestarter(container, "mysql", tgtConn), terminate
}

// startPostgresChaos boots a Postgres container (wal_level=logical when
// logical is set — required when PG is the SOURCE; harmless extra
// config for a target) and returns its DSN plus a restart closure.
func startPostgresChaos(t *testing.T, logical bool) (dsn string, restart chaosRestartFn, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fixedPort, _ := chaosFixedPortModifier(t, "5432/tcp")
	opts := []testcontainers.ContainerCustomizer{
		// source_db, matching the other PG helpers: the pre-baked image
		// (task #68) has already run initdb with source_db baked in, so a
		// different POSTGRES_DB here would be silently ignored.
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{HostConfigModifier: fixedPort},
		}),
	}
	if logical {
		opts = append(opts, testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=8",
					"-c", "max_replication_slots=8",
				},
			},
		}))
	}
	container, err := pgtc.Run(ctx, pgPrebakedImage, opts...)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	conn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, chaosRestarter(container, "pgx", conn), terminate
}

// chaosRow is the items-table row shape both legs assert on.
type chaosRow struct {
	ID    int64
	Label string
	Qty   int64
}

// readChaosRows returns the items rows ordered by id, or an error —
// callers polling for convergence treat errors as "not yet".
func readChaosRows(driver, dsn string) ([]chaosRow, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id, label, qty FROM items ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []chaosRow
	for rows.Next() {
		var r chaosRow
		if err := rows.Scan(&r.ID, &r.Label, &r.Qty); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// waitForChaosRows polls until the target's items rows equal want
// EXACTLY (same ids, same values, no extras — deletions included, which
// the package's count-based `>= n` helpers cannot assert).
func waitForChaosRows(t *testing.T, driver, dsn string, want []chaosRow, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, err := readChaosRows(driver, dsn)
		if err == nil && chaosRowsEqual(got, want) {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	got, err := readChaosRows(driver, dsn)
	t.Logf("convergence timeout: target rows = %+v (read err: %v); want %+v", got, err, want)
	return false
}

func chaosRowsEqual(got, want []chaosRow) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// assertStreamerAlive pins the same-PID ride-out: Run must NOT have
// returned during the outage — the retry loop absorbed it in process.
func assertStreamerAlive(t *testing.T, runErr <-chan error, phase string) {
	t.Helper()
	select {
	case err := <-runErr:
		t.Fatalf("%s: Streamer.Run exited during the outage window (want same-PID ride-out via the retry budget): %v", phase, err)
	default:
	}
}

// pgTableOID reads the target table's OID — the drop+recreate witness
// for the PG target: a forced restart-from-scratch with a non-idempotent
// source drops + recreates the in-scope tables, which changes the OID.
func pgTableOID(t *testing.T, dsn, table string) uint32 {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var oid uint32
	if err := db.QueryRowContext(ctx, "SELECT $1::regclass::oid", table).Scan(&oid); err != nil {
		t.Fatalf("read table OID for %q: %v", table, err)
	}
	return oid
}

// mysqlTableCreateTime reads the target table's CREATE_TIME with the
// information_schema stats cache disabled (information_schema_stats_expiry
// caches TABLES rows for a day by default, which would let a
// drop+recreate hide behind a stale row and green the witness vacuously).
func mysqlTableCreateTime(t *testing.T, dsn, schema, table string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		t.Fatalf("disable stats cache: %v", err)
	}
	var created sql.NullString
	if err := conn.QueryRowContext(
		ctx,
		"SELECT CREATE_TIME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		schema, table,
	).Scan(&created); err != nil {
		t.Fatalf("read CREATE_TIME for %s.%s: %v", schema, table, err)
	}
	if !created.Valid {
		t.Fatalf("CREATE_TIME for %s.%s is NULL", schema, table)
	}
	return created.String
}

// countSluiceSlots counts sluice replication slots on the PG source —
// after both restarts exactly ONE must exist (warm resume reuses the
// existing slot; a second one would mean a stray re-establishment).
func countSluiceSlots(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sluice%'").Scan(&n); err != nil {
		t.Fatalf("count slots: %v", err)
	}
	return n
}

// chaosStreamer builds the Streamer for a chaos leg with a retry budget
// generous enough to ride a real container restart (~10-40s, mostly
// dial-refused failures at ≤2s backoff) while still loud-bounded — a
// target that never comes back exhausts the budget and fails.
func chaosStreamer(source, target string, sourceDSN, targetDSN, streamID string) (*Streamer, error) {
	srcEng, ok := engines.Get(source)
	if !ok {
		return nil, fmt.Errorf("%s engine not registered", source)
	}
	tgtEng, ok := engines.Get(target)
	if !ok {
		return nil, fmt.Errorf("%s engine not registered", target)
	}
	return &Streamer{
		Source:                srcEng,
		Target:                tgtEng,
		SourceDSN:             sourceDSN,
		TargetDSN:             targetDSN,
		StreamID:              streamID,
		ApplyRetryAttempts:    300,
		ApplyRetryBackoffBase: 250 * time.Millisecond,
		ApplyRetryBackoffCap:  2 * time.Second,
	}, nil
}

// TestStreamer_ChaosLite_MySQLToPG_ContainerRestarts — MySQL (binlog)
// source → PG target. Target restart with pending writes, then source
// restart, asserting ride-out / warm resume / DELETE replay /
// byte-identical convergence per the file header.
func TestStreamer_ChaosLite_MySQLToPG_ContainerRestarts(t *testing.T) {
	srcDSN, restartSource, srcCleanup := startMySQLBinlogChaos(t)
	defer srcCleanup()
	tgtDSN, restartTarget, tgtCleanup := startPostgresChaos(t, false /* target: no logical WAL needed */)
	defer tgtCleanup()

	applyMySQLDDL(t, srcDSN, `
		CREATE TABLE items (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			label VARCHAR(255) NOT NULL,
			qty   INT          NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO items (id, label, qty) VALUES (1, 'r1', 1), (2, 'r2', 2), (3, 'r3', 3);
	`)

	s, err := chaosStreamer("mysql", "postgres", srcDSN, tgtDSN, "chaoslite-mysql-pg")
	if err != nil {
		t.Fatal(err)
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(streamCtx) }()

	// ---- Phase 1: cold copy + CDC probe (anchor written) ----
	if !waitForChaosRows(t, "pgx", tgtDSN, []chaosRow{{1, "r1", 1}, {2, "r2", 2}, {3, "r3", 3}}, 120*time.Second) {
		t.Fatal("phase 1: bulk copy never delivered the seed rows to the PG target")
	}
	applyMySQLDDL(t, srcDSN, "INSERT INTO items (id, label, qty) VALUES (4, 'r4', 4);")
	if !waitForChaosRows(t, "pgx", tgtDSN, []chaosRow{{1, "r1", 1}, {2, "r2", 2}, {3, "r3", 3}, {4, "r4", 4}}, 60*time.Second) {
		t.Fatal("phase 1: CDC probe row never arrived — the stream is not in the CDC phase")
	}
	oidBefore := pgTableOID(t, tgtDSN, "items")

	// ---- Phase 2: TARGET restart with pending writes ----
	// Stop the target, commit writes on the (healthy) source while it is
	// provably down — including the outage-window DELETE — then bring it
	// back. The stream must ride the outage in process and warm-resume:
	// r5 arrives, r1's update arrives, and the DELETE of r2 is REPLAYED
	// (a re-snapshot at NOW would leave r2 as a silent orphan — the D0-4
	// divergence).
	restartTarget(t, func() {
		applyMySQLDDL(t, srcDSN, `
			INSERT INTO items (id, label, qty) VALUES (5, 'r5', 5);
			UPDATE items SET label = 'r1-updated' WHERE id = 1;
			DELETE FROM items WHERE id = 2;
		`)
	})

	wantAfterTargetRestart := []chaosRow{{1, "r1-updated", 1}, {3, "r3", 3}, {4, "r4", 4}, {5, "r5", 5}}
	if !waitForChaosRows(t, "pgx", tgtDSN, wantAfterTargetRestart, 180*time.Second) {
		t.Fatal("phase 2: target never converged after the TARGET restart — outage writes (insert/update) or the outage-window DELETE were not replayed on warm resume")
	}
	assertStreamerAlive(t, runErr, "phase 2 (target restart)")
	if oidAfter := pgTableOID(t, tgtDSN, "items"); oidAfter != oidBefore {
		t.Fatalf("phase 2: PG target table OID changed %d → %d — the tables were dropped + recreated (restart-from-scratch), not warm-resumed", oidBefore, oidAfter)
	}

	// ---- Phase 3: SOURCE restart ----
	restartSource(t, nil)
	applyMySQLDDL(t, srcDSN, `
		INSERT INTO items (id, label, qty) VALUES (6, 'r6', 6);
		DELETE FROM items WHERE id = 3;
	`)
	wantFinal := []chaosRow{{1, "r1-updated", 1}, {4, "r4", 4}, {5, "r5", 5}, {6, "r6", 6}}
	if !waitForChaosRows(t, "pgx", tgtDSN, wantFinal, 180*time.Second) {
		t.Fatal("phase 3: target never converged after the SOURCE restart — the CDC reopen did not ride the outage")
	}
	assertStreamerAlive(t, runErr, "phase 3 (source restart)")
	if oidAfter := pgTableOID(t, tgtDSN, "items"); oidAfter != oidBefore {
		t.Fatalf("phase 3: PG target table OID changed %d → %d — restart-from-scratch after the source restart", oidBefore, oidAfter)
	}

	// ---- Phase 4: byte-identical convergence + clean shutdown ----
	srcRows, err := readChaosRows("mysql", srcDSN)
	if err != nil {
		t.Fatalf("read source rows: %v", err)
	}
	if !chaosRowsEqual(srcRows, wantFinal) {
		t.Fatalf("source rows drifted from the script: %+v", srcRows)
	}
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_ChaosLite_PGToMySQL_ContainerRestarts — PG (logical)
// source → MySQL target: the reverse leg, same phases. Here the
// restart-from-scratch witness is the outage-window DELETE itself (an
// idempotent PG source re-snapshots with UPSERT and a slot recreated at
// NOW, which never replays the DELETE → orphan row → convergence
// fails); CREATE_TIME is the drop+recreate belt and the slot count pins
// single-slot warm resume.
func TestStreamer_ChaosLite_PGToMySQL_ContainerRestarts(t *testing.T) {
	srcDSN, restartSource, srcCleanup := startPostgresChaos(t, true /* source: logical WAL */)
	defer srcCleanup()
	tgtDSN, restartTarget, tgtCleanup := startMySQLTargetChaos(t)
	defer tgtCleanup()

	// Every INSERT supplies an explicit id (BY DEFAULT permits it): the
	// identity sequence pre-allocates values in WAL (SEQ_LOG_VALS=32), so
	// the source restart jumps generated ids (observed: the post-restart
	// row landed as id 34) — correct PG behaviour, but it would make the
	// byte-identical convergence script nondeterministic.
	applyDDL(t, srcDSN, `
		CREATE TABLE items (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			label VARCHAR(255) NOT NULL,
			qty   INT          NOT NULL
		);
		INSERT INTO items (id, label, qty) VALUES (1, 'r1', 1), (2, 'r2', 2), (3, 'r3', 3);
	`)

	s, err := chaosStreamer("postgres", "mysql", srcDSN, tgtDSN, "chaoslite-pg-mysql")
	if err != nil {
		t.Fatal(err)
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(streamCtx) }()

	// ---- Phase 1: cold copy + CDC probe ----
	if !waitForChaosRows(t, "mysql", tgtDSN, []chaosRow{{1, "r1", 1}, {2, "r2", 2}, {3, "r3", 3}}, 120*time.Second) {
		t.Fatal("phase 1: bulk copy never delivered the seed rows to the MySQL target")
	}
	applyDDL(t, srcDSN, "INSERT INTO items (id, label, qty) VALUES (4, 'r4', 4);")
	if !waitForChaosRows(t, "mysql", tgtDSN, []chaosRow{{1, "r1", 1}, {2, "r2", 2}, {3, "r3", 3}, {4, "r4", 4}}, 60*time.Second) {
		t.Fatal("phase 1: CDC probe row never arrived — the stream is not in the CDC phase")
	}
	createTimeBefore := mysqlTableCreateTime(t, tgtDSN, "target_db", "items")
	if n := countSluiceSlots(t, srcDSN); n != 1 {
		t.Fatalf("phase 1: %d sluice slots on the source; want exactly 1", n)
	}

	// ---- Phase 2: TARGET restart with pending writes ----
	restartTarget(t, func() {
		applyDDL(t, srcDSN, `
			INSERT INTO items (id, label, qty) VALUES (5, 'r5', 5);
			UPDATE items SET label = 'r1-updated' WHERE id = 1;
			DELETE FROM items WHERE id = 2;
		`)
	})

	wantAfterTargetRestart := []chaosRow{{1, "r1-updated", 1}, {3, "r3", 3}, {4, "r4", 4}, {5, "r5", 5}}
	if !waitForChaosRows(t, "mysql", tgtDSN, wantAfterTargetRestart, 180*time.Second) {
		t.Fatal("phase 2: target never converged after the TARGET restart — the outage-window DELETE was not replayed (re-snapshot-at-NOW orphan, the D0-4 divergence) or the outage writes were lost")
	}
	assertStreamerAlive(t, runErr, "phase 2 (target restart)")
	if ct := mysqlTableCreateTime(t, tgtDSN, "target_db", "items"); ct != createTimeBefore {
		t.Fatalf("phase 2: MySQL target table CREATE_TIME changed %s → %s — tables were dropped + recreated", createTimeBefore, ct)
	}

	// ---- Phase 3: SOURCE restart ----
	// The replication slot is durable across a PG restart; warm resume
	// must reattach to the SAME slot and stream the post-restart writes.
	restartSource(t, nil)
	applyDDL(t, srcDSN, `
		INSERT INTO items (id, label, qty) VALUES (6, 'r6', 6);
		DELETE FROM items WHERE id = 3;
	`)
	wantFinal := []chaosRow{{1, "r1-updated", 1}, {4, "r4", 4}, {5, "r5", 5}, {6, "r6", 6}}
	if !waitForChaosRows(t, "mysql", tgtDSN, wantFinal, 180*time.Second) {
		t.Fatal("phase 3: target never converged after the SOURCE restart — the CDC reopen did not ride the outage")
	}
	assertStreamerAlive(t, runErr, "phase 3 (source restart)")
	if n := countSluiceSlots(t, srcDSN); n != 1 {
		t.Fatalf("phase 3: %d sluice slots on the source after both restarts; want exactly 1 (warm resume reuses the slot)", n)
	}
	if ct := mysqlTableCreateTime(t, tgtDSN, "target_db", "items"); ct != createTimeBefore {
		t.Fatalf("phase 3: MySQL target table CREATE_TIME changed %s → %s — tables were dropped + recreated", createTimeBefore, ct)
	}

	// ---- Phase 4: byte-identical convergence + clean shutdown ----
	srcRows, err := readChaosRows("pgx", srcDSN)
	if err != nil {
		t.Fatalf("read source rows: %v", err)
	}
	if !chaosRowsEqual(srcRows, wantFinal) {
		t.Fatalf("source rows drifted from the script: %+v", srcRows)
	}
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
