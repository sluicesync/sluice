//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

// Audit 2026-07-23 D0-5 / owner call Q1 — the GROUND-TRUTH gate for the
// engine temporal-literal semantics, the collation matrices' sibling for the
// temporal axis. Compile normalizes a temporal literal that is finer-grained
// than the column to the SOURCE ENGINE's own coercion of it
// (ir.TemporalLiteralSemantics); this matrix asserts, against each REAL
// server's own WHERE verdict, that Compile(...).Eval(...) classifies every
// boundary row exactly as the server does — per engine, per literal shape,
// per operator class. The three engines resolve the mismatch three different
// ways (PG casts the literal to the column, truncating a DATE's time-of-day
// and rounding fractional seconds HALF-EVEN to µs; MySQL promotes the DATE
// column and rounds HALF-UP; MariaDB promotes and TRUNCATES), so a green
// cell on one engine proves nothing about the others — the Bug-74 family
// discipline applied to the temporal-coercion axis. The oracle is the real
// server, never the code under test.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
)

// realMariaDBImage matches the LTS line the engines-mysql shard already
// exercises (flavor_mariadb_integration_test.go), so no new image joins CI.
const realMariaDBImage = "mariadb:11.8"

// startRealMariaDB boots a throwaway MariaDB and returns a live *sql.DB, the
// server version, and a cleanup. Generic container: the MariaDB entrypoint's
// init phase runs a socket-only temp server, so a TCP listen + ping loop is
// the reliable readiness signal (mirrors the engines/mysql shared helper).
func startRealMariaDB(t *testing.T) (db *sql.DB, version string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        realMariaDBImage,
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MARIADB_ROOT_PASSWORD": "rootpw",
				"MARIADB_DATABASE":      "sluice_rp",
			},
			WaitingFor: wait.ForListeningPort("3306/tcp").WithStartupTimeout(4 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		t.Fatalf("boot real mariadb: %v", err)
	}
	terminate := func() { _ = container.Terminate(context.Background()) }

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		t.Fatalf("container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		terminate()
		t.Fatalf("container port: %v", err)
	}
	dsn := fmt.Sprintf("root:rootpw@tcp(%s:%s)/sluice_rp?parseTime=true&charset=utf8mb4", host, port.Port())
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		if err := conn.PingContext(ctx); err == nil {
			break
		} else if time.Now().After(deadline) {
			_ = conn.Close()
			terminate()
			t.Fatalf("mariadb never became ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	return conn, version, func() {
		_ = conn.Close()
		terminate()
	}
}

// temporalMatrixRow is one boundary row, stored on the server and mirrored
// client-side per docs/value-types.md (FamilyTemporal decodes to a UTC
// time.Time; a date at midnight).
type temporalMatrixRow struct {
	id int
	d  time.Time // date column value
	dt time.Time // datetime(6)/timestamp column value
}

// temporalMatrixRows are the boundary rows every engine's matrix stores:
// the DATE truncation/promotion boundary day, the µs half-boundary floor
// and its rounded-up twin, the carry twin (+1s) and the no-carry twin
// (.999999) that separate the three engines' fractional-second rules.
func temporalMatrixRows() []temporalMatrixRow {
	mk := func(h, m, s, ns int) time.Time { return time.Date(2026, 1, 15, h, m, s, ns, time.UTC) }
	return []temporalMatrixRow{
		{1, time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), mk(8, 30, 0, 123456000)},
		{2, time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC), mk(8, 30, 0, 123457000)},
		{3, time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC), mk(8, 30, 1, 0)},
		{4, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), mk(8, 30, 0, 999999000)},
	}
}

// temporalMatrixPredicates are the literal-shape × operator-class cells: the
// DATE × time-bearing shapes (=, <, !=, 3VL negation, IN with a mixed list),
// and the fractional-second shapes at the half-boundary (.1234565 — the
// flavor discriminator), the 7-digit round/truncate split, and the carry
// boundary (.9999995). Every predicate compiles on every engine; the
// EXPECTED verdict is read from the live server per row, never hand-written.
func temporalMatrixPredicates() []string {
	return []string{
		// DATE × time-bearing literal (PG truncates; MySQL/MariaDB promote).
		"d = '2026-01-15 08:30:00'",
		"d < '2026-01-15 12:00:00'",
		"d != '2026-01-15 12:00:00'",
		"NOT (d >= '2026-01-15 12:00:00')",
		"d IN ('2026-01-15 08:30', '2026-02-01')",
		"d NOT IN ('2026-01-15 08:30:00')",
		"d = '2026-01-15 00:00:00'",
		// Fractional seconds beyond µs (PG half-even; MySQL half-up;
		// MariaDB truncate; carry vs no-carry).
		"dt = '2026-01-15 08:30:00.1234565'",
		"dt = '2026-01-15 08:30:00.1234575'",
		"dt = '2026-01-15 08:30:00.1234567'",
		"dt >= '2026-01-15 08:30:00.1234561'",
		"dt = '2026-01-15 08:30:00.9999995'",
		"dt < '2026-01-15 08:30:00.9999995'",
		"dt IN ('2026-01-15 08:30:00.1234565', '2026-02-01 00:00:00')",
		// Control cells at engine granularity (byte-identical pass-through).
		"d = '2026-01-15'",
		"dt = '2026-01-15 08:30:00.123456'",
	}
}

// runTemporalMatrix drives the matrix against one live engine: create the
// two-column table, store the boundary rows, then per (predicate × row)
// assert the server's own WHERE verdict (the verdict SQL, interpolating the
// trusted constant predicates above) equals the shipped Compile(...).Eval(...)
// classification under the engine's real resolver.
func runTemporalMatrix(
	t *testing.T,
	db *sql.DB,
	resolver ir.CollationResolver,
	createTable string,
	insert string,
	verdict func(pred string, id int) string,
) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS t_temporal"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, createTable); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := temporalMatrixRows()
	for _, r := range rows {
		if _, err := db.ExecContext(ctx, insert,
			r.id, r.d.Format("2006-01-02"), r.dt.Format("2006-01-02 15:04:05.999999")); err != nil {
			t.Fatalf("insert row %d: %v", r.id, err)
		}
	}

	infos := ColumnInfosFromIR(resolver, []*ir.Column{
		{Name: "d", Type: ir.Date{}},
		{Name: "dt", Type: ir.DateTime{Precision: 6}},
	}, false)

	for _, pred := range temporalMatrixPredicates() {
		p, err := Compile("t_temporal", pred, infos)
		if err != nil {
			t.Fatalf("Compile(%q): %v", pred, err)
		}
		for _, r := range rows {
			var serverMatches int
			if err := db.QueryRowContext(ctx, verdict(pred, r.id)).Scan(&serverMatches); err != nil {
				t.Fatalf("server verdict for %q row %d: %v", pred, r.id, err)
			}
			server := serverMatches > 0
			client := p.Eval(ir.Row{"id": int64(r.id), "d": r.d, "dt": r.dt})
			if server != client {
				t.Errorf("DIVERGENCE on %q row %d (d=%s dt=%s): server=%v client=%v — the client evaluator does not reproduce this engine's temporal-literal coercion",
					pred, r.id, r.d.Format("2006-01-02"), r.dt.Format("2006-01-02 15:04:05.999999"), server, client)
			}
		}
	}
}

// TestRealPostgres_TemporalLiteralMatrix: PG casts the literal to the
// column's type — DATE truncates the time-of-day, timestamps round
// fractional seconds half-even to µs (observed 16.14, re-proven live here).
func TestRealPostgres_TemporalLiteralMatrix(t *testing.T) {
	db, version, cleanup := startRealPostgres(t)
	defer cleanup()
	t.Logf("real Postgres server version: %s", version)

	runTemporalMatrix(
		t, db, testPGResolver,
		"CREATE TABLE t_temporal (id int PRIMARY KEY, d date, dt timestamp)",
		"INSERT INTO t_temporal (id, d, dt) VALUES ($1, $2::date, $3::timestamp)",
		func(pred string, id int) string {
			return fmt.Sprintf("SELECT count(*) FROM t_temporal WHERE id = %d AND (%s)", id, pred)
		},
	)
}

// TestRealMySQL_TemporalLiteralMatrix: MySQL promotes the DATE column to
// datetime and rounds fractional seconds half-up (observed 8.0.46,
// re-proven live here).
func TestRealMySQL_TemporalLiteralMatrix(t *testing.T) {
	db, version, cleanup := startRealMySQL(t)
	defer cleanup()
	t.Logf("real MySQL server version: %s", version)

	runTemporalMatrix(
		t, db, testMySQLResolver,
		"CREATE TABLE t_temporal (id INT PRIMARY KEY, d DATE, dt DATETIME(6))",
		"INSERT INTO t_temporal (id, d, dt) VALUES (?, ?, ?)",
		func(pred string, id int) string {
			return fmt.Sprintf("SELECT count(*) FROM t_temporal WHERE id = %d AND (%s)", id, pred)
		},
	)
}

// TestRealMariaDB_TemporalLiteralMatrix: MariaDB promotes like MySQL but
// TRUNCATES fractional seconds beyond µs — no rounding, no carry (observed
// 11.8.8, re-proven live here). The flavor split is exactly why the mysql
// engine's resolver is flavor-parameterized on this axis.
func TestRealMariaDB_TemporalLiteralMatrix(t *testing.T) {
	db, version, cleanup := startRealMariaDB(t)
	defer cleanup()
	t.Logf("real MariaDB server version: %s", version)

	runTemporalMatrix(
		t, db, testMariaDBResolver,
		"CREATE TABLE t_temporal (id INT PRIMARY KEY, d DATE, dt DATETIME(6))",
		"INSERT INTO t_temporal (id, d, dt) VALUES (?, ?, ?)",
		func(pred string, id int) string {
			return fmt.Sprintf("SELECT count(*) FROM t_temporal WHERE id = %d AND (%s)", id, pred)
		},
	)
}
