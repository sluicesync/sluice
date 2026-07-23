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
// and rounding fractional seconds by its DOUBLE-MEDIATED rint(strtod·10⁶)
// rule; MySQL promotes the DATE column and rounds HALF-UP on the exact
// digits; MariaDB promotes and TRUNCATES), so a green cell on one engine
// proves nothing about the others — the Bug-74 family discipline applied to
// the temporal-coercion axis. The oracle is the real server, never the code
// under test: stored row values are READ BACK from the server (so typmod/fsp
// storage coercion is the server's own), and the randomized fraction sweep
// (PG) gates the whole fraction CLASS rather than hand-picked
// representatives — the review-F1 lesson, where an exact-decimal half-even
// implementation agreed on every hand-picked boundary and silently diverged
// on ~0.1% of 7-digit fractions ('.0001255' → .000125 on PG, .000126 under
// exact decimal rounding).

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
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

// temporalMatrixRow is one boundary row's INSERT literals, per column
// ("" = SQL NULL). The stored values are read back from the server after
// insert (never re-derived client-side), so typmod/fsp storage coercion —
// e.g. '.5' into timestamp(0)/DATETIME — is the server's own.
type temporalMatrixRow struct {
	id  int
	d   string // date column
	dt  string // full-precision datetime(6)/timestamp
	dt0 string // fsp/typmod 0
	dt3 string // fsp/typmod 3
}

// temporalMatrixRows are the boundary rows every engine's matrix stores: the
// DATE truncation/promotion boundary day; the µs half-boundary floor and its
// rounded-up twin; the carry twin (+1s) and the no-carry twin (.999999) that
// separate the fractional-second rules; the .000125/.000126/.000127 triple
// the review-F1 double-mediated pair lands on; fsp-coerced dt0/dt3 values;
// and an all-NULL row (the 3VL arc).
func temporalMatrixRows() []temporalMatrixRow {
	return []temporalMatrixRow{
		{1, "2026-01-15", "2026-01-15 08:30:00.123456", "2026-01-15 08:30:00", "2026-01-15 08:30:00.123"},
		{2, "2026-01-14", "2026-01-15 08:30:00.123457", "2026-01-15 08:30:01", "2026-01-15 08:30:00.124"},
		{3, "2026-01-16", "2026-01-15 08:30:01", "2026-01-15 08:30:00.5", "2026-01-15 08:30:00.1235"},
		{4, "2026-02-01", "2026-01-15 08:30:00.999999", "2026-01-15 08:30:00", "2026-01-15 08:30:00.123"},
		{5, "2026-01-15", "2026-01-15 08:30:00.000125", "2026-01-15 08:30:00", "2026-01-15 08:30:00.123"},
		{6, "2026-01-15", "2026-01-15 08:30:00.000126", "2026-01-15 08:30:00", "2026-01-15 08:30:00.123"},
		{7, "2026-01-15", "2026-01-15 08:30:00.000127", "2026-01-15 08:30:00", "2026-01-15 08:30:00.123"},
		{8, "", "", "", ""}, // all-NULL row: UNKNOWN→false must match the server's 0-row verdict
	}
}

// temporalMatrixPredicates are the literal-shape × operator-class cells: the
// DATE × time-bearing shapes (=, <, !=, 3VL negation, IN with a mixed list),
// the fractional-second shapes at the half-boundary (.1234565 — the flavor
// discriminator), the review-F1 double-mediated pair (.0001255/.0001265),
// 8- and 9-digit fractions, the carry boundary (.9999995), and the fsp/typmod
// cells (dt0/dt3 — pinning that a ≤6-digit literal is NOT truncated to the
// column's typmod, previously a comment-only observation). Every predicate
// compiles on every engine; the EXPECTED verdict is read from the live
// server per row, never hand-written.
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
		// Fractional seconds beyond µs (PG double-mediated rint; MySQL
		// half-up; MariaDB truncate; carry vs no-carry).
		"dt = '2026-01-15 08:30:00.1234565'",
		"dt = '2026-01-15 08:30:00.1234575'",
		"dt = '2026-01-15 08:30:00.1234567'",
		"dt >= '2026-01-15 08:30:00.1234561'",
		"dt = '2026-01-15 08:30:00.9999995'",
		"dt < '2026-01-15 08:30:00.9999995'",
		"dt IN ('2026-01-15 08:30:00.1234565', '2026-02-01 00:00:00')",
		// The review-F1 double-mediated pair: PG → .000125 / .000127 (exact
		// decimal half-even would give .000126 for both).
		"dt = '2026-01-15 08:30:00.0001255'",
		"dt = '2026-01-15 08:30:00.0001265'",
		// 8- and 9-digit fractions.
		"dt >= '2026-01-15 08:30:00.00012650'",
		"dt < '2026-01-15 08:30:00.000126501'",
		"dt = '2026-01-15 08:30:00.123456501'",
		// fsp/typmod: a ≤6-digit literal is compared at the TYPE's µs
		// resolution, never truncated to the column's declared precision —
		// and a >6-digit literal on a fsp-3 column still rounds to µs.
		"dt0 = '2026-01-15 08:30:00.5'",
		"dt0 < '2026-01-15 08:30:00.5'",
		"dt3 = '2026-01-15 08:30:00.1235'",
		"dt3 >= '2026-01-15 08:30:00.1234565'",
		// NULL-position coverage (the all-NULL row rides every cell above;
		// these two make the presence tests explicit).
		"d IS NULL",
		"dt IS NOT NULL",
		// Control cells at engine granularity (byte-identical pass-through).
		"d = '2026-01-15'",
		"dt = '2026-01-15 08:30:00.123456'",
	}
}

// runTemporalMatrix drives the matrix against one live engine: create the
// table, store the boundary rows (server-coerced), READ THE STORED VALUES
// BACK, then per (predicate × row) assert the server's own WHERE verdict
// (the verdict SQL, interpolating the trusted constant predicates above)
// equals the shipped Compile(...).Eval(...) classification under the
// engine's real resolver, evaluated on the read-back row.
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
	nullable := func(s string) any {
		if s == "" {
			return nil
		}
		return s
	}
	for _, r := range rows {
		if _, err := db.ExecContext(ctx, insert,
			r.id, nullable(r.d), nullable(r.dt), nullable(r.dt0), nullable(r.dt3)); err != nil {
			t.Fatalf("insert row %d: %v", r.id, err)
		}
	}

	// Read the STORED values back — the server's own coercion (incl. the
	// typmod/fsp rounding of dt0/dt3 at insert) is the row the client
	// evaluates, per the value contract (temporal → UTC time.Time; NULL →
	// nil).
	clientRows := make(map[int]ir.Row, len(rows))
	for _, r := range rows {
		var d, dt, dt0, dt3 sql.NullTime
		if err := db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT d, dt, dt0, dt3 FROM t_temporal WHERE id = %d", r.id)).Scan(&d, &dt, &dt0, &dt3); err != nil {
			t.Fatalf("read back row %d: %v", r.id, err)
		}
		row := ir.Row{"id": int64(r.id)}
		for name, v := range map[string]sql.NullTime{"d": d, "dt": dt, "dt0": dt0, "dt3": dt3} {
			if v.Valid {
				row[name] = v.Time.UTC()
			} else {
				row[name] = nil
			}
		}
		clientRows[r.id] = row
	}

	infos := ColumnInfosFromIR(resolver, []*ir.Column{
		{Name: "d", Type: ir.Date{}},
		{Name: "dt", Type: ir.DateTime{Precision: 6}},
		{Name: "dt0", Type: ir.DateTime{Precision: 0}},
		{Name: "dt3", Type: ir.DateTime{Precision: 3}},
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
			client := p.Eval(clientRows[r.id])
			if server != client {
				t.Errorf("DIVERGENCE on %q row %d (%v): server=%v client=%v — the client evaluator does not reproduce this engine's temporal-literal coercion",
					pred, r.id, clientRows[r.id], server, client)
			}
		}
	}
}

// TestRealPostgres_TemporalLiteralMatrix: PG casts the literal to the
// column's type — DATE truncates the time-of-day, timestamps round
// fractional seconds by the double-mediated rint(strtod·10⁶) rule
// (observed 16.14, re-proven live here).
func TestRealPostgres_TemporalLiteralMatrix(t *testing.T) {
	db, version, cleanup := startRealPostgres(t)
	defer cleanup()
	t.Logf("real Postgres server version: %s", version)

	runTemporalMatrix(
		t, db, testPGResolver,
		"CREATE TABLE t_temporal (id int PRIMARY KEY, d date, dt timestamp, dt0 timestamp(0), dt3 timestamp(3))",
		"INSERT INTO t_temporal (id, d, dt, dt0, dt3) VALUES ($1, $2::date, $3::timestamp, $4::timestamp(0), $5::timestamp(3))",
		func(pred string, id int) string {
			return fmt.Sprintf("SELECT count(*) FROM t_temporal WHERE id = %d AND (%s)", id, pred)
		},
	)
}

// TestRealPostgres_TemporalFractionSweep gates the fraction CLASS, not
// representatives (review F1): N randomized 7–9-digit fractions per run —
// half of them forced onto the exact-decimal half boundary (a trailing '5'
// at digit 7), where PG's double-mediated rint and exact decimal half-even
// disagree ~50% of the time — each asserted server-vs-client: the client's
// normalized literal must equal the server's own cast of the SAME literal,
// to the microsecond, via eq on the cast value and its ±1µs neighbors. The
// seed is logged so any failure replays.
func TestRealPostgres_TemporalFractionSweep(t *testing.T) {
	db, version, cleanup := startRealPostgres(t)
	defer cleanup()
	t.Logf("real Postgres server version: %s", version)

	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("sweep seed: %d", seed)

	infos := ColumnInfosFromIR(testPGResolver, []*ir.Column{{Name: "dt", Type: ir.DateTime{}}}, false)
	ctx := context.Background()

	const n = 300
	for i := 0; i < n; i++ {
		var digits []byte
		if i%2 == 0 {
			// Forced half-boundary: 6 random digits + '5' — the class where
			// the double mediation decides the direction.
			for j := 0; j < 6; j++ {
				digits = append(digits, byte('0'+rng.Intn(10)))
			}
			digits = append(digits, '5')
		} else {
			nd := 7 + rng.Intn(3) // 7..9 digits
			for j := 0; j < nd; j++ {
				digits = append(digits, byte('0'+rng.Intn(10)))
			}
		}
		lit := "2026-01-15 08:30:00." + string(digits)

		var serverCast time.Time
		if err := db.QueryRowContext(ctx, "SELECT ($1 || $2)::timestamp", "2026-01-15 08:30:00.", string(digits)).Scan(&serverCast); err != nil {
			t.Fatalf("server cast of %q: %v", lit, err)
		}
		serverCast = serverCast.UTC()

		p, err := Compile("t", fmt.Sprintf("dt = '%s'", lit), infos)
		if err != nil {
			t.Fatalf("Compile(%q): %v", lit, err)
		}
		if !p.Eval(ir.Row{"dt": serverCast}) {
			t.Fatalf("seed %d: client-normalized %q does not match the server's own cast %s — the double-mediated rounding diverged", seed, lit, serverCast.Format("2006-01-02 15:04:05.000000"))
		}
		if p.Eval(ir.Row{"dt": serverCast.Add(time.Microsecond)}) {
			t.Fatalf("seed %d: %q matches the server cast +1µs — the client literal is off by one microsecond", seed, lit)
		}
		if p.Eval(ir.Row{"dt": serverCast.Add(-time.Microsecond)}) {
			t.Fatalf("seed %d: %q matches the server cast -1µs — the client literal is off by one microsecond", seed, lit)
		}
	}
}

// TestRealMySQL_TemporalLiteralMatrix: MySQL promotes the DATE column to
// datetime and rounds fractional seconds half-up on the exact digits
// (observed 8.0.46, re-proven live here).
func TestRealMySQL_TemporalLiteralMatrix(t *testing.T) {
	db, version, cleanup := startRealMySQL(t)
	defer cleanup()
	t.Logf("real MySQL server version: %s", version)

	runTemporalMatrix(
		t, db, testMySQLResolver,
		"CREATE TABLE t_temporal (id INT PRIMARY KEY, d DATE, dt DATETIME(6), dt0 DATETIME, dt3 DATETIME(3))",
		"INSERT INTO t_temporal (id, d, dt, dt0, dt3) VALUES (?, ?, ?, ?, ?)",
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
		"CREATE TABLE t_temporal (id INT PRIMARY KEY, d DATE, dt DATETIME(6), dt0 DATETIME, dt3 DATETIME(3))",
		"INSERT INTO t_temporal (id, d, dt, dt0, dt3) VALUES (?, ?, ?, ?, ?)",
		func(pred string, id int) string {
			return fmt.Sprintf("SELECT count(*) FROM t_temporal WHERE id = %d AND (%s)", id, pred)
		},
	)
}
