//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

// Audit 2026-07-18 F0-3 / M1.1 GATE — the Postgres companion to the real-MySQL
// collation matrix. It boots a REAL Postgres and asserts, against the server's
// OWN collation-aware `=`, that sluice's client-side Compile(...).Eval(...)
// classification agrees for the collations it ADMITS, and REFUSES at compile
// the one it cannot reproduce:
//
//   - the DEFAULT collation (empty name in the IR) — byte-exact `=`, admitted;
//   - a DETERMINISTIC named collation ("C") — byte-exact `=`, admitted; and
//   - a NON-DETERMINISTIC ICU collation (CREATE COLLATION … deterministic=false)
//     — collation-aware `=` sluice cannot reproduce, so Compile must REFUSE.
//
// The determinism signal is ground-truthed from PG's OWN
// pg_collation.collisdeterministic (the same column the schema reader
// captures), not hand-asserted — so a reader that mis-read determinism, or an
// evaluator that admitted a non-deterministic collation, fails here. This is
// the CLAUDE.md "verification must not ride the reader under test" discipline:
// the oracle is the real server, not sluice's own view.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// realPGImage is the pre-baked postgres:16 image (ICU-enabled, so a
// non-deterministic collation can be created); reusing it keeps this test off
// a cold docker.io pull.
const realPGImage = "ghcr.io/sluicesync/sluice-postgres:16-prebaked"

// startRealPostgres boots a throwaway Postgres 16 and returns a live *sql.DB,
// its server version, and a cleanup.
func startRealPostgres(t *testing.T) (db *sql.DB, version string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	const bootTimeout = 4 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), bootTimeout)
	defer cancel()

	// The prebaked image has a pre-populated data dir, so the entrypoint skips
	// first-init and IGNORES POSTGRES_DB — the default "postgres" database is
	// what exists (mirrors the flatfile integration harness).
	container, err := pgtc.Run(
		ctx,
		realPGImage,
		pgtc.WithDatabase("postgres"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		testcontainers.WithWaitStrategyAndDeadline(
			bootTimeout,
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections"),
				wait.ForListeningPort("5432/tcp"),
			),
		),
	)
	if err != nil {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		t.Fatalf("boot real postgres: %v", err)
	}
	terminate := func() { _ = container.Terminate(context.Background()) }

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("postgres connection string: %v", err)
	}
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		terminate()
		t.Fatalf("ping: %v", err)
	}
	_ = conn.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	return conn, version, func() {
		_ = conn.Close()
		terminate()
	}
}

// pgCollationDeterminism reads pg_collation.collisdeterministic for a named
// collation and maps it to the IR carrier — the SAME signal the schema reader
// captures, ground-truthed here from the catalog rather than hand-asserted.
func pgCollationDeterminism(t *testing.T, ctx context.Context, db *sql.DB, name string) ir.CollationDeterminism {
	t.Helper()
	var det bool
	if err := db.QueryRowContext(ctx, "SELECT collisdeterministic FROM pg_collation WHERE collname = $1 LIMIT 1", name).Scan(&det); err != nil {
		t.Fatalf("read collisdeterministic for %q: %v", name, err)
	}
	if det {
		return ir.CollationDeterministic
	}
	return ir.CollationNonDeterministic
}

// TestRealPostgres_CollationMatrix is the PG family×shape ground-truth gate.
func TestRealPostgres_CollationMatrix(t *testing.T) {
	db, version, cleanup := startRealPostgres(t)
	defer cleanup()
	t.Logf("real Postgres server version: %s", version)

	ctx := context.Background()

	// Create a non-deterministic ICU collation (straight from the PG docs).
	const ndColl = "sluice_nd_icu"
	mustExecPG(t, ctx, db, fmt.Sprintf(
		"CREATE COLLATION %s (provider = icu, locale = 'und-u-ks-level2', deterministic = false)", ndColl,
	))

	// The stored corpus + literals: case variants, trailing space, accent.
	stored := []string{"EU", "EU ", "eu", "Eu", "US", "café", "cafe"}
	literals := []string{"EU", "eu", "café", "cafe", "US"}

	// ---- Admitted collations: Eval must EQUAL PG's own WHERE ----
	admitted := []struct {
		name   string         // "" = the database DEFAULT collation
		irType func() ir.Type // builds the IR column type
		clause string         // COLLATE clause for CREATE TABLE (empty = default)
	}{
		{
			name:   "default",
			irType: func() ir.Type { return ir.Text{} },
			clause: "",
		},
		{
			name: "C-deterministic",
			irType: func() ir.Type {
				return ir.Text{Collation: "C", Determinism: pgCollationDeterminism(t, ctx, db, "C")}
			},
			clause: `COLLATE "C"`,
		},
	}

	for _, coll := range admitted {
		t.Run(coll.name, func(t *testing.T) {
			infos := ColumnInfosFromIR("postgres", []*ir.Column{{Name: "v", Type: coll.irType()}}, false)

			tbl := "t_" + sanitizeIdent(coll.name)
			mustExecPG(t, ctx, db, "DROP TABLE IF EXISTS "+tbl)
			mustExecPG(t, ctx, db, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, v TEXT %s)", tbl, coll.clause))
			for i, s := range stored {
				if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, v) VALUES ($1, $2)", tbl), i, s); err != nil {
					t.Fatalf("insert %q: %v", s, err)
				}
			}

			for _, lit := range literals {
				// Ground truth: the ids PG matches with its own `=`.
				pgMatch := map[int]bool{}
				rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id FROM %s WHERE v = $1", tbl), lit)
				if err != nil {
					t.Fatalf("SELECT WHERE v=%q: %v", lit, err)
				}
				for rows.Next() {
					var id int
					if err := rows.Scan(&id); err != nil {
						_ = rows.Close()
						t.Fatalf("scan: %v", err)
					}
					pgMatch[id] = true
				}
				closeErr := rows.Err()
				_ = rows.Close()
				if closeErr != nil {
					t.Fatalf("rows err: %v", closeErr)
				}

				predText := "v = '" + doubleQuotesPG(lit) + "'"
				p, err := Compile("t", predText, infos)
				if err != nil {
					t.Fatalf("Compile(%q) on %s refused unexpectedly (a deterministic/default collation must be ALLOWED): %v",
						predText, coll.name, err)
				}
				for i, s := range stored {
					want := pgMatch[i]
					got := p.Eval(ir.Row{"v": s})
					if got != want {
						t.Errorf("DIVERGENCE collation=%s stored=%q literal=%q: sluice.Eval=%v, PG WHERE=%v",
							coll.name, s, lit, got, want)
					}
				}
			}
		})
	}

	// ---- Non-deterministic ICU collation: Compile must REFUSE ----
	t.Run("nondeterministic ICU refuses at compile (F0-3 fence)", func(t *testing.T) {
		det := pgCollationDeterminism(t, ctx, db, ndColl)
		if det != ir.CollationNonDeterministic {
			t.Fatalf("expected %q to read as non-deterministic from the catalog, got %v", ndColl, det)
		}
		infos := ColumnInfosFromIR("postgres", []*ir.Column{
			{Name: "v", Type: ir.Text{Collation: ndColl, Determinism: det}},
		}, false)
		_, err := Compile("t", "v = 'x'", infos)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeWhereCDCUnsupportedPredicate {
			t.Fatalf("non-deterministic ICU collation: want coded refusal %s, got %v",
				sluicecode.CodeWhereCDCUnsupportedPredicate, err)
		}

		// And PROVE the refusal is warranted: PG's `=` under this collation is
		// collation-aware, so a byte compare WOULD diverge — 'EU' matches 'eu'.
		mustExecPG(t, ctx, db, "DROP TABLE IF EXISTS t_nd")
		mustExecPG(t, ctx, db, fmt.Sprintf(`CREATE TABLE t_nd (id INT PRIMARY KEY, v TEXT COLLATE %s)`, ndColl))
		mustExecPG(t, ctx, db, "INSERT INTO t_nd VALUES (1, 'eu')")
		var n int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t_nd WHERE v = 'EU'").Scan(&n); err != nil {
			t.Fatalf("nd count: %v", err)
		}
		if n != 1 {
			t.Fatalf("expected the non-deterministic collation to match 'eu' against 'EU' (proving byte-compare would diverge); got count=%d", n)
		}
	})
}

func mustExecPG(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// doubleQuotesPG escapes a value for embedding inside a single-quoted grammar
// string literal (SQL doubles a quote to escape it).
func doubleQuotesPG(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}

// sanitizeIdent turns a subtest label into a bare SQL identifier.
func sanitizeIdent(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
