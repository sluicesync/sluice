//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

// Audit 2026-07-18 F-T1 / M0.1 — the GROUND-TRUTH gate for the collation
// comparator. The shipped unit test (collation_test.go) verifies Vitess's
// evalengine against hand-written booleans, i.e. Vitess against itself
// (writer-verifies-writer) — which is exactly why the F0-1/F0-2 PAD-SPACE
// Critical shipped green. This test boots a REAL MySQL 8.0 and, for a matrix
// of collations × value/literal shapes, asserts sluice's
// Compile(...).Eval(...) classification EQUALS the server's own
// `SELECT ... WHERE col = 'literal'`. If any cell diverges, the client-side
// comparator does not reproduce the source's `=` for that (collation, value,
// literal) — a silent-loss hole, reported loudly rather than adjusted away.
//
// This is the CLAUDE.md "verification must not ride the reader under test"
// discipline applied to the collation codec: the oracle is the real server,
// not the same Vitess library the code under test uses.

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// realMySQLImage is the pre-baked MySQL 8.0 image the engines-mysql shard
// pulls; reusing it keeps this test off a cold docker.io pull.
const realMySQLImage = "ghcr.io/sluicesync/sluice-mysql:8.0-prebaked"

// startRealMySQL boots a throwaway MySQL 8.0 and returns a live *sql.DB, the
// server version string, and a cleanup. Self-contained (the shared-container
// helper lives in the mysql package and is not importable here).
func startRealMySQL(t *testing.T) (db *sql.DB, version string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	const (
		user   = "root"
		pass   = "rootpw"
		testDB = "sluice_rp"
	)
	// The prebaked image has a pre-populated data dir, so the entrypoint skips
	// first-init and IGNORES MYSQL_DATABASE — we create our own DB below rather
	// than rely on WithDatabase.
	container, err := mysqltc.Run(
		ctx,
		realMySQLImage,
		mysqltc.WithDatabase("ignored_prebaked"),
		mysqltc.WithUsername(user),
		mysqltc.WithPassword(pass),
		testcontainers.WithWaitStrategyAndDeadline(
			4*time.Minute,
			wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(4*time.Minute),
		),
	)
	if err != nil {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		t.Fatalf("boot real mysql: %v", err)
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
	base := fmt.Sprintf("%s:%s@tcp(%s:%s)/", user, pass, host, port.Port())

	// Connect with no database first (auth-only), wait for readiness, then
	// create our test DB — the prebaked image ignores MYSQL_DATABASE.
	boot, err := sql.Open("mysql", base+"?parseTime=true&charset=utf8mb4")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	var lastPing error
	for {
		if lastPing = boot.PingContext(ctx); lastPing == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = boot.Close()
			terminate()
			t.Fatalf("mysql never became ready (last ping error: %v)", lastPing)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if _, err := boot.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+testDB+" CHARACTER SET utf8mb4"); err != nil {
		_ = boot.Close()
		terminate()
		t.Fatalf("create test db: %v", err)
	}
	_ = boot.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	_ = boot.Close()

	conn, err := sql.Open("mysql", base+testDB+"?parseTime=true&charset=utf8mb4")
	if err != nil {
		terminate()
		t.Fatalf("open test db: %v", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		terminate()
		t.Fatalf("ping test db: %v", err)
	}
	return conn, version, func() {
		_ = conn.Close()
		terminate()
	}
}

// TestRealMySQL_CollationMatrix is the family×shape ground-truth gate.
func TestRealMySQL_CollationMatrix(t *testing.T) {
	db, version, cleanup := startRealMySQL(t)
	defer cleanup()
	t.Logf("real MySQL server version: %s", version)

	ctx := context.Background()

	// Each collation is a column of its own charset. All five are UTF-8 family
	// (utf8mb4 / utf8mb3), so the F0-6 fix ALLOWS them — the matrix proves the
	// PAD-SPACE (F0-1/F0-2), case, accent, and expansion axes reproduce the
	// server exactly.
	collations := []struct {
		name    string
		charset string
	}{
		{"utf8mb4_general_ci", "utf8mb4"}, // PAD SPACE, case/accent-insensitive
		{"utf8mb4_0900_ai_ci", "utf8mb4"}, // NO PAD, case/accent-insensitive
		{"utf8mb4_0900_as_cs", "utf8mb4"}, // NO PAD, case+accent-sensitive
		{"utf8mb4_bin", "utf8mb4"},        // PAD SPACE, byte-exact
		{"utf8_general_ci", "utf8mb3"},    // PAD SPACE, ci, non-utf8mb4 (utf8mb3)
	}

	// The nasty stored values: trailing/leading/internal space, case variants,
	// accent, expansion (ß/ss), and a distinct value.
	//
	// A1 (audit 2026-07-19): the last four stored values are the UCA
	// canonical-equivalence + ignorable shapes that separate a GENUINE byte-exact
	// collation (utf8mb4_bin) from a UCA one (utf8mb4_0900_as_cs). Under
	// _0900_as_cs MySQL's `=` treats an NFC/NFD pair and a soft-hyphen-bearing
	// value as EQUAL (canonical equivalence / UCA-ignorable weight); a
	// client-side byte compare does NOT. The NFC/NFD café bytes are raw UTF-8 in
	// this source (NFC c3a9 vs NFD 65cc81); the guard just below fails loudly if a
	// tool ever normalizes the file and collapses them. Routing _as_cs to
	// byte-exact (HEAD) diverges here; _bin (real memcmp) agrees; ci/ai go through
	// the Vitess fold.
	stored := []string{
		"EU", "EU ", "EU  ", " EU", "E U",
		"eu", "Eu", "US",
		"café", "cafe",
		"ß", "ss", "STRASSE", "strasse",
		"café",      // NFC: 'caf' + U+00E9 (precomposed e-acute)
		"café",     // NFD: 'cafe' + U+0301 (combining acute); UCA-equal to NFC
		"ab", "a­b", // U+00AD soft hyphen (UCA-ignorable); UCA-equal to "ab"
	}
	literals := []string{
		"EU", "EU ", "eu",
		"café", "cafe",
		"ss", "ß", "strasse",
		"US", " EU",
		"café", // matches BOTH NFC+NFD stored under _0900_as_cs (UCA); only NFC byte-exact
		"ab",   // matches BOTH "ab"+"a­b" stored under _0900_as_cs; only "ab" byte-exact
	}

	// Robustness guard for the A1 shapes: these exact byte sequences MUST survive
	// in the source, or the _as_cs divergence goes untested. Constructed from
	// explicit bytes so a source normalization that collapsed NFC/NFD (or
	// stripped the soft hyphen) is caught loudly rather than silently passing.
	nfcCafe := string([]byte{'c', 'a', 'f', 0xc3, 0xa9})      // café NFC
	nfdCafe := string([]byte{'c', 'a', 'f', 'e', 0xcc, 0x81}) // café NFD
	softHyphen := "a" + string([]byte{0xc2, 0xad}) + "b"      // a<U+00AD>b
	if nfcCafe == nfdCafe || !slices.Contains(stored, nfcCafe) ||
		!slices.Contains(stored, nfdCafe) || !slices.Contains(stored, softHyphen) {
		t.Fatalf("A1 shape values missing/collapsed by source normalization: nfc=%x nfd=%x soft=%x",
			nfcCafe, nfdCafe, softHyphen)
	}

	for _, coll := range collations {
		t.Run(coll.name, func(t *testing.T) {
			infos := ColumnInfosFromIR(testMySQLResolver, []*ir.Column{{Name: "v", Type: ir.Varchar{Collation: coll.name}}}, false)

			tbl := "t_" + coll.name // collation names are valid identifier chars
			mustExecCM(t, ctx, db, "DROP TABLE IF EXISTS "+tbl)
			mustExecCM(t, ctx, db, fmt.Sprintf(
				"CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(64) CHARACTER SET %s COLLATE %s)",
				tbl, coll.charset, coll.name,
			))
			for i, s := range stored {
				if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, v) VALUES (?, ?)", tbl), i, s); err != nil {
					t.Fatalf("insert %q into %s: %v", s, coll.name, err)
				}
			}

			for _, lit := range literals {
				// Ground truth: the set of stored ids the server matches with
				// its own collation-aware `=`.
				mysqlMatch := map[int]bool{}
				rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id FROM %s WHERE v = ?", tbl), lit)
				if err != nil {
					t.Fatalf("SELECT WHERE v=%q on %s: %v", lit, coll.name, err)
				}
				for rows.Next() {
					var id int
					if err := rows.Scan(&id); err != nil {
						_ = rows.Close()
						t.Fatalf("scan: %v", err)
					}
					mysqlMatch[id] = true
				}
				closeErr := rows.Err()
				_ = rows.Close()
				if closeErr != nil {
					t.Fatalf("rows err: %v", closeErr)
				}

				// sluice: compile `v = 'lit'` (all five collations are allowed
				// by the fix, so Compile must NOT refuse), then classify every
				// stored value and compare to the server verdict.
				predText := "v = '" + doubleQuotes(lit) + "'"
				p, err := Compile("t", predText, infos)
				if err != nil {
					t.Fatalf("Compile(%q) on %s refused unexpectedly (the fix should ALLOW this UTF-8 collation): %v",
						predText, coll.name, err)
				}
				for i, s := range stored {
					want := mysqlMatch[i]
					got := p.Eval(ir.Row{"v": s})
					if got != want {
						t.Errorf("DIVERGENCE collation=%s stored=%q literal=%q: sluice.Eval=%v, MySQL WHERE=%v",
							coll.name, s, lit, got, want)
					}
				}
			}
		})
	}

	// F0-6 refusal fence: a non-UTF-8 charset collation must REFUSE at compile
	// (sluice's UTF-8 row bytes would be mis-decoded under latin1), not compare.
	t.Run("latin1_swedish_ci refuses at compile (F0-6 fence)", func(t *testing.T) {
		infos := ColumnInfosFromIR(testMySQLResolver, []*ir.Column{{Name: "v", Type: ir.Varchar{Collation: "latin1_swedish_ci"}}}, false)
		_, err := Compile("t", "v = 'x'", infos)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeWhereCDCUnsupportedPredicate {
			t.Fatalf("latin1_swedish_ci: want coded refusal %s, got %v", sluicecode.CodeWhereCDCUnsupportedPredicate, err)
		}
	})
}

func mustExecCM(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// doubleQuotes escapes a value for embedding inside a single-quoted SQL/grammar
// string literal (SQL doubles a quote to escape it). None of the matrix
// literals contain a quote, but this keeps the predicate text well-formed.
func doubleQuotes(s string) string {
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
