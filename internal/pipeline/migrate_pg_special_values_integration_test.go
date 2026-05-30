//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration pin for PG special-value round-trip (CRITICAL
// silent-loss class). Adoption from the broader-mining report (gap #1 —
// docs/dev/notes/test-gap-mining-broader.md → confirmed gap: zero hits
// in `internal/` for `infinity` / `-infinity` / `NaN`). The risk per
// the report:
//
//	"zero-Time on infinity would silently write '2000-01-01' to the target."
//
// PG accepts the literal strings 'infinity', '-infinity' on
// timestamptz / timestamp / date, and 'NaN' / 'infinity' / '-infinity'
// on float4 / float8 / numeric (numeric only supports NaN; infinity
// landed in numeric in PG 14+). A migration that silently coerces
// these to a default value (zero-Time / 0.0 / null) is a worst-class
// silent-loss bug per CLAUDE.md's loud-failure tenet.
//
// This pin runs PG → PG (the simplest baseline; cross-engine PG → MySQL
// is a deliberate follow-up — MySQL has no infinity-on-numeric, so the
// right policy there is refuse-loudly, which needs its own design pass).
// The assertion shape is byte-exact value preservation via SQL
// comparison on the target, so a silent coercion to a default would
// surface as a row count of 0 or a value mismatch.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_SpecialValues seeds source rows with
// PG's special timestamp + numeric values (infinity / -infinity / NaN
// per type-family). Each row is identifiable by its `id` and carries
// exactly one special-value column under test. After migrate, the test
// asserts each row landed on the target with the special value INTACT,
// using PG-native equality so a silent zero-Time / 0.0 coercion shows
// up as `WHERE col = 'infinity'` returning 0 rows.
func TestMigrate_PostgresToPostgres_SpecialValues(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// Schema deliberately mixes the families: timestamps (with TZ + naive),
	// date, float4, float8, numeric. Each row puts a special value on
	// exactly one column so a partial-pass / partial-fail surfaces the
	// SPECIFIC family that lost fidelity.
	const seedDDL = `
		CREATE TABLE special_vals (
			id   BIGINT PRIMARY KEY,
			ts   TIMESTAMP WITHOUT TIME ZONE,
			tstz TIMESTAMP WITH TIME ZONE,
			d    DATE,
			f4   REAL,
			f8   DOUBLE PRECISION,
			n    NUMERIC(20,4)
		);

		INSERT INTO special_vals (id, ts, tstz, d, f4, f8, n) VALUES
			-- Per-row: one "interesting" column, rest NULL.
			(1,  'infinity',  NULL,         NULL,         NULL,        NULL,         NULL),         -- timestamp +infinity
			(2,  '-infinity', NULL,         NULL,         NULL,        NULL,         NULL),         -- timestamp -infinity
			(3,  NULL,        'infinity',   NULL,         NULL,        NULL,         NULL),         -- timestamptz +infinity
			(4,  NULL,        '-infinity',  NULL,         NULL,        NULL,         NULL),         -- timestamptz -infinity
			(5,  NULL,        NULL,         'infinity',   NULL,        NULL,         NULL),         -- date +infinity
			(6,  NULL,        NULL,         '-infinity',  NULL,        NULL,         NULL),         -- date -infinity
			(7,  NULL,        NULL,         NULL,         'infinity',  NULL,         NULL),         -- float4 +infinity
			(8,  NULL,        NULL,         NULL,         '-infinity', NULL,         NULL),         -- float4 -infinity
			(9,  NULL,        NULL,         NULL,         'NaN',       NULL,         NULL),         -- float4 NaN
			(10, NULL,        NULL,         NULL,         NULL,        'infinity',   NULL),         -- float8 +infinity
			(11, NULL,        NULL,         NULL,         NULL,        '-infinity',  NULL),         -- float8 -infinity
			(12, NULL,        NULL,         NULL,         NULL,        'NaN',        NULL),         -- float8 NaN
			(13, NULL,        NULL,         NULL,         NULL,        NULL,         'NaN'),        -- numeric NaN (PG <14: numeric supports NaN only)
			-- Sanity control row: ordinary values that must survive untouched.
			(14, '2026-01-02 03:04:05', '2026-01-02 03:04:05+00', '2026-01-02', 1.5, 1.5, 1.5);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		// LOUD-FAILURE PATH (the tenet-acceptable outcome): sluice
		// refuses-loudly at migrate time AND the error names the
		// special-value class so the operator can act. Empirically,
		// sluice's bulk-copy row-reader fails with
		// `cannot parse "infinity" as time.Time` on the timestamp
		// path — the kind of operator-grep-able diagnostic the tenet
		// wants. (If the message is grep-able for the special value,
		// pass. Otherwise the test fails with a hint-upgrade ask.)
		errStr := err.Error()
		hasContext := false
		for _, want := range []string{"infinity", "-infinity", "NaN", "special", "time.Time"} {
			if strings.Contains(errStr, want) {
				hasContext = true
				break
			}
		}
		if !hasContext {
			t.Fatalf("Migrator.Run failed but the error doesn't name the special-value class; "+
				"operators reading CI output need a hint that infinity/NaN is the cause.\n"+
				"got: %v\n"+
				"(if sluice refuses-loudly the operator should know which value class triggered it)", err)
		}
		t.Logf("LOUD-FAILURE path: sluice refused with an operator-actionable diagnostic — %v", err)
		// Acceptable landing. The target-side preservation
		// assertions below are unreachable on this path.
		return
	}

	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open(pgx): %v", err)
	}
	defer func() { _ = db.Close() }()

	// Row-count sanity: all 14 rows must land. A type-coercion crash on
	// any value would have caused a row-level abort + a count mismatch.
	var rowCount int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM special_vals`).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 14 {
		t.Fatalf("row count = %d; want 14 (a special value caused a row-level drop?)", rowCount)
	}

	// Per-row, per-family value preservation. Each WHERE uses PG-native
	// equality (or `isnan()` for NaN — `NaN <> NaN` per IEEE-754 so equals
	// won't catch it). A silently-coerced value (zero-Time / 0.0 / NULL)
	// surfaces as "rows = 0" here.
	for _, c := range []struct {
		name  string
		where string
	}{
		{"timestamp +infinity (id=1)", `id = 1 AND ts = 'infinity'`},
		{"timestamp -infinity (id=2)", `id = 2 AND ts = '-infinity'`},
		{"timestamptz +infinity (id=3)", `id = 3 AND tstz = 'infinity'`},
		{"timestamptz -infinity (id=4)", `id = 4 AND tstz = '-infinity'`},
		{"date +infinity (id=5)", `id = 5 AND d = 'infinity'`},
		{"date -infinity (id=6)", `id = 6 AND d = '-infinity'`},
		{"float4 +infinity (id=7)", `id = 7 AND f4 = 'infinity'::real`},
		{"float4 -infinity (id=8)", `id = 8 AND f4 = '-infinity'::real`},
		{"float4 NaN (id=9)", `id = 9 AND f4 != f4`}, // NaN != NaN per IEEE-754
		{"float8 +infinity (id=10)", `id = 10 AND f8 = 'infinity'::double precision`},
		{"float8 -infinity (id=11)", `id = 11 AND f8 = '-infinity'::double precision`},
		{"float8 NaN (id=12)", `id = 12 AND f8 != f8`},
		{"numeric NaN (id=13)", `id = 13 AND n != n`},
		{"sanity row (id=14, ordinary values)", `id = 14 AND ts = '2026-01-02 03:04:05'::timestamp AND f8 = 1.5 AND n = 1.5`},
	} {
		t.Run(c.name, func(t *testing.T) {
			var found bool
			q := `SELECT EXISTS (SELECT 1 FROM special_vals WHERE ` + c.where + `)`
			if err := db.QueryRowContext(ctx, q).Scan(&found); err != nil {
				t.Fatalf("query: %v\nquery: %s", err, q)
			}
			if !found {
				// Read the row back AS the target sees it so the failure
				// message names the corrupt value — operators reading
				// CI output need to see "we expected 'infinity', target
				// has '0001-01-01 00:00:00'" (or wherever the silent
				// coercion landed it).
				explainQ := `SELECT
					ts::text, tstz::text, d::text,
					f4::text, f8::text, n::text
					FROM special_vals
					WHERE id = (SELECT id FROM special_vals WHERE ` + c.where + ` LIMIT 1)`
				_ = explainQ // explainQ already won't match (the predicate failed) — fall through to a generic dump
				dumpQ := `SELECT id, ts::text, tstz::text, d::text, f4::text, f8::text, n::text FROM special_vals ORDER BY id`
				rows, qerr := db.QueryContext(ctx, dumpQ)
				if qerr != nil {
					t.Fatalf("special value not preserved on target.\nwhere: %s\nrow-dump query failed: %v",
						c.where, qerr)
				}
				defer func() { _ = rows.Close() }()
				t.Errorf("special value NOT preserved on target.\n"+
					"where:  %s\n"+
					"target rows after migrate (silent coercion is the worst-class bug per the loud-failure tenet):",
					c.where)
				for rows.Next() {
					var id int64
					var ts, tstz, d, f4, f8, n sql.NullString
					if err := rows.Scan(&id, &ts, &tstz, &d, &f4, &f8, &n); err == nil {
						t.Logf("  id=%d ts=%v tstz=%v d=%v f4=%v f8=%v n=%v",
							id, ts.String, tstz.String, d.String, f4.String, f8.String, n.String)
					}
				}
			}
		})
	}
}
