//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SL-COLL-1 durable class-catcher (audit 2026-07-19 gate-proposal). The
// PAD-SPACE vs NO-PAD classification that drives filtered-sync trailing-space
// fidelity (collationNoPad) is a NAME-based heuristic: NO-PAD is the
// utf8mb4_0900_* family, the `binary` collation, and MariaDB's `*_nopad_*`
// variants. A name heuristic can silently drift from the server's real
// PAD_ATTRIBUTE — which is exactly how SL-COLL-1 shipped (MariaDB `utf8mb4_nopad_bin`
// was mis-treated PAD SPACE). These tests ground-truth the heuristic against the
// REAL server so a future collation whose NO-PAD-ness escapes the name rule fails
// CI instead of mis-padding a filtered `sync --where`.
//
// The oracle is chosen PER SERVER by whether it exposes PAD_ATTRIBUTE:
//   - Where information_schema.COLLATIONS has PAD_ATTRIBUTE (MySQL 8 always;
//     MariaDB from 12.1), assert collationNoPad(name) == (PAD_ATTRIBUTE == "NO PAD")
//     for EVERY collation the catalog knows — the strong, exhaustive oracle.
//   - Where it does not (MariaDB's 11.x LTS line + 12.0), ground-truth BEHAVIORALLY:
//     a stored 'EU ' (trailing space) matches `= 'EU'` under PAD SPACE and does not
//     under NO PAD, and collationNoPad must agree. This is the version-robust
//     fallback — the classifier itself keys off the name in EVERY version, so it is
//     validated whichever oracle a given release supports.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// assertCatalogPadParity asserts collationNoPad(name) == (PAD_ATTRIBUTE == "NO PAD")
// for every collation in information_schema.COLLATIONS, with a vacuous-run guard.
// Used wherever the server exposes the PAD_ATTRIBUTE column (MySQL 8; MariaDB 12.1+).
// label names the server in failure messages.
func assertCatalogPadParity(ctx context.Context, t *testing.T, db *sql.DB, label string, minCollations int) {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT COLLATION_NAME, PAD_ATTRIBUTE FROM information_schema.COLLATIONS")
	if err != nil {
		t.Fatalf("%s: query information_schema.COLLATIONS: %v", label, err)
	}
	defer func() { _ = rows.Close() }()

	var checked, noPadSeen int
	for rows.Next() {
		var name, pad string
		if err := rows.Scan(&name, &pad); err != nil {
			t.Fatalf("%s: scan: %v", label, err)
		}
		checked++
		wantNoPad := pad == "NO PAD"
		if wantNoPad {
			noPadSeen++
		}
		if got := collationNoPad(name); got != wantNoPad {
			t.Errorf("%s: collationNoPad(%q) = %v; server PAD_ATTRIBUTE=%q wants %v (name heuristic diverged from the catalog — SL-COLL-1 class)", label, name, got, pad, wantNoPad)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: rows: %v", label, err)
	}
	// Vacuous-parity guard: a query that returned nothing, or a server with no
	// NO-PAD collations, would pass every assertion by running none.
	if checked < minCollations {
		t.Fatalf("%s: only %d collations checked; expected >=%d (vacuous-parity guard)", label, checked, minCollations)
	}
	if noPadSeen == 0 {
		t.Fatalf("%s: no NO-PAD collations seen in the catalog; the parity check was vacuous (expected a NO-PAD family)", label)
	}
	t.Logf("%s: catalog parity — %d collations checked against PAD_ATTRIBUTE (%d NO PAD), all agree", label, checked, noPadSeen)
}

// collationsHasPadAttribute reports whether information_schema.COLLATIONS exposes a
// PAD_ATTRIBUTE column on this server (MySQL 8 always; MariaDB only from 12.1 —
// absent through the 11.x LTS line + 12.0).
func collationsHasPadAttribute(ctx context.Context, db *sql.DB) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLLATIONS' AND COLUMN_NAME='PAD_ATTRIBUTE'").Scan(&n)
	return n > 0, err
}

// TestCollationPadAttribute_MySQLLiveCatalogParity asserts collationNoPad agrees
// with real MySQL's information_schema.COLLATIONS.PAD_ATTRIBUTE for every
// collation the server knows — the strong deterministic SL-COLL-1 gate.
func TestCollationPadAttribute_MySQLLiveCatalogParity(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "collation_pad_parity")
	defer cleanup()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assertCatalogPadParity(ctx, t, db, "mysql", 100)
}

// TestCollationPadAttribute_MariaDBLiveParity ground-truths collationNoPad against
// real MariaDB across the supported LTS spread, picking the oracle by version. On
// MariaDB 12.1+ (where information_schema.COLLATIONS.PAD_ATTRIBUTE exists) it runs
// the same EXHAUSTIVE catalog assertion as the MySQL half. On the 11.x LTS line +
// 12.0 (no PAD_ATTRIBUTE column) it falls back to a behavioral probe: a stored
// 'EU ' matches `= 'EU'` under PAD SPACE and not under NO PAD. Either way the
// name-based classifier is validated against the real server — this is the test
// that would have caught SL-COLL-1 before release.
func TestCollationPadAttribute_MariaDBLiveParity(t *testing.T) {
	// Behavioral-fallback probe set (used only where the catalog column is absent).
	cases := []struct {
		collation string
		wantNoPad bool // what the server's `=` should do (verified below)
	}{
		{"utf8mb4_nopad_bin", true},   // NO PAD — the SL-COLL-1 miss
		{"utf8mb4_general_ci", false}, // PAD SPACE (legacy default)
		{"utf8mb4_bin", false},        // PAD SPACE (non-nopad binary)
		{"utf8mb4_unicode_nopad_ci", true},
	}
	for _, image := range mariadbLTSImages() {
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "collation_pad_parity")
			db, err := sql.Open("mysql", dsn+"&multiStatements=true")
			if err != nil {
				t.Fatalf("open %s: %v", image, err)
			}
			defer func() { _ = db.Close() }()

			detectCtx, detectCancel := context.WithTimeout(context.Background(), 30*time.Second)
			hasCol, err := collationsHasPadAttribute(detectCtx, db)
			detectCancel()
			if err != nil {
				t.Fatalf("%s: detect PAD_ATTRIBUTE column: %v", image, err)
			}

			// MariaDB 12.1+ exposes PAD_ATTRIBUTE — use the strong catalog oracle,
			// the identical exhaustive assertion the MySQL half runs (every MariaDB
			// collation's name-classification checked against its real pad attribute).
			if hasCol {
				t.Logf("%s: PAD_ATTRIBUTE present (12.1+) — full catalog parity", image)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				assertCatalogPadParity(ctx, t, db, image, 100)
				return
			}

			// 11.x LTS line / 12.0: no PAD_ATTRIBUTE column — behavioral probe.
			t.Logf("%s: no PAD_ATTRIBUTE column (<12.1) — behavioral probe", image)
			// Non-vacuous guard (audit 2026-07-19c J-COLL-2): count how many cases
			// of each class actually RAN, so an image where every collation is
			// unavailable (all skipped) fails instead of passing on zero checks.
			var ranNoPad, ranPad int
			for _, tc := range cases {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				// A fresh table per collation; store 'EU ' WITH a trailing space,
				// then ask whether `= 'EU'` matches it. Match ⇒ PAD SPACE; no
				// match ⇒ NO PAD.
				stmts := "DROP TABLE IF EXISTS pad_probe;" +
					"CREATE TABLE pad_probe (v VARCHAR(8) CHARACTER SET utf8mb4 COLLATE " + tc.collation + " NOT NULL);" +
					"INSERT INTO pad_probe (v) VALUES ('EU ');"
				if _, err := db.ExecContext(ctx, stmts); err != nil {
					cancel()
					// Only a genuine "Unknown collation" (errno 1273) is a legit
					// skip — any other CREATE error would silently mask a real
					// failure as a skip, re-vacuuming the gate.
					var me *gomysql.MySQLError
					if errors.As(err, &me) && me.Number == 1273 {
						t.Logf("%s: collation %q unavailable (errno 1273) — skipping", image, tc.collation)
						continue
					}
					t.Fatalf("%s: unexpected error probing collation %q (not errno-1273 unavailable): %v", image, tc.collation, err)
				}
				var matched int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pad_probe WHERE v = 'EU'").Scan(&matched); err != nil {
					cancel()
					t.Fatalf("%s: count under %q: %v", image, tc.collation, err)
				}
				cancel()

				serverNoPad := matched == 0
				if serverNoPad != tc.wantNoPad {
					t.Errorf("%s: server `=` under %q is NoPad=%v (matched=%d); expected NoPad=%v — the test's premise about this collation is wrong",
						image, tc.collation, serverNoPad, matched, tc.wantNoPad)
				}
				if got := collationNoPad(tc.collation); got != serverNoPad {
					t.Errorf("%s: collationNoPad(%q) = %v but real MariaDB `=` behaves NoPad=%v (stored 'EU ' %s '= EU') — SL-COLL-1 class regression",
						image, tc.collation, got, serverNoPad, map[bool]string{true: "did NOT match", false: "matched"}[serverNoPad])
				}
				if tc.wantNoPad {
					ranNoPad++
				} else {
					ranPad++
				}
			}
			// The behavioral parity check must have exercised BOTH classes on this
			// image, else it proved nothing (a NO-PAD-only or PAD-only run is vacuous
			// for the classifier). utf8mb4_general_ci / utf8mb4_bin (PAD) and
			// utf8mb4_nopad_bin (NO PAD) are standard on every supported LTS line.
			if ranNoPad == 0 || ranPad == 0 {
				t.Fatalf("%s: vacuous parity run — NO-PAD cases ran=%d, PAD-SPACE cases ran=%d; both must be >0", image, ranNoPad, ranPad)
			}
		})
	}
}
