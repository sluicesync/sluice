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
// Two oracles because the two engines expose PAD-ness differently:
//   - MySQL publishes information_schema.COLLATIONS.PAD_ATTRIBUTE, so we assert
//     collationNoPad(name) == (PAD_ATTRIBUTE == "NO PAD") for EVERY collation.
//   - MariaDB's PAD_ATTRIBUTE column is version-dependent (absent through the
//     11.x LTS line + 12.0, added in 12.1), so it is not a version-robust oracle;
//     we ground-truth BEHAVIORALLY across the whole LTS spread instead: a stored
//     'EU ' (trailing space) matches `= 'EU'` under PAD SPACE and does not under
//     NO PAD, and collationNoPad must agree with what the server does.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

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

	rows, err := db.QueryContext(ctx, "SELECT COLLATION_NAME, PAD_ATTRIBUTE FROM information_schema.COLLATIONS")
	if err != nil {
		t.Fatalf("query information_schema.COLLATIONS: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var checked, noPadSeen int
	for rows.Next() {
		var name, pad string
		if err := rows.Scan(&name, &pad); err != nil {
			t.Fatalf("scan: %v", err)
		}
		checked++
		wantNoPad := pad == "NO PAD"
		if wantNoPad {
			noPadSeen++
		}
		if got := collationNoPad(name); got != wantNoPad {
			t.Errorf("collationNoPad(%q) = %v; server PAD_ATTRIBUTE=%q wants %v (name heuristic diverged from the catalog — SL-COLL-1 class)", name, got, pad, wantNoPad)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	// Vacuous-parity guard: a query that returned nothing, or a server with no
	// NO-PAD collations, would pass every assertion by running none.
	if checked < 100 {
		t.Fatalf("only %d collations checked; expected >100 on real MySQL 8", checked)
	}
	if noPadSeen == 0 {
		t.Fatalf("no NO-PAD collations seen in the catalog; the parity check was vacuous (expected the utf8mb4_0900_* family)")
	}
	t.Logf("collationNoPad parity: %d collations checked against PAD_ATTRIBUTE (%d NO PAD), all agree", checked, noPadSeen)
}

// TestCollationPadAttribute_MariaDBLiveBehaviorParity ground-truths collationNoPad
// against real MariaDB's actual `=` behavior across the supported LTS spread. Since
// MariaDB's PAD_ATTRIBUTE column is version-dependent (absent through 11.x/12.0,
// added in 12.1), a stored trailing space is the version-robust oracle that works
// on every release. This is the test that would have caught SL-COLL-1 before release.
func TestCollationPadAttribute_MariaDBLiveBehaviorParity(t *testing.T) {
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
			// The parity check must have exercised BOTH classes on this image,
			// else it proved nothing (a NO-PAD-only or PAD-only run is vacuous for
			// the classifier). utf8mb4_general_ci / utf8mb4_bin (PAD) and
			// utf8mb4_nopad_bin (NO PAD) are standard on every supported LTS line.
			if ranNoPad == 0 || ranPad == 0 {
				t.Fatalf("%s: vacuous parity run — NO-PAD cases ran=%d, PAD-SPACE cases ran=%d; both must be >0", image, ranNoPad, ranPad)
			}
		})
	}
}
