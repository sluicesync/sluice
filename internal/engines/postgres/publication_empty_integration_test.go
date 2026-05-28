//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for the empty-publication refuse-loudly guard in
// [ensurePublication]. A publication that exists but has no member
// tables (and is not FOR ALL TABLES) emits no pgoutput rows ever, so
// streaming from it pins the slot's confirmed_flush_lsn forever — the
// run exits 0 while replicating nothing. That stale-publication shape
// (e.g. a leftover from an aborted run; DROP SCHEMA does not drop
// publications) bit a sluice-testing cycle as a confusing silent CDC
// stall. The guard refuses loudly with a recovery hint instead.
//
// The pin also covers the no-false-positive cases: FOR ALL TABLES, a
// scoped FOR TABLE publication with a member, and (PG 15+) a
// FOR TABLES IN SCHEMA publication must all pass the tables==nil
// no-op path untouched.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestEnsurePublication_RefusesStaleEmptyPublication(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Probe whether the server has the schema-level publication
	// catalog (PG 15+) so the FOR-TABLES-IN-SCHEMA sub-case can be
	// skipped cleanly on older servers rather than failing to create.
	var hasSchemaPubs bool
	if err := db.QueryRowContext(
		ctx,
		`SELECT to_regclass('pg_catalog.pg_publication_namespace') IS NOT NULL`,
	).Scan(&hasSchemaPubs); err != nil {
		t.Fatalf("probe pg_publication_namespace: %v", err)
	}

	t.Run("empty publication refuses loudly", func(t *testing.T) {
		const pub = "refuse_empty_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub) // no FOR clause => empty, not FOR ALL TABLES
		t.Cleanup(func() { applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub) })

		err := ensurePublication(ctx, db, pub, "public", nil)
		if err == nil {
			t.Fatal("expected refusal for stale empty publication; got nil")
		}
		msg := err.Error()
		// The message must name the publication and hand the operator
		// a concrete recovery path — silent-stall debugging is exactly
		// what this guard exists to short-circuit.
		for _, want := range []string{pub, "no tables", "DROP PUBLICATION", "silently stall"} {
			if !strings.Contains(msg, want) {
				t.Errorf("refusal message missing %q; got: %s", want, msg)
			}
		}
	})

	t.Run("FOR ALL TABLES is respected", func(t *testing.T) {
		const pub = "allow_alltables_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub+` FOR ALL TABLES`)
		t.Cleanup(func() { applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub) })

		if err := ensurePublication(ctx, db, pub, "public", nil); err != nil {
			t.Fatalf("FOR ALL TABLES publication should be respected (no-op); got error: %v", err)
		}
	})

	t.Run("scoped publication with a member is respected", func(t *testing.T) {
		const pub = "allow_member_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `DROP TABLE IF EXISTS pubmember`)
		applyPGSQL(t, dsn, `CREATE TABLE pubmember (id INT PRIMARY KEY)`)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub+` FOR TABLE pubmember`)
		t.Cleanup(func() {
			applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
			applyPGSQL(t, dsn, `DROP TABLE IF EXISTS pubmember`)
		})

		if err := ensurePublication(ctx, db, pub, "public", nil); err != nil {
			t.Fatalf("scoped publication with a member should be respected; got error: %v", err)
		}
	})

	t.Run("FOR TABLES IN SCHEMA is respected (PG15+)", func(t *testing.T) {
		if !hasSchemaPubs {
			t.Skip("server predates FOR TABLES IN SCHEMA (pg_publication_namespace); nothing to assert")
		}
		const pub = "allow_schema_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub+` FOR TABLES IN SCHEMA public`)
		t.Cleanup(func() { applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub) })

		// pg_publication_rel is empty for a schema-level publication, so
		// a rel-count-only check would false-refuse here. The guard must
		// see the pg_publication_namespace membership and stay quiet.
		if err := ensurePublication(ctx, db, pub, "public", nil); err != nil {
			t.Fatalf("FOR TABLES IN SCHEMA publication should be respected; got error: %v", err)
		}
	})
}
