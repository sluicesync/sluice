//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for the empty-publication diagnostic in
// [ensurePublication]'s no-scope (tables == nil) path. A publication
// that is NOT FOR ALL TABLES and has no member tables can never emit a
// pgoutput row, so streaming from it pins the slot's confirmed_flush_lsn
// and replicates nothing — a silent stall that is painful to diagnose
// (a sluice-testing cycle hit it as a confusing leftover `sluice_pub`).
//
// ensurePublication WARNS (does not refuse) in that case: an empty
// publication legitimately occurs on this no-scope path — a reader
// reusing a publication whose tables were just dropped (DROP SCHEMA
// does not drop publications), which is exactly the shared-container
// shape the rest of the suite relies on — and the streamer's own scoped
// EnsurePublication call is what establishes scope in the normal flow.
// A hard refusal here would break those callers. This pin therefore
// asserts (a) no error on any publication shape on the no-scope path
// (a regression guard against re-introducing a hard refusal) and (b)
// the warning fires ONLY for the genuinely-empty, non-FOR-ALL-TABLES
// shape — not for FOR ALL TABLES, a scoped publication with a member,
// or (PG15+) FOR TABLES IN SCHEMA.

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestEnsurePublication_EmptyPublicationWarnsButDoesNotRefuse(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Probe whether the server has the schema-level publication catalog
	// (PG 15+) so the FOR-TABLES-IN-SCHEMA sub-case can be skipped
	// cleanly on older servers rather than failing to create.
	var hasSchemaPubs bool
	if err := db.QueryRowContext(
		ctx,
		`SELECT to_regclass('pg_catalog.pg_publication_namespace') IS NOT NULL`,
	).Scan(&hasSchemaPubs); err != nil {
		t.Fatalf("probe pg_publication_namespace: %v", err)
	}

	// warnMarker is the substring the empty-publication warning carries;
	// asserting on it pins that the diagnostic fired (or didn't).
	const warnMarker = "has no tables and is not FOR ALL TABLES"

	// callCapturingLog runs ensurePublication(name, nil) with the default
	// slog logger swapped for a buffer so the test can assert whether the
	// empty-publication warning fired. Subtests are sequential (no
	// t.Parallel), so the global swap is bounded to each call.
	callCapturingLog := func(t *testing.T, name string) (string, error) {
		t.Helper()
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
		defer slog.SetDefault(prev)
		err := ensurePublication(ctx, db, name, "public", nil)
		return buf.String(), err
	}

	t.Run("empty publication warns, does not error", func(t *testing.T) {
		const pub = "warn_empty_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub) // no FOR clause => empty, not FOR ALL TABLES
		t.Cleanup(func() { applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub) })

		logs, err := callCapturingLog(t, pub)
		if err != nil {
			t.Fatalf("empty publication must NOT hard-refuse on the no-scope path; got error: %v", err)
		}
		if !strings.Contains(logs, warnMarker) {
			t.Errorf("expected empty-publication WARN naming the silent-stall diagnostic; got logs: %s", logs)
		}
		if !strings.Contains(logs, pub) {
			t.Errorf("warning should name the publication %q; got logs: %s", pub, logs)
		}
	})

	t.Run("FOR ALL TABLES is respected without warning", func(t *testing.T) {
		const pub = "allow_alltables_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub+` FOR ALL TABLES`)
		t.Cleanup(func() { applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub) })

		logs, err := callCapturingLog(t, pub)
		if err != nil {
			t.Fatalf("FOR ALL TABLES publication should be respected (no-op); got error: %v", err)
		}
		// FOR ALL TABLES has zero pg_publication_rel rows but is NOT
		// empty — the prior bug was warning (then erroring) on it.
		if strings.Contains(logs, warnMarker) {
			t.Errorf("FOR ALL TABLES must NOT trip the empty-publication warning; got logs: %s", logs)
		}
	})

	t.Run("scoped publication with a member is respected without warning", func(t *testing.T) {
		const pub = "allow_member_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `DROP TABLE IF EXISTS pubmember`)
		applyPGSQL(t, dsn, `CREATE TABLE pubmember (id INT PRIMARY KEY)`)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub+` FOR TABLE pubmember`)
		t.Cleanup(func() {
			applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
			applyPGSQL(t, dsn, `DROP TABLE IF EXISTS pubmember`)
		})

		logs, err := callCapturingLog(t, pub)
		if err != nil {
			t.Fatalf("scoped publication with a member should be respected; got error: %v", err)
		}
		if strings.Contains(logs, warnMarker) {
			t.Errorf("a scoped publication with a member must NOT warn; got logs: %s", logs)
		}
	})

	t.Run("FOR TABLES IN SCHEMA is respected without warning (PG15+)", func(t *testing.T) {
		if !hasSchemaPubs {
			t.Skip("server predates FOR TABLES IN SCHEMA (pg_publication_namespace); nothing to assert")
		}
		const pub = "allow_schema_pub"
		applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub)
		applyPGSQL(t, dsn, `CREATE PUBLICATION `+pub+` FOR TABLES IN SCHEMA public`)
		t.Cleanup(func() { applyPGSQL(t, dsn, `DROP PUBLICATION IF EXISTS `+pub) })

		// pg_publication_rel is empty for a schema-level publication, so a
		// rel-count-only check would false-warn here. publicationIsEmpty
		// must see the pg_publication_namespace membership and stay quiet.
		logs, err := callCapturingLog(t, pub)
		if err != nil {
			t.Fatalf("FOR TABLES IN SCHEMA publication should be respected; got error: %v", err)
		}
		if strings.Contains(logs, warnMarker) {
			t.Errorf("FOR TABLES IN SCHEMA must NOT trip the empty-publication warning; got logs: %s", logs)
		}
	})
}
