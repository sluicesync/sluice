//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 149b against real Postgres: setup creates its bookkeeping
// tables with CREATE TABLE IF NOT EXISTS, so before this a pre-existing
// relation at one of those names was ADOPTED — setup returned success, the
// capture triggers went in, and the first change failed on the operator's own
// write path.
//
// The oracle in every assertion is the SERVER'S OWN CATALOG (pg_trigger /
// pg_class), not sluice's return value: a refusal that had already installed
// the triggers would satisfy a test that only inspected the error.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestSetup_RefusesAForeignRelationAtTheChangeLogName is the repro-then-refuse
// pin, plus the over-refusal guard (a healthy re-setup must stay silent) and
// the upgrade guard (an install written by the ORIGINAL release must still be
// accepted).
func TestSetup_RefusesAForeignRelationAtTheChangeLogName(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT);
-- A perfectly ordinary user table that happens to carry the name. It even has
-- an `+"`id`"+` column, so nothing short of a shape probe distinguishes it.
CREATE TABLE sluice_change_log (id BIGSERIAL PRIMARY KEY, note TEXT);
INSERT INTO sluice_change_log (note) VALUES ('the operator''s own audit trail');
`)

	_, err := Setup(context.Background(), dsn, SetupOptions{Tables: []string{"t"}})
	if err == nil {
		t.Fatal("setup ADOPTED a foreign relation at the change-log name; want a loud refusal")
	}
	for _, want := range []string{"refuse-loudly", "sluice_change_log", "txid", "pk_jsonb"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal should name %q; got: %v", want, err)
		}
	}

	// Catalog oracle: no capture trigger anywhere, no meta table, the user's
	// table and its row untouched.
	if n := countRows(t, dsn,
		`SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname LIKE 'sluice_capture%'`); n != 0 {
		t.Errorf("refused setup installed %d capture trigger(s)", n)
	}
	for _, unwanted := range []string{ChangeLogMetaTable, ChangeLogConsumersTable} {
		if n := countRows(t, dsn,
			`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			  WHERE n.nspname = 'public' AND c.relname = '`+unwanted+`'`); n != 0 {
			t.Errorf("refused setup created %q", unwanted)
		}
	}
	if n := countRows(t, dsn, `SELECT count(*) FROM public.sluice_change_log`); n != 1 {
		t.Errorf("the user's row count = %d; want 1", n)
	}

	// Now clear the collision and prove the SAME source proceeds — the refusal
	// is about the shape, not about the name existing.
	applyPGSQL(t, dsn, `ALTER TABLE sluice_change_log RENAME TO operator_audit_trail`)
	if _, err := Setup(context.Background(), dsn, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("setup after resolving the collision: %v", err)
	}
	if n := countRows(t, dsn,
		`SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname LIKE 'sluice_capture%'`); n == 0 {
		t.Error("setup succeeded but installed no capture trigger")
	}

	// Over-refusal guard: setup is idempotent, so a re-run finds all of its own
	// tables present and must accept every one of them.
	if _, err := Setup(context.Background(), dsn, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("healthy re-setup refused: %v", err)
	}
}

// TestSetup_AcceptsAnInstallWrittenByTheOriginalRelease pins the compatibility
// direction with a fixture that is what an OLDER binary actually wrote —
// recovered verbatim from this engine's DDL history, including the two indexes
// the N-16 diet later dropped and WITHOUT the consumer registry that arrived at
// schema_version 2. A floor derived from today's render would have refused it.
func TestSetup_AcceptsAnInstallWrittenByTheOriginalRelease(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT);

CREATE TABLE sluice_change_log (
    id            BIGSERIAL PRIMARY KEY,
    txid          BIGINT NOT NULL,
    committed_at  TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    schema_name   TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    op            CHAR(1) NOT NULL,
    pk_jsonb      JSONB NOT NULL,
    before_jsonb  JSONB,
    after_jsonb   JSONB
);
CREATE INDEX sluice_change_log_id_idx ON sluice_change_log (id);
CREATE INDEX sluice_change_log_table_idx ON sluice_change_log (schema_name, table_name, id);

CREATE TABLE sluice_change_log_meta (
    singleton_pk   BOOLEAN PRIMARY KEY DEFAULT TRUE,
    schema_version INT NOT NULL,
    installed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sluice_change_log_meta_singleton CHECK (singleton_pk = TRUE)
);
INSERT INTO sluice_change_log_meta (singleton_pk, schema_version) VALUES (TRUE, 1);
`)

	if _, err := Setup(context.Background(), dsn, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("setup refused a legitimate pre-N-16, schema_version=1 install: %v", err)
	}
	if n := countRows(t, dsn,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE n.nspname = 'public' AND c.relname = '`+ChangeLogConsumersTable+`'`); n != 1 {
		t.Error("the v1→v2 migration did not create the consumer registry")
	}
}

// countRows runs a single-value COUNT query against dsn.
func countRows(t *testing.T, dsn, q string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", q, err)
	}
	return n
}
