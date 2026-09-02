// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

// SEC-CRIT-1 (audit 2026-09-01): the SEC-1 search_path class, one layer
// out. SEC-1 (v0.134.1) pinned search_path inside the SECURITY DEFINER
// capture functions. The SQL sluice ITSELF sends on its client connection
// — typically a superuser's, with the default `"$user", public` path — was
// never swept, and PostgreSQL resolves an unqualified call by BEST TYPE
// MATCH across every schema on the path before it considers schema order.
// So an unprivileged role that can CREATE in `public` plants an
// exact-typed overload of a polymorphic or implicitly-cast built-in
// (`to_jsonb(anyelement)`, `array_to_json(anyarray)`, `quote_ident(text)`
// over a `name`), and the next CDC open runs it as the connecting
// superuser. Reproduced to `ALTER ROLE … SUPERUSER` on real PG 16 through
// readInstallMeta's `to_jsonb(m)`.
//
// The durable fix is TestPGClientSQLQualifiesEveryCatalogFunction
// (internal/engines), which requires every pg_catalog function the two
// Postgres engine packages spell to be `pg_catalog.`-qualified. This test
// is that gate's anti-vacuity half on a real server: it proves (1) the
// decoys CAN fire — an unqualified call from the same superuser connection
// hits them — and (2) none of the paths the gate covers fires them once
// qualified. Reach, stated so the name cannot be read as broader than the
// truth: `trigger setup` (loadTableShape's `to_jsonb(array_agg(…))`), the
// pgtrigger CDC open (readInstallMeta, the capture-shape and capture-body
// doors, the echo-loop probe), and the logical-replication engine's schema
// read (`array_to_json`, `unnest`, `pg_get_expr`, `quote_ident` …). The
// postgres engine's replica-identity preflight and applier paths run the
// same qualified SQL but are not driven here (they need wal_level=logical
// and a target); the source-text gate covers them by construction.
//
// Each decoy RAISEs rather than escalating, so a hijack is a loud test
// failure naming the function that was resolved to the decoy.
func TestClientSQL_DecoyOverloadsInPublicNeverFire(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	applyPGSQL(t, dsn, `
CREATE TABLE decoy_t (id BIGINT PRIMARY KEY, tags TEXT[], note TEXT DEFAULT 'x', CHECK (id > 0));
CREATE INDEX decoy_t_note_idx ON decoy_t (lower(note));
CREATE ROLE lowpriv LOGIN PASSWORD 'lowpriv';
GRANT CREATE ON SCHEMA public TO lowpriv;
`)
	// The meta table's rowtype must exist before a decoy can be typed on
	// it, so set up FIRST on a clean schema; the decoys then sit in front
	// of every later open and re-setup.
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"decoy_t"}}); err != nil {
		t.Fatalf("initial Setup: %v", err)
	}

	// Exact-typed overloads for every built-in family the two engines call
	// with a non-exact signature. Naming a rowtype in a signature needs no
	// privilege on the table itself.
	decoys := []string{
		`CREATE FUNCTION public.to_jsonb(public.sluice_change_log_meta) RETURNS jsonb LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED to_jsonb(meta)'; END $$`,
		`CREATE FUNCTION public.to_jsonb(text[]) RETURNS jsonb LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED to_jsonb(text[])'; END $$`,
		`CREATE FUNCTION public.to_jsonb(name[]) RETURNS jsonb LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED to_jsonb(name[])'; END $$`,
		`CREATE FUNCTION public.array_to_json(text[]) RETURNS json LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED array_to_json(text[])'; END $$`,
		`CREATE FUNCTION public.array_to_json(name[]) RETURNS json LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED array_to_json(name[])'; END $$`,
		`CREATE FUNCTION public.array_to_json(int2[]) RETURNS json LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED array_to_json(int2[])'; END $$`,
		`CREATE FUNCTION public.quote_ident(name) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED quote_ident(name)'; END $$`,
		`CREATE FUNCTION public.unnest(text[]) RETURNS SETOF text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED unnest(text[])'; END $$`,
		`CREATE FUNCTION public.unnest(name[]) RETURNS SETOF name LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED unnest(name[])'; END $$`,
		`CREATE FUNCTION public.unnest(int2[]) RETURNS SETOF int2 LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED unnest(int2[])'; END $$`,
		`CREATE FUNCTION public.array_position(int2[], int2) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED array_position(int2[],int2)'; END $$`,
		`CREATE FUNCTION public.array_length(int2[], int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED array_length(int2[],int)'; END $$`,
		`CREATE FUNCTION public.format(text, name) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED format(text,name)'; END $$`,
		`CREATE FUNCTION public.format(text, text) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED format(text,text)'; END $$`,
		`CREATE FUNCTION public.count(name) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'HIJACKED count(name)'; END $$`,
	}
	for _, d := range decoys {
		if err := applyPGSQLAs(t, dsn, "lowpriv", "lowpriv", d); err != nil {
			t.Fatalf("plant decoy as lowpriv: %v\n%s", err, d)
		}
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Anti-vacuity: the trap is armed. The SAME superuser connection,
	// spelling the call the way readInstallMeta did before the fix, must
	// resolve to the decoy. A green run below without this red is exactly
	// the vacuous pass this subtest exists to rule out.
	t.Run("armed: an UNQUALIFIED call from the superuser connection hits the decoy", func(t *testing.T) {
		var v string
		err := db.QueryRowContext(ctx, `SELECT to_jsonb(m)::text FROM public.sluice_change_log_meta m WHERE m.singleton_pk`).Scan(&v)
		if err == nil || !strings.Contains(err.Error(), "HIJACKED to_jsonb(meta)") {
			t.Fatalf("the decoy did NOT capture an unqualified to_jsonb(m) (err=%v, v=%q) — PostgreSQL resolution "+
				"did not prefer the exact-typed overload, so this test proves nothing about the fix", err, v)
		}
		err = db.QueryRowContext(ctx, `SELECT quote_ident(relname) FROM pg_catalog.pg_class WHERE relname = 'decoy_t'`).Scan(&v)
		if err == nil || !strings.Contains(err.Error(), "HIJACKED quote_ident(name)") {
			t.Fatalf("the implicit-cast decoy did NOT capture an unqualified quote_ident(name) (err=%v) — the class is "+
				"wider than polymorphic built-ins and this arm proves that half", err)
		}
	})

	t.Run("trigger setup re-run never resolves to a decoy", func(t *testing.T) {
		if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"decoy_t"}}); err != nil {
			t.Fatalf("Setup with decoys planted: %v", err)
		}
	})

	t.Run("pgtrigger CDC open never resolves to a decoy", func(t *testing.T) {
		r, err := openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("openCDCReader with decoys planted: %v", err)
		}
		_ = r.(*CDCReader).Close()
	})

	t.Run("postgres engine schema read never resolves to a decoy", func(t *testing.T) {
		eng, ok := engines.Get("postgres")
		if !ok {
			t.Fatal("engines.Get(postgres): not registered")
		}
		sr, err := eng.OpenSchemaReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenSchemaReader with decoys planted: %v", err)
		}
		schema, err := sr.ReadSchema(ctx)
		if err != nil {
			t.Fatalf("ReadSchema with decoys planted: %v", err)
		}
		found := false
		for _, tbl := range schema.Tables {
			if tbl.Name == "decoy_t" {
				found = true
			}
		}
		if !found {
			t.Fatalf("schema read did not return decoy_t — the read did not exercise the catalog paths this arm covers")
		}
	})

	// Every failure above would carry a HIJACKED marker; make the absence
	// explicit in the log so a reader of a green run knows what was proven.
	t.Logf("%d decoys planted in public by lowpriv; setup, CDC open and schema read all resolved to pg_catalog", len(decoys))
}
