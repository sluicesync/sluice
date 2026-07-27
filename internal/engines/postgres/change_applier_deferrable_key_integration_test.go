//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 211 — the DEFERRABLE-key arbiter gate, against a real Postgres.
//
// v0.103.1 made a Postgres target CARRY a source's `PRIMARY KEY …
// DEFERRABLE`. That was correct (an immediate target key is STRICTER
// than the source's, and aborts bulk key shifts that commit on the
// source) and it made the CDC apply path illegal: `INSERT … ON CONFLICT
// (<pk>) DO UPDATE` needs an arbiter, and PG refuses a deferrable
// constraint as one — SQLSTATE 55000, terminally, with zero retries, and
// every warm resume wedged on the same change while the rest of the
// stream stalled behind it.
//
// The shape matrix below is the point. The obvious filter — "skip a
// constraint declared INITIALLY DEFERRED" — is WRONG: PG clears
// pg_index.indimmediate for `DEFERRABLE INITIALLY IMMEDIATE` too, and
// refuses it as an arbiter identically (ground-truthed on PG 16.14
// before this was written). And the exclusion must not turn ordinary
// tables — immediate PKs, truly-keyless tables — into refusals, which is
// the regression risk of the fix. So every cell runs: both deferrable
// initial modes, the PK and the non-PK UNIQUE branch, the
// deferrable-plus-usable-alternative cases that must SUCCEED, and the
// keyless case that must still take its plain-INSERT path.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// applyDeferrableEvents opens an applier against dsn and pushes events
// through it, RETURNING Apply's error instead of failing the test —
// these cases are about which error comes back, not whether one does.
func applyDeferrableEvents(t *testing.T, dsn string, events []ir.Change) error {
	t.Helper()
	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	return applier.Apply(ctx, testStreamID, ch)
}

// pgScalarInt runs a single-value integer query against dsn.
func pgScalarInt(t *testing.T, dsn, query string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// openDeferrableApplier opens an applier and its preflight surface,
// failing loudly if the optional surface is not implemented — an
// unimplemented preflight is an INERT gate, which is the failure mode
// this whole file exists to prevent.
func openDeferrableApplier(t *testing.T, ctx context.Context, dsn string) (ir.ChangeApplier, ir.UpsertKeyPreflighter) {
	t.Helper()
	applier, err := (Engine{}).OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	pf, ok := applier.(ir.UpsertKeyPreflighter)
	if !ok {
		t.Fatal("the Postgres applier does not implement ir.UpsertKeyPreflighter — the preflight is inert")
	}
	return applier, pf
}

// assertDeferrableKeyRefusal asserts err is the coded Bug-211 refusal,
// carries its remedy hint, and names the offending table.
func assertDeferrableKeyRefusal(t *testing.T, err error, table string) {
	t.Helper()
	if err == nil {
		t.Fatalf("operation on %q succeeded; expected the deferrable-key refusal", table)
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetDeferrableKey {
		t.Fatalf("failed, but not with %s.\n got: %v", sluicecode.CodeTargetDeferrableKey, err)
	}
	if coded.Hint == "" {
		t.Errorf("refusal carries no remedy hint: %v", err)
	}
	if !strings.Contains(err.Error(), table) {
		t.Errorf("refusal does not name the offending table %q: %v", table, err)
	}
}

// TestApplier_DeferrableKey_ShapeMatrix walks every target-table key
// shape the arbiter selection has to distinguish. Cases marked
// wantRefusal must produce the coded refusal; the rest must apply AND be
// idempotent on replay (the property the ON CONFLICT exists for).
func TestApplier_DeferrableKey_ShapeMatrix(t *testing.T) {
	cases := []struct {
		name        string
		ddl         string
		table       string
		row         ir.Row
		wantRefusal bool
		// wantRowsAfterReplay is 1 for every arbitrable table (the
		// replay upserts) and 2 for the keyless one, whose documented
		// pre-existing contract is a plain INSERT.
		wantRowsAfterReplay int
	}{
		{
			name:                "immediate PK — the ordinary case, must be unaffected",
			table:               "imm_pk",
			ddl:                 `CREATE TABLE imm_pk (id BIGINT PRIMARY KEY, v TEXT);`,
			row:                 ir.Row{"id": int64(1), "v": "a"},
			wantRowsAfterReplay: 1,
		},
		{
			name:        "DEFERRABLE INITIALLY DEFERRED PK, no alternative",
			table:       "def_pk",
			ddl:         `CREATE TABLE def_pk (id BIGINT, v TEXT, CONSTRAINT def_pk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);`,
			row:         ir.Row{"id": int64(1), "v": "a"},
			wantRefusal: true,
		},
		{
			// The cell a condeferred-based filter would miss: PG clears
			// indimmediate for this too and refuses it as an arbiter.
			name:        "DEFERRABLE INITIALLY IMMEDIATE PK, no alternative",
			table:       "defimm_pk",
			ddl:         `CREATE TABLE defimm_pk (id BIGINT, v TEXT, CONSTRAINT defimm_pk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY IMMEDIATE);`,
			row:         ir.Row{"id": int64(1), "v": "a"},
			wantRefusal: true,
		},
		{
			// A deferrable PK is not fatal when the table carries an
			// arbitrable key as well — sluice keys on that instead.
			name:  "DEFERRABLE PK plus an immediate NOT NULL UNIQUE",
			table: "def_pk_uk",
			ddl: `CREATE TABLE def_pk_uk (id BIGINT, v TEXT, k BIGINT NOT NULL,
			        CONSTRAINT def_pk_uk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
			        CONSTRAINT def_pk_uk_k UNIQUE (k));`,
			row:                 ir.Row{"id": int64(1), "v": "a", "k": int64(10)},
			wantRowsAfterReplay: 1,
		},
		{
			// The non-PK branch carried the identical omission.
			name:        "no PK, only a DEFERRABLE UNIQUE",
			table:       "def_uk",
			ddl:         `CREATE TABLE def_uk (id BIGINT NOT NULL, v TEXT, CONSTRAINT def_uk_k UNIQUE (id) DEFERRABLE INITIALLY DEFERRED);`,
			row:         ir.Row{"id": int64(1), "v": "a"},
			wantRefusal: true,
		},
		{
			name:  "no PK, a DEFERRABLE UNIQUE and an immediate one",
			table: "mixed_uk",
			ddl: `CREATE TABLE mixed_uk (id BIGINT NOT NULL, v TEXT, k BIGINT NOT NULL,
			        CONSTRAINT mixed_uk_def UNIQUE (id) DEFERRABLE INITIALLY DEFERRED,
			        CONSTRAINT mixed_uk_imm UNIQUE (k));`,
			row:                 ir.Row{"id": int64(1), "v": "a", "k": int64(10)},
			wantRowsAfterReplay: 1,
		},
		{
			// Truly keyless: no key of any kind, so nothing was rejected
			// for deferrability and the plain-INSERT path must survive.
			// Over-refusing here would break every keyless table.
			name:                "no PK, no UNIQUE — the keyless plain-INSERT path",
			table:               "keyless",
			ddl:                 `CREATE TABLE keyless (id BIGINT, v TEXT);`,
			row:                 ir.Row{"id": int64(1), "v": "a"},
			wantRowsAfterReplay: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForApplier(t)
			defer cleanup()
			applyPGApplier(t, dsn, tc.ddl)

			event := []ir.Change{ir.Insert{Schema: "public", Table: tc.table, Row: tc.row}}
			err := applyDeferrableEvents(t, dsn, event)

			if tc.wantRefusal {
				assertDeferrableKeyRefusal(t, err, tc.table)
				return
			}
			if err != nil {
				t.Fatalf("apply into %q failed; expected success: %v", tc.table, err)
			}
			if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM "+tc.table); got != 1 {
				t.Fatalf("row count after first apply = %d; want 1", got)
			}
			if err := applyDeferrableEvents(t, dsn, event); err != nil {
				t.Fatalf("replay into %q failed: %v", tc.table, err)
			}
			if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM "+tc.table); got != tc.wantRowsAfterReplay {
				t.Fatalf("row count after replay = %d; want %d", got, tc.wantRowsAfterReplay)
			}
		})
	}
}

// TestApplier_DeferrableKey_UpdateAndDeleteStillWork pins that the
// exclusion is scoped to ARBITER selection. UPDATE and DELETE key on the
// full before-image WHERE, not on a conflict key, so a deferrable PK is
// perfectly usable for them — refusing those too would be an
// over-refusal that breaks a table sluice can in fact serve.
func TestApplier_DeferrableKey_UpdateAndDeleteStillWork(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE def_pk (id BIGINT, v TEXT,
		  CONSTRAINT def_pk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
		INSERT INTO def_pk (id, v) VALUES (1, 'a'), (2, 'b');
	`)

	err := applyDeferrableEvents(t, dsn, []ir.Change{
		ir.Update{
			Schema: "public", Table: "def_pk",
			Before: ir.Row{"id": int64(1), "v": "a"},
			After:  ir.Row{"id": int64(1), "v": "z"},
		},
		ir.Delete{
			Schema: "public", Table: "def_pk",
			Before: ir.Row{"id": int64(2), "v": "b"},
		},
	})
	if err != nil {
		t.Fatalf("UPDATE/DELETE against a deferrable-PK table failed: %v", err)
	}
	if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM def_pk WHERE v = 'z'"); got != 1 {
		t.Fatalf("UPDATE did not land: %d rows with v='z'", got)
	}
	if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM def_pk"); got != 1 {
		t.Fatalf("DELETE did not land: %d rows remain", got)
	}
}

// TestApplier_PreflightUpsertKeys walks the preflight itself: it must
// name every offending table at once, stay silent about the ones it can
// serve, and honour the stream's table scope so a deferrable-key table
// the stream never touches is never refused over.
func TestApplier_PreflightUpsertKeys(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE plain   (id BIGINT PRIMARY KEY, v TEXT);
		CREATE TABLE dpk_a   (id BIGINT, v TEXT, CONSTRAINT dpk_a_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
		CREATE TABLE dpk_b   (id BIGINT, v TEXT, CONSTRAINT dpk_b_pk PRIMARY KEY (id) DEFERRABLE INITIALLY IMMEDIATE);
		CREATE TABLE rescued (id BIGINT, v TEXT, k BIGINT NOT NULL,
		  CONSTRAINT rescued_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
		  CONSTRAINT rescued_k UNIQUE (k));
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, pf := openDeferrableApplier(t, ctx, dsn)

	// Whole schema in scope: both unarbitrable tables must be named, and
	// the two serviceable ones must not be.
	err := pf.PreflightUpsertKeys(ctx, nil)
	assertDeferrableKeyRefusal(t, err, "dpk_a")
	if !strings.Contains(err.Error(), "dpk_b") {
		t.Errorf("preflight named only one offender; it must name every one: %v", err)
	}
	for _, clean := range []string{"plain", "rescued"} {
		if strings.Contains(err.Error(), clean) {
			t.Errorf("preflight named %q, which has a usable arbiter: %v", clean, err)
		}
	}

	// Scoped away: the stream does not touch those tables, so there is
	// nothing to refuse.
	inScope := func(table string) bool { return table == "plain" || table == "rescued" }
	if err := pf.PreflightUpsertKeys(ctx, inScope); err != nil {
		t.Fatalf("preflight refused over out-of-scope tables: %v", err)
	}
}

// TestApplier_PreflightUpsertKeys_CleanSchema is the over-refusal guard
// in its own right: an ordinary schema — including sluice's own control
// tables, which the preflight walks like any other, and shapes the
// arbiter selection ALREADY skipped for other reasons (a partial unique
// index, a nullable-column unique) — must pass silently.
func TestApplier_PreflightUpsertKeys_CleanSchema(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users    (id BIGINT PRIMARY KEY, email TEXT NOT NULL UNIQUE);
		CREATE TABLE events   (id BIGINT, payload TEXT);
		CREATE TABLE tickets  (a BIGINT NOT NULL, b BIGINT NOT NULL, CONSTRAINT tickets_ab UNIQUE (a, b));
		CREATE TABLE nullable (id BIGINT, v TEXT, CONSTRAINT nullable_id UNIQUE (id));
		CREATE TABLE partials (id BIGINT NOT NULL, v TEXT);
		CREATE UNIQUE INDEX partials_live ON partials (id) WHERE v IS NOT NULL;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, pf := openDeferrableApplier(t, ctx, dsn)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	if err := pf.PreflightUpsertKeys(ctx, nil); err != nil {
		t.Fatalf("preflight refused an ordinary schema: %v", err)
	}
}
