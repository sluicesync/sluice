//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SOURCE-DROPPED-UNIQUE-KEY-MERGE, the Postgres conflict-identity guard
// (unique_key_conflict_guard.go), on a real Postgres 16 through both apply
// doors: the serial Apply and the pipelined ApplyBatch.
//
// The independent expected value is the target's own rows read back after
// each run: a refusal must leave the target exactly as it was; a pass must
// leave it as the source would.

package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

func ukcPGRows(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func TestUniqueKeyConflictGuard_Postgres(t *testing.T) {
	const kdRows = "SELECT concat_ws(':', coalesce(id::text, 'NULL'), k, coalesce(v, 'NULL')) FROM kd"
	for _, batched := range []bool{false, true} {
		door := "serial"
		if batched {
			door = "batched"
		}
		t.Run(door, func(t *testing.T) {
			ins := func(pos int, id any, k string, v any) ir.Insert {
				return ir.Insert{Position: kcgPGPos(pos), Schema: "public", Table: "kd", Row: ir.Row{"id": id, "k": k, "v": v}}
			}
			t.Run("deferrable primary key: a different row refuses, the same row converges", func(t *testing.T) {
				dsn, cleanup := newSharedPGDB(t, "ukc_defpk_"+door)
				defer cleanup()
				// Postgres refuses a deferrable key as an ON CONFLICT arbiter, so
				// the applier keys on kd_k and the primary key is the identity.
				applyPGApplier(t, dsn, `CREATE TABLE kd (id int PRIMARY KEY DEFERRABLE INITIALLY IMMEDIATE, k text NOT NULL, v text);
					CREATE UNIQUE INDEX kd_k ON kd (k);
					INSERT INTO kd VALUES (1,'a','one'),(2,'b',NULL);`)
				before := ukcPGRows(t, dsn, kdRows)
				err := kcgPGApply(t, dsn, batched, ins(1, int64(99), "a", "ninetynine"))
				if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) || !strings.Contains(err.Error(), "UNIQUE-KEY-CONFLICT-OTHER-ROW") {
					t.Fatalf("err = %v; want %s", err, ir.ErrUniqueKeyConflictOtherRow)
				}
				if !strings.Contains(err.Error(), `("k")`) {
					t.Errorf("the refusal does not name the key: %v", err)
				}
				if got := ukcPGRows(t, dsn, kdRows); got != before {
					t.Fatalf("the refusal changed the target: %s -> %s", before, got)
				}
				if err := kcgPGApply(t, dsn, batched, ins(2, int64(1), "a", "one"), ins(3, int64(2), "b", "two")); err != nil {
					t.Fatalf("a re-delivery of the same rows refused: %v", err)
				}
				if got, want := ukcPGRows(t, dsn, kdRows), "1:a:one,2:b:two"; got != want {
					t.Fatalf("target %s; want %s", got, want)
				}
			})

			t.Run("no primary key, two unique indexes, the arbiter is the one the source dropped", func(t *testing.T) {
				dsn, cleanup := newSharedPGDB(t, "ukc_arb_"+door)
				defer cleanup()
				// The review's measured shape: UNIQUE(code) is the arbiter (the
				// only all-NOT-NULL index), UNIQUE(email, id) survives beside it.
				// Before the union identity, INSERT (99,'y@a','a',…) ON CONFLICT
				// (code) DO UPDATE rewrote row 1 into row 99: INSERT 0 1, row 1
				// gone, no error.
				applyPGApplier(t, dsn, `CREATE TABLE ta (id int, email text, code text NOT NULL, v text);
					CREATE UNIQUE INDEX ta_code ON ta (code); CREATE UNIQUE INDEX ta_email_id ON ta (email, id);
					INSERT INTO ta VALUES (1,'x@a','a','one');`)
				const taRows = "SELECT concat_ws(':', id::text, email, code, v) FROM ta"
				ta := func(pos int, id int64, email, code, v string) ir.Insert {
					return ir.Insert{Position: kcgPGPos(pos), Schema: "public", Table: "ta", Row: ir.Row{"id": id, "email": email, "code": code, "v": v}}
				}
				before := ukcPGRows(t, dsn, taRows)
				err := kcgPGApply(t, dsn, batched, ta(1, 99, "y@a", "a", "ninetynine"))
				if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
					t.Fatalf("err = %v; want %s", err, ir.ErrUniqueKeyConflictOtherRow)
				}
				if got := ukcPGRows(t, dsn, taRows); got != before {
					t.Fatalf("the refusal changed the target: %s -> %s", before, got)
				}
				// The same row at a newer image, agreeing on every unique index:
				// converges.
				if err := kcgPGApply(t, dsn, batched, ta(2, 1, "x@a", "a", "uno")); err != nil {
					t.Fatalf("a newer-image re-send agreeing on every key refused: %v", err)
				}
				if got, want := ukcPGRows(t, dsn, taRows), "1:x@a:a:uno"; got != want {
					t.Fatalf("target %s; want %s", got, want)
				}
				// The stated trade (UNIQUE-KEY-GUARD-OVERLAP-REFUSAL): the same
				// source row re-sent after ANOTHER unique column changed refuses,
				// loud, target unchanged.
				err = kcgPGApply(t, dsn, batched, ta(3, 1, "z@a", "a", "uno"))
				if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
					t.Fatalf("trade: err = %v; want %s", err, ir.ErrUniqueKeyConflictOtherRow)
				}
				if got, want := ukcPGRows(t, dsn, taRows), "1:x@a:a:uno"; got != want {
					t.Fatalf("trade: target %s; want %s", got, want)
				}
			})

			t.Run("no primary key, an expression unique index beside the arbiter", func(t *testing.T) {
				// Round 3 of the review, measured: with expression elements
				// dropped from the count, UNIQUE(lower(email)) vanished, the
				// table looked like one index, and the guard stayed unarmed —
				// INSERT (99,'y@a','a',…) ON CONFLICT (code) replaced row 1.
				// The mixed index (k, lower(e)) contributed only k.
				for _, sh := range []struct{ name, ddl, rows string }{
					{"lower(email)", `CREATE TABLE tx (id int, email text, code text NOT NULL, v text);
						CREATE UNIQUE INDEX tx_code ON tx (code); CREATE UNIQUE INDEX tx_lemail ON tx (lower(email));
						INSERT INTO tx VALUES (1,'x@a','a','one');`, "1:x@a:a:one"},
					{"mixed (id, lower(email))", `CREATE TABLE tx (id int, email text, code text NOT NULL, v text);
						CREATE UNIQUE INDEX tx_code ON tx (code); CREATE UNIQUE INDEX tx_mixed ON tx (id, lower(email));
						INSERT INTO tx VALUES (1,'x@a','a','one');`, "1:x@a:a:one"},
				} {
					t.Run(sh.name, func(t *testing.T) {
						dsn, cleanup := newSharedPGDB(t, "ukc_expr_"+strings.NewReplacer("(", "", ")", "", " ", "", ",", "").Replace(sh.name)+"_"+door)
						defer cleanup()
						applyPGApplier(t, dsn, sh.ddl)
						const txRows = "SELECT concat_ws(':', id::text, email, code, v) FROM tx"
						tx := func(pos int, id int64, email, v string) ir.Insert {
							return ir.Insert{Position: kcgPGPos(pos), Schema: "public", Table: "tx", Row: ir.Row{"id": id, "email": email, "code": "a", "v": v}}
						}
						// A different row: id differs (and email, for the mixed index's
						// expression to be the only other disagreement, id is kept 1).
						diff := tx(1, 99, "y@a", "ninetynine")
						if strings.HasPrefix(sh.name, "mixed") {
							diff = tx(1, 1, "y@a", "ninetynine") // differs only in the expression's column
						}
						err := kcgPGApply(t, dsn, batched, diff)
						if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
							t.Fatalf("err = %v; want %s", err, ir.ErrUniqueKeyConflictOtherRow)
						}
						if got := ukcPGRows(t, dsn, txRows); got != sh.rows {
							t.Fatalf("the refusal changed the target: %s", got)
						}
						if err := kcgPGApply(t, dsn, batched, tx(2, 1, "x@a", "uno")); err != nil {
							t.Fatalf("a newer-image re-send agreeing on every key refused: %v", err)
						}
						if got, want := ukcPGRows(t, dsn, txRows), "1:x@a:a:uno"; got != want {
							t.Fatalf("target %s; want %s", got, want)
						}
					})
				}
			})

			t.Run("no primary key, a unique index reading no column: every conflict refuses", func(t *testing.T) {
				dsn, cleanup := newSharedPGDB(t, "ukc_const_"+door)
				defer cleanup()
				applyPGApplier(t, dsn, `CREATE TABLE tc (id int, code text NOT NULL, v text);
					CREATE UNIQUE INDEX tc_code ON tc (code); CREATE UNIQUE INDEX tc_one ON tc ((1));
					INSERT INTO tc VALUES (1,'a','one');`)
				err := kcgPGApply(t, dsn, batched, ir.Insert{Position: kcgPGPos(1), Schema: "public", Table: "tc", Row: ir.Row{"id": int64(1), "code": "a", "v": "one"}})
				if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
					t.Fatalf("err = %v; want the loud refusal (the guard cannot name that index's columns)", err)
				}
				if got, want := ukcPGRows(t, dsn, "SELECT concat_ws(':', id::text, code, v) FROM tc"), "1:a:one"; got != want {
					t.Fatalf("target %s; want %s", got, want)
				}
			})

			t.Run("no primary key, one unique index: the newer-image re-send still converges", func(t *testing.T) {
				dsn, cleanup := newSharedPGDB(t, "ukc_nopk_"+door)
				defer cleanup()
				// The documented contract the guard must not break (see the
				// file comment of unique_key_conflict_guard.go): this table is
				// not guarded.
				applyPGApplier(t, dsn, `CREATE TABLE kd (id int, k text NOT NULL, v text); CREATE UNIQUE INDEX kd_k ON kd (k);
					INSERT INTO kd VALUES (1,'a','one');`)
				if err := kcgPGApply(t, dsn, batched, ins(1, int64(1), "a", "uno")); err != nil {
					t.Fatalf("a newer-image re-send refused: %v", err)
				}
				if got, want := ukcPGRows(t, dsn, kdRows), "1:a:uno"; got != want {
					t.Fatalf("target %s; want %s", got, want)
				}
			})

			t.Run("primary key: another unique index raises unique_violation, as before", func(t *testing.T) {
				dsn, cleanup := newSharedPGDB(t, "ukc_pk_"+door)
				defer cleanup()
				applyPGApplier(t, dsn, `CREATE TABLE kd (id int PRIMARY KEY, k text NOT NULL, v text); CREATE UNIQUE INDEX kd_k ON kd (k);
					INSERT INTO kd VALUES (1,'a','one');`)
				before := ukcPGRows(t, dsn, kdRows)
				err := kcgPGApply(t, dsn, batched, ins(1, int64(99), "a", "ninetynine"))
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
					t.Fatalf("err = %v; want unique_violation (the guard is unarmed on a primary-key arbiter)", err)
				}
				if got := ukcPGRows(t, dsn, kdRows); got != before {
					t.Fatalf("target changed: %s -> %s", before, got)
				}
			})
		})
	}
}

// TestUniqueKeyConflictGuard_AcceptedSilentResidual_NullKeyColumnsMerge pins
// the Postgres half of a residual the guard DELIBERATELY does not close
// (round-4 review; the MySQL twin's comment has the reasoning), so a future
// fix flips it on purpose: with no primary key, UNIQUE(code) the arbiter and
// UNIQUE(id) nullable beside it, rows (NULL,'a','one') and (NULL,'a','two')
// agree on every unique index's columns under IS NOT DISTINCT FROM, so the
// second overwrites the first at exit 0. A partial unique index behaves the
// same for rows outside its predicate (its predicate is not read).
func TestUniqueKeyConflictGuard_AcceptedSilentResidual_NullKeyColumnsMerge(t *testing.T) {
	for _, batched := range []bool{false, true} {
		t.Run(fmt.Sprintf("batched=%t", batched), func(t *testing.T) {
			dsn, cleanup := newSharedPGDB(t, fmt.Sprintf("ukc_nullres_%t", batched))
			defer cleanup()
			applyPGApplier(t, dsn, `CREATE TABLE tn (id int, code text NOT NULL, v text);
				CREATE UNIQUE INDEX tn_code ON tn (code); CREATE UNIQUE INDEX tn_id ON tn (id);
				INSERT INTO tn VALUES (NULL,'a','one');`)
			err := kcgPGApply(t, dsn, batched, ir.Insert{Position: kcgPGPos(1), Schema: "public", Table: "tn", Row: ir.Row{"id": nil, "code": "a", "v": "two"}})
			if err != nil {
				t.Fatalf("the residual is now refused (%v): flip this pin deliberately and update the backlog", err)
			}
			if got, want := ukcPGRows(t, dsn, "SELECT concat_ws(':', coalesce(id::text, 'NULL'), code, v) FROM tn"), "NULL:a:two"; got != want {
				t.Fatalf("target %s; want the accepted merge %s", got, want)
			}
		})
	}
}
