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
