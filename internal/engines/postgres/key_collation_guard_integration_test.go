//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-FIRST-BOUNDARY option 3, the Postgres applier backstop
// (key_collation_guard.go), on a real Postgres 16, through both apply
// doors: the serial Apply and the batched (pipelined) ApplyBatch.
//
// The independent expected value is the target's rows read back by
// encode(convert_to(k,'UTF8'),'hex') after the run.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func kcgPGRows(t *testing.T, dsn, table, idCol string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(fmt.Sprintf("SELECT %s::text, encode(convert_to(k, 'UTF8'), 'hex') FROM %s", idCol, table))
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id, k string
		if err := rows.Scan(&id, &k); err != nil {
			t.Fatal(err)
		}
		out = append(out, id+":"+k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func kcgPGApply(t *testing.T, dsn string, batched bool, changes ...ir.Change) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	ch := make(chan ir.Change, len(changes))
	for _, c := range changes {
		ch <- c
	}
	close(ch)
	if batched {
		return applier.(ir.BatchedChangeApplier).ApplyBatch(ctx, "kcg-stream", ch, 100)
	}
	return applier.Apply(ctx, "kcg-stream", ch)
}

func kcgPGPos(n int) ir.Position {
	return ir.Position{Engine: "postgres", Token: fmt.Sprintf("0/%X", n+16)}
}

// kcgPGText is a source text type as the Postgres reader projects it.
func kcgPGText(coll string, det ir.CollationDeterminism) ir.Type {
	return ir.Text{Size: ir.TextLong, Collation: coll, Determinism: det}
}

// TestKeyCollationGuard_Postgres is the backstop's matrix on Postgres: a
// nondeterministic ICU key on the target, the source's key declared "C"
// (or another named deterministic collation) — every merge refuses KEY-COLLATION-MERGE with
// the target untouched, both doors; an identical re-apply passes; a table
// whose every column is the key (whose DO NOTHING would drop the row)
// refuses; and a source whose own collation is case-insensitive under
// another name re-delivers a case-only key change and converges.
func TestKeyCollationGuard_Postgres(t *testing.T) {
	for _, batched := range []bool{false, true} {
		door := "serial"
		if batched {
			door = "batched"
		}
		batched := batched
		t.Run(door, func(t *testing.T) {
			dsn, cleanup := newSharedPGDB(t, "kcg_"+door)
			defer cleanup()
			applyPGApplier(t, dsn, `CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
				CREATE COLLATION IF NOT EXISTS ci_ai (provider = icu, locale = 'und-u-ks-level1', deterministic = false);`)
			wantMerge := func(t *testing.T, err error, table, idCol, before string) {
				t.Helper()
				if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), "KEY-COLLATION-MERGE") {
					t.Fatalf("err = %v; want %s", err, ir.ErrKeyCollationMerge)
				}
				if got := kcgPGRows(t, dsn, table, idCol); got != before {
					t.Fatalf("the refusal changed the target: %s → %s", before, got)
				}
			}
			snap := func(table string, cols ...*ir.Column) ir.SchemaSnapshot {
				return ir.SchemaSnapshot{Schema: "public", Table: table, Position: kcgPGPos(1), IR: &ir.Table{Schema: "public", Name: table, Columns: cols}}
			}
			for _, src := range []struct {
				name string
				typ  ir.Type
			}{
				{"source C", kcgPGText("C", ir.CollationDeterministic)},
				{"source ucs_basic", kcgPGText("ucs_basic", ir.CollationDeterministic)},
			} {
				src := src
				t.Run(src.name+"/pk merge", func(t *testing.T) {
					table := "kd_" + strings.ToLower(strings.ReplaceAll(src.name, " ", "_"))
					applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int); INSERT INTO "+table+" VALUES (1,'a',1);")
					before := kcgPGRows(t, dsn, table, "id")
					err := kcgPGApply(
						t, dsn, batched,
						snap(table, &ir.Column{Name: "id", Type: ir.Integer{Width: 32}}, &ir.Column{Name: "k", Type: src.typ}, &ir.Column{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true}),
						ir.Insert{Position: kcgPGPos(2), Schema: "public", Table: table, Row: ir.Row{"id": int64(1), "k": "a", "d": int64(11)}},
						ir.Insert{Position: kcgPGPos(3), Schema: "public", Table: table, Row: ir.Row{"id": int64(99), "k": "A", "d": int64(99)}},
					)
					if batched {
						wantMerge(t, err, table, "id", before)
						return
					}
					if !errors.Is(err, ir.ErrKeyCollationMerge) {
						t.Fatalf("err = %v; want %s", err, ir.ErrKeyCollationMerge)
					}
					if got, want := kcgPGRows(t, dsn, table, "id"), "1:61"; got != want {
						t.Fatalf("target %s; want %s", got, want)
					}
				})
			}
			t.Run("every column is the key", func(t *testing.T) {
				applyPGApplier(t, dsn, "CREATE TABLE ak (k text COLLATE ci NOT NULL PRIMARY KEY); INSERT INTO ak VALUES ('a');")
				before := kcgPGRows(t, dsn, "ak", "k")
				s := snap("ak", &ir.Column{Name: "k", Type: kcgPGText("C", ir.CollationDeterministic)})
				if err := kcgPGApply(t, dsn, batched, s, ir.Insert{Position: kcgPGPos(2), Schema: "public", Table: "ak", Row: ir.Row{"k": "a"}}); err != nil {
					t.Fatalf("an identical re-apply refused: %v", err)
				}
				err := kcgPGApply(t, dsn, batched, s, ir.Insert{Position: kcgPGPos(3), Schema: "public", Table: "ak", Row: ir.Row{"k": "A"}})
				wantMerge(t, err, "ak", "k", before)
			})
			t.Run("a case-insensitive source re-delivers a case-only key change", func(t *testing.T) {
				applyPGApplier(t, dsn, "CREATE TABLE rd (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int);")
				s := snap("rd", &ir.Column{Name: "id", Type: ir.Integer{Width: 32}}, &ir.Column{Name: "k", Type: kcgPGText("ci_ai", ir.CollationNonDeterministic)}, &ir.Column{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true})
				insert := ir.Insert{Position: kcgPGPos(2), Schema: "public", Table: "rd", Row: ir.Row{"id": int64(1), "k": "a", "d": int64(1)}}
				update := ir.Update{
					Position: kcgPGPos(3), Schema: "public", Table: "rd",
					Before: ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
					After:  ir.Row{"id": int64(1), "k": "A", "d": int64(1)},
				}
				if err := kcgPGApply(t, dsn, batched, s, insert, update); err != nil {
					t.Fatalf("first delivery: %v", err)
				}
				if err := kcgPGApply(t, dsn, batched, s, insert, update); err != nil {
					t.Fatalf("the re-delivery refused (a wedge): %v", err)
				}
				if got, want := kcgPGRows(t, dsn, "rd", "id"), "1:41"; got != want {
					t.Fatalf("after the re-delivery the target is %s; want %s", got, want)
				}
			})
		})
	}
}

// TestPostgresKeyCollationPremise_EquatingChangeIsRefused binds the TRUE half
// of what the Postgres reader's catalog-as-of-now collation read was argued
// from (cdc_relations.go, projectCollation): a source cannot move a key
// column to a collation that equates two of its EXISTING values, because
// the unique index rebuild refuses. The conclusion once drawn from it — that
// "now" is always at least as discriminating as any replayed state — is
// false; TestPostgresKeyCollationPremise_LooseningAfterADeleteIsAllowed
// below pins the counter-example (KEY-COLLATION review F-D).
func TestPostgresKeyCollationPremise_EquatingChangeIsRefused(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "kcg_premise")
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
		CREATE TABLE kp (k text COLLATE "C" NOT NULL PRIMARY KEY); INSERT INTO kp VALUES ('a'), ('A');`)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `ALTER TABLE kp ALTER COLUMN k TYPE text COLLATE ci`); err == nil {
		t.Fatal("the source moved a key holding 'a' and 'A' to a collation that equates them; the premise the " +
			"reader's catalog-as-of-now collation read rests on no longer holds")
	} else if !strings.Contains(err.Error(), "23505") && !strings.Contains(strings.ToLower(err.Error()), "duplicate") {
		t.Fatalf("refused, but not as a unique violation: %v", err)
	}
}

// TestPostgresKeyCollationPremise_LooseningAfterADeleteIsAllowed pins the
// counter-example that makes the catalog-as-of-now read a stated residual
// rather than a safe one (KEY-COLLATION review F-D): the values that would
// collide need not exist when the collation is loosened. Tighten `ci` to
// "C", insert 'A' beside 'a', delete 'A', loosen back to `ci` — every step
// succeeds, so a reader replaying that window after the loosening compares
// 'A' with 'a' under `ci`. If this ever started refusing, the residual named
// in projectCollation's comment and the CHANGELOG would be worth re-reading.
func TestPostgresKeyCollationPremise_LooseningAfterADeleteIsAllowed(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "kcg_premise_loosen")
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
		CREATE TABLE kl (k text COLLATE ci NOT NULL PRIMARY KEY); INSERT INTO kl VALUES ('a');`)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, stmt := range []string{
		`ALTER TABLE kl ALTER COLUMN k TYPE text COLLATE "C"`,
		`INSERT INTO kl VALUES ('A')`,
		`DELETE FROM kl WHERE k = 'A'`,
		`ALTER TABLE kl ALTER COLUMN k TYPE text COLLATE ci`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v (the sequence that makes the catalog-as-of-now read unsafe no longer runs; "+
				"re-read projectCollation's FALSE PREMISE note)", stmt, err)
		}
	}
}

// TestPostgresDefaultCollationIsDeterministic binds the premise the
// readers' default-collation stamp rests on (types.go collationDeterminism,
// cdc_reader.go resolveColumnCollations): a database's DEFAULT collation is
// deterministic, even when the database is created with a case-insensitive
// ICU locale (measured on Postgres 16, 2026-10-12: 'a' = 'A' is false there
// and pg_collation's 'default' row reports collisdeterministic). If this
// ever failed, the SchemaReader's COALESCE would stamp a nondeterministic
// default as deterministic and the key-collation guard would compare it by
// bytes where the source does not.
func TestPostgresDefaultCollationIsDeterministic(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "kcg_default_premise")
	defer cleanup()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const name = "kcg_icu_ci_default"
	_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name)
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name+
		" LOCALE_PROVIDER icu ICU_LOCALE 'und-u-ks-level2' TEMPLATE template0"); err != nil {
		t.Fatalf("create an ICU-locale database: %v", err)
	}
	defer func() { _, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name) }()
	host, port, user, password := ensureSharedPostgres(t)
	icu, err := sql.Open("pgx", sharedPGDSN(host, port, user, password, name))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = icu.Close() }()
	var eq, det bool
	if err := icu.QueryRowContext(ctx, `SELECT 'a' = 'A',
		(SELECT collisdeterministic FROM pg_catalog.pg_collation WHERE collname = 'default')`).Scan(&eq, &det); err != nil {
		t.Fatal(err)
	}
	if eq || !det {
		t.Fatalf("on a database created with a case-insensitive ICU locale, 'a' = 'A' is %v and the default "+
			"collation reports collisdeterministic=%v: the readers' deterministic stamp for the default no longer holds", eq, det)
	}
}
