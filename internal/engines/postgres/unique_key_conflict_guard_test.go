// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestNewPGConflictIdentity pins where the Postgres guard arms: a table with a
// primary key that is not its ON CONFLICT key (a deferrable one), keyed on
// the primary key; a table with no primary key and two or more unique
// indexes, keyed on all their columns. Keyed on its primary key a conflict
// through the arbiter is the same row; with no primary key and one unique
// index the arbiter cannot tell a merge from the newer-image re-send.
func TestNewPGConflictIdentity(t *testing.T) {
	if newPGConflictIdentity([]string{"id"}, []string{"id"}, nil) != nil {
		t.Error("armed on a table keyed on its primary key")
	}
	if newPGConflictIdentity(nil, nil, nil) != nil {
		t.Error("armed on a keyless table (plain INSERT)")
	}
	if newPGConflictIdentity(nil, []string{"k"}, nil) != nil {
		t.Error("armed on a table without a primary key: its one arbiter cannot tell a merge from a newer-image re-send")
	}
	u := newPGConflictIdentity(nil, []string{"code"}, [][]string{{"code"}, {"email", "id"}})
	if u == nil {
		t.Fatal("unarmed on a keyless table with two unique indexes (the review's arbiter-dropped shape)")
	}
	if got, _ := u.columns([]string{"id", "email", "code", "v"}); strings.Join(got, ",") != "code,email,id" {
		t.Errorf("keyless, two indexes: identity %v; want code,email,id", got)
	}
	if got, _ := u.columns([]string{"code", "v"}); got != nil {
		t.Errorf("a row missing an index column: identity %v; want unarmed", got)
	}
	// An index whose expression reads no nameable column: every conflict refuses.
	x := newPGConflictIdentity(nil, []string{"code"}, [][]string{{"code"}, nil})
	if _, all := x.columns([]string{"code", "v"}); !all {
		t.Error("an unnameable expression index did not widen to refusing every conflict")
	}
	if w := x.wrap([]string{"code", "v"}, "true"); !strings.HasPrefix(w, "CASE WHEN false THEN") {
		t.Errorf("refuse-all predicate: %s", w)
	}
	d := newPGConflictIdentity([]string{"id"}, []string{"k"}, nil) // a deferrable PK the arbiter skipped
	if got, _ := d.columns([]string{"id", "k", "v"}); strings.Join(got, ",") != "id" {
		t.Errorf("deferrable primary key: identity %v; want id", got)
	}
	if got, _ := d.columns([]string{"k", "v"}); got != nil {
		t.Errorf("a row without the primary key: identity %v; want unarmed", got)
	}
}

// TestBuildGuardedInsertSQL_ConflictIdentity pins the statement: armed, the
// DO UPDATE's WHERE compares every identity column as text under "C" and
// raises otherwise; with nil the statement is byte-identical to
// [buildInsertSQL]; an all-key table keeps DO NOTHING when only the identity
// guard is armed.
func TestBuildGuardedInsertSQL_ConflictIdentity(t *testing.T) {
	row := ir.Row{"id": int64(99), "k": "a", "v": "ninetynine"}
	plain, _, err := buildInsertSQL("public", "kd", row, []string{"k"}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	same, _, err := buildGuardedInsertSQL("public", "kd", row, []string{"k"}, nil, nil, nil, nil)
	if err != nil || same != plain {
		t.Fatalf("nil identity changed the statement:\n%s\n%s", plain, same)
	}
	got, _, err := buildGuardedInsertSQL("public", "kd", row, []string{"k"}, nil, nil, nil, newPGConflictIdentity([]string{"id"}, []string{"k"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`INSERT INTO "public"."kd" AS "sluice_existing"`,
		`ON CONFLICT ("k") DO UPDATE SET`,
		`WHERE CASE WHEN ("sluice_existing"."id")::text COLLATE "C" IS NOT DISTINCT FROM (EXCLUDED."id")::text COLLATE "C"`,
		`THEN true ELSE (('UNIQUE-KEY-CONFLICT-OTHER-ROW' || pg_catalog.left(pg_catalog.concat(EXCLUDED."`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("guarded insert lacks %q:\n%s", want, got)
		}
	}
	// With the key-collation guard armed too, its predicate is the OUTER
	// check: a collation merge is reported as one, and the identity runs for
	// the rows it passed.
	coll := &pgKeyCollationPlan{compare: map[string]string{"k": "C"}, marker: "k"}
	both, _, err := buildGuardedInsertSQL("public", "kd", row, []string{"k"}, nil, nil, coll, newPGConflictIdentity([]string{"id"}, []string{"k"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	kc, uk := strings.Index(both, "KEY-COLLATION-MERGE"), strings.Index(both, "UNIQUE-KEY-CONFLICT-OTHER-ROW")
	if kc < 0 || uk < 0 || !strings.Contains(both, `COLLATE "C" THEN CASE WHEN ("sluice_existing"`) {
		t.Errorf("composed guards misplaced:\n%s", both)
	}
	allKey, _, err := buildGuardedInsertSQL("public", "kk", ir.Row{"k": "a"}, []string{"k"}, nil, nil, nil, newPGConflictIdentity([]string{"id"}, []string{"k"}, nil))
	if err != nil || !strings.HasSuffix(allKey, "DO NOTHING") {
		t.Errorf("all-key table: %s (%v)", allKey, err)
	}
	// A row that does not supply the primary key: nothing to compare, the
	// statement is unguarded.
	noPK := ir.Row{"k": "a", "v": "x"}
	p1, _, _ := buildInsertSQL("public", "kd", noPK, []string{"k"}, nil, nil, nil)
	p2, _, _ := buildGuardedInsertSQL("public", "kd", noPK, []string{"k"}, nil, nil, nil, newPGConflictIdentity([]string{"id"}, []string{"k"}, nil))
	if p1 != p2 {
		t.Errorf("a row without the primary key was guarded: %s", p2)
	}
}

// TestApplyInsertGuardError_Attribution pins which error the guard owns: a
// 22P02 carrying its marker on a statement that carried it, and the
// classifier's fallback for a pipelined error that arrives without its plan.
func TestApplyInsertGuardError_Attribution(t *testing.T) {
	g := newPGConflictIdentity([]string{"id"}, []string{"k"}, nil)
	raise := &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "UNIQUE-KEY-CONFLICT-OTHER-ROW"`}
	err := applyInsertGuardError(fmt.Errorf("batch: %w", raise), nil, g, "public", "kd")
	if !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) || !ir.IsTerminal(err) {
		t.Fatalf("not attributed: %v", err)
	}
	for _, want := range []string{`("k")`, "--restart-from-scratch", `"public"."kd"`, "`sluice sync from-backup run`", "`--reset-target-data --yes`", "`sluice slot drop sluice_<--slot-name>"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q: %v", want, err)
		}
	}
	if err := applyInsertGuardError(raise, nil, nil, "public", "kd"); errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
		t.Error("an unguarded statement's error was attributed")
	}
	other := &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "x"`}
	if err := applyInsertGuardError(other, nil, g, "public", "kd"); errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
		t.Error("a 22P02 without the marker was attributed")
	}
	if !errors.Is(classifyApplierError(raise), ir.ErrUniqueKeyConflictOtherRow) {
		t.Error("the classifier does not attribute the raise")
	}
	if ir.ErrUniqueKeyConflictOtherRow.Error() != uniqueKeyConflictMarker {
		t.Error("the sentinel's text is not the marker")
	}
}
