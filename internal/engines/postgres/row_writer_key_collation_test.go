// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestBuildBatchUpsert_KeyCollationGuard pins the copy statement's shape
// (KEY-COLLATION-ROWWRITER): unguarded it is byte-identical to before;
// guarded the target is aliased, the guard's CASE is the DO UPDATE's WHERE,
// a table whose every column is the key gets a guarded self-assignment in
// place of DO NOTHING, and beside a Neki shard-key guard both predicates
// are ANDed with the shard predicate naming the alias (the table's own
// name is hidden by it).
func TestBuildBatchUpsert_KeyCollationGuard(t *testing.T) {
	src := ir.Text{Size: ir.TextLong, Collation: "C", Determinism: ir.CollationDeterministic}
	table := &ir.Table{
		Name: "kd",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: src},
			{Name: "tenant", Type: ir.Integer{Width: 32}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}},
	}
	plan, unexpressible := planPGKeyCollationGuard(table, []string{"k"}, map[string]targetKeyCollation{"k": {name: "ci"}})
	if plan == nil || len(unexpressible) != 0 {
		t.Fatalf("not armed: %+v %v", plan, unexpressible)
	}

	plain, _ := buildBatchUpsert("public", table, 1, []string{"k"}, upsertShardKeyPlan{}, nil)
	if strings.Contains(plain, keyCollationExistingAlias) || strings.Contains(plain, "WHERE") {
		t.Fatalf("an unguarded statement changed: %s", plain)
	}
	got, _ := buildBatchUpsert("public", table, 1, []string{"k"}, upsertShardKeyPlan{}, plan)
	for _, want := range []string{
		`INSERT INTO "public"."kd" AS "sluice_existing" (`,
		` DO UPDATE SET "id" = EXCLUDED."id", "tenant" = EXCLUDED."tenant" WHERE CASE WHEN "sluice_existing"."k" COLLATE "C" IS NOT DISTINCT FROM EXCLUDED."k" COLLATE "C" THEN true ELSE`,
		keyCollationMergeMarker,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("guarded statement lacks %q:\n%s", want, got)
		}
	}

	allKey := &ir.Table{Name: "ak", Columns: []*ir.Column{{Name: "k", Type: src}}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}}
	gotAll, _ := buildBatchUpsert("public", allKey, 1, []string{"k"}, upsertShardKeyPlan{}, plan)
	if strings.Contains(gotAll, "DO NOTHING") ||
		!strings.Contains(gotAll, ` DO UPDATE SET "k" = "sluice_existing"."k" WHERE CASE WHEN `) ||
		!strings.Contains(gotAll, " THEN false ELSE ") {
		t.Fatalf("an all-key table is not guarded in place of DO NOTHING:\n%s", gotAll)
	}

	shard := upsertShardKeyPlan{omitFromSet: []string{"tenant"}, guard: true}
	gotShard, guarded := buildBatchUpsert("public", table, 1, []string{"k"}, shard, plan)
	if !guarded {
		t.Fatal("the shard-key guard is no longer reported")
	}
	if !strings.Contains(gotShard, ` WHERE "sluice_existing"."tenant" IS NOT DISTINCT FROM EXCLUDED."tenant" AND CASE WHEN `) {
		t.Fatalf("the two predicates are not joined over the alias:\n%s", gotShard)
	}
}

// TestCopyKeyCollationMergeRefusal_NeedsTheGuardAndTheMarker pins the
// attribution: only the guard's invalid cast carrying the marker, from a
// statement that carried the guard, is a merge.
func TestCopyKeyCollationMergeRefusal_NeedsTheGuardAndTheMarker(t *testing.T) {
	raise := &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "KEY-COLLATION-MERGE"`}
	plan := &pgKeyCollationPlan{compare: map[string]string{"k": "C"}, marker: "k"}
	err := copyKeyCollationMergeRefusal(raise, plan, "public", "kd")
	var kce *ir.KeyCollationMergeError
	if !errors.As(err, &kce) || !strings.Contains(err.Error(), "run the copy again") {
		t.Fatalf("the guard's raise was not attributed with the copy's remedy: %v", err)
	}
	if errors.Is(copyKeyCollationMergeRefusal(raise, nil, "public", "kd"), ir.ErrKeyCollationMerge) {
		t.Error("a statement without the guard was attributed")
	}
	other := &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "x"`}
	if errors.Is(copyKeyCollationMergeRefusal(other, plan, "public", "kd"), ir.ErrKeyCollationMerge) {
		t.Error("an ordinary invalid cast was attributed")
	}
}
