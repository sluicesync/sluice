// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestKeyCollationMergeRemedy_NamesTheCollationThatConverges pins Bug 305's
// remedy text over the plans the guard actually builds (planKeyCollationGuard
// over a real source/target pair): the remedy names, per armed column, the
// collation that compares as the source's — on MariaDB the NO PAD uca1400
// form, never the source's own 0900 name — and offers the re-copy only where
// sluice re-creates the key with that collation. For a key armed to compare
// bytes (a Postgres source collation, which a MySQL target cannot declare)
// the re-copy recreates the target's default collation, so it is not offered.
func TestKeyCollationMergeRemedy_NamesTheCollationThatConverges(t *testing.T) {
	varchar := func(cs, coll string) ir.Type { return ir.Varchar{Length: 20, Charset: cs, Collation: coll} }
	source := func(k ir.Type) *ir.Table {
		return &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "k", Type: k}}}
	}
	// What an older sluice created on MariaDB from a MySQL 8 `0900_ai_ci` key.
	padSpace := map[string]*ir.Column{
		"id": {Name: "id", Type: ir.Integer{Width: 32}},
		"k":  {Name: "k", Type: varchar("utf8mb4", "utf8mb4_uca1400_ai_ci")},
	}
	mysqlCI := map[string]*ir.Column{
		"id": {Name: "id", Type: ir.Integer{Width: 32}},
		"k":  {Name: "k", Type: varchar("utf8mb4", "utf8mb4_0900_ai_ci")},
	}
	pgC := ir.Varchar{Length: 20, Collation: "C", Determinism: ir.CollationDeterministic}
	cases := []struct {
		name      string
		plan      *keyCollationPlan
		want      []string
		forbidden []string
	}{
		{
			"mysql 8 to mariadb, the primary key",
			planKeyCollationGuard(source(varchar("utf8mb4", "utf8mb4_0900_ai_ci")), padSpace, targetUniqueKeys{pk: []string{"k"}, uniques: [][]string{{"k"}}}, mariadbKeyGuardCollations),
			[]string{"COLLATE utf8mb4_uca1400_nopad_ai_ci", "--restart-from-scratch", "converges either way"},
			[]string{"utf8mb4_0900_ai_ci", "does not converge"},
		},
		{
			"mysql 8 to mariadb, a unique key beside the primary key",
			planKeyCollationGuard(source(varchar("utf8mb4", "utf8mb4_0900_ai_ci")), padSpace, targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"id"}, {"k"}}}, mariadbKeyGuardCollations),
			[]string{"COLLATE utf8mb4_uca1400_nopad_ai_ci", "--restart-from-scratch"},
			[]string{"utf8mb4_0900_ai_ci"},
		},
		{
			"a postgres byte-compared key on a mysql _ci primary key",
			planKeyCollationGuard(source(pgC), mysqlCI, targetUniqueKeys{pk: []string{"k"}, uniques: [][]string{{"k"}}}, mysqlKeyGuardCollations),
			[]string{"does not converge", "COLLATE utf8mb4_bin"},
			[]string{"--restart-from-scratch", "converges either way"},
		},
	}
	for _, tc := range cases {
		if tc.plan == nil {
			t.Fatalf("%s: the guard armed nothing — the cell is not exercising a refusal", tc.name)
		}
		got := keyCollationMergeRemedy(tc.plan, "s", "kd")
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: remedy lacks %q:\n%s", tc.name, w, got)
			}
		}
		for _, f := range tc.forbidden {
			if strings.Contains(got, f) {
				t.Errorf("%s: remedy says %q:\n%s", tc.name, f, got)
			}
		}
	}
}
