// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

func kcgCols(kt ir.Type) map[string]*ir.Column {
	return map[string]*ir.Column{
		"id": {Name: "id", Type: ir.Integer{Width: 32}},
		"k":  {Name: "k", Type: kt},
		"d":  {Name: "d", Type: ir.Integer{Width: 32}},
	}
}

func kcgSource(kt ir.Type) *ir.Table {
	return &ir.Table{Name: "kd", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "k", Type: kt}, {Name: "d", Type: ir.Integer{Width: 32}},
	}}
}

// TestPlanKeyCollationGuard is the arming truth table (operator's choice
// (b): armed by the SOURCE's collation). Expected values from the design.
func TestPlanKeyCollationGuard(t *testing.T) {
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}
	bin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	cases := []struct {
		name        string
		source      *ir.Table
		target      map[string]*ir.Column
		pk          []string
		uniques     [][]string
		wantArmed   bool
		wantBytes   bool
		wantKeyLens []int
		remap       map[string]string
		wantColl    string
	}{
		// Review F-A: the cold start remaps a MySQL 8 `0900_ai_ci` (NO PAD)
		// key onto MariaDB's PAD SPACE `uca1400_ai_ci`, which equates 'a'
		// and 'a ' where the source does not. Judged by the emitter's remap
		// this was "unchanged" and unguarded (measured: a silent overwrite on
		// MariaDB 11.4). It must arm, compared under the NO PAD form.
		{
			"MySQL 0900_ai_ci on a MariaDB uca1400_ai_ci target: armed, NO PAD compare", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_ai_ci"}),
			[]string{"k"},
			nil, true, false,
			[]int{1},
			FlavorMariaDB.keyGuardCollations(), "utf8mb4_uca1400_nopad_ai_ci",
		},
		{
			"MySQL 0900_as_cs on a MariaDB uca1400_as_cs target: armed, NO PAD compare", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_as_cs"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_as_cs"}),
			[]string{"k"},
			nil, true, false,
			[]int{1},
			FlavorMariaDB.keyGuardCollations(), "utf8mb4_uca1400_nopad_as_cs",
		},
		{
			"MySQL 0900_bin on a MariaDB nopad_bin target: the same comparison, unarmed", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_nopad_bin"}),
			[]string{"k"},
			nil, false, false, nil,
			FlavorMariaDB.keyGuardCollations(), "",
		},
		{
			"MySQL 0900_ai_ci on a MariaDB NO PAD uca1400 target: the same comparison, unarmed", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_nopad_ai_ci"}),
			[]string{"k"},
			nil, false, false, nil,
			FlavorMariaDB.keyGuardCollations(), "",
		},
		{
			"MySQL 0900_bin on a MariaDB uca1400_ai_ci target: armed, compared as nopad_bin", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_ai_ci"}),
			[]string{"k"},
			nil, true, false,
			[]int{1},
			FlavorMariaDB.keyGuardCollations(), "utf8mb4_nopad_bin",
		},
		// The reverse: a MariaDB uca1400 source on the MySQL 0900 column the
		// cold start created. The 0900 target is NO PAD, so it never equates
		// what a PAD SPACE source keeps apart: unarmed, no probe for a
		// collation MySQL does not have.
		{
			"MariaDB uca1400_ai_ci on a MySQL 0900_ai_ci target: unarmed", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_ai_ci"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}),
			[]string{"k"},
			nil, false, false, nil,
			FlavorVanilla.keyGuardCollations(), "",
		},
		{
			"MariaDB uca1400_nopad_as_cs on a MySQL 0900_as_cs target: unarmed", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_nopad_as_cs"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_as_cs"}),
			[]string{"k"},
			nil, false, false, nil,
			FlavorVanilla.keyGuardCollations(), "",
		},
		{
			"MariaDB uca1400_as_cs on a MySQL 0900_ai_ci target: armed, compared as 0900_as_cs", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_as_cs"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}),
			[]string{"k"},
			nil, true, false,
			[]int{1},
			FlavorVanilla.keyGuardCollations(), "utf8mb4_0900_as_cs",
		},
		// KEY-COLLATION-PG-DEFAULT-TO-MYSQL, operator's choice (b): a
		// Postgres database-default key (no name, deterministic) is armed
		// only as a UNIQUE key beside a primary key it is not part of.
		{"Postgres default collation, unique beside a primary key: armed", kcgSource(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}), kcgCols(ci), []string{"id"}, [][]string{{"k"}}, true, true, []int{1}, nil, ""},
		{"Postgres default collation, primary key: not armed (no new wedge)", kcgSource(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"Postgres default collation, unique with no primary key: not armed", kcgSource(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}), kcgCols(ci), nil, [][]string{{"k"}}, false, false, nil, nil, ""},
		{"Postgres default collation, unique key also holding the primary key column: not armed", kcgSource(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}), kcgCols(ci), []string{"k"}, [][]string{{"k", "id"}}, false, false, nil, nil, ""},
		{"Postgres deterministic source through a domain", kcgSource(ir.Domain{Name: "code", BaseType: ir.Text{Size: ir.TextLong, Collation: "C", Determinism: ir.CollationDeterministic}}), kcgCols(ci), []string{"k"}, nil, true, true, []int{1}, nil, ""},
		{"no source shape", nil, kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"source and target agree: no guard, no cost", kcgSource(ci), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"same collation in another case", kcgSource(ir.Varchar{Length: 20, Charset: "UTF8MB4", Collation: "UTF8MB4_GENERAL_CI"}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"source _bin, target _ci, primary key", kcgSource(bin), kcgCols(ci), []string{"k"}, nil, true, false, []int{1}, nil, ""},
		{"source _bin, target _ci, unique key beside a primary key", kcgSource(bin), kcgCols(ci), []string{"id"}, [][]string{{"k"}}, true, false, []int{1}, nil, ""},
		{"source _bin, target _ci, no key on k", kcgSource(bin), kcgCols(ci), []string{"id"}, nil, false, false, nil, nil, ""},
		{"source collation unknown (VStream)", kcgSource(ir.Varchar{Length: 20}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"Postgres deterministic source", kcgSource(ir.Text{Size: ir.TextLong, Collation: "C", Determinism: ir.CollationDeterministic}), kcgCols(ci), []string{"k"}, nil, true, true, []int{1}, nil, ""},
		// Review 2026-10-12 finding 1: a nondeterministic Postgres key (ICU
		// ks-level2 / level3) is created under the target's default, which
		// equates what the source keeps apart. Armed exactly as the database
		// default is: bytes, and only as a UNIQUE key beside a primary key.
		{"Postgres nondeterministic source, primary key: not armed (no new wedge)", kcgSource(ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: ir.CollationNonDeterministic}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"Postgres nondeterministic source, unique beside a primary key: armed as bytes", kcgSource(ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: ir.CollationNonDeterministic}), kcgCols(ci), []string{"id"}, [][]string{{"k"}}, true, true, []int{1}, nil, ""},
		{"Postgres nondeterministic source, unique with no primary key: not armed", kcgSource(ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: ir.CollationNonDeterministic}), kcgCols(ci), nil, [][]string{{"k"}}, false, false, nil, nil, ""},
		// Review 2026-10-12 finding 5: a named Postgres collation whose
		// determinism no reader stamped (a manifest written before v0.159.0)
		// arms as the nondeterministic case does — right for either kind.
		{"Postgres named collation of unknown determinism, unique beside a primary key: armed as bytes", kcgSource(ir.Text{Size: ir.TextLong, Collation: "ci"}), kcgCols(ci), []string{"id"}, [][]string{{"k"}}, true, true, []int{1}, nil, ""},
		{"Postgres named collation of unknown determinism, primary key: not armed at apply", kcgSource(ir.Text{Size: ir.TextLong, Collation: "C"}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"Postgres default collation of unknown determinism: not armed (indistinguishable from VStream)", kcgSource(ir.Text{Size: ir.TextLong}), kcgCols(ci), []string{"id"}, [][]string{{"k"}}, false, false, nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := planKeyCollationGuard(tc.source, tc.target, targetUniqueKeys{pk: tc.pk, uniques: tc.uniques}, tc.remap)
			if (p != nil) != tc.wantArmed {
				t.Fatalf("armed = %v; want %v (%+v)", p != nil, tc.wantArmed, p)
			}
			if p == nil {
				return
			}
			if p.armed["k"].bytes != tc.wantBytes {
				t.Fatalf("bytes = %v; want %v", p.armed["k"].bytes, tc.wantBytes)
			}
			if len(p.keys) != len(tc.wantKeyLens) {
				t.Fatalf("keys = %v; want %d", p.keys, len(tc.wantKeyLens))
			}
			if tc.wantColl != "" && p.armed["k"].collation != tc.wantColl {
				t.Fatalf("compared under %q; want %q", p.armed["k"].collation, tc.wantColl)
			}
		})
	}
}

// TestKeyCollationGuard_RendersFirstAndOnce pins the clause shape the
// integration matrix measured: the guard is the FIRST assignment (MySQL
// evaluates the list against the row as updated so far), it replaces its
// column's plain assignment, and the raise carries the marker.
func TestKeyCollationGuard_RendersFirstAndOnce(t *testing.T) {
	p := planKeyCollationGuard(kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}),
		kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}), targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}}, nil)
	for _, upsert := range []upsertSpelling{upsertRowAlias, upsertValuesFunc} {
		assign, hang, ok := p.render("s", "kd", []string{"d", "id", "k"}, upsert)
		if !ok || hang != "d" {
			t.Fatalf("render: ok=%v hang=%q", ok, hang)
		}
		clause := onDuplicateKeyUpdateClause([]string{"d", "id", "k"}, []string{"id"}, upsert, assign, hang)
		body := strings.TrimPrefix(clause, upsert.clauseOpen())
		if !strings.HasPrefix(body, assign) {
			t.Fatalf("the guard is not the first assignment: %s", clause)
		}
		if strings.Count(clause, "`d` =") != 1 {
			t.Fatalf("the hang column is assigned twice: %s", clause)
		}
		for _, want := range []string{"KEY-COLLATION-MERGE", "18446744073709551615", "COLLATE utf8mb4_bin", "`s`.`kd`.`k`"} {
			if !strings.Contains(assign, want) {
				t.Errorf("guard %q lacks %q", assign, want)
			}
		}
		// Review F-B: the raise is never a value arm (its type would be
		// aggregated with the column's: error 4078 on MariaDB UUID / INET6 /
		// GEOMETRY, and a CAST to CHAR silently changed FLOAT values). Both
		// value arms are the column's own incoming value.
		newD := upsert.newRowRef("`d`")
		if want := ", IF((18446744073709551615 + "; !strings.Contains(assign, want) {
			t.Errorf("the raise is not in a condition: %s", assign)
		}
		if !strings.HasSuffix(assign, ") = 0, "+newD+", "+newD+"))") {
			t.Errorf("the raise's arms are not the column's own value: %s", assign)
		}
		if strings.Contains(strings.ToUpper(assign), " AS CHAR") {
			t.Errorf("the guard converts a value through a string: %s", assign)
		}
	}
	if _, _, ok := p.render("s", "kd", []string{"d", "id"}, upsertRowAlias); ok {
		t.Fatal("a guard whose key column is not in the insert rendered")
	}
}

// TestKeyCollationMergeRemedy_LeadsWithARecopyWhenTheIdentityIsArmed pins
// the remedy's honesty (review LOW): when the key that identifies a row is
// itself armed, or there is no primary key, a re-delivered change of that
// key cannot be told from a second row, the refusal repeats, and only a
// re-copy converges; otherwise aligning the collation does.
func TestKeyCollationMergeRemedy_LeadsWithARecopyWhenTheIdentityIsArmed(t *testing.T) {
	armedK := map[string]armedCollation{"k": {charset: "utf8mb4", collation: "utf8mb4_bin"}}
	for _, tc := range []struct {
		name   string
		plan   *keyCollationPlan
		recopy bool
	}{
		{"no primary key", &keyCollationPlan{armed: armedK}, true},
		{"the primary key is the armed column", &keyCollationPlan{pk: []string{"k"}, armed: armedK}, true},
		{"the armed column is a unique key beside the primary key", &keyCollationPlan{pk: []string{"id"}, armed: armedK}, false},
	} {
		got := keyCollationMergeRemedy(tc.plan, "s", "t")
		if lead := strings.HasPrefix(got, "The key that identifies a row"); lead != tc.recopy {
			t.Errorf("%s: remedy leads with the re-copy = %v; want %v: %s", tc.name, lead, tc.recopy, got)
		}
	}
}

// TestKeyCollationMergeRefusal_NeedsTheNumberAndTheMarker pins the
// attribution (review LOW): a 1690 is the guard's raise only when the
// server's text names the marker; an ordinary overflow in the same guarded
// statement surfaces as itself, and the marker without the number is not
// the guard either.
func TestKeyCollationMergeRefusal_NeedsTheNumberAndTheMarker(t *testing.T) {
	p := &keyCollationPlan{pk: []string{"k"}}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"1690 with the marker", &mysql.MySQLError{Number: 1690, Message: "BIGINT UNSIGNED value is out of range in '18446744073709551615 + octet_length(concat('KEY-COLLATION-MERGE',`new`.`k`))'"}, true},
		{"1690 without the marker", &mysql.MySQLError{Number: 1690, Message: "BIGINT value is out of range in '(`t`.`n` + 1)'"}, false},
		{"the marker on another error", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'KEY-COLLATION-MERGE' for key 'k'"}, false},
		{"wrapped 1690 with the marker", fmt.Errorf("vtgate: %w", &mysql.MySQLError{Number: 1690, Message: "out of range in 'KEY-COLLATION-MERGE'"}), true},
	}
	for _, tc := range cases {
		got := errors.Is(keyCollationMergeRefusal(tc.err, p, "s", "t"), ir.ErrKeyCollationMerge)
		if got != tc.want {
			t.Errorf("%s: attributed = %v; want %v", tc.name, got, tc.want)
		}
	}
	if err := keyCollationMergeRefusal(cases[0].err, nil, "s", "t"); errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Error("an unguarded statement's 1690 was attributed to the guard")
	}
}

// TestKeyCollationGuardUnsupported_NamesTheSourcesOwnCollation pins the
// GUARD-UNSUPPORTED refusal for the cross-flavor arm: a MySQL 8
// `utf8mb4_0900_ai_ci` key on a MariaDB target is compared as
// `utf8mb4_uca1400_nopad_ai_ci`, which MariaDB below 10.10 does not have
// (measured on 10.5 and 10.6: the first CDC write to the table refuses).
// The refusal must attribute 0900 to the source and the NO PAD form to the
// comparison, not claim the source declares a collation it does not; it is
// terminal and carries the merge sentinel the restart roster keys on.
func TestKeyCollationGuardUnsupported_NamesTheSourcesOwnCollation(t *testing.T) {
	src := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	tgt := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}
	ac, armed := armedFor(src, tgt, FlavorMariaDB.keyGuardCollations())
	if !armed {
		t.Fatal("a 0900_ai_ci key on a MariaDB general_ci column is not armed")
	}
	cause := &mysql.MySQLError{Number: 1273, Message: "Unknown collation: 'utf8mb4_uca1400_nopad_ai_ci'"}
	err := keyCollationGuardUnsupported("`t`.`kd`", "k", ac, cause)
	msg := err.Error()
	for _, want := range []string{
		keyCollationGuardUnsupportedMarker,
		"COLLATE utf8mb4_0900_ai_ci (compared on this target as utf8mb4_uca1400_nopad_ai_ci)",
		"a server version that has utf8mb4_uca1400_nopad_ai_ci",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal lacks %q:\n%s", want, msg)
		}
	}
	if !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Error("the refusal does not carry ir.ErrKeyCollationMerge (the restart roster's key)")
	}
	var term ir.TerminalError
	if !errors.As(err, &term) || !term.Terminal() {
		t.Error("the refusal is not terminal")
	}

	// A same-flavor collation the target lacks has no separate comparison
	// name, and the message says nothing about one.
	same, _ := armedFor(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_ja_0900_as_cs"}, tgt, FlavorVanilla.keyGuardCollations())
	if msg := keyCollationGuardUnsupported("`t`.`kd`", "k", same, cause).Error(); strings.Contains(msg, "compared on this target") ||
		!strings.Contains(msg, "COLLATE utf8mb4_ja_0900_as_cs, which") {
		t.Errorf("same-flavor refusal mis-rendered:\n%s", msg)
	}
}

// TestKeyCollationGuard_PrefixKeyPartsCompareThePrefix pins review
// 2026-10-12 #2 finding 1 at the render: a prefix key part (SUB_PART n) is
// compared as LEFT(x, n) on BOTH sides of both comparisons — the target's,
// because that is what the server matched on, and the source's, applied
// before the CONVERT/CAST — while a whole-column part and the primary key's
// same-row check stay whole.
func TestKeyCollationGuard_PrefixKeyPartsCompareThePrefix(t *testing.T) {
	for _, src := range []ir.Type{
		ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"},
		ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic},
	} {
		p := planKeyCollationGuard(kcgSource(src), kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}),
			targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}, uniqueSubs: [][]int{{5}}}, nil)
		if p == nil {
			t.Fatalf("%v: not armed", src)
		}
		assign, _, ok := p.render("s", "kd", []string{"id", "k", "d"}, upsertRowAlias)
		if !ok {
			t.Fatal("render refused")
		}
		tgt := "LEFT(`s`.`kd`.`k`, 5) <=> LEFT(" + upsertRowAlias.newRowRef("`k`") + ", 5)"
		if !strings.Contains(assign, tgt) {
			t.Errorf("%v: the target comparison is not on the prefix:\n%s", src, assign)
		}
		if strings.Count(assign, "LEFT(") != 4 {
			t.Errorf("%v: want the prefix on both sides of both comparisons (4 LEFTs):\n%s", src, assign)
		}
		if !strings.Contains(assign, "(`s`.`kd`.`id` <=> "+upsertRowAlias.newRowRef("`id`")+")") {
			t.Errorf("%v: the primary key's same-row check is not on the whole value:\n%s", src, assign)
		}
	}
}
