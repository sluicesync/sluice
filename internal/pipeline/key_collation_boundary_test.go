// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// kcVarchar is a MySQL-dialect character type with collation coll.
func kcVarchar(cs, coll string) ir.Type {
	return ir.Varchar{Length: 20, Charset: cs, Collation: coll}
}

// kcTable is kd(id INT, k <kt>, u <ut>) keyed as keys says: "pk" (k is the
// primary key), "uq" (id is the primary key, k a UNIQUE index), "none"
// (id is the primary key, k unindexed).
func kcTable(kt, ut ir.Type, keys string) *ir.Table {
	t := &ir.Table{
		Schema: "src", Name: "kd",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: kt},
			{Name: "u", Type: ut, Nullable: true},
		},
	}
	switch keys {
	case "pk":
		t.PrimaryKey = &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}
	case "uq":
		t.PrimaryKey = &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}
		t.Indexes = []*ir.Index{{Name: "uk", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}}
	case "nonunique":
		t.PrimaryKey = &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}
		t.Indexes = []*ir.Index{{Name: "ik", Columns: []ir.IndexColumn{{Column: "k"}}}}
	default:
		t.PrimaryKey = &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}
	}
	return t
}

// TestJudgeKeyCollation_Matrix is the key-collation door's truth table
// (KEY-COLLATION-FIRST-BOUNDARY): every measured change shape (a `_ci` →
// `_bin` collation, a character-set change in place, PAD SPACE → NO PAD)
// on a PRIMARY KEY and on a UNIQUE column, against each prior the stream
// can hold — none, one the change differs from, one it matches — plus the
// cases that must NOT fire: a non-key column, a non-unique index, a change
// stream that carries no collation (pgoutput, VStream), a foreign dialect,
// and a target that already holds the new collation. The expected values
// are written from the design, not derived from the function.
func TestJudgeKeyCollation_Matrix(t *testing.T) {
	t.Parallel()
	ci := kcVarchar("utf8mb4", "utf8mb4_0900_ai_ci")
	bin := kcVarchar("utf8mb4", "utf8mb4_bin")
	nopad := kcVarchar("utf8mb4", "utf8mb4_0900_bin")
	ascii := kcVarchar("ascii", "ascii_bin")
	plain := ir.Integer{Width: 32}
	none := ir.Varchar{Length: 20} // a change stream that carries no collation
	type want int
	const (
		noChange want = iota
		proven
		unproven
	)
	cases := []struct {
		name             string
		post, prior, tgt *ir.Table
		sameEngine       bool
		targetEngine     string
		want             want
	}{
		{"pk ci→bin, prior ci", kcTable(bin, plain, "pk"), kcTable(ci, plain, "pk"), kcTable(ci, plain, "pk"), true, "mysql", proven},
		{"uq ci→bin, prior ci", kcTable(bin, plain, "uq"), kcTable(ci, plain, "uq"), kcTable(ci, plain, "uq"), true, "mysql", proven},
		{"pk charset in place, prior utf8mb4", kcTable(ascii, plain, "pk"), kcTable(ci, plain, "pk"), kcTable(ci, plain, "pk"), true, "mysql", proven},
		{"pk PAD SPACE→NO PAD, prior bin", kcTable(nopad, plain, "pk"), kcTable(bin, plain, "pk"), kcTable(bin, plain, "pk"), true, "mysql", proven},
		{"cross-flavor engines, proven by the prior", kcTable(bin, plain, "pk"), kcTable(ci, plain, "pk"), kcTable(kcVarchar("utf8mb4", "utf8mb4_uca1400_ai_ci"), plain, "pk"), false, "mariadb", proven},
		{"pk ci→bin, no prior, same engine", kcTable(bin, plain, "pk"), nil, kcTable(ci, plain, "pk"), true, "mysql", unproven},
		{"pk ci→bin, prior without a collation, same engine", kcTable(bin, plain, "pk"), kcTable(none, plain, "pk"), kcTable(ci, plain, "pk"), true, "mysql", unproven},
		{"pk differs, no prior, different engines", kcTable(bin, plain, "pk"), nil, kcTable(ci, plain, "pk"), false, "mariadb", noChange},
		{"source unchanged, target differs on purpose", kcTable(ci, plain, "pk"), kcTable(ci, plain, "pk"), kcTable(bin, plain, "pk"), true, "mysql", noChange},
		{"target already holds the new collation", kcTable(bin, plain, "pk"), kcTable(ci, plain, "pk"), kcTable(bin, plain, "pk"), true, "mysql", noChange},
		{"same collation in another case", kcTable(kcVarchar("UTF8MB4", "UTF8MB4_BIN"), plain, "pk"), kcTable(ci, plain, "pk"), kcTable(bin, plain, "pk"), true, "mysql", noChange},
		{"non-key column", kcTable(bin, plain, "none"), kcTable(ci, plain, "none"), kcTable(ci, plain, "none"), true, "mysql", noChange},
		{"non-unique index", kcTable(bin, plain, "nonunique"), kcTable(ci, plain, "nonunique"), kcTable(ci, plain, "nonunique"), true, "mysql", noChange},
		{"change stream carries no collation", kcTable(none, plain, "pk"), kcTable(ci, plain, "pk"), kcTable(ci, plain, "pk"), true, "postgres", noChange},
		{"foreign dialect on the target (MySQL → Postgres)", kcTable(bin, plain, "pk"), kcTable(ci, plain, "pk"), kcTable(ir.Text{Size: ir.TextLong}, plain, "pk"), false, "postgres", noChange},
		{
			"postgres collation, prior ci", kcTable(ir.Text{Size: ir.TextLong, Collation: "C"}, plain, "pk"),
			kcTable(ir.Text{Size: ir.TextLong, Collation: "ci"}, plain, "pk"), kcTable(ir.Text{Size: ir.TextLong, Collation: "ci"}, plain, "pk"),
			true, "postgres", proven,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := judgeKeyCollation(tc.post, tc.prior, tc.tgt, tc.sameEngine, tc.targetEngine)
			switch tc.want {
			case noChange:
				if len(got) != 0 {
					t.Fatalf("judged %+v; want no change", got)
				}
			case proven, unproven:
				if len(got) != 1 || got[0].column != "k" {
					t.Fatalf("judged %+v; want one change on k", got)
				}
				if got[0].unproven() != (tc.want == unproven) {
					t.Fatalf("unproven = %t; want %t (%+v)", got[0].unproven(), tc.want == unproven, got[0])
				}
				if !strings.Contains(got[0].diff().source, keyCollationMarker) {
					t.Errorf("the diff does not carry %s: %+v", keyCollationMarker, got[0].diff())
				}
			}
		})
	}
}

// TestWithKeyCollation_Routes pins what a verdict becomes: one proven change
// on an otherwise matching table forwards an ALTER of the target column
// carrying the target's type (the synthesized pre-state), and everything
// else refuses with the remedy.
func TestWithKeyCollation_Routes(t *testing.T) {
	t.Parallel()
	tgtType := kcVarchar("utf8mb4", "utf8mb4_0900_ai_ci")
	provenChange := keyCollationChange{column: "k", prior: "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", now: "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", targetType: tgtType}
	unprovenChange := provenChange
	unprovenChange.prior = ""
	if v := withKeyCollation(witnessVerdict{kind: witnessMatch}, nil); v.kind != witnessMatch {
		t.Fatalf("no change: kind %d", v.kind)
	}
	v := withKeyCollation(witnessVerdict{kind: witnessMatch}, []keyCollationChange{provenChange})
	if v.kind != witnessForwardAlter || v.altered != "k" || !sameDeclaredCollation(v.targetType, tgtType) {
		t.Fatalf("one proven change: %+v; want a forwarded ALTER of k against the target's type", v)
	}
	for name, in := range map[string]struct {
		v       witnessVerdict
		changes []keyCollationChange
	}{
		"unproven":               {witnessVerdict{kind: witnessMatch}, []keyCollationChange{unprovenChange}},
		"two changes":            {witnessVerdict{kind: witnessMatch}, []keyCollationChange{provenChange, provenChange}},
		"beside an added column": {witnessVerdict{kind: witnessForwardAdd, added: []string{"x"}}, []keyCollationChange{provenChange}},
		"beside a target-only":   {witnessVerdict{kind: witnessTargetOnly, targetOnly: []string{"x"}}, []keyCollationChange{provenChange}},
	} {
		got := withKeyCollation(in.v, in.changes)
		if got.kind != witnessRefuse || !strings.Contains(got.remedy, keyCollationMarker) {
			t.Errorf("%s: %+v; want a refusal carrying %s", name, got, keyCollationMarker)
		}
	}
}

// TestFirstBoundaryWitness_KeyCollation runs the door through the witness's
// own verdict against a fake target catalog: the seed is the prior, and
// without one the history at a proven-later position is.
func TestFirstBoundaryWitness_KeyCollation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ci := kcVarchar("utf8mb4", "utf8mb4_0900_ai_ci")
	bin := kcVarchar("utf8mb4", "utf8mb4_bin")
	plain := ir.Integer{Width: 32}
	target := kcTable(ci, plain, "pk")
	post := kcTable(bin, plain, "pk")
	t.Run("seed prior forwards", func(t *testing.T) {
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"kd": target}}, "mysql", "mysql")
		v, err := w.verdict(ctx, post, ir.Position{}, kcTable(ci, plain, "pk"))
		if err != nil || v.kind != witnessForwardAlter || v.altered != "k" {
			t.Fatalf("verdict = %+v, %v; want a forwarded ALTER of k", v, err)
		}
	})
	t.Run("no prior refuses", func(t *testing.T) {
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"kd": target}}, "mysql", "mysql")
		v, err := w.verdict(ctx, post, ir.Position{}, nil)
		if err != nil || v.kind != witnessRefuse {
			t.Fatalf("verdict = %+v, %v; want a refusal", v, err)
		}
		if err := resumeDivergenceRefusal("kd", v, ""); !strings.Contains(err.Error(), keyCollationMarker) {
			t.Fatalf("refusal %v does not carry %s", err, keyCollationMarker)
		}
	})
	t.Run("an unchanged key passes", func(t *testing.T) {
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"kd": target}}, "mysql", "mysql")
		if v, err := w.verdict(ctx, kcTable(ci, plain, "pk"), ir.Position{}, nil); err != nil || v.kind != witnessMatch {
			t.Fatalf("verdict = %+v, %v; want a match", v, err)
		}
	})
}
