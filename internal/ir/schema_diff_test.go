// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"reflect"
	"strings"
	"testing"
)

func TestDiffSchemas_NoChanges(t *testing.T) {
	s := &Schema{Tables: []*Table{
		{
			Name: "users",
			Columns: []*Column{
				{Name: "id", Type: Integer{Width: 64}},
				{Name: "email", Type: Varchar{Length: 255}},
			},
		},
	}}
	d := DiffSchemas(s, s, DiffOptions{})
	if d.HasChanges() {
		t.Errorf("expected no changes; got %+v", d)
	}
}

func TestDiffSchemas_TableMissingAndExtra(t *testing.T) {
	exp := &Schema{Tables: []*Table{
		{Name: "users", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}},
		{Name: "orders", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}},
	}}
	act := &Schema{Tables: []*Table{
		{Name: "users", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}},
		{Name: "deprecated_log", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if !reflect.DeepEqual(d.TablesMissing, []string{"orders"}) {
		t.Errorf("missing = %v; want [orders]", d.TablesMissing)
	}
	if !reflect.DeepEqual(d.TablesExtra, []string{"deprecated_log"}) {
		t.Errorf("extra = %v; want [deprecated_log]", d.TablesExtra)
	}
	if len(d.TablesMismatched) != 0 {
		t.Errorf("mismatched = %v; want none", d.TablesMismatched)
	}
}

func TestDiffSchemas_IgnoreExtras(t *testing.T) {
	exp := &Schema{Tables: []*Table{
		{Name: "users", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}},
	}}
	act := &Schema{Tables: []*Table{
		{Name: "users", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}, {Name: "extra_col", Type: Varchar{Length: 10}}}},
		{Name: "other_app_table", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}},
	}}
	d := DiffSchemas(exp, act, DiffOptions{IgnoreExtras: true})
	if len(d.TablesExtra) != 0 {
		t.Errorf("expected no extras under IgnoreExtras; got %v", d.TablesExtra)
	}
	for _, td := range d.TablesMismatched {
		if len(td.ColumnsExtra) != 0 {
			t.Errorf("expected no column extras under IgnoreExtras for %s; got %v", td.Name, td.ColumnsExtra)
		}
	}
}

func TestDiffSchemas_ColumnMissingAndExtra(t *testing.T) {
	exp := &Schema{Tables: []*Table{
		{
			Name: "users",
			Columns: []*Column{
				{Name: "id", Type: Integer{Width: 64}},
				{Name: "email", Type: Varchar{Length: 255}},
				{Name: "created_at", Type: Timestamp{Precision: 6}},
			},
		},
	}}
	act := &Schema{Tables: []*Table{
		{
			Name: "users",
			Columns: []*Column{
				{Name: "id", Type: Integer{Width: 64}},
				{Name: "email", Type: Varchar{Length: 255}},
				{Name: "legacy_field", Type: Varchar{Length: 50}},
			},
		},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.TablesMismatched) != 1 {
		t.Fatalf("expected 1 mismatched table; got %d", len(d.TablesMismatched))
	}
	td := d.TablesMismatched[0]
	if td.Name != "users" {
		t.Errorf("table name = %q; want users", td.Name)
	}
	if !reflect.DeepEqual(td.ColumnsMissing, []string{"created_at"}) {
		t.Errorf("missing = %v; want [created_at]", td.ColumnsMissing)
	}
	if !reflect.DeepEqual(td.ColumnsExtra, []string{"legacy_field"}) {
		t.Errorf("extra = %v; want [legacy_field]", td.ColumnsExtra)
	}
}

func TestDiffSchemas_ColumnTypeMismatch(t *testing.T) {
	cases := []struct {
		name    string
		expType Type
		actType Type
	}{
		{"varchar length", Varchar{Length: 255}, Varchar{Length: 100}},
		{"int width", Integer{Width: 64}, Integer{Width: 32}},
		{"decimal precision", Decimal{Precision: 18, Scale: 4}, Decimal{Precision: 10, Scale: 2}},
		{"text size", Text{Size: TextLong}, Text{Size: TextRegular}},
		{"json binary flag", JSON{Binary: true}, JSON{Binary: false}},
		{"timestamp tz", Timestamp{Precision: 6, WithTimeZone: true}, Timestamp{Precision: 6}},
		{"different family", Integer{Width: 32}, Varchar{Length: 10}},
		{"uuid vs char", UUID{}, Char{Length: 36}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exp := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{{Name: "c", Type: tc.expType}}}}}
			act := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{{Name: "c", Type: tc.actType}}}}}
			d := DiffSchemas(exp, act, DiffOptions{})
			if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].ColumnsMismatched) != 1 {
				t.Fatalf("expected one column mismatch; got %+v", d)
			}
			cd := d.TablesMismatched[0].ColumnsMismatched[0]
			if cd.ExpectedType == "" || cd.ActualType == "" {
				t.Errorf("expected/actual type strings should be set; got %+v", cd)
			}
			if cd.ExpectedType == cd.ActualType {
				t.Errorf("expected != actual type renderings; got %+v", cd)
			}
		})
	}
}

func TestDiffSchemas_NullabilityMismatch(t *testing.T) {
	exp := &Schema{Tables: []*Table{
		{Name: "t", Columns: []*Column{{Name: "c", Type: Integer{Width: 32}, Nullable: false}}},
	}}
	act := &Schema{Tables: []*Table{
		{Name: "t", Columns: []*Column{{Name: "c", Type: Integer{Width: 32}, Nullable: true}}},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.TablesMismatched) != 1 {
		t.Fatalf("expected one table mismatch; got %+v", d)
	}
	cd := d.TablesMismatched[0].ColumnsMismatched[0]
	if cd.ExpectedNullable == nil || cd.ActualNullable == nil {
		t.Fatalf("nullable pointers should be set; got %+v", cd)
	}
	if *cd.ExpectedNullable || !*cd.ActualNullable {
		t.Errorf("expected nullable=false / actual nullable=true; got exp=%v act=%v",
			*cd.ExpectedNullable, *cd.ActualNullable)
	}
	// Type fields should be empty when only nullability differs.
	if cd.ExpectedType != "" || cd.ActualType != "" {
		t.Errorf("expected type fields empty when only nullability differs; got %+v", cd)
	}
}

// TestDiffSchemas_CharsetCollationMismatch covers the v0.11.0 (or
// whatever version this lands in) charset/collation drift detection.
// Both fields surface as separate ColumnDiff fields and combine
// independently — a column can have both, just one, or neither.
func TestDiffSchemas_CharsetCollationMismatch(t *testing.T) {
	cases := []struct {
		name                       string
		exp                        Type
		act                        Type
		wantCharset, wantCollation bool
	}{
		{
			"charset only",
			Varchar{Length: 255, Charset: "utf8mb4"},
			Varchar{Length: 255, Charset: "latin1"},
			true, false,
		},
		{
			"collation only",
			Varchar{Length: 255, Collation: "utf8mb4_general_ci"},
			Varchar{Length: 255, Collation: "utf8mb4_bin"},
			false, true,
		},
		{
			"both differ",
			Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"},
			Varchar{Length: 255, Charset: "latin1", Collation: "latin1_swedish_ci"},
			true, true,
		},
		{
			"text type also tracks charset/collation",
			Text{Size: TextLong, Collation: "utf8mb4_bin"},
			Text{Size: TextLong, Collation: "utf8mb4_general_ci"},
			false, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exp := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{{Name: "c", Type: tc.exp}}}}}
			act := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{{Name: "c", Type: tc.act}}}}}
			d := DiffSchemas(exp, act, DiffOptions{})
			if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].ColumnsMismatched) != 1 {
				t.Fatalf("expected one column mismatch; got %+v", d)
			}
			cd := d.TablesMismatched[0].ColumnsMismatched[0]
			if tc.wantCharset && (cd.ExpectedCharset == "" || cd.ActualCharset == "") {
				t.Errorf("expected charset fields populated; got %+v", cd)
			}
			if !tc.wantCharset && (cd.ExpectedCharset != "" || cd.ActualCharset != "") {
				t.Errorf("did not expect charset fields populated; got %+v", cd)
			}
			if tc.wantCollation && (cd.ExpectedCollation == "" || cd.ActualCollation == "") {
				t.Errorf("expected collation fields populated; got %+v", cd)
			}
			if !tc.wantCollation && (cd.ExpectedCollation != "" || cd.ActualCollation != "") {
				t.Errorf("did not expect collation fields populated; got %+v", cd)
			}
			// Type-string fields stay empty when only charset/collation
			// differs — Type.String() doesn't include them.
			if cd.ExpectedType != "" || cd.ActualType != "" {
				t.Errorf("type strings should be empty when only charset/collation differs; got %+v", cd)
			}
		})
	}
}

// TestDiffSchemas_EmptySourceCharsetCollationNoDrift pins the
// v0.11.2 fix: when the source/expected side has an empty charset
// or collation, comparison is skipped for that field rather than
// surfacing the difference as drift. Covers the cross-engine
// retarget case (PG→MySQL UUID→Char(36) where the retargeted Char
// has no charset/collation) plus the non-character-type case.
func TestDiffSchemas_EmptySourceCharsetCollationNoDrift(t *testing.T) {
	t.Run("empty source charset + populated target → no drift", func(t *testing.T) {
		exp := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Char{Length: 36}}, // retargeted from PG UUID; no charset
		}}}}
		act := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Char{Length: 36, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
		}}}}
		d := DiffSchemas(exp, act, DiffOptions{})
		if len(d.TablesMismatched) != 0 {
			t.Errorf("retargeted column should not surface as drift; got %+v", d.TablesMismatched)
		}
	})
	t.Run("populated source + empty target → drift surfaces (asymmetric)", func(t *testing.T) {
		// The asymmetry is intentional: the source/expected side is
		// authoritative. Empty-source means "any actual is fine";
		// populated-source means "actual must match." So a target
		// missing the source's declared charset IS drift.
		exp := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 255, Charset: "utf8mb4"}},
		}}}}
		act := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 255}},
		}}}}
		d := DiffSchemas(exp, act, DiffOptions{})
		if len(d.TablesMismatched) != 1 {
			t.Fatalf("expected drift when source declares charset and target doesn't; got %+v", d)
		}
	})
	t.Run("both populated, different → drift (existing behaviour preserved)", func(t *testing.T) {
		exp := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 255, Charset: "utf8mb4"}},
		}}}}
		act := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 255, Charset: "latin1"}},
		}}}}
		d := DiffSchemas(exp, act, DiffOptions{})
		if len(d.TablesMismatched) != 1 {
			t.Fatalf("expected drift for differing charsets; got %+v", d)
		}
		cd := d.TablesMismatched[0].ColumnsMismatched[0]
		if cd.ExpectedCharset != "utf8mb4" || cd.ActualCharset != "latin1" {
			t.Errorf("charset fields = %q/%q; want utf8mb4/latin1", cd.ExpectedCharset, cd.ActualCharset)
		}
	})
}

// TestDiffSchemas_IgnoreCharsetCollation pins the suppression
// behaviour: when DiffOptions.IgnoreCharsetCollation is set,
// columns whose only drift was charset/collation drop out of
// ColumnsMismatched entirely; columns with additional drift keep
// surfacing minus the charset/collation fields.
func TestDiffSchemas_IgnoreCharsetCollation(t *testing.T) {
	t.Run("only-charset drift suppressed", func(t *testing.T) {
		exp := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}},
		}}}}
		act := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 255, Charset: "latin1", Collation: "latin1_swedish_ci"}},
		}}}}
		d := DiffSchemas(exp, act, DiffOptions{IgnoreCharsetCollation: true})
		if len(d.TablesMismatched) != 0 {
			t.Errorf("expected no mismatches under IgnoreCharsetCollation; got %+v", d.TablesMismatched)
		}
	})

	t.Run("type drift survives charset suppression", func(t *testing.T) {
		exp := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 255, Charset: "utf8mb4"}},
		}}}}
		act := &Schema{Tables: []*Table{{Name: "t", Columns: []*Column{
			{Name: "c", Type: Varchar{Length: 100, Charset: "latin1"}},
		}}}}
		d := DiffSchemas(exp, act, DiffOptions{IgnoreCharsetCollation: true})
		if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].ColumnsMismatched) != 1 {
			t.Fatalf("expected the type drift to survive; got %+v", d)
		}
		cd := d.TablesMismatched[0].ColumnsMismatched[0]
		if cd.ExpectedType != "Varchar(255)" || cd.ActualType != "Varchar(100)" {
			t.Errorf("type drift should survive; got %+v", cd)
		}
		if cd.ExpectedCharset != "" || cd.ActualCharset != "" {
			t.Errorf("charset fields should be cleared; got %+v", cd)
		}
	})
}

func TestDiffSchemas_IndexAddedRemoved(t *testing.T) {
	exp := &Schema{Tables: []*Table{
		{
			Name:    "users",
			Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}, {Name: "email", Type: Varchar{Length: 255}}},
			Indexes: []*Index{
				{Name: "users_email_idx", Columns: []IndexColumn{{Column: "email"}}, Unique: true},
				{Name: "users_id_idx", Columns: []IndexColumn{{Column: "id"}}},
			},
		},
	}}
	act := &Schema{Tables: []*Table{
		{
			Name:    "users",
			Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}, {Name: "email", Type: Varchar{Length: 255}}},
			Indexes: []*Index{
				{Name: "users_id_idx", Columns: []IndexColumn{{Column: "id"}}},
				{Name: "legacy_idx", Columns: []IndexColumn{{Column: "email"}}},
			},
		},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.TablesMismatched) != 1 {
		t.Fatalf("expected one table mismatch; got %+v", d)
	}
	td := d.TablesMismatched[0]
	if !reflect.DeepEqual(td.IndexesMissing, []string{"users_email_idx"}) {
		t.Errorf("missing indexes = %v; want [users_email_idx]", td.IndexesMissing)
	}
	if !reflect.DeepEqual(td.IndexesExtra, []string{"legacy_idx"}) {
		t.Errorf("extra indexes = %v; want [legacy_idx]", td.IndexesExtra)
	}
}

func TestDiffSchemas_PrimaryKeyTracked(t *testing.T) {
	pk := &Index{Name: "users_pkey", Columns: []IndexColumn{{Column: "id"}}, Unique: true}
	exp := &Schema{Tables: []*Table{
		{Name: "users", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}, PrimaryKey: pk},
	}}
	act := &Schema{Tables: []*Table{
		{Name: "users", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].IndexesMissing) != 1 {
		t.Fatalf("expected pk-as-index missing; got %+v", d)
	}
	if d.TablesMismatched[0].IndexesMissing[0] != "users_pkey" {
		t.Errorf("missing index = %q; want users_pkey", d.TablesMismatched[0].IndexesMissing[0])
	}
}

func TestDiffSchemas_NilInputsReturnEmpty(t *testing.T) {
	d := DiffSchemas(nil, nil, DiffOptions{})
	if d.HasChanges() {
		t.Errorf("nil inputs should produce no diff; got %+v", d)
	}
	d = DiffSchemas(&Schema{}, nil, DiffOptions{})
	if d.HasChanges() {
		t.Errorf("nil actual should produce no diff; got %+v", d)
	}
}

func TestDiffSchemas_SortedOutput(t *testing.T) {
	exp := &Schema{Tables: []*Table{
		{Name: "z_table", Columns: []*Column{{Name: "id", Type: Integer{Width: 32}}}},
		{Name: "a_table", Columns: []*Column{{Name: "id", Type: Integer{Width: 32}}}},
		{Name: "m_table", Columns: []*Column{{Name: "id", Type: Integer{Width: 32}}}},
	}}
	act := &Schema{}
	d := DiffSchemas(exp, act, DiffOptions{})
	want := []string{"a_table", "m_table", "z_table"}
	if !reflect.DeepEqual(d.TablesMissing, want) {
		t.Errorf("missing = %v; want sorted %v", d.TablesMissing, want)
	}
}

func TestDiffSchemas_DefaultLiteralMismatch(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Integer{Width: 32},
			Default: DefaultLiteral{Value: "1"},
		}},
	}}}
	act := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Integer{Width: 32},
			Default: DefaultLiteral{Value: "2"},
		}},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].ColumnsMismatched) != 1 {
		t.Fatalf("expected literal-default mismatch; got %+v", d)
	}
	cd := d.TablesMismatched[0].ColumnsMismatched[0]
	if cd.ExpectedDefault == "" || cd.ActualDefault == "" {
		t.Errorf("expected/actual default fields should be set; got %+v", cd)
	}
	if cd.DefaultLowConfidence {
		t.Errorf("literal-vs-literal mismatch should be high confidence; got %+v", cd)
	}
}

func TestDiffSchemas_DefaultExpressionLowConfidence(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Timestamp{Precision: 6},
			Default: DefaultExpression{Expr: "CURRENT_TIMESTAMP(0)"},
		}},
	}}}
	act := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Timestamp{Precision: 6},
			Default: DefaultExpression{Expr: "now() AT TIME ZONE 'UTC'"},
		}},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].ColumnsMismatched) != 1 {
		t.Fatalf("expected expr-default mismatch; got %+v", d)
	}
	cd := d.TablesMismatched[0].ColumnsMismatched[0]
	if !cd.DefaultLowConfidence {
		t.Errorf("expr-vs-expr default mismatch should set DefaultLowConfidence; got %+v", cd)
	}
}

func TestDiffSchemas_DefaultMissingHighConfidence(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Integer{Width: 32},
			Default: DefaultLiteral{Value: "0"},
		}},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "c", Type: Integer{Width: 32}}},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	cd := d.TablesMismatched[0].ColumnsMismatched[0]
	if cd.DefaultLowConfidence {
		t.Errorf("missing-on-one-side default drift should be high confidence; got %+v", cd)
	}
	if cd.ActualDefault != "<none>" {
		t.Errorf("actual default should be <none>; got %q", cd.ActualDefault)
	}
}

func TestDiffSchemas_DefaultEquivalencesSuppressDrift(t *testing.T) {
	cases := []struct {
		name string
		exp  string
		act  string
	}{
		{"now() vs CURRENT_TIMESTAMP", "now()", "CURRENT_TIMESTAMP"},
		{"now() vs CURRENT_TIMESTAMP(6)", "now()", "CURRENT_TIMESTAMP(6)"},
		{"CURRENT_TIMESTAMP vs now()", "CURRENT_TIMESTAMP", "now()"},
		{"current_date vs CURRENT_DATE", "current_date", "CURRENT_DATE"},
		{"whitespace tolerant", "current_timestamp ( 6 )", "CURRENT_TIMESTAMP(6)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exp := &Schema{Tables: []*Table{{
				Name: "t",
				Columns: []*Column{{
					Name: "c", Type: Timestamp{Precision: 6},
					Default: DefaultExpression{Expr: tc.exp},
				}},
			}}}
			act := &Schema{Tables: []*Table{{
				Name: "t",
				Columns: []*Column{{
					Name: "c", Type: Timestamp{Precision: 6},
					Default: DefaultExpression{Expr: tc.act},
				}},
			}}}
			d := DiffSchemas(exp, act, DiffOptions{})
			if d.HasChanges() {
				t.Errorf("expected no drift for %s vs %s; got %+v", tc.exp, tc.act, d)
			}
		})
	}
}

func TestDiffSchemas_GeneratedExprMismatch(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Integer{Width: 32},
			GeneratedExpr: "(price * 1.1)", GeneratedStored: true,
		}},
	}}}
	act := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Integer{Width: 32},
			GeneratedExpr: "(price * 1.2)", GeneratedStored: true,
		}},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	cd := d.TablesMismatched[0].ColumnsMismatched[0]
	if cd.ExpectedGeneratedExpr != "(price * 1.1)" || cd.ActualGeneratedExpr != "(price * 1.2)" {
		t.Errorf("expected generated-expr fields populated; got %+v", cd)
	}
}

func TestDiffSchemas_GeneratedExprMissingOnOneSide(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name: "t",
		Columns: []*Column{{
			Name: "c", Type: Integer{Width: 32},
			GeneratedExpr: "(price * 1.1)", GeneratedStored: true,
		}},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "c", Type: Integer{Width: 32}}},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	cd := d.TablesMismatched[0].ColumnsMismatched[0]
	if cd.ExpectedGeneratedExpr == "" || cd.ActualGeneratedExpr != "" {
		t.Errorf("expected generated-expr asymmetry; got %+v", cd)
	}
}

func TestDiffSchemas_CheckConstraintMissingExtra(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "qty_nonneg", Expr: "qty >= 0"},
		},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "legacy_check", Expr: "qty < 1000"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	td := d.TablesMismatched[0]
	if !reflect.DeepEqual(td.ChecksMissing, []string{"qty_nonneg"}) {
		t.Errorf("missing checks = %v; want [qty_nonneg]", td.ChecksMissing)
	}
	if !reflect.DeepEqual(td.ChecksExtra, []string{"legacy_check"}) {
		t.Errorf("extra checks = %v; want [legacy_check]", td.ChecksExtra)
	}
}

func TestDiffSchemas_CheckConstraintMismatch(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "qty_range", Expr: "qty >= 0"},
		},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "qty_range", Expr: "qty > 0"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	td := d.TablesMismatched[0]
	if len(td.ChecksMismatched) != 1 {
		t.Fatalf("expected one CHECK mismatch; got %+v", td)
	}
	cd := td.ChecksMismatched[0]
	if cd.Name != "qty_range" || cd.ExpectedExpr != "qty >= 0" || cd.ActualExpr != "qty > 0" {
		t.Errorf("CHECK diff = %+v; want qty_range expected=qty >= 0 actual=qty > 0", cd)
	}
}

func TestDiffSchemas_CheckConstraintIgnoreExtras(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "legacy_check", Expr: "qty < 1000"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{IgnoreExtras: true})
	if d.HasChanges() {
		t.Errorf("expected no drift under IgnoreExtras; got %+v", d)
	}
}

func TestDiffSchemas_CheckConstraintsUnnamedSkipped(t *testing.T) {
	// Anonymous CHECKs aren't matched across sides — they'd produce
	// false positives on cross-engine spelling differences. The diff
	// silently drops them.
	exp := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "", Expr: "qty >= 0"},
		},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if d.HasChanges() {
		t.Errorf("unnamed CHECK should not surface as drift; got %+v", d)
	}
}

func TestDiffSchemas_Summary_IncludesNewCategories(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "a", Expr: "qty > 0"},
			{Name: "b", Expr: "qty < 100"},
		},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "qty", Type: Integer{Width: 32}}},
		CheckConstraints: []*CheckConstraint{
			{Name: "b", Expr: "qty < 50"},
			{Name: "c", Expr: "qty != 7"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	got := d.Summary()
	for _, want := range []string{"missing CHECK", "extra CHECK", "CHECK mismatch"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q missing %q", got, want)
		}
	}
}

// ADR-0053 EXCLUDE constraint diff tests. Mirror the CHECK shape
// exactly — EXCLUDE constraints follow the same set-semantics
// (matched by Name; Definition equality byte-exact). PG-only; MySQL
// sides always carry empty slices.

func TestDiffSchemas_ExcludeConstraintMissingExtra(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}},
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "range_no_overlap", Definition: "EXCLUDE USING gist (id WITH =)"},
		},
	}}}
	act := &Schema{Tables: []*Table{{
		Name:    "t",
		Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}},
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "legacy_overlap", Definition: "EXCLUDE USING gist (id WITH &&)"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	td := d.TablesMismatched[0]
	if !reflect.DeepEqual(td.ExcludesMissing, []string{"range_no_overlap"}) {
		t.Errorf("missing excludes = %v; want [range_no_overlap]", td.ExcludesMissing)
	}
	if !reflect.DeepEqual(td.ExcludesExtra, []string{"legacy_overlap"}) {
		t.Errorf("extra excludes = %v; want [legacy_overlap]", td.ExcludesExtra)
	}
}

func TestDiffSchemas_ExcludeConstraintDefinitionMismatch(t *testing.T) {
	// Byte-exact Definition equality — predicate-whitespace difference
	// surfaces as a real mismatch (pg_get_constraintdef is server-
	// canonicalized so a real divergence here means hand-edit on one
	// side).
	exp := &Schema{Tables: []*Table{{
		Name: "t",
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "ex", Definition: "EXCLUDE USING gist (id WITH &&) WHERE (id > 0)"},
		},
	}}}
	act := &Schema{Tables: []*Table{{
		Name: "t",
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "ex", Definition: "EXCLUDE USING gist (id WITH &&) WHERE ((id > 0))"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	td := d.TablesMismatched[0]
	if len(td.ExcludesMismatched) != 1 {
		t.Fatalf("expected one EXCLUDE mismatch; got %+v", td)
	}
	ed := td.ExcludesMismatched[0]
	if ed.Name != "ex" {
		t.Errorf("mismatch Name = %q; want %q", ed.Name, "ex")
	}
	if !strings.Contains(ed.ExpectedDefinition, "(id > 0)") {
		t.Errorf("ExpectedDefinition lost predicate: %q", ed.ExpectedDefinition)
	}
	if !strings.Contains(ed.ActualDefinition, "((id > 0))") {
		t.Errorf("ActualDefinition lost predicate: %q", ed.ActualDefinition)
	}
}

func TestDiffSchemas_ExcludeConstraintIgnoreExtras(t *testing.T) {
	exp := &Schema{Tables: []*Table{{Name: "t"}}}
	act := &Schema{Tables: []*Table{{
		Name: "t",
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "legacy_overlap", Definition: "EXCLUDE USING gist (id WITH &&)"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{IgnoreExtras: true})
	if d.HasChanges() {
		t.Errorf("expected no drift under IgnoreExtras for extra EXCLUDE; got %+v", d)
	}
}

func TestDiffSchemas_Summary_IncludesExcludeCategories(t *testing.T) {
	exp := &Schema{Tables: []*Table{{
		Name: "t",
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "a", Definition: "EXCLUDE USING gist (id WITH &&)"},
			{Name: "b", Definition: "EXCLUDE USING gist (id WITH =)"},
		},
	}}}
	act := &Schema{Tables: []*Table{{
		Name: "t",
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "b", Definition: "EXCLUDE USING gist (id WITH <>)"},
			{Name: "c", Definition: "EXCLUDE USING gist (id WITH @>)"},
		},
	}}}
	d := DiffSchemas(exp, act, DiffOptions{})
	got := d.Summary()
	for _, want := range []string{"missing EXCLUDE", "extra EXCLUDE", "EXCLUDE mismatch"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q missing %q", got, want)
		}
	}
}

// TestDiffSchemas_ViewsMissingAndExtra covers the view-level
// missing/extra set-semantics added in the view-support Phase 1
// commit. Mirrors TestDiffSchemas_TableMissingAndExtra.
func TestDiffSchemas_ViewsMissingAndExtra(t *testing.T) {
	exp := &Schema{Views: []*View{
		{Name: "active_users", Definition: "SELECT id FROM users WHERE active"},
		{Name: "recent_orders", Definition: "SELECT id FROM orders WHERE created_at > NOW() - INTERVAL '7 days'"},
	}}
	act := &Schema{Views: []*View{
		{Name: "active_users", Definition: "SELECT id FROM users WHERE active"},
		{Name: "deprecated_view", Definition: "SELECT 1"},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if !reflect.DeepEqual(d.ViewsMissing, []string{"recent_orders"}) {
		t.Errorf("ViewsMissing = %v; want [recent_orders]", d.ViewsMissing)
	}
	if !reflect.DeepEqual(d.ViewsExtra, []string{"deprecated_view"}) {
		t.Errorf("ViewsExtra = %v; want [deprecated_view]", d.ViewsExtra)
	}
	if !d.HasChanges() {
		t.Errorf("expected HasChanges()=true on view drift")
	}
}

// TestDiffSchemas_ViewsIgnoreExtras verifies the IgnoreExtras opt
// suppresses extra-on-target views (mirrors the table behaviour).
func TestDiffSchemas_ViewsIgnoreExtras(t *testing.T) {
	exp := &Schema{Views: []*View{
		{Name: "v1", Definition: "SELECT 1"},
	}}
	act := &Schema{Views: []*View{
		{Name: "v1", Definition: "SELECT 1"},
		{Name: "other_app_view", Definition: "SELECT 2"},
	}}
	d := DiffSchemas(exp, act, DiffOptions{IgnoreExtras: true})
	if len(d.ViewsExtra) != 0 {
		t.Errorf("ViewsExtra = %v; want empty under IgnoreExtras", d.ViewsExtra)
	}
}

// TestDiffSchemas_ViewsMismatched_DefinitionDrift covers the trim-
// and-equal definition comparison. A view whose body changes
// (whitespace-insensitive) surfaces in ViewsMismatched.
func TestDiffSchemas_ViewsMismatched_DefinitionDrift(t *testing.T) {
	exp := &Schema{Views: []*View{
		{Name: "v1", Definition: "SELECT id, email FROM users"},
	}}
	act := &Schema{Views: []*View{
		{Name: "v1", Definition: "SELECT id FROM users"},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.ViewsMismatched) != 1 {
		t.Fatalf("ViewsMismatched len = %d; want 1", len(d.ViewsMismatched))
	}
	got := d.ViewsMismatched[0]
	if got.Name != "v1" {
		t.Errorf("Name = %q; want v1", got.Name)
	}
	if got.ExpectedDefinition == "" || got.ActualDefinition == "" {
		t.Errorf("expected both definitions populated on body drift, got %+v", got)
	}
}

// TestDiffSchemas_ViewsMismatched_MaterializedFlag covers the
// materialized-flag drift case: same body, different materialized
// flag.
func TestDiffSchemas_ViewsMismatched_MaterializedFlag(t *testing.T) {
	exp := &Schema{Views: []*View{
		{Name: "v1", Definition: "SELECT 1", Materialized: true},
	}}
	act := &Schema{Views: []*View{
		{Name: "v1", Definition: "SELECT 1", Materialized: false},
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	if len(d.ViewsMismatched) != 1 {
		t.Fatalf("ViewsMismatched len = %d; want 1", len(d.ViewsMismatched))
	}
	got := d.ViewsMismatched[0]
	if got.ExpectedMaterialized == nil || got.ActualMaterialized == nil {
		t.Fatalf("expected both materialized pointers populated, got %+v", got)
	}
	if *got.ExpectedMaterialized != true || *got.ActualMaterialized != false {
		t.Errorf("ExpectedMaterialized=%v ActualMaterialized=%v; want true/false",
			*got.ExpectedMaterialized, *got.ActualMaterialized)
	}
}

// TestDiffSchemas_ViewsSummary verifies the Summary() rollup picks
// up view-level drift counts.
func TestDiffSchemas_ViewsSummary(t *testing.T) {
	exp := &Schema{Views: []*View{
		{Name: "a", Definition: "SELECT 1"},
		{Name: "b", Definition: "SELECT 2"},
	}}
	act := &Schema{Views: []*View{
		{Name: "a", Definition: "SELECT 1 WHERE TRUE"}, // mismatched
		{Name: "c", Definition: "SELECT 3"},            // extra
	}}
	d := DiffSchemas(exp, act, DiffOptions{})
	got := d.Summary()
	for _, want := range []string{"missing view", "extra view", "view mismatch"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q missing %q", got, want)
		}
	}
}
