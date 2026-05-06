package ir

import (
	"reflect"
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
