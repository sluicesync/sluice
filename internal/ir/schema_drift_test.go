// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"strings"
	"testing"
)

// driftTable builds a single-table fixture with the given columns
// (plus an implicit `id INT PRIMARY KEY`). Shared helper across the
// DiffTable test matrix.
func driftTable(name string, cols ...*Column) *Table {
	pk := &Column{Name: "id", Type: Integer{Width: 32}}
	all := append([]*Column{pk}, cols...)
	return &Table{
		Schema:  "public",
		Name:    name,
		Columns: all,
		PrimaryKey: &Index{
			Name:    "pk_" + name,
			Columns: []IndexColumn{{Column: "id"}},
		},
	}
}

// TestDiffTable_NoChanges verifies the zero-drift case — identical
// pre and post produce a report with HasChanges() == false.
func TestDiffTable_NoChanges(t *testing.T) {
	pre := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false})
	r := DiffTable(pre, post)
	if r.HasChanges() {
		t.Errorf("DiffTable on identical inputs reported changes: %+v", r)
	}
}

// TestDiffTable_ColumnAdded covers Class A: column added.
func TestDiffTable_ColumnAdded(t *testing.T) {
	pre := driftTable("users")
	post := driftTable("users", &Column{Name: "nickname", Type: Varchar{Length: 50}, Nullable: true})
	r := DiffTable(pre, post)
	if len(r.ColumnsAdded) != 1 {
		t.Fatalf("ColumnsAdded len = %d, want 1", len(r.ColumnsAdded))
	}
	got := r.ColumnsAdded[0]
	if got.Name != "nickname" || got.Type != "Varchar(50)" || !got.Nullable {
		t.Errorf("ColumnsAdded[0] = %+v; want {nickname, Varchar(50), nullable=true}", got)
	}
	if len(r.ColumnsDropped) != 0 || len(r.ColumnsAltered) != 0 || len(r.ColumnsRenamed) != 0 {
		t.Errorf("unexpected other-column drift: %+v", r)
	}
}

// TestDiffTable_ColumnDropped covers Class A: column dropped. The
// dropped entry's Type/Nullable should be the PRE-side values (so the
// operator sees what they're losing).
func TestDiffTable_ColumnDropped(t *testing.T) {
	pre := driftTable("users", &Column{Name: "legacy", Type: Text{Size: TextRegular}, Nullable: false})
	post := driftTable("users")
	r := DiffTable(pre, post)
	if len(r.ColumnsDropped) != 1 {
		t.Fatalf("ColumnsDropped len = %d, want 1", len(r.ColumnsDropped))
	}
	got := r.ColumnsDropped[0]
	if got.Name != "legacy" || got.Nullable {
		t.Errorf("ColumnsDropped[0] = %+v; want pre-side projection", got)
	}
}

// TestDiffTable_ColumnTypeAltered covers Class A: type change.
func TestDiffTable_ColumnTypeAltered(t *testing.T) {
	pre := driftTable("users", &Column{Name: "score", Type: Integer{Width: 32}, Nullable: false})
	post := driftTable("users", &Column{Name: "score", Type: Integer{Width: 64}, Nullable: false})
	r := DiffTable(pre, post)
	if len(r.ColumnsAltered) != 1 {
		t.Fatalf("ColumnsAltered len = %d, want 1", len(r.ColumnsAltered))
	}
	got := r.ColumnsAltered[0]
	if got.Name != "score" {
		t.Errorf("altered name = %q; want %q", got.Name, "score")
	}
	wantKinds := []ColumnAlterKind{ColumnAlterType}
	if len(got.AlterKinds) != 1 || got.AlterKinds[0] != wantKinds[0] {
		t.Errorf("AlterKinds = %v; want %v", got.AlterKinds, wantKinds)
	}
	if got.Before.Type == got.After.Type {
		t.Errorf("before/after type rendered identically: %s", got.Before.Type)
	}
}

// TestDiffTable_ColumnNullabilityAltered covers Class A: nullability
// change (orthogonal to type change).
func TestDiffTable_ColumnNullabilityAltered(t *testing.T) {
	pre := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: true})
	post := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false})
	r := DiffTable(pre, post)
	if len(r.ColumnsAltered) != 1 {
		t.Fatalf("ColumnsAltered len = %d, want 1", len(r.ColumnsAltered))
	}
	got := r.ColumnsAltered[0]
	if len(got.AlterKinds) != 1 || got.AlterKinds[0] != ColumnAlterNullable {
		t.Errorf("AlterKinds = %v; want [Nullable]", got.AlterKinds)
	}
	if got.Before.Nullable == got.After.Nullable {
		t.Errorf("before/after nullable identical")
	}
}

// TestDiffTable_ColumnDefaultAltered covers Class A: default change.
func TestDiffTable_ColumnDefaultAltered(t *testing.T) {
	pre := driftTable("users", &Column{
		Name: "status", Type: Varchar{Length: 20}, Nullable: false,
		Default: DefaultLiteral{Value: "active"},
	})
	post := driftTable("users", &Column{
		Name: "status", Type: Varchar{Length: 20}, Nullable: false,
		Default: DefaultLiteral{Value: "inactive"},
	})
	r := DiffTable(pre, post)
	if len(r.ColumnsAltered) != 1 {
		t.Fatalf("ColumnsAltered len = %d, want 1", len(r.ColumnsAltered))
	}
	got := r.ColumnsAltered[0]
	if len(got.AlterKinds) != 1 || got.AlterKinds[0] != ColumnAlterDefault {
		t.Errorf("AlterKinds = %v; want [Default]", got.AlterKinds)
	}
}

// TestDiffTable_ColumnTypeAndNullabilityAltered covers the
// multi-kind case — Bug 74 class-matrix discipline: a single column
// can carry more than one AlterKind on the same boundary.
func TestDiffTable_ColumnTypeAndNullabilityAltered(t *testing.T) {
	pre := driftTable("users", &Column{Name: "score", Type: Integer{Width: 32}, Nullable: true})
	post := driftTable("users", &Column{Name: "score", Type: Integer{Width: 64}, Nullable: false})
	r := DiffTable(pre, post)
	if len(r.ColumnsAltered) != 1 {
		t.Fatalf("ColumnsAltered len = %d, want 1", len(r.ColumnsAltered))
	}
	got := r.ColumnsAltered[0]
	if len(got.AlterKinds) != 2 {
		t.Fatalf("AlterKinds = %v; want 2 entries", got.AlterKinds)
	}
	// Order is deterministic by the makeColumnAlterEntry insertion
	// order — type first, nullable second.
	if got.AlterKinds[0] != ColumnAlterType || got.AlterKinds[1] != ColumnAlterNullable {
		t.Errorf("AlterKinds = %v; want [Type, Nullable]", got.AlterKinds)
	}
}

// TestDiffTable_ColumnRenamed covers Class A: rename detection. A
// single add + single drop with otherwise-equal attributes pairs as
// a rename — the dropped+added entries must NOT appear in the
// add/drop slices (no double-counting).
func TestDiffTable_ColumnRenamed(t *testing.T) {
	pre := driftTable("users", &Column{Name: "old_email", Type: Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &Column{Name: "new_email", Type: Varchar{Length: 100}, Nullable: false})
	r := DiffTable(pre, post)
	if len(r.ColumnsRenamed) != 1 {
		t.Fatalf("ColumnsRenamed len = %d, want 1; full: %+v", len(r.ColumnsRenamed), r)
	}
	got := r.ColumnsRenamed[0]
	if got.OldName != "old_email" || got.NewName != "new_email" {
		t.Errorf("rename = %+v; want old_email → new_email", got)
	}
	if len(r.ColumnsAdded) != 0 || len(r.ColumnsDropped) != 0 {
		t.Errorf("rename leaked into add/drop slices: %+v", r)
	}
}

// TestDiffTable_RenameWithTypeChangeIsNotRename covers the
// "different attributes means not a rename" edge — a same-shape
// add/drop with a TYPE difference must NOT pair as a rename; the
// classifier falls through to combo (add + drop) per the v0.78.0
// rename heuristic contract.
func TestDiffTable_RenameWithTypeChangeIsNotRename(t *testing.T) {
	pre := driftTable("users", &Column{Name: "old", Type: Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &Column{Name: "new", Type: Integer{Width: 32}, Nullable: false})
	r := DiffTable(pre, post)
	if len(r.ColumnsRenamed) != 0 {
		t.Errorf("ColumnsRenamed expected empty (type differs); got %+v", r.ColumnsRenamed)
	}
	if len(r.ColumnsAdded) != 1 || len(r.ColumnsDropped) != 1 {
		t.Errorf("ColumnsAdded/Dropped = %d/%d; want 1/1 (drop+add combo)", len(r.ColumnsAdded), len(r.ColumnsDropped))
	}
}

// TestDiffTable_IndexAdded covers Class B: index added (named).
func TestDiffTable_IndexAdded(t *testing.T) {
	pre := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false})
	post.Indexes = []*Index{{
		Name:    "ix_users_email",
		Columns: []IndexColumn{{Column: "email"}},
		Unique:  true,
	}}
	r := DiffTable(pre, post)
	if len(r.IndexesAdded) != 1 {
		t.Fatalf("IndexesAdded len = %d, want 1", len(r.IndexesAdded))
	}
	got := r.IndexesAdded[0]
	if got.Name != "ix_users_email" || got.Columns != "email" || !got.Unique {
		t.Errorf("IndexesAdded[0] = %+v; want {ix_users_email, email, unique}", got)
	}
}

// TestDiffTable_IndexDropped covers Class B: index dropped.
func TestDiffTable_IndexDropped(t *testing.T) {
	pre := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false})
	pre.Indexes = []*Index{{
		Name:    "ix_users_email",
		Columns: []IndexColumn{{Column: "email"}},
		Unique:  false,
	}}
	post := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false})
	r := DiffTable(pre, post)
	if len(r.IndexesDropped) != 1 {
		t.Fatalf("IndexesDropped len = %d, want 1", len(r.IndexesDropped))
	}
	got := r.IndexesDropped[0]
	if got.Name != "ix_users_email" || got.Unique {
		t.Errorf("IndexesDropped[0] = %+v; want non-unique on email", got)
	}
}

// TestDiffTable_CheckConstraint covers Class C: CHECK shapes (add,
// drop, alter). All three on one report — the slices are
// independent.
func TestDiffTable_CheckConstraint(t *testing.T) {
	pre := driftTable("orders", &Column{Name: "qty", Type: Integer{Width: 32}, Nullable: false})
	pre.CheckConstraints = []*CheckConstraint{
		{Name: "qty_positive", Expr: "qty > 0"},
		{Name: "qty_under_1000", Expr: "qty < 1000"},
	}
	post := driftTable("orders", &Column{Name: "qty", Type: Integer{Width: 32}, Nullable: false})
	post.CheckConstraints = []*CheckConstraint{
		{Name: "qty_positive", Expr: "qty >= 1"},       // altered
		{Name: "qty_under_100k", Expr: "qty < 100000"}, // added (and qty_under_1000 dropped)
	}
	r := DiffTable(pre, post)
	if len(r.ChecksAdded) != 1 || r.ChecksAdded[0].Name != "qty_under_100k" {
		t.Errorf("ChecksAdded = %+v; want [qty_under_100k]", r.ChecksAdded)
	}
	if len(r.ChecksDropped) != 1 || r.ChecksDropped[0].Name != "qty_under_1000" {
		t.Errorf("ChecksDropped = %+v; want [qty_under_1000]", r.ChecksDropped)
	}
	if len(r.ChecksAltered) != 1 || r.ChecksAltered[0].Name != "qty_positive" {
		t.Errorf("ChecksAltered = %+v; want [qty_positive]", r.ChecksAltered)
	}
}

// TestDiffTable_ForeignKey covers Class C: foreign key shapes (add,
// drop, alter — by referential action change).
func TestDiffTable_ForeignKey(t *testing.T) {
	pre := driftTable("orders", &Column{Name: "user_id", Type: Integer{Width: 32}, Nullable: false})
	pre.ForeignKeys = []*ForeignKey{
		{
			Name: "fk_orders_user", Columns: []string{"user_id"},
			ReferencedTable: "users", ReferencedColumns: []string{"id"},
			OnDelete: FKActionRestrict, OnUpdate: FKActionRestrict,
		},
	}
	post := driftTable("orders", &Column{Name: "user_id", Type: Integer{Width: 32}, Nullable: false})
	post.ForeignKeys = []*ForeignKey{
		{
			Name: "fk_orders_user", Columns: []string{"user_id"},
			ReferencedTable: "users", ReferencedColumns: []string{"id"},
			OnDelete: FKActionCascade, OnUpdate: FKActionRestrict, // OnDelete changed
		},
	}
	r := DiffTable(pre, post)
	if len(r.ForeignKeysAltered) != 1 {
		t.Fatalf("ForeignKeysAltered len = %d, want 1", len(r.ForeignKeysAltered))
	}
	if r.ForeignKeysAltered[0].Name != "fk_orders_user" {
		t.Errorf("altered FK name = %q; want fk_orders_user", r.ForeignKeysAltered[0].Name)
	}
}

// TestDiffTable_MultiShapeCombo covers the Bug 74 class-matrix
// across categories: add + drop + index in a single boundary.
// Operators see all entries — the renderer doesn't suppress.
func TestDiffTable_MultiShapeCombo(t *testing.T) {
	pre := driftTable(
		"users",
		&Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false},
		&Column{Name: "legacy", Type: Text{Size: TextRegular}, Nullable: true},
	)
	pre.Indexes = []*Index{{
		Name:    "ix_email",
		Columns: []IndexColumn{{Column: "email"}},
	}}
	post := driftTable(
		"users",
		&Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false},
		&Column{Name: "nickname", Type: Varchar{Length: 50}, Nullable: true},
	)
	post.Indexes = []*Index{
		{Name: "ix_email", Columns: []IndexColumn{{Column: "email"}}},
		{Name: "ix_nickname", Columns: []IndexColumn{{Column: "nickname"}}},
	}
	r := DiffTable(pre, post)
	// Column 'legacy' dropped, 'nickname' added. Because both add
	// and drop exist with DIFFERENT attribute sets (varchar vs text),
	// rename pairing is NOT triggered — see Test_RenameWithTypeChange.
	if len(r.ColumnsAdded) != 1 || r.ColumnsAdded[0].Name != "nickname" {
		t.Errorf("ColumnsAdded = %+v; want [nickname]", r.ColumnsAdded)
	}
	if len(r.ColumnsDropped) != 1 || r.ColumnsDropped[0].Name != "legacy" {
		t.Errorf("ColumnsDropped = %+v; want [legacy]", r.ColumnsDropped)
	}
	if len(r.IndexesAdded) != 1 || r.IndexesAdded[0].Name != "ix_nickname" {
		t.Errorf("IndexesAdded = %+v; want [ix_nickname]", r.IndexesAdded)
	}
}

// TestDiffTable_DeterministicOrdering pins the per-slice ordering
// contract: alphabetical by identifying name. Operators paste output
// into tickets, so determinism is load-bearing.
func TestDiffTable_DeterministicOrdering(t *testing.T) {
	pre := driftTable("users")
	post := driftTable(
		"users",
		&Column{Name: "zebra", Type: Varchar{Length: 10}, Nullable: true},
		&Column{Name: "alpha", Type: Varchar{Length: 10}, Nullable: true},
		&Column{Name: "mango", Type: Varchar{Length: 10}, Nullable: true},
	)
	r := DiffTable(pre, post)
	if len(r.ColumnsAdded) != 3 {
		t.Fatalf("ColumnsAdded len = %d, want 3", len(r.ColumnsAdded))
	}
	want := []string{"alpha", "mango", "zebra"}
	for i, c := range r.ColumnsAdded {
		if c.Name != want[i] {
			t.Errorf("ColumnsAdded[%d].Name = %q; want %q", i, c.Name, want[i])
		}
	}
}

// TestDiffTable_DefaultDriftReadability covers the operator-readable
// rendering of DEFAULT values — the rendered form should be stable
// and grep-able. Literal vs no-default vs expression all distinguish.
func TestDiffTable_DefaultDriftReadability(t *testing.T) {
	pre := driftTable("users")
	post := driftTable(
		"users",
		&Column{Name: "literal_def", Type: Integer{Width: 32}, Default: DefaultLiteral{Value: "42"}},
		&Column{Name: "expr_def", Type: Timestamp{}, Default: DefaultExpression{Expr: "now()"}},
		&Column{Name: "no_def", Type: Varchar{Length: 10}, Default: DefaultNone{}},
	)
	r := DiffTable(pre, post)
	got := map[string]string{}
	for _, c := range r.ColumnsAdded {
		got[c.Name] = c.Default
	}
	checks := map[string]string{
		"literal_def": "'42'",
		"expr_def":    "now()",
		"no_def":      "<none>",
	}
	for name, want := range checks {
		if got[name] != want {
			t.Errorf("Default for %q = %q; want %q", name, got[name], want)
		}
	}
}

// TestDiffTable_NilPostTreatsAsTableDrop verifies that a nil post
// surfaces every pre-column as dropped — defensive handling for the
// rare "table dropped" boundary (currently not on the v1 catalog,
// but the diff evaluator should be robust against nil inputs so
// callers don't have to gate).
func TestDiffTable_NilPostTreatsAsTableDrop(t *testing.T) {
	pre := driftTable(
		"users",
		&Column{Name: "email", Type: Varchar{Length: 100}, Nullable: false},
		&Column{Name: "name", Type: Varchar{Length: 100}, Nullable: false},
	)
	r := DiffTable(pre, nil)
	if len(r.ColumnsDropped) != 3 { // id (pk) + email + name
		t.Errorf("ColumnsDropped len = %d, want 3", len(r.ColumnsDropped))
	}
	if r.Schema != "public" || r.Table != "users" {
		t.Errorf("Schema/Table on nil-post = %q/%q; want public/users", r.Schema, r.Table)
	}
}

// TestDiffTable_BothNilReturnsEmpty verifies the (nil, nil) edge.
// Defensive — production callers always have at least one side, but
// returning the zero-value report is the right behaviour.
func TestDiffTable_BothNilReturnsEmpty(t *testing.T) {
	r := DiffTable(nil, nil)
	if r.HasChanges() {
		t.Errorf("DiffTable(nil, nil) reported changes")
	}
}

// TestDiffTable_UnnamedIndexesSkipped verifies the "unnamed indexes
// are skipped" contract — mirrors DiffSchemas's policy. An unnamed
// index added on post-side must not appear in IndexesAdded.
func TestDiffTable_UnnamedIndexesSkipped(t *testing.T) {
	pre := driftTable("users")
	post := driftTable("users")
	post.Indexes = []*Index{{
		Name:    "",
		Columns: []IndexColumn{{Column: "id"}},
	}}
	r := DiffTable(pre, post)
	if len(r.IndexesAdded) != 0 {
		t.Errorf("IndexesAdded should not include unnamed indexes; got %+v", r.IndexesAdded)
	}
}

// TestDiffTable_ContextFieldsPopulated verifies that Schema and Table
// on the report mirror the post-side IR (or pre-side when post is
// nil). Operators rely on these to know which table the rendered
// drift refers to.
func TestDiffTable_ContextFieldsPopulated(t *testing.T) {
	pre := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 100}})
	post := driftTable("users", &Column{Name: "email", Type: Varchar{Length: 200}})
	r := DiffTable(pre, post)
	if r.Schema != "public" {
		t.Errorf("Schema = %q; want public", r.Schema)
	}
	if r.Table != "users" {
		t.Errorf("Table = %q; want users", r.Table)
	}
	// Sanity — the rendered Type strings should differ on the
	// altered entry; if the helper accidentally produced identical
	// strings, the operator-action message would be misleading.
	if len(r.ColumnsAltered) != 1 {
		t.Fatalf("ColumnsAltered len = %d", len(r.ColumnsAltered))
	}
	if r.ColumnsAltered[0].Before.Type == r.ColumnsAltered[0].After.Type {
		t.Errorf("rendered before/after type identical: %q", r.ColumnsAltered[0].Before.Type)
	}
	if !strings.Contains(r.ColumnsAltered[0].Before.Type, "100") {
		t.Errorf("rendered before-type missing length 100: %q", r.ColumnsAltered[0].Before.Type)
	}
}
