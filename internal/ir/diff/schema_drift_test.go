// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// driftTable builds a single-table fixture with the given columns
// (plus an implicit `id INT PRIMARY KEY`). Shared helper across the
// TableDrift test matrix.
func driftTable(name string, cols ...*ir.Column) *ir.Table {
	pk := &ir.Column{Name: "id", Type: ir.Integer{Width: 32}}
	all := append([]*ir.Column{pk}, cols...)
	return &ir.Table{
		Schema:  "public",
		Name:    name,
		Columns: all,
		PrimaryKey: &ir.Index{
			Name:    "pk_" + name,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
}

// TestTableDrift_NoChanges verifies the zero-drift case — identical
// pre and post produce a report with HasChanges() == false.
func TestTableDrift_NoChanges(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false})
	r := TableDrift(pre, post)
	if r.HasChanges() {
		t.Errorf("TableDrift on identical inputs reported changes: %+v", r)
	}
}

// TestTableDrift_ColumnAdded covers Class A: column added.
func TestTableDrift_ColumnAdded(t *testing.T) {
	pre := driftTable("users")
	post := driftTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 50}, Nullable: true})
	r := TableDrift(pre, post)
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

// TestTableDrift_ColumnDropped covers Class A: column dropped. The
// dropped entry's Type/Nullable should be the PRE-side values (so the
// operator sees what they're losing).
func TestTableDrift_ColumnDropped(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "legacy", Type: ir.Text{Size: ir.TextRegular}, Nullable: false})
	post := driftTable("users")
	r := TableDrift(pre, post)
	if len(r.ColumnsDropped) != 1 {
		t.Fatalf("ColumnsDropped len = %d, want 1", len(r.ColumnsDropped))
	}
	got := r.ColumnsDropped[0]
	if got.Name != "legacy" || got.Nullable {
		t.Errorf("ColumnsDropped[0] = %+v; want pre-side projection", got)
	}
}

// TestTableDrift_ColumnTypeAltered covers Class A: type change.
func TestTableDrift_ColumnTypeAltered(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "score", Type: ir.Integer{Width: 32}, Nullable: false})
	post := driftTable("users", &ir.Column{Name: "score", Type: ir.Integer{Width: 64}, Nullable: false})
	r := TableDrift(pre, post)
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

// TestTableDrift_ColumnNullabilityAltered covers Class A: nullability
// change (orthogonal to type change).
func TestTableDrift_ColumnNullabilityAltered(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: true})
	post := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false})
	r := TableDrift(pre, post)
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

// TestTableDrift_ColumnDefaultAltered covers Class A: default change.
func TestTableDrift_ColumnDefaultAltered(t *testing.T) {
	pre := driftTable("users", &ir.Column{
		Name: "status", Type: ir.Varchar{Length: 20}, Nullable: false,
		Default: ir.DefaultLiteral{Value: "active"},
	})
	post := driftTable("users", &ir.Column{
		Name: "status", Type: ir.Varchar{Length: 20}, Nullable: false,
		Default: ir.DefaultLiteral{Value: "inactive"},
	})
	r := TableDrift(pre, post)
	if len(r.ColumnsAltered) != 1 {
		t.Fatalf("ColumnsAltered len = %d, want 1", len(r.ColumnsAltered))
	}
	got := r.ColumnsAltered[0]
	if len(got.AlterKinds) != 1 || got.AlterKinds[0] != ColumnAlterDefault {
		t.Errorf("AlterKinds = %v; want [Default]", got.AlterKinds)
	}
}

// TestTableDrift_ColumnTypeAndNullabilityAltered covers the
// multi-kind case — Bug 74 class-matrix discipline: a single column
// can carry more than one AlterKind on the same boundary.
func TestTableDrift_ColumnTypeAndNullabilityAltered(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "score", Type: ir.Integer{Width: 32}, Nullable: true})
	post := driftTable("users", &ir.Column{Name: "score", Type: ir.Integer{Width: 64}, Nullable: false})
	r := TableDrift(pre, post)
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

// TestTableDrift_ColumnRenamed covers Class A: rename detection. A
// single add + single drop with otherwise-equal attributes pairs as
// a rename — the dropped+added entries must NOT appear in the
// add/drop slices (no double-counting).
func TestTableDrift_ColumnRenamed(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "old_email", Type: ir.Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &ir.Column{Name: "new_email", Type: ir.Varchar{Length: 100}, Nullable: false})
	r := TableDrift(pre, post)
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

// TestTableDrift_RenameWithTypeChangeIsNotRename covers the
// "different attributes means not a rename" edge — a same-shape
// add/drop with a TYPE difference must NOT pair as a rename; the
// classifier falls through to combo (add + drop) per the v0.78.0
// rename heuristic contract.
func TestTableDrift_RenameWithTypeChangeIsNotRename(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "old", Type: ir.Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &ir.Column{Name: "new", Type: ir.Integer{Width: 32}, Nullable: false})
	r := TableDrift(pre, post)
	if len(r.ColumnsRenamed) != 0 {
		t.Errorf("ColumnsRenamed expected empty (type differs); got %+v", r.ColumnsRenamed)
	}
	if len(r.ColumnsAdded) != 1 || len(r.ColumnsDropped) != 1 {
		t.Errorf("ColumnsAdded/Dropped = %d/%d; want 1/1 (drop+add combo)", len(r.ColumnsAdded), len(r.ColumnsDropped))
	}
}

// TestTableDrift_IndexAdded covers Class B: index added (named).
func TestTableDrift_IndexAdded(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false})
	post := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false})
	post.Indexes = []*ir.Index{{
		Name:    "ix_users_email",
		Columns: []ir.IndexColumn{{Column: "email"}},
		Unique:  true,
	}}
	r := TableDrift(pre, post)
	if len(r.IndexesAdded) != 1 {
		t.Fatalf("IndexesAdded len = %d, want 1", len(r.IndexesAdded))
	}
	got := r.IndexesAdded[0]
	if got.Name != "ix_users_email" || got.Columns != "email" || !got.Unique {
		t.Errorf("IndexesAdded[0] = %+v; want {ix_users_email, email, unique}", got)
	}
}

// TestTableDrift_IndexDropped covers Class B: index dropped.
func TestTableDrift_IndexDropped(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false})
	pre.Indexes = []*ir.Index{{
		Name:    "ix_users_email",
		Columns: []ir.IndexColumn{{Column: "email"}},
		Unique:  false,
	}}
	post := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false})
	r := TableDrift(pre, post)
	if len(r.IndexesDropped) != 1 {
		t.Fatalf("IndexesDropped len = %d, want 1", len(r.IndexesDropped))
	}
	got := r.IndexesDropped[0]
	if got.Name != "ix_users_email" || got.Unique {
		t.Errorf("IndexesDropped[0] = %+v; want non-unique on email", got)
	}
}

// TestTableDrift_CheckConstraint covers Class C: CHECK shapes (add,
// drop, alter). All three on one report — the slices are
// independent.
func TestTableDrift_CheckConstraint(t *testing.T) {
	pre := driftTable("orders", &ir.Column{Name: "qty", Type: ir.Integer{Width: 32}, Nullable: false})
	pre.CheckConstraints = []*ir.CheckConstraint{
		{Name: "qty_positive", Expr: "qty > 0"},
		{Name: "qty_under_1000", Expr: "qty < 1000"},
	}
	post := driftTable("orders", &ir.Column{Name: "qty", Type: ir.Integer{Width: 32}, Nullable: false})
	post.CheckConstraints = []*ir.CheckConstraint{
		{Name: "qty_positive", Expr: "qty >= 1"},       // altered
		{Name: "qty_under_100k", Expr: "qty < 100000"}, // added (and qty_under_1000 dropped)
	}
	r := TableDrift(pre, post)
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

// TestTableDrift_ForeignKey covers Class C: foreign key shapes (add,
// drop, alter — by referential action change).
func TestTableDrift_ForeignKey(t *testing.T) {
	pre := driftTable("orders", &ir.Column{Name: "user_id", Type: ir.Integer{Width: 32}, Nullable: false})
	pre.ForeignKeys = []*ir.ForeignKey{
		{
			Name: "fk_orders_user", Columns: []string{"user_id"},
			ReferencedTable: "users", ReferencedColumns: []string{"id"},
			OnDelete: ir.FKActionRestrict, OnUpdate: ir.FKActionRestrict,
		},
	}
	post := driftTable("orders", &ir.Column{Name: "user_id", Type: ir.Integer{Width: 32}, Nullable: false})
	post.ForeignKeys = []*ir.ForeignKey{
		{
			Name: "fk_orders_user", Columns: []string{"user_id"},
			ReferencedTable: "users", ReferencedColumns: []string{"id"},
			OnDelete: ir.FKActionCascade, OnUpdate: ir.FKActionRestrict, // OnDelete changed
		},
	}
	r := TableDrift(pre, post)
	if len(r.ForeignKeysAltered) != 1 {
		t.Fatalf("ForeignKeysAltered len = %d, want 1", len(r.ForeignKeysAltered))
	}
	if r.ForeignKeysAltered[0].Name != "fk_orders_user" {
		t.Errorf("altered FK name = %q; want fk_orders_user", r.ForeignKeysAltered[0].Name)
	}
}

// TestTableDrift_MultiShapeCombo covers the Bug 74 class-matrix
// across categories: add + drop + index in a single boundary.
// Operators see all entries — the renderer doesn't suppress.
func TestTableDrift_MultiShapeCombo(t *testing.T) {
	pre := driftTable(
		"users",
		&ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false},
		&ir.Column{Name: "legacy", Type: ir.Text{Size: ir.TextRegular}, Nullable: true},
	)
	pre.Indexes = []*ir.Index{{
		Name:    "ix_email",
		Columns: []ir.IndexColumn{{Column: "email"}},
	}}
	post := driftTable(
		"users",
		&ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false},
		&ir.Column{Name: "nickname", Type: ir.Varchar{Length: 50}, Nullable: true},
	)
	post.Indexes = []*ir.Index{
		{Name: "ix_email", Columns: []ir.IndexColumn{{Column: "email"}}},
		{Name: "ix_nickname", Columns: []ir.IndexColumn{{Column: "nickname"}}},
	}
	r := TableDrift(pre, post)
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

// TestTableDrift_DeterministicOrdering pins the per-slice ordering
// contract: alphabetical by identifying name. Operators paste output
// into tickets, so determinism is load-bearing.
func TestTableDrift_DeterministicOrdering(t *testing.T) {
	pre := driftTable("users")
	post := driftTable(
		"users",
		&ir.Column{Name: "zebra", Type: ir.Varchar{Length: 10}, Nullable: true},
		&ir.Column{Name: "alpha", Type: ir.Varchar{Length: 10}, Nullable: true},
		&ir.Column{Name: "mango", Type: ir.Varchar{Length: 10}, Nullable: true},
	)
	r := TableDrift(pre, post)
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

// TestTableDrift_DefaultDriftReadability covers the operator-readable
// rendering of DEFAULT values — the rendered form should be stable
// and grep-able. Literal vs no-default vs expression all distinguish.
func TestTableDrift_DefaultDriftReadability(t *testing.T) {
	pre := driftTable("users")
	post := driftTable(
		"users",
		&ir.Column{Name: "literal_def", Type: ir.Integer{Width: 32}, Default: ir.DefaultLiteral{Value: "42"}},
		&ir.Column{Name: "expr_def", Type: ir.Timestamp{}, Default: ir.DefaultExpression{Expr: "now()"}},
		&ir.Column{Name: "no_def", Type: ir.Varchar{Length: 10}, Default: ir.DefaultNone{}},
	)
	r := TableDrift(pre, post)
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

// TestTableDrift_NilPostTreatsAsTableDrop verifies that a nil post
// surfaces every pre-column as dropped — defensive handling for the
// rare "table dropped" boundary (currently not on the v1 catalog,
// but the diff evaluator should be robust against nil inputs so
// callers don't have to gate).
func TestTableDrift_NilPostTreatsAsTableDrop(t *testing.T) {
	pre := driftTable(
		"users",
		&ir.Column{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false},
		&ir.Column{Name: "name", Type: ir.Varchar{Length: 100}, Nullable: false},
	)
	r := TableDrift(pre, nil)
	if len(r.ColumnsDropped) != 3 { // id (pk) + email + name
		t.Errorf("ColumnsDropped len = %d, want 3", len(r.ColumnsDropped))
	}
	if r.Schema != "public" || r.Table != "users" {
		t.Errorf("Schema/Table on nil-post = %q/%q; want public/users", r.Schema, r.Table)
	}
}

// TestTableDrift_BothNilReturnsEmpty verifies the (nil, nil) edge.
// Defensive — production callers always have at least one side, but
// returning the zero-value report is the right behaviour.
func TestTableDrift_BothNilReturnsEmpty(t *testing.T) {
	r := TableDrift(nil, nil)
	if r.HasChanges() {
		t.Errorf("TableDrift(nil, nil) reported changes")
	}
}

// TestTableDrift_UnnamedIndexesSkipped verifies the "unnamed indexes
// are skipped" contract — mirrors Schemas's policy. An unnamed
// index added on post-side must not appear in IndexesAdded.
func TestTableDrift_UnnamedIndexesSkipped(t *testing.T) {
	pre := driftTable("users")
	post := driftTable("users")
	post.Indexes = []*ir.Index{{
		Name:    "",
		Columns: []ir.IndexColumn{{Column: "id"}},
	}}
	r := TableDrift(pre, post)
	if len(r.IndexesAdded) != 0 {
		t.Errorf("IndexesAdded should not include unnamed indexes; got %+v", r.IndexesAdded)
	}
}

// TestTableDrift_ContextFieldsPopulated verifies that Schema and Table
// on the report mirror the post-side IR (or pre-side when post is
// nil). Operators rely on these to know which table the rendered
// drift refers to.
func TestTableDrift_ContextFieldsPopulated(t *testing.T) {
	pre := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 100}})
	post := driftTable("users", &ir.Column{Name: "email", Type: ir.Varchar{Length: 200}})
	r := TableDrift(pre, post)
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
