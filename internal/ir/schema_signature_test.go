// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestSchemaSignature_TrueDelta pins ADR-0049 DP-1 sign-off point ii:
// a signature change is a TRUE delta only when the (ordered
// column-name, ordered IR-type) tuple actually differs. A no-op
// re-emit (same names + types) must compare Equal so the reader does
// NOT write a no-op history version (retention ∝ DDL count, not
// stream/reconnect count). Bug-74 discipline: the type axis is pinned
// across families AND parameter shapes (varchar length, decimal
// precision, enum value-set, bit width, temporal precision), not one
// representative — a signature that ignores a parameter change is a
// silent-loss (the post-delta schema would never be snapshotted).
func TestSchemaSignature_TrueDelta(t *testing.T) {
	base := &Table{Name: "t", Columns: []*Column{
		{Name: "id", Type: Integer{Width: 64}},
		{Name: "amt", Type: Decimal{Precision: 10, Scale: 2}},
		{Name: "label", Type: Varchar{Length: 32}},
	}}

	same := []struct {
		name string
		tbl  *Table
	}{
		{"identical re-emit", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "amt", Type: Decimal{Precision: 10, Scale: 2}},
			{Name: "label", Type: Varchar{Length: 32}},
		}}},
		// Nullability / default / comment are NOT part of the decode
		// contract — same signature, no new version.
		{"nullability differs only", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}, Nullable: true},
			{Name: "amt", Type: Decimal{Precision: 10, Scale: 2}},
			{Name: "label", Type: Varchar{Length: 32}, Comment: "x"},
		}}},
	}
	for _, s := range same {
		if !SchemaSignatureOf(base).Equal(SchemaSignatureOf(s.tbl)) {
			t.Errorf("%s: want Equal (no-op re-emit), got delta", s.name)
		}
	}

	deltas := []struct {
		name string
		tbl  *Table
	}{
		{"ADD column", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "amt", Type: Decimal{Precision: 10, Scale: 2}},
			{Name: "label", Type: Varchar{Length: 32}},
			{Name: "extra", Type: Boolean{}},
		}}},
		{"DROP column", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "label", Type: Varchar{Length: 32}},
		}}},
		{"RENAME column", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "amount", Type: Decimal{Precision: 10, Scale: 2}},
			{Name: "label", Type: Varchar{Length: 32}},
		}}},
		{"REORDER columns", &Table{Name: "t", Columns: []*Column{
			{Name: "amt", Type: Decimal{Precision: 10, Scale: 2}},
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "label", Type: Varchar{Length: 32}},
		}}},
		{"MODIFY varchar length", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "amt", Type: Decimal{Precision: 10, Scale: 2}},
			{Name: "label", Type: Varchar{Length: 64}},
		}}},
		{"MODIFY decimal precision", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "amt", Type: Decimal{Precision: 12, Scale: 4}},
			{Name: "label", Type: Varchar{Length: 32}},
		}}},
		{"MODIFY int width", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 32}},
			{Name: "amt", Type: Decimal{Precision: 10, Scale: 2}},
			{Name: "label", Type: Varchar{Length: 32}},
		}}},
		{"MODIFY family int→text", &Table{Name: "t", Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "amt", Type: Text{Size: TextLong}},
			{Name: "label", Type: Varchar{Length: 32}},
		}}},
	}
	for _, d := range deltas {
		if SchemaSignatureOf(base).Equal(SchemaSignatureOf(d.tbl)) {
			t.Errorf("%s: want delta, got Equal (no-op) — would silently skip the version", d.name)
		}
	}
}

// TestSchemaSignature_NilAndEmpty: a nil table's signature differs
// from a non-empty one (the first boundary for a table is always a
// true delta — there is no prior version to be equal to).
func TestSchemaSignature_NilAndEmpty(t *testing.T) {
	nilSig := SchemaSignatureOf(nil)
	if !nilSig.Equal(SchemaSignatureOf(&Table{})) {
		t.Error("nil and empty-columns table should share the zero signature")
	}
	nonEmpty := SchemaSignatureOf(&Table{Columns: []*Column{{Name: "a", Type: Integer{Width: 32}}}})
	if nilSig.Equal(nonEmpty) {
		t.Error("nil signature must differ from a non-empty table (first boundary = true delta)")
	}
}

// TestSchemaSnapshot_ChangeContract pins the new sealed Change
// variant: it is NOT a no-op boundary (carries a position + table)
// and its QualifiedName composes schema.table like the row events.
func TestSchemaSnapshot_ChangeContract(t *testing.T) {
	var c Change = SchemaSnapshot{
		Position: Position{Engine: "mysql", Token: "tok"},
		Schema:   "app",
		Table:    "users",
		IR:       &Table{Name: "users"},
	}
	if c.Pos().Token != "tok" {
		t.Errorf("Pos().Token = %q, want tok", c.Pos().Token)
	}
	if c.QualifiedName() != "app.users" {
		t.Errorf("QualifiedName() = %q, want app.users", c.QualifiedName())
	}
}
