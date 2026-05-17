// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func col(name string, t ir.Type) *ir.Column {
	return &ir.Column{Name: name, Type: t}
}

func TestNotesFor_SameEngine_Empty(t *testing.T) {
	src := col("id", ir.UUID{})
	tgt := col("id", ir.UUID{})
	got := NotesFor(src, tgt, "postgres", "postgres")
	if got != nil {
		t.Errorf("expected nil for same-engine; got %#v", got)
	}
}

func TestNotesFor_NilColumns_Empty(t *testing.T) {
	if NotesFor(nil, col("x", ir.UUID{}), "postgres", "mysql") != nil {
		t.Errorf("expected nil for nil src col")
	}
	if NotesFor(col("x", ir.UUID{}), nil, "postgres", "mysql") != nil {
		t.Errorf("expected nil for nil tgt col")
	}
}

func TestNotesFor_MySQLJSON_To_PG_JSONB_HasMessage(t *testing.T) {
	src := col("data", ir.JSON{})
	tgt := col("data", ir.JSON{Binary: true})
	got := NotesFor(src, tgt, "mysql", "postgres")
	if len(got) != 1 {
		t.Fatalf("expected 1 note; got %d (%#v)", len(got), got)
	}
	if got[0].Column != "data" {
		t.Errorf("column = %q; want data", got[0].Column)
	}
	if got[0].Message == "" {
		t.Errorf("expected non-empty note message for json -> jsonb")
	}
	if !strings.Contains(strings.ToLower(got[0].Message), "key-order") {
		t.Errorf("message = %q; want mention of key-order semantics", got[0].Message)
	}
	if got[0].SourceType != "json" {
		t.Errorf("source = %q; want json", got[0].SourceType)
	}
	if got[0].TargetType != "jsonb" {
		t.Errorf("target = %q; want jsonb", got[0].TargetType)
	}
}

func TestNotesFor_TypeChange_BareNote(t *testing.T) {
	// PG uuid -> MySQL char(36): no per-pair note message in registry,
	// but the bare note (source/target rendering) still emits.
	src := col("id", ir.UUID{})
	tgt := col("id", ir.Char{Length: 36})
	got := NotesFor(src, tgt, "postgres", "mysql")
	if len(got) != 1 {
		t.Fatalf("expected 1 note; got %d", len(got))
	}
	if got[0].SourceType != "uuid" {
		t.Errorf("source = %q; want uuid", got[0].SourceType)
	}
	if got[0].TargetType != "char(36)" {
		t.Errorf("target = %q; want char(36)", got[0].TargetType)
	}
	if got[0].Message != "" {
		// No registry message on this pair; bare note should have empty Message.
		t.Errorf("expected bare note (empty Message); got %q", got[0].Message)
	}
}

func TestNotesFor_NoTypeChange_NoNote(t *testing.T) {
	// VARCHAR(255) on both sides — same rendering — no note.
	src := col("email", ir.Varchar{Length: 255})
	tgt := col("email", ir.Varchar{Length: 255})
	got := NotesFor(src, tgt, "mysql", "postgres")
	if got != nil {
		t.Errorf("expected nil note for same rendering; got %#v", got)
	}
}

func TestHintsFor_PG_UUID_to_MySQL(t *testing.T) {
	src := col("id", ir.UUID{})
	tgt := col("id", ir.Char{Length: 36})
	got := HintsFor("users", src, tgt, "postgres", "mysql")
	if len(got) != 1 {
		t.Fatalf("expected 1 hint; got %d (%#v)", len(got), got)
	}
	if got[0].Column != "id" {
		t.Errorf("column = %q; want id", got[0].Column)
	}
	if got[0].SuggestedOverride != "--type-override users.id=binary_uuid" {
		t.Errorf("override = %q; want --type-override users.id=binary_uuid", got[0].SuggestedOverride)
	}
	if !strings.Contains(strings.ToLower(got[0].Message), "binary") {
		t.Errorf("message = %q; want mention of binary", got[0].Message)
	}
}

// TestHintsFor_MySQL_ENUM_to_PG_NoHint confirms that MySQL ENUM →
// PG does not fire a hint. Sluice's PG writer emits a real
// `CREATE TYPE … AS ENUM` by default (see ddl_emit.go), so there is
// no operator-preferable alternative to surface. The original
// ADR-0024 entry anticipated a TEXT+CHECK default and pointed at a
// `pg_enum` override that does not exist; removing the entry keeps
// the registry honest.
func TestHintsFor_MySQL_ENUM_to_PG_NoHint(t *testing.T) {
	src := col("status", ir.Enum{Values: []string{"a", "b"}})
	tgt := col("status", ir.Enum{Values: []string{"a", "b"}})
	got := HintsFor("orders", src, tgt, "mysql", "postgres")
	if len(got) != 0 {
		t.Errorf("expected 0 hints; got %d (%#v)", len(got), got)
	}
}

func TestHintsFor_PG_TEXT_to_MySQL(t *testing.T) {
	src := col("body", ir.Text{Size: ir.TextLong})
	tgt := col("body", ir.Text{Size: ir.TextLong})
	got := HintsFor("posts", src, tgt, "postgres", "mysql")
	if len(got) != 1 {
		t.Fatalf("expected 1 hint; got %d (%#v)", len(got), got)
	}
	if got[0].SuggestedOverride != "--type-override posts.body=mediumtext" {
		t.Errorf("override = %q; want mediumtext suggestion", got[0].SuggestedOverride)
	}
}

func TestHintsFor_MySQL_JSON_to_PG_NoHint(t *testing.T) {
	// JSONB is the right default; no hint, only a note (covered above).
	src := col("data", ir.JSON{})
	tgt := col("data", ir.JSON{Binary: true})
	got := HintsFor("events", src, tgt, "mysql", "postgres")
	if len(got) != 0 {
		t.Errorf("expected no hint for json->jsonb (only a note); got %d (%#v)", len(got), got)
	}
}

func TestHintsFor_MySQL_DATETIME_to_PG(t *testing.T) {
	src := col("created_at", ir.DateTime{Precision: 6})
	tgt := col("created_at", ir.Timestamp{Precision: 6})
	got := HintsFor("events", src, tgt, "mysql", "postgres")
	if len(got) != 1 {
		t.Fatalf("expected 1 hint; got %d", len(got))
	}
	if got[0].SuggestedOverride != "--type-override events.created_at=timestamptz" {
		t.Errorf("override = %q; want timestamptz suggestion", got[0].SuggestedOverride)
	}
}

func TestHintsFor_PG_UnboundedNumeric_to_MySQL(t *testing.T) {
	src := col("amount", ir.Decimal{Unconstrained: true})
	tgt := col("amount", ir.Decimal{Precision: 65, Scale: 30})
	got := HintsFor("ledger", src, tgt, "postgres", "mysql")
	if len(got) != 1 {
		t.Fatalf("expected 1 hint; got %d (%#v)", len(got), got)
	}
	if !strings.Contains(got[0].SuggestedOverride, "decimal:precision=N,scale=M") {
		t.Errorf("override = %q; want decimal:precision=N,scale=M template", got[0].SuggestedOverride)
	}
}

func TestHintsFor_BoundedNumeric_NoHint(t *testing.T) {
	// PG numeric(10,2) — bounded — no hint.
	src := col("price", ir.Decimal{Precision: 10, Scale: 2})
	tgt := col("price", ir.Decimal{Precision: 10, Scale: 2})
	got := HintsFor("products", src, tgt, "postgres", "mysql")
	if len(got) != 0 {
		t.Errorf("expected no hint for bounded numeric; got %d (%#v)", len(got), got)
	}
}

func TestHintsFor_SameEngine_Empty(t *testing.T) {
	src := col("id", ir.UUID{})
	tgt := col("id", ir.UUID{})
	got := HintsFor("users", src, tgt, "postgres", "postgres")
	if got != nil {
		t.Errorf("expected nil for same-engine; got %#v", got)
	}
}

func TestHintsFor_PlanetScaleAlias(t *testing.T) {
	// PlanetScale should fire the same hints as vanilla MySQL.
	src := col("id", ir.UUID{})
	tgt := col("id", ir.Char{Length: 36})
	got := HintsFor("users", src, tgt, "postgres", "planetscale")
	if len(got) != 1 {
		t.Fatalf("expected hint to fire for planetscale target; got %d", len(got))
	}
}

func TestRenderTypeForNote_Coverage(t *testing.T) {
	cases := []struct {
		t      ir.Type
		engine string
		want   string
	}{
		{ir.Boolean{}, "mysql", "tinyint(1)"},
		{ir.Boolean{}, "postgres", "boolean"},
		{ir.Integer{Width: 64, Unsigned: true}, "mysql", "bigint unsigned"},
		// Bug 11: `bigint unsigned` renders as PG `bigint` (uniform
		// mapping), matching the DDL emitter. The (2^63, 2^64) loss is
		// surfaced via the dedicated unsigned-bigint notice, not a
		// divergent type rendering.
		{ir.Integer{Width: 64, Unsigned: true}, "postgres", "bigint"},
		{ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}, "postgres", "bigint"},
		{ir.UUID{}, "postgres", "uuid"},
		{ir.UUID{}, "mysql", "char(36)"},
		{ir.JSON{Binary: true}, "postgres", "jsonb"},
		{ir.JSON{}, "mysql", "json"},
		{ir.Text{Size: ir.TextLong}, "postgres", "text"},
		{ir.Text{Size: ir.TextLong}, "mysql", "longtext"},
		{ir.DateTime{Precision: 6}, "postgres", "timestamp(6)"},
		{ir.DateTime{Precision: 6}, "mysql", "datetime(6)"},
		{ir.Timestamp{Precision: 6, WithTimeZone: true}, "postgres", "timestamp(6)tz"},
		{ir.Decimal{Unconstrained: true}, "postgres", "numeric"},
		{ir.Decimal{Unconstrained: true}, "mysql", "decimal(65,30)"},
		{ir.Decimal{Precision: 10, Scale: 2}, "postgres", "numeric(10,2)"},
		{ir.Decimal{Precision: 10, Scale: 2}, "mysql", "decimal(10,2)"},
	}
	for _, tc := range cases {
		got := renderTypeForNote(tc.t, tc.engine)
		if got != tc.want {
			t.Errorf("renderTypeForNote(%T, %q) = %q; want %q", tc.t, tc.engine, got, tc.want)
		}
	}
}
