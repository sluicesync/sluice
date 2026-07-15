// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ormSchema models the universal Rails/Laravel/Django shape: every id
// is `bigint unsigned AUTO_INCREMENT PK`, every FK child is
// `bigint unsigned`.
func ormSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
				{Name: "email", Type: ir.Varchar{Length: 255}},
			},
		},
		{
			Name: "posts",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
				{Name: "user_id", Type: ir.Integer{Width: 64, Unsigned: true}},
				{Name: "views", Type: ir.Integer{Width: 32, Unsigned: true}}, // not 64-bit → not flagged
			},
		},
	}}
}

func TestScanUnsignedBigintNotices_ORMShape(t *testing.T) {
	got := ScanUnsignedBigintNotices(ormSchema(), "mysql", "postgres")
	// id (users), id (posts), user_id (posts) — 3 unsigned-bigint
	// columns; `views` is 32-bit unsigned and must NOT be flagged.
	if len(got) != 3 {
		t.Fatalf("notices = %d (%+v); want 3", len(got), got)
	}
	// Sorted by (table, column): posts.id, posts.user_id, users.id.
	want := []UnsignedBigintNotice{
		{Table: "posts", Column: "id", AutoIncrement: true},
		{Table: "posts", Column: "user_id", AutoIncrement: false},
		{Table: "users", Column: "id", AutoIncrement: true},
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("notices[%d] = %+v; want %+v", i, got[i], w)
		}
	}
}

func TestScanUnsignedBigintNotices_NonCrossEngineIsNil(t *testing.T) {
	if got := ScanUnsignedBigintNotices(ormSchema(), "mysql", "mysql"); got != nil {
		t.Errorf("same-engine notices = %+v; want nil", got)
	}
	if got := ScanUnsignedBigintNotices(ormSchema(), "postgres", "mysql"); got != nil {
		t.Errorf("PG→MySQL notices = %+v; want nil (reverse direction unaffected)", got)
	}
	if got := ScanUnsignedBigintNotices(nil, "mysql", "postgres"); got != nil {
		t.Errorf("nil-schema notices = %+v; want nil", got)
	}
}

func TestScanUnsignedBigintNotices_SignedAndOtherWidthsUnaffected(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "signed_big", Type: ir.Integer{Width: 64}},              // signed bigint — not flagged
			{Name: "u_int", Type: ir.Integer{Width: 32, Unsigned: true}},   // unsigned int — not flagged
			{Name: "u_small", Type: ir.Integer{Width: 16, Unsigned: true}}, // unsigned smallint — not flagged
			{Name: "u_big", Type: ir.Integer{Width: 64, Unsigned: true}},   // the only flagged one
			{Name: "dec", Type: ir.Decimal{Precision: 20}},                 // not an Integer
			{Name: "bool", Type: ir.Boolean{}},                             // not an Integer
		},
	}}}
	got := ScanUnsignedBigintNotices(s, "mysql", "postgres")
	if len(got) != 1 || got[0].Column != "u_big" {
		t.Fatalf("notices = %+v; want exactly [t.u_big]", got)
	}
}

func TestScanUnsignedBigintNotices_PlanetScaleSourceCovered(t *testing.T) {
	if got := ScanUnsignedBigintNotices(ormSchema(), "planetscale", "postgres"); len(got) != 3 {
		t.Errorf("planetscale→PG notices = %d; want 3 (PS is MySQL-wire)", len(got))
	}
}

// TestScanUnsignedBigintNotices_EveryMySQLFamilySourceCovered pins the
// Bug 186 class: the SAME schema must produce the SAME advisory through
// every MySQL-family source name — the live driver, the flavors, and
// the mydumper dump reader (which folds through the same translator).
// Bug 186's shape was exactly this asymmetry: WARN via "mysql", silence
// via "mydumper"/"vitess". The family list itself is pinned against the
// engine registry's DDLDialect in mysql_family_registry_test.go.
func TestScanUnsignedBigintNotices_EveryMySQLFamilySourceCovered(t *testing.T) {
	for _, src := range []string{"mysql", "planetscale", "vitess", "mydumper"} {
		if got := ScanUnsignedBigintNotices(ormSchema(), src, "postgres"); len(got) != 3 {
			t.Errorf("%s→PG notices = %d; want 3 (Bug 186: the advisory must not depend on which MySQL-family door the schema came through)", src, len(got))
		}
		if err := UnsignedBigintNoticeError(ormSchema(), src, "postgres", "migrate"); err == nil {
			t.Errorf("%s→PG UnsignedBigintNoticeError = nil; want the advisory (Bug 186)", src)
		}
	}
}

// TestScanMySQLToPGGaps_EveryMySQLFamilySourceCovered pins the sibling
// half of the Bug 186 class sweep: the expression-gap scanner keyed
// strictly on "mysql", silently skipping planetscale/vitess/mydumper
// sources whose IR carries the identical mysql-dialect expressions.
func TestScanMySQLToPGGaps_EveryMySQLFamilySourceCovered(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name: "c", Type: ir.Integer{Width: 64},
			GeneratedExpr:        "SHA2(payload, 256)",
			GeneratedExprDialect: "mysql",
		}},
	}}}
	for _, src := range []string{"mysql", "planetscale", "vitess", "mydumper"} {
		if got := ScanMySQLToPGGaps(s, src, "postgres", nil); len(got) == 0 {
			t.Errorf("%s→PG gaps = 0; want the sha1 gap regardless of the MySQL-family source name (Bug 186 sweep)", src)
		}
	}
}

func TestUnsignedBigintNoticeError_LoudAndActionable(t *testing.T) {
	err := UnsignedBigintNoticeError(ormSchema(), "mysql", "postgres", "migrate")
	if err == nil {
		t.Fatal("UnsignedBigintNoticeError = nil; want a non-nil advisory")
	}
	msg := err.Error()
	for _, want := range []string{
		"migrate",             // contextID surfaced
		"bigint unsigned",     // the source type named
		"bigint",              // the target type named
		"2^63-1",              // the ceiling stated
		"9223372036854775807", // the explicit numeric ceiling
		"deliberate",          // it's a documented policy, not a bug
		"Migration proceeds",  // it's a NOTICE, not a refusal
		"--type-override",     // the escape hatch
		"decimal(20,0)",       // the WORKING remediation token (CLI paren form; not the bogus `=numeric`)
		"posts.user_id",       // names an affected FK child column
		"users.id",            // names an affected PK column
		"AUTO_INCREMENT",      // flags the autoincrement key
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("notice message missing %q\nfull message:\n%s", want, msg)
		}
	}
	// Non-cross-engine pair → nil.
	if UnsignedBigintNoticeError(ormSchema(), "mysql", "mysql", "migrate") != nil {
		t.Error("same-engine UnsignedBigintNoticeError != nil; want nil")
	}
	// Schema with no unsigned-bigint column → nil.
	clean := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}}},
	}}}
	if UnsignedBigintNoticeError(clean, "mysql", "postgres", "schema preview") != nil {
		t.Error("clean-schema UnsignedBigintNoticeError != nil; want nil")
	}
}
