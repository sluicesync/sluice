// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The index-NAME half of the shape-compare retarget (Bug 234).
//
// Scope, stated so these names cannot be read as broader than the truth:
// these grade [RetargetForShapeCompare]'s SECONDARY-index renaming and
// the [PGIndexName] rule it rides. The PRIMARY KEY half of the same axis
// lives in internal/ir/diff (matched by role, not renamed) and is graded
// by TestPrimaryKeyMatchedByRoleAcrossEveryEngineNamingConvention there;
// the end-to-end evidence for both is the migrate-then-diff round-trip
// in internal/pipeline.

// TestPGIndexName_RuleMatrix walks every arm of the rule, not one
// representative: already-prefixed, each convention prefix, the exact-
// suffix edge case, the plain prepend, the >63-byte overflow, and empty.
func TestPGIndexName_RuleMatrix(t *testing.T) {
	longTable := strings.Repeat("t", 40)
	longIndex := strings.Repeat("i", 30)

	cases := []struct {
		name  string
		table string
		src   string
		want  string
	}{
		{"empty source name", "orders", "", ""},
		{"plain prepend", "orders", "idx_email", "orders_idx_email"},
		{"already table-prefixed", "orders", "orders_email_idx", "orders_email_idx"},
		{"convention prefix ix_", "orders", "ix_orders_email", "ix_orders_email"},
		{"convention prefix idx_", "orders", "idx_orders_email", "idx_orders_email"},
		{"convention prefix uix_", "orders", "uix_orders_email", "uix_orders_email"},
		{"convention prefix uidx_", "orders", "uidx_orders_email", "uidx_orders_email"},
		{"convention prefix uniq_", "orders", "uniq_orders_email", "uniq_orders_email"},
		{"convention prefix uq_", "orders", "uq_orders_email", "uq_orders_email"},
		{"convention prefix pk_", "orders", "pk_orders_email", "pk_orders_email"},
		{"convention prefix fk_", "orders", "fk_orders_email", "fk_orders_email"},
		{"convention prefix chk_", "orders", "chk_orders_email", "chk_orders_email"},
		{"convention prefix ck_", "orders", "ck_orders_email", "ck_orders_email"},
		{"convention prefix exact suffix", "orders", "idx_orders", "idx_orders"},
		{"prepend would overflow 63 bytes", longTable, longIndex, longIndex},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PGIndexName(tc.table, tc.src); got != tc.want {
				t.Errorf("PGIndexName(%q, %q) = %q; want %q", tc.table, tc.src, got, tc.want)
			}
		})
	}

	// The overflow arm has to actually overflow, or it is grading the
	// plain-prepend arm under another name.
	if len(longTable+"_"+longIndex) <= MaxPGIdentifierLen {
		t.Fatalf("setup: %q + %q is %d bytes, not over the %d-byte limit — the overflow case is vacuous",
			longTable, longIndex, len(longTable+"_"+longIndex), MaxPGIdentifierLen)
	}
}

// TestPGIndexName_IsIdempotent is the property the expected side depends
// on: [RetargetForShapeCompare] runs the transformation over schemas
// read from a Postgres SOURCE too, whose names have already been through
// it once. A non-idempotent rule would double-prefix them and invent the
// very phantom drift this closes.
func TestPGIndexName_IsIdempotent(t *testing.T) {
	for _, src := range []string{
		"idx_email", "orders_email_idx", "ix_orders_email", "idx_orders",
		strings.Repeat("i", 60),
	} {
		once := PGIndexName("orders", src)
		if twice := PGIndexName("orders", once); twice != once {
			t.Errorf("PGIndexName is not idempotent for %q: once=%q twice=%q", src, once, twice)
		}
	}
}

// TestPGEffectiveIndexName_ConstraintBackedIsVerbatim pins the one
// carve-out: a constraint-backed unique index is re-created as `ALTER
// TABLE … ADD CONSTRAINT <source name>`, so the catalog holds the source
// name and the expected side must NOT prefix it. Getting this backwards
// would trade one phantom for another.
func TestPGEffectiveIndexName_ConstraintBackedIsVerbatim(t *testing.T) {
	plain := &ir.Index{Name: "uq_email", Unique: true}
	backed := &ir.Index{Name: "uq_email", Unique: true, ConstraintBacked: true}

	if got, want := PGEffectiveIndexName("orders", plain), "orders_uq_email"; got != want {
		t.Errorf("plain unique index: got %q; want %q", got, want)
	}
	if got, want := PGEffectiveIndexName("orders", backed), "uq_email"; got != want {
		t.Errorf("constraint-backed unique index: got %q; want %q", got, want)
	}
	if got := PGEffectiveIndexName("orders", nil); got != "" {
		t.Errorf("nil index: got %q; want empty", got)
	}
}

// TestRetargetForShapeCompare_RenamesIndexesForAPostgresTarget is the
// load-bearing assertion for the MySQL/SQLite→Postgres direction of Bug
// 234, and for the SAME-engine postgres→postgres pair, where no type
// rule fires at all and the pre-fix entry point returned the schema
// untouched.
func TestRetargetForShapeCompare_RenamesIndexesForAPostgresTarget(t *testing.T) {
	build := func() *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name:    "orders",
			Columns: []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 255}}},
			PrimaryKey: &ir.Index{
				Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{
				{Name: "idx_email", Columns: []ir.IndexColumn{{Column: "email"}}},
				{Name: "orders_created_idx", Columns: []ir.IndexColumn{{Column: "created_at"}}},
				{Name: "uq_email", Unique: true, ConstraintBacked: true},
			},
		}}}
	}

	cases := []struct {
		name       string
		src, tgt   string
		wantFirst  string
		wantSecond string
		wantThird  string
	}{
		{"mysql to postgres", "mysql", "postgres", "orders_idx_email", "orders_created_idx", "uq_email"},
		{"sqlite to postgres", "sqlite", "postgres", "orders_idx_email", "orders_created_idx", "uq_email"},
		{"postgres to postgres", "postgres", "postgres", "orders_idx_email", "orders_created_idx", "uq_email"},
		{"postgres to postgres-trigger", "postgres", "postgres-trigger", "orders_idx_email", "orders_created_idx", "uq_email"},
		// Verbatim targets: the names must survive UNCHANGED. A rule
		// that fired here would invent drift on the pair Bug 234 was
		// filed from.
		{"postgres to mysql", "postgres", "mysql", "idx_email", "orders_created_idx", "uq_email"},
		{"postgres to vitess", "postgres", "vitess", "idx_email", "orders_created_idx", "uq_email"},
		{"mysql to sqlite", "mysql", "sqlite", "idx_email", "orders_created_idx", "uq_email"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := build()
			out := RetargetForShapeCompare(in, tc.src, tc.tgt)
			got := out.Tables[0].Indexes
			if got[0].Name != tc.wantFirst || got[1].Name != tc.wantSecond || got[2].Name != tc.wantThird {
				t.Errorf("index names = %q/%q/%q; want %q/%q/%q",
					got[0].Name, got[1].Name, got[2].Name, tc.wantFirst, tc.wantSecond, tc.wantThird)
			}
			// The PRIMARY KEY is never renamed — that half of the axis
			// is closed by role matching in internal/ir/diff, and a
			// rename here would fight it.
			if out.Tables[0].PrimaryKey.Name != "PRIMARY" {
				t.Errorf("primary key renamed to %q; the retarget must leave it alone",
					out.Tables[0].PrimaryKey.Name)
			}
			// The INPUT must not be mutated: the diff orchestrator keeps
			// the untouched schema for the DDL-suggestion side.
			if in.Tables[0].Indexes[0].Name != "idx_email" {
				t.Errorf("input schema mutated: index[0] is now %q", in.Tables[0].Indexes[0].Name)
			}
		})
	}
}

// TestRetargetForEngine_LeavesIndexNamesAlone pins the lane split: the
// EMIT entry point must not pre-apply the naming transformation, because
// the target's own DDL emitter applies it as it writes.
func TestRetargetForEngine_LeavesIndexNamesAlone(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name:    "orders",
		Indexes: []*ir.Index{{Name: "idx_email"}},
	}}}
	out := RetargetForEngine(s, "mysql", "postgres")
	if got := out.Tables[0].Indexes[0].Name; got != "idx_email" {
		t.Errorf("RetargetForEngine renamed the index to %q; the emit lane must leave names alone", got)
	}
}
