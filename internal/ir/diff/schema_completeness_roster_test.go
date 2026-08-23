// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// The IR-completeness roster (audit 2026-08-04 G-6).
//
// # What this gate is for
//
// Three separate findings in one sweep were the SAME defect: the IR grew
// a field to close a named silent-loss item, and [Schemas] was never
// taught to compare it. `Schema.Sequences` (a re-optioned sequence that
// made the target hand out different numbers), `Table.RLSEnabled` and
// `Table.Policies` (a target with row-level security switched off,
// reported "in sync", exit 0). Each was invisible for exactly as long as
// nobody happened to look, because nothing FAILS when a struct gains a
// field.
//
// So this walks [ir.Schema] and [ir.Table] by reflection and requires
// every field to be either PROBED — mutated on one side of an otherwise
// identical pair, with [Schemas] required to report it — or listed in
// the exemption table with a written reason. Adding a field to either
// struct fails this test until somebody makes that choice.
//
// # What it reaches, and what it does not
//
// It reaches the FIELDS OF [ir.Schema] AND [ir.Table] ONLY. It does NOT
// walk [ir.Column], [ir.Index], [ir.ForeignKey], [ir.Policy] or
// [ir.Sequence]: a probe here proves the diff reaches the CONTAINER (a
// column-type change surfaces, so Columns is compared), and says nothing
// about whether every field of the contained struct is. Those rosters
// are [TestContainedIRCompletenessRosterEveryFieldComparedOrExempt]
// (this package; built 2026-08-22, the G-6 follow-on), which also walks
// [ir.IndexColumn] and binds each probe to the nested diff field its
// own comparator populates.
//
// # Why each probe names its expected summary text
//
// A probe that only asserted `HasChanges()` would pass when the mutation
// fired SOME OTHER comparator — the "verification with no independent
// expected value" shape. Requiring the probe to name the summary phrase
// its own comparator produces binds the mutation to the comparison it is
// supposed to prove, so a Policies probe cannot be satisfied by a column
// diff.

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// irFieldProbe mutates one IR field on the actual side of an
// otherwise-identical schema pair and names the summary phrase the
// comparison must produce.
type irFieldProbe struct {
	mutate        func(actual *ir.Schema)
	wantInSummary string
}

// rosterBaseline is the both-sides-identical starting schema. Every
// compared field is populated, so a probe's mutation is the ONLY
// difference between the two sides and a reported drift can only have
// come from that field.
func rosterBaseline() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{
				Schema: "public",
				Name:   "orders",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "tenant_id", Type: ir.Integer{Width: 64}},
				},
				PrimaryKey: &ir.Index{Name: "orders_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
				Indexes: []*ir.Index{
					{Name: "idx_tenant", Columns: []ir.IndexColumn{{Column: "tenant_id"}}},
				},
				ForeignKeys: []*ir.ForeignKey{
					{
						Name:              "fk_orders_tenant",
						Columns:           []string{"tenant_id"},
						ReferencedTable:   "tenants",
						ReferencedColumns: []string{"id"},
					},
				},
				CheckConstraints: []*ir.CheckConstraint{
					{Name: "id_positive", Expr: "id > 0"},
				},
				ExcludeConstraints: []*ir.ExcludeConstraint{
					{Name: "no_overlap", Definition: "EXCLUDE USING gist (tenant_id WITH =)"},
				},
				RLSEnabled: true,
				RLSForced:  true,
				Policies: []*ir.Policy{
					{
						Name:       "tenant_isolation",
						Command:    "ALL",
						Permissive: true,
						Roles:      []string{"app"},
						Using:      "tenant_id = current_setting('app.tenant')::bigint",
						Check:      "tenant_id = current_setting('app.tenant')::bigint",
					},
				},
				Comment: "orders table",
			},
		},
		Views: []*ir.View{
			{Name: "recent_orders", Definition: "SELECT id FROM orders"},
		},
		Sequences: []*ir.Sequence{
			{Name: "order_number_seq", Start: 1000, Increment: 5, MinValue: 1, MaxValue: 1 << 40, Cache: 1},
		},
	}
}

// schemaFieldProbes covers [ir.Schema]. Keys are field names; every field
// not here must appear in schemaFieldExempt.
var schemaFieldProbes = map[string]irFieldProbe{
	"Tables": {
		mutate:        func(s *ir.Schema) { s.Tables = nil },
		wantInSummary: "missing table",
	},
	"Views": {
		mutate:        func(s *ir.Schema) { s.Views[0].Definition = "SELECT id, total FROM orders" },
		wantInSummary: "view mismatch",
	},
	"Sequences": {
		// THE B-10 case: a sequence re-optioned on the target produces
		// different numbers than the source's would, and nothing compared
		// it.
		mutate:        func(s *ir.Schema) { s.Sequences[0].Increment = 1 },
		wantInSummary: "sequence mismatch",
	},
}

var schemaFieldExempt = map[string]string{}

// tableFieldProbes covers [ir.Table].
var tableFieldProbes = map[string]irFieldProbe{
	"Name": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Name = "orders_v2" },
		wantInSummary: "extra table",
	},
	"Columns": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Columns[1].Type = ir.Integer{Width: 32} },
		wantInSummary: "type mismatch",
	},
	"PrimaryKey": {
		mutate: func(s *ir.Schema) {
			s.Tables[0].PrimaryKey.Columns = []ir.IndexColumn{{Column: "tenant_id"}}
		},
		wantInSummary: "index mismatch",
	},
	"Indexes": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Indexes[0].Unique = true },
		wantInSummary: "index mismatch",
	},
	"ForeignKeys": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ForeignKeys[0].OnDelete = ir.FKActionCascade },
		wantInSummary: "foreign-key mismatch",
	},
	"CheckConstraints": {
		mutate:        func(s *ir.Schema) { s.Tables[0].CheckConstraints[0].Expr = "id > 100" },
		wantInSummary: "CHECK mismatch",
	},
	"ExcludeConstraints": {
		mutate:        func(s *ir.Schema) { s.Tables[0].ExcludeConstraints[0].Definition = "EXCLUDE USING gist (id WITH =)" },
		wantInSummary: "EXCLUDE mismatch",
	},
	"RLSEnabled": {
		// THE B-10 case, and the one with the worst consequence: a target
		// with `DISABLE ROW LEVEL SECURITY` reported "in sync", exit 0,
		// while every tenant read every tenant's rows.
		mutate:        func(s *ir.Schema) { s.Tables[0].RLSEnabled = false },
		wantInSummary: "row-level-security mismatch",
	},
	"RLSForced": {
		mutate:        func(s *ir.Schema) { s.Tables[0].RLSForced = false },
		wantInSummary: "row-level-security mismatch",
	},
	"Policies": {
		mutate:        func(s *ir.Schema) { s.Tables[0].Policies[0].Using = "true" },
		wantInSummary: "policy mismatch",
	},
}

var tableFieldExempt = map[string]string{
	"Schema":  "tables are matched by BARE NAME: the diff's target-side reader is pinned to one namespace via --target-schema (ADR-0031), so the schema part carries no discriminating information and comparing it would report every cross-namespace diff as a whole-schema replacement",
	"Comment": "a table comment is documentation, not enforcement — it cannot change which rows are legal, which is the property `schema diff` exists to confirm. Deliberately out of scope, same class as the index METHOD the IndexDiff comment excludes",
}

// probedFieldFloor guards against a walker that has stopped matching the
// types it names. If reflection returned an empty or tiny field list
// every roster entry would be trivially satisfied and this file would
// pass while proving nothing.
const (
	schemaProbedFloor = 3
	tableProbedFloor  = 10
)

// TestIRCompletenessRosterSchemaAndTableFieldsAreComparedOrExempt is the
// G-6 gate. See the file comment for scope: it reaches ir.Schema and
// ir.Table fields ONLY.
func TestIRCompletenessRosterSchemaAndTableFieldsAreComparedOrExempt(t *testing.T) {
	t.Run("ir.Schema", func(t *testing.T) {
		assertRosterCovers(t, reflect.TypeOf(ir.Schema{}), schemaFieldProbes, schemaFieldExempt, schemaProbedFloor)
	})
	t.Run("ir.Table", func(t *testing.T) {
		assertRosterCovers(t, reflect.TypeOf(ir.Table{}), tableFieldProbes, tableFieldExempt, tableProbedFloor)
	})
}

// assertRosterCovers walks typ's exported fields and enforces the three
// properties that make this a gate rather than a list:
//
//  1. Every field is probed or exempted (a NEW field fails).
//  2. Every roster key is a real field (a RENAMED field fails, rather
//     than leaving a stale entry that silently covers nothing).
//  3. At least floor fields are probed (a walker that stopped matching
//     the type fails loudly instead of passing on zero findings).
//
// Then it RUNS each probe: the mutation must make [Schemas] report drift,
// and the summary must name that field's own comparison.
func assertRosterCovers(t *testing.T, typ reflect.Type, probes map[string]irFieldProbe, exempt map[string]string, floor int) {
	t.Helper()

	fields := make(map[string]bool, typ.NumField())
	for i := range typ.NumField() {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		fields[f.Name] = true
	}
	if len(fields) == 0 {
		t.Fatalf("reflection found NO exported fields on %s — the walker has stopped matching the type", typ)
	}

	for name := range fields {
		_, probed := probes[name]
		reason, exempted := exempt[name]
		switch {
		case probed && exempted:
			t.Errorf("%s.%s is both probed and exempted — pick one", typ.Name(), name)
		case !probed && !exempted:
			t.Errorf("%s.%s is neither COMPARED by a probe nor exempted.\n"+
				"  Add a probe to prove diff.Schemas() reports it, or an exemption naming why it cannot change which rows are legal.\n"+
				"  This is the gate that Sequences, RLSEnabled and Policies each slipped past.", typ.Name(), name)
		case exempted && strings.TrimSpace(reason) == "":
			t.Errorf("%s.%s is exempted with an empty reason", typ.Name(), name)
		}
	}

	// Anti-vacuity (2): a roster key naming a field that no longer exists
	// covers nothing, and reads as though it does.
	for name := range probes {
		if !fields[name] {
			t.Errorf("probe roster names %s.%s, which is not a field of the type (renamed or removed?)", typ.Name(), name)
		}
	}
	for name := range exempt {
		if !fields[name] {
			t.Errorf("exemption roster names %s.%s, which is not a field of the type (renamed or removed?)", typ.Name(), name)
		}
	}

	// Anti-vacuity (3).
	if len(probes) < floor {
		t.Errorf("only %d probed field(s) on %s; floor is %d — either the walker broke or fields were mass-exempted",
			len(probes), typ.Name(), floor)
	}

	// The behavioural half: each probe must actually fire.
	for name, p := range probes {
		t.Run(name, func(t *testing.T) {
			expected := rosterBaseline()
			actual := rosterBaseline()

			// Control: identical schemas must report NOTHING. Without this
			// a probe could "pass" on a baseline that already drifts.
			if got := Schemas(expected, rosterBaseline(), Options{}); got.HasChanges() {
				t.Fatalf("baseline compares UNEQUAL to itself (%s) — the probe result would be meaningless", got.Summary())
			}

			p.mutate(actual)
			d := Schemas(expected, actual, Options{})
			if !d.HasChanges() {
				t.Fatalf("mutating %s.%s produced NO drift — the field is not compared", typ.Name(), name)
			}
			if got := d.Summary(); !strings.Contains(got, p.wantInSummary) {
				t.Errorf("mutating %s.%s summarised as %q; want it to contain %q.\n"+
					"  Some comparison fired, but not this field's own — the probe does not prove what it claims.",
					typ.Name(), name, got, p.wantInSummary)
			}
		})
	}
}
