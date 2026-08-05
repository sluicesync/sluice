// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The diff-OUTPUT completeness roster (audit 2026-08-04 G-6, render half).
//
// # What this gate is for
//
// [irdiff.Schemas] can see a delta and the command can exit 1 while the
// operator is told NOTHING about it. That shipped: v0.112.0 added the
// foreign-key comparison and wired HasChanges, but no text section and no
// summary counter — so `schema diff` printed a per-table header with
// nothing under it, and a CI consumer keying off `summary` read zeros
// while the process exited non-zero (Bug 227). EXCLUDE constraints had
// been in the same state since ADR-0053 and nobody had noticed, which is
// the whole argument for a gate over a review.
//
// So this walks [irdiff.TableDiff] and [irdiff.SchemaDiff] by reflection
// and requires every field to be either PROBED — populated on an
// otherwise-empty diff, with the text renderer required to SAY something
// about it and [summarise] required to COUNT it — or exempted with a
// written reason.
//
// # What it reaches, and what it does not
//
// It reaches the FIELDS OF [irdiff.TableDiff] AND [irdiff.SchemaDiff]
// only, against the TEXT renderer and the JSON summary counts. It does
// NOT walk the nested per-object diff structs ([irdiff.ColumnDiff],
// [irdiff.IndexDiff], [irdiff.ForeignKeyDiff], [irdiff.PolicyDiff],
// [irdiff.SequenceDiff]) — a probe here proves the SECTION exists, not
// that every attribute within it renders. It also does not check the JSON
// body, which embeds SchemaDiff wholesale and therefore cannot omit a
// field the way the hand-written counts and renderer can.

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

// diffFieldProbe populates one diff field and names both halves of what
// must happen to it: a substring the text render must contain, and the
// summary counter it must increment.
//
// Both are required. A field that renders but is not counted is the CI
// consumer's blind spot; a field that is counted but not rendered is the
// human's. Bug 227 was both at once.
type diffFieldProbe struct {
	populate func(td *irdiff.TableDiff, d *irdiff.SchemaDiff)
	wantText string

	// Exactly one of wantCount / countedElsewhere must be set.
	// countedElsewhere names the field that carries this one's rollup, for
	// the per-table breakdown of a schema-level counter — an escape hatch
	// narrow enough that using it is a statement, not a shrug.
	wantCount        func(c DiffJSONCounts) int
	countedElsewhere string
}

var tableDiffProbes = map[string]diffFieldProbe{
	"ColumnsMissing": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ColumnsMissing = []string{"email"} },
		wantText:  "ADD COLUMN",
		wantCount: func(c DiffJSONCounts) int { return c.ColumnsMissing },
	},
	"ColumnsExtra": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ColumnsExtra = []string{"legacy"} },
		wantText:  "DROP COLUMN",
		wantCount: func(c DiffJSONCounts) int { return c.ColumnsExtra },
	},
	"ColumnsMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedType: "NUMERIC(20,8)", ActualType: "NUMERIC(10,2)"}}
		},
		wantText:  "NUMERIC(20,8)",
		wantCount: func(c DiffJSONCounts) int { return c.ColumnsMismatched },
	},
	"IndexesMissing": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.IndexesMissing = []string{"idx_email"} },
		wantText:  "idx_email",
		wantCount: func(c DiffJSONCounts) int { return c.IndexesMissing },
	},
	"IndexesExtra": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.IndexesExtra = []string{"idx_stale"} },
		wantText:  "DROP INDEX",
		wantCount: func(c DiffJSONCounts) int { return c.IndexesExtra },
	},
	"IndexesMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", UniqueMismatched: true, ExpectedUnique: true}}
		},
		wantText:  "ACCEPTS ROWS THE SOURCE REJECTS",
		wantCount: func(c DiffJSONCounts) int { return c.IndexesMismatched },
	},
	"ChecksMissing": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ChecksMissing = []string{"qty_nonneg"} },
		wantText:  "qty_nonneg",
		wantCount: func(c DiffJSONCounts) int { return c.ChecksMissing },
	},
	"ChecksExtra": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ChecksExtra = []string{"ck_stale"} },
		wantText:  "CHECK not in source schema",
		wantCount: func(c DiffJSONCounts) int { return c.ChecksExtra },
	},
	"ChecksMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ChecksMismatched = []irdiff.CheckDiff{{Name: "qty", ExpectedExpr: "qty > 0", ActualExpr: "qty >= 0"}}
		},
		wantText:  "CHECK",
		wantCount: func(c DiffJSONCounts) int { return c.ChecksMismatched },
	},

	// The C-7 / Bug 227 block: comparison existed, render and counts did not.
	"ForeignKeysMissing": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ForeignKeysMissing = []string{"fk_orders_user"} },
		wantText:  "ACCEPTS ORPHAN ROWS THE SOURCE REJECTS",
		wantCount: func(c DiffJSONCounts) int { return c.ForeignKeysMissing },
	},
	"ForeignKeysExtra": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ForeignKeysExtra = []string{"fk_stale"} },
		wantText:  "foreign key not in source schema",
		wantCount: func(c DiffJSONCounts) int { return c.ForeignKeysExtra },
	},
	"ForeignKeysMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ForeignKeysMismatched = []irdiff.ForeignKeyDiff{
				{Name: "fk_orders_user", ExpectedOnDelete: "RESTRICT", ActualOnDelete: "CASCADE"},
			}
		},
		wantText:  "SILENTLY DESTROYS child rows",
		wantCount: func(c DiffJSONCounts) int { return c.ForeignKeysMismatched },
	},
	"ForeignKeysUnnamed": {
		// Unreachable on a real MySQL or PG rig — both engines auto-name FK
		// constraints — so this is deliberately pinned at the unit level
		// rather than chased with an integration fixture.
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ForeignKeysUnnamed = 2 },
		wantText: "carry no constraint name and were NOT compared",
		// The per-table breakdown; the rollup is schema-level because a
		// table with nothing but uncompared FKs never reaches
		// TablesMismatched at all.
		countedElsewhere: "SchemaDiff.ForeignKeysUnnamed",
	},

	// EXCLUDE: the same omission, since ADR-0053, found by sweeping the
	// siblings of the FK finding rather than by anyone reporting it.
	"ExcludesMissing": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ExcludesMissing = []string{"no_overlap"} },
		wantText:  "ACCEPTS OVERLAPPING ROWS THE SOURCE REJECTS",
		wantCount: func(c DiffJSONCounts) int { return c.ExcludesMissing },
	},
	"ExcludesExtra": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.ExcludesExtra = []string{"ex_stale"} },
		wantText:  "EXCLUDE not in source schema",
		wantCount: func(c DiffJSONCounts) int { return c.ExcludesExtra },
	},
	"ExcludesMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ExcludesMismatched = []irdiff.ExcludeDiff{{Name: "no_overlap", ExpectedDefinition: "a", ActualDefinition: "b"}}
		},
		wantText:  "forbid DIFFERENT overlaps",
		wantCount: func(c DiffJSONCounts) int { return c.ExcludesMismatched },
	},

	// The B-10 block.
	"RLSMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.RLSMismatched = true
			td.ExpectedRLSEnabled = true
		},
		wantText:  "EVERY ROW IS VISIBLE TO EVERY ROLE",
		wantCount: func(c DiffJSONCounts) int { return c.RLSMismatched },
	},
	"PoliciesMissing": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.PoliciesMissing = []string{"tenant_isolation"} },
		wantText:  "UNFILTERED",
		wantCount: func(c DiffJSONCounts) int { return c.PoliciesMissing },
	},
	"PoliciesExtra": {
		populate:  func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) { td.PoliciesExtra = []string{"pol_stale"} },
		wantText:  "DROP POLICY",
		wantCount: func(c DiffJSONCounts) int { return c.PoliciesExtra },
	},
	"PoliciesMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.PoliciesMismatched = []irdiff.PolicyDiff{
				{Name: "tenant_isolation", ExpectedUsing: "tenant_id = 1", ActualUsing: ""},
			}
		},
		wantText:  "RETURNS EVERY ROW",
		wantCount: func(c DiffJSONCounts) int { return c.PoliciesMismatched },
	},
}

var tableDiffExempt = map[string]string{
	"Name": "the table name is the SECTION IDENTITY, not a delta: it renders in every per-table header and there is nothing to count. The header is asserted by the harness on every probe below, so it is covered behaviourally even though it has no roster entry of its own",

	// The RLS flag pairs are reported as ONE unit by design (see
	// irdiff.TableDiff) — RLSMismatched is the gate, and it IS probed.
	"ExpectedRLSEnabled": "rendered and counted THROUGH RLSMismatched, which gates it; on its own it is the zero value and means nothing. See the RLSMismatched probe",
	"ActualRLSEnabled":   "rendered and counted THROUGH RLSMismatched, which gates it; on its own it is the zero value and means nothing. See the RLSMismatched probe",
	"ExpectedRLSForced":  "rendered and counted THROUGH RLSMismatched, which gates it; on its own it is the zero value and means nothing. See the RLSMismatched probe",
	"ActualRLSForced":    "rendered and counted THROUGH RLSMismatched, which gates it; on its own it is the zero value and means nothing. See the RLSMismatched probe",
}

var schemaDiffProbes = map[string]diffFieldProbe{
	"TablesMissing": {
		populate:  func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) { d.TablesMissing = []string{"audit_log"} },
		wantText:  "audit_log (missing on target)",
		wantCount: func(c DiffJSONCounts) int { return c.TablesMissing },
	},
	"TablesExtra": {
		populate:  func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) { d.TablesExtra = []string{"scratch"} },
		wantText:  "DROP TABLE",
		wantCount: func(c DiffJSONCounts) int { return c.TablesExtra },
	},
	"TablesMismatched": {
		populate: func(td *irdiff.TableDiff, d *irdiff.SchemaDiff) {
			td.ColumnsMissing = []string{"email"}
			d.TablesMismatched = []irdiff.TableDiff{*td}
		},
		wantText:  "(mismatched)",
		wantCount: func(c DiffJSONCounts) int { return c.TablesMismatched },
	},
	"ViewsMissing": {
		populate:  func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) { d.ViewsMissing = []string{"v_recent"} },
		wantText:  "view v_recent (missing on target)",
		wantCount: func(c DiffJSONCounts) int { return c.ViewsMissing },
	},
	"ViewsExtra": {
		populate:  func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) { d.ViewsExtra = []string{"v_stale"} },
		wantText:  "DROP VIEW",
		wantCount: func(c DiffJSONCounts) int { return c.ViewsExtra },
	},
	"ViewsMismatched": {
		populate: func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) {
			d.ViewsMismatched = []irdiff.ViewDiff{{Name: "v_recent", ExpectedDefinition: "SELECT 1", ActualDefinition: "SELECT 2"}}
		},
		wantText:  "view v_recent (mismatched)",
		wantCount: func(c DiffJSONCounts) int { return c.ViewsMismatched },
	},
	"SequencesMissing": {
		populate:  func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) { d.SequencesMissing = []string{"order_number_seq"} },
		wantText:  "sequence order_number_seq (missing on target)",
		wantCount: func(c DiffJSONCounts) int { return c.SequencesMissing },
	},
	"SequencesExtra": {
		populate:  func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) { d.SequencesExtra = []string{"seq_stale"} },
		wantText:  "DROP SEQUENCE",
		wantCount: func(c DiffJSONCounts) int { return c.SequencesExtra },
	},
	"SequencesMismatched": {
		populate: func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) {
			d.SequencesMismatched = []irdiff.SequenceDiff{
				{Name: "order_number_seq", ExpectedIncrement: "5", ActualIncrement: "1"},
			}
		},
		wantText:  "PRODUCES DIFFERENT VALUES",
		wantCount: func(c DiffJSONCounts) int { return c.SequencesMismatched },
	},
	"ForeignKeysUnnamed": {
		// Renders in the PREAMBLE, not a per-table section: the tables
		// whose FKs went uncompared are frequently the ones with no other
		// drift, so a per-table-only note would be unreachable.
		populate: func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) {
			d.ForeignKeysUnnamed = 2
			d.TablesExtra = []string{"scratch"} // give the renderer a reason to run
		},
		wantText:  "COVERAGE: 2 foreign key(s) carry no constraint name",
		wantCount: func(c DiffJSONCounts) int { return c.ForeignKeysUnnamed },
	},
}

var schemaDiffExempt = map[string]string{}

const (
	tableDiffProbedFloor  = 18
	schemaDiffProbedFloor = 10
)

// TestDiffSurfaceRosterEveryTableDiffFieldIsRenderedAndCounted is the G-6
// render half. See the file comment for exactly which surfaces it reaches.
func TestDiffSurfaceRosterEveryTableDiffFieldIsRenderedAndCounted(t *testing.T) {
	t.Run("TableDiff", func(t *testing.T) {
		assertDiffRosterCovers(t, reflect.TypeOf(irdiff.TableDiff{}), tableDiffProbes, tableDiffExempt, tableDiffProbedFloor)
	})
	t.Run("SchemaDiff", func(t *testing.T) {
		assertDiffRosterCovers(t, reflect.TypeOf(irdiff.SchemaDiff{}), schemaDiffProbes, schemaDiffExempt, schemaDiffProbedFloor)
	})
}

func assertDiffRosterCovers(t *testing.T, typ reflect.Type, probes map[string]diffFieldProbe, exempt map[string]string, floor int) {
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
			t.Errorf("%s.%s is neither RENDERED-and-COUNTED by a probe nor exempted.\n"+
				"  A diff field with no render surface makes `schema diff` exit 1 and print nothing;\n"+
				"  one with no summary counter makes a CI consumer read zero while it does. That was Bug 227.",
				typ.Name(), name)
		case exempted && strings.TrimSpace(reason) == "":
			t.Errorf("%s.%s is exempted with an empty reason", typ.Name(), name)
		}
	}
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
	if len(probes) < floor {
		t.Errorf("only %d probed field(s) on %s; floor is %d — either the walker broke or fields were mass-exempted",
			len(probes), typ.Name(), floor)
	}

	for name, p := range probes {
		t.Run(name, func(t *testing.T) {
			td := irdiff.TableDiff{Name: "orders"}
			var d irdiff.SchemaDiff
			p.populate(&td, &d)
			// A TableDiff probe has to be attached to the schema-level diff
			// to reach the renderer; a SchemaDiff probe may already have
			// done so itself.
			if len(d.TablesMismatched) == 0 && !reflect.DeepEqual(td, irdiff.TableDiff{Name: "orders"}) {
				d.TablesMismatched = []irdiff.TableDiff{td}
			}
			if !d.HasChanges() {
				t.Fatalf("probe for %s.%s produced a diff with no changes — the renderer short-circuits on that", typ.Name(), name)
			}

			var sb strings.Builder
			if err := renderDiffText(&sb, diffBundle{
				srcEngine:  "postgres",
				tgtEngine:  "postgres",
				tgtDialect: ir.DDLDialectANSI,
				diff:       d,
				expected:   &ir.Schema{},
				actual:     &ir.Schema{},
			}); err != nil {
				t.Fatalf("renderDiffText: %v", err)
			}
			if got := sb.String(); !strings.Contains(got, p.wantText) {
				t.Errorf("text render of %s.%s does not mention it.\n  want substring: %q\n  got:\n%s",
					typ.Name(), name, p.wantText, got)
			}
			switch {
			case p.wantCount == nil && p.countedElsewhere == "":
				t.Errorf("probe for %s.%s sets neither wantCount nor countedElsewhere", typ.Name(), name)
			case p.wantCount != nil && p.countedElsewhere != "":
				t.Errorf("probe for %s.%s sets both wantCount and countedElsewhere — pick one", typ.Name(), name)
			case p.wantCount != nil:
				if n := p.wantCount(summarise(d)); n == 0 {
					t.Errorf("summarise() counted ZERO for %s.%s — a CI consumer keying off `summary` sees no drift while the command exits 1",
						typ.Name(), name)
				}
			}
		})
	}
}
