// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// gc2Base is a table carrying one of every class the door compares:
//
//	CREATE TABLE t (id int PRIMARY KEY, x int, tenant text NOT NULL DEFAULT 'a',
//	                CONSTRAINT x_pos CHECK (x > 0));
//	ALTER TABLE t ENABLE ROW LEVEL SECURITY;
//	CREATE POLICY iso ON t USING (tenant = current_user);
func gc2Base() *pgRelationFacts {
	return &pgRelationFacts{
		schema: "public", name: "t",
		rlsEnabled: true,
		constraints: map[string]pgConstraintFact{
			"t_pkey": {kind: "p", compare: "PRIMARY KEY (id)", display: "PRIMARY KEY (id)", attnums: []int16{1}},
			"x_pos":  {kind: "c", compare: "CHECK ((x > 0))", display: "CHECK ((x > 0))", attnums: []int16{2}},
		},
		policies: map[string]string{"iso": "cmd=* permissive=true roles=[public] using=((tenant = CURRENT_USER)) with_check=()"},
		columns: map[int16]pgColumnFact{
			1: {name: "id", typ: "integer", notNull: true},
			2: {name: "x", typ: "integer"},
			3: {name: "tenant", typ: "text", notNull: true, def: "'a'::text"},
		},
	}
}

func cloneFacts(f *pgRelationFacts) *pgRelationFacts {
	out := *f
	out.constraints = map[string]pgConstraintFact{}
	for k, v := range f.constraints {
		out.constraints[k] = v
	}
	out.policies = map[string]string{}
	for k, v := range f.policies {
		out.policies[k] = v
	}
	out.columns = map[int16]pgColumnFact{}
	for k, v := range f.columns {
		out.columns[k] = v
	}
	return &out
}

// TestDiffRelationFacts_RefusesEveryUncarriedClass pins one case per class
// GC-2 names, each graded by the delta line it must produce. The first case
// is the filing's sharpest shape: a column and a foreign key added in ONE
// statement — the column forwards, so the FK is the part that must refuse.
func TestDiffRelationFacts_RefusesEveryUncarriedClass(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*pgRelationFacts)
		want   string
	}{
		{"add column + add foreign key in one statement", func(f *pgRelationFacts) {
			f.columns[4] = pgColumnFact{name: "owner", typ: "integer"}
			f.constraints["fk_owner"] = pgConstraintFact{kind: "f", compare: "fk conkey=4 confrelid=16500", display: "FOREIGN KEY (owner) REFERENCES users(id)", attnums: []int16{4}}
		}, `ADD CONSTRAINT "fk_owner" FOREIGN KEY (owner) REFERENCES users(id)`},
		{"add unique", func(f *pgRelationFacts) {
			f.constraints["x_uq"] = pgConstraintFact{kind: "u", compare: "UNIQUE (x)", display: "UNIQUE (x)", attnums: []int16{2}}
		}, `ADD CONSTRAINT "x_uq" UNIQUE (x)`},
		{"add exclude", func(f *pgRelationFacts) {
			f.constraints["no_overlap"] = pgConstraintFact{kind: "x", compare: "EXCLUDE USING gist (x WITH =)", display: "EXCLUDE USING gist (x WITH =)", attnums: []int16{2}}
		}, `ADD CONSTRAINT "no_overlap" EXCLUDE`},
		{"drop check on a column that still exists", func(f *pgRelationFacts) {
			delete(f.constraints, "x_pos")
		}, `DROP CONSTRAINT "x_pos"`},
		{"drop primary key", func(f *pgRelationFacts) {
			delete(f.constraints, "t_pkey")
		}, `DROP CONSTRAINT "t_pkey"`},
		{"validate / redefine a constraint", func(f *pgRelationFacts) {
			f.constraints["x_pos"] = pgConstraintFact{kind: "c", compare: "CHECK ((x > 10))", display: "CHECK ((x > 10))", attnums: []int16{2}}
		}, `CONSTRAINT "x_pos" changed`},
		{"disable row level security", func(f *pgRelationFacts) { f.rlsEnabled = false }, "row level security DISABLED"},
		{"force row level security", func(f *pgRelationFacts) { f.rlsForced = true }, "FORCE row level security ENABLED"},
		{"drop policy", func(f *pgRelationFacts) { delete(f.policies, "iso") }, `DROP POLICY "iso"`},
		{"create policy", func(f *pgRelationFacts) { f.policies["ro"] = "cmd=r" }, `CREATE POLICY "ro"`},
		{"alter policy", func(f *pgRelationFacts) { f.policies["iso"] = "cmd=* using=(true)" }, `ALTER POLICY "iso"`},
		{"set not null", func(f *pgRelationFacts) {
			c := f.columns[2]
			c.notNull = true
			f.columns[2] = c
		}, `ALTER COLUMN "x" SET NOT NULL`},
		{"drop not null", func(f *pgRelationFacts) {
			c := f.columns[3]
			c.notNull = false
			f.columns[3] = c
		}, `ALTER COLUMN "tenant" DROP NOT NULL`},
		{"set default", func(f *pgRelationFacts) {
			c := f.columns[2]
			c.def = "7"
			f.columns[2] = c
		}, `ALTER COLUMN "x" SET DEFAULT 7`},
		{"drop default", func(f *pgRelationFacts) {
			c := f.columns[3]
			c.def = ""
			f.columns[3] = c
		}, `ALTER COLUMN "tenant" DROP DEFAULT`},
		{"add identity", func(f *pgRelationFacts) {
			c := f.columns[1]
			c.identity = "a"
			f.columns[1] = c
		}, `identity none -> GENERATED ALWAYS`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := cloneFacts(gc2Base())
			tc.mutate(cur)
			deltas := diffRelationFacts(gc2Base(), cur)
			if len(deltas) == 0 {
				t.Fatalf("no delta; want one containing %q — the change would be ignored at exit 0", tc.want)
			}
			if !strings.Contains(strings.Join(deltas, "; "), tc.want) {
				t.Errorf("deltas = %q; want one containing %q", deltas, tc.want)
			}
		})
	}
}

// TestDiffRelationFacts_ExemptsConsequencesOfHandledColumnChanges pins the
// exemptions: each is a change the pipeline's column-shape path already
// forwards or refuses, so the door refusing it would end streams that the
// forward handles correctly.
func TestDiffRelationFacts_ExemptsConsequencesOfHandledColumnChanges(t *testing.T) {
	cases := []struct {
		name   string
		base   func(*pgRelationFacts)
		mutate func(*pgRelationFacts)
	}{
		{"no change", nil, func(*pgRelationFacts) {}},
		{"added column's own NOT NULL and DEFAULT", nil, func(f *pgRelationFacts) {
			f.columns[4] = pgColumnFact{name: "n", typ: "integer", notNull: true, def: "0"}
		}},
		{"drop column cascades its check", nil, func(f *pgRelationFacts) {
			delete(f.columns, 2)
			delete(f.constraints, "x_pos")
		}},
		{"rename column rewrites its constraint and the policy text", nil, func(f *pgRelationFacts) {
			f.columns[3] = pgColumnFact{name: "org", typ: "text", notNull: true, def: "'a'::text"}
			f.policies["iso"] = "cmd=* permissive=true roles=[public] using=((org = CURRENT_USER)) with_check=()"
			c := f.columns[1]
			c.name = "pk"
			f.columns[1] = c
			f.constraints["t_pkey"] = pgConstraintFact{kind: "p", compare: "PRIMARY KEY (pk)", display: "PRIMARY KEY (pk)", attnums: []int16{1}}
		}},
		{"retype re-renders the default and the check", func(f *pgRelationFacts) { f.columns[2] = pgColumnFact{name: "x", typ: "integer", def: "0"} }, func(f *pgRelationFacts) {
			f.columns[2] = pgColumnFact{name: "x", typ: "bigint", def: "0::bigint"}
			f.constraints["x_pos"] = pgConstraintFact{kind: "c", compare: "CHECK ((x > (0)::bigint))", display: "CHECK ((x > (0)::bigint))", attnums: []int16{2}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := gc2Base()
			if tc.base != nil {
				tc.base(base)
			}
			cur := cloneFacts(base)
			tc.mutate(cur)
			if deltas := diffRelationFacts(base, cur); len(deltas) != 0 {
				t.Errorf("deltas = %q; want none — this change is handled by the column-shape path", deltas)
			}
		})
	}
}

// TestDiffRelationFacts_ForeignKeyComparesStructureNotText: renaming the
// REFERENCED table rewrites pg_get_constraintdef on the referencing side
// with nothing changed there. The FK compares on its structured tuple, so
// a display-only difference is not a refusal — and a real change to the
// tuple (here ON DELETE) still is.
func TestDiffRelationFacts_ForeignKeyComparesStructureNotText(t *testing.T) {
	base := gc2Base()
	base.constraints["fk"] = pgConstraintFact{kind: "f", compare: "fk conkey=2 confrelid=16500 del=a", display: "FOREIGN KEY (x) REFERENCES users(id)", attnums: []int16{2}}

	renamedParent := cloneFacts(base)
	renamedParent.constraints["fk"] = pgConstraintFact{kind: "f", compare: "fk conkey=2 confrelid=16500 del=a", display: "FOREIGN KEY (x) REFERENCES accounts(id)", attnums: []int16{2}}
	if deltas := diffRelationFacts(base, renamedParent); len(deltas) != 0 {
		t.Errorf("referenced-table rename: deltas = %q; want none", deltas)
	}

	cascade := cloneFacts(base)
	cascade.constraints["fk"] = pgConstraintFact{kind: "f", compare: "fk conkey=2 confrelid=16500 del=c", display: "FOREIGN KEY (x) REFERENCES users(id) ON DELETE CASCADE", attnums: []int16{2}}
	if deltas := diffRelationFacts(base, cascade); len(deltas) == 0 {
		t.Error("ON DELETE change: no delta; want a refusal")
	}
}

// TestUnforwardedChangeError_IsTerminalAndCarriesMarker: the refusal must
// not be retried (a retry re-baselines and accepts the change), and must
// carry the grep-stable marker and the re-baseline warning.
func TestUnforwardedChangeError_IsTerminalAndCarriesMarker(t *testing.T) {
	err := error(&terminalPGError{err: unforwardedChangeError("public", "t", []string{`ADD CONSTRAINT "fk" FOREIGN KEY (x) REFERENCES u(id)`})})
	if !ir.IsTerminal(err) {
		t.Error("refusal is not terminal; a retry would re-baseline and silently accept the change")
	}
	for _, want := range []string{unforwardedChangeMarker, "public.t", `ADD CONSTRAINT "fk"`, "fresh baseline", "accepts the difference permanently"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err, want)
		}
	}
	if got := classifyReaderError(err); !errors.Is(got, err) || !ir.IsTerminal(got) {
		t.Error("classifyReaderError re-wrapped the terminal refusal")
	}
}

// TestUnforwardedDoor_BothProtocolArmsCallIt: dispatchWAL carries the V1
// and V2 RelationMessage arms as duplicates (the UPR-2 lesson); both must
// route through the door, and the baseline must be taken in StreamChanges.
func TestUnforwardedDoor_BothProtocolArmsCallIt(t *testing.T) {
	b, err := os.ReadFile("cdc_reader.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if n := strings.Count(src, "r.gradeUnforwardedClasses(ctx, m.RelationID, entry)"); n != 2 {
		t.Errorf("%d RelationMessage arm(s) call gradeUnforwardedClasses; want 2 (V1 and V2)", n)
	}
	if n := strings.Count(src, "r.gradeRelationSchemaRace(relations, m.RelationID, entry)"); n != 2 {
		t.Fatalf("found %d gradeRelationSchemaRace arm calls; the arm anchor this test counts against has moved", n)
	}
	if !strings.Contains(src, "r.captureUnforwardedBaseline(ctx)") {
		t.Error("StreamChanges no longer captures the baseline; the door is inert")
	}
}
