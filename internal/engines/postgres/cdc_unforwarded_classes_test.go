// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql"
	"errors"
	"fmt"
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
		policies: map[string]pgPolicyFact{"iso": {
			compare: "cmd=* permissive=true roles=[public] using={OPEXPR :args ({VAR :varattno 3})} with_check=",
			display: "cmd=* permissive=true roles=[public] using=((tenant = CURRENT_USER)) with_check=()",
		}},
		columns: map[int16]pgColumnFact{
			1: {name: "id", typ: "integer", notNull: true},
			2: {name: "x", typ: "integer"},
			3: {name: "tenant", typ: "text", notNull: true, def: "'a'::text"},
		},
		uniqueIndexes: map[string]pgConstraintFact{
			"x_uidx": {kind: "i", compare: "unique index key=2 option=0 nulls_not_distinct=false pred= exprs=", display: "CREATE UNIQUE INDEX x_uidx ON public.t USING btree (x)", attnums: []int16{2}, identity: "x class_coll=1978 0"},
		},
	}
}

func cloneFacts(f *pgRelationFacts) *pgRelationFacts {
	out := *f
	out.constraints = map[string]pgConstraintFact{}
	for k, v := range f.constraints {
		out.constraints[k] = v
	}
	out.policies = map[string]pgPolicyFact{}
	for k, v := range f.policies {
		out.policies[k] = v
	}
	out.columns = map[int16]pgColumnFact{}
	for k, v := range f.columns {
		out.columns[k] = v
	}
	out.uniqueIndexes = map[string]pgConstraintFact{}
	for k, v := range f.uniqueIndexes {
		out.uniqueIndexes[k] = v
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
		{"create policy", func(f *pgRelationFacts) { f.policies["ro"] = pgPolicyFact{compare: "cmd=r", display: "cmd=r"} }, `CREATE POLICY "ro"`},
		{"alter policy", func(f *pgRelationFacts) {
			f.policies["iso"] = pgPolicyFact{compare: "cmd=* using={CONST true}", display: "cmd=* using=(true)"}
		}, `ALTER POLICY "iso"`},
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
		{"retype plus SET DEFAULT in one statement (the retype must not exempt it)", func(f *pgRelationFacts) {
			f.columns[2] = pgColumnFact{name: "x", typ: "bigint", def: "7"}
		}, `ALTER COLUMN "x" SET DEFAULT 7`},
		{"rename plus a policy edit in the same window (the rename must not exempt it)", func(f *pgRelationFacts) {
			c := f.columns[3]
			c.name = "org"
			f.columns[3] = c
			f.policies["iso"] = pgPolicyFact{compare: "cmd=* using={CONST true}", display: "cmd=* using=(true)"}
		}, `ALTER POLICY "iso"`},
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
			// The readable text follows the rename; the stored node tree names
			// the column by attnum and does not (measured on PG 16).
			p := f.policies["iso"]
			p.display = "cmd=* permissive=true roles=[public] using=((org = CURRENT_USER)) with_check=()"
			f.policies["iso"] = p
			c := f.columns[1]
			c.name = "pk"
			f.columns[1] = c
			f.constraints["t_pkey"] = pgConstraintFact{kind: "p", compare: "PRIMARY KEY (pk)", display: "PRIMARY KEY (pk)", attnums: []int16{1}}
		}},
		{"retype re-renders the check", func(f *pgRelationFacts) { f.columns[2] = pgColumnFact{name: "x", typ: "integer", def: "0"} }, func(f *pgRelationFacts) {
			f.columns[2] = pgColumnFact{name: "x", typ: "numeric", def: "0"}
			f.constraints["x_pos"] = pgConstraintFact{kind: "c", compare: "CHECK ((x > (0)::numeric))", display: "CHECK ((x > (0)::numeric))", attnums: []int16{2}}
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

// TestUnforwardedBaseline_CarriedAcrossReaders pins GC-32 on this engine:
// a reader handed the previous reader's baseline starts from it and does
// NOT read the catalog. Discriminating by construction — the pool points
// at a port nothing listens on, so a fresh catalog read fails and only
// adoption succeeds.
func TestUnforwardedBaseline_CarriedAcrossReaders(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	fresh := &CDCReader{db: db}
	if err := fresh.captureUnforwardedBaseline(t.Context()); err == nil {
		t.Fatal("a reader with no carried baseline did not read the catalog (the unreachable pool should have failed it); this test cannot discriminate")
	}

	prev := &CDCReader{}
	prev.unforwarded.facts = map[uint32]*pgRelationFacts{16400: gc2Base()}
	carried := prev.UnforwardedBaseline()
	if carried == nil {
		t.Fatal("UnforwardedBaseline returned nil for a reader with a baseline")
	}

	next := &CDCReader{db: db}
	next.SetUnforwardedBaseline(carried)
	if err := next.captureUnforwardedBaseline(t.Context()); err != nil {
		t.Fatalf("captureUnforwardedBaseline with a carried baseline: %v — it re-read the catalog, so a retry would absorb a pending change (GC-32)", err)
	}
	if got := next.unforwarded.facts[16400]; got == nil || got.name != "t" {
		t.Errorf("next reader's baseline = %v; want the carried entry for OID 16400", next.unforwarded.facts)
	}

	// The snapshot is a copy: a later re-baseline on the new reader must not
	// reach back into the old one's map.
	next.unforwarded.facts[16401] = gc2Base()
	if _, leaked := prev.unforwarded.facts[16401]; leaked {
		t.Error("the carried baseline aliases the previous reader's map")
	}

	other := &CDCReader{db: db}
	other.SetUnforwardedBaseline(map[string]int{"not": 1})
	if err := other.captureUnforwardedBaseline(t.Context()); err == nil {
		t.Error("a foreign value was adopted as a baseline; it must be ignored")
	}
}

// TestDiffRelationFacts_ExcludeExpressionElementIsNotALostColumn: an
// EXCLUDE (or any constraint) with an expression element records attnum 0
// in conkey — measured `{2,0}` for `EXCLUDE USING gist (room WITH =,
// tsrange(s, e) WITH &&)`. Attnum 0 is not a column, so it must not make
// a DROP CONSTRAINT look like the cascade of a dropped column.
func TestDiffRelationFacts_ExcludeExpressionElementIsNotALostColumn(t *testing.T) {
	base := gc2Base()
	base.constraints["no_overlap"] = pgConstraintFact{kind: "x", compare: "EXCLUDE …", display: "EXCLUDE USING gist (x WITH =, tsrange(a, b) WITH &&)", attnums: []int16{2, 0}}
	cur := cloneFacts(base)
	delete(cur.constraints, "no_overlap")
	deltas := diffRelationFacts(base, cur)
	if !strings.Contains(strings.Join(deltas, "; "), `DROP CONSTRAINT "no_overlap"`) {
		t.Errorf("deltas = %q; want the EXCLUDE drop refused — attnum 0 is an expression, not a dropped column", deltas)
	}
}

// TestUnforwardedBaseline_PassesOnWhatItWasHandedWhenNeverStarted pins the
// 2026-09-23 pre-tag review's F2: an attempt can fail between being handed
// a baseline and StreamChanges (a seed-loader read, a standby/wal_level
// check). Its snapshot must be what it was handed — a nil there dropped the
// chain, and the attempt after it took a fresh baseline that absorbed the
// change.
func TestUnforwardedBaseline_PassesOnWhatItWasHandedWhenNeverStarted(t *testing.T) {
	first := &CDCReader{}
	first.unforwarded.facts = map[uint32]*pgRelationFacts{16400: gc2Base()}
	middle := &CDCReader{}
	middle.SetUnforwardedBaseline(first.UnforwardedBaseline())
	// middle never reaches StreamChanges.
	got, ok := middle.UnforwardedBaseline().(pgUnforwardedBaseline)
	if !ok || got[16400] == nil {
		t.Fatalf("a reader that was handed a baseline and never started returned %v; want the handed baseline", middle.UnforwardedBaseline())
	}
}

// TestDiffRelationFacts_PlainUniqueIndex pins SOURCE-DROPPED-UNIQUE-KEY-MERGE's
// source-side half: a plain unique index (no constraint owns it) refuses on
// DROP, CREATE and a change of its structure, exactly as a UNIQUE constraint
// does; a rename of its column is no change (the comparison is attnum-based),
// a retype explains a structural change, and a DROP that went with its column
// is the target's own DROP COLUMN's cascade.
func TestDiffRelationFacts_PlainUniqueIndex(t *testing.T) {
	refuse := []struct {
		name   string
		mutate func(*pgRelationFacts)
		want   string
	}{
		{"drop", func(f *pgRelationFacts) { delete(f.uniqueIndexes, "x_uidx") }, `DROP INDEX "x_uidx" (was CREATE UNIQUE INDEX x_uidx`},
		{"create", func(f *pgRelationFacts) {
			f.uniqueIndexes["tenant_uidx"] = pgConstraintFact{kind: "i", compare: "unique index key=3", display: "CREATE UNIQUE INDEX tenant_uidx ON public.t USING btree (tenant)", attnums: []int16{3}}
		}, `CREATE UNIQUE INDEX "tenant_uidx"`},
		{"made partial", func(f *pgRelationFacts) {
			u := f.uniqueIndexes["x_uidx"]
			u.compare += " pred={OPEXPR}"
			f.uniqueIndexes["x_uidx"] = u
		}, `UNIQUE INDEX "x_uidx" changed`},
	}
	for _, tc := range refuse {
		cur := cloneFacts(gc2Base())
		tc.mutate(cur)
		deltas := diffRelationFacts(gc2Base(), cur)
		if len(deltas) != 1 || !strings.Contains(deltas[0], tc.want) {
			t.Errorf("%s: deltas %q; want one naming %q", tc.name, deltas, tc.want)
		}
	}

	renamed := cloneFacts(gc2Base())
	renamed.columns[2] = pgColumnFact{name: "x2", typ: "integer"}
	u := renamed.uniqueIndexes["x_uidx"]
	u.display = "CREATE UNIQUE INDEX x_uidx ON public.t USING btree (x2)"
	renamed.uniqueIndexes["x_uidx"] = u
	if d := diffRelationFacts(gc2Base(), renamed); len(d) != 0 {
		t.Errorf("a column rename refused through its unique index: %q", d)
	}

	retyped := cloneFacts(gc2Base())
	retyped.columns[2] = pgColumnFact{name: "x", typ: "bigint"}
	u = retyped.uniqueIndexes["x_uidx"]
	u.compare += " (rebuilt)"
	retyped.uniqueIndexes["x_uidx"] = u
	if d := diffRelationFacts(gc2Base(), retyped); len(d) != 0 {
		t.Errorf("a key column's retype refused through its unique index: %q", d)
	}

	idxRenamed := cloneFacts(gc2Base())
	idxRenamed.uniqueIndexes["x_uidx2"] = idxRenamed.uniqueIndexes["x_uidx"]
	delete(idxRenamed.uniqueIndexes, "x_uidx")
	if d := diffRelationFacts(gc2Base(), idxRenamed); len(d) != 0 {
		t.Errorf("an ALTER INDEX RENAME refused: %q", d)
	}
	// Same structure under a new name but another collation (identity differs):
	// not a rename.
	recollated := cloneFacts(gc2Base())
	ru := recollated.uniqueIndexes["x_uidx"]
	ru.identity = "x class_coll=1978 950"
	recollated.uniqueIndexes["x_uidx_c"] = ru
	delete(recollated.uniqueIndexes, "x_uidx")
	if d := diffRelationFacts(gc2Base(), recollated); len(d) != 2 {
		t.Errorf("a new-name CREATE with another collation: deltas %q; want DROP and CREATE", d)
	}
	// A DROP of one index and a CREATE of a DIFFERENT one under another name
	// is not a rename: both refuse.
	swapped := cloneFacts(gc2Base())
	delete(swapped.uniqueIndexes, "x_uidx")
	swapped.uniqueIndexes["tenant_uidx"] = pgConstraintFact{kind: "i", compare: "unique index key=3", display: "CREATE UNIQUE INDEX tenant_uidx ON public.t USING btree (tenant)", attnums: []int16{3}, identity: "tenant class_coll=3126 100"}
	if d := diffRelationFacts(gc2Base(), swapped); len(d) != 2 {
		t.Errorf("a DROP plus a different CREATE: deltas %q; want both", d)
	}

	dropped := cloneFacts(gc2Base())
	delete(dropped.columns, 2)
	delete(dropped.uniqueIndexes, "x_uidx")
	delete(dropped.constraints, "x_pos")
	if d := diffRelationFacts(gc2Base(), dropped); len(d) != 0 {
		t.Errorf("a unique index dropped with its column refused: %q", d)
	}
}

// TestUniqueKeysLostWithColumn pins the round-3 handoff: every unique key
// kind (a plain unique index, a UNIQUE constraint, a PRIMARY KEY) lost with
// a dropped column is named — and still exempt from this door's refusal —
// while a key that merely changed, a renamed index, an expression element
// (attnum 0) and a non-unique constraint kind are not handed on.
func TestUniqueKeysLostWithColumn(t *testing.T) {
	cols := func(attnums ...int16) map[int16]pgColumnFact {
		out := map[int16]pgColumnFact{}
		for _, a := range attnums {
			out[a] = pgColumnFact{name: fmt.Sprint("c", a), typ: "int4"}
		}
		return out
	}
	key := func(kind string, attnums ...int16) pgConstraintFact {
		return pgConstraintFact{kind: kind, compare: fmt.Sprint(kind, attnums), display: "x", identity: fmt.Sprint(kind, attnums), attnums: attnums}
	}
	prior := &pgRelationFacts{
		columns: cols(1, 2, 3, 4),
		constraints: map[string]pgConstraintFact{
			"kd_pkey": key("p", 3), "kd_u": key("u", 2, 4), "kd_chk": key("c", 4), "kd_x": key("x", 4),
		},
		uniqueIndexes: map[string]pgConstraintFact{
			"kd_g": key("i", 4), "kd_expr": key("i", 0), "kd_keep": key("i", 1), "kd_old": key("i", 2),
		},
	}
	cur := &pgRelationFacts{
		columns:     cols(1, 2, 3), // attnum 4 dropped (b, or a generated g)
		constraints: map[string]pgConstraintFact{"kd_pkey": key("p", 3)},
		uniqueIndexes: map[string]pgConstraintFact{
			"kd_keep": key("i", 1), "kd_new": key("i", 2), // kd_old renamed to kd_new
		},
	}
	if got := strings.Join(uniqueKeysLostWithColumn(prior, cur), ","); got != "kd_u,kd_g" {
		t.Fatalf("handed on %q, want kd_u,kd_g", got)
	}
	if deltas := diffRelationFacts(prior, cur); len(deltas) != 1 || !strings.Contains(deltas[0], "kd_expr") {
		t.Fatalf("door deltas %v: want only kd_expr (an expression index is not lost with a column)", deltas)
	}
	// A primary key lost with its column is handed on too.
	cur.columns = cols(1, 2)
	cur.constraints = map[string]pgConstraintFact{}
	if got := strings.Join(uniqueKeysLostWithColumn(prior, cur), ","); got != "kd_pkey,kd_u,kd_g" {
		t.Fatalf("handed on %q, want kd_pkey,kd_u,kd_g", got)
	}
	if uniqueKeysLostWithColumn(nil, cur) != nil {
		t.Fatal("no prior: nothing to hand on")
	}
}
