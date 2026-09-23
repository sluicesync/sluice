// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// gc2MySQLBase is a table carrying one of every class the door compares:
//
//	CREATE TABLE t (
//	  id     INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
//	  x      INT,
//	  tenant VARCHAR(20) NOT NULL DEFAULT 'a',
//	  owner  INT,
//	  code   VARCHAR(40),
//	  a      INT,
//	  b      INT,
//	  g      INT AS (x + 1) STORED,
//	  UNIQUE KEY code_uq (code(10)),
//	  UNIQUE KEY pair_uq (a, b),
//	  CONSTRAINT x_pos CHECK (x > 0),
//	  CONSTRAINT fk_owner FOREIGN KEY (owner) REFERENCES db.users (id));
func gc2MySQLBase() *mysqlTableFacts {
	return &mysqlTableFacts{
		schema: "db", name: "t",
		columns: map[string]mysqlColumnFact{
			"id":     {typ: "int", extra: "auto_increment"},
			"x":      {typ: "int", nullable: true},
			"tenant": {typ: "varchar(20)", def: "a", hasDef: true},
			"owner":  {typ: "int", nullable: true},
			"code":   {typ: "varchar(40)", nullable: true},
			"a":      {typ: "int", nullable: true},
			"b":      {typ: "bigint", nullable: true},
			"g":      {typ: "int", nullable: true, extra: "STORED GENERATED", generated: "(`x` + 1)"},
		},
		constraints: map[string]mysqlConstraintFact{
			"PRIMARY KEY PRIMARY": {kind: kindPrimaryKey, name: "PRIMARY", columns: []string{"id"}},
			"UNIQUE code_uq":      {kind: kindUnique, name: "code_uq", columns: []string{"code"}, detail: " part1-prefix=10"},
			"UNIQUE pair_uq":      {kind: kindUnique, name: "pair_uq", columns: []string{"a", "b"}},
			"CHECK x_pos":         {kind: kindCheck, name: "x_pos", detail: "((`x` > 0))"},
			"FOREIGN KEY fk_owner": {
				kind: kindForeignKey, name: "fk_owner", columns: []string{"owner"},
				refTable: "db.users", refColumns: []string{"id"}, detail: "ON UPDATE RESTRICT ON DELETE RESTRICT MATCH NONE",
			},
		},
	}
}

func cloneMySQLFacts(f *mysqlTableFacts) *mysqlTableFacts {
	out := *f
	out.columns = map[string]mysqlColumnFact{}
	for k, v := range f.columns {
		out.columns[k] = v
	}
	out.constraints = map[string]mysqlConstraintFact{}
	for k, v := range f.constraints {
		v.columns = append([]string(nil), v.columns...)
		v.refColumns = append([]string(nil), v.refColumns...)
		out.constraints[k] = v
	}
	return &out
}

// TestDiffTableFacts_RefusesEveryUncarriedClass pins one case per class the
// binlog lane's boundary projection does not carry, each graded by the
// delta line it must produce. The first case is the filing's sharpest
// shape: a column and a foreign key added in ONE statement — the column
// forwards, so the FK is the part that must refuse.
func TestDiffTableFacts_RefusesEveryUncarriedClass(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*mysqlTableFacts)
		want   string
	}{
		{"add column + add foreign key in one statement", func(f *mysqlTableFacts) {
			f.columns["parent"] = mysqlColumnFact{typ: "int", nullable: true}
			f.constraints["FOREIGN KEY fk_parent"] = mysqlConstraintFact{
				kind: kindForeignKey, name: "fk_parent", columns: []string{"parent"},
				refTable: "db.p", refColumns: []string{"id"}, detail: "ON UPDATE RESTRICT ON DELETE RESTRICT MATCH NONE",
			}
		}, `ADD CONSTRAINT "fk_parent" FOREIGN KEY (parent) REFERENCES db.p (id)`},
		{"add column + unique key on it", func(f *mysqlTableFacts) {
			f.columns["email"] = mysqlColumnFact{typ: "varchar(80)", nullable: true}
			f.constraints["UNIQUE email"] = mysqlConstraintFact{kind: kindUnique, name: "email", columns: []string{"email"}}
		}, `ADD CONSTRAINT "email" UNIQUE (email)`},
		{"add unique on an existing column", func(f *mysqlTableFacts) {
			f.constraints["UNIQUE x_uq"] = mysqlConstraintFact{kind: kindUnique, name: "x_uq", columns: []string{"x"}}
		}, `ADD CONSTRAINT "x_uq" UNIQUE (x)`},
		{"drop unique", func(f *mysqlTableFacts) { delete(f.constraints, "UNIQUE code_uq") }, `DROP CONSTRAINT "code_uq"`},
		{"unique prefix length changed", func(f *mysqlTableFacts) {
			c := f.constraints["UNIQUE code_uq"]
			c.detail = " part1-prefix=20"
			f.constraints["UNIQUE code_uq"] = c
		}, `CONSTRAINT "code_uq" changed`},
		{"drop foreign key", func(f *mysqlTableFacts) { delete(f.constraints, "FOREIGN KEY fk_owner") }, `DROP CONSTRAINT "fk_owner"`},
		{"foreign key ON DELETE changed", func(f *mysqlTableFacts) {
			c := f.constraints["FOREIGN KEY fk_owner"]
			c.detail = "ON UPDATE RESTRICT ON DELETE CASCADE MATCH NONE"
			f.constraints["FOREIGN KEY fk_owner"] = c
		}, `CONSTRAINT "fk_owner" changed`},
		{"add check", func(f *mysqlTableFacts) {
			f.constraints["CHECK t_len"] = mysqlConstraintFact{kind: kindCheck, name: "t_len", detail: "((length(`tenant`) < 5))"}
		}, `ADD CONSTRAINT "t_len" CHECK`},
		{"drop check", func(f *mysqlTableFacts) { delete(f.constraints, "CHECK x_pos") }, `DROP CONSTRAINT "x_pos"`},
		{"alter check NOT ENFORCED", func(f *mysqlTableFacts) {
			c := f.constraints["CHECK x_pos"]
			c.detail += " NOT ENFORCED"
			f.constraints["CHECK x_pos"] = c
		}, `CONSTRAINT "x_pos" changed: CHECK ((` + "`x`" + ` > 0)) -> CHECK ((` + "`x`" + ` > 0)) NOT ENFORCED`},
		{"drop primary key", func(f *mysqlTableFacts) { delete(f.constraints, "PRIMARY KEY PRIMARY") }, "DROP PRIMARY KEY (was PRIMARY KEY (id))"},
		{"widen primary key", func(f *mysqlTableFacts) {
			c := f.constraints["PRIMARY KEY PRIMARY"]
			c.columns = []string{"id", "tenant"}
			f.constraints["PRIMARY KEY PRIMARY"] = c
		}, "PRIMARY KEY changed: PRIMARY KEY (id) -> PRIMARY KEY (id, tenant)"},
		{"set default", func(f *mysqlTableFacts) {
			c := f.columns["x"]
			c.def, c.hasDef = "7", true
			f.columns["x"] = c
		}, `ALTER COLUMN "x" SET DEFAULT 7`},
		{"change default", func(f *mysqlTableFacts) {
			c := f.columns["tenant"]
			c.def = "b"
			f.columns["tenant"] = c
		}, `ALTER COLUMN "tenant" SET DEFAULT b (was a)`},
		{"drop default", func(f *mysqlTableFacts) {
			c := f.columns["tenant"]
			c.def, c.hasDef = "", false
			f.columns["tenant"] = c
		}, `ALTER COLUMN "tenant" DROP DEFAULT (was a)`},
		{"retype that discards the default (MODIFY without DEFAULT)", func(f *mysqlTableFacts) {
			f.columns["tenant"] = mysqlColumnFact{typ: "varchar(40)"}
		}, `ALTER COLUMN "tenant" DROP DEFAULT`},
		{"retype that discards auto_increment", func(f *mysqlTableFacts) {
			f.columns["id"] = mysqlColumnFact{typ: "bigint"}
		}, `ALTER COLUMN "id" attributes "auto_increment" -> ""`},
		{"add on update current_timestamp", func(f *mysqlTableFacts) {
			c := f.columns["x"]
			c.extra = "on update CURRENT_TIMESTAMP"
			f.columns["x"] = c
		}, `ALTER COLUMN "x" attributes "" -> "on update CURRENT_TIMESTAMP"`},
		{"generation expression changed", func(f *mysqlTableFacts) {
			c := f.columns["g"]
			c.generated = "(`x` + 2)"
			f.columns["g"] = c
		}, `ALTER COLUMN "g" generation expression`},
		// MySQL drops a column OUT of a composite index and keeps the rest;
		// Postgres drops the whole index. Which the target did depends on
		// the target engine, so the shrink refuses.
		{"composite key shrinks when one of its columns is dropped", func(f *mysqlTableFacts) {
			delete(f.columns, "b")
			f.constraints["UNIQUE pair_uq"] = mysqlConstraintFact{kind: kindUnique, name: "pair_uq", columns: []string{"a"}}
		}, `CONSTRAINT "pair_uq" changed: UNIQUE (a, b) -> UNIQUE (a)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := cloneMySQLFacts(gc2MySQLBase())
			tc.mutate(cur)
			deltas := diffTableFacts(gc2MySQLBase(), cur, nil)
			if len(deltas) == 0 {
				t.Fatalf("no delta; want one containing %q — the change would be ignored at exit 0", tc.want)
			}
			if !strings.Contains(strings.Join(deltas, "; "), tc.want) {
				t.Errorf("deltas = %q; want one containing %q", deltas, tc.want)
			}
		})
	}
}

// TestDiffTableFacts_ExemptsConsequencesOfHandledColumnChanges pins the
// exemptions: each is a change the pipeline's column-shape path already
// forwards or refuses, so the door refusing it would end streams that the
// forward handles correctly.
func TestDiffTableFacts_ExemptsConsequencesOfHandledColumnChanges(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*mysqlTableFacts)
	}{
		{"no change", func(*mysqlTableFacts) {}},
		{"added column's own default and attributes", func(f *mysqlTableFacts) {
			f.columns["n"] = mysqlColumnFact{typ: "int", def: "0", hasDef: true, extra: "on update CURRENT_TIMESTAMP"}
		}},
		{"nullability change is ADR-0091's, not this door's", func(f *mysqlTableFacts) {
			c := f.columns["x"]
			c.nullable = false
			f.columns["x"] = c
		}},
		{"drop column cascades its single-column unique key", func(f *mysqlTableFacts) {
			delete(f.columns, "code")
			delete(f.constraints, "UNIQUE code_uq")
		}},
		{"drop column with its foreign key", func(f *mysqlTableFacts) {
			delete(f.columns, "owner")
			delete(f.constraints, "FOREIGN KEY fk_owner")
		}},
		{"drop column takes a check with it (coarse: no dependency info)", func(f *mysqlTableFacts) {
			delete(f.columns, "x")
			delete(f.constraints, "CHECK x_pos")
		}},
		{"rename column rewrites its key and foreign key", func(f *mysqlTableFacts) {
			f.columns["org_owner"] = f.columns["owner"]
			delete(f.columns, "owner")
			c := f.constraints["FOREIGN KEY fk_owner"]
			c.columns = []string{"org_owner"}
			f.constraints["FOREIGN KEY fk_owner"] = c
		}},
		{"rename column re-renders a check (coarse)", func(f *mysqlTableFacts) {
			f.columns["y"] = f.columns["x"]
			delete(f.columns, "x")
			f.constraints["CHECK x_pos"] = mysqlConstraintFact{kind: kindCheck, name: "x_pos", detail: "((`y` > 0))"}
		}},
		{"retype re-renders the default value", func(f *mysqlTableFacts) {
			f.columns["tenant"] = mysqlColumnFact{typ: "char(20)", def: "a ", hasDef: true}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := gc2MySQLBase()
			cur := cloneMySQLFacts(base)
			tc.mutate(cur)
			if deltas := diffTableFacts(base, cur, nil); len(deltas) != 0 {
				t.Errorf("deltas = %q; want none — this change is handled by the column-shape path", deltas)
			}
		})
	}
}

// TestDiffTableFacts_ForeignKeyParentRename: renaming the REFERENCED table
// or column rewrites this table's foreign key with nothing changed here.
// It is exempt only when the old reference no longer exists; a foreign key
// re-pointed while its old target still exists is refused.
func TestDiffTableFacts_ForeignKeyParentRename(t *testing.T) {
	repoint := func(refTable, refCol string) *mysqlTableFacts {
		cur := cloneMySQLFacts(gc2MySQLBase())
		c := cur.constraints["FOREIGN KEY fk_owner"]
		c.refTable, c.refColumns = refTable, []string{refCol}
		cur.constraints["FOREIGN KEY fk_owner"] = c
		return cur
	}
	parent := func(cols ...string) *mysqlTableFacts {
		f := &mysqlTableFacts{columns: map[string]mysqlColumnFact{}}
		for _, c := range cols {
			f.columns[c] = mysqlColumnFact{typ: "int"}
		}
		return f
	}

	if got := fkParentsToRead(gc2MySQLBase(), repoint("db.accounts", "id")); strings.Join(got, ",") != "db.users,db.accounts" {
		t.Errorf("fkParentsToRead = %q; want both references", got)
	}

	cells := []struct {
		name       string
		cur        *mysqlTableFacts
		tablesNow  map[string]*mysqlTableFacts
		wantRefuse bool
	}{
		{"parent table renamed (old name gone)", repoint("db.accounts", "id"), map[string]*mysqlTableFacts{"db.accounts": parent("id")}, false},
		{"parent column renamed (old column gone)", repoint("db.users", "uid"), map[string]*mysqlTableFacts{"db.users": parent("uid")}, false},
		{"re-pointed while the old parent still exists", repoint("db.accounts", "id"), map[string]*mysqlTableFacts{"db.users": parent("id"), "db.accounts": parent("id")}, true},
		{"re-pointed to another column that coexists with the old", repoint("db.users", "uid"), map[string]*mysqlTableFacts{"db.users": parent("id", "uid")}, true},
		{"new parent unreadable", repoint("db.accounts", "id"), map[string]*mysqlTableFacts{}, true},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			deltas := diffTableFacts(gc2MySQLBase(), c.cur, c.tablesNow)
			if got := len(deltas) > 0; got != c.wantRefuse {
				t.Errorf("refused = %t (deltas %q); want %t", got, deltas, c.wantRefuse)
			}
		})
	}
}

// TestColumnRename_MatchesClassifyShapesRule: exactly one gone and one new
// with identical attributes is a rename; a gone/new pair that differs in
// anything else (here nullability, which the diff never compares) is not,
// and neither is a two-for-two swap.
func TestColumnRename_MatchesClassifyShapesRule(t *testing.T) {
	base := gc2MySQLBase()

	renamed := cloneMySQLFacts(base)
	renamed.columns["y"] = renamed.columns["x"]
	delete(renamed.columns, "x")
	if from, to, ok := columnRename(base, renamed); !ok || from != "x" || to != "y" {
		t.Errorf("columnRename = %q→%q ok=%t; want x→y", from, to, ok)
	}

	reshaped := cloneMySQLFacts(renamed)
	c := reshaped.columns["y"]
	c.nullable = false
	reshaped.columns["y"] = c
	if _, _, ok := columnRename(base, reshaped); ok {
		t.Error("a drop+add differing in nullability was inferred as a rename; ClassifyShape does not")
	}

	two := cloneMySQLFacts(renamed)
	two.columns["o2"] = two.columns["owner"]
	delete(two.columns, "owner")
	if _, _, ok := columnRename(base, two); ok {
		t.Error("a two-column swap was inferred as a rename; the pair-up is ambiguous")
	}
}

// TestColumnFact_FoldsPerFlavor pins the read-time folding the diff relies
// on: MariaDB's bare-keyword NULL is "no default", MySQL's DEFAULT_GENERATED
// distinguishes an expression default from a literal with the same text,
// and INVISIBLE is not part of the compared attributes.
func TestColumnFact_FoldsPerFlavor(t *testing.T) {
	some := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	if f := columnFact(FlavorMariaDB, "int", "YES", some("NULL"), "", ""); f.hasDef {
		t.Error("mariadb: bare NULL keyword read as a declared default")
	}
	if f := columnFact(FlavorVanilla, "varchar(4)", "YES", some("NULL"), "", ""); !f.hasDef {
		t.Error("mysql: the string default 'NULL' read as no default")
	}
	lit := columnFact(FlavorVanilla, "varchar(10)", "YES", some("now()"), "", "")
	expr := columnFact(FlavorVanilla, "varchar(10)", "YES", some("now()"), "DEFAULT_GENERATED", "")
	if lit.def == expr.def {
		t.Error("a literal default and an expression default with the same text compare equal")
	}
	if f := columnFact(FlavorVanilla, "int", "YES", sql.NullString{}, "INVISIBLE on update CURRENT_TIMESTAMP", ""); f.extra != "on update CURRENT_TIMESTAMP" {
		t.Errorf("extra = %q; want INVISIBLE folded out", f.extra)
	}
}

// TestIsMariaDBJSONMarker: MariaDB's auto CHECK on a JSON (longtext) column
// is the type, not a constraint — adding a JSON column must not read as
// adding a CHECK. A user CHECK that merely mentions json_valid is kept.
func TestIsMariaDBJSONMarker(t *testing.T) {
	f := &mysqlTableFacts{columns: map[string]mysqlColumnFact{"doc": {typ: "longtext"}, "note": {typ: "varchar(10)"}}}
	if !isMariaDBJSONMarker(f, "json_valid(`doc`)") {
		t.Error("auto json_valid CHECK on a longtext column not recognised")
	}
	if isMariaDBJSONMarker(f, "json_valid(`doc`) and length(`doc`) > 2") {
		t.Error("a user CHECK mentioning json_valid was dropped")
	}
	if isMariaDBJSONMarker(f, "json_valid(`note`)") {
		t.Error("json_valid on a non-longtext column was dropped")
	}
}

// TestUnforwardedChangeError_IsTerminalAndCarriesMarker: the refusal must
// not be retried (a retry re-baselines and accepts the change) by any
// consumer, whichever question it asks — even when the quoted catalog
// text happens to contain a transient shape's wording, which the reader
// classifier's text heuristics would otherwise wrap as retriable.
func TestUnforwardedChangeError_IsTerminalAndCarriesMarker(t *testing.T) {
	err := error(&terminalMySQLError{err: unforwardedChangeError("db", "t", []string{
		`ADD CONSTRAINT "fk" FOREIGN KEY (x) REFERENCES db.u (id)`,
		`ALTER COLUMN "note" SET DEFAULT connection reset by peer`,
	})})
	if !ir.IsTerminal(err) {
		t.Error("refusal is not terminal")
	}
	for _, want := range []string{unforwardedChangeMarker, "db.t", `ADD CONSTRAINT "fk"`, "fresh baseline", "accepts the difference permanently"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err, want)
		}
	}
	classified := classifyReaderError(err)
	// Identity IS the property: any re-wrap could add a retriable layer.
	if classified != err { //nolint:errorlint // see above
		t.Fatalf("classifyReaderError re-wrapped the terminal refusal: %T", classified)
	}
	var re ir.RetriableError
	if errors.As(classified, &re) && re.Retriable() {
		t.Error("the refusal answers Retriable()==true to an errors.As consumer; the ADR-0038 loop would retry and re-baseline")
	}

	// Control: the same transient wording WITHOUT the terminal wrapper is
	// what the classifier would retry — proving the text above is live.
	plain := errors.New("mysql: cdc: read: connection reset by peer")
	if !errors.As(classifyReaderError(plain), &re) || !re.Retriable() {
		t.Fatal("control: classifyReaderError no longer retries a connection reset; the terminal pin above proves nothing")
	}
}

// TestUnforwardedDoor_WiredIntoTheBinlogReader: the baseline is taken in
// StreamChanges and the per-table diff runs in dispatchRows on a rebuild.
// A source check, because the wiring (not the diff) is what a refactor of
// either function would silently drop; the behaviour is pinned end to end
// by TestStreamer_MySQLSource_UnforwardedSchemaChange_Refuses.
func TestUnforwardedDoor_WiredIntoTheBinlogReader(t *testing.T) {
	b, err := os.ReadFile("cdc_reader.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if n := strings.Count(src, "r.gradeUnforwardedClasses(ctx, qn)"); n != 1 {
		t.Errorf("%d call(s) of gradeUnforwardedClasses in cdc_reader.go; want 1 (dispatchRows' rebuild)", n)
	}
	if !strings.Contains(src, "r.captureUnforwardedBaseline(ctx)") {
		t.Error("StreamChanges no longer captures the baseline; the door is inert")
	}
}

// TestUnforwardedDoor_InertWithoutACatalogPool: a reader built without a
// schema pool (unit-test struct literals) neither baselines nor grades.
func TestUnforwardedDoor_InertWithoutACatalogPool(t *testing.T) {
	r := &CDCReader{}
	if err := r.captureUnforwardedBaseline(t.Context()); err != nil || r.UnforwardedBaseline() != nil {
		t.Errorf("baseline without a pool: err=%v baseline=%v; want inert", err, r.UnforwardedBaseline())
	}
	if err := r.gradeUnforwardedClasses(t.Context(), "db.t"); err != nil {
		t.Errorf("grade without a pool: %v; want inert", err)
	}
}

// TestUnforwardedBaseline_CarriedAcrossReaders pins GC-32 on the binlog
// lane: a reader handed the previous reader's baseline starts from it and
// does NOT read the catalog. The pool points at a port nothing listens on,
// so a fresh read fails and only adoption succeeds.
func TestUnforwardedBaseline_CarriedAcrossReaders(t *testing.T) {
	db, err := sql.Open("mysql", "nobody:x@tcp(127.0.0.1:1)/none?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	fresh := &CDCReader{db: db}
	if err := fresh.captureUnforwardedBaseline(t.Context()); err == nil {
		t.Fatal("a reader with no carried baseline did not read the catalog (the unreachable pool should have failed it); this test cannot discriminate")
	}

	prev := &CDCReader{}
	prev.unforwarded.facts = map[string]*mysqlTableFacts{"src.t": gc2MySQLBase()}
	carried := prev.UnforwardedBaseline()
	if carried == nil {
		t.Fatal("UnforwardedBaseline returned nil for a reader with a baseline")
	}

	next := &CDCReader{db: db}
	next.SetUnforwardedBaseline(carried)
	if err := next.captureUnforwardedBaseline(t.Context()); err != nil {
		t.Fatalf("captureUnforwardedBaseline with a carried baseline: %v — it re-read the catalog, so a retry would absorb a pending change (GC-32)", err)
	}
	if next.unforwarded.facts["src.t"] == nil {
		t.Errorf("next reader's baseline = %v; want the carried entry", next.unforwarded.facts)
	}
	next.unforwarded.facts["src.u"] = gc2MySQLBase()
	if _, leaked := prev.unforwarded.facts["src.u"]; leaked {
		t.Error("the carried baseline aliases the previous reader's map")
	}

	other := &CDCReader{db: db}
	other.SetUnforwardedBaseline(map[uint32]int{1: 1})
	if err := other.captureUnforwardedBaseline(t.Context()); err == nil {
		t.Error("a foreign value was adopted as a baseline; it must be ignored")
	}
}
