// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestColumnIdentity_DDLSequences pins RENAME-SWAP-INVISIBLE's ledger: each
// sequence of statements between two rows of `app.kd` (id, b, c), and
// whether the next row refuses. The refusing rows are the shapes measured
// silent on MySQL 8.4; the passing rows are the changes the downstream
// doors already see correctly (a plain rename, drop, add or reorder) and
// the pure permutation the column-order doors judge.
func TestColumnIdentity_DDLSequences(t *testing.T) {
	cases := []struct {
		name  string
		stmts []string
		// refuse names the columns the refusal must name; nil passes.
		refuse []string
	}{
		{"swap then restore the order", []string{
			"ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b",
			"ALTER TABLE kd MODIFY COLUMN b INT AFTER id",
		}, []string{`"b" now holds the column that was "c"`, `"c" now holds the column that was "b"`}},
		{"swap by CHANGE with AFTER in one ALTER", []string{
			"ALTER TABLE kd CHANGE COLUMN b c INT, CHANGE COLUMN c b INT AFTER id",
		}, []string{`"b" now holds the column that was "c"`}},
		{"drop and re-add in one ALTER", []string{
			"ALTER TABLE kd DROP COLUMN c, ADD COLUMN c INT",
		}, []string{`"c" is a new column`}},
		{"drop then re-add", []string{
			"ALTER TABLE kd DROP COLUMN c", "ALTER TABLE kd ADD COLUMN c INT",
		}, []string{`"c" is a new column`}},
		{"rename away then re-add", []string{
			"ALTER TABLE kd RENAME COLUMN c TO tmp, ADD COLUMN c INT",
		}, []string{`"c" is a new column`}},
		{"drop then rename onto", []string{
			"ALTER TABLE kd DROP COLUMN b, RENAME COLUMN c TO b",
		}, []string{`"b" now holds the column that was "c"`}},
		{"drop then rename onto, two ALTERs, qualified, other default db", []string{
			"ALTER TABLE app.kd DROP COLUMN b", "ALTER TABLE `app`.`KD` RENAME COLUMN C TO B",
		}, []string{`"b" now holds the column that was "c"`}},
		// Any FIRST/AFTER move can restore a swapped order, so a permutation
		// next to one is not left to the order doors.
		{"swap with a column moved", []string{
			"ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b, MODIFY COLUMN id INT FIRST",
		}, []string{`"b" now holds`}},
		// An ADD … FIRST moves no existing column relative to another.
		{"swap with a column added first", []string{
			"ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b, ADD COLUMN d INT FIRST",
		}, nil},

		{"plain swap: the column-order doors judge it", []string{
			"ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b",
		}, nil},
		// The stated cost of refusing at the DDL: a swap through a temporary
		// name is, at its second statement, `DROP b; RENAME c TO b`.
		{"rotation over three statements refuses at the second (cost)", []string{
			"ALTER TABLE kd RENAME COLUMN b TO tmp", "ALTER TABLE kd RENAME COLUMN c TO b", "ALTER TABLE kd RENAME COLUMN tmp TO c",
		}, []string{`"b" now holds the column that was "c"`}},
		// Review F2: statements that name the table without replacing it.
		{"a no-op CREATE TABLE IF NOT EXISTS keeps the ledger", []string{
			"ALTER TABLE kd DROP COLUMN c", "CREATE TABLE IF NOT EXISTS kd (id INT PRIMARY KEY)", "ALTER TABLE kd ADD COLUMN c INT",
		}, []string{`"c" is a new column`}},
		{"DROP TEMPORARY TABLE keeps the ledger", []string{
			"ALTER TABLE kd DROP COLUMN c", "DROP TEMPORARY TABLE IF EXISTS kd", "ALTER TABLE kd ADD COLUMN c INT",
		}, []string{`"c" is a new column`}},
		// Review F3 (a): MariaDB's IF [NOT] EXISTS, stripped and re-parsed.
		{"MariaDB DROP COLUMN IF EXISTS then ADD", []string{
			"ALTER TABLE kd DROP COLUMN IF EXISTS c, ADD COLUMN c INT",
		}, []string{`"c" is a new column`}},
		{"MariaDB IF EXISTS on both clauses, two ALTERs", []string{
			"ALTER TABLE kd DROP COLUMN IF EXISTS c", "ALTER TABLE kd ADD COLUMN IF NOT EXISTS c INT",
		}, []string{`"c" is a new column`}},
		{"rename away and back", []string{
			"ALTER TABLE kd RENAME COLUMN b TO tmp", "ALTER TABLE kd RENAME COLUMN tmp TO b",
		}, nil},
		{"plain rename", []string{"ALTER TABLE kd RENAME COLUMN b TO x"}, nil},
		{"plain drop", []string{"ALTER TABLE kd DROP COLUMN c"}, nil},
		{"plain add", []string{"ALTER TABLE kd ADD COLUMN d INT AFTER id"}, nil},
		{"reorder", []string{"ALTER TABLE kd MODIFY COLUMN c INT AFTER id"}, nil},
		{"retype in place", []string{"ALTER TABLE kd MODIFY COLUMN c BIGINT, CHANGE COLUMN b b VARCHAR(10)"}, nil},
		{"another table's ledger is its own", []string{"ALTER TABLE other RENAME COLUMN b TO c, RENAME COLUMN c TO b", "ALTER TABLE kd MODIFY COLUMN b INT AFTER id"}, nil},
		{"another database", []string{"ALTER TABLE x.kd DROP COLUMN c, ADD COLUMN c INT"}, nil},
		{"the table dropped and re-created", []string{
			"ALTER TABLE kd DROP COLUMN c", "DROP TABLE kd", "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT)",
		}, nil},
		{"the ledger follows a table rename", []string{
			"ALTER TABLE kd2 RENAME COLUMN b TO c, RENAME COLUMN c TO b", "RENAME TABLE kd TO kd_old, kd2 TO kd",
			"ALTER TABLE kd MODIFY COLUMN b INT AFTER id",
		}, []string{`"b" now holds the column that was "c"`}},
		{"the ledger follows ALTER … RENAME TO", []string{
			"ALTER TABLE kd2 RENAME COLUMN b TO c, RENAME COLUMN c TO b, RENAME TO kd",
			"ALTER TABLE kd MODIFY COLUMN b INT AFTER id",
		}, []string{`"b" now holds the column that was "c"`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &CDCReader{schema: "app"}
			// The stream stops at the first refused DDL, as the reader does.
			var atDDL error
			for _, s := range c.stmts {
				if atDDL = r.noteColumnIdentityDDL(context.Background(), s, "app"); atDDL != nil {
					break
				}
			}
			if c.refuse == nil {
				if atDDL != nil {
					t.Fatalf("refused at the DDL a change the downstream doors see: %v", atDDL)
				}
				if err := r.judgeColumnIdentity("app.kd"); err != nil {
					t.Fatalf("refused a change the downstream doors see: %v", err)
				}
				return
			}
			// Review F1: the refusal comes at the DDL itself, so a restart
			// between the DDL and the table's next row cannot forget it.
			if !errors.Is(atDDL, ir.ErrColumnIdentityChanged) {
				t.Fatalf("no refusal at the DDL (err = %v), want COLUMN-IDENTITY-CHANGED", atDDL)
			}
			for _, want := range c.refuse {
				if !strings.Contains(atDDL.Error(), want) {
					t.Errorf("refusal does not say %s: %v", want, atDDL)
				}
			}
			// The ledger is kept: the row path refuses the same change.
			if again := r.judgeColumnIdentity("app.kd"); !errors.Is(again, ir.ErrColumnIdentityChanged) {
				t.Errorf("the row-path judgement passed: %v", again)
			}
		})
	}
}

// TestColumnIdentity_SettlesAtEachShape: a passing judgement makes the new
// shape the reference, so a later, separate change is judged on its own.
func TestColumnIdentity_SettlesAtEachShape(t *testing.T) {
	r := &CDCReader{schema: "app"}
	if err := r.noteColumnIdentityDDL(context.Background(), "ALTER TABLE kd DROP COLUMN c", "app"); err != nil {
		t.Fatal(err)
	}
	if err := r.judgeColumnIdentity("app.kd"); err != nil {
		t.Fatal(err)
	}
	// A row of (id, b) was decoded; re-adding c now is a plain ADD.
	if err := r.noteColumnIdentityDDL(context.Background(), "ALTER TABLE kd ADD COLUMN c INT", "app"); err != nil {
		t.Fatalf("a re-ADD after the drop was decoded refused at the DDL: %v", err)
	}
	if err := r.judgeColumnIdentity("app.kd"); err != nil {
		t.Fatalf("a re-ADD after the drop was decoded refused: %v", err)
	}
}

// TestColumnIdentity_ScopeGated: a table the stream does not replicate never
// refuses at its DDL — not another database, not a filtered-out table.
func TestColumnIdentity_ScopeGated(t *testing.T) {
	r := &CDCReader{schema: "app", scopeAllowed: func(_, table string) bool { return table != "skip" }}
	for _, s := range []string{
		"ALTER TABLE skip DROP COLUMN c, ADD COLUMN c INT",
		"ALTER TABLE other.kd DROP COLUMN c, ADD COLUMN c INT",
		"ALTER TABLE skip DROP COLUMN c, ADD COLUMN c UUID",
	} {
		if err := r.noteColumnIdentityDDL(context.Background(), s, "app"); err != nil {
			t.Errorf("%s: refused an out-of-scope table: %v", s, err)
		}
	}
	if err := r.noteColumnIdentityDDL(context.Background(), "ALTER TABLE kd DROP COLUMN c, ADD COLUMN c INT", "app"); !errors.Is(err, ir.ErrColumnIdentityChanged) {
		t.Errorf("the in-scope table was not refused: %v", err)
	}
}

// TestColumnIdentity_Unparsed pins review F3's rule for a statement the
// parser cannot read: refuse when it names an in-scope table and spells
// DROP, CHANGE or RENAME, or when the table's ledger already vacated a
// name; otherwise flow.
func TestColumnIdentity_Unparsed(t *testing.T) {
	cases := []struct {
		name   string
		stmts  []string
		refuse bool
	}{
		{"a routine ADD of a type the parser lacks flows", []string{"ALTER TABLE kd ADD COLUMN u UUID"}, false},
		{"its word DROP inside a string does not count", []string{"ALTER TABLE kd ADD COLUMN u UUID COMMENT 'drop me'"}, false},
		{"…nor inside a quoted identifier", []string{"ALTER TABLE kd ADD COLUMN `drop` UUID"}, false},
		{"unparsed DROP and re-ADD", []string{"ALTER TABLE kd DROP COLUMN c, ADD COLUMN c UUID"}, true},
		{"parsed DROP, then unparsed re-ADD", []string{"ALTER TABLE kd DROP COLUMN c", "ALTER TABLE kd ADD COLUMN c UUID"}, true},
		{"parsed swap, then an unparsed move", []string{
			"ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b", "ALTER TABLE kd MODIFY COLUMN b UUID AFTER id",
		}, true},
		// The stated cost: an unrelated DROP INDEX next to an unparsed ADD.
		{"unparsed ADD beside a DROP INDEX refuses (cost)", []string{"ALTER TABLE kd ADD COLUMN u UUID, DROP INDEX i"}, true},
		{"unparsed, another table", []string{"ALTER TABLE x.kd DROP COLUMN c, ADD COLUMN c UUID"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &CDCReader{schema: "app"}
			var err error
			for _, s := range c.stmts {
				if err = r.noteColumnIdentityDDL(context.Background(), s, "app"); err != nil {
					break
				}
			}
			if got := errors.Is(err, ir.ErrColumnIdentityChanged); got != c.refuse {
				t.Fatalf("refused = %v (err = %v), want %v", got, err, c.refuse)
			}
		})
	}
}

// TestStripColumnIfExists pins the MariaDB `IF [NOT] EXISTS` removal: only
// on column clauses of an ALTER, never inside quotes, never through an
// executable comment.
func TestStripColumnIfExists(t *testing.T) {
	cases := []struct{ in, want string }{
		{"ALTER TABLE kd DROP COLUMN IF EXISTS c, ADD COLUMN IF NOT EXISTS c INT", "ALTER TABLE kd DROP COLUMN c, ADD COLUMN c INT"},
		{"ALTER TABLE kd DROP IF EXISTS c", "ALTER TABLE kd DROP c"},
		{"alter table kd change column if exists b c int", "alter table kd change column b c int"},
		{"ALTER TABLE kd MODIFY IF EXISTS c BIGINT", "ALTER TABLE kd MODIFY c BIGINT"},
		{"ALTER TABLE kd RENAME COLUMN IF EXISTS c TO d", "ALTER TABLE kd RENAME COLUMN c TO d"},
		// Not column clauses, or not reachable by position: unchanged.
		{"ALTER TABLE kd DROP INDEX IF EXISTS i", "ALTER TABLE kd DROP INDEX IF EXISTS i"},
		{"ALTER TABLE IF EXISTS kd ADD COLUMN x INT", "ALTER TABLE IF EXISTS kd ADD COLUMN x INT"},
		{"ALTER TABLE kd ADD COLUMN `IF` INT COMMENT 'DROP IF EXISTS c'", "ALTER TABLE kd ADD COLUMN `IF` INT COMMENT 'DROP IF EXISTS c'"},
		{"ALTER TABLE kd /*!50100 DROP COLUMN IF EXISTS c */", "ALTER TABLE kd /*!50100 DROP COLUMN IF EXISTS c */"},
		{"CREATE TABLE IF NOT EXISTS kd (id INT)", "CREATE TABLE IF NOT EXISTS kd (id INT)"},
	}
	for _, c := range cases {
		got, changed := stripColumnIfExists(c.in)
		if got != c.want || changed != (c.in != c.want) {
			t.Errorf("stripColumnIfExists(%q) = %q, %v; want %q", c.in, got, changed, c.want)
		}
	}
}
