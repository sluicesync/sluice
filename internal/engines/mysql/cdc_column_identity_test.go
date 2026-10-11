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
		{"rotation over three statements", []string{
			"ALTER TABLE kd RENAME COLUMN b TO tmp", "ALTER TABLE kd RENAME COLUMN c TO b", "ALTER TABLE kd RENAME COLUMN tmp TO c",
		}, nil},
		{"rename away and back", []string{
			"ALTER TABLE kd RENAME COLUMN b TO tmp", "ALTER TABLE kd RENAME COLUMN tmp TO b",
		}, nil},
		{"plain rename", []string{"ALTER TABLE kd RENAME COLUMN b TO x"}, nil},
		{"plain drop", []string{"ALTER TABLE kd DROP COLUMN c"}, nil},
		{"plain add", []string{"ALTER TABLE kd ADD COLUMN d INT AFTER id"}, nil},
		{"reorder", []string{"ALTER TABLE kd MODIFY COLUMN c INT AFTER id"}, nil},
		{"retype in place", []string{"ALTER TABLE kd MODIFY COLUMN c BIGINT, CHANGE COLUMN b b VARCHAR(10)"}, nil},
		{"another table", []string{"ALTER TABLE other DROP COLUMN c, ADD COLUMN c INT"}, nil},
		{"another database", []string{"ALTER TABLE x.kd DROP COLUMN c, ADD COLUMN c INT"}, nil},
		{"the table dropped and re-created", []string{
			"ALTER TABLE kd DROP COLUMN c", "DROP TABLE kd", "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT)",
		}, nil},
		{"the ledger follows a table rename", []string{
			"ALTER TABLE kd2 DROP COLUMN c, ADD COLUMN c INT", "RENAME TABLE kd TO kd_old, kd2 TO kd",
		}, []string{`"c" is a new column`}},
		{"the ledger follows ALTER … RENAME TO", []string{
			"ALTER TABLE kd2 DROP COLUMN c, ADD COLUMN c INT, RENAME TO kd",
		}, []string{`"c" is a new column`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &CDCReader{}
			for _, s := range c.stmts {
				r.noteColumnIdentityDDL(context.Background(), s, "app")
			}
			err := r.judgeColumnIdentity("app.kd")
			if c.refuse == nil {
				if err != nil {
					t.Fatalf("refused a change the downstream doors see: %v", err)
				}
				return
			}
			if !errors.Is(err, ir.ErrColumnIdentityChanged) {
				t.Fatalf("err = %v, want COLUMN-IDENTITY-CHANGED", err)
			}
			for _, want := range c.refuse {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal does not say %s: %v", want, err)
				}
			}
			// The binlog is the same on a restart: the refusal does not settle.
			if again := r.judgeColumnIdentity("app.kd"); !errors.Is(again, ir.ErrColumnIdentityChanged) {
				t.Errorf("the second judgement passed: %v", again)
			}
		})
	}
}

// TestColumnIdentity_SettlesAtEachShape: a passing judgement makes the new
// shape the reference, so a later, separate change is judged on its own.
func TestColumnIdentity_SettlesAtEachShape(t *testing.T) {
	r := &CDCReader{}
	r.noteColumnIdentityDDL(context.Background(), "ALTER TABLE kd DROP COLUMN c", "app")
	if err := r.judgeColumnIdentity("app.kd"); err != nil {
		t.Fatal(err)
	}
	// A row of (id, b) was decoded; re-adding c now is a plain ADD.
	r.noteColumnIdentityDDL(context.Background(), "ALTER TABLE kd ADD COLUMN c INT", "app")
	if err := r.judgeColumnIdentity("app.kd"); err != nil {
		t.Fatalf("a re-ADD after the drop was decoded refused: %v", err)
	}
}

// TestColumnIdentity_UnparsedIsNotTracked states the residual: a statement
// the parser cannot read records nothing (it is logged, not refused).
func TestColumnIdentity_UnparsedIsNotTracked(t *testing.T) {
	r := &CDCReader{}
	r.noteColumnIdentityDDL(context.Background(), "ALTER TABLE kd DROP COLUMN c, ADD COLUMN c UUID", "app")
	if err := r.judgeColumnIdentity("app.kd"); err != nil {
		t.Fatalf("unparsed statement: %v", err)
	}
	if len(r.columnIdentity) != 0 {
		t.Fatalf("an unparsed statement recorded %v", r.columnIdentity)
	}
}
