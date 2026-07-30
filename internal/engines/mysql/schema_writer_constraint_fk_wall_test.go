// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFKWallRecoveryArmed pins the gate for the roadmap-item-109 FK
// statement-wall recovery — and, load-bearing, the same-engine MySQL→MySQL
// NO-OP: on a vanilla-MySQL target the recovery is ALWAYS disarmed even when
// the orchestrator declared the copied rows FK-consistent, so the constraints
// phase is byte-identical to before (no foreign_key_checks toggling, plain
// validating ADD). Only a VStream flavor (PlanetScale/Vitess — the sole MySQL
// family with the errno-3024 wall) AND an explicit declaration arms it.
func TestFKWallRecoveryArmed(t *testing.T) {
	cases := []struct {
		name       string
		flavor     Flavor
		consistent bool
		want       bool
	}{
		{"vanilla + declared consistent = DISARMED (same-engine no-op)", FlavorVanilla, true, false},
		{"vanilla + not declared = disarmed", FlavorVanilla, false, false},
		{"planetscale + declared consistent = ARMED", FlavorPlanetScale, true, true},
		{"planetscale + not declared = disarmed", FlavorPlanetScale, false, false},
		{"vitess + declared consistent = ARMED", FlavorVitess, true, true},
		{"mariadb + declared consistent = DISARMED (no VStream wall)", FlavorMariaDB, true, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			w := &SchemaWriter{flavor: c.flavor, copiedRowsFKConsistent: c.consistent}
			if got := w.fkWallRecoveryArmed(); got != c.want {
				t.Errorf("fkWallRecoveryArmed() = %v; want %v", got, c.want)
			}
		})
	}
}

// TestSetCopiedRowsForeignKeyConsistent pins the setter and the zero-value
// default (v0.99.51 lesson: the SAFE behaviour — full validation — must be the
// zero value, so every non-migrate construction path validates).
func TestSetCopiedRowsForeignKeyConsistent(t *testing.T) {
	w := &SchemaWriter{}
	if w.copiedRowsFKConsistent {
		t.Fatal("zero value must be false (validate) — the safe default for every non-migrate path")
	}
	var d ir.ForeignKeyConsistencyDeclarer = w
	d.SetCopiedRowsForeignKeyConsistent(true)
	if !w.copiedRowsFKConsistent {
		t.Fatal("SetCopiedRowsForeignKeyConsistent(true) did not set the field")
	}
	d.SetCopiedRowsForeignKeyConsistent(false)
	if w.copiedRowsFKConsistent {
		t.Fatal("SetCopiedRowsForeignKeyConsistent(false) did not clear the field")
	}
}

// TestIsConstraintBuildWalled pins the recovery trigger, deliberately narrower
// than the index build's isIndexBuildWalled: ONLY errno 3024 (the statement
// wall foreign_key_checks=0 can sidestep) triggers the metadata-only recovery.
// A safe-migrations 1105 direct-DDL block, an orphaned-child 1452, a generic
// 1105, a non-MySQL error, and nil must all be NOT-walled — recovering any of
// them would either be futile (1105) or silently land a violated FK (1452).
func TestIsConstraintBuildWalled(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"errno 3024 statement wall", &gomysql.MySQLError{Number: 3024, Message: "Query execution was interrupted, maximum statement execution time exceeded"}, true},
		{"errno 3024 wrapped", fmt.Errorf("mysql: add foreign key: %w", &gomysql.MySQLError{Number: 3024, Message: "maximum statement execution time exceeded"}), true},
		{"errno 1105 direct DDL disabled (safe-migrations) — NOT walled", &gomysql.MySQLError{Number: 1105, Message: "direct DDL is disabled"}, false},
		{"errno 1452 orphaned child — NOT walled (must fail loudly)", &gomysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row: a foreign key constraint fails"}, false},
		{"errno 1062 duplicate — NOT walled", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry"}, false},
		{"generic 1105 — NOT walled", &gomysql.MySQLError{Number: 1105, Message: "vttablet: something else"}, false},
		{"non-MySQL error — NOT walled", errors.New("dial tcp: connection refused"), false},
		{"nil — NOT walled", nil, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := isConstraintBuildWalled(c.err); got != c.want {
				t.Errorf("isConstraintBuildWalled(%v) = %v; want %v", c.err, got, c.want)
			}
		})
	}
}

// TestBuildOrphanScanQuery pins the bounded LEFT-JOIN orphan probe SQL for the
// scalar and composite cases — the load-bearing shape of the item-109
// verification. A gap here (wrong join, wrong NULL sentinel, an unbounded
// range) would let a violated FK read as clean, so the exact text is pinned.
func TestBuildOrphanScanQuery(t *testing.T) {
	w := &SchemaWriter{schema: "app"}

	t.Run("scalar PK + scalar FK, both bounds", func(t *testing.T) {
		child := &ir.Table{
			Name:       "events",
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}
		fk := &ir.ForeignKey{Columns: []string{"customer_id"}, ReferencedTable: "customers", ReferencedColumns: []string{"id"}}
		q, args := w.buildOrphanScanQuery(child, fk, []any{int64(10)}, []any{int64(20)})
		want := "SELECT c.`id` FROM `app`.`events` c" +
			" LEFT JOIN `app`.`customers` p ON c.`customer_id` = p.`id`" +
			" WHERE c.`customer_id` IS NOT NULL AND p.`id` IS NULL" +
			" AND (c.`id`) > (?) AND (c.`id`) <= (?) LIMIT 1"
		if q != want {
			t.Errorf("query =\n  %s\nwant\n  %s", q, want)
		}
		if len(args) != 2 || args[0] != int64(10) || args[1] != int64(20) {
			t.Errorf("args = %v; want [10 20]", args)
		}
	})

	t.Run("composite PK + composite FK, no bounds (single unbounded chunk)", func(t *testing.T) {
		child := &ir.Table{
			Name:       "line_items",
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "order_id"}, {Column: "seq"}}},
		}
		fk := &ir.ForeignKey{Columns: []string{"warehouse", "bin"}, ReferencedTable: "slots", ReferencedColumns: []string{"wh", "b"}}
		q, args := w.buildOrphanScanQuery(child, fk, nil, nil)
		want := "SELECT c.`order_id`, c.`seq` FROM `app`.`line_items` c" +
			" LEFT JOIN `app`.`slots` p ON c.`warehouse` = p.`wh` AND c.`bin` = p.`b`" +
			" WHERE c.`warehouse` IS NOT NULL AND c.`bin` IS NOT NULL AND p.`wh` IS NULL LIMIT 1"
		if q != want {
			t.Errorf("query =\n  %s\nwant\n  %s", q, want)
		}
		if len(args) != 0 {
			t.Errorf("args = %v; want empty", args)
		}
	})

	t.Run("cross-schema parent honours ReferencedSchema", func(t *testing.T) {
		child := &ir.Table{Name: "events", PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}}
		fk := &ir.ForeignKey{Columns: []string{"customer_id"}, ReferencedSchema: "other", ReferencedTable: "customers", ReferencedColumns: []string{"id"}}
		q, _ := w.buildOrphanScanQuery(child, fk, nil, nil)
		if !strings.Contains(q, "LEFT JOIN `other`.`customers` p") {
			t.Errorf("cross-schema parent not honoured: %s", q)
		}
	})
}

// TestTableIndexCoversFKColumns pins the backing-index precondition check: the
// metadata-only shortcut is taken ONLY when an index already covers the FK's
// referencing columns as a left prefix (otherwise ADD FOREIGN KEY would build
// the backing index, which itself walls).
func TestTableIndexCoversFKColumns(t *testing.T) {
	fkCols := []string{"customer_id"}
	cases := []struct {
		name  string
		table *ir.Table
		want  bool
	}{
		{
			name:  "PK covers as left prefix",
			table: &ir.Table{PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "customer_id"}, {Column: "id"}}}},
			want:  true,
		},
		{
			name:  "secondary index covers",
			table: &ir.Table{Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Column: "customer_id"}}}}},
			want:  true,
		},
		{
			name:  "composite left-prefix cover",
			table: &ir.Table{Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Column: "customer_id"}, {Column: "created_at"}}}}},
			want:  true,
		},
		{
			name:  "no covering index",
			table: &ir.Table{Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Column: "other"}}}}},
			want:  false,
		},
		{
			name:  "wrong-order index does not cover",
			table: &ir.Table{Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Column: "created_at"}, {Column: "customer_id"}}}}},
			want:  false,
		},
		{
			name:  "partial (WHERE-predicate) index does not count",
			table: &ir.Table{Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Column: "customer_id"}}, Predicate: "customer_id > 0"}}},
			want:  false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := tableIndexCoversFKColumns(c.table, fkCols); got != c.want {
				t.Errorf("tableIndexCoversFKColumns = %v; want %v", got, c.want)
			}
		})
	}
	// A composite FK needs the composite left prefix; a single-column index
	// does NOT cover a 2-column FK.
	twoCol := &ir.Table{Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Column: "a"}}}}}
	if tableIndexCoversFKColumns(twoCol, []string{"a", "b"}) {
		t.Error("a single-column index must not cover a 2-column FK tuple")
	}
}
