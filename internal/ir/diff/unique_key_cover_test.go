// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func ukcCol(name string) ir.IndexColumn { return ir.IndexColumn{Column: name} }

func ukcPrefix(name string, n int) ir.IndexColumn { return ir.IndexColumn{Column: name, Length: n} }

func ukcExpr(text, dialect string) ir.IndexColumn {
	return ir.IndexColumn{Expression: text, ExpressionDialect: dialect}
}

func ukcKey(name string, cols ...ir.IndexColumn) *ir.Index {
	return &ir.Index{Name: name, Unique: true, Columns: cols}
}

// ukcTable builds a table over plain text columns a, b, c, k, id, plus
// extra, with the given primary key (nil: none) and unique indexes.
func ukcTable(pk *ir.Index, keys ...*ir.Index) *ir.Table {
	t := &ir.Table{Name: "t", PrimaryKey: pk, Indexes: keys}
	for _, n := range []string{"id", "a", "b", "c", "k"} {
		t.Columns = append(t.Columns, &ir.Column{Name: n, Type: ir.Text{}})
	}
	return t
}

func ukcNames(keys []*ir.Index) string {
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = k.Name
	}
	return strings.Join(names, ",")
}

// TestUncoveredTargetUniqueKeys_Matrix pins every part family (plain column,
// prefix, expression same-dialect and cross-dialect, index collation) and
// every key-level shape (primary key, subset, order, partial, NULLS NOT
// DISTINCT, target-only columns) in both directions: a cell that must be
// covered and its sibling that must not.
func TestUncoveredTargetUniqueKeys_Matrix(t *testing.T) {
	pkID := ukcKey("PRIMARY", ukcCol("id"))
	cells := []struct {
		name   string
		source *ir.Table
		target *ir.Table
		want   string // comma-joined names of the uncovered target keys
	}{
		// ---- plain columns
		{"same key", ukcTable(pkID, ukcKey("kd_k", ukcCol("k"))), ukcTable(pkID, ukcKey("kd_k", ukcCol("k"))), ""},
		{"the source dropped the key", ukcTable(pkID), ukcTable(pkID, ukcKey("kd_k", ukcCol("k"))), "kd_k"},
		{"same key under another name", ukcTable(pkID, ukcKey("src_name", ukcCol("k"))), ukcTable(pkID, ukcKey("tgt_name", ukcCol("k"))), ""},
		{"the same name over another column", ukcTable(pkID, ukcKey("kd_k", ukcCol("a"))), ukcTable(pkID, ukcKey("kd_k", ukcCol("k"))), "kd_k"},
		{"column names differ only in case", ukcTable(pkID, ukcKey("kd_k", ukcCol("K"))), ukcTable(pkID, ukcKey("kd_k", ukcCol("k"))), ""},
		{"key order is not identity", ukcTable(pkID, ukcKey("ab", ukcCol("a"), ukcCol("b"))), ukcTable(pkID, ukcKey("ba", ukcCol("b"), ukcCol("a"))), ""},

		// ---- the primary key
		{"target primary key is the source's", ukcTable(pkID), ukcTable(pkID), ""},
		{"target primary key the source does not enforce", ukcTable(pkID), ukcTable(ukcKey("PRIMARY", ukcCol("k"))), "PRIMARY"},
		{"target primary key a source unique key implies", ukcTable(nil, ukcKey("kd_k", ukcCol("k"))), ukcTable(ukcKey("PRIMARY", ukcCol("k"))), ""},
		{"a source primary key implies a target unique key over it", ukcTable(pkID), ukcTable(pkID, ukcKey("id_dup", ukcCol("id"))), ""},

		// ---- the subset rule
		{"source key a subset of the target key", ukcTable(pkID, ukcKey("a", ukcCol("a"))), ukcTable(pkID, ukcKey("ab", ukcCol("a"), ukcCol("b"))), ""},
		{"target key a subset of the source key", ukcTable(pkID, ukcKey("ab", ukcCol("a"), ukcCol("b"))), ukcTable(pkID, ukcKey("a", ukcCol("a"))), "a"},
		{"source primary key a subset of a target composite", ukcTable(pkID), ukcTable(pkID, ukcKey("idk", ukcCol("id"), ukcCol("k"))), ""},

		// ---- prefix parts
		{"same prefix", ukcTable(pkID, ukcKey("c", ukcPrefix("c", 10))), ukcTable(pkID, ukcKey("c", ukcPrefix("c", 10))), ""},
		{"source prefix implies the whole column", ukcTable(pkID, ukcKey("c", ukcPrefix("c", 10))), ukcTable(pkID, ukcKey("c", ukcCol("c"))), ""},
		{"source prefix implies a longer prefix", ukcTable(pkID, ukcKey("c", ukcPrefix("c", 10))), ukcTable(pkID, ukcKey("c", ukcPrefix("c", 20))), ""},
		{"source prefix does not imply a shorter prefix", ukcTable(pkID, ukcKey("c", ukcPrefix("c", 10))), ukcTable(pkID, ukcKey("c", ukcPrefix("c", 5))), "c"},
		{"whole source column does not imply a prefix", ukcTable(pkID, ukcKey("c", ukcCol("c"))), ukcTable(pkID, ukcKey("c", ukcPrefix("c", 10))), "c"},

		// ---- expression parts, one dialect
		{"same expression, spelled differently", ukcTable(pkID, ukcKey("le", ukcExpr("lower(`k`)", "mysql"))), ukcTable(pkID, ukcKey("other", ukcExpr("( LOWER(k) )", "mysql"))), ""},
		{"a different expression", ukcTable(pkID, ukcKey("le", ukcExpr("lower(k)", "mysql"))), ukcTable(pkID, ukcKey("le", ukcExpr("upper(k)", "mysql"))), "le"},
		{"a literal differing in case is a different key", ukcTable(pkID, ukcKey("le", ukcExpr("if(k = 'A', 1, k)", "mysql"))), ukcTable(pkID, ukcKey("le", ukcExpr("if(k = 'a', 1, k)", "mysql"))), "le"},
		{"the writer's hidden-collation pin is not a difference", ukcTable(pkID, ukcKey("le", ir.IndexColumn{Expression: "lower(`k`)", ExpressionDialect: "mysql", HiddenCollation: "utf8mb4_bin"})), ukcTable(pkID, ukcKey("le", ir.IndexColumn{Expression: "(lower(`k`) collate utf8mb4_bin)", ExpressionDialect: "mysql", HiddenCollation: "utf8mb4_bin"})), ""},
		{"the pin is stripped under the peer's collation when one side records none", ukcTable(pkID, ukcKey("le", ir.IndexColumn{Expression: "lower(`k`)", ExpressionDialect: "mysql", HiddenCollation: "utf8mb4_bin"})), ukcTable(pkID, ukcKey("le", ukcExpr("(lower(`k`) collate utf8mb4_bin)", "mysql"))), ""},
		{"expression against a plain column", ukcTable(pkID, ukcKey("k", ukcCol("k"))), ukcTable(pkID, ukcKey("k", ukcExpr("lower(k)", "mysql"))), "k"},

		// ---- expression parts across dialects: the name is the identity
		{"cross dialect, same name", ukcTable(pkID, ukcKey("le", ukcExpr("lower((k)::text)", "postgres"))), ukcTable(pkID, ukcKey("le", ukcExpr("lower(`k`)", "mysql"))), ""},
		{"cross dialect, another name", ukcTable(pkID, ukcKey("le", ukcExpr("lower((k)::text)", "postgres"))), ukcTable(pkID, ukcKey("other", ukcExpr("lower(`k`)", "mysql"))), "other"},
		{"cross dialect, same name, a plain part moved", ukcTable(pkID, ukcKey("le", ukcCol("a"), ukcExpr("lower((k)::text)", "postgres"))), ukcTable(pkID, ukcKey("le", ukcCol("b"), ukcExpr("lower(`k`)", "mysql"))), "le"},
		{"cross dialect, same name, expression against column", ukcTable(pkID, ukcKey("le", ukcExpr("lower((k)::text)", "postgres"))), ukcTable(pkID, ukcKey("le", ukcCol("k"))), "le"},

		// ---- partial keys
		{"same predicate", ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(a IS NULL)", PredicateDialect: "postgres"}), ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(a IS NULL)", PredicateDialect: "postgres"}), ""},
		{"a partial source key does not imply a full target key", ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(a IS NULL)", PredicateDialect: "postgres"}), ukcTable(pkID, ukcKey("p", ukcCol("k"))), "p"},
		{"a full source key implies a partial target key", ukcTable(pkID, ukcKey("p", ukcCol("k"))), ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(a IS NULL)", PredicateDialect: "postgres"}), ""},
		{"a different predicate", ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(a IS NULL)", PredicateDialect: "postgres"}), ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(b IS NULL)", PredicateDialect: "postgres"}), "p"},
		{"cross-dialect predicate, same name", ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "a IS NULL", PredicateDialect: "sqlite"}), ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(a IS NULL)", PredicateDialect: "postgres"}), ""},
		{"cross-dialect predicate, another name", ukcTable(pkID, &ir.Index{Name: "p", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "a IS NULL", PredicateDialect: "sqlite"}), ukcTable(pkID, &ir.Index{Name: "q", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, Predicate: "(a IS NULL)", PredicateDialect: "postgres"}), "q"},

		// ---- NULLS NOT DISTINCT
		{"target NULLS NOT DISTINCT, source not", ukcTable(pkID, ukcKey("k", ukcCol("k"))), ukcTable(pkID, &ir.Index{Name: "k", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, ConstraintNullsNotDistinct: true}), "k"},
		{"both NULLS NOT DISTINCT", ukcTable(pkID, &ir.Index{Name: "k", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, ConstraintNullsNotDistinct: true}), ukcTable(pkID, &ir.Index{Name: "k", Unique: true, Columns: []ir.IndexColumn{ukcCol("k")}, ConstraintNullsNotDistinct: true}), ""},

		// ---- index collations
		{"source NOCASE implies a binary target key", ukcTable(pkID, ukcKey("k", ir.IndexColumn{Column: "k", Collation: "NOCASE"})), ukcTable(pkID, ukcKey("k", ukcCol("k"))), ""},
		{"a binary source key does not imply a NOCASE target key", ukcTable(pkID, ukcKey("k", ukcCol("k"))), ukcTable(pkID, ukcKey("k", ir.IndexColumn{Column: "k", Collation: "NOCASE"})), "k"},
		{"NOCASE does not imply RTRIM", ukcTable(pkID, ukcKey("k", ir.IndexColumn{Column: "k", Collation: "NOCASE"})), ukcTable(pkID, ukcKey("k", ir.IndexColumn{Column: "k", Collation: "RTRIM"})), "k"},

		// ---- the two shapes the apply guard cannot see (residuals (a) and (b))
		{"(a) keyless, the only unique key dropped", ukcTable(nil), ukcTable(nil, ukcKey("kd_k", ukcCol("k"))), "kd_k"},
		{"(b) keyless, a NULLable key kept and the other dropped", ukcTable(nil, ukcKey("kd_id", ukcCol("id"))), ukcTable(nil, ukcKey("kd_id", ukcCol("id")), ukcKey("kd_k", ukcCol("k"))), "kd_k"},
		{"keyless on both sides", ukcTable(nil), ukcTable(nil), ""},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			if got := ukcNames(UncoveredTargetUniqueKeys(c.source, c.target)); got != c.want {
				t.Errorf("uncovered = %q, want %q", got, c.want)
			}
		})
	}
}

// TestUncoveredTargetUniqueKeys_TargetOnlyColumns pins the exemption for a
// key on a column the target generates on every insert, and its limits.
func TestUncoveredTargetUniqueKeys_TargetOnlyColumns(t *testing.T) {
	src := ukcTable(nil, ukcKey("kd_k", ukcCol("k")))
	withCol := func(c *ir.Column, keys ...*ir.Index) *ir.Table {
		tbl := ukcTable(nil, keys...)
		tbl.Columns = append(tbl.Columns, c)
		return tbl
	}
	cells := []struct {
		name   string
		source *ir.Table
		target *ir.Table
		want   string
	}{
		{"auto-increment surrogate primary key", ukcTable(nil), withCol(&ir.Column{Name: "my_row_id", Type: ir.Integer{Width: 64, AutoIncrement: true}}, nil), ""},
		{"identity surrogate", ukcTable(nil), withCol(&ir.Column{Name: "sid", Type: ir.Integer{Width: 64}, Identity: &ir.IdentityOptions{}}, ukcKey("sid_u", ukcCol("sid"))), ""},
		{"a defaulted target-only column", ukcTable(nil), withCol(&ir.Column{Name: "extra", Type: ir.Integer{Width: 32}}, ukcKey("ex", ukcCol("extra"))), "ex"},
		{"a shard column added to a key the source enforces", src, withCol(&ir.Column{Name: "shard", Type: ir.Text{}}, ukcKey("kd_k", ukcCol("shard"), ukcCol("k"))), ""},
		{"a shard column added to a key the source dropped", ukcTable(nil), withCol(&ir.Column{Name: "shard", Type: ir.Text{}}, ukcKey("kd_k", ukcCol("shard"), ukcCol("k"))), "kd_k"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			// A nil key in withCol's list is the surrogate-PK cell: attach it.
			if len(c.target.Indexes) > 0 && c.target.Indexes[0] == nil {
				c.target.Indexes = nil
				c.target.PrimaryKey = ukcKey("PRIMARY", ukcCol("my_row_id"))
			}
			if got := ukcNames(UncoveredTargetUniqueKeys(c.source, c.target)); got != c.want {
				t.Errorf("uncovered = %q, want %q", got, c.want)
			}
		})
	}
	if got := UncoveredTargetUniqueKeys(nil, ukcTable(nil, ukcKey("k", ukcCol("k")))); got != nil {
		t.Errorf("a table the source no longer has must not be judged here, got %v", got)
	}
}
