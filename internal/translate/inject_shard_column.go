// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// InjectShardColumn returns a copy of s with a sluice-injected
// discriminator column named `name` appended to every PK-bearing
// table, and that table's primary key rewritten as a composite
// (discriminator, ...original PK columns). ADR-0048 Shape A's
// schema half — the I/O-free, copy-on-write IR pass the orchestrator
// runs right after [ApplyMappings] / [ApplyExpressionOverrides] and
// before any schema-apply / cross-engine supportability preflight.
//
// `valueType` is the IR type of the discriminator column. The
// orchestrator threads in [ir.Varchar] for the typical
// `--inject-shard-column source_shard_id=us-east-1` shape (VALUE
// is a stable engine-neutral string the per-shard run stamps onto
// every row); future expansion can thread an integer or UUID without
// changing this signature. The column is always NOT NULL, has no
// default (every row gets a stamped value via the value-half wraps
// — there is no NULL row), and is marked
// [ir.Column.SluiceInjected]=true so `schema diff` / `verify`
// suppress it from `ColumnsExtra` against the per-shard source.
//
// Tables WITHOUT a declared primary key refuse loudly: a composite
// PK requires a base PK, and Shape A's correctness rests on the
// composite PK making each shard's rows disjoint regardless of
// resume status (DP-2). Operator-actionable recovery: add a PK to
// the source table or `--exclude-table` it from the consolidated
// stream.
//
// Pure function — no engine I/O, no shared mutation. Copy-on-write:
// tables that get the column rewritten are duplicated; everything
// else shares pointers with `s`.
func InjectShardColumn(s *ir.Schema, name string, valueType ir.Type) (*ir.Schema, error) {
	if s == nil {
		return nil, fmt.Errorf("translate: InjectShardColumn: schema is nil")
	}
	if name == "" {
		return nil, fmt.Errorf("translate: InjectShardColumn: column name is empty")
	}
	if valueType == nil {
		return nil, fmt.Errorf("translate: InjectShardColumn: value type is nil")
	}

	out := &ir.Schema{
		Tables: make([]*ir.Table, len(s.Tables)),
		Views:  s.Views,
	}
	for i, tbl := range s.Tables {
		if tbl == nil {
			out.Tables[i] = tbl
			continue
		}
		if tbl.PrimaryKey == nil || len(tbl.PrimaryKey.Columns) == 0 {
			// Composite-PK correctness is what makes Shape A disjoint
			// across shards (ADR-0048 Decision 1). A table without a
			// base PK has nothing to compose against — refuse loudly
			// so the operator either adds a PK on the source or
			// excludes the table from the consolidated stream.
			return nil, fmt.Errorf(
				"translate: InjectShardColumn: table %q has no primary key — "+
					"Shape A consolidation requires a composite PK (discriminator, …source PK) "+
					"on every consolidated table (ADR-0048). Recovery: add a PRIMARY KEY to %q on the source, "+
					"or re-run with --exclude-table=%s to skip it from the consolidated stream",
				tbl.Name, tbl.Name, tbl.Name,
			)
		}
		// Reject the silent-collision case where the operator's source
		// schema *already* has a column named `name` — stamping over
		// it would either redefine a real source column or shadow it
		// to the operator's confusion. Both flavours are operator
		// error; refuse with a recovery suggestion.
		if existing := findColumn(tbl, name); existing != nil {
			return nil, fmt.Errorf(
				"translate: InjectShardColumn: table %q already has a column named %q — "+
					"`--inject-shard-column %s=…` would collide with the existing source column. "+
					"Recovery: pick a different discriminator column name, "+
					"or rename the source column out of the way",
				tbl.Name, name, name,
			)
		}
		out.Tables[i] = rewriteTableForShard(tbl, name, valueType)
	}
	return out, nil
}

// rewriteTableForShard returns a copy of tbl with the discriminator
// column appended and the primary key rewritten as a composite
// (discriminator, ...origPKCols). Indexes / FKs / CHECKs / EXCLUDEs
// are deliberately NOT rewritten — they ride on the same shared-
// pointer slice as the original tbl so callers that further mutate
// the schema downstream don't observe spooky aliasing on those
// sub-structures (the slices themselves are not modified by this
// function, only the column list and PrimaryKey are reallocated).
//
// The discriminator column is always:
//   - NOT NULL (no default; every row gets a value via the value-
//     half wraps — there's no legal NULL row),
//   - SluiceInjected=true (provenance bit — diff/verify suppression),
//   - placed LAST in the column list (matches the spike's observed
//     shape and keeps the source's leading-column ordering stable),
//   - the LEADING entry in the rewritten composite PK (so the
//     composite key sorts shard-first, which is what the populated-
//     target bypass and the IdempotentRowWriter's ON CONFLICT
//     target rely on).
func rewriteTableForShard(tbl *ir.Table, name string, valueType ir.Type) *ir.Table {
	out := *tbl
	out.Columns = make([]*ir.Column, 0, len(tbl.Columns)+1)
	out.Columns = append(out.Columns, tbl.Columns...)
	out.Columns = append(out.Columns, &ir.Column{
		Name:           name,
		Type:           valueType,
		Nullable:       false,
		SluiceInjected: true,
	})
	// Composite PK: discriminator leads, then the original PK columns
	// in their declared order. The leading-shard ordering is the
	// disjointness invariant the populated-target preflight (DP-2)
	// asserts on; encoding it here keeps every consumer (bulk-copy
	// ON CONFLICT, CDC WHERE, diff) consistent without each having
	// to know the shard-first rule.
	npk := *tbl.PrimaryKey
	npk.Columns = make([]ir.IndexColumn, 0, len(tbl.PrimaryKey.Columns)+1)
	npk.Columns = append(npk.Columns, ir.IndexColumn{Column: name})
	npk.Columns = append(npk.Columns, tbl.PrimaryKey.Columns...)
	out.PrimaryKey = &npk
	return &out
}

// findColumn returns the *ir.Column with the matching name on tbl,
// or nil if no column with that name exists. Used by
// InjectShardColumn's pre-rewrite collision check.
func findColumn(tbl *ir.Table, name string) *ir.Column {
	if tbl == nil {
		return nil
	}
	for _, c := range tbl.Columns {
		if c != nil && c.Name == name {
			return c
		}
	}
	return nil
}
