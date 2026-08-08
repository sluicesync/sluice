// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_MySQLDispatchRoster is this engine's
// instantiation of the Bug 233 gate: every column-type dispatch in the
// package either reads the STORAGE type through ir.UnwrapDomain or
// carries a written reason below.
//
// See the domaingate package doc for what a pass proves and what it
// does not. In short: it grades which BRANCH runs for a DOMAIN-typed
// column, and says nothing about what that branch then computes.
func TestDomainTransparency_MySQLDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "mysql",
		// The floor is well under the current site count; it exists to
		// catch the dispatch shape being refactored out from under the
		// walker, not to track the count.
		MinSites: 15,
		Allowed:  mysqlDomainDispatchExemptions,
	})
}

// mysqlDomainDispatchExemptions is the fail-by-default roster. Two
// reasons appear, and the distinction between them is the whole map:
//
//   - SOURCE-SIDE — the schema being dispatched on was read from a
//     MySQL catalog, and MySQL has no DOMAIN, so this engine's own
//     SchemaReader can never populate ir.Domain. Unwrapping would be
//     inert, and the site would then imply a hazard it does not have.
//   - TARGET-DERIVED — the descriptors come from the TARGET's
//     information_schema (the ChangeApplier's whole design), which is
//     the same argument one level out.
//
// A site that lands a VALUE or emits DDL from a SOURCE schema is never
// exempt: that is the population Bug 233 lived in.
var mysqlDomainDispatchExemptions = map[string]string{
	"cdc_reader.go:decodeBinlogRow:col.Type": "SOURCE-SIDE: the binlog reader types its values from the " +
		"MySQL source's own schema, which has no DOMAIN.",
	"change_applier.go:placeholderFor:col.Type": "TARGET-DERIVED: the applier resolves column types from " +
		"the TARGET's information_schema (its own doc says so, and it is the reason the array-leaf " +
		"refusal exists next door), so no descriptor it holds can be an ir.Domain.",
	"row_reader.go:stream:col.Type": "SOURCE-SIDE: the row stream decodes MySQL's own result set against " +
		"the MySQL-read schema.",
	"row_reader.go:selectColumnExpr:c.Type": "SOURCE-SIDE: the SELECT list is built against the MySQL " +
		"source's own columns.",
	"schema_reader.go:jsonValidCheckTarget:col.Type": "SOURCE-SIDE: this IS the MySQL catalog read that " +
		"produces the IR; nothing upstream of it can have wrapped a type.",
	"schema_reader.go:applyMariaDBGeometrySRID:col.Type": "SOURCE-SIDE: same MySQL catalog read.",
}
