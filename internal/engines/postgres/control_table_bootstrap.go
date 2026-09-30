// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "sluicesync.dev/sluice/internal/ir"

// bootstrapSchema qualifies the printed bootstrap statements. The control
// tables live in the DSN's `schema` (default public); an operator whose
// DSN names another schema edits the qualifier, and the runtime refusal
// ([controlTable.ddlRequired]) always prints the statement for the schema
// actually in use.
const bootstrapSchema = "public"

// bootstrapControlTables is the migrate-state + cdc-state set, the same set
// the MySQL family prints: every table a `migrate` or `sync` run ensures on
// every start. Keysets and target-metrics history are left out on the MySQL
// family's terms — each is ensured only by the feature that uses it, whose
// refusal names its own statement.
func bootstrapControlTables(schema string) []controlTable {
	return append(
		migrateStateTables(schema),
		cdcStateTable(schema),
		schemaHistoryTable(schema),
		shardConsolidationLeaseTable(schema),
		skippedTablesTable(schema),
		controlTable{name: applyMarksTableName, schema: schema, create: applyMarksTableDDL(schema)},
	)
}

// ControlTableDDLGuidance implements [ir.ControlTableDDLProvider].
func (e Engine) ControlTableDDLGuidance() []string {
	return []string{
		"Run as the tables' owner, or a role with CREATE on the schema. Every statement",
		"is idempotent, and brings an older-shape table to the current one. Afterwards",
		"a sync role needs only SELECT, INSERT, UPDATE and DELETE on these tables.",
		"The statements name schema " + bootstrapSchema + "; if your target DSN sets ?schema=, use that schema instead.",
	}
}

// ControlTableDDL implements [ir.ControlTableDDLProvider]: the statements
// that bring every control table to its current shape — each table's
// CREATE, then the ADD COLUMN / CREATE INDEX statements later releases
// added — single-sourced from the [controlTable] values the ensure paths
// execute (GC-40 (a)). Every statement is idempotent, so an owner can run
// the whole set against a fresh or an older-shape target, after which a
// role holding only SELECT/INSERT/UPDATE/DELETE on the tables starts
// without issuing any DDL.
func (e Engine) ControlTableDDL() []ir.ControlTableStatement {
	var out []ir.ControlTableStatement
	for _, t := range bootstrapControlTables(bootstrapSchema) {
		for _, stmt := range t.statements() {
			out = append(out, ir.ControlTableStatement{Table: t.name, DDL: stmt})
		}
	}
	return out
}
