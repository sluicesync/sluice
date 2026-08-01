// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The mechanical gate for the control-table identifier collation.
//
// Every MySQL control-table CREATE declares `DEFAULT CHARSET=utf8mb4`
// with no COLLATE clause; on MySQL 8 that resolves to the case- AND
// accent-INSENSITIVE utf8mb4_0900_ai_ci. Any character column that is a
// KEY part or an equality PREDICATE then folds two distinct source
// identifiers into one row — `Foo` and `foo`, or `café` and `cafe` —
// and because the writes are `ON DUPLICATE KEY UPDATE`, one identifier's
// state silently OVERWRITES the other's. On
// `sluice_migrate_table_progress` that means a `--resume` restarts `Foo`
// from `foo`'s LastPK and never copies the rows below it, exit 0.
//
// A comment saying "identifier columns are binary-collated" would be a
// hypothesis. This is the test that fails when it stops being true: it
// re-derives the rule from the RENDERED DDL rather than restating it, so
// a NEW key column added to any of these tables without the COLLATE
// clause fails the build.
package mysql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// controlDDLPredicateColumns lists, per control table, the character
// columns that are NOT key parts but ARE used as SQL equality
// predicates — the other way a collation folds two identifiers
// together. Key parts are derived from the DDL itself (see
// TestControlTableIdentifierColumnsAreBinaryCollated); these cannot be,
// so they are curated, with the query that needs them named.
var controlDDLPredicateColumns = map[string][]string{
	// loadRetainedSchemaVersions / the applier schema cache / diagnose
	// all select `WHERE stream_id = ? AND schema_name = ? AND table_name = ?`.
	"sluice_cdc_schema_history": {"stream_id", "schema_name", "table_name"},
	// The ADR-0054 lease CAS is `WHERE target_table_full_name = ? AND
	// lease_holder_stream_id = ?` — a case-folded holder id would let
	// the wrong stream extend or release another's lease.
	"sluice_shard_consolidation_lease": {"lease_holder_stream_id"},
}

// controlDDLs is every control-table CREATE the MySQL engine emits,
// rendered exactly as it is shipped. Keep this in sync with the DDL
// renderers — TestControlDDLInventoryIsComplete guards that.
func controlDDLs(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		controlTableName:                 controlTableDDL(""),
		shardConsolidationLeaseTableName: shardConsolidationLeaseTableDDL(""),
		schemaHistoryTableName:           schemaHistoryTableDDL(""),
		cdcQueryTimeoutRaiseTableName:    cdcQueryTimeoutRaiseTableDDL(""),
		migrateStateTableName:            migrateStateHeaderDDL(),
		migrateProgressTableName:         migrateProgressDDL(),
		keysetTableName:                  keysetTableDDL(),
		targetMetricsHistoryTableName:    targetMetricsHistoryTableDDL(),
	}
}

// TestControlTableIdentifierColumnsAreBinaryCollated is the gate. For
// every control-table DDL: parse the column definitions and the key
// column lists out of the rendered statement, then require that every
// CHARACTER column named in a key — or in the curated predicate list —
// carries `COLLATE utf8mb4_bin`.
//
// Derived from the DDL, not restated: adding `owner_name VARCHAR(255)`
// to a PRIMARY KEY without the collate clause fails here.
func TestControlTableIdentifierColumnsAreBinaryCollated(t *testing.T) {
	for table, ddl := range controlDDLs(t) {
		t.Run(table, func(t *testing.T) {
			cols := parseDDLColumns(t, ddl)
			need := map[string]string{}
			for _, c := range parseDDLKeyColumns(ddl) {
				need[c] = "key part"
			}
			for _, c := range controlDDLPredicateColumns[table] {
				need[c] = "equality predicate"
			}
			checked := 0
			for name, def := range cols {
				why, wanted := need[name]
				if !wanted || !ddlColumnIsCharacterTyped(def) {
					continue
				}
				checked++
				if !strings.Contains(def, controlIdentifierCollateClause) {
					t.Errorf("column %q (%s) is a character %s but does not declare %q:\n\t%s",
						name, table, why, controlIdentifierCollateClause, def)
				}
			}
			if checked == 0 {
				t.Errorf("no character identifier column found — the DDL parse is vacuous, not clean:\n%s", ddl)
			}
			// Curated predicate columns must actually exist, or the list
			// has rotted against a renamed column.
			for _, c := range controlDDLPredicateColumns[table] {
				if _, ok := cols[c]; !ok {
					t.Errorf("curated predicate column %q is not in the %s DDL any more", c, table)
				}
			}
		})
	}
}

// TestControlDDLInventoryIsComplete fails when a control-table CREATE
// exists in the package that the collation gate above never sees —
// otherwise the gate would stay green precisely by not knowing about
// the new table, which is the self-referential failure mode a gate is
// supposed to be immune to.
//
// The inventory is re-derived from the SOURCE: every control-table DDL
// ends its CREATE with the shared `ENGINE=InnoDB DEFAULT
// CHARSET=utf8mb4` table-options suffix, and only inside a string
// literal (comments mentioning it are excluded by the `//` test). The
// user-table emitter in ddl_emit.go writes a bare `ENGINE=InnoDB` with
// no charset clause, so it is correctly outside this set — a source
// table's own charset/collation is carried faithfully there and must
// never be overridden with utf8mb4_bin.
func TestControlDDLInventoryIsComplete(t *testing.T) {
	const marker = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package sources: %v", err)
	}
	var sites []string
	seenExempt := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || !strings.Contains(trimmed, marker) {
				continue
			}
			if _, exempt := controlDDLCollationExempt[f]; exempt {
				seenExempt[f] = true
				continue
			}
			sites = append(sites, fmt.Sprintf("%s:%d", f, i+1))
		}
	}
	if len(sites) != len(controlDDLs(t)) {
		t.Fatalf("found %d non-exempt control-table DDL sites in the package but controlDDLs covers %d;\n"+
			"a control table was added or removed without updating the collation gate\nsites: %v",
			len(sites), len(controlDDLs(t)), sites)
	}
	// An exemption that no longer names a real DDL site is a stale
	// decision, not a passing one.
	for f := range controlDDLCollationExempt {
		if !seenExempt[f] {
			t.Errorf("%s is listed as collation-exempt but emits no control-table DDL any more; drop the exemption", f)
		}
	}
}

// controlDDLCollationExempt names the DDL sites that deliberately do
// NOT get binary-collated identifier columns, with the reason. An
// exemption is a decision that has to be written down, not a gap.
var controlDDLCollationExempt = map[string]string{
	// The source-side heartbeat table (ADR-0061). Its stream_id is a
	// display label, not a key: rows are keyed by an AUTO_INCREMENT id,
	// the table is written append-only, and the only read is the prune
	// `WHERE ts < …`. No equality is ever taken on stream_id, so two
	// case-differing stream ids cannot collide. It is also the one table
	// here whose NAME the operator chooses (--heartbeat-table-name), and
	// it is created on the SOURCE, where sluice is a guest.
	"heartbeat_writer.go": "stream_id is an append-only display label, never a key or an equality predicate",
}

// parseDDLColumns pulls `name <rest-of-definition>` pairs out of a
// CREATE TABLE body, skipping the key clauses and the trailing table
// options.
func parseDDLColumns(t *testing.T, ddl string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, line := range strings.Split(ddl, "\n") {
		def := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if def == "" {
			continue
		}
		upper := strings.ToUpper(def)
		switch {
		case strings.HasPrefix(upper, "CREATE TABLE"), strings.HasPrefix(upper, ")"),
			strings.HasPrefix(upper, "PRIMARY KEY"), strings.HasPrefix(upper, "KEY "),
			strings.HasPrefix(upper, "UNIQUE "), strings.HasPrefix(upper, "ON UPDATE"):
			continue
		}
		name, rest, ok := strings.Cut(def, " ")
		if !ok {
			continue
		}
		out[strings.Trim(name, "`")] = strings.TrimSpace(rest)
	}
	if len(out) == 0 {
		t.Fatalf("parsed no columns out of:\n%s", ddl)
	}
	return out
}

// parseDDLKeyColumns returns every column named in a PRIMARY KEY,
// UNIQUE, or KEY clause of the rendered DDL.
func parseDDLKeyColumns(ddl string) []string {
	var out []string
	for _, line := range strings.Split(ddl, "\n") {
		clause := strings.TrimSpace(line)
		upper := strings.ToUpper(clause)
		if !strings.HasPrefix(upper, "PRIMARY KEY") && !strings.HasPrefix(upper, "KEY ") &&
			!strings.HasPrefix(upper, "UNIQUE ") {
			continue
		}
		open := strings.Index(clause, "(")
		closeIdx := strings.LastIndex(clause, ")")
		if open < 0 || closeIdx < open {
			continue
		}
		for _, c := range strings.Split(clause[open+1:closeIdx], ",") {
			out = append(out, strings.Trim(strings.TrimSpace(c), "`"))
		}
	}
	return out
}

// ddlColumnIsCharacterTyped reports whether a column definition names a
// type a collation applies to. Non-character key parts (BIGINT, INT,
// TIMESTAMP) are outside the rule, and a BLOB is already byte-exact.
func ddlColumnIsCharacterTyped(def string) bool {
	upper := strings.ToUpper(strings.TrimSpace(def))
	for _, p := range []string{"VARCHAR", "CHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"} {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return false
}
