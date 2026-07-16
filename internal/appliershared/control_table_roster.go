// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

// # The sluice control-table roster (roadmap item 65b)
//
// Every table sluice itself creates on a source or target — CDC
// positions, migrate-state, schema history, capture logs, … — is
// bookkeeping, never user data. The engines' schema readers exclude
// the roster from user-table enumeration so a promoted ex-target (the
// cutover flow: yesterday's sync target becomes today's migration
// source) doesn't carry sluice internals along as "your migration has
// an extra table" surprises.
//
// The roster lives HERE, once, because the readers previously each
// kept a hand-maintained subset (5 of 11 names on PG, 3 on MySQL) and
// new control tables silently missed both. Names owned by packages
// this one cannot import without a cycle (pgtrigger → postgres →
// appliershared; pipeline → appliershared) are duplicated as literals
// — the same precedent the PG reader's Bug 93 exclusion used — and a
// both-directions source-scan test (control_table_roster_test.go, the
// error-code doc-sync pattern) pins every literal against the owning
// package's constant so the duplication cannot drift and a future
// control table cannot be forgotten.

import "log/slog"

// ControlTableNames returns the full roster of sluice-owned
// control/bookkeeping table names, every one excluded from user-table
// enumeration by the engines' schema readers. All names are
// sluice-reserved: a user table with one of these names would itself
// collide with sluice's own artifact, so excluding them on every
// engine — even where a given table cannot occur (MySQL has no
// trigger-CDC engine, so no change-log trio) — is harmless and keeps
// the roster single-sourced.
//
// Returned as a fresh slice so callers can't mutate the roster.
func ControlTableNames() []string {
	return []string{
		// This package's own tables.
		ControlTableName,                 // sluice_cdc_state
		ShardConsolidationLeaseTableName, // sluice_shard_consolidation_lease

		// internal/migratestate (HeaderTableName / ProgressTableName —
		// literal to keep this package's dependency surface at ir only).
		"sluice_migrate_state",
		"sluice_migrate_table_progress",

		// Per-engine bookkeeping (mysql/postgres each define the same
		// package-private constants; schema_history.go /
		// target_metrics_history.go / keyset_store.go).
		"sluice_cdc_schema_history",
		"sluice_target_metrics_history",
		"sluice_keysets",

		// internal/pipeline's source-side heartbeat table
		// (DefaultSourceHeartbeatTableName). The name is the DEFAULT —
		// an operator-renamed table (--source-heartbeat-table-name) is
		// not excludable here (use --exclude-table for a custom name).
		"sluice_heartbeat",

		// The trigger-CDC capture tables (pgtrigger.ChangeLogTable /
		// ChangeLogMetaTable; the sqlite trio adds ChangeLogColumnsTable).
		"sluice_change_log",
		"sluice_change_log_meta",
		"sluice_change_log_columns",
	}
}

// controlTableSet is the roster as a membership set, built once.
var controlTableSet = func() map[string]bool {
	set := map[string]bool{}
	for _, name := range ControlTableNames() {
		set[name] = true
	}
	return set
}()

// IsControlTable reports whether name is on the control-table roster.
// Every schema-reader door — live catalog readers (mysql, postgres,
// sqlite, d1) and file/dump readers (mydumper, flatfile) — consults
// this ONE predicate so the exclusion cannot diverge per door (the
// audit-2026-07-15 MED-D0-6 class: the roster reached the live readers
// but not the dump/flat-file doors). Exact match, matching how sluice
// itself names the tables (always lowercase).
func IsControlTable(name string) bool {
	return controlTableSet[name]
}

// LogExcludedControlTables surfaces a roster exclusion the moment it
// actually bites: one INFO naming the count (the loud-failure tenet —
// a silently thinner table list looks like a sluice bug from the
// outside), the names themselves at DEBUG. No-op when nothing was
// excluded, so ordinary sources stay quiet.
func LogExcludedControlTables(engine string, names []string) {
	if len(names) == 0 {
		return
	}
	slog.Info("excluded sluice control tables present on the source (sluice bookkeeping, never user data; "+
		"they are not enumerated as user tables)",
		slog.String("engine", engine), slog.Int("count", len(names)))
	slog.Debug("excluded control tables", slog.String("engine", engine), slog.Any("tables", names))
}
