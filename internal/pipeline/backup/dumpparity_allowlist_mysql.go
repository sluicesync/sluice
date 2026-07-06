// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// DumpParityAllowlistMySQL is the single reviewable answer to "what does
// sluice knowingly not carry on a same-engine MySQL → MySQL migration?"
// (roadmap item 51, Phase 2). It is the MySQL sibling of
// DumpParityAllowlist; the two are kept as separate lists (rather than
// one list with an engine axis) because the KEY grammars differ — PG
// keys are verb + schema-qualified name ("CREATE TABLE public.orders"),
// MySQL keys are decomposed element identities ("orders COLUMN id",
// "orders CONSTRAINT orders_customer_fk", "orders OPTIONS") — so a
// shared pattern list would be a false economy.
//
// Every entry cites the doc/ADR/source that declares the degradation, or
// carries the DumpParityTriageCitation marker for a latent gap the
// oracle found that has NOT been triaged into a documented decision yet.
// TRIAGE entries are loudly banner-logged by the harness on every run;
// they are debt, not policy.
//
// Patterns match statement KEYS under path.Match glob semantics (via the
// shared MatchDumpParityAllowlist). First match wins — keep entries
// most-specific-first. Every hit is logged in the test output so the
// active list is visible in CI logs.
var DumpParityAllowlistMySQL = []dumpParityAllowlistEntry{
	{
		Pattern:  "*sluice_migrate_*",
		Reason:   "sluice's own per-target migrate-state tables (resume machinery: sluice_migrate_state + sluice_migrate_table_progress); operational metadata, not migrated schema, so every element of them is sluice-target-only",
		Citation: "internal/engines/mysql/migration_state.go",
	},
}
