// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// The corpus dump-parity allowlists (roadmap item 52 × item 51): the
// single reviewable answer to "what does sluice knowingly not carry
// when the item-51 restore-parity oracle is pointed at the item-52
// real-world corpus schemas?"
//
// These are SEPARATE from DumpParityAllowlist / DumpParityAllowlistMySQL
// (the kitchen-sink lists) on purpose. The kitchen-sink lists are keyed
// to the seed's specific object names ("CREATE TABLE public.orders",
// "CREATE SEQUENCE public.legacy_counters_id_seq"); the corpus schemas
// are real applications with arbitrary object names, so the corpus lists
// match by statement CLASS (glob over the object-identity KEY) so one
// entry holds across every corpus member. Every entry cites the
// doc/ADR/source that declares the degradation, or carries the
// DumpParityTriageCitation marker for a latent gap the oracle found that
// has NOT been triaged into a documented decision yet — same discipline
// as the kitchen-sink lists.
//
// Patterns match statement KEYS (see dumpStatementKey / the MySQL
// element keyer) under path.Match glob semantics via the shared
// MatchDumpParityAllowlist. First match wins — keep entries
// most-specific-first. Every hit is logged in the harness output so the
// active list is visible in CI logs.

// DumpParityCorpusAllowlistPG covers the generic same-engine PG → PG
// degradations sluice applies to any real schema.
var DumpParityCorpusAllowlistPG = []dumpParityAllowlistEntry{
	{
		Pattern:  "*sluice_migrate_*",
		Reason:   "sluice's own per-target migrate-state tables (resume machinery); operational metadata, not migrated schema",
		Citation: "internal/pipeline/resume.go",
	},
}

// DumpParityCorpusAllowlistMySQL covers the generic same-engine
// MySQL → MySQL degradations sluice applies to any real schema.
var DumpParityCorpusAllowlistMySQL = []dumpParityAllowlistEntry{
	{
		Pattern:  "*sluice_migrate_*",
		Reason:   "sluice's own per-target migrate-state tables (resume machinery: sluice_migrate_state + sluice_migrate_table_progress); operational metadata, not migrated schema",
		Citation: "internal/engines/mysql/migration_state.go",
	},
}
