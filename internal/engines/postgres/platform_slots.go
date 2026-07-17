// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Platform-internal replication slots — the named roster of slots a
// managed provider itself owns and depends on. sluice must never treat
// one as a leaked/abandoned consumer, and dropping one breaks the
// PLATFORM (its backups or its consensus machinery), not a sluice
// stream.
//
// Two wire-ups consume the roster:
//
//   - [SlotManager.List] annotates matching rows so `sluice slot list`
//     labels them platform-internal instead of leaving them to read as
//     stray consumers.
//   - [SlotManager.Drop] refuses a roster slot without --force —
//     "abandoned slot cleanup" muscle memory must not take the
//     provider's backup daemon with it.
//
// The ADR-0059 slot-health probe needs no wire-up: it is scoped to
// sluice's own slot by name, so a platform slot never reaches it.
//
// Add new entries as providers are probed — one line each, with the
// live-probe evidence in the comment. Only ALWAYS-present, exactly-
// named platform slots belong here; workload-created platform slots
// with variable names (e.g. Supabase's supabase_realtime*) stay out —
// a name-prefix roster would start swallowing user slots.
var platformInternalSlots = map[string]string{
	// Neon: every endpoint carries this always-present physical slot,
	// part of Neon's safekeeper (WAL proposer) consensus architecture.
	// Live-validated 2026-07-15.
	"wal_proposer_slot": "Neon safekeeper (WAL proposer) slot",

	// Aiven-lineage platforms (Vultr Managed PG live-probed 2026-07-16;
	// Aiven proper and DO Managed PG share the platform and very likely
	// the slot — unprobed): pghoard is the platform's own WAL-archiver/
	// backup daemon, always present and ACTIVE.
	"pghoard_local": "Aiven-lineage pghoard backup daemon (Vultr, Aiven; likely DigitalOcean)",
}

// platformInternalSlotNote reports whether name is a known
// platform-internal slot, returning its provider note for messages.
func platformInternalSlotNote(name string) (string, bool) {
	note, ok := platformInternalSlots[name]
	return note, ok
}
