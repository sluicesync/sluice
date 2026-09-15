// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// SkippedTableRemedy is the operator remedy every skip surface (the
// applier's once-per-table WARN, `sync health`, `sync status`, the
// stop summary) names, spelled once so the wording cannot drift: the
// skipped rows are still on the source, and the operator either
// re-attaches the table or makes the exclusion explicit.
//
// The parenthetical is scoped to ROW changes on purpose. It used to say
// "the source still holds every skipped row", which is false for the
// one op that removes rows: a skipped TRUNCATE means the source has
// already dropped them (audit 2026-09-15 A0915-MYSQL-HIGH-1 measured
// the WARN steering an operator away from the fix). The ledger and the
// CLI surfaces that read from it do not know the op, so this shared
// text stays true for every op; the applier's WARN, which does know
// the op, names the TRUNCATE-specific remedy through
// [SkippedTableRemedyFor].
const SkippedTableRemedy = "re-attach it with `sluice schema add-table` (fresh table snapshot — for skipped INSERT/UPDATE/DELETE events the source still holds every row; a skipped TRUNCATE is the one event for which it does not), or make the exclusion explicit with a table filter"

// SkippedTableTruncateRemedy is the remedy the applier's WARN names when
// the first skipped event for a table is a TRUNCATE: the sentence that
// holds for row changes ("the source still holds every skipped row") is
// exactly wrong here, and the likeliest reason a TRUNCATE alone misses
// its target table — the target holds it under another spelling — is
// the case where the target is now AHEAD of the source and needs the
// truncate applied by hand before anything is re-attached.
const SkippedTableTruncateRemedy = "the skipped event is a TRUNCATE, so the source does NOT still hold the rows it dropped; if the target holds this table under another spelling it is now AHEAD of the source — truncate it deliberately, or re-attach it with `sluice schema add-table` (fresh table snapshot), or make the exclusion explicit with a table filter"

// SkippedTableRemedyFor picks the remedy for the first skipped op on a
// table: [SkippedTableTruncateRemedy] for "truncate", [SkippedTableRemedy]
// for every row change. The op strings are the applier's own
// ("insert"/"update"/"delete"/"truncate").
func SkippedTableRemedyFor(op string) string {
	if op == "truncate" {
		return SkippedTableTruncateRemedy
	}
	return SkippedTableRemedy
}

// SkippedTableRecord is one row of the durable unknown-target-table
// skip ledger (`sluice_cdc_skipped_tables`, audit C-11): when a CDC
// stream carries changes for a table the TARGET lacks, every engine's
// applier skips them — WARN once per table — and counts every skip
// here, per (stream, table). The record is what makes the skip
// health-visible rather than log-only: `sync status`, the `sync stop`
// summary, and `sync health` all surface it, and a nonzero SkipCount
// trips `sync health`.
//
// Why skip-and-count instead of halting (the operator decision,
// 2026-08-12): the blast radius is inverted. The source still holds
// every skipped row, so a skipped table is always recoverable with
// `sluice schema add-table` (fresh table snapshot) — while a halted
// stream lags EVERY table and, if the halt outlives binlog/slot
// retention, loses the position itself, converting one table's
// operator-induced drift into a whole-database re-snapshot.
type SkippedTableRecord struct {
	// StreamID is the sync stream whose applier skipped the changes.
	StreamID string

	// Table is the target-side qualified "schema.table" (MySQL:
	// "database.table") name the stream carried changes for, stored
	// verbatim as the applier routed it.
	Table string

	// SkipCount is the cumulative number of skipped change events for
	// this (stream, table). At-least-once: a rolled-back batch that
	// retries re-counts its skips, so the count answers "did skips
	// happen, and roughly how many" — it is not an exactly-once ledger.
	// The applier coalesces the durable writes (audit H-4): skips are
	// accumulated in memory and flushed as one UPSERT per table at each
	// position-write boundary, so the count advances at tx / batch /
	// checkpoint granularity rather than per event — the at-least-once
	// contract is unchanged.
	SkipCount int64

	// FirstPosition / LastPosition are the source position tokens of
	// the first and most recent skipped events, stored verbatim
	// (opaque engine tokens — never parsed or normalized on this path).
	FirstPosition string
	LastPosition  string

	// FirstSkippedAt / LastSkippedAt are target-clock timestamps of the
	// first and most recent skip.
	FirstSkippedAt time.Time
	LastSkippedAt  time.Time
}

// SkippedTableLister is the optional surface a [ChangeApplier]
// implements to expose its durable unknown-target-table skip records
// (audit C-11). Same optional-interface shape as
// [ShardConsolidationLeaseLister]: the CLI (`sync status`, `sync stop`,
// `sync health`) and the streamer's stop summary type-assert for it;
// engines without the surface simply don't surface skip records.
//
// Both shipping CDC-target engines (mysql, postgres) implement it —
// pinned in each engine's capabilities_assert.go. Tolerant of the
// control table being absent (a never-CDC'd target returns an empty
// list, not an error).
type SkippedTableLister interface {
	ListSkippedTables(ctx context.Context) ([]SkippedTableRecord, error)
}
