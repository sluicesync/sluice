// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// preflightSpanningUnloggedTables runs the source engine's UNLOGGED-table
// census over the spanning sync's selected schema set (capture-
// completeness G2, 2026-08-26). A Postgres multi-schema spanning sync
// streams through a FOR ALL TABLES publication, which SILENTLY EXCLUDES
// unlogged tables while the cold copy includes them — the target would
// receive each table's initial rows and then freeze it at the snapshot
// forever, at exit 0. The census turns that into a coded refusal
// (SLUICE-E-CDC-UNLOGGED-TABLE) before anything is created on either
// side.
//
// Called at BOTH spanning stream-open chokepoints — [Streamer.
// coldStartMultiDatabase] (before the spanning snapshot opener creates
// the slot/publication) and [Streamer.warmResumeMultiDatabase] (before
// the server-wide CDC reader opens), because `ALTER TABLE … SET
// UNLOGGED` succeeds mid-sync under FOR ALL TABLES and silently drops
// the table from the publication (observed on PG 16); a flip during a
// live streaming window remains undetectable until the next open.
//
// The predicate hands the engine the sync's EFFECTIVE table scope so an
// unlogged table the operator already excluded via --exclude-table never
// trips the door (the Bug 246 discipline; [migcore.TableFilter.Allows]
// is exactly the predicate [migcore.ApplyTableFilter] scopes each
// database's schema with). Sources without the surface (MySQL — no
// unlogged concept) skip silently.
func (s *Streamer) preflightSpanningUnloggedTables(ctx context.Context, schemas []string) error {
	pf, ok := s.Source.(ir.UnloggedCapturePreflighter)
	if !ok {
		return nil
	}
	return pf.PreflightSpanningUnloggedTables(ctx, s.SourceDSN, schemas, func(_, table string) bool {
		return s.Filter.Allows(table)
	})
}

// preflightUnloggedAddTable is the `schema add-table` REGISTRATION door
// of the same census (audit 2026-08-27 A7). The spanning census above
// cannot cover a live-added table by construction — its predicate is the
// sync's BASE filter, and the effective scope a live add extends is base
// ∪ live-added (streamer_filter_flip.go) — so an UNLOGGED table
// registered mid-stream would backfill its rows once and then freeze
// forever at exit 0, the exact shape G2 refuses at stream open. Refusing
// at registration closes the gap at its source: the table never enters
// any live-add set, so no census predicate downstream has to see it.
//
// Runs before ANY side effect of [AddTable.Run] — target DDL, publication
// extend, snapshot — and before the dry-run report, so the plan surfaces
// the refusal too. Sources without the surface (MySQL family — no
// unlogged concept) skip silently, mirroring the spanning door.
func (a *AddTable) preflightUnloggedAddTable(ctx context.Context) error {
	pf, ok := a.Source.(ir.UnloggedCapturePreflighter)
	if !ok {
		return nil
	}
	return pf.PreflightAddTableUnlogged(ctx, a.SourceDSN, a.TableName)
}
