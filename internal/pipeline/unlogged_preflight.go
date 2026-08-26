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
