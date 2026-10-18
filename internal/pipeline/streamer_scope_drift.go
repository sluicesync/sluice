// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The warm-resume table-scope door (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321).
//
// # The harm
//
// A warm resume copies nothing. It continues the change stream from the
// persisted position, so a table that joins the stream's scope on a restart
// — a widened `--include-table`, a narrowed `--exclude-table`, a table
// created on the source since the cold start of a stream with no filter —
// is never copied. On a Postgres pgoutput source it is not even streamed:
// only the cold start scopes the publication (streamer_coldstart.go's
// EnsurePublication), and the warm resume's reader re-ensures it with no
// table list, which leaves it as it is. Measured on PG 18.6 → MySQL 8.4 and
// → PG 18.6 by the v0.162.0 regression cycle: the stream came back green,
// the added table stayed empty or absent on the target against four source
// rows, and nothing was logged.
//
// The second arm has the same root: `sluice schema add-table` adds the table
// to the publication before it copies it (it must — ADR-0021/0036 ordering),
// and a copy that failed left it there. A later restart that included the
// table streamed changes into a table whose copy never landed.
//
// # What the door compares, and against what independent evidence
//
// The scope is the current flags evaluated over the source catalog
// ([Streamer.streamTablesOf], the cold start's own two prunes). The question
// is which of those tables the stream established, and the answer must not
// come from the same predicate — re-evaluating the flags proves only that
// they agree with themselves. Two pieces of evidence, neither derived from
// the current flags:
//
//   - pg_publication_tables (pgoutput source, single-namespace stream): the
//     server's own expansion of the publication the stream reads through.
//     The cold start wrote it from the scope it COPIED, add-table extends it,
//     and nothing a warm resume does changes it — and it is not merely a
//     record but the delivery truth itself, since pgoutput decodes nothing
//     for a table the publication does not list. A table in scope and not in
//     it was never copied by this stream and cannot be streamed by it.
//   - the add-table pending record ([ir.PendingAddedTablesStore], every
//     source): written by add-table before its first change and cleared only
//     after the copy and the hand-off succeeded. It is evidence about the
//     copy, which publication membership is not (the membership is written
//     before the copy runs).
//
// # What it does not reach, stated
//
// Each of these is a table the stream streams but never copied, which no
// evidence the stream keeps today can tell from one it did copy — closing
// them needs a persisted per-stream record of the tables the stream
// established, which sluice does not keep:
//
//   - a MySQL-family binlog or VStream source, and the trigger-CDC sources:
//     their change stream carries every table, so a widened table is
//     streamed from the persisted position. With no target table the
//     applier already skips its events loudly (sluice_cdc_skipped_tables,
//     `sync health`); with a pre-created target table, rows written before
//     the persisted position never arrive. (Rows written after it do, which
//     is why the cycle's pre-created MySQL cell read EXACT: the table was
//     created during the stopped window.) INCLUDE-TABLE-SCOPE-DRIFT-STREAMED.
//   - a pgoutput stream re-widened to a table it narrowed away: the table
//     stayed in the publication while the narrowed run's filter dropped its
//     changes. INCLUDE-TABLE-RE-WIDEN.
//   - a pgoutput stream whose publication is FOR ALL TABLES (an operator's
//     --publication-name, a multi-schema stream): every table is delivered,
//     so a never-copied one passes.
//   - a publication that no longer exists at the warm resume: there is no
//     evidence to read, and the reader's open recreates it FOR ALL TABLES
//     (the Bug 267/270 path, unchanged here).
//   - an add-table that failed under a binary older than this door: it left
//     no pending record.
//
// # Narrowing is not refused
//
// A table that LEAVES the scope stops being applied: that is the operator's
// choice, the rows already on the target stay as they were, and no row the
// operator asked for goes missing. Refusing it would turn a deliberate
// configuration change into an outage. The loss is in coming back
// (INCLUDE-TABLE-RE-WIDEN above), which is where the residual is named.

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// publicationMembershipChecker is the optional source-engine surface the
// door reads publication membership through. Postgres implements it on its
// publication-scoped engine value; engines without publications omit it.
type publicationMembershipChecker interface {
	// UnpublishedTables returns which of tables the publication does not
	// deliver, and the publication's name; an empty name means there was no
	// publication to read (no evidence either way).
	UnpublishedTables(ctx context.Context, dsn string, tables []string) (missing []string, publication string, err error)
}

// publicationTableDropper is the optional source-engine surface add-table
// undoes its publication add through when the copy fails (Postgres).
type publicationTableDropper interface {
	DropPublicationTables(ctx context.Context, dsn string, tables []string) error
}

// scopeDriftTable is one table the door refuses, with the evidence that
// convicted it.
type scopeDriftTable struct {
	name string
	// unpublished: the stream's publication does not deliver it.
	unpublished bool
	// addIncomplete: its `sluice schema add-table` started and did not
	// complete.
	addIncomplete bool
}

// phaseRefuseIncludeTableScopeDrift is the warm-resume door. It runs on the
// two warm-resume dispatch branches of [Streamer.phaseOpenChangeStream],
// before the CDC reader opens, so a refusal has applied nothing and has not
// touched the publication.
func (s *Streamer) phaseRefuseIncludeTableScopeDrift(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	liveAdded, err := s.uniqueKeyDoorLiveAdded(ctx, applier, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	inScope := func(table string) bool { return s.Filter.Allows(table) || liveAdded[table] }

	drift := map[string]*scopeDriftTable{}
	entry := func(name string) *scopeDriftTable {
		if drift[name] == nil {
			drift[name] = &scopeDriftTable{name: name}
		}
		return drift[name]
	}

	if store, ok := applier.(ir.PendingAddedTablesStore); ok {
		pending, err := store.ReadPendingAddedTables(ctx, streamID)
		if err != nil {
			return controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
		}
		for _, t := range pending {
			if inScope(t) {
				entry(t).addIncomplete = true
			}
		}
	}

	unpublished, publication, err := s.unpublishedInScopeTables(ctx, inScope)
	if err != nil {
		return err
	}
	for _, t := range unpublished {
		entry(t).unpublished = true
	}

	if len(drift) == 0 {
		return nil
	}
	tables := make([]scopeDriftTable, 0, len(drift))
	for _, d := range drift {
		tables = append(tables, *d)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].name < tables[j].name })
	slog.ErrorContext(ctx, "warm resume refused: INCLUDE-TABLE-SCOPE-DRIFT",
		slog.String("stream_id", streamID), slog.Int("tables", len(tables)))
	return s.includeTableScopeDriftError(streamID, publication, tables)
}

// unpublishedInScopeTables returns the in-scope source tables the stream's
// publication does not deliver, with the publication's name for the message.
// Empty, with no error, where there is no publication evidence: a source
// without publications, a multi-namespace stream (whose spanning publication
// is FOR ALL TABLES), or a publication that does not exist.
func (s *Streamer) unpublishedInScopeTables(ctx context.Context, inScope func(string) bool) (unpublished []string, publication string, err error) {
	checker, ok := s.Source.(publicationMembershipChecker)
	if !ok || s.multiDatabaseMode() {
		return nil, "", nil
	}
	sr, schema, err := s.openScopedSourceSchema(ctx, s.SourceDSN, inScope)
	if err != nil {
		return nil, "", fmt.Errorf("pipeline: include-table scope door: %w", err)
	}
	defer migcore.CloseIf(sr)
	if schema == nil {
		return nil, "", nil
	}
	tables := s.streamTablesOf(schema, inScope)
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	unpublished, publication, err = checker.UnpublishedTables(ctx, s.SourceDSN, names)
	if err != nil {
		return nil, "", connectHint(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	return unpublished, publication, nil
}

// includeTableScopeDriftError renders the refusal. The remedies are the ones
// TestIncludeTableScopeDrift_* measured through the real CLI.
func (s *Streamer) includeTableScopeDriftError(streamID, publication string, tables []scopeDriftTable) error {
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		var why []string
		if t.unpublished {
			why = append(why, fmt.Sprintf("not in publication %q, so the source never sends its changes", publication))
		}
		if t.addIncomplete {
			why = append(why, "its `sluice schema add-table` started and did not complete, so its copy may be partial")
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", t.name, strings.Join(why, "; ")))
	}
	return fmt.Errorf(
		"pipeline: warm resume refused: %w: stream %q would resume with %d table(s) in its scope that it never "+
			"finished bringing in: %s. A warm resume copies nothing, so each would stay missing rows on the target "+
			"while the stream reported healthy; nothing has been applied by this run. "+
			"Remedy, per table, with the stream stopped: run `sluice schema add-table <table> --source-driver %s "+
			"--source <SOURCE-DSN> --target-driver %s --target <TARGET-DSN> --stream-id %s --yes` (it refuses a "+
			"target table that already holds rows: drop or truncate a partial copy first), then re-run this command "+
			"unchanged. Or take the table out of scope (--include-table / --exclude-table). Or re-copy every table "+
			"with --restart-from-scratch (on a Postgres source, first drop the stream's replication slot with "+
			"`sluice slot drop`, as that run's refusal instructs)",
		ir.ErrIncludeTableScopeDrift, streamID, len(tables), strings.Join(parts, ", "),
		s.Source.Name(), s.Target.Name(), streamID,
	)
}

// clearPendingAddedTablesAfterColdStart clears the add-table pending record
// for tables a cold start has just copied in full: the copy is the add's
// remedy (--restart-from-scratch is one of the door's named escapes). A
// failure WARNs rather than failing a cold start whose copy succeeded — the
// record it leaves only makes the next warm resume refuse, loudly, naming
// the table.
func (s *Streamer) clearPendingAddedTablesAfterColdStart(ctx context.Context, applier ir.ChangeApplier, streamID string, copied []string) {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok || len(copied) == 0 {
		return
	}
	if err := store.ClearPendingAddedTables(ctx, streamID, copied); err != nil {
		slog.WarnContext(ctx, "cold start: could not clear the add-table pending record for the tables it copied; "+
			"the next warm resume will refuse INCLUDE-TABLE-SCOPE-DRIFT naming any of them that was pending",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
	}
}
