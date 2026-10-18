// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The warm-resume table-scope door (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321).
//
// # The harm
//
// A warm resume copies nothing. It continues the change stream from the
// persisted position, so a table that joins the stream's scope on a restart
// is never copied. On a Postgres pgoutput source it is not even streamed
// (only the cold start scopes the publication); on a binlog, VStream or
// trigger source it is streamed from the persisted position, so a target
// table pre-created for it receives the later rows and never the earlier
// ones. Measured by the v0.162.0 regression cycle: a stream widened with
// --include-table came back green while the added table stayed empty or
// absent, at exit 0. And `sluice schema add-table` adds its table to the
// publication before it copies it, so a failed add left a table the next
// restart streamed into with its original rows missing.
//
// # What the door compares, and against what independent evidence
//
// The scope is the current flags over the source catalog
// ([Streamer.readStreamScope], the cold start's own prunes). Re-evaluating
// the flags proves only that they agree with themselves, so the scope is
// compared against records the current flags did not write:
//
//   - the ESTABLISHED record ([ir.EstablishedTablesStore], every source):
//     what the cold start actually copied, plus every add-table that
//     completed, narrowed whenever a resume dropped a table (its changes are
//     discarded from then on). The primary evidence. It is per stream, so it
//     cannot be blinded by another stream the way a shared publication is:
//     `sluice_pub` is the union of every stream's scope, and
//     guardPublicationNarrowing refuses to narrow it while another slot
//     exists, so its membership says nothing about what THIS stream copied.
//   - the add-table PENDING record ([ir.PendingAddedTablesStore], every
//     source): an add that started and did not complete. It is evidence about
//     the copy, which publication membership is not (the add joins the
//     publication before it copies).
//   - for a stream with no established record (cold-started before it
//     existed) only: the publication's own pg_publication_tables membership,
//     and only when no other sluice slot exists on the source, so the
//     publication cannot be another stream's union. Otherwise there is no
//     evidence, and the door says so.
//
// # Whose intent the door enforces
//
// A table that is not established refuses only when an --include-table entry
// names it LITERALLY ([migcore.TableFilter.NamesLiterally]): the operator
// asked for that table and would otherwise get it with rows missing. A table
// admitted by a glob, or by an unfiltered stream, keeps ADR-0021's
// behaviour — a table created on the source after the cold start stays out
// until `sluice schema add-table` brings it in — and the door WARNs once per
// resume naming every such table, because "stays out" has always been silent
// and the cost of the rule is exactly that. A pending add-table refuses
// whatever admits its table: the stream would apply changes onto a partial
// copy.
//
// # Once per process
//
// The door runs at the first stream open of a Run only (scopeDriftDoorDone):
// the scope cannot change within a process, and a retry of a live stream
// while a `--no-drain` add-table is still copying would otherwise trip the
// pending record that add-table is about to clear.
//
// # Narrowing is not refused
//
// A table that LEAVES the scope stops being applied: the operator's choice,
// and the rows already on the target stay as they were. The record narrows
// with it, so coming BACK to that table — whose changes this resume begins
// discarding — refuses (INCLUDE-TABLE-RE-WIDEN, closed by the record).

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
// legacy fallback reads publication membership through. Postgres implements
// it on its publication-scoped engine value; engines without publications
// omit it.
type publicationMembershipChecker interface {
	// UnpublishedTables returns which of tables the publication does not
	// deliver and the publication's name (empty: there is no publication, no
	// evidence either way), and whether another sluice slot exists on the
	// source (shared: the membership may be another stream's).
	UnpublishedTables(ctx context.Context, dsn string, tables []string) (missing []string, publication string, shared bool, err error)
}

// publicationTableDropper is the optional source-engine surface add-table
// undoes its publication add through when the copy fails (Postgres).
type publicationTableDropper interface {
	DropPublicationTables(ctx context.Context, dsn string, tables []string) error
}

// scopeTable is one source table in the stream's current scope.
type scopeTable struct {
	namespace string // "" on a single-namespace stream
	name      string
}

// key is the table's entry in the established record.
func (t scopeTable) key() string { return establishedKey(t.namespace, t.name) }

func (t scopeTable) display() string {
	if t.namespace == "" {
		return t.name
	}
	return t.namespace + "." + t.name
}

// establishedKey renders an established-record entry: the bare name on a
// single-namespace stream, `<namespace>\x00<table>` on a multi-database one
// (a NUL cannot occur in either identifier, so the split is unambiguous).
func establishedKey(namespace, table string) string {
	if namespace == "" {
		return table
	}
	return namespace + "\x00" + table
}

// bareTableOfKey is the table name of an established-record entry.
func bareTableOfKey(key string) string {
	if i := strings.LastIndexByte(key, 0); i >= 0 {
		return key[i+1:]
	}
	return key
}

// establishedKeysOf renders the entries for tables a cold start copied,
// keyed by the namespace each carries (empty on a single-namespace stream).
func establishedKeysOf(tables []*ir.Table) []string {
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		out = append(out, establishedKey(t.Schema, t.Name))
	}
	return out
}

// scopeDriftTable is one table the door refuses, with the evidence that
// convicted it.
type scopeDriftTable struct {
	name string
	// notEstablished: the established record does not hold it.
	notEstablished bool
	// unpublished: (no record) the stream's unshared publication does not
	// deliver it.
	unpublished bool
	// addIncomplete: its `sluice schema add-table` started and did not
	// complete.
	addIncomplete bool
}

// establishedEvidence is what the door learned about which in-scope tables
// the stream established.
type establishedEvidence struct {
	// recorded: the stream has an established record; established is it.
	recorded    bool
	established []string
	// unestablished are the in-scope tables the evidence convicts.
	unestablished []scopeTable
	// publication names the publication the legacy fallback read; set only
	// when that fallback supplied the evidence.
	publication string
}

// phaseRefuseIncludeTableScopeDrift is the warm-resume door. It runs on the
// two warm-resume dispatch branches of [Streamer.phaseOpenChangeStream],
// before the CDC reader opens, so a refusal has applied nothing and has not
// touched the publication.
func (s *Streamer) phaseRefuseIncludeTableScopeDrift(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	if s.scopeDriftDoorDone {
		return nil
	}
	liveAdded, err := s.uniqueKeyDoorLiveAdded(ctx, applier, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	inScope := func(table string) bool { return s.Filter.Allows(table) || liveAdded[table] }
	scope, err := s.readStreamScope(ctx, inScope)
	if err != nil {
		return err
	}

	drift := map[string]*scopeDriftTable{}
	entry := func(t scopeTable) *scopeDriftTable {
		if drift[t.key()] == nil {
			drift[t.key()] = &scopeDriftTable{name: t.display()}
		}
		return drift[t.key()]
	}

	if err := s.judgePendingAdds(ctx, applier, streamID, scope, inScope, entry); err != nil {
		return err
	}

	ev, err := s.readEstablishedEvidence(ctx, applier, streamID, scope)
	if err != nil {
		return err
	}
	var outside []string
	for _, t := range ev.unestablished {
		if !s.Filter.NamesLiterally(t.name) {
			outside = append(outside, t.display())
			continue
		}
		if ev.publication != "" {
			entry(t).unpublished = true
		} else {
			entry(t).notEstablished = true
		}
	}
	if len(outside) > 0 {
		sort.Strings(outside)
		slog.WarnContext(ctx, "INCLUDE-TABLE-NOT-ESTABLISHED: tables the stream's filter admits were never brought into "+
			"this stream and are NOT streamed with their existing rows — a table created on the source after the cold "+
			"start stays out until `sluice schema add-table` copies it (ADR-0021); name a table in --include-table to "+
			"have a warm resume refuse instead of leaving it out",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(outside, ", ")))
	}

	if len(drift) > 0 {
		tables := make([]scopeDriftTable, 0, len(drift))
		for _, d := range drift {
			tables = append(tables, *d)
		}
		sort.Slice(tables, func(i, j int) bool { return tables[i].name < tables[j].name })
		slog.ErrorContext(ctx, "warm resume refused: INCLUDE-TABLE-SCOPE-DRIFT",
			slog.String("stream_id", streamID), slog.Int("tables", len(tables)))
		return s.includeTableScopeDriftError(streamID, ev.publication, tables)
	}

	if ev.recorded {
		s.narrowEstablished(ctx, applier, streamID, ev.established, scope)
	}
	return nil
}

// readStreamScope reads the source tables the stream applies to, per
// namespace: the catalog through the same prunes the cold start applies
// ([Streamer.streamTablesOf]), plus the live-added tables inScope admits.
func (s *Streamer) readStreamScope(ctx context.Context, inScope func(string) bool) ([]scopeTable, error) {
	scopes, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		return nil, fmt.Errorf("pipeline: include-table scope door: %w", err)
	}
	var out []scopeTable
	for _, sc := range scopes {
		sr, schema, err := s.openScopedSourceSchema(ctx, sc.sourceDSN, inScope)
		if err != nil {
			return nil, fmt.Errorf("pipeline: include-table scope door: %w", err)
		}
		migcore.CloseIf(sr)
		if schema == nil {
			continue
		}
		for _, t := range s.streamTablesOf(schema, inScope) {
			out = append(out, scopeTable{namespace: sc.namespace, name: t.Name})
		}
	}
	return out, nil
}

// judgePendingAdds marks every in-scope table whose add-table is pending. A
// pending name the source no longer has (dropped or renamed since the add
// failed) cannot be streamed into and no add-table could ever clear it, so it
// is ignored with a WARN rather than refused forever.
func (s *Streamer) judgePendingAdds(ctx context.Context, applier ir.ChangeApplier, streamID string, scope []scopeTable, inScope func(string) bool, entry func(scopeTable) *scopeDriftTable) error {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return nil
	}
	pending, err := store.ReadPendingAddedTables(ctx, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	var absent []string
	for _, p := range pending {
		if !inScope(p) {
			continue
		}
		found := false
		for _, t := range scope {
			if t.name == p {
				entry(t).addIncomplete = true
				found = true
			}
		}
		if !found {
			absent = append(absent, p)
		}
	}
	if len(absent) > 0 {
		slog.WarnContext(ctx, "a pending `sluice schema add-table` names a table the source no longer has; ignoring it "+
			"(the next cold start clears the record)",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(absent, ", ")))
	}
	return nil
}

// readEstablishedEvidence returns the in-scope tables the stream did not
// establish, from its established record or, without one, from its
// publication when that is provably this stream's alone.
func (s *Streamer) readEstablishedEvidence(ctx context.Context, applier ir.ChangeApplier, streamID string, scope []scopeTable) (establishedEvidence, error) {
	var ev establishedEvidence
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		est, recorded, err := store.ReadEstablishedTables(ctx, streamID)
		if err != nil {
			return ev, controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
		}
		if recorded {
			ev.recorded, ev.established = true, est
			have := make(map[string]bool, len(est))
			for _, k := range est {
				have[k] = true
			}
			for _, t := range scope {
				if !have[t.key()] {
					ev.unestablished = append(ev.unestablished, t)
				}
			}
			return ev, nil
		}
	}

	// No record: a stream cold-started before it existed. Seeding one now
	// from the current scope would bless exactly the never-copied table this
	// door is for, so the record waits for the next cold start.
	if checker, ok := s.Source.(publicationMembershipChecker); ok && !s.multiDatabaseMode() {
		names := make([]string, 0, len(scope))
		for _, t := range scope {
			names = append(names, t.name)
		}
		missing, publication, shared, err := checker.UnpublishedTables(ctx, s.SourceDSN, names)
		if err != nil {
			return ev, connectHint(fmt.Errorf("pipeline: include-table scope door: %w", err))
		}
		if publication != "" && !shared {
			miss := make(map[string]bool, len(missing))
			for _, m := range missing {
				miss[m] = true
			}
			for _, t := range scope {
				if miss[t.name] {
					ev.unestablished = append(ev.unestablished, t)
				}
			}
			ev.publication = publication
			return ev, nil
		}
	}
	slog.WarnContext(ctx, "INCLUDE-TABLE-SCOPE-UNRECORDED: this stream has no record of the tables it established "+
		"(it was cold-started by an older sluice, or the record could not be written), so a table added to its scope "+
		"without `sluice schema add-table` cannot be told from one it copied; the next cold start "+
		"(--restart-from-scratch) records it",
		slog.String("stream_id", streamID))
	return ev, nil
}

// narrowEstablished drops from the established record every table this
// resume no longer applies — out of scope now, or gone from the source —
// because its changes are discarded from here on and coming back to it needs
// a fresh copy. A failed write WARNs: it is the re-widen check that weakens,
// not this stream.
func (s *Streamer) narrowEstablished(ctx context.Context, applier ir.ChangeApplier, streamID string, established []string, scope []scopeTable) {
	keep := make(map[string]bool, len(scope))
	for _, t := range scope {
		keep[t.key()] = true
	}
	next := make([]string, 0, len(established))
	var dropped []string
	for _, k := range established {
		if keep[k] {
			next = append(next, k)
		} else {
			dropped = append(dropped, strings.ReplaceAll(k, "\x00", "."))
		}
	}
	if len(dropped) == 0 {
		return
	}
	store := applier.(ir.EstablishedTablesStore)
	if err := store.RecordEstablishedTables(ctx, streamID, next); err != nil {
		slog.WarnContext(ctx, "could not narrow the stream's established-table record; bringing a dropped table back "+
			"into scope later would not be refused", slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return
	}
	slog.InfoContext(ctx, "tables left this stream's scope: their changes are no longer applied, and bringing one back "+
		"needs `sluice schema add-table`", slog.String("stream_id", streamID), slog.String("tables", strings.Join(dropped, ", ")))
}

// includeTableScopeDriftError renders the refusal. The remedies are the ones
// TestIncludeTableScopeDrift_* measured through the real CLI.
func (s *Streamer) includeTableScopeDriftError(streamID, publication string, tables []scopeDriftTable) error {
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		var why []string
		if t.notEstablished {
			why = append(why, "named by --include-table and never copied by this stream (not in its established-table record)")
		}
		if t.unpublished {
			why = append(why, fmt.Sprintf("named by --include-table and not in publication %q, so the source never sends its changes", publication))
		}
		if t.addIncomplete {
			why = append(why, "its `sluice schema add-table` started and did not complete, so its copy may be partial "+
				"(if that add-table is still running, wait for it to finish and start again)")
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

// recordColdStartEstablished records the set a cold-start lane copied as the
// stream's established tables, and clears the add-table pending record: with
// the established record written, a pending table the copy did not include
// is simply not established (and a pending name the source no longer has is
// gone with it — L2), so the pending record has nothing left to say. When
// the record could not be written only the copied tables' pending entries
// are cleared. Failures WARN: the copy succeeded, and what a missing record
// costs is the evidence of the next warm resume, which says so itself.
func (s *Streamer) recordColdStartEstablished(ctx context.Context, applier ir.ChangeApplier, streamID string) {
	keys := s.coldStartEstablished
	s.coldStartEstablished = nil
	if keys == nil {
		return
	}
	recorded := false
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		if err := store.RecordEstablishedTables(ctx, streamID, keys); err != nil {
			slog.WarnContext(ctx, "INCLUDE-TABLE-SCOPE-UNRECORDED: cold start could not record the tables it copied; "+
				"a later warm resume cannot tell a table added to the scope from one this copy brought in "+
				"(on a control table this role cannot ALTER, add the established_tables column, or re-run with a role that can)",
				slog.String("stream_id", streamID), slog.String("error", err.Error()))
		} else {
			recorded = true
		}
	}
	pstore, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return
	}
	pending, err := pstore.ReadPendingAddedTables(ctx, streamID)
	if err != nil || len(pending) == 0 {
		return
	}
	toClear := pending
	if !recorded {
		copied := make(map[string]bool, len(keys))
		for _, k := range keys {
			copied[bareTableOfKey(k)] = true
		}
		toClear = toClear[:0:0]
		for _, p := range pending {
			if copied[p] {
				toClear = append(toClear, p)
			}
		}
	}
	if err := pstore.ClearPendingAddedTables(ctx, streamID, toClear); err != nil {
		slog.WarnContext(ctx, "cold start: could not clear the add-table pending record; the next warm resume will "+
			"refuse INCLUDE-TABLE-SCOPE-DRIFT naming any of those tables still in scope",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
	}
}
