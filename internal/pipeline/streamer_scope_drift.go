// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The warm-resume table-scope door (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321).
//
// # The harm
//
// A warm resume copies nothing. It continues the change stream from the
// persisted position, so a table that joins the stream's scope on a restart
// is never copied. On a Postgres pgoutput source a table outside the
// publication is not even streamed; a table inside it — or any table on a
// binlog, VStream or trigger source whose target counterpart exists — is
// streamed from the persisted position onto a target copy that is missing
// everything before it. The v0.162.0 regression cycle and two reviews
// measured every one of those shapes at exit 0.
//
// # The rule, stated once
//
// A warm resume refuses an in-scope table whose changes it would APPLY onto a
// target copy this stream did not make or did not keep up. Evidence, none of
// it derived from the current flags:
//
//   - the ESTABLISHED record ([ir.EstablishedRecord], every source): what
//     each cold start actually copied plus every completed add-table, written
//     by the copy path. Per stream, so another stream's publication member
//     cannot pass as ours (S1).
//   - the DROPPED set beside it: tables that left the scope after they were
//     established. Their changes were discarded from then on — on pgoutput
//     the table stays in the publication and the dispatch filter drops them —
//     so a scope that re-admits one, by ANY means, refuses (N1).
//   - the PENDING add-table record ([ir.PendingAddedTablesStore]): an add
//     that started and did not complete; with a fresh heartbeat it is still
//     running (restartable, [ir.ErrIncludeTableAddInProgress]), with a stale
//     one it died.
//   - pg_publication_tables (pgoutput, single namespace): an ESTABLISHED
//     table missing from a FOR TABLE publication is no longer sent at all — a
//     table dropped and recreated while the stream was stopped leaves the
//     publication (N2). Missing membership is a true positive whether or not
//     the publication is shared.
//   - for a stream with no record (cold-started before it existed, or a
//     cold start whose record write failed): its publication, for literal
//     includes, only when no other sluice slot exists. Otherwise no evidence,
//     and the door says so (INCLUDE-TABLE-SCOPE-UNRECORDED).
//
// # When "not established" is a WARN and when it is a refusal
//
// A table never established refuses outright when an --include-table entry
// names it literally: the operator asked for it. Admitted by a glob or an
// unfiltered stream, it keeps ADR-0021's "a table created on the source
// stays out until add-table" — but only where that is TRUE, i.e. where its
// changes are provably not applied:
//
//   - pgoutput, the table is not in the publication: the server never sends
//     a change for it. WARN (INCLUDE-TABLE-NOT-ESTABLISHED).
//   - any other source, the target has no such table: the applier skips its
//     changes loudly and counts them (sluice_cdc_skipped_tables, `sync
//     health`). WARN.
//
// Everything else applies changes onto a copy that was never made — the
// publication delivers the table (a shared publication, FOR ALL TABLES, a
// multi-schema stream's spanning publication, or a missing publication the
// reader would recreate FOR ALL TABLES), or a binlog/VStream/trigger source
// with the target table present — and refuses, whatever admitted it.
//
// # Once per process
//
// The door runs at the first stream open of a Run only (scopeDriftDoorDone):
// the scope cannot change within a process, and the in-process retry ladder
// must not trip over an add-table a live stream is legitimately waiting on.
//
// # Narrowing is not refused
//
// A table that LEAVES the scope stops being applied: the operator's choice,
// and the rows already on the target stay as they were. The record moves it
// to the dropped set, so coming back to it refuses for every scope shape
// (INCLUDE-TABLE-RE-WIDEN: literal, glob and unfiltered, every source).

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// publicationMembershipChecker is the optional source-engine surface the door
// reads publication membership through. Postgres implements it on its
// publication-scoped engine value; engines without publications omit it.
type publicationMembershipChecker interface {
	UnpublishedTables(ctx context.Context, dsn string, tables []string) (ir.PublicationMembership, error)
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

// scopeDriftTable is one table the door convicts, with every reason.
type scopeDriftTable struct {
	name string
	// dropped: it left the scope after it was established; its changes since
	// were discarded.
	dropped bool
	// notEstablished: never copied, and named literally by --include-table.
	notEstablished bool
	// appliedWhy, non-empty: never copied, admitted by a glob or an
	// unfiltered stream, and its changes would be applied (the reason why).
	appliedWhy string
	// unpublished: established, but missing from the FOR TABLE publication
	// (N2); legacyUnpublished: no record, literal, missing from an unshared
	// publication.
	unpublished, legacyUnpublished bool
	// addInProgress / addIncomplete: a pending add-table with a fresh / stale
	// heartbeat; heartbeatAge is its age.
	addInProgress, addIncomplete bool
	heartbeatAge                 time.Duration
}

// onlyInProgress reports a table convicted by nothing but a live add-table.
func (d scopeDriftTable) onlyInProgress() bool {
	return d.addInProgress && !d.addIncomplete && !d.dropped && !d.notEstablished &&
		d.appliedWhy == "" && !d.unpublished && !d.legacyUnpublished
}

// streamScope is the current scope with the namespace pairs it was read
// through.
type streamScope struct {
	tables []scopeTable
	pairs  map[string]uniqueKeyDoorScope // by namespace
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

	if err := s.judgePendingAdds(ctx, applier, streamID, scope.tables, inScope, entry); err != nil {
		return err
	}
	membership, err := s.readPublicationMembership(ctx, scope.tables)
	if err != nil {
		return err
	}
	rec, err := readEstablishedRecord(ctx, applier, streamID)
	if err != nil {
		return err
	}
	if rec.Recorded {
		if err := s.judgeAgainstRecord(ctx, streamID, scope, rec, membership, entry); err != nil {
			return err
		}
	} else {
		s.judgeLegacy(ctx, streamID, scope.tables, membership, entry)
	}

	if len(drift) > 0 {
		return s.scopeDriftRefusal(ctx, streamID, membership, drift)
	}
	if rec.Recorded {
		s.narrowEstablished(ctx, applier, streamID, rec, scope)
	}
	return nil
}

// readStreamScope reads the source tables the stream applies to, per
// namespace: the catalog through the same prunes the cold start applies
// ([Streamer.streamTablesOf]), plus the live-added tables inScope admits.
func (s *Streamer) readStreamScope(ctx context.Context, inScope func(string) bool) (streamScope, error) {
	pairs, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		return streamScope{}, fmt.Errorf("pipeline: include-table scope door: %w", err)
	}
	out := streamScope{pairs: map[string]uniqueKeyDoorScope{}}
	for _, sc := range pairs {
		out.pairs[sc.namespace] = sc
		sr, schema, err := s.openScopedSourceSchema(ctx, sc.sourceDSN, inScope)
		if err != nil {
			return streamScope{}, fmt.Errorf("pipeline: include-table scope door: %w", err)
		}
		migcore.CloseIf(sr)
		if schema == nil {
			continue
		}
		for _, t := range s.streamTablesOf(schema, inScope) {
			out.tables = append(out.tables, scopeTable{namespace: sc.namespace, name: t.Name})
		}
	}
	return out, nil
}

// readPublicationMembership reads the publication's membership for the
// in-scope tables: pgoutput, single-namespace streams only (nil otherwise).
func (s *Streamer) readPublicationMembership(ctx context.Context, scope []scopeTable) (*ir.PublicationMembership, error) {
	checker, ok := s.Source.(publicationMembershipChecker)
	if !ok || s.multiDatabaseMode() {
		return nil, nil
	}
	names := make([]string, 0, len(scope))
	for _, t := range scope {
		names = append(names, t.name)
	}
	m, err := checker.UnpublishedTables(ctx, s.SourceDSN, names)
	if err != nil {
		return nil, connectHint(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	return &m, nil
}

func readEstablishedRecord(ctx context.Context, applier ir.ChangeApplier, streamID string) (ir.EstablishedRecord, error) {
	store, ok := applier.(ir.EstablishedTablesStore)
	if !ok {
		return ir.EstablishedRecord{}, nil
	}
	rec, err := store.ReadEstablishedRecord(ctx, streamID)
	if err != nil {
		return ir.EstablishedRecord{}, controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	return rec, nil
}

// judgePendingAdds marks every in-scope table whose add-table is pending:
// in progress while its heartbeat is fresh, incomplete once stale. A pending
// name the source no longer has (dropped or renamed since) cannot be streamed
// into, so it is ignored with a WARN naming the abandon path.
func (s *Streamer) judgePendingAdds(ctx context.Context, applier ir.ChangeApplier, streamID string, scope []scopeTable, inScope func(string) bool, entry func(scopeTable) *scopeDriftTable) error {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return nil
	}
	pending, err := store.ReadPendingAddedTables(ctx, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	now := time.Now()
	var absent []string
	for _, p := range pending {
		if !inScope(p.Table) {
			continue
		}
		found := false
		for _, t := range scope {
			if t.name != p.Table {
				continue
			}
			found = true
			e := entry(t)
			e.heartbeatAge = now.Sub(p.HeartbeatAt).Round(time.Second)
			if p.Fresh(now) {
				e.addInProgress = true
			} else {
				e.addIncomplete = true
			}
		}
		if !found {
			absent = append(absent, p.Table)
		}
	}
	if len(absent) > 0 {
		slog.WarnContext(ctx, "a pending `sluice schema add-table` names a table the source no longer has; ignoring it "+
			"(`sluice schema add-table --abandon <table>` removes the record)",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(absent, ", ")))
	}
	return nil
}

// judgeAgainstRecord applies the established/dropped record — and, on
// pgoutput, the publication — to every in-scope table.
func (s *Streamer) judgeAgainstRecord(ctx context.Context, streamID string, scope streamScope, rec ir.EstablishedRecord, m *ir.PublicationMembership, entry func(scopeTable) *scopeDriftTable) error {
	established, dropped := toSet(rec.Established), toSet(rec.Dropped)
	missing := map[string]bool{}
	if m != nil {
		missing = toSet(m.Missing)
	}
	var candidates []scopeTable // never established, admitted by a glob or no filter
	for _, t := range scope.tables {
		switch {
		case dropped[t.key()]:
			entry(t).dropped = true
		case !established[t.key()]:
			if s.Filter.NamesLiterally(t.name) {
				entry(t).notEstablished = true
			} else {
				candidates = append(candidates, t)
			}
		case m != nil && m.Publication != "" && !m.AllTables && missing[t.name]:
			// N2: established, and the FOR TABLE publication no longer lists
			// it. Shared or not, a missing member is never sent.
			entry(t).unpublished = true
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	applied, err := s.changesWouldApply(ctx, scope, candidates, m)
	if err != nil {
		return err
	}
	var outside []string
	for _, t := range candidates {
		if why := applied[t.key()]; why != "" {
			entry(t).appliedWhy = why
		} else {
			outside = append(outside, t.display())
		}
	}
	if len(outside) > 0 {
		sort.Strings(outside)
		slog.WarnContext(ctx, "INCLUDE-TABLE-NOT-ESTABLISHED: tables the stream's filter admits were never brought into "+
			"this stream, and their changes are NOT applied (not in the publication, or no target table — skipped and "+
			"counted); they stay out until `sluice schema add-table` copies them (ADR-0021)",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(outside, ", ")))
	}
	return nil
}

// changesWouldApply returns, for each never-established candidate, why its
// changes would be applied by this stream ("" when they provably are not —
// the only case the door lets through with a WARN). See the file comment.
func (s *Streamer) changesWouldApply(ctx context.Context, scope streamScope, candidates []scopeTable, m *ir.PublicationMembership) (map[string]string, error) {
	out := map[string]string{}
	if _, publicationSource := s.Source.(publicationMembershipChecker); publicationSource {
		for _, t := range candidates {
			switch {
			case m == nil:
				// A multi-schema stream reads through a spanning FOR ALL
				// TABLES publication.
				out[t.key()] = "the stream's spanning publication is FOR ALL TABLES, so its changes are applied"
			case m.Publication == "":
				out[t.key()] = "the stream's publication does not exist, and the reader recreates it FOR ALL TABLES, so its changes would be applied"
			case !toSet(m.Missing)[t.name]:
				out[t.key()] = fmt.Sprintf("publication %q delivers it, so its changes are applied", m.Publication)
			}
		}
		return out, nil
	}
	// A source whose change stream carries every table: its changes reach the
	// applier, which applies them wherever the target table exists.
	byNamespace := map[string]map[string]bool{}
	for _, t := range candidates {
		if byNamespace[t.namespace] == nil {
			byNamespace[t.namespace] = map[string]bool{}
		}
		byNamespace[t.namespace][strings.ToLower(t.name)] = true
	}
	for ns, names := range byNamespace {
		tgt, err := s.readUniqueKeyDoorTarget(ctx, scope.pairs[ns], names)
		// A namespace with no target database yet holds no target tables.
		if err != nil && (ns == "" || !ir.IsDatabaseNotFound(err)) {
			return nil, fmt.Errorf("pipeline: include-table scope door: read target tables: %w", err)
		}
		for _, t := range candidates {
			if t.namespace == ns && matchTargetTable(tgt, t.name) != nil {
				out[t.key()] = "its target table exists, so its changes from the stream's position are applied onto it"
			}
		}
	}
	return out, nil
}

// judgeLegacy is the no-record fallback: the publication, for literal
// includes, only when no other sluice slot exists on the source.
func (s *Streamer) judgeLegacy(ctx context.Context, streamID string, scope []scopeTable, m *ir.PublicationMembership, entry func(scopeTable) *scopeDriftTable) {
	// Seeding a record now from the scope that passed would record a
	// never-copied table as copied — the case this door exists for — so the
	// record waits for the next cold start.
	if m != nil && m.Publication != "" && !m.Shared {
		missing := toSet(m.Missing)
		for _, t := range scope {
			if missing[t.name] && s.Filter.NamesLiterally(t.name) {
				entry(t).legacyUnpublished = true
			}
		}
		return
	}
	slog.WarnContext(ctx, "INCLUDE-TABLE-SCOPE-UNRECORDED: this stream has no record of the tables it established "+
		"(it was cold-started by an older sluice, or a cold start could not write the record), so a table added to its "+
		"scope without `sluice schema add-table` cannot be told from one it copied; the next cold start "+
		"(--restart-from-scratch) records it",
		slog.String("stream_id", streamID))
}

// scopeDriftRefusal renders the door's verdict: the restartable
// INCLUDE-TABLE-ADD-IN-PROGRESS when every conviction is a live add-table,
// the terminal INCLUDE-TABLE-SCOPE-DRIFT otherwise.
func (s *Streamer) scopeDriftRefusal(ctx context.Context, streamID string, m *ir.PublicationMembership, drift map[string]*scopeDriftTable) error {
	tables := make([]scopeDriftTable, 0, len(drift))
	inProgress := true
	for _, d := range drift {
		tables = append(tables, *d)
		inProgress = inProgress && d.onlyInProgress()
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].name < tables[j].name })
	if inProgress {
		names := make([]string, 0, len(tables))
		for _, t := range tables {
			names = append(names, fmt.Sprintf("%s (heartbeat %s ago)", t.name, t.heartbeatAge))
		}
		slog.WarnContext(ctx, "warm resume deferred: INCLUDE-TABLE-ADD-IN-PROGRESS", slog.String("stream_id", streamID))
		return fmt.Errorf("pipeline: warm resume deferred: %w: stream %q has a `sluice schema add-table` still running "+
			"for %s; it clears its record when the copy is handed to the stream. Start again once it finishes "+
			"(a supervisor restarts this automatically). If the add-table process is gone, the entry goes stale after "+
			"%s and this becomes INCLUDE-TABLE-SCOPE-DRIFT",
			ir.ErrIncludeTableAddInProgress, streamID, strings.Join(names, ", "), ir.PendingAddStaleAfter)
	}
	publication := ""
	if m != nil {
		publication = m.Publication
	}
	slog.ErrorContext(ctx, "warm resume refused: INCLUDE-TABLE-SCOPE-DRIFT",
		slog.String("stream_id", streamID), slog.Int("tables", len(tables)))
	return s.includeTableScopeDriftError(streamID, publication, tables)
}

// narrowEstablished moves every established table this resume no longer
// applies into the dropped set — its changes are discarded from here on —
// and forgets the ones gone from the source (an established one, or a
// dropped one: a table recreated under the name later is a new table, which
// the established record already does not hold). Dropped is written first,
// so a stop between the two writes leaves a table in both sets, which refuses,
// rather than in neither. A failed write WARNs: it is the re-widen check that
// weakens, not this stream.
func (s *Streamer) narrowEstablished(ctx context.Context, applier ir.ChangeApplier, streamID string, rec ir.EstablishedRecord, scope streamScope) {
	inScope := map[string]bool{}
	for _, t := range scope.tables {
		inScope[t.key()] = true
	}
	var leaving, keepEst []string
	for _, k := range rec.Established {
		if inScope[k] {
			keepEst = append(keepEst, k)
		} else {
			leaving = append(leaving, k)
		}
	}
	if len(leaving) == 0 && len(rec.Dropped) == 0 {
		return
	}
	present, err := s.presentInSource(ctx, scope, append(append([]string(nil), leaving...), rec.Dropped...))
	if err != nil {
		slog.WarnContext(ctx, "could not read the source catalog to narrow the established-table record; it is left as it "+
			"was", slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return
	}
	var dropped []string
	for _, k := range append(append([]string(nil), rec.Dropped...), leaving...) {
		if present[k] {
			dropped = append(dropped, k)
		}
	}
	store := applier.(ir.EstablishedTablesStore)
	if !sameSet(dropped, rec.Dropped) {
		if err := store.RecordDroppedTables(ctx, streamID, dropped); err != nil {
			slog.WarnContext(ctx, "could not update the stream's dropped-table record; bringing a table back into scope "+
				"later may not be refused", slog.String("stream_id", streamID), slog.String("error", err.Error()))
			return
		}
	}
	if len(leaving) > 0 {
		if err := store.RecordEstablishedTables(ctx, streamID, keepEst); err != nil {
			slog.WarnContext(ctx, "could not narrow the stream's established-table record", slog.String("stream_id", streamID),
				slog.String("error", err.Error()))
			return
		}
		shown := make([]string, len(leaving))
		for i, k := range leaving {
			shown[i] = strings.ReplaceAll(k, "\x00", ".")
		}
		slog.InfoContext(ctx, "tables left this stream's scope: their changes are no longer applied, and bringing one back "+
			"refuses until it is copied again", slog.String("stream_id", streamID), slog.String("tables", strings.Join(shown, ", ")))
	}
}

// presentInSource reports which of keys name a table the source catalog
// still has, reading only those tables.
func (s *Streamer) presentInSource(ctx context.Context, scope streamScope, keys []string) (map[string]bool, error) {
	want := map[string]map[string]bool{}
	for _, k := range keys {
		ns := ""
		if i := strings.LastIndexByte(k, 0); i >= 0 {
			ns = k[:i]
		}
		if want[ns] == nil {
			want[ns] = map[string]bool{}
		}
		want[ns][bareTableOfKey(k)] = true
	}
	present := map[string]bool{}
	for ns, names := range want {
		sc, ok := scope.pairs[ns]
		if !ok {
			continue // a namespace no longer selected: nothing of it is present to this stream
		}
		pred := func(name string) bool { return names[name] }
		sr, schema, err := s.openScopedSourceSchema(ctx, sc.sourceDSN, pred)
		if err != nil {
			return nil, err
		}
		migcore.CloseIf(sr)
		if schema == nil {
			continue
		}
		for _, t := range schema.Tables {
			if t != nil && names[t.Name] {
				present[establishedKey(ns, t.Name)] = true
			}
		}
	}
	return present, nil
}

// includeTableScopeDriftError renders the refusal. The remedies are the ones
// TestIncludeTableScopeDrift_* measured through the real CLI.
func (s *Streamer) includeTableScopeDriftError(streamID, publication string, tables []scopeDriftTable) error {
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		var why []string
		if t.dropped {
			why = append(why, "it left this stream's scope earlier and its changes since then were discarded, so its target copy is stale")
		}
		if t.notEstablished {
			why = append(why, "named by --include-table and never copied by this stream (not in its established-table record)")
		}
		if t.appliedWhy != "" {
			why = append(why, "never copied by this stream, and "+t.appliedWhy)
		}
		if t.unpublished {
			why = append(why, fmt.Sprintf("no longer in publication %q (dropped and recreated on the source, or removed from it), "+
				"so the source has stopped sending its changes", publication))
		}
		if t.legacyUnpublished {
			why = append(why, fmt.Sprintf("named by --include-table and not in publication %q, so the source never sends its changes", publication))
		}
		if t.addIncomplete {
			why = append(why, fmt.Sprintf("its `sluice schema add-table` started and did not complete (last heartbeat %s ago), "+
				"so its copy may be partial", t.heartbeatAge))
		}
		if t.addInProgress {
			why = append(why, fmt.Sprintf("its `sluice schema add-table` is still running (heartbeat %s ago)", t.heartbeatAge))
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", t.name, strings.Join(why, "; ")))
	}
	remedy := "Remedy, per table, with the stream stopped: empty the target table, run `sluice schema add-table <table> " +
		"--source-driver " + s.Source.Name() + " --source <SOURCE-DSN> --target-driver " + s.Target.Name() +
		" --target <TARGET-DSN> --stream-id " + streamID + " --yes`, then re-run this command unchanged. Or take the " +
		"table out of scope (--include-table / --exclude-table). Or re-copy every table with --restart-from-scratch " +
		"(on a Postgres source, first drop the stream's replication slot with `sluice slot drop`, as that run's refusal instructs)"
	if s.multiDatabaseMode() {
		// add-table names a bare table and cannot extend a multi-database
		// stream's namespace-qualified record (F3), so it would loop here.
		remedy = "Remedy: take the table out of scope, or re-copy the stream with --restart-from-scratch " +
			"(`schema add-table` does not record a table on a multi-database stream)"
	}
	return fmt.Errorf("pipeline: warm resume refused: %w: stream %q would apply changes for %d table(s) onto target "+
		"copies it did not make or did not keep up: %s. A warm resume copies nothing, so each would stay wrong on the "+
		"target while the stream reported healthy; nothing has been applied by this run. %s",
		ir.ErrIncludeTableScopeDrift, streamID, len(tables), strings.Join(parts, ", "), remedy)
}

// resetStreamRecordsForColdStart is a cold start's first step on every lane:
// the apply marks go (ADR-0190) and the established record is reset.
func (s *Streamer) resetStreamRecordsForColdStart(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	if err := clearApplyMarksForColdStart(ctx, applier, streamID); err != nil {
		return err
	}
	return s.resetEstablishedForColdStart(ctx, applier, streamID)
}

// resetEstablishedForColdStart sets the stream's established record to "not
// recorded" before a cold start copies anything (N3), remembering the dropped
// set so tables the copy does not cover stay dropped. Any failure from here
// to [Streamer.recordColdStartEstablished] then leaves the stream unrecorded,
// never holding a record of an older copy: a record that names a table the
// new anchor skipped over would pass it with every change between the old
// position and the new anchor missing. A reset that fails stops the cold
// start (restartable): proceeding would be exactly that stale record.
func (s *Streamer) resetEstablishedForColdStart(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	s.coldStartPriorDropped = nil
	store, ok := applier.(ir.EstablishedTablesStore)
	if !ok {
		return nil
	}
	rec, err := store.ReadEstablishedRecord(ctx, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: cold start: read established-table record: %w", err))
	}
	s.coldStartPriorDropped = rec.Dropped
	if err := store.ResetEstablishedRecord(ctx, streamID); err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: cold start: reset established-table record: %w", err))
	}
	return nil
}

// recordColdStartEstablished records the set a cold-start lane copied as the
// stream's established tables, keeps every previously dropped table the copy
// did not cover dropped, and clears the add-table pending entries of the
// tables it copied — only those (N1): a pending table the copy left out is
// still a partial copy if a later scope re-admits it. Failures WARN: the
// copy succeeded, and the record stays unrecorded, which the next warm
// resume says itself.
func (s *Streamer) recordColdStartEstablished(ctx context.Context, applier ir.ChangeApplier, streamID string) {
	keys, priorDropped := s.coldStartEstablished, s.coldStartPriorDropped
	s.coldStartEstablished, s.coldStartPriorDropped = nil, nil
	if keys == nil {
		return
	}
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		copied := toSet(keys)
		var dropped []string
		for _, k := range priorDropped {
			if !copied[k] {
				dropped = append(dropped, k)
			}
		}
		if err := store.RecordEstablishedTables(ctx, streamID, keys); err != nil {
			slog.WarnContext(ctx, "INCLUDE-TABLE-SCOPE-UNRECORDED: cold start could not record the tables it copied; "+
				"a later warm resume cannot tell a table added to the scope from one this copy brought in "+
				"(on a control table this role cannot ALTER, add the established_tables and dropped_tables columns)",
				slog.String("stream_id", streamID), slog.String("error", err.Error()))
		} else if err := store.RecordDroppedTables(ctx, streamID, dropped); err != nil {
			slog.WarnContext(ctx, "cold start could not record the stream's dropped tables; re-admitting one later may "+
				"not be refused", slog.String("stream_id", streamID), slog.String("error", err.Error()))
		}
	}
	pstore, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return
	}
	copied := make([]string, 0, len(keys))
	for _, k := range keys {
		copied = append(copied, bareTableOfKey(k))
	}
	if err := pstore.ClearPendingAddedTables(ctx, streamID, copied); err != nil {
		slog.WarnContext(ctx, "cold start: could not clear the add-table pending record of the tables it copied; the "+
			"next warm resume will refuse INCLUDE-TABLE-SCOPE-DRIFT naming them",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
	}
}

func toSet(xs []string) map[string]bool {
	out := make(map[string]bool, len(xs))
	for _, x := range xs {
		out[x] = true
	}
	return out
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	bs := toSet(b)
	for _, x := range a {
		if !bs[x] {
			return false
		}
	}
	return true
}
