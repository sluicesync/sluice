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
//   - the COLD-START-IN-PROGRESS mark ([ir.EstablishedRecord.ColdStartInProgress]):
//     a cold start passed its point of no return and did not finish. It
//     refuses every warm resume (COLD-START-INCOMPLETE) before anything else
//     is read. A recorded stream never returns to "not recorded".
//   - the PENDING add-table record ([ir.PendingAddedTablesStore]): an add
//     that started and did not complete; with a fresh heartbeat (by the
//     control-table server's clock) it is still running, with a stale one it
//     died. A running add overrides the convictions of its own table that its
//     completion cures (F-A); when every refused table is such a table the
//     verdict is restartable ([ir.ErrIncludeTableAddInProgress]).
//   - pg_publication_tables (pgoutput, single namespace): an ESTABLISHED
//     table missing from a FOR TABLE publication is no longer sent at all — a
//     table dropped and recreated while the stream was stopped leaves the
//     publication (N2). Missing membership is a true positive whether or not
//     the publication is shared.
//   - for a stream with no record (cold-started by an older sluice, or only
//     ever cold-started on a control table this role cannot ALTER): its
//     publication, for literal includes, only when no other sluice slot
//     exists on the same database. Otherwise no evidence, and the door says
//     so (INCLUDE-TABLE-SCOPE-UNRECORDED).
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
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
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
	// droppedUnknown: never established, on a stream whose dropped set was
	// unreadable at its last cold start ([ir.EstablishedRecord.DroppedAllUncopied]).
	droppedUnknown bool
	// notEstablished: never copied, and named literally by --include-table.
	notEstablished bool
	// appliedWhy, non-empty: never copied, admitted by a glob or an
	// unfiltered stream, and its changes would be applied (the reason why).
	appliedWhy string
	// unpublished: established, but missing from the FOR TABLE publication
	// (N2); legacyUnpublished: no record, literal, missing from an unshared
	// publication.
	unpublished, legacyUnpublished bool
	// addAbandoned: its add-table was retired by `--abandon` and its changes
	// would still be applied onto the possibly partial copy (R4-1).
	addAbandoned bool
	// renamedFrom: the table is an established one renamed on the source
	// (proved by id or acknowledged), but the target holds no table under the
	// new name, or still holds the old one — the target was not renamed.
	// renameTargetState says which.
	renamedFrom, renameTargetState string
	// identityChanged, non-empty: established, but the source table under
	// its name is not the one the stream recorded (TABLE-IDENTITY-CHANGED,
	// R6-S3) — why, naming a swap where one is proved.
	identityChanged string
	// renameCandidates, set on a never-established conviction: established
	// tables gone from the source, any of which this table may be renamed
	// from without an id that proves it (the refusal names
	// --accept-table-rename).
	renameCandidates []string
	// addMalformed: the raw text of the table's pending record, which does
	// not decode (R6 L1); judged like a dead add.
	addMalformed string
	// addInProgress / addIncomplete: a pending add-table with a fresh / stale
	// heartbeat; heartbeatAge is its age.
	addInProgress, addIncomplete bool
	heartbeatAge                 time.Duration
}

// yieldToLiveAdd lets a running add-table (F-A) override the convictions it
// is about to cure: a table being brought in is, until it completes, never
// established — literally named, would-apply, previously dropped, missing
// from a no-record stream's publication — and every one of those is what the
// add's completion fixes, so a restart after it is the remedy, not an
// operator decision. N2 (established and gone from the publication) is not
// something an add cures, and still convicts.
func (d *scopeDriftTable) yieldToLiveAdd() {
	if d.addInProgress {
		d.dropped, d.droppedUnknown, d.notEstablished, d.appliedWhy, d.legacyUnpublished = false, false, false, "", false
	}
}

// onlyInProgress reports a table convicted by nothing but a live add-table
// (after [scopeDriftTable.yieldToLiveAdd]).
func (d scopeDriftTable) onlyInProgress() bool {
	return d.addInProgress && !d.addIncomplete && !d.addAbandoned && !d.dropped && !d.droppedUnknown && !d.notEstablished && d.renamedFrom == "" &&
		d.appliedWhy == "" && !d.unpublished && !d.legacyUnpublished && d.identityChanged == ""
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
	rec, err := readEstablishedRecord(ctx, applier, streamID)
	if err != nil {
		return err
	}
	if rec.ColdStartInProgress {
		slog.ErrorContext(ctx, "warm resume refused: INCLUDE-TABLE-SCOPE-DRIFT (COLD-START-INCOMPLETE)", slog.String("stream_id", streamID))
		return coldStartIncompleteError(streamID)
	}
	inScope := func(table string) bool {
		return scopeAdmits(table, s.Filter, func(t string) bool { return liveAdded[t] })
	}
	scope, err := s.readStreamScope(ctx, inScope)
	if err != nil {
		return err
	}
	present := map[string]bool{}
	for _, t := range scope.tables {
		present[t.name] = true
	}
	warnLiveAddedNotInCatalog(ctx, streamID, liveAdded, present)

	drift := map[string]*scopeDriftTable{}
	entry := func(t scopeTable) *scopeDriftTable {
		if drift[t.key()] == nil {
			drift[t.key()] = &scopeDriftTable{name: t.display()}
		}
		return drift[t.key()]
	}

	abandoned, clearable, err := s.judgePendingAdds(ctx, applier, streamID, scope.tables, inScope, entry)
	if err != nil {
		return err
	}
	membership, err := s.readPublicationMembership(ctx, scope.tables)
	if err != nil {
		return err
	}
	var notApplied map[string]bool
	var fix ir.RecordNarrowing // renames and rebuilt ids the record follows
	if rec.Recorded {
		if notApplied, fix, err = s.judgeAgainstRecord(ctx, streamID, scope, rec, membership, entry); err != nil {
			return err
		}
	} else {
		s.judgeLegacy(ctx, streamID, scope.tables, membership, entry)
		notApplied = legacyNotDelivered(scope.tables, membership)
	}
	// R4-1: an abandoned add's table may hold a partial copy. It convicts
	// unless this stream provably applies nothing onto it; then its entry has
	// done its job and is cleared, like one out of scope.
	for _, t := range abandoned {
		if notApplied[t.key()] {
			clearable = append(clearable, t.name)
		} else {
			entry(t).addAbandoned = true
		}
	}

	if len(drift) > 0 {
		return s.scopeDriftRefusal(ctx, streamID, membership, drift)
	}
	if rec.Recorded {
		if err := s.narrowEstablished(ctx, applier, streamID, rec, scope, fix); err != nil {
			return err
		}
	}
	clearAbandonedAdds(ctx, applier, streamID, clearable)
	return nil
}

// warnLiveAddedNotInCatalog WARNs about live-added entries the source catalog
// does not hold (Bug 321 R9 S-2). The ordinary cause is a table dropped on
// the source. The one that matters: a MySQL target's comma-list set holding
// a name an older release recorded with a comma in it, which now reads back
// as several names — the real table is admitted by none of them, and one of
// the pieces may admit an unrelated table. sluice cannot tell the two apart,
// so it names the entries and the remedy.
func warnLiveAddedNotInCatalog(ctx context.Context, streamID string, liveAdded, present map[string]bool) {
	var missing []string
	for t := range liveAdded {
		if !present[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)
	slog.WarnContext(ctx, "live-added tables the source catalog does not hold: dropped on the source, or — on a MySQL "+
		"target — pieces of a table name an older release recorded with a comma in it, which no longer admit that table. "+
		"Check each, and `sluice schema add-table <table> --abandon --yes` (with the stream's flags) any entry that "+
		"should not be there", slog.String("stream_id", streamID), slog.String("entries", strings.Join(missing, ", ")))
}

// clearAbandonedAdds removes abandoned pending entries a passing door found
// harmless (out of scope, gone from the source, or not applied). A failure
// WARNs: the entry keeps `sync health` at ADD-TABLE ABANDONED, which is loud.
func clearAbandonedAdds(ctx context.Context, applier ir.ChangeApplier, streamID string, tables []string) {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok || len(tables) == 0 {
		return
	}
	if err := store.ClearPendingAddedTables(ctx, streamID, tables); err != nil {
		slog.WarnContext(ctx, "could not clear abandoned add-table records this stream applies nothing onto; `sync health` "+
			"keeps reporting them", slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return
	}
	slog.InfoContext(ctx, "cleared abandoned add-table records: this stream applies no changes onto those tables",
		slog.String("stream_id", streamID), slog.String("tables", strings.Join(tables, ", ")))
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
		return ir.EstablishedRecord{}, recordReadFailure(err)
	}
	return rec, nil
}

// malformedPendingRecovery is the recovery for a pending record with no
// readable table name.
func malformedPendingRecovery(streamID string) string {
	return fmt.Sprintf("clear the stream's pending add-table records with `UPDATE sluice_cdc_state SET "+
		"pending_added_tables = NULL WHERE stream_id = '%s'` on the target, then bring each table they named in again "+
		"with `sluice schema add-table` (empty its target table first) or re-copy the stream", streamID)
}

// recordReadFailure classifies a failed read of the stream's records: a
// value that does not decode is the terminal INCLUDE-TABLE-SCOPE-DRIFT
// refusal, naming the recovery (a restart reads the same bytes, R6 L1);
// anything else is a restartable control-read failure.
func recordReadFailure(err error) error {
	var bad *ir.UndecodableTableSetError
	if errors.As(err, &bad) {
		return fmt.Errorf("pipeline: warm resume refused: %w: %w", ir.ErrIncludeTableScopeDrift, err)
	}
	return controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
}

// judgePendingAdds marks every in-scope table whose add-table is pending:
// in progress while its heartbeat is fresh, incomplete once stale. An
// ABANDONED entry (R4-1) is not judged here: it is returned, for the door to
// convict unless the table's changes are provably not applied, and an
// abandoned entry for a table out of scope or gone from the source is
// returned as clearable — nothing applies changes onto that table's partial
// copy any more. Any other pending name the source no longer has (dropped or
// renamed since) cannot be streamed into, so it is ignored with a WARN naming
// the abandon path.
func (s *Streamer) judgePendingAdds(ctx context.Context, applier ir.ChangeApplier, streamID string, scope []scopeTable, inScope func(string) bool, entry func(scopeTable) *scopeDriftTable) (abandoned []scopeTable, clearable []string, err error) {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		return nil, nil, nil
	}
	// now is the control-table server's clock, the one every entry was
	// stamped with (S-C): this host's clock is never compared with another's.
	pending, now, err := store.ReadPendingAddedTables(ctx, streamID)
	if err != nil {
		return nil, nil, controlReadFailure(fmt.Errorf("pipeline: include-table scope door: %w", err))
	}
	var absent []string
	for _, p := range pending {
		if p.Malformed != "" && p.Table == "" {
			// No table name can be read out of it, so no scope judges it.
			return nil, nil, fmt.Errorf("pipeline: warm resume refused: %w: stream %q has a pending add-table record that "+
				"does not decode (%q), so the door cannot tell which table it covers; recovery: %s",
				ir.ErrIncludeTableScopeDrift, streamID, p.Malformed, malformedPendingRecovery(streamID))
		}
		if !inScope(p.Table) {
			if p.Abandoned() {
				clearable = append(clearable, p.Table)
			}
			continue
		}
		if !p.Abandoned() {
			warnFutureHeartbeat(ctx, p, now)
		}
		found := false
		for _, t := range scope {
			if t.name != p.Table {
				continue
			}
			found = true
			if p.Abandoned() {
				abandoned = append(abandoned, t)
				continue
			}
			e := entry(t)
			switch {
			case p.Malformed != "":
				// A record that does not decode is judged like a dead add:
				// its copy may be partial.
				e.addIncomplete, e.addMalformed = true, p.Malformed
			case p.Fresh(now):
				e.heartbeatAge = now.Sub(p.HeartbeatAt).Round(time.Second)
				e.addInProgress = true
			default:
				e.heartbeatAge = now.Sub(p.HeartbeatAt).Round(time.Second)
				e.addIncomplete = true
			}
		}
		switch {
		case found:
		case p.Abandoned():
			clearable = append(clearable, p.Table)
		default:
			absent = append(absent, p.Table)
		}
	}
	if len(absent) > 0 {
		slog.WarnContext(ctx, "a pending `sluice schema add-table` names a table the source no longer has; ignoring it "+
			"(`sluice schema add-table --abandon <table>` retires the record)",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(absent, ", ")))
	}
	return abandoned, clearable, nil
}

// judgeAgainstRecord applies the established/dropped record — and, on
// pgoutput, the publication — to every in-scope table. It returns the
// never-established tables whose changes are provably not applied (the WARN
// case), by key.
//
// It also returns what the record must follow once the door passes (fix):
// the renames it proved or the operator acknowledged
// ([Streamer.detectRenames]), old entry → new entry, and the new ids of
// tables rebuilt under an engine whose ids a rebuild changes
// ([Streamer.judgeTableIdentities]).
func (s *Streamer) judgeAgainstRecord(ctx context.Context, streamID string, scope streamScope, rec ir.EstablishedRecord, m *ir.PublicationMembership, entry func(scopeTable) *scopeDriftTable) (notApplied map[string]bool, fix ir.RecordNarrowing, err error) {
	established, dropped := toSet(rec.Established), toSet(rec.Dropped)
	missing := map[string]bool{}
	if m != nil {
		missing = toSet(m.Missing)
	}
	ids := s.readTableIdentities(ctx, scope, scope.tables, len(rec.IDs) > 0)
	followed, targetNotRenamed, gone, err := s.detectRenames(ctx, streamID, scope, rec, established, dropped, ids)
	if err != nil {
		return nil, fix, err
	}
	fix.Renames = map[string]string{}
	for newKey, oldKey := range followed {
		fix.Renames[oldKey] = newKey
	}
	var changed map[string]string
	changed, fix.IDs = s.judgeTableIdentities(ctx, streamID, scope, rec, established, ids)
	var candidates []scopeTable // never established, admitted by a glob or no filter
	for _, t := range scope.tables {
		if why := changed[t.key()]; why != "" {
			entry(t).identityChanged = why
		}
		switch {
		case followed[t.key()] != "":
			// A RENAME TABLE the stream's record follows: the same table.
		case targetNotRenamed[t.key()].old != "":
			miss := targetNotRenamed[t.key()]
			entry(t).renamedFrom, entry(t).renameTargetState = miss.old, miss.targetState(t.name)
		case rec.DroppedAllUncopied && !established[t.key()]:
			// The dropped set was unreadable at the last cold start (R6 L1):
			// any table that cold start did not copy may be a stale one.
			entry(t).droppedUnknown = true
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
	s.scopeDriftGone = gone
	notApplied = map[string]bool{}
	if len(candidates) == 0 {
		return notApplied, fix, nil
	}
	applied, err := s.changesWouldApply(ctx, scope, candidates, m)
	if err != nil {
		return nil, fix, err
	}
	var outside []string
	for _, t := range candidates {
		if why := applied[t.key()]; why != "" {
			entry(t).appliedWhy = why
		} else {
			outside = append(outside, t.display())
			notApplied[t.key()] = true
		}
	}
	if len(outside) > 0 {
		sort.Strings(outside)
		slog.WarnContext(ctx, "INCLUDE-TABLE-NOT-ESTABLISHED: tables the stream's filter admits were never brought into "+
			"this stream, and their changes are NOT applied (not in the publication, or no target table — skipped and "+
			"counted); they stay out until `sluice schema add-table` copies them (ADR-0021)",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(outside, ", ")))
	}
	return notApplied, fix, nil
}

// legacyNotDelivered is the no-record stream's "provably not applied": a
// FOR TABLE publication that does not list the table. Every other no-record
// shape has no such proof.
func legacyNotDelivered(scope []scopeTable, m *ir.PublicationMembership) map[string]bool {
	out := map[string]bool{}
	if m == nil || m.Publication == "" || m.AllTables {
		return out
	}
	missing := toSet(m.Missing)
	for _, t := range scope {
		if missing[t.name] {
			out[t.key()] = true
		}
	}
	return out
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
// includes, only when no other sluice slot exists on the same source database.
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
		"(it was cold-started by an older sluice, or only on a control table this role cannot ALTER), so a table added to its "+
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
		d.yieldToLiveAdd()
		if len(s.scopeDriftGone) > 0 && (d.notEstablished || d.appliedWhy != "") {
			d.renameCandidates = s.scopeDriftGone
		}
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
// the established record already does not hold). The source catalog is read
// first, for the entries the record held when the door read it; the write is
// one row-locked read-modify-write of both sets
// ([ir.EstablishedTablesStore.NarrowEstablishedRecord], R4-4), so a
// `schema add-table` completing concurrently is judged under the lock rather
// than overwritten by a replace computed from an older read.
//
// A narrowing that cannot be recorded REFUSES the start (R6 S1,
// INCLUDE-TABLE-NARROWING-UNRECORDED). It used to WARN and go on: the table
// stayed established while its changes were discarded, and a later re-widen
// passed it with the narrowed window missing on the target, at exit 0. The
// likeliest cause is the dropped_tables column missing on a control table
// this role cannot ALTER, which the refusal names. A failure that only
// concerns pruning dropped entries the source no longer has (nothing is
// leaving) WARNs: keeping a stale dropped entry can only refuse more.
func (s *Streamer) narrowEstablished(ctx context.Context, applier ir.ChangeApplier, streamID string, rec ir.EstablishedRecord, scope streamScope, fix ir.RecordNarrowing) error {
	inScope := make([]string, 0, len(scope.tables))
	in := map[string]bool{}
	for _, t := range scope.tables {
		inScope = append(inScope, t.key())
		in[t.key()] = true
	}
	var planned []string // established tables this resume no longer applies
	for _, k := range rec.Established {
		if !in[k] && fix.Renames[k] == "" {
			planned = append(planned, k)
		}
	}
	candidates := append(append([]string(nil), rec.Dropped...), planned...)
	if len(candidates) == 0 && len(fix.Renames) == 0 && len(fix.IDs) == 0 {
		return nil
	}
	fail := func(step string, err error) error {
		if len(planned) == 0 {
			slog.WarnContext(ctx, "could not prune the stream's dropped-table record; it is left as it was",
				slog.String("stream_id", streamID), slog.String("step", step), slog.String("error", err.Error()))
			return nil
		}
		return narrowingUnrecordedError(streamID, planned, step, err)
	}
	present, err := s.presentInSource(ctx, scope, candidates)
	if err != nil {
		return fail("read the source catalog", err)
	}
	var gone []string
	for _, k := range candidates {
		if !present[k] {
			gone = append(gone, k)
		}
	}
	fix.InScope, fix.Gone = inScope, gone
	leaving, err := applier.(ir.EstablishedTablesStore).NarrowEstablishedRecord(ctx, streamID, fix)
	if err != nil {
		return fail("record the narrowing", err)
	}
	for old, renamed := range fix.Renames {
		slog.InfoContext(ctx, "the stream's record follows a table renamed while it was stopped",
			slog.String("stream_id", streamID), slog.String("from", strings.ReplaceAll(old, "\x00", ".")),
			slog.String("to", strings.ReplaceAll(renamed, "\x00", ".")))
	}
	for k, id := range fix.IDs {
		slog.WarnContext(ctx, "the stream's record takes a table's new engine id: its id changed while the stream was "+
			"stopped and nothing shows it is a different table. On this engine a rebuild (OPTIMIZE TABLE, a copying "+
			"ALTER, TRUNCATE) assigns a new id, and so does a DROP and re-CREATE under the same name, which sluice cannot "+
			"tell apart (TABLE-RECREATED-WHILE-STOPPED): if the table was re-created while stopped, re-copy it",
			slog.String("stream_id", streamID), slog.String("table", strings.ReplaceAll(k, "\x00", ".")),
			slog.String("old_id", rec.IDs[k]), slog.String("new_id", id))
	}
	if len(leaving) > 0 {
		shown := make([]string, len(leaving))
		for i, k := range leaving {
			shown[i] = strings.ReplaceAll(k, "\x00", ".")
		}
		slog.WarnContext(ctx, "tables left this stream's scope: their changes are NO LONGER APPLIED from here on, and "+
			"bringing one back later refuses (INCLUDE-TABLE-RE-WIDEN) until it is copied again",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(shown, ", ")))
	}
	return nil
}

// narrowingUnrecordedError is the R6 S1 refusal.
func narrowingUnrecordedError(streamID string, leaving []string, step string, err error) error {
	shown := make([]string, len(leaving))
	for i, k := range leaving {
		shown[i] = strings.ReplaceAll(k, "\x00", ".")
	}
	return fmt.Errorf("pipeline: warm resume refused: INCLUDE-TABLE-NARROWING-UNRECORDED: this start narrows stream %q's "+
		"scope — %s leave it, and their changes would no longer be applied — but it could not %s, so a later start that "+
		"brings them back could not know their target copies went stale. Nothing was applied. Either restore the scope "+
		"the stream had (the same --include-table / --exclude-table as before), or fix what the error names (on a control "+
		"table this role cannot ALTER, ship the statement it gives) and start again: %w",
		streamID, strings.Join(shown, ", "), step, err)
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
			why = append(why, "it is on this stream's dropped record (it left this stream's scope earlier, the running stream "+
				"stopped its add-table with LIVE-ADDED-TABLE-EXCLUDED, or a re-copy over an unreadable record could not rule "+
				"either out), so changes to it were discarded and its target copy may be stale")
		}
		if t.renamedFrom != "" {
			why = append(why, fmt.Sprintf("it is %s renamed on the source, but %s: the target was not renamed — rename "+
				"the target table too (the drained model; a different table already holding the new name must be dropped "+
				"or renamed away first), then start again", t.renamedFrom, t.renameTargetState))
		}
		if t.identityChanged != "" {
			why = append(why, TableIdentityChangedMarker+": "+t.identityChanged+", so its target copy is of a different "+
				"table and applying its changes onto it would mix the two")
		}
		if len(t.renameCandidates) > 0 {
			why = append(why, fmt.Sprintf("if it is one of this stream's tables renamed while the stream was stopped (%s, gone "+
				"from the source) and the target table was renamed too, acknowledge it with --accept-table-rename <old>=%s — "+
				"sluice could not prove the rename by the table's id", strings.Join(t.renameCandidates, ", "), t.name))
		}
		if t.droppedUnknown {
			why = append(why, "the stream's dropped-table record could not be read at its last cold start, so every table that "+
				"cold start did not copy counts as possibly stale")
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
		switch {
		case t.addMalformed != "":
			why = append(why, fmt.Sprintf("its pending add-table record does not decode (%q), so its copy may be partial; "+
				"`sluice schema add-table --abandon %s --yes` retires the record", t.addMalformed, t.name))
		case t.addIncomplete:
			why = append(why, fmt.Sprintf("its `sluice schema add-table` started and did not complete (last heartbeat %s ago), "+
				"so its copy may be partial", t.heartbeatAge))
		}
		if t.addAbandoned {
			why = append(why, "its `sluice schema add-table` was abandoned (`--abandon`), so its target copy may be partial "+
				"and this stream would keep applying changes onto it")
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

// coldStartIncompleteError is the door's refusal of a stream whose record
// reads COLD-START-IN-PROGRESS: a cold start passed its point of no return
// and did not finish, so the target holds neither the copy the persisted
// position describes nor a finished new one.
func coldStartIncompleteError(streamID string) error {
	return fmt.Errorf("pipeline: warm resume refused: %w: COLD-START-INCOMPLETE: stream %q began a cold start that "+
		"did not finish — it had started changing the target (its established-table record reads cold start in "+
		"progress), so the persisted position no longer describes what the target holds, and resuming from it "+
		"would leave every table the cold start touched wrong while the stream reported healthy. Re-run the cold "+
		"start: the same `sluice sync start` command with --restart-from-scratch if that is what was interrupted "+
		"(on a Postgres source, first drop the stream's replication slot with `sluice slot drop` if that run's "+
		"refusal says it exists), or with --reset-target-data if that was",
		ir.ErrIncludeTableScopeDrift, streamID)
}

// beginColdStartRecord marks the stream's established record
// COLD-START-IN-PROGRESS ([ir.EstablishedRecord.ColdStartInProgress]) at a
// cold start's point of no return: immediately before its first destructive
// step (the target drop of --reset-target-data or --restart-from-scratch),
// or, on a branch that drops nothing, once its last refusal has passed and
// the next step is the first target write of the copy. Everything before
// that point — every preflight, the snapshot open that refuses "slot already
// exists" — leaves the record exactly as it was, so a refused cold start
// leaves the stream judged on the record it had. (The EnsurePublication that
// runs before those refusals may have widened the publication; the record,
// not the publication, is what convicts.) A record is never set back to "not
// recorded": that state is for streams that never had one.
//
// A failure to mark a RECORDED stream stops the cold start before it changes
// anything: proceeding would leave a record describing a target this cold
// start is about to change. A stream with no record (no row yet, or an older
// stream on a control table this role cannot ALTER) has nothing to protect,
// and the failure WARNs.
func beginColdStartRecord(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	store, ok := applier.(ir.EstablishedTablesStore)
	if !ok {
		return nil
	}
	err := store.BeginColdStartRecord(ctx, streamID)
	if err == nil {
		return nil
	}
	if rec, rerr := store.ReadEstablishedRecord(ctx, streamID); rerr == nil && !rec.Recorded {
		slog.WarnContext(ctx, "cold start could not mark the established-table record in progress; the stream has no "+
			"record yet, so there is nothing to protect", slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return nil
	}
	return controlReadFailure(fmt.Errorf("pipeline: cold start: mark the established-table record in progress before "+
		"changing the target (the target was not changed): %w", err))
}

// recordColdStartEstablished ends a successful cold-start lane's
// COLD-START-IN-PROGRESS state: the dropped set loses the tables the copy
// covered (read now, so a drop recorded since is kept), the established set
// becomes the copied set, and the add-table pending entries of the copied
// tables — only those (N1): a pending table the copy left out is still a
// partial copy if a later scope re-admits it — are cleared. Dropped is
// written before established, so until the established write lands the
// record still reads in progress (which refuses) rather than half-written.
// Failures are logged, not returned — the copy and the hand-off succeeded
// and the stream is running — at ERROR when the stream holds the in-progress
// mark (the next warm resume refuses COLD-START-INCOMPLETE until a cold start
// records it; the same holds for a process killed between the anchor write
// and this one: loud, never a stale record), and as the
// INCLUDE-TABLE-SCOPE-UNRECORDED WARN when it has no record at all.
func (s *Streamer) recordColdStartEstablished(ctx context.Context, applier ir.ChangeApplier, streamID string) {
	keys := s.coldStartEstablished
	s.coldStartEstablished = nil
	if keys == nil {
		return
	}
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		if err := recordEstablishedAfterCopy(ctx, store, streamID, keys, s.establishedIDs(ctx, keys), s.sourceTableKeys); err != nil {
			if rec, rerr := store.ReadEstablishedRecord(ctx, streamID); rerr == nil && !rec.Recorded {
				// A first cold start on a control table this role cannot
				// ALTER: there never was a record, and there still is none.
				slog.WarnContext(ctx, "INCLUDE-TABLE-SCOPE-UNRECORDED: cold start could not record the tables it copied, and "+
					"the stream has no record; a later warm resume cannot tell a table added to its scope from one this copy "+
					"brought in (add the established_tables and dropped_tables columns to the control table)",
					slog.String("stream_id", streamID), slog.String("error", err.Error()))
			} else {
				slog.ErrorContext(ctx, "cold start could not record the tables it copied; the next warm resume of this stream "+
					"refuses COLD-START-INCOMPLETE until a cold start records them",
					slog.String("stream_id", streamID), slog.String("error", err.Error()))
			}
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

// recordEstablishedAfterCopy writes the copied set: dropped minus copied,
// then established.
//
// A record that does not decode (R6 L1) is overwritten: the cold start's
// copy is the truth about what is established. The dropped set cannot be
// recomputed from an unreadable value, and treating it as empty could pass a
// stale table as never-established. So the cold start writes the dropped set
// it CAN name (R6-L1): the old dropped entries still readable, plus every
// table the source catalog holds that this copy did not cover — any table
// that ever left the scope and is still on the source is among them — minus
// what it copied. uncopied lists those catalog tables. Only when it cannot
// (the catalog read fails) does the all-uncopied marker
// ([ir.EstablishedRecord.DroppedAllUncopied]) stand in for the set; a later
// cold start that can enumerate replaces it, so it does not outlive the
// failure that wrote it (R6-L1: re-appending it on every cold start made
// every new table of an unfiltered or glob stream refuse, forever). Both
// directions never yield a false "not dropped".
func recordEstablishedAfterCopy(ctx context.Context, store ir.EstablishedTablesStore, streamID string, keys []string, ids map[string]string, uncopied func(context.Context) ([]string, error)) error {
	rec, err := store.ReadEstablishedRecord(ctx, streamID)
	var bad *ir.UndecodableTableSetError
	switch {
	case errors.As(err, &bad):
		slog.ErrorContext(ctx, "cold start: the stream's established-table record does not decode; overwriting it with the "+
			"tables this copy covered, and counting every table the source holds that it did not copy as dropped",
			slog.String("stream_id", streamID), slog.String("column", bad.Column), slog.Int("discarded_bytes", bad.RawBytes))
		// Nothing of the record is readable, so nothing names an old dropped
		// entry: the catalog's uncopied tables are the whole set.
		rec = ir.EstablishedRecord{DroppedAllUncopied: true}
	case err != nil:
		return err
	}
	copied := toSet(keys)
	var dropped []string
	for _, k := range rec.Dropped {
		if !copied[k] {
			dropped = append(dropped, k)
		}
	}
	if rec.DroppedAllUncopied {
		dropped = concreteDroppedSet(ctx, streamID, dropped, copied, uncopied)
		rec.Dropped = append(rec.Dropped, ir.DroppedAllUncopiedEntry) // what is stored, so the write below runs
	}
	if !sameSet(dropped, rec.Dropped) || bad != nil {
		if err := store.RecordDroppedTables(ctx, streamID, dropped); err != nil {
			return err
		}
	}
	return store.RecordEstablishedTables(ctx, streamID, keys, ids)
}

// concreteDroppedSet is the dropped set a cold start writes in place of the
// all-uncopied marker (R6-L1): named (the old entries it can still read,
// minus copied) plus the source catalog's tables the copy did not cover. A
// catalog it cannot read keeps the marker, which a later cold start replaces.
func concreteDroppedSet(ctx context.Context, streamID string, named []string, copied map[string]bool, uncopied func(context.Context) ([]string, error)) []string {
	var catalog []string
	err := errors.New("no source catalog reader")
	if uncopied != nil {
		catalog, err = uncopied(ctx)
	}
	if err != nil {
		slog.WarnContext(ctx, "cold start: could not read the source catalog to name the tables this copy did not cover; "+
			"keeping the all-uncopied marker, so every table it did not copy still counts as dropped until a cold start "+
			"can read the catalog", slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return append(named, ir.DroppedAllUncopiedEntry)
	}
	set := toSet(named)
	for _, k := range catalog {
		if !copied[k] && !set[k] {
			set[k] = true
			named = append(named, k)
		}
	}
	sort.Strings(named)
	return named
}

// sourceTableKeys lists every table in the source catalog, per namespace, as
// established-record keys — the universe [concreteDroppedSet] subtracts the
// copy from. A Postgres reader lists the names through its scope push-down
// without reading any table (the push-down is asked about every name, and
// told no); other readers read the schema.
func (s *Streamer) sourceTableKeys(ctx context.Context) ([]string, error) {
	pairs, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, sc := range pairs {
		var names []string
		sr, schema, err := s.openScopedSourceSchema(ctx, sc.sourceDSN, func(name string) bool {
			names = append(names, name)
			return false
		})
		if err != nil {
			return nil, err
		}
		migcore.CloseIf(sr)
		if _, scoped := sr.(ir.TableScoper); !scoped && schema != nil {
			names = names[:0]
			for _, t := range schema.Tables {
				if t != nil {
					names = append(names, t.Name)
				}
			}
		}
		for _, n := range names {
			if appliershared.IsControlTable(n) || s.Filter.IsSluiceOwned(n) {
				continue // never in any stream's scope
			}
			out = append(out, establishedKey(sc.namespace, n))
		}
	}
	return out, nil
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
