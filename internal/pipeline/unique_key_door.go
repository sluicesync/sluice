// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The unique-key door (UNIQUE-KEY-STOPPED-WINDOW-DOOR).
//
// # The harm
//
// Every sluice target applies a source INSERT as an upsert, and the upsert
// resolves a conflict through a unique key: on a MySQL-family target any
// unique key the table has (ON DUPLICATE KEY UPDATE), on Postgres the
// table's ON CONFLICT key (a conflict through another key raises 23505). A
// target unique key the source does not enforce can turn the source's next
// duplicate of it into an overwrite of a different row, at exit 0
// (SOURCE-DROPPED-UNIQUE-KEY-MERGE). The source-side doors see a DROP INDEX
// only live — pgoutput at the next RelationMessage, the binlog at the DDL
// event — and both baseline at StreamChanges, so a DROP made while the
// stream was stopped is in the baseline; a postgres-trigger, sqlite/d1
// trigger or VStream source sees none at all. The apply-time guard
// (engines/{mysql,postgres}/unique_key_conflict_guard.go) is the backstop
// where every path converges, but two shapes are invisible to it: a table
// with no primary key and exactly one unique key (the merge and the
// newer-image re-send are the same statement), and rows that differ only in
// a NULL or partial-predicate unique-key column. Both need the source's
// CURRENT unique keys, which the applier does not hold.
//
// # The door
//
// So the stream reads both catalogs and compares them: per in-scope table,
// every target unique key (primary key included) must be implied by a
// source one — the subset rule in [irdiff.UncoveredTargetUniqueKeys],
// compared through the IR, never as DDL text. A target key no source key
// implies refuses SOURCE-UNIQUE-KEY-ABSENT, terminal, naming each key.
//
//   - At stream OPEN (---- 4c ----, beside the upsert-key preflight): after
//     the change stream is open, so a cold start's tables exist, and before
//     the first change of the run is applied. Every dispatch branch passes
//     through it — cold start, warm resume, --restart-from-scratch,
//     --reset-target-data, the stopped-cold-start resume, the multi-database
//     fan-out (each namespace against its target counterpart), a fleet leg —
//     because it compares the two catalogs and consults no persisted state:
//     "no cdc-state row" is not "no drift" here.
//   - MID-STREAM, only on a source whose change stream carries no DDL
//     (CDCTriggers, CDCVStream): a re-check every [uniqueKeyRecheckInterval]
//     over the tables that have a target unique key besides the primary key
//     (where the merge happens), run before the first change of a
//     transaction that touches one of them is forwarded, and skipped while
//     none does. See [interceptUniqueKeyRecheck] for the window it leaves.
//     pgoutput and the binlog are not re-checked: their own doors refuse a
//     live unique-key DROP before the next row of the table (postgres and
//     mysql cdc_unforwarded_classes.go).
//   - At a SCHEMA BOUNDARY that moves a table's column list, on every
//     source, for that table, judged by the schema intercept that handles
//     it: before its DDL when the boundary only adds columns (every kept
//     column unchanged), else after its DDL, whether or not it was
//     forwarded ([uniqueKeyBoundaryGate]):
//     a Postgres source drops an index outright
//     with a column it covers, a forwarded DROP COLUMN narrows a
//     MySQL-family target key that included the column (UNIQUE(a,b) →
//     UNIQUE(a)), and a DROP COLUMN at the first boundary after a cold start
//     is not forwarded at all (ADR-0091 §3), leaving the target's column and
//     its key — none of which a source-side door sees, because the source's
//     own change is the column. A table's FIRST boundary of the run is
//     compared with the column list this door read at open (pgoutput sends
//     a snapshot at each table's first Relation of a session, so a DROP
//     COLUMN made before the table's first change arrives as that first
//     boundary; round-2 review HIGH-1). Column lists compare with their
//     Postgres attnums, so a column dropped and re-added under its name is a
//     move, and a boundary whose reader reports a unique key lost with a
//     column ([ir.SchemaSnapshot.UniqueKeysMoved]) is checked even when no
//     list moved: a dropped GENERATED column is not in a pgoutput column list
//     at all (round-3 review). A catalog read that fails there stops the
//     stream (restartable) rather than pass the boundary unjudged.
//
// Tables live-added while the stream runs (ADR-0034) join the re-check only
// at the next start; `schema add-table` judged a pre-created one before
// handing it over, and a table it creates copies the source's keys. The door
// runs after a cold start's bulk copy (it needs the tables to exist), so an
// extra key on a PRE-CREATED target refuses only once the copy is done; the
// copy itself cannot merge through such a key (a plain-INSERT or COPY
// duplicate is loud, the idempotent copy writer refuses an undeclared key).
//
// **Independent expected value:** the source catalog's unique keys, read
// fresh by the source engine's own SchemaReader, against the target
// catalog's, read by the target engine's — neither derived from the stream's
// state or the apply path.
//
// # Policy for a key the target has on purpose
//
// A key the operator built into the target beyond the source's is exactly
// the merge risk, so it refuses too, and is accepted by name with
// `--accept-target-only-unique-key <table>.<key>` (standing, repeatable).
// Keys sluice itself adds are covered without it: a key copied from the
// source is implied by it; a promoted PK-substitute unique index is a source
// index; the `--inject-shard-column` composite only adds a column to a key
// the source enforces (the subset rule); a surrogate the target generates
// on every insert (AUTO_INCREMENT, identity, MySQL's invisible primary key)
// cannot collide. Control tables are never in scope.
//
// # Why the apply guard does not stand down for declared keys
//
// It was considered (it would remove the UNIQUE-KEY-GUARD-OVERLAP-REFUSAL
// trade) and rejected as unsound: the declared set this door reads is fresh
// as of stream OPEN, but the changes a run applies start at the persisted
// position, which can predate it — a DROP INDEX, a duplicate insert, a
// delete of it and a re-CREATE of the key, all while stopped, leave a key
// the source declares at open and a replayed duplicate — which the guard
// refuses where it is armed and which is SILENT on the two shapes it
// cannot see (no primary key and one unique key; rows differing only in a
// NULL or partial-predicate column). Mid-stream, a trigger or VStream
// source's set is fresh only to within the re-check window. The guard stays
// as it is.

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"
)

// uniqueKeyRecheckInterval is the mid-stream re-check's period on a source
// whose change stream carries no DDL. It bounds the window in which a merge
// through a key the source dropped mid-run can be applied before the stream
// stops (for the shapes the apply guard cannot see). Each re-check reads
// only the watched tables on both sides.
const uniqueKeyRecheckInterval = 15 * time.Second

// SourceUniqueKeyRecheckFailedMarker is the grep-stable token on the WARN a
// mid-stream re-check logs when it cannot read a catalog, and on the error
// that stops the stream after [uniqueKeyRecheckMaxFailures] consecutive
// failures: on a source whose change stream carries no DDL the re-check is
// the only mid-stream protection, so it fails closed rather than WARN
// forever while not watching.
const SourceUniqueKeyRecheckFailedMarker = "SOURCE-UNIQUE-KEY-RECHECK-FAILED"

// uniqueKeyRecheckMaxFailures is how many consecutive re-check catalog-read
// failures stop the stream — about a minute at the default period. A
// transient blip WARNs and the next re-check tries again.
const uniqueKeyRecheckMaxFailures = 4

// uniqueKeyDoorScope is one source namespace and its target counterpart.
type uniqueKeyDoorScope struct {
	// namespace is the source namespace on a multi-database stream, ""
	// on a single-database one.
	namespace string
	// targetNamespace qualifies table names in the refusal: the renamed
	// target namespace on a multi-database stream, --target-schema on a
	// single one, "" when the target's DSN names it.
	targetNamespace string
	sourceDSN       string
	targetDSN       string
	targetSchema    string
}

// uniqueKeyDoorState is what the stream-open door hands the mid-stream
// checks.
type uniqueKeyDoorState struct {
	scopes []uniqueKeyDoorScope
	// watched lists, per scope index, the source table names whose target
	// has a unique key besides its primary key.
	watched map[int][]string
	// periodic arms the periodic re-check: a source whose change stream
	// carries no DDL.
	periodic bool
	// openCols is each in-scope source table's column list as the
	// stream-open door read it, keyed by [uniqueKeyColsKey]: the boundary
	// check's baseline for a table's FIRST schema snapshot of the run (the
	// pgoutput reader emits one at each table's first Relation of a
	// session, so a DROP COLUMN made before the table's first change in the
	// run arrives as that first snapshot — round-2 review HIGH-1).
	openCols map[string]string
	// acceptedShapes is each accepted key's shape as the open door saw it
	// ([uniqueKeyCompare.acceptedShapes]); read-only after open.
	acceptedShapes map[string]string
	// openTypes is each source table's column types as the open door read them.
	openTypes map[string]map[string]ir.Type
}

// uniqueKeyColsKey keys a table's column list by source namespace ("" on a
// single-namespace stream; folded) and table name as spelled
// ([uniqueKeyBoundaryGate] folds it for a read fallback only).
func uniqueKeyColsKey(namespace, table string) string {
	return strings.ToLower(namespace) + "\x00" + table
}

// sourceSeesUniqueKeyDDLLive reports a source whose change stream refuses a
// unique-key DROP it reads: pgoutput (the RelationMessage door) and the
// MySQL-family binlog (the DDL-event door).
func sourceSeesUniqueKeyDDLLive(source ir.Engine) bool {
	switch source.Capabilities().CDC {
	case ir.CDCLogicalReplication, ir.CDCBinlog:
		return true
	}
	return false
}

// phaseRefuseTargetOnlyUniqueKeys is the stream-open door (---- 4c ----).
// It records the tables to watch for the mid-stream re-check on a source
// that needs one.
func (s *Streamer) phaseRefuseTargetOnlyUniqueKeys(ctx context.Context, applier ir.ChangeApplier, streamID string, warmResumed bool) error {
	s.uniqueKeyDoor = nil
	s.uniqueKeyRecheckErr.Store(nil)
	scopes, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		return err
	}
	liveAdded, err := s.uniqueKeyDoorLiveAdded(ctx, applier, streamID)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: unique-key door: %w", err))
	}
	openCols, acceptedShapes, openTypes := map[string]string{}, map[string]string{}, map[string]map[string]ir.Type{}
	absent, watched, err := s.compareUniqueKeys(ctx, scopes, uniqueKeyCompare{
		liveAdded: liveAdded, cols: openCols, collations: s.uniqueKeyCollationsArmed(!warmResumed),
		acceptedShapes: acceptedShapes, recordAccepted: true, colTypes: openTypes,
	})
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: unique-key door: %w", err))
	}
	if len(absent) > 0 {
		return s.uniqueKeyRefusal(absent, ir.UniqueKeyDoorAtOpen)
	}
	periodic := !sourceSeesUniqueKeyDDLLive(s.Source) && len(watched) > 0
	s.uniqueKeyDoor = &uniqueKeyDoorState{
		scopes: scopes, watched: watched, periodic: periodic, openCols: openCols, acceptedShapes: acceptedShapes, openTypes: openTypes,
	}
	if periodic {
		slog.InfoContext(ctx, "unique-key door: this source's change stream carries no DDL; re-checking the target's "+
			"unique keys against the source's periodically",
			slog.String("stream_id", streamID),
			slog.Duration("interval", s.uniqueKeyRecheckPeriod()),
			slog.Int("tables", countWatched(watched)))
	}
	return nil
}

// uniqueKeyCollationsArmed reports whether a comparison judges key-column
// collation direction ([keyColumnWeakening]). A source whose change stream
// carries DDL hands a source column's collation change to the key-collation
// door (key_collation_boundary.go), which FORWARDS it to the target in
// forward mode; judging it here at a warm open, before the forward is
// replayed, would refuse a stream that heals itself (pinned by the
// Review2ColumnCollation binlog warm-open cell, which fails if this is
// broken; the KeyCollationFirstBoundary warm-primed cells tolerate any
// refusal and do not), and judging it at a boundary
// would duplicate that door. So on such a source the direction is judged
// only at a cold start's open, when no earlier change is waiting to be
// replayed and a mismatch can only be a target pre-created that way. A
// source whose stream carries no DDL (VStream, the trigger engines) has no
// such door for a stopped-window change, and is judged on every check: the
// VStream half of VSTREAM-KEY-COLUMN-COLLATION.
//
// Stated residual: a DDL-carrying source whose key column is ALTERed during
// a cold start's copy differs from the target created before it at that
// open, and refuses (loudly) although the forward would have healed it.
func (s *Streamer) uniqueKeyCollationsArmed(coldOpen bool) bool {
	return coldOpen || !sourceSeesUniqueKeyDDLLive(s.Source)
}

// uniqueKeyRefusal builds the refusal for this stream.
func (s *Streamer) uniqueKeyRefusal(absent []ir.AbsentUniqueKey, when ir.UniqueKeyDoorWhen) *ir.SourceUniqueKeyAbsentError {
	return &ir.SourceUniqueKeyAbsentError{
		Keys:              absent,
		When:              when,
		SourceSeesDDLLive: sourceSeesUniqueKeyDDLLive(s.Source),
		ShardConsolidated: s.InjectShardColumn.Engaged(),
		RecheckInterval:   s.uniqueKeyRecheckPeriod().String(),
	}
}

func countWatched(watched map[int][]string) int {
	n := 0
	for _, w := range watched {
		n += len(w)
	}
	return n
}

func (s *Streamer) uniqueKeyRecheckPeriod() time.Duration {
	if s.uniqueKeyRecheckEvery > 0 {
		return s.uniqueKeyRecheckEvery
	}
	return uniqueKeyRecheckInterval
}

// uniqueKeyDoorScopes resolves the namespace pairs the stream applies to:
// the stream's own pair, or one per selected database on a
// multi-database stream, each against its (possibly renamed) target
// namespace.
func (s *Streamer) uniqueKeyDoorScopes(ctx context.Context) ([]uniqueKeyDoorScope, error) {
	if !s.multiDatabaseMode() {
		return []uniqueKeyDoorScope{{
			targetNamespace: s.TargetSchema, sourceDSN: s.SourceDSN, targetDSN: s.TargetDSN, targetSchema: s.TargetSchema,
		}}, nil
	}
	selected, _, err := s.resolveStreamDatabases(ctx)
	if err != nil {
		return nil, err
	}
	// The fan-out already refused a flat target for two or more namespaces
	// (streamer_multidb.go); asked again here so this path's target arm
	// cannot be read as reachable for one.
	if err := migcore.ValidateMultiNamespaceTarget(s.Target, "multi-namespace sync unique-key door", selected); err != nil {
		return nil, err
	}
	srcDeriver, ok := s.Source.(ir.DatabaseDSNDeriver)
	if !ok {
		return nil, fmt.Errorf("pipeline: unique-key door: source engine %q cannot derive a per-database DSN", s.Source.Name())
	}
	tgtDeriver, _ := s.Target.(ir.DatabaseDSNDeriver)
	scopes := make([]uniqueKeyDoorScope, 0, len(selected))
	for _, db := range selected {
		srcDSN, err := srcDeriver.WithDatabase(s.SourceDSN, db)
		if err != nil {
			return nil, fmt.Errorf("pipeline: unique-key door: derive source DSN for %q: %w", db, err)
		}
		// A flat target passed the validation above only with one source
		// namespace, whose tables it holds unqualified: its own DSN.
		tgtDSN := s.TargetDSN
		if tgtDeriver != nil {
			if tgtDSN, err = tgtDeriver.WithDatabase(s.TargetDSN, s.NamespaceMap.Apply(db)); err != nil {
				return nil, fmt.Errorf("pipeline: unique-key door: derive target DSN for %q: %w", db, err)
			}
		}
		scopes = append(scopes, uniqueKeyDoorScope{
			namespace: db, targetNamespace: s.NamespaceMap.Apply(db), sourceDSN: srcDSN, targetDSN: tgtDSN,
		})
	}
	return scopes, nil
}

// uniqueKeyDoorLiveAdded is the stream's live-added tables (ADR-0034): in
// scope although the operator's filter may not name them. A read failure
// fails the open (restartable, not the marker): judging only the filter's
// tables would leave the live-added ones unjudged, silently.
func (s *Streamer) uniqueKeyDoorLiveAdded(ctx context.Context, applier ir.ChangeApplier, streamID string) (map[string]bool, error) {
	reader, ok := applier.(liveAddedTablesReader)
	if !ok {
		return nil, nil
	}
	tables, err := reader.ReadLiveAddedTables(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("read live-added tables: %w", err)
	}
	out := make(map[string]bool, len(tables))
	for _, t := range tables {
		out[t] = true
	}
	return out, nil
}

// uniqueKeyCompare is one comparison's inputs beyond the scopes.
type uniqueKeyCompare struct {
	// liveAdded holds tables live-added to the stream (the open door).
	liveAdded map[string]bool
	// only, when non-nil, restricts the read to those tables per scope
	// (the re-checks).
	only map[int][]string
	// cols, when non-nil, receives each source table's column list (the
	// open door's baseline for the boundary check).
	cols map[string]string
	// colTypes, when non-nil, receives each source table's column types (the
	// boundary check's early-call baseline, [uniqueKeyBoundaryGate.typesKept]).
	colTypes map[string]map[string]ir.Type
	// collations arms the collation direction ([keyColumnWeakening]); see
	// [Streamer.uniqueKeyCollationsArmed].
	collations bool
	// acceptedShapes binds each --accept-target-only-unique-key acceptance
	// to the key's shape as accepted (round-5 review MEDIUM-1): the open
	// door records it (recordAccepted), and a later check honours the
	// acceptance only while the key still has that shape. The flag names a
	// key, and a forwarded DROP COLUMN can narrow a MySQL-family key under
	// the same name (UNIQUE (a, x) becomes UNIQUE (a)); passing it by name
	// let the next row duplicating a overwrite another at exit 0. nil (the
	// add-table check, which judges once) honours the name alone. Written
	// only by the open door, read-only after.
	acceptedShapes map[string]string
	recordAccepted bool
}

// acceptedShapeKey keys a target key for [uniqueKeyCompare.acceptedShapes].
func acceptedShapeKey(targetNamespace, table, key string) string {
	return strings.ToLower(targetNamespace) + "\x00" + strings.ToLower(table) + "\x00" + strings.ToLower(key)
}

// acceptedShape renders what an acceptance is bound to: the key's parts
// (with any index collation), NULLS NOT DISTINCT and its predicate.
func acceptedShape(key *ir.Index) string {
	s := renderKeyParts(key)
	if key.ConstraintNullsNotDistinct {
		s += " NULLS NOT DISTINCT"
	}
	if p := strings.TrimSpace(key.Predicate); p != "" {
		s += " WHERE " + p
	}
	return s
}

// compareUniqueKeys reads both catalogs for every scope and returns the
// target unique keys no source key implies (minus the accepted ones), and
// per scope the tables to watch mid-stream.
func (s *Streamer) compareUniqueKeys(ctx context.Context, scopes []uniqueKeyDoorScope, req uniqueKeyCompare) ([]ir.AbsentUniqueKey, map[int][]string, error) {
	liveAdded, only, cols := req.liveAdded, req.only, req.cols
	weakening := func(st, tt *ir.Table) func(string, string) bool {
		if !req.collations {
			return nil
		}
		return keyColumnWeakening(s.Source, s.Target, st, tt)
	}
	accept := parseAcceptedUniqueKeys(s.AcceptTargetOnlyUniqueKeys)
	// ambiguous counts, per 2-part acceptance, the namespaces it accepted a
	// key in (round-5 review LOW-3): on a multi-database stream `<table>.<key>`
	// silently matched every namespace.
	ambiguous := map[int]map[string]bool{}
	var absent []ir.AbsentUniqueKey
	watched := map[int][]string{}
	for i, sc := range scopes {
		var restrict map[string]bool
		if only != nil {
			if len(only[i]) == 0 {
				continue
			}
			restrict = lowerSet(only[i])
		}
		inScope := func(name string) bool {
			if restrict != nil {
				return restrict[strings.ToLower(name)]
			}
			return s.Filter.Allows(name) || liveAdded[name]
		}
		src, err := s.readUniqueKeyDoorSource(ctx, sc, inScope)
		if err != nil {
			return nil, nil, err
		}
		if len(src) == 0 {
			continue
		}
		names := make(map[string]bool, len(src))
		for _, t := range src {
			names[strings.ToLower(t.Name)] = true
		}
		tgt, err := s.readUniqueKeyDoorTarget(ctx, sc, names)
		if err != nil && sc.namespace != "" && ir.IsDatabaseNotFound(err) {
			// A multi-database stream re-resolves its selection from the live
			// source on every start, so a namespace created since the cold
			// start has no target counterpart yet (MySQL errno 1049; a missing
			// Postgres schema reads back empty instead): it holds no keys to
			// judge, and refusing would break a configuration that runs. Only
			// that class: any other read failure fails the open.
			slog.WarnContext(ctx, "unique-key door: the target namespace does not exist yet; its tables are not judged",
				slog.String("namespace", sc.namespace), slog.String("err", err.Error()))
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		for _, st := range src {
			if cols != nil {
				cols[uniqueKeyColsKey(sc.namespace, st.Name)] = columnList(st)
				if req.colTypes != nil {
					req.colTypes[uniqueKeyColsKey(sc.namespace, st.Name)] = columnTypes(st)
				}
			}
			tt := matchTargetTable(tgt, st.Name)
			if tt == nil {
				continue // not created yet, or dropped: other doors' question
			}
			if hasSecondaryUniqueKey(tt) {
				watched[i] = append(watched[i], st.Name)
			}
			weakened := weakening(st, tt)
			for _, key := range uncoveredTargetKeys(st, tt, s.Target.Name(), weakened) {
				table := tt.Name
				if sc.targetNamespace != "" {
					table = sc.targetNamespace + "." + table
				}
				entry, accepted := accept.match(tt.Name, key.Name, sc.namespace, sc.targetNamespace)
				if !accepted {
					absent = append(absent, absentUniqueKey(table, tt, key, weakened))
					continue
				}
				if accept[entry].namespace == "" && len(scopes) > 1 {
					if ambiguous[entry] == nil {
						ambiguous[entry] = map[string]bool{}
					}
					ambiguous[entry][sc.targetNamespace] = true
				}
				if req.acceptedShapes == nil {
					continue
				}
				shapeKey, shape := acceptedShapeKey(sc.targetNamespace, tt.Name, key.Name), acceptedShape(key)
				if req.recordAccepted {
					req.acceptedShapes[shapeKey] = shape
					continue
				}
				recorded, ok := req.acceptedShapes[shapeKey]
				if ok && recorded == shape {
					continue
				}
				k := absentUniqueKey(table, tt, key, weakened)
				k.AcceptedShape, k.AcceptedUnrecorded = recorded, !ok
				absent = append(absent, k)
			}
		}
	}
	for entry, namespaces := range ambiguous {
		if len(namespaces) > 1 {
			e := accept[entry]
			return nil, nil, fmt.Errorf("--accept-target-only-unique-key %s.%s matches the key in %d databases of this "+
				"multi-database stream (%s); name the database it is meant for: <database>.%s.%s",
				e.table, e.key, len(namespaces), strings.Join(sortedNamespaces(namespaces), ", "), e.table, e.key)
		}
	}
	sort.SliceStable(absent, func(a, b int) bool {
		if absent[a].Table != absent[b].Table {
			return absent[a].Table < absent[b].Table
		}
		return absent[a].Key < absent[b].Key
	})
	return absent, watched, nil
}

// readUniqueKeyDoorSource reads the source's in-scope tables. Verbatim
// extension passthrough is on: only keys are compared, so a column type the
// cross-engine copy would refuse is not this door's concern (the cold start
// refuses it on its own read).
func (s *Streamer) readUniqueKeyDoorSource(ctx context.Context, sc uniqueKeyDoorScope, inScope func(string) bool) ([]*ir.Table, error) {
	sr, err := s.Source.OpenSchemaReader(ctx, sc.sourceDSN)
	if err != nil {
		return nil, connectHint(fmt.Errorf("open source schema reader: %w", err))
	}
	defer migcore.CloseIf(sr)
	if err := applyEnabledPGExtensions(ctx, sr, s.EnabledPGExtensions); err != nil {
		return nil, connectHint(fmt.Errorf("enable PG extensions on source: %w", err))
	}
	migcore.ApplyVerbatimExtensionPassthrough(sr, true)
	if scoper, ok := sr.(ir.TableScoper); ok {
		scoper.SetTableScope(inScope)
	}
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, connectHint(fmt.Errorf("read source schema: %w", err))
	}
	if schema == nil {
		return nil, nil
	}
	if err := applyUniqueKeyFacts(ctx, sr, schema); err != nil {
		return nil, connectHint(fmt.Errorf("read source unique-key facts: %w", err))
	}
	var out []*ir.Table
	for _, t := range schema.Tables {
		if t == nil || !inScope(t.Name) {
			continue
		}
		// An ORM migration-history table the stream skips is not applied to.
		if s.SkipORMTables {
			if _, ok := recognizeORMTable(t); ok && !explicitlyIncluded(t.Name, s.Filter) {
				continue
			}
		}
		out = append(out, t)
	}
	return out, nil
}

// readUniqueKeyDoorTarget reads the target tables whose names (folded)
// are in names, the way `schema diff` opens the target.
func (s *Streamer) readUniqueKeyDoorTarget(ctx context.Context, sc uniqueKeyDoorScope, names map[string]bool) ([]*ir.Table, error) {
	tr, err := s.Target.OpenSchemaReader(ctx, sc.targetDSN)
	if err != nil {
		return nil, connectHint(fmt.Errorf("open target schema reader: %w", err))
	}
	defer migcore.CloseIf(tr)
	if sc.targetSchema != "" {
		migcore.ApplyTargetSchema(tr, sc.targetSchema)
	}
	if err := applyEnabledPGExtensions(ctx, tr, s.EnabledPGExtensions); err != nil {
		return nil, connectHint(fmt.Errorf("enable PG extensions on target: %w", err))
	}
	migcore.ApplyVerbatimExtensionPassthrough(tr, true)
	inScope := func(name string) bool { return names[strings.ToLower(name)] }
	if scoper, ok := tr.(ir.TableScoper); ok {
		scoper.SetTableScope(inScope)
	}
	schema, err := tr.ReadSchema(ctx)
	if err != nil {
		return nil, connectHint(fmt.Errorf("read target schema: %w", err))
	}
	if schema == nil {
		return nil, nil
	}
	if err := applyUniqueKeyFacts(ctx, tr, schema); err != nil {
		return nil, connectHint(fmt.Errorf("read target unique-key facts: %w", err))
	}
	var out []*ir.Table
	for _, t := range schema.Tables {
		if t != nil && inScope(t.Name) {
			out = append(out, t)
		}
	}
	return out, nil
}

// applyUniqueKeyFacts folds a reader's [ir.UniqueKeyFact]s into the tables
// it just read, in place (the schema is this door's own read, consumed
// nowhere else): a unique index the server does not enforce is dropped from
// the table (it can neither cover a target key nor merge a row), NULLS NOT
// DISTINCT is set for every unique index, and a per-part INDEX collation is
// carried as the part's Collation (dialect "postgres"), which the cover rule
// compares. A reader without the surface (every engine but Postgres) leaves
// the tables as read.
func applyUniqueKeyFacts(ctx context.Context, reader any, schema *ir.Schema) error {
	facts, err := readUniqueKeyFacts(ctx, reader)
	if err != nil {
		return err
	}
	foldUniqueKeyFacts(facts, schema)
	return nil
}

// readUniqueKeyFacts is the fetch half of [applyUniqueKeyFacts]: nil for a
// reader without the surface.
func readUniqueKeyFacts(ctx context.Context, reader any) (map[string]map[string]ir.UniqueKeyFact, error) {
	fr, ok := reader.(ir.UniqueKeyFactReader)
	if !ok {
		return nil, nil
	}
	return fr.UniqueKeyFacts(ctx)
}

// foldUniqueKeyFacts is the fold half of [applyUniqueKeyFacts], in place.
func foldUniqueKeyFacts(facts map[string]map[string]ir.UniqueKeyFact, schema *ir.Schema) {
	for _, t := range schema.Tables {
		if t == nil || facts[t.Name] == nil {
			continue
		}
		tf := facts[t.Name]
		kept := t.Indexes[:0]
		for _, idx := range t.Indexes {
			if idx == nil || !idx.Unique {
				kept = append(kept, idx)
				continue
			}
			f, has := tf[idx.Name]
			if !has {
				kept = append(kept, idx)
				continue
			}
			if !f.Enforcing {
				continue
			}
			idx.ConstraintNullsNotDistinct = idx.ConstraintNullsNotDistinct || f.NullsNotDistinct
			for i := range idx.Columns {
				if i < len(f.PartCollations) && f.PartCollations[i] != "" {
					idx.Columns[i].Collation = f.PartCollations[i]
					if i < len(f.Parts) {
						idx.Columns[i].CollationDialect = f.Parts[i].CollationDialect
					}
				}
			}
			kept = append(kept, idx)
		}
		t.Indexes = kept
		if pk := t.PrimaryKey; pk != nil {
			if f, has := tf[pk.Name]; has && !f.Enforcing {
				t.PrimaryKey = nil
			}
		}
		addUnlistedUniqueKeys(t, tf)
	}
}

// cloneTableKeys copies t deeply enough for [foldUniqueKeyFacts] to change
// its keys without touching t: add-table folds facts into the table it is
// about to CREATE, whose keys must stay as read.
// cloneTableColumns copies t with its own Column values, so a later in-place
// rewrite of the original's columns (an override) does not reach the copy.
func cloneTableColumns(t *ir.Table) *ir.Table {
	if t == nil {
		return nil
	}
	cp := *t
	cp.Columns = make([]*ir.Column, len(t.Columns))
	for i, c := range t.Columns {
		if c != nil {
			col := *c
			cp.Columns[i] = &col
		}
	}
	return &cp
}

func cloneTableKeys(t *ir.Table) *ir.Table {
	cp := *t
	cloneIndex := func(idx *ir.Index) *ir.Index {
		if idx == nil {
			return nil
		}
		c := *idx
		c.Columns = append([]ir.IndexColumn(nil), idx.Columns...)
		return &c
	}
	cp.PrimaryKey = cloneIndex(t.PrimaryKey)
	cp.Indexes = make([]*ir.Index, len(t.Indexes))
	for i, idx := range t.Indexes {
		cp.Indexes[i] = cloneIndex(idx)
	}
	return &cp
}

// addUnlistedUniqueKeys adds to t every enforcing unique index the facts
// describe and the schema read did not list — a Postgres partitioned
// table's, whose keys the reader's index read (ordinary tables only) leaves
// out, so without this the door would see a partitioned target as keyless
// and pass it (round-2 review L-a).
func addUnlistedUniqueKeys(t *ir.Table, tf map[string]ir.UniqueKeyFact) {
	listed := map[string]bool{}
	if t.PrimaryKey != nil {
		listed[t.PrimaryKey.Name] = true
	}
	for _, idx := range t.Indexes {
		if idx != nil {
			listed[idx.Name] = true
		}
	}
	names := make([]string, 0, len(tf))
	for name := range tf {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := tf[name]
		if listed[name] || !f.Enforcing || len(f.Parts) == 0 {
			continue
		}
		idx := &ir.Index{
			Name: name, Unique: true, Columns: append([]ir.IndexColumn(nil), f.Parts...),
			ConstraintNullsNotDistinct: f.NullsNotDistinct, Predicate: f.Predicate, PredicateDialect: f.PredicateDialect,
		}
		if f.Primary && t.PrimaryKey == nil {
			t.PrimaryKey = idx
			continue
		}
		t.Indexes = append(t.Indexes, idx)
	}
}

// uncoveredTargetKeys is the cover rule with the source's secondary index
// names mapped to the names the target engine holds them under (a Postgres
// target table-prefixes them, translate.PGEffectiveIndexName), since a
// cross-dialect expression or partial key is matched by name.
//
// Two source keys can map to one target name (`kd_x` and `x` on table kd
// both become `kd_x`; only an operator-built target reaches that, since
// sluice's own writer would have refused the collision): the name then
// identifies neither, so both lose it and can cover a target key only
// structurally (round-2 review L-c).
func uncoveredTargetKeys(source, target *ir.Table, targetEngine string, weakened func(sourceColumn, targetColumn string) bool) []*ir.Index {
	mapped := translate.WithTargetIndexNames(source, targetEngine)
	if mapped != nil {
		seen := map[string]int{}
		for _, idx := range mapped.Indexes {
			if idx != nil && idx.Unique && idx.Name != "" {
				seen[strings.ToLower(idx.Name)]++
			}
		}
		var out []*ir.Index
		for i, idx := range mapped.Indexes {
			if idx == nil || !idx.Unique || seen[strings.ToLower(idx.Name)] < 2 {
				continue
			}
			if out == nil {
				out = append([]*ir.Index(nil), mapped.Indexes...)
			}
			nameless := *idx
			nameless.Name = ""
			out[i] = &nameless
		}
		if out != nil {
			t := *mapped
			t.Indexes = out
			mapped = &t
		}
	}
	return irdiff.UncoveredTargetUniqueKeysWith(mapped, target, irdiff.CoverOptions{ColumnWeakened: weakened})
}

// keyColumnWeakening is the cover rule's collation direction for one table
// pair (round-2 review MEDIUM-2): it reports a target column that equates
// values its source column keeps apart (case, accents, trailing spaces),
// read through each engine's [ir.CollationLooseningResolver]. A column
// either side cannot classify is not weakened: an unknown is not evidence.
//
// It is armed for a same-family pair and for a Postgres-family target, and
// returns nil otherwise. The lane it withholds, a Postgres or SQLite source
// into a MySQL-family target, would refuse working configurations: a
// Postgres text key is byte-exact, and the target column sluice creates for
// it takes the target's default collation, which is case-insensitive
// (`utf8mb4_0900_ai_ci`), so every such key would refuse at every open.
// That lane's case-folding merge is the key-collation guard's
// (KEY-COLLATION-PG-DEFAULT-TO-MYSQL), which refuses it at apply time for a
// unique key beside a primary key and documents the primary key as
// uncovered.
//
// A Postgres nondeterministic collation reports case AND accent loosening
// because which one it folds is not recorded, so a MySQL `_as_ci` source
// column against a Postgres ICU `ks-level2` target column refuses although
// only case folds there: a named, conservative false refusal, which
// --accept-target-only-unique-key answers.
func keyColumnWeakening(source, target ir.Engine, st, tt *ir.Table) func(sourceColumn, targetColumn string) bool {
	if source == nil || target == nil {
		return nil
	}
	sr, ok1 := source.(ir.CollationLooseningResolver)
	tr, ok2 := target.(ir.CollationLooseningResolver)
	if !ok1 && ok2 && translate.SameStorageShapeFamily(target.Name(), "postgres") {
		return foldingTargetColumn(tr, st, tt)
	}
	if !ok1 || !ok2 {
		return nil
	}
	if !translate.SameStorageShapeFamily(source.Name(), target.Name()) && !translate.SameStorageShapeFamily(target.Name(), "postgres") {
		return nil
	}
	sameFamily := translate.SameStorageShapeFamily(source.Name(), target.Name())
	return func(sourceColumn, targetColumn string) bool {
		sc, tc := columnNamed(st, sourceColumn), columnNamed(tt, targetColumn)
		if sc == nil || tc == nil {
			return false
		}
		sl, sok := sr.CollationLoosening(sc.Type)
		tl, tok := tr.CollationLoosening(tc.Type)
		if sok && tok {
			return tl&^sl != 0
		}
		return unknownCollationWeakened(sc.Type, tc.Type, tl, tok, sameFamily)
	}
}

// foldingTargetColumn is the collation direction for a source engine with
// no [ir.CollationLooseningResolver] (sqlite-trigger, d1-trigger) into a
// Postgres-family target (round-5 review MEDIUM-2): fail closed, narrowly.
// A target key column KNOWN to fold case or accents (citext, a
// nondeterministic collation) is weakened unless the source column's own
// collation folds the same (SQLite NOCASE folds case). The default mapping
// — a SQLite TEXT key onto a deterministic Postgres text column — reports
// no folding and passes, and so does a bpchar column's trailing-space rule,
// which a SQLite CHAR declaration lands on by default. Withholding all of
// it, as before, let 'a' and 'A' merge on a pre-created citext target.
func foldingTargetColumn(tr ir.CollationLooseningResolver, st, tt *ir.Table) func(sourceColumn, targetColumn string) bool {
	return func(sourceColumn, targetColumn string) bool {
		sc, tc := columnNamed(st, sourceColumn), columnNamed(tt, targetColumn)
		if sc == nil || tc == nil {
			return false
		}
		tl, tok := tr.CollationLoosening(tc.Type)
		fold := tl & (ir.LooseCase | ir.LooseAccent)
		if !tok || fold == 0 {
			return false
		}
		var sourceFold ir.CollationLoosening
		if coll, ok := characterCollation(sc.Type); ok && strings.EqualFold(strings.TrimSpace(coll), "nocase") {
			sourceFold = ir.LooseCase
		}
		return fold&^sourceFold != 0
	}
}

// unknownCollationWeakened decides a key column pair one side's resolver
// could not classify (round-3 review LOW): the conservative answer is the
// loud one, so a character column pair of one engine family counts as
// weakened unless both name the same collation or the target's is known to
// fold nothing. Across families the names are not comparable and the pair
// is not judged. A non-character column is never weakened.
func unknownCollationWeakened(source, target ir.Type, tl ir.CollationLoosening, tok, sameFamily bool) bool {
	sn, sChar := characterCollation(source)
	tn, tChar := characterCollation(target)
	switch {
	case !sChar || !tChar:
		return false
	case tok && tl == 0:
		return false
	case !sameFamily:
		return false
	}
	return !strings.EqualFold(sn, tn)
}

// characterCollation returns a character type's collation name.
func characterCollation(t ir.Type) (string, bool) {
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Char:
		return v.Collation, true
	case ir.Varchar:
		return v.Collation, true
	case ir.Text:
		return v.Collation, true
	}
	return "", false
}

// columnNamed finds a column by its exact name, else the one column equal
// to it without case.
func columnNamed(t *ir.Table, name string) *ir.Column {
	var folded *ir.Column
	n := 0
	for _, c := range t.Columns {
		switch {
		case c == nil:
		case c.Name == name:
			return c
		case strings.EqualFold(c.Name, name):
			folded = c
			n++
		}
	}
	if n == 1 {
		return folded
	}
	return nil
}

// matchTargetTable finds the target table for a source table: the exact
// name, else the one name equal without case (a target that folds
// identifiers). Two case-variants of one name on the target match neither.
func matchTargetTable(tables []*ir.Table, name string) *ir.Table {
	var folded []*ir.Table
	for _, t := range tables {
		if t.Name == name {
			return t
		}
		if strings.EqualFold(t.Name, name) {
			folded = append(folded, t)
		}
	}
	if len(folded) == 1 {
		return folded[0]
	}
	return nil
}

// hasSecondaryUniqueKey reports a unique index besides the primary key: the
// tables the mid-stream re-check watches (a merge needs a conflict through
// a key other than the row's own identity).
func hasSecondaryUniqueKey(t *ir.Table) bool {
	for _, idx := range t.Indexes {
		if idx != nil && idx.Unique && idx != t.PrimaryKey {
			return true
		}
	}
	return false
}

// absentUniqueKey renders one refused key. table is the name the refusal
// shows (namespace-qualified on a multi-database stream). A key a table
// constraint owns — a Postgres UNIQUE constraint, a named primary key — is
// marked, since `DROP INDEX` refuses it (measured: a keyless table's unique
// key lands on a Postgres target as a constraint, SQLSTATE 2BP01).
func absentUniqueKey(table string, t *ir.Table, key *ir.Index, weakened func(sourceColumn, targetColumn string) bool) ir.AbsentUniqueKey {
	k := ir.AbsentUniqueKey{
		Table:      table,
		Key:        key.Name,
		Columns:    renderKeyParts(key),
		Constraint: key.ConstraintBacked || (key == t.PrimaryKey && key.ConstraintNamed),
	}
	if weakened != nil {
		for _, c := range key.Columns {
			if c.Expression == "" && weakened(c.Column, c.Column) {
				k.LooserColumns = append(k.LooserColumns, c.Column)
			}
		}
	}
	return k
}

// renderKeyParts renders a key's parts for the refusal: "(k)", "(c(10))",
// "(lower(email))".
func renderKeyParts(key *ir.Index) string {
	parts := make([]string, len(key.Columns))
	for i, c := range key.Columns {
		switch {
		case c.Expression != "":
			parts[i] = c.Expression
		case c.Length > 0:
			parts[i] = fmt.Sprintf("%s(%d)", c.Column, c.Length)
		default:
			parts[i] = c.Column
		}
		if c.Collation != "" {
			parts[i] += " COLLATE " + c.Collation
		}
	}
	out := "(" + strings.Join(parts, ", ") + ")"
	if p := strings.TrimSpace(key.Predicate); p != "" {
		out += " WHERE " + p
	}
	return out
}

func lowerSet(names []string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[strings.ToLower(n)] = true
	}
	return out
}

// acceptedUniqueKeys is the parsed --accept-target-only-unique-key list:
// `<table>.<key>`, or `<namespace>.<table>.<key>` on a multi-database
// stream. Names compare without case.
type acceptedUniqueKeys []acceptedUniqueKey

type acceptedUniqueKey struct{ namespace, table, key string }

func parseAcceptedUniqueKeys(entries []string) acceptedUniqueKeys {
	var out acceptedUniqueKeys
	for _, e := range entries {
		parts := strings.Split(strings.TrimSpace(e), ".")
		switch len(parts) {
		case 2:
			out = append(out, acceptedUniqueKey{table: parts[0], key: parts[1]})
		case 3:
			out = append(out, acceptedUniqueKey{namespace: parts[0], table: parts[1], key: parts[2]})
		}
	}
	return out
}

// validateAcceptedUniqueKeys refuses an entry that is not `<table>.<key>`
// or `<namespace>.<table>.<key>`: a typo would otherwise accept nothing and
// look like a refusal that ignores the flag.
func validateAcceptedUniqueKeys(entries []string) error {
	for _, e := range entries {
		parts := strings.Split(strings.TrimSpace(e), ".")
		ok := len(parts) == 2 || len(parts) == 3
		for _, p := range parts {
			if p == "" {
				ok = false
			}
		}
		if !ok {
			return fmt.Errorf("pipeline: --accept-target-only-unique-key %q: want <table>.<key> (or <namespace>.<table>.<key> "+
				"on a multi-database stream)", e)
		}
	}
	return nil
}

func (a acceptedUniqueKeys) allows(namespace, table, key string) bool {
	_, ok := a.match(table, key, namespace)
	return ok
}

// match returns the entry accepting table.key in any of namespaces (a
// 3-part entry must name one of them; a 2-part entry matches any).
func (a acceptedUniqueKeys) match(table, key string, namespaces ...string) (int, bool) {
	for i, e := range a {
		if !strings.EqualFold(e.table, table) || !strings.EqualFold(e.key, key) {
			continue
		}
		if e.namespace == "" {
			return i, true
		}
		for _, ns := range namespaces {
			if strings.EqualFold(e.namespace, ns) {
				return i, true
			}
		}
	}
	return 0, false
}

func sortedNamespaces(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
