// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The unique-key door (UNIQUE-KEY-STOPPED-WINDOW-DOOR).
//
// # The harm
//
// Every sluice target applies a source INSERT as an upsert, and an upsert
// resolves a conflict through ANY unique key the target has. A target unique
// key the source does not enforce turns the source's next duplicate of it
// into an overwrite of a different row, at exit 0
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
//     (CDCTriggers, CDCVStream): a re-check every [uniqueKeyRecheckInterval],
//     run at a transaction boundary before the next change is forwarded,
//     over the tables that have a target unique key besides the primary key
//     (where the merge happens). See [interceptUniqueKeyRecheck] for the
//     window it leaves. pgoutput and the binlog are not re-checked: their
//     own doors refuse a live unique-key DROP before the next row of the
//     table (postgres and mysql cdc_unforwarded_classes.go).
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
// the source declares at open and a replayed duplicate the guard alone can
// refuse. Mid-stream, a trigger or VStream source's set is fresh only to
// within the re-check window. The guard stays as it is.

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// uniqueKeyRecheckInterval is the mid-stream re-check's period on a source
// whose change stream carries no DDL. It bounds the window in which a merge
// through a key the source dropped mid-run can be applied before the stream
// stops (for the shapes the apply guard cannot see). Each re-check reads
// only the watched tables on both sides.
const uniqueKeyRecheckInterval = 15 * time.Second

// SourceUniqueKeyRecheckFailedMarker is the grep-stable token on the WARN a
// mid-stream re-check logs when it cannot read a catalog. The stream keeps
// running — a transient read failure must not end it — and the next
// re-check tries again; a WARN repeating on every interval means the door is
// not watching.
const SourceUniqueKeyRecheckFailedMarker = "SOURCE-UNIQUE-KEY-RECHECK-FAILED"

// uniqueKeyDoorScope is one source namespace and its target counterpart.
type uniqueKeyDoorScope struct {
	// namespace is the source namespace on a multi-database stream, ""
	// on a single-database one; it qualifies table names in the refusal.
	namespace    string
	sourceDSN    string
	targetDSN    string
	targetSchema string
}

// uniqueKeyDoorState is what the stream-open door hands the re-check.
type uniqueKeyDoorState struct {
	scopes []uniqueKeyDoorScope
	// watched lists, per scope index, the source table names whose target
	// has a unique key besides its primary key.
	watched map[int][]string
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
func (s *Streamer) phaseRefuseTargetOnlyUniqueKeys(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	s.uniqueKeyDoor = nil
	s.uniqueKeyRecheckErr.Store(nil)
	scopes, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		return err
	}
	liveAdded := s.uniqueKeyDoorLiveAdded(ctx, applier, streamID)
	absent, watched, err := s.compareUniqueKeys(ctx, scopes, liveAdded, nil)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: unique-key door: %w", err))
	}
	if len(absent) > 0 {
		return &ir.SourceUniqueKeyAbsentError{
			Keys:              absent,
			When:              ir.UniqueKeyDoorAtOpen,
			SourceSeesDDLLive: sourceSeesUniqueKeyDDLLive(s.Source),
		}
	}
	if !sourceSeesUniqueKeyDDLLive(s.Source) && len(watched) > 0 {
		s.uniqueKeyDoor = &uniqueKeyDoorState{scopes: scopes, watched: watched}
		slog.InfoContext(ctx, "unique-key door: this source's change stream carries no DDL; re-checking the target's "+
			"unique keys against the source's periodically",
			slog.String("stream_id", streamID),
			slog.Duration("interval", s.uniqueKeyRecheckPeriod()),
			slog.Int("tables", countWatched(watched)))
	}
	return nil
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
		return []uniqueKeyDoorScope{{sourceDSN: s.SourceDSN, targetDSN: s.TargetDSN, targetSchema: s.TargetSchema}}, nil
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
		scopes = append(scopes, uniqueKeyDoorScope{namespace: db, sourceDSN: srcDSN, targetDSN: tgtDSN})
	}
	return scopes, nil
}

// uniqueKeyDoorLiveAdded is the stream's live-added tables (ADR-0034): in
// scope although the operator's filter may not name them. A read failure
// leaves them out — the add-table door judged each when it was added.
func (s *Streamer) uniqueKeyDoorLiveAdded(ctx context.Context, applier ir.ChangeApplier, streamID string) map[string]bool {
	reader, ok := applier.(liveAddedTablesReader)
	if !ok {
		return nil
	}
	tables, err := reader.ReadLiveAddedTables(ctx, streamID)
	if err != nil {
		slog.DebugContext(ctx, "unique-key door: live-added tables unreadable; judging the filter's tables",
			slog.String("stream_id", streamID), slog.String("err", err.Error()))
		return nil
	}
	out := make(map[string]bool, len(tables))
	for _, t := range tables {
		out[t] = true
	}
	return out
}

// compareUniqueKeys reads both catalogs for every scope and returns the
// target unique keys no source key implies (minus the accepted ones), and
// per scope the tables to watch mid-stream. only, when non-nil, restricts
// the read to those tables per scope (the re-check).
func (s *Streamer) compareUniqueKeys(
	ctx context.Context,
	scopes []uniqueKeyDoorScope,
	liveAdded map[string]bool,
	only map[int][]string,
) ([]ir.AbsentUniqueKey, map[int][]string, error) {
	accept := parseAcceptedUniqueKeys(s.AcceptTargetOnlyUniqueKeys)
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
		if err != nil && sc.namespace != "" {
			// A multi-database stream re-resolves its selection from the live
			// source on every start, so a namespace created since the cold
			// start has no target counterpart yet (MySQL errors on the missing
			// database): it holds no keys to judge, and turning it into a
			// refusal would break a configuration that runs. The same degrade
			// as loadMultiDatabaseTargetZoneWitness.
			slog.WarnContext(ctx, "unique-key door: the target namespace could not be read; its tables are not judged",
				slog.String("namespace", sc.namespace), slog.String("err", err.Error()))
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		for _, st := range src {
			tt := matchTargetTable(tgt, st.Name)
			if tt == nil {
				continue // not created yet, or dropped: other doors' question
			}
			if hasSecondaryUniqueKey(tt) {
				watched[i] = append(watched[i], st.Name)
			}
			for _, key := range irdiff.UncoveredTargetUniqueKeys(st, tt) {
				if accept.allows(sc.namespace, tt.Name, key.Name) {
					continue
				}
				table := tt.Name
				if sc.namespace != "" {
					table = sc.namespace + "." + table
				}
				absent = append(absent, absentUniqueKey(table, tt, key))
			}
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
	var out []*ir.Table
	for _, t := range schema.Tables {
		if t != nil && inScope(t.Name) {
			out = append(out, t)
		}
	}
	return out, nil
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
func absentUniqueKey(table string, t *ir.Table, key *ir.Index) ir.AbsentUniqueKey {
	return ir.AbsentUniqueKey{
		Table:      table,
		Key:        key.Name,
		Columns:    renderKeyParts(key),
		Constraint: key.ConstraintBacked || (key == t.PrimaryKey && key.ConstraintNamed),
	}
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
	for _, e := range a {
		if e.namespace != "" && !strings.EqualFold(e.namespace, namespace) {
			continue
		}
		if strings.EqualFold(e.table, table) && strings.EqualFold(e.key, key) {
			return true
		}
	}
	return false
}

// interceptUniqueKeyRecheck re-runs the door every period on the watched
// tables, at a transaction boundary — a TxBegin, or a row change outside a
// transaction — before that change is forwarded, so a refusal stops the
// stream between source transactions. A refusal is stored in errStore and
// the channel closed; a catalog read failure WARNs
// ([SourceUniqueKeyRecheckFailedMarker]) and the stream continues.
//
// # The window it leaves, stated
//
// The re-check runs AFTER the change it precedes was read from the source,
// so a source DROP committed before that change is visible to it: the
// transaction that would merge, and everything after it, is refused. What
// it does not cover is the transactions forwarded between the DROP and the
// next re-check — up to one period of them. For the shapes the apply guard
// sees (a table with a primary key, or with two or more unique keys and
// rows that differ in a non-NULL key column) those still refuse at their
// first colliding row; for the two it cannot see (no primary key and one
// unique key; rows differing only in NULL or partial-predicate columns) a
// merge in that window is applied, and the re-check's refusal then names a
// re-copy as the remedy. Holding every change until a re-check started after
// it would close the window at the cost of up to a period of added latency on
// every change of the stream; not built.
func interceptUniqueKeyRecheck(
	ctx context.Context,
	in <-chan ir.Change,
	period time.Duration,
	check func(context.Context) (*ir.SourceUniqueKeyAbsentError, error),
	errStore *atomic.Pointer[error],
	now func() time.Time,
) <-chan ir.Change {
	if check == nil {
		return in
	}
	out := make(chan ir.Change, migcore.RowChanBuffer)
	// The stream-open door just ran: the first period starts now.
	last := now()
	go func() {
		defer close(out)
		inTx := false
		for {
			select {
			case <-ctx.Done():
				return
			case c, ok := <-in:
				if !ok {
					return
				}
				boundary := false
				switch c.(type) {
				case ir.TxBegin:
					boundary, inTx = true, true
				case ir.TxCommit:
					inTx = false
				default:
					boundary = !inTx
				}
				if boundary && now().Sub(last) >= period {
					last = now()
					refusal, err := check(ctx)
					switch {
					case err != nil:
						slog.WarnContext(ctx, SourceUniqueKeyRecheckFailedMarker+": the periodic re-check of the "+
							"target's unique keys against the source's could not read a catalog; the stream continues "+
							"and the next re-check tries again",
							slog.String("err", err.Error()))
					case refusal != nil:
						var e error = refusal
						errStore.Store(&e)
						return
					}
				}
				if !forwardChange(ctx, out, c) {
					return
				}
			}
		}
	}()
	return out
}

// uniqueKeyRecheck builds the re-check intercept's check, or nil when the
// stream needs none (a source that sees unique-key DDL live, or nothing to
// watch).
func (s *Streamer) uniqueKeyRecheck() func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
	door := s.uniqueKeyDoor
	if door == nil || len(door.watched) == 0 {
		return nil
	}
	period := s.uniqueKeyRecheckPeriod()
	return func(ctx context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
		absent, _, err := s.compareUniqueKeys(ctx, door.scopes, nil, door.watched)
		if err != nil || len(absent) == 0 {
			return nil, err
		}
		return &ir.SourceUniqueKeyAbsentError{
			Keys:            absent,
			When:            ir.UniqueKeyDoorMidStream,
			RecheckInterval: period.String(),
		}, nil
	}
}
