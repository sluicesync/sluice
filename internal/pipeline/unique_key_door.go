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
//   - After a SCHEMA CHANGE the stream forwarded to the target, on every
//     source, for that table: a forwarded DROP COLUMN narrows a MySQL-family
//     target key that included the column (UNIQUE(a,b) → UNIQUE(a)) while a
//     Postgres source drops such an index outright, which no source-side
//     door sees because the source's own change is the column.
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
	liveAdded, err := s.uniqueKeyDoorLiveAdded(ctx, applier, streamID)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: unique-key door: %w", err))
	}
	absent, watched, err := s.compareUniqueKeys(ctx, scopes, liveAdded, nil)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: unique-key door: %w", err))
	}
	if len(absent) > 0 {
		return s.uniqueKeyRefusal(absent, ir.UniqueKeyDoorAtOpen)
	}
	periodic := !sourceSeesUniqueKeyDDLLive(s.Source) && len(watched) > 0
	s.uniqueKeyDoor = &uniqueKeyDoorState{scopes: scopes, watched: watched, periodic: periodic}
	if periodic {
		slog.InfoContext(ctx, "unique-key door: this source's change stream carries no DDL; re-checking the target's "+
			"unique keys against the source's periodically",
			slog.String("stream_id", streamID),
			slog.Duration("interval", s.uniqueKeyRecheckPeriod()),
			slog.Int("tables", countWatched(watched)))
	}
	return nil
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
			tt := matchTargetTable(tgt, st.Name)
			if tt == nil {
				continue // not created yet, or dropped: other doors' question
			}
			if hasSecondaryUniqueKey(tt) {
				watched[i] = append(watched[i], st.Name)
			}
			for _, key := range uncoveredTargetKeys(st, tt, s.Target.Name()) {
				if accept.allows(sc.namespace, tt.Name, key.Name) || accept.allows(sc.targetNamespace, tt.Name, key.Name) {
					continue
				}
				table := tt.Name
				if sc.targetNamespace != "" {
					table = sc.targetNamespace + "." + table
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
	fr, ok := reader.(ir.UniqueKeyFactReader)
	if !ok {
		return nil
	}
	facts, err := fr.UniqueKeyFacts(ctx)
	if err != nil {
		return err
	}
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
					idx.Columns[i].CollationDialect = "postgres"
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
	}
	return nil
}

// uncoveredTargetKeys is the cover rule with the source's secondary index
// names mapped to the names the target engine holds them under (a Postgres
// target table-prefixes them, translate.PGEffectiveIndexName), since a
// cross-dialect expression or partial key is matched by name.
func uncoveredTargetKeys(source, target *ir.Table, targetEngine string) []*ir.Index {
	return irdiff.UncoveredTargetUniqueKeys(translate.WithTargetIndexNames(source, targetEngine), target)
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
