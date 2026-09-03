// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The CDC reader's prior-shape seed (SLM-1, audit 2026-09-01; the
// warm-resume witness is SLM-1b).
//
// A reader that refuses a value-changing schema delta needs the shape the
// table had BEFORE the delta. The MySQL lanes derived that from a memo
// their own boundary emitter wrote, which is empty at every table's first
// boundary after a start — so the first DDL per table per process was
// never checked, and Shape A forwards exactly that boundary. This file is
// the streamer's half of the fix: it knows the prior on both paths and
// hands it to the reader through [schemaSeedSetter] before the stream
// starts.
//
//   - Cold start: the RAW source IR the SchemaReader produced, captured in
//     [Streamer.coldStartPrepareSchema] before mappings and the Shape A
//     rewrite touch it (those rewrite types FOR THE TARGET).
//   - Warm resume: the TARGET's current schema, read through the target
//     engine's SchemaReader ([loadTargetZoneWitness]) — an independent
//     witness of the zone family every in-scope temporal column was last
//     committed under — with the retained schema-history version at the
//     persisted position ([loadRetainedSchemaSeed]) as the fallback for
//     any table the target cannot witness ([mergeWarmResumeSeed]).
//
// # Why the target, and not the history alone (SLM-1b)
//
// The first cut seeded a warm resume from the history alone, and the
// pre-tag value-fidelity review found two gaps in that:
//
//   - A recovery loop. A table with a retained row → source `MODIFY c
//     TIMESTAMP` → refusal → the operator follows the hint (stop, ALTER
//     the target, `sync start` on the same stream) → the DDL QueryEvent
//     sits after the persisted position and replays → prior = history
//     (DATETIME) → the same refusal, forever. Only `--restart-from-scratch`
//     or disabling forwarding exit it, and the hint names neither.
//   - An understated residual. History rows exist only for boundaries the
//     reader emitted, so on warm resume the seed reached almost no table:
//     a clean stop → source-only swap → `sync start` → no prior → the
//     boundary primed and every post-DDL row landed in the target's
//     DATETIME column at exit 0.
//
// The target closes both. Its column type is what the operator's own
// ALTER left there, so after the drained-model recovery the witness
// already matches `cur` and the replayed DDL passes; and it exists for
// every table the stream ever created, so a table with no history row
// still has a prior.
//
// # What a warm resume still cannot witness (the honest residual)
//
// The target witnesses a column's zone family only where the target holds
// the SAME family the source did. A table with a `--type-override` on a
// temporal column, a table the target lacks, or a target whose read-back
// does not distinguish the pair (a SQLite `DATETIME` reads as
// `ir.Timestamp{WithTimeZone:false}` — not a sync target today, pinned
// anyway) falls back to the history seed, which covers only tables with
// an emitted boundary: for those tables a swap performed while the stream
// was stopped, on a table with no history row, is still primed rather
// than refused. Stated here rather than hidden; the fallback is logged per
// table so an operator can see which tables resume on which prior.
//
// Two more, from the Postgres source's side (SLM-1c). The witness family
// is the TIMESTAMP pair only: a target `time`/`timetz` read-back is not
// admitted, because a MySQL target holds a source `timetz` as plain
// `TIME` (the zone is dropped by documented policy) and would witness
// every zoned time column as its naive sibling — a phantom swap at the
// next boundary — so a stopped-stream `time`⇄`timetz` swap on a
// Postgres source resumes with no prior for that column and is primed.
// And the loader is installed only when a path re-applies deltas to the
// target ([Streamer.schemaDeltaAppliesToTarget]); under
// `--schema-changes=refuse` no lane is seeded, so a stopped-stream swap
// in that mode primes on the Postgres lane too (the MySQL lanes' refusal
// is unarmed there by the same choice).
//
// # Why the witness is safe to hand the reader
//
// The witness is projected to the zone-family columns ONLY
// ([zoneWitnessProjection]): a target `TEXT` for a source `JSON` never
// enters the prior, so it cannot read as a phantom delta anywhere. That
// is defence in depth — the prior each lane holds (`priorSig` on the
// binlog reader, `schemaSeedSig` on both VStream lanes) is consulted by
// the zone predicate alone; the ADR-0049 true-delta emission gate reads
// the lane's own `snapshotSig`, which a seed never touches.
//
// The cold-start seed for the pipeline's own intercepts
// ([synthesizeColdStartSeedSnapshots]) is a different artifact with a
// different consumer: post-mapping, normalized through the engine's
// comparison lens, and shaped as SchemaSnapshots. The two are kept apart
// on purpose — a mapped type in the reader's prev would read as a phantom
// swap on every overridden column.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"
)

// retainedSchemaSeedLimit bounds the warm-resume history read behind
// [loadRetainedSchemaSeed]. Rows are per boundary, not per table, and the
// ADR-0049 true-delta gate keeps the count small in practice. Reaching the
// bound is WARNed as a possibly-incomplete seed, never treated as complete.
const retainedSchemaSeedLimit = 10_000

// schemaSeedLoader produces the prior-shape seed for the reader about to
// be wired. It is a closure rather than a value so the warm-resume witness
// read of the target ([loadTargetZoneWitness]) runs ONLY for a reader
// that accepts a seed: the trigger-CDC lanes never implement
// [schemaSeedSetter], and reading their target's catalog on every resume
// would be a new failure surface for a lane that consumes nothing from it.
type schemaSeedLoader func(ctx context.Context) ([]*ir.Table, error)

// staticSchemaSeed wraps a seed already in hand (the cold-start source
// IR) as a loader.
func staticSchemaSeed(tables []*ir.Table) schemaSeedLoader {
	return func(context.Context) ([]*ir.Table, error) { return tables, nil }
}

// rawReaderSchemaSeed lists the in-scope tables of the raw source schema
// as the reader's cold-start prior. The pointers are the SchemaReader's
// own: every later pass ([translate.ApplyMappings],
// [translate.ApplyExpressionOverrides], [translate.InjectShardColumn]) is
// copy-on-write, so they stay the untouched source projection.
func rawReaderSchemaSeed(schema *ir.Schema) []*ir.Table {
	if schema == nil {
		return nil
	}
	out := make([]*ir.Table, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil {
			out = append(out, t)
		}
	}
	return out
}

// warmResumeSchemaSeedLoader is the loader [Streamer.phaseOpenChangeStream]
// installs before dispatching to [Streamer.warmResume] — the one place
// with the applier, the source and the persisted position in hand.
func (s *Streamer) warmResumeSchemaSeedLoader(applier ir.ChangeApplier, streamID string, persisted ir.Position) schemaSeedLoader {
	return func(ctx context.Context) ([]*ir.Table, error) {
		return s.loadWarmResumeSchemaSeed(ctx, applier, streamID, persisted)
	}
}

// loadWarmResumeSchemaSeed builds the warm-resume prior: the target's
// zone witness per table, with the retained history version as the
// fallback for any table the target cannot witness. Both reads are loud
// on error — the resume is armed for a refusal whose prev they supply,
// and proceeding without one would reopen the window silently.
func (s *Streamer) loadWarmResumeSchemaSeed(ctx context.Context, applier ir.ChangeApplier, streamID string, persisted ir.Position) ([]*ir.Table, error) {
	history, err := loadRetainedSchemaSeed(ctx, applier, s.Source, streamID, persisted)
	if err != nil {
		return nil, err
	}
	witness, err := s.loadTargetZoneWitness(ctx)
	if err != nil {
		return nil, err
	}
	return mergeWarmResumeSeed(ctx, streamID, witness, history, s.Mappings)
}

// loadTargetZoneWitness reads the target's current schema through the
// target engine's own SchemaReader and returns every table it holds,
// keyed by bare table name — the key both the history seed and the
// reader's cache resolve a source table under.
//
// Every table the target holds is returned, not only the operator's
// filtered set: the witness is consulted by name at a boundary of a table
// the stream already scopes, so over-inclusion changes nothing, while
// filtering here would have to reproduce the live-add merge
// ([tableAllowedWithLiveAdd]) whose sidecar has not started yet.
//
// The read is a projection, not a schema apply, so the reader is opened
// the way `schema diff` opens the target's: `--target-schema` and the
// `--enable-pg-extension` allowlist applied, and verbatim extension
// passthrough ON — an unrecognised non-temporal type on some table must
// not turn a warm resume into a refusal, and a verbatim column is simply
// not a zone-family member.
func (s *Streamer) loadTargetZoneWitness(ctx context.Context) (map[string]*ir.Table, error) {
	if s.Target == nil {
		return nil, errors.New("pipeline: load target zone witness: nil target engine")
	}
	tr, err := s.Target.OpenSchemaReader(ctx, s.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("pipeline: load target zone witness: open target schema reader: %w", err)
	}
	defer migcore.CloseIf(tr)
	migcore.ApplyTargetSchema(tr, s.TargetSchema)
	if err := applyEnabledPGExtensions(ctx, tr, s.EnabledPGExtensions); err != nil {
		return nil, fmt.Errorf("pipeline: load target zone witness: enable PG extensions on target reader: %w", err)
	}
	migcore.ApplyVerbatimExtensionPassthrough(tr, true)
	actual, err := tr.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("pipeline: load target zone witness: read target schema: %w", err)
	}
	out := map[string]*ir.Table{}
	if actual == nil {
		return out, nil
	}
	for _, t := range actual.Tables {
		if t != nil {
			out[t.Name] = t
		}
	}
	return out, nil
}

// mergeWarmResumeSeed resolves the prior per table: the target's zone
// witness where the target can witness the table, the retained history
// version otherwise. Tables are visited in name order so the log — and the
// seed's order — is deterministic.
//
// A table resumes on the HISTORY seed (or, with none, on no prior) when:
//
//   - the target does not hold it;
//   - a `--type-override` touches one of its temporal columns
//     ([overrideTouchesTemporal]): a mapped type stands between source and
//     target, so the target's family says nothing about the source's;
//   - the target holds a column the history types as a zone-family
//     member but reads it back as something else ([witnessCoversHistory]):
//     the target cannot distinguish the pair for that column, and a prior
//     it cannot express would read as a phantom swap.
//
// The seed's contract is source-projection tables; the witness tables
// carry the target's read-back of exactly the two IR types the source
// reader's projection produces for the pair, and nothing else.
func mergeWarmResumeSeed(ctx context.Context, streamID string, witness map[string]*ir.Table, history []*ir.Table, mappings []config.Mapping) ([]*ir.Table, error) {
	historyByName := make(map[string]*ir.Table, len(history))
	for _, t := range history {
		if t != nil {
			historyByName[t.Name] = t
		}
	}
	overrides, err := temporalOverridesByTable(mappings)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(witness)+len(historyByName))
	seen := map[string]bool{}
	for name := range witness {
		names = append(names, name)
		seen[name] = true
	}
	for name := range historyByName {
		if !seen[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	out := make([]*ir.Table, 0, len(names))
	fallback := func(name, reason string, hist *ir.Table) {
		attrs := []any{
			slog.String("stream_id", streamID),
			slog.String("table", name),
			slog.String("reason", reason),
		}
		if hist == nil {
			slog.WarnContext(ctx, "warm resume: the target cannot witness this table's zone family and no schema-history version is retained for it; it resumes WITHOUT a prior shape, so its FIRST schema boundary is not checked for a session-zone cast", attrs...)
			return
		}
		slog.InfoContext(ctx, "warm resume: the target cannot witness this table's zone family; it resumes on the retained schema-history version (a target ALTER cannot clear a session-zone refusal on this table)", attrs...)
		out = append(out, hist)
	}
	for _, name := range names {
		hist := historyByName[name]
		tgt, held := witness[name]
		if !held {
			fallback(name, "the target does not hold the table", hist)
			continue
		}
		if col, touched := overrideTouchesTemporal(overrides[name], tgt, hist); touched {
			fallback(name, fmt.Sprintf("a --type-override stands between source and target on temporal column %q", col), hist)
			continue
		}
		if col, covered := witnessCoversHistory(tgt, hist); !covered {
			fallback(name, fmt.Sprintf("the target reads column %q back as a type outside the zone-family pair", col), hist)
			continue
		}
		out = append(out, zoneWitnessProjection(tgt))
	}
	return out, nil
}

// zoneFamilyMember reports whether a target read-back type is one of the
// two IR types the source reader's own projection produces for the pair —
// `ir.Timestamp{WithTimeZone:true}` (MySQL `TIMESTAMP`, PG `timestamptz`)
// or `ir.DateTime` (MySQL `DATETIME`, PG `timestamp`). It is deliberately
// NOT [zoneFamily]: that predicate classifies `ir.Timestamp{WithTimeZone:
// false}` as zone-naive too, and a target whose read-back collapses the
// pair to that shape (SQLite's `DATETIME`) would witness a source
// `TIMESTAMP` as its own sibling — a phantom swap at the next boundary.
// The forward mapping this relies on is pinned by
// TestSchemaDiffAfterMigrate_MySQLToPostgres_TypeFamilyMatrix (Postgres)
// and is the identity on a MySQL-family target
// (internal/engines/mysql.translateType).
func zoneFamilyMember(t ir.Type) bool {
	switch v := t.(type) {
	case ir.Timestamp:
		return v.WithTimeZone
	case ir.DateTime:
		return true
	}
	return false
}

// isTemporalFamily reports whether t is any temporal IR type — the set
// an override has to touch to make the target's read-back say nothing
// about the source's zone family.
func isTemporalFamily(t ir.Type) bool {
	switch t.(type) {
	case ir.Timestamp, ir.DateTime, ir.Date, ir.Time, ir.Interval:
		return true
	}
	return false
}

// zoneWitnessProjection is the seed table for a witnessed target table:
// bare name (the reader binds its own database; a PG target's schema name
// must not leak into the key), and ONLY the zone-family member columns,
// each carrying the target's read-back type. Every other column is
// omitted rather than carried as a target type — the predicate skips a
// column absent from prev, which is exactly "no prior knowledge" for a
// column whose target type says nothing about the source's.
func zoneWitnessProjection(tgt *ir.Table) *ir.Table {
	out := &ir.Table{Name: tgt.Name}
	for _, c := range tgt.Columns {
		if c != nil && zoneFamilyMember(c.Type) {
			out.Columns = append(out.Columns, &ir.Column{Name: c.Name, Type: c.Type})
		}
	}
	return out
}

// witnessCoversHistory checks that every column the retained history
// types as a zone-family member is read back by the target as a member
// too, where the target still holds the column at all. A column the
// target no longer has is not a coverage gap — a drained-model DROP on
// both sides leaves the history stale, and the next boundary's `cur`
// will lack the column as well. Returns the first uncovered column.
func witnessCoversHistory(tgt, hist *ir.Table) (string, bool) {
	if hist == nil {
		return "", true
	}
	targetTypes := make(map[string]ir.Type, len(tgt.Columns))
	for _, c := range tgt.Columns {
		if c != nil {
			targetTypes[c.Name] = c.Type
		}
	}
	for _, h := range hist.Columns {
		if h == nil || !zoneFamilyMember(h.Type) {
			continue
		}
		if tt, held := targetTypes[h.Name]; held && !zoneFamilyMember(tt) {
			return h.Name, false
		}
	}
	return "", true
}

// temporalOverride is one resolved `--type-override` / `mappings:` entry
// on a column, keyed for [overrideTouchesTemporal].
type temporalOverride struct {
	column string
	target ir.Type
}

// temporalOverridesByTable resolves the operator's mappings per table.
// An unresolvable mapping is an error here as it is on the cold-start
// path — it would have refused the stream's first run.
func temporalOverridesByTable(mappings []config.Mapping) (map[string][]temporalOverride, error) {
	if len(mappings) == 0 {
		return nil, nil
	}
	out := map[string][]temporalOverride{}
	for _, m := range mappings {
		ty, err := translate.ResolveTargetType(m)
		if err != nil {
			return nil, fmt.Errorf("pipeline: warm resume seed: %w", err)
		}
		out[m.Table] = append(out[m.Table], temporalOverride{column: m.Column, target: ty})
	}
	return out, nil
}

// overrideTouchesTemporal reports whether any override on the table
// stands between the source and target on a temporal column — the
// override's own target type is temporal, the target reads the column
// back as temporal, or the retained history types it as temporal. Any of
// the three means the target's read-back of that column is the OVERRIDE's
// family, not the source's, and the witness must not speak for the table.
// Returns the first such column.
func overrideTouchesTemporal(overrides []temporalOverride, tgt, hist *ir.Table) (string, bool) {
	if len(overrides) == 0 {
		return "", false
	}
	columnType := func(t *ir.Table, name string) ir.Type {
		if t == nil {
			return nil
		}
		for _, c := range t.Columns {
			if c != nil && c.Name == name {
				return c.Type
			}
		}
		return nil
	}
	for _, o := range overrides {
		if isTemporalFamily(o.target) || isTemporalFamily(columnType(tgt, o.column)) || isTemporalFamily(columnType(hist, o.column)) {
			return o.column, true
		}
	}
	return "", false
}

// loadRetainedSchemaSeed builds the warm-resume history fallback from the
// applier's ADR-0049 schema history: for every table with retained
// versions, the version in effect at the persisted position. That is a
// real prior — the history row and the position it anchors are committed
// on one transaction (ADR-0049 #4a), so the latest retained version per
// table is at or before persisted.
//
// Resolution is per table through [ir.ResolveSchemaVersion] under the
// source's [ir.PositionOrderer] rather than "first row wins": the
// listing is ordered by a second-precision created_at, and two boundaries
// of one table inside one second tie. A table whose versions cannot be
// resolved (no orderer, an ambiguous partial order) resumes WITHOUT a
// history prior — WARNed and stated, since the alternative is a guessed
// prev that could refuse a forwardable DDL.
//
// An applier without [ir.SchemaHistoryReader] yields no history: the
// target witness alone carries the resume on that pair. A read error is
// returned, not degraded: the resume is armed for a refusal whose prev
// this read supplies, and proceeding without it would reopen the window
// silently.
func loadRetainedSchemaSeed(ctx context.Context, applier ir.ChangeApplier, source ir.Engine, streamID string, persisted ir.Position) ([]*ir.Table, error) {
	reader, ok := applier.(ir.SchemaHistoryReader)
	if !ok {
		return nil, nil
	}
	rows, err := reader.ListSchemaHistory(ctx, streamID, retainedSchemaSeedLimit)
	if err != nil {
		return nil, fmt.Errorf("pipeline: load retained schema seed: %w", err)
	}
	if len(rows) >= retainedSchemaSeedLimit {
		slog.WarnContext(
			ctx, "warm resume: schema history reached the seed read bound; tables whose latest version lies past it resume without a history prior",
			slog.String("stream_id", streamID),
			slog.Int("limit", retainedSchemaSeedLimit),
		)
	}
	orderer, _ := source.(ir.PositionOrderer)
	type tableKey struct{ schema, table string }
	var order []tableKey
	versions := map[tableKey][]ir.RetainedSchemaVersion{}
	for _, row := range rows {
		k := tableKey{row.SchemaName, row.TableName}
		if _, seen := versions[k]; !seen {
			order = append(order, k)
		}
		versions[k] = append(versions[k], ir.RetainedSchemaVersion{
			// The anchors were persisted by the same reader that stamped
			// the persisted position, so they share its engine name.
			Anchor:    ir.Position{Engine: persisted.Engine, Token: row.AnchorPosition},
			TableJSON: row.TableJSON,
		})
	}
	out := make([]*ir.Table, 0, len(order))
	for _, k := range order {
		tbl, err := resolveRetainedSeedTable(orderer, versions[k], persisted)
		if err != nil {
			if errors.Is(err, errRetainedSeedUnresolvable) {
				slog.WarnContext(
					ctx, "warm resume: retained schema versions for a table cannot be resolved at the persisted position; it resumes without a history prior, so unless the target witnesses it its FIRST schema boundary is not checked for a session-zone cast",
					slog.String("stream_id", streamID),
					slog.String("table", k.schema+"."+k.table),
					slog.String("reason", err.Error()),
				)
				continue
			}
			return nil, fmt.Errorf("pipeline: load retained schema seed: %s.%s: %w", k.schema, k.table, err)
		}
		if tbl.Schema == "" {
			tbl.Schema = k.schema
		}
		if tbl.Name == "" {
			tbl.Name = k.table
		}
		out = append(out, tbl)
	}
	return out, nil
}

// errRetainedSeedUnresolvable marks a table that has retained versions
// but no single one resolvable at the persisted position — the
// degrade-with-WARN arm of [loadRetainedSchemaSeed], distinct from a
// corrupt row, which is an error.
var errRetainedSeedUnresolvable = errors.New("no single retained version resolves at the persisted position")

// resolveRetainedSeedTable picks the retained version in effect at
// persisted. A single version needs no ordering; several do, and without
// an orderer (or with an ambiguous partial order) the answer is
// [errRetainedSeedUnresolvable] rather than a guess.
func resolveRetainedSeedTable(orderer ir.PositionOrderer, versions []ir.RetainedSchemaVersion, persisted ir.Position) (*ir.Table, error) {
	if len(versions) == 1 {
		tbl, err := ir.UnmarshalTable(versions[0].TableJSON)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, errors.New("retained schema version decodes to no table")
		}
		return tbl, nil
	}
	if orderer == nil {
		return nil, fmt.Errorf("%w: %d versions retained and the source engine implements no PositionOrderer", errRetainedSeedUnresolvable, len(versions))
	}
	tbl, err := ir.ResolveSchemaVersion(orderer, versions, persisted)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errRetainedSeedUnresolvable, err)
	}
	return tbl, nil
}

// wireReaderSchemaSeed runs the pending seed loader for a reader that
// accepts a seed and hands the result over; the loader is cleared first
// so every attempt derives its own (a warm resume after a cold start in
// the same process must not inherit the cold-start seed — its prior is
// the target witness). Readers without the surface never run the loader
// and silently keep their old behaviour. A loader error is the caller's
// to surface: the reader is armed for a refusal whose prev the seed
// supplies.
func (s *Streamer) wireReaderSchemaSeed(ctx context.Context, r ir.CDCReader) error {
	load := s.readerSchemaSeed
	s.readerSchemaSeed = nil
	if load == nil {
		return nil
	}
	setter, ok := r.(schemaSeedSetter)
	if !ok {
		return nil
	}
	seed, err := load(ctx)
	if err != nil {
		return err
	}
	if len(seed) > 0 {
		setter.SetSchemaSeed(seed)
	}
	return nil
}
