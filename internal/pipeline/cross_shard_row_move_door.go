// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The cross-shard row-move door (CROSS-SHARD-VINDEX-NOT-IN-PK, Bugs 337 and
// 338).
//
// # The harm
//
// --allow-cross-shard-merge (Bug 152's opt-out) asserts that a sharded
// source's keys are unique ACROSS shards at rest. It does not assert that a
// key stays on one shard. On Vitess a table whose primary vindex is not
// within its primary key — the standard layout of sequence ids sharded by a
// tenant column, `PRIMARY KEY(id)` with `hash(tenant)` — moves a row to
// another shard while keeping its id: vtgate refuses an in-place UPDATE of a
// vindex column (VT12001), so the application writes a DELETE on the old
// shard and an INSERT on the new one. The merged stream receives the two
// shards' changes in whatever order vtgate forwards them, and nothing orders
// changes from different shards (event timestamps are whole seconds, the
// shards' GTIDs are not comparable, and vtgate's MinimizeSkew aligns only
// past a 2 s skew). So:
//
//   - a MySQL target, which deletes and upserts by primary key alone, loses
//     the moved row whenever the old shard's DELETE is applied after the new
//     shard's INSERT — every time for the insert-then-delete order (Bug 337,
//     measured 70/70 rows lost);
//   - any target, Postgres included, loses a row moved away and back when the
//     return hop is applied before the outbound one (Bug 338).
//
// The stream stays healthy, nothing is logged above DEBUG, and the target
// silently holds fewer rows than the source.
//
// # When it runs: whenever the stream merges shards, not when a flag says so
//
// The door keys on the TOPOLOGY, not on --allow-cross-shard-merge. Nothing
// persists that flag, and Bug 152's collision door, which requires it, runs
// only on a fresh cold start (not on a warm resume, --schema-already-applied,
// --reset-target-data or a resumed copy; streamer_coldstart.go). A stream
// that merges shards without the flag on its current start merges them all
// the same. So every stream without --inject-shard-column is judged, and
// the source decides whether it merges: the shards the change stream will
// actually read ([ir.ShardRoutingReader.StreamShards], the DSN's
// vstream_shards pin if set, else the discovered layout). One shard merges
// nothing and passes; an unsharded or non-Vitess source passes; a sharded
// source that resolves to NO shard is unverified and refuses (it is not
// unsharded).
//
// # The judgement
//
// Every in-scope table with a primary key is judged on its routing in the
// SOURCE keyspace ([ir.ShardRoutingReader.ShardRouting]): its primary vindex
// must be FUNCTIONAL (computed from its columns) and lie within its primary
// key, because then the key fixes the vindex value, which fixes the shard,
// and every change to a key comes from one shard in that shard's order.
// Anything else refuses, naming each table, its vindex and its key, before
// anything is read from the source or written to the target:
//
//   - primary-vindex columns outside the primary key — the moved-row shape;
//   - a lookup (non-functional) primary vindex, which places a row by a table
//     it consults, so even a key containing its columns does not fix the
//     shard;
//   - a reference table (every shard holds every row, so each change arrives
//     once per shard, interleaved);
//   - a table the VSchema has no entry for, and a routing or shard set that
//     could not be read at all (a silent-loss guard does not skip what it
//     cannot see). UNVERIFIED PREMISE: that a PlanetScale role may read the
//     VSchema was never measured (PLANETSCALE-SHOW-VSCHEMA-ACCESS-UNMEASURED);
//     if it may not, every merged PlanetScale stream lands here, loudly.
//
// The routing is read when the stream starts. An `ALTER VSCHEMA` made while
// the stream runs is judged only at its next start.
//
// The remedy is --inject-shard-column with ONE STREAM PER SHARD (each source
// DSN pinned with vstream_shards=<shard>, each with its own VALUE): the
// target key becomes (VALUE, primary key), so the two shards' copies of a key
// are two rows, each written by one stream in its own shard's order, and a
// move is the deletion of one row and the insertion of another — exact in any
// order. A single stream over every shard with --inject-shard-column stamps
// ONE value on every shard's rows and separates nothing; this door does not
// see that shape (Bug 152's door returns before it too), which is why the
// remedy text spells the per-shard topology out
// (INJECT-SHARD-COLUMN-MERGED-STREAM, filed).
//
// --assert-no-cross-shard-row-moves is the operator's guarantee that the
// application never moves a row to another shard while keeping its primary
// key; it turns the refusal into a WARN carrying CROSS-SHARD-ROW-MOVES-ASSERTED
// and naming every table it covered. It does not cover a reference table:
// that hazard is the routing itself, not an application behaviour, so the
// refusal stands for one whatever is asserted.
//
// # What it does not judge, stated so the name is not read wider
//
//   - A table without a primary key. A keyless table is applied by full row
//     image with no upsert (at-least-once, ADR-0010), so a reordered move
//     leaves a duplicate, never a loss. A table with a UNIQUE key but no
//     primary key, and a secondary UNIQUE key on a table that has one, are
//     upsert-conflict keys the same move can hand between shards; they are
//     filed (docs/dev/audit-backlog.md, CROSS-SHARD-UNIQUE-KEY-RESIDENCY),
//     not judged here.
//   - Secondary vindexes. Only the primary vindex places a row on a shard.
//
// # Who reaches it
//
// [Streamer.Run]: every `sync start` and every `sync run` leg, every attempt,
// before the cold-start/warm-resume dispatch, so a cold start and a warm
// resume refuse alike, with or without --allow-cross-shard-merge on this
// start. Its scope is the stream's table filter: a table `schema add-table`
// brought in is judged here again only if the filter admits it (no
// --include-table, or the table named there); otherwise it was judged once,
// by [AddTable.Run], when it was added. [AddTable.Run] judges the table it
// adds against the shards its --source DSN resolves to (add-table stamps no
// discriminator). The enumeration of the paths that do not reach it, with
// reasons, is TestCrossShardRowMoveDoorRoster_EveryMergeEntryPoint.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// crossShardVindexNotInPKMarker is the grep-stable marker the refusal
// carries.
const crossShardVindexNotInPKMarker = "CROSS-SHARD-VINDEX-NOT-IN-PK"

// crossShardRowMovesAssertedMarker is the grep-stable marker the WARN
// carries when --assert-no-cross-shard-row-moves turns the refusal into a
// recorded assertion.
const crossShardRowMovesAssertedMarker = "CROSS-SHARD-ROW-MOVES-ASSERTED"

// errCrossShardRowMoveRefused is the sentinel cause of the refusal; tests
// assert with errors.Is without coupling to the message text.
var errCrossShardRowMoveRefused = errors.New("pipeline: cross-shard merge refused: a key can be held by more than one shard")

// crossShardDoorSurface is the command a door call speaks for; it picks the
// remedy the refusal can honestly name.
type crossShardDoorSurface int

const (
	// surfaceStream is `sync start` / a `sync run` leg: the remedy is one
	// stream per shard with --inject-shard-column.
	surfaceStream crossShardDoorSurface = iota
	// surfaceAddTable is `schema add-table`, which stamps no discriminator:
	// per-shard inject is not something it can do.
	surfaceAddTable
)

func (s crossShardDoorSurface) String() string {
	if s == surfaceAddTable {
		return "schema add-table"
	}
	return "sync start"
}

// crossShardRowMoveDoor is one entry point's call of the door.
type crossShardRowMoveDoor struct {
	source    ir.Engine
	sourceDSN string
	surface   crossShardDoorSurface
	// merging is whether this run would merge shards into one target table if
	// the source is sharded: true on any stream without --inject-shard-column
	// (whatever --allow-cross-shard-merge says on this start) and always on
	// `schema add-table`. Whether shards are actually merged is the source's
	// answer ([ir.ShardRoutingReader.StreamShards]).
	merging bool
	// asserted is --assert-no-cross-shard-row-moves.
	asserted bool
	// readSchema reads the in-scope source schema. It is called only once
	// the stream has been found to read more than one shard, so an unsharded
	// or single-shard stream pays nothing.
	readSchema func(ctx context.Context) (*ir.Schema, error)
}

// crossShardKeyHazard is one table whose key can be held by more than one
// shard.
type crossShardKeyHazard struct {
	table   string
	pk      []string
	routing ir.ShardRouting
	// outside is the primary-vindex columns that are not in pk
	// ([ir.ShardRoutingVindex] only).
	outside []string
}

// assertable reports whether --assert-no-cross-shard-row-moves covers the
// hazard. A reference table is the one shape it does not: every shard holds
// every row whatever the application does.
func (h crossShardKeyHazard) assertable() bool {
	return h.routing.Kind != ir.ShardRoutingEveryShard
}

func (h crossShardKeyHazard) String() string {
	key := "primary key (" + strings.Join(h.pk, ", ") + ")"
	switch h.routing.Kind {
	case ir.ShardRoutingVindex:
		return fmt.Sprintf("%s: primary vindex (%s) has column(s) %s outside its %s",
			h.table, strings.Join(h.routing.PrimaryVindex, ", "), strings.Join(h.outside, ", "), key)
	case ir.ShardRoutingNonFunctional:
		return fmt.Sprintf("%s: primary vindex (%s) is a %s vindex, which places a row by lookup, not by its columns, "+
			"so its %s does not fix the shard",
			h.table, strings.Join(h.routing.PrimaryVindex, ", "), vindexTypeName(h.routing.VindexType), key)
	case ir.ShardRoutingEveryShard:
		return fmt.Sprintf("%s: a reference table — every shard holds every row, so each change to its %s arrives once per shard",
			h.table, key)
	default:
		return fmt.Sprintf("%s: the source keyspace's VSchema has no entry for it, so nothing establishes which shard a %s value lives on",
			h.table, key)
	}
}

func vindexTypeName(t string) string {
	if t == "" {
		return "untyped"
	}
	return t
}

// judgeKeyResidency is the DECISION, split from the reads so every shape is
// graded without a cluster: the hazard when table's key can be held by more
// than one shard in turn, or nil when its routing proves a key lives on one
// shard. Column names compare case-insensitively, as MySQL's do.
func judgeKeyResidency(table string, pk []string, routing ir.ShardRouting) *crossShardKeyHazard {
	switch {
	case routing.Kind == ir.ShardRoutingVindex && len(routing.PrimaryVindex) > 0:
		var outside []string
		for _, vc := range routing.PrimaryVindex {
			if !containsFold(pk, vc) {
				outside = append(outside, vc)
			}
		}
		if len(outside) == 0 {
			return nil
		}
		return &crossShardKeyHazard{table: table, pk: pk, routing: routing, outside: outside}
	default:
		// NonFunctional never fixes the shard, EveryShard holds the key on
		// every shard, and Unknown (the zero value) proves nothing. A vindex
		// routing with no columns proves nothing either, and is judged as
		// Unknown rather than passing on an empty "every column is in the
		// key".
		if routing.Kind == ir.ShardRoutingVindex {
			routing = ir.ShardRouting{Kind: ir.ShardRoutingUnknown}
		}
		return &crossShardKeyHazard{table: table, pk: pk, routing: routing}
	}
}

func containsFold(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

// streamShards resolves the shards the stream reads: the source's own answer
// when it can give one ([ir.ShardRoutingReader.StreamShards]), else the
// keyspace layout ([ir.ShardDiscoverer]), which can only over-judge. sharded
// is false for a source that is not sharded at all.
func (d crossShardRowMoveDoor) streamShards(ctx context.Context) (shards []string, sharded bool, err error) {
	if r, ok := d.source.(ir.ShardRoutingReader); ok {
		return r.StreamShards(ctx, d.sourceDSN)
	}
	disc, ok := d.source.(ir.ShardDiscoverer)
	if !ok {
		return nil, false, nil
	}
	shards, err = disc.DiscoverShards(ctx, d.sourceDSN)
	return shards, true, err
}

// run applies the door: nil when the run merges no shards, the stream reads
// one shard (or the source is not sharded), or every keyed table's routing
// proves a key lives on one shard; otherwise the refusal, or — under the
// assertion, when every hazard is assertable — a WARN and nil.
func (d crossShardRowMoveDoor) run(ctx context.Context) error {
	if !d.merging {
		return nil
	}
	shards, sharded, err := d.streamShards(ctx)
	switch {
	case err != nil:
		return d.unverified(ctx, nil, fmt.Sprintf("could not determine which shards the stream reads (%v)", err))
	case !sharded:
		return nil
	case len(shards) == 0:
		// Not "unsharded": a sharded source answered with no shard at all
		// (a keyspace vtgate does not serve, a discovery that came back
		// empty). Bug 152's door takes this as one shard; this one does not
		// (BUG152-ZERO-SHARDS-PASSES, filed).
		return d.unverified(ctx, nil, "the sharded source resolved to no shards")
	case len(shards) == 1:
		return nil
	}
	schema, err := d.readSchema(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: %s: read the source schema for the cross-shard row-move check: %w", d.surface, err))
	}
	keyed := map[string][]string{}
	var names []string
	if schema != nil {
		for _, t := range schema.Tables {
			if t == nil || t.PrimaryKey == nil || len(t.PrimaryKey.Columns) == 0 {
				continue
			}
			pk := make([]string, 0, len(t.PrimaryKey.Columns))
			for _, c := range t.PrimaryKey.Columns {
				pk = append(pk, c.Column)
			}
			keyed[t.Name] = pk
			names = append(names, t.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	reader, ok := d.source.(ir.ShardRoutingReader)
	if !ok {
		return d.unverified(ctx, names, fmt.Sprintf("the %s source reads %d shards but cannot report how it places rows on them", d.source.Name(), len(shards)))
	}
	routes, err := reader.ShardRouting(ctx, d.sourceDSN, names)
	if err != nil {
		return d.unverified(ctx, names, fmt.Sprintf("could not read the source keyspace's VSchema (%v)", err))
	}
	var hazards []crossShardKeyHazard
	for _, name := range names {
		if h := judgeKeyResidency(name, keyed[name], routes[name]); h != nil {
			hazards = append(hazards, *h)
		}
	}
	if len(hazards) == 0 {
		return nil
	}
	return d.verdict(ctx, shards, hazards)
}

// remedy is the exact way out this surface can honestly name.
func (d crossShardRowMoveDoor) remedy() string {
	if d.surface == surfaceAddTable {
		return "`schema add-table` stamps no shard discriminator, so it cannot bring this table into a per-shard " +
			"--inject-shard-column stream; for such a stream, include the table in each per-shard stream's own cold " +
			"start instead (its --include-table, re-copied with --restart-from-scratch). If the stream merges every " +
			"shard into one table (--allow-cross-shard-merge), the exact alternative is to rebuild it as one stream per " +
			"shard with --inject-shard-column."
	}
	return "The exact remedy is one stream per shard: pin each source DSN to one shard with vstream_shards=<shard> " +
		"and give each its own --inject-shard-column NAME=VALUE (instead of --allow-cross-shard-merge), so the target " +
		"key is (NAME, primary key) and each shard's copy of a key is its own row (ADR-0048); one stream over every " +
		"shard with --inject-shard-column stamps one VALUE on all of them and is not a remedy."
}

// assertOffer is the sentence that offers the assertion, or "" where it is
// not offered.
func (d crossShardRowMoveDoor) assertOffer() string {
	if d.surface == surfaceAddTable {
		return "If the stream merges every shard into one table (--allow-cross-shard-merge) and the application never " +
			"moves a row of this table to another shard while keeping its primary key, pass " +
			"--assert-no-cross-shard-row-moves to proceed on that guarantee (a row that does move can then be lost " +
			"silently); do not pass it for a per-shard --inject-shard-column stream."
	}
	return "If the application never moves a row of these tables to another shard while keeping its primary key, " +
		"pass --assert-no-cross-shard-row-moves to proceed on that guarantee (a row that does move can then be lost silently)."
}

// verdict refuses the hazards, or records the assertion when it covers all of
// them.
func (d crossShardRowMoveDoor) verdict(ctx context.Context, shards []string, hazards []crossShardKeyHazard) error {
	described := make([]string, len(hazards))
	var unassertable []string
	for i, h := range hazards {
		described[i] = h.String()
		if !h.assertable() {
			unassertable = append(unassertable, h.table)
		}
	}
	if d.asserted && len(unassertable) == 0 {
		tables := make([]string, len(hazards))
		for i, h := range hazards {
			tables[i] = h.table
		}
		slog.WarnContext(ctx, crossShardRowMovesAssertedMarker+": --assert-no-cross-shard-row-moves: the operator guarantees "+
			"that the application never moves a row of these tables to another shard while keeping its primary key. "+
			"sluice cannot check this and does not order changes from different shards: if a row ever does move "+
			"(a DELETE on one shard and an INSERT of the same key on another), the target can silently lose it — "+
			"every time on a MySQL target when the INSERT is written first, and on any target when a row moves away "+
			"and back within about two seconds",
			slog.String("surface", d.surface.String()),
			slog.Int("shards", len(shards)),
			slog.String("tables", strings.Join(tables, ", ")),
			slog.String("detail", strings.Join(described, "; ")))
		return nil
	}
	assertNote := d.assertOffer()
	if len(unassertable) > 0 {
		assertNote = "--assert-no-cross-shard-row-moves does not cover " + strings.Join(unassertable, ", ") +
			": a reference table's rows are on every shard whatever the application does, so exclude it " +
			"(--exclude-table) and replicate it from the keyspace it is materialised from, or use per-shard streams."
	}
	return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
		"%w: %s: %s: this stream merges the source's %d shards (%s) into one target table without a shard "+
			"discriminator, and these tables can hold one primary-key value on more than one shard: %s. Vitess refuses "+
			"an in-place UPDATE of a vindex column (VT12001), so a row moves shard as a DELETE on one shard and an INSERT "+
			"of the same key on another, and sluice cannot order changes from different shards: a MySQL target loses the "+
			"moved row whenever the INSERT is applied first, and any target loses a row that moves away and back — "+
			"silently, with the stream healthy. Nothing has been read or written. %s A target already written by such a "+
			"merged stream may be missing rows: compare it with the source (sluice verify) before relying on it. %s",
		errCrossShardRowMoveRefused, crossShardVindexNotInPKMarker, d.surface, len(shards), strings.Join(shards, ","),
		strings.Join(described, "; "), d.remedy(), assertNote,
	))
}

// unverified is the door's answer when the shard set or the routing could
// not be established at all: refused like a hazard, and covered by the
// assertion like one, with the reason in the message.
func (d crossShardRowMoveDoor) unverified(ctx context.Context, tables []string, why string) error {
	if d.asserted {
		slog.WarnContext(ctx, crossShardRowMovesAssertedMarker+": --assert-no-cross-shard-row-moves: the routing of the "+
			"merged tables could not be verified, so the operator's guarantee that no row moves to another shard while "+
			"keeping its primary key is all that stands between a moved row and its silent loss (a reference table "+
			"among them, whose rows are on every shard, is not detected either)",
			slog.String("surface", d.surface.String()),
			slog.String("tables", strings.Join(tables, ", ")),
			slog.String("why", why))
		return nil
	}
	return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
		"%w: %s: %s: this stream merges a sharded source into one target table without a shard discriminator, and "+
			"%s, so nothing proves that a primary-key value lives on one shard — refusing rather than risk a silent loss "+
			"(a row moved to another shard as a DELETE and an INSERT of the same key can be applied in the wrong order). "+
			"%s %s (A transient vtgate error clears on retry; a role that may not run SHOW VSCHEMA does not.)",
		errCrossShardRowMoveRefused, crossShardVindexNotInPKMarker, d.surface, why, d.remedy(), d.assertOffer(),
	))
}

// preflightCrossShardRowMoves is the door for [Streamer.Run]: every
// attempt's entry, before the cold-start/warm-resume dispatch, so both
// refuse alike. It runs on every stream without --inject-shard-column,
// whatever --allow-cross-shard-merge says on this start (the flag is not
// persisted, and a warm resume merges without it). The scope is the stream's
// table filter with the engine defaults and the custom heartbeat table
// applied, as [Streamer.phaseResolveStreamIdentity] will apply them.
//
// Multi-database mode is not judged: the VStream flavors refuse a
// multi-database snapshot (mysql cdc_snapshot.go), so no merged multi-shard
// stream runs in it.
func (s *Streamer) preflightCrossShardRowMoves(ctx context.Context) error {
	if s.multiDatabaseMode() {
		return nil
	}
	return crossShardRowMoveDoor{
		source:    s.Source,
		sourceDSN: s.SourceDSN,
		surface:   surfaceStream,
		merging:   !s.InjectShardColumn.Engaged(),
		asserted:  s.AssertNoCrossShardRowMoves,
		readSchema: func(ctx context.Context) (*ir.Schema, error) {
			filter, _ := migcore.EffectiveTableFilter(s.Filter, s.Source, s.SourceDSN)
			if name := s.customSourceHeartbeatTable(); name != "" {
				filter = filter.WithSluiceOwnedTable(name)
			}
			return readScopedSourceSchema(ctx, s.Source, s.SourceDSN, filter)
		},
	}.run(ctx)
}

// readScopedSourceSchema reads the source schema and keeps the tables filter
// allows. It prunes after the read rather than pushing the scope down, so the
// filter's unmatched-pattern census (Bug 273) is left to the paths that own
// it.
func readScopedSourceSchema(ctx context.Context, source ir.Engine, dsn string, filter migcore.TableFilter) (*ir.Schema, error) {
	sr, err := source.OpenSchemaReader(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil || schema == nil {
		return schema, err
	}
	kept := make([]*ir.Table, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil && filter.Allows(t.Name) {
			kept = append(kept, t)
		}
	}
	schema.Tables = kept
	return schema, nil
}

// preflightCrossShardRowMoves is the door for [AddTable.Run], over the one
// table being added (scoped, as read from the source). add-table stamps no
// discriminator, so it merges whenever its --source DSN resolves to more
// than one shard — the stream's own resolved set when the operator passes
// the stream's DSN (a per-shard stream's pin resolves to one shard and
// passes).
func (a *AddTable) preflightCrossShardRowMoves(ctx context.Context, scoped *ir.Schema) error {
	return crossShardRowMoveDoor{
		source:     a.Source,
		sourceDSN:  a.SourceDSN,
		surface:    surfaceAddTable,
		merging:    true,
		asserted:   a.AssertNoCrossShardRowMoves,
		readSchema: func(context.Context) (*ir.Schema, error) { return scoped, nil },
	}.run(ctx)
}
