// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The target-witnessed first boundary (TWFB; GC-44, amends ADR-0091 §3).
//
// # The gap
//
// Both schema-snapshot intercepts classify a boundary against the shape they
// cached for the table, and the only pre-state they had besides an earlier
// snapshot was the cold-start seed — consumed once, cleared by
// [Streamer.phaseWireInterceptChain] after its first use. Every other wiring
// of an intercept (a warm resume, a supervisor restart, an in-process
// ADR-0038 retry, a VStream reshard reopen) started with an empty cache, so
// each table's FIRST snapshot per intercept instance took the `!hadPre`
// branch: it was cached as the baseline, passed downstream as history, and
// never applied. A change made while the stream was stopped, or after it
// restarted and before the table's first row, therefore never reached the
// target. The loud shapes (a VARCHAR widen refused by the target at 22001,
// an ADD COLUMN refused as schema drift) announced themselves; a type widen
// WITHIN a family — DATETIME fsp, DECIMAL scale, FLOAT → DOUBLE, a PG
// numeric/timestamp precision — did not: the target kept the narrow column
// and rounded every following value, at exit 0.
//
// # The fix: ask the target
//
// The first boundary has no pre-state, but the stream's own target does —
// it holds exactly what every earlier boundary and the cold start left
// there. So on `!hadPre` the intercept renders the snapshot as the target
// would store it (the Shape A discriminator, then
// [translate.RetargetForShapeCompare]) and compares it against the target's
// read-back of the table, column names and types only, through
// [witnessCompareType]'s lens on both sides; a difference is re-checked
// against a fresh catalog read before it is acted on:
//
//   - equal → the baseline is accepted, as before.
//   - the snapshot carries columns the target lacks, and nothing else →
//     forward ADD COLUMN, with the added-column backfill, against a
//     synthesized pre-state (the snapshot minus those columns).
//   - exactly one shared column differs, within one type family on the
//     allowlist, and the TARGET is narrower ([witnessWidthOrder]) → forward
//     ALTER COLUMN TYPE against a synthesized pre-state carrying the
//     TARGET's type, so the session-zone door sees the zone family the
//     target actually holds.
//   - the same, but the target is WIDER → a WARN, and the target keeps its
//     type: a first boundary cannot tell an old shape from a new one, and
//     narrowing a target that already holds wider values is silent loss —
//     UNLESS the stream's retained history proves the source held the
//     target's type and narrowed it ([firstBoundaryWitness.historyPriorAt]):
//     then the narrowing is forwarded like a live one, and a target holding
//     any other type refuses (GC-44 F5 third review).
//   - the target carries columns the snapshot lacks, and nothing else → a
//     WARN (a DROP COLUMN made while stopped is benign: the target keeps
//     the column, as the drained model would).
//   - a column an operator --type-override names is not compared.
//   - anything else — added and dropped together (a possible rename), more
//     than one type change, a change across families the target does not
//     already hold ([targetHoldsEverySnapshotValue]) — refuses BEFORE the
//     snapshot goes downstream, with the grep-stable marker
//     [resumeDivergenceMarker] naming each column and both types.
//
// Nothing is persisted: the check is a pure function of the snapshot and
// the catalog, so a restart re-derives the same verdict. ADR-0049 history
// is written only after a match or an applied forward.
//
// The cold-start seed guard (ADR-0091 §3) consults the same witness for an
// ALTER COLUMN TYPE classified against the seed: the target agreeing with
// the snapshot means the seed was a phantom; disagreeing means it was not
// ([seedBoundaryNeedsWitness]).
//
// # The independent expected value
//
// Named per the 2026-08-01 rule: the TARGET's catalog, read through the
// target engine's own SchemaReader. It shares no code with the change
// stream's projection, and it is what every row after the boundary will
// actually be written into.
//
// # Availability
//
// Postgres and VStream take a first boundary on every resume, and the
// binlog lane does too once it is armed for first touch, so a phantom
// mismatch here would refuse a healthy stream. The lens is exactly the
// set of differences measured between a change-stream projection and a
// target read-back of a column the stream itself created; the
// anti-phantom family matrix pins it for every family the stream maps in
// mysql→postgres, postgres→postgres, postgres→mysql, mysql→mysql,
// mariadb→postgres and mariadb→mysql, plus geometry in the postgis job and
// a VStream source (schema_forward_witness_family_integration_test.go).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
	"sluicesync.dev/sluice/internal/translate"
)

// resumeDivergenceMarker is the grep-stable marker every first-boundary
// refusal carries, so an operator can search a log for it. It is the text of
// [ir.ErrResumeSchemaDivergence], which the refusal wraps so the fleet
// supervisor does not restart a leg into the same refusal forever
// (refusalsARestartRepeats).
var resumeDivergenceMarker = ir.ErrResumeSchemaDivergence.Error()

// targetCatalogLoader reads the target's tables keyed by bare name —
// [Streamer.loadTargetZoneWitness] in production, a fake in unit tests.
type targetCatalogLoader func(ctx context.Context) (map[string]*ir.Table, error)

// targetCatalogWitness is the target's catalog, read lazily and memoized
// for one intercept instance. The only read the target engine offers is the
// whole catalog, so the memo is kept per TABLE: a table absent from it
// (one the target created after the read — a live add) re-reads once, a
// table this intercept has itself changed is marked stale and re-read on
// its next lookup only, and a table whose memo disagrees with a snapshot is
// re-checked against a fresh read once ([targetCatalogWitness.recheck]).
// Every other table keeps its memo. Used only on the intercept's goroutine.
type targetCatalogWitness struct {
	load   targetCatalogLoader
	tables map[string]*ir.Table
	loaded bool

	// refreshed / rechecked bound the full re-reads a table can cause to one
	// each per intercept instance; stale marks a table this intercept
	// altered.
	refreshed map[string]bool
	rechecked map[string]bool
	stale     map[string]bool
}

// newTargetCatalogWitness returns a witness over load. initial, when
// non-nil, is a catalog read already made on this attempt — the SLM-1b
// warm-resume seed read ([Streamer.loadWarmResumeSchemaSeed]) — and saves
// the first read. It may be older than a peer's DDL (Shape A); a mismatch
// is always re-checked against a fresh read before it is acted on.
func newTargetCatalogWitness(load targetCatalogLoader, initial map[string]*ir.Table) *targetCatalogWitness {
	w := &targetCatalogWitness{
		load:      load,
		refreshed: map[string]bool{},
		rechecked: map[string]bool{},
		stale:     map[string]bool{},
	}
	if initial != nil {
		w.tables, w.loaded = initial, true
	}
	return w
}

// lookup returns the target's table named name (case-insensitive when the
// exact spelling misses and exactly one table folds onto it — a MySQL
// target folding identifier case).
func (w *targetCatalogWitness) lookup(ctx context.Context, name string) (*ir.Table, bool, error) {
	if !w.loaded || w.stale[name] {
		if err := w.read(ctx); err != nil {
			return nil, false, err
		}
	}
	if t, ok := w.find(name); ok {
		return t, true, nil
	}
	if w.refreshed[name] {
		return nil, false, nil
	}
	w.refreshed[name] = true
	if err := w.read(ctx); err != nil {
		return nil, false, err
	}
	t, ok := w.find(name)
	return t, ok, nil
}

// recheck re-reads the catalog for a table whose memo disagreed with a
// snapshot, once per table per instance, and returns the fresh read-back.
func (w *targetCatalogWitness) recheck(ctx context.Context, name string) (*ir.Table, bool, error) {
	if !w.rechecked[name] {
		w.rechecked[name] = true
		if err := w.read(ctx); err != nil {
			return nil, false, err
		}
	}
	t, ok := w.find(name)
	return t, ok, nil
}

// forget marks name stale — the intercept has just changed it on the
// target — so its next lookup reads the change back. Other tables keep
// their memo.
func (w *targetCatalogWitness) forget(name string) {
	w.stale[name] = true
}

func (w *targetCatalogWitness) read(ctx context.Context) error {
	tables, err := w.load(ctx)
	if err != nil {
		return err
	}
	w.tables, w.loaded = tables, true
	clear(w.stale)
	return nil
}

func (w *targetCatalogWitness) find(name string) (*ir.Table, bool) {
	if t, ok := w.tables[name]; ok && t != nil {
		return t, true
	}
	var hit *ir.Table
	for n, t := range w.tables {
		if t != nil && strings.EqualFold(n, name) {
			if hit != nil {
				return nil, false
			}
			hit = t
		}
	}
	return hit, hit != nil
}

// firstBoundaryWitness is everything the first-boundary check needs: the
// catalog, and how the stream renders a source table as target storage.
type firstBoundaryWitness struct {
	catalog *targetCatalogWitness

	// history is this stream's own retained schema version per table at the
	// persisted position (nil when the warm resume loaded none). Read by
	// Shape A's peer-added check ([shapeAPeerAdded]), by the unforwarded
	// check as a table's last accepted shape ([priorFor]), and by the
	// narrowing rules ([firstBoundaryWitness.historyPriorAt]).
	history []*ir.Table

	// retained is what else the history says about each version (keyed by
	// the same pointers: its anchor and every shape recorded for the table),
	// and orderer the source engine's position order: the anchor and the
	// order tell a boundary proven after the version from a replay
	// ([firstBoundaryWitness.historyPriorAt]); the recorded shapes serve the
	// unforwarded check where the order is unknown ([priorFor]).
	retained retainedHistory
	orderer  ir.PositionOrderer

	sourceEngine string
	targetEngine string

	// mappings are the operator's --type-override entries. An overridden
	// column's TYPE is the operator's decision, not a rendering of the
	// source's, so the witness does not compare it ([witnessOptions.pinned]).
	mappings []config.Mapping

	// shardColumn is the Shape A discriminator (--inject-shard-column);
	// "" on the single-stream path.
	shardColumn string
}

// witnessVerdictKind is the outcome of comparing a first boundary against
// the target.
type witnessVerdictKind int

const (
	// witnessMatch — the target holds what the snapshot says.
	witnessMatch witnessVerdictKind = iota
	// witnessUnwitnessed — the target cannot speak for the table: it does
	// not hold it, or the engine pair has no storage-shape rendering.
	witnessUnwitnessed
	// witnessTargetOnly — the target carries columns the snapshot lacks,
	// and differs in nothing else.
	witnessTargetOnly
	// witnessForwardAdd — the snapshot carries columns the target lacks,
	// and differs in nothing else.
	witnessForwardAdd
	// witnessForwardAlter — exactly one shared column differs, within an
	// allowlisted type family, and the TARGET is the narrower side (or the
	// pair is the zone-sibling swap, which the forward path's own door
	// refuses with its specific message).
	witnessForwardAlter
	// witnessTargetWider — exactly one shared column differs, within an
	// allowlisted family, and the TARGET is the wider side. Never forwarded:
	// see [witnessWidthOrder].
	witnessTargetWider
	// witnessRefuse — anything else.
	witnessRefuse
)

// witnessColumnDiff is one column the snapshot and the target disagree on,
// rendered for the refusal. An absent side reads "(absent)".
type witnessColumnDiff struct {
	column string
	source string
	target string
}

// witnessVerdict is the classified comparison.
type witnessVerdict struct {
	kind witnessVerdictKind

	// added are the snapshot's columns the target lacks (witnessForwardAdd).
	added []string
	// altered is the one shared column whose type differs, and targetType
	// the target's read-back of it (witnessForwardAlter).
	altered    string
	targetType ir.Type
	// targetOnly are the target's columns the snapshot lacks.
	targetOnly []string
	// diffs is every disagreement, for the refusal and the logs.
	diffs []witnessColumnDiff
	// reason says why the target cannot witness the table
	// (witnessUnwitnessed).
	reason string
	// narrowedFrom is the type the stream's retained history says the
	// source held before it narrowed the altered column (nil unless that
	// proof decided the verdict — [witnessOptions.priorExpected]).
	narrowedFrom ir.Type
}

// narrowedDiffs annotates column's entry in diffs with the type the source
// narrowed it from, for the log line or the refusal.
func narrowedDiffs(diffs []witnessColumnDiff, column string, from ir.Type) []witnessColumnDiff {
	out := append([]witnessColumnDiff(nil), diffs...)
	for i := range out {
		if out[i].column == column {
			out[i].source = fmt.Sprintf("%s (narrowed from %s)", out[i].source, from)
		}
	}
	return out
}

// errWitnessNoTable is returned by [firstBoundaryWitness.expected] when the
// rendering yields no table — defensive; the inputs always carry one.
var errWitnessNoTable = errors.New("rendering the snapshot as target storage produced no table")

// verdict compares the snapshot table post (RAW — the shape the forward
// paths retarget from) against the target's read-back of the same table.
// pos is the boundary's position, which decides whether the stream's
// retained history may judge a narrowing ([firstBoundaryWitness.historyPriorAt]).
func (w *firstBoundaryWitness) verdict(ctx context.Context, post *ir.Table, pos ir.Position) (witnessVerdict, error) {
	if post == nil {
		return witnessVerdict{kind: witnessUnwitnessed, reason: "the snapshot carries no table"}, nil
	}
	if !translate.HasShapeCompareMapping(w.sourceEngine, w.targetEngine) {
		return witnessVerdict{kind: witnessUnwitnessed, reason: fmt.Sprintf(
			"no storage-shape rendering exists for %s → %s", w.sourceEngine, w.targetEngine,
		)}, nil
	}
	target, held, err := w.catalog.lookup(ctx, post.Name)
	if err != nil {
		return witnessVerdict{}, err
	}
	if !held {
		return witnessVerdict{kind: witnessUnwitnessed, reason: "the target does not hold the table"}, nil
	}
	expected, err := w.expected(post)
	if err != nil {
		return witnessVerdict{}, err
	}
	opts := w.options(post)
	if prior := w.historyPriorAt(post.Name, pos); prior != nil {
		if opts.priorExpected, err = w.expected(prior); err != nil {
			return witnessVerdict{}, err
		}
	}
	v := classifyWitness(expected, target, opts)
	if v.kind == witnessMatch {
		return v, nil
	}
	// The memo may predate a change the target has since taken — a Shape A
	// peer's coordinated DDL, or anything an operator ran — so a difference
	// is decided against a fresh read, never against the memo.
	target, held, err = w.catalog.recheck(ctx, post.Name)
	if err != nil {
		return witnessVerdict{}, err
	}
	if !held {
		return witnessVerdict{kind: witnessUnwitnessed, reason: "the target does not hold the table"}, nil
	}
	return classifyWitness(expected, target, opts), nil
}

// expected renders post as the target would store it — the Shape A
// discriminator, then the compare-lane retarget. Operator overrides are not
// applied: an overridden column is not compared at all
// ([witnessOptions.pinned]).
func (w *firstBoundaryWitness) expected(post *ir.Table) (*ir.Table, error) {
	schema := &ir.Schema{Tables: []*ir.Table{post}}
	if w.shardColumn != "" {
		var err error
		schema, err = translate.InjectShardColumn(schema, w.shardColumn, ir.Varchar{Length: 64})
		if err != nil {
			return nil, fmt.Errorf("inject the shard column into %q: %w", post.Name, err)
		}
	}
	schema = translate.RetargetForShapeCompare(schema, w.sourceEngine, w.targetEngine)
	if schema == nil || len(schema.Tables) == 0 || schema.Tables[0] == nil {
		return nil, errWitnessNoTable
	}
	return schema.Tables[0], nil
}

// witnessOptions are the per-stream facts [classifyWitness] needs beyond
// the two tables.
type witnessOptions struct {
	// pinned are the columns an operator --type-override names. Their TYPE
	// is not compared: the override decided what the target holds, the
	// source's type says nothing about it, and the target emitter renders
	// an override in ways no source rendering predicts (a MySQL target reads
	// a `json` override back as binary JSON, a Postgres target a `mediumtext`
	// one as `text`). So the witness never alters an overridden column and
	// never refuses over one. The cost, stated: an override CHANGED between
	// runs, and a source type change on an overridden column, are not seen
	// at a first boundary — exactly as before GC-44, and the CDC→CDC forward
	// path does not apply overrides either. Presence (added / target-only)
	// is still compared.
	pinned map[string]bool

	// jsonAsLongText: the source's change stream reads a JSON column as long
	// TEXT (MariaDB, whose JSON is a LONGTEXT alias — see
	// [translate.ProjectsJSONAsLongText]), so a long-TEXT snapshot column
	// against a JSON target column is equal. Scoped to that source and that
	// direction so a real JSON ⇄ TEXT change on any other source is still
	// seen.
	jsonAsLongText bool

	// priorExpected is the shape the source held immediately before this
	// boundary — the stream's retained history version, never one a replay
	// precedes ([firstBoundaryWitness.historyPriorAt]) — rendered as target
	// storage like the snapshot. nil when there is none. With it a target
	// WIDER than the snapshot is no longer ambiguous: where the source
	// narrowed the column ([sourceNarrowed]) and the target still holds the
	// source's old type, the narrowing is forwarded as a live one would be;
	// where the target holds anything else, it refuses (GC-44 F5 third
	// review).
	priorExpected *ir.Table
}

// sourceNarrowed reports whether a column whose type was prior is now
// snapshot, a type that cannot hold every value prior could: the source
// narrowed it, and its own ALTER converted (rounded, truncated, relabelled)
// the values it stores. Any family [witnessWidthOrder] orders counts —
// temporal precision, decimal on either axis, character length, float
// width, integer width, an enum/set label removed, numeric → integer.
func sourceNarrowed(prior, snapshot ir.Type) bool {
	return witnessWidthOrder(prior, snapshot) == targetWider
}

// sameStorage reports whether two lens-applied types store the same
// values: equal, or two spellings of one storage.
func sameStorage(a, b ir.Type) bool {
	return reflect.DeepEqual(a, b) || witnessWidthOrder(a, b) == sameWidth
}

// priorColumns indexes the prior shape's lens-applied columns by name (nil
// when there is no prior).
func priorColumns(priorExpected *ir.Table) map[string]*ir.Column {
	if priorExpected == nil {
		return nil
	}
	return columnsByNameIR(witnessCompareTable(priorExpected, nil))
}

// historyFor returns this stream's retained version of the table named
// name, or nil.
func (w *firstBoundaryWitness) historyFor(name string) *ir.Table {
	for _, t := range w.history {
		if t != nil && t.Name == name {
			return t
		}
	}
	return nil
}

// historyPriorAt is the table's retained version as PROOF of the shape the
// source held before the boundary at pos — the evidence the forward path's
// first boundary needs before it forwards a narrowing ([sourceNarrowed]) —
// or nil when there is none, or when nothing proves the boundary is not a
// REPLAY.
//
// The replay is why. A Postgres transaction that writes, ALTERs a column
// and writes again is re-delivered from its start after a crash or a
// restart, pre-ALTER relation first, while the history already holds the
// post-ALTER version. Read against it the replay looks exactly like the
// source NARROWING the column, and forwarding that narrows a target that is
// exactly right (measured on postgres:16 by the crash cells: the forwarded
// `numeric(12,4)` was narrowed back to `(10,2)`, and a forwarded `text`
// narrowed to `varchar(16)` failed 22001 on every retry).
//
// So the proof here is strict: the boundary must lie STRICTLY AFTER the
// version's anchor under the source's own order ([provenAfter]). That is
// the whole test — a boundary proven after the version shows a change the
// source made since, whatever shape it shows, including a shape the table
// held before (the GC-44 fourth review's narrow-back: a MySQL `(12,4)` →
// `(10,2)` made while the stream was stopped, against a history that also
// held the older `(10,2)`, was read as "a recorded shape, possibly a
// replay" and kept with the WARN — a replay cannot lie after the version
// it replays past). A MySQL source proves it (a GTID set or a binlog
// file/position; the first-touch boundary is anchored at the stream's
// start, past every version it resolved). A Postgres source cannot: its
// boundaries are anchored at LSN 0/0 (the relation message carries no WAL
// position), which also makes every version of a table share one history
// key, so the history holds only the latest shape. A narrowing made while a
// Postgres stream was stopped is therefore kept with the WARN, as before
// (GC-44 F23). The unforwarded check, whose outcome is a refusal rather
// than an ALTER, judges with the weaker [acceptedBoundary.evidenceFor] and
// marks what this test cannot prove AMBIGUOUS.
func (w *firstBoundaryWitness) historyPriorAt(name string, pos ir.Position) *ir.Table {
	if w == nil {
		return nil
	}
	h := w.historyFor(name)
	if h == nil || !provenAfter(w.orderer, pos, w.retained[h].anchor) {
		return nil
	}
	return h
}

// provenAfter reports whether position p is PROVEN strictly after anchor.
func provenAfter(orderer ir.PositionOrderer, p, anchor ir.Position) bool {
	return provenBefore(orderer, anchor, p)
}

// recordedShape reports whether post has the same columns — names and
// lens-applied types ([witnessCompareTable]) — as any of shapes.
func recordedShape(shapes []*ir.Table, post *ir.Table) bool {
	if post == nil {
		return false
	}
	p := witnessCompareTable(post, nil)
	for _, s := range shapes {
		if s == nil {
			continue
		}
		if len(irdiff.TableColumnShapeWithOptions(witnessCompareTable(s, nil), p, irdiff.ShapeCompareOptions{ColumnTypesOnly: true})) == 0 {
			return true
		}
	}
	return false
}

// provenBefore reports whether position p is PROVEN strictly before anchor:
// the anchor is at or after p and p is not at or after the anchor. Anything
// the orderer cannot answer is false.
func provenBefore(orderer ir.PositionOrderer, p, anchor ir.Position) bool {
	if orderer == nil || p.Token == "" || anchor.Token == "" {
		return false
	}
	anchorAtOrAfter, err := orderer.PositionAtOrAfter(anchor, p)
	if err != nil || !anchorAtOrAfter {
		return false
	}
	pAtOrAfter, err := orderer.PositionAtOrAfter(p, anchor)
	return err == nil && !pAtOrAfter
}

// options builds the witnessOptions for post.
func (w *firstBoundaryWitness) options(post *ir.Table) witnessOptions {
	opts := witnessOptions{jsonAsLongText: translate.ProjectsJSONAsLongText(w.sourceEngine)}
	for _, m := range w.mappings {
		if m.Table == post.Name {
			if opts.pinned == nil {
				opts.pinned = map[string]bool{}
			}
			opts.pinned[m.Column] = true
		}
	}
	return opts
}

// classifyWitness is the pure comparison: expected (the snapshot rendered
// as target storage) against target (the catalog's read-back), both seen
// through [witnessCompareType]. Columns are matched by name; a target
// column with a generation expression and no expected counterpart is not
// a divergence (the Postgres change stream never carries generated
// columns, and the target derives them itself).
func classifyWitness(expected, target *ir.Table, opts witnessOptions) witnessVerdict {
	exp := witnessCompareTable(expected, nil)
	act := witnessCompareTable(target, exp)
	priorCols := priorColumns(opts.priorExpected)
	// An overridden column's type is not compared below (the lens equates
	// it); a source narrowing on one is still a change no first boundary
	// can forward — the forward path does not apply overrides — so it
	// refuses, judged on the source's own types before the lens equates
	// them.
	var narrowedPinned []witnessColumnDiff
	for _, c := range exp.Columns {
		if pc, ok := priorCols[c.Name]; ok && opts.pinned[c.Name] && sourceNarrowed(pc.Type, c.Type) {
			narrowedPinned = append(narrowedPinned, witnessColumnDiff{
				column: c.Name, source: fmt.Sprintf("%s (narrowed from %s)", c.Type, pc.Type), target: "(--type-override)",
			})
		}
	}
	reconcilePairs(exp, act, opts)
	mismatches := irdiff.TableColumnShapeWithOptions(exp, act, irdiff.ShapeCompareOptions{ColumnTypesOnly: true})
	if len(narrowedPinned) > 0 {
		return witnessVerdict{kind: witnessRefuse, diffs: narrowedPinned}
	}
	if len(mismatches) == 0 {
		return witnessVerdict{kind: witnessMatch}
	}
	expCols, actCols := columnsByNameIR(exp), columnsByNameIR(act)
	rawTarget := columnsByNameIR(target)
	var v witnessVerdict
	var typeDiffs []string
	for _, m := range mismatches {
		v.diffs = append(v.diffs, witnessColumnDiff{column: m.Column, source: m.Expected, target: m.Actual})
		_, inExp := expCols[m.Column]
		_, inAct := actCols[m.Column]
		switch {
		case inExp && !inAct:
			v.added = append(v.added, m.Column)
		case !inExp && inAct:
			v.targetOnly = append(v.targetOnly, m.Column)
		default:
			typeDiffs = append(typeDiffs, m.Column)
		}
	}
	// Report added columns in the snapshot's own order, so a forwarded
	// ADD COLUMN lands them as the source declared them.
	v.added = inTableOrder(expected, v.added)
	switch {
	case len(v.added) > 0 && len(v.targetOnly) == 0 && len(typeDiffs) == 0:
		v.kind = witnessForwardAdd
	case len(v.targetOnly) > 0 && len(v.added) == 0 && len(typeDiffs) == 0:
		v.kind = witnessTargetOnly
	case len(typeDiffs) == 1 && len(v.added) == 0 && len(v.targetOnly) == 0:
		name := typeDiffs[0]
		v.altered = name
		v.targetType = rawTarget[name].Type
		if sessionZoneSiblingSwap(actCols[name].Type, expCols[name].Type) {
			v.kind = witnessForwardAlter
			break
		}
		switch witnessWidthOrder(actCols[name].Type, expCols[name].Type) {
		case targetNarrower:
			v.kind = witnessForwardAlter
		case targetWider:
			v.kind = witnessTargetWider
			pc, known := priorCols[name]
			if !known || !sourceNarrowed(pc.Type, expCols[name].Type) {
				break
			}
			// The retained history proves the source held the wider type
			// and narrowed it. Where the target still holds that type, the
			// narrowing is the source's own change the stream missed:
			// forward it, as the live path would have. Where the target
			// holds anything else (widened past the source on purpose) the
			// rows it kept were never converted the way the source's were
			// and no first boundary can say which type the operator wants.
			v.narrowedFrom = pc.Type
			v.diffs = narrowedDiffs(v.diffs, name, pc.Type)
			if sameStorage(actCols[name].Type, pc.Type) && forwardableAlter(pc.Type) {
				v.kind = witnessForwardAlter
			} else {
				v.kind = witnessRefuse
			}
		case sameWidth:
			// Two spellings of one storage (a bare temporal against its
			// engine default): nothing to do.
			return witnessVerdict{kind: witnessMatch}
		default:
			v.kind = witnessRefuse
		}
	default:
		v.kind = witnessRefuse
	}
	return v
}

// widthOrder is how the target's column compares with the snapshot's
// rendering of it, within one allowlisted family.
type widthOrder int

const (
	// notComparable — across families with no "holds every value" relation
	// ([targetHoldsEverySnapshotValue]), a sign change, or a zone-kind
	// change: refuse.
	notComparable widthOrder = iota
	// targetNarrower — the source widened the column: forward the ALTER.
	targetNarrower
	// targetWider — the target already holds MORE than the snapshot says.
	targetWider
	// mixedWidth — wider on one axis and narrower on another (a decimal
	// whose precision grew and scale shrank): refuse.
	mixedWidth
	// sameWidth — the two differ only in spelling.
	sameWidth
)

// witnessWidthOrder is the allowlist, with its direction. The families are
// those whose values the target's own ALTER converts the way the source's
// did — temporal precision (zone kind unchanged), decimal precision/scale,
// character length, float width, integer width (sign unchanged); anything
// else is notComparable and refuses: with no observed pre-state, a change
// across families is as likely a rename or a hand-altered target as a
// source DDL, and guessing wrong is silent.
//
// DIRECTION IS LOAD-BEARING (GC-44 review). Only a target NARROWER than the
// snapshot is forwarded. A target WIDER than the snapshot is never
// narrowed, because the first boundary cannot tell an old shape from a new
// one: a target an operator widened on purpose would be narrowed, changing
// every later value that needs the wider type (TestTWFB_OperatorWidenedTarget_*
// measures it), and a Postgres replay after a crash in the middle of a
// transaction `UPDATE; ALTER c numeric(12,4); INSERT …` starts with the
// PRE-ALTER relation while the target already holds the widened column and
// rows written under it (TestTWFB_PostgresCrashMidTransaction_KeepsTheWiderTarget
// pins that it converges; there the narrowing was healed by the next
// relation and the rows' re-delivery, on every apply configuration measured,
// but a narrowing that cannot hold those rows — a varchar or int — refuses
// with 22001/22003 instead). A genuine source NARROWING is
// therefore not forwarded on this order alone (WARNed instead); it is
// forwarded only where the retained history PROVES it
// ([firstBoundaryWitness.historyPriorAt], in [classifyWitness]), and the
// CDC→CDC path, which sees both sides, still forwards one seen live.
//
// ACROSS families the order is one-sided: [targetHoldsEverySnapshotValue]
// can only answer targetWider. A source change across families made while
// the stream was stopped is still not forwarded (refused, as before), but a
// target that already holds the wider family — the replay of a forwarded
// varchar → text, say — is kept instead of refusing every start.
func witnessWidthOrder(target, snapshot ir.Type) widthOrder {
	if o := sameFamilyWidthOrder(target, snapshot); o != notComparable {
		return o
	}
	if targetHoldsEverySnapshotValue(target, snapshot) {
		return targetWider
	}
	return notComparable
}

// targetHoldsEverySnapshotValue is the cross-family "target is wider"
// relation: every value a column of the snapshot's type can carry through
// the change stream is stored by the target's type unchanged, so keeping
// the target loses nothing. Each arm states its value-fidelity argument;
// anything not listed is not wider.
//
//   - varchar(n) ⊂ text whose byte capacity covers n characters at 4 bytes
//     each (utf8mb4's worst case; a Postgres text is unbounded). Both keep
//     a value's trailing spaces, so nothing is trimmed.
//   - an integer ⊂ a decimal with a non-negative scale whose integer digits
//     (precision − scale) hold the type's widest value (int8: 3, int16: 5,
//     int24: 7 signed / 8 unsigned, int32: 10, int64: 19 signed / 20
//     unsigned), or an unconstrained decimal. Such a decimal stores an
//     integer exactly; a negative scale rounds it ([decimalHoldsIntegerDigits]).
//   - an unsigned integer ⊂ a signed integer strictly wider (uint32 ⊂ int64
//     holds 0 … 2³²−1).
//   - enum labels ⊂ a strict superset of them: the stream carries the
//     LABEL, not the ordinal, and an enum stores the label it is given.
//   - set labels ⊂ a strict superset that keeps them in the same relative
//     order: a SET renders a value in its declaration order, so a reordered
//     superset reads 'a,b' back as 'b,a'.
//
// CHAR is deliberately NOT here, in either arm it once had (char(n) ⊂
// varchar(m ≥ n), char(n) ⊂ text — GC-44 F5 third review). The values do
// not survive the way those arms claimed: a Postgres source sends bpchar
// PADDED to n ('ab' as 'ab   '), while the target's rows were written
// before the change — by a char → varchar cast, which strips the padding,
// or by the copy of a varchar column — so the target holds 'ab' and the
// stream sends 'ab   '. A key-scoped UPDATE or DELETE then matches no row
// and an INSERT lands padded (measured on postgres:16: a varchar(5) key
// changed to char(5) while the stream was stopped). A MySQL source sends
// CHAR stripped, but nothing here proves which lane a type came from, so
// CHAR across families refuses and the drained model converges it.
//
// json ⊂ jsonb is deliberately NOT here: jsonb normalizes (duplicate keys
// collapse to the last, whitespace and key order are rewritten), so it does
// not hold every json value unchanged. Neither is int32 ⊂ double (exact,
// but the float family's own rendering is not this relation's to vouch
// for) — both stay notComparable and refuse.
func targetHoldsEverySnapshotValue(target, snapshot ir.Type) bool {
	switch s := snapshot.(type) {
	case ir.Varchar:
		if t, ok := target.(ir.Text); ok {
			return textCovers(t.Size, s.Length)
		}
	case ir.Integer:
		switch t := target.(type) {
		case ir.Decimal:
			return decimalHoldsIntegerDigits(t, integerDigits(s))
		case ir.Integer:
			return s.Unsigned && !t.Unsigned && t.Width > s.Width
		}
	case ir.Enum:
		if t, ok := target.(ir.Enum); ok {
			return strictLabelSuperset(t.Values, s.Values, false)
		}
	case ir.Set:
		if t, ok := target.(ir.Set); ok {
			return strictLabelSuperset(t.Values, s.Values, true)
		}
	}
	return false
}

// decimalHoldsIntegerDigits reports whether d stores every integer of up to
// digits decimal digits unchanged: unconstrained, or a NON-NEGATIVE scale
// with at least digits integer digits. A negative scale (Postgres 15+
// `numeric(5,-2)`) rounds to a power of ten — `numeric(5,-2)` stores 1234
// as 1200 — so however many integer digits precision − scale counts, it
// holds no integer type (GC-44 F5 third review).
func decimalHoldsIntegerDigits(d ir.Decimal, digits int) bool {
	return d.Unconstrained || (d.Scale >= 0 && d.Precision-d.Scale >= digits)
}

// textCovers reports whether a TEXT of size holds n characters at 4 bytes
// each.
func textCovers(size ir.TextSize, n int) bool {
	var capacity int64
	switch size {
	case ir.TextTiny:
		capacity = 255
	case ir.TextRegular:
		capacity = 65_535
	case ir.TextMedium:
		capacity = 16_777_215
	case ir.TextLong:
		capacity = 4_294_967_295
	}
	return n > 0 && int64(n)*4 <= capacity
}

// integerDigits is the number of decimal digits the integer type's widest
// value needs.
func integerDigits(i ir.Integer) int {
	switch i.Width {
	case 8:
		return 3
	case 16:
		return 5
	case 24:
		if i.Unsigned {
			return 8
		}
		return 7
	case 32:
		return 10
	}
	if i.Unsigned {
		return 20
	}
	return 19
}

// strictLabelSuperset reports whether target carries every label of
// snapshot and at least one more. Both sides must know their labels (the
// lens equates an enum whose labels one side does not know).
//
// ordered additionally requires snapshot's labels to appear in target in
// the SAME relative order — the SET case. A MySQL SET stores a bitmask over
// its declared labels and renders a value in DECLARATION order, so 'a,b'
// written into SET('b','a','c') reads back 'b,a': a different string from
// the one the source holds, though every label survives (GC-44 F5 third
// review, L2). An ENUM stores one label, which renders as written whatever
// the order.
func strictLabelSuperset(target, snapshot []string, ordered bool) bool {
	if len(snapshot) == 0 || len(target) <= len(snapshot) {
		return false
	}
	pos := make(map[string]int, len(target))
	for i, v := range target {
		pos[v] = i
	}
	last := -1
	for _, v := range snapshot {
		i, ok := pos[v]
		if !ok || (ordered && i < last) {
			return false
		}
		last = i
	}
	return true
}

// sameFamilyWidthOrder is [witnessWidthOrder] within one family.
func sameFamilyWidthOrder(target, snapshot ir.Type) widthOrder {
	switch s := snapshot.(type) {
	case ir.DateTime:
		t, ok := target.(ir.DateTime)
		if !ok {
			return notComparable
		}
		return orderInts(effectivePrecision(t.Precision, t.PrecisionUnspecified), effectivePrecision(s.Precision, s.PrecisionUnspecified))
	case ir.Time:
		t, ok := target.(ir.Time)
		if !ok || t.WithTimeZone != s.WithTimeZone {
			return notComparable
		}
		return orderInts(effectivePrecision(t.Precision, t.PrecisionUnspecified), effectivePrecision(s.Precision, s.PrecisionUnspecified))
	case ir.Timestamp:
		t, ok := target.(ir.Timestamp)
		if !ok || t.WithTimeZone != s.WithTimeZone {
			return notComparable
		}
		return orderInts(effectivePrecision(t.Precision, t.PrecisionUnspecified), effectivePrecision(s.Precision, s.PrecisionUnspecified))
	case ir.Decimal:
		t, ok := target.(ir.Decimal)
		if !ok {
			return notComparable
		}
		return orderDecimals(t, s)
	case ir.Varchar:
		t, ok := target.(ir.Varchar)
		if !ok {
			return notComparable
		}
		return orderInts(t.Length, s.Length)
	case ir.Char:
		t, ok := target.(ir.Char)
		if !ok {
			return notComparable
		}
		return orderInts(t.Length, s.Length)
	case ir.Float:
		t, ok := target.(ir.Float)
		if !ok {
			return notComparable
		}
		return orderInts(floatWidth(t.Precision), floatWidth(s.Precision))
	case ir.Integer:
		t, ok := target.(ir.Integer)
		if !ok || t.Unsigned != s.Unsigned {
			return notComparable
		}
		return orderInts(int(t.Width), int(s.Width))
	case ir.Array:
		t, ok := target.(ir.Array)
		if !ok || t.Element == nil || s.Element == nil {
			return notComparable
		}
		// An array is ordered by its element — the change stream carries
		// the element's modifier since GC-44 F5's third review (the
		// Postgres projection threads the column typmod onto it). A target
		// element WIDER is kept like any wider column; a NARROWER one is
		// not forwarded (no first-boundary path ALTERs an array column's
		// element, and the live path refuses that change at the reader),
		// so it refuses as mixed.
		o := sameFamilyWidthOrder(t.Element, s.Element)
		switch {
		case o == targetNarrower:
			return mixedWidth
		case o == notComparable && arrayElementHoldsEverySnapshotValue(t.Element, s.Element):
			return targetWider
		}
		return o
	}
	return notComparable
}

// arrayElementHoldsEverySnapshotValue is the cross-family "target is wider"
// relation for an array ELEMENT, deliberately narrower than the scalar
// [targetHoldsEverySnapshotValue]:
//
//   - varchar(n)[] ⊂ text[]: kept. Before GC-44 F5's third review the
//     element modifier was erased on both sides (`varchar(n)` and `char(n)`
//     read as text), so a `varchar(16)[]` source against a `text[]` target
//     matched; threading the modifier made it notComparable and refused that
//     stream on every start (GC-44 fourth review, LOW, loud). Every element
//     value a varchar(n) carries is stored by text unchanged, trailing spaces
//     included — the scalar arm's argument, element by element.
//   - char(n)[] ⊂ text[]: NOT kept — refused (GC-44 F25). A Postgres source
//     sends a bpchar element PADDED to n, while a text[] target written
//     before the change (by a cast, or by the copy of a varchar[] column)
//     holds it stripped: the scalar CHAR finding of the third review, per
//     element. It matched before the third review, so this is a loud
//     regression for a char(n)[] source against a text[] target; the drained
//     model (`ALTER … TYPE char(n)[]` on the target, or `text[]` on the
//     source) converges it.
//   - every other scalar arm — integer ⊂ decimal, unsigned ⊂ wider signed,
//     enum / set label supersets — is NOT extended to elements: each was
//     notComparable for arrays before the third review too, and an array's
//     encode path is per target-OID (the Bug 74 lesson), which nothing here
//     has measured. They refuse, as they always did.
func arrayElementHoldsEverySnapshotValue(target, snapshot ir.Type) bool {
	s, ok := snapshot.(ir.Varchar)
	if !ok {
		return false
	}
	t, ok := target.(ir.Text)
	return ok && textCovers(t.Size, s.Length)
}

// forwardableAlter reports whether a first boundary may forward an ALTER
// COLUMN TYPE on a column of type t. Array columns are never altered there
// (see the Array arm of [sameFamilyWidthOrder]).
func forwardableAlter(t ir.Type) bool {
	_, isArray := t.(ir.Array)
	return !isArray
}

// orderInts orders a target measure against a snapshot measure.
func orderInts(target, snapshot int) widthOrder {
	switch {
	case target < snapshot:
		return targetNarrower
	case target > snapshot:
		return targetWider
	}
	return sameWidth
}

// effectivePrecision is a temporal's stored precision: an unspecified one
// is the engine default, 6, on every engine that leaves it unspecified
// (Postgres; a MySQL target's bare precision is materialized by the
// compare lane before it gets here).
func effectivePrecision(p int, unspecified bool) int {
	if unspecified {
		return 6
	}
	return p
}

// floatWidth ranks the float widths.
func floatWidth(p ir.FloatPrecision) int {
	if p == ir.FloatDouble {
		return 2
	}
	return 1
}

// orderDecimals orders two decimals on BOTH axes a value needs — the
// integer digits (precision − scale) and the scale. An unconstrained
// decimal is unbounded on both. Narrower only when the target is no wider
// on either axis; mixed when the axes disagree.
func orderDecimals(target, snapshot ir.Decimal) widthOrder {
	const unbounded = 1 << 30
	axes := func(d ir.Decimal) (intDigits, scale int) {
		if d.Unconstrained {
			return unbounded, unbounded
		}
		return d.Precision - d.Scale, d.Scale
	}
	ti, ts := axes(target)
	si, ss := axes(snapshot)
	io, so := orderInts(ti, si), orderInts(ts, ss)
	switch {
	case io == so:
		return io
	case io == sameWidth:
		return so
	case so == sameWidth:
		return io
	}
	return mixedWidth
}

// witnessCompareTable copies t's columns through [witnessCompareType] for
// the comparison. With expected non-nil (the target side), a generated
// column the expected side does not carry is dropped — see
// [classifyWitness]. Name and columns only; nothing else is compared.
func witnessCompareTable(t, expected *ir.Table) *ir.Table {
	out := &ir.Table{Schema: t.Schema, Name: t.Name}
	var expCols map[string]*ir.Column
	if expected != nil {
		expCols = columnsByNameIR(expected)
	}
	for _, c := range t.Columns {
		if c == nil {
			continue
		}
		if expected != nil && c.GeneratedExpr != "" {
			if _, carried := expCols[c.Name]; !carried {
				continue
			}
		}
		nc := ir.Column{Name: c.Name, Type: witnessCompareType(c.Type)}
		out.Columns = append(out.Columns, &nc)
	}
	return out
}

// witnessCompareType is the comparison lens, applied to BOTH sides. Each arm
// erases a difference measured between a change-stream projection and a
// target read-back of a column the stream itself created. All but one
// leave what a value can hold intact:
//
//   - Integer.AutoIncrement: catalog-only; no change stream carries it.
//   - character charset/collation: pgoutput and VStream carry neither, and
//     across engines it is translation, not drift. UNVERIFIED PREMISE —
//     this arm is the exception: a collation change is not storage-neutral
//     everywhere (a `_ci` → `_bin` change on a MySQL key column changes
//     which values collide), and a stopped-time collation change is
//     therefore not seen at a boundary. Filed as GC-44 F17.
//   - a constraint-free Decimal{0,0} is the legacy unconstrained spelling.
//   - Geometry: the subtype and Z/M flags are erased (the change streams
//     do not carry them reliably); geometry-vs-geography is kept, and so is
//     a positive SRID, which [reconcilePairs] compares when BOTH sides
//     declare one (both lanes report an SRID they cannot establish as 0).
//   - Enum: the type name is a Postgres catalog detail; pgoutput sends a
//     bare OID, so its labels are unknown too, and
//     [reconcilePairs] compares labels only when both sides carry
//     them.
//   - Domain: pgoutput unwraps it to its base type; the catalog reads the
//     wrapper. Compared through the storage type.
//   - Array: compared through its element, modifier included. The
//     Postgres projection threads the column's typmod onto the element
//     (GC-44 F5 third review); before it the element was resolved at typmod
//     -1 and erased here on both sides, so `numeric(10,2)[]` →
//     `numeric(10,4)[]` made while the stream was stopped was never seen
//     (the reader's mid-stream gate refuses that change only for a relation
//     it has already cached in the same stream).
//   - JSON vs long TEXT on a MariaDB source, and overridden columns:
//     pairwise, in [reconcilePairs].
func witnessCompareType(t ir.Type) ir.Type {
	switch v := t.(type) {
	case ir.Integer:
		v.AutoIncrement = false
		return v
	case ir.Char:
		v.Charset, v.Collation, v.Determinism = "", "", ir.CollationDeterminismUnknown
		return v
	case ir.Varchar:
		v.Charset, v.Collation, v.Determinism = "", "", ir.CollationDeterminismUnknown
		return v
	case ir.Text:
		v.Charset, v.Collation, v.Determinism = "", "", ir.CollationDeterminismUnknown
		return v
	case ir.Decimal:
		if v.Unconstrained || (v.Precision == 0 && v.Scale == 0) {
			return ir.Decimal{Unconstrained: true}
		}
		return v
	case ir.Geometry:
		return ir.Geometry{IsGeography: v.IsGeography, SRID: max(v.SRID, 0)}
	case ir.Enum:
		return ir.Enum{Values: v.Values}
	case ir.Domain:
		base := ir.UnwrapDomain(v)
		if _, still := base.(ir.Domain); still {
			return v
		}
		return witnessCompareType(base)
	case ir.Array:
		if v.Element != nil {
			v.Element = witnessCompareType(v.Element)
		}
		return v
	}
	return t
}

// reconcilePairs applies the lens rules that need BOTH sides of a column at
// once:
//
//   - an overridden column ([witnessOptions.pinned]) is not compared.
//   - an enum whose labels one side does not know (the pgoutput projection:
//     a bare OID) equals the other side's enum; labels are compared only
//     when both sides carry them.
//   - on a source whose change stream reads JSON as long TEXT
//     ([witnessOptions.jsonAsLongText] — MariaDB), a long-TEXT snapshot
//     column equals a JSON target column. MariaDB's JSON is a LONGTEXT
//     alias with an auto json_valid CHECK; its SchemaReader recovers the
//     JSON identity from that CHECK, but the binlog boundary projection
//     reads the column as LONGTEXT, so every MariaDB JSON column read as a
//     type change against the target the cold start created (measured by
//     TestStreamer_MariaDBToPostgres: a RESUME-SCHEMA-DIVERGENCE on every
//     resume). The residual, stated: on a MariaDB source a JSON ⇄ LONGTEXT
//     change made while the stream was stopped is not seen at the first
//     boundary. Every other source still compares JSON and TEXT as
//     different families.
//   - a geometry column's SRID is compared only when BOTH sides declare a
//     positive one. pgoutput carries no SRID, and both readers report one
//     they cannot establish as 0 ([ir.GeometrySRIDUnknown] contained), so
//     0 is "unknown" here; a positive SRID on each side that differs is a
//     real change (the Postgres applier re-stamps the TARGET's SRID on
//     every row, so new rows would carry the old label — GC-44 F5 third
//     review). The residual, stated: a stopped-time SRID change on a
//     Postgres source, or to or from a MySQL column with no declared SRID,
//     is not seen (GC-44 F16) — refusing whenever the target alone knows
//     the SRID would refuse every Postgres geometry stream on every start.
func reconcilePairs(exp, act *ir.Table, opts witnessOptions) {
	actCols := columnsByNameIR(act)
	for _, e := range exp.Columns {
		a, ok := actCols[e.Name]
		if !ok {
			continue
		}
		if opts.pinned[e.Name] {
			e.Type = a.Type
			continue
		}
		ee, eIsEnum := e.Type.(ir.Enum)
		ae, aIsEnum := a.Type.(ir.Enum)
		if eIsEnum && aIsEnum && (len(ee.Values) == 0 || len(ae.Values) == 0) {
			e.Type, a.Type = ir.Enum{}, ir.Enum{}
			continue
		}
		if opts.jsonAsLongText && isLongText(e.Type) {
			if _, targetJSON := a.Type.(ir.JSON); targetJSON {
				e.Type = a.Type
			}
		}
		eg, eIsGeom := e.Type.(ir.Geometry)
		ag, aIsGeom := a.Type.(ir.Geometry)
		if eIsGeom && aIsGeom && (eg.SRID == 0 || ag.SRID == 0) {
			eg.SRID, ag.SRID = 0, 0
			e.Type, a.Type = eg, ag
		}
	}
}

// isLongText reports whether t is a long TEXT.
func isLongText(t ir.Type) bool {
	text, ok := t.(ir.Text)
	return ok && text.Size == ir.TextLong
}

// columnsByNameIR indexes t's columns by name.
func columnsByNameIR(t *ir.Table) map[string]*ir.Column {
	out := make(map[string]*ir.Column, len(t.Columns))
	for _, c := range t.Columns {
		if c != nil {
			out[c.Name] = c
		}
	}
	return out
}

// inTableOrder returns names sorted by their position in t.
func inTableOrder(t *ir.Table, names []string) []string {
	pos := make(map[string]int, len(t.Columns))
	for i, c := range t.Columns {
		if c != nil {
			pos[c.Name] = i
		}
	}
	out := append([]string(nil), names...)
	sort.SliceStable(out, func(i, j int) bool { return pos[out[i]] < pos[out[j]] })
	return out
}

// render lists every disagreement for a log line or a refusal.
func (v witnessVerdict) render() string {
	parts := make([]string, 0, len(v.diffs))
	for _, d := range v.diffs {
		parts = append(parts, fmt.Sprintf("column %q: source %s, target %s", d.column, d.source, d.target))
	}
	return strings.Join(parts, "; ")
}

// resumeDivergenceRefusal is the loud outcome: the first boundary disagrees
// with the target in a way no observed pre-state can explain.
func resumeDivergenceRefusal(tableName string, v witnessVerdict, hint string) error {
	return fmt.Errorf(
		"%w: the first schema boundary for %q after this stream (re)started does not match the target table, "+
			"and the difference is not one sluice can forward without knowing what the source changed (%s). "+
			"Refusing before the boundary is recorded or any row after it is applied; nothing has been written. "+
			"Reconcile the target with the source via the drained model; the check runs again at this table's "+
			"first schema boundary on the next start. %s",
		ir.ErrResumeSchemaDivergence, tableName, v.render(), hint,
	)
}

// routeWitnessedBoundary handles a boundary whose pre-state the intercept
// cannot trust — a table's first snapshot per intercept instance, or an
// ALTER COLUMN TYPE classified against the cold-start seed. post is the
// comparison-form snapshot table and snap the raw snapshot. It returns the
// pre-state the boundary was forwarded against (nil when nothing was
// forwarded: a match, a target-only difference, an unwitnessable table),
// so the caller can plan the added-column backfill exactly as it does for
// a CDC→CDC boundary.
func routeWitnessedBoundary(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	post *ir.Table,
	snap ir.SchemaSnapshot,
) (*ir.Table, error) {
	v, err := deps.witness.verdict(ctx, snap.IR, snap.Position)
	if err != nil {
		// Not the divergence marker: nothing has been compared, and a fresh
		// run may read the catalog fine.
		return nil, fmt.Errorf("read the target catalog to check the first schema boundary for %q "+
			"(the boundary is not accepted unchecked): %w", tableName, err)
	}
	switch v.kind {
	case witnessMatch:
		slog.DebugContext(ctx, "schema-forward: first boundary matches the target", "table", tableName)
		return nil, nil
	case witnessUnwitnessed:
		slog.WarnContext(ctx,
			"schema-forward: the target cannot witness this table's first schema boundary; accepting it as the "+
				"baseline — a change made to the source while the stream was stopped is NOT checked for this table",
			"table", tableName, "reason", v.reason)
		return nil, nil
	case witnessTargetOnly:
		slog.WarnContext(ctx,
			"schema-forward: the target holds columns the source no longer has (a DROP COLUMN made while the stream "+
				"was stopped, or a column added on the target by hand); the target keeps them and they receive no "+
				"values from here — drop them on the target if that is intended",
			"table", tableName, "target_only_columns", v.targetOnly)
		return nil, nil
	case witnessTargetWider:
		logTargetWider(ctx, tableName, v)
		return nil, nil
	case witnessRefuse:
		return nil, resumeDivergenceRefusal(tableName, v, forwardRecoveryHint(tableName))
	}
	pre := witnessSynthesizedPre(post, v)
	slog.InfoContext(ctx,
		"schema-forward: the first schema boundary after a (re)start differs from the target; forwarding the "+
			"difference (target-witnessed, GC-44)",
		"table", tableName, "difference", v.render())
	if err := routeForwardBoundary(ctx, deps, tableName, pre, post, snap, false); err != nil {
		return nil, err
	}
	deps.witness.catalog.forget(snap.IR.Name)
	return pre, nil
}

// logTargetWider is the WARN for [witnessTargetWider], shared by both
// intercepts.
func logTargetWider(ctx context.Context, tableName string, v witnessVerdict) {
	slog.WarnContext(ctx,
		"schema-forward: the target column is WIDER than the source's at the first schema boundary after a "+
			"(re)start, and this stream holds no earlier shape of the table that says which side changed; the "+
			"target keeps its type. It may be a replay of the shape BEFORE a change the target already took, or "+
			"a target widened on purpose — or the SOURCE narrowed the column while the stream was stopped, in "+
			"which case its own ALTER converted the rows it stores and the target's copies were not converted: "+
			"compare them, and narrow the target (or re-copy the table) via the drained model if so",
		"table", tableName, "difference", v.render())
}

// witnessSynthesizedPre is the pre-state a witnessed forward is classified
// against: post minus the columns the target lacks, or post with the one
// altered column carrying the target's type.
func witnessSynthesizedPre(post *ir.Table, v witnessVerdict) *ir.Table {
	added := make(map[string]bool, len(v.added))
	for _, n := range v.added {
		added[n] = true
	}
	pre := *post
	pre.Columns = make([]*ir.Column, 0, len(post.Columns))
	for _, c := range post.Columns {
		if c == nil || added[c.Name] {
			continue
		}
		if v.kind == witnessForwardAlter && c.Name == v.altered {
			was := *c
			was.Type = v.targetType
			pre.Columns = append(pre.Columns, &was)
			continue
		}
		pre.Columns = append(pre.Columns, c)
	}
	return &pre
}

// seedBoundaryNeedsWitness reports whether a boundary classified against
// the cold-start seed is an ALTER COLUMN TYPE — the one mutating shape the
// ADR-0091 §3 seed guard no longer skips blind (GC-44 D2). Its phantom (a
// seed-vs-projection fidelity difference) and a genuine widen are
// indistinguishable from the pair alone, and skipping the genuine one
// rounds every following value; the target tells them apart.
func seedBoundaryNeedsWitness(pre, post *ir.Table) bool {
	shape, err := ClassifyShape(pre, post)
	return err == nil && shape.Kind == ShapeKindAlterColumnType
}
