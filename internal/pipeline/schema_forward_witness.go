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
//     narrowing a target that already holds wider values is silent loss.
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
	// persisted position (nil when the warm resume loaded none). Read only
	// by Shape A ([shapeAPeerAddedColumns]).
	history []*ir.Table

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
}

// errWitnessNoTable is returned by [firstBoundaryWitness.expected] when the
// rendering yields no table — defensive; the inputs always carry one.
var errWitnessNoTable = errors.New("rendering the snapshot as target storage produced no table")

// verdict compares the snapshot table post (RAW — the shape the forward
// paths retarget from) against the target's read-back of the same table.
func (w *firstBoundaryWitness) verdict(ctx context.Context, post *ir.Table) (witnessVerdict, error) {
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
	reconcilePairs(exp, act, opts)
	mismatches := irdiff.TableColumnShapeWithOptions(exp, act, irdiff.ShapeCompareOptions{ColumnTypesOnly: true})
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
// therefore not forwarded at a first boundary (WARNed instead); the CDC→CDC
// path, which sees both sides, still forwards one seen live.
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
//   - varchar(n) / char(n) ⊂ text whose byte capacity covers n characters
//     at 4 bytes each (utf8mb4's worst case; a Postgres text is unbounded).
//     Storage keeps trailing spaces in both, so nothing is trimmed.
//   - char(n) ⊂ varchar(m ≥ n). A char value is at most n characters as the
//     stream delivers it — padded to n on Postgres (pgoutput sends the
//     bpchar text), stripped on MySQL — and varchar(m) stores either
//     unchanged. What differs is COMPARISON (bpchar ignores trailing spaces,
//     varchar does not), not the stored value.
//   - an integer ⊂ a decimal whose integer digits (precision − scale) hold
//     the type's widest value (int8: 3, int16: 5, int24: 7 signed / 8
//     unsigned, int32: 10, int64: 19 signed / 20 unsigned), or an
//     unconstrained decimal. A decimal stores an integer exactly.
//   - an unsigned integer ⊂ a signed integer strictly wider (uint32 ⊂ int64
//     holds 0 … 2³²−1).
//   - enum / set labels ⊂ a strict superset of them. The stream carries the
//     LABEL, not the ordinal, so the target's own label order is irrelevant.
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
	case ir.Char:
		switch t := target.(type) {
		case ir.Varchar:
			return t.Length >= s.Length
		case ir.Text:
			return textCovers(t.Size, s.Length)
		}
	case ir.Integer:
		switch t := target.(type) {
		case ir.Decimal:
			return t.Unconstrained || t.Precision-t.Scale >= integerDigits(s)
		case ir.Integer:
			return s.Unsigned && !t.Unsigned && t.Width > s.Width
		}
	case ir.Enum:
		if t, ok := target.(ir.Enum); ok {
			return strictLabelSuperset(t.Values, s.Values)
		}
	case ir.Set:
		if t, ok := target.(ir.Set); ok {
			return strictLabelSuperset(t.Values, s.Values)
		}
	}
	return false
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
func strictLabelSuperset(target, snapshot []string) bool {
	if len(snapshot) == 0 || len(target) <= len(snapshot) {
		return false
	}
	have := make(map[string]bool, len(target))
	for _, v := range target {
		have[v] = true
	}
	for _, v := range snapshot {
		if !have[v] {
			return false
		}
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
	}
	return notComparable
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
// target read-back of a column the stream itself created — never one that
// changes what a value can hold:
//
//   - Integer.AutoIncrement: catalog-only; no change stream carries it.
//   - character charset/collation: pgoutput and VStream carry neither, and
//     across engines it is translation, not drift.
//   - a constraint-free Decimal{0,0} is the legacy unconstrained spelling.
//   - Geometry: the change streams carry geometry-vs-geography and nothing
//     else (pgoutput sends a bare OID; the SRID is recovered at apply).
//   - Enum: the type name is a Postgres catalog detail; pgoutput sends a
//     bare OID, so its labels are unknown too, and
//     [reconcilePairs] compares labels only when both sides carry
//     them.
//   - Domain: pgoutput unwraps it to its base type; the catalog reads the
//     wrapper. Compared through the storage type.
//   - Array: the element is resolved at typmod -1 on the wire, so its
//     modifier is erased on both sides (an array typmod-only ALTER is
//     refused at the Postgres reader before it can reach this).
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
		return ir.Geometry{IsGeography: v.IsGeography}
	case ir.Enum:
		return ir.Enum{Values: v.Values}
	case ir.Domain:
		base := ir.UnwrapDomain(v)
		if _, still := base.(ir.Domain); still {
			return v
		}
		return witnessCompareType(base)
	case ir.Array:
		v.Element = witnessCompareType(eraseWitnessArrayModifier(v.Element))
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
	}
}

// isLongText reports whether t is a long TEXT.
func isLongText(t ir.Type) bool {
	text, ok := t.(ir.Text)
	return ok && text.Size == ir.TextLong
}

// eraseWitnessArrayModifier maps an array element to its modifier-free
// form — what a change stream resolving the element at typmod -1 reads.
func eraseWitnessArrayModifier(t ir.Type) ir.Type {
	switch v := t.(type) {
	case ir.Decimal:
		return ir.Decimal{Unconstrained: true}
	case ir.Char, ir.Varchar:
		return ir.Text{Size: ir.TextLong}
	case ir.DateTime:
		return ir.DateTime{PrecisionUnspecified: true}
	case ir.Time:
		return ir.Time{WithTimeZone: v.WithTimeZone, PrecisionUnspecified: true}
	case ir.Timestamp:
		return ir.Timestamp{WithTimeZone: v.WithTimeZone, PrecisionUnspecified: true}
	}
	return t
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
	v, err := deps.witness.verdict(ctx, snap.IR)
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
			"(re)start; the target keeps its type (a first boundary never narrows a column — it may be a replay "+
			"of the shape BEFORE a change the target already took, or a target widened on purpose). If the source "+
			"really narrowed it, narrow the target yourself via the drained model",
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
