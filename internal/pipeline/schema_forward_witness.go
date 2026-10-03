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
// would store it (mappings, the Shape A discriminator, then
// [translate.RetargetForShapeCompare]) and compares it against the target's
// read-back of the table, column names and types only, through
// [witnessCompareType]'s lens on both sides:
//
//   - equal → the baseline is accepted, as before.
//   - the snapshot carries columns the target lacks, and nothing else →
//     forward ADD COLUMN, with the added-column backfill, against a
//     synthesized pre-state (the snapshot minus those columns).
//   - exactly one shared column differs, within one type family on the
//     allowlist ([witnessForwardableTypeChange]) → forward ALTER COLUMN TYPE
//     against a synthesized pre-state carrying the TARGET's type, so the
//     session-zone door sees the zone family the target actually holds.
//   - the target carries columns the snapshot lacks, and nothing else → a
//     WARN (a DROP COLUMN made while stopped is benign: the target keeps
//     the column, as the drained model would).
//   - anything else — added and dropped together (a possible rename), more
//     than one type change, a change across families — refuses BEFORE the
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
// for one intercept instance. A table absent from the memo re-reads the
// catalog once (a table the target created after the read — a live add),
// and a table this intercept has itself changed is forgotten so its next
// lookup reads the change back. Used only on the intercept's goroutine.
type targetCatalogWitness struct {
	load      targetCatalogLoader
	tables    map[string]*ir.Table
	loaded    bool
	refreshed map[string]bool
}

// newTargetCatalogWitness returns a witness over load. initial, when
// non-nil, is a catalog read already made on this attempt — the SLM-1b
// warm-resume seed read ([Streamer.loadWarmResumeSchemaSeed]) — and saves
// the first read.
func newTargetCatalogWitness(load targetCatalogLoader, initial map[string]*ir.Table) *targetCatalogWitness {
	w := &targetCatalogWitness{load: load, refreshed: map[string]bool{}}
	if initial != nil {
		w.tables, w.loaded = initial, true
	}
	return w
}

// lookup returns the target's table named name (case-insensitive when the
// exact spelling misses and exactly one table folds onto it — a MySQL
// target folding identifier case).
func (w *targetCatalogWitness) lookup(ctx context.Context, name string) (*ir.Table, bool, error) {
	if !w.loaded {
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

// forget drops the memo, so the next lookup reads the catalog again — the
// intercept has just changed the target.
func (w *targetCatalogWitness) forget() {
	w.loaded = false
	w.tables = nil
}

func (w *targetCatalogWitness) read(ctx context.Context) error {
	tables, err := w.load(ctx)
	if err != nil {
		return err
	}
	w.tables, w.loaded = tables, true
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

	sourceEngine string
	targetEngine string

	// mappings are the operator's --type-override entries; a mapped column
	// is held to its override's type, exactly as the cold start created it.
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
	// allowlisted type family (or across the zone-sibling pair, which the
	// forward path's own door refuses with its specific message).
	witnessForwardAlter
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
	return classifyWitness(expected, target), nil
}

// expected renders post as the target would store it — the operator's
// type overrides, the Shape A discriminator, then the compare-lane retarget —
// the same passes, in the same order, the cold start ran before creating
// the table ([Streamer.coldStartPrepareSchema]).
func (w *firstBoundaryWitness) expected(post *ir.Table) (*ir.Table, error) {
	schema := &ir.Schema{Tables: []*ir.Table{post}}
	schema, err := translate.ApplyMappings(schema, mappingsFor(post, w.mappings))
	if err != nil {
		return nil, fmt.Errorf("apply --type-override to %q: %w", post.Name, err)
	}
	if w.shardColumn != "" {
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

// mappingsFor keeps the overrides that name post and a column it still
// carries. [translate.ApplyMappings] refuses an override naming an absent
// column, which is right for a whole schema at cold start and wrong here: a
// column dropped on the source since then is exactly a case this check
// classifies, not an operator typo.
func mappingsFor(post *ir.Table, mappings []config.Mapping) []config.Mapping {
	if len(mappings) == 0 {
		return nil
	}
	cols := make(map[string]bool, len(post.Columns))
	for _, c := range post.Columns {
		if c != nil {
			cols[c.Name] = true
		}
	}
	var out []config.Mapping
	for _, m := range mappings {
		if m.Table == post.Name && cols[m.Column] {
			out = append(out, m)
		}
	}
	return out
}

// classifyWitness is the pure comparison: expected (the snapshot rendered
// as target storage) against target (the catalog's read-back), both seen
// through [witnessCompareType]. Columns are matched by name; a target
// column with a generation expression and no expected counterpart is not
// a divergence (the Postgres change stream never carries generated
// columns, and the target derives them itself).
func classifyWitness(expected, target *ir.Table) witnessVerdict {
	exp := witnessCompareTable(expected, nil)
	act := witnessCompareTable(target, exp)
	reconcilePairs(exp, act)
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
		if witnessForwardableTypeChange(actCols[name].Type, expCols[name].Type) ||
			sessionZoneSiblingSwap(actCols[name].Type, expCols[name].Type) {
			v.kind = witnessForwardAlter
			v.altered = name
			v.targetType = rawTarget[name].Type
		} else {
			v.kind = witnessRefuse
		}
	default:
		v.kind = witnessRefuse
	}
	return v
}

// witnessForwardableTypeChange is the allowlist: the type changes the first
// boundary forwards when only the target can say what the column was. Each
// stays within one family whose values the target's own ALTER converts the
// way the source's did — temporal precision (zone kind unchanged), decimal
// precision/scale, character length, float width, integer width (sign
// unchanged). Everything else refuses: with no observed pre-state, a change
// across families is as likely a rename, an override, or a target someone
// altered by hand as a source DDL, and guessing wrong is silent.
func witnessForwardableTypeChange(from, to ir.Type) bool {
	switch t := to.(type) {
	case ir.DateTime:
		_, ok := from.(ir.DateTime)
		return ok
	case ir.Time:
		f, ok := from.(ir.Time)
		return ok && f.WithTimeZone == t.WithTimeZone
	case ir.Timestamp:
		f, ok := from.(ir.Timestamp)
		return ok && f.WithTimeZone == t.WithTimeZone
	case ir.Decimal:
		_, ok := from.(ir.Decimal)
		return ok
	case ir.Varchar:
		_, ok := from.(ir.Varchar)
		return ok
	case ir.Char:
		_, ok := from.(ir.Char)
		return ok
	case ir.Float:
		_, ok := from.(ir.Float)
		return ok
	case ir.Integer:
		f, ok := from.(ir.Integer)
		return ok && f.Unsigned == t.Unsigned
	}
	return false
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
//   - JSON vs long TEXT (MariaDB's JSON alias): pairwise, in
//     [reconcilePairs].
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

// reconcilePairs applies the two lens rules that need BOTH sides of a
// column at once:
//
//   - an enum whose labels one side does not know (the pgoutput projection:
//     a bare OID) equals the other side's enum; labels are compared only
//     when both sides carry them.
//   - JSON on one side and a long TEXT on the other are equal. MariaDB's
//     JSON is a LONGTEXT alias with an auto json_valid CHECK; its
//     SchemaReader recovers the JSON identity from that CHECK, but the
//     binlog boundary projection reads the column as LONGTEXT, so every
//     MariaDB JSON column read as a type change against the target the
//     cold start created (measured by TestStreamer_MariaDBToPostgres: a
//     RESUME-SCHEMA-DIVERGENCE on every resume). What the rule stops
//     seeing is a source JSON ⇄ LONGTEXT change made while the stream was
//     stopped. A text target holds a JSON document as written, and a JSON
//     target refuses a non-JSON text loudly — but a jsonb target re-renders
//     valid JSON text (whitespace, key order). That residual is accepted
//     and stated: it is narrower than the phantom refusal on every MariaDB
//     JSON column it replaces, and before GC-44 the change was not seen
//     either.
func reconcilePairs(exp, act *ir.Table) {
	actCols := columnsByNameIR(act)
	for _, e := range exp.Columns {
		a, ok := actCols[e.Name]
		if !ok {
			continue
		}
		ee, eIsEnum := e.Type.(ir.Enum)
		ae, aIsEnum := a.Type.(ir.Enum)
		if eIsEnum && aIsEnum && (len(ee.Values) == 0 || len(ae.Values) == 0) {
			e.Type, a.Type = ir.Enum{}, ir.Enum{}
			continue
		}
		if jsonOrLongText(e.Type, a.Type) || jsonOrLongText(a.Type, e.Type) {
			e.Type, a.Type = ir.Text{Size: ir.TextLong}, ir.Text{Size: ir.TextLong}
		}
	}
}

// jsonOrLongText reports whether x is JSON and y a long TEXT.
func jsonOrLongText(x, y ir.Type) bool {
	_, xJSON := x.(ir.JSON)
	yText, yIsText := y.(ir.Text)
	return xJSON && yIsText && yText.Size == ir.TextLong
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
			"Reconcile the target with the source via the drained model; every start repeats this check. %s",
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
	deps.witness.catalog.forget()
	return pre, nil
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
