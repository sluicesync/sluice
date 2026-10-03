// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The unforwarded-stream boundary check (GC-44 F5).
//
// # The gap
//
// Three stream shapes forward no source DDL: `--schema-changes=refuse`, a
// multi-database stream (ADR-0091 forwarding is single-database only), and
// `--inject-shard-column` with `--no-coordinate-live-ddl` (Shape A's drained
// model). [Streamer.phaseWireInterceptChain] wired no schema-snapshot
// intercept for any of them, so a boundary went straight to the applier,
// which records it as ADR-0049 history and moves on. Nothing refused. The
// Postgres reader's own refuse-mode gate (checkSchemaRace) refuses a shape
// change only against a relation it cached earlier in the SAME stream, so a
// change made while the stream was stopped arrives as a first-touch relation
// and passes; the binlog reader has no such gate at all. So a type widen
// within its family — `DATETIME` → `DATETIME(6)`, `numeric(10,2)` →
// `numeric(10,4)` — left the target's narrow column in place and every
// following value was rounded into it at exit 0 (measured on MySQL 8 refuse
// mode, MySQL 8 multi-database, and Postgres 16 refuse mode). The help text
// and the multi-database log line both said such a stream refuses.
//
// # The check
//
// Every boundary on such a stream is compared with the target table the
// rows after it will be written into — the same rendering and lens as the
// target-witnessed first boundary (schema_forward_witness.go:
// [firstBoundaryWitness.expected], [witnessCompareTable], [reconcilePairs])
// — and judged column by column ([judgeUnforwardedColumns]):
//
//   - a column the source has and the target lacks refuses: its values
//     would be dropped (or the row refused at apply);
//   - a column whose type differs refuses UNLESS the target's type holds
//     every value of the source's — the forward path's own direction rule,
//     [witnessWidthOrder], placing the target at or above the source within
//     one family — which passes with
//     a WARN. That is the drained model run ahead of the source, which
//     these modes exist for — and it is also what a Postgres replay shows:
//     a transaction that wrote rows, altered the table and wrote again
//     re-delivers its pre-ALTER relation first, against a target that
//     already took the change;
//   - a column only the target has is a WARN, as on the forward path;
//   - a --type-override column, whose type the lens does not compare, is
//     judged on width alone ([overriddenColumnRefused]).
//
// A refusal carries [ir.ErrSchemaChangeRefused] (SCHEMA-CHANGE-REFUSED) and
// stops the stream BEFORE the boundary goes downstream, so no history row
// is written and no row after it is applied. Before refusing, the catalog
// is read again once: the memo may predate the operator's own ALTER on the
// target.
//
// The binlog lane emits a boundary only at a DDL unless it is armed for
// first touch ([Streamer.firstTouchBoundariesConsumed]); it now is on these
// streams too, so a change made while the stream was stopped is checked at
// the table's first row of the next one.
//
// # Durability: none needed
//
// Nothing is persisted. The check is a pure function of the source's current
// table shape and the target's catalog, and both survive a restart, so the
// next start refuses again — at the replayed DDL boundary, or at the first
// touch — until the target holds what the source sends. The
// UNFORWARDED-SCHEMA-CHANGE door persists its refusal because ITS evidence
// is a source-side baseline a restart would retake; this one's evidence is
// the target, which a restart does not move. So there is nothing to
// acknowledge: reconciling the target is the remedy and the next start
// confirms it.
//
// # Where the target cannot speak
//
// An engine pair with no storage-shape rendering, or a table the target does
// not hold: the boundary is classified against the table's last accepted
// shape ([priorFor] — this intercept's own, else the stream's retained
// ADR-0049 history, which never records a refused boundary, so a refusal
// here repeats on the next start) and any structural change refuses. A
// table with neither is accepted with a WARN, as the forward path's
// unwitnessed arm does: the one residual where a restart forgets. A
// multi-database namespace whose catalog cannot be read is NOT this case —
// the read error stops the stream ([Streamer.readNamespaceTargetCatalog]).
//
// # The independent expected value
//
// The target's catalog, read through the target engine's own SchemaReader —
// what every row after the boundary will actually be written into. It shares
// no code with the change stream's projection.

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

// schemaChangeRefusedMarker is the grep-stable marker every refusal of this
// check carries — the text of [ir.ErrSchemaChangeRefused], which the refusal
// wraps so the fleet supervisor does not restart a leg into it forever.
var schemaChangeRefusedMarker = ir.ErrSchemaChangeRefused.Error()

// unforwardedBoundaryDeps is what [interceptSchemaChangeRefuse] needs.
type unforwardedBoundaryDeps struct {
	// witnessFor returns the target witness for a boundary's source
	// namespace (ir.SchemaSnapshot.Schema): one for the single-database
	// stream whatever the namespace, one per namespace on a multi-database
	// stream. nil, or a nil return, means the target cannot speak.
	witnessFor func(namespace string) *firstBoundaryWitness

	// normalizer is the source engine's Bug 84/86 comparison lens, applied
	// to every snapshot before it is cached for the unwitnessed fallback —
	// the same lens the forward intercept applies.
	normalizer ir.CDCSchemaSnapshotNormalizer

	// why names the reason this stream forwards nothing, for the refusal.
	why string

	// forwardRemedy is true when `--schema-changes=forward` would make this
	// stream apply such changes itself — a single-database stream under
	// refuse mode. It is false for a multi-database stream (forwarding is
	// single-database only) and for Shape A under --no-coordinate-live-ddl
	// (the forward intercept never engages under --inject-shard-column), so
	// the hint does not offer them a flag that changes nothing.
	forwardRemedy bool
}

// interceptSchemaChangeRefuse wraps the change channel of a stream that
// forwards no source DDL (see the file comment). Every ir.SchemaSnapshot is
// judged against the target before it is passed on; a refusal closes the
// out-channel and stores the error in errStore for the streamer's settle
// path, exactly as the forward intercept does.
func interceptSchemaChangeRefuse(
	ctx context.Context,
	in <-chan ir.Change,
	deps unforwardedBoundaryDeps,
	errStore *atomic.Pointer[error],
) <-chan ir.Change {
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		// accepted holds each table's last ACCEPTED boundary — the prior
		// shape for the unwitnessed fallback and for an overridden column.
		accepted := map[string]acceptedBoundary{}
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				snap, isSnap := c.(ir.SchemaSnapshot)
				if !isSnap {
					if !forwardChange(ctx, out, c) {
						return
					}
					continue
				}
				key := snap.QualifiedName()
				post := normalizeSnapshotForComparison(deps.normalizer, snap.IR)
				if err := judgeUnforwardedBoundary(ctx, deps, key, accepted[key], post, snap); err != nil {
					slog.ErrorContext(ctx, "schema change refused", "table", key, "error", err)
					wrapped := fmt.Errorf("pipeline: schema change on a stream that does not forward DDL: %w", err)
					errStore.Store(&wrapped)
					return
				}
				accepted[key] = acceptedBoundary{compared: post, raw: snap.IR}
				if !forwardChange(ctx, out, c) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// acceptedBoundary is a table's last boundary this intercept passed on, in
// both forms: compared (through the source engine's comparison lens, for
// [ClassifyShape]) and raw (the snapshot's own IR, for rendering as target
// storage the way the current boundary is).
type acceptedBoundary struct {
	compared *ir.Table
	raw      *ir.Table
}

// priorFor returns the table's last accepted shape: this intercept's own,
// else the stream's retained ADR-0049 version at the persisted position —
// a boundary a refusal stopped was never recorded, so after a restart the
// history still holds the shape before it. The zero value when there is
// neither.
func priorFor(deps unforwardedBoundaryDeps, w *firstBoundaryWitness, last acceptedBoundary, table string) acceptedBoundary {
	if last.raw != nil || w == nil {
		return last
	}
	if hist := w.historyFor(table); hist != nil {
		return acceptedBoundary{compared: normalizeSnapshotForComparison(deps.normalizer, hist), raw: hist}
	}
	return last
}

// judgeUnforwardedBoundary decides one boundary: nil to pass it on, or the
// refusal. last is the table's last accepted boundary on this intercept
// (zero for its first), post the comparison-form snapshot table and snap
// the raw snapshot.
func judgeUnforwardedBoundary(
	ctx context.Context,
	deps unforwardedBoundaryDeps,
	tableName string,
	last acceptedBoundary,
	post *ir.Table,
	snap ir.SchemaSnapshot,
) error {
	var w *firstBoundaryWitness
	if deps.witnessFor != nil {
		w = deps.witnessFor(snap.Schema)
	}
	prior := priorFor(deps, w, last, snap.Table)
	reason := "the stream has no target witness"
	if w != nil {
		j, err := w.judgeUnforwarded(ctx, snap.IR, prior.raw)
		if err == nil && len(j.refused) > 0 && snap.IR != nil {
			// The verdict re-reads a disagreeing table once per intercept;
			// on a long-lived stream the operator may have ALTERed the target
			// again since (the drained model, applied while this stream ran),
			// so read it again before refusing.
			w.catalog.forget(snap.IR.Name)
			j, err = w.judgeUnforwarded(ctx, snap.IR, prior.raw)
		}
		if err != nil {
			// Not the marker: nothing has been compared, and a fresh run may
			// read the catalog fine.
			return fmt.Errorf("read the target catalog to check the schema boundary for %q "+
				"(the boundary is not accepted unchecked): %w", tableName, err)
		}
		if j.reason == "" {
			return j.settle(ctx, tableName, deps)
		}
		reason = j.reason
	}
	// The target cannot speak for this table: fall back to its last accepted
	// shape, which shares the change stream's projection — this intercept's
	// own, or the retained history version, so a refusal here repeats on the
	// next start as long as the stream has one.
	if prior.compared == nil {
		slog.WarnContext(ctx,
			"schema change check: the target cannot witness this table's schema boundary and the stream holds "+
				"no earlier shape for it; accepting it — a change made to the source while the stream was "+
				"stopped is NOT checked for this table",
			"table", tableName, "reason", reason)
		return nil
	}
	shape, err := ClassifyShape(prior.compared, post)
	if err == nil && shape.Kind == ShapeKindNone {
		return nil
	}
	what := shape.Kind.String()
	if err != nil {
		what = err.Error()
	}
	return fmt.Errorf(
		"%w: the source changed %q (%s; the target cannot be read to compare: %s) and this stream does not "+
			"forward schema changes (%s). Refusing before the boundary is recorded or any row after it is "+
			"applied.%s %s",
		ir.ErrSchemaChangeRefused, tableName, what, reason, deps.why,
		renderDriftForRefusal(prior.compared, post), unforwardedRecoveryHint(tableName, deps.forwardRemedy, false),
	)
}

// unforwardedJudgement is a boundary judged against the target, column by
// column.
type unforwardedJudgement struct {
	// reason says why the target cannot witness the table; nothing else is
	// set when it is non-empty.
	reason string
	// refused are the columns whose values the target would not hold: a
	// source column the target lacks, or a type the target's does not hold.
	refused []witnessColumnDiff
	// added says one of the refused columns is a source column the target
	// lacks — the remedy is an ADD COLUMN, which the hint qualifies.
	added bool
	// ahead are type differences the target's type holds — the drained
	// model applied on the target before the source.
	ahead []witnessColumnDiff
	// targetOnly are the target's columns the source lacks.
	targetOnly []string
}

// judgeUnforwarded compares post (RAW) with the target's read-back of the
// same table. It reaches the target through [firstBoundaryWitness.verdict] —
// the same rendering, the same unwitnessable cases, the same re-read of a
// table whose memo disagrees — and then judges the pair column by column
// whatever the verdict says: the forward verdict answers "what can be
// forwarded", and it does not compare an overridden column's type at all,
// while a stream that forwards nothing asks every column "can the target
// hold what the source sends". prior is the table's last accepted RAW shape
// (nil when there is none), consulted for an overridden column only.
func (w *firstBoundaryWitness) judgeUnforwarded(ctx context.Context, post, prior *ir.Table) (unforwardedJudgement, error) {
	v, err := w.verdict(ctx, post)
	if err != nil {
		return unforwardedJudgement{}, err
	}
	if v.kind == witnessUnwitnessed {
		return unforwardedJudgement{reason: v.reason}, nil
	}
	target, held, err := w.catalog.lookup(ctx, post.Name)
	if err != nil {
		return unforwardedJudgement{}, err
	}
	if !held {
		return unforwardedJudgement{reason: "the target does not hold the table"}, nil
	}
	expected, err := w.expected(post)
	if err != nil {
		return unforwardedJudgement{}, err
	}
	var priorExpected *ir.Table
	if prior != nil {
		if priorExpected, err = w.expected(prior); err != nil {
			return unforwardedJudgement{}, err
		}
	}
	return judgeUnforwardedColumns(expected, target, w.options(post), priorExpected), nil
}

// judgeUnforwardedColumns is the pure judgement: expected (the snapshot
// rendered as target storage) against target, both through the witness
// lens ([witnessCompareTable], [reconcilePairs]) exactly as
// [classifyWitness] sees them, each differing column judged on its own:
//
//   - a source column the target lacks refuses;
//   - a type difference passes only when [witnessWidthOrder] puts the
//     target at or above the source's width (the same direction rule the
//     forward path uses to never narrow); a narrower target, a change
//     across families, a sign or zone-kind change, a decimal wider on one
//     axis and narrower on the other, and the session-zone sibling swap all
//     refuse;
//   - a column only the target has is noted;
//   - an OVERRIDDEN column ([overriddenColumnRefused]) is judged on width
//     alone, because the lens does not compare its type at all.
//
// priorExpected is the table's last accepted shape rendered the same way
// (nil when there is none).
func judgeUnforwardedColumns(expected, target *ir.Table, opts witnessOptions, priorExpected *ir.Table) unforwardedJudgement {
	exp := witnessCompareTable(expected, nil)
	act := witnessCompareTable(target, exp)
	// The lens equates an overridden column's types; keep the source's own
	// compared type first, so its width can still be judged below.
	pinnedSource := map[string]ir.Type{}
	for _, c := range exp.Columns {
		if opts.pinned[c.Name] {
			pinnedSource[c.Name] = c.Type
		}
	}
	reconcilePairs(exp, act, opts)
	mismatches := irdiff.TableColumnShapeWithOptions(exp, act, irdiff.ShapeCompareOptions{ColumnTypesOnly: true})
	expCols, actCols := columnsByNameIR(exp), columnsByNameIR(act)
	var j unforwardedJudgement
	for _, m := range mismatches {
		d := witnessColumnDiff{column: m.Column, source: m.Expected, target: m.Actual}
		e, inExp := expCols[m.Column]
		a, inAct := actCols[m.Column]
		switch {
		case inExp && !inAct:
			j.refused = append(j.refused, d)
			j.added = true
		case !inExp && inAct:
			j.targetOnly = append(j.targetOnly, m.Column)
		case sessionZoneSiblingSwap(a.Type, e.Type):
			j.refused = append(j.refused, d)
		default:
			switch witnessWidthOrder(a.Type, e.Type) {
			case targetWider:
				j.ahead = append(j.ahead, d)
			case sameWidth:
				// Two spellings of one storage: nothing differs.
			default:
				j.refused = append(j.refused, d)
			}
		}
	}
	var priorCols map[string]*ir.Column
	if priorExpected != nil {
		priorCols = columnsByNameIR(witnessCompareTable(priorExpected, nil))
	}
	for name, source := range pinnedSource {
		a, held := actCols[name]
		if !held {
			continue // judged above as a column the target lacks
		}
		var prior ir.Type
		if pc, ok := priorCols[name]; ok {
			prior = pc.Type
		}
		if overriddenColumnRefused(a.Type, source, prior) {
			j.refused = append(j.refused, witnessColumnDiff{
				column: name, source: source.String(), target: a.Type.String() + " (--type-override)",
			})
		}
	}
	return j
}

// overriddenColumnRefused judges a column an operator --type-override names.
// The witness lens does not compare such a column's type — the override,
// not the source, decided what the target holds, and the target emitter
// renders an override in ways no source rendering predicts — so on its own
// the check saw an overridden column by presence only, on EVERY boundary,
// and a live `DECIMAL(10,2)` → `DECIMAL(14,4)` under an override of
// `numeric(12,2)` was rounded to two places at exit 0 (GC-44 F5 review).
//
// So an overridden column is judged on width, where its two types fall in
// one comparable family ([witnessWidthOrder]): a target at least as wide
// as the source's type passes, and so does a pair across families (a
// `json` override on a text column: the override's own decision, nothing
// to compare). A target NARROWER than the source refuses — unless the
// source's type is what it was at the table's last accepted boundary
// (prior): then the narrowing is the override the operator chose and the
// copy already applied, not a change the source has made since. With no
// prior (a table's first boundary with no retained history) a narrower
// override refuses: the check cannot tell a deliberate narrowing from a
// widening made while the stream was stopped, and only one of those loses
// data silently.
func overriddenColumnRefused(target, source, prior ir.Type) bool {
	switch witnessWidthOrder(target, source) {
	case targetNarrower, mixedWidth:
		return prior == nil || !reflect.DeepEqual(prior, source)
	}
	return false
}

// settle logs an accepted judgement or returns its refusal.
func (j unforwardedJudgement) settle(ctx context.Context, tableName string, deps unforwardedBoundaryDeps) error {
	if len(j.refused) > 0 {
		return fmt.Errorf(
			"%w: the source table %q no longer matches the target, and this stream does not forward schema "+
				"changes (%s); the target cannot hold what the source now sends (%s), so applying the rows "+
				"after this boundary would change or drop their values. Refusing before the boundary is "+
				"recorded or any row after it is applied; nothing has been written. %s",
			ir.ErrSchemaChangeRefused, tableName, deps.why, renderWitnessDiffs(j.refused),
			unforwardedRecoveryHint(tableName, deps.forwardRemedy, j.added),
		)
	}
	if len(j.ahead) > 0 {
		slog.WarnContext(ctx,
			"schema change check: the target already holds a wider column than the source sends "+
				"(applied ahead of the source — the drained model — or a replayed pre-ALTER shape); "+
				"every value fits, accepting the boundary",
			"table", tableName, "difference", renderWitnessDiffs(j.ahead))
	}
	if len(j.targetOnly) > 0 {
		slog.WarnContext(ctx,
			"schema change check: the target holds columns the source does not have (added on the target "+
				"ahead of the source, or a DROP COLUMN this stream did not forward); the target keeps them and "+
				"they receive no values from this stream",
			"table", tableName, "target_only_columns", j.targetOnly)
	}
	if len(j.ahead) == 0 && len(j.targetOnly) == 0 {
		slog.DebugContext(ctx, "schema change check: boundary matches the target", "table", tableName)
	}
	return nil
}

// renderWitnessDiffs lists column disagreements for a log line or refusal.
func renderWitnessDiffs(diffs []witnessColumnDiff) string {
	return witnessVerdict{diffs: diffs}.render()
}

// unforwardedRecoveryHint is the remedy for a refusal of this check. There
// is nothing to acknowledge, because the check re-reads the target on every
// start. forwardRemedy offers --schema-changes=forward, only where that flag
// would forward (see [unforwardedBoundaryDeps.forwardRemedy]); added says a
// refused column is one the target lacks, whose ADD COLUMN on the target is
// not followed by any backfill from this stream.
func unforwardedRecoveryHint(tableName string, forwardRemedy, added bool) string {
	hint := fmt.Sprintf(
		"recovery: apply the same change to %q on the target (run 'sluice sync stop --wait' first if the "+
			"stream is not already stopped), then re-run 'sluice sync start' with the SAME --stream-id; "+
			"every start compares the target again, so nothing needs acknowledging.",
		tableName,
	)
	if added {
		hint += " An ADD COLUMN you run on the target is not backfilled by this stream: the rows the target " +
			"already holds keep whatever that ALTER gives them (its DEFAULT, or NULL), and only rows changed " +
			"after the restart carry the source's values — copy the column's existing values yourself if they matter."
	}
	if forwardRemedy {
		hint += " Or run this stream with --schema-changes=forward, which applies such changes itself."
	}
	return hint
}

// unforwardedStreamReason names why this stream forwards no source DDL,
// for the refusal.
func (s *Streamer) unforwardedStreamReason() string {
	switch {
	case s.multiDatabaseMode():
		return "a multi-database stream; forwarding is single-database only (ADR-0091)"
	case s.InjectShardColumn.Engaged():
		return "--inject-shard-column with --no-coordinate-live-ddl"
	case !s.forwardSchemaEnabled():
		return "--schema-changes=refuse"
	}
	return "schema-change forwarding is not engaged for this stream"
}

// unforwardedForwardRemedy reports whether --schema-changes=forward would
// make this stream forward the change it refused: a single-database stream
// without --inject-shard-column, which here means one under refuse mode.
func (s *Streamer) unforwardedForwardRemedy() bool {
	return !s.multiDatabaseMode() && !s.InjectShardColumn.Engaged() && !s.forwardSchemaEnabled()
}

// unforwardedBoundaryWitnesses returns the witness lookup for
// [interceptSchemaChangeRefuse]: single is the stream's own witness (built
// by the caller, which consumes the warm-resume catalog and history reads),
// served for every namespace; a multi-database stream gets one witness per
// source namespace, carrying that namespace's retained history
// ([Streamer.namespaceHistory]) and reading its target catalog through
// [Streamer.readNamespaceTargetCatalog]. The lookup runs on the intercept's
// goroutine only; everything it needs from the Streamer is captured here,
// on the wiring goroutine.
func (s *Streamer) unforwardedBoundaryWitnesses(single *firstBoundaryWitness) func(string) *firstBoundaryWitness {
	history := s.namespaceHistory
	s.namespaceHistory = nil
	if !s.multiDatabaseMode() || single == nil {
		return func(string) *firstBoundaryWitness { return single }
	}
	deriver := s.namespaceTargetDeriver
	byNamespace := map[string]*firstBoundaryWitness{}
	return func(namespace string) *firstBoundaryWitness {
		if w, ok := byNamespace[namespace]; ok {
			return w
		}
		w := &firstBoundaryWitness{
			catalog: newTargetCatalogWitness(func(ctx context.Context) (map[string]*ir.Table, error) {
				return s.readNamespaceTargetCatalog(ctx, namespace, deriver)
			}, nil),
			history:      history[namespace],
			sourceEngine: single.sourceEngine,
			targetEngine: single.targetEngine,
			mappings:     single.mappings,
		}
		byNamespace[namespace] = w
		return w
	}
}

// readNamespaceTargetCatalog reads one source namespace's target catalog
// for the unforwarded-stream check, and FAILS on a read it cannot make.
//
// It is deliberately not [Streamer.loadMultiDatabaseTargetZoneWitness],
// which turns any read error into an empty catalog with a WARN: right for
// the warm-resume seed it was written for, wrong here, where an empty
// catalog makes every table "not held" and a change made while the stream
// was stopped would be accepted with a WARN (GC-44 F5 review). The
// single-database witness errors loudly on the same failure; this matches
// it. The error is not the marker, so the stream stops restartable and the
// next start reads again. A namespace the target simply does not hold reads
// back empty (a Postgres schema reader filters on a name nothing matches),
// which is the honest "not held".
func (s *Streamer) readNamespaceTargetCatalog(ctx context.Context, namespace string, deriver ir.DatabaseDSNDeriver) (map[string]*ir.Table, error) {
	target := s.NamespaceMap.Apply(namespace)
	if deriver == nil {
		return nil, fmt.Errorf("pipeline: the target engine cannot derive a DSN for namespace %q, "+
			"so its catalog cannot be read to check schema boundaries", target)
	}
	dsn, err := deriver.WithDatabase(s.TargetDSN, target)
	if err != nil {
		return nil, fmt.Errorf("pipeline: derive target DSN for namespace %q: %w", target, err)
	}
	return s.loadTargetZoneWitnessFromDSN(ctx, dsn)
}
