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
//   - a column only the target has is a WARN, as on the forward path.
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
// An engine pair with no storage-shape rendering, a table the target does
// not hold, or a multi-database namespace whose catalog cannot be read: the
// boundary is classified against the last boundary this intercept saw for
// the table (CDC against CDC, the same projection on both sides) and any
// structural change refuses; a table's first boundary is accepted with a
// WARN, exactly as the forward path's unwitnessed arm does.
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
		// cache holds each table's last ACCEPTED boundary, for the
		// unwitnessed fallback only.
		cache := map[string]*ir.Table{}
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
				if err := judgeUnforwardedBoundary(ctx, deps, key, cache[key], post, snap); err != nil {
					slog.ErrorContext(ctx, "schema change refused", "table", key, "error", err)
					wrapped := fmt.Errorf("pipeline: schema change on a stream that does not forward DDL: %w", err)
					errStore.Store(&wrapped)
					return
				}
				cache[key] = post
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

// judgeUnforwardedBoundary decides one boundary: nil to pass it on, or the
// refusal. pre is the table's last accepted boundary on this intercept (nil
// for its first), post the comparison-form snapshot table and snap the raw
// snapshot.
func judgeUnforwardedBoundary(
	ctx context.Context,
	deps unforwardedBoundaryDeps,
	tableName string,
	pre, post *ir.Table,
	snap ir.SchemaSnapshot,
) error {
	var w *firstBoundaryWitness
	if deps.witnessFor != nil {
		w = deps.witnessFor(snap.Schema)
	}
	reason := "the stream has no target witness"
	if w != nil {
		j, err := w.judgeUnforwarded(ctx, snap.IR)
		if err == nil && len(j.refused) > 0 && snap.IR != nil {
			// The verdict re-reads a disagreeing table once per intercept;
			// on a long-lived stream the operator may have ALTERed the target
			// again since (the drained model, applied while this stream ran),
			// so read it again before refusing.
			w.catalog.forget(snap.IR.Name)
			j, err = w.judgeUnforwarded(ctx, snap.IR)
		}
		if err != nil {
			// Not the marker: nothing has been compared, and a fresh run may
			// read the catalog fine.
			return fmt.Errorf("read the target catalog to check the schema boundary for %q "+
				"(the boundary is not accepted unchecked): %w", tableName, err)
		}
		if j.reason == "" {
			return j.settle(ctx, tableName, deps.why)
		}
		reason = j.reason
	}
	// The target cannot speak for this table: fall back to the last
	// boundary this intercept accepted, which shares the change stream's
	// projection.
	if pre == nil {
		slog.WarnContext(ctx,
			"schema change check: the target cannot witness this table's schema boundary and the stream has "+
				"seen no earlier one; accepting it — a change made to the source while the stream was stopped "+
				"is NOT checked for this table",
			"table", tableName, "reason", reason)
		return nil
	}
	shape, err := ClassifyShape(pre, post)
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
		renderDriftForRefusal(pre, post), unforwardedRecoveryHint(tableName),
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
	// ahead are type differences the target's type holds — the drained
	// model applied on the target before the source.
	ahead []witnessColumnDiff
	// targetOnly are the target's columns the source lacks.
	targetOnly []string
}

// judgeUnforwarded compares post (RAW) with the target's read-back of the
// same table. It reaches the target through [firstBoundaryWitness.verdict] —
// the same rendering, the same unwitnessable cases, the same re-read of a
// table whose memo disagrees — and, when that is not a match, judges the
// pair column by column: the forward verdict answers "what can be
// forwarded", and a stream that forwards nothing asks a different question
// of each column, "can the target hold what the source sends".
func (w *firstBoundaryWitness) judgeUnforwarded(ctx context.Context, post *ir.Table) (unforwardedJudgement, error) {
	v, err := w.verdict(ctx, post)
	if err != nil {
		return unforwardedJudgement{}, err
	}
	switch v.kind {
	case witnessUnwitnessed:
		return unforwardedJudgement{reason: v.reason}, nil
	case witnessMatch:
		return unforwardedJudgement{}, nil
	}
	// The memo is current: the verdict re-read the table on its mismatch.
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
	return judgeUnforwardedColumns(expected, target, w.options(post)), nil
}

// judgeUnforwardedColumns is the pure judgement: expected (the snapshot
// rendered as target storage) against target, both through the witness
// lens ([witnessCompareTable], [reconcilePairs]) exactly as
// [classifyWitness] sees them, each differing column judged on its own:
//
//   - a source column the target lacks refuses;
//   - a type difference passes only when [witnessWidthOrder] puts the
//     target at or above the source's width in one family (the same
//     direction rule the forward path uses to never narrow); a narrower
//     target, a change across families, a sign or zone-kind change, a
//     decimal wider on one axis and narrower on the other, and the
//     session-zone sibling swap all refuse;
//   - a column only the target has is noted.
func judgeUnforwardedColumns(expected, target *ir.Table, opts witnessOptions) unforwardedJudgement {
	exp := witnessCompareTable(expected, nil)
	act := witnessCompareTable(target, exp)
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
	return j
}

// settle logs an accepted judgement or returns its refusal.
func (j unforwardedJudgement) settle(ctx context.Context, tableName, why string) error {
	if len(j.refused) > 0 {
		return fmt.Errorf(
			"%w: the source table %q no longer matches the target, and this stream does not forward schema "+
				"changes (%s); the target cannot hold what the source now sends (%s), so applying the rows "+
				"after this boundary would change or drop their values. Refusing before the boundary is "+
				"recorded or any row after it is applied; nothing has been written. %s",
			ir.ErrSchemaChangeRefused, tableName, why, renderWitnessDiffs(j.refused),
			unforwardedRecoveryHint(tableName),
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

// unforwardedRecoveryHint is the remedy for a refusal of this check: there
// is nothing to acknowledge, because the check re-reads the target on every
// start.
func unforwardedRecoveryHint(tableName string) string {
	return fmt.Sprintf(
		"recovery: apply the same change to %q on the target (run 'sluice sync stop --wait' first if the "+
			"stream is not already stopped), then re-run 'sluice sync start' with the SAME --stream-id; "+
			"every start compares the target again, so nothing needs acknowledging. A single-database "+
			"stream can instead run with --schema-changes=forward, which applies such changes itself.",
		tableName,
	)
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

// unforwardedBoundaryWitnesses returns the witness lookup for
// [interceptSchemaChangeRefuse]: single is the stream's own witness (built
// by the caller, which consumes the warm-resume catalog read), served for
// every namespace; a multi-database stream gets one witness per source
// namespace, each reading that namespace's target catalog through the
// per-namespace DSN the multi-database open resolved after its preflights
// ([Streamer.namespaceTargetDeriver], [Streamer.loadMultiDatabaseTargetZoneWitness],
// which degrades to an empty catalog — the unwitnessed fallback — with a
// WARN when the namespace cannot be read). Called from the intercept's
// goroutine only.
func (s *Streamer) unforwardedBoundaryWitnesses(streamID string, single *firstBoundaryWitness) func(string) *firstBoundaryWitness {
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
				return s.loadMultiDatabaseTargetZoneWitness(ctx, streamID, namespace, deriver)
			}, nil),
			sourceEngine: single.sourceEngine,
			targetEngine: single.targetEngine,
			mappings:     single.mappings,
		}
		byNamespace[namespace] = w
		return w
	}
}
