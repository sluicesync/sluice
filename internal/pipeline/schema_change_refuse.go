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
//   - …except where the table's prior shape proves the SOURCE NARROWED the
//     column ([sourceNarrowed]; GC-44 F5 third review): its own ALTER
//     converted the rows it stores and the target's copies were never
//     converted, so that refuses. The prior is this intercept's last
//     accepted boundary, else the retained history version, never the
//     cold-start read, and never for a possible replay — a boundary showing
//     a shape the stream already recorded, or one proven to lie before the
//     prior ([acceptedBoundary.evidenceFor]);
//   - a column only the target has refuses where that prior shows the
//     source had it — the source's DROP COLUMN, which leaves the target
//     holding values the source no longer has (one dropped and one added
//     together are reported as the likely RENAME COLUMN) — and a column
//     the source replaced under the same name refuses ([replacedColumns]);
//   - a column only the target has, with no such prior, is a WARN, as on
//     the forward path;
//   - a --type-override column, whose type the lens does not compare, is
//     judged on width alone ([overriddenColumnRefused]) and on a proven
//     source narrowing.
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
// table shape, the target's catalog and the stream's retained history (which
// never records a refused boundary), and all three survive a restart, so the
// next start refuses again — at the replayed DDL boundary, or at the first
// touch — until the target holds what the source sends. (The narrowing
// rule's history half is position-free on Postgres, whose history keeps one
// shape per table: GC-44 F23.) The
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
	"strings"
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

	// coldStart is the RAW source shape of every table a cold start read
	// before this wiring ([Streamer.unforwardedColdStartPrior]; nil after a
	// warm resume). It is a table's prior for its first boundary when the
	// intercept has none of its own and the history has none — consulted
	// for an overridden column's "unchanged since" test only: its fidelity
	// differs from the change stream's (ADR-0091 §3), so it never judges a
	// narrowing, a returning column, or the unwitnessed fallback.
	coldStart []ir.SchemaSnapshot

	// orderer is the source engine's position order, which tells a
	// replayed boundary from a new one ([provenBefore]). nil: unknown, and
	// the narrowing rules then apply.
	orderer ir.PositionOrderer
}

// priorOrigin says where a table's prior shape came from, which decides
// what it may be used for ([acceptedBoundary.judges]).
type priorOrigin int

const (
	priorNone      priorOrigin = iota
	priorIntercept             // this intercept's own last accepted boundary
	priorHistory               // the stream's retained ADR-0049 version
	priorColdStart             // the cold start's raw source read
)

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
	coldStart := newColdStartPriors(deps.coldStart)
	go func() {
		defer close(out)
		// accepted holds each table's last ACCEPTED boundary — the prior
		// shape for the unwitnessed fallback, the narrowing, dropped- and
		// replaced-column rules, and an overridden column.
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
				last := accepted[key]
				if last.raw == nil {
					last = coldStart.lookup(key, snap.Table)
				}
				prior, err := judgeUnforwardedBoundary(ctx, deps, key, last, post, snap)
				if err != nil {
					slog.ErrorContext(ctx, "schema change refused", "table", key, "error", err)
					wrapped := fmt.Errorf("pipeline: schema change on a stream that does not forward DDL: %w", err)
					errStore.Store(&wrapped)
					return
				}
				accepted[key] = acceptedBoundary{
					compared: post, raw: snap.IR, position: snap.Position, origin: priorIntercept,
					recorded: withRecorded(prior.recorded, snap.IR),
				}
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

// acceptedBoundary is a table's prior shape, in both forms: compared
// (through the source engine's comparison lens, for [ClassifyShape]) and
// raw (the snapshot's own IR, for rendering as target storage the way the
// current boundary is), with where it came from and the position from which
// it held (zero when unknown).
type acceptedBoundary struct {
	compared *ir.Table
	raw      *ir.Table
	position ir.Position
	origin   priorOrigin

	// recorded are the shapes the stream has already seen for the table —
	// every retained history version, then every boundary this intercept
	// accepted. A boundary showing one of them may be a replay
	// ([isReplayOf]) and is never evidence of a narrowing.
	recorded []*ir.Table
}

// evidenceFor reports whether this prior may judge a CHANGE the boundary
// post at pos shows — a narrowing, or the unwitnessed fallback's structural
// change: it [acceptedBoundary.judges] the boundary, and post is not a
// shape the stream already recorded (a replay re-delivers only those; on a
// Postgres source, whose boundaries are anchored at LSN 0/0, that is the
// only replay test there is).
func (p acceptedBoundary) evidenceFor(orderer ir.PositionOrderer, pos ir.Position, post *ir.Table) bool {
	return p.judges(orderer, pos) && !recordedShape(p.recorded, post)
}

// withRecorded returns recorded with post appended unless it is already
// there.
func withRecorded(recorded []*ir.Table, post *ir.Table) []*ir.Table {
	if post == nil || recordedShape(recorded, post) {
		return recorded
	}
	return append(append([]*ir.Table(nil), recorded...), post)
}

// judges reports whether this prior may stand as the shape the source held
// IMMEDIATELY before a boundary at pos — the evidence the narrowing and
// returning-column rules act on, and the unwitnessed fallback's baseline.
// Not a cold-start read (its fidelity differs from the change stream's,
// ADR-0091 §3), and not when the boundary is PROVEN to lie before the
// prior's own position: that is a replay (a Postgres transaction
// re-delivered from its start, pre-ALTER relation first), whose shape
// predates the prior and says nothing about what the source did since
// ([firstBoundaryWitness.historyPriorAt] has the full argument).
func (p acceptedBoundary) judges(orderer ir.PositionOrderer, pos ir.Position) bool {
	switch p.origin {
	case priorIntercept, priorHistory:
		return !provenBefore(orderer, pos, p.position)
	}
	return false
}

// priorFor returns the table's prior shape: this intercept's own last
// accepted boundary (or the cold-start read the caller substituted), else
// the stream's retained ADR-0049 version at the persisted position — a
// boundary a refusal stopped was never recorded, so after a restart the
// history still holds the shape before it. The zero value when there is
// neither. An intercept-own prior always wins; a history version wins over
// the cold-start read (it is CDC-fidelity, the cold start's is not).
func priorFor(deps unforwardedBoundaryDeps, w *firstBoundaryWitness, last acceptedBoundary, table string) acceptedBoundary {
	if last.origin == priorIntercept || w == nil {
		return last
	}
	if hist := w.historyFor(table); hist != nil {
		r := w.retained[hist]
		return acceptedBoundary{
			compared: normalizeSnapshotForComparison(deps.normalizer, hist), raw: hist,
			position: r.anchor, origin: priorHistory,
			recorded: append([]*ir.Table{hist}, r.recorded...),
		}
	}
	return last
}

// judgeUnforwardedBoundary decides one boundary: a nil error to pass it on,
// or the refusal. last is the table's last accepted boundary on this
// intercept (or its cold-start read; zero when neither), post the
// comparison-form snapshot table and snap the raw snapshot. It returns the
// prior it judged against, whose recorded shapes the caller carries on.
func judgeUnforwardedBoundary(
	ctx context.Context,
	deps unforwardedBoundaryDeps,
	tableName string,
	last acceptedBoundary,
	post *ir.Table,
	snap ir.SchemaSnapshot,
) (acceptedBoundary, error) {
	var w *firstBoundaryWitness
	if deps.witnessFor != nil {
		w = deps.witnessFor(snap.Schema)
	}
	prior := priorFor(deps, w, last, snap.Table)
	judges := prior.judges(deps.orderer, snap.Position)
	changed := prior.evidenceFor(deps.orderer, snap.Position, snap.IR)
	reason := "the stream has no target witness"
	if w != nil {
		evidence := unforwardedPrior{raw: prior.raw, judges: changed}
		if judges {
			evidence.returned = replacedColumns(prior.raw, snap.IR)
		}
		j, err := w.judgeUnforwarded(ctx, snap.IR, evidence)
		if err == nil && len(j.refused) > 0 && snap.IR != nil {
			// The verdict re-reads a disagreeing table once per intercept;
			// on a long-lived stream the operator may have ALTERed the target
			// again since (the drained model, applied while this stream ran),
			// so read it again before refusing.
			w.catalog.forget(snap.IR.Name)
			j, err = w.judgeUnforwarded(ctx, snap.IR, evidence)
		}
		if err != nil {
			// Not the marker: nothing has been compared, and a fresh run may
			// read the catalog fine.
			return prior, fmt.Errorf("read the target catalog to check the schema boundary for %q "+
				"(the boundary is not accepted unchecked): %w", tableName, err)
		}
		if j.reason == "" {
			return prior, j.settle(ctx, tableName, deps)
		}
		reason = j.reason
	}
	// The target cannot speak for this table: fall back to its last accepted
	// shape, which shares the change stream's projection — this intercept's
	// own, or the retained history version, so a refusal here repeats on the
	// next start as long as the stream has one. Never the cold-start read
	// (its fidelity differs, so it would refuse a phantom), and never for a
	// possible replay (it would refuse the replay of a change the stream
	// already took).
	switch {
	case prior.compared == nil || prior.origin == priorColdStart:
		slog.WarnContext(ctx,
			"schema change check: the target cannot witness this table's schema boundary and the stream holds "+
				"no earlier shape for it; accepting it — a change made to the source while the stream was "+
				"stopped is NOT checked for this table",
			"table", tableName, "reason", reason)
		return prior, nil
	case !changed:
		slog.DebugContext(ctx, "schema change check: an unwitnessed boundary shows a shape the stream already "+
			"recorded (or a replay); nothing to judge", "table", tableName)
		return prior, nil
	}
	shape, err := ClassifyShape(prior.compared, post)
	if err == nil && shape.Kind == ShapeKindNone {
		return prior, nil
	}
	what := shape.Kind.String()
	if err != nil {
		what = err.Error()
	}
	return prior, fmt.Errorf(
		"%w: the source changed %q (%s; the target cannot be read to compare: %s) and this stream does not "+
			"forward schema changes (%s). Refusing before the boundary is recorded or any row after it is "+
			"applied.%s %s",
		ir.ErrSchemaChangeRefused, tableName, what, reason, deps.why,
		renderDriftForRefusal(prior.compared, post), unforwardedRecoveryHint(tableName, deps.forwardRemedy, false),
	)
}

// unforwardedPrior is what the judgement knows about the table's prior
// shape. raw (nil when there is none) is consulted for an overridden
// column's "unchanged since" test whatever its origin; the narrowing and
// dropped-column rules apply only when judges is set
// ([acceptedBoundary.evidenceFor]).
type unforwardedPrior struct {
	raw    *ir.Table
	judges bool
	// returned are columns the snapshot carries under a name the prior had,
	// but which the source replaced ([replacedColumns]).
	returned map[string]bool
}

// replacedColumns are the columns of post that the source REPLACED since
// prior: the same name, a different non-zero stable column id
// (pg_attribute.attnum) — a DROP + ADD of the same name and type, or a
// same-type swap through renames, inside one relation message, which a
// name-and-type comparison cannot see. The target kept the old column with
// its OLD values, which the source no longer holds (GC-44 F5 third review).
//
// A DROP and a later ADD of the same name seen as two boundaries needs no
// rule of its own: the DROP refuses while the target still holds the column
// ([judgeUnforwardedColumns]), and once the target has dropped it too the
// ADD refuses as a column the target lacks.
//
// In-process evidence only: a history version carries no stable ids (they
// are not persisted), so the same replacement across a restart is not seen,
// and the Postgres reader emits no boundary at all for a replacement that
// leaves every name and type in place (GC-44 F22).
func replacedColumns(prior, post *ir.Table) map[string]bool {
	if post == nil || prior == nil {
		return nil
	}
	priorCols := columnsByNameIR(prior)
	var out map[string]bool
	for _, c := range post.Columns {
		if c == nil {
			continue
		}
		if pc, ok := priorCols[c.Name]; ok && pc.StableID != 0 && c.StableID != 0 && pc.StableID != c.StableID {
			if out == nil {
				out = map[string]bool{}
			}
			out[c.Name] = true
		}
	}
	return out
}

// coldStartPriors indexes the cold-start read ([unforwardedBoundaryDeps.coldStart])
// by qualified name, with a bare-name fallback for a MySQL source whose
// schema reader leaves Table.Schema empty while its change stream names the
// database (the Bug 83 seed key shape, [lookupSeedCache]) — used only when
// exactly one table carries that bare name.
type coldStartPriors struct {
	byKey  map[string]*ir.Table
	byName map[string]*ir.Table
}

func newColdStartPriors(seed []ir.SchemaSnapshot) coldStartPriors {
	p := coldStartPriors{byKey: map[string]*ir.Table{}, byName: map[string]*ir.Table{}}
	ambiguous := map[string]bool{}
	for _, s := range seed {
		if s.IR == nil {
			continue
		}
		p.byKey[s.QualifiedName()] = s.IR
		if _, seen := p.byName[s.Table]; seen {
			ambiguous[s.Table] = true
		}
		p.byName[s.Table] = s.IR
	}
	for name := range ambiguous {
		delete(p.byName, name)
	}
	return p
}

// lookup returns the cold-start prior for a boundary on key / table, or the
// zero value.
func (p coldStartPriors) lookup(key, table string) acceptedBoundary {
	t, ok := p.byKey[key]
	if !ok {
		t, ok = p.byName[table]
	}
	if !ok {
		return acceptedBoundary{}
	}
	return acceptedBoundary{compared: t, raw: t, origin: priorColdStart}
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
	// addedCols / droppedCols name the source columns the target lacks and
	// the columns the source dropped while the target kept them; one of
	// each is reported as the likely RENAME COLUMN it usually is.
	addedCols   []string
	droppedCols []string
	// narrowed says one of the refused columns is one the source NARROWED
	// while the target kept the wider type ([sourceNarrowed]).
	narrowed bool
	// returned says one of the refused columns is one the source replaced
	// under the same name ([replacedColumns]).
	returned bool
	// ahead are type differences the target's type holds and no evidence
	// says the source narrowed — the drained model applied on the target
	// before the source, a replayed pre-ALTER shape, or a narrowing the
	// stream has no earlier shape to recognise.
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
// hold what the source sends". prior is what is known of the table's prior
// shape ([unforwardedPrior]).
func (w *firstBoundaryWitness) judgeUnforwarded(ctx context.Context, post *ir.Table, prior unforwardedPrior) (unforwardedJudgement, error) {
	// No position: the verdict here only says whether the target can
	// witness the table; the narrowing evidence is prior's.
	v, err := w.verdict(ctx, post, ir.Position{})
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
	judged := judgedPrior{judges: prior.judges, returned: prior.returned}
	if prior.raw != nil {
		if judged.expected, err = w.expected(prior.raw); err != nil {
			return unforwardedJudgement{}, err
		}
	}
	return judgeUnforwardedColumns(expected, target, w.options(post), judged), nil
}

// judgedPrior is [unforwardedPrior] with the prior shape rendered as target
// storage, the form [judgeUnforwardedColumns] compares.
type judgedPrior struct {
	expected *ir.Table
	judges   bool
	returned map[string]bool
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
//   - a target WIDER than the source refuses after all when the prior
//     proves the source NARROWED the column ([sourceNarrowed]): its own
//     ALTER converted the rows it stores, the target's copies of them were
//     never converted, and the two now differ at exit 0 (GC-44 F5 third
//     review — a live `numeric(14,4)` → `numeric(12,2)` rounded the source
//     and the target kept `1.2345`). Without that proof — no prior, a
//     cold-start read, a replay — it passes with a WARN as before;
//   - a column only the target has refuses where the prior shows the
//     source had it (the source's DROP COLUMN; with an added column beside
//     it, the likely RENAME COLUMN — [unforwardedJudgement.shapeNote]), and
//     is noted otherwise;
//   - a column the source replaced under the same name ([replacedColumns])
//     refuses: the target kept the old column and its old values;
//   - an OVERRIDDEN column ([overriddenColumnRefused]) is judged on width
//     alone, because the lens does not compare its type at all, and refuses
//     a proven source narrowing too.
func judgeUnforwardedColumns(expected, target *ir.Table, opts witnessOptions, prior judgedPrior) unforwardedJudgement {
	exp := witnessCompareTable(expected, nil)
	act := witnessCompareTable(target, exp)
	// Each column's compared source type, kept before the lens equates an
	// overridden column's types, so its width can still be judged below.
	sourceType := map[string]ir.Type{}
	for _, c := range exp.Columns {
		sourceType[c.Name] = c.Type
	}
	priorCols := priorColumns(prior.expected)
	// narrowedFrom is the type a judging prior says the source held before
	// it narrowed column (nil, false when no evidence says so).
	narrowedFrom := func(column string) (ir.Type, bool) {
		pc, ok := priorCols[column]
		if !prior.judges || !ok || !sourceNarrowed(pc.Type, sourceType[column]) {
			return nil, false
		}
		return pc.Type, true
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
			j.addedCols = append(j.addedCols, m.Column)
		case !inExp && inAct:
			// A column only the target has is the drained model run ahead
			// (dropped on the target first) or an unforwarded DROP. Where a
			// prior that judges this boundary shows the SOURCE still had the
			// column, it is the source's DROP COLUMN — the target keeps the
			// column with values the source no longer holds, and a refuse-mode
			// stream refused it before GC-44 F5's reader-gate relaxation
			// (the drained-model recovery pin, drop-column/refuse); refuse it
			// here. With no such prior it stays the WARN.
			if _, had := priorCols[m.Column]; had && prior.judges {
				d.source = "(dropped on the source)"
				j.refused = append(j.refused, d)
				j.droppedCols = append(j.droppedCols, m.Column)
				break
			}
			j.targetOnly = append(j.targetOnly, m.Column)
		case sessionZoneSiblingSwap(a.Type, e.Type):
			j.refused = append(j.refused, d)
		default:
			switch witnessWidthOrder(a.Type, e.Type) {
			case targetWider:
				if from, ok := narrowedFrom(m.Column); ok {
					d.source = fmt.Sprintf("%s (narrowed from %s)", d.source, from)
					j.refused = append(j.refused, d)
					j.narrowed = true
					break
				}
				j.ahead = append(j.ahead, d)
			case sameWidth:
				// Two spellings of one storage: nothing differs.
			default:
				j.refused = append(j.refused, d)
			}
		}
	}
	for _, c := range exp.Columns {
		if !opts.pinned[c.Name] {
			continue
		}
		a, held := actCols[c.Name]
		if !held {
			continue // judged above as a column the target lacks
		}
		source := sourceType[c.Name]
		var priorType ir.Type
		if pc, ok := priorCols[c.Name]; ok {
			priorType = pc.Type
		}
		from, narrowed := narrowedFrom(c.Name)
		switch {
		case overriddenColumnRefused(a.Type, source, priorType):
			j.refused = append(j.refused, witnessColumnDiff{
				column: c.Name, source: source.String(), target: a.Type.String() + " (--type-override)",
			})
		case narrowed:
			j.refused = append(j.refused, witnessColumnDiff{
				column: c.Name, source: fmt.Sprintf("%s (narrowed from %s)", source, from),
				target: a.Type.String() + " (--type-override)",
			})
			j.narrowed = true
		}
	}
	for _, c := range exp.Columns {
		a, held := actCols[c.Name]
		if !prior.returned[c.Name] || !held {
			continue // a column the target lacks is judged above
		}
		j.refused = append(j.refused, witnessColumnDiff{
			column: c.Name, source: sourceType[c.Name].String() + " (replaced on the source under the same name)",
			target: a.Type.String() + " (the column the target kept, with its old values)",
		})
		j.returned = true
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
				"changes (%s); the target cannot hold what the source now sends (%s)%s, so applying the rows "+
				"after this boundary would change or drop their values. Refusing before the boundary is "+
				"recorded or any row after it is applied; nothing has been written. %s",
			ir.ErrSchemaChangeRefused, tableName, deps.why, renderWitnessDiffs(j.refused), j.shapeNote(),
			unforwardedRecoveryHint(tableName, deps.forwardRemedy, j.added)+j.remedyNotes(),
		)
	}
	if len(j.ahead) > 0 {
		slog.WarnContext(ctx,
			"schema change check: the target column is wider than the source's; every value the source sends "+
				"from here fits, so the boundary is accepted. This is the drained model (the target changed "+
				"ahead of the source) or a replayed pre-ALTER shape — OR a source narrowing this check has no "+
				"earlier shape to recognise, in which case the rows the target already holds kept the values "+
				"the source's own ALTER converted: compare them, and narrow the target or re-copy the table if "+
				"the source did narrow it",
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

// shapeNote names the source DDL a refusal most likely saw, where the
// column diff alone reads awkwardly: one column dropped and one the target
// lacks is usually a RENAME COLUMN (indistinguishable from DROP + ADD
// without a stable column id); a dropped column alone is a DROP COLUMN.
func (j unforwardedJudgement) shapeNote() string {
	switch {
	case len(j.droppedCols) == 1 && len(j.addedCols) == 1:
		return fmt.Sprintf(" — likely RENAME COLUMN %s → %s (or DROP COLUMN %s plus ADD COLUMN %s)",
			j.droppedCols[0], j.addedCols[0], j.droppedCols[0], j.addedCols[0])
	case len(j.droppedCols) > 0:
		return fmt.Sprintf(" — DROP COLUMN %s on the source; the target still holds the column, with values the "+
			"source no longer has", strings.Join(j.droppedCols, ", "))
	}
	return ""
}

// remedyNotes qualifies the remedy for the refusal classes whose "apply the
// same change on the target" is not the whole story.
func (j unforwardedJudgement) remedyNotes() string {
	var notes string
	if j.narrowed {
		notes += " A column the source NARROWED: its own ALTER converted the values it stores (rounded, " +
			"truncated or relabelled them), and the rows the target already holds were never converted — " +
			"narrow the target column the same way (or re-copy the table) before restarting."
	}
	if j.returned {
		notes += " A column the source replaced under the same name (dropped and added back, or swapped through " +
			"renames): the target still holds the OLD column and its old values — drop and re-add it on the target, and copy the " +
			"source's values into it, before restarting."
	}
	return notes
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
		"Drained-model recovery: apply the same change to %q on the target (run 'sluice sync stop --wait' "+
			"first if the stream is not already stopped), then re-run 'sluice sync start' with the SAME "+
			"--stream-id; every start compares the target again, so nothing needs acknowledging.",
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
			retained:     single.retained,
			orderer:      single.orderer,
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
