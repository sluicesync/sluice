// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// RENAME-SWAP-FORWARD — a change of column order is a schema boundary
// sluice cannot read.
//
// # The defect
//
// `ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b` on a source
// that `sync start` follows was applied to the target as nothing: every row
// the target already held kept its pre-swap values under its pre-swap
// names, at exit 0 (measured: MySQL 8.0 and Postgres 16, live, with the
// rows after the swap landing correctly by name — the harm is the existing
// rows, and a later UPDATE carrying the whole row heals one row at a time).
// Every schema comparison sluice makes pairs columns BY NAME — the forward
// classifier ([ClassifyShape]), the target-witnessed first boundary
// ([classifyWitness]), the unforwarded-stream check
// ([judgeUnforwardedColumns]) and chain replay's aspect classifier — and a
// same-type swap leaves every name and every name's type in place. So did a
// swap of two DIFFERENTLY typed columns, one level down: both names change
// type, and the classifier read two altered columns as no change at all
// ([multiColumnAlterError] closes that sibling).
//
// # Why this refuses instead of forwarding
//
// What a swap does change is the ORDER the source declares the names in:
// the values stay in their positions and the names move. A reorder
// (`MODIFY c INT AFTER id`, MySQL/MariaDB) moves the values WITH the names
// and produces the same shape, and the target needs the opposite treatment
// (a reorder needs no DDL at all — sluice addresses rows by name; a swap
// needs the same RENAMEs). Nothing in the boundary tells them apart: MySQL
// has no stable column id (ADR-0091 §3), and the Postgres attnum that would
// prove a permutation is never seen at a first boundary or against the
// cold-start seed. The DDL text would tell them apart, but no schema
// boundary carries it (ir.SchemaSnapshot is shape-only, ADR-0049); carrying
// it is the forwarding option recorded under RENAME-SWAP-DDL-TEXT in the
// audit backlog. Until then the boundary refuses, before it is recorded and
// before any row after it is applied, naming the columns whose relative
// order flipped.
//
// # The way out, and its independent expected value
//
// The operator applies the source's change on the target, and the TARGET'S
// OWN column order — read through the target engine's SchemaReader, sharing
// no code with the change stream — proves it: every flipped pair must have
// MOVED, from the old order in the table's recorded target order
// ([ir.ColumnOrderRecord], written at the cold start and on the transaction
// of every accepted boundary that changes it) to the new order on the target
// now. Comparing the target now with the new order alone is NOT proof, and
// that was a review finding against this check's first cut: a target that
// already held the new order for an unrelated reason — an `ADD COLUMN x
// AFTER id` forwarded to the END of the target table, then the source
// swapping x and b — accepted the swap with no DDL, silently. The record is
// what tells "the operator moved it" from "it was already there". A
// boundary whose table has no record (a stream from before this release, a
// target that cannot keep one), or whose record already shows the new order,
// cannot be proven and refuses with the only safe remedy: re-copy.
//
// A restart re-derives all of it: the refused boundary is never recorded,
// and its first boundary is compared with the record (and with the retained
// schema history, or with no history the target itself), so a restart that
// changed nothing refuses again.
//
// # The cost, stated
//
// A plain MySQL/MariaDB column reorder, which used to forward as a no-op
// (ADR-0091 §4), now refuses until the target is reordered the same way —
// and on a target that cannot reorder (Postgres) until the table is
// re-copied. A Postgres source cannot reorder, so there the refusal only
// ever fires on a real permutation.
//
// # What is still not seen
//
// A permutation that leaves the declared order unchanged — a swap followed,
// before the table's next row, by a reorder that restores the old order —
// changes nothing a shape can show; the binlog reader does not even emit a
// boundary for it (measured, MySQL 8.4). Neither does a MySQL `DROP COLUMN
// b, RENAME COLUMN c TO b` or `RENAME COLUMN c TO tmp, ADD COLUMN c`, which
// a Postgres source's stable column ids do expose
// ([columnsWhoseIdentityChanged]). Only the DDL text closes those.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// columnOrderFlip is a pair of columns declared in the opposite relative
// order on the two sides of a boundary: first precedes second in the new
// shape and followed it in the old one.
type columnOrderFlip struct {
	first, second string
}

func (f columnOrderFlip) String() string {
	return fmt.Sprintf("%q now before %q", f.first, f.second)
}

// columnOrderFlips returns every pair of columns present in both prior and
// post whose relative order differs, in post's order. A column on one side
// only is ignored: an added or dropped column is its own change, and a
// table that gained one has not reordered the others. nil when either side
// is nil.
func columnOrderFlips(prior, post *ir.Table) []columnOrderFlip {
	if prior == nil || post == nil {
		return nil
	}
	return flipsBetween(ir.ColumnNames(prior), ir.ColumnNames(post))
}

// flipsBetween is [columnOrderFlips] over name lists.
func flipsBetween(prior, post []string) []columnOrderFlip {
	if prior == nil || post == nil {
		return nil
	}
	priorPos := make(map[string]int, len(prior))
	for i, n := range prior {
		priorPos[n] = i
	}
	shared := make([]string, 0, len(post))
	for _, n := range post {
		if _, ok := priorPos[n]; ok {
			shared = append(shared, n)
		}
	}
	var flips []columnOrderFlip
	for i := range shared {
		for j := i + 1; j < len(shared); j++ {
			if priorPos[shared[i]] > priorPos[shared[j]] {
				flips = append(flips, columnOrderFlip{first: shared[i], second: shared[j]})
			}
		}
	}
	return flips
}

// columnsWhoseIdentityChanged returns the columns of post that kept a name
// of prior but not its stable column id (Postgres attnum,
// [ir.Column.StableID]; both sides must carry one): the source dropped the
// name's column and something else now holds the name — a DROP + ADD of the
// same name, or a DROP COLUMN b followed by RENAME COLUMN c TO b. Every such
// column refuses, with no exemption for an id that moved to another name: a
// permutation flips the declared order too and is refused for that first
// ([columnOrderFlips]); one that does not would still leave the target's
// copy of the name holding another column's values. In-process evidence
// only — the history codec does not persist stable ids, and a MySQL source
// has none, so the same shapes there are invisible (RENAME-SWAP-INVISIBLE).
func columnsWhoseIdentityChanged(prior, post *ir.Table) []string {
	priorCols := columnsByNameIR(prior)
	var out []string
	for _, c := range post.Columns {
		if c == nil || c.StableID == 0 {
			continue
		}
		if pc, ok := priorCols[c.Name]; ok && pc.StableID != 0 && pc.StableID != c.StableID {
			out = append(out, c.Name)
		}
	}
	return out
}

// namePosition is name's index in names: an exact match, else the one name
// that folds onto it case-insensitively (a MySQL target's identifiers).
func namePosition(names []string, name string) (int, bool) {
	folded, matches := 0, 0
	for i, n := range names {
		if n == name {
			return i, true
		}
		if strings.EqualFold(n, name) {
			folded, matches = i, matches+1
		}
	}
	return folded, matches == 1
}

// declaresBefore reports whether names holds both a and b, a first.
func declaresBefore(names []string, a, b string) bool {
	pa, okA := namePosition(names, a)
	pb, okB := namePosition(names, b)
	return okA && okB && pa < pb
}

// columnOrderVerdict is a boundary's column-order judgement: every flipped
// pair is either proven applied on the target, still in the old order there
// (unapplied: the operator's change is not on the target yet), or not
// provable at all (unprovable: no record, or a recorded target order that
// did not hold the old order — the target diverged before the change).
type columnOrderVerdict struct {
	flips      []columnOrderFlip
	unapplied  []columnOrderFlip
	unprovable []columnOrderFlip
}

func (v columnOrderVerdict) refuses() bool {
	return len(v.unapplied) > 0 || len(v.unprovable) > 0
}

// judgeColumnOrder judges post against the target's current column order.
// The flips are those of post against prior (the intercept's pre-state, the
// retained history, or nil) AND against the record's source side, so a
// change made while the stream was stopped is seen through the record even
// with no history; record is nil when the table has none.
func judgeColumnOrder(record *ir.ColumnOrderRecord, prior, target, post []string) columnOrderVerdict {
	var v columnOrderVerdict
	add := func(fs []columnOrderFlip) {
		for _, f := range fs {
			if !slices.Contains(v.flips, f) {
				v.flips = append(v.flips, f)
			}
		}
	}
	add(flipsBetween(prior, post))
	if record != nil {
		add(flipsBetween(record.SourceColumns, post))
	}
	for _, f := range v.flips {
		switch {
		case record == nil || !declaresBefore(record.TargetColumns, f.second, f.first):
			v.unprovable = append(v.unprovable, f)
		case !declaresBefore(target, f.first, f.second):
			v.unapplied = append(v.unapplied, f)
		}
	}
	return v
}

// columnOrderRecords is the stream's [ir.ColumnOrderRecord]s in memory:
// loaded at a warm resume, written at a cold start, and updated as the
// intercept attaches a changed record to an accepted boundary. available
// is false when the target keeps none — then nothing is attached, and
// every column-order change refuses as unprovable.
type columnOrderRecords struct {
	mu        sync.Mutex
	available bool
	byKey     map[[2]string]ir.ColumnOrderRecord
}

func newColumnOrderRecords(available bool, records []ir.ColumnOrderRecord) *columnOrderRecords {
	r := &columnOrderRecords{available: available, byKey: map[[2]string]ir.ColumnOrderRecord{}}
	for _, rec := range records {
		r.byKey[[2]string{rec.Schema, rec.Table}] = rec
	}
	return r
}

// lookup returns the table's record: the exact (schema, table) one, else
// the one record with that table name (a MySQL cold start names the table
// without its database, the change stream with it).
func (r *columnOrderRecords) lookup(schema, table string) (*ir.ColumnOrderRecord, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.byKey[[2]string{schema, table}]; ok {
		return &rec, true
	}
	var hit *ir.ColumnOrderRecord
	for k, rec := range r.byKey {
		if k[1] != table {
			continue
		}
		if hit != nil {
			return nil, false
		}
		rec := rec
		hit = &rec
	}
	return hit, hit != nil
}

func (r *columnOrderRecords) set(rec ir.ColumnOrderRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byKey[[2]string{rec.Schema, rec.Table}] = rec
}

func (r *columnOrderRecords) keeps() bool { return r != nil && r.available }

// judgeOrder is [judgeColumnOrder] for the boundary post (RAW, the
// snapshot's own IR) against this witness's target, deciding a refusal on a
// fresh catalog read (the memo may predate the operator's own ALTER). prior
// nil with no record makes the target its own prior: a stream with neither
// still sees a change the target does not hold, and cannot prove one.
func (w *firstBoundaryWitness) judgeOrder(ctx context.Context, prior, post *ir.Table) (columnOrderVerdict, error) {
	record, _ := w.orders.lookup(post.Schema, post.Name)
	judge := func(target *ir.Table) columnOrderVerdict {
		p := prior
		if p == nil && record == nil {
			p = target
		}
		return judgeColumnOrder(record, ir.ColumnNames(p), ir.ColumnNames(target), ir.ColumnNames(post))
	}
	target, _, err := w.catalog.lookup(ctx, post.Name)
	if err != nil {
		return columnOrderVerdict{}, err
	}
	if v := judge(target); !v.refuses() {
		return v, nil
	}
	if target, _, err = w.catalog.recheck(ctx, post.Name); err != nil {
		return columnOrderVerdict{}, err
	}
	return judge(target), nil
}

// attachColumnOrder returns snap carrying the table's new record when the
// source's column list has changed since the record (or there is none), so
// the applier writes it on the boundary's own transaction; snap unchanged
// otherwise, or when the target keeps no records or does not hold the
// table. The target is read fresh: the boundary may just have forwarded DDL.
func (w *firstBoundaryWitness) attachColumnOrder(ctx context.Context, snap ir.SchemaSnapshot) (ir.SchemaSnapshot, error) {
	if w == nil || !w.orders.keeps() || snap.IR == nil {
		return snap, nil
	}
	source := ir.ColumnNames(snap.IR)
	if rec, ok := w.orders.lookup(snap.Schema, snap.Table); ok && slices.Equal(rec.SourceColumns, source) {
		return snap, nil
	}
	target, held, err := w.catalog.recheck(ctx, snap.IR.Name)
	if err != nil {
		return snap, fmt.Errorf("read the target catalog to record the column order of %q: %w", snap.QualifiedName(), err)
	}
	if !held {
		return snap, nil
	}
	snap.TargetColumns = ir.ColumnNames(target)
	w.orders.set(ir.ColumnOrderRecord{Schema: snap.Schema, Table: snap.Table, SourceColumns: source, TargetColumns: snap.TargetColumns})
	return snap, nil
}

// columnOrderChangedError is the refusal of a boundary whose column order
// changed and is not proven on the target. It carries [ir.ErrColumnOrderChanged].
type columnOrderChangedError struct {
	flips []columnOrderFlip
}

func (e *columnOrderChangedError) Error() string {
	return fmt.Sprintf("%s: the source now declares columns in a different order (%s). A RENAME COLUMN swap or "+
		"rotation (`RENAME COLUMN b TO c, RENAME COLUMN c TO b` — the values stay put and the names move) and a "+
		"column reorder (`MODIFY … AFTER`/`FIRST` — the values move with the names) look identical in the "+
		"table's shape, and only the first leaves the target's existing rows under the wrong column names, so "+
		"sluice cannot apply this boundary without knowing which it was",
		ir.ErrColumnOrderChanged, renderColumnOrderFlips(e.flips))
}

func (e *columnOrderChangedError) Unwrap() error { return ir.ErrColumnOrderChanged }

// renderColumnOrderFlips lists the first few flips; a reorder of a wide
// table flips many pairs, and the first ones name the columns involved.
func renderColumnOrderFlips(flips []columnOrderFlip) string {
	const shown = 6
	parts := make([]string, 0, shown+1)
	for i, f := range flips {
		if i == shown {
			parts = append(parts, fmt.Sprintf("and %d more pair(s)", len(flips)-shown))
			break
		}
		parts = append(parts, f.String())
	}
	return strings.Join(parts, "; ")
}

// columnOrderRecopyRemedy is the remedy when the change cannot be proven on
// the target. `schema add-table` is NOT offered: it re-copies one table but
// keeps the stream's persisted position, so the change stream replays the
// rows written before this boundary — under the OLD column names — onto the
// re-copied table, which is the very misplacement this refusal prevents.
const columnOrderRecopyRemedy = "Re-copy the stream: `sluice sync start --reset-target-data --yes` with the stream's other flags unchanged, " +
	"which drops every table of the stream on the target and copies it again from a fresh snapshot (ADR-0023). " +
	"On a Postgres source, first drop the stream's replication slot via `sluice slot drop` — the re-copy opens a new " +
	"one and refuses while the old one exists."

// columnOrderRemedy is what the operator does about v, for a target that
// can (or cannot) reorder columns in place.
func columnOrderRemedy(v columnOrderVerdict, targetReorders bool) string {
	if len(v.unprovable) > 0 {
		return "sluice cannot prove the target was changed to match: it holds no record of this table's target " +
			"column order from before the change (a stream started before this release, or a target that cannot " +
			"keep sluice_cdc_column_order), or the recorded target order already disagreed with the source on " +
			"these columns (" + renderColumnOrderFlips(v.unprovable) + "). Renaming or reordering the target now " +
			"cannot be told apart from a target that was already that way. " + columnOrderRecopyRemedy
	}
	if targetReorders {
		return "Apply the source's change on the target yourself: the same RENAME COLUMN statements if the source " +
			"swapped or rotated names (do NOT update values instead — the check reads the target's column order), " +
			"or the same MODIFY … AFTER/FIRST if it reordered them (MySQL rebuilds the table to do it). Then " +
			"restart; the check passes once the target's order has moved to the source's."
	}
	return "If the source swapped or rotated names, apply the same RENAME COLUMN statements on the target (do NOT " +
		"update values instead — the check reads the target's column order), then restart. If it REORDERED " +
		"columns, this target cannot follow — it has no in-place column reorder — so " + columnOrderRecopyRemedy
}

// columnOrderRefusal is the forward path's refusal for tableName.
func columnOrderRefusal(tableName string, v columnOrderVerdict, targetReorders bool) error {
	return fmt.Errorf("%w on %q. "+refusedBeforeBoundary+" %s",
		&columnOrderChangedError{flips: v.flips}, tableName, columnOrderRemedy(v, targetReorders))
}

// withColumnOrder turns w into a refusal naming the flips, keeping the
// column differences it already found; the remedy rides the verdict.
func withColumnOrder(v witnessVerdict, order columnOrderVerdict, targetReorders bool) witnessVerdict {
	diffs := append([]witnessColumnDiff(nil), v.diffs...)
	for _, f := range append(append([]columnOrderFlip(nil), order.unprovable...), order.unapplied...) {
		diffs = append(diffs, witnessColumnDiff{
			column: f.first,
			source: fmt.Sprintf("now declared before %q (%s — a RENAME swap or a reorder)", f.second, ir.ErrColumnOrderChanged),
			target: fmt.Sprintf("not proven to have moved from after %q", f.second),
		})
	}
	return witnessVerdict{kind: witnessRefuse, diffs: diffs, remedy: columnOrderRemedy(order, targetReorders)}
}

// historyOrderPrior is the table's retained history version as the shape a
// first boundary's column order is compared with, or nil: none retained,
// or the boundary at pos is PROVEN to precede the version (a replay, whose
// older shape says nothing about what the source did since —
// [firstBoundaryWitness.historyPriorAt] has the argument). Unlike that
// narrowing proof this one does not need the boundary proven AFTER the
// version: a Postgres source can prove neither (its boundaries are anchored
// at 0/0), and a refusal is the outcome either way.
func (w *firstBoundaryWitness) historyOrderPrior(name string, pos ir.Position) *ir.Table {
	if w == nil {
		return nil
	}
	h := w.historyFor(name)
	if h == nil || provenBefore(w.orderer, pos, w.retained[h].anchor) {
		return nil
	}
	return h
}

// routeColumnOrderBoundary decides a forward-path boundary whose column
// order changed against the intercept's own pre-state (the cold-start seed
// or an earlier snapshot), or against the table's record. It refuses
// unless every flipped pair is proven moved on the target, in which case
// the boundary is checked against the target like a first boundary, and the
// pre-state that check forwarded against is returned (nil when it forwarded
// nothing).
func routeColumnOrderBoundary(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	pre, post *ir.Table,
	snap ir.SchemaSnapshot,
) (*ir.Table, error) {
	if deps.witness == nil {
		flips := columnOrderFlips(pre, post)
		return nil, columnOrderRefusal(tableName, columnOrderVerdict{flips: flips, unprovable: flips}, false)
	}
	v, err := deps.witness.judgeOrder(ctx, pre, snap.IR)
	if err != nil {
		return nil, fmt.Errorf("read the target catalog to check the column order of %q "+
			"(the boundary is not accepted unchecked): %w", tableName, err)
	}
	if v.refuses() {
		return nil, columnOrderRefusal(tableName, v, deps.witness.targetReorders)
	}
	slog.InfoContext(ctx,
		"schema-forward: the source changed a table's column order and the target's order has moved from its "+
			"recorded order to the source's (the change was applied there); checking the rest of the boundary "+
			"against the target",
		"table", tableName, "columns", renderColumnOrderFlips(v.flips))
	return routeWitnessedBoundary(ctx, deps, tableName, post, snap)
}

// columnOrderChangeSeen reports whether the boundary flips a column pair
// against the intercept's pre-state or the table's record (both pre and
// post in comparison form; snap the raw snapshot).
func columnOrderChangeSeen(w *firstBoundaryWitness, pre, post *ir.Table, snap ir.SchemaSnapshot) bool {
	if len(columnOrderFlips(pre, post)) > 0 {
		return true
	}
	if w == nil || snap.IR == nil {
		return false
	}
	rec, ok := w.orders.lookup(snap.Schema, snap.Table)
	return ok && len(flipsBetween(rec.SourceColumns, ir.ColumnNames(snap.IR))) > 0
}

// refuseUnheldColumnOrder is the unforwarded-stream check's column-order
// rule ([judgeUnforwardedBoundary]): prior is the table's last accepted
// shape, or its cold-start read, or nil when the stream knows none; post is
// the boundary's. Nothing is forwarded on such a stream, so the only way
// through is the operator applying the change on the target, which the
// target's order and the table's record prove.
func refuseUnheldColumnOrder(
	ctx context.Context,
	w *firstBoundaryWitness,
	tableName string,
	prior, post *ir.Table,
	deps unforwardedBoundaryDeps,
) error {
	if post == nil {
		return nil
	}
	v, err := w.judgeOrder(ctx, prior, post)
	if err != nil {
		return fmt.Errorf("read the target catalog to check the column order of %q "+
			"(the boundary is not accepted unchecked): %w", tableName, err)
	}
	if !v.refuses() {
		return nil
	}
	return fmt.Errorf("%w: %w on %q, and this stream does not forward schema changes (%s). "+
		refusedBeforeBoundary+" %s",
		ir.ErrSchemaChangeRefused, &columnOrderChangedError{flips: v.flips}, tableName, deps.why,
		columnOrderRemedy(v, w.targetReorders))
}

// phaseLoadColumnOrders gives the stream its column-order records before
// the intercepts are wired: after a cold start, the target's order of every
// table the cold start created or adopted, written through the applier;
// otherwise the records the stream already has. A target that keeps none
// is a WARN, not a failure: every column-order change on it then refuses as
// unprovable, which is the safe direction.
func (s *Streamer) phaseLoadColumnOrders(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	store, ok := applier.(ir.ColumnOrderStore)
	if !ok {
		s.columnOrders = newColumnOrderRecords(false, nil)
		return nil
	}
	if len(s.unforwardedColdStartPrior) == 0 {
		records, err := store.ReadColumnOrders(ctx, streamID)
		return s.settleColumnOrders(ctx, streamID, records, err)
	}
	records, err := s.coldStartColumnOrders(ctx)
	if err != nil {
		return err
	}
	if err := store.WriteColumnOrders(ctx, streamID, records); err != nil {
		return s.settleColumnOrders(ctx, streamID, nil, err)
	}
	return s.settleColumnOrders(ctx, streamID, records, nil)
}

// settleColumnOrders installs the records read or written, turning an
// unavailable store into the WARN.
func (s *Streamer) settleColumnOrders(ctx context.Context, streamID string, records []ir.ColumnOrderRecord, err error) error {
	switch {
	case errors.Is(err, ir.ErrColumnOrderStoreUnavailable):
		slog.WarnContext(ctx,
			"the target cannot keep column-order records ("+ir.ErrColumnOrderStoreUnavailable.Error()+"); a "+
				"source change to a table's column order (a RENAME swap or rotation, or a reorder) will refuse "+
				"on this stream rather than be accepted, even after you apply it on the target",
			"stream_id", streamID, "reason", err.Error())
		s.columnOrders = newColumnOrderRecords(false, nil)
		return nil
	case err != nil:
		return fmt.Errorf("pipeline: column-order records: %w", err)
	}
	s.columnOrders = newColumnOrderRecords(true, records)
	return nil
}

// coldStartColumnOrders reads the target's column order for every table
// the cold start created or adopted, paired with the source's from the
// same raw read the cold start copied from.
func (s *Streamer) coldStartColumnOrders(ctx context.Context) ([]ir.ColumnOrderRecord, error) {
	catalogs := map[string]map[string]*ir.Table{}
	read := func(namespace string) (map[string]*ir.Table, error) {
		if c, ok := catalogs[namespace]; ok {
			return c, nil
		}
		var (
			c   map[string]*ir.Table
			err error
		)
		if s.multiDatabaseMode() {
			c, err = s.readNamespaceTargetCatalog(ctx, namespace, s.namespaceTargetDeriver)
		} else {
			c, err = s.loadTargetZoneWitness(ctx)
		}
		if err != nil {
			return nil, fmt.Errorf("pipeline: read the target catalog to record column orders: %w", err)
		}
		catalogs[namespace] = c
		return c, nil
	}
	out := make([]ir.ColumnOrderRecord, 0, len(s.unforwardedColdStartPrior))
	for _, snap := range s.unforwardedColdStartPrior {
		if snap.IR == nil {
			continue
		}
		catalog, err := read(snap.Schema)
		if err != nil {
			return nil, err
		}
		target, held := newTargetCatalogWitness(nil, catalog).find(snap.Table)
		if !held {
			continue
		}
		out = append(out, ir.ColumnOrderRecord{
			Schema: snap.Schema, Table: snap.Table,
			SourceColumns: ir.ColumnNames(snap.IR), TargetColumns: ir.ColumnNames(target),
		})
	}
	return out, nil
}
