// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// The added table's replay window (Bug 333; the class is described at
// [ir.AddedTableReplayWindow]). `schema add-table` records, for a source that
// declares [ir.AddedTableReplayWindowSource], the position its copy is
// complete at ([AddTable.recordReplayWindow]); the restarted stream applies
// the table's changes up to that position through [interceptAddedTableReplay],
// which rewrites each into a form that converges on a target already holding
// the copy, and past it lets every change through untouched.
//
// The rewrite, per change of a table whose window is open:
//
//	INSERT                 → as it is: every target applier upserts an INSERT
//	UPDATE, key unchanged  → INSERT of the after-image (an upsert)
//	UPDATE, key changed    → DELETE of the old key, then INSERT of the after-image
//	DELETE                 → DELETE addressed by the key alone
//
// Nothing in the window is compare-and-set any more, so a replayed change can
// no longer miss a row it should have moved (the silent VStream arm), and a
// key change no longer moves whatever row sits at its old key (the wedges).
// A DELETE by key can still affect zero rows, but only when the row is already
// absent, which is the state the DELETE leaves anyway: the window has no
// silent miss to report.

// Markers of the window's operator-facing lines.
const (
	// AddedTableReplayWindowMarker names the INFO lines that open and close
	// a table's window.
	AddedTableReplayWindowMarker = "ADDED-TABLE-REPLAY-WINDOW"
	// AddedTableCopyPositionUnrecordedMarker names the one-time WARN for an
	// added table whose add recorded no copy position (an add by sluice
	// v0.163.10 or earlier): its restart replays as those versions did.
	AddedTableCopyPositionUnrecordedMarker = "ADDED-TABLE-COPY-POSITION-UNRECORDED"
	// AddedTableReplayImageIncompleteMarker names the refusal of a change
	// in a window whose images do not carry the table's key, or whose after
	// image is empty: the idempotent form cannot be built from it.
	AddedTableReplayImageIncompleteMarker = "ADDED-TABLE-REPLAY-IMAGE-INCOMPLETE"
	// AddedTableCopyPositionUnorderedMarker names the refusal of a change
	// whose position the source engine cannot order against the copy's.
	AddedTableCopyPositionUnorderedMarker = "ADDED-TABLE-COPY-POSITION-UNORDERED"
)

// addedTableReplayKey is the key the window addresses rows by: the primary
// key, else the deterministic non-null unique key the copy's idempotent
// writer upserts on (every column NOT NULL, no expression, no predicate;
// fewest columns, then the smallest index name — the rule both engines'
// effectiveUpsertKeyColumns apply). nil for a table with neither, which the
// copy has already refused (errKeylessIdempotent).
func addedTableReplayKey(t *ir.Table) []string {
	if pk := migcore.TablePKColumns(t); len(pk) > 0 {
		return pk
	}
	if t == nil {
		return nil
	}
	notNull := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		if c != nil && !c.Nullable {
			notNull[c.Name] = true
		}
	}
	var best *ir.Index
	for _, idx := range t.Indexes {
		if idx == nil || !idx.Unique || len(idx.Columns) == 0 || strings.TrimSpace(idx.Predicate) != "" {
			continue
		}
		eligible := true
		for _, c := range idx.Columns {
			if c.Expression != "" || !notNull[c.Column] {
				eligible = false
				break
			}
		}
		if eligible && (best == nil || len(idx.Columns) < len(best.Columns) ||
			(len(idx.Columns) == len(best.Columns) && idx.Name < best.Name)) {
			best = idx
		}
	}
	if best == nil {
		return nil
	}
	out := make([]string, len(best.Columns))
	for i, c := range best.Columns {
		out[i] = c.Column
	}
	return out
}

// prepareReplayWindow asks the target, before the add changes anything,
// whether it can record the table's replay window, so a target that cannot
// (a control table this role cannot ALTER) refuses the add here instead of
// after its copy. A source that needs no window asks nothing.
func (a *AddTable) prepareReplayWindow(ctx context.Context, applier ir.ChangeApplier) error {
	if _, ok := a.Source.(ir.AddedTableReplayWindowSource); !ok {
		return nil
	}
	store, ok := applier.(ir.AddedTableReplayWindowStore)
	if !ok {
		return a.noReplayWindowStore()
	}
	if err := store.PrepareAddedTableReplayWindows(ctx, a.StreamID); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
			"pipeline: add-table: the target cannot record the position %q's copy will be complete at, which the "+
				"restarted stream needs (%s; nothing was changed): %w", a.TableName, AddedTableReplayWindowMarker, err,
		))
	}
	return nil
}

// noReplayWindowStore is the refusal for a target applier without
// [ir.AddedTableReplayWindowStore] (none ships; both CDC targets have it).
func (a *AddTable) noReplayWindowStore() error {
	return fmt.Errorf("pipeline: add-table: the %s target cannot record the position %q's copy is complete at, which "+
		"the restarted stream needs to replay the table's changes onto the copy safely (%s); bring the table in with a "+
		"re-copy instead (`sync start --restart-from-scratch` with --include-table naming it)",
		a.Target.Name(), a.TableName, AddedTableReplayWindowMarker)
}

// recordReplayWindow records the table's replay window once its copy has
// landed, before the add is recorded complete: from the source's declaration
// ([ir.AddedTableReplayWindowSource]) on, a completed add without its window
// is exactly the unguarded replay Bug 333 lost rows to, so every way the
// record can fail refuses the add, and the pending entry stays (the next
// start refuses INCLUDE-TABLE-SCOPE-DRIFT rather than replay unguarded). A
// source that does not declare it needs no window and records nothing.
func (a *AddTable) recordReplayWindow(ctx context.Context, applier ir.ChangeApplier, stream *ir.SnapshotStream, table *ir.Table) error {
	src, ok := a.Source.(ir.AddedTableReplayWindowSource)
	if !ok {
		return nil
	}
	store, ok := applier.(ir.AddedTableReplayWindowStore)
	if !ok {
		return a.noReplayWindowStore()
	}
	// The VStream copy writes its final position at the stream-wide
	// COPY_COMPLETED; the barrier orders this read after that write.
	if err := stream.WaitCopyComplete(ctx); err != nil {
		return fmt.Errorf("pipeline: add-table: wait for the copy of %q to record its position: %w", a.TableName, err)
	}
	if stream.Position.Token == "" {
		return fmt.Errorf("pipeline: add-table: the copy of %q reported no source position, so the restarted stream "+
			"could not tell which of the table's changes the copy already holds (%s); nothing is recorded complete — "+
			"re-run the add", a.TableName, AddedTableReplayWindowMarker)
	}
	resumeFrom, found, err := applier.ReadPosition(ctx, a.StreamID)
	if err != nil || !found {
		return fmt.Errorf("pipeline: add-table: read the position the stopped stream resumes from, which bounds %q's "+
			"replay window (%s; found=%v): %w", a.TableName, AddedTableReplayWindowMarker, found, err)
	}
	// The target stamps a recovered position with its own engine's name; the
	// source decodes it (as runOnce does before resuming from it).
	copyAt, err := src.AddedTableCopyPosition(stream.Position, retagPositionForSource(resumeFrom, a.Source.Name()))
	if err != nil {
		return fmt.Errorf("pipeline: add-table: bound %q's replay window (%s): %w", a.TableName, AddedTableReplayWindowMarker, err)
	}
	key := addedTableReplayKey(table)
	if len(key) == 0 {
		return fmt.Errorf("pipeline: add-table: %q has no primary key and no non-null unique key, so its replay "+
			"cannot address rows by key (%s)", a.TableName, AddedTableReplayWindowMarker)
	}
	var columns []string
	for _, c := range table.Columns {
		if c != nil && !c.IsGenerated() {
			columns = append(columns, c.Name)
		}
	}
	w := ir.AddedTableReplayWindow{Table: a.TableName, CopyPosition: copyAt, Key: key, Columns: columns}
	if err := store.RecordAddedTableReplayWindows(ctx, a.StreamID, []ir.AddedTableReplayWindow{w}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
			"pipeline: add-table: record the position %q's copy is complete at (sluice_cdc_state.added_table_copy_positions): %w",
			a.TableName, err,
		))
	}
	slog.InfoContext(ctx, "add-table: recorded the position the copy is complete at; the restarted stream replays the "+
		"table's changes up to it idempotently, then applies normally ("+AddedTableReplayWindowMarker+")",
		slog.String("table", a.TableName), slog.String("key", strings.Join(key, ",")))
	return nil
}

// replayWindow is one open window during a run: the recorded window, the
// lower-cased key and column sets, and whether a change past the copy's
// position has been seen (positions only grow, so the window then stays
// closed). Owned by the one intercept goroutine of a wiring; a reshard
// re-wiring starts after the previous one has exited.
type replayWindow struct {
	ir.AddedTableReplayWindow
	key     map[string]bool
	columns map[string]bool
	closed  bool
}

// newReplayWindow indexes w's key and columns, lower-cased (an image keeps
// its own spelling; the engines fold identifiers).
func newReplayWindow(w ir.AddedTableReplayWindow) *replayWindow {
	rw := &replayWindow{AddedTableReplayWindow: w, key: map[string]bool{}, columns: map[string]bool{}}
	for _, k := range w.Key {
		rw.key[strings.ToLower(k)] = true
	}
	for _, c := range w.Columns {
		rw.columns[strings.ToLower(c)] = true
	}
	return rw
}

// replayWindows is a run's open windows, keyed by bare table name (the
// live-added set's own key: [scopeAdmits] matches the unqualified name).
type replayWindows struct {
	byTable map[string]*replayWindow
	orderer ir.PositionOrderer
}

// phaseLoadAddedTableReplayWindows reads the stream's windows at a warm
// resume and keeps the ones still open: those whose copy position the
// persisted position does not yet cover. A cold start records none (its copy
// re-seeds every table, [Streamer.resetReplayWindowsAfterColdStart]). It
// also reports, once, each live-added table with no record at all
// ([AddedTableCopyPositionUnrecordedMarker]).
func (s *Streamer) phaseLoadAddedTableReplayWindows(ctx context.Context, applier ir.ChangeApplier, streamID string, warmResumed bool, persisted ir.Position) error {
	s.replayWindows = nil
	s.replayWindowErr.Store(nil)
	src, ok := s.Source.(ir.AddedTableReplayWindowSource)
	if !ok || !warmResumed {
		return nil
	}
	store, ok := applier.(ir.AddedTableReplayWindowStore)
	if !ok {
		return nil
	}
	recorded, err := store.ReadAddedTableReplayWindows(ctx, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: read the stream's added-table copy positions: %w", err))
	}
	s.noteUnrecordedAddedTables(ctx, store, streamID, recorded)
	open := &replayWindows{byTable: map[string]*replayWindow{}, orderer: src}
	for _, w := range recorded {
		if !w.HasWindow() {
			continue
		}
		covered, err := src.PositionAtOrAfter(persisted, w.CopyPosition)
		if err != nil {
			return fmt.Errorf("pipeline: %s: the stream's position cannot be ordered against the position %q's copy is "+
				"complete at, so this start cannot tell whether the table's replay is finished; re-copy the stream with "+
				"`sync start --restart-from-scratch`: %w", AddedTableCopyPositionUnorderedMarker, w.Table, err)
		}
		if covered {
			continue
		}
		open.byTable[w.Table] = newReplayWindow(w)
		slog.InfoContext(ctx, AddedTableReplayWindowMarker+": the stream resumes from before this added table's copy; its "+
			"changes up to the copy's position are applied idempotently (upsert of the after-image, delete by key), then "+
			"normally", slog.String("stream_id", streamID), slog.String("table", w.Table), slog.String("key", strings.Join(w.Key, ",")))
	}
	if len(open.byTable) > 0 {
		s.replayWindows = open
	}
	return nil
}

// noteUnrecordedAddedTables WARNs, once per table, for a live-added table the
// stream has no copy-position entry for — an add by a release that did not
// record one — and records that it has none, so later starts stay quiet. The
// record is best-effort: a failure WARNs and the next start reports again.
func (s *Streamer) noteUnrecordedAddedTables(ctx context.Context, store ir.AddedTableReplayWindowStore, streamID string, recorded []ir.AddedTableReplayWindow) {
	have := make(map[string]bool, len(recorded))
	for _, w := range recorded {
		have[w.Table] = true
	}
	var missing []string
	for _, t := range s.startLiveAdded {
		if !have[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)
	slog.WarnContext(ctx, AddedTableCopyPositionUnrecordedMarker+": these tables were added by a sluice release that "+
		"recorded no copy position (v0.163.10 or earlier), so this start cannot bound their replay: a change made around "+
		"their add replays as it did then, which a primary-key change or delete during the add could leave diverged "+
		"(Bug 333). Compare each with the source; `sync start --restart-from-scratch` re-copies the stream. Reported once",
		slog.String("stream_id", streamID), slog.String("tables", strings.Join(missing, ", ")))
	none := make([]ir.AddedTableReplayWindow, len(missing))
	for i, t := range missing {
		none[i] = ir.AddedTableReplayWindow{Table: t}
	}
	if err := store.RecordAddedTableReplayWindows(ctx, streamID, none); err != nil {
		slog.WarnContext(ctx, "could not record that these added tables have no copy position; the next start reports them again",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
	}
}

// resetReplayWindowsAfterColdStart replaces the stream's windows once a cold
// start has copied its tables: the copy re-seeded every table, and its stream
// starts at the copy's own position, so no window is owed. The live-added
// tables it copied are recorded as having none, so a later start does not
// report them as unrecorded. A failure WARNs: a stale window left behind is
// covered by the new stream's position, and so never opens.
func (s *Streamer) resetReplayWindowsAfterColdStart(ctx context.Context, applier ir.ChangeApplier, streamID string) {
	if _, ok := s.Source.(ir.AddedTableReplayWindowSource); !ok {
		return
	}
	store, ok := applier.(ir.AddedTableReplayWindowStore)
	if !ok {
		return
	}
	none := make([]ir.AddedTableReplayWindow, 0, len(s.startLiveAdded))
	for _, t := range s.startLiveAdded {
		none = append(none, ir.AddedTableReplayWindow{Table: t})
	}
	if err := store.ResetAddedTableReplayWindows(ctx, streamID, none); err != nil {
		slog.WarnContext(ctx, "cold start: could not reset the stream's added-table copy positions",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
	}
}

// interceptAddedTableReplay is the window's stage: a verbatim pass-through
// when no window is open, otherwise each change of a table whose window is
// open is rewritten ([replayWindows.route]). A refusal is stored in errStore
// and closes the channel, like every intercept.
func interceptAddedTableReplay(ctx context.Context, in <-chan ir.Change, windows *replayWindows, errStore *atomic.Pointer[error]) <-chan ir.Change {
	if windows == nil || len(windows.byTable) == 0 {
		return in
	}
	out := make(chan ir.Change, migcore.RowChanBuffer)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case c, ok := <-in:
				if !ok {
					return
				}
				emit, err := windows.route(ctx, c)
				if err != nil {
					errStore.Store(&err)
					return
				}
				for _, e := range emit {
					if !forwardChange(ctx, out, e) {
						return
					}
				}
			}
		}
	}()
	return out
}

// route returns the change(s) to forward for c.
func (r *replayWindows) route(ctx context.Context, c ir.Change) ([]ir.Change, error) {
	switch c.(type) {
	case ir.Insert, ir.Update, ir.Delete:
	default:
		return []ir.Change{c}, nil
	}
	w := r.byTable[ir.UnqualifiedTableName(c.QualifiedName())]
	if w == nil || w.closed {
		return []ir.Change{c}, nil
	}
	open, err := r.inWindow(w, c.Pos())
	if err != nil {
		return nil, err
	}
	if !open {
		w.closed = true
		slog.InfoContext(ctx, AddedTableReplayWindowMarker+": the stream is past this added table's copy position; its "+
			"changes are applied normally from here", slog.String("table", w.Table))
		return []ir.Change{c}, nil
	}
	return w.rewrite(c)
}

// inWindow reports whether a change at pos is one the copy may already hold:
// true unless pos is provably at or after the copy's position. Rows carry the
// position from before their own transaction ([ir.AddedTableReplayWindowSource]),
// so a transaction the copy holds is never at or after it, and the first one
// past it is. An unordered pair (another shard still behind the copy, say)
// stays in the window, which is safe: the idempotent form converges whatever
// the target holds, so applying it to a change past the copy is merely
// slower. A change with no position stays in it too.
func (r *replayWindows) inWindow(w *replayWindow, pos ir.Position) (bool, error) {
	if pos.Token == "" {
		return true, nil
	}
	past, err := r.orderer.PositionAtOrAfter(pos, w.CopyPosition)
	if err != nil {
		return false, fmt.Errorf("pipeline: %s: a change to added table %q cannot be ordered against the position its "+
			"copy is complete at, so it cannot be applied safely; re-copy the stream with `sync start "+
			"--restart-from-scratch`: %w", AddedTableCopyPositionUnorderedMarker, w.Table, err)
	}
	return !past, nil
}

// rewrite is the idempotent form of one row change in an open window.
func (w *replayWindow) rewrite(c ir.Change) ([]ir.Change, error) {
	switch v := c.(type) {
	case ir.Insert:
		if err := w.requireKey("INSERT", "row", v.Row); err != nil {
			return nil, err
		}
		return []ir.Change{v}, nil
	case ir.Update:
		if err := w.requireKey("UPDATE", "before", v.Before); err != nil {
			return nil, err
		}
		if err := w.requireKey("UPDATE", "after", v.After); err != nil {
			return nil, err
		}
		keyKept := w.keyUnchanged(v.Before, v.After)
		if missing := w.missingColumns(v.After); len(missing) > 0 {
			return w.partialUpdate(v, keyKept, missing)
		}
		upsert := ir.Insert{Position: v.Position, Schema: v.Schema, Table: v.Table, Row: v.After, CommitTime: v.CommitTime, ApplyID: v.ApplyID}
		if keyKept {
			return []ir.Change{upsert}, nil
		}
		// The old key's delete carries no apply identity: it is not a change
		// the source transaction numbered, and a zero identity is never
		// skipped, which is the at-least-once a window wants.
		del := ir.Delete{Position: v.Position, Schema: v.Schema, Table: v.Table, Before: w.keyOnly(v.Before), CommitTime: v.CommitTime}
		return []ir.Change{del, upsert}, nil
	case ir.Delete:
		if err := w.requireKey("DELETE", "before", v.Before); err != nil {
			return nil, err
		}
		v.Before = w.keyOnly(v.Before)
		return []ir.Change{v}, nil
	}
	return []ir.Change{c}, nil
}

// missingColumns lists, sorted, the table's recorded columns an after-image
// does not carry.
func (w *replayWindow) missingColumns(after ir.Row) []string {
	if len(w.columns) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(after))
	for name := range after {
		seen[strings.ToLower(name)] = true
	}
	var missing []string
	for c := range w.columns {
		if !seen[c] {
			missing = append(missing, c)
		}
	}
	sort.Strings(missing)
	return missing
}

// partialUpdate is the window's answer to an UPDATE whose after-image is
// partial — on Postgres, pgoutput's unchanged-TOAST omission ('u'), which
// REPLICA IDENTITY FULL does not lift on an unfiltered table. An upsert of
// it would write the omitted columns as NULL wherever it inserts.
//
// A key-preserving one is replayed as a key-addressed UPDATE of the columns
// it carries, which converges: the omitted columns are unchanged by it, so
// their value at the copy's position is whatever the row held before it,
// and the target holds exactly that — the copy's value, or the last earlier
// replayed change's that wrote them in full (an omitted column is one that
// did not change). Its row exists when it is replayed: the source row did,
// and every earlier change that removed the key on the target (a delete, a
// key change away) was followed on the source, and so in the replay, by the
// one that put it back. If the copy holds no row at the key, a later change
// removed it, and the zero-row miss is the right outcome.
//
// A key-changing one cannot be replayed safely: the new row needs the
// omitted values and nothing in the change carries them, while moving the
// target's row at the old key is the very key-addressed replay that lost
// rows (Bug 336: the row at the old key need not be this one). It refuses.
func (w *replayWindow) partialUpdate(v ir.Update, keyKept bool, missing []string) ([]ir.Change, error) {
	if keyKept {
		v.Before = w.keyOnly(v.Before)
		return []ir.Change{v}, nil
	}
	return nil, fmt.Errorf("pipeline: %s: a primary-key change to added table %q, inside the replay up to its copy's "+
		"position, arrived without column(s) %s in its new row (Postgres omits an unchanged TOASTed value), so neither "+
		"form of it is safe: an upsert would store them as NULL, and moving the row at the old key can move a "+
		"different row. The stream stopped rather than guess. Re-copy: with the stream stopped, run "+
		"`sluice sync start --source-driver=... --source=... --target-driver=... --target=... --stream-id=... "+
		"--restart-from-scratch` (it re-copies every table of the stream)",
		AddedTableReplayImageIncompleteMarker, w.Table, strings.Join(missing, ", "))
}

// requireKey refuses an image that does not carry every key column: the
// idempotent form addresses the row by them, and a row it cannot address is
// a change it cannot apply. The readers that declare a window refuse partial
// images before here ([ir.AddedTableReplayWindowSource]); this is the belt.
func (w *replayWindow) requireKey(op, image string, row ir.Row) error {
	seen := make(map[string]bool, len(w.key))
	for name := range row {
		if lower := strings.ToLower(name); w.key[lower] {
			seen[lower] = true
		}
	}
	if len(seen) == len(w.key) {
		return nil
	}
	var missing []string
	for k := range w.key {
		if !seen[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	return fmt.Errorf("pipeline: %s: a change (%s) to added table %q, inside the replay up to its copy's position, has a %s image "+
		"without key column(s) %s, so it cannot be applied idempotently; the stream stopped rather than apply it as a "+
		"compare-and-set that may miss the row silently. Re-copy the stream with `sync start --restart-from-scratch`",
		AddedTableReplayImageIncompleteMarker, op, w.Table, image, strings.Join(missing, ", "))
}

// keyOnly narrows an image to the key columns, keeping the image's own
// spelling of each (the applier binds the names the target knows).
func (w *replayWindow) keyOnly(row ir.Row) ir.Row {
	out := make(ir.Row, len(w.key))
	for name, v := range row {
		if w.key[strings.ToLower(name)] {
			out[name] = v
		}
	}
	return out
}

// keyUnchanged reports whether every key column holds the same value before
// and after. Any doubt reads as changed: a delete of the old key plus an
// upsert of the new row is right whether or not the key moved, while an
// upsert alone leaves the old row behind when it did.
func (w *replayWindow) keyUnchanged(before, after ir.Row) bool {
	lowerAfter := make(map[string]any, len(w.key))
	for name, v := range after {
		if lower := strings.ToLower(name); w.key[lower] {
			lowerAfter[lower] = v
		}
	}
	for name, b := range before {
		lower := strings.ToLower(name)
		if !w.key[lower] {
			continue
		}
		a, ok := lowerAfter[lower]
		if !ok || !sameKeyValue(b, a) {
			return false
		}
	}
	return true
}

// sameKeyValue compares two decoded key values: bytes by content, times by
// instant, everything else by deep equality (a different Go type is a
// different value — the safe answer here).
func sameKeyValue(a, b any) bool {
	switch av := a.(type) {
	case []byte:
		bv, ok := b.([]byte)
		return ok && bytes.Equal(av, bv)
	case time.Time:
		bv, ok := b.(time.Time)
		return ok && av.Equal(bv)
	}
	return reflect.DeepEqual(a, b)
}
