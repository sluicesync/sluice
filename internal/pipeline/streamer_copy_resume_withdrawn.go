package pipeline

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// copyResumeWithdrawnMarker names the refusal of a persisted position that
// carries a mid-COPY cursor (VSTREAM-COPY-RESUME-RECREATED-TABLE).
//
// Resuming a stopped VStream copy from its cursor cannot make the target
// match the source. On a resume, Vitess (vttablet's uvstreamer,
// buildTablePlan and shouldSendEventForTable) copies again from the start
// every in-scope table that has no cursor, including the tables the
// stopped run had already finished, and drops their changes from the
// stopped window, as for a table not yet copied. The table holding the
// cursor gets changes only for rows at or below it, and the target can
// hold rows above it (the checkpoint trails what was written). The
// re-copy writes by upsert, which removes nothing, so every row the
// source deleted while the stream was stopped stays on the target, and
// so does every old row of a table dropped and re-created under its name.
// Measured on Vitess 24: 6,701 rows on the target against 701 on the
// source, and a stopped-window DELETE of 20 rows that survived.
//
// So since v0.163.3 the cold start persists no mid-COPY checkpoint, and a
// cursor-bearing position, which only a release before that can have
// written, is refused here rather than resumed. A re-copy with
// --restart-from-scratch clears the in-scope target tables first
// ([freshCopyReason.clearsTarget]), so it cannot keep a stale row.
const copyResumeWithdrawnMarker = "VSTREAM-COPY-RESUME-WITHDRAWN"

// restartUnclearedMarker names the refusal of a re-copy that was asked to
// clear the target (--restart-from-scratch) but cannot, because
// --schema-already-applied forbids the drop-and-recreate the clear is.
const restartUnclearedMarker = "RESTART-FROM-SCRATCH-TARGET-NOT-CLEARED"

// refuseUnclearedRestart refuses --restart-from-scratch combined with
// --schema-already-applied on an upsert reader (VStream) while an in-scope
// target table still holds rows. That cold start takes the
// schema-already-applied branch, which skips the clear, and the upsert copy
// then keeps every target row the source has deleted since it was copied,
// silently — the harm VSTREAM-COPY-RESUME-WITHDRAWN names --restart-from-scratch
// as the remedy for. A target whose emptiness cannot be probed is refused
// too: the clear cannot be proven to be unnecessary. With the tables
// emptied by hand, the same command copies into them correctly.
func refuseUnclearedRestart(ctx context.Context, schema *ir.Schema, rw ir.RowWriter) error {
	checker, ok := rw.(ir.TableEmptyChecker)
	if !ok {
		return fmt.Errorf("pipeline: cold start refused: %s: --restart-from-scratch with --schema-already-applied "+
			"cannot clear the target (that would drop the tables you pre-created), and this target cannot report "+
			"whether its tables are empty; empty the in-scope target tables yourself, then re-run the same command",
			restartUnclearedMarker)
	}
	for _, table := range schema.Tables {
		empty, err := checker.IsTableEmpty(ctx, table)
		if err != nil {
			return fmt.Errorf("pipeline: %s: probe of target table %q: %w", restartUnclearedMarker, table.Name, err)
		}
		if !empty {
			return fmt.Errorf("pipeline: cold start refused: %s: --restart-from-scratch with --schema-already-applied "+
				"does not clear the target (that would drop the tables you pre-created), and target table %q still "+
				"holds rows: an upsert re-copy over them would keep every row the source has deleted since they were "+
				"copied. Empty the in-scope target tables yourself (e.g. TRUNCATE), then re-run the same command; "+
				"no target table was changed by this run", restartUnclearedMarker, table.Name)
		}
	}
	return nil
}

// copyResumeWithdrawnError carries [ir.ErrIncludeTableScopeDrift], as
// COLD-START-INCOMPLETE does: both refuse a stream whose cold start
// stopped before it finished, a restart reads the same position back, and
// the supervisor must not retry it ([refusalsARestartRepeats]).
func copyResumeWithdrawnError(streamID string) error {
	return fmt.Errorf("pipeline: resume refused: %w: %s: stream %q stopped during its cold start's copy, and the "+
		"persisted position is that copy's mid-table cursor, written by a sluice release before v0.163.3. Resuming "+
		"a VStream copy from its cursor keeps every row the source deleted while the stream was stopped (the tables "+
		"already copied are copied again by upsert, which removes nothing), so this release no longer resumes one; "+
		"nothing has been applied by this run. Re-copy the stream: the same `sluice sync start` command with "+
		"--restart-from-scratch, which clears the in-scope target tables before copying them",
		ir.ErrIncludeTableScopeDrift, copyResumeWithdrawnMarker, streamID)
}
