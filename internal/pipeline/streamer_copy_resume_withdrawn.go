package pipeline

import (
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
