// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Audit F-E1, the in-run sibling of the replay doors. flushWithReparentRetry
// used to license a re-send with the RECORDED-schema predicate alone
// (irbackup.TableReplayIdempotent). A recorded table keyed on `id` copied
// into a target keyed only on an AUTO_INCREMENT surrogate `sid` passed that
// check — and a re-sent batch whose first attempt committed before its
// acknowledgement was lost drew fresh `sid` values, collided with nothing,
// and doubled the batch at exit 0. The gate now asks the replay doors'
// predicate (irbackup.JudgeReplayKey through this writer's ProbeReplayKey).
//
// The oracle is the DRIVER's exec count — the second statement never
// reaching the wire — and the scripted catalog's probe count, not the
// writer's own report. These pins reach all three cores that ride
// flushWithReparentRetry; the real-server committed-but-ack-lost pin is
// TestColdCopyRetry_SurrogateKeyedTarget_AckLostRefuses (integration).

// writeCore is one of the three bulk-write cores behind flushWithReparentRetry.
type writeCore struct {
	name  string
	bulk  ir.BulkLoadMethod
	write func(w *RowWriter, ctx context.Context, table *ir.Table, rows <-chan ir.Row) error
}

var reparentRetryCores = []writeCore{
	{"batched INSERT", ir.BulkLoadBatchedInsert, func(w *RowWriter, ctx context.Context, t *ir.Table, r <-chan ir.Row) error {
		return w.WriteRows(ctx, t, r)
	}},
	{"idempotent upsert", ir.BulkLoadBatchedInsert, func(w *RowWriter, ctx context.Context, t *ir.Table, r <-chan ir.Row) error {
		return w.WriteRowsIdempotent(ctx, t, r)
	}},
	{"LOAD DATA", ir.BulkLoadLoadDataInfile, func(w *RowWriter, ctx context.Context, t *ir.Table, r <-chan ir.Row) error {
		return w.WriteRows(ctx, t, r)
	}},
}

// TestColdCopyReparentRetry_UnsuppliedTargetKeyRefusesOnEveryCore is the
// crux: recorded table keyed, target keyed only on a surrogate the rows do
// not carry (and, as the fail-closed cell, a target the probe cannot find).
// Every core must refuse instead of re-sending.
func TestColdCopyReparentRetry_UnsuppliedTargetKeyRefusesOnEveryCore(t *testing.T) {
	targets := []struct {
		name    string
		target  *scriptTarget
		verdict irbackup.ReplayKeyVerdict
	}{
		{"surrogate-only key", surrogateScriptTarget, irbackup.ReplayKeylessTarget},
		{"table not found", &scriptTarget{missing: true}, irbackup.ReplayTargetAbsent},
	}
	for _, core := range reparentRetryCores {
		for _, tgt := range targets {
			t.Run(core.name+"/"+tgt.name, func(t *testing.T) {
				withFastReparentBackoff(t, 12)
				withSmallLoadDataSegments(t, 1<<20)
				script := &flushScript{execErrs: []error{vttabletUnavailable()}, target: tgt.target}
				w := &RowWriter{db: newScriptDB(t, script), bulkLoad: core.bulk}

				err := core.write(w, context.Background(), pinReparentTable(), feedReparentRows(3))
				if err == nil {
					t.Fatal("a re-send into a target whose key the rows do not supply returned nil: " +
						"the batch was re-sent and, had the first attempt committed, doubled")
				}
				ce, ok := sluicecode.FromError(err)
				if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
					t.Fatalf("refusal = %v (coded=%v); want %s", err, ok, sluicecode.CodeCopyRetryAmbiguousKeyless)
				}
				if !containsAll(err.Error(), tgt.verdict.Describe()) {
					t.Errorf("the refusal must say which judgment failed (%q); got: %v", tgt.verdict.Describe(), err)
				}
				if got := script.execCalls.Load(); got != 1 {
					t.Errorf("bulk-write exec calls = %d; want 1 — the ambiguous batch must not be re-sent", got)
				}
				if got := script.replayProbes.Load(); got != 1 {
					t.Errorf("target probes = %d; want 1 — the refusal must come from the TARGET judgment", got)
				}
			})
		}
	}
}

// TestColdCopyReparentRetry_UnansweredProbeNeverLicensesAResend pins the
// gate's failure arms. A probe that hits the same transient as the write is
// ridden: no re-send until a probe answers, then the re-send. A probe that
// fails terminally ends the copy loudly, with nothing re-sent and no
// "keyless" verdict invented.
func TestColdCopyReparentRetry_UnansweredProbeNeverLicensesAResend(t *testing.T) {
	t.Run("transient probe failure is ridden", func(t *testing.T) {
		withFastReparentBackoff(t, 12)
		script := &flushScript{
			execErrs:        []error{vttabletUnavailable()},
			replayProbeErrs: []error{vttabletUnavailable()},
		}
		w := &RowWriter{db: newScriptDB(t, script), bulkLoad: ir.BulkLoadBatchedInsert}
		if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
			t.Fatalf("a keyed copy whose first probe met the transient must still converge: %v", err)
		}
		if got := script.replayProbes.Load(); got != 2 {
			t.Errorf("target probes = %d; want 2 (one failed, one answered)", got)
		}
		if got := script.execCalls.Load(); got != 2 {
			t.Errorf("exec calls = %d; want 2 — exactly one re-send, and only after a probe answered", got)
		}
	})
	t.Run("terminal probe failure is loud", func(t *testing.T) {
		withFastReparentBackoff(t, 12)
		probeErr := &gomysql.MySQLError{Number: 1142, Message: "SELECT command denied to user for table 'TABLES'"}
		script := &flushScript{execErrs: []error{vttabletUnavailable()}, replayProbeErrs: []error{probeErr}}
		w := &RowWriter{db: newScriptDB(t, script), bulkLoad: ir.BulkLoadBatchedInsert}
		err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2))
		if err == nil {
			t.Fatal("a probe that cannot answer must not let the copy through")
		}
		if !errors.Is(err, probeErr) || !containsAll(err.Error(), "judge whether re-sending") {
			t.Errorf("the failure must carry the probe's own error and say what it was for; got: %v", err)
		}
		if _, coded := sluicecode.FromError(err); coded {
			t.Errorf("an unanswered probe must not be reported as a keyless verdict: %v", err)
		}
		if got := script.execCalls.Load(); got != 1 {
			t.Errorf("exec calls = %d; want 1 — nothing re-sent unjudged", got)
		}
	})
}

// TestColdCopyReparentRetry_ProbesTheTargetOncePerTable pins the cost side:
// a copy that never meets a transient never probes, and a table whose
// batches meet several transients is probed once and the answer reused.
func TestColdCopyReparentRetry_ProbesTheTargetOncePerTable(t *testing.T) {
	t.Run("no transient, no probe", func(t *testing.T) {
		withFastReparentBackoff(t, 12)
		clean := &flushScript{}
		w := &RowWriter{db: newScriptDB(t, clean), bulkLoad: ir.BulkLoadBatchedInsert}
		if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(3)); err != nil {
			t.Fatalf("clean copy: %v", err)
		}
		if got := clean.replayProbes.Load(); got != 0 {
			t.Errorf("a copy that met no transient probed the target %d times; want 0", got)
		}
	})
	t.Run("two retried batches, one probe", func(t *testing.T) {
		withFastReparentBackoff(t, 12)
		// Two flushes (maxRowsPerBatch 1), each meeting a transient first.
		bumpy := &flushScript{execErrs: []error{vttabletUnavailable(), nil, vttabletUnavailable(), nil}}
		w := &RowWriter{db: newScriptDB(t, bumpy), bulkLoad: ir.BulkLoadBatchedInsert, maxRowsPerBatch: 1}
		if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
			t.Fatalf("a keyed copy must still ride its transients: %v", err)
		}
		if got := bumpy.execCalls.Load(); got != 4 {
			t.Fatalf("exec calls = %d; want 4 (two batches, each transient + re-send) — the scenario did not happen", got)
		}
		if got := bumpy.replayProbes.Load(); got != 1 {
			t.Errorf("target probes = %d over two retried batches of one table; want 1 (memoised)", got)
		}
	})
}
