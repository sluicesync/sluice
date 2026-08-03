// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The marker-less-source resume-position gate (audit 2026-08-01 S3).
//
// CheckpointOnlyAtTxBoundary makes the serial loop persist the resume position
// ONLY on an ir.TxCommit. Its justification is a MySQL BINLOG constraint —
// go-mysql seeks to a byte offset, so a ROWS event whose TABLE_MAP was earlier
// in the same transaction fails to resume — but the flag is set on the MySQL
// ChangeApplier, which is the TARGET. A target-side flag encoding a
// source-side constraint holds only while the source is also MySQL binlog.
//
// It is not. Two whole source families emit NO transaction markers at all:
// VStream (PlanetScale / Vitess) and every trigger-CDC engine (pgtrigger,
// sqlite, d1-trigger). Point any of them at a MySQL-family target on the
// serial apply path — which is the DEFAULT, since the concurrent lane path
// needs --apply-concurrency > 1 — and there is no TxCommit to trigger the
// boundary write, so the durable position never moves off the cold-start
// value for the life of the stream.
//
// Consequence, in order of how it bites: every restart replays the entire CDC
// history since cold-start; auto-prune on a trigger-CDC source never advances
// its frontier; and once the source's retention has passed the stale position,
// the resume is refused and forces an auto-resnapshot — which on an idempotent
// source (VStream is one) does not apply the gap's DELETEs (audit S5), at
// which point rows deleted at the source survive on the target permanently.
//
// This test pins the OBSERVABLE behaviour of the shared loop rather than any
// engine's wiring, so it stays honest wherever the flag ends up being set.

package appliershared

import (
	"context"
	"strings"
	"testing"
)

// TestRunBatchLoop_MarkerlessSource_StillAdvancesThePosition is the S3 gate.
//
// A source that emits no TxBegin/TxCommit, applied under
// CheckpointOnlyAtTxBoundary, must still persist a resume position. The
// mid-transaction hazard the flag guards cannot arise from such a source:
// with no transaction markers there is no "mid-transaction" for the loop to
// land in, so withholding the position buys nothing and costs the stream its
// ability to resume at all.
func TestRunBatchLoop_MarkerlessSource_StillAdvancesThePosition(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.CheckpointOnlyAtTxBoundary = true

	// A marker-less stream: rows only, exactly what a VStream or trigger-CDC
	// source delivers. No txBegin, no txCommit anywhere.
	ch := feed(true, insertAt("p1"), insertAt("p2"), insertAt("p3"))
	if err := RunBatchLoop(context.Background(), cfg, "stream", ch, 2); err != nil {
		t.Fatalf("RunBatchLoop: %v", err)
	}

	var wrote []string
	for _, e := range rec.list() {
		if strings.HasPrefix(e, "writePosition:") {
			wrote = append(wrote, strings.TrimPrefix(e, "writePosition:"))
		}
	}
	if len(wrote) == 0 {
		t.Fatalf("a marker-less source applied under CheckpointOnlyAtTxBoundary persisted NO resume "+
			"position at all.\nevents: %v\n\n"+
			"VStream and every trigger-CDC engine emit no ir.TxCommit, and the serial loop's only boundary "+
			"write hangs off one. Against a MySQL-family target on the default serial path the durable "+
			"position therefore never moves off the cold-start value: every restart replays the whole CDC "+
			"history, trigger-CDC auto-prune never advances, and once source retention passes the stale "+
			"position the resume is refused and forces a re-snapshot — which on an idempotent source does "+
			"not apply the gap's DELETEs (audit S5). The flag encodes a MySQL-BINLOG-SOURCE constraint; it "+
			"must not apply to a source that carries no transaction markers.",
			rec.list())
	}
	if got := wrote[len(wrote)-1]; got != "p3" {
		t.Errorf("last persisted position = %q, want the last applied change %q", got, "p3")
	}
}

// TestRunBatchLoop_MarkeredSource_StillWithholdsMidTxPosition is the control
// that keeps the fix honest. The ADR-0010 behaviour for a genuinely
// transactional source must be unchanged: a mid-transaction flush commits its
// data and does NOT advance the position, because a MySQL binlog resume cannot
// start there. A fix that simply always wrote the position would satisfy the
// test above and re-open the crash-loop this flag was built for — found live
// on the large-scale program.
func TestRunBatchLoop_MarkeredSource_StillWithholdsMidTxPosition(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.CheckpointOnlyAtTxBoundary = true

	ch := feed(true, txBegin("tb"), insertAt("p1"), insertAt("p2"), txCommit("tc"))
	if err := RunBatchLoop(context.Background(), cfg, "stream", ch, 1); err != nil {
		t.Fatalf("RunBatchLoop: %v", err)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "commit",
		"begin", "dispatch:p2", "commit",
		"begin", "writePosition:tc", "commit",
	})
}
