//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// TestApplyIdentity_SequenceRestartCannotReissueAMarkedName pins the txid
// stamp in [triggercdc.ChangeApplyID] against the one state a change-log id
// alone cannot survive: an `ALTER SEQUENCE … RESTART` that re-issues the id
// an ADR-0190 apply mark still names. [verifyChangeLogSequence] grades only
// the sequence's configuration, so a restart is invisible to it (its own
// SCOPE note says so), and the mark is the last line.
//
// The shape is the worst case the stamp exists for. The target is KEYLESS
// (the capture requires a source key, the target need not have one), so its
// mark is per table and no key collision is loud; the re-issued change
// is byte-identical to the marked one, so the digest tripwire agrees; the
// resume watermark is the position the crash persisted before the marked
// change. Without the stamp both changes are `postgres-trigger:1`, ordinal
// 1, and the tracker skips the new row at exit 0.
//
// The independent evidence is the server's: the change log itself reports
// both changes at id 1 (the premise that the name was re-issued), and the
// control cell proves the tracker WOULD skip a change carrying the old name,
// so a no-skip verdict is the stamp's doing and not a tracker that never
// skips.
func TestApplyIdentity_SequenceRestartCannotReissueAMarkedName(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	setupCaptureTable(t, ctx, dsn, "id_reset")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	r := &CDCReader{db: db, schema: "public", batchSize: defaultBatchSize}

	// The marked change, as the fenced lane applied it before the crash: the
	// watermark (0) is the position persisted at its predecessor.
	applyPGSQL(t, dsn, `INSERT INTO id_reset (id, label) VALUES (1, 'same')`)
	marked := pollOneAtID(t, ctx, r, 1)

	// The operator's reset: the source row goes, the change log is drained
	// (the prune's effect), and the sequence restarts exactly at the
	// watermark's successor. Then the identical row is captured again.
	applyPGSQL(t, dsn, `DELETE FROM id_reset WHERE id = 1`)
	applyPGSQL(t, dsn, `DELETE FROM public.`+ChangeLogTable)
	applyPGSQL(t, dsn, "ALTER SEQUENCE "+changeLogSeq+" RESTART WITH 1")
	applyPGSQL(t, dsn, `INSERT INTO id_reset (id, label) VALUES (1, 'same')`)
	reissued := pollOneAtID(t, ctx, r, 1)

	assertReissueNotSkipped(t, "id_reset", marked, reissued)
}

// pollOneAtID polls the change log from watermark 0 until it emits, and
// requires exactly one change at change-log id wantID — the premise every
// cell here rests on.
func pollOneAtID(t *testing.T, ctx context.Context, r *CDCReader, wantID int64) ir.Change {
	t.Helper()
	var b pollBatch
	var err error
	for range 50 {
		if b, err = r.poll(ctx, 0); err != nil {
			t.Fatalf("poll: %v", err)
		}
		if len(b.events) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(b.events) != 1 {
		t.Fatalf("poll(0) emitted %d change(s); want exactly 1", len(b.events))
	}
	c := b.events[0]
	p, _, err := decodePos(c.Pos())
	if err != nil {
		t.Fatalf("decodePos: %v", err)
	}
	if p.LastID != wantID {
		t.Fatalf("the change sits at change-log id %d; want %d — this test's premise is that the "+
			"restarted sequence re-issued the marked change's id, and it did not", p.LastID, wantID)
	}
	return c
}

// assertReissueNotSkipped is the verdict: a durable mark left by the marked
// change must not skip the re-issued one on a keyless target.
func assertReissueNotSkipped(t *testing.T, table string, marked, reissued ir.Change) {
	t.Helper()
	keyless := applymarks.Subject{Table: table}
	if applymarks.ChangeDigest(marked, table, nil) != applymarks.ChangeDigest(reissued, table, nil) {
		t.Fatal("the two changes differ in content; this test needs them identical so only the identity can tell them apart")
	}

	var writer applymarks.Tracker
	writer.Load("s", "", nil)
	d, err := writer.Decide(marked, keyless)
	if err != nil || len(d.Marks) == 0 {
		t.Fatalf("the marked change wrote no mark on a keyless target (marks %d, err %v)", len(d.Marks), err)
	}
	decide := func(c ir.Change) (applymarks.Decision, error) {
		var resumed applymarks.Tracker
		resumed.Load("s", "", d.Marks)
		return resumed.Decide(c, keyless)
	}

	// Control: a change carrying the marked change's OWN identity is skipped,
	// so the verdict below measures the identity, not a tracker that never
	// skips.
	sameName := reissued.(ir.Insert)
	sameName.ApplyID = ir.ApplyIDOf(marked)
	if got, err := decide(sameName); err != nil || !got.Skip {
		t.Fatalf("control: the tracker did not skip a change carrying the mark's own name (skip %v, err %v)", got.Skip, err)
	}

	if ir.ApplyIDOf(marked) == ir.ApplyIDOf(reissued) {
		t.Errorf("the re-issued change carries the marked change's identity %v; a restarted id "+
			"sequence can re-issue a name a durable mark still holds", ir.ApplyIDOf(reissued))
	}
	got, err := decide(reissued)
	if got.Skip {
		t.Fatalf("SILENT SKIP: the change re-issued at the marked change's id was skipped on a keyless "+
			"target (identity %v); it must apply or refuse", ir.ApplyIDOf(reissued))
	}
	t.Logf("re-issued change: identity %v, err %v (marked: %v)", ir.ApplyIDOf(reissued), err, ir.ApplyIDOf(marked))
}
