//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-server pins for the TxCommit position convention (audit
// 2026-09-01 A2-1, the Postgres sibling of item 132) and for the
// recovery it unblocks: a mid-stream DDL refusal whose drained-model
// hint the operator followed must not re-fire on the warm resume.
//
// Before the fix the TxCommit carried CommitLSN — the commit record's
// START — and logical decoding re-delivers a transaction whose commit
// record starts at or after the requested LSN, so every resume from a
// cleanly-persisted boundary replayed the last applied transaction:
// its RelationMessage (rendered from the HISTORIC catalog, pre-DDL
// shape) seeded the relation cache, the first post-DDL transaction
// then classified against it, and the identical refusal fired again
// after the operator had applied the DDL on the target exactly as
// told. Observed on four runs and two shapes by the audit.

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// collectChanges drains ch into a slice until stop reports done, the
// channel closes (closed=true), or timeout fires. Every change is kept
// — boundary events included — because the positions on TxBegin /
// TxCommit are what these pins are about.
func collectChanges(t *testing.T, ctx context.Context, ch <-chan ir.Change, timeout time.Duration, stop func(got []ir.Change) bool) (got []ir.Change, closed bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if stop != nil && stop(got) {
			return got, false
		}
		select {
		case c, ok := <-ch:
			if !ok {
				return got, true
			}
			got = append(got, c)
		case <-deadline.C:
			t.Logf("collectChanges: timed out after %v with %d changes", timeout, len(got))
			return got, false
		case <-ctx.Done():
			return got, false
		}
	}
}

// lastTxCommit returns the position of the last TxCommit in got —
// what an applier persists at a clean source-transaction boundary.
func lastTxCommit(t *testing.T, got []ir.Change) ir.Position {
	t.Helper()
	for i := len(got) - 1; i >= 0; i-- {
		if c, ok := got[i].(ir.TxCommit); ok {
			return c.Position
		}
	}
	t.Fatal("no TxCommit observed")
	return ir.Position{}
}

// insertIDs lists the id column of every Insert in got, in order.
func insertIDs(got []ir.Change) []int64 {
	var ids []int64
	for _, c := range got {
		if ins, ok := c.(ir.Insert); ok {
			switch v := ins.Row["id"].(type) {
			case int64:
				ids = append(ids, v)
			case int32:
				ids = append(ids, int64(v))
			case int:
				ids = append(ids, int64(v))
			}
		}
	}
	return ids
}

// firstInsert returns the first Insert in got, or ok=false.
func firstInsert(got []ir.Change) (ir.Insert, bool) {
	for _, c := range got {
		if ins, ok := c.(ir.Insert); ok {
			return ins, true
		}
	}
	return ir.Insert{}, false
}

// sawInsertID reports whether an Insert with the given id is in got.
func sawInsertID(got []ir.Change, id int64) bool {
	for _, v := range insertIDs(got) {
		if v == id {
			return true
		}
	}
	return false
}

func openCDC(t *testing.T, ctx context.Context, dsn string, forward bool) *CDCReader {
	t.Helper()
	rdr, err := (Engine{}).OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc, ok := rdr.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *CDCReader", rdr)
	}
	cdc.SetSchemaForward(forward)
	return cdc
}

// TestPGCDC_TxCommitPositionIsPostCommit pins the convention in both
// directions on a real server:
//
//   - a TxCommit's position is strictly past the transaction's row
//     positions, and resuming from it delivers the NEXT transaction
//     first — the one it closed is not re-delivered;
//   - a row's position is the pre-transaction point, and resuming from
//     it re-delivers that row's whole transaction (the mid-transaction
//     at-least-once convention ADR-0010 idempotency absorbs).
//
// Mutation: emitting CommitLSN on the TxCommit again fails the first
// half (id=1 is re-delivered ahead of id=2).
func TestPGCDC_TxCommitPositionIsPostCommit(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE txpos (id INT PRIMARY KEY, v TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// ---- Session 1: one transaction; capture its three positions.
	rdr1 := openCDC(t, ctx, dsn, true)
	ch1, err := rdr1.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("session 1 StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	applyPGSQL(t, dsn, `INSERT INTO txpos VALUES (1, 'a');`)
	got1, _ := collectChanges(t, ctx, ch1, 30*time.Second, func(got []ir.Change) bool {
		_, isCommit := lastChange(got).(ir.TxCommit)
		return sawInsertID(got, 1) && isCommit
	})
	if err := rdr1.Close(); err != nil {
		t.Fatalf("session 1 Close: %v", err)
	}
	if !sawInsertID(got1, 1) {
		t.Fatalf("session 1 never delivered id=1; got %v", got1)
	}
	row1, _ := firstInsert(got1)
	var begin1 ir.TxBegin
	for _, c := range got1 {
		if b, ok := c.(ir.TxBegin); ok {
			begin1 = b
		}
	}
	commit1 := lastTxCommit(t, got1)
	beginLSN, rowLSN, commitLSN := positionLSN(t, begin1.Position), positionLSN(t, row1.Position), positionLSN(t, commit1)
	if rowLSN != beginLSN {
		t.Errorf("row position %s != TxBegin position %s; rows must carry the pre-transaction point", rowLSN, beginLSN)
	}
	if commitLSN <= rowLSN {
		t.Fatalf("TxCommit position %s is not past the row position %s — the boundary is re-deliverable on resume (the A2-1 replay)", commitLSN, rowLSN)
	}

	// ---- Session 2: resume from the commit; the next transaction is
	// first and the closed one is not re-delivered.
	applyPGSQL(t, dsn, `INSERT INTO txpos VALUES (2, 'b');`)
	rdr2 := openCDC(t, ctx, dsn, true)
	// Keep the slot's confirmed_flush at this session's start so
	// session 3 can resume from a point inside what this session read.
	rdr2.HoldSlotAckAtCommitted()
	ch2, err := rdr2.StreamChanges(ctx, commit1)
	if err != nil {
		t.Fatalf("session 2 StreamChanges(commit of tx 1): %v", err)
	}
	got2, _ := collectChanges(t, ctx, ch2, 30*time.Second, func(got []ir.Change) bool {
		_, isCommit := lastChange(got).(ir.TxCommit)
		return sawInsertID(got, 2) && isCommit
	})
	if err := rdr2.Close(); err != nil {
		t.Fatalf("session 2 Close: %v", err)
	}
	if ids := insertIDs(got2); len(ids) == 0 || ids[0] != 2 {
		t.Fatalf("resume from tx 1's TxCommit delivered inserts %v; want [2] first — id=1 is the transaction that position closed", ids)
	}
	if sawInsertID(got2, 1) {
		t.Errorf("resume from tx 1's TxCommit re-delivered id=1: the TxCommit position is not a post-commit point")
	}
	row2, _ := firstInsert(got2)

	// ---- Session 3: resume from tx 2's ROW position; tx 2 replays.
	rdr3 := openCDC(t, ctx, dsn, true)
	ch3, err := rdr3.StreamChanges(ctx, row2.Position)
	if err != nil {
		t.Fatalf("session 3 StreamChanges(row of tx 2): %v", err)
	}
	got3, _ := collectChanges(t, ctx, ch3, 30*time.Second, func(got []ir.Change) bool {
		return sawInsertID(got, 2)
	})
	if err := rdr3.Close(); err != nil {
		t.Fatalf("session 3 Close: %v", err)
	}
	if ids := insertIDs(got3); len(ids) == 0 || ids[0] != 2 {
		t.Errorf("resume from tx 2's row position delivered inserts %v; want [2] — a mid-transaction position must re-deliver its transaction whole", ids)
	}
}

func lastChange(got []ir.Change) ir.Change {
	if len(got) == 0 {
		return nil
	}
	return got[len(got)-1]
}

// TestPGCDC_DrainedModelRecoveryResumes is the A2-1 gate at the reader:
// for every refused mid-stream DDL shape, DML → DDL on the source →
// the refusal (the anti-vacuity floor: the first stream MUST refuse) →
// a restart from the position an applier would have persisted (the
// last TxCommit) delivers the next row in its post-DDL shape with no
// refusal and without re-delivering the closed transaction. The
// pipeline-level half — the DDL applied on a real target, the row
// landing there, `sluice_cdc_skipped_tables` staying empty — is
// TestStreamer_PGToPG_DrainedModelRecovery in internal/pipeline.
//
// Matrix: every reader-level refusal shape the audit observed or the
// gate named, under the mode it refuses in. RENAME TABLE refuses in
// both modes (forward exercised); RENAME COLUMN and DROP COLUMN refuse
// only under refuse mode (forward routes them to the intercept); the
// projection-invisible interval typmod gate refuses in both (both
// exercised — the audit observed the re-refusal under both).
func TestPGCDC_DrainedModelRecoveryResumes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		forward bool
		create  string
		ddl     string // the source DDL + the post-DDL insert (id 2)
		want    []string
		// check asserts the resumed row carries the post-DDL shape.
		check func(t *testing.T, ins ir.Insert)
	}{
		{
			name:    "rename-table/forward",
			forward: true,
			create:  `CREATE TABLE t8 (id INT PRIMARY KEY, v TEXT);`,
			ddl:     `ALTER TABLE t8 RENAME TO t8b; INSERT INTO t8b VALUES (2, 'b');`,
			want:    []string{"RENAME public.t8 → public.t8b", "Drained-model recovery"},
			check: func(t *testing.T, ins ir.Insert) {
				if ins.Table != "t8b" {
					t.Errorf("resumed row routed to %q; want the renamed table t8b", ins.Table)
				}
			},
		},
		{
			name:    "rename-column/refuse",
			forward: false,
			create:  `CREATE TABLE t (id INT PRIMARY KEY, v TEXT);`,
			ddl:     `ALTER TABLE t RENAME COLUMN v TO w; INSERT INTO t VALUES (2, 'b');`,
			want:    []string{"RENAME COLUMN v → w", "Drained-model recovery"},
			check: func(t *testing.T, ins ir.Insert) {
				if _, ok := ins.Row["w"]; !ok {
					t.Errorf("resumed row %v lacks the renamed column w", ins.Row)
				}
				if _, ok := ins.Row["v"]; ok {
					t.Errorf("resumed row %v still carries the old column v", ins.Row)
				}
			},
		},
		{
			name:    "drop-column/refuse",
			forward: false,
			create:  `CREATE TABLE t (id INT PRIMARY KEY, v TEXT, c INT);`,
			ddl:     `ALTER TABLE t DROP COLUMN c; INSERT INTO t VALUES (2, 'b');`,
			want:    []string{"DROP COLUMN", "Drained-model recovery"},
			check: func(t *testing.T, ins ir.Insert) {
				if _, ok := ins.Row["c"]; ok {
					t.Errorf("resumed row %v still carries the dropped column c", ins.Row)
				}
			},
		},
		{
			name:    "interval-typmod/forward",
			forward: true,
			create:  `CREATE TABLE t (id INT PRIMARY KEY, iv INTERVAL(6));`,
			ddl:     `ALTER TABLE t ALTER COLUMN iv TYPE INTERVAL(3); INSERT INTO t VALUES (2, '1.5 seconds');`,
			want:    []string{"cannot be forwarded", `column "iv"`, "Drained-model recovery"},
			check: func(t *testing.T, ins ir.Insert) {
				if _, ok := ins.Row["iv"]; !ok {
					t.Errorf("resumed row %v lacks iv", ins.Row)
				}
			},
		},
		{
			name:    "interval-typmod/refuse",
			forward: false,
			create:  `CREATE TABLE t (id INT PRIMARY KEY, iv INTERVAL(6));`,
			ddl:     `ALTER TABLE t ALTER COLUMN iv TYPE INTERVAL(3); INSERT INTO t VALUES (2, '1.5 seconds');`,
			want:    []string{"ALTER COLUMN TYPE iv", "Drained-model recovery"},
			check:   func(t *testing.T, ins ir.Insert) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForCDC(t)
			defer cleanup()
			applyPGSQL(t, dsn, tc.create)
			table := "t"
			if strings.HasPrefix(tc.name, "rename-table") {
				table = "t8"
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			// ---- Run 1: the last applied transaction touches the table
			// (the audit's boundary condition for the wedge), then the
			// DDL and the first post-DDL row refuse.
			rdr1 := openCDC(t, ctx, dsn, tc.forward)
			ch1, err := rdr1.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("run 1 StreamChanges: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
			applyPGSQL(t, dsn, `INSERT INTO `+table+` (id) VALUES (1);`)
			got1, _ := collectChanges(t, ctx, ch1, 30*time.Second, func(got []ir.Change) bool {
				_, isCommit := lastChange(got).(ir.TxCommit)
				return sawInsertID(got, 1) && isCommit
			})
			if !sawInsertID(got1, 1) {
				t.Fatalf("run 1 never delivered id=1; got %v", got1)
			}
			persisted := lastTxCommit(t, got1)

			applyPGSQL(t, dsn, tc.ddl)
			rest, closed := collectChanges(t, ctx, ch1, 60*time.Second, nil)
			if !closed {
				t.Fatalf("run 1 did not refuse within 60s (got %v) — the floor: the first stream must refuse", rest)
			}
			streamErr := rdr1.Err()
			if err := rdr1.Close(); err != nil {
				t.Fatalf("run 1 Close: %v", err)
			}
			if streamErr == nil {
				t.Fatal("run 1 ended with no error; the floor requires the refusal")
			}
			for _, want := range tc.want {
				if !strings.Contains(streamErr.Error(), want) {
					t.Errorf("run 1 refusal missing %q; got: %v", want, streamErr)
				}
			}
			if sawInsertID(rest, 2) {
				t.Errorf("run 1 delivered the post-DDL row before refusing: %v", rest)
			}

			// ---- Run 2: the operator has applied the DDL on the target
			// and restarts with the same stream-id: resume from the
			// persisted boundary.
			rdr2 := openCDC(t, ctx, dsn, tc.forward)
			ch2, err := rdr2.StreamChanges(ctx, persisted)
			if err != nil {
				t.Fatalf("run 2 StreamChanges(persisted boundary): %v", err)
			}
			got2, closed2 := collectChanges(t, ctx, ch2, 30*time.Second, func(got []ir.Change) bool {
				return sawInsertID(got, 2)
			})
			resumeErr := rdr2.Err()
			if err := rdr2.Close(); err != nil {
				t.Fatalf("run 2 Close: %v", err)
			}
			if closed2 || resumeErr != nil {
				t.Fatalf("run 2 refused AGAIN on the warm resume (err=%v; got %v): the drained-model recovery the hint prescribes cannot run", resumeErr, got2)
			}
			if sawInsertID(got2, 1) {
				t.Errorf("run 2 re-delivered id=1 — the closed transaction replayed with its pre-DDL relation shape (the A2-1 mechanism); got %v", got2)
			}
			ins, ok := firstInsert(got2)
			if !ok || insertIDs(got2)[0] != 2 {
				t.Fatalf("run 2 did not deliver the post-DDL row id=2 first; got %v", got2)
			}
			tc.check(t, ins)
		})
	}
}

// TestPGCDC_DrainedModelRecoveryResumes_FromPreFixPosition pins the
// upgrade path: a position persisted by an OLDER binary is the
// transaction's CommitLSN (the pre-commit point), so the first resume
// under the new binary still replays that transaction once — and, if
// the operator had already applied the refused DDL on the target, the
// refusal fires ONE more time. What makes that a one-time cost rather
// than the old wedge is that the replayed transaction's TxCommit now
// carries the post-commit point, which the applier persists at the
// boundary flush BEFORE the refusal kills the stream; the restart after
// that starts at the next transaction. This test resumes from the
// pre-fix position (the replayed transaction's TxBegin position IS
// what an older binary persisted), asserts the one-time re-refusal,
// and then resumes from the TxCommit that replay emitted.
func TestPGCDC_DrainedModelRecoveryResumes_FromPreFixPosition(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()
	applyPGSQL(t, dsn, `CREATE TABLE t8 (id INT PRIMARY KEY, v TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rdr1 := openCDC(t, ctx, dsn, true)
	ch1, err := rdr1.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("run 1 StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	applyPGSQL(t, dsn, `INSERT INTO t8 (id) VALUES (1);`)
	got1, _ := collectChanges(t, ctx, ch1, 30*time.Second, func(got []ir.Change) bool {
		_, isCommit := lastChange(got).(ir.TxCommit)
		return sawInsertID(got, 1) && isCommit
	})
	var preFix ir.Position // what a pre-fix binary persisted: the tx's CommitLSN
	for _, c := range got1 {
		if b, ok := c.(ir.TxBegin); ok {
			preFix = b.Position
		}
	}
	if preFix.Token == "" {
		t.Fatalf("run 1 emitted no TxBegin; got %v", got1)
	}
	applyPGSQL(t, dsn, `ALTER TABLE t8 RENAME TO t8b; INSERT INTO t8b VALUES (2, 'b');`)
	if _, closed := collectChanges(t, ctx, ch1, 60*time.Second, nil); !closed {
		t.Fatal("run 1 did not refuse within 60s")
	}
	if err := rdr1.Err(); err == nil || !strings.Contains(err.Error(), "RENAME public.t8 → public.t8b") {
		t.Fatalf("run 1 refusal = %v; want the RENAME TABLE refusal", err)
	}
	_ = rdr1.Close()

	// ---- Run 2: resume from the pre-fix position. The closed
	// transaction replays once, its TxCommit now carries the post-commit
	// point, and the refusal fires one more time.
	rdr2 := openCDC(t, ctx, dsn, true)
	ch2, err := rdr2.StreamChanges(ctx, preFix)
	if err != nil {
		t.Fatalf("run 2 StreamChanges(pre-fix position): %v", err)
	}
	got2, closed2 := collectChanges(t, ctx, ch2, 60*time.Second, nil)
	err2 := rdr2.Err()
	_ = rdr2.Close()
	if !closed2 || err2 == nil {
		t.Fatalf("run 2 from a pre-fix position did not re-refuse (err=%v; got %v): the one-time replay of an older binary's boundary is expected", err2, got2)
	}
	if !sawInsertID(got2, 1) {
		t.Fatalf("run 2 did not replay id=1 from the pre-fix position; got %v", got2)
	}
	advanced := lastTxCommit(t, got2)
	if positionLSN(t, advanced) <= positionLSN(t, preFix) {
		t.Fatalf("the replayed transaction's TxCommit %s is not past the pre-fix position %s — the restart after this one would wedge again", positionLSN(t, advanced), positionLSN(t, preFix))
	}

	// ---- Run 3: the position the applier persisted during run 2.
	rdr3 := openCDC(t, ctx, dsn, true)
	ch3, err := rdr3.StreamChanges(ctx, advanced)
	if err != nil {
		t.Fatalf("run 3 StreamChanges: %v", err)
	}
	got3, closed3 := collectChanges(t, ctx, ch3, 30*time.Second, func(got []ir.Change) bool {
		return sawInsertID(got, 2)
	})
	err3 := rdr3.Err()
	_ = rdr3.Close()
	if closed3 || err3 != nil {
		t.Fatalf("run 3 refused (err=%v; got %v): the upgrade path did not converge", err3, got3)
	}
	ins, ok := firstInsert(got3)
	if !ok || insertIDs(got3)[0] != 2 || ins.Table != "t8b" {
		t.Fatalf("run 3 did not deliver id=2 on t8b first; got %v", got3)
	}
}
