//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestChangeApplier_ApplyMarksUnavailable_DefaultLanesCrossCheckpoints is Bug
// 293's MySQL pin (GC-41 (d)): with the mark table unusable
// (APPLY-MARKS-UNAVAILABLE) the tracker is Disable()d and never loaded, and
// v0.156.5's default lane path panicked at its first checkpoint — CloseTxs
// writing into the nil closed map — before the position could advance, so
// every restart replayed and panicked again. The serial pins could not see
// it: the serial CloseOpen only ranges the nil maps. This drives the DEFAULT
// shape (batch size > 1, W lanes) through three checkpoints, each closing a
// source transaction, and asserts the position advances every time and the
// target converges — the keyless table holding each row exactly once.
func TestChangeApplier_ApplyMarksUnavailable_DefaultLanesCrossCheckpoints(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, `CREATE TABLE lmu_items (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(32) NOT NULL, UNIQUE KEY (code)) ENGINE=InnoDB;
		CREATE TABLE lmu_log (v VARCHAR(32) NOT NULL) ENGINE=InnoDB;`)
	defer applyMySQLApplier(t, dsn, `DROP TABLE IF EXISTS lmu_items; DROP TABLE IF EXISTS lmu_log;`)

	logs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	applier, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer closeApplier(applier)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	applyMySQLApplier(t, dsn, `DROP TABLE sluice_cdc_apply_marks;`)
	a := applier.(*ChangeApplier)
	a.SetApplyConcurrency(concurrentLanesW)

	applyLaneTxsAcrossCheckpoints(ctx, t, a, testStreamID, 3, func(n int64) []ir.Change {
		return []ir.Change{
			ir.Insert{Schema: "target_db", Table: "lmu_items", Row: ir.Row{"id": n, "code": fmt.Sprintf("c%d", n)}},
			ir.Insert{Schema: "target_db", Table: "lmu_log", Row: ir.Row{"v": fmt.Sprintf("c%d", n)}},
		}
	})
	if !strings.Contains(logs.String(), applymarks.UnavailableMarker) {
		t.Errorf("no %s WARN with the mark table absent — the run did not take the disabled-tracker path under test:\n%s",
			applymarks.UnavailableMarker, logs.String())
	}
	for _, table := range []string{"lmu_items", "lmu_log"} {
		if got := countAllRows(t, dsn, "target_db", table); got != 3 {
			t.Errorf("%s holds %d rows; want 3 — the lanes did not converge (the keyless table must hold each row once)", table, got)
		}
	}
}

// applyLaneTxsAcrossCheckpoints streams txs identity-carrying source
// transactions through ONE ApplyBatch run (batch size > 1, so the lanes the
// caller wired take them) and, after each, waits until the persisted position
// reaches that transaction's commit — so the run crosses a checkpoint per
// transaction, each closing a transaction (the Bug 293 CloseTxs path), rather
// than one final checkpoint at channel close. rows(n) is transaction n's row
// changes; their positions and apply identities are stamped here. The
// Postgres package carries the twin.
func applyLaneTxsAcrossCheckpoints(ctx context.Context, t *testing.T, a *ChangeApplier, streamID string, txs int, rows func(n int64) []ir.Change) {
	t.Helper()
	ch := make(chan ir.Change, 64)
	done := make(chan error, 1)
	go func() { done <- a.ApplyBatch(ctx, streamID, ch, 50) }()
	for n := int64(1); n <= int64(txs); n++ {
		pos := ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf(`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-%d"}`, 100+n)}
		txID := fmt.Sprintf("lanes-tx-%d", n)
		ch <- ir.TxBegin{Position: pos}
		for i, c := range rows(n) {
			id := ir.ApplyID{TxID: txID, Seq: uint64(i + 1)}
			switch v := c.(type) {
			case ir.Insert:
				v.Position, v.ApplyID = pos, id
				c = v
			case ir.Update:
				v.Position, v.ApplyID = pos, id
				c = v
			case ir.Delete:
				v.Position, v.ApplyID = pos, id
				c = v
			}
			ch <- c
		}
		ch <- ir.TxCommit{Position: pos}
		deadline := time.Now().Add(30 * time.Second)
		for {
			select {
			case err := <-done:
				t.Fatalf("ApplyBatch returned before transaction %d was checkpointed: %v", n, err)
			default:
			}
			got, ok, err := a.ReadPosition(ctx, streamID)
			if err == nil && ok && got.Token == pos.Token {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the position never reached transaction %d's commit %s (last %q, ok=%v, err=%v): its checkpoint did not persist",
					n, pos.Token, got.Token, ok, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	close(ch)
	if err := <-done; err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}
