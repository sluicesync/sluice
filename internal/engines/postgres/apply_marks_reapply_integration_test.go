//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestChangeApplier_ApplyMarks_SecondApplyAfterTheTableVanished pins the
// second-Apply shape of APPLY-MARKS-UNAVAILABLE (ADR-0190 operator decision
// 2): one applier whose FIRST apply run loaded its marks — enabling the
// tracker — and whose mark table is gone by the SECOND run must disable the
// tracker for that run, WARN, and apply exactly as before ADR-0190. A tracker
// left enabled from the first run would write marks into a table that no
// longer exists and stop the stream. A fresh applier cannot show this: its
// tracker starts disabled, so the crash suite's marks_unavailable cell passes
// with or without the disable.
func TestChangeApplier_ApplyMarks_SecondApplyAfterTheTableVanished(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE TABLE rsm (id BIGINT PRIMARY KEY, u TEXT UNIQUE);`)

	logs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// One source transaction inserting row n into a secondary-unique table:
	// a marked class, so the change writes a mark whenever marks are in force.
	tx := func(n int64, lsn string) []ir.Change {
		pos := ir.Position{Engine: "postgres", Token: `{"lsn":"` + lsn + `"}`}
		return []ir.Change{
			ir.TxBegin{Position: pos},
			ir.Insert{
				Schema: "public", Table: "rsm", Row: ir.Row{"id": n, "u": "u" + lsn}, Position: pos,
				ApplyID: ir.ApplyID{TxID: "tx:" + lsn, Seq: 1},
			},
			ir.TxCommit{Position: pos},
		}
	}

	// Run 1: the mark table exists (EnsureControlTable), so marks are in force.
	pumpChanges(t, ctx, applier, tx(1, "0/16B2C00"))
	if strings.Contains(logs.String(), applymarks.UnavailableMarker) {
		t.Fatalf("the first run already ran without marks, so the second-run shape is not exercised:\n%s", logs.String())
	}
	applyPGApplier(t, dsn, `DROP TABLE sluice_cdc_apply_marks;`)

	// Run 2 on the SAME applier, with no EnsureControlTable in between (it
	// would recreate the table).
	ch := make(chan ir.Change, 3)
	for _, c := range tx(2, "0/16B2D00") {
		ch <- c
	}
	close(ch)
	if err := applier.Apply(ctx, testStreamID, ch); err != nil {
		t.Fatalf("the second apply run failed with its mark table gone: %v — it must WARN %s and apply as before ADR-0190",
			err, applymarks.UnavailableMarker)
	}
	if !strings.Contains(logs.String(), applymarks.UnavailableMarker) {
		t.Errorf("the second apply run did not WARN %s", applymarks.UnavailableMarker)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rsm`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("rsm holds %d rows (err %v); want both runs' rows", n, err)
	}
}
