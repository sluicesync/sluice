//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 163 on a real PostgreSQL 16: the full-backup snapshot
// opener anchors at the change log's settled id INSIDE the sweep's own
// REPEATABLE READ view, and the post-sweep capturer on the fallback door
// records nothing on purpose. The in-flight-transaction settle + clamp
// half of the anchor is shared code with the sync cold start
// (openAnchoredSnapshot) and is pinned there by
// TestSnapshotStream_InFlightTxnAnchor_NoGap; what these pins add is the
// BACKUP door's use of it and the snapshot's read consistency.

package pgtrigger

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestBackupSnapshot_AnchorsAtTheSettledChangeLogID drives
// Engine.OpenBackupSnapshot: the Position decodes to the change log's
// MAX(id) at open under the trigger tag, a row committed AFTER the open is
// INVISIBLE to the snapshot's Rows (REPEATABLE READ — the consistency the
// SQLite twin does not have), and its change-log id is ABOVE the anchor,
// so an incremental resuming at the anchor replays exactly it.
func TestBackupSnapshot_AnchorsAtTheSettledChangeLogID(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	applyPGSQL(t, dsn, `CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT);`)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"items"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	applyPGSQL(t, dsn, `INSERT INTO items (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c');`)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var maxID int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM public.sluice_change_log`).Scan(&maxID); err != nil {
		t.Fatalf("read MAX(id): %v", err)
	}

	snap, err := (Engine{}).OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}
	defer func() { _ = snap.Close() }()

	p, ok, err := decodePos(snap.Position)
	if err != nil || !ok {
		t.Fatalf("decode backup position: ok=%v err=%v", ok, err)
	}
	if p.LastID != maxID {
		t.Errorf("backup anchor last_id=%d; want %d (the change log's MAX(id) at open, no txn in flight)", p.LastID, maxID)
	}
	if snap.Position.Engine != EngineName {
		t.Errorf("position engine = %q; want %q", snap.Position.Engine, EngineName)
	}
	if id, err := AppliedLastID(snap.Position.Token); err != nil || id != maxID {
		t.Errorf("AppliedLastID(token) = (%d, %v); want (%d, nil) — the incremental resumes through this decoder", id, err, maxID)
	}

	// Committed AFTER the snapshot opened: invisible to Rows, above the anchor.
	applyPGSQL(t, dsn, `INSERT INTO items (id, label) VALUES (4, 'd');`)
	var afterMax int64
	if err := db.QueryRowContext(ctx, `SELECT MAX(id) FROM public.sluice_change_log`).Scan(&afterMax); err != nil {
		t.Fatalf("read MAX(id) after: %v", err)
	}
	if afterMax <= p.LastID {
		t.Fatalf("post-open change-log id %d is not above the anchor %d (test premise broken)", afterMax, p.LastID)
	}
	rows, err := snap.Rows.ReadRows(ctx, &ir.Table{
		Name:       "items",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "label", Type: ir.Text{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var ids []int64
	for r := range rows {
		ids = append(ids, r["id"].(int64))
	}
	if err := snap.Rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("snapshot sweep read ids %v; want exactly the three rows visible at open (REPEATABLE READ)", ids)
	}
}

// TestCaptureBackupPosition_IsUnavailableOnTheFallbackDoor pins the
// post-sweep capturer on the trigger reader: never a position, always
// ErrPositionUnavailable, with the remedy that fits — `trigger setup` when
// the change log is absent, "fix the cause and re-run" when present — and
// NEVER the composed postgres reader's WAL LSN, which is what this engine
// recorded before item 163.
func TestCaptureBackupPosition_IsUnavailableOnTheFallbackDoor(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	applyPGSQL(t, dsn, `CREATE TABLE items (id BIGINT PRIMARY KEY);`)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	capture := func(t *testing.T, wantMention string) {
		t.Helper()
		sr, err := (Engine{}).OpenSchemaReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenSchemaReader: %v", err)
		}
		defer func() {
			if c, ok := sr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}()
		if _, ok := sr.(*SchemaReader); !ok {
			t.Fatalf("OpenSchemaReader returned %T; want the trigger engine's *SchemaReader wrapper", sr)
		}
		capturer, ok := sr.(irbackup.PositionCapturer)
		if !ok {
			t.Fatalf("%T does not implement irbackup.PositionCapturer", sr)
		}
		pos, err := capturer.CaptureBackupPosition(ctx, "sluice_slot")
		if !errors.Is(err, irbackup.ErrPositionUnavailable) {
			t.Fatalf("CaptureBackupPosition = (%+v, %v); want errors.Is ErrPositionUnavailable", pos, err)
		}
		if pos != (ir.Position{}) {
			t.Errorf("position = %+v; want empty (a WAL LSN here is the pre-item-163 foreign-position defect)", pos)
		}
		if !strings.Contains(err.Error(), wantMention) {
			t.Errorf("err = %v; want the remedy to mention %q", err, wantMention)
		}
	}
	t.Run("change log absent", func(t *testing.T) { capture(t, "trigger setup") })
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"items"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Run("change log present", func(t *testing.T) { capture(t, "re-run `backup full`") })
}
