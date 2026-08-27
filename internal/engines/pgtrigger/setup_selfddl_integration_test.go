//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 257 integration pin: a `trigger setup` re-run over an existing
// STREAMED install must write ZERO op='X' rows — the install's own
// sluice_capture_ddl_trg event trigger records DDL, and before the
// suppression fix it recorded setup's OWN statements (the ADR-0185 meta
// ADD COLUMN migration on every re-run; the opt-in's two ENABLE ALWAYS
// ALTERs per table), so the next warm resume refused "observed source-side
// DDL" and steered a needless full re-copy.
//
// The pre-existing door test (TestCDCOpen_CaptureShapeDoor "re-setup
// repairs every defect") only OPENED the reader after repair, which is
// exactly why the poisoning shipped invisible — an X row past the
// watermark refuses at the first poll, not at open. Every stage here
// therefore CONSUMES a fresh change through the resumed stream.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSetup_ReRunOverStreamedInstall_WritesNoSelfDDL drives the re-setup ×
// posture matrix over one live install, asserting after each re-run that
// (1) the change log holds zero op='X' rows and (2) a warm resume from the
// persisted position consumes a NEW change cleanly. The final stage
// reconstructs the install a PRE-fix binary leaves behind — the capture-DDL
// function WITHOUT the suppression check, plus the pre-v3 meta shape — so
// the first re-setup over an OLD install is pinned too: that run's meta
// ALTER is a REAL column add fired while the old function body (which
// ignores the GUC) is still installed, and only the plan ordering (the
// function's CREATE OR REPLACE ahead of every TAG-watched statement) keeps
// it unrecorded.
func TestSetup_ReRunOverStreamedInstall_WritesNoSelfDDL(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE selfddl_t (id BIGINT PRIMARY KEY, note TEXT)`)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	setup := func(t *testing.T, optIn bool) {
		t.Helper()
		if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"selfddl_t"}, CaptureReplicatedWrites: optIn}); err != nil {
			t.Fatalf("Setup(optIn=%t): %v", optIn, err)
		}
	}
	wantZeroXRows := func(t *testing.T, after string) {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sluice_change_log WHERE op = 'X'`).Scan(&n); err != nil {
			t.Fatalf("count X rows: %v", err)
		}
		if n == 0 {
			return
		}
		var detail sql.NullString
		_ = db.QueryRowContext(
			ctx,
			`SELECT string_agg(table_name || ' [' || (pk_jsonb->>'command_tag') || ']', '; ' ORDER BY id) FROM sluice_change_log WHERE op = 'X'`,
		).Scan(&detail)
		t.Fatalf("%s wrote %d op='X' row(s) for sluice's OWN DDL (%s) — the next warm resume would refuse "+
			"\"observed source-side DDL\" and steer a full re-copy (Bug 257)", after, n, detail.String)
	}

	// streamOne resumes the stream at `from`, drives one INSERT, and
	// demands it arrives — the consume-not-just-open half. Returns the
	// consumed event's position as the next resume point.
	nextID := int64(0)
	streamOne := func(t *testing.T, from ir.Position) ir.Position {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("openCDCReader: %v", err)
		}
		reader := r.(*CDCReader)
		defer func() { _ = reader.Close() }()
		out, err := reader.StreamChanges(ctx, from)
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		nextID++
		applyPGSQL(t, dsn, fmt.Sprintf(`INSERT INTO selfddl_t (id, note) VALUES (%d, 'x')`, nextID))
		got := drainEvents(t, out, 1, 20*time.Second)
		if len(got) != 1 {
			t.Fatalf("warm resume consumed %d event(s), want the INSERT id=%d; reader.Err() = %v", len(got), nextID, reader.Err())
		}
		ins, ok := got[0].(ir.Insert)
		if !ok {
			t.Fatalf("consumed %T, want ir.Insert", got[0])
		}
		if ins.Row["id"] != nextID {
			t.Fatalf("consumed Insert id=%v, want %d", ins.Row["id"], nextID)
		}
		return got[0].Pos()
	}

	// Fresh install: setup's ALTERs precede CREATE EVENT TRIGGER, so a
	// fresh install was never affected — the control cell.
	setup(t, false)
	wantZeroXRows(t, "a FRESH plain setup")
	pos := streamOne(t, ir.Position{}) // zero position anchors "from now"

	t.Run("plain re-setup over the streamed install", func(t *testing.T) {
		setup(t, false)
		wantZeroXRows(t, "a plain re-setup") // pre-fix: 1 X row (the meta ALTER)
		pos = streamOne(t, pos)
	})

	t.Run("opt-in re-setup over the streamed install", func(t *testing.T) {
		setup(t, true)
		wantZeroXRows(t, "an opt-in re-setup") // pre-fix: 3 X rows (meta + both ENABLE ALWAYS)
		pos = streamOne(t, pos)
	})

	t.Run("converge back to plain", func(t *testing.T) {
		setup(t, false)
		wantZeroXRows(t, "the converge-to-plain re-setup")
		pos = streamOne(t, pos)
	})

	t.Run("first re-setup over an OLD-binary install (pre-suppression function, pre-v3 meta)", func(t *testing.T) {
		// Reconstruct the pre-fix on-disk shape: the capture-DDL function
		// body without the suppression check, derived by stripping exactly
		// the block the fix added — with the strip VERIFIED (a strip whose
		// marker stopped matching would silently test the fixed body).
		newFn := renderCaptureDDLFunction("public", `"public"."`+ChangeLogTable+`"`)
		oldFn := strings.Replace(newFn, captureDDLSuppressionCheck, "", 1)
		if oldFn == newFn || strings.Contains(oldFn, setupSessionGUC) {
			t.Fatalf("failed to reconstruct the pre-fix function body (suppression block not stripped)")
		}
		// The v0.132.2 meta shape: no posture column, schema_version 2 —
		// which also makes the coming re-setup's ADD COLUMN IF NOT EXISTS
		// a REAL column addition, the loudest recordable shape. Staged on
		// ONE suppressed session (the still-installed FIXED function
		// honors the GUC) so the staging itself writes no X rows — a
		// DELETE cleanup here would punch a hole in the change-log id run
		// and trip the gap-freedom guard on the next resume. The old
		// function body lands LAST, once the recordable staging is done.
		applyPGSQL(t, dsn, `
			SET `+setupSessionGUC+` = 'on';
			ALTER TABLE sluice_change_log_meta DROP COLUMN capture_replicated_writes;
			UPDATE sluice_change_log_meta SET schema_version = 2;
		`)
		applyPGSQL(t, dsn, oldFn) // CREATE FUNCTION's tag is unwatched — records nothing
		wantZeroXRows(t, "staging the old-binary install shape")

		setup(t, false)
		wantZeroXRows(t, "the first re-setup over an old-binary install")
		pos = streamOne(t, pos)
	})

	t.Run("operator DDL still records and refuses (suppression must not over-reach)", func(t *testing.T) {
		// The other mutation direction, pinned live: the suppression is
		// scoped to setup's own session, so genuine operator DDL keeps
		// recording and the warm resume keeps refusing on it.
		applyPGSQL(t, dsn, `ALTER TABLE selfddl_t ADD COLUMN extra TEXT`)
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sluice_change_log WHERE op = 'X'`).Scan(&n); err != nil {
			t.Fatalf("count X rows: %v", err)
		}
		if n != 1 {
			t.Fatalf("operator ALTER TABLE recorded %d op='X' row(s), want 1 — the Bug 257 suppression must not swallow operator DDL", n)
		}
		r, err := openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("openCDCReader: %v", err)
		}
		reader := r.(*CDCReader)
		defer func() { _ = reader.Close() }()
		out, err := reader.StreamChanges(ctx, pos)
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		if got := drainEvents(t, out, 1, 20*time.Second); len(got) != 0 {
			t.Fatalf("resume past an operator DDL marker emitted %d event(s), want the refusal instead: %+v", len(got), got)
		}
		if err := reader.Err(); err == nil || !strings.Contains(err.Error(), "observed source-side DDL") {
			t.Fatalf("resume past an operator DDL marker did not refuse with the §7 message; Err() = %v", err)
		}
	})
}
