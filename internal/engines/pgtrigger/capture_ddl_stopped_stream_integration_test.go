//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SLM-1f, REGRADED BY MEASUREMENT (filed 2026-09-02 during SLM-1c as "the
// postgres-trigger lane has no relation cache and no boundary door for this
// class at all — a stopped-stream zone swap lands rows unchecked"; measured
// 2026-09-03 and found NARROWER than that).
//
// A Postgres event trigger fires whether or not sluice is running, so the
// swap is recorded as an op='X' DDL marker at the moment it happens, and a
// warm resume meets that marker on its next poll and REFUSES. The lane has
// no zone-SPECIFIC door, which is what the filing saw; it does not need one,
// because its generic DDL refusal already covers the class.
//
// The residual is real but different: an --allow-polled-fingerprint install
// has no event triggers, so no DDL is visible to it at all — the documented
// DDL-DETECTION-ABSENT posture, which is not specific to this class.
//
// This pin exists because that reasoning is only worth as much as the
// measurement behind it, and the FIRST attempt at it resumed from
// ir.Position{} — the from-now sentinel, which skips every pre-existing
// change-log row. It measured a cold start, saw no refusal, and would have
// recorded a false confirmation of the filing.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestTriggerLane_StoppedStreamDDLIsRefusedOnResume(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	mustExec := func(stmt string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	mustExec(`CREATE TABLE public.zt (id int PRIMARY KEY, ts timestamp)`)
	mustExec(`INSERT INTO public.zt VALUES (1, '2026-06-15 20:00:00')`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"zt"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	mustExec(`DELETE FROM public.sluice_change_log WHERE op = 'X'`)

	// A WARM-RESUME position, taken BEFORE the swap. The first cut of this
	// probe resumed from ir.Position{}, which is the from-now sentinel and
	// skips every pre-existing change-log row — so it measured a cold start
	// and would have recorded a false confirmation.
	var lastID int64
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id),0) FROM public.sluice_change_log").Scan(&lastID); err != nil {
		t.Fatalf("read last id: %v", err)
	}
	resumeFrom, err := encodePos(pgTriggerPos{LastID: lastID})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}

	// The stream is NOT running. The source-side swap happens anyway, under
	// a non-UTC session, exactly as SLM-1c's shape does on the native lane.
	mustExec(`SET TimeZone='Asia/Tokyo'; ALTER TABLE public.zt ALTER COLUMN ts TYPE timestamptz`)
	mustExec(`INSERT INTO public.zt VALUES (2, '2026-06-15 20:00:00+00')`)

	n := countXRows(t, ctx, db)
	t.Logf("op='X' markers recorded while the stream was stopped: %d (%s)",
		n, strings.Join(xRowDetail(t, ctx, db), " | "))

	// Now open a reader and poll, the way a resume does.
	eng := Engine{}
	generic, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	reader, ok := generic.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *CDCReader", generic)
	}

	streamCtx, streamCancel := context.WithTimeout(ctx, 30*time.Second)
	defer streamCancel()
	changes, err := reader.StreamChanges(streamCtx, resumeFrom)
	if err != nil {
		t.Logf("StreamChanges refused at open: %v", err)
		return
	}
	for range changes {
		// drain until the reader stops
	}
	readerErr := reader.Err()
	if readerErr == nil {
		t.Fatalf("the stopped-stream zone swap produced %d DDL marker(s) and the resume delivered rows with NO "+
			"error — the trigger lane would be carrying post-swap rows into a target whose column still has the "+
			"old zone semantics, silently (SLM-1f)", n)
	}
	if !strings.Contains(readerErr.Error(), "DDL") {
		t.Errorf("the resume refused, but not for the DDL it saw: %v", readerErr)
	}
	if n != 1 {
		t.Errorf("op='X' markers = %d; want exactly 1 — the refusal above must be caused by THIS swap, not by "+
			"leftover markers", n)
	}
}
