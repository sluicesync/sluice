//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-07-23 D0-2 gate — warm-resume reconciliation of the pushed
// publication row filter with the current `--where` flags.
//
// ADR-0176 made `--where` the first flag whose effect is DURABLE
// source-side catalog state: the pushed predicate lives on the stream's
// publication, and warm resume deliberately never re-ensures it. Pre-fix,
// resuming with a widened, changed, or REMOVED `--where` silently left the
// server filtering on the stale predicate — under-delivery that is
// unobservable client-side by construction, persisting across every
// restart, at exit 0 with `sync status` green. The fix records the pushed
// subset's canonical hash in sluice_cdc_state (publication_name's exact
// sibling) and refuses a drifted warm resume with
// SLUICE-E-WHERE-PUSHDOWN-DRIFT naming the escapes.
//
// Named TestPublicationScope_* so the suite rides the existing
// pipeline-rest-other CI shard filter.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestPublicationScope_PushdownFilterDrift drives the whole D0-2 arc on
// real PG: cold start filtered → stop → (widened --where refuses) →
// (removed --where refuses) → (same --where resumes byte-identically and
// still delivers) → (--restart-from-scratch escape re-establishes under
// the new predicate). The two refusal arcs are RED on pre-fix HEAD (they
// silently resumed).
func TestPublicationScope_PushdownFilterDrift(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE orders (id int PRIMARY KEY, note text);
		ALTER TABLE orders REPLICA IDENTITY FULL;
		INSERT INTO orders (id, note) VALUES (1, 'seed');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	newStreamer := func(filters map[string]string, restart bool) *Streamer {
		return &Streamer{
			Source:             pgEng,
			Target:             pgEng,
			SourceDSN:          sourceDSN,
			TargetDSN:          targetDSN,
			StreamID:           "drift-flt",
			SlotName:           "drift_flt",
			Filter:             migcore.TableFilter{Include: []string{"orders"}},
			RowFilters:         filters,
			RestartFromScratch: restart,
		}
	}
	runUntilCanceled := func(t *testing.T, s *Streamer, waitRows int) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- s.Run(ctx) }()
		deadline := time.Now().Add(90 * time.Second)
		for pollRowCount(targetDSN, "orders") < waitRows {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("stream never delivered %d rows (rows=%d)", waitRows, pollRowCount(targetDSN, "orders"))
			}
			select {
			case err := <-errCh:
				t.Fatalf("stream exited before delivering %d rows: %v", waitRows, err)
			case <-time.After(250 * time.Millisecond):
			}
		}
		cancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("run exited with error: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("run did not exit after cancel")
		}
	}
	// expectDriftRefusal runs s and requires the coded refusal — RED on
	// pre-fix HEAD, where the resume proceeded silently (detected here as
	// Run still streaming after the grace window).
	expectDriftRefusal := func(t *testing.T, s *Streamer, label string) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- s.Run(ctx) }()
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatalf("%s: Run returned nil; want the SLUICE-E-WHERE-PUSHDOWN-DRIFT refusal", label)
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeWherePushdownDrift {
				t.Fatalf("%s: err = %v; want %s", label, err, sluicecode.CodeWherePushdownDrift)
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("%s: D0-2 regression — the drifted warm resume was NOT refused (the server keeps filtering on the stale pushed predicate, silently under-delivering)", label)
		}
	}

	// ---- Cold start with the original filter; the hash is recorded ----
	runUntilCanceled(t, newStreamer(map[string]string{"orders": "id < 100"}, false), 1)
	recordedHash := pgQueryOne[string](t, targetDSN,
		`SELECT COALESCE(row_filter_hash, '') FROM sluice_cdc_state WHERE stream_id = $1`, "drift-flt")
	if recordedHash == "" {
		t.Fatal("cold start did not record row_filter_hash beside publication_name")
	}

	// ---- Widened --where on resume: refuse (server still filters id<100;
	// rows 100..199 would silently never arrive) ----
	expectDriftRefusal(t, newStreamer(map[string]string{"orders": "id < 200"}, false), "widened --where")

	// ---- REMOVED --where on resume: refuse (D0-2's worst variant —
	// pre-fix there was zero operator-visible signal) ----
	expectDriftRefusal(t, newStreamer(nil, false), "removed --where")

	// ---- Same filter: resumes byte-identically and still delivers ----
	sameCtx, sameCancel := context.WithCancel(context.Background())
	sameErr := make(chan error, 1)
	go func() { sameErr <- newStreamer(map[string]string{"orders": "id < 100"}, false).Run(sameCtx) }()
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (2, 'cdc-warm');")
	if !waitForRowCount(t, targetDSN, "orders", 2, 90*time.Second) {
		sameCancel()
		t.Fatal("same-filter warm resume did not deliver — the drift check broke the byte-identical resume path")
	}
	select {
	case err := <-sameErr:
		t.Fatalf("same-filter resume exited early: %v", err)
	default:
	}
	sameCancel()
	select {
	case err := <-sameErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("same-filter resume exited with error: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("same-filter resume did not exit after cancel")
	}
	if got := pgQueryOne[string](t, targetDSN,
		`SELECT COALESCE(row_filter_hash, '') FROM sluice_cdc_state WHERE stream_id = $1`, "drift-flt"); got != recordedHash {
		t.Errorf("same-filter resume changed the recorded hash: %q -> %q", recordedHash, got)
	}

	// ---- The --restart-from-scratch escape: re-establishes under the NEW
	// predicate (rescopes the publication, re-snapshots, re-records). A PG
	// cold restart also requires dropping the old slot first — the
	// pre-existing loud slot-exists refusal names that step itself ("drop
	// it before starting a snapshot stream"), so this is the operator
	// following the guided escape chain, not test-side magic. ----
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (150, 'wide-scope');")
	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dropSlotWithRetry(t, srcDB, "sluice_drift_flt", 30*time.Second)
	runUntilCanceled(t, newStreamer(map[string]string{"orders": "id < 200"}, true), 3)
	if got := pgQueryOne[string](t, targetDSN,
		`SELECT COALESCE(row_filter_hash, '') FROM sluice_cdc_state WHERE stream_id = $1`, "drift-flt"); got == recordedHash || got == "" {
		t.Errorf("restart-from-scratch did not re-record the new filter's hash (got %q, old %q)", got, recordedHash)
	}
}
