//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0176 prerequisite chunk — per-stream publications with the
// control-state ratchet, end to end on real Postgres:
//
//   - a NEW filtered PG-source stream derives a per-stream publication
//     (`sluice_<stream-id>`), records it in its sluice_cdc_state row,
//     and a warm resume ratchets onto the RECORD without the operator
//     re-passing --publication-name — while the shared `sluice_pub`
//     default is never created for it;
//   - cleanup parity: the pre-anchor abandon teardown that drops the
//     stream's just-created slot (Bug 177) also drops its per-stream
//     publication — and NEVER drops the shared `sluice_pub` default.
//
// The legacy floor (unfiltered streams stay on `sluice_pub`) is pinned
// implicitly and non-vacuously by the pre-existing ADR-0175 gates in
// publication_scope_conflict_pg_integration_test.go: those streams
// carry no filter and would fail if they silently derived per-stream
// names. Named TestPublicationScope_* so the suite rides the existing
// pipeline-rest-other CI shard filter.

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// publicationExists reports whether the named publication exists on dsn.
func publicationExists(t *testing.T, dsn, name string) bool {
	t.Helper()
	return pgQueryOne[bool](t, dsn,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, name)
}

// waitForRecordedPublication polls the target's sluice_cdc_state row
// until publication_name matches want (the record lands with the
// cold-start anchor write, after bulk-copy).
func waitForRecordedPublication(t *testing.T, targetDSN, streamID, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := pgQueryOne[string](t, targetDSN,
			`SELECT COALESCE((SELECT publication_name FROM sluice_cdc_state WHERE stream_id = $1), '')`, streamID)
		if got == want {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// TestPublicationScope_FilteredNewStreamDerivesAndRatchets is the
// chunk's end-to-end gate: derive → record → warm-resume reuse.
func TestPublicationScope_FilteredNewStreamDerivesAndRatchets(t *testing.T) {
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

	newStreamer := func() *Streamer {
		return &Streamer{
			Source:    pgEng,
			Target:    pgEng,
			SourceDSN: sourceDSN,
			TargetDSN: targetDSN,
			StreamID:  "wave-flt",
			SlotName:  "wave_flt",
			// NO PublicationName: (b) of the derivation condition. The
			// row filter is (a); the fresh control table is (c).
			Filter:     migcore.TableFilter{Include: []string{"orders"}},
			RowFilters: map[string]string{"orders": "id < 100"},
		}
	}

	// ---- Cold start: derive + record ----
	ctxA, cancelA := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	go func() { errA <- newStreamer().Run(ctxA) }()

	if !waitForRowCount(t, targetDSN, "orders", 1, 60*time.Second) {
		cancelA()
		t.Fatal("cold-start snapshot never delivered the seed row")
	}
	// The per-stream default was derived from the stream id...
	if !publicationExists(t, sourceDSN, "sluice_wave_flt") {
		t.Error("derived per-stream publication sluice_wave_flt does not exist on the source")
	}
	// ...and the shared default was never created for this stream.
	if publicationExists(t, sourceDSN, "sluice_pub") {
		t.Error("shared default sluice_pub was created for a stream that derived a per-stream publication")
	}
	// The record lands with the cold-start anchor write.
	if !waitForRecordedPublication(t, targetDSN, "wave-flt", "sluice_wave_flt", 60*time.Second) {
		t.Fatal("sluice_cdc_state.publication_name never recorded the derived per-stream publication")
	}
	// CDC through the derived publication delivers (in-scope row).
	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (2, 'cdc-cold');")
	if !waitForRowCount(t, targetDSN, "orders", 2, 60*time.Second) {
		t.Fatal("CDC through the derived per-stream publication never delivered")
	}

	cancelA()
	select {
	case err := <-errA:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("cold-start run exited with error: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("cold-start run did not exit after cancel")
	}

	// ---- Warm resume: the ratchet reuses the RECORD, no flag passed ----
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	errB := make(chan error, 1)
	go func() { errB <- newStreamer().Run(ctxB) }()

	applyDDL(t, sourceDSN, "INSERT INTO orders (id, note) VALUES (3, 'cdc-warm');")
	if !waitForRowCount(t, targetDSN, "orders", 3, 90*time.Second) {
		t.Fatal("warm resume did not deliver — the ratchet failed to reuse the recorded per-stream publication " +
			"(a resume against the wrong/default publication receives nothing for this table)")
	}
	// Still no shared default, and the record is unchanged.
	if publicationExists(t, sourceDSN, "sluice_pub") {
		t.Error("warm resume created the shared default sluice_pub despite the recorded per-stream publication")
	}
	if got := pgQueryOne[string](t, targetDSN,
		`SELECT COALESCE(publication_name, '') FROM sluice_cdc_state WHERE stream_id = $1`, "wave-flt"); got != "sluice_wave_flt" {
		t.Errorf("recorded publication after warm resume = %q; want sluice_wave_flt", got)
	}

	select {
	case err := <-errB:
		t.Fatalf("warm-resume run exited early: %v", err)
	default:
	}
}

// TestPublicationScope_AbandonDropsPerStreamPublicationOnly pins
// cleanup parity: the pre-anchor abandon teardown (here: the Bug 9
// populated-target refusal, which fires AFTER the publication and slot
// are created) drops the stream's per-stream publication alongside its
// slot — and the sibling default-publication run proves the shared
// `sluice_pub` is NEVER dropped by the same path.
func TestPublicationScope_AbandonDropsPerStreamPublicationOnly(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE orders (id int PRIMARY KEY, note text);
		INSERT INTO orders (id, note) VALUES (1, 'seed');
	`)
	// Pre-populate the target so the cold start refuses (Bug 9) after
	// EnsurePublication + slot creation — the abandon path under test.
	applyDDL(t, targetDSN, `
		CREATE TABLE orders (id int PRIMARY KEY, note text);
		INSERT INTO orders (id, note) VALUES (99, 'pre-existing');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	run := func(streamID, slot, publication string) error {
		s := &Streamer{
			Source:          pgEng,
			Target:          pgEng,
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			StreamID:        streamID,
			SlotName:        slot,
			PublicationName: publication,
			Filter:          migcore.TableFilter{Include: []string{"orders"}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		return s.Run(ctx)
	}

	// ---- Per-stream publication: refused cold start leaves NOTHING ----
	if err := run("wave-gone", "wave_gone", "wave_gone"); err == nil {
		t.Fatal("cold start into a populated target did not refuse; the abandon path never ran")
	}
	if publicationExists(t, sourceDSN, "sluice_wave_gone") {
		t.Error("abandoned cold start orphaned its per-stream publication sluice_wave_gone (cleanup parity broken)")
	}
	if slotExists := pgQueryOne[bool](t, sourceDSN,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, "sluice_wave_gone"); slotExists {
		t.Error("abandoned cold start orphaned its slot (Bug 177 regression)")
	}

	// ---- Shared default: the SAME refusal must NOT drop sluice_pub ----
	if err := run("wave-default", "wave_default", ""); err == nil {
		t.Fatal("default-publication cold start into a populated target did not refuse")
	}
	if !publicationExists(t, sourceDSN, "sluice_pub") {
		t.Error("the abandon teardown dropped the SHARED default sluice_pub — it may serve other streams and must never be dropped")
	}
}
