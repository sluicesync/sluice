//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-07-23 D0-1 gate — the unchanged-TOAST spurious-DELETE class.
//
// On a filtered PG sync whose predicate references a column that TOASTs
// out-of-line, pgoutput's UPDATE new-tuple carries that column as 'u'
// (unchanged TOAST) whenever a DIFFERENT column changed — even under
// REPLICA IDENTITY FULL, where the OLD tuple is complete. Pre-fix, the
// decoded After omitted the column, the row-move evaluation read it as
// UNKNOWN→false, and an in-scope sibling-column UPDATE was mis-classified
// as a move-OUT — emitting a DELETE for a row still in scope at the
// source, silently, at exit 0. The fix backfills After's omitted columns
// from the RI-FULL Before in the reader (exact — PG guarantees unchanged
// ⇒ old == new), with an After-side completeness refusal in route() as
// the loud belt (SLUICE-E-WHERE-CDC-AFTER-IMAGE).

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_WhereFilter_PGUnchangedToastSiblingUpdate drives a filtered
// PG→PG sync with the predicate on a forced-out-of-line TEXT column, then
// UPDATEs only a SIBLING column. The row must remain on the target with
// the sibling update applied and the TOASTed value intact — pre-fix it was
// spuriously DELETEd (the D0-1 silent-loss shape, RED on the unfixed code).
func TestStreamer_WhereFilter_PGUnchangedToastSiblingUpdate(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	// STORAGE EXTERNAL forces out-of-line TOAST (no compression) for any
	// value past the ~2KB threshold, making the 'u' new-tuple datum
	// deterministic; the 9,600-byte md5 chain matches the audit's observed
	// repro. REPLICA IDENTITY FULL is the filtered-sync precondition.
	applyDDL(t, sourceDSN, `
		CREATE TABLE docs (id BIGINT NOT NULL PRIMARY KEY, body TEXT NOT NULL, rev INT NOT NULL);
		ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL;
		ALTER TABLE docs REPLICA IDENTITY FULL;
		INSERT INTO docs (id, body, rev)
		SELECT 1, string_agg(md5(g::text), ''), 1 FROM generate_series(1, 300) g;
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer := &Streamer{
		Source:     pgEng,
		Target:     pgEng,
		SourceDSN:  sourceDSN,
		TargetDSN:  targetDSN,
		StreamID:   "where-pg-toast",
		RowFilters: map[string]string{"docs": "body != ''"},
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCount(t, targetDSN, "docs", 1, 90*time.Second) {
		t.Fatalf("filtered cold-start never delivered the TOASTed seed row (rows=%d)", pollRowCount(targetDSN, "docs"))
	}

	// The crux: change ONLY the sibling column. pgoutput delivers the new
	// tuple with body as unchanged-TOAST ('u'); the row stays in scope.
	applyDDL(t, sourceDSN, "UPDATE docs SET rev = 2 WHERE id = 1;")
	// Sentinel: once it lands, CDC has drained through the sibling UPDATE.
	applyDDL(t, sourceDSN, "INSERT INTO docs (id, body, rev) VALUES (2, 'sentinel', 1);")

	deadline := time.Now().Add(90 * time.Second)
	for pollRowCount(targetDSN, "docs") < 1 || !pgRowExistsByID(t, streamCtx, targetDSN, "docs", 2) {
		if time.Now().After(deadline) {
			t.Fatalf("sentinel insert (id=2) never landed on the target (rows=%d)", pollRowCount(targetDSN, "docs"))
		}
		select {
		case err := <-runErr:
			t.Fatalf("stream exited before the sentinel landed: %v", err)
		case <-time.After(250 * time.Millisecond):
		}
	}

	// D0-1: the in-scope row must NOT have been deleted by the sibling
	// UPDATE (pre-fix: route() saw After without body → UNKNOWN → move-OUT
	// → spurious DELETE).
	if !pgRowExistsByID(t, streamCtx, targetDSN, "docs", 1) {
		t.Fatal("D0-1 regression: a sibling-column UPDATE on a row whose filtered column is unchanged TOAST " +
			"was routed as a move-OUT and DELETEd the row from the target")
	}
	// The UPDATE itself must have applied…
	if got := pgQueryOne[int](t, targetDSN, `SELECT rev FROM docs WHERE id = 1`); got != 2 {
		t.Errorf("sibling UPDATE did not apply: target rev = %d, want 2", got)
	}
	// …and the TOASTed value must be byte-identical to the source's (the
	// backfill copies the decoded Before value — never a placeholder).
	srcMD5 := pgQueryOne[string](t, sourceDSN, `SELECT md5(body) FROM docs WHERE id = 1`)
	if got := pgQueryOne[string](t, targetDSN, `SELECT md5(body) FROM docs WHERE id = 1`); got != srcMD5 {
		t.Errorf("TOASTed column diverged after the sibling UPDATE: target md5 %s, source md5 %s", got, srcMD5)
	}

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run exited with error: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("run did not exit after cancel")
	}
}
