//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0060 (F11) — CDC apply-side schema-drift diff in refuse-loudly
// messages. End-to-end PG → PG integration test: a source ALTER
// outside the ADD-COLUMN auto-forwarding scope (drop column, rename
// column, alter type, create index, drop index) triggers a
// refuse-loudly error from the streamer, and the surfaced error
// names the SPECIFIC column / index / constraint that drifted.
//
// The test runs ONE testcontainers PG instance per scenario (cold-
// start + replication slot), exercises the refused-shape, and
// asserts both the shape name (existing contract from ADR-0058) and
// the drift report entries (new contract from ADR-0060).

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_SchemaDrift_PG_RefuseLoudlyIncludesDriftReport pins
// the F11 contract end-to-end on PG → PG: a refused-shape scenario
// surfaces an error message that NAMES the specific column that
// drifted, with the operator-action hint inline.
//
// ADR-0091 narrowed the refusal catalog: under default-on forwarding
// the intercept now FORWARDS drop-column / alter-column-type (so they
// no longer produce a refusal to assert against), and the seed-guard
// SKIPS a destructive shape at the first post-cold-start boundary
// anyway. RENAME COLUMN remains the canonical refuse-loudly shape (it
// is ambiguous with drop+add — data-loss risk — and is never
// seed-guarded), so it is the F11 drift-report-in-refusal pin here.
// Drop/alter/index/check FORWARDING is covered by the per-shape
// forward integration matrix; multi-shape combos by the unit drift
// tests.
//
// Known limitation: CREATE INDEX / DROP INDEX produce no pgoutput
// RelationMessage (no column-shape change), so the intercept never
// classifies them on PG via CDC — see [Engine.NormalizeForCDCComparison]
// (ADR-0091) which strips secondary indexes from the comparison for
// exactly this reason.
func TestStreamer_SchemaDrift_PG_RefuseLoudlyIncludesDriftReport(t *testing.T) {
	scenarios := []struct {
		name string
		// preDDL is applied to the source after the table exists and
		// before sluice starts streaming (it creates the "pre"
		// shape).
		preDDL string
		// driftDDL is the source-side change that triggers refuse-
		// loudly mid-stream. Must be a shape ADR-0091 still refuses
		// (RENAME or a multi-shape combo).
		driftDDL string
		// Substrings that MUST appear in the surfaced error: the
		// shape name + the rendered drift entries (ADR-0060 contract).
		wantSubstrs []string
	}{
		{
			name:   "rename-column",
			preDDL: "ALTER TABLE widgets ADD COLUMN old_label VARCHAR(100);",
			driftDDL: `ALTER TABLE widgets RENAME COLUMN old_label TO new_label;
INSERT INTO widgets (id, name, new_label) VALUES (10, 'post-rename', 'lbl');`,
			wantSubstrs: []string{
				"RENAME COLUMN",
				"[column-renamed]",
				"old_label",
				"new_label",
				"drained model",
			},
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
			defer cleanup()

			// Source setup: base widgets table + per-scenario preDDL.
			baseDDL := `
				CREATE TABLE widgets (
					id INT PRIMARY KEY,
					name TEXT NOT NULL
				);
				ALTER TABLE widgets REPLICA IDENTITY FULL;
				INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
			`
			applyPGDDL(t, sourceDSN, baseDDL)
			if sc.preDDL != "" {
				applyPGDDL(t, sourceDSN, sc.preDDL)
			}

			pgEng, ok := engines.Get("postgres")
			if !ok {
				t.Fatal("postgres engine not registered")
			}

			// Default-on forwarding (ADR-0091) engages the intercept;
			// the refuse-loudly path is taken because RENAME is the one
			// shape the intercept refuses (data-loss-ambiguous).
			streamer := &Streamer{
				Source:    pgEng,
				Target:    pgEng,
				SourceDSN: sourceDSN,
				TargetDSN: targetDSN,
				StreamID:  "test-drift-pg-" + sc.name,
			}

			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()

			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			// Wait for cold-start to land the seed rows.
			if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
				t.Fatalf("phase A: bulk-copy never landed seed rows")
			}

			// Trigger the drift.
			applyPGDDL(t, sourceDSN, sc.driftDDL)

			// Wait for the refuse-loudly error to surface.
			var err error
			select {
			case err = <-runErr:
			case <-time.After(60 * time.Second):
				t.Fatal("streamer did not surface refuse-loudly error within timeout")
			}
			if err == nil {
				t.Fatal("streamer returned nil error; expected refuse-loudly")
			}
			msg := err.Error()
			for _, want := range sc.wantSubstrs {
				if !strings.Contains(msg, want) {
					t.Errorf("refusal error missing %q\nfull message:\n%s", want, msg)
				}
			}
		})
	}
}
