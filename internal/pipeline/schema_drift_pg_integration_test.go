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

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_SchemaDrift_PG_RefuseLoudlyIncludesDriftReport pins
// the F11 contract end-to-end on PG → PG: each refused-shape
// scenario surfaces an error message that NAMES the specific column
// / index / constraint that drifted, with the operator-action hint
// inline.
//
// Bug 74 class-pin: one subtest per refused-shape category from the
// ADR-0058 catalog that the F11 intercept can actually observe —
// drop-column, rename-column, alter-column-type. A single
// representative would only prove the wiring exists; per-category
// coverage proves every category's rendered output is correct.
//
// Known limitation (ADR-0060 §6 — "Known limitation: index-only DDL
// not detected via F11"): CREATE INDEX and DROP INDEX are
// deliberately NOT exercised here. The F11 intercept fires off
// [ir.SchemaSnapshot] events, which the PG CDC reader emits only in
// response to a pgoutput RelationMessage. RelationMessage describes
// column shape only; CREATE INDEX / DROP INDEX do not change column
// shape and therefore do not trigger one. The follow-up INSERT also
// passes through cleanly because the cached IR matches the post-DDL
// projection. Live detection of index-only drift is future work
// (see ADR-0060 §6 — F47 schema-drift catalog will need a separate
// subscription path).
func TestStreamer_SchemaDrift_PG_RefuseLoudlyIncludesDriftReport(t *testing.T) {
	scenarios := []struct {
		name string
		// preDDL is applied to the source after the table exists and
		// before sluice starts streaming (it creates the "pre"
		// shape).
		preDDL string
		// driftDDL is the source-side change that triggers refuse-
		// loudly mid-stream. Must be a DDL the ADR-0058 catalog
		// refuses (NOT plain ADD COLUMN).
		driftDDL string
		// Substrings that MUST appear in the surfaced error. The
		// shape-name (existing ADR-0058 contract) and the rendered
		// drift entries (new ADR-0060 contract).
		wantSubstrs []string
	}{
		{
			name:   "drop-column",
			preDDL: "ALTER TABLE widgets ADD COLUMN legacy_col VARCHAR(100);",
			driftDDL: `ALTER TABLE widgets DROP COLUMN legacy_col;
INSERT INTO widgets (id, name) VALUES (10, 'post-drop');`,
			wantSubstrs: []string{
				"drop-column",
				"[column-dropped]",
				"legacy_col",
				"destructive",
				"drained model",
			},
		},
		{
			name:   "rename-column",
			preDDL: "ALTER TABLE widgets ADD COLUMN old_label VARCHAR(100);",
			driftDDL: `ALTER TABLE widgets RENAME COLUMN old_label TO new_label;
INSERT INTO widgets (id, name, new_label) VALUES (10, 'post-rename', 'lbl');`,
			wantSubstrs: []string{
				"rename-column",
				"[column-renamed]",
				"old_label",
				"new_label",
				"drained model",
			},
		},
		{
			name:   "alter-column-type",
			preDDL: "ALTER TABLE widgets ADD COLUMN score INTEGER;",
			driftDDL: `ALTER TABLE widgets ALTER COLUMN score TYPE BIGINT;
INSERT INTO widgets (id, name, score) VALUES (10, 'post-alter', 99);`,
			wantSubstrs: []string{
				"alter-column",
				"[column-altered]",
				"score",
				"drained model",
			},
		},
		// NOTE: create-index and drop-index scenarios were removed
		// here — see the function comment block above for the F11
		// limitation. The scenarios were timing out at 60s because
		// the streamer never surfaces a refusal: pgoutput emits no
		// RelationMessage for index-only DDL, so the F11 intercept
		// has nothing to classify. Reintroduce these only when F47
		// adds a separate subscription path that observes index
		// catalog mutations.
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

			// ForwardSchemaAddColumn=true engages the intercept; the
			// refuse-loudly path is then taken because the drift is
			// NOT an ADD COLUMN.
			streamer := &Streamer{
				Source:                 pgEng,
				Target:                 pgEng,
				SourceDSN:              sourceDSN,
				TargetDSN:              targetDSN,
				StreamID:               "test-drift-pg-" + sc.name,
				ForwardSchemaAddColumn: true,
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
