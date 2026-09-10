// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The cold-start block in `sync status` (2026-09-08 user report).
//
// A cold start writes progress rows for hours before its sluice_cdc_state
// row exists, so without this block `sync status` reports a live migration
// exactly as it reports a dead one — which is what sent a real operator to
// strace mid-migration.
func TestStatusRendersColdStartsInProgress(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	coldStarts := []ir.MigrationState{{
		MigrationID: "sync-prod-cutover",
		Phase:       ir.MigrationPhaseBulkCopy,
		StartedAt:   now.Add(-90 * time.Minute),
		UpdatedAt:   now.Add(-4 * time.Second),
	}}

	t.Run("text: replaces the not-found message an operator would misread", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		if err := renderStatus(&buf, nil, nil, nil, coldStarts, statusRenderOpts{Format: "text"}, now); err != nil {
			t.Fatalf("renderStatus: %v", err)
		}
		got := buf.String()
		// The stream id, not the migration id: the operator never typed
		// "sync-prod-cutover" and should not have to recognise it.
		for _, want := range []string{"cold start in progress", "prod-cutover", "bulk_copy", "4s ago"} {
			if !strings.Contains(got, want) {
				t.Errorf("cold-start block does not contain %q:\n%s", want, got)
			}
		}
		if strings.Contains(got, "no streams recorded on target") {
			t.Errorf("the empty-state message rendered ALONGSIDE a live cold start, which reads as a "+
				"contradiction:\n%s", got)
		}
		// The recovery instruction is the point of the block: without it
		// the operator still cannot tell working from dead.
		//
		// It asserts the PHASE, not the age. This used to require the
		// phrase "keeps climbing", from guidance that said a climbing
		// LAST PROGRESS WRITE age alone meant the run was gone. That was
		// wrong for the whole copy phase: the header row this renders
		// moves on markPhase/markComplete, while per-table progress goes
		// to a different table the lister does not read, so a long copy of
		// one large table holds the age still on a perfectly healthy run
		// (found by the A0909-P2b work). Pinning the old phrase would pin
		// the old, wrong advice.
		if !strings.Contains(got, "PHASE advancing means it is working") {
			t.Errorf("the block does not name the PHASE as the liveness signal, so an operator still cannot "+
				"tell a working cold start from a dead one — and the age alone cannot tell them, which is "+
				"why this asserts the phase:\n%s", got)
		}
	})

	t.Run("text: the migration id is never shown raw", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		if err := renderStatus(&buf, nil, nil, nil, coldStarts, statusRenderOpts{Format: "text"}, now); err != nil {
			t.Fatalf("renderStatus: %v", err)
		}
		if strings.Contains(buf.String(), "sync-prod-cutover") {
			t.Errorf("the internal migration id leaked into operator output:\n%s", buf.String())
		}
	})

	t.Run("json: the block is additive and omitted when empty", func(t *testing.T) {
		t.Parallel()
		var withCS, without bytes.Buffer
		if err := renderStatus(&withCS, nil, nil, nil, coldStarts, statusRenderOpts{Format: "json"}, now); err != nil {
			t.Fatalf("renderStatus: %v", err)
		}
		if err := renderStatus(&without, nil, nil, nil, nil, statusRenderOpts{Format: "json"}, now); err != nil {
			t.Fatalf("renderStatus: %v", err)
		}
		if strings.Contains(without.String(), "cold_starts_in_progress") {
			t.Errorf("the key appears with no cold starts; existing jq filters see a new field for no reason:\n%s",
				without.String())
		}
		var doc struct {
			ColdStarts []struct {
				StreamID         string `json:"stream_id"`
				Phase            string `json:"phase"`
				LastProgressAgeS int64  `json:"last_progress_age_seconds"`
			} `json:"cold_starts_in_progress"`
		}
		if err := json.Unmarshal(withCS.Bytes(), &doc); err != nil {
			t.Fatalf("decode: %v\n%s", err, withCS.String())
		}
		if len(doc.ColdStarts) != 1 {
			t.Fatalf("got %d cold starts, want 1", len(doc.ColdStarts))
		}
		if doc.ColdStarts[0].StreamID != "prod-cutover" {
			t.Errorf("stream_id = %q; want the operator-facing id", doc.ColdStarts[0].StreamID)
		}
		if doc.ColdStarts[0].LastProgressAgeS != 4 {
			t.Errorf("last_progress_age_seconds = %d; want 4 — the field a monitor uses to tell working from dead",
				doc.ColdStarts[0].LastProgressAgeS)
		}
	})

	t.Run("filter: --stream-id matches the stream, not the prefix", func(t *testing.T) {
		t.Parallel()
		// "prod" must not match "prod-cutover". A prefix match here would
		// attribute one migration's progress to a different stream.
		states := []ir.MigrationState{
			{MigrationID: "sync-prod", Phase: ir.MigrationPhaseBulkCopy},
			{MigrationID: "sync-prod-cutover", Phase: ir.MigrationPhaseIndexes},
		}
		got := filterColdStarts(append([]ir.MigrationState(nil), states...), "prod")
		if len(got) != 1 || got[0].MigrationID != "sync-prod" {
			t.Fatalf("filterColdStarts(\"prod\") = %+v; want exactly the sync-prod row", got)
		}
	})

	t.Run("filter: a finished cold start is not in progress", func(t *testing.T) {
		t.Parallel()
		// The pipeline marks the row complete and leaves it; rendering it
		// under the in-progress header with a climbing age teaches the
		// operator to distrust the one signal the section provides
		// (A0909-P3). Both the all-streams and the one-stream forms drop it.
		states := []ir.MigrationState{
			{MigrationID: "sync-done", Phase: ir.MigrationPhaseComplete, UpdatedAt: now.Add(-6 * time.Hour)},
			{MigrationID: "sync-live", Phase: ir.MigrationPhaseIndexes, UpdatedAt: now.Add(-4 * time.Second)},
		}
		got := filterColdStarts(append([]ir.MigrationState(nil), states...), "")
		if len(got) != 1 || got[0].MigrationID != "sync-live" {
			t.Fatalf("filterColdStarts(\"\") = %+v; want only the live row", got)
		}
		if got := filterColdStarts(append([]ir.MigrationState(nil), states...), "done"); len(got) != 0 {
			t.Fatalf("filterColdStarts(\"done\") = %+v; want nothing — the copy finished", got)
		}
		var buf bytes.Buffer
		if err := renderStatus(&buf, nil, nil, nil, filterColdStarts(append([]ir.MigrationState(nil), states[:1]...), ""), statusRenderOpts{Format: "text"}, now); err != nil {
			t.Fatalf("renderStatus: %v", err)
		}
		if strings.Contains(buf.String(), "cold start in progress") {
			t.Fatalf("a completed cold start rendered under the in-progress header:\n%s", buf.String())
		}
	})
}

// The two packages agree on a STORED string, not on a shared symbol: the
// pipeline writes migration ids under this prefix and the CLI reads them
// back. A change on one side alone is a data-format break that would show
// up as "no cold starts ever appear", which is silent and looks exactly
// like the bug this feature fixed.
//
// The gate reads the pipeline's source rather than importing it, because
// syncMigrationID is unexported and the coupling being checked is the
// literal, not the function.
func TestSyncMigrationIDPrefixMatchesThePipeline(t *testing.T) {
	t.Parallel()

	src, err := os.ReadFile("../../internal/pipeline/resume.go")
	if err != nil {
		t.Fatalf("read the pipeline's resume.go: %v", err)
	}
	re := regexp.MustCompile(`func syncMigrationID\(streamID string\) string \{ return "([^"]+)" \+ streamID \}`)
	m := re.FindSubmatch(src)
	if m == nil {
		t.Fatalf("could not find syncMigrationID's literal in the pipeline. If it was reshaped, re-point this "+
			"gate rather than deleting it: the CLI's %q is only correct because it matches that function.",
			syncMigrationIDPrefix)
	}
	if got := string(m[1]); got != syncMigrationIDPrefix {
		t.Errorf("the pipeline writes migration ids under %q but the CLI looks for %q.\n"+
			"  Nothing fails loudly when these diverge — `sync status` simply never shows a cold start again, "+
			"which is indistinguishable from the blackout this feature exists to fix.", got, syncMigrationIDPrefix)
	}
}
