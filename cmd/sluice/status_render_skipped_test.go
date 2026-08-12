// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Pins for the audit-C-11 skip-ledger surfacing in `sync status` and
// the `sync stop` summary.

func statusSkips() []ir.SkippedTableRecord {
	return []ir.SkippedTableRecord{
		{
			StreamID:       "s1",
			Table:          "public.orders",
			SkipCount:      4,
			FirstPosition:  "tok-first",
			LastPosition:   "tok-last",
			FirstSkippedAt: time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC),
			LastSkippedAt:  time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC),
		},
	}
}

func TestRenderStatusText_SkippedTablesBlock(t *testing.T) {
	streams := []ir.StreamStatus{{
		StreamID:  "s1",
		Position:  ir.Position{Engine: "postgres", Token: "tok"},
		UpdatedAt: time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC),
	}}
	var buf bytes.Buffer
	now := time.Date(2026, 8, 12, 10, 0, 30, 0, time.UTC)
	if err := renderStatus(&buf, streams, nil, statusSkips(), statusRenderOpts{Format: "text"}, now); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"SKIPPED TABLES (target lacks them",
		"public.orders",
		"remedy:",
		"schema add-table",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("status text missing %q:\n%s", want, out)
		}
	}
}

func TestRenderStatusText_NoSkipsNoBlock(t *testing.T) {
	streams := []ir.StreamStatus{{
		StreamID:  "s1",
		UpdatedAt: time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC),
	}}
	var buf bytes.Buffer
	if err := renderStatus(&buf, streams, nil, nil, statusRenderOpts{Format: "text"}, time.Now()); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	if strings.Contains(buf.String(), "SKIPPED TABLES") {
		t.Fatalf("empty ledger rendered a skip block:\n%s", buf.String())
	}
}

func TestRenderStatusJSON_SkippedTables(t *testing.T) {
	var buf bytes.Buffer
	if err := renderStatus(&buf, nil, nil, statusSkips(), statusRenderOpts{Format: "json"}, time.Now()); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	var doc struct {
		Skipped []struct {
			StreamID      string `json:"stream_id"`
			Table         string `json:"table"`
			SkipCount     int64  `json:"skip_count"`
			FirstPosition string `json:"first_position"`
			LastPosition  string `json:"last_position"`
		} `json:"skipped_tables"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, buf.String())
	}
	if len(doc.Skipped) != 1 {
		t.Fatalf("skipped_tables rows = %d; want 1\n%s", len(doc.Skipped), buf.String())
	}
	got := doc.Skipped[0]
	if got.Table != "public.orders" || got.SkipCount != 4 || got.FirstPosition != "tok-first" || got.LastPosition != "tok-last" {
		t.Fatalf("skipped_tables row = %+v; want verbatim ledger values", got)
	}
}

func TestFilterSkippedTables(t *testing.T) {
	records := []ir.SkippedTableRecord{
		{StreamID: "s1", Table: "a"},
		{StreamID: "s2", Table: "b"},
	}
	got := filterSkippedTables(records, "s2")
	if len(got) != 1 || got[0].Table != "b" {
		t.Fatalf("filterSkippedTables = %+v; want the s2 row only", got)
	}
}

// skipListingApplier extends the stop tests' fakeApplier with the
// [ir.SkippedTableLister] surface.
type skipListingApplier struct {
	fakeApplier
	records []ir.SkippedTableRecord
}

func (f *skipListingApplier) ListSkippedTables(context.Context) ([]ir.SkippedTableRecord, error) {
	return f.records, nil
}

func TestPrintSkippedTablesSummary_StopSummaryNamesTablesAndRemedy(t *testing.T) {
	app := &skipListingApplier{records: append(statusSkips(), ir.SkippedTableRecord{
		StreamID: "other", Table: "public.elsewhere", SkipCount: 9,
	})}
	var buf bytes.Buffer
	printSkippedTablesSummary(context.Background(), app, "s1", &buf)
	out := buf.String()
	for _, want := range []string{"public.orders", "4 skipped event(s)", "remedy:", "schema add-table"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stop summary missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "elsewhere") {
		t.Fatalf("stop summary leaked another stream's ledger row:\n%s", out)
	}
}

func TestPrintSkippedTablesSummary_SilentWhenEmpty(t *testing.T) {
	var buf bytes.Buffer
	printSkippedTablesSummary(context.Background(), &skipListingApplier{}, "s1", &buf)
	if buf.Len() != 0 {
		t.Fatalf("empty ledger printed output:\n%s", buf.String())
	}
}
