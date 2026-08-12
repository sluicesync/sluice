// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// These pin the audit-C-11 health surface: a nonzero durable
// unknown-target-table skip count TRIPS `sluice sync health` (exit 1),
// naming the tables and the remedy. applySkippedTablesHealth is the
// pure fold the Run path calls — the mutation target ("make health
// ignore the counter" must turn these red).

func skippedRecord(stream, table string, count int64) ir.SkippedTableRecord {
	return ir.SkippedTableRecord{
		StreamID:      stream,
		Table:         table,
		SkipCount:     count,
		FirstPosition: "pos-first",
		LastPosition:  "pos-last",
		LastSkippedAt: time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC),
	}
}

func TestApplySkippedTablesHealth_NonzeroCountTrips(t *testing.T) {
	r := HealthResult{StreamID: "s1", Found: true}
	applySkippedTablesHealth(&r, []ir.SkippedTableRecord{
		skippedRecord("s1", "public.orders", 3),
		skippedRecord("s1", "public.invoices", 2),
	}, "s1")

	if !r.SkippedTablesTripped {
		t.Fatal("SkippedTablesTripped = false; want true for a nonzero skip count")
	}
	if r.SkippedEventsTotal != 5 {
		t.Fatalf("SkippedEventsTotal = %d; want 5", r.SkippedEventsTotal)
	}
	if len(r.SkippedTables) != 2 {
		t.Fatalf("SkippedTables rows = %d; want 2", len(r.SkippedTables))
	}
	if r.SkippedTables[0].FirstPosition != "pos-first" || r.SkippedTables[0].LastPosition != "pos-last" {
		t.Fatalf("position tokens not carried verbatim: %+v", r.SkippedTables[0])
	}
}

func TestApplySkippedTablesHealth_OtherStreamsAndZeroCountsDoNotTrip(t *testing.T) {
	r := HealthResult{StreamID: "s1", Found: true}
	applySkippedTablesHealth(&r, []ir.SkippedTableRecord{
		skippedRecord("other-stream", "public.orders", 7),
		skippedRecord("s1", "public.ghosts", 0),
	}, "s1")

	if r.SkippedTablesTripped {
		t.Fatal("SkippedTablesTripped = true; want false (only other streams / zero counts present)")
	}
	if r.SkippedEventsTotal != 0 || len(r.SkippedTables) != 0 {
		t.Fatalf("result carries foreign/zero rows: total=%d rows=%d", r.SkippedEventsTotal, len(r.SkippedTables))
	}
}

func TestSkippedTablesError_NamesTablesAndRemedy(t *testing.T) {
	r := HealthResult{StreamID: "s1", Found: true}
	applySkippedTablesHealth(&r, []ir.SkippedTableRecord{
		skippedRecord("s1", "public.orders", 3),
	}, "s1")
	err := skippedTablesError{streamID: "s1", result: r}

	if err.ExitCode() != 1 {
		t.Fatalf("ExitCode = %d; want 1", err.ExitCode())
	}
	msg := err.Error()
	for _, want := range []string{"public.orders", "3", "schema add-table", "table filter"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error text missing %q:\n%s", want, msg)
		}
	}
}

func TestRenderHealthText_SkippedTablesBlock(t *testing.T) {
	r := HealthResult{StreamID: "s1", Found: true, UpdatedAt: "2026-08-12T10:00:00Z"}
	applySkippedTablesHealth(&r, []ir.SkippedTableRecord{
		skippedRecord("s1", "public.orders", 3),
	}, "s1")

	var buf bytes.Buffer
	if err := renderHealthText(&buf, r); err != nil {
		t.Fatalf("renderHealthText: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"state: SKIPPING (3 event(s) skipped for 1 table(s) the target lacks)",
		"skipped_table: public.orders count=3",
		"skipped_tables_remedy:",
		"schema add-table",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("health text missing %q:\n%s", want, out)
		}
	}
}
