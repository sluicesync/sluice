// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestWarnSkippedTable_HintIsOpAware pins the one skip surface that knows
// the op (audit 2026-09-15 A0915-MYSQL-HIGH-1): a skipped TRUNCATE must not
// be told "the source still holds every skipped row" — the source has
// already dropped them — and a skipped row change keeps the shared
// remedy. Every op the appliers pass is graded, not one representative.
func TestWarnSkippedTable_HintIsOpAware(t *testing.T) {
	capture := func(op string) string {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		WarnSkippedTable(context.Background(), "mysql", op, "app.t1")
		return buf.String()
	}
	for _, op := range []string{"insert", "update", "delete"} {
		got := capture(op)
		if !strings.Contains(got, ir.SkippedTableRemedy) {
			t.Errorf("op=%s: WARN hint is not the shared row-change remedy:\n%s", op, got)
		}
		if strings.Contains(got, "does NOT still hold") {
			t.Errorf("op=%s: WARN carries the TRUNCATE wording for a row change:\n%s", op, got)
		}
	}
	got := capture("truncate")
	if !strings.Contains(got, ir.SkippedTableTruncateRemedy) {
		t.Fatalf("op=truncate: WARN hint is not the TRUNCATE remedy:\n%s", got)
	}
	if strings.Contains(got, "the source still holds every") {
		t.Fatalf("op=truncate: WARN still claims the source holds the rows a TRUNCATE dropped:\n%s", got)
	}
	// The shared text, which the op-blind CLI surfaces print, must itself
	// stay true for a TRUNCATE: it may only claim the rows are on the
	// source for the row-change ops.
	if !strings.Contains(ir.SkippedTableRemedy, "TRUNCATE") {
		t.Fatalf("the shared remedy no longer scopes its 'source still holds' claim to row changes: %q", ir.SkippedTableRemedy)
	}
}
