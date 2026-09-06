// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestApplyTableFilter_EmitsTheUnmatchedMarker grades the WIRING, which the
// Bug 272 helper pins next door deliberately do not: they prove
// UnmatchedPatterns computes the right answer, and prove nothing about
// whether an operator is ever told it.
//
// That gap is the shape this repo keeps paying for — a pin that grades the
// function rather than the path — and it matters here because the whole
// finding is that the run exits 0 either way. A detection nobody surfaces is
// indistinguishable from no detection.
//
// TABLE-FILTER-PATTERN-UNMATCHED is asserted by name because it is the
// operator's grep handle (the POSITION-MODE / STALE-CAPTURE-FUNCTION /
// CHANGE-LOG-PAGE-UNORDERED convention). A marker that silently changes
// spelling breaks every saved search and alert built on it, so the literal
// belongs in a test rather than only at the emit site.
func TestApplyTableFilter_EmitsTheUnmatchedMarker(t *testing.T) {
	newSchema := func() *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{
			{Name: "users"}, {Name: "orders"}, {Name: "pii"},
		}}
	}

	capture := func(t *testing.T, patterns []string, include bool) string {
		t.Helper()
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)

		var f TableFilter
		var err error
		if include {
			f, err = NewTableFilter(patterns, nil)
		} else {
			f, err = NewTableFilter(nil, patterns)
		}
		if err != nil {
			t.Fatalf("NewTableFilter(%v): %v", patterns, err)
		}
		if err := ApplyTableFilter(context.Background(), newSchema(), f); err != nil {
			t.Fatalf("ApplyTableFilter: %v", err)
		}
		return buf.String()
	}

	t.Run("a schema-qualified exclude pattern warns with the marker and the bare-name remedy", func(t *testing.T) {
		got := capture(t, []string{"public.pii"}, false)
		for _, want := range []string{
			"TABLE-FILTER-PATTERN-UNMATCHED",
			"--exclude-table",
			"public.pii",
			"WILL be copied",
			// The remedy has to name the fix, not just the problem: this is
			// the branch an operator reaches after copying a qualified name
			// out of sluice's own diagnostics.
			"BARE table name",
			// Quoted by the emitter and escaped again by the TextHandler, so
			// the literal here carries the escaping the operator actually sees.
			`write \"pii\"`,
		} {
			if !strings.Contains(got, want) {
				t.Errorf("warn output missing %q.\ngot: %s", want, got)
			}
		}
	})

	t.Run("include mode names its own flag and effect", func(t *testing.T) {
		got := capture(t, []string{"users", "public.orders"}, true)
		if !strings.Contains(got, "TABLE-FILTER-PATTERN-UNMATCHED") || !strings.Contains(got, "--include-table") {
			t.Errorf("include-mode warn missing the marker or the flag.\ngot: %s", got)
		}
		// The matched pattern must NOT be reported, or the warning is noise
		// and an operator learns to ignore it.
		if strings.Contains(got, `pattern=users`) {
			t.Errorf("a pattern that DID match was reported as unmatched.\ngot: %s", got)
		}
	})

	t.Run("a fully-matching filter is silent", func(t *testing.T) {
		// The no-false-fire floor. Without it every assertion above would
		// pass against a door that warned unconditionally.
		if got := capture(t, []string{"pii"}, false); strings.Contains(got, "TABLE-FILTER-PATTERN-UNMATCHED") {
			t.Errorf("a filter whose every pattern matched still warned — a warning that always fires "+
				"is one operators learn to ignore.\ngot: %s", got)
		}
	})
}
