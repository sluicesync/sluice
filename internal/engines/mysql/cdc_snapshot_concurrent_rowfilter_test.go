// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql"
	"testing"
)

// --- Bug 201: the concurrent cold-copy reader carries `--where` filters ---

// TestConcurrentBinlogRows_SetRowFiltersStampsEveryInnerReader pins the fix
// shape: SetRowFilters (ir.RowFilterSetter — the compile-time assertion lives
// next to the type) must stamp the map onto EVERY inner pinned-snapshot
// RowReader, because each concurrent table leg builds its SELECTs from its
// own reader's rowFilters. A single-reader stamp would filter one group and
// silently copy the others unfiltered — the exact class the pipeline's
// refusal gate exists to prevent.
func TestConcurrentBinlogRows_SetRowFiltersStampsEveryInnerReader(t *testing.T) {
	rows := newConcurrentBinlogRows(
		[]*sql.Conn{nil, nil, nil},
		[][]string{{"a"}, {"b"}, {"c"}},
		"db", nil, zeroDateInherit,
	)
	filters := map[string]string{"a": "v > 100", "c": "v > 50"}
	rows.SetRowFilters(filters)

	if len(rows.readers) != 3 {
		t.Fatalf("reader count = %d; want 3", len(rows.readers))
	}
	for i, rr := range rows.readers {
		if got := rr.rowFilters["a"]; got != "v > 100" {
			t.Errorf("inner reader %d rowFilters[a] = %q; want %q (unstamped leg would copy unfiltered)", i, got, "v > 100")
		}
		if got := rr.rowFilters["c"]; got != "v > 50" {
			t.Errorf("inner reader %d rowFilters[c] = %q; want %q", i, got, "v > 50")
		}
	}
}

// TestConcurrentBinlogRows_SwapConnectionsRestampsRowFilters pins the
// ADR-0111 recovery interaction: a re-snapshot builds FRESH inner readers,
// and dropping the filters there would silently resume filtered tables
// unfiltered mid-copy (the zeroDate re-stamp discipline, extended to
// rowFilters).
func TestConcurrentBinlogRows_SwapConnectionsRestampsRowFilters(t *testing.T) {
	rows := newConcurrentBinlogRows(
		[]*sql.Conn{nil, nil},
		[][]string{{"a"}, {"b"}},
		"db", nil, zeroDateInherit,
	)
	rows.SetRowFilters(map[string]string{"b": "id > 7"})

	rows.swapConnections([]*sql.Conn{nil, nil}, nil)

	if len(rows.readers) != 2 {
		t.Fatalf("post-swap reader count = %d; want 2", len(rows.readers))
	}
	for i, rr := range rows.readers {
		if got := rr.rowFilters["b"]; got != "id > 7" {
			t.Errorf("post-swap inner reader %d rowFilters[b] = %q; want %q (a recovery must never resume a filtered table unfiltered)", i, got, "id > 7")
		}
	}
}
