// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pin for audit 2026-07-26 SL-11 — `--backfill-added-column` must respect
// `--where`.
//
// The backfill intercept is wired DOWNSTREAM of the row-filter intercept, so
// its synthetic Updates never pass through route(). The backfill therefore
// paginated the ENTIRE table and emitted one Update per source row, in-scope
// or not. Nothing leaked (an Update is not an upsert, so out-of-scope ones
// matched zero target rows and were swallowed at DEBUG) — but the filter's
// whole point on a subset sync, bounded source read volume, was silently
// defeated, and the interaction was undocumented.
//
// The fix pushes the predicate into the backfill READER rather than reordering
// the intercepts, because the synthetic Update carries a PK-only Before and
// would trip the image-completeness belt on every row. This pins that the
// reader actually receives the filters.
package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// recordingRowFilterReader records the filters handed to it.
type recordingRowFilterReader struct {
	got map[string]string
}

func (r *recordingRowFilterReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return nil, nil
}
func (r *recordingRowFilterReader) SetRowFilters(f map[string]string) { r.got = f }

func TestBackfillReaderReceivesRowFilters(t *testing.T) {
	rr := &recordingRowFilterReader{}
	filters := map[string]string{"orders": "region = 'EU'"}

	if err := migcore.ApplyRowFilters(rr, filters, "mysql"); err != nil {
		t.Fatalf("ApplyRowFilters: %v", err)
	}
	if len(rr.got) != 1 || rr.got["orders"] != "region = 'EU'" {
		t.Errorf("backfill reader received %v; want the stream's --where set. Without it the backfill "+
			"paginates the ENTIRE table on a filtered sync, defeating the bounded-read-volume property the "+
			"filter exists for (audit SL-11).", rr.got)
	}
}

// TestBackfillReaderWithoutFiltersIsUnchanged: the unfiltered path must be
// byte-identical to before — ApplyRowFilters is a no-op with no filters, so a
// reader that cannot filter is still usable.
func TestBackfillReaderWithoutFiltersIsUnchanged(t *testing.T) {
	rr := &recordingRowFilterReader{}
	if err := migcore.ApplyRowFilters(rr, nil, "mysql"); err != nil {
		t.Fatalf("ApplyRowFilters with no filters returned an error: %v", err)
	}
	if rr.got != nil {
		t.Errorf("SetRowFilters was called with %v on an unfiltered stream", rr.got)
	}
}
