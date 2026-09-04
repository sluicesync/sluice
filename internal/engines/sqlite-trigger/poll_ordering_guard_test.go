// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fixedPageExecutor serves one page verbatim, in the order given, so the test
// controls exactly what the pump is handed. The embedded interface is nil on
// purpose: any method this test does not expect to be called panics rather
// than returning a plausible zero value.
type fixedPageExecutor struct {
	executor
	ids   []int64
	table string
}

func (f *fixedPageExecutor) pollChangeLog(context.Context, int64, int) ([]rawChangeRow, error) {
	out := make([]rawChangeRow, 0, len(f.ids))
	for _, id := range f.ids {
		out = append(out, rawChangeRow{
			id:  id,
			op:  "I",
			tbl: f.table,
		})
	}
	return out, nil
}

// The pump's watermark is the page's maximum id, so the ordering of a page is
// a correctness property of the whole stream, not of the query that produced
// it.
//
// WHY THIS EXISTS. Bug 266 was an SQL alias that made the D1 poll sort
// lexicographically. The SQL is fixed and pinned. But the fix and its pin
// both grade the QUERY, and the thing that turned a wrong query into
// permanent loss was the pump: it advances `lastSeen` to the highest id in
// whatever page it received, so a page that is not ascending moves the
// watermark past rows that page never returned. A keyset resume then starts
// above them and they can never be read again — captured, undelivered,
// unreachable, at exit 0.
//
// Nothing between the query and the watermark asked whether the page was
// ordered, which is why an ordering defect could survive as silent loss
// rather than surfacing. The guard grades what the pump was HANDED, so a
// future transport, a re-broken alias, or a server that changes its ordering
// becomes a loud refusal instead of missing rows.
//
// The distinction matters for what this can catch: the SQL pin cannot see a
// transport that reorders after the query, and this cannot see a query that
// is wrong but consistently ascending. They are complementary, and the second
// was missing.
func TestPoll_RefusesAnOutOfOrderPage(t *testing.T) {
	table := "t"
	for _, tc := range []struct {
		name       string
		ids        []int64
		wantRefuse bool
		why        string
	}{
		{
			name: "an ascending page is delivered",
			ids:  []int64{1, 2, 3, 9, 10, 11, 12},
			why:  "the ordinary case, including ids that span the digit boundary",
		},
		{
			// Exactly the shape Bug 266 produced.
			name:       "a lexicographically ordered page refuses",
			ids:        []int64{1, 10, 11, 12, 2, 3, 9},
			wantRefuse: true,
			why:        "the watermark would jump to 12 and 2, 3 and 9 could never be read again",
		},
		{
			name:       "a descending page refuses",
			ids:        []int64{12, 11, 3, 2, 1},
			wantRefuse: true,
			why:        "the first row alone would advance the watermark past every later one",
		},
		{
			// The subtle one: ascending except for a single late row. A
			// spot-check of first-and-last would miss it.
			name:       "one row out of place near the end refuses",
			ids:        []int64{1, 2, 3, 9, 12, 10},
			wantRefuse: true,
			why:        "10 arrives after 12, so 10 and 11 fall below the advanced watermark",
		},
		{
			name: "a single-row page is trivially ordered",
			ids:  []int64{7},
			why:  "nothing to compare against",
		},
		{
			name: "an empty page is not an ordering failure",
			ids:  nil,
			why:  "the steady-state nothing-new poll",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &CDCReader{
				exec:      &fixedPageExecutor{ids: tc.ids, table: table},
				colTypes:  map[string]map[string]ir.Type{table: {}},
				batchSize: 100,
			}
			events, newLast, err := r.poll(context.Background(), 0)

			if tc.wantRefuse {
				if err == nil {
					t.Fatalf("poll accepted a page ordered %v and advanced the watermark to %d — %s",
						tc.ids, newLast, tc.why)
				}
				if !strings.Contains(err.Error(), "ascending id order") {
					t.Fatalf("refused, but not as an ordering failure: %v", err)
				}
				if newLast != 0 {
					t.Fatalf("watermark advanced to %d on a refused page; it must stay put so the next "+
						"poll re-reads the same range", newLast)
				}
				return
			}
			if err != nil {
				t.Fatalf("poll refused a correctly ordered page %v: %v", tc.ids, err)
			}
			if len(events) != len(tc.ids) {
				t.Fatalf("delivered %d events for %d rows", len(events), len(tc.ids))
			}
			var want int64
			for _, id := range tc.ids {
				if id > want {
					want = id
				}
			}
			if newLast != want {
				t.Fatalf("watermark = %d, want %d", newLast, want)
			}
		})
	}
}
