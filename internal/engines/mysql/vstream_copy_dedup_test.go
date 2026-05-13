// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/query"

	"github.com/orware/sluice/internal/ir"
)

const priKeyFlag = uint32(query.MySqlFlag_PRI_KEY_FLAG)

// pkField returns a query.Field marked with PRI_KEY_FLAG. Test
// helper to keep the FIELD-event fixtures readable.
func pkField(name string) *query.Field {
	return &query.Field{Name: name, Flags: priKeyFlag}
}

// nonPkField returns a query.Field without PRI_KEY_FLAG.
func nonPkField(name string) *query.Field {
	return &query.Field{Name: name, Flags: 0}
}

// TestCopyDedupTracker_NilTrackerKeepsEverything is the boundary
// case — the snapshot stream constructor allocates a non-nil
// tracker today, but the API contract permits a nil receiver so
// engines that want to opt out of dedup can pass nil without a
// type assertion at every call site.
func TestCopyDedupTracker_NilTrackerKeepsEverything(t *testing.T) {
	var nilTracker *copyDedupTracker
	row := ir.Row{"id": int64(42)}
	if !nilTracker.shouldKeep("ks/-/users", row) {
		t.Errorf("nil tracker must keep every row")
	}
	// recordFields on nil receiver must not panic.
	nilTracker.recordFields("ks/-/users", []*query.Field{pkField("id")})
	if got := nilTracker.summary(); got != "" {
		t.Errorf("nil tracker summary = %q; want empty", got)
	}
}

// TestCopyDedupTracker_NoPKTableKeepsEverything covers the case
// where a FIELD event arrives but no field carries PRI_KEY_FLAG.
// Tables without a declared PK can't be deduped meaningfully, so
// the tracker must fall through and keep every row.
func TestCopyDedupTracker_NoPKTableKeepsEverything(t *testing.T) {
	tr := newCopyDedupTracker()
	tr.recordFields("ks/-/events", []*query.Field{
		nonPkField("payload"),
		nonPkField("ts"),
	})

	for i, payload := range []string{"a", "b", "a"} {
		row := ir.Row{"payload": payload, "ts": int64(1000 + i)}
		if !tr.shouldKeep("ks/-/events", row) {
			t.Errorf("row %d (payload=%q) should keep; got drop", i, payload)
		}
	}
}

// TestCopyDedupTracker_SingleIntPKMonotonic is the canonical happy
// path: int PK, rows arrive in PK-ascending order, no drops.
// Mirrors the well-behaved-source case the v0.42.0 retest did NOT
// reproduce — the dedup must be a no-op when Vitess plays nicely.
func TestCopyDedupTracker_SingleIntPKMonotonic(t *testing.T) {
	tr := newCopyDedupTracker()
	scope := "ks/-/users"
	tr.recordFields(scope, []*query.Field{pkField("id"), nonPkField("email")})

	for _, id := range []int64{1, 2, 3, 100, 101, 9999} {
		row := ir.Row{"id": id, "email": "x"}
		if !tr.shouldKeep(scope, row) {
			t.Errorf("id=%d should keep (monotonic forward); got drop", id)
		}
	}
	if got := tr.summary(); got != "" {
		t.Errorf("summary = %q; want empty (no drops on monotonic input)", got)
	}
}

// TestCopyDedupTracker_SingleIntPKBehindTheScanDropped is the
// GitHub issue #14 fix-validation. Mirrors the v0.42.0 retest's
// repro shape: Vitess emits a PK that's already past the COPY
// scan's max-seen marker, and the tracker drops it.
func TestCopyDedupTracker_SingleIntPKBehindTheScanDropped(t *testing.T) {
	tr := newCopyDedupTracker()
	scope := "sync-source/-/events"
	tr.recordFields(scope, []*query.Field{pkField("id"), nonPkField("uuid")})

	// Forward COPY emits 1..1100 (sampled for the test).
	for _, id := range []int64{1, 100, 500, 1000, 1100} {
		if !tr.shouldKeep(scope, ir.Row{"id": id, "uuid": "fwd"}) {
			t.Errorf("forward id=%d wrongly dropped", id)
		}
	}

	// Vitess re-emits the row at id=1179 to the destination.
	// Then Vitess emits a behind-the-scan event at id=545
	// (a row written during the scan, picked up by the binlog
	// tail). Tracker max is now 1179. The id=545 emission must
	// drop — it's behind the scan.
	if !tr.shouldKeep(scope, ir.Row{"id": int64(1179), "uuid": "fwd-1179"}) {
		t.Errorf("forward id=1179 wrongly dropped")
	}
	if tr.shouldKeep(scope, ir.Row{"id": int64(545), "uuid": "behind-545"}) {
		t.Errorf("behind-the-scan id=545 wrongly kept (issue #14 repro)")
	}
	// A re-emission of id=1179 (same PK) is also behind-the-scan.
	if tr.shouldKeep(scope, ir.Row{"id": int64(1179), "uuid": "behind-1179-reemit"}) {
		t.Errorf("re-emission of id=1179 wrongly kept (issue #14 repro)")
	}

	summary := tr.summary()
	if !strings.Contains(summary, scope+"=2") {
		t.Errorf("summary = %q; want one scope with 2 drops", summary)
	}
}

// TestCopyDedupTracker_PerScopeIndependence verifies that two
// different (keyspace, shard, table) scopes track independently —
// progress on one scope doesn't affect dedup decisions on another.
// This matters for multi-shard streams where each shard's COPY
// phase runs independently.
func TestCopyDedupTracker_PerScopeIndependence(t *testing.T) {
	tr := newCopyDedupTracker()
	tr.recordFields("ks/-80/events", []*query.Field{pkField("id")})
	tr.recordFields("ks/80-/events", []*query.Field{pkField("id")})

	// Shard -80 emits up to id=500.
	tr.shouldKeep("ks/-80/events", ir.Row{"id": int64(500)})
	// Shard 80- emits up to id=100 (started later).
	tr.shouldKeep("ks/80-/events", ir.Row{"id": int64(100)})

	// id=200 forward on shard 80- (above 80-'s 100) — keep.
	if !tr.shouldKeep("ks/80-/events", ir.Row{"id": int64(200)}) {
		t.Errorf("shard 80-: id=200 wrongly dropped (above shard's own max)")
	}
	// id=200 on shard -80 is BEHIND -80's max of 500 — drop.
	if tr.shouldKeep("ks/-80/events", ir.Row{"id": int64(200)}) {
		t.Errorf("shard -80: id=200 wrongly kept (below shard's max of 500)")
	}
}

// TestCopyDedupTracker_CompositePK exercises lexicographic tuple
// comparison. Composite PK (region, id): forward emissions advance
// either component; behind-the-scan on either component drops.
func TestCopyDedupTracker_CompositePK(t *testing.T) {
	tr := newCopyDedupTracker()
	scope := "ks/-/regional_events"
	tr.recordFields(scope, []*query.Field{
		pkField("region"),
		pkField("id"),
		nonPkField("payload"),
	})

	cases := []struct {
		name   string
		row    ir.Row
		expect bool // keep?
	}{
		{"first row (us, 1)", ir.Row{"region": "us", "id": int64(1), "payload": "x"}, true},
		{"forward in same region (us, 2)", ir.Row{"region": "us", "id": int64(2)}, true},
		{"forward to new region (eu, 1)", ir.Row{"region": "eu", "id": int64(1)}, false}, // "eu" < "us"
		{"forward to new region (zz, 1)", ir.Row{"region": "zz", "id": int64(1)}, true},
		{"behind in earlier region (us, 999)", ir.Row{"region": "us", "id": int64(999)}, false}, // "us" < "zz"
		{"forward in zz (zz, 5)", ir.Row{"region": "zz", "id": int64(5)}, true},
		{"behind in same region (zz, 3)", ir.Row{"region": "zz", "id": int64(3)}, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := tr.shouldKeep(scope, c.row)
			if got != c.expect {
				t.Errorf("shouldKeep(%v) = %v; want %v", c.row, got, c.expect)
			}
		})
	}
}

// TestComparePKCell exercises every supported value type's compare
// path plus the nil-handling and type-mismatch fall-through.
func TestComparePKCell(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		a, b any
		want int
	}{
		{"int64 less", int64(1), int64(2), -1},
		{"int64 greater", int64(5), int64(2), 1},
		{"int64 equal", int64(42), int64(42), 0},
		{"int64 negative", int64(-5), int64(-3), -1},
		{"uint64 less", uint64(10), uint64(20), -1},
		{"uint64 equal", uint64(42), uint64(42), 0},
		{"int64 vs uint64 positive", int64(5), uint64(10), -1},
		{"int64 vs uint64 large", int64(100), uint64(1 << 63), -1},
		{"float64", 1.5, 2.5, -1},
		{"string less", "a", "b", -1},
		{"string greater", "z", "a", 1},
		{"string equal", "foo", "foo", 0},
		{"bytes equal", []byte("hello"), []byte("hello"), 0},
		{"bytes less", []byte("abc"), []byte("abd"), -1},
		{"time less", now, now.Add(time.Second), -1},
		{"time equal", now, now, 0},
		{"bool false less than true", false, true, -1},
		{"bool true greater than false", true, false, 1},
		{"bool equal", true, true, 0},
		{"nil vs nil", nil, nil, 0},
		{"nil less", nil, int64(1), -1},
		{"nil greater", int64(1), nil, 1},
		{"type mismatch falls open", int64(1), "foo", 0},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := comparePKCell(c.a, c.b)
			// normalize to -1/0/1 for cmp.Compare's tri-state
			if got < 0 {
				got = -1
			} else if got > 0 {
				got = 1
			}
			if got != c.want {
				t.Errorf("comparePKCell(%v, %v) = %d; want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

// TestComparePKTuple_DifferentLengths covers the defensive branch
// where two PK tuples have different lengths. Should not happen in
// practice (PK column count is stable per scope), but the function
// must return a deterministic value rather than panic.
func TestComparePKTuple_DifferentLengths(t *testing.T) {
	a := []any{int64(1), int64(2)}
	b := []any{int64(1)}
	if got := comparePKTuple(a, b); got <= 0 {
		t.Errorf("comparePKTuple(len=2, len=1) = %d; want > 0", got)
	}
	if got := comparePKTuple(b, a); got >= 0 {
		t.Errorf("comparePKTuple(len=1, len=2) = %d; want < 0", got)
	}
}

// TestExtractPKTuple_MissingColumn covers the case where the row
// map is missing a column the PK metadata expects. extractPKTuple
// returns nil; the tracker treats this as "can't dedup" and keeps
// the row, so the operator sees the symptom rather than a silent
// drop.
func TestExtractPKTuple_MissingColumn(t *testing.T) {
	row := ir.Row{"id": int64(1)}
	got := extractPKTuple(row, []string{"id", "missing"})
	if got != nil {
		t.Errorf("extractPKTuple with missing column = %v; want nil", got)
	}
}

// TestCopyDedupTracker_RecordFieldsIdempotent confirms a re-emitted
// FIELD event with the same columns doesn't reset progress. Vitess
// can re-emit FIELD events on DDL or stream restart; the dedup must
// preserve max-seen state across them.
func TestCopyDedupTracker_RecordFieldsIdempotent(t *testing.T) {
	tr := newCopyDedupTracker()
	scope := "ks/-/users"
	tr.recordFields(scope, []*query.Field{pkField("id")})

	tr.shouldKeep(scope, ir.Row{"id": int64(100)})

	// FIELD re-emit with the same column set.
	tr.recordFields(scope, []*query.Field{pkField("id")})

	// Behind-the-scan emission should STILL drop (max=100 preserved).
	if tr.shouldKeep(scope, ir.Row{"id": int64(50)}) {
		t.Errorf("FIELD re-emit wrongly reset max-seen; id=50 was kept")
	}
}

// TestCopyDedupTracker_SummaryFormat pins the observability output
// shape so a future format change doesn't silently break operator
// log grep / dashboards.
func TestCopyDedupTracker_SummaryFormat(t *testing.T) {
	tr := newCopyDedupTracker()
	tr.recordFields("ks/-/users", []*query.Field{pkField("id")})
	tr.recordFields("ks/-/events", []*query.Field{pkField("id")})

	tr.shouldKeep("ks/-/users", ir.Row{"id": int64(100)})
	if !tr.shouldKeep("ks/-/events", ir.Row{"id": int64(200)}) {
		t.Fatal("forward emission wrongly dropped")
	}
	// Drop a behind-the-scan event on users.
	tr.shouldKeep("ks/-/users", ir.Row{"id": int64(50)})
	tr.shouldKeep("ks/-/users", ir.Row{"id": int64(75)})

	got := tr.summary()
	if !strings.Contains(got, "ks/-/users=2") {
		t.Errorf("summary = %q; want 'ks/-/users=2' substring", got)
	}
	if strings.Contains(got, "ks/-/events") {
		t.Errorf("summary = %q; should NOT mention zero-drop scope 'ks/-/events'", got)
	}
}
