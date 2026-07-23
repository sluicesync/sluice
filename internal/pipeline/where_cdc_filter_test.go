// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// usersFilter builds a whereCDCFilter for `users` WHERE country='US' over a
// Postgres source schema, exercising the real buildWhereCDCFilter compile
// path (not a hand-built predicate).
func usersFilter(t *testing.T) *whereCDCFilter {
	t.Helper()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "country", Type: ir.Varchar{}}, // PG empty collation ⇒ case-sensitive
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}
	f, err := buildWhereCDCFilter(ir.ByteExactCollationResolver{}, map[string]string{"users": "country = 'US'"}, schema, false)
	if err != nil {
		t.Fatalf("buildWhereCDCFilter: %v", err)
	}
	return f
}

func inScope() ir.Row  { return ir.Row{"id": int64(1), "country": "US"} }
func outScope() ir.Row { return ir.Row{"id": int64(1), "country": "CA"} }

// TestRowMoveTable pins EVERY cell of the ADR-0173 row-move table, and
// asserts the move-IN → INSERT and move-OUT → DELETE cells NON-VACUOUSLY
// (the emitted op type + payload, not just "dropped/not").
func TestRowMoveTable(t *testing.T) {
	f := usersFilter(t)

	t.Run("INSERT in-scope -> INSERT", func(t *testing.T) {
		got := route1(t, f, ir.Insert{Table: "users", Row: inScope()})
		if _, ok := got.(ir.Insert); !ok {
			t.Fatalf("got %T; want ir.Insert", got)
		}
	})
	t.Run("INSERT out-of-scope -> drop", func(t *testing.T) {
		assertDropped(t, f, ir.Insert{Table: "users", Row: outScope()})
	})
	t.Run("DELETE in-scope -> DELETE", func(t *testing.T) {
		got := route1(t, f, ir.Delete{Table: "users", Before: inScope()})
		if _, ok := got.(ir.Delete); !ok {
			t.Fatalf("got %T; want ir.Delete", got)
		}
	})
	t.Run("DELETE out-of-scope -> drop", func(t *testing.T) {
		assertDropped(t, f, ir.Delete{Table: "users", Before: outScope()})
	})
	t.Run("UPDATE (yes,yes) -> UPDATE", func(t *testing.T) {
		got := route1(t, f, ir.Update{Table: "users", Before: inScope(), After: ir.Row{"id": int64(1), "country": "US"}})
		if _, ok := got.(ir.Update); !ok {
			t.Fatalf("got %T; want ir.Update", got)
		}
	})
	t.Run("UPDATE (no,no) -> drop", func(t *testing.T) {
		assertDropped(t, f, ir.Update{Table: "users", Before: outScope(), After: ir.Row{"id": int64(1), "country": "MX"}})
	})
	t.Run("UPDATE (no,yes) move-IN -> INSERT the after-image", func(t *testing.T) {
		after := ir.Row{"id": int64(7), "country": "US"}
		got := route1(t, f, ir.Update{Position: ir.Position{Token: "p"}, Table: "users", Before: outScope(), After: after})
		ins, ok := got.(ir.Insert)
		if !ok {
			t.Fatalf("move-IN got %T; want ir.Insert", got)
		}
		if ins.Row["id"] != int64(7) || ins.Row["country"] != "US" {
			t.Errorf("move-IN INSERT carries the wrong row: %v", ins.Row)
		}
		if ins.Position.Token != "p" {
			t.Errorf("move-IN INSERT lost the source position")
		}
	})
	t.Run("UPDATE (yes,no) move-OUT -> DELETE by key", func(t *testing.T) {
		before := ir.Row{"id": int64(9), "country": "US"}
		got := route1(t, f, ir.Update{Position: ir.Position{Token: "q"}, Table: "users", Before: before, After: outScope()})
		del, ok := got.(ir.Delete)
		if !ok {
			t.Fatalf("move-OUT got %T; want ir.Delete", got)
		}
		if del.Before["id"] != int64(9) {
			t.Errorf("move-OUT DELETE carries the wrong before-image: %v", del.Before)
		}
		// The before-image is re-narrowed to the PK for the applier's WHERE.
		if _, present := del.Before["country"]; present {
			t.Errorf("move-OUT DELETE before-image was not narrowed to the PK: %v", del.Before)
		}
		if del.Position.Token != "q" {
			t.Errorf("move-OUT DELETE lost the source position")
		}
	})
}

// TestRowMovePassthrough pins that a change on an UNFILTERED table, and
// every non-row event, flows through verbatim.
func TestRowMovePassthrough(t *testing.T) {
	f := usersFilter(t)
	// A different table has no predicate.
	got := route1(t, f, ir.Insert{Table: "orders", Row: outScope()})
	if _, ok := got.(ir.Insert); !ok {
		t.Fatalf("unfiltered table dropped: %T", got)
	}
	for _, c := range []ir.Change{
		ir.Truncate{Table: "users"},
		ir.SchemaSnapshot{Table: "users"},
		ir.TxBegin{},
		ir.TxCommit{},
	} {
		out, err := f.route(c)
		if err != nil {
			t.Fatalf("route(%T) err: %v", c, err)
		}
		if len(out) != 1 {
			t.Errorf("route(%T) forwarded %d changes; want 1 (verbatim)", c, len(out))
		}
	}
}

// TestRowMoveMissingBeforeImage pins the loud-failure belt: a filtered
// UPDATE/DELETE without a before-image is a coded refusal, never a silent
// mis-classification.
func TestRowMoveMissingBeforeImage(t *testing.T) {
	f := usersFilter(t)
	for _, c := range []ir.Change{
		ir.Update{Table: "users", Before: nil, After: inScope()},
		ir.Delete{Table: "users", Before: nil},
	} {
		_, err := f.route(c)
		if err == nil {
			t.Fatalf("route(%T without before-image): want a refusal", c)
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeWhereCDCBeforeImage {
			t.Errorf("route(%T): code = %v; want %s", c, err, sluicecode.CodeWhereCDCBeforeImage)
		}
	}
}

// TestRowMovePartialBeforeImage pins the ADR-0174 belt-and-suspenders floor:
// a filtered UPDATE/DELETE whose before-image is PRESENT but OMITS a column
// the predicate references (a self-hosted Vitess / MySQL on binlog_row_image
// != FULL that slipped past the reader's guards) is a coded refusal — never a
// silent move-OUT-as-drop leak. The evaluator would read the missing column as
// NULL/UNKNOWN and mis-classify a now-out-of-scope row as "never in scope",
// leaking it on the target.
func TestRowMovePartialBeforeImage(t *testing.T) {
	f := usersFilter(t) // predicate references `country`
	// before-image carries only the PK (`id`), not the filtered `country`.
	pkOnly := ir.Row{"id": int64(1)}
	for _, c := range []ir.Change{
		ir.Update{Table: "users", Before: pkOnly, After: inScope()},
		ir.Delete{Table: "users", Before: pkOnly},
	} {
		_, err := f.route(c)
		if err == nil {
			t.Fatalf("route(%T with PK-only before-image): want a refusal, got nil", c)
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeWhereCDCBeforeImage {
			t.Errorf("route(%T): code = %v; want %s", c, err, sluicecode.CodeWhereCDCBeforeImage)
		}
		if !strings.Contains(err.Error(), "country") {
			t.Errorf("route(%T): refusal must name the missing column; got %v", c, err)
		}
	}

	// A FULL before-image (carries `country`) is NOT refused — the guard only
	// fires on a genuinely partial image, not on every filtered change.
	if _, err := f.route(ir.Update{Table: "users", Before: inScope(), After: inScope()}); err != nil {
		t.Fatalf("full before-image must not be refused: %v", err)
	}
}

// TestRowMovePartialAfterImage pins the audit 2026-07-23 D0-1 belt: a
// filtered UPDATE whose AFTER-image omits a column the predicate references
// (pgoutput's unchanged-TOAST omission, had it slipped past the PG reader's
// backfill) is a coded SLUICE-E-WHERE-CDC-AFTER-IMAGE refusal — never a
// silent move-OUT. Pre-fix, route() evaluated the incomplete After as
// UNKNOWN→false and emitted a spurious DELETE for an in-scope row.
func TestRowMovePartialAfterImage(t *testing.T) {
	f := usersFilter(t) // predicate references `country`
	// Before is FULL and in-scope; After omits the filtered column — the
	// exact unchanged-TOAST decode shape.
	partialAfter := ir.Row{"id": int64(1)}
	got, err := f.route(ir.Update{Table: "users", Before: inScope(), After: partialAfter})
	if err == nil {
		if len(got) == 1 {
			if _, isDelete := got[0].(ir.Delete); isDelete {
				t.Fatal("D0-1 regression: partial after-image routed as move-OUT → spurious DELETE for an in-scope row")
			}
		}
		t.Fatal("route(UPDATE with partial after-image): want a coded refusal, got nil error")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeWhereCDCAfterImage {
		t.Errorf("code = %v; want %s", err, sluicecode.CodeWhereCDCAfterImage)
	}
	if !strings.Contains(err.Error(), "country") {
		t.Errorf("refusal must name the missing column; got %v", err)
	}

	// An After that carries every referenced column but omits an
	// UNREFERENCED one is NOT refused: the evaluator never reads it, and
	// absent-key still means "preserve the target's value" for the applier.
	okAfter := ir.Row{"id": int64(1), "country": "US"} // no `name`-style extras needed
	if _, err := f.route(ir.Update{Table: "users", Before: inScope(), After: okAfter}); err != nil {
		t.Fatalf("complete-for-the-predicate after-image must not be refused: %v", err)
	}
}

// TestInterceptWhereFilterChannel exercises the goroutine wrapper end to
// end, asserting the move-IN and move-OUT translations reach the OUT
// channel as the right op (the non-vacuous channel-level pin).
func TestInterceptWhereFilterChannel(t *testing.T) {
	f := usersFilter(t)
	in := make(chan ir.Change, 4)
	in <- ir.Insert{Table: "users", Row: outScope()}                      // dropped
	in <- ir.Update{Table: "users", Before: outScope(), After: inScope()} // move-IN
	in <- ir.Update{Table: "users", Before: inScope(), After: outScope()} // move-OUT
	in <- ir.Insert{Table: "users", Row: inScope()}                       // kept
	close(in)

	var errStore atomic.Pointer[error]
	out := interceptWhereFilter(context.Background(), in, f, &errStore)
	var got []ir.Change
	for c := range out {
		got = append(got, c)
	}
	if errStore.Load() != nil {
		t.Fatalf("unexpected error: %v", *errStore.Load())
	}
	if len(got) != 3 {
		t.Fatalf("got %d changes; want 3 (drop, INSERT, DELETE, INSERT)", len(got))
	}
	if _, ok := got[0].(ir.Insert); !ok {
		t.Errorf("move-IN did not surface as INSERT: %T", got[0])
	}
	if _, ok := got[1].(ir.Delete); !ok {
		t.Errorf("move-OUT did not surface as DELETE: %T", got[1])
	}
	if _, ok := got[2].(ir.Insert); !ok {
		t.Errorf("kept INSERT missing: %T", got[2])
	}
}

// TestInterceptWhereFilterNil pins the zero-cost passthrough: a nil filter
// returns the input channel verbatim (no goroutine, no transformation).
func TestInterceptWhereFilterNil(t *testing.T) {
	in := make(chan ir.Change)
	var errStore atomic.Pointer[error]
	var recv <-chan ir.Change = in
	if out := interceptWhereFilter(context.Background(), in, nil, &errStore); out != recv {
		t.Error("nil filter must return the input channel verbatim")
	}
}

// TestBuildWhereCDCFilterRefusals pins the sync-start refusals: a --where
// key naming no source table, and an unsupported predicate.
func TestBuildWhereCDCFilterRefusals(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "country", Type: ir.Varchar{}}},
	}}}

	t.Run("unknown table -> coded refusal", func(t *testing.T) {
		_, err := buildWhereCDCFilter(ir.ByteExactCollationResolver{}, map[string]string{"nope": "country = 'US'"}, schema, false)
		assertCoded(t, err, sluicecode.CodeWhereCDCUnsupportedPredicate)
		if !strings.Contains(err.Error(), "nope") {
			t.Errorf("refusal %q does not name the table", err.Error())
		}
	})
	t.Run("unsupported predicate -> coded refusal", func(t *testing.T) {
		_, err := buildWhereCDCFilter(ir.ByteExactCollationResolver{}, map[string]string{"users": "lower(country) = 'us'"}, schema, false)
		assertCoded(t, err, sluicecode.CodeWhereCDCUnsupportedPredicate)
	})
	t.Run("empty filters -> nil filter", func(t *testing.T) {
		f, err := buildWhereCDCFilter(ir.ByteExactCollationResolver{}, nil, schema, false)
		if err != nil || f != nil {
			t.Errorf("empty filters: got (%v, %v); want (nil, nil)", f, err)
		}
	})
}

func route1(t *testing.T, f *whereCDCFilter, c ir.Change) ir.Change {
	t.Helper()
	out, err := f.route(c)
	if err != nil {
		t.Fatalf("route(%T): unexpected error %v", c, err)
	}
	if len(out) != 1 {
		t.Fatalf("route(%T): got %d changes; want 1", c, len(out))
	}
	return out[0]
}

func assertDropped(t *testing.T, f *whereCDCFilter, c ir.Change) {
	t.Helper()
	out, err := f.route(c)
	if err != nil {
		t.Fatalf("route(%T): unexpected error %v", c, err)
	}
	if len(out) != 0 {
		t.Fatalf("route(%T): got %d changes; want 0 (dropped)", c, len(out))
	}
}

// TestServerSidePadSpaceDetection pins the A0 (audit 2026-07-19) detection that
// the VStream refusal rides on: a predicate on a PAD-SPACE-collation string
// column is recorded (the VStream server-side filter is NO-PAD and would
// diverge), while a NO-PAD (_0900_) column is not. Uses the REAL MySQL resolver
// so the collation → PAD_ATTRIBUTE mapping is the production one.
func TestServerSidePadSpaceDetection(t *testing.T) {
	r := mysql.Engine{}.CollationResolver()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "region", Type: ir.Varchar{Collation: "utf8mb4_general_ci"}}, // PAD SPACE
			{Name: "zone", Type: ir.Varchar{Collation: "utf8mb4_0900_ai_ci"}},   // NO PAD
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}

	t.Run("PAD-SPACE column recorded", func(t *testing.T) {
		f, err := buildWhereCDCFilter(r, map[string]string{"orders": "region = 'EU'"}, schema, false)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := f.serverSidePadSpaceColumns(); got["orders"] != "region" {
			t.Fatalf("serverSidePadSpaceColumns = %v; want orders→region (general_ci is PAD SPACE)", got)
		}
	})

	t.Run("NO-PAD column not recorded", func(t *testing.T) {
		f, err := buildWhereCDCFilter(r, map[string]string{"orders": "zone = 'EU'"}, schema, false)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := f.serverSidePadSpaceColumns(); len(got) != 0 {
			t.Fatalf("serverSidePadSpaceColumns = %v; want empty (0900 is NO PAD)", got)
		}
	})

	t.Run("nil filter is safe", func(t *testing.T) {
		var f *whereCDCFilter
		if got := f.serverSidePadSpaceColumns(); got != nil {
			t.Fatalf("nil.serverSidePadSpaceColumns = %v; want nil", got)
		}
	})
}

// TestServerSideTemporalCoercionDetection pins the F3 hazard detection
// (2026-07-23 value-fidelity review): a predicate whose temporal term
// engages the SOURCE engine's literal coercion — a sub-µs fraction Compile
// normalized, or a time-bearing literal on a DATE column (the MySQL-family
// promote comparison) — is recorded, so a VStream source routes the table
// through the A0 client-side fallback instead of pushing the RAW text to
// vtgate's evalengine, whose own coercion of those shapes is UNVERIFIED
// (ADR-0174 residuals). Engine-granular literals are NOT recorded (the
// pushed filter and the client agree wherever the backing mysqld evaluates).
func TestServerSideTemporalCoercionDetection(t *testing.T) {
	r := mysql.Engine{}.CollationResolver()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "d", Type: ir.Date{}},
			{Name: "dt", Type: ir.DateTime{Precision: 6}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}

	cases := []struct {
		name string
		pred string
		want string // recorded column, "" = not recorded
	}{
		{"sub-µs fraction (normalized) recorded", "dt = '2026-01-15 08:30:00.1234567'", "dt"},
		{"time-bearing literal on DATE recorded", "d < '2026-01-15 08:30:00'", "d"},
		{"time-bearing IN member on DATE recorded", "d IN ('2026-01-15', '2026-01-16 08:30')", "d"},
		{"pure-date literal not recorded", "d = '2026-01-15'", ""},
		{"µs-granular datetime literal not recorded", "dt = '2026-01-15 08:30:00.123456'", ""},
		{"non-temporal predicate not recorded", "id > 3", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := buildWhereCDCFilter(r, map[string]string{"orders": tc.pred}, schema, false)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			got := f.serverSideTemporalCoercionColumns()["orders"]
			if got != tc.want {
				t.Fatalf("serverSideTemporalCoercionColumns[orders] = %q; want %q", got, tc.want)
			}
			// The forced-table union and the COPY keep-predicate must engage
			// together: a recorded table gets a client-side filter, an
			// unrecorded (and non-pad) one does not.
			forced := f.clientForcedTables()
			if (tc.want != "") != forced["orders"] {
				t.Fatalf("clientForcedTables()[orders] = %v; want %v", forced["orders"], tc.want != "")
			}
			keep := f.clientCopyFilter()
			if (tc.want != "") != (keep != nil) {
				t.Fatalf("clientCopyFilter() nil-ness = %v; want engaged=%v", keep == nil, tc.want != "")
			}
		})
	}

	t.Run("temporal-forced table filters client-side, sibling stays server-kept", func(t *testing.T) {
		twoTables := &ir.Schema{Tables: []*ir.Table{
			schema.Tables[0],
			{
				Name: "widgets",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{}},
					{Name: "n", Type: ir.Integer{}},
				},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
		}}
		f, err := buildWhereCDCFilter(r, map[string]string{
			"orders":  "dt >= '2026-01-15 08:30:00.1234565'", // forced (MySQL rounds half-up → .123457)
			"widgets": "n > 3",                               // server-filtered
		}, twoTables, false)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		keep := f.clientCopyFilter()
		if keep == nil {
			t.Fatal("clientCopyFilter() = nil; want the fallback keep-predicate")
		}
		// The forced table evaluates the ENGINE-faithful (half-up) predicate:
		// .123457 is kept, .123456 is dropped.
		if !keep("orders", ir.Row{"dt": time.Date(2026, 1, 15, 8, 30, 0, 123457000, time.UTC)}) {
			t.Error("keep(orders, .123457) = false; want true (>= the half-up-rounded literal)")
		}
		if keep("orders", ir.Row{"dt": time.Date(2026, 1, 15, 8, 30, 0, 123456000, time.UTC)}) {
			t.Error("keep(orders, .123456) = true; want false (below the half-up-rounded literal)")
		}
		// The server-filtered sibling is kept unconditionally (no
		// double-filtering).
		if !keep("widgets", ir.Row{"n": int64(0)}) {
			t.Error("keep(widgets, out-of-scope row) = false; want true (server-filtered table is not re-filtered)")
		}
	})

	t.Run("nil filter is safe", func(t *testing.T) {
		var f *whereCDCFilter
		if got := f.serverSideTemporalCoercionColumns(); got != nil {
			t.Fatalf("nil.serverSideTemporalCoercionColumns = %v; want nil", got)
		}
		if got := f.clientForcedTables(); got != nil {
			t.Fatalf("nil.clientForcedTables = %v; want nil", got)
		}
	})
}

// TestClientCopyFloatSingleDetection pins the SL1 hazard detection (audit
// 2026-07-19): a table routed to the client-side COPY fallback (pad-forced)
// whose predicate references a single-precision FLOAT column in an ordering term
// is recorded, so preflight can refuse it on a VStream source — the cold-start
// COPY carrier display-rounds single-precision FLOAT and the keep would compare
// on the lossy value. DOUBLE (full-precision carrier) and non-pad (server-side,
// exact-value) tables are NOT recorded.
func TestClientCopyFloatSingleDetection(t *testing.T) {
	r := mysql.Engine{}.CollationResolver()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "region", Type: ir.Varchar{Collation: "utf8mb4_general_ci"}}, // PAD SPACE
			{Name: "zone", Type: ir.Varchar{Collation: "utf8mb4_0900_ai_ci"}},   // NO PAD
			{Name: "amount", Type: ir.Float{Precision: ir.FloatSingle}},         // lossy carrier
			{Name: "price", Type: ir.Float{Precision: ir.FloatDouble}},          // full precision
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}

	t.Run("pad-forced + single-precision FLOAT ordering recorded", func(t *testing.T) {
		f, err := buildWhereCDCFilter(r, map[string]string{"orders": "region = 'EU' AND amount > 0.1"}, schema, false)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := f.clientCopyFloatSingleColumns(); got["orders"] != "amount" {
			t.Fatalf("clientCopyFloatSingleColumns = %v; want orders→amount (pad-forced + FLOAT ordering = SL1 hazard)", got)
		}
	})

	t.Run("pad-forced + single-precision FLOAT IS NULL NOT recorded (display-round-insensitive)", func(t *testing.T) {
		// F-WR-1: an IS NULL presence test on a FLOAT can't be affected by the
		// carrier's display-rounding, so it must NOT be refused as a lossy
		// ordering term. Recorded via ValueComparedColumns, which skips IS NULL.
		f, err := buildWhereCDCFilter(r, map[string]string{"orders": "region = 'EU' AND amount IS NULL"}, schema, false)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := f.clientCopyFloatSingleColumns(); len(got) != 0 {
			t.Fatalf("clientCopyFloatSingleColumns = %v; want empty (FLOAT IS NULL is display-round-insensitive — wrong-refusal otherwise)", got)
		}
	})

	t.Run("pad-forced + DOUBLE ordering NOT recorded", func(t *testing.T) {
		f, err := buildWhereCDCFilter(r, map[string]string{"orders": "region = 'EU' AND price > 0.1"}, schema, false)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := f.clientCopyFloatSingleColumns(); len(got) != 0 {
			t.Fatalf("clientCopyFloatSingleColumns = %v; want empty (DOUBLE carrier is full-precision)", got)
		}
	})

	t.Run("non-pad + single-precision FLOAT NOT recorded (server-filtered, exact)", func(t *testing.T) {
		f, err := buildWhereCDCFilter(r, map[string]string{"orders": "zone = 'EU' AND amount > 0.1"}, schema, false)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := f.clientCopyFloatSingleColumns(); len(got) != 0 {
			t.Fatalf("clientCopyFloatSingleColumns = %v; want empty (NO-PAD table is server-filtered on the exact value)", got)
		}
	})

	t.Run("nil filter is safe", func(t *testing.T) {
		var f *whereCDCFilter
		if got := f.clientCopyFloatSingleColumns(); got != nil {
			t.Fatalf("nil.clientCopyFloatSingleColumns = %v; want nil", got)
		}
	})
}

// TestClientCopyFilter pins the A0 fallback's core (audit 2026-07-19 #66): the
// cold-start COPY keep-predicate filters a PAD-SPACE table CLIENT-side with the
// PAD-faithful comparator (keeping the trailing-space 'EU ' the NO-PAD VStream
// server filter would drop), while a non-PAD-SPACE (server-filtered) table is
// kept unconditionally (no double-filtering).
func TestClientCopyFilter(t *testing.T) {
	r := mysql.Engine{}.CollationResolver()
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "orders",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{}},
				{Name: "region", Type: ir.Varchar{Collation: "utf8mb4_general_ci"}}, // PAD SPACE
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		},
		{
			Name: "widgets",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{}},
				{Name: "zone", Type: ir.Varchar{Collation: "utf8mb4_0900_ai_ci"}}, // NO PAD
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}}
	f, err := buildWhereCDCFilter(r, map[string]string{
		"orders":  "region = 'EU'",
		"widgets": "zone = 'EU'",
	}, schema, false)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	keep := f.clientCopyFilter()
	if keep == nil {
		t.Fatal("clientCopyFilter is nil; want non-nil (orders is PAD SPACE)")
	}
	// PAD-SPACE table 'orders': filtered client-side, PAD-faithfully.
	if !keep("orders", ir.Row{"id": int64(1), "region": "EU"}) {
		t.Error("keep(orders, region='EU') = false; want true (in scope)")
	}
	if !keep("orders", ir.Row{"id": int64(2), "region": "EU "}) {
		t.Error("keep(orders, region='EU ') = false; want true (PAD-faithful — the row the NO-PAD server would drop)")
	}
	if keep("orders", ir.Row{"id": int64(3), "region": "US"}) {
		t.Error("keep(orders, region='US') = true; want false (out of scope)")
	}
	// NON-PAD table 'widgets': server-filtered → the copy filter keeps everything.
	if !keep("widgets", ir.Row{"id": int64(4), "zone": "US"}) {
		t.Error("keep(widgets, zone='US') = false; want true (server-filtered, not re-filtered)")
	}
}

// preflightRecordingSource is a fake filtered-sync source for
// preflightRowFilters unit pins: a recordingEngine (serving the schema)
// plus the FilteredCDCPreflighter and CollationResolverProvider
// surfaces. The preflighter records the exact table names it is handed
// and mimics the ENGINES' exact-name lookup semantics (PG resolves the
// name against pg_class.relname; an unfound name reads as
// not-REPLICA-IDENTITY-FULL) via the fail map.
type preflightRecordingSource struct {
	*recordingEngine
	preflighted [][]string
	fail        map[string]error
}

func (s *preflightRecordingSource) PreflightFilteredCDCBeforeImage(_ context.Context, _ string, tables []string) error {
	s.preflighted = append(s.preflighted, tables)
	for _, tb := range tables {
		if err := s.fail[tb]; err != nil {
			return err
		}
	}
	return nil
}

func (*preflightRecordingSource) CollationResolver() ir.CollationResolver {
	return ir.ByteExactCollationResolver{}
}

// TestPreflightRowFilters_BeforeImageProbeRunsOnCanonicalNames is the
// audit 2026-07-23 D0-10 pin (the carried 07-19 finding): the
// before-image probe must run AFTER ValidateRowFilterKeys, on the
// schema-cased table names. Pre-fix it probed the RAW `--where` keys —
// the engines look tables up by exact name, so a mere case-mismatch
// produced a spurious SLUICE-E-WHERE-CDC-BEFORE-IMAGE naming a REPLICA
// IDENTITY remedy that cannot fix a casing typo (loud-but-misleading).
func TestPreflightRowFilters_BeforeImageProbeRunsOnCanonicalNames(t *testing.T) {
	newSource := func() *preflightRecordingSource {
		eng := newRecordingEngine("fake-pg")
		eng.schema = &ir.Schema{Tables: []*ir.Table{{
			Name: "Orders", // schema-cased: mixed case, as PG can hold it
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}}}
		return &preflightRecordingSource{
			recordingEngine: eng,
			// Exact-name semantics: the raw upper-cased key does not resolve,
			// so a probe on it would refuse — exactly the pre-fix shape.
			fail: map[string]error{
				"ORDERS": missingBeforeImage("UPDATE", "", "ORDERS"),
			},
		}
	}

	t.Run("case-mismatched key canonicalizes, then probes — no spurious refusal", func(t *testing.T) {
		src := newSource()
		s := &Streamer{Source: src, SourceDSN: "dsn", RowFilters: map[string]string{"ORDERS": "id < 5"}}
		if err := s.preflightRowFilters(context.Background()); err != nil {
			t.Fatalf("preflightRowFilters = %v; want nil (RED pre-fix: the raw-key probe refused with the before-image code)", err)
		}
		if len(src.preflighted) != 1 || len(src.preflighted[0]) != 1 || src.preflighted[0][0] != "Orders" {
			t.Errorf("before-image probe received %v; want the canonical [Orders]", src.preflighted)
		}
	})

	t.Run("unknown key gets the unknown-table refusal, never the before-image one", func(t *testing.T) {
		src := newSource()
		src.fail["ghost"] = missingBeforeImage("UPDATE", "", "ghost")
		s := &Streamer{Source: src, SourceDSN: "dsn", RowFilters: map[string]string{"ghost": "id < 5"}}
		err := s.preflightRowFilters(context.Background())
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeWhereFilterUnknownTable {
			t.Fatalf("err = %v; want coded %s (the correct refusal for a bad key)", err, sluicecode.CodeWhereFilterUnknownTable)
		}
		if len(src.preflighted) != 0 {
			t.Errorf("the before-image probe must not run on an unvalidated key set; got %v", src.preflighted)
		}
	})

	t.Run("a genuine before-image misconfiguration still refuses loudly", func(t *testing.T) {
		src := newSource()
		src.fail["Orders"] = missingBeforeImage("UPDATE", "", "Orders")
		s := &Streamer{Source: src, SourceDSN: "dsn", RowFilters: map[string]string{"orders": "id < 5"}}
		err := s.preflightRowFilters(context.Background())
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeWhereCDCBeforeImage {
			t.Fatalf("err = %v; want coded %s (the probe's own refusal must still propagate)", err, sluicecode.CodeWhereCDCBeforeImage)
		}
	})
}
