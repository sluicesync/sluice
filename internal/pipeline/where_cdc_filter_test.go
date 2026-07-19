// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

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
