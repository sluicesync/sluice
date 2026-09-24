// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// GC-36 (1) — the Shape A boundary router forwarded ADD COLUMN from the
// raw CDC projection, which on a Postgres or VStream source carries no
// DEFAULT, so every row the consolidated target already held landed NULL
// where the sources hold the default. These pins grade the router's own
// DEFAULT steps (the §2a door, the carry, the lease checksum) against a
// stub source catalog; the real-server pin is the ShapeA lane of the
// pre-existing-row gate
// (TestStreamer_AddColumnForward_PreexistingRowDefaults_ShapeAPostgresToPostgres).

// defaultsFixturePre / defaultsFixturePost are one boundary: a source table
// public.w gains `a` (no in-band DEFAULT — the pgoutput / VStream shape)
// and `b` (an in-band DEFAULT — the MySQL binlog shape).
func defaultsFixturePre() *ir.Table {
	return &ir.Table{Schema: "public", Name: "w", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
}

func defaultsFixturePost() *ir.Table {
	return &ir.Table{Schema: "public", Name: "w", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "a", Type: ir.Integer{Width: 32}},
		{Name: "b", Type: ir.Integer{Width: 32}, Default: ir.DefaultLiteral{Value: "7"}},
	}}
}

const defaultsFixtureDDL = "ir-schema:w:fixture"

// literalDefaults returns readers whose catalog declares `a DEFAULT value`
// and records every column the carrier was asked for.
func literalDefaults(value string, asked *[]string) sourceDefaultReaders {
	read := func(_ context.Context, schema, table, _ string) (ir.DefaultValue, error) {
		if schema != "public" || table != "w" {
			return nil, errors.New("read against " + schema + "." + table + ", not the source table public.w")
		}
		return ir.DefaultLiteral{Value: value}, nil
	}
	carrier := func(ctx context.Context, schema, table, column string) (ir.DefaultValue, error) {
		if asked != nil {
			*asked = append(*asked, column)
		}
		return read(ctx, schema, table, column)
	}
	return sourceDefaultReaders{prober: read, carrier: carrier}
}

func newDefaultsTestRouter(t *testing.T, store *fakeLeaseStore, streamID string, prober ShardConsolidationProber, applier ir.ShapeDeltaApplier, clock *mockClock, defaults sourceDefaultReaders) *BoundaryRouter {
	t.Helper()
	cfg := LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}
	router, err := NewBoundaryRouter(newTestLeaseManager(t, store, streamID, cfg, clock), applier, prober, "postgres", "postgres", defaults)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}
	router.observePollInterval = 5 * time.Millisecond
	router.observeTimeout = 200 * time.Millisecond
	return router
}

// TestRouteBoundary_AddColumn_CarriesTheSourceDefault: every arm that
// reaches the applier (the held-lease apply, the takeover re-apply) and the
// takeover probe are handed the added column WITH the source's DEFAULT; an
// in-band default is kept and not re-read; the intercept's cached post is
// not mutated.
func TestRouteBoundary_AddColumn_CarriesTheSourceDefault(t *testing.T) {
	t.Parallel()
	for _, takeover := range []bool{false, true} {
		clock := newMockClock(time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
		store := newFakeLeaseStore(clock.Now)
		if takeover {
			mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}, clock)
			lease, err := mgrA.Acquire(context.Background(), "public.w", defaultsFixtureDDL)
			if err != nil {
				t.Fatalf("mgrA.Acquire: %v", err)
			}
			mgrA.Release(context.Background(), lease)
			clock.Advance(31 * time.Second)
		}
		applier := &capturingShapeApplier{}
		prober := &capturingProber{}
		var asked []string
		router := newDefaultsTestRouter(t, store, "stream-b", prober, applier, clock, literalDefaults("42", &asked))

		post := defaultsFixturePost()
		if err := router.RouteBoundary(context.Background(), "public.w", defaultsFixturePre(), post, defaultsFixtureDDL, 2, ir.Position{}); err != nil {
			t.Fatalf("takeover=%v: RouteBoundary: %v", takeover, err)
		}

		consumers := applier.seen
		if takeover {
			consumers = append(append([]capturedTable(nil), prober.seen...), applier.seen...)
			if len(prober.seen) != 1 {
				t.Errorf("takeover: probe calls = %d; want 1", len(prober.seen))
			}
		}
		if len(applier.seen) != 1 {
			t.Fatalf("takeover=%v: applier calls = %d; want 1", takeover, len(applier.seen))
		}
		for _, c := range consumers {
			if got := c.col.Default; got != (ir.DefaultLiteral{Value: "42"}) {
				t.Errorf("takeover=%v: %s handed column %q with DEFAULT %#v; want the source's 42", takeover, c.method, c.col.Name, got)
			}
			var inBand *ir.Column
			for _, col := range c.table.Columns {
				if col.Name == "b" {
					inBand = col
				}
			}
			if inBand == nil || inBand.Default != (ir.DefaultLiteral{Value: "7"}) {
				t.Errorf("takeover=%v: %s: the in-band DEFAULT of b was not kept: %#v", takeover, c.method, inBand)
			}
		}
		if len(asked) != 1 || asked[0] != "a" {
			t.Errorf("takeover=%v: carrier asked for %v; want only a (b carried its DEFAULT in-band)", takeover, asked)
		}
		if post.Columns[1].Default != nil {
			t.Errorf("takeover=%v: RouteBoundary mutated the caller's post (the intercept's cache entry)", takeover)
		}
	}
}

// TestRouteBoundary_AddColumn_DefaultReadFailureRefusesBeforeTheLease: a
// catalog read that fails must stop the forward — adding the column bare is
// the silent outcome — and it must do so before any lease exists, with the
// fleet-wide drained-model hint.
func TestRouteBoundary_AddColumn_DefaultReadFailureRefusesBeforeTheLease(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	applier := &capturingShapeApplier{}
	failing := func(context.Context, string, string, string) (ir.DefaultValue, error) {
		return nil, errors.New("catalog read failed")
	}
	defaults := literalDefaults("42", nil)
	defaults.carrier = failing
	router := newDefaultsTestRouter(t, store, "stream-a", &fakeProber{}, applier, clock, defaults)

	err := router.RouteBoundary(context.Background(), "public.w", defaultsFixturePre(), defaultsFixturePost(), defaultsFixtureDDL, 2, ir.Position{})
	if err == nil || !strings.Contains(err.Error(), "catalog read failed") {
		t.Fatalf("RouteBoundary = %v; want a refusal carrying the read error", err)
	}
	if !strings.Contains(err.Error(), "on every shard") {
		t.Errorf("refusal does not carry the Shape A (fleet-wide) recovery hint: %v", err)
	}
	if len(applier.seen) != 0 {
		t.Errorf("applier was called %d times after a failed DEFAULT read", len(applier.seen))
	}
	if _, ok := store.snapshot("public.w"); ok {
		t.Error("a lease row exists; the refusal must come before the lease is taken")
	}
}

// TestRouteBoundary_AddColumn_VolatileDefaultRefuses: the ADR-0058 §2a door
// on the Shape A path. A volatile DEFAULT would be evaluated once, by the
// target, at ALTER time.
func TestRouteBoundary_AddColumn_VolatileDefaultRefuses(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	applier := &capturingShapeApplier{}
	defaults := literalDefaults("42", nil)
	defaults.prober = func(context.Context, string, string, string) (ir.DefaultValue, error) {
		return ir.DefaultExpression{Expr: "now()"}, nil
	}
	router := newDefaultsTestRouter(t, store, "stream-a", &fakeProber{}, applier, clock, defaults)

	err := router.RouteBoundary(context.Background(), "public.w", defaultsFixturePre(), defaultsFixturePost(), defaultsFixtureDDL, 2, ir.Position{})
	if err == nil || !strings.Contains(err.Error(), "ADR-0058 §2a") {
		t.Fatalf("RouteBoundary = %v; want the §2a computed-DEFAULT refusal", err)
	}
	if len(applier.seen) != 0 {
		t.Errorf("applier was called %d times for a volatile DEFAULT", len(applier.seen))
	}
}

// TestRouteBoundary_AddColumn_PeerWithADifferentDefaultRefuses: the carried
// DEFAULT is part of the lease checksum. A peer shard whose source declares
// the same DEFAULT observes the holder's apply; one whose source declares a
// different DEFAULT refuses instead of accepting the holder's fill of its
// pre-existing rows.
func TestRouteBoundary_AddColumn_PeerWithADifferentDefaultRefuses(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	route := func(streamID, def string, applier *capturingShapeApplier) error {
		r := newDefaultsTestRouter(t, store, streamID, &fakeProber{}, applier, clock, literalDefaults(def, nil))
		return r.RouteBoundary(context.Background(), "public.w", defaultsFixturePre(), defaultsFixturePost(), defaultsFixtureDDL, 2, ir.Position{})
	}

	holder := &capturingShapeApplier{}
	if err := route("stream-a", "42", holder); err != nil {
		t.Fatalf("holder: %v", err)
	}
	same := &capturingShapeApplier{}
	if err := route("stream-b", "42", same); err != nil {
		t.Errorf("peer with the same DEFAULT: %v; want the peer-applied checksum match", err)
	}
	differs := &capturingShapeApplier{}
	err := route("stream-c", "43", differs)
	if !errors.Is(err, ErrLeaseChecksumMismatch) {
		t.Errorf("peer with a different DEFAULT: %v; want ErrLeaseChecksumMismatch", err)
	}
	if len(same.seen)+len(differs.seen) != 0 {
		t.Errorf("a peer applied the DDL (%d + %d calls); only the holder applies", len(same.seen), len(differs.seen))
	}
}

// TestRouteBoundary_NothingCarriedKeepsTheLeaseText: a boundary that
// carried no real DEFAULT — the column has none, or it arrived in-band —
// records exactly the ddl_text it was handed, so those checksums match
// across binaries from before the carry.
func TestRouteBoundary_NothingCarriedKeepsTheLeaseText(t *testing.T) {
	t.Parallel()
	none := func(context.Context, string, string, string) (ir.DefaultValue, error) {
		return ir.DefaultNone{}, nil
	}
	for name, post := range map[string]*ir.Table{
		"no default": defaultsFixturePost(),
		"in-band": {Schema: "public", Name: "w", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Integer{Width: 32}, Default: ir.DefaultLiteral{Value: "7"}},
		}},
	} {
		clock := newMockClock(time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
		store := newFakeLeaseStore(clock.Now)
		router := newDefaultsTestRouter(t, store, "stream-a", &fakeProber{}, &capturingShapeApplier{}, clock,
			sourceDefaultReaders{prober: none, carrier: none})
		if err := router.RouteBoundary(context.Background(), "public.w", defaultsFixturePre(), post, defaultsFixtureDDL, 2, ir.Position{}); err != nil {
			t.Fatalf("%s: RouteBoundary: %v", name, err)
		}
		row, ok := store.snapshot("public.w")
		if !ok {
			t.Fatalf("%s: no lease row", name)
		}
		if row.DDLText != defaultsFixtureDDL || row.DDLChecksum != ChecksumDDLText(defaultsFixtureDDL) {
			t.Errorf("%s: lease recorded ddl_text %q checksum %q; want the unchanged %q", name, row.DDLText, row.DDLChecksum, defaultsFixtureDDL)
		}
	}
}
