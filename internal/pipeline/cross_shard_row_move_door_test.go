// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestJudgeKeyResidency_ShapeMatrix grades the door's decision over every
// shape the routing and the key can take. The independent expected value is
// the residency argument itself: a key lives on one shard exactly when every
// primary-vindex column is a key column (the key then fixes the vindex
// value, which fixes the shard).
func TestJudgeKeyResidency_ShapeMatrix(t *testing.T) {
	vindex := func(cols ...string) ir.ShardRouting {
		return ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: cols}
	}
	cases := []struct {
		name        string
		pk          []string
		routing     ir.ShardRouting
		wantHazard  bool
		wantOutside []string
		assertable  bool
	}{
		{name: "single-column vindex == primary key", pk: []string{"id"}, routing: vindex("id")},
		{name: "vindex is the leading column of a composite key", pk: []string{"tenant", "id"}, routing: vindex("tenant")},
		{name: "vindex is a trailing column of a composite key", pk: []string{"id", "tenant"}, routing: vindex("tenant")},
		{name: "multi-column vindex wholly within the key, other order", pk: []string{"b", "a", "c"}, routing: vindex("a", "b")},
		{name: "vindex column differs in case only", pk: []string{"Tenant", "id"}, routing: vindex("TENANT")},
		{
			name: "vindex not in the key (sequence id sharded by tenant)", pk: []string{"id"}, routing: vindex("tenant"),
			wantHazard: true, wantOutside: []string{"tenant"}, assertable: true,
		},
		{
			name: "multi-column vindex partly in the key", pk: []string{"a", "c"}, routing: vindex("a", "b"),
			wantHazard: true, wantOutside: []string{"b"}, assertable: true,
		},
		{
			name: "multi-column vindex wholly outside the key", pk: []string{"id"}, routing: vindex("a", "b"),
			wantHazard: true, wantOutside: []string{"a", "b"}, assertable: true,
		},
		{
			name: "reference table: every shard holds every row", pk: []string{"id"},
			routing: ir.ShardRouting{Kind: ir.ShardRoutingEveryShard}, wantHazard: true,
		},
		{
			name: "table the VSchema does not know", pk: []string{"id"},
			routing: ir.ShardRouting{Kind: ir.ShardRoutingUnknown}, wantHazard: true, assertable: true,
		},
		{
			name: "lookup (non-functional) primary vindex wholly inside the key", pk: []string{"id"},
			routing:    ir.ShardRouting{Kind: ir.ShardRoutingNonFunctional, PrimaryVindex: []string{"id"}, VindexType: "lookup_unique"},
			wantHazard: true, assertable: true,
		},
		{
			name: "zero-value routing (nothing established it)", pk: []string{"id"},
			routing: ir.ShardRouting{}, wantHazard: true, assertable: true,
		},
		{
			name: "vindex routing with no columns is not a pass", pk: []string{"id"},
			routing: ir.ShardRouting{Kind: ir.ShardRoutingVindex}, wantHazard: true, assertable: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := judgeKeyResidency("t", tc.pk, tc.routing)
			if (h != nil) != tc.wantHazard {
				t.Fatalf("hazard = %v; want hazard=%v", h, tc.wantHazard)
			}
			if h == nil {
				return
			}
			if !slices.Equal(h.outside, tc.wantOutside) {
				t.Errorf("outside = %v; want %v", h.outside, tc.wantOutside)
			}
			if h.assertable() != tc.assertable {
				t.Errorf("assertable = %v; want %v", h.assertable(), tc.assertable)
			}
			if !strings.Contains(h.String(), "primary key ("+strings.Join(tc.pk, ", ")+")") {
				t.Errorf("description %q does not name the primary key %v", h.String(), tc.pk)
			}
		})
	}
}

// xsRoutedSource is a sharded fake source that also reports routing. Its
// stream reads the embedded shard list (the keyspace layout) unless
// streamShards overrides it; notSharded answers as a non-sharded flavor.
type xsRoutedSource struct {
	xsShardSource
	routes       map[string]ir.ShardRouting
	routeErr     error
	asked        *[]string
	streamShards []string
	notSharded   bool
}

func (s xsRoutedSource) StreamShards(context.Context, string) (shards []string, sharded bool, err error) {
	if s.notSharded {
		return nil, false, nil
	}
	if s.streamShards != nil {
		return s.streamShards, true, nil
	}
	return s.shards, true, s.err
}

func (s xsRoutedSource) ShardRouting(_ context.Context, _ string, tables []string) (map[string]ir.ShardRouting, error) {
	if s.asked != nil {
		*s.asked = append(*s.asked, tables...)
	}
	return s.routes, s.routeErr
}

// xsVindexTable is xt from the Bug 337 rig: PRIMARY KEY(id), sharded by
// hash(tenant).
func xsVindexTable(name string) *ir.Table {
	t := xsPKTable(name)
	t.Columns = append(t.Columns, &ir.Column{Name: "tenant", Type: ir.Integer{Width: 64}})
	return t
}

var (
	xsTenantVindex = ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: []string{"tenant"}}
	xsIDVindex     = ir.ShardRouting{Kind: ir.ShardRoutingVindex, PrimaryVindex: []string{"id"}}
)

func xsDoor(source ir.Engine, schema *ir.Schema, asserted bool) crossShardRowMoveDoor {
	return crossShardRowMoveDoor{
		source: source, sourceDSN: "dsn", surface: surfaceStream, merging: true, asserted: asserted,
		readSchema: func(context.Context) (*ir.Schema, error) { return schema, nil },
	}
}

func TestCrossShardRowMoveDoor(t *testing.T) {
	ctx := context.Background()
	two := xsShardSource{shards: []string{"-80", "80-"}}

	t.Run("vindex outside the key → refuse naming table, vindex and key", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{"xt": xsTenantVindex}}
		err := xsDoor(src, xsSchema(xsVindexTable("xt")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("got %v; want errCrossShardRowMoveRefused", err)
		}
		for _, want := range []string{
			crossShardVindexNotInPKMarker, "xt: primary vindex (tenant)", "primary key (id)", "-80,80-",
			"--inject-shard-column", "vstream_shards=<shard>", "--assert-no-cross-shard-row-moves", "sync start",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not carry %q: %v", want, err)
			}
		}
	})

	t.Run("every table's vindex within its key → pass", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{"a": xsIDVindex, "b": xsIDVindex}}
		if err := xsDoor(src, xsSchema(xsPKTable("a"), xsPKTable("b")), false).run(ctx); err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("every hazard is named, not only the first", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{
			"ok": xsIDVindex, "xt": xsTenantVindex, "yt": xsTenantVindex,
		}}
		err := xsDoor(src, xsSchema(xsVindexTable("yt"), xsPKTable("ok"), xsVindexTable("xt")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("got %v; want refusal", err)
		}
		if !strings.Contains(err.Error(), "xt: primary vindex") || !strings.Contains(err.Error(), "yt: primary vindex") {
			t.Errorf("refusal should name both xt and yt: %v", err)
		}
		if strings.Contains(err.Error(), "ok: ") {
			t.Errorf("refusal names the safe table: %v", err)
		}
	})

	t.Run("asserted → WARN carrying the assertion, nil", func(t *testing.T) {
		logs := captureSlog(t)
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{"xt": xsTenantVindex}}
		if err := xsDoor(src, xsSchema(xsVindexTable("xt")), true).run(ctx); err != nil {
			t.Fatalf("got %v; want nil under the assertion", err)
		}
		out := logs.String()
		for _, want := range []string{"level=WARN", crossShardRowMovesAssertedMarker, "tables=xt", "primary vindex (tenant)"} {
			if !strings.Contains(out, want) {
				t.Errorf("WARN does not carry %q:\n%s", want, out)
			}
		}
	})

	t.Run("asserted does not cover a reference table", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{
			"xt": xsTenantVindex, "ref": {Kind: ir.ShardRoutingEveryShard},
		}}
		err := xsDoor(src, xsSchema(xsVindexTable("xt"), xsPKTable("ref")), true).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("got %v; want refusal for a reference table under the assertion", err)
		}
		if !strings.Contains(err.Error(), "does not cover ref") {
			t.Errorf("refusal should say the assertion does not cover ref: %v", err)
		}
	})

	t.Run("a table the routing answer omits → refuse (unknown, not skipped)", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{}}
		err := xsDoor(src, xsSchema(xsPKTable("orphan")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) || !strings.Contains(err.Error(), "VSchema has no entry") {
			t.Fatalf("got %v; want refusal naming the missing VSchema entry", err)
		}
	})

	t.Run("VSchema unreadable → refuse; asserted → WARN", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, routeErr: errors.New("permission denied for SHOW VSCHEMA")}
		err := xsDoor(src, xsSchema(xsVindexTable("xt")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("got %v; want fail-closed refusal carrying the read error", err)
		}
		logs := captureSlog(t)
		if err := xsDoor(src, xsSchema(xsVindexTable("xt")), true).run(ctx); err != nil {
			t.Fatalf("asserted: got %v; want nil", err)
		}
		if !strings.Contains(logs.String(), crossShardRowMovesAssertedMarker) || !strings.Contains(logs.String(), "could not be verified") {
			t.Errorf("asserted unreadable VSchema should WARN that the routing is unverified:\n%s", logs.String())
		}
	})

	t.Run("sharded source that cannot report routing → refuse", func(t *testing.T) {
		err := xsDoor(two, xsSchema(xsVindexTable("xt")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("got %v; want refusal (a ShardDiscoverer without a ShardRoutingReader proves nothing)", err)
		}
	})

	t.Run("shard discovery error → refuse", func(t *testing.T) {
		boom := xsRoutedSource{xsShardSource: xsShardSource{err: errors.New("vtgate unreachable")}}
		err := xsDoor(boom, xsSchema(xsVindexTable("xt")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) || !strings.Contains(err.Error(), "vtgate unreachable") {
			t.Fatalf("got %v; want fail-closed refusal", err)
		}
	})

	t.Run("keyless tables only → pass without asking for routing", func(t *testing.T) {
		var asked []string
		src := xsRoutedSource{xsShardSource: two, asked: &asked}
		if err := xsDoor(src, xsSchema(xsKeylessTable("events"), xsUniqueTable("users")), false).run(ctx); err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if len(asked) != 0 {
			t.Errorf("routing was read for %v; a table without a primary key is not judged", asked)
		}
	})

	t.Run("single shard / unsharded keyspace → pass without reading the schema", func(t *testing.T) {
		d := xsDoor(xsShardSource{shards: []string{"-"}}, nil, false)
		d.readSchema = func(context.Context) (*ir.Schema, error) { panic("schema read on an unsharded source") }
		if err := d.run(ctx); err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("not merging → pass without discovering shards", func(t *testing.T) {
		d := xsDoor(stubEngine{}, nil, false)
		d.merging = false
		d.readSchema = func(context.Context) (*ir.Schema, error) { panic("schema read when not merging") }
		if err := d.run(ctx); err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("stream pinned to one shard of a sharded keyspace → pass, not judged against the keyspace", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, streamShards: []string{"-80"}, routes: map[string]ir.ShardRouting{"xt": xsTenantVindex}}
		d := xsDoor(src, nil, false)
		d.readSchema = func(context.Context) (*ir.Schema, error) { panic("schema read for a single-shard stream") }
		if err := d.run(ctx); err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("sharded source resolving to NO shards → refuse (unverified, not unsharded)", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: xsShardSource{shards: []string{}}, routes: map[string]ir.ShardRouting{"xt": xsTenantVindex}}
		err := xsDoor(src, xsSchema(xsVindexTable("xt")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) || !strings.Contains(err.Error(), "resolved to no shards") {
			t.Fatalf("got %v; want the no-shards refusal", err)
		}
	})

	t.Run("routing reader answering not-sharded → pass without reading the schema", func(t *testing.T) {
		d := xsDoor(xsRoutedSource{notSharded: true}, nil, false)
		d.readSchema = func(context.Context) (*ir.Schema, error) { panic("schema read on a non-sharded source") }
		if err := d.run(ctx); err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("lookup (non-functional) primary vindex inside the key → refuse, assertable", func(t *testing.T) {
		lk := ir.ShardRouting{Kind: ir.ShardRoutingNonFunctional, PrimaryVindex: []string{"id"}, VindexType: "lookup_unique"}
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{"lt": lk}}
		err := xsDoor(src, xsSchema(xsPKTable("lt")), false).run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) || !strings.Contains(err.Error(), "lookup_unique vindex") {
			t.Fatalf("got %v; want a refusal naming the lookup vindex", err)
		}
		if err := xsDoor(src, xsSchema(xsPKTable("lt")), true).run(ctx); err != nil {
			t.Fatalf("asserted: got %v; want nil", err)
		}
	})

	t.Run("add-table surface names neither per-shard inject nor an unconditional assertion", func(t *testing.T) {
		src := xsRoutedSource{xsShardSource: two, routes: map[string]ir.ShardRouting{"xt": xsTenantVindex}}
		d := xsDoor(src, xsSchema(xsVindexTable("xt")), false)
		d.surface = surfaceAddTable
		err := d.run(ctx)
		if !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("got %v; want refusal", err)
		}
		for _, want := range []string{"schema add-table", "stamps no shard discriminator", "do not pass it for a per-shard"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("add-table refusal lacks %q: %v", want, err)
			}
		}
		if strings.Contains(err.Error(), "pin each source DSN to one shard") {
			t.Errorf("add-table refusal offers the per-shard inject remedy, which add-table cannot carry out: %v", err)
		}
	})

	t.Run("non-sharded engine → pass", func(t *testing.T) {
		if err := xsDoor(stubEngine{}, xsSchema(xsVindexTable("xt")), false).run(ctx); err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("schema read error → surfaced, not a pass", func(t *testing.T) {
		d := xsDoor(xsRoutedSource{xsShardSource: two}, nil, false)
		d.readSchema = func(context.Context) (*ir.Schema, error) { return nil, errors.New("schema boom") }
		if err := d.run(ctx); err == nil || !strings.Contains(err.Error(), "schema boom") {
			t.Fatalf("got %v; want the schema read error", err)
		}
	})
}

// TestStreamerRun_CrossShardRowMoveDoorRefusesBeforeAnyAttempt pins the
// wiring the door's reach rests on: [Streamer.Run] refuses an exposed table
// before any attempt opens the applier or a change stream, so a cold start
// and a warm resume are refused alike — and the zero-value Streamer (every
// construction that is not the CLI) refuses rather than passes.
func TestStreamerRun_CrossShardRowMoveDoorRefusesBeforeAnyAttempt(t *testing.T) {
	src := xsRoutedSchemaSource{
		xsRoutedSource: xsRoutedSource{
			xsShardSource: xsShardSource{shards: []string{"-80", "80-"}},
			routes:        map[string]ir.ShardRouting{"xt": xsTenantVindex},
		},
		schema: xsSchema(xsVindexTable("xt"), xsPKTable("excluded")),
	}
	filter, err := migcore.NewTableFilter(nil, []string{"excluded"})
	if err != nil {
		t.Fatal(err)
	}
	// Both settings of --allow-cross-shard-merge refuse: the flag is not
	// persisted, and a warm resume (or a cold start the collision door skips)
	// merges the shards without it.
	for _, allow := range []bool{false, true} {
		s := &Streamer{
			Source: src, Target: stubEngine{}, SourceDSN: "src", TargetDSN: "dst",
			AllowCrossShardMerge: allow, Filter: filter,
			// A runOnce that runs is the failure: the door must stop Run first.
			runOnceFn: func(context.Context) error { t.Fatal("runOnce reached past the door"); return nil },
		}
		err = s.Run(context.Background())
		if !errors.Is(err, errCrossShardRowMoveRefused) {
			t.Fatalf("allow-cross-shard-merge=%v: Run = %v; want the cross-shard row-move refusal", allow, err)
		}
		if strings.Contains(err.Error(), "excluded:") {
			t.Errorf("the refusal judged a table the filter excludes: %v", err)
		}
	}

	reached := false
	proceed := func(context.Context) error { reached = true; return nil }
	asserted := &Streamer{
		Source: src, Target: stubEngine{}, SourceDSN: "src", TargetDSN: "dst", Filter: filter,
		AssertNoCrossShardRowMoves: true, runOnceFn: proceed,
	}
	if err := asserted.Run(context.Background()); err != nil || !reached {
		t.Fatalf("asserted Run = %v (runOnce reached=%v); want the attempt to proceed", err, reached)
	}

	// A per-shard --inject-shard-column stream is not judged: its
	// discriminator keeps each shard's rows apart.
	reached = false
	injected := &Streamer{
		Source: src, Target: stubEngine{}, SourceDSN: "src", TargetDSN: "dst", Filter: filter,
		InjectShardColumn: ShardColumnSpec{Name: "src_shard", Value: "1"}, runOnceFn: proceed,
	}
	if err := injected.Run(context.Background()); err != nil || !reached {
		t.Fatalf("inject Run = %v (runOnce reached=%v); want the attempt to proceed", err, reached)
	}
}

// xsRoutedSchemaSource is xsRoutedSource with a source schema to read.
type xsRoutedSchemaSource struct {
	xsRoutedSource
	schema *ir.Schema
}

func (s xsRoutedSchemaSource) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return staticXSSchemaReader{s.schema}, nil
}

type staticXSSchemaReader struct{ schema *ir.Schema }

func (r staticXSSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	tables := append([]*ir.Table(nil), r.schema.Tables...)
	return &ir.Schema{Tables: tables}, nil
}
