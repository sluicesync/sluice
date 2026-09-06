// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ---- the shape the fan-out seed has to get right ----
//
// Both readers key their prior by (namespace, table). A fan-out seed that
// carried bare names would collide the moment two selected namespaces hold a
// same-named table — the ordinary case for a `--schemas`/`--databases` run —
// and one namespace would resume on the other's prior: a phantom refusal on
// one table and, on the other, exactly the silent prime this whole surface
// exists to prevent. Every pin below is about the KEY, not about the
// zone-swap predicate, which schema_seed_witness_test.go already covers.

// TestMultiDatabaseColdStartSchemaSeed_KeysEveryNamespace pins the fan-out
// cold-start accumulator: one entry per (namespace, table), each carrying its
// OWN namespace's column types, and a defensive namespace fill for a reader
// that leaves Table.Schema empty.
func TestMultiDatabaseColdStartSchemaSeed_KeysEveryNamespace(t *testing.T) {
	sales := &ir.Schema{Tables: []*ir.Table{
		{Schema: "sales", Name: "events", Columns: []*ir.Column{
			{Name: "c", Type: ir.Timestamp{WithTimeZone: true}},
		}},
	}}
	// billing holds a SAME-NAMED table with the other zone family — the
	// collision the bare-name key would have merged.
	billing := &ir.Schema{Tables: []*ir.Table{
		{Schema: "billing", Name: "events", Columns: []*ir.Column{
			{Name: "c", Type: ir.DateTime{}},
		}},
	}}
	// An engine whose scoped reader left the namespace off: the accumulator
	// fills it rather than letting the table resolve under the server-wide
	// reader's bound namespace.
	unstamped := &ir.Schema{Tables: []*ir.Table{
		{Name: "ledger", Columns: []*ir.Column{{Name: "c", Type: ir.DateTime{}}}},
	}}

	var seed []*ir.Table
	seed = multiDatabaseColdStartSchemaSeed(seed, "sales", sales)
	seed = multiDatabaseColdStartSchemaSeed(seed, "billing", billing)
	seed = multiDatabaseColdStartSchemaSeed(seed, "audit", unstamped)

	want := map[string]ir.Type{
		"sales.events":   ir.Timestamp{WithTimeZone: true},
		"billing.events": ir.DateTime{},
		"audit.ledger":   ir.DateTime{},
	}
	got := map[string]ir.Type{}
	for _, tbl := range seed {
		key := tbl.Schema + "." + tbl.Name
		if _, dup := got[key]; dup {
			t.Fatalf("seed carries %q twice", key)
		}
		got[key] = tbl.Columns[0].Type
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cold-start fan-out seed = %#v; want %#v", got, want)
	}
	// The defensive fill must not write through to the SchemaReader's own
	// table — the copy loop keeps using that schema after the capture.
	if unstamped.Tables[0].Schema != "" {
		t.Fatal("the namespace fill mutated the source reader's table in place")
	}
}

// TestMultiDatabaseWarmResumeSeed_WitnessesEveryNamespace pins the fan-out
// warm-resume prior: the target is witnessed ONCE PER SELECTED NAMESPACE
// through that namespace's own target DSN, every seed table is stamped with
// its SOURCE namespace (not the --map-database target name), and the retained
// history is partitioned by namespace so one namespace's history can never
// stand in as another's prior.
func TestMultiDatabaseWarmResumeSeed_WitnessesEveryNamespace(t *testing.T) {
	ctx := context.Background()
	target := &fanOutTargetEngine{schemas: map[string][]*ir.Table{
		// The TARGET namespace names: sales is renamed to sales_v2 by the
		// map below, billing is not.
		"sales_v2": {{Schema: "sales_v2", Name: "events", Columns: []*ir.Column{
			{Name: "c", Type: ir.Timestamp{WithTimeZone: true}},
		}}},
		"billing": {{Schema: "billing", Name: "events", Columns: []*ir.Column{
			{Name: "c", Type: ir.DateTime{}},
		}}},
	}}
	s := &Streamer{
		Target:       target,
		TargetDSN:    "server://target",
		NamespaceMap: mustNamespaceRenameMap(t, "sales=sales_v2"),
	}
	// History for a table the target does NOT hold, in one namespace only.
	applier := &seedHistoryApplier{rows: []ir.RetainedSchemaVersionRow{{
		SchemaName:     "billing",
		TableName:      "invoices",
		AnchorPosition: "1",
		TableJSON: mustTableJSON(t, &ir.Table{Schema: "billing", Name: "invoices", Columns: []*ir.Column{
			{Name: "c", Type: ir.Timestamp{WithTimeZone: true}},
		}}),
	}}}

	seed, err := s.loadMultiDatabaseWarmResumeSchemaSeed(
		ctx, applier, "stream-1", ir.Position{Engine: "e", Token: "9"},
		[]string{"sales", "billing"}, target,
	)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]ir.Type{}
	for _, tbl := range seed {
		got[tbl.Schema+"."+tbl.Name] = seedColumnType([]*ir.Table{tbl}, tbl.Name, "c")
	}
	want := map[string]ir.Type{
		// Witnessed through the RENAMED target namespace, keyed under the
		// SOURCE one — the reader sees relations by their source namespace.
		"sales.events": ir.Timestamp{WithTimeZone: true},
		// The sibling namespace's same-named table keeps its OWN prior.
		"billing.events": ir.DateTime{},
		// The history fallback stays inside its namespace.
		"billing.invoices": ir.Timestamp{WithTimeZone: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fan-out warm-resume seed = %#v; want %#v", got, want)
	}
	sort.Strings(target.read)
	if !reflect.DeepEqual(target.read, []string{"server://target?db=billing", "server://target?db=sales_v2"}) {
		t.Fatalf("target read through %v; want one read per selected namespace, through its own derived DSN", target.read)
	}
}

// TestMultiDatabaseWarmResumeSeed_DegradesOnAnUnreadableNamespace pins the
// documented degrade: a namespace whose target counterpart cannot be read
// (commonest cause — a source database created since cold start, admitted by
// the live re-resolve, with no target namespace yet) resumes on its retained
// history alone rather than failing a resume that works today. The SIBLING
// namespaces keep their witness.
func TestMultiDatabaseWarmResumeSeed_DegradesOnAnUnreadableNamespace(t *testing.T) {
	ctx := context.Background()
	target := &fanOutTargetEngine{
		schemas: map[string][]*ir.Table{
			"billing": {{Schema: "billing", Name: "events", Columns: []*ir.Column{
				{Name: "c", Type: ir.DateTime{}},
			}}},
		},
		openErr: map[string]error{"brand_new": errors.New("schema does not exist")},
	}
	s := &Streamer{Target: target, TargetDSN: "server://target"}
	applier := &seedHistoryApplier{rows: []ir.RetainedSchemaVersionRow{{
		SchemaName:     "brand_new",
		TableName:      "events",
		AnchorPosition: "1",
		TableJSON: mustTableJSON(t, &ir.Table{Schema: "brand_new", Name: "events", Columns: []*ir.Column{
			{Name: "c", Type: ir.Timestamp{WithTimeZone: true}},
		}}),
	}}}

	seed, err := s.loadMultiDatabaseWarmResumeSchemaSeed(
		ctx, applier, "stream-1", ir.Position{Engine: "e", Token: "9"},
		[]string{"billing", "brand_new"}, target,
	)
	if err != nil {
		t.Fatalf("an unreadable target namespace failed the whole resume: %v", err)
	}
	got := map[string]ir.Type{}
	for _, tbl := range seed {
		got[tbl.Schema+"."+tbl.Name] = seedColumnType([]*ir.Table{tbl}, tbl.Name, "c")
	}
	want := map[string]ir.Type{
		"billing.events":   ir.DateTime{},
		"brand_new.events": ir.Timestamp{WithTimeZone: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("seed after a degraded namespace = %#v; want %#v", got, want)
	}

	// A nil deriver takes the same degrade rather than reading the bound
	// namespace and speaking for every namespace with one answer.
	seed, err = s.loadMultiDatabaseWarmResumeSchemaSeed(
		ctx, applier, "stream-1", ir.Position{Engine: "e", Token: "9"},
		[]string{"billing", "brand_new"}, nil,
	)
	if err != nil {
		t.Fatalf("a target with no per-namespace DSN deriver failed the resume: %v", err)
	}
	for _, tbl := range seed {
		if tbl.Schema == "billing" && tbl.Name == "events" {
			t.Fatal("a nil deriver still produced a target witness; it must degrade to history only")
		}
	}
}

// TestMultiDatabaseWarmResumeSeed_HistoryErrorIsLoud pins the half that stays
// loud: the retained-history read feeds the same refusal the witness does, and
// degrading it would reopen the window silently.
func TestMultiDatabaseWarmResumeSeed_HistoryErrorIsLoud(t *testing.T) {
	s := &Streamer{Target: &fanOutTargetEngine{}, TargetDSN: "server://target"}
	applier := &seedHistoryApplier{err: errors.New("control table unreachable")}
	if _, err := s.loadMultiDatabaseWarmResumeSchemaSeed(
		context.Background(), applier, "stream-1", ir.Position{Engine: "e", Token: "9"},
		[]string{"billing"}, &fanOutTargetEngine{},
	); err == nil {
		t.Fatal("a schema-history read error degraded to a partial seed instead of failing the resume")
	}
}

// ---- fakes ----

// fanOutTargetEngine is a target that namespaces by a `?db=` DSN parameter,
// the way MySQL's and Postgres's derivers do with a database / schema
// component. It records every DSN it was read through so the pins can assert
// the fan-out witnessed each namespace separately.
type fanOutTargetEngine struct {
	stubEngineBase
	schemas map[string][]*ir.Table
	openErr map[string]error
	read    []string
}

func (e *fanOutTargetEngine) Name() string { return "fanout-target" }

func (e *fanOutTargetEngine) WithDatabase(dsn, database string) (string, error) {
	return dsn + "?db=" + database, nil
}

func (e *fanOutTargetEngine) EnsureDatabase(context.Context, string, string) error { return nil }

func (e *fanOutTargetEngine) OpenSchemaReader(_ context.Context, dsn string) (ir.SchemaReader, error) {
	ns := dsn
	if i := strings.Index(dsn, "?db="); i >= 0 {
		ns = dsn[i+len("?db="):]
	}
	e.read = append(e.read, dsn)
	if err := e.openErr[ns]; err != nil {
		return nil, fmt.Errorf("open %q: %w", ns, err)
	}
	return &fanOutSchemaReader{tables: e.schemas[ns]}, nil
}

type fanOutSchemaReader struct{ tables []*ir.Table }

func (r *fanOutSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	return &ir.Schema{Tables: r.tables}, nil
}

func mustTableJSON(t *testing.T, tbl *ir.Table) []byte {
	t.Helper()
	b, err := ir.MarshalTable(tbl)
	if err != nil {
		t.Fatalf("marshal %s.%s: %v", tbl.Schema, tbl.Name, err)
	}
	return b
}

func mustNamespaceRenameMap(t *testing.T, pairs ...string) NamespaceRenameMap {
	t.Helper()
	m, err := NewNamespaceRenameMap(pairs)
	if err != nil {
		t.Fatalf("NewNamespaceRenameMap(%v): %v", pairs, err)
	}
	return m
}
