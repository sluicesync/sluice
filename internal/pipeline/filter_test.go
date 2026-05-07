// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestNewTableFilterMutualExclusion checks that supplying both
// Include and Exclude is rejected up front, before the filter
// participates in any migration.
func TestNewTableFilterMutualExclusion(t *testing.T) {
	_, err := NewTableFilter([]string{"users"}, []string{"audit_log"})
	if err == nil {
		t.Fatal("expected error for both include and exclude; got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("err = %v; want a mutual-exclusion message", err)
	}
}

// TestNewTableFilterRejectsBadPattern uses a malformed character
// class to trigger path.Match's syntax error path. Filters that
// reach the matcher with bogus patterns would silently no-op every
// table; rejecting at construction is safer.
func TestNewTableFilterRejectsBadPattern(t *testing.T) {
	cases := []struct {
		name    string
		include []string
		exclude []string
	}{
		{"bad include", []string{"users", "[unclosed"}, nil},
		{"bad exclude", nil, []string{"[unclosed"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewTableFilter(c.include, c.exclude); err == nil {
				t.Errorf("expected error for malformed pattern; got nil")
			}
		})
	}
}

// TestTableFilterAllows is the table-driven contract test for the
// Allows method. Covers literal names, globs, and edge cases.
func TestTableFilterAllows(t *testing.T) {
	cases := []struct {
		name   string
		filter TableFilter
		table  string
		want   bool
	}{
		{"empty filter passes everything", TableFilter{}, "users", true},
		{"empty filter passes empty name", TableFilter{}, "", true},

		{"include literal match", TableFilter{Include: []string{"users"}}, "users", true},
		{"include literal miss", TableFilter{Include: []string{"users"}}, "orders", false},
		{"include multi-literal hit", TableFilter{Include: []string{"users", "orders"}}, "orders", true},
		{"include glob hit", TableFilter{Include: []string{"audit_*"}}, "audit_log", true},
		{"include glob miss", TableFilter{Include: []string{"audit_*"}}, "users", false},
		{"include glob and literal", TableFilter{Include: []string{"users", "audit_*"}}, "audit_login", true},
		{"include question-mark glob", TableFilter{Include: []string{"t?bl"}}, "tabl", true},
		{"include character class", TableFilter{Include: []string{"[ab]_thing"}}, "a_thing", true},
		{"include character class miss", TableFilter{Include: []string{"[ab]_thing"}}, "c_thing", false},

		{"exclude literal match", TableFilter{Exclude: []string{"audit_log"}}, "audit_log", false},
		{"exclude literal miss", TableFilter{Exclude: []string{"audit_log"}}, "users", true},
		{"exclude glob match", TableFilter{Exclude: []string{"tmp_*"}}, "tmp_export", false},
		{"exclude glob miss", TableFilter{Exclude: []string{"tmp_*"}}, "users", true},
		{"exclude multi", TableFilter{Exclude: []string{"audit_log", "sessions"}}, "sessions", false},

		{"name with special characters survives literal", TableFilter{Include: []string{"users-archive"}}, "users-archive", true},
		{"empty string with literal include miss", TableFilter{Include: []string{"users"}}, "", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.filter.Allows(c.table); got != c.want {
				t.Errorf("Allows(%q) = %v; want %v (filter=%+v)", c.table, got, c.want, c.filter)
			}
		})
	}
}

// TestTableFilterIsEmpty confirms the helper distinguishes the
// zero-value filter (no rules) from any populated filter.
func TestTableFilterIsEmpty(t *testing.T) {
	if !(TableFilter{}).IsEmpty() {
		t.Error("zero-value TableFilter should be empty")
	}
	if (TableFilter{Include: []string{"x"}}).IsEmpty() {
		t.Error("filter with Include should not be empty")
	}
	if (TableFilter{Exclude: []string{"x"}}).IsEmpty() {
		t.Error("filter with Exclude should not be empty")
	}
}

// TestApplyTableFilterPrunes is the orchestrator-side
// schema-prune test: given a schema with three tables and a
// filter that allows one, the schema's Tables slice should be
// reduced to that one and the log line should report the
// matched/excluded counts.
func TestApplyTableFilterPrunes(t *testing.T) {
	logs := captureSlog(t)

	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users"},
			{Name: "orders"},
			{Name: "audit_log"},
		},
	}
	filter := TableFilter{Exclude: []string{"audit_*"}}
	if err := applyTableFilter(context.Background(), schema, filter); err != nil {
		t.Fatalf("applyTableFilter: %v", err)
	}
	if len(schema.Tables) != 2 {
		t.Fatalf("schema.Tables = %d; want 2", len(schema.Tables))
	}
	for _, tab := range schema.Tables {
		if tab.Name == "audit_log" {
			t.Errorf("audit_log should have been pruned")
		}
	}
	if !strings.Contains(logs.String(), "table filter applied") {
		t.Errorf("expected info log; got %q", logs.String())
	}
	if !strings.Contains(logs.String(), "matched=2") {
		t.Errorf("log should report matched=2; got %q", logs.String())
	}
	if !strings.Contains(logs.String(), "excluded=1") {
		t.Errorf("log should report excluded=1; got %q", logs.String())
	}
}

// TestApplyTableFilterEmptyResultErrors confirms the orchestrator
// surfaces a clear error when the filter excludes every table —
// almost always user error worth catching loudly.
func TestApplyTableFilterEmptyResultErrors(t *testing.T) {
	_ = captureSlog(t) // consume log noise

	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}},
	}
	filter := TableFilter{Include: []string{"nonexistent"}}
	err := applyTableFilter(context.Background(), schema, filter)
	if err == nil {
		t.Fatal("expected error for empty-result filter; got nil")
	}
	if !strings.Contains(err.Error(), "every source table") {
		t.Errorf("err = %v; want a 'excluded every source table' message", err)
	}
}

// TestApplyTableFilterEmptyFilterNoOp checks that an empty filter
// neither prunes nor logs (the orchestrator's hot path should not
// emit a "filter applied" line on every migration).
func TestApplyTableFilterEmptyFilterNoOp(t *testing.T) {
	logs := captureSlog(t)

	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}},
	}
	if err := applyTableFilter(context.Background(), schema, TableFilter{}); err != nil {
		t.Fatalf("applyTableFilter: %v", err)
	}
	if len(schema.Tables) != 2 {
		t.Errorf("empty filter should not prune; got %d tables", len(schema.Tables))
	}
	if strings.Contains(logs.String(), "table filter applied") {
		t.Errorf("empty filter should not log; got %q", logs.String())
	}
}

// TestFilterChangesDropsExcluded covers the streamer-side dispatch
// filter: events whose qualified name doesn't pass the filter must
// be dropped, allowed events must propagate intact, and the
// goroutine must terminate on input close.
func TestFilterChangesDropsExcluded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ir.Change, 4)
	in <- ir.Insert{Schema: "public", Table: "users", Row: ir.Row{"id": 1}}
	in <- ir.Insert{Schema: "public", Table: "audit_log", Row: ir.Row{"id": 2}}
	in <- ir.Insert{Schema: "", Table: "audit_login", Row: ir.Row{"id": 3}}
	in <- ir.Insert{Schema: "", Table: "orders", Row: ir.Row{"id": 4}}
	close(in)

	filter := TableFilter{Exclude: []string{"audit_*"}}
	out := filterChanges(ctx, in, filter)

	received := make([]string, 0, 4)
	for c := range out {
		ins := c.(ir.Insert)
		received = append(received, ins.Table)
	}
	want := []string{"users", "orders"}
	if len(received) != len(want) {
		t.Fatalf("received = %v; want %v", received, want)
	}
	for i, w := range want {
		if received[i] != w {
			t.Errorf("received[%d] = %q; want %q", i, received[i], w)
		}
	}
}

// TestFilterChangesEmptyFilterPassthrough confirms the optimisation
// path: an empty filter returns events unchanged, so callers don't
// pay a goroutine + channel-hop tax on every event. We can't
// compare channel identity through the read-only return type, so
// we send a marker change and confirm it arrives intact.
func TestFilterChangesEmptyFilterPassthrough(t *testing.T) {
	ctx := context.Background()
	in := make(chan ir.Change, 1)
	in <- ir.Insert{Table: "users", Row: ir.Row{"id": int64(7)}}
	close(in)

	out := filterChanges(ctx, in, TableFilter{})
	c, ok := <-out
	if !ok {
		t.Fatalf("expected event from empty-filter channel")
	}
	ins, ok := c.(ir.Insert)
	if !ok || ins.Table != "users" {
		t.Errorf("got %#v; want ir.Insert{Table:\"users\"}", c)
	}
	if _, more := <-out; more {
		t.Errorf("channel should be closed after one event")
	}
}

// TestFilterChangesDebugLogOnly checks that a dropped event emits
// at debug level (not info) — info-level drops would spam any log
// aggregator on a busy stream.
func TestFilterChangesDebugLogOnly(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	// Set the handler to Info: a debug-level message should NOT
	// appear in the buffer.
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx := context.Background()
	in := make(chan ir.Change, 1)
	in <- ir.Insert{Table: "audit_log"}
	close(in)
	out := filterChanges(ctx, in, TableFilter{Exclude: []string{"audit_*"}})
	for range out {
	}
	if strings.Contains(buf.String(), "cdc event dropped") {
		t.Errorf("drop log surfaced at info level; want debug-only. log=%q", buf.String())
	}
}

// TestChangeAllowedStripsSchemaPrefix covers the schema-prefix
// stripping logic: filter patterns target unqualified names, so a
// "public.users" change must be checked against "users".
func TestChangeAllowedStripsSchemaPrefix(t *testing.T) {
	cases := []struct {
		name   string
		change ir.Change
		filter TableFilter
		want   bool
	}{
		{
			"schema-qualified passes literal include",
			ir.Insert{Schema: "public", Table: "users"},
			TableFilter{Include: []string{"users"}},
			true,
		},
		{
			"schema-qualified caught by exclude",
			ir.Insert{Schema: "public", Table: "audit_log"},
			TableFilter{Exclude: []string{"audit_*"}},
			false,
		},
		{
			"unqualified passes",
			ir.Insert{Table: "users"},
			TableFilter{Include: []string{"users"}},
			true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := changeAllowed(c.change, c.filter); got != c.want {
				t.Errorf("changeAllowed = %v; want %v", got, c.want)
			}
		})
	}
}

// fakeExcluder is a minimal ir.Engine + ir.DefaultTableExcluder
// implementation used by the engine-default-merge tests below. The
// embedded stubEngine satisfies ir.Engine via no-op implementations
// of every method; DefaultExcludePatterns is the only behaviour the
// merge logic cares about.
type fakeExcluder struct {
	stubEngine
	patterns []string
}

func (f fakeExcluder) DefaultExcludePatterns(string) []string { return f.patterns }

// TestEffectiveTableFilter_MergesEngineDefaults covers the Bug 22
// auto-exclude logic for PlanetScale's `_vt_*` Vitess shadow tables
// (and any future engine that opts into [ir.DefaultTableExcluder]).
//
// The merge is gated three ways: (a) engine must implement the
// interface; (b) operator must not have supplied --include-table;
// (c) duplicate patterns are deduplicated by string equality.
func TestEffectiveTableFilter_MergesEngineDefaults(t *testing.T) {
	engine := fakeExcluder{patterns: []string{"_vt_*"}}

	cases := []struct {
		name        string
		in          TableFilter
		wantExclude []string
		wantAdded   []string
	}{
		{
			"empty filter gets engine defaults appended",
			TableFilter{},
			[]string{"_vt_*"},
			[]string{"_vt_*"},
		},
		{
			"operator exclude is preserved and engine defaults appended",
			TableFilter{Exclude: []string{"audit_*"}},
			[]string{"audit_*", "_vt_*"},
			[]string{"_vt_*"},
		},
		{
			"operator include short-circuits the merge",
			TableFilter{Include: []string{"users"}},
			nil, // unchanged: Include preserved, Exclude stays empty
			nil,
		},
		{
			"engine pattern already in operator exclude is deduplicated",
			TableFilter{Exclude: []string{"_vt_*", "audit_*"}},
			[]string{"_vt_*", "audit_*"},
			nil, // nothing added — operator already had it
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, added := effectiveTableFilter(c.in, engine, "")
			if !equalSlices(added, c.wantAdded) {
				t.Errorf("added = %v; want %v", added, c.wantAdded)
			}
			if c.in.Include != nil {
				if !equalSlices(got.Include, c.in.Include) {
					t.Errorf("Include = %v; want %v (preserved)", got.Include, c.in.Include)
				}
			}
			if !equalSlices(got.Exclude, c.wantExclude) {
				t.Errorf("Exclude = %v; want %v", got.Exclude, c.wantExclude)
			}
		})
	}
}

// TestEffectiveTableFilter_NonExcluderEngine asserts that an engine
// not implementing [ir.DefaultTableExcluder] short-circuits cleanly.
// Vanilla MySQL (which has no `_vt_*` reserved prefix) goes through
// this path.
func TestEffectiveTableFilter_NonExcluderEngine(t *testing.T) {
	in := TableFilter{Exclude: []string{"audit_*"}}
	got, added := effectiveTableFilter(in, stubEngine{}, "")
	if added != nil {
		t.Errorf("added = %v; want nil (engine doesn't implement the interface)", added)
	}
	if !equalSlices(got.Exclude, in.Exclude) {
		t.Errorf("Exclude = %v; want %v (unchanged)", got.Exclude, in.Exclude)
	}
}

// TestEffectiveTableFilter_EmptyDefaults covers an engine that
// implements DefaultTableExcluder but returns no patterns (e.g.
// vanilla MySQL once the shared mysql.Engine grows the method).
// Equivalent to no opt-in.
func TestEffectiveTableFilter_EmptyDefaults(t *testing.T) {
	in := TableFilter{Exclude: []string{"audit_*"}}
	got, added := effectiveTableFilter(in, fakeExcluder{patterns: nil}, "")
	if added != nil {
		t.Errorf("added = %v; want nil (no defaults declared)", added)
	}
	if !equalSlices(got.Exclude, in.Exclude) {
		t.Errorf("Exclude = %v; want %v (unchanged)", got.Exclude, in.Exclude)
	}
}

// equalSlices is a string-slice equality helper used by the merge
// tests; nil and []string{} are treated as equal.
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
