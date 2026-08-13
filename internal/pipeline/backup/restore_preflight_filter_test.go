// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// The Bug 244 SIBLING pins (backlog 2026-08-11, closed 2026-08-13): the
// emit preflights on BOTH restore paths grade the FILTERED schema view,
// so `--exclude-table` on a table the target cannot hold is a working
// remedy instead of a re-refusal — the same filter-blind-door-defeats-
// its-own-remedy shape Bug 244 closed for the chain walk, one phase
// earlier. The recording engine asserts PER PREFLIGHT, so a single call
// regressing to the unfiltered schema is caught by name.

// preflightRecordingEngine records the table set each preflight surface
// was handed. It embeds stubEngine so any NON-preflight engine use
// still panics (the doors under test must not touch the engine beyond
// the preflight surfaces).
type preflightRecordingEngine struct {
	stubEngine
	saw map[string][]string
}

func newPreflightRecordingEngine() *preflightRecordingEngine {
	return &preflightRecordingEngine{saw: map[string][]string{}}
}

func (e *preflightRecordingEngine) record(surface string, s *ir.Schema) {
	names := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		names = append(names, t.Name)
	}
	e.saw[surface] = names
}

func (e *preflightRecordingEngine) PreflightTables(s *ir.Schema) error {
	e.record("tables", s)
	return nil
}

func (e *preflightRecordingEngine) PreflightIndexes(s *ir.Schema) error {
	e.record("indexes", s)
	return nil
}

func (e *preflightRecordingEngine) PreflightViews(s *ir.Schema) error {
	e.record("views", s)
	return nil
}

func (e *preflightRecordingEngine) PreflightColumnTypes(s *ir.Schema) error {
	e.record("column-types", s)
	return nil
}

func (e *preflightRecordingEngine) PreflightTableNameFold(_ context.Context, _ string, s *ir.Schema) error {
	e.record("name-fold", s)
	return nil
}

// preflightSurfaces is the roster the per-surface assertions run over —
// one entry per optional preflight interface the doors call. A new
// preflight added to the doors without a recorder here fails the
// completeness check in the tests below.
var preflightSurfaces = []string{"tables", "indexes", "views", "column-types", "name-fold"}

func bug244SiblingSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{Name: "bad", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}},
		{Name: "good", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}},
	}}
}

func assertAllSurfacesSaw(t *testing.T, e *preflightRecordingEngine, want []string) {
	t.Helper()
	if len(e.saw) != len(preflightSurfaces) {
		t.Fatalf("preflight surfaces reached = %d (%v); want %d — a door call is missing or a recorder is stale",
			len(e.saw), e.saw, len(preflightSurfaces))
	}
	for _, surface := range preflightSurfaces {
		got, ok := e.saw[surface]
		if !ok {
			t.Errorf("surface %q was never called", surface)
			continue
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("surface %q graded tables %v; want %v", surface, got, want)
		}
	}
}

// TestRestoreDoor_EmitPreflightsHonourTheTableFilter drives the
// single-manifest door in all three filter modes.
func TestRestoreDoor_EmitPreflightsHonourTheTableFilter(t *testing.T) {
	ctx := context.Background()

	run := func(filter migcore.TableFilter) *preflightRecordingEngine {
		e := newPreflightRecordingEngine()
		r := &Restore{Target: e, TargetDSN: "dsn", Filter: filter}
		if err := r.refuseUnrepresentableTargetShape(ctx, nil, bug244SiblingSchema()); err != nil {
			t.Fatalf("door refused a clean schema: %v", err)
		}
		return e
	}

	assertAllSurfacesSaw(t, run(migcore.TableFilter{}), []string{"bad", "good"})

	excl, err := migcore.NewTableFilter(nil, []string{"bad"})
	if err != nil {
		t.Fatal(err)
	}
	assertAllSurfacesSaw(t, run(excl), []string{"good"})

	incl, err := migcore.NewTableFilter([]string{"good"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertAllSurfacesSaw(t, run(incl), []string{"good"})
}

// TestChainRestoreDoor_EmitPreflightsHonourTheTableFilter is the
// chain-walk twin — the door that receives the ROOT manifest's schema.
func TestChainRestoreDoor_EmitPreflightsHonourTheTableFilter(t *testing.T) {
	ctx := context.Background()

	run := func(filter migcore.TableFilter) *preflightRecordingEngine {
		e := newPreflightRecordingEngine()
		r := &ChainRestore{Target: e, TargetDSN: "dsn", Filter: filter}
		if err := r.refuseUnrepresentableTargetShape(ctx, bug244SiblingSchema()); err != nil {
			t.Fatalf("door refused a clean schema: %v", err)
		}
		return e
	}

	assertAllSurfacesSaw(t, run(migcore.TableFilter{}), []string{"bad", "good"})

	excl, err := migcore.NewTableFilter(nil, []string{"bad"})
	if err != nil {
		t.Fatal(err)
	}
	assertAllSurfacesSaw(t, run(excl), []string{"good"})

	incl, err := migcore.NewTableFilter([]string{"good"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertAllSurfacesSaw(t, run(incl), []string{"good"})
}

// TestRestoreDoor_VerbatimGateHonoursTheTableFilter pins the SIXTH
// gate in the door — the ADR-0047 verbatim-extension refusal, which the
// backlog filing did not name (the sibling-sweep found it in the same
// block): a verbatim-typed column in an EXCLUDED table must not refuse
// a restore that excludes it, and must keep refusing without the
// filter.
func TestRestoreDoor_VerbatimGateHonoursTheTableFilter(t *testing.T) {
	ctx := context.Background()
	schema := bug244SiblingSchema()
	schema.Tables[0].Columns = append(schema.Tables[0].Columns,
		&ir.Column{Name: "path", Type: ir.VerbatimType{Definition: "ltree"}})

	r := &Restore{Target: newPreflightRecordingEngine(), TargetDSN: "dsn"}
	err := r.refuseUnrepresentableTargetShape(ctx, nil, schema)
	if err == nil || !strings.Contains(err.Error(), "verbatim") {
		t.Fatalf("unfiltered door did not raise the verbatim refusal: %v", err)
	}

	excl, ferr := migcore.NewTableFilter(nil, []string{"bad"})
	if ferr != nil {
		t.Fatal(ferr)
	}
	r = &Restore{Target: newPreflightRecordingEngine(), TargetDSN: "dsn", Filter: excl}
	if err := r.refuseUnrepresentableTargetShape(ctx, nil, schema); err != nil {
		t.Fatalf("--exclude-table on the verbatim-bearing table still refuses: %v", err)
	}
}

// TestFilteredSchemaView pins the helper's contract: identity on an
// empty filter (the same pointer, no copy), a SHALLOW copy under a real
// filter that never mutates the input (the chain door's root schema is
// re-read by later segments), and non-table fields carried through.
func TestFilteredSchemaView(t *testing.T) {
	s := bug244SiblingSchema()
	if got := filteredSchemaView(s, migcore.TableFilter{}); got != s {
		t.Error("empty filter must return the schema itself")
	}

	excl, err := migcore.NewTableFilter(nil, []string{"bad"})
	if err != nil {
		t.Fatal(err)
	}
	sv := filteredSchemaView(s, excl)
	if len(sv.Tables) != 1 || sv.Tables[0].Name != "good" {
		t.Fatalf("filtered view tables = %v; want [good]", sv.Tables)
	}
	if len(s.Tables) != 2 {
		t.Fatalf("input schema was mutated to %d tables; the chain door's root schema must survive", len(s.Tables))
	}
	if filteredSchemaView(nil, excl) != nil {
		t.Error("nil schema must stay nil")
	}
}
