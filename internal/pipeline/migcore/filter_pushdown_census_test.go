// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// scopedReader is a minimal [ir.TableScoper] that behaves the way the real
// Postgres reader does: it asks the scope predicate about EVERY candidate
// table and then omits the ones it rejects from the schema it returns.
//
// That behaviour is the whole point of this file. The v0.142.0 pin for the
// unmatched-pattern warning built its schema by hand, so the excluded table
// was still present when ApplyTableFilter ran — a shape no Postgres run
// ever produces. It passed, and the regression shipped.
type scopedReader struct {
	sourceTables []string
	scope        func(string) bool
}

func (r *scopedReader) SetTableScope(allow func(tableName string) bool) { r.scope = allow }

// readSchema mirrors postgres.readTables: consult the predicate per table,
// drop what it rejects.
func (r *scopedReader) readSchema() *ir.Schema {
	s := &ir.Schema{}
	for _, name := range r.sourceTables {
		if r.scope != nil && !r.scope(name) {
			continue
		}
		s.Tables = append(s.Tables, &ir.Table{Name: name})
	}
	return s
}

var _ ir.TableScoper = (*scopedReader)(nil)

// TestUnmatchedCensus_SurvivesTheScopePushDown is the Bug 273 pin.
//
// Bug 273 was a v0.142.0 regression: on a Postgres source the new
// TABLE-FILTER-PATTERN-UNMATCHED warning fired on EVERY `--exclude-table`
// pattern, including ones that had correctly excluded their table. Under an
// exclude filter no pattern can ever match a survivor, so it was
// unconditional — the warning carried no information at all on the exact
// flag and engine Bug 272 was filed on.
//
// Why it graded HIGH rather than cosmetic: the remedy branch keys on the
// PATTERN'S SHAPE, not on what happened. An operator running a correct
// `--exclude-table=pii` was told their PII table was being copied and handed
// a remedy suggesting `public.pii` — which is precisely the input that
// copies the PII for real. The warning pointed from a working configuration
// at the broken one.
//
// The mechanism was an interaction, not a typo: the Bug-76 push-down
// (`ApplyTableScope` → `postgres.readTables`) drops a scoped-out table
// before the pipeline sees it. The comment at that push-down says "the
// post-read TableFilter remains the authoritative prune", which is TRUE for
// pruning and became insufficient the moment v0.142.0 asked that same
// post-read view a different question. A written invariant that stays true
// for its original purpose while a new caller quietly depends on more.
func TestUnmatchedCensus_SurvivesTheScopePushDown(t *testing.T) {
	run := func(t *testing.T, source []string, include, exclude []string, pushDown bool) (string, *ir.Schema) {
		t.Helper()
		f, err := NewTableFilter(include, exclude)
		if err != nil {
			t.Fatalf("NewTableFilter: %v", err)
		}
		r := &scopedReader{sourceTables: source}
		if pushDown {
			// The Postgres shape.
			ApplyTableScope(r, f)
		}
		schema := r.readSchema()

		var buf safeBuffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		if err := ApplyTableFilter(context.Background(), schema, f); err != nil {
			// An "excluded every table" error is a legitimate outcome for
			// some cells; the caller asserts on the log either way.
			buf.WriteString("\nERR: " + err.Error())
		}
		return buf.String(), schema
	}

	source := []string{"users", "orders", "pii"}

	t.Run("a WORKING exclusion does not warn, with the push-down active", func(t *testing.T) {
		out, schema := run(t, source, nil, []string{"pii"}, true)
		if strings.Contains(out, "TABLE-FILTER-PATTERN-UNMATCHED") {
			t.Errorf("a correct --exclude-table=pii warned that it matched nothing (Bug 273).\n"+
				"The push-down removed `pii` before ApplyTableFilter saw the schema, so the census "+
				"concluded the pattern was dead. Worse than a false alarm: the remedy offered is "+
				"`public.pii`, which is the input that actually copies the table.\ngot: %s", out)
		}
		if len(schema.Tables) != 2 {
			t.Errorf("expected 2 surviving tables, got %d — the harness is not modelling the push-down", len(schema.Tables))
		}
	})

	t.Run("a genuinely dead exclude pattern still warns, push-down active", func(t *testing.T) {
		// The other direction, without which the fix could be "never warn".
		out, _ := run(t, source, nil, []string{"public.pii"}, true)
		if !strings.Contains(out, "TABLE-FILTER-PATTERN-UNMATCHED") {
			t.Errorf("a schema-qualified exclude pattern matched nothing and did NOT warn — "+
				"the Bug 273 fix has silenced Bug 272's whole point.\ngot: %s", out)
		}
		if !strings.Contains(out, `write \"pii\"`) {
			t.Errorf("the bare-name remedy is missing from the surviving warn.\ngot: %s", out)
		}
	})

	t.Run("MySQL shape is unchanged: no push-down, same two verdicts", func(t *testing.T) {
		// The engine WITHOUT the push-down was correct throughout Bug 273
		// and must stay correct — this is the sibling that proved the
		// defect was an interaction rather than a logic error.
		if out, _ := run(t, source, nil, []string{"pii"}, false); strings.Contains(out, "TABLE-FILTER-PATTERN-UNMATCHED") {
			t.Errorf("no-push-down source warned on a working exclusion.\ngot: %s", out)
		}
		if out, _ := run(t, source, nil, []string{"public.pii"}, false); !strings.Contains(out, "TABLE-FILTER-PATTERN-UNMATCHED") {
			t.Errorf("no-push-down source did not warn on a dead pattern.\ngot: %s", out)
		}
	})

	t.Run("include mode, push-down active, both verdicts", func(t *testing.T) {
		if out, _ := run(t, source, []string{"users"}, nil, true); strings.Contains(out, "TABLE-FILTER-PATTERN-UNMATCHED") {
			t.Errorf("a working --include-table warned.\ngot: %s", out)
		}
		if out, _ := run(t, source, []string{"users", "public.orders"}, nil, true); !strings.Contains(out, "TABLE-FILTER-PATTERN-UNMATCHED") {
			t.Errorf("a dead --include-table pattern did not warn.\ngot: %s", out)
		}
	})

	t.Run("the census is per-run, not global", func(t *testing.T) {
		// Two filters built separately must not share a census; a global
		// would make a later run inherit an earlier run's table names and
		// silently suppress real findings.
		a, _ := NewTableFilter(nil, []string{"pii"})
		b, _ := NewTableFilter(nil, []string{"pii"})
		ra := &scopedReader{sourceTables: source}
		ApplyTableScope(ra, a)
		ra.readSchema()
		if got := len(b.census.seen()); got != 0 {
			t.Errorf("a second filter's census already holds %d name(s) — the census is shared across runs", got)
		}
	})

	t.Run("a copied filter still writes to one census", func(t *testing.T) {
		// The pointer field is load-bearing: TableFilter is passed by copy
		// at a dozen call sites, and ApplyTableScope receives a copy.
		f, _ := NewTableFilter(nil, []string{"pii"})
		cp := f
		r := &scopedReader{sourceTables: source}
		ApplyTableScope(r, cp)
		r.readSchema() // the predicate only records when the reader consults it
		if got := len(f.census.seen()); got != len(source) {
			t.Errorf("the original filter's census holds %d of %d names — a copy is writing to its own census, "+
				"which is the shape that would reintroduce Bug 273 at any call site that copies", got, len(source))
		}
	})
}

// fakeDefaultExcluder is an engine that contributes a default exclusion, the
// way PlanetScale contributes `_vt_*`.
type fakeDefaultExcluder struct{ ir.Engine }

func (fakeDefaultExcluder) DefaultExcludePatterns(string) []string { return []string{"_vt_*"} }

// TestEngineDefaultExclusionsAreNeverReportedUnmatched pins the invariant
// that UnmatchedPatterns' own doc comment used to merely ASSERT.
//
// EffectiveTableFilter merges an engine's default exclusions into Exclude,
// and the unmatched census reads Exclude — so on a PlanetScale database with
// no `_vt_*` shadow tables, which is the quiet healthy case, sluice warned on
// every run that a pattern the operator never typed had matched nothing. The
// comment said this could not happen. Nothing implemented that. Two audit
// workers found it independently on the same day.
func TestEngineDefaultExclusionsAreNeverReportedUnmatched(t *testing.T) {
	base, err := NewTableFilter(nil, []string{"pii"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	eff, added := EffectiveTableFilter(base, fakeDefaultExcluder{}, "dsn")
	if len(added) != 1 || added[0] != "_vt_*" {
		t.Fatalf("engine default not merged (added=%v) — this test would prove nothing", added)
	}

	// A source with no shadow tables AND a working operator exclusion.
	names := []string{"users", "orders", "pii"}
	got := eff.UnmatchedPatterns(names)
	for _, p := range got {
		if p == "_vt_*" {
			t.Errorf("the engine's own default `_vt_*` was reported unmatched — an operator who never "+
				"typed it is told their filter is broken, on every clean PlanetScale run. got=%v", got)
		}
	}

	// Anti-vacuity in the other direction: an operator pattern that really
	// does match nothing must still be reported through the merged filter,
	// or this fix has silenced the feature on every PlanetScale source.
	base2, _ := NewTableFilter(nil, []string{"public.pii"})
	eff2, _ := EffectiveTableFilter(base2, fakeDefaultExcluder{}, "dsn")
	var sawOperatorPattern bool
	for _, p := range eff2.UnmatchedPatterns(names) {
		if p == "public.pii" {
			sawOperatorPattern = true
		}
		if p == "_vt_*" {
			t.Errorf("engine default reported alongside the operator's: %v", eff2.UnmatchedPatterns(names))
		}
	}
	if !sawOperatorPattern {
		t.Error("a genuinely dead OPERATOR pattern went unreported through a merged filter — the " +
			"engine-default carve-out has swallowed the real finding")
	}
}

// TestFanOutReportsOnceAgainstTheWholeUniverse pins the fourth false-fire arm
// of Bug 273: a multi-database fan-out calls the filter door once per
// database, over that database's tables only, so a pattern naming a table in
// database A looked unmatched while database B was being processed — one
// false warning per pass.
//
// Reporting per pass cannot be made correct, because no single pass has the
// universe. So the fan-out form is quiet and the caller reports once. Both
// halves are asserted here, because either alone is a defect: quiet-with-no
// -final-report silently drops the Bug 272 protection for exactly the
// multi-database operators who most need it.
func TestFanOutReportsOnceAgainstTheWholeUniverse(t *testing.T) {
	// Two databases; `orders` lives only in the second.
	dbA := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	// dbB carries a second table on purpose: excluding the ONLY table in a
	// fan-out pass trips the door's "excluded every source table" refusal,
	// which is itself wrong per-pass (other databases still have tables) and
	// is filed separately. Keeping it out of this fixture isolates what this
	// test grades.
	dbB := &ir.Schema{Tables: []*ir.Table{{Name: "orders"}, {Name: "widgets"}}}

	f, err := NewTableFilter(nil, []string{"orders"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}

	capture := func(fn func()) string {
		var buf safeBuffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		fn()
		return buf.String()
	}

	perPass := capture(func() {
		if err := ApplyTableFilterQuiet(context.Background(), dbA, f); err != nil {
			t.Fatalf("pass A: %v", err)
		}
		if err := ApplyTableFilterQuiet(context.Background(), dbB, f); err != nil {
			t.Fatalf("pass B: %v", err)
		}
	})
	if strings.Contains(perPass, "TABLE-FILTER-PATTERN-UNMATCHED") {
		t.Errorf("a fan-out pass warned. Database A does not contain `orders`, but database B does — "+
			"no single pass has the universe, so a per-pass report is a false fire by construction.\ngot: %s", perPass)
	}

	final := capture(func() { ReportUnmatchedPatterns(context.Background(), f) })
	if strings.Contains(final, "TABLE-FILTER-PATTERN-UNMATCHED") {
		t.Errorf("the final report warned about `orders`, which database B DID contain — the accumulated "+
			"census is not accumulating across passes.\ngot: %s", final)
	}

	// The other direction, without which "quiet" could just mean "broken":
	// a pattern absent from EVERY database must still be reported once.
	f2, _ := NewTableFilter(nil, []string{"nosuch"})
	dbA2 := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	dbB2 := &ir.Schema{Tables: []*ir.Table{{Name: "orders"}, {Name: "widgets"}}}
	out := capture(func() {
		_ = ApplyTableFilterQuiet(context.Background(), dbA2, f2)
		_ = ApplyTableFilterQuiet(context.Background(), dbB2, f2)
		ReportUnmatchedPatterns(context.Background(), f2)
	})
	if n := strings.Count(out, "TABLE-FILTER-PATTERN-UNMATCHED"); n != 1 {
		t.Errorf("a genuinely dead pattern produced %d markers across a two-database fan-out; want exactly 1 "+
			"(0 = the protection is gone for multi-db runs, 2 = the per-pass false fire is back).\ngot: %s", n, out)
	}
}
