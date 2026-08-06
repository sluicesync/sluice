// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"sort"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"

	// Blank-imported for their init() self-registration — the same set
	// index_preflight_roster_test.go links, and the same reason.
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// WHICH TARGETS REFUSE A COLLIDING INDEX NAMESPACE, kept honest against the
// registry (roadmap items 120 and 134).
//
// # Why this gate rather than each engine's own test
//
// Item 120 closed the collision on Postgres. Item 134 found the identical
// silent no-op sitting open on SQLite for the whole time since — filed a day
// late, by a reconciler, because a commit message had enumerated the open
// findings and been wrong by one. Two engine-local tests would have been green
// throughout: Postgres's proved Postgres refuses, SQLite had none, and nothing
// anywhere asked the question of the SET.
//
// So this is the sibling-sweep step in mechanical form. It is BEHAVIOURAL
// rather than a type-assert: there is no `ir.IndexNamespaceChecker` interface
// to pin, and inventing one would prove only that a method exists. It builds a
// real collision, hands it to the engine's own [ir.IndexEmitPreflighter], and
// requires a refusal carrying [sluicecode.CodeSchemaIndexNameCollision].
//
// # Why the fixture is per-engine and not shared
//
// The COLLISION SHAPE is a property of the engine's naming rule, and a single
// fixture would have quietly checked only one engine. Postgres prepends
// `<table>_` to a source index name that is not already table-scoped, so two
// tables' identically named indexes DO NOT collide there — its collision is the
// transform's non-injectivity, within one table. SQLite applies no transform, so
// its collision is exactly the pair Postgres disambiguates. A shared "two
// tables, one index name" fixture passes on Postgres for a correct reason,
// which would have read as coverage.
//
// The roster is fail-by-default on the fixture too: a non-exempt engine with no
// entry in [indexNamespaceCollision] fails here, so a new schema-scoped target
// cannot join the registry without someone stating its collision shape.
//
// # Scope, stated so it cannot be read as broader
//
// It proves each target-capable engine REFUSES ONE real collision through the
// preflight surface. It does not prove the engine's notion of "collides" is
// complete — SQLite additionally folds ASCII case, Postgres additionally walks
// source-named primary keys and the naming-convention prefixes; those are
// pinned by each engine's own tests. Nor does it reach the OTHER silent
// `IF NOT EXISTS` no-op found alongside item 134: a SQLite
// `CREATE VIEW IF NOT EXISTS` whose name matches an existing table or view
// returns OK and creates nothing. That one loses a view rather than an index,
// carries no error code yet, and is filed rather than folded in here.
var indexNamespaceExempt = map[string]string{
	// Source-only engines: no index DDL is ever emitted for them as a target,
	// so there is no target namespace to collide in. Same set and same reasons
	// as indexPreflightExempt.
	"csv":            "source only — OpenSchemaWriter returns ErrNotImplemented",
	"tsv":            "source only — the same flatfile engine as csv",
	"ndjson":         "source only — the same flatfile engine as csv",
	"mydumper":       "source only — a dump directory is read, never written",
	"d1":             "migrate/sync SOURCE only; a D1 TARGET goes through the `sqlite` engine, which is checked",
	"sqlite-trigger": "CDC source only (ADR-0134) — a SQLite target uses the `sqlite` engine",
	"d1-trigger":     "CDC source only — OpenSchemaWriter returns ErrNotImplemented",

	// The MySQL family is exempt for a reason about MySQL, not about sluice:
	// index names there are TABLE-scoped, so `CREATE INDEX user_id` on two
	// different tables creates two distinct objects and the collision this
	// gate names cannot occur. That is exactly why sluice has to PREFIX names
	// on a Postgres target — the transform whose non-injectivity is item 120.
	"mysql":       "MySQL index names are TABLE-scoped: two tables' identically named indexes are distinct objects, so there is no namespace to collide in",
	"planetscale": "same engine as mysql — index names are table-scoped",
	"vitess":      "same engine as mysql — index names are table-scoped",
	"mariadb":     "same engine as mysql — index names are table-scoped",
}

// indexNamespaceCollision holds each non-exempt target's OWN collision shape.
// Every entry is a schema whose indexes land on one target identifier — which
// pair does that is decided by the engine's naming rule, not by this file.
var indexNamespaceCollision = map[string]func() *ir.Schema{
	// Postgres and its trigger-CDC wrapper (which delegates the whole schema
	// write to postgres.Engine). The collision is the table-scoping transform's
	// non-injectivity, WITHIN one table: `pgIndexName("posts","user_id")` and
	// `pgIndexName("posts","posts_user_id")` both render `posts_user_id`, and
	// MySQL auto-naming a single-column index after its column is what makes
	// the pair routine.
	"postgres":         pgIndexNamespaceCollision,
	"postgres-trigger": pgIndexNamespaceCollision,

	// SQLite applies no transform at all, so the collision is the pair
	// Postgres disambiguates: two tables, one index name. The second is the
	// UNIQUE one — the loss whose consequence is duplicate rows on the target.
	"sqlite": sqliteIndexNamespaceCollision,
}

func pgIndexNamespaceCollision() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "app", Name: "posts",
		Columns: []*ir.Column{{Name: "user_id", Type: ir.Integer{Width: 64}}},
		Indexes: []*ir.Index{
			{Name: "user_id", Columns: []ir.IndexColumn{{Column: "user_id"}}},
			{Name: "posts_user_id", Unique: true, Columns: []ir.IndexColumn{{Column: "user_id"}}},
		},
	}}}
}

func sqliteIndexNamespaceCollision() *ir.Schema {
	col := func() []*ir.Column { return []*ir.Column{{Name: "user_id", Type: ir.Integer{Width: 64}}} }
	return &ir.Schema{Tables: []*ir.Table{
		{
			Schema: "app", Name: "posts", Columns: col(),
			Indexes: []*ir.Index{{Name: "user_id", Columns: []ir.IndexColumn{{Column: "user_id"}}}},
		},
		{
			Schema: "app", Name: "comments", Columns: col(),
			Indexes: []*ir.Index{{
				Name: "user_id", Unique: true,
				Columns: []ir.IndexColumn{{Column: "user_id"}},
			}},
		},
	}}
}

func TestEveryTargetRefusesACollidingIndexNamespace(t *testing.T) {
	names := engines.Names()

	// Anti-vacuity floor, same shape as the preflight roster's: a truncated
	// registry would pass every assertion below.
	if len(names) < 8 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list has drifted from cmd/sluice and "+
			"this gate is checking a subset of the fleet", len(names), names)
	}

	var checked, exempted []string
	for _, name := range names {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() reported %q but engines.Get did not return it", name)
		}
		reason, isExempt := indexNamespaceExempt[name]
		if isExempt {
			if reason == "" {
				t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is "+
					"indistinguishable from an oversight", name)
			}
			exempted = append(exempted, name)
			continue
		}
		pf, isPreflighter := e.(ir.IndexEmitPreflighter)
		if !isPreflighter {
			t.Errorf("engine %q is not exempt and does not implement ir.IndexEmitPreflighter, so nothing "+
				"asks it about a colliding index namespace before the copy", name)
			continue
		}
		fixture, hasFixture := indexNamespaceCollision[name]
		if !hasFixture {
			t.Errorf("engine %q is a non-exempt target with no entry in indexNamespaceCollision, so this "+
				"roster is not actually checking it. State its collision shape (the pair of source indexes "+
				"that land on ONE identifier under its naming rule) or exempt it with the reason its names "+
				"cannot collide.", name)
			continue
		}

		err := pf.PreflightIndexes(fixture())
		if err == nil {
			t.Errorf("engine %q ACCEPTED its own collision fixture. Its index names are schema-scoped and it "+
				"emits CREATE INDEX IF NOT EXISTS, so the second build is a SILENT no-op and the target "+
				"permanently loses that index — when the loser is the UNIQUE one, it accepts duplicate rows "+
				"the source rejects, at exit 0 (roadmap items 120/134).", name)
			continue
		}
		ce, coded := sluicecode.FromError(err)
		if !coded || ce.Code != sluicecode.CodeSchemaIndexNameCollision {
			t.Errorf("engine %q refused the colliding namespace, but not with %s: %v\n"+
				"Operators route on the code, and the docs give one remedy for it — a refusal that arrives "+
				"under some other code (or none) reads as an unrelated failure.",
				name, sluicecode.CodeSchemaIndexNameCollision, err)
			continue
		}
		checked = append(checked, name)

		// The MUTATION control, in the other direction: the SAME fixture with
		// the second index renamed out of the way must pass. Without it, an
		// engine whose preflight refused every two-index schema would satisfy
		// the assertion above.
		clean := fixture()
		last := clean.Tables[len(clean.Tables)-1]
		last.Indexes[len(last.Indexes)-1].Name = "sluice_roster_distinct_name_idx"
		if err := pf.PreflightIndexes(clean); err != nil {
			t.Errorf("engine %q refused the same schema with the colliding index RENAMED (%v); the refusal "+
				"above is then not evidence of a namespace check", name, err)
		}
	}

	for name := range indexNamespaceExempt {
		if _, ok := engines.Get(name); !ok {
			t.Errorf("indexNamespaceExempt names %q, which is not a registered engine — the exemption is "+
				"stale; drop it", name)
		}
	}
	// The anti-vacuity floor, both halves: an exemption map that grew to cover
	// everything would silence this gate entirely, and a checked set smaller
	// than the schema-scoped targets means one of them stopped being asked.
	if len(exempted) >= len(names) {
		t.Fatalf("every registered engine (%d of %d) is exempt from the index-namespace check; the roster "+
			"has stopped requiring anything", len(exempted), len(names))
	}
	sort.Strings(checked)
	if len(checked) < 3 {
		t.Errorf("only %d engines refuse a colliding index namespace (%v). postgres, postgres-trigger and "+
			"sqlite are all schema-scoped targets and all must.", len(checked), checked)
	}
}
