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
	// index_namespace_roster_test.go links, and the same reason.
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// WHICH TARGETS REFUSE A VIEW WHOSE NAME IS ALREADY TAKEN, kept honest against
// the registry (roadmap item 147).
//
// # Why this gate
//
// The index sibling next door (index_namespace_roster_test.go) says in its own
// scope note that it does NOT reach this class, and that the class "carries no
// error code yet". Both halves are now false, which is the point: item 147 was
// found while ground-truthing item 134, filed as an explicit out-of-scope note
// rather than a guess, and closed here. This is the enumeration that keeps the
// next engine from re-opening it.
//
// # What it reaches, stated so the name cannot be read as broader
//
//   - It asks each registered engine, through the connection-free
//     [ir.ViewEmitPreflighter] surface, to refuse ONE real collision: a view
//     whose name a TABLE in the same schema already holds. It does not prove an
//     engine's notion of "collides" is complete (SQLite additionally folds ASCII
//     case, and additionally refuses view-vs-view; those are pinned by the
//     engine's own tests).
//   - The exemptions below are CLAIMS ABOUT THE ENGINE, not omissions, and each
//     names the statement that makes the collision loud there. Note what the
//     Postgres/MySQL exemption does NOT cover, said here rather than left
//     implied: `CREATE OR REPLACE VIEW` silently REPLACES an existing VIEW, so
//     two source views resolving to one target name lose one there too — on
//     Postgres reachable through NAMEDATALEN truncation, which
//     `validatePGIdentifier` does not currently walk for views. That is the
//     SLUICE-E-SCHEMA-IDENTIFIER-TOO-LONG class on an object kind it does not
//     yet reach, a different item, and it is out of scope here.
var viewNamespaceExempt = map[string]string{
	// Source-only engines: no view DDL is ever emitted for them as a target,
	// so there is no target namespace to collide in. Same set and same reasons
	// as indexNamespaceExempt.
	"csv":            "source only — OpenSchemaWriter returns ErrNotImplemented",
	"tsv":            "source only — the same flatfile engine as csv",
	"ndjson":         "source only — the same flatfile engine as csv",
	"mydumper":       "source only — a dump directory is read, never written",
	"d1":             "migrate/sync SOURCE only; a D1 TARGET goes through the `sqlite` engine, which is checked",
	"sqlite-trigger": "CDC source only (ADR-0134) — a SQLite target uses the `sqlite` engine",
	"d1-trigger":     "CDC source only — OpenSchemaWriter returns ErrNotImplemented",

	// The engines whose view emit cannot lose a view to a TABLE of the same
	// name, because the server refuses it. Both emit `CREATE OR REPLACE VIEW`,
	// which has no IF-NOT-EXISTS semantics at all: there is no no-op branch to
	// fall into.
	"postgres":         "emits `CREATE OR REPLACE VIEW`; PostgreSQL raises 42P07 / `… is not a view` when the name is held by a base table — loud, so there is no silent no-op to preflight",
	"postgres-trigger": "delegates its whole schema write to postgres.Engine — same `CREATE OR REPLACE VIEW`, same loud refusal",
	"mysql":            "emits `CREATE OR REPLACE VIEW`; MySQL raises ER_WRONG_OBJECT (1347) when the name is held by a base table — loud",
	"planetscale":      "same engine as mysql — same statement, same loud refusal",
	"vitess":           "same engine as mysql — same statement, same loud refusal",
	"mariadb":          "same engine as mysql — same statement, same loud refusal",
}

// viewNamespaceCollision holds each non-exempt target's OWN collision shape.
// SQLite's is the plain one: a source table and a source view carrying one
// name, in a namespace that holds both.
var viewNamespaceCollision = map[string]func() *ir.Schema{
	"sqlite": sqliteViewNamespaceCollision,
}

func sqliteViewNamespaceCollision() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{{
			Schema: "app", Name: "orders",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}},
		Views: []*ir.View{{
			Schema: "app", Name: "orders",
			Definition: "SELECT id FROM orders",
		}},
	}
}

func TestEveryTargetRefusesACollidingViewNamespace(t *testing.T) {
	names := engines.Names()

	// Anti-vacuity floor, same shape as the index roster's.
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
		reason, isExempt := viewNamespaceExempt[name]
		if isExempt {
			if reason == "" {
				t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is "+
					"indistinguishable from an oversight", name)
			}
			exempted = append(exempted, name)
			continue
		}
		pf, isPreflighter := e.(ir.ViewEmitPreflighter)
		if !isPreflighter {
			t.Errorf("engine %q is not exempt and does not implement ir.ViewEmitPreflighter, so nothing "+
				"asks it whether a declared view can actually be created before the copy runs", name)
			continue
		}
		fixture, hasFixture := viewNamespaceCollision[name]
		if !hasFixture {
			t.Errorf("engine %q is a non-exempt target with no entry in viewNamespaceCollision, so this "+
				"roster is not actually checking it. State its collision shape, or exempt it with the "+
				"statement its view emit uses and the error the server raises instead.", name)
			continue
		}

		err := pf.PreflightViews(fixture())
		if err == nil {
			t.Errorf("engine %q ACCEPTED its own view-collision fixture. Its view emit carries IF NOT "+
				"EXISTS into a namespace it shares with tables, so the CREATE returns OK and creates "+
				"NOTHING — the view the source declared is absent on the target at exit 0 (item 147).", name)
			continue
		}
		ce, coded := sluicecode.FromError(err)
		if !coded || ce.Code != sluicecode.CodeSchemaViewNameCollision {
			t.Errorf("engine %q refused the colliding view, but not with %s: %v\n"+
				"Operators route on the code. In particular this must NOT arrive as %s — a view colliding "+
				"with a table is not an index-namespace problem and its remedy renames a different object.",
				name, sluicecode.CodeSchemaViewNameCollision, err, sluicecode.CodeSchemaIndexNameCollision)
			continue
		}
		checked = append(checked, name)

		// The MUTATION control, in the other direction: the SAME fixture with
		// the view renamed out of the way must pass. Without it, an engine
		// whose preflight refused every schema carrying a view would satisfy
		// the assertion above.
		clean := fixture()
		clean.Views[len(clean.Views)-1].Name = "sluice_roster_distinct_view_name"
		if err := pf.PreflightViews(clean); err != nil {
			t.Errorf("engine %q refused the same schema with the colliding view RENAMED (%v); the refusal "+
				"above is then not evidence of a namespace check", name, err)
		}
	}

	for name := range viewNamespaceExempt {
		if _, ok := engines.Get(name); !ok {
			t.Errorf("viewNamespaceExempt names %q, which is not a registered engine — the exemption is "+
				"stale; drop it", name)
		}
	}
	if len(exempted) >= len(names) {
		t.Fatalf("every registered engine (%d of %d) is exempt from the view-namespace check; the roster "+
			"has stopped requiring anything", len(exempted), len(names))
	}
	sort.Strings(checked)
	if len(checked) < 1 {
		t.Errorf("no engine refuses a colliding view namespace (%v). sqlite is the one target whose view "+
			"emit can lose a view silently, and it must.", checked)
	}
}
