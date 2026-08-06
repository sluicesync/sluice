// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/engines/internal/namecollide"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// validateSQLiteIndexNamespace refuses a schema in which two source INDEXES
// resolve to one SQLite index identifier (roadmap item 134 — the sibling of
// the Postgres check at [postgres.validatePGIndexNamespace], item 120, on the
// engine that had none).
//
// # Why this is a silent-loss refusal
//
// SQLite index names are SCHEMA-scoped, not table-scoped. MySQL's are
// table-scoped and MySQL auto-names a single-column index after its column, so
// `CREATE INDEX user_id` on both `posts` and `comments` is routine rather than
// exotic — and unlike Postgres, sluice applies NO disambiguating transform
// here, so the two names arrive at the target identical. [emitCreateIndex]
// emits `CREATE INDEX IF NOT EXISTS` (which keeps the index phase idempotent
// under `--resume`), so the second build is a SILENT no-op: the target keeps
// whichever index sorted first and permanently loses the other. When the loser
// is the UNIQUE one, the target accepts duplicate rows the source rejects,
// forever, at exit 0.
//
// Ground truth (modernc.org/sqlite, the driver this engine uses):
//
//	CREATE INDEX IF NOT EXISTS "idx_v" ON "a" ("v")         -> ok
//	CREATE UNIQUE INDEX IF NOT EXISTS "idx_v" ON "b" ("v")  -> ok, NO-OP
//	CREATE UNIQUE INDEX IF NOT EXISTS "IDX_V" ON "b" ("v")  -> ok, NO-OP
//	sqlite_schema afterwards: one index `idx_v`, on table `a`
//
// The third line is why the comparison folds ASCII case: SQLite compares
// identifiers case-insensitively for ASCII even inside double quotes, so two
// source indexes differing only in case collide too.
//
// # WHAT THIS GATE REACHES, stated rather than implied
//
// It reaches INDEX-vs-INDEX only. SQLite's object namespace is flatter than
// that — tables, views and indexes share it — and the other pairs behave
// differently, ground-truthed on the same driver:
//
//   - index vs an existing TABLE or VIEW name: `CREATE INDEX IF NOT EXISTS "a"`
//     against a table `a` is a LOUD error ("there is already a table named a"),
//     with or without IF NOT EXISTS. Not silent, so not this gate's class; it
//     surfaces at the index phase.
//   - VIEW vs an existing table or view name: `CREATE VIEW IF NOT EXISTS "a"`
//     against a table `a` returns OK and creates NOTHING — a genuine silent
//     drop, of the same shape and on the same engine. It is NOT closed here:
//     the object lost is a view rather than an index, which is a different
//     operator-facing refusal and a different error code. Filed rather than
//     folded in, so this gate's name stays no broader than what it checks.
//
// It reaches no PRIMARY KEY either, and that one is genuinely vacuous rather
// than deferred: [emitTableDef] renders a PK as an inline `PRIMARY KEY (...)`
// clause and SQLite auto-names its backing index `sqlite_autoindex_<table>_N`,
// so a source PK name never enters this namespace at all — unlike Postgres,
// where a source-named PK constraint takes a pg_class row.
//
// # Why refuse rather than rename
//
// Same adjudication as the Postgres sibling: a silently renamed index is its
// own surprise (application code and schema diffs name indexes), and the
// operator genuinely needs to know their source carries two indexes sluice
// cannot tell apart on the target.
//
// Callers pass the WHOLE table set where they have it, because the namespace is
// database-wide and a collision across two tables is the common shape.
func validateSQLiteIndexNamespace(tables []*ir.Table) error {
	type origin struct{ table, index string }
	// ASCII fold, per the ground truth above. strings.ToLower is Unicode-aware
	// where SQLite's built-in comparison is ASCII-only, so this folds a
	// SUPERSET of what SQLite folds: the residual is over-refusal of a pair
	// differing only in non-ASCII case, which is loud and rare, versus the
	// under-refusal it would otherwise risk, which is the silent no-op this
	// whole function exists to stop.
	seen := namecollide.New[origin](strings.ToLower)
	for _, table := range tables {
		if table == nil {
			continue
		}
		for _, idx := range table.Indexes {
			if idx == nil || idx.Name == "" {
				// An index with no name never reaches emitCreateIndex — it
				// refuses one outright — so there is nothing to collide.
				continue
			}
			prior, dup := seen.Claim(idx.Name, origin{table: table.Name, index: idx.Name})
			if !dup {
				continue
			}
			return sluicecode.Wrap(
				sluicecode.CodeSchemaIndexNameCollision,
				"rename one of the two source indexes so they no longer resolve to the same SQLite name, "+
					"then re-run",
				fmt.Errorf(
					"sqlite: index-name collision: source index %q on table %q and source index %q on table %q "+
						"both resolve to the SQLite identifier %q; SQLite index names are schema-scoped (and "+
						"compared case-insensitively) and sluice emits CREATE INDEX IF NOT EXISTS, so the second "+
						"build would SILENTLY no-op and that index would be missing on the target (if it is the "+
						"UNIQUE one, the target would accept duplicate rows the source rejects) — rename one of "+
						"the two source indexes",
					prior.index, prior.table, idx.Name, table.Name, idx.Name,
				),
			)
		}
	}
	return nil
}
