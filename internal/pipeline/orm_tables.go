// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ORM / framework migration-bookkeeping table recognition (ADR-0143).
//
// Tools like Flyway, Prisma, Rails, Django, … keep a small table that
// records which schema migrations have been applied to a database. When
// sluice copies a source that contains one of these tables, carrying it
// to the target is almost always wrong: the rows record migrations that
// ran against the SOURCE engine, so the ORM on the target side concludes
// those migrations already ran when the target's schema was in fact built
// by sluice. The chosen policy is loud-skip-by-default — recognize these
// tables and drop them from the migration, announcing each skip, with
// --include-orm-tables to keep them.
//
// Recognition splits into two classes:
//
//   - DISTINCTIVE names (e.g. `flyway_schema_history`, `_prisma_migrations`)
//     are framework-specific enough that the name alone is a safe signal —
//     a real application table is extraordinarily unlikely to be called
//     that. These match on name only.
//   - GENERIC names (`schema_migrations`, `migrations`, `migration`) collide
//     with plausible application tables, so each carries a COLUMN-SHAPE guard
//     and is recognized only when BOTH the name and the shape match. A table
//     whose name matches a generic rule but whose shape does not is kept
//     (it is application data) — never silently dropped — and the caller
//     emits a name-collision warning so the operator knows why.
//
// Matching is engine-neutral: the shape guards test the dialect-neutral IR
// type families (ir.Text / ir.Varchar / ir.Char, ir.Integer), never an
// engine-specific SQL type string, so the rules apply to ANY source engine.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// ormRemediation is the shared "why this is skipped / what to do instead"
// reason surfaced on every skip notice. The per-table notice also names the
// specific framework (rule.orm); the remediation is the same regardless of
// which framework it is.
const ormRemediation = "records migrations applied to the SOURCE engine; carrying that history to the target makes the ORM think target migrations already ran — re-baseline your ORM/framework on the target instead"

// ormRule recognizes one ORM/framework migration-bookkeeping table.
type ormRule struct {
	// orm is the human-facing framework name surfaced on the skip notice.
	orm string
	// remediation is the actionable "do this instead" reason for the notice.
	remediation string
	// nameMatch reports whether the already-lowercased table name identifies
	// this rule.
	nameMatch func(name string) bool
	// shapeMatch, when non-nil, is the additional column-shape guard a GENERIC
	// table name requires before it is recognized. nil means the name alone is
	// distinctive enough (the low-false-positive class).
	shapeMatch func(*ir.Table) bool
}

// generic reports whether this rule is a generic-name rule (one whose name
// collides with plausible application tables and therefore carries a
// column-shape guard).
func (r ormRule) generic() bool { return r.shapeMatch != nil }

// matches reports whether t is recognized by this rule — name (always) plus
// shape (for generic rules).
func (r ormRule) matches(t *ir.Table) bool {
	if !r.nameMatch(strings.ToLower(t.Name)) {
		return false
	}
	if r.shapeMatch == nil {
		return true
	}
	return r.shapeMatch(t)
}

// exactName builds a name matcher that accepts any of the given (lowercase)
// literals.
func exactName(names ...string) func(string) bool {
	return func(n string) bool {
		for _, name := range names {
			if n == name {
				return true
			}
		}
		return false
	}
}

// prefixName builds a name matcher that accepts any name beginning with the
// given (lowercase) prefix — used for Drizzle, whose table name varies
// (`__drizzle_migrations` and variants) but always starts `__drizzle`.
func prefixName(prefix string) func(string) bool {
	return func(n string) bool { return strings.HasPrefix(n, prefix) }
}

// isTextFamily reports whether t is a CHAR/VARCHAR/TEXT-family IR type — the
// family a migration-version / migration-name column lands in.
func isTextFamily(t ir.Type) bool {
	switch t.(type) {
	case ir.Text, ir.Varchar, ir.Char:
		return true
	default:
		return false
	}
}

// isIntegerFamily reports whether t is an integer IR type — the family a
// batch / apply_time / surrogate-id column lands in.
func isIntegerFamily(t ir.Type) bool {
	_, ok := t.(ir.Integer)
	return ok
}

// shapeRailsSchemaMigrations is the guard for the `schema_migrations` name
// (Rails / Ecto / golang-migrate / dbmate): exactly ONE column named
// `version` of a text family. (Ported from pscale's
// looksLikeRailsSchemaMigrations.)
func shapeRailsSchemaMigrations(t *ir.Table) bool {
	if len(t.Columns) != 1 {
		return false
	}
	c := t.Columns[0]
	return strings.EqualFold(c.Name, "version") && isTextFamily(c.Type)
}

// shapeLaravelMigrations is the guard for the `migrations` name (Laravel /
// gormigrate): the column set is exactly {id integer, migration text, batch
// integer}, order-independent and case-insensitive. `migration` (text) and
// `batch` (integer) are required; `id` (integer) is the optional surrogate;
// any OTHER column disqualifies the table (it is application data). Kept
// tight on purpose — Laravel's table is exactly those three.
func shapeLaravelMigrations(t *ir.Table) bool {
	var hasMigration, hasBatch bool
	for _, c := range t.Columns {
		switch strings.ToLower(c.Name) {
		case "migration":
			if !isTextFamily(c.Type) {
				return false
			}
			hasMigration = true
		case "batch":
			if !isIntegerFamily(c.Type) {
				return false
			}
			hasBatch = true
		case "id":
			if !isIntegerFamily(c.Type) {
				return false
			}
		default:
			return false
		}
	}
	return hasMigration && hasBatch
}

// shapeYiiMigration is the guard for the `migration` name (Yii): the column
// set is exactly {version text, apply_time integer}, order-independent and
// case-insensitive. Both are required; any other column disqualifies it.
func shapeYiiMigration(t *ir.Table) bool {
	var hasVersion, hasApplyTime bool
	for _, c := range t.Columns {
		switch strings.ToLower(c.Name) {
		case "version":
			if !isTextFamily(c.Type) {
				return false
			}
			hasVersion = true
		case "apply_time":
			if !isIntegerFamily(c.Type) {
				return false
			}
			hasApplyTime = true
		default:
			return false
		}
	}
	return hasVersion && hasApplyTime
}

// ormRules is the recognition table. DISTINCTIVE-name rules (name-only) come
// first, GENERIC-name rules (name + shape guard) last; recognizeORMTable
// returns the first match. The two classes never overlap — distinctive names
// are exact/prefix literals that no generic name equals.
var ormRules = []ormRule{
	// --- Distinctive names (name-only, low false-positive) ---
	{orm: "Drizzle", remediation: ormRemediation, nameMatch: prefixName("__drizzle")},
	{orm: "Prisma", remediation: ormRemediation, nameMatch: exactName("_prisma_migrations")},
	{orm: "Knex", remediation: ormRemediation, nameMatch: exactName("knex_migrations", "knex_migrations_lock")},
	{orm: "Sequelize", remediation: ormRemediation, nameMatch: exactName("sequelizemeta")},
	{orm: "Rails ActiveRecord", remediation: ormRemediation, nameMatch: exactName("ar_internal_metadata")},
	{orm: "Flyway", remediation: ormRemediation, nameMatch: exactName("flyway_schema_history")},
	{orm: "Liquibase", remediation: ormRemediation, nameMatch: exactName("databasechangelog", "databasechangeloglock")},
	{orm: "Django", remediation: ormRemediation, nameMatch: exactName("django_migrations")},
	{orm: "Alembic", remediation: ormRemediation, nameMatch: exactName("alembic_version")},
	{orm: "TypeORM", remediation: ormRemediation, nameMatch: exactName("typeorm_metadata")},
	{orm: "Goose", remediation: ormRemediation, nameMatch: exactName("goose_db_version")},
	{orm: "EF Core", remediation: ormRemediation, nameMatch: exactName("__efmigrationshistory")},
	{orm: "Doctrine/Symfony", remediation: ormRemediation, nameMatch: exactName("doctrine_migration_versions")},
	{orm: "Phinx/CakePHP", remediation: ormRemediation, nameMatch: exactName("phinxlog")},
	{orm: "sqlx", remediation: ormRemediation, nameMatch: exactName("_sqlx_migrations")},
	{orm: "Diesel", remediation: ormRemediation, nameMatch: exactName("__diesel_schema_migrations")},
	{orm: "SeaORM", remediation: ormRemediation, nameMatch: exactName("seaql_migrations")},
	{orm: "MikroORM", remediation: ormRemediation, nameMatch: exactName("mikro_orm_migrations")},
	{orm: "node-pg-migrate", remediation: ormRemediation, nameMatch: exactName("pgmigrations")},
	{orm: "Atlas", remediation: ormRemediation, nameMatch: exactName("atlas_schema_revisions")},
	{orm: "Aerich", remediation: ormRemediation, nameMatch: exactName("aerich")},
	{orm: "Fluent", remediation: ormRemediation, nameMatch: exactName("_fluent_migrations")},

	// --- Generic names (name + column-shape guard) ---
	{orm: "Rails/Ecto/golang-migrate/dbmate", remediation: ormRemediation, nameMatch: exactName("schema_migrations"), shapeMatch: shapeRailsSchemaMigrations},
	{orm: "Laravel/gormigrate", remediation: ormRemediation, nameMatch: exactName("migrations"), shapeMatch: shapeLaravelMigrations},
	{orm: "Yii", remediation: ormRemediation, nameMatch: exactName("migration"), shapeMatch: shapeYiiMigration},
}

// recognizeORMTable reports whether t is a recognized ORM/framework
// migration-bookkeeping table, returning the matching rule (for the skip
// notice's orm name + remediation). For a generic-name rule this is true ONLY
// when the column shape also matches.
func recognizeORMTable(t *ir.Table) (ormRule, bool) {
	for _, r := range ormRules {
		if r.matches(t) {
			return r, true
		}
	}
	return ormRule{}, false
}

// ormNameCollision reports a table whose NAME matches a GENERIC ORM
// migration-table rule but whose column SHAPE does not — i.e. a real
// application table that happens to share the bookkeeping name. The caller
// keeps such a table (it is data, not bookkeeping) and warns about the clash.
func ormNameCollision(t *ir.Table) (orm string, collided bool) {
	lower := strings.ToLower(t.Name)
	for _, r := range ormRules {
		if !r.generic() {
			continue
		}
		if r.nameMatch(lower) && !r.shapeMatch(t) {
			return r.orm, true
		}
	}
	return "", false
}

// explicitlyIncluded reports whether the operator named tableName as an exact
// literal in --include-table. An explicit include always wins over ORM-skip:
// the operator named the table, so sluice keeps it. A glob include that
// merely happens to match does NOT count — naming is exact.
func explicitlyIncluded(tableName string, filter TableFilter) bool {
	for _, p := range filter.Include {
		if strings.EqualFold(p, tableName) {
			return true
		}
	}
	return false
}

// applyORMTableSkip removes recognized ORM/framework migration-bookkeeping
// tables from schema.Tables in place, announcing each skip LOUDLY (ADR-0143).
//
// It is a no-op when skip is false — the zero-value-safe default for every
// PROGRAMMATIC / broker / test / internal Migrator/Streamer construction
// (those must never suddenly start dropping tables); only the CLI defaults it
// on. Behaviour with skip=false is byte-identical to before this feature.
//
// A table the operator named explicitly via --include-table is never skipped.
// A table whose NAME matches a GENERIC rule but whose COLUMN SHAPE does not is
// KEPT (it is application data) with a one-time name-collision warning, so a
// real application table is never silently dropped — the loud-failure /
// no-silent-loss discipline applied to a false-positive skip.
func applyORMTableSkip(ctx context.Context, schema *ir.Schema, skip bool, filter TableFilter) {
	if !skip {
		return
	}
	kept := schema.Tables[:0]
	for _, t := range schema.Tables {
		if rule, ok := recognizeORMTable(t); ok && !explicitlyIncluded(t.Name, filter) {
			slog.WarnContext(
				ctx, "migration: skipping ORM migration-history table — pass --include-orm-tables to keep it",
				slog.String("table", t.Name),
				slog.String("orm", rule.orm),
				slog.String("reason", rule.remediation),
			)
			continue
		}
		if orm, collided := ormNameCollision(t); collided {
			slog.WarnContext(ctx, fmt.Sprintf(
				"migration: table %q matches the %s migration-table name but its columns don't — treating as application data (NOT skipped); rename it if it is migration bookkeeping you want excluded",
				t.Name, orm,
			))
		}
		kept = append(kept, t)
	}
	schema.Tables = kept
}
