// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// ControlTableDDLRequiredMarker is the grep-stable marker on the error a
// control-table ensure returns when the table, a column or an index is
// genuinely missing and this role cannot add it (GC-40 (a)).
const ControlTableDDLRequiredMarker = "CONTROL-TABLE-DDL-REQUIRED"

// # Detect first, then DDL (GC-40 (a), Bug 291)
//
// Every ensure used to run CREATE TABLE IF NOT EXISTS and a series of
// ALTER TABLE … ADD COLUMN IF NOT EXISTS on every start. PostgreSQL checks
// CREATE on the schema BEFORE it evaluates IF NOT EXISTS, and ALTER TABLE
// needs table ownership even when the column is already there — so every
// sync role had to own the control tables and hold CREATE on their schema,
// and a DML-only role stopped at startup with "permission denied for
// schema" / "must be owner of table" on a target whose tables were all
// present. [controlTable.ensure] probes the catalog and runs only the DDL
// that is actually missing, the shape ensureApplyMarksTable and
// ensureUnforwardedRefusalColumn already had: on a current target it
// issues no DDL at all, and when DDL is genuinely needed and refused, the
// error names the missing object and the statement to run as the owner.

// controlTable is one sluice-owned table on a Postgres target as its
// ensure path guarantees it: the CREATE a fresh target gets, then every
// column and index a later release added, each applied only when missing.
// The same value renders the bootstrap statements `sluice control-tables
// ddl --engine postgres` prints ([Engine.ControlTableDDL]), so the printed
// set and what the ensure executes cannot drift apart.
type controlTable struct {
	name    string // unqualified, as the catalog and the operator see it
	schema  string
	create  string
	columns []controlColumn
	indexes []controlIndex

	// utcDefaults names the naive TIMESTAMP columns whose writes RELY on the
	// column DEFAULT for their value. Each must default to [utcNowSQL]: a
	// table an older binary created carries DEFAULT CURRENT_TIMESTAMP, which
	// stores the SESSION zone's wall-clock digits — hours in the future on
	// an east-of-UTC database, hours in the past west of it (GC-40 LOW-1/2).
	// A table whose writes name every timestamp explicitly lists nothing
	// here: its legacy default is never evaluated.
	utcDefaults []string
}

// controlColumn is one column added after its table first shipped. def is
// the ADD COLUMN definition ("slot_name TEXT NULL").
type controlColumn struct {
	name, def string
}

// controlIndex is one index on a control table; ddl is its full CREATE
// INDEX IF NOT EXISTS statement.
type controlIndex struct {
	name, ddl string
}

func (t controlTable) ref() string { return quoteIdent(t.schema) + "." + quoteIdent(t.name) }

func (t controlTable) addColumnDDL(c controlColumn) string {
	return "ALTER TABLE " + t.ref() + " ADD COLUMN IF NOT EXISTS " + c.def
}

func (t controlTable) utcDefaultDDL(column string) string {
	return "ALTER TABLE " + t.ref() + " ALTER COLUMN " + quoteIdent(column) + " SET DEFAULT (" + utcNowSQL + ")"
}

// statements is the table's full current shape as idempotent DDL, in the
// order a fresh target needs it: missing columns, indexes and the UTC
// defaults the writes rely on. Applying it to an older-shape table upgrades
// it; applying it twice is a no-op.
func (t controlTable) statements() []string {
	out := make([]string, 0, 1+len(t.columns)+len(t.indexes)+len(t.utcDefaults))
	out = append(out, t.create)
	for _, c := range t.columns {
		out = append(out, t.addColumnDDL(c))
	}
	for _, ix := range t.indexes {
		out = append(out, ix.ddl)
	}
	for _, c := range t.utcDefaults {
		out = append(out, t.utcDefaultDDL(c))
	}
	return out
}

// ensure brings the table to its current shape, issuing only the DDL whose
// object is missing. A current target sees catalog reads and nothing else,
// which is what lets a role holding only SELECT/INSERT/UPDATE/DELETE on the
// tables run.
func (t controlTable) ensure(ctx context.Context, db *sql.DB) error {
	present, err := relationPresent(ctx, db, t.ref())
	if err != nil {
		return t.detectError("table", err)
	}
	if !present {
		if _, err := db.ExecContext(ctx, t.create); err != nil {
			return t.ddlRequired("does not exist", t.create, err)
		}
	}
	if err := t.ensureColumns(ctx, db, t.columns); err != nil {
		return err
	}
	for _, ix := range t.indexes {
		present, err := relationPresent(ctx, db, quoteIdent(t.schema)+"."+quoteIdent(ix.name))
		if err != nil {
			return t.detectError("index "+ix.name, err)
		}
		if present {
			continue
		}
		if _, err := db.ExecContext(ctx, ix.ddl); err != nil {
			return t.ddlRequired("lacks index "+ix.name, ix.ddl, err)
		}
	}
	return t.ensureUTCDefaults(ctx, db)
}

// ensureUTCDefaults re-points each [controlTable.utcDefaults] column whose
// DEFAULT is not the UTC expression (a table created before v0.99.263
// carries CURRENT_TIMESTAMP) at [utcNowSQL].
//
// A role that may not ALTER the table is refused only when the legacy
// default would actually store the wrong digits — a session whose TimeZone
// is not UTC all year — because every write to the column would then land
// hours off, and the readers (the backfill concurrent-run guard, cold-start
// ages) would act on it. On a UTC session the legacy default is harmless,
// so the role gets a WARN naming the statement and runs.
func (t controlTable) ensureUTCDefaults(ctx context.Context, db *sql.DB) error {
	if len(t.utcDefaults) == 0 {
		return nil
	}
	stale, err := nonUTCDefaults(ctx, db, t.ref(), t.utcDefaults)
	if err != nil {
		return t.detectError("column defaults", err)
	}
	for _, c := range stale {
		stmt := t.utcDefaultDDL(c)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			utc, zerr := sessionZoneIsUTC(ctx, db)
			if zerr != nil || !utc {
				return t.ddlRequired("has a session-clock DEFAULT on "+c+" (it would store this session's local time, not UTC)", stmt, err)
			}
			slog.WarnContext(ctx, "postgres: "+ControlTableDDLRequiredMarker+": a sluice control table column still defaults to the session clock; "+
				"harmless while this session's TimeZone is UTC, wrong by the zone's offset under any other — have the table's owner run the statement",
				slog.String("table", t.ref()), slog.String("column", c), slog.String("statement", stmt), slog.String("cause", err.Error()))
		}
	}
	return nil
}

// nonUTCDefaults returns which of columns do not default to the UTC
// expression, including a column with no default at all. pg_get_expr
// renders the UTC default `timezone('utc'::text, now())`; it is matched on
// its `'utc'::text` argument, which CURRENT_TIMESTAMP never carries.
func nonUTCDefaults(ctx context.Context, db *sql.DB, ref string, columns []string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.name
		FROM pg_catalog.unnest($2::text[]) AS c(name)
		LEFT JOIN pg_catalog.pg_attribute a
			ON a.attrelid = pg_catalog.to_regclass($1) AND a.attname = c.name AND NOT a.attisdropped
		LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attnum IS NOT NULL
		  AND pg_catalog.strpos(COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), ''), '''utc''::text') = 0`,
		ref, columns)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// sessionZoneIsUTC reports whether this session's TimeZone renders a
// January and a July instant with UTC's digits — i.e. a legacy
// CURRENT_TIMESTAMP default stores UTC here all year (a DST zone that is
// UTC+0 in winter, such as Europe/London, is not).
func sessionZoneIsUTC(ctx context.Context, db *sql.DB) (bool, error) {
	var utc bool
	err := db.QueryRowContext(ctx, `SELECT
		pg_catalog.timezone(pg_catalog.current_setting('TimeZone'), timestamptz '2026-01-15 12:00:00+00') = timestamp '2026-01-15 12:00:00'
		AND pg_catalog.timezone(pg_catalog.current_setting('TimeZone'), timestamptz '2026-07-15 12:00:00+00') = timestamp '2026-07-15 12:00:00'`).Scan(&utc)
	return utc, err
}

// ensureColumns adds whichever of cols the table lacks. The table itself
// must exist; a missing one is refused by name rather than surfacing the
// ALTER's bare "relation … does not exist".
func (t controlTable) ensureColumns(ctx context.Context, db *sql.DB, cols []controlColumn) error {
	if len(cols) == 0 {
		return nil
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}
	missing, tableFound, err := missingColumns(ctx, db, t.ref(), names)
	if err != nil {
		return t.detectError("columns", err)
	}
	if !tableFound {
		return t.ddlRequired("does not exist", t.create, errTableAbsent)
	}
	for _, c := range cols {
		if !missing[c.name] {
			continue
		}
		stmt := t.addColumnDDL(c)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return t.ddlRequired("lacks column "+c.name, stmt, err)
		}
	}
	return nil
}

// errTableAbsent stands in for a driver error when the probe, not a DDL
// statement, found the table missing.
var errTableAbsent = errors.New("the table is absent")

// ddlRequired is the loud refusal for control-table DDL that is needed and
// did not run. It keeps the driver error in the chain (%w: the SQLSTATE stays
// classifiable, and retryOnCatalogRace still sees a 23505 catalog race),
// names the object and the statement, and words the cause from the error
// rather than assuming one:
//
//   - the probe found the table absent on a path that never creates it
//     ([errTableAbsent] — the refusal-record door a `--schema-already-applied`
//     run reaches): the table must be created, by its owner;
//   - 42501: this role may not run the DDL — the privilege remedy;
//   - 3F000: the control schema itself is missing — the DSN's schema=;
//   - anything else (a lock timeout, a catalog race that outlived its
//     retry): the raw cause, with no claim about privileges.
func (t controlTable) ddlRequired(what, stmt string, err error) error {
	oneLine := strings.Join(strings.Fields(stmt), " ")
	state, _ := ir.SQLStateOf(err)
	switch {
	case errors.Is(err, errTableAbsent):
		return fmt.Errorf("postgres: %s: sluice control table %s %s: %w — this path never creates control tables; "+
			"have the owner create it with the statement below, or apply the full set `sluice control-tables ddl --engine postgres` prints, then re-run: %s",
			ControlTableDDLRequiredMarker, t.ref(), what, err, oneLine)
	case state == pgSQLStateInsufficientPrivilege:
		return fmt.Errorf("postgres: %s: sluice control table %s %s and this role may not create it: %w — "+
			"run the statement below as the table's owner (or a role with CREATE on schema %q), "+
			"or apply the full set `sluice control-tables ddl --engine postgres` prints, then re-run: %s",
			ControlTableDDLRequiredMarker, t.ref(), what, err, t.schema, oneLine)
	case state == sqlStateInvalidSchemaName:
		return fmt.Errorf("postgres: %s: schema %q does not exist, so sluice control table %s cannot be created there: %w — "+
			"check the target DSN's schema= parameter (sluice's control schema), or create the schema",
			ControlTableDDLRequiredMarker, t.schema, t.ref(), err)
	default:
		return fmt.Errorf("postgres: %s: sluice control table %s %s and the statement that fixes it failed: %w — the statement was: %s",
			ControlTableDDLRequiredMarker, t.ref(), what, err, oneLine)
	}
}

// sqlStateInvalidSchemaName is 3F000, the missing-schema SQLSTATE
// [controlTable.ddlRequired] words its own refusal for (42501 is the shared
// [pgSQLStateInsufficientPrivilege]).
const sqlStateInvalidSchemaName = "3F000"

// detectError wraps a catalog-probe failure. A 42501 there is a role without
// USAGE on the control schema — to_regclass cannot resolve a name in a
// schema the role may not use — which no DDL fixes, so it is named as such.
func (t controlTable) detectError(what string, err error) error {
	if state, _ := ir.SQLStateOf(err); state == pgSQLStateInsufficientPrivilege {
		return fmt.Errorf("postgres: this role cannot look up sluice control table %s — it lacks USAGE on schema %q "+
			"(GRANT USAGE ON SCHEMA %s TO <role>): %w", t.ref(), t.schema, quoteIdent(t.schema), err)
	}
	return fmt.Errorf("postgres: ensure %s: detect %s: %w", t.name, what, err)
}

// relationPresent reports whether ref (schema-qualified and quoted) names an
// existing relation. to_regclass returns NULL instead of raising, and reading
// the catalog needs no privilege on the relation itself.
func relationPresent(ctx context.Context, db *sql.DB, ref string) (bool, error) {
	var present bool
	err := db.QueryRowContext(ctx, `SELECT pg_catalog.to_regclass($1) IS NOT NULL`, ref).Scan(&present)
	return present, err
}

// missingColumns returns which of names the relation lacks, and whether the
// relation exists at all. One round trip.
func missingColumns(ctx context.Context, db *sql.DB, ref string, names []string) (missing map[string]bool, tableFound bool, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.name, pg_catalog.to_regclass($1) IS NOT NULL
		FROM pg_catalog.unnest($2::text[]) AS c(name)
		WHERE NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_attribute a
			WHERE a.attrelid = pg_catalog.to_regclass($1) AND a.attname = c.name AND NOT a.attisdropped)`,
		ref, names)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	missing = map[string]bool{}
	tableFound = true
	for rows.Next() {
		var (
			name  string
			found bool
		)
		if err := rows.Scan(&name, &found); err != nil {
			return nil, false, err
		}
		missing[name] = true
		tableFound = found
	}
	return missing, tableFound, rows.Err()
}
