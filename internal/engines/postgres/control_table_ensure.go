// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

// statements is the table's full current shape as idempotent DDL, in the
// order a fresh target needs it. Applying it to an older-shape table
// upgrades it; applying it twice is a no-op.
func (t controlTable) statements() []string {
	out := make([]string, 0, 1+len(t.columns)+len(t.indexes))
	out = append(out, t.create)
	for _, c := range t.columns {
		out = append(out, t.addColumnDDL(c))
	}
	for _, ix := range t.indexes {
		out = append(out, ix.ddl)
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
		return fmt.Errorf("postgres: ensure %s: detect table: %w", t.name, err)
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
			return fmt.Errorf("postgres: ensure %s: detect index %s: %w", t.name, ix.name, err)
		}
		if present {
			continue
		}
		if _, err := db.ExecContext(ctx, ix.ddl); err != nil {
			return t.ddlRequired("lacks index "+ix.name, ix.ddl, err)
		}
	}
	return nil
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
		return fmt.Errorf("postgres: ensure %s: detect columns: %w", t.name, err)
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

// ddlRequired is the loud refusal for DDL this role could not run. It keeps
// the driver error in the chain (%w: the SQLSTATE stays classifiable, and
// retryOnCatalogRace still sees a 23505 catalog race) and names the object,
// the statement and where the full set comes from.
func (t controlTable) ddlRequired(what, stmt string, err error) error {
	return fmt.Errorf("postgres: %s: sluice control table %s %s and this role could not create it: %w — "+
		"run the statement below as the table's owner (or a role with CREATE on schema %q), "+
		"or apply the full set `sluice control-tables ddl --engine postgres` prints, then re-run: %s",
		ControlTableDDLRequiredMarker, t.ref(), what, err, t.schema, strings.Join(strings.Fields(stmt), " "))
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
