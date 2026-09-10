// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Turning PostgreSQL's opclass-missing error into an actionable one.
//
// # What an operator saw before this
//
// A schema using `btree_gist` — an EXCLUDE constraint of the canonical shape
// `EXCLUDE USING gist (room WITH =, during WITH &&)`, or a mixed index like
// `CREATE INDEX … USING gist (tenant_id, during)` — copied to a target that
// does not have the extension installed died with PostgreSQL's own words:
//
//	postgres: create table "bg_src": ERROR: data type integer has no default
//	operator class for access method "gist" (SQLSTATE 42704)
//
// Measured 2026-09-10 against a PlanetScale Neki target. Nothing in that
// sentence says `btree_gist`, and nothing says what to do. sluice already
// refuses a missing extension properly when an extension owns a COLUMN TYPE
// ([missingExtensionError], SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED); the same
// schema failing over an extension-owned OPERATOR CLASS fell through to the
// driver, which is the one place this engine is supposed not to leave the
// operator.
//
// # Why classify the error rather than preflight the catalog
//
// A preflight would be better — it would refuse before creating anything.
// Doing it honestly means carrying each index column's owning extension
// through the IR, which the schema reader can already see (it reads
// `opclass_ext_owned` from `pg_depend`) but does not name. That is a real
// change to a rostered IR struct and is filed as follow-on work.
//
// What is here instead is cheap and reliable: PostgreSQL's message is
// deterministic and names BOTH the type and the access method, and the two
// extensions that exist to supply exactly these opclasses are well known. So
// the failure is re-raised at the moment it happens with the extension named
// and the install command spelled out — no catalog rules are re-derived, and
// an unrecognised combination degrades to a generic-but-still-actionable
// message rather than a guess.

// opclassMissingRe matches PostgreSQL's undefined-opclass message, capturing
// the data type and the access method it names. The wording has been stable
// across every supported major version; a wording change makes this classifier
// inert (the original error still surfaces), never wrong.
var opclassMissingRe = regexp.MustCompile(
	`data type ([^ ]+(?: [^ ]+)*?) has no default operator class for access method "([^"]+)"`,
)

// opclassExtensionFor names the contrib extension that supplies btree-style
// operator classes for `am`, or "" when there is no well-known answer.
//
// Only the two extensions whose entire purpose is this are claimed.
// `btree_gist` and `btree_gin` exist to let scalar types participate in GiST
// and GIN indexes — which is precisely the situation the error reports — so
// naming them is a statement of fact, not a heuristic. Anything else returns
// "" and the caller says "the extension that provides it" instead of
// inventing a name.
func opclassExtensionFor(am string) string {
	switch strings.ToLower(am) {
	case "gist":
		return "btree_gist"
	case "gin":
		return "btree_gin"
	default:
		return ""
	}
}

// annotateMissingOpclass re-raises a DDL failure caused by a missing operator
// class as a coded, actionable refusal. Any other error is returned unchanged,
// so this is safe to wrap every DDL site with.
func annotateMissingOpclass(err error, what string) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42704" {
		return err
	}
	m := opclassMissingRe.FindStringSubmatch(pgErr.Message)
	if m == nil {
		return err
	}
	dataType, am := m[1], m[2]

	ext := opclassExtensionFor(am)
	install := "install the extension that provides a " + am + " operator class for " + dataType
	names := "an extension"
	if ext != "" {
		install = "run `CREATE EXTENSION " + ext + ";` on the target"
		names = ext
	}

	return &sluicecode.CodedError{
		Code: sluicecode.CodeSchemaExtensionNotEnabled,
		Hint: install + ", then re-run; sluice does not auto-install extensions per the " +
			"contain-Postgres-complexity tenet",
		Err: fmt.Errorf(
			"postgres: %s needs a %q operator class for %s, which core PostgreSQL does not provide and "+
				"this target does not have installed — this is %s"+
				"\nthe source schema uses it through an index or EXCLUDE constraint that mixes a scalar "+
				"column into a %s index (the shape `EXCLUDE USING %s (a WITH =, b WITH &&)` produces)"+
				"\nunderlying error: %w",
			what, am, dataType, names, am, am, err,
		),
	}
}
