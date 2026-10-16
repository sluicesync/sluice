// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// The conflict-identity guard, Postgres half (SOURCE-DROPPED-UNIQUE-KEY-MERGE;
// the MySQL twin, engines/mysql/unique_key_conflict_guard.go, has the full
// rule and the measured shape).
//
// # Where Postgres can merge
//
// `INSERT … ON CONFLICT (key) DO UPDATE` arbitrates on exactly the named key
// ([loadConflictKey]): a conflict through any other unique index raises
// unique_violation, loud already. So a table whose conflict key is its
// primary key is safe — a different row reached through another index is a
// 23505, never an overwrite. The merge needs the conflict key to BE a unique
// index other than the primary key: a table with no primary key (or with a
// deferrable one, which Postgres refuses as an arbiter) keyed on its
// deterministic NOT NULL unique index. When the source drops that index and
// inserts a row duplicating its key, the DO UPDATE overwrites the existing
// row's every other column with the incoming row's — measured on v0.161.0,
// PG → PG (c5g) — at exit 0.
//
// # The rule, and what it cannot reach
//
// The identity is the MySQL twin's: the primary key when the row supplies
// it, else the columns of every unique key. On Postgres the second arm adds
// nothing — the arbiter is one key, and a matched row that disagrees on
// any OTHER unique index is already a unique_violation — so the guard is
// armed only on a table with a primary key that is not its conflict key (a
// DEFERRABLE one, which [loadConflictKey] skips as an arbiter). There, a
// matched row with a different primary key raises
// UNIQUE-KEY-CONFLICT-OTHER-ROW and rolls the statement back.
//
// The measured c5g shape — no primary key, ONE unique index as the arbiter
// — is not guarded, stated so it cannot be read as broader. The merge and
// the newer-image re-send the ON CONFLICT exists for (a re-applied Insert
// carrying a changed non-key column, pinned by
// TestChangeApplier_NoPKUniqueKeyInsert_IdempotentReplay) are the same
// statement against the same target row, and nothing the applier holds
// tells them apart: the CDC boundary shapes carry no unique indexes
// (projectRelation) and are stale across exactly the DROP. Comparing every
// column was built and rejected — it refuses every newer-image re-send — and
// so was the filing's "an arbiter the source also declares", which needs the
// unknowable source key set, with a plain INSERT as the alternative refusing
// every re-delivery. That shape is closed at the source instead: the
// pgoutput door now refuses a live DROP INDEX (cdc_unforwarded_classes.go),
// and a DROP made while the stream was stopped is the filed stream-open
// comparison (UNIQUE-KEY-STOPPED-WINDOW-DOOR).
//
// The comparison is on each value's text form under COLLATE "C" — every
// type has a text output, while some (json) have no equality operator at
// all — so it is byte-exact and never weaker than the column's own
// equality.
//
// # Mechanism
//
// The key-collation guard's (key_collation_guard.go): the existing row is
// aliased and the DO UPDATE's WHERE is a CASE whose else-branch is an
// invalid integer cast carrying the marker. When the key-collation guard is
// armed too, its predicate is the OUTER check and this one is its agreeing
// value, so a key the target's collation equates reports as that guard's
// merge; the identity is checked for every row it passed. The cast's
// operand is `pg_catalog.concat(EXCLUDED.<col>)`, which references the row (so the planner cannot fold it) and is never NULL (a
// NULL would make the cast NULL instead of an error).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// uniqueKeyConflictMarker is the literal the guard's raise carries; the
// refusal's sentinel ([ir.ErrUniqueKeyConflictOtherRow]) is the same text.
const uniqueKeyConflictMarker = "UNIQUE-KEY-CONFLICT-OTHER-ROW"

// pgConflictIdentity arms the guard for one table: it has a primary key that
// is not its conflict key (a DEFERRABLE one, which Postgres refuses as an
// arbiter, so [loadConflictKey] fell through to a unique index). pk is that
// primary key, key the conflict key, for the refusal's text.
type pgConflictIdentity struct {
	pk  []string
	key []string
}

// newPGConflictIdentity returns the guard for a table whose conflict key is
// key and whose primary key is pk, or nil when the conflict key IS the
// primary key (any other index's conflict is a unique_violation), there is
// no primary key (see the file comment), or no conflict key (a plain INSERT).
func newPGConflictIdentity(pk, key []string) *pgConflictIdentity {
	if len(key) == 0 || len(pk) == 0 {
		return nil
	}
	if len(pk) == len(key) {
		same := true
		for i := range pk {
			if pk[i] != key[i] {
				same = false
				break
			}
		}
		if same {
			return nil
		}
	}
	return &pgConflictIdentity{pk: append([]string(nil), pk...), key: append([]string(nil), key...)}
}

// conflictIdentityFor returns the conflict-identity guard for an insert into
// schema.table (routed) keyed on key, or nil when the conflict key is the
// table's primary key. The primary key comes from the applier's cached
// metadata ([ChangeApplier.pkForRedact]), so the steady state costs nothing.
func (a *ChangeApplier) conflictIdentityFor(ctx context.Context, schema, table string, key []string) (*pgConflictIdentity, error) {
	if len(key) == 0 {
		return nil, nil
	}
	pk, err := a.pkForRedact(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: primary-key lookup for the conflict-identity guard on %s.%s: %w", schema, table, err)
	}
	return newPGConflictIdentity(pk, key), nil
}

// columns is the identity an insert of cols must find on the matched row:
// the primary key when the insert names every column of it, else nil
// (unarmed for that statement).
func (g *pgConflictIdentity) columns(cols []string) []string {
	have := make(map[string]bool, len(cols))
	for _, c := range cols {
		have[c] = true
	}
	for _, p := range g.pk {
		if !have[p] {
			return nil
		}
	}
	return g.pk
}

// wrap returns the DO UPDATE's predicate for an insert of cols: inner (the
// key-collation predicate, or "true") when the matched row carries the
// identity, the marker's raise otherwise. A nil guard, or one whose identity
// the insert does not supply, returns inner.
func (g *pgConflictIdentity) wrap(cols []string, inner string) string {
	if g == nil {
		return inner
	}
	ident := g.columns(cols)
	if len(ident) == 0 {
		return inner
	}
	eq := make([]string, len(ident))
	for i, c := range ident {
		eq[i] = "(" + quoteIdent(keyCollationExistingAlias) + "." + quoteIdent(c) + ")::text COLLATE \"C\" IS NOT DISTINCT FROM (EXCLUDED." +
			quoteIdent(c) + ")::text COLLATE \"C\""
	}
	raise := "(('" + uniqueKeyConflictMarker + "' || pg_catalog.left(pg_catalog.concat(EXCLUDED." + quoteIdent(cols[0]) + "), 0))::int IS NULL)"
	return "CASE WHEN " + strings.Join(eq, " AND ") + " THEN " + inner + " ELSE " + raise + " END"
}

// isUniqueKeyConflictRaise reports whether err is the guard's raise: an
// invalid integer cast whose text keeps the marker.
func isUniqueKeyConflictRaise(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22P02" && strings.Contains(pgErr.Message, uniqueKeyConflictMarker)
}

// applyUniqueKeyConflictError attributes a guarded CDC insert's failure to
// [ir.UniqueKeyConflictError], naming the table, the key and the remedy;
// anything else is returned unchanged.
func applyUniqueKeyConflictError(err error, g *pgConflictIdentity, schema, table string) error {
	if g == nil || !isUniqueKeyConflictRaise(err) {
		return err
	}
	quoted := make([]string, len(g.key))
	for i, c := range g.key {
		quoted[i] = quoteIdent(c)
	}
	keyDesc := "the unique index on (" + strings.Join(quoted, ", ") + "), this table's ON CONFLICT key"
	return &ir.UniqueKeyConflictError{
		Schema: schema, Table: table, Cause: err, Keys: keyDesc,
		Remedy: "Check whether the source still declares that index. If it does not, drop it on the target " +
			"(`DROP INDEX <schema>.<index>`; find it with `\\d " + quoteIdent(schema) + "." + quoteIdent(table) + "`) and " +
			"restart with the same --stream-id — the table then has no conflict key and the row lands beside the one it " +
			"matched, as on the source. If the source still declares it, the matched target row is stale (a newer " +
			"version of it, or a unique value that moved between rows, inside a re-delivery window) and a re-copy " +
			"converges: restart with --restart-from-scratch.",
	}
}

// applyInsertGuardError is the CDC insert's attribution of a guarded
// statement's failure to whichever guard raised it.
func applyInsertGuardError(err error, coll *pgKeyCollationPlan, identity *pgConflictIdentity, schema, table string) error {
	if identity != nil && isUniqueKeyConflictRaise(err) {
		return applyUniqueKeyConflictError(err, identity, schema, table)
	}
	return applyKeyCollationMergeError(err, coll, schema, table)
}
