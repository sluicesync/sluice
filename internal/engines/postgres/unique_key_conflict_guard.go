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
// it, else the columns of every unique index (the arbiter included) when
// the table has at least two. The second arm is NOT redundant on Postgres:
// a DO UPDATE rewrites the matched row's OTHER unique columns too, and a
// unique_violation fires only when a THIRD row already holds the new value
// (review finding, measured on postgres:16: `t(id, email, code, v)` keyed on
// UNIQUE(code) beside UNIQUE(email, id), `INSERT (99,'y@a','a',…) ON
// CONFLICT (code) DO UPDATE` onto (1,'x@a','a',…) returned INSERT 0 1 and
// row 1 was gone). Guarded:
//
//   - a table with a primary key that is not its conflict key (a DEFERRABLE
//     one, which [loadConflictKey] skips as an arbiter): the primary key;
//   - a table with no primary key and two or more unique indexes: every
//     unique index's columns, so a matched row disagreeing on any of them
//     refuses — including when the dropped index IS the arbiter.
//
// A table keyed on its primary key needs nothing: a conflict through the
// arbiter IS the same row.
//
// The shape left open — no primary key and exactly ONE unique index — is
// stated so it cannot be read as broader. The merge and the newer-image
// re-send the ON CONFLICT exists for (a re-applied Insert carrying a changed
// non-key column, pinned by
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
// The cost of the union arm, stated: a newer image of the SAME source row
// whose other unique column changed (a keyless row whose `email` was
// updated, re-sent by an ADR-0072 resume or replayed in an overlap window
// onto a target that already holds the update) refuses where it used to
// converge — loud (UNIQUE-KEY-GUARD-OVERLAP-REFUSAL).
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
// operand is `pg_catalog.concat(EXCLUDED.<col>)`, which references the row
// (so the planner cannot fold it) and is never NULL (a NULL would make the
// cast NULL instead of an error).

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

// pgConflictIdentity arms the guard for one table whose conflict key is not
// its primary key. pk is the primary key (empty when there is none);
// uniqueCols is every unique index's plain columns, the primary key excluded
// and the arbiter included; key is the conflict key, for the refusal's text.
type pgConflictIdentity struct {
	pk         []string
	uniqueCols [][]string
	key        []string
}

// newPGConflictIdentity returns the guard for a table whose conflict key is
// key, or nil when the conflict key IS the primary key, there is no conflict
// key (a plain INSERT), or the table has no primary key and fewer than two
// unique indexes (see the file comment).
func newPGConflictIdentity(pk, key []string, uniqueCols [][]string) *pgConflictIdentity {
	if len(key) == 0 {
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
	if len(pk) == 0 && len(uniqueCols) < 2 {
		return nil
	}
	return &pgConflictIdentity{pk: append([]string(nil), pk...), uniqueCols: uniqueCols, key: append([]string(nil), key...)}
}

// conflictIdentityFor returns the conflict-identity guard for an insert into
// schema.table (routed) keyed on key, or nil when unarmed. The primary key
// and the unique indexes come from cached catalog reads, so the steady state
// costs nothing.
func (a *ChangeApplier) conflictIdentityFor(ctx context.Context, schema, table string, key []string) (*pgConflictIdentity, error) {
	if len(key) == 0 {
		return nil, nil
	}
	pk, err := a.pkForRedact(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: primary-key lookup for the conflict-identity guard on %s.%s: %w", schema, table, err)
	}
	var uniqueCols [][]string
	if len(pk) == 0 {
		if uniqueCols, err = a.uniqueIndexColumnsFor(ctx, schema, table); err != nil {
			return nil, fmt.Errorf("postgres: applier: unique-index lookup for the conflict-identity guard on %s.%s: %w", schema, table, err)
		}
	}
	return newPGConflictIdentity(pk, key, uniqueCols), nil
}

// uniqueIndexColumnsFor returns, cached per table, every unique index's
// plain key columns (expression elements skipped; the primary key
// excluded). A partial index counts: agreeing on its columns is required of
// the same row whether or not the predicate covers it.
func (a *ChangeApplier) uniqueIndexColumnsFor(ctx context.Context, schema, table string) ([][]string, error) {
	qn := schemaTableKey(schema, table)
	a.cacheMu.RLock()
	v, ok := a.uniqueIndexColsCache[qn]
	a.cacheMu.RUnlock()
	if ok {
		return v, nil
	}
	rows, err := a.db.QueryContext(ctx, `
		SELECT ix.indexrelid, a.attname
		FROM   pg_index ix
		JOIN   pg_class      tcl ON tcl.oid = ix.indrelid
		JOIN   pg_namespace  n   ON n.oid   = tcl.relnamespace
		JOIN   LATERAL pg_catalog.unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
		JOIN   pg_attribute  a   ON a.attrelid = ix.indrelid AND a.attnum = u.attnum
		WHERE  n.nspname = $1 AND tcl.relname = $2
		  AND  ix.indisunique AND NOT ix.indisprimary AND ix.indisvalid
		ORDER  BY ix.indexrelid, u.ord`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	var last uint32
	for rows.Next() {
		var oid uint32
		var col string
		if err := rows.Scan(&oid, &col); err != nil {
			return nil, err
		}
		if len(out) == 0 || oid != last {
			out = append(out, nil)
			last = oid
		}
		out[len(out)-1] = append(out[len(out)-1], col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	a.cacheMu.Lock()
	if a.uniqueIndexColsCache == nil {
		a.uniqueIndexColsCache = map[string][][]string{}
	}
	a.uniqueIndexColsCache[qn] = out
	a.cacheMu.Unlock()
	return out, nil
}

// columns is the identity an insert of cols must find on the matched row:
// the primary key when the insert names every column of it, else the union
// of the unique indexes' columns when there are two or more and the insert
// names every one, else nil (unarmed for that statement).
func (g *pgConflictIdentity) columns(cols []string) []string {
	have := make(map[string]bool, len(cols))
	for _, c := range cols {
		have[c] = true
	}
	pick := func(want []string) []string {
		seen := map[string]bool{}
		out := make([]string, 0, len(want))
		for _, w := range want {
			if !have[w] {
				return nil
			}
			if !seen[w] {
				seen[w] = true
				out = append(out, w)
			}
		}
		return out
	}
	if len(g.pk) > 0 {
		if out := pick(g.pk); out != nil {
			return out
		}
	}
	if len(g.uniqueCols) < 2 {
		return nil
	}
	var union []string
	for _, k := range g.uniqueCols {
		union = append(union, k...)
	}
	return pick(union)
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
