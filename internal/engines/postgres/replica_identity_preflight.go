// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The SOURCE-side replica-identity refusal (roadmap item 93).
//
// PostgreSQL will not use a DEFERRABLE key's index as a replica
// identity — relcache skips every index that is not
// `pg_index.indimmediate` before it records the table's primary-key /
// replica-identity index — so a table whose only key is a deferrable
// PRIMARY KEY has NO usable replica identity. Cold start adds it to the
// publication with `pubupdate=t pubdelete=t`, and from that moment
// Postgres refuses the SOURCE APPLICATION's own writes to it:
//
//	ERROR: cannot update table "dpk" because it does not have a replica
//	       identity and publishes updates
//	ERROR: cannot delete from table "dpk" because it does not have a
//	       replica identity and publishes deletes
//
// INSERT keeps working, so the table looks healthy until the first
// update — and the breakage lands in the operator's own application,
// where nothing points back at sluice. That is why this refuses rather
// than warns, and why it runs BEFORE `EnsurePublication`: putting the
// source into a state where the operator's writes fail and only then
// complaining IS the defect.
//
// Same catalog bit as Bug 211, opposite side of the pipeline —
// `indimmediate=false` is also what makes a deferrable key illegal as an
// `ON CONFLICT` arbiter on the TARGET
// ([ChangeApplier.PreflightUpsertKeys], upsert_key_preflight.go). The
// two are deliberately separate checks with separate codes because the
// rules differ: an immediate UNIQUE index alongside a deferrable PK
// rescues the TARGET (`ON CONFLICT` can arbitrate on any unique index),
// but it does NOT rescue the SOURCE — `REPLICA IDENTITY DEFAULT`
// resolves to the primary key and nothing else, so that shape is
// refused here and the refusal names the `REPLICA IDENTITY USING INDEX`
// remedy that makes it usable. Ground-truthed on PostgreSQL 16 before
// this was written; see the family matrix in
// replica_identity_preflight_integration_test.go.

// replicaIdentityRow is one source table's raw replica-identity catalog
// state, read verbatim so [replicaIdentityUsable] — the part that
// encodes PostgreSQL's resolution rules — stays a pure function with no
// database in the way.
type replicaIdentityRow struct {
	Table string

	// Identity is pg_class.relreplident: 'd' (default — the primary key),
	// 'i' (an explicitly nominated index), 'f' (FULL), 'n' (NOTHING).
	Identity string

	// PrimaryKeyIndex is the table's PRIMARY KEY index name ("" when it
	// has none) and PrimaryKeyUsable whether that index satisfies the
	// relcache filter (valid + unique + immediate + non-partial). A
	// DEFERRABLE primary key is present-but-unusable — the item-93 shape.
	PrimaryKeyIndex  string
	PrimaryKeyUsable bool

	// ChosenIndex is the index the table nominates via `REPLICA IDENTITY
	// USING INDEX` ("" when none does) and ChosenIndexUsable whether it
	// passes the same filter.
	ChosenIndex       string
	ChosenIndexUsable bool

	// CandidateIndex is the narrowest remedy available without changing a
	// constraint: an immediate, valid, non-partial UNIQUE index over
	// NOT NULL columns that the operator could point `REPLICA IDENTITY
	// USING INDEX` at. "" when the table has none.
	CandidateIndex string
}

// replicaIdentityUsable applies PostgreSQL's OWN replica-identity
// resolution to one catalog row: it reports whether an UPDATE/DELETE on
// the table can be published, and — when it cannot — the reason, phrased
// for the operator.
//
// Pure on purpose: this is the decision the refusal hangs on, so it is
// unit-testable across every relreplident × index shape without a
// server (the family matrix, not one representative).
func replicaIdentityUsable(row replicaIdentityRow) (usable bool, reason string) {
	switch row.Identity {
	case "f":
		// FULL publishes the whole old row; no index is involved and no
		// key is required. Always usable — including for a keyless table.
		return true, ""
	case "n":
		return false, "REPLICA IDENTITY is explicitly set to NOTHING, which publishes no old-row identity at all"
	case "i":
		switch {
		case row.ChosenIndex == "":
			return false, "REPLICA IDENTITY is set to USING INDEX but the table nominates no index (the nominated index was dropped)"
		case !row.ChosenIndexUsable:
			return false, fmt.Sprintf(
				"the index %q it nominates with REPLICA IDENTITY USING INDEX is not usable as one (DEFERRABLE, partial, or invalid)",
				row.ChosenIndex,
			)
		}
		return true, ""
	case "d":
		switch {
		case row.PrimaryKeyIndex == "":
			return false, "it has no PRIMARY KEY, and REPLICA IDENTITY DEFAULT resolves to the primary key and nothing else — a UNIQUE index alone does not make one"
		case !row.PrimaryKeyUsable:
			return false, fmt.Sprintf(
				"its PRIMARY KEY index %q is DEFERRABLE, and PostgreSQL will not use a non-immediate index "+
					"(pg_index.indimmediate = false) as a replica identity — REPLICA IDENTITY DEFAULT resolves to nothing",
				row.PrimaryKeyIndex,
			)
		}
		return true, ""
	default:
		// An relreplident letter this build does not know. Refuse rather
		// than assume it publishes an identity: a wrong "usable" here is
		// exactly the silent breakage the preflight exists to prevent.
		return false, fmt.Sprintf("pg_class.relreplident is %q, which this build of sluice does not recognise", row.Identity)
	}
}

// replicaIdentityGap names one in-scope source table that cannot publish
// UPDATE/DELETE, with the reason and (when one exists) the index the
// operator could nominate.
type replicaIdentityGap struct {
	Table          string
	Reason         string
	CandidateIndex string
}

// replicaIdentityHint is the remedy that rides the coded refusal. Like
// [deferrableUpsertKeyHint] it is the same advice for one table or
// twenty, so it carries no names.
const replicaIdentityHint = "on the SOURCE, either make the table's key immediate " +
	"(ALTER TABLE … DROP CONSTRAINT …, then re-add it WITHOUT DEFERRABLE), or set a replica identity " +
	"(ALTER TABLE … REPLICA IDENTITY FULL, or REPLICA IDENTITY USING INDEX <an immediate NOT NULL UNIQUE index>), " +
	"or take the table out of scope with --exclude-table"

// errUnusableReplicaIdentity is the aggregated coded refusal, naming
// every offending table at once (the [errDeferrableUpsertKeys] shape).
func errUnusableReplicaIdentity(schema string, gaps []replicaIdentityGap) error {
	parts := make([]string, len(gaps))
	for i, g := range gaps {
		qualified := qualifyForRemedy(schema, g.Table)
		detail := fmt.Sprintf("%s (%s", qualified, g.Reason)
		if g.CandidateIndex != "" {
			detail += fmt.Sprintf(
				"; the table already carries an immediate NOT NULL UNIQUE index %q, so "+
					"`ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;` is the narrowest fix",
				g.CandidateIndex, qualified, quoteIdent(g.CandidateIndex),
			)
		} else {
			detail += fmt.Sprintf("; `ALTER TABLE %s REPLICA IDENTITY FULL;` makes it publishable as-is", qualified)
		}
		parts[i] = detail + ")"
	}
	return sluicecode.Wrap(
		sluicecode.CodeSourceReplicaIdentity,
		replicaIdentityHint,
		fmt.Errorf(
			"postgres: source table(s) %s have no usable replica identity, and this sync is about to add them to its "+
				"publication with UPDATE and DELETE publishing. From that moment PostgreSQL refuses the SOURCE "+
				"APPLICATION's own writes to them (\"cannot update table \\\"…\\\" because it does not have a replica "+
				"identity and publishes updates\", and the same for DELETE) — INSERT keeps working, so the table looks "+
				"healthy until the first update and the breakage surfaces in your application, not in sluice. Nothing has "+
				"been changed on the source: sluice refuses BEFORE touching the publication. Fix each table on the source "+
				"and re-run, or take it out of scope with --exclude-table. REPLICA IDENTITY FULL is always sufficient "+
				"(it publishes the whole old row, at the cost of WAL volume on every UPDATE/DELETE)",
			strings.Join(parts, ", "),
		),
	)
}

// PreflightReplicaIdentity implements [ir.ReplicaIdentityPreflighter]: it
// reads every in-scope table's replica-identity state in ONE catalog
// query and refuses, naming all of them at once, if any of them cannot
// publish an UPDATE/DELETE.
//
// tables is the stream's post-filter table list, so a table the sync
// never touches is never refused over (`--exclude-table` is one of the
// remedies the refusal names). An empty list is a no-op.
//
// Scope note: ordinary tables only (relkind 'r'). A declaratively
// partitioned parent's identity lives on its leaves, and sluice refuses
// partitioned tables at an earlier cold-start preflight (Bug 100), so
// reaching one here is not possible.
func (r *SchemaReader) PreflightReplicaIdentity(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	inScope := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		inScope[t] = struct{}{}
	}

	rows, err := r.replicaIdentityRows(ctx)
	if err != nil {
		return err
	}

	var gaps []replicaIdentityGap
	for _, row := range rows {
		if _, ok := inScope[row.Table]; !ok {
			continue
		}
		usable, reason := replicaIdentityUsable(row)
		if usable {
			continue
		}
		gaps = append(gaps, replicaIdentityGap{
			Table:          row.Table,
			Reason:         reason,
			CandidateIndex: row.CandidateIndex,
		})
	}
	if len(gaps) == 0 {
		return nil
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].Table < gaps[j].Table })
	return errUnusableReplicaIdentity(r.schema, gaps)
}

// replicaIdentityRows reads the replica-identity state of every ordinary
// table in the reader's schema in one pass.
//
// The three LATERAL sub-selects mirror what PostgreSQL's relcache does
// when it resolves a replica identity: an index is only a candidate if it
// is valid, unique, immediate and non-partial — the same four predicates,
// applied to the primary-key index ('d'), the nominated index ('i'), and
// the best free-standing remedy index. They are SELECTed rather than
// filtered in a WHERE clause for the [loadConflictKey] reason: a
// deferrable-key table and a genuinely keyless one want different prose,
// and a predicate would make them indistinguishable.
func (r *SchemaReader) replicaIdentityRows(ctx context.Context) ([]replicaIdentityRow, error) {
	const q = `
		SELECT c.relname,
		       c.relreplident,
		       COALESCE(pk.name, ''),
		       COALESCE(pk.usable, false),
		       COALESCE(ri.name, ''),
		       COALESCE(ri.usable, false),
		       COALESCE(cand.name, '')
		FROM   pg_class     c
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		LEFT   JOIN LATERAL (
		         SELECT ci.relname AS name,
		                (i.indisvalid AND i.indisunique AND i.indimmediate AND i.indpred IS NULL) AS usable
		         FROM   pg_index i
		         JOIN   pg_class ci ON ci.oid = i.indexrelid
		         WHERE  i.indrelid = c.oid AND i.indisprimary
		         LIMIT  1
		       ) pk ON TRUE
		LEFT   JOIN LATERAL (
		         SELECT ci.relname AS name,
		                (i.indisvalid AND i.indisunique AND i.indimmediate AND i.indpred IS NULL) AS usable
		         FROM   pg_index i
		         JOIN   pg_class ci ON ci.oid = i.indexrelid
		         WHERE  i.indrelid = c.oid AND i.indisreplident
		         LIMIT  1
		       ) ri ON TRUE
		LEFT   JOIN LATERAL (
		         SELECT min(ci.relname) AS name
		         FROM   pg_index i
		         JOIN   pg_class ci ON ci.oid = i.indexrelid
		         WHERE  i.indrelid  = c.oid
		           AND  i.indisvalid AND i.indisunique AND i.indimmediate
		           AND  i.indpred IS NULL AND i.indexprs IS NULL
		           AND  NOT EXISTS (
		                  SELECT 1
		                  FROM   unnest(i.indkey) AS k(attnum)
		                  JOIN   pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
		                  WHERE  NOT a.attnotnull)
		       ) cand ON TRUE
		WHERE  n.nspname = $1
		  AND  c.relkind = 'r'
		ORDER  BY c.relname`

	catRows, err := r.catalogQuery(ctx, q, r.schema)
	if err != nil {
		return nil, fmt.Errorf("postgres: read replica identity for schema %q: %w", r.schema, err)
	}
	defer func() { _ = catRows.Close() }()

	var out []replicaIdentityRow
	for catRows.Next() {
		var row replicaIdentityRow
		if err := catRows.Scan(
			&row.Table,
			&row.Identity,
			&row.PrimaryKeyIndex,
			&row.PrimaryKeyUsable,
			&row.ChosenIndex,
			&row.ChosenIndexUsable,
			&row.CandidateIndex,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan replica identity row: %w", err)
		}
		out = append(out, row)
	}
	if err := catRows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read replica identity for schema %q: %w", r.schema, err)
	}
	return out, nil
}

// compile-time assertion that the reader satisfies the preflighter
// surface — without it the pipeline's optional type-assert would
// silently skip the whole check.
var _ ir.ReplicaIdentityPreflighter = (*SchemaReader)(nil)
