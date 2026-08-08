// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The Postgres half of the generated-PRIMARY-KEY identity class — the
// sibling the 2026-08-07 MySQL fix (engines/mysql/cdc_generated_pk.go)
// enumerated as confirmed, silent and unfixed. Same consequence,
// entirely different mechanism, which is why the two refusals share a
// code and share nothing else.
//
// PostgreSQL ACCEPTS a PRIMARY KEY on a STORED generated column. It
// then refuses to publish that column: before PG 18 pgoutput has no way
// to carry a generated column at all, so `pg_publication_tables.attnames`
// lists only the non-generated ones and the RelationMessage that reaches
// this reader simply does not mention the key. Ground-truthed on a real
// PostgreSQL 16.14 (see cdc_generated_pk_integration_test.go); the raw
// wire for `PRIMARY KEY (a, g)` with `g` generated, relreplident='d':
//
//	R … public od d numcols=3  [key]a  c  b            -- g absent
//	D … K 3 cols: a='1', c=null-marker, b=null-marker  -- g absent
//
// Note what that is NOT: the value is present in the WAL (test_decoding
// renders `DELETE: a[integer]:1 g[integer]:10`), so this is pgoutput's
// column filter, not a WAL gap — and it is exactly why bumping the
// publication's `publish_generated_columns` on PG 18 fixes it while
// nothing sluice can decode differently does.
//
// [CDCReader.resolveIdentityKeyCols] then reads the identity off the
// wire, so the identity SHRINKS silently, and the damage differs by how
// much of the key is generated:
//
//   - PART of the key generated (`PRIMARY KEY (a, g)`): IdentityKeyCols
//     becomes {a} — a NON-UNIQUE prefix. The applier's WHERE is `a = $1`
//     and a DELETE of one source row deletes EVERY target row sharing
//     that prefix. Proven end to end against real servers with the row
//     counts read from the target: source 3 -> 2, target 3 -> 1
//     (TestGeneratedPrimaryKey_UnpublishedIdentityOverDeletes).
//   - ALL of the key generated: no column carries the wire key flag,
//     IdentityKeyCols is empty, and [filterBeforeToKeyCols]'s PK-less
//     fallback hands back the whole decoded old tuple — which under
//     DEFAULT is null markers, so the WHERE is `a IS NULL AND b IS NULL`,
//     matches nothing, ADR-0010 absorbs the miss and the position
//     advances. Bug 8 reached from the other direction.
//
// Both are silent. Neither is carriable: the value is not on the wire.
// So this refuses, matching the MySQL sibling — and the MySQL sibling
// refuses rather than carries PRECISELY because this half cannot carry,
// and a contract that differs per engine is worse than a uniform one.
//
// # What the refusal keys on, and why it is not a version check
//
// PostgreSQL 18 added the `publish_generated_columns` publication
// option. With it set to `stored` the RelationMessage DOES carry the
// column, flagged as a key, and the Delete tuple carries its value —
// verified on a real PostgreSQL 18.4:
//
//	R … public od d numcols=4  [key]a  c  b  [key]g
//	D … K 4 cols: a='1', c=null, b=null, g='10'
//
// So the hazard is a property of the PUBLICATION, not of the server
// version, and the check keys on the strongest available evidence: a
// generated identity column that is ABSENT from the RelationMessage
// sluice just received. That is the wire's own account of what it will
// carry, it needs no version branch, and it cannot over-refuse a
// publication that does carry the column.
//
// (PostgreSQL 18 also closes the silent half at the source: it refuses
// the application's own UPDATE/DELETE with "Replica identity must not
// contain unpublished generated columns." That is the item-93 harm
// rather than a silent one, and it is why the cold-start preflight —
// replica_identity_preflight.go — carries this check too, ahead of
// scoping the publication.)
//
// That last paragraph is an environmental fact, so it is measured
// rather than asserted:
// [TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity] pins it
// in both directions on whatever server the harness booted. Naming what
// it found, because the sentence above is narrower than it reads in one
// place and wider in another: PostgreSQL 18 refuses only once a
// publication that publishes update/delete covers the table (no
// publication, an insert-only one, or one scoped elsewhere all leave the
// writes working, and the shape stays silent) — and under REPLICA
// IDENTITY FULL its notion of the identity is EVERY column, so it also
// refuses a FULL table whose generated column is nowhere near the
// primary key. That last cell is the one the preflight below
// deliberately does not grade; see its own known-limit note.

// identityColumns is schema.table's EFFECTIVE replica-identity column
// set as the catalog knows it — All in key order, plus the Generated
// subset. Both are needed: Generated says whether there is a hazard, and
// the two LENGTHS together say which hazard, which is the difference
// between "this DELETE removes too many rows" and "this DELETE removes
// none", and those want different prose.
type identityColumns struct {
	All       []string
	Generated []string
}

// identityGeneratedColumns returns schema.table's EFFECTIVE
// replica-identity column set with its STORED generated members marked —
// the columns whose absence from the wire destroys row identity.
//
// "Effective" follows PostgreSQL's own resolution, and matters: under
// REPLICA IDENTITY USING INDEX the identity is the nominated index, NOT
// the primary key, so comparing against the PK there would both
// over-refuse (a generated PK column outside the nominated index is
// harmless) and under-refuse (a generated column inside it is not).
//
//	'd' DEFAULT     -> the PRIMARY KEY index's columns
//	'i' USING INDEX -> the pg_index.indisreplident index's columns
//	'f' FULL        -> the PRIMARY KEY index's columns, because that is
//	                   what resolveIdentityKeyCols narrows a FULL table
//	                   to; a FULL table with no PK has no narrowing and
//	                   so nothing to lose here.
//	'n' NOTHING     -> empty: no old tuple is emitted at all, and the
//	                   source-side preflight already refuses the shape.
//
// Generated is empty for the overwhelmingly common table whose identity
// carries no generated column, which is the only cost this adds to a
// normal stream: one indexed catalog query per RelationMessage,
// alongside the two already issued there.
func identityGeneratedColumns(ctx context.Context, db *sql.DB, schema, table string) (identityColumns, error) {
	const q = `
		SELECT a.attname, a.attgenerated <> ''
		FROM   pg_class      cl
		JOIN   pg_namespace  n  ON n.oid = cl.relnamespace
		JOIN   pg_index      ix ON ix.indrelid = cl.oid
		JOIN   LATERAL unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
		JOIN   pg_attribute  a  ON a.attrelid = cl.oid AND a.attnum = u.attnum
		WHERE  n.nspname  = $1
		  AND  cl.relname = $2
		  AND  cl.relkind = 'r'
		  AND  CASE cl.relreplident
		         WHEN 'i' THEN ix.indisreplident
		         WHEN 'n' THEN false
		         ELSE ix.indisprimary
		       END
		ORDER  BY u.ord`
	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		return identityColumns{}, err
	}
	defer func() { _ = rows.Close() }()

	var out identityColumns
	for rows.Next() {
		var (
			name      string
			generated bool
		)
		if err := rows.Scan(&name, &generated); err != nil {
			return identityColumns{}, err
		}
		out.All = append(out.All, name)
		if generated {
			out.Generated = append(out.Generated, name)
		}
	}
	if err := rows.Err(); err != nil {
		return identityColumns{}, err
	}
	return out, nil
}

// refuseUnpublishedGeneratedIdentity returns a coded, stream-fatal error
// when any of entry's identity columns is generated AND absent from the
// RelationMessage — i.e. when the identity sluice is about to narrow to
// is missing a key the publication will never send.
//
// identity is the effective identity column set
// ([identityGeneratedColumns]); the ABSENCE test against entry.Columns
// is what makes this self-scoping — a PG 18 publication carrying
// `publish_generated_columns = stored` lists the column in the
// RelationMessage and this returns nil.
//
// nil when the identity carries no generated column, which is every
// ordinary table.
func refuseUnpublishedGeneratedIdentity(entry *relationCacheEntry, identity identityColumns) error {
	if len(identity.Generated) == 0 {
		return nil
	}
	published := make(map[string]struct{}, len(entry.Columns))
	for _, col := range entry.Columns {
		published[col.Name] = struct{}{}
	}
	var missing []string
	for _, name := range identity.Generated {
		if _, ok := published[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		// PG 18 with `publish_generated_columns = stored`: the column is
		// on the wire, the identity is whole, nothing to refuse.
		return nil
	}
	// Which hazard, decided on the catalog's own account of the identity
	// rather than on IdentityKeyCols — under FULL the latter is the
	// catalog PK (generated members included), so it cannot tell the two
	// apart.
	consequence := "the WHERE would be built from a NON-UNIQUE prefix of the key, so applying one source DELETE " +
		"would delete every target row sharing that prefix"
	if len(missing) == len(identity.All) {
		// Nothing published carries the identity: the narrowing has no
		// key at all and falls back to the full old tuple, which under
		// DEFAULT/USING INDEX is null markers.
		consequence = "no published column carries the row identity at all, so the WHERE would match no row and the " +
			"change would be silently dropped while the stream position advanced"
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCGeneratedPrimaryKey,
		"on the SOURCE, give the table a non-generated row identity — demote the generated column to an ordinary "+
			"column, or point `ALTER TABLE … REPLICA IDENTITY USING INDEX` at an immediate NOT NULL UNIQUE index over "+
			"non-generated columns; or copy the table with `sluice migrate` instead, or drop it from the stream with "+
			"--exclude-table. On PostgreSQL 18+ a publication created WITH (publish_generated_columns = stored) also "+
			"carries it, but sluice does not create one",
		fmt.Errorf(
			"postgres: cdc: %s.%s cannot be replicated: its replica identity includes generated column(s) %s, and "+
				"PostgreSQL does not publish generated columns (pg_publication_tables.attnames omits them, and the "+
				"RelationMessage for this table does not mention them). %s. The stream stops here rather than diverging",
			entry.Schema, entry.Name, strings.Join(quoteEachIdent(missing), ", "), consequence,
		),
	)
}

// quoteEachIdent wraps each name in double quotes for a message.
func quoteEachIdent(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = fmt.Sprintf("%q", n)
	}
	return out
}
