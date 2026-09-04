// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sort"
)

// A multi-namespace cold start needs a database-wide logical slot, so the
// publication it opens is FOR ALL TABLES. That reaches every table in the
// database, and a permanent logged table with no usable replica identity
// stops accepting UPDATE and DELETE the moment the publication exists —
// in whatever application owns it, which on an unselected schema is one
// that has nothing to do with this sync.
//
// [SchemaReader.PreflightReplicaIdentity] covers the tables the operator
// selected and REFUSES over them. This covers the ones they did not, and
// WARNS. That split is the operator's decision (audit A2-4b), recorded on
// [ir.PublicationExposureAuditor] with the reasoning.
//
// MEASURED, because the at-risk set is narrower than "every table" and
// guessing it wider would name tables that were never in danger. On real
// PostgreSQL 16.15, 17.11, 18.6 and 19beta1 — identical on all four — a
// `FOR ALL TABLES` publication resolves through pg_publication_tables to
// exactly `relkind='r' AND relpersistence='p'`. An UNLOGGED table, a
// partitioned PARENT, a view and a materialized view are all outside it;
// a leaf partition is inside. The same four servers all refuse the write
// with `cannot update table "x" because it does not have a replica
// identity and publishes updates`, while INSERT keeps working — which is
// why this is worth a warning at all: the breakage is partial and
// surfaces as an application error with nothing pointing back here.

// exposureCandidate is one at-risk relation, already qualified.
type exposureCandidate struct {
	namespace string
	table     string
	reason    string
}

// AuditPublicationExposure implements [ir.PublicationExposureAuditor].
//
// covered is asked per TABLE rather than per namespace. Skipping whole
// selected namespaces is what left the --exclude-table case reported by
// nobody; see the interface doc. The system catalogs are skipped
// unconditionally — they are not the operator's to fix and pg_catalog's
// tables are not published.
func (r *SchemaReader) AuditPublicationExposure(ctx context.Context, covered func(namespace, table string) bool) ([]string, error) {
	return auditPublicationExposure(ctx, r.catalogQuery, covered)
}

// catalogQueryFunc is the read this audit needs, so the same body can serve
// the SchemaReader-bound sync path and the db-bound backup path.
type catalogQueryFunc func(ctx context.Context, q string, args ...any) (*catalogRows, error)

// auditPublicationExposure is the shared core. It exists because
// ensureAllTablesPublication has TWO callers and the first cut of this
// surface reached one of them (audit VF review of v0.141.0, HIGH-1): the
// multi-schema sync, and `backup full --chain-slot`. The backup case is the
// worse of the two -- it deliberately PERSISTS the publication so the
// chain's incrementals can decode through it, so the exposure outlives the
// run -- and it had neither a refusal nor a warning.
func auditPublicationExposure(ctx context.Context, query catalogQueryFunc, covered func(namespace, table string) bool) ([]string, error) {
	rows, err := exposureRowsVia(ctx, query, covered)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(rows))
	for _, c := range rows {
		out = append(out, fmt.Sprintf("%s.%s (%s)", c.namespace, c.table, c.reason))
	}
	sort.Strings(out)
	return out, nil
}

// exposureRows asks the same replica-identity question
// [SchemaReader.replicaIdentityRows] asks, database-wide and restricted
// to what a FOR ALL TABLES publication actually covers.
//
// It deliberately does NOT run the PG-18 published-generated-column
// exemption that the refusing preflight runs. That exemption asks whether
// some publication already carries a generated identity column, and its
// purpose there is to avoid REFUSING a working configuration. Here the
// outcome is a warning, the cost of an extra line is a sentence an
// operator can check in a second, and the cost of the extra round trip is
// paid on every multi-schema cold start. Stated rather than left to be
// discovered: this can name a table that PG 18's
// `publish_generated_columns` would have rescued.
func exposureRowsVia(ctx context.Context, query catalogQueryFunc, covered func(namespace, table string) bool) ([]exposureCandidate, error) {
	const q = `
		SELECT n.nspname,
		       c.relname,
		       c.relreplident,
		       COALESCE(pk.name, ''),
		       COALESCE(pk.usable, false),
		       COALESCE(ri.usable, false)
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
		         SELECT (i.indisvalid AND i.indisunique AND i.indimmediate AND i.indpred IS NULL) AS usable
		         FROM   pg_index i
		         WHERE  i.indrelid = c.oid AND i.indisreplident
		         LIMIT  1
		       ) ri ON TRUE
		WHERE  c.relkind = 'r'
		  AND  c.relpersistence = 'p'
		  AND  n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND  n.nspname NOT LIKE 'pg_toast%'
		  AND  n.nspname NOT LIKE 'pg_temp%'
		ORDER  BY n.nspname, c.relname`

	catRows, err := query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: audit publication exposure: %w", err)
	}
	defer func() { _ = catRows.Close() }()

	var out []exposureCandidate
	for catRows.Next() {
		var (
			ns, table string
			replIdent string
			pkName    string
			pkUsable  bool
			riUsable  bool
		)
		if err := catRows.Scan(&ns, &table, &replIdent, &pkName, &pkUsable, &riUsable); err != nil {
			return nil, fmt.Errorf("postgres: scan publication exposure row: %w", err)
		}
		// Asked per table, not per namespace. A nil predicate covers
		// nothing, so every at-risk table is reported -- the safe default
		// for a caller that did not say what its refusal already grades.
		if covered != nil && covered(ns, table) {
			continue
		}
		// The same four cases replicaIdentityUsable decides, restated
		// here because this read has no index names to suggest and no
		// generated-column column to exempt: FULL is always fine, an
		// explicit NOTHING never is, and 'd'/'i' turn on whether the
		// index they name is usable.
		//
		// Two limits of the restatement, written down because both are
		// invisible from the query and both are now pinned by
		// TestAuditPublicationExposure_MatchesRealPublicationCoverage:
		//
		// The 'i' arm fires, in practice, only when the table nominates
		// NOTHING -- the operator dropped the index it named, which
		// leaves relreplident='i' and no indisreplident row at all.
		// MEASURED on 16.15/17.11/18.6: PostgreSQL refuses at ALTER time
		// every index shape the usability predicate would reject (partial,
		// non-unique, non-immediate, invalid, nullable column) and refuses
		// to drop the NOT NULL afterwards, so ri.usable=false over a
		// PRESENT index is not reachable by any DDL the server accepts.
		// The four predicates in the ri LATERAL are therefore defensive;
		// COALESCE's false is what actually decides, and a mutation run
		// that replaces only that LATERAL's predicate with TRUE survives.
		//
		// "no primary key" is IMPRECISE for a DEFERRABLE key: the key
		// exists and publishes nothing (indimmediate=false). The
		// classification is right and the sentence is not, because the
		// query carries usability as a bare boolean and cannot tell the
		// two apart. The refusing sibling replicaIdentityUsable does
		// distinguish them ("its PRIMARY KEY index %q is DEFERRABLE");
		// closing the gap here costs one more column in the pk LATERAL.
		switch replIdent {
		case "f":
			continue
		case "n":
			out = append(out, exposureCandidate{ns, table, "REPLICA IDENTITY NOTHING"})
		case "i":
			if !riUsable {
				out = append(out, exposureCandidate{ns, table, "REPLICA IDENTITY USING INDEX names an unusable index"})
			}
		default:
			switch {
			case pkUsable:
				// Nothing to report.
			case pkName == "":
				out = append(out, exposureCandidate{ns, table, "no primary key"})
			default:
				// A PRIMARY KEY that EXISTS and is not usable as a replica
				// identity: DEFERRABLE (non-immediate) is the case operators
				// actually create. Saying "no primary key" here contradicts
				// the operator's own \d output, which is how a true warning
				// gets dismissed as a bug in the tool. The refusing sibling
				// in replica_identity_preflight.go has always drawn this
				// distinction; this one did not until 2026-09-04.
				out = append(out, exposureCandidate{
					ns, table,
					"PRIMARY KEY " + pkName + " is not usable as a replica identity (deferrable, invalid, partial or non-unique)",
				})
			}
		}
	}
	if err := catRows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: iterate publication exposure rows: %w", err)
	}
	return out, nil
}
