// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// classifyPublicationPermission turns Postgres's raw SQLSTATE 42501 on a
// publication DDL into the coded, steered refusal an operator can act on.
//
// WHY THIS EXISTS (upstream review UPR-4, from PlanetScale pgcopydb fork
// PR #59). sluice creates the publication CDC reads through, and ran that DDL
// with nothing between the operator and the server's error. A role without the
// right grant got, at cold start:
//
//	pipeline: ensure publication scope: postgres: create publication
//	"sluice_pub": ERROR: permission denied for database appdb (SQLSTATE 42501)
//
// — uncoded, with no remedy, and no hint matched it: migcore/hints.go carries
// only "permission denied for schema" and "permission denied for replication",
// neither of which is a substring of any of the three real failures.
//
// THE THREE FAILURES, measured on PostgreSQL 16 against an unprivileged LOGIN
// REPLICATION role. All are 42501, and they need DIFFERENT grants:
//
//	CREATE PUBLICATION p FOR TABLE t1  -> permission denied for database appdb
//	  (same, after GRANT CREATE)       -> must be owner of table t1
//	CREATE PUBLICATION p FOR ALL TABLES-> must be superuser to create FOR ALL
//	                                      TABLES publication
//
// WHY CLASSIFICATION AND NOT A PREFLIGHT PROBE. The review that surfaced this
// proposed an attempt-probe — `BEGIN; CREATE PUBLICATION …; ROLLBACK;` before
// the real DDL — over a catalog read of rolsuper/has_database_privilege,
// because a catalog-based predecessor was live-wrong on RDS for pgtrigger.
// That reasoning is right and this goes one step further in the same
// direction: the REAL statement is already the best possible probe of itself.
// Classifying its error costs no extra round trip, cannot drift from the DDL
// the way a separately-rendered probe can, and reports what the server
// actually said rather than what a catalog implies it would say. The probe was
// measured to leave zero residue, so it was viable — it is simply strictly
// worse than not needing one.
//
// Timing is not sacrificed by classifying rather than preflighting: on cold
// start the publication is created BEFORE the replication slot and before any
// row moves, so this still fails with nothing to clean up.
//
// Classification is STRUCTURAL — on the SQLSTATE, never on the message text
// (CLAUDE.md). The remedy names all three requirements rather than guessing
// which one bit, because distinguishing them WOULD require text matching; the
// server's own message travels with the wrapped error and says which.
func classifyPublicationPermission(ctx context.Context, db *sql.DB, name string, err error) error {
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		return err
	}
	// Identity is read only on the failure path, so the happy path pays
	// nothing. A failure here is swallowed: the refusal must survive a
	// degraded catalog read, and "unknown" is a worse answer than none only
	// if it is presented as fact.
	who := publicationRoleContext(ctx, db)
	return sluicecode.Wrap(sluicecode.CodeCDCPublicationPermission,
		"grant the connecting role what the publication needs, or connect as one that has it: "+
			"CREATE on the database (GRANT CREATE ON DATABASE <db> TO <role>) AND ownership of every "+
			"table in scope (ALTER TABLE <t> OWNER TO <role>, or make the role a member of the owning "+
			"role). A database-wide FOR ALL TABLES publication — which a multi-schema `sync start` and "+
			"`backup full --chain-slot` both require — additionally needs SUPERUSER, and no grant "+
			"substitutes for it on stock PostgreSQL. If that is not available, scope the run to a "+
			"single schema so sluice creates a FOR TABLE publication instead",
		fmt.Errorf("postgres: cdc: the connecting role may not create publication %q%s (SQLSTATE 42501): %w",
			name, who, err))
}

// publicationRoleContext renders " as role X on database Y" for the refusal
// above, or "" when the catalog cannot be read. Best-effort by design — see
// the call site.
func publicationRoleContext(ctx context.Context, db *sql.DB) string {
	if db == nil {
		return ""
	}
	var role, database string
	if err := db.QueryRowContext(ctx, "SELECT current_user, pg_catalog.current_database()").Scan(&role, &database); err != nil {
		return ""
	}
	return fmt.Sprintf(" as role %q on database %q", role, database)
}
