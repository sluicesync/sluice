// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Constraints-phase foreign-key statement-wall recovery (roadmap item 109)
//
// The FK sibling of the ADR-0148 index-build fallback. PlanetScale/Vitess kill
// a long synchronous DDL at their max-statement-execution-time wall (errno
// 3024, ~900 s), and `ALTER … ADD FOREIGN KEY` is HEAVIER than an index build
// because InnoDB validates every child row against the parent as it adds the
// constraint — so a large child table's FK add cannot finish under the wall
// (field-captured 2026-07-30: a 153M-row events table, FK died at elapsed
// 15m0.0006s, and every `--resume` re-hit the identical deterministic wall).
//
// Unlike the index wall there is NO deploy-request fallback for FKs (Vitess
// Online DDL refuses FK-participating tables without the experimental
// --unsafe-allow-foreign-keys strategy), so the recovery here is client-side:
// re-issue the ADD metadata-only under session foreign_key_checks=0. That was
// proven on the live failure case — against the 153M-row table the same ADD
// under foreign_key_checks=0 returned in 0.082 s, created a real FK (present in
// information_schema.table_constraints and SHOW CREATE TABLE), and was NOT
// routed to Online DDL.
//
// SAFETY — this skips validation of the EXISTING child rows, which is sound
// ONLY because it engages exclusively when [SchemaWriter.copiedRowsFKConsistent]
// is set: an UNFILTERED migrate from an FK-enforcing source, whose per-table
// reparent reconciliation (ADR-0141) re-derives every touched table to exactly
// match that source BEFORE the constraints phase. On that path the child rows
// are FK-valid by construction. And it only ever substitutes for a validation
// that could not have completed anyway (the constraint was walling), so it
// removes no functioning loud net: a small table still validates normally (and
// a genuinely orphaned child still fails loudly with errno 1452), and a
// `--where` run — which can legitimately orphan children — never arms this, so
// its walled FK surfaces the coded SLUICE-E-CONSTRAINT-STATEMENT-TIME-LIMIT
// refusal instead of silently landing a violated constraint.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// SetCopiedRowsForeignKeyConsistent implements
// [ir.ForeignKeyConsistencyDeclarer]: the migrate orchestrator sets this true
// before the constraints phase ONLY on the safe-by-construction path (see the
// field doc and the interface doc). The default false keeps full target-side
// FK validation.
func (w *SchemaWriter) SetCopiedRowsForeignKeyConsistent(consistent bool) {
	w.copiedRowsFKConsistent = consistent
}

// Compile-time proof the setter surface is exposed so the orchestrator's
// declaration engages.
var _ ir.ForeignKeyConsistencyDeclarer = (*SchemaWriter)(nil)

// fkWallRecoveryArmed reports whether the roadmap-item-109 FK statement-wall
// recovery may engage. It requires BOTH: the orchestrator declared the copied
// rows FK-consistent, AND the target is a VStream flavor (PlanetScale/Vitess —
// the only MySQL family with the errno-3024 statement wall). On vanilla MySQL
// usesVStream() is false, so this is always false and the FK phase is
// byte-identical to before — the same-engine MySQL→MySQL no-op the tests pin.
func (w *SchemaWriter) fkWallRecoveryArmed() bool {
	return w.copiedRowsFKConsistent && w.flavor.usesVStream()
}

// addForeignKeyWithWallRecovery runs one `ALTER … ADD FOREIGN KEY`. With
// recovery disarmed it is exactly the pre-item-109 direct exec. With it armed
// (a VStream target on the FK-consistent-by-construction migrate path):
//
//  1. A clearly-huge child table skips the doomed validating attempt and adds
//     the FK metadata-only straight away (the ~900 s wall is a foregone
//     conclusion — mirrors the index fallback's DATA_LENGTH pre-probe).
//  2. Otherwise it attempts the validating ADD, and ONLY if that hits the
//     errno-3024 wall does it re-issue the constraint metadata-only. A small
//     table thus still validates, and a genuinely orphaned child still fails
//     loudly (errno 1452) rather than landing a violated FK.
func (w *SchemaWriter) addForeignKeyWithWallRecovery(
	ctx context.Context,
	armed bool,
	table string,
	fk *ir.ForeignKey,
	stmt string,
) error {
	if armed && w.indexFallbackTableClearlyHuge(ctx, table) {
		slog.InfoContext(ctx,
			"mysql: child table is clearly past PlanetScale's statement-time wall; adding its foreign key metadata-only under foreign_key_checks=0 (the copied rows are FK-consistent by construction — roadmap item 109)",
			slog.String("table", table),
			slog.String("foreign_key", fk.Name),
			slog.Int64("huge_threshold_bytes", indexFallbackHugeTableBytes))
		return w.addForeignKeyMetadataOnly(ctx, table, fk, stmt)
	}

	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		if armed && isConstraintBuildWalled(err) {
			slog.WarnContext(ctx,
				"mysql: ADD FOREIGN KEY hit PlanetScale's statement-time wall (errno 3024); re-adding it metadata-only under foreign_key_checks=0 — the copied rows are FK-consistent by construction, so InnoDB's child-row validation is redundant (roadmap item 109)",
				slog.String("table", table),
				slog.String("foreign_key", fk.Name),
				slog.String("direct_error", err.Error()))
			if rerr := w.addForeignKeyMetadataOnly(ctx, table, fk, stmt); rerr != nil {
				return fmt.Errorf("mysql: add foreign key %q on %q: metadata-only recovery after the statement-time wall failed: %w", fk.Name, table, rerr)
			}
			return nil
		}
		return fmt.Errorf("mysql: add foreign key %q on %q: %w", fk.Name, table, wrapDDLError(err))
	}
	return nil
}

// addForeignKeyMetadataOnly adds one foreign key with session
// foreign_key_checks=0, which makes InnoDB skip the child-row validation scan —
// a metadata-only change that completes instantly regardless of table size.
//
// The session variable MUST run on the SAME connection as the ADD, so this
// acquires a dedicated [*sql.Conn] rather than using the pool: setting it via
// w.db could land the SET on one pooled connection and the ADD on another,
// silently re-validating (and re-walling). It restores foreign_key_checks
// before the connection returns to the pool so the relaxed setting can never
// leak to a subsequent statement — in particular to a LATER FK in this same
// phase whose (smaller) table should still validate.
func (w *SchemaWriter) addForeignKeyMetadataOnly(ctx context.Context, table string, fk *ir.ForeignKey, stmt string) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: acquire connection for metadata-only foreign key %q on %q: %w", fk.Name, table, err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return fmt.Errorf("mysql: disable foreign_key_checks for metadata-only foreign key %q on %q: %w", fk.Name, table, wrapDDLError(err))
	}
	// Restore on the way out — after the ADD, before Close returns the
	// connection to the pool. Uses a cancel-immune context so a cancelled ctx
	// still restores; a restore that fails only does so on an already-dead
	// connection, which the pool discards rather than reuses, so no live
	// connection carries foreign_key_checks=0 forward.
	defer func() {
		if _, rerr := conn.ExecContext(context.WithoutCancel(ctx), "SET SESSION foreign_key_checks = 1"); rerr != nil {
			slog.WarnContext(ctx,
				"mysql: could not restore foreign_key_checks=1 on the metadata-only FK connection (it was likely already closed by the server; the pool discards a dead connection rather than reusing it)",
				slog.String("table", table),
				slog.String("foreign_key", fk.Name),
				slog.String("err", rerr.Error()))
		}
	}()

	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysql: add foreign key %q on %q metadata-only: %w", fk.Name, table, wrapDDLError(err))
	}
	return nil
}

// isConstraintBuildWalled reports whether err is the errno-3024 statement-time
// wall (ER_QUERY_TIMEOUT: "Query execution was interrupted, maximum statement
// execution time exceeded") — the ONLY shape the FK metadata-only recovery
// applies to. Deliberately NARROWER than the index build's [isIndexBuildWalled]:
// it excludes errno 1105 "direct DDL is disabled" (safe-migrations), because
// that is a policy block, not a validation-cost wall — foreign_key_checks=0
// cannot make a direct-DDL-blocked ALTER succeed. Everything else is NOT walled:
// a real DDL fault (a bad FK, a missing parent) must fail loudly, and an
// orphaned-child validation failure (errno 1452) must NOT be recovered.
func isConstraintBuildWalled(err error) bool {
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 3024
}
