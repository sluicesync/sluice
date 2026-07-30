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
	"strings"

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
// (a VStream target on the FK-consistent-by-construction migrate path) AND the
// FK's referencing columns already covered by an index (see below):
//
//  1. A clearly-huge child table skips the doomed validating attempt and adds
//     the FK metadata-only straight away (the ~900 s wall is a foregone
//     conclusion — mirrors the index fallback's DATA_LENGTH pre-probe).
//  2. Otherwise it attempts the validating ADD, and ONLY if that hits the
//     errno-3024 wall does it re-issue the constraint metadata-only. A small
//     table thus still validates, and a genuinely orphaned child still fails
//     loudly (errno 1452) rather than landing a violated FK.
//
// Every metadata-only add is RECORDED (w.unvalidatedFKs) so the orchestrator
// proves it clean with a bounded chunked orphan scan afterward — the skipped
// InnoDB validation is recovered out-of-band, loudly, without re-hitting the
// wall.
//
// BACKING-INDEX PRECONDITION. The metadata-only shortcut is taken ONLY when an
// index already covers the FK's referencing columns. `SET foreign_key_checks=0`
// skips InnoDB's O(rows) child-row VALIDATION, but NOT the structural backing
// index `ADD FOREIGN KEY` builds when none covers the referencing columns — and
// that index build itself walls on a huge table. In the normal migrate flow the
// source's FK-backing index is in the schema and built in the index phase
// (before constraints), so this holds and the metadata add is genuinely O(1)
// (0.082 s live-measured). When it does NOT hold we must not shortcut into a
// backing-index-build wall: we fall through to the validating ADD, whose
// errno-3024 surfaces the coded SLUICE-E-CONSTRAINT-STATEMENT-TIME-LIMIT remedy
// (loud, never silent).
func (w *SchemaWriter) addForeignKeyWithWallRecovery(
	ctx context.Context,
	armed bool,
	table *ir.Table,
	fk *ir.ForeignKey,
	stmt string,
) error {
	shortcut := armed && tableIndexCoversFKColumns(table, fk.Columns)
	if armed && !shortcut {
		slog.WarnContext(ctx,
			"mysql: the FK's referencing columns have no covering index, so ADD FOREIGN KEY would build one — which itself walls on a huge table; taking the ordinary validating ADD instead (a wall then surfaces the coded statement-time-limit remedy). Ensure the backing index is built (it normally is, from the source schema's index set) — roadmap item 109",
			slog.String("table", table.Name),
			slog.String("foreign_key", fk.Name),
			slog.Any("referencing_columns", fk.Columns))
	}

	if shortcut && w.indexFallbackTableClearlyHuge(ctx, table.Name) {
		slog.InfoContext(ctx,
			"mysql: child table is clearly past PlanetScale's statement-time wall; adding its foreign key metadata-only under foreign_key_checks=0 (the copied rows are FK-consistent by construction, verified by a chunked orphan scan afterward — roadmap item 109)",
			slog.String("table", table.Name),
			slog.String("foreign_key", fk.Name),
			slog.Int64("huge_threshold_bytes", indexFallbackHugeTableBytes))
		if err := w.addForeignKeyMetadataOnly(ctx, table.Name, fk, stmt); err != nil {
			return err
		}
		w.recordUnvalidatedFK(table.Name, fk)
		return nil
	}

	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		if shortcut && isConstraintBuildWalled(err) {
			slog.WarnContext(ctx,
				"mysql: ADD FOREIGN KEY hit PlanetScale's statement-time wall (errno 3024); re-adding it metadata-only under foreign_key_checks=0, then a chunked orphan scan proves the child rows clean (roadmap item 109)",
				slog.String("table", table.Name),
				slog.String("foreign_key", fk.Name),
				slog.String("direct_error", err.Error()))
			if rerr := w.addForeignKeyMetadataOnly(ctx, table.Name, fk, stmt); rerr != nil {
				return fmt.Errorf("mysql: add foreign key %q on %q: metadata-only recovery after the statement-time wall failed: %w", fk.Name, table.Name, rerr)
			}
			w.recordUnvalidatedFK(table.Name, fk)
			return nil
		}
		return fmt.Errorf("mysql: add foreign key %q on %q: %w", fk.Name, table.Name, wrapDDLError(err))
	}
	return nil
}

// recordUnvalidatedFK notes a foreign key added metadata-only (validation
// skipped), for the orchestrator's post-constraints chunked orphan scan.
func (w *SchemaWriter) recordUnvalidatedFK(table string, fk *ir.ForeignKey) {
	w.unvalidatedFKs = append(w.unvalidatedFKs, ir.UnvalidatedForeignKey{Table: table, FK: fk})
}

// UnvalidatedForeignKeys implements [ir.UnvalidatedForeignKeyReporter]: the FKs
// this writer added metadata-only under foreign_key_checks=0 this run, which
// the orchestrator must prove clean with a bounded chunked orphan scan.
//
// NARROW LIMITATION, flagged deliberately: this reports only FKs added THIS
// process invocation. A crash in the tiny window between a metadata-only ADD
// committing and the orphan scan completing would leave an unvalidated FK that
// a subsequent `--resume` skips (the idempotent prefetch sees it present and
// does not re-add it), so it is never scanned. The window is seconds within one
// phase, and only bites a source that actually has orphans AND a crash in that
// window AND a resume rather than a fresh run — but it IS a hole. Closing it
// robustly needs cross-run persistence of "which FKs are unvalidated" (a
// separate chunk); until then the remedy on a suspected crash is a fresh run or
// a manual orphan check.
func (w *SchemaWriter) UnvalidatedForeignKeys() []ir.UnvalidatedForeignKey {
	return w.unvalidatedFKs
}

// Compile-time proof the writer exposes the surfaces the orchestrator's
// item-109 orphan-scan verification drives.
var (
	_ ir.UnvalidatedForeignKeyReporter = (*SchemaWriter)(nil)
	_ ir.ForeignKeyOrphanScanner       = (*SchemaWriter)(nil)
	_ ir.ForeignKeyDropper             = (*SchemaWriter)(nil)
)

// tableIndexCoversFKColumns reports whether an existing index (or the PK) on
// table covers fkCols as a LEFT PREFIX — i.e. `ADD FOREIGN KEY` will reuse it
// rather than building a fresh backing index. Pure IR logic mirroring the
// pipeline's indexLeftPrefixCovers (the writer cannot import the pipeline);
// the index phase runs before constraints, so an index present in the IR is
// present on the target. A partial (WHERE-predicate) index does not count.
func tableIndexCoversFKColumns(table *ir.Table, fkCols []string) bool {
	if table == nil || len(fkCols) == 0 {
		return false
	}
	covers := func(idx *ir.Index) bool {
		if idx == nil || idx.Predicate != "" || len(idx.Columns) < len(fkCols) {
			return false
		}
		for i, col := range fkCols {
			if idx.Columns[i].Column != col {
				return false
			}
		}
		return true
	}
	if covers(table.PrimaryKey) {
		return true
	}
	for _, idx := range table.Indexes {
		if covers(idx) {
			return true
		}
	}
	return false
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

// ScanForeignKeyOrphan implements [ir.ForeignKeyOrphanScanner]: it looks for a
// single child row in the half-open PK range (lowerPK, upperPK] whose
// (non-NULL) foreign-key value has no matching parent, returning that child's
// primary key. A LEFT JOIN to the parent with `parent.<ref0> IS NULL` finds the
// unmatched rows; `LIMIT 1` stops at the first. The PK-range predicate is the
// ADR-0096 tuple comparison (`(pk) > (lo) AND (pk) <= (hi)`), so each call is
// bounded to one chunk and cannot itself hit the statement-time wall.
//
// MySQL foreign keys are always MATCH SIMPLE (MySQL parses but IGNORES the
// MATCH clause), so a row is checked only when EVERY referencing column is
// non-NULL — exactly what InnoDB's own validation would have enforced.
func (w *SchemaWriter) ScanForeignKeyOrphan(ctx context.Context, child *ir.Table, fk *ir.ForeignKey, lowerPK, upperPK []any) (violatingKey []any, found bool, err error) {
	if child == nil || child.PrimaryKey == nil || len(child.PrimaryKey.Columns) == 0 {
		return nil, false, fmt.Errorf("mysql: ScanForeignKeyOrphan: child table %q has no primary key to bound the scan", tableNameOrEmpty(child))
	}
	if fk == nil || len(fk.Columns) == 0 || len(fk.Columns) != len(fk.ReferencedColumns) {
		return nil, false, errors.New("mysql: ScanForeignKeyOrphan: foreign key has no (or mismatched) referencing/referenced columns")
	}
	pkCols := child.PrimaryKey.Columns
	if (len(lowerPK) != 0 && len(lowerPK) != len(pkCols)) || (len(upperPK) != 0 && len(upperPK) != len(pkCols)) {
		return nil, false, fmt.Errorf("mysql: ScanForeignKeyOrphan: PK-bound arity mismatch (table %q has %d PK columns)", child.Name, len(pkCols))
	}

	query, args := w.buildOrphanScanQuery(child, fk, lowerPK, upperPK)
	rows, qerr := w.db.QueryContext(ctx, query, args...)
	if qerr != nil {
		return nil, false, fmt.Errorf("mysql: ScanForeignKeyOrphan on %q: %w", child.Name, wrapDDLError(qerr))
	}
	defer func() { _ = rows.Close() }()

	tuples, serr := scanKeysetBoundaryRows(rows, len(pkCols))
	if serr != nil {
		return nil, false, fmt.Errorf("mysql: ScanForeignKeyOrphan on %q: %w", child.Name, serr)
	}
	if len(tuples) == 0 {
		return nil, false, nil
	}
	return tuples[0], true, nil
}

// buildOrphanScanQuery renders the bounded LEFT JOIN orphan probe for one
// chunk. Child aliased `c`, parent `p`; the SELECT projects the child PK so the
// caller can name a violating key.
func (w *SchemaWriter) buildOrphanScanQuery(child *ir.Table, fk *ir.ForeignKey, lowerPK, upperPK []any) (query string, args []any) {
	childRef := w.qualifiedTableRef(w.schema, child.Name)
	parentSchema := fk.ReferencedSchema
	if parentSchema == "" {
		parentSchema = w.schema
	}
	parentRef := w.qualifiedTableRef(parentSchema, fk.ReferencedTable)

	pkCols := child.PrimaryKey.Columns
	pkProj := make([]string, len(pkCols))
	pkTupleList := make([]string, len(pkCols))
	for i, pc := range pkCols {
		pkProj[i] = "c." + quoteIdent(pc.Column)
		pkTupleList[i] = "c." + quoteIdent(pc.Column)
	}
	pkTuple := "(" + strings.Join(pkTupleList, ", ") + ")"

	joinConds := make([]string, len(fk.Columns))
	notNullConds := make([]string, len(fk.Columns))
	for i := range fk.Columns {
		joinConds[i] = "c." + quoteIdent(fk.Columns[i]) + " = p." + quoteIdent(fk.ReferencedColumns[i])
		notNullConds[i] = "c." + quoteIdent(fk.Columns[i]) + " IS NOT NULL"
	}

	conds := make([]string, 0, len(notNullConds)+3)
	conds = append(conds, notNullConds...)
	// After the LEFT JOIN, the first referenced column being NULL means no
	// parent row matched — the referenced columns are a parent PK/UNIQUE, so
	// they are non-NULL in any matched row.
	conds = append(conds, "p."+quoteIdent(fk.ReferencedColumns[0])+" IS NULL")

	if len(lowerPK) != 0 {
		conds = append(conds, pkTuple+" > ("+sqlPlaceholders(len(lowerPK))+")")
		args = append(args, lowerPK...)
	}
	if len(upperPK) != 0 {
		conds = append(conds, pkTuple+" <= ("+sqlPlaceholders(len(upperPK))+")")
		args = append(args, upperPK...)
	}

	query = "SELECT " + strings.Join(pkProj, ", ") +
		" FROM " + childRef + " c" +
		" LEFT JOIN " + parentRef + " p ON " + strings.Join(joinConds, " AND ") +
		" WHERE " + strings.Join(conds, " AND ") +
		" LIMIT 1"
	return query, args
}

// DropForeignKey implements [ir.ForeignKeyDropper]: removes a named foreign key
// from a child table (used to undo an unvalidated metadata-only add when the
// orphan scan finds a violation, so the target never keeps a trusted-wrong FK).
func (w *SchemaWriter) DropForeignKey(ctx context.Context, childTable, fkName string) error {
	stmt := "ALTER TABLE " + w.qualifiedTableRef(w.schema, childTable) + " DROP FOREIGN KEY " + quoteIdent(fkName)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysql: drop foreign key %q on %q: %w", fkName, childTable, wrapDDLError(err))
	}
	return nil
}

// qualifiedTableRef renders `schema`.`table` when schema is non-empty, else
// `table` — the same convention the reader's range queries use.
func (w *SchemaWriter) qualifiedTableRef(schema, table string) string {
	if schema != "" {
		return quoteIdent(schema) + "." + quoteIdent(table)
	}
	return quoteIdent(table)
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
