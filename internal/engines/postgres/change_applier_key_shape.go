// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// # GC-42: key-scoped writes against a key that is not unique (yet)
//
// A Postgres target can hold a DEFERRABLE primary key, unique constraint or
// exclusion constraint — sluice CARRIES a source's deferrable PRIMARY KEY
// (ddl_emit.go, since v0.103.1), and an operator can add any of them by
// hand. Such a constraint is checked only when its deferred re-check fires,
// so mid-transaction a key value may be shared by two rows.
//
// The loss: every UPDATE/DELETE the applier issues names its row by the
// change's before-image, and a source under REPLICA IDENTITY FULL (or a
// postgres-trigger source) has that image narrowed to the PRIMARY KEY (Bug
// 92). While a deferrable key is transiently shared, `DELETE … WHERE id = 2`
// deleted BOTH rows and the commit-time check then passed on the (unique)
// final state — silent, at exit 0.
//
// The guard (G1): a key-scoped write that matched more than one row is
// refused ([appliershared.RefuseKeyScopedMultiMatch]) and its transaction
// rolled back. It reaches every apply path: [ChangeApplier.dispatch]
// (per-change, serial batch fall-back, lane serial fall-back, lane barrier)
// reads the exec result through [checkKeyScopedResult];
// [ChangeApplier.dispatchPipelined] (batch and lanes) tags the queued
// statement with [guardKeyScopedWrite] and [ChangeApplier.sendBatchUnderDeadline]
// grades the command tags. TestKeyScopedWriteRoster holds both to it.
//
// The guard reads only the affected-row count each statement already returns,
// so it costs nothing on the healthy path. It has no exemption: its sibling,
// the keyless one-row address ([ChangeApplier.rowAddressFor]), makes a keyless
// table's whole-row write touch exactly one of its identical copies — before
// it, that write deleted or rewrote EVERY copy, silent since v0.1.0. Choosing
// the address needs the table's key for whole-row writes only, read once per
// table per run ([ChangeApplier.tableHasRowKey]).
//
// # Replica mode and the deferred re-check: detected, not changed
//
// `session_replication_role = replica` — the Bug 164 FK bypass — also
// switches the deferred re-check off: PG implements it as an internal
// constraint trigger (unique_key_recheck, tgenabled 'O'), and replica mode
// suppresses every 'O' trigger. Running a deferrable-key table's writes under
// `origin` instead (GC-42's first cut) was withdrawn: origin also fires the
// operator's own triggers on replicated rows (a BEFORE UPDATE `updated_at =
// now()` rewrites the source's value; ENABLE REPLICA triggers stop firing for
// INSERT/UPDATE but not DELETE, which breaks a postgres-trigger relay
// downstream), and it turned a converging key shift under REPLICA IDENTITY
// USING INDEX into a permanent refusal. G1 alone closes the loss; what
// replica mode leaves is a constraint-enforcement gap — the rows still match
// the source — named by a WARN ([ChangeApplier.noteUnenforcedDeferrable]).
//
// Why a WARN and not a refusal at startup for a target STRICTER than its
// source (a deferrable constraint the source lacks): the harm needs the
// source to hold rows that violate the target's constraint, and deciding
// that statically means matching constraint column sets, operators and
// predicates across engines (a MySQL source has no deferrable constraints at
// all, yet enforces an equivalent UNIQUE immediately). A refusal keyed on
// "deferrable on the target" alone would stop every sync into a target that
// merely re-added the source's own DEFERRABLE (what the schema reader's hint
// suggests), which is the common, harmless case. The WARN names the table,
// the constraint and the REINDEX check that settles it.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// deferredCheckFailedMarker is the grep-stable marker on a commit refused by
// a deferred constraint's re-check. It fires only where the re-check runs:
// an apply role WITHOUT the replica privilege.
const deferredCheckFailedMarker = "DEFERRED-KEY-CHECK-FAILED-AT-COMMIT"

// deferredCheckOffMarker is the grep-stable marker on the WARN that a table's
// deferrable constraint is not re-checked under replica mode.
const deferredCheckOffMarker = "DEFERRED-KEY-CHECK-OFF-IN-REPLICA-MODE"

// The SQLSTATEs a deferred re-check raises at COMMIT.
const (
	pgUniqueViolation    = "23505"
	pgExclusionViolation = "23P01"
)

// tableRowKeySQL reports whether a TARGET table has a PRIMARY KEY or a
// NOT NULL, non-partial, non-expression unique index — of ANY deferrability:
// a deferrable key still identifies one row once its transaction commits,
// which is exactly the promise a multi-row match breaks. Read only for a
// whole-row write ([ChangeApplier.rowAddressFor]).
const tableRowKeySQL = `
	SELECT EXISTS (
		SELECT 1 FROM pg_index ix
		JOIN pg_class c ON c.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
		  AND (ix.indisprimary OR (
		       ix.indisunique AND ix.indpred IS NULL AND ix.indexprs IS NULL
		       AND NOT EXISTS (
		           SELECT 1 FROM pg_attribute a
		           WHERE a.attrelid = ix.indrelid
		             AND a.attnum = ANY (ix.indkey::int2[])
		             AND NOT a.attnotnull))))`

// tableDeferrableSQL names a TARGET table's DEFERRABLE primary key, unique
// and exclusion constraints, comma-joined (empty when there are none). It reads
// pg_constraint.condeferrable for p/u/x and, as a cross-check that does not
// depend on a constraint row existing, every unique or exclusion index whose
// pg_index.indimmediate is false.
const tableDeferrableSQL = `
	SELECT COALESCE((
		SELECT pg_catalog.string_agg(d.name, ', ' ORDER BY d.name) FROM (
			SELECT co.conname::text AS name
			FROM pg_constraint co
			WHERE co.conrelid = c.oid AND co.contype IN ('p', 'u', 'x') AND co.condeferrable
			UNION
			SELECT ic.relname::text
			FROM pg_index ix JOIN pg_class ic ON ic.oid = ix.indexrelid
			WHERE ix.indrelid = c.oid
			  AND (ix.indisunique OR ix.indisexclusion)
			  AND NOT ix.indimmediate
		) d
	), '')
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1 AND c.relname = $2`

// tableHasRowKey reports whether the TARGET table has a key
// ([tableRowKeySQL]), cached per table for the applier run and dropped on a
// schema boundary. It is read only for a write whose before-image covers the
// whole row ([ChangeApplier.rowAddressFor]) — a key-narrowed write needs no
// key knowledge — on the primary pool, never on an apply transaction or a
// pinned pipelined backend. A probe error is returned: guessing either way
// would decide how many rows the write may touch.
func (a *ChangeApplier) tableHasRowKey(ctx context.Context, schema, table string) (bool, error) {
	qn := schemaTableKey(schema, table)
	if keyed, ok := a.cachedRowKey(qn); ok {
		return keyed, nil
	}
	var keyed bool
	if err := a.db.QueryRowContext(ctx, tableRowKeySQL, schema, table).Scan(&keyed); err != nil {
		return false, fmt.Errorf("postgres: applier: read the key of %s.%s: %w", schema, table, err)
	}
	a.storeRowKey(qn, keyed)
	return keyed, nil
}

func (a *ChangeApplier) cachedRowKey(qn string) (keyed, ok bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	keyed, ok = a.rowKeyCache[qn]
	return keyed, ok
}

func (a *ChangeApplier) storeRowKey(qn string, keyed bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.rowKeyCache == nil {
		a.rowKeyCache = make(map[string]bool)
	}
	a.rowKeyCache[qn] = keyed
}

// rowAddress is how an UPDATE/DELETE's WHERE reaches its row.
type rowAddress int

const (
	// addressEveryMatch: the WHERE as built. Right for a keyed table — the
	// key makes the match unique — and for a key-narrowed before-image,
	// where the multi-row guard refuses any second match.
	addressEveryMatch rowAddress = iota
	// addressOneRow: exactly one of the rows the whole-row WHERE matches. A
	// keyless table can hold identical rows, a source changes ONE of them,
	// and its whole image matches every copy — before GC-42's keyless fix
	// the target deleted or rewrote them all (silent since v0.1.0). Which
	// copy is immaterial: they are identical in every column.
	addressOneRow
)

// rowAddressFor picks the address for a write: addressOneRow exactly when the
// target table has no key AND the before-image names every non-generated
// target column (a whole-row predicate). A key-narrowed image against a
// keyless table keeps addressEveryMatch, so a second match is refused rather
// than resolved by picking a row the source may not have meant.
func (a *ChangeApplier) rowAddressFor(ctx context.Context, schema, table string, before ir.Row, colTypes map[string]*ir.Column) (rowAddress, error) {
	if !appliershared.WholeRowImage(before, colTypes) {
		return addressEveryMatch, nil
	}
	keyed, err := a.tableHasRowKey(ctx, schema, table)
	if err != nil || keyed {
		return addressEveryMatch, err
	}
	return addressOneRow, nil
}

// rowTargetSQL renders the WHERE body for addr. The one-row form addresses
// the physical row by (tableoid, ctid): ctid alone repeats across the
// partitions (or inheritance children) of the table the statement names, so
// a bare `ctid = …` against a partitioned parent would also hit the other
// partitions' rows at that position. The inner predicate is the same
// NULL-aware whole-row WHERE, so a NULL column still matches its NULL.
func rowTargetSQL(tableRef, whereSQL string, addr rowAddress) string {
	if addr != addressOneRow {
		return whereSQL
	}
	return "(tableoid, ctid) = (SELECT tableoid, ctid FROM " + tableRef + " WHERE " + whereSQL + " LIMIT 1)"
}

// keyScopedWrite is the G1 check carried by an UPDATE/DELETE.
type keyScopedWrite struct {
	op     string // "update" / "delete"
	before ir.Row
}

// guardKeyScopedWrite returns the G1 check for a write. Every UPDATE/DELETE
// carries one: a key-scoped write may match at most one row (its key), and a
// keyless whole-row write is addressed to one row ([addressOneRow]), so more
// than one is never right.
func guardKeyScopedWrite(op string, before ir.Row) *keyScopedWrite {
	return &keyScopedWrite{op: op, before: before}
}

// verdict refuses a write that matched more than one row. Decided from the
// count alone — the healthy path reads nothing.
func (k *keyScopedWrite) verdict(schema, table string, matched int64) error {
	if k == nil || matched <= 1 {
		return nil
	}
	return appliershared.RefuseKeyScopedMultiMatch(engineNamePostgres, k.op, schema, table, k.before, matched)
}

// checkKeyScopedResult is G1 on the serial path: it reads the exec's
// affected-row count and grades it. pgx's stdlib driver always reports the
// count (from the command tag), so a read error is a driver change the guard
// must not paper over, and is returned.
func checkKeyScopedResult(op, schema, table string, before ir.Row, res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: applier: read rows affected by %s on %s.%s: %w", op, schema, table, err)
	}
	return guardKeyScopedWrite(op, before).verdict(schema, table, n)
}

// markDeferrableChecked records that qn's deferrable constraints have been
// read for this run and reports whether THIS call recorded it, so the read
// (and its WARN) happens once per table even under concurrent lanes. A schema
// boundary forgets the mark ([ChangeApplier.invalidateMetadataCaches]).
func (a *ChangeApplier) markDeferrableChecked(qn string) (firstTime bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.deferrableChecked == nil {
		a.deferrableChecked = make(map[string]bool)
	}
	if a.deferrableChecked[qn] {
		return false
	}
	a.deferrableChecked[qn] = true
	return true
}

// noteUnenforcedDeferrable WARNs that a deferrable constraint on the table is
// not re-checked during apply: the applier runs in replica mode, which
// switches the re-check off. It reads the catalog ONCE per table per applier
// run (on first touch, again after a schema boundary) and only in replica
// mode — a role without the replica privilege never leaves origin, so its
// checks fire and there is nothing to say. Detection only, and never a reason
// to stop the apply: a probe error is logged and the table is not re-read.
//
// Per table on first touch, rather than one catalog sweep at open: the
// applier's scope is not known at open (multi-database routing, `schema
// add-table`, a routed schema), and a sweep would read every table of a
// target the stream may barely touch.
func (a *ChangeApplier) noteUnenforcedDeferrable(ctx context.Context, schema, table string) {
	qn := schemaTableKey(schema, table)
	if !a.foreignKeyBypassAvailable(ctx) || !a.markDeferrableChecked(qn) {
		return
	}
	var deferrable string
	if err := a.db.QueryRowContext(ctx, tableDeferrableSQL, schema, table).Scan(&deferrable); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			slog.WarnContext(ctx, "postgres: applier: could not read the table's DEFERRABLE constraints for the "+
				deferredCheckOffMarker+" check; continuing without it",
				slog.String("table", qn), slog.String("err", err.Error()))
		}
		return
	}
	if deferrable == "" {
		return
	}
	slog.WarnContext(ctx,
		"postgres: applier: "+deferredCheckOffMarker+": this target table's DEFERRABLE constraint is NOT re-checked "+
			"while sluice applies changes — the apply transactions run with session_replication_role=replica (the FK "+
			"bypass), which switches PostgreSQL's deferred uniqueness/exclusion check off; rows are applied exactly as "+
			"the source sent them, so if the target's constraint is stricter than the source's (the source lacks it) the "+
			"table can hold rows the constraint forbids, and REINDEX TABLE would fail on them",
		slog.String("table", qn),
		slog.String("deferrable_constraints", deferrable),
		slog.String("hint", "give the target the same constraint as the source (if the source has no such constraint, "+
			"drop it on the target or make it NOT DEFERRABLE so a violating row is refused at its statement); "+
			"`REINDEX TABLE <table>` checks whether any violating row already landed"))
}

// annotateDeferredCheckFailure marks a COMMIT refused by a deferred
// constraint's re-check. A 23505 (unique) or 23P01 (exclusion) raised AT
// COMMIT can only come from a deferred check — an immediate constraint
// raises at its statement — so no catalog read is needed to tell. The
// SQLSTATE stays in the chain, so the error classifies exactly as before
// (terminal). Every other error passes through unchanged.
func annotateDeferredCheckFailure(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || (pgErr.Code != pgUniqueViolation && pgErr.Code != pgExclusionViolation) {
		return err
	}
	return fmt.Errorf("%s: a DEFERRABLE constraint's commit-time check refused the target transaction (%s) — "+
		"either a source transaction that is valid only as a whole was split across target transactions (a lane or "+
		"batch boundary, a key change applied alone as a lane barrier, or --apply-batch-size 1), or the target is "+
		"stricter than the source (it holds a deferrable key the source does not); nothing in this target "+
		"transaction was written: %w",
		deferredCheckFailedMarker, strings.TrimSpace(pgErr.ConstraintName), err)
}
