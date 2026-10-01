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
// reads its command tag. TestKeyScopedWriteRoster holds both to it.
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
// the source — named by a one-time WARN ([ChangeApplier.noteUnenforcedDeferrable]).
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
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// deferredCheckFailedMarker is the grep-stable marker on a commit refused by
// a deferred constraint's re-check. It fires only where the re-check runs:
// an apply role WITHOUT the replica privilege.
const deferredCheckFailedMarker = "DEFERRED-KEY-CHECK-FAILED-AT-COMMIT"

// deferredCheckOffMarker is the grep-stable marker on the one-time WARN that
// a table's deferrable constraint is not re-checked under replica mode.
const deferredCheckOffMarker = "DEFERRED-KEY-CHECK-OFF-IN-REPLICA-MODE"

// The SQLSTATEs a deferred re-check raises at COMMIT.
const (
	pgUniqueViolation    = "23505"
	pgExclusionViolation = "23P01"
)

// tableKeyShape is what the applier needs to know about a TARGET table's keys
// for GC-42.
type tableKeyShape struct {
	// keyed reports a PRIMARY KEY or a NOT NULL, non-partial, non-expression
	// unique index — of ANY deferrability: a deferrable key still identifies
	// one row once its transaction commits, which is exactly the promise a
	// multi-row match breaks.
	keyed bool

	// deferrable names the table's DEFERRABLE primary key, unique and
	// exclusion constraints, comma-joined; empty when there are none.
	deferrable string
}

// keyShapeEntry is a cached [tableKeyShape] with the time it was read.
type keyShapeEntry struct {
	shape    tableKeyShape
	loadedAt time.Time
}

// keyShapeTTL bounds how long a cached [tableKeyShape] is trusted. A schema
// boundary drops the entry outright ([ChangeApplier.invalidateMetadataCaches]);
// the TTL covers what no stream event announces — an operator adding or
// dropping a key on the target mid-stream.
var keyShapeTTL = 30 * time.Second

// tableKeyShapeSQL reads [tableKeyShape] from the TARGET catalog in one round
// trip. The deferrable list reads pg_constraint.condeferrable for p/u/x and,
// as a cross-check that does not depend on a constraint row existing, every
// unique or exclusion index whose pg_index.indimmediate is false.
const tableKeyShapeSQL = `
	SELECT
		EXISTS (
			SELECT 1 FROM pg_index ix
			WHERE ix.indrelid = c.oid
			  AND (ix.indisprimary OR (
			       ix.indisunique AND ix.indpred IS NULL AND ix.indexprs IS NULL
			       AND NOT EXISTS (
			           SELECT 1 FROM pg_attribute a
			           WHERE a.attrelid = ix.indrelid
			             AND a.attnum = ANY (ix.indkey::int2[])
			             AND NOT a.attnotnull)))
		),
		COALESCE((
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

// keyShapeFor returns the cached [tableKeyShape] for a routed table, reading
// the target catalog on a miss or an expired entry, and names an unenforced
// deferrable constraint the first time it sees one. A probe error is
// returned, never defaulted: guessing "keyless" would narrow G1 — the silent
// direction. A table the catalog does not have reads as the zero shape; the
// dispatch arms resolve that case first (the C-11 skip).
func (a *ChangeApplier) keyShapeFor(ctx context.Context, schema, table string) (tableKeyShape, error) {
	qn := schemaTableKey(schema, table)
	if s, ok := a.cachedKeyShape(qn); ok {
		return s, nil
	}
	var s tableKeyShape
	err := a.db.QueryRowContext(ctx, tableKeyShapeSQL, schema, table).Scan(&s.keyed, &s.deferrable)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return tableKeyShape{}, fmt.Errorf("postgres: applier: read key shape of %s: %w", qn, err)
	}
	a.storeKeyShape(qn, s)
	a.noteUnenforcedDeferrable(ctx, qn, s)
	return s, nil
}

// cachedKeyShape is the cacheMu-guarded read of keyShapeCache (see the
// accessor note in change_applier_concurrent.go); an entry older than
// [keyShapeTTL] reads as a miss.
func (a *ChangeApplier) cachedKeyShape(qn string) (tableKeyShape, bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	e, ok := a.keyShapeCache[qn]
	if !ok || time.Since(e.loadedAt) >= keyShapeTTL {
		return tableKeyShape{}, false
	}
	return e.shape, true
}

func (a *ChangeApplier) storeKeyShape(qn string, s tableKeyShape) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.keyShapeCache == nil {
		a.keyShapeCache = make(map[string]keyShapeEntry)
	}
	a.keyShapeCache[qn] = keyShapeEntry{shape: s, loadedAt: time.Now()}
}

// markWarnedDeferredOff records that the deferred-check WARN has been emitted
// for qn and reports whether THIS call recorded it, so it fires once per
// table even under concurrent lanes.
func (a *ChangeApplier) markWarnedDeferredOff(qn string) (firstTime bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.warnedDeferredOff == nil {
		a.warnedDeferredOff = make(map[string]bool)
	}
	if a.warnedDeferredOff[qn] {
		return false
	}
	a.warnedDeferredOff[qn] = true
	return true
}

// noteUnenforcedDeferrable WARNs, once per table, that a deferrable
// constraint on it is not re-checked during apply: the applier runs in
// replica mode, which switches the re-check off. A role without the replica
// privilege never leaves origin, so its checks fire and there is nothing to
// say. Detection only — see the file header for why apply is not changed.
func (a *ChangeApplier) noteUnenforcedDeferrable(ctx context.Context, qn string, s tableKeyShape) {
	if s.deferrable == "" || !a.foreignKeyBypassAvailable(ctx) || !a.markWarnedDeferredOff(qn) {
		return
	}
	slog.WarnContext(ctx,
		"postgres: applier: "+deferredCheckOffMarker+": this target table's DEFERRABLE constraint is NOT re-checked "+
			"while sluice applies changes — the apply transactions run with session_replication_role=replica (the FK "+
			"bypass), which switches PostgreSQL's deferred uniqueness/exclusion check off; rows are applied exactly as "+
			"the source sent them, so if the target's constraint is stricter than the source's (the source lacks it) the "+
			"table can hold rows the constraint forbids, and REINDEX TABLE would fail on them",
		slog.String("table", qn),
		slog.String("deferrable_constraints", s.deferrable),
		slog.String("hint", "give the target the same constraint as the source (if the source has no such constraint, "+
			"drop it on the target or make it NOT DEFERRABLE so a violating row is refused at its statement); "+
			"`REINDEX TABLE <table>` checks whether any violating row already landed"))
}

// keyScopedWrite is the G1 check carried by a queued UPDATE/DELETE, read
// against the statement's command tag at flush time.
type keyScopedWrite struct {
	op     string // "update" / "delete"
	before ir.Row
}

// guardKeyScopedWrite returns the G1 check for a write, or nil when it is
// exempt ([appliershared.KeyScopedWriteExempt]: a keyless table addressed by
// its whole row).
func guardKeyScopedWrite(s tableKeyShape, op string, before ir.Row, colTypes map[string]*ir.Column) *keyScopedWrite {
	if appliershared.KeyScopedWriteExempt(s.keyed, before, colTypes) {
		return nil
	}
	return &keyScopedWrite{op: op, before: before}
}

// check refuses a key-scoped write that matched more than one row. A nil
// check (exempt) and zero or one row pass.
func (k *keyScopedWrite) check(schema, table string, matched int64) error {
	if k == nil || matched <= 1 {
		return nil
	}
	return appliershared.RefuseKeyScopedMultiMatch(engineNamePostgres, k.op, schema, table, k.before, matched)
}

// checkKeyScopedResult is G1 on the serial path: it reads the exec's
// affected-row count and refuses more than one. pgx's stdlib driver always
// reports the count (from the command tag), so a read error is a driver
// change the guard must not paper over, and is returned.
func checkKeyScopedResult(s tableKeyShape, op, schema, table string, before ir.Row, colTypes map[string]*ir.Column, res sql.Result) error {
	k := guardKeyScopedWrite(s, op, before, colTypes)
	if k == nil {
		return nil
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: applier: read rows affected by %s on %s.%s: %w", op, schema, table, err)
	}
	return k.check(schema, table, n)
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
