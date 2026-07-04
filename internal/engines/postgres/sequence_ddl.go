// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// Standalone-sequence emission (item-51 TRIAGE finding #1, the writer
// half of sequence_reader.go). Sequences are created BEFORE tables —
// a carried `DEFAULT nextval('seq')` column fails CREATE TABLE if the
// sequence doesn't exist yet — and their OWNED BY bindings are applied
// AFTER tables, once the owning columns exist.

// createSequences establishes every standalone sequence in s on the
// target, in one of two modes per sequence:
//
//   - Missing on the target → CREATE SEQUENCE + the setval position
//     prime in ONE transaction. Atomicity is load-bearing (delta
//     review finding #1): as two autocommit statements, a crash
//     between them left a created-but-unprimed sequence that a naive
//     resume skipped entirely — post-cutover nextval() then re-issued
//     numbers the copied rows already held, the exact silent class
//     this feature exists to close. Sequence DDL is transactional in
//     PG, so the pair commits or vanishes together.
//   - Already exists on the target (resume re-run, operator
//     pre-created, chain-restore tail) → FORWARD-ONLY RE-PRIME:
//     setval to the captured source position when the target sits
//     BEHIND it in the sequence's direction, and never otherwise.
//     Rewind-safe by construction — an advanced target sequence is
//     never regressed — while the crashed-before-prime window and the
//     pre-created-unprimed shape both heal. Every exists-path outcome
//     is logged at WARN naming the sequence and both positions: a
//     pre-existing sequence on a migrate target is surprising enough
//     that the operator should see what sluice decided about it.
func (w *SchemaWriter) createSequences(ctx context.Context, s *ir.Schema) error {
	for _, seq := range s.Sequences {
		if seq == nil {
			continue
		}
		if seq.Name == "" || seq.Increment == 0 {
			// A sluice-bug condition, not operator data: the PG reader
			// always populates both (PG rejects INCREMENT 0 at source).
			return fmt.Errorf("postgres: create sequence: malformed IR sequence (name=%q increment=%d)",
				seq.Name, seq.Increment)
		}
		exists, err := w.sequenceExists(ctx, seq.Name)
		if err != nil {
			return fmt.Errorf("postgres: probe sequence %q: %w", seq.Name, err)
		}
		if exists {
			if err := w.reprimeExistingSequence(ctx, seq); err != nil {
				return err
			}
			continue
		}
		if err := w.createAndPrimeSequence(ctx, seq); err != nil {
			return err
		}
	}
	return nil
}

// ReprimeSequences forward-only re-primes (creating if missing) every
// standalone sequence in s. Exported for the pipeline's chain-restore
// tail (delta review finding #2): the chain's base full primes
// sequences at the BASE manifest's captured position, then incremental
// links apply rows that consumed later values — without a tail
// re-prime from the newest link's schema, the restored sequence
// silently re-issues every number the links consumed. Forward-only
// semantics make the call safe at any point: it can only advance a
// lagging sequence, never rewind one.
func (w *SchemaWriter) ReprimeSequences(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return nil
	}
	return w.createSequences(ctx, s)
}

// createAndPrimeSequence runs CREATE SEQUENCE + the position prime in
// a single transaction (see createSequences for why atomicity is
// load-bearing). Fixtures without a captured position
// (LastValueValid=false, the zero value) create unprimed and start
// fresh at Start.
func (w *SchemaWriter) createAndPrimeSequence(ctx context.Context, seq *ir.Sequence) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin create-sequence tx for %q: %w", seq.Name, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, emitCreateSequence(w.schema, seq)); err != nil {
		return fmt.Errorf("postgres: create sequence %q: %w", seq.Name, err)
	}
	if seq.LastValueValid {
		if err := w.setvalSequence(ctx, tx, seq, seq.LastValue, seq.LastValueIsCalled); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres: commit create-sequence tx for %q: %w", seq.Name, err)
	}
	return nil
}

// reprimeExistingSequence is the exists-path half of createSequences:
// forward-only setval when the target sits behind the captured source
// position, WARN-logged either way.
func (w *SchemaWriter) reprimeExistingSequence(ctx context.Context, seq *ir.Sequence) error {
	if !seq.LastValueValid {
		slog.Warn("postgres: sequence already exists on target and the IR carries no captured position; leaving it untouched",
			slog.String("sequence", seq.Name))
		return nil
	}
	targetLV, targetCalled, err := readSequencePositionOn(ctx, w.db, w.schema, seq.Name)
	if err != nil {
		return fmt.Errorf("postgres: read position of existing target sequence %q: %w", seq.Name, err)
	}
	if !sequencePositionBehind(seq.Increment, targetLV, targetCalled, seq.LastValue, seq.LastValueIsCalled) {
		slog.Warn("postgres: sequence already exists on target at or beyond the captured source position; skipping re-prime (forward-only)",
			slog.String("sequence", seq.Name),
			slog.Int64("target_last_value", targetLV),
			slog.Bool("target_is_called", targetCalled),
			slog.Int64("source_last_value", seq.LastValue),
			slog.Bool("source_is_called", seq.LastValueIsCalled))
		return nil
	}
	if err := w.setvalSequence(ctx, w.db, seq, seq.LastValue, seq.LastValueIsCalled); err != nil {
		return err
	}
	slog.Warn("postgres: sequence already existed on target BEHIND the captured source position; re-primed forward",
		slog.String("sequence", seq.Name),
		slog.Int64("target_last_value_before", targetLV),
		slog.Bool("target_is_called_before", targetCalled),
		slog.Int64("primed_last_value", seq.LastValue),
		slog.Bool("primed_is_called", seq.LastValueIsCalled))
	return nil
}

// sqlExecer is the ExecContext subset shared by *sql.DB and *sql.Tx,
// so the setval prime can run inside the create transaction or as a
// standalone forward-only re-prime.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// setvalSequence applies setval(seq, lastValue, isCalled) on execer.
func (w *SchemaWriter) setvalSequence(ctx context.Context, execer sqlExecer, seq *ir.Sequence, lastValue int64, isCalled bool) error {
	regclass := quoteSQLString(quoteIdent(w.schema) + "." + quoteIdent(seq.Name))
	q := fmt.Sprintf("SELECT setval(%s, $1, $2)", regclass)
	if _, err := execer.ExecContext(ctx, q, lastValue, isCalled); err != nil {
		return fmt.Errorf("postgres: prime sequence %q to %d (is_called=%t): %w",
			seq.Name, lastValue, isCalled, err)
	}
	return nil
}

// bindSequenceOwners applies `ALTER SEQUENCE ... OWNED BY` for every
// standalone sequence that carried an owner. Runs after the tables
// phase so the owning column exists. Idempotent — re-binding the same
// owner is a no-op on the catalog.
func (w *SchemaWriter) bindSequenceOwners(ctx context.Context, s *ir.Schema) error {
	for _, seq := range s.Sequences {
		if seq == nil || seq.OwnedByTable == "" || seq.OwnedByColumn == "" {
			continue
		}
		if _, err := w.db.ExecContext(ctx, emitAlterSequenceOwnedBy(w.schema, seq)); err != nil {
			return fmt.Errorf("postgres: bind sequence %q owner %s.%s: %w",
				seq.Name, seq.OwnedByTable, seq.OwnedByColumn, err)
		}
	}
	return nil
}

// sequenceExists probes pg_class for a relkind='S' relation with the
// given name in the writer's schema.
func (w *SchemaWriter) sequenceExists(ctx context.Context, name string) (bool, error) {
	const q = `
		SELECT EXISTS (
			SELECT 1
			FROM   pg_class     c
			JOIN   pg_namespace n ON n.oid = c.relnamespace
			WHERE  n.nspname = $1
			  AND  c.relname = $2
			  AND  c.relkind = 'S'
		)`
	var exists bool
	if err := w.db.QueryRowContext(ctx, q, w.schema, name).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// readSequencePositionOn reads a sequence's live position — last_value
// + is_called, selected from the sequence relation itself (the
// pg_sequences view hides is_called). Shared by the schema reader's
// capture, the writer's forward-only re-prime, and the cutover
// standalone-sequence prime.
func readSequencePositionOn(ctx context.Context, db *sql.DB, schema, name string) (lastValue int64, isCalled bool, err error) {
	q := fmt.Sprintf(`SELECT last_value, is_called FROM %s.%s`,
		quoteIdent(schema), quoteIdent(name))
	if err := db.QueryRowContext(ctx, q).Scan(&lastValue, &isCalled); err != nil {
		return 0, false, err
	}
	return lastValue, isCalled, nil
}

// sequencePositionBehind reports whether position (aLV, aCalled) is
// STRICTLY behind (bLV, bCalled) in the direction the sequence moves
// (increment sign). Within the same last_value, (v, is_called=false)
// — "v not yet issued" — is behind (v, is_called=true). This is the
// forward-only comparison every re-prime path gates on: a true result
// means setval to b cannot rewind a.
func sequencePositionBehind(increment, aLV int64, aCalled bool, bLV int64, bCalled bool) bool {
	if aLV != bLV {
		if increment < 0 {
			return aLV > bLV
		}
		return aLV < bLV
	}
	return !aCalled && bCalled
}

// emitCreateSequence renders the CREATE SEQUENCE statement for seq
// with EVERY option explicit. Emitting the exact catalog values
// (rather than only non-defaults) reproduces the source catalog for
// ascending and descending sequences alike without re-deriving PG's
// direction-dependent defaults — and an identical catalog is what
// makes the item-51 pg_dump parity oracle read the two sides as
// equal. `AS <type>` is emitted only when the IR carries a data type
// and it isn't the bigint default, matching pg_dump's shape.
func emitCreateSequence(schema string, seq *ir.Sequence) string {
	stmt := "CREATE SEQUENCE " + quoteIdent(schema) + "." + quoteIdent(seq.Name)
	if seq.DataType != "" && seq.DataType != "bigint" {
		stmt += " AS " + seq.DataType
	}
	stmt += fmt.Sprintf(" START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d CACHE %d",
		seq.Start, seq.Increment, seq.MinValue, seq.MaxValue, seq.Cache)
	if seq.Cycle {
		stmt += " CYCLE"
	} else {
		stmt += " NO CYCLE"
	}
	return stmt + ";"
}

// emitAlterSequenceOwnedBy renders the OWNED BY binding for a
// standalone-but-owned sequence. Shared by the apply path
// ([SchemaWriter.bindSequenceOwners]) and PreviewDDL.
func emitAlterSequenceOwnedBy(schema string, seq *ir.Sequence) string {
	return fmt.Sprintf("ALTER SEQUENCE %s.%s OWNED BY %s.%s.%s;",
		quoteIdent(schema), quoteIdent(seq.Name),
		quoteIdent(schema), quoteIdent(seq.OwnedByTable), quoteIdent(seq.OwnedByColumn))
}
