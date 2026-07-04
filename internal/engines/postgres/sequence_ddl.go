// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// Standalone-sequence emission (item-51 TRIAGE finding #1, the writer
// half of sequence_reader.go). Sequences are created BEFORE tables —
// a carried `DEFAULT nextval('seq')` column fails CREATE TABLE if the
// sequence doesn't exist yet — and their OWNED BY bindings are applied
// AFTER tables, once the owning columns exist.

// createSequences emits CREATE SEQUENCE + the setval position prime
// for every standalone sequence in s. A sequence that already exists
// on the target is skipped entirely — including the prime, so a
// re-run (resume, add-table) can never rewind a sequence the target
// has advanced since. Mirrors the idempotency posture of the enum
// phase's guarded CREATE TYPE (Bug 154).
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
			slog.Debug("postgres: sequence already exists on target; skipping create + position prime",
				slog.String("sequence", seq.Name))
			continue
		}
		if _, err := w.db.ExecContext(ctx, emitCreateSequence(w.schema, seq)); err != nil {
			return fmt.Errorf("postgres: create sequence %q: %w", seq.Name, err)
		}
		if err := w.primeSequencePosition(ctx, seq); err != nil {
			return err
		}
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

// primeSequencePosition replays the source's captured position onto
// the just-created sequence so post-migration nextval() continues
// exactly where the source would — without it, the target restarts at
// Start and re-issues numbers the copied rows already consumed.
// Fixtures without a captured position (LastValueValid=false, the
// zero value) skip the prime and the sequence starts fresh at Start.
func (w *SchemaWriter) primeSequencePosition(ctx context.Context, seq *ir.Sequence) error {
	if !seq.LastValueValid {
		return nil
	}
	regclass := quoteSQLString(quoteIdent(w.schema) + "." + quoteIdent(seq.Name))
	q := fmt.Sprintf("SELECT setval(%s, $1, $2)", regclass)
	if _, err := w.db.ExecContext(ctx, q, seq.LastValue, seq.LastValueIsCalled); err != nil {
		return fmt.Errorf("postgres: prime sequence %q to %d (is_called=%t): %w",
			seq.Name, seq.LastValue, seq.LastValueIsCalled, err)
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
