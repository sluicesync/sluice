// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
)

// redactRow applies the operator-configured redaction strategies to
// the row's per-column values. PII Phase 1 (roadmap item 15a; GitHub
// issue #24); see [docs/dev/notes/prep-pii-redaction-phase-1.md] for
// the design rationale.
//
// reg == nil or reg.Empty() returns the input row verbatim with
// zero copying and zero allocations — the no-redactions hot path
// stays free.
//
// When at least one rule is registered, the function iterates the
// table's column list looking up each column name in the registry;
// matched columns get their value replaced by the strategy's
// `Redact` return. Columns without a matching rule pass through
// verbatim. The row map is mutated in place (not copied) because
// the row is owned by the caller's read goroutine and not aliased
// after this call.
//
// Wrapping errors: a strategy refusal (e.g. Null on NOT NULL,
// Truncate on a non-string column) returns a wrapped error naming
// the schema + table + column. Callers should treat redaction
// errors as terminal (operator misconfiguration); the pipeline's
// existing fail-fast posture is the right default.
func redactRow(reg *redact.Registry, schema, table string, row ir.Row, cols []*ir.Column) error {
	if reg.Empty() {
		return nil
	}
	for _, col := range cols {
		if col == nil {
			continue
		}
		strategy := reg.Get(schema, table, col.Name)
		if strategy == nil {
			continue
		}
		val := row[col.Name]
		newVal, err := strategy.Redact(col, val)
		if err != nil {
			return fmt.Errorf("pipeline: redact %s.%s.%s via %s: %w",
				schema, table, col.Name, strategy.Name(), err)
		}
		row[col.Name] = newVal
	}
	return nil
}
