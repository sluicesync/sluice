// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
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
// pkColumns + streamID together produce the per-(row, column) seed
// passed to randomize:* strategies (PII Phase 2.c, v0.59.0). When
// pkColumns is empty (the table has no primary key) and any
// registered rule for this table is a randomize:* strategy, the
// strategy refuses with an operator-actionable error — replay-stable
// randomization requires a PK. streamID may be empty (migrate path).
// Phase 1 and Phase 2.a / 2.b strategies ignore the seed entirely.
//
// Wrapping errors: a strategy refusal (e.g. Null on NOT NULL,
// Truncate on a non-string column) returns a wrapped error naming
// the schema + table + column. Callers should treat redaction
// errors as terminal (operator misconfiguration); the pipeline's
// existing fail-fast posture is the right default.
func redactRow(reg *redact.Registry, schema, table string, row ir.Row, cols []*ir.Column, pkColumns []string, streamID string) error {
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
		seed := deriveSeedIfNeeded(strategy, table, col.Name, row, pkColumns, streamID)
		newVal, err := strategy.Redact(col, val, seed)
		if err != nil {
			return fmt.Errorf("pipeline: redact %s.%s.%s via %s: %w",
				schema, table, col.Name, strategy.Name(), err)
		}
		row[col.Name] = newVal
	}
	return nil
}

// deriveSeedIfNeeded returns a per-row [redact.DeriveRowSeed] result
// when strategy is a randomize:* family member, or nil otherwise.
// Centralises the strategy-name probe + PK-value extraction so
// redactRow + redactRows stay readable.
//
// When pkColumns is empty for a randomize:* strategy, this still
// returns nil — the strategy's Redact then surfaces the
// "PK required" refusal with full operator detail (column name +
// strategy name). Preflight should have caught the no-PK case
// before this runs.
func deriveSeedIfNeeded(strategy redact.Strategy, table, column string, row ir.Row, pkColumns []string, streamID string) []byte {
	if strategy == nil {
		return nil
	}
	if !strings.HasPrefix(strategy.Name(), "randomize:") {
		return nil
	}
	if len(pkColumns) == 0 {
		return nil
	}
	pkValues := make([]any, len(pkColumns))
	for i, c := range pkColumns {
		pkValues[i] = row[c]
	}
	return redact.DeriveRowSeed(streamID, table, column, pkColumns, pkValues)
}

// redactRows wraps a row channel with the operator-configured
// redaction policy. Each row read from src is passed through
// [redactRow] before being forwarded to the returned channel.
//
// When reg is nil or empty, the function returns src verbatim
// (zero-cost passthrough) — no goroutine is spawned and the
// returned errFn always reports nil. This is the load-bearing
// fast path for the no-redactions case so default operators pay
// nothing for the feature.
//
// When at least one rule is registered, a goroutine reads from
// src, applies redactRow in-place, and forwards. On a redaction
// error (strategy refusal), the goroutine:
//
//  1. Stores the error so errFn returns it.
//  2. Closes the output channel.
//  3. Returns. The downstream consumer (writer / applier) sees
//     the channel close and exits cleanly; the caller checks
//     errFn after that exit to surface the redaction error.
//
// The error-propagation contract is *intentionally* not via ctx
// cancellation: a downstream WriteRows seeing a redacted channel
// close cleanly is a legitimate "no more rows" signal; the caller
// disambiguates by checking errFn() AFTER WriteRows returns nil.
//
//nolint:gocritic // named results would conflict with the bidi `out` channel built internally
func redactRows(
	ctx context.Context,
	src <-chan ir.Row,
	reg *redact.Registry,
	schema, table string,
	cols []*ir.Column,
	pkColumns []string,
	streamID string,
) (<-chan ir.Row, func() error) {
	if reg.Empty() {
		return src, func() error { return nil }
	}
	out := make(chan ir.Row)
	var (
		mu    sync.Mutex
		fnErr error
	)
	storeFn := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if fnErr == nil {
			fnErr = err
		}
	}
	errFn := func() error {
		mu.Lock()
		defer mu.Unlock()
		return fnErr
	}
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case row, ok := <-src:
				if !ok {
					return
				}
				if err := redactRow(reg, schema, table, row, cols, pkColumns, streamID); err != nil {
					storeFn(err)
					return
				}
				select {
				case out <- row:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, errFn
}

// tablePKColumns returns the PK column names of table in declaration
// order, or nil for tables without a primary key. Mirrors
// [primaryKeyColumnNames] in migrate_bulk.go; lives here because
// callers in non-bulk paths (copyTable, backup, etc.) also need
// it for randomize:* seed plumbing. Defensive on nil table.
func tablePKColumns(table *ir.Table) []string {
	if table == nil || table.PrimaryKey == nil {
		return nil
	}
	out := make([]string, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		out[i] = c.Column
	}
	return out
}
