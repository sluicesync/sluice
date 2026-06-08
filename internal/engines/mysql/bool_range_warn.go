// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"log/slog"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// boolRangeWarner emits a one-time loud WARN per column when a MySQL
// TINYINT(1) (ir.Boolean) column yields a value outside {0,1} — which
// sluice's documented bool convention silently collapses to true/false,
// losing the real integer (Vector D). It is intentionally NON-fatal: the
// value is still carried as a bool per the convention, but the operator is
// told so they can re-run with `--type-override <table>.<col>=smallint` (or
// `=int`) to preserve the integer end-to-end.
//
// Warn-once is keyed by `table.column` so a full-table scan logs at most one
// line per offending column rather than one per row. A parallel chunked copy
// uses several readers, so a column may be warned a small bounded number of
// times across workers — acceptable; loud beats silent, and the message is
// idempotent.
type boolRangeWarner struct {
	mu     sync.Mutex
	warned map[string]struct{}
}

func newBoolRangeWarner() *boolRangeWarner {
	return &boolRangeWarner{warned: make(map[string]struct{})}
}

// observe warns once for col if raw is an out-of-range TINYINT(1) value.
// table is the source table name (for the `--type-override table.col`
// hint); col must be the ir.Boolean column being decoded. Callers should
// gate the call on the column actually being ir.Boolean, but observe also
// no-ops for in-range / non-integer raws so an unconditional call is safe.
func (w *boolRangeWarner) observe(table string, col *ir.Column, raw any) {
	n, oob := tinyBoolOutOfRange(raw)
	if !oob {
		return
	}
	key := table + "." + col.Name

	w.mu.Lock()
	_, seen := w.warned[key]
	if !seen {
		w.warned[key] = struct{}{}
	}
	w.mu.Unlock()
	if seen {
		return
	}

	slog.Warn(
		"mysql: TINYINT(1) column holds a value outside {0,1}; sluice maps TINYINT(1) to boolean "+
			"per MySQL convention, so this and any other non-0/1 values in the column are collapsed "+
			"to true and the integer is lost",
		slog.String("column", key),
		slog.Int64("example_value", n),
		slog.String("hint", "re-run with --type-override "+key+"=smallint (or =int) to preserve the integer value"),
	)
}
