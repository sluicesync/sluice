// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
)

// copyTableIdempotent is the add-table variant of [copyTable]: it
// routes the row stream through [ir.IdempotentRowWriter.WriteRowsIdempotent]
// when the writer exposes it (INSERT ... ON CONFLICT (pk) DO UPDATE),
// falling back to plain [ir.RowWriter.WriteRows] otherwise. See
// [runBulkCopyForAddTable] for the v0.24.0 → Phase B fix rationale.
//
// Goroutine-lifecycle handling mirrors [copyTable] exactly — same
// child-ctx + defer-cancel shape so the row reader unwinds cleanly
// on error.
func copyTableIdempotent(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table, redactor *redact.Registry, streamID string) (retErr error) {
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	// PII Phase 1: same wrap as [copyTable] — nil/empty Registry
	// short-circuits to pass-through.
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, migcore.TablePKColumns(table), streamID)
	idem, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		slog.DebugContext(
			ctx, "add-table: row writer does not implement IdempotentRowWriter; falling back to plain WriteRows (the [publication-add, snapshot-open] overlap window may surface as a duplicate-key error under sustained load)",
			slog.String("table", table.Name),
		)
		if err := rw.WriteRows(copyCtx, table, redacted); err != nil {
			return fmt.Errorf("write rows: %w", err)
		}
		if err := redactErrFn(); err != nil {
			return fmt.Errorf("redact rows: %w", err)
		}
		// The writer drained the stream; surface any sticky reader
		// error so a mid-stream decode abort fails loudly (Bug 68).
		return migcore.ReaderStreamErr(rr, table)
	}
	if err := idem.WriteRowsIdempotent(copyCtx, table, redacted); err != nil {
		return fmt.Errorf("write rows (idempotent): %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	return migcore.ReaderStreamErr(rr, table)
}

// copyTable opens the source-side row stream, hands it off to the
// target writer, and waits for completion. The reader's lifetime
// covers exactly one table; the writer is reused across tables.
//
// A [progressTicker] sits in the pipe between reader and writer: it
// counts every row the orchestrator hands to the writer and emits a
// slog line every [progressInterval]. Stop is called via defer so
// progress reporting terminates even on writer error.
//
// Goroutine lifecycle on the error path (Bug 9): the row reader
// (e.g. postgres/row_reader.go::stream) and the tee both block on
// "out <- row" with a select on ctx.Done(). When WriteRows returns
// an error, neither goroutine has any reason to unwind on its own —
// the writer abandoned its consumer end of the channel, but the
// parent ctx is still alive (the caller may want to continue with
// other phases). Without an explicit cancel, both goroutines wedge
// forever; on a Postgres source that means the snapshot transaction
// never commits and PG shows "idle in transaction" sessions.
//
// The fix: derive a child context that's cancelled regardless of
// outcome (defer cancel). The reader and tee see ctx.Done() fire,
// drop their pending sends, and exit cleanly. The parent ctx is
// untouched, so the orchestrator can decide what to do next.
//

func copyTable(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table, redactor *redact.Registry, shard ShardColumnSpec) (retErr error) {
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	// Async row-count for ETA reporting. Best-effort: failures are
	// logged at warn level and the ETA stays unknown for the table's
	// duration. The engine row readers' [ir.RowCounter] implementations
	// short-circuit to (0, nil) on snapshot-pinned readers (single
	// *sql.Conn) so the streamer's snapshot path doesn't deadlock
	// against the in-flight row stream. See progress.go for the full
	// semantics.
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	// PII Phase 1: wrap the row stream with redaction if the operator
	// has configured rules. nil/empty Registry is a zero-cost
	// passthrough — redactRows returns the teed channel verbatim.
	//
	// streamID is empty for migrate runs (Migrator has no stream
	// identity); randomize:* strategies produce stable-per-row outputs
	// within a single migrate run because the seed is fully determined
	// by table + column + PK values. PK-less tables refuse on a
	// randomize:* rule via the strategy's own seed-required check;
	// preflight catches the same case earlier with a richer message.
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, migcore.TablePKColumns(table), "")
	// ADR-0048 Shape A: stamp the operator-supplied discriminator
	// onto every row before the writer sees it (the value half
	// of DP-1's two-surface split; sibling to the optional
	// ShardColumnSetter on the CDC path). shardStampRows is a
	// zero-cost passthrough when shard.Name is empty.
	stamped, _ := shardStampRows(copyCtx, redacted, shard.Name, shard.Value)
	if err := rw.WriteRows(copyCtx, table, stamped); err != nil {
		return fmt.Errorf("write rows: %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	// The writer returned without error, but it may have observed a
	// truncated stream because the reader aborted mid-table on a
	// scan/decode failure. Surface that loudly (Bug 68).
	return migcore.ReaderStreamErr(rr, table)
}

// copyTableColdStartIdempotent is the upsert-form of [copyTable] used
// when the snapshot reader declares [ir.IdempotentCopyReader] (Bug 125
// — the MySQL VStream COPY phase re-emits rows and can deliver them out
// of PK order). Routes the row stream through
// [ir.IdempotentRowWriter.WriteRowsIdempotent] so the re-emissions
// absorb via ON DUPLICATE KEY UPDATE / ON CONFLICT instead of colliding
// on a unique key.
//
// Goroutine-lifecycle, redaction, shard-stamping, and the Bug-68
// loud-failure gate are identical to [copyTable] — only the write call
// differs. A target writer that doesn't implement [ir.IdempotentRowWriter]
// is a loud refusal rather than a silent fallback: the reader has
// declared its rows NEED idempotent writes, so falling back to plain
// INSERT would re-introduce the duplicate-key collision the dedup
// removal was meant to fix. (Both shipping target engines implement
// the surface; this guards a future engine that forgets it.)
func copyTableColdStartIdempotent(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table, redactor *redact.Registry, shard ShardColumnSpec) (retErr error) {
	idem, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		return fmt.Errorf(
			"pipeline: table %q: snapshot reader requires an idempotent bulk-copy writer "+
				"(VStream COPY re-emits rows, Bug 125) but the target row writer does not "+
				"implement IdempotentRowWriter",
			table.Name,
		)
	}

	// Bug 125 cross-engine guard. A NO-PRIMARY-KEY table relies on the
	// writer upserting on a unique key to absorb VStream's COPY catchup
	// re-emissions. A writer whose idempotent path plain-INSERTs no-PK
	// tables (the Postgres target today) would DUPLICATE those rows now
	// that the source-side dedup is gone — refuse loudly rather than
	// silently corrupt. PK tables are safe on any idempotent writer
	// (ON CONFLICT / ON DUPLICATE KEY on the PK absorbs re-emissions),
	// so the guard only fires for PK-less tables.
	if len(migcore.TablePKColumns(table)) == 0 {
		icw, capable := idem.(ir.IdempotentCopyWriter)
		if !capable || !icw.HandlesNoPKIdempotentCopy() {
			return fmt.Errorf(
				"pipeline: table %q has no PRIMARY KEY and the target's idempotent bulk-copy "+
					"writer does not support no-PK upsert (VStream COPY re-emits rows out of order, "+
					"Bug 125; a plain INSERT would duplicate them). Add a PRIMARY KEY to the source "+
					"table, or migrate it with `migrate` instead of `sync start`",
				table.Name,
			)
		}
	}

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, migcore.TablePKColumns(table), "")
	stamped, _ := shardStampRows(copyCtx, redacted, shard.Name, shard.Value)
	if err := idem.WriteRowsIdempotent(copyCtx, table, stamped); err != nil {
		return fmt.Errorf("write rows (idempotent): %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	return migcore.ReaderStreamErr(rr, table)
}
