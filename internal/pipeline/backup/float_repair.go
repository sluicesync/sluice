// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// # VStream-COPY FLOAT display-rounding repair — the backup path
//
// A `backup full` on a PlanetScale/Vitess (VStream) source archives rows
// from vttablet's rowstreamer, which renders single-precision FLOAT at
// mysqld's 6-significant-digit display precision (8388608 → 8388610). By
// DEFAULT sluice archives EXACT float32: for each FLOAT-bearing table with
// a usable PK it re-reads those columns exactly over a separate SQL scan
// (the ADR-0153 `(col * 1E0)` projection) into a per-table PK→floats map,
// then PATCHES each streamed COPY row's FLOAT columns from the map before
// the row is archived. Non-FLOAT columns keep their COPY-snapshot values.
//
// Position correctness is UNAFFECTED: the exact scan is a separate read
// connection and never touches the recorded snapshot VGTID / chain
// EndPosition.
//
// The wart (named): a bounded WITHIN-ROW temporal skew. The exact FLOAT
// value reflects a read instant slightly AFTER the snapshot VGTID, so a
// FLOAT row that CHANGED during the read window carries a FLOAT column
// newer than the rest of its (VGTID-snapshot) columns. This skew is ZERO
// on a quiescent source; it SELF-HEALS on a chain restore because the
// incrementals replay from the full's recorded position (EndPosition =
// the COPY VGTID) FORWARD, re-applying every post-VGTID change (idempotent
// upsert) so the row converges; and it persists only for a STANDALONE-full
// restore of a source with concurrent FLOAT writes — where a logical
// VStream snapshot is already per-shard-fuzzy, not a global instant.
// `--no-float-exact-reread` keeps the rounded-but-perfectly-consistent
// archive; `--strict-float` refuses instead.
//
// Memory: the patch map holds PK + FLOAT values for ONE table (the VStream
// backup sweep is serial — TableParallelism engages only for a shareable
// exported snapshot, which VStream is not), and is freed after the table.
// Only the repairable FLOAT tables build a map; keyless FLOAT tables pass
// through rounded (WARNed). `--no-float-exact-reread` avoids the buffer
// entirely.

// floatPatchTable is one table's exact-re-read plan for the backup patch.
type floatPatchTable struct {
	// srcRead is the trimmed SOURCE table (PK + repairable FLOAT columns,
	// SOURCE types) driving the exact `(col * 1E0)` cursor scan.
	srcRead *ir.Table
	// pkCols are the PK column names, in PK order (the patch-map key).
	pkCols []string
	// floatCols are the non-PK single-precision FLOAT columns to patch.
	floatCols []string
}

// floatExactPatchReader wraps the VStream backup snapshot [ir.RowReader]
// and patches single-precision FLOAT columns with exact values re-read
// from the source. A table not in plan (no repairable FLOAT column) streams
// through unchanged.
type floatExactPatchReader struct {
	inner     ir.RowReader
	source    ir.Engine
	sourceDSN string
	plan      map[string]floatPatchTable
	batchSize int

	mu  sync.Mutex
	err error
}

func newFloatExactPatchReader(inner ir.RowReader, source ir.Engine, sourceDSN string, plan map[string]floatPatchTable) *floatExactPatchReader {
	return &floatExactPatchReader{
		inner:     inner,
		source:    source,
		sourceDSN: sourceDSN,
		plan:      plan,
		batchSize: migcore.DefaultBulkBatchSize,
	}
}

// ReadRows patches the FLOAT columns of a repairable table's rows with
// exact source values; other tables stream through unchanged.
func (r *floatExactPatchReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	p, ok := r.plan[table.Name]
	if !ok {
		return r.inner.ReadRows(ctx, table)
	}
	exact, err := r.buildExactMap(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("backup: float re-read %q: %w", table.Name, err)
	}
	inCh, err := r.inner.ReadRows(ctx, table)
	if err != nil {
		return nil, err
	}
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for row := range inCh {
			if floats, ok := exact[floatPatchKey(row, p.pkCols)]; ok {
				for col, v := range floats {
					row[col] = v
				}
			}
			select {
			case out <- row:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Err surfaces the underlying reader's streaming error plus any exact-scan
// error captured during a ReadRows patch.
func (r *floatExactPatchReader) Err() error {
	r.mu.Lock()
	e := r.err
	r.mu.Unlock()
	if e != nil {
		return e
	}
	return r.inner.Err()
}

// buildExactMap cursor-scans the source for (PK, FLOAT) tuples and returns
// a map from the PK key to the exact FLOAT column values. Bounded to this
// one table's PK+float footprint (serial sweep).
func (r *floatExactPatchReader) buildExactMap(ctx context.Context, p floatPatchTable) (map[string]map[string]any, error) {
	rr, err := r.source.OpenRowReader(ctx, r.sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("open source reader: %w", err)
	}
	defer migcore.CloseIf(rr)
	br, ok := rr.(ir.BatchedRowReader)
	if !ok {
		return nil, fmt.Errorf("source reader does not support cursor-paginated reads")
	}

	exact := make(map[string]map[string]any)
	var cursor []any
	for {
		batchCtx, cancel := context.WithCancel(ctx)
		rowsCh, err := br.ReadRowsBatch(batchCtx, p.srcRead, cursor, r.batchSize)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("read batch: %w", err)
		}
		var last []any
		n := 0
		for row := range rowsCh {
			floats := make(map[string]any, len(p.floatCols))
			for _, c := range p.floatCols {
				floats[c] = row[c]
			}
			exact[floatPatchKey(row, p.pkCols)] = floats
			last = pkValues(row, p.pkCols)
			n++
		}
		cancel()
		if serr := migcore.ReaderStreamErr(rr, p.srcRead); serr != nil {
			return nil, serr
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		cursor = last
	}
	return exact, nil
}

// floatPatchKey renders a collision-safe key from a row's PK values, in PK
// column order. A NUL separator keeps distinct tuples distinct across the
// string/int families a PK can mix.
func floatPatchKey(row ir.Row, pkCols []string) string {
	var b strings.Builder
	for i, c := range pkCols {
		if i > 0 {
			b.WriteByte(0)
		}
		fmt.Fprintf(&b, "%v", row[c])
	}
	return b.String()
}

func pkValues(row ir.Row, pkCols []string) []any {
	vals := make([]any, len(pkCols))
	for i, c := range pkCols {
		vals[i] = row[c]
	}
	return vals
}

// planBackupFloatRepair builds the per-table exact-re-read plan for the
// backup patch: every table with a usable PK AND at least one non-PK
// single-precision FLOAT column. Keyless / float-PK-only tables are
// omitted (they cannot be patched — the caller WARNs and archives rounded).
func planBackupFloatRepair(schema *ir.Schema) map[string]floatPatchTable {
	plan := make(map[string]floatPatchTable)
	for _, t := range schema.Tables {
		floatCols := migcore.SinglePrecisionFloatColumns(t)
		if len(floatCols) == 0 {
			continue
		}
		pkCols := migcore.PrimaryKeyColumnNames(t)
		if len(pkCols) == 0 {
			continue
		}
		pkSet := make(map[string]struct{}, len(pkCols))
		for _, c := range pkCols {
			pkSet[c] = struct{}{}
		}
		var repair []string
		for _, c := range floatCols {
			if _, isPK := pkSet[c.Name]; !isPK {
				repair = append(repair, c.Name)
			}
		}
		if len(repair) == 0 {
			continue
		}
		plan[t.Name] = floatPatchTable{
			srcRead:   trimmedBackupReadTable(t, pkCols, repair),
			pkCols:    pkCols,
			floatCols: repair,
		}
	}
	return plan
}

// trimmedBackupReadTable builds the SOURCE-typed table the exact scan
// reads: PK columns + the repairable FLOAT columns, each shallow-copied so
// the captured FloatSingle type is independent of any later schema mutation.
func trimmedBackupReadTable(src *ir.Table, pkCols, floatCols []string) *ir.Table {
	want := make(map[string]struct{}, len(pkCols)+len(floatCols))
	for _, c := range pkCols {
		want[c] = struct{}{}
	}
	for _, c := range floatCols {
		want[c] = struct{}{}
	}
	cols := make([]*ir.Column, 0, len(want))
	for _, c := range src.Columns {
		if _, ok := want[c.Name]; ok {
			cp := *c
			cols = append(cols, &cp)
		}
	}
	trimmed := &ir.Table{Schema: src.Schema, Name: src.Name, Columns: cols}
	if src.PrimaryKey != nil {
		pk := *src.PrimaryKey
		trimmed.PrimaryKey = &pk
	}
	return trimmed
}
