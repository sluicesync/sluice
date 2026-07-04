// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0150 byte-targeted batch composer
// (row_writer_bytebatch.go). These exercise the flush-boundary
// arithmetic directly — the byte target as the primary trigger, the
// row-count safety ceiling, the placeholder bound, oversize-row
// handling, boundary exactness, and the statement-count regression pin
// against the pre-ADR-0150 500-row behaviour. The end-to-end value
// fidelity of the composed statements is pinned by the integration
// tests in row_writer_bytebatch_integration_test.go.

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// byteBatchTable returns an ir.Table with cols non-generated columns,
// enough shape for newInsertBatcher's placeholder clamp to see the
// column count. Column types are irrelevant to the composer (it only
// counts columns and estimates row bytes).
func byteBatchTable(cols int) *ir.Table {
	t := &ir.Table{Name: "bb"}
	for i := 0; i < cols; i++ {
		t.Columns = append(t.Columns, &ir.Column{
			Name: "c" + strconv.Itoa(i),
			Type: ir.Integer{Width: 64},
		})
	}
	return t
}

// narrowRow is the regression fixture's narrow row: ~29 estimated bytes
// (int64 8 + 20-char string + bool 1) across 3 columns — the shape that
// used to cap at 500-row / ~15 KB statements.
func narrowRow() ir.Row {
	return ir.Row{
		"id":   int64(1),
		"s":    strings.Repeat("s", 20),
		"flag": true,
	}
}

// TestInsertBatcher_ByteTargetIsPrimaryTrigger pins that with 1 KiB
// rows the batch fills at the ~1 MiB byte target — well before the
// 10k-row safety ceiling — and that the accumulated estimate lands
// within one row of the target (the flush fires on the first row that
// reaches it; nothing overshoots by more than that row).
func TestInsertBatcher_ByteTargetIsPrimaryTrigger(t *testing.T) {
	w := &RowWriter{}
	b := w.newInsertBatcher(byteBatchTable(2))

	const rowBytes = 1024
	row := ir.Row{"id": int64(1), "payload": strings.Repeat("p", rowBytes-8)}

	n := 0
	for !b.full() {
		b.add(row)
		n++
		if n > defaultMaxRowsPerBatch {
			t.Fatalf("batch never filled after %d rows", n)
		}
	}
	wantRows := int(defaultStatementByteTarget) / rowBytes // 1024
	if n != wantRows {
		t.Errorf("filled at %d rows; want %d (byte target / row estimate)", n, wantRows)
	}
	if b.bytes < defaultStatementByteTarget || b.bytes >= defaultStatementByteTarget+rowBytes {
		t.Errorf("accumulated estimate %d outside [target, target+row) = [%d, %d)",
			b.bytes, defaultStatementByteTarget, defaultStatementByteTarget+int64(rowBytes))
	}
	if n >= defaultMaxRowsPerBatch {
		t.Errorf("row ceiling bound first (%d rows); the byte target must be the primary trigger", n)
	}
}

// TestInsertBatcher_RowCeilingStillBinds pins both ceilings: the
// configured maxRowsPerBatch override (the test seam the integration
// suite uses) and the raised default, for rows so narrow the byte
// target never fires.
func TestInsertBatcher_RowCeilingStillBinds(t *testing.T) {
	t.Run("configured override", func(t *testing.T) {
		w := &RowWriter{maxRowsPerBatch: 100}
		b := w.newInsertBatcher(byteBatchTable(2))
		for i := 0; i < 99; i++ {
			b.add(ir.Row{"a": true})
		}
		if b.full() {
			t.Fatalf("full at 99 rows; want the configured 100-row ceiling")
		}
		b.add(ir.Row{"a": true})
		if !b.full() {
			t.Fatalf("not full at 100 rows; the configured ceiling must bind")
		}
	})
	t.Run("default ceiling", func(t *testing.T) {
		w := &RowWriter{}
		b := w.newInsertBatcher(byteBatchTable(2))
		// bool rows estimate 1 byte each — 10k of them sit far under
		// the 1 MiB byte target, so only the ceiling can stop them.
		for i := 0; i < defaultMaxRowsPerBatch-1; i++ {
			b.add(ir.Row{"a": true})
		}
		if b.full() {
			t.Fatalf("full at %d rows; want the %d default ceiling", defaultMaxRowsPerBatch-1, defaultMaxRowsPerBatch)
		}
		b.add(ir.Row{"a": true})
		if !b.full() {
			t.Fatalf("not full at %d rows; the default ceiling must bind", defaultMaxRowsPerBatch)
		}
	})
}

// TestInsertBatcher_PlaceholderBoundClampsWideTables pins the MySQL
// 16-bit prepared-statement parameter limit protection: rows × columns
// never exceeds maxBulkInsertPlaceholders, and even a table wider than
// the whole placeholder budget still ships one row at a time (rowCeil
// never drops below 1).
func TestInsertBatcher_PlaceholderBoundClampsWideTables(t *testing.T) {
	w := &RowWriter{}

	const cols = 200
	b := w.newInsertBatcher(byteBatchTable(cols))
	wantCeil := maxBulkInsertPlaceholders / cols // 300
	if b.rowCeil != wantCeil {
		t.Errorf("rowCeil = %d for a %d-col table; want %d (placeholder bound)", b.rowCeil, cols, wantCeil)
	}
	for i := 0; i < wantCeil; i++ {
		b.add(ir.Row{"a": true})
	}
	if !b.full() {
		t.Errorf("not full at the placeholder-derived ceiling %d", wantCeil)
	}

	// Wider than the whole placeholder budget: integer division would
	// give 0; the composer must clamp to 1 so the write still proceeds
	// (the server, not sluice, is the loud authority on whether such a
	// statement is executable at all).
	huge := w.newInsertBatcher(byteBatchTable(maxBulkInsertPlaceholders + 1))
	if huge.rowCeil != 1 {
		t.Errorf("rowCeil = %d for a wider-than-budget table; want 1", huge.rowCeil)
	}
}

// TestInsertBatcher_OversizeSingleRowShipsAlone pins the never-split /
// never-refuse contract: a single row whose estimate alone exceeds the
// byte target makes a fresh batch immediately full, so it ships as a
// one-row statement.
func TestInsertBatcher_OversizeSingleRowShipsAlone(t *testing.T) {
	w := &RowWriter{}
	b := w.newInsertBatcher(byteBatchTable(2))
	b.add(ir.Row{"id": int64(1), "blob": strings.Repeat("z", 2<<20)}) // ~2 MiB
	if !b.full() {
		t.Fatalf("oversize row did not fill the batch")
	}
	if len(b.rows) != 1 {
		t.Fatalf("oversize row batched with %d rows; want exactly 1", len(b.rows))
	}
}

// TestInsertBatcher_OperatorByteCapClampsDownOnly pins the
// --max-buffer-bytes interaction: a value below the 1 MiB target lowers
// the effective per-statement trigger; a value above it does NOT raise
// the target (the ~1 MiB statement size is a round-trip-amortization
// choice, not a memory bound).
func TestInsertBatcher_OperatorByteCapClampsDownOnly(t *testing.T) {
	const rowBytes = 1024
	row := ir.Row{"id": int64(1), "payload": strings.Repeat("p", rowBytes-8)}

	below := &RowWriter{maxBufferBytes: 64 << 10} // 64 KiB
	b := below.newInsertBatcher(byteBatchTable(2))
	if b.byteTarget != 64<<10 {
		t.Errorf("byteTarget = %d with a 64 KiB operator cap; want %d", b.byteTarget, 64<<10)
	}
	n := 0
	for !b.full() {
		b.add(row)
		n++
	}
	if n != 64 {
		t.Errorf("64 KiB cap filled at %d rows of 1 KiB; want 64", n)
	}

	above := &RowWriter{maxBufferBytes: 8 << 20} // 8 MiB
	if got := above.newInsertBatcher(byteBatchTable(2)).byteTarget; got != defaultStatementByteTarget {
		t.Errorf("byteTarget = %d with an 8 MiB operator cap; want the %d default target (cap must not raise it)",
			got, defaultStatementByteTarget)
	}
}

// TestInsertBatcher_FlushBoundaryExactness drives the composer through
// the writer loop's add/full/flush shape and pins that no row is lost
// or duplicated at flush boundaries, for both an exact-multiple corpus
// and a remainder corpus.
func TestInsertBatcher_FlushBoundaryExactness(t *testing.T) {
	const rowBytes = 1024
	const perFlush = 64 // 64 KiB cap / 1 KiB rows

	cases := []struct {
		name        string
		total       int
		wantFlushes []int
	}{
		{"exact multiple", perFlush * 3, []int{64, 64, 64}},
		{"remainder", perFlush*3 + 10, []int{64, 64, 64, 10}},
		{"single short batch", 5, []int{5}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := &RowWriter{maxBufferBytes: perFlush * rowBytes}
			b := w.newInsertBatcher(byteBatchTable(2))

			var flushes []int
			seen := 0
			flush := func() {
				if b.empty() {
					return
				}
				flushes = append(flushes, len(b.rows))
				seen += len(b.rows)
				b.reset()
			}
			for i := 0; i < c.total; i++ {
				b.add(ir.Row{"id": int64(i), "payload": strings.Repeat("p", rowBytes-8)})
				if b.full() {
					flush()
				}
			}
			flush() // the channel-close final flush

			if seen != c.total {
				t.Errorf("flushed %d rows total; want %d (no loss, no dup)", seen, c.total)
			}
			if len(flushes) != len(c.wantFlushes) {
				t.Fatalf("flush sizes %v; want %v", flushes, c.wantFlushes)
			}
			for i := range flushes {
				if flushes[i] != c.wantFlushes[i] {
					t.Errorf("flush[%d] = %d rows; want %d (full sizes %v)", i, flushes[i], c.wantFlushes[i], c.wantFlushes)
				}
			}
		})
	}
}

// TestInsertBatcher_StatementCountRegression is the ADR-0150 payoff
// pin: a narrow-row corpus must produce at least ~10× fewer statements
// than the pre-ADR-0150 fixed 500-row batching did. If a future edit
// quietly re-tightens the ceilings (or breaks the byte accumulation),
// this catches the throughput regression by shape, without a database.
func TestInsertBatcher_StatementCountRegression(t *testing.T) {
	const total = 100_000
	// The pre-ADR-0150 behaviour: a fixed 500-row primary trigger
	// (the old defaultMaxRowsPerBatch value, kept literal here as the
	// historical baseline).
	const oldStatements = total / 500 // 200

	w := &RowWriter{}
	b := w.newInsertBatcher(byteBatchTable(3))
	newStatements := 0
	for i := 0; i < total; i++ {
		b.add(narrowRow())
		if b.full() {
			newStatements++
			b.reset()
		}
	}
	if !b.empty() {
		newStatements++
	}

	if newStatements*10 > oldStatements {
		t.Errorf("narrow-row corpus composed %d statements; old behaviour was %d — want ≥10× fewer (≤%d)",
			newStatements, oldStatements, oldStatements/10)
	}
}

// TestNoteTierCPUBoundTarget pins the ADR-0150 companion operator hint:
// exactly one INFO per PlanetScale-flavor writer no matter how many
// batched writes engage, and complete silence for every other flavor
// (the zero-value-safe default).
func TestNoteTierCPUBoundTarget(t *testing.T) {
	capture := func(fn func()) string {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prev)
		fn()
		return buf.String()
	}

	ctx := context.Background()

	ps := &RowWriter{tierCPUBoundTarget: true}
	out := capture(func() {
		ps.noteTierCPUBoundTarget(ctx)
		ps.noteTierCPUBoundTarget(ctx)
		ps.noteTierCPUBoundTarget(ctx)
	})
	if n := strings.Count(out, "tier-CPU-bound"); n != 1 {
		t.Errorf("PlanetScale-flavor writer emitted the tier hint %d times across 3 engagements; want exactly 1:\n%s", n, out)
	}

	vanilla := &RowWriter{}
	if out := capture(func() { vanilla.noteTierCPUBoundTarget(ctx) }); out != "" {
		t.Errorf("non-PlanetScale writer emitted output; want silence:\n%s", out)
	}
}
