// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// defaultFloatRereadMaxRows caps the per-table PK→exact-FLOAT map the
// backup exact re-read buffers, so the repair is BOUNDED-memory (a
// whole-table buffer would OOM by default on a large FLOAT table — the
// ADR-0071 tenet). A table WHOSE re-read would exceed this many distinct
// rows falls back LOUDLY (default: WARN + rounded; --strict-float: refuse)
// rather than buffering unbounded. 2,000,000 rows is a few hundred MB
// worst case (~PK key + a small float map per row); operators raise it
// with --float-reread-max-rows when they have the headroom and want exact
// on a bigger table.
//
// Why a bounded MERGE-JOIN isn't used instead (which would be O(1) memory
// and need no cap): the VStream COPY row stream is NOT PK-ordered — Vitess
// can scan by a cheaper unique key than the table's PK AND re-emits rows
// already past the scan during binlog catchup (out of order, with
// duplicates; see [ir.IdempotentCopyReader] + cdc_vstream_snapshot.go's
// CopyNeedsIdempotentWriter). A merge-join against the PK-ordered exact
// scan needs monotonic keys on both sides; the COPY side has neither, so a
// safe global merge-join is infeasible. The capped buffer is bounded +
// honest for every case (unsharded, sharded, re-emitting).
const defaultFloatRereadMaxRows = 2_000_000

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
	// maxRows bounds the per-table PK→float map; a table that would exceed
	// it falls back loudly (see [defaultFloatRereadMaxRows]).
	maxRows int
	// strict makes an over-cap table REFUSE (--strict-float) instead of
	// falling back to the rounded archive.
	strict bool

	mu  sync.Mutex
	err error
}

func newFloatExactPatchReader(inner ir.RowReader, source ir.Engine, sourceDSN string, plan map[string]floatPatchTable, maxRows int, strict bool) *floatExactPatchReader {
	if maxRows <= 0 {
		maxRows = defaultFloatRereadMaxRows
	}
	return &floatExactPatchReader{
		inner:     inner,
		source:    source,
		sourceDSN: sourceDSN,
		plan:      plan,
		batchSize: migcore.DefaultBulkBatchSize,
		maxRows:   maxRows,
		strict:    strict,
	}
}

// ReadRows patches the FLOAT columns of a repairable table's rows with
// exact source values; other tables stream through unchanged.
func (r *floatExactPatchReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	p, ok := r.plan[table.Name]
	if !ok {
		return r.inner.ReadRows(ctx, table)
	}
	exact, overCap, err := r.buildExactMap(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("backup: float re-read %q: %w", table.Name, err)
	}
	if overCap {
		// Bounded-memory floor: the exact re-read would need more than
		// maxRows buffered PK→float entries. Never buffer unbounded (OOM);
		// fall back LOUDLY.
		if r.strict {
			return nil, sluicecode.Wrap(sluicecode.CodeVStreamFloatLossy,
				"raise --float-reread-max-rows, or use --no-float-exact-reread (rounded, consistent)",
				fmt.Errorf("backup: --strict-float: table %q has more than %d rows — too large for the in-memory exact "+
					"FLOAT re-read (bounded-memory floor). Raise --float-reread-max-rows if you have the headroom, or use "+
					"--no-float-exact-reread to archive the rounded-but-consistent values", table.Name, r.maxRows))
		}
		slog.WarnContext(ctx,
			"backup: table too large for in-memory exact FLOAT re-read — archiving the display-rounded values for this "+
				"table (bounded-memory floor). Raise --float-reread-max-rows to repair it exactly, --no-float-exact-reread "+
				"to silence this per-table (rounded everywhere), or --strict-float to refuse instead",
			slog.String("table", table.Name), slog.Int("max_rows", r.maxRows))
		return r.inner.ReadRows(ctx, table)
	}
	inCh, err := r.inner.ReadRows(ctx, table)
	if err != nil {
		return nil, err
	}
	out := make(chan ir.Row)
	exactCount := len(exact)
	go func() {
		defer close(out)
		var streamed, patched int64
		for row := range inCh {
			if floats, ok := exact[floatPatchKey(row, p.pkCols)]; ok {
				for col, v := range floats {
					row[col] = v
				}
				patched++
			}
			streamed++
			select {
			case out <- row:
			case <-ctx.Done():
				return
			}
		}
		// M0.1 0-patched-of-N tripwire (audit item 58) + the audit-2026-07-11
		// M-2 widening + the audit-2026-07-12 MED-2 close. A run that patched
		// NOTHING while the table demonstrably has (or had) rows is a silent
		// FLOAT-fidelity loss the default posture must not swallow silently —
		// the over-cap and unrepairable rounded-fallbacks both WARN, and the
		// file's contract is "rounded but SIGNALLED", never silent. Three shapes:
		//
		//   (1) streamed>0 && patched==0 — a systemic PK-rendering divergence
		//       (the SL-F2 class: the exact-scan PK and the COPY PK render
		//       differently), leaving every single-precision FLOAT column
		//       display-rounded. Unambiguous, so --strict-float REFUSES it
		//       (a per-row miss is a benign post-re-read temporal skip and is
		//       tolerated — only a TOTAL miss trips this); the default WARNs.
		//   (2) streamed==0 — the COPY delivered NO rows though the exact scan
		//       found some. AMBIGUOUS: a whole-table copy dropout (bad) OR a
		//       table that was empty at the COPY position and filled during the
		//       window so only the later exact scan sees rows (legit). Refusing
		//       would false-positive the legit case, so this WARNs in BOTH
		//       postures rather than refuses.
		//   (3) exactCount==0 && streamed>0 — the mirror of (2), the
		//       audit-2026-07-12 MED-2 shape: the COPY streamed rows but the
		//       exact re-read found NONE, so every streamed row keeps its
		//       VStream display rounding with patched==0. AMBIGUOUS like (2):
		//       every row deleted between the COPY and the re-read (legit
		//       temporal skip — v0.99.228's "a legitimately empty source table
		//       never trips" contract) OR a systemic exact-scan cursor/predicate
		//       defect (bad). Refusing would false-positive the mass-delete
		//       case, so this too WARNs in BOTH postures rather than refuses —
		//       the file's contract is "rounded but SIGNALLED", never silent.
		switch {
		case exactCount > 0 && streamed > 0 && patched == 0 && r.strict:
			r.mu.Lock()
			r.err = sluicecode.Wrap(sluicecode.CodeVStreamFloatLossy,
				"the exact re-read returned rows but none matched a streamed row's primary key — a PK whose exact-scan and COPY renderings diverge; add --no-float-exact-reread for a rounded-but-consistent archive, or drop --strict-float",
				fmt.Errorf("backup: --strict-float: table %q exact FLOAT re-read patched 0 of %d streamed row(s) despite a %d-row exact map — every single-precision FLOAT column would silently retain its VStream display rounding",
					table.Name, streamed, exactCount))
			r.mu.Unlock()
		case exactCount > 0 && streamed > 0 && patched == 0:
			slog.WarnContext(ctx,
				"backup: exact FLOAT re-read matched none of the streamed rows; archiving this table's single-precision FLOATs VStream display-rounded (pass --strict-float to refuse instead) — a primary key whose exact-scan and COPY renderings diverge",
				slog.String("table", table.Name),
				slog.Int64("streamed", streamed),
				slog.Int("exact_rows", exactCount))
		case exactCount > 0 && streamed == 0:
			slog.WarnContext(ctx,
				"backup: exact FLOAT re-read found rows but the COPY streamed none for this table — a table empty at the snapshot position and filled during the window (legit) or a whole-table copy dropout; verify this table's row count",
				slog.String("table", table.Name),
				slog.Int("exact_rows", exactCount))
		case exactCount == 0 && streamed > 0:
			slog.WarnContext(ctx,
				"backup: exact FLOAT re-read found no rows though the COPY streamed some — every row deleted between the snapshot and the re-read (legit) or a systemic exact-scan defect; this table's single-precision FLOATs are archived VStream display-rounded, verify its row count",
				slog.String("table", table.Name),
				slog.Int64("streamed", streamed))
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
// a map from the PK key to the exact FLOAT column values. It stops and
// returns overCap=true the moment the map would exceed r.maxRows entries —
// so the buffer is BOUNDED (never a whole-table OOM); the caller then falls
// back loudly. Bounded to this one table's PK+float footprint (serial
// sweep).
func (r *floatExactPatchReader) buildExactMap(ctx context.Context, p floatPatchTable) (exact map[string]map[string]any, overCap bool, err error) {
	rr, err := r.source.OpenRowReader(ctx, r.sourceDSN)
	if err != nil {
		return nil, false, fmt.Errorf("open source reader: %w", err)
	}
	defer migcore.CloseIf(rr)
	br, ok := rr.(ir.BatchedRowReader)
	if !ok {
		return nil, false, fmt.Errorf("source reader does not support cursor-paginated reads")
	}

	exact = make(map[string]map[string]any)
	var cursor []any
	for {
		batchCtx, cancel := context.WithCancel(ctx)
		rowsCh, berr := br.ReadRowsBatch(batchCtx, p.srcRead, cursor, r.batchSize)
		if berr != nil {
			cancel()
			return nil, false, fmt.Errorf("read batch: %w", berr)
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
			if len(exact) > r.maxRows {
				// Over the bounded-memory floor — stop scanning and drop the
				// partial map so the caller falls back loudly.
				cancel()
				return nil, true, nil
			}
		}
		cancel()
		if serr := migcore.ReaderStreamErr(rr, p.srcRead); serr != nil {
			return nil, false, serr
		}
		if cerr := ctx.Err(); cerr != nil {
			return nil, false, cerr
		}
		if n == 0 {
			break
		}
		cursor = last
	}
	return exact, false, nil
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
		// A single-precision FLOAT in the PK makes the table non-repairable
		// (SL-F1): the patch map keys on the PK, but floatPatchKey renders the
		// EXACT re-read PK on one side and the display-rounded COPY PK on the
		// other, so the keys never match and every non-PK FLOAT silently
		// retains its rounding while --strict-float (which only refuses tables
		// absent from the plan) exits 0 with a rounded archive. Omit it here so
		// its columns land in applyVStreamFloatPolicy's unrepairable set — WARN
		// + rounded by default, upfront refusal under --strict-float.
		if migcore.PrimaryKeyHasSinglePrecisionFloat(t) {
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
