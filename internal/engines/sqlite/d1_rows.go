// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// D1RowReader streams rows from a live Cloudflare D1 table for the bulk-copy
// phase, over the HTTP query API. It implements [ir.RowReader] with the SAME
// sticky-error contract as the file reader (Bug 68): a per-cell fidelity refusal
// during streaming aborts the read and surfaces via [Err] after the channel
// closes.
//
// The load-bearing difference from the file reader is the SELECT projection:
// each user column is read via `typeof(c)` AND
// `CASE typeof(c) WHEN 'blob' THEN hex(c) ELSE CAST(c AS TEXT) END`, so values
// arrive as EXACT text (integers > 2^53 included) rather than as lossy JSON
// numbers. The (typeof, text/hex) pair is reconstructed into the file path's Go
// storage-class value by [d1StorageValue] and decoded by the shared
// [decodeCell] — inheriting the file engine's full storage-class fidelity.
type D1RowReader struct {
	client  *d1Client
	dateEnc dateEncoding

	// pageSize overrides the keyset page size; 0 means [d1PageSize]. It exists
	// so tests can force multi-page stitching without staging d1PageSize rows.
	pageSize int

	mu  sync.Mutex
	err error
}

// effectivePageSize is the configured page size, defaulting to [d1PageSize].
func (r *D1RowReader) effectivePageSize() int {
	if r.pageSize > 0 {
		return r.pageSize
	}
	return d1PageSize
}

// d1PageSize bounds each keyset page so a response stays under D1's
// response-size limit (D1 caps a query response at ~1 MB / 100 MB depending on
// plan; a modest page keeps well clear and bounds memory). It is deliberately
// const — within-table chunking parallelism is a deferred follow-up.
const d1PageSize = 1000

// d1RowChanBuffer bounds the output channel so HTTP fetch + decode overlap the
// downstream write while preserving back-pressure (mirrors the file reader's
// rowChanBuffer).
const d1RowChanBuffer = 64

// Close releases the reader. The HTTP transport has no pool/temp file, so it is
// a no-op (present for the orchestrator's io.Closer probe). Safe to call twice.
func (r *D1RowReader) Close() error { return nil }

// Err returns the error, if any, that terminated the most recently returned
// channel. Only valid after the channel has been fully drained.
func (r *D1RowReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *D1RowReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// ReadRows streams the rows of table over the returned channel, paginating by
// keyset (PK, else the implicit rowid; a table with neither is refused — see
// [planPagination]). The channel closes
// when the table is fully read, when ctx is cancelled, or when a value fails the
// storage-class fidelity check (in which case [Err] returns the cause).
func (r *D1RowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("d1: ReadRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("d1: ReadRows: table %q has no columns", table.Name)
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	plan, err := r.planPagination(ctx, table)
	if err != nil {
		return nil, err
	}

	out := make(chan ir.Row, d1RowChanBuffer)
	go r.stream(ctx, table, plan, out)
	return out, nil
}

// pagePlan captures how a table is paginated. orderCols are the table-qualified
// ORDER BY / keyset columns (qualified so the bound compares the TYPED column,
// not the CAST-text alias — the lexical-sort trap the MySQL keyset hit). When
// useRowid is set, the key is the implicit rowid — referenced through rowidName,
// the alias no declared column shadows (see [resolveRowidName]) and projected
// under rowidAlias — rather than user columns. There is no third strategy: a
// table with neither a safe PK keyset nor a reachable rowid is refused loudly
// ([sluicecode.CodeBulkCopyNoPaginationKey]), never read by LIMIT/OFFSET.
type pagePlan struct {
	typeofPrefix  string   // collision-free prefix for the per-column typeof aliases
	typeofAliases []string // per-column typeof alias, indexed by column position (hoisted: built once per table, not per cell)
	rowidAlias    string   // projection alias carrying CAST(<rowidName> AS TEXT), when useRowid
	rowidName     string   // the unshadowed implicit-rowid name (rowid / _rowid_ / oid), when useRowid
	orderCols     []string // unqualified key column names (user PK cols, or {rowidName})
	useRowid      bool
}

// newPagePlan seeds a pagePlan with the table's collision-free typeof prefix
// and the per-column alias slice. The aliases are precomputed HERE — once per
// table — so the per-cell decode loops (this reader's decodeRow, the staging
// insert loop) index a slice instead of rebuilding the alias string for every
// cell (audit P-4).
func newPagePlan(cols []*ir.Column) pagePlan {
	p := pagePlan{typeofPrefix: typeofPrefix(cols)}
	p.rowidAlias = p.typeofPrefix + "rowid"
	p.typeofAliases = make([]string, len(cols))
	for i := range cols {
		p.typeofAliases[i] = typeofAlias(p.typeofPrefix, i)
	}
	return p
}

// planPagination chooses the pagination strategy for a table: keyset on the PK
// if present and text-param-safe, else keyset on the implicit rowid (every
// SQLite/D1 base table without a PK is a rowid table — WITHOUT ROWID requires a
// PK). A BLOB-affinity key column cannot be keyset-bounded by a text param (see
// [pkKeysetSafe]) and routes to rowid. A table with neither — a WITHOUT ROWID
// table keyed only by a BLOB column, or a rowid table whose declared columns
// shadow every rowid alias — is refused loudly with
// [sluicecode.CodeBulkCopyNoPaginationKey] rather than looped forever or read
// by LIMIT/OFFSET. (The former OFFSET fallback was reachable only through a
// failed rowid probe, which is exactly the path audit LA-1 found routing a
// shadowed `rowid` column into the keyset; it is gone, not narrowed.)
func (r *D1RowReader) planPagination(ctx context.Context, table *ir.Table) (pagePlan, error) {
	p := newPagePlan(table.Columns)

	if table.PrimaryKey != nil && len(table.PrimaryKey.Columns) > 0 && pkKeysetSafe(table) {
		for _, ic := range table.PrimaryKey.Columns {
			p.orderCols = append(p.orderCols, ic.Column)
		}
		return p, nil
	}
	// No PK, or a BLOB-affinity (or no-declared-type) key column that can't be
	// bounded by a text param: SQLite ranks BLOB above every TEXT and applies no
	// numeric coercion to the param, so `blobcol > ?(text)` is ALWAYS true and
	// the page never advances (infinite loop + duplicate rows). The integer
	// rowid compares exactly, so key on it — through the alias the table does
	// not shadow.
	why := "no primary key"
	if table.PrimaryKey != nil && len(table.PrimaryKey.Columns) > 0 {
		why = "BLOB-affinity primary key, which a text-param bound never advances"
	}
	name, err := r.resolveRowidName(ctx, table, why)
	if err != nil {
		return pagePlan{}, err
	}
	p.useRowid = true
	p.rowidName = name
	p.orderCols = []string{name}
	return p, nil
}

// rowidNames are the three names SQLite resolves to a rowid table's implicit
// rowid — UNLESS the table declares a column of that name, in which case the
// name binds the user column (SQLite's documented shadowing rule, matched
// case-insensitively like every SQLite identifier). Preference order: `rowid`
// first so the common (unshadowed) table's SQL is byte-identical to before.
var rowidNames = [...]string{"rowid", "_rowid_", "oid"}

// resolveRowidName returns the implicit-rowid name the keyset can safely use
// for table: the first of [rowidNames] that no declared column shadows, proven
// reachable by a `SELECT <name> … LIMIT 1` probe. It is the LA-1 fix — the
// former probe was `SELECT rowid`, which SUCCEEDS on a PK-less table whose user
// column is named `rowid` because the name binds that column, so the keyset
// paginated on a non-unique user column and dropped every row sharing a page-
// boundary value (2,500 → 2,000 rows at exit 0 on real D1). Declared columns
// come from `PRAGMA table_xinfo` (the server's own catalog, generated columns
// included — a generated column shadows too), not from the IR, so the check
// holds however the caller built the table.
//
// Every failure is a loud [sluicecode.CodeBulkCopyNoPaginationKey] refusal
// naming the table and the remedy — all three names shadowed, or the probe
// failing (a WITHOUT ROWID table, which reaches here only with a BLOB-only PK,
// or a transport error). Nothing falls through to a shadowed column or to
// LIMIT/OFFSET.
func (r *D1RowReader) resolveRowidName(ctx context.Context, table *ir.Table, why string) (string, error) {
	declared, err := r.client.queryRows(ctx, "PRAGMA table_xinfo("+quotePragmaArg(table.Name)+")")
	if err != nil {
		return "", noPaginationKey(table.Name, why, fmt.Errorf("read declared columns: %w", err))
	}
	shadowed := map[string]bool{}
	for _, row := range declared {
		name, err := rowString(row, "name")
		if err != nil {
			return "", noPaginationKey(table.Name, why, fmt.Errorf("read declared columns: %w", err))
		}
		shadowed[strings.ToLower(name)] = true
	}
	for _, name := range rowidNames {
		if shadowed[name] {
			continue
		}
		// The name is unshadowed, so it can only mean the implicit rowid — which
		// a WITHOUT ROWID table does not have (the probe errors there).
		if _, err := r.client.queryRows(ctx, "SELECT "+quoteIdent(name)+" FROM "+quoteIdent(table.Name)+" LIMIT 1"); err != nil {
			return "", noPaginationKey(table.Name, why, fmt.Errorf("probe of the implicit rowid as %s failed: %w", name, err))
		}
		return name, nil
	}
	return "", noPaginationKey(table.Name, why, errors.New(
		"declared columns shadow every implicit-rowid name (rowid, _rowid_, oid), so no reference can reach it",
	))
}

// noPaginationKeyHint is the remedy carried by [sluicecode.CodeBulkCopyNoPaginationKey].
const noPaginationKeyHint = "declare a non-BLOB PRIMARY KEY on the table (or rename the columns shadowing rowid/_rowid_/oid) and re-run"

// noPaginationKey is the coded refusal for a table sluice cannot paginate
// without risking a silently short or looping read: why names what ruled out a
// PK keyset, cause what ruled out the rowid.
func noPaginationKey(table, why string, cause error) error {
	return sluicecode.Wrap(sluicecode.CodeBulkCopyNoPaginationKey, noPaginationKeyHint,
		fmt.Errorf("d1: table %q has no key sluice can keyset-paginate on: %s, and %w; %s",
			table, why, cause, noPaginationKeyHint))
}

// pkKeysetSafe reports whether every primary-key column can be keyset-bounded by
// a text param. A BLOB-affinity column (resolved to ir.Blob — which also covers
// the no-declared-type case) cannot (BLOB outranks TEXT and the text param gets
// no coercion), so its presence makes the PK keyset unsafe and routes to rowid.
// A PK column not found in the table (shouldn't happen) is treated as unsafe so
// the caller falls back to the exact rowid path rather than risk a bad bound.
func pkKeysetSafe(table *ir.Table) bool {
	for _, ic := range table.PrimaryKey.Columns {
		col := findColumn(table, ic.Column)
		if col == nil {
			return false
		}
		if _, isBlob := col.Type.(ir.Blob); isBlob {
			return false
		}
	}
	return true
}

// findColumn returns the named column of table, or nil if absent.
func findColumn(table *ir.Table, name string) *ir.Column {
	for _, c := range table.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// countRows returns the server-side `COUNT(*)` of table — the independent
// expected value the paginated read is checked against (audit LA-3). The count
// is projected as TEXT and parsed exactly, the same discipline as every other
// integer this transport reads (a JSON number would round past 2^53).
func (r *D1RowReader) countRows(ctx context.Context, table string) (int64, error) {
	rows, err := r.client.queryRows(ctx, "SELECT CAST(COUNT(*) AS TEXT) AS n FROM "+quoteIdent(table))
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, fmt.Errorf("COUNT(*) returned %d rows; want 1", len(rows))
	}
	text, ok, err := jsonString(rows[0]["n"])
	if err != nil {
		return 0, fmt.Errorf("COUNT(*) result is not a text scalar: %w", err)
	}
	if !ok {
		return 0, errors.New("COUNT(*) result is NULL")
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("COUNT(*) result %q: %w", text, err)
	}
	return n, nil
}

// rowCountMismatchHint is the remedy carried by [sluicecode.CodeBulkCopyRowCountMismatch].
const rowCountMismatchHint = "do not trust this copy; migrate the table through `wrangler d1 export` + `sluice migrate --source-driver sqlite` and report the table's DDL as a sluice bug"

// checkRowCount is the LA-3 bracket's verdict, taken after the final page:
// before and after are the server-side COUNT(*) around the read, delivered is
// what pagination produced.
//
//   - before == after (a quiescent source — the documented D1 assumption) and
//     delivered differs: the READER lost or duplicated rows. That is the exact
//     shape of LA-1, and it is refused with
//     [sluicecode.CodeBulkCopyRowCountMismatch] so the run cannot exit 0 over
//     a short copy.
//   - before != after: writes landed during the read, so the delivered count is
//     not comparable to either and the copy is not a point-in-time snapshot.
//     That is WARNed, not refused — a live database would otherwise be
//     unmigratable, and the keyset read is documented as non-snapshot under
//     concurrent writes. The WARN names all three numbers.
func checkRowCount(ctx context.Context, table string, before, after, delivered int64) error {
	if before != after {
		slog.WarnContext(ctx, "d1: table changed during the read; the copy is not a point-in-time snapshot and its row count cannot be verified",
			slog.String("table", table),
			slog.Int64("count_before", before),
			slog.Int64("count_after", after),
			slog.Int64("rows_delivered", delivered))
		return nil
	}
	if delivered != before {
		return sluicecode.Wrap(sluicecode.CodeBulkCopyRowCountMismatch, rowCountMismatchHint,
			fmt.Errorf("pagination delivered %d rows but the source's COUNT(*) was %d before and after the read "+
				"(the source was quiescent, so the reader lost or duplicated rows); %s",
				delivered, before, rowCountMismatchHint))
	}
	return nil
}

// d1Page is one fetched page handed from the prefetching fetcher goroutine to
// the decode loop. err carries that page's fetch failure, delivered IN
// SEQUENCE so it surfaces exactly when the sequential loop would have reached
// the page. final marks the stream's terminal page (short page or fetch
// failure): if the channel closes WITHOUT a final page, the fetch was aborted
// mid-table and the consumer must NOT report a clean read (silent truncation).
type d1Page struct {
	rows  []d1Row
	err   error
	final bool
}

// stream decodes fetched pages and pushes IR Rows onto out, closing it when
// done. It owns the sticky error.
//
// Single-page prefetch (ADR-0151): pages arrive from [fetchPages], a fetcher
// goroutine that issues page N+1's HTTP request while page N's rows decode and
// stream downstream — hiding one HTTP round-trip per page. The handoff channel
// is UNBUFFERED, so the fetcher holds at most ONE page beyond the one being
// consumed (bounded memory ≈ one extra page), never an N-deep pipeline — the
// same shape as the chain-replay read-ahead (pipeline.streamIncrementalChanges).
// This is RTT-hiding, NOT within-table chunking (which stays deferred — see
// [d1PageSize]). Row order is unchanged: a single fetcher and a single consumer
// hand pages over FIFO, and rows decode in page order.
func (r *D1RowReader) stream(ctx context.Context, table *ir.Table, plan pagePlan, out chan<- ir.Row) {
	defer close(out)

	enc := resolveDateEncoding(r.dateEnc)

	// Cancelling fetchCtx on any early return (decode refusal, downstream
	// cancellation) aborts the fetcher's in-flight HTTP request and its
	// blocked handoff, so the goroutine is always reaped — no leak.
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()
	pages := make(chan d1Page) // unbuffered: exactly one page of read-ahead
	go r.fetchPages(fetchCtx, table, plan, pages)

	var (
		ordinal  int64 // 1-based row counter, for error context
		sawFinal bool
	)
	for page := range pages {
		if page.err != nil {
			r.setErr(fmt.Errorf("d1: table %q: %w", table.Name, page.err))
			return
		}
		for _, raw := range page.rows {
			ordinal++
			row, _, err := r.decodeRow(table, plan, raw, enc, ordinal)
			if err != nil {
				r.setErr(err)
				return
			}
			select {
			case out <- row:
			case <-ctx.Done():
				r.setErr(ctx.Err())
				return
			}
		}
		sawFinal = page.final
	}
	if !sawFinal {
		// The channel closed without a terminal page: the fetcher was
		// aborted (ctx cancellation) mid-table. Report the cause loudly —
		// a clean return here would be a SILENTLY TRUNCATED read.
		if err := ctx.Err(); err != nil {
			r.setErr(err)
			return
		}
		r.setErr(fmt.Errorf("d1: table %q: page fetch aborted before the final page", table.Name))
	}
}

// fetchPages issues the page requests strictly in order, one page ahead of the
// decode loop, and hands each page over the unbuffered pages channel (closing
// it when the table is exhausted or a page fails). The requests are
// byte-identical to the pre-prefetch sequential loop's: same SQL, same bound
// params, same order — page N+1's keyset bound is the exact-text key of page
// N's LAST row via the same [extractKey] path, so the > 2^53 string-bound
// discipline (ADR-0132 §6) is unchanged.
//
// A fetch error is delivered as that page's [d1Page.err] and ends the fetch —
// the consumer surfaces it only after the prior page's rows have streamed,
// exactly as the sequential loop would have. A key-extraction failure on the
// last row stops fetching WITHOUT delivering an error: the consumer's own
// per-row decodeRow deterministically reproduces the same loud refusal (with
// full table/row context) when it reaches that row, so the error is never
// duplicated and never lost.
//
// The read is bracketed by a server-side COUNT(*) (audit LA-3): one before the
// first page, one after the final page, judged by [checkRowCount]. A verdict
// against the read travels as the final page's err, so both consumers (the
// stream loop and the staging materializer) surface it through the page-error
// path they already have. The counts are the only evidence in this lane that
// does not come from the reader under test.
func (r *D1RowReader) fetchPages(ctx context.Context, table *ir.Table, plan pagePlan, pages chan<- d1Page) {
	defer close(pages)

	deliver := func(page d1Page) bool {
		select {
		case pages <- page:
			return true
		case <-ctx.Done():
			return false
		}
	}

	before, err := r.countRows(ctx, table.Name)
	if err != nil {
		deliver(d1Page{err: fmt.Errorf("count rows before the read: %w", err), final: true})
		return
	}

	projection := buildD1Projection(table, plan)
	pageSize := r.effectivePageSize()

	var (
		lastKey []string // exact-text bound from the previous page
		ordinal int64    // rows fetched so far (error context for extractKey; the delivered total)
	)
	for {
		sql, params := buildD1PageQuery(table, plan, projection, lastKey, pageSize)
		rows, err := r.client.queryRows(ctx, sql, params...)
		if err != nil {
			// A failed page ends the stream when the consumer reaches it.
			deliver(d1Page{err: fmt.Errorf("read page: %w", err), final: true})
			return
		}
		ordinal += int64(len(rows))
		// A short (or empty) page is the last page: close the bracket and
		// let its verdict ride the page.
		if len(rows) < pageSize {
			after, err := r.countRows(ctx, table.Name)
			if err != nil {
				err = fmt.Errorf("count rows after the read: %w", err)
			} else {
				err = checkRowCount(ctx, table.Name, before, after, ordinal)
			}
			deliver(d1Page{rows: rows, err: err, final: true})
			return
		}
		if !deliver(d1Page{rows: rows}) {
			return
		}
		key, err := r.extractKey(table, plan, rows[len(rows)-1], ordinal)
		if err != nil {
			return // consumer reproduces this refusal at the same row
		}
		lastKey = key
	}
}

// decodeRow turns one D1 result row into an [ir.Row], and returns the exact-text
// keyset bound for the next page. Every decode error is wrapped with
// table/column/row so the operator can find the offending cell (the loud-failure
// tenet).
func (r *D1RowReader) decodeRow(table *ir.Table, plan pagePlan, raw d1Row, enc dateEncoding, ordinal int64) (ir.Row, []string, error) {
	row := make(ir.Row, len(table.Columns))
	for i, col := range table.Columns {
		typeofText, ok, err := jsonString(raw[plan.typeofAliases[i]])
		if err != nil {
			return nil, nil, fmt.Errorf("d1: table %q column %q row %d: decode typeof: %w",
				table.Name, col.Name, ordinal, err)
		}
		if !ok {
			typeofText = "null"
		}
		storage, err := d1StorageValue(typeofText, raw[col.Name])
		if err != nil {
			return nil, nil, fmt.Errorf("d1: table %q column %q row %d: %w",
				table.Name, col.Name, ordinal, err)
		}
		v, err := decodeCell(storage, col.Type, enc)
		if err != nil {
			return nil, nil, fmt.Errorf("d1: table %q column %q row %d: %w",
				table.Name, col.Name, ordinal, err)
		}
		row[col.Name] = v
	}

	key, err := r.extractKey(table, plan, raw, ordinal)
	if err != nil {
		return nil, nil, err
	}
	return row, key, nil
}

// extractKey reads the exact-text values of the keyset columns from a result
// row, to bound the next page. For a PK keyset the key columns are user columns
// (read from their value projection); for a rowid keyset it is the rowid alias.
// A NULL key value is refused
// loudly — a NULL in a keyset column would make pagination skip/loop.
func (r *D1RowReader) extractKey(table *ir.Table, plan pagePlan, raw d1Row, ordinal int64) ([]string, error) {
	if plan.useRowid {
		text, ok, err := jsonString(raw[plan.rowidAlias])
		if err != nil || !ok {
			return nil, fmt.Errorf("d1: table %q row %d: missing rowid for keyset pagination", table.Name, ordinal)
		}
		return []string{text}, nil
	}
	key := make([]string, len(plan.orderCols))
	for i, c := range plan.orderCols {
		text, ok, err := jsonString(raw[c])
		if err != nil || !ok {
			return nil, fmt.Errorf("d1: table %q row %d: primary-key column %q is NULL/absent; "+
				"cannot keyset-paginate (carry the table with a non-NULL key)", table.Name, ordinal, c)
		}
		key[i] = text
	}
	return key, nil
}

// buildD1Projection renders the SELECT list: for each user column, the
// typeof-aliased storage class and the CAST/hex exact-text value (aliased to the
// real column name so the decoded ir.Row is keyed correctly). For a rowid
// keyset it also projects CAST(<rowidName> AS TEXT) — the unshadowed implicit-
// rowid name — under the collision-free rowid alias so the next page's bound
// is exact.
func buildD1Projection(table *ir.Table, plan pagePlan) string {
	parts := make([]string, 0, len(table.Columns)*2+1)
	for i, c := range table.Columns {
		q := quoteIdent(c.Name)
		// typeof → the actual storage class (integer/real/text/blob/null);
		// value → EXACT text per storage class (blob→hex, real→the lossless
		// format('%!.20g') render, else CAST AS TEXT). The (typeof, value)
		// pair is built by the SHARED
		// [CapturedTypeofExpr] / [CapturedValueExpr] so this reader projection
		// and the sqlite-trigger capture trigger body (ADR-0135) can never drift
		// on the encoding — see CapturedValueExpr for the per-class rationale.
		parts = append(
			parts,
			CapturedTypeofExpr(q)+" AS "+quoteIdent(plan.typeofAliases[i]),
			CapturedValueExpr(q)+" AS "+q,
		)
	}
	if plan.useRowid {
		parts = append(parts, "CAST("+quoteIdent(plan.rowidName)+" AS TEXT) AS "+quoteIdent(plan.rowidAlias))
	}
	return strings.Join(parts, ", ")
}

// buildD1PageQuery assembles one page's SQL + positional params. The keyset
// predicate and ORDER BY are TABLE-QUALIFIED so they bind the typed column, not
// the CAST-text output alias (ordering the alias would sort integers lexically
// — the bug the MySQL keyset path hit). Bound values are passed as STRINGS so a
// > 2^53 bound is not rounded through a JSON number; SQLite applies the bound
// column's affinity to the text param, recovering the exact comparison.
func buildD1PageQuery(table *ir.Table, plan pagePlan, projection string, lastKey []string, pageSize int) (sql string, params []string) {
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(projection)
	b.WriteString(" FROM ")
	b.WriteString(quoteIdent(table.Name))

	qualified := qualifiedKeyCols(table.Name, plan.orderCols)
	if len(lastKey) > 0 {
		b.WriteString(" WHERE ")
		b.WriteString(keysetPredicate(qualified))
		params = lastKey
	}
	b.WriteString(" ORDER BY ")
	b.WriteString(strings.Join(qualified, ", "))
	b.WriteString(" LIMIT ")
	b.WriteString(strconv.Itoa(pageSize))
	return b.String(), params
}

// keysetPredicate renders the "strictly greater than the last key" comparison.
// For a single key column it is `col > ?`; for a composite key it is the SQL
// row-value comparison `(a, b) > (?, ?)`, which SQLite supports and which gives
// correct lexicographic tuple ordering without an unfolded OR-chain.
func keysetPredicate(qualifiedCols []string) string {
	if len(qualifiedCols) == 1 {
		return qualifiedCols[0] + " > ?"
	}
	placeholders := make([]string, len(qualifiedCols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return "(" + strings.Join(qualifiedCols, ", ") + ") > (" + strings.Join(placeholders, ", ") + ")"
}

// qualifiedKeyCols table-qualifies each key column (`"t"."c"`). The implicit
// rowid qualifies the same way (`"t"."rowid"` / `"t"."_rowid_"` / `"t"."oid"`
// is the rowid of t whenever t declares no column of that name).
func qualifiedKeyCols(table string, cols []string) []string {
	out := make([]string, len(cols))
	qt := quoteIdent(table)
	for i, c := range cols {
		out[i] = qt + "." + quoteIdent(c)
	}
	return out
}

// typeofPrefix returns a column-alias prefix guaranteed not to collide with any
// real column name (or "rowid"): it is extended until strictly longer than the
// longest such name, so every alias built from it (prefix+"t"+index,
// prefix+"rowid") is longer than — and therefore distinct from — every column.
func typeofPrefix(cols []*ir.Column) string {
	maxLen := len("rowid")
	for _, c := range cols {
		if len(c.Name) > maxLen {
			maxLen = len(c.Name)
		}
	}
	p := "__sluice_d1_"
	for len(p) <= maxLen {
		p += "_"
	}
	return p
}

// typeofAlias is the per-column typeof output alias for column index i.
func typeofAlias(prefix string, i int) string {
	return prefix + "t" + strconv.Itoa(i)
}
