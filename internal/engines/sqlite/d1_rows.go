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

	// pageSize overrides the maximum keyset page size; 0 means [d1PageSize].
	// It exists so tests can force multi-page stitching without staging
	// d1PageSize rows. The page controller still shrinks pages below it when
	// the rows are wide (see [pageRowsFor]).
	pageSize int

	mu  sync.Mutex
	err error
}

// effectivePageSize is the configured maximum page size, defaulting to
// [d1PageSize].
func (r *D1RowReader) effectivePageSize() int {
	if r.pageSize > 0 {
		return r.pageSize
	}
	return d1PageSize
}

// d1PageSize is the MAXIMUM rows per keyset page — the page a table of
// ordinary (sub-kilobyte) rows gets, exactly as before audit LA-2. It is not
// what keeps a response under the transport's cap: pages are sized in BYTES by
// [pageRowsFor] against [pageByteBudget], and this is only the ceiling that
// bounds per-page memory and the prefetch's one-page read-ahead for narrow
// rows. Within-table chunking parallelism remains a deferred follow-up.
const d1PageSize = 1000

// pageByteBudget is the response size a page is sized to stay under: half the
// client's cap ([d1MaxResponseBytes]). The gap is the controller's headroom —
// page N+1 is sized from page N's measured bytes per row, so rows may double
// in width between two pages before a page overflows the cap and has to be
// re-requested smaller. Half costs no throughput: measured on real D1
// (2026-09-02, 16 KiB BLOB rows) the transport ran at its full ~13–15 MB/s
// for every page of 3 MB or more, and a 4 MiB page hides the ~50 ms RTT as
// well as a 32 MB one did.
func pageByteBudget(responseCap int) int64 {
	return int64(responseCap / 2)
}

// pageRowsFor is the page controller's one rule: how many rows of the given
// response width (bytes per row — a server-side estimate for the first page,
// the previous page's measured value after that) fit the byte budget. At most
// maxRows, so rows narrower than budget/maxRows page exactly as before LA-2;
// at least 1, so a row wider than the budget is still requested, as a page
// of one — whether THAT fits is decided by the cap (twice the budget), and a
// row the cap cannot hold is refused by name in [fetchPages]. A zero or
// negative width means "unknown, or an empty table" and takes the full page.
func pageRowsFor(width, budget int64, maxRows int) int {
	if width <= 0 {
		return maxRows
	}
	n := budget / width
	switch {
	case n < 1:
		return 1
	case n > int64(maxRows):
		return maxRows
	default:
		return int(n)
	}
}

// rowWidthExpr renders the SQL estimate of one row's size in a query-API
// response: per column, the byte length of the value as [buildD1Projection]
// renders it — `hex()` doubles a BLOB, a NULL costs its four-letter literal,
// text and numbers cost their bytes (`CAST(… AS BLOB)` so an embedded NUL
// does not stop the count short) — plus the fixed JSON framing of each cell
// (column name, typeof alias and class, quotes and separators). It is an
// ESTIMATE: JSON escaping can inflate a text (each `"`/`\` doubles, a control
// character becomes `\u00XX`), and the controller's 2× headroom plus its
// overflow retry absorb that. Measured on real D1 (2026-09-02): 32,772
// estimated vs 32,839 actual bytes per 16 KiB-BLOB row.
func rowWidthExpr(table *ir.Table, plan pagePlan) string {
	overhead := len(`{},`)
	parts := make([]string, 0, len(table.Columns))
	for i, c := range table.Columns {
		q := quoteIdent(c.Name)
		parts = append(parts,
			"COALESCE(CASE typeof("+q+") WHEN 'blob' THEN 2 * LENGTH("+q+") ELSE LENGTH(CAST("+q+" AS BLOB)) END, 4)")
		// `"<name>":"…","<typeofAlias>":"integer",` around the value bytes.
		overhead += len(c.Name) + len(plan.typeofAliases[i]) + len(`"":"","":"",`) + len("integer")
	}
	if plan.useRowid {
		// `"<rowidAlias>":"<rowid digits>",` — a rowid is at most 20 digits.
		overhead += len(plan.rowidAlias) + len(`"":"",`) + 20
	}
	return strconv.Itoa(overhead) + " + " + strings.Join(parts, " + ")
}

// probeMaxRowWidth returns the estimated response bytes of the WIDEST row
// among the first page's candidates — the first maxRows rows in keyset order
// — so the first page can be sized to fit whatever those rows hold. It is MAX
// rather than an average because there is no measured page before the first;
// from the second page on the previous page's measured bytes per row take
// over ([fetchPages]). Cost: one LENGTH() pass over at most maxRows rows
// through the keyset index — 57 ms for 1,000 rows of 16 KiB BLOBs on real D1
// (2026-09-02). An empty table probes as 0 (the full page is requested and
// comes back empty). Every failure is loud; there is no fall-through to a
// blind full-size page.
func (r *D1RowReader) probeMaxRowWidth(ctx context.Context, table *ir.Table, plan pagePlan, maxRows int) (int64, error) {
	qualified := qualifiedKeyCols(table.Name, plan.orderCols)
	sql := "SELECT CAST(COALESCE(MAX(w), 0) AS TEXT) AS w FROM (SELECT " + rowWidthExpr(table, plan) +
		" AS w FROM " + quoteIdent(table.Name) + " ORDER BY " + strings.Join(qualified, ", ") +
		" LIMIT " + strconv.Itoa(maxRows) + ")"
	rows, err := r.client.queryRows(ctx, sql)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, fmt.Errorf("row-width probe returned %d rows; want 1", len(rows))
	}
	text, ok, err := jsonString(rows[0]["w"])
	if err != nil {
		return 0, fmt.Errorf("row-width probe result is not a text scalar: %w", err)
	}
	if !ok {
		return 0, errors.New("row-width probe result is NULL")
	}
	w, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("row-width probe result %q: %w", text, err)
	}
	return w, nil
}

// rowTooLargeHint is the remedy carried by [sluicecode.CodeBulkCopyRowTooLarge].
const rowTooLargeHint = "shrink or NULL the row's oversized values at the source, exclude the table (--exclude-table), or migrate it through `wrangler d1 export` + `sluice migrate --source-driver sqlite`, which streams the file and has no per-response cap"

// rowTooLarge is the coded refusal for a single row the response cap cannot
// hold — reached only after the page controller has shrunk the page to one
// row and that one row still overflowed. The row is the next in keyset order
// after lastKey (the first row when lastKey is empty); its key and estimated
// response size are read back with a one-row probe so the operator can find
// it. A failing probe still refuses, naming what is known — the refusal is
// never downgraded to the raw overflow error.
func (r *D1RowReader) rowTooLarge(ctx context.Context, table *ir.Table, plan pagePlan, lastKey []string, limit int) error {
	position := "the first row"
	if len(lastKey) > 0 {
		position = "the row after " + keyText(plan, lastKey)
	}
	desc := position
	if key, width, err := r.probeNextRow(ctx, table, plan, lastKey); err != nil {
		desc += fmt.Sprintf(" (its key and size could not be probed: %v)", err)
	} else {
		// The width is the LENGTH() estimate, i.e. the values before JSON
		// encoding — which can multiply a text of control characters
		// sixfold (`\u00XX`), so it may read well under the cap it exceeded.
		desc = fmt.Sprintf("the row with %s (about %d bytes of values before JSON encoding)", key, width)
	}
	return sluicecode.Wrap(sluicecode.CodeBulkCopyRowTooLarge, rowTooLargeHint,
		fmt.Errorf("d1: table %q: %s exceeds sluice's %d-byte response cap even as a page of one row; %s",
			table.Name, desc, limit, rowTooLargeHint))
}

// probeNextRow reads the key and estimated response width of the first row
// after lastKey in keyset order (the first row of the table when lastKey is
// empty) — the row [rowTooLarge] names. Only the key columns and a LENGTH()
// sum are projected, so the probe itself cannot overflow the cap.
func (r *D1RowReader) probeNextRow(ctx context.Context, table *ir.Table, plan pagePlan, lastKey []string) (key string, width int64, err error) {
	qualified := qualifiedKeyCols(table.Name, plan.orderCols)
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, col := range qualified {
		b.WriteString("CAST(" + col + " AS TEXT) AS " + quoteIdent("k"+strconv.Itoa(i)) + ", ")
	}
	b.WriteString("CAST(" + rowWidthExpr(table, plan) + " AS TEXT) AS w FROM " + quoteIdent(table.Name))
	var params []string
	if len(lastKey) > 0 {
		b.WriteString(" WHERE " + keysetPredicate(qualified))
		params = lastKey
	}
	b.WriteString(" ORDER BY " + strings.Join(qualified, ", ") + " LIMIT 1")
	rows, err := r.client.queryRows(ctx, b.String(), params...)
	if err != nil {
		return "", 0, err
	}
	if len(rows) != 1 {
		return "", 0, fmt.Errorf("probe returned %d rows; want 1", len(rows))
	}
	keyVals := make([]string, len(plan.orderCols))
	for i := range plan.orderCols {
		text, ok, err := jsonString(rows[0]["k"+strconv.Itoa(i)])
		if err != nil || !ok {
			return "", 0, fmt.Errorf("probe returned no key value for %s", plan.orderCols[i])
		}
		keyVals[i] = text
	}
	text, ok, err := jsonString(rows[0]["w"])
	if err != nil || !ok {
		return "", 0, errors.New("probe returned no width")
	}
	if width, err = strconv.ParseInt(text, 10, 64); err != nil {
		return "", 0, fmt.Errorf("probe width %q: %w", text, err)
	}
	return keyText(plan, keyVals), width, nil
}

// keyText renders a keyset key for an error message: `id=7` for a single key
// column (the implicit rowid included), `(a, b)=(1, x)` for a composite one.
func keyText(plan pagePlan, key []string) string {
	if len(plan.orderCols) == 1 {
		return plan.orderCols[0] + "=" + key[0]
	}
	return "(" + strings.Join(plan.orderCols, ", ") + ")=(" + strings.Join(key, ", ") + ")"
}

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
// textBytesExpr builds the server-side companion to decodeRow's textBytes
// (audit LA-4): the summed byte length of every cell whose storage class is
// text, over the columns this read projects.
//
// `length(CAST(c AS BLOB))` is the STORED byte count. The /query API
// replaces each invalid UTF-8 byte with U+FFFD server-side, three bytes for
// one, so a mangled cell arrives longer than it is stored and the two sums
// disagree — the independent expected value the decode path could not have
// (measured on live D1: a 3-byte cell `x'FFFE61'` arrives as 7 bytes).
//
// The typeof() guard is what makes the two sides count the same cells: a
// blob arrives as a JSON array and an integer as a number, so neither
// reaches decodeRow's string branch, and neither may reach this sum.
func textBytesExpr(table *ir.Table) string {
	parts := make([]string, 0, len(table.Columns))
	for _, c := range table.Columns {
		// A GENERATED column is derived and never written — staging skips it
		// on insert and a target recomputes it — so a mangle there cannot
		// persist, and counting it on one side only is how this first
		// diverged (a VIRTUAL column duplicating another text column made the
		// server read double the client, caught by
		// TestStageD1Table_RowidShadowMatrix). Excluded from BOTH sides.
		if c.IsGenerated() {
			continue
		}
		q := quoteIdent(c.Name)
		// Measure the EXPRESSION THE PROJECTION DELIVERS, not the raw column.
		// The two sides then align by construction rather than by coincidence:
		// the client counts len() of the string CapturedValueExpr produced, so
		// the server must weigh that same string. Measuring the bare column
		// diverged the moment a projected value was not the column itself —
		// caught by TestStageD1Table_RowidShadowMatrix on a generated column.
		parts = append(parts,
			"COALESCE(SUM(CASE WHEN "+CapturedTypeofExpr(q)+"='text' THEN length(CAST("+CapturedValueExpr(q)+" AS BLOB)) ELSE 0 END),0)")
	}
	if len(parts) == 0 {
		return "0"
	}
	return strings.Join(parts, " + ")
}

func (r *D1RowReader) countRows(ctx context.Context, table *ir.Table) (count, textBytes int64, err error) {
	rows, err := r.client.queryRows(ctx,
		"SELECT CAST(COUNT(*) AS TEXT) AS n, CAST("+textBytesExpr(table)+" AS TEXT) AS b FROM "+quoteIdent(table.Name))
	if err != nil {
		return 0, 0, err
	}
	if len(rows) != 1 {
		return 0, 0, fmt.Errorf("COUNT(*) returned %d rows; want 1", len(rows))
	}
	bText, bOK, bErr := jsonString(rows[0]["b"])
	if bErr != nil || !bOK {
		return 0, 0, fmt.Errorf("text-byte sum is not a text scalar: %w", bErr)
	}
	textBytes, bErr = strconv.ParseInt(bText, 10, 64)
	if bErr != nil {
		return 0, 0, fmt.Errorf("text-byte sum %q is not an integer: %w", bText, bErr)
	}
	text, ok, err := jsonString(rows[0]["n"])
	if err != nil {
		return 0, 0, fmt.Errorf("COUNT(*) result is not a text scalar: %w", err)
	}
	if !ok {
		return 0, 0, errors.New("COUNT(*) result is NULL")
	}
	count, err = strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("COUNT(*) result %q: %w", text, err)
	}
	return count, textBytes, nil
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

	// The LA-4 byte bracket, set on the final page only. srcTextBytes is
	// the server's own SUM over every text-storage cell of the projected
	// columns, read in the same round trip as the closing COUNT(*);
	// quiescent says the two COUNT(*) readings agreed, which is what makes
	// the byte comparison meaningful (a table being written under the read
	// changes both numbers legitimately).
	srcTextBytes int64
	quiescent    bool
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
		ordinal      int64 // 1-based row counter, for error context
		sawFinal     bool
		gotTextBytes int64 // LA-4: delivered bytes of every text-storage cell
		srcTextBytes int64 // the server's own sum, from the closing bracket
		quiescent    bool
	)
	for page := range pages {
		if page.final {
			srcTextBytes, quiescent = page.srcTextBytes, page.quiescent
		}
		if page.err != nil {
			r.setErr(fmt.Errorf("d1: table %q: %w", table.Name, page.err))
			return
		}
		for _, raw := range page.rows {
			ordinal++
			row, _, cellBytes, err := r.decodeRow(table, plan, raw, enc, ordinal)
			gotTextBytes += cellBytes
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
		return
	}
	// LA-4: the byte half of the bracket. D1 stores invalid-UTF-8 TEXT
	// intact but replaces every invalid byte with U+FFFD in the /query JSON
	// -- SERVER-SIDE, so the cell arrives as valid UTF-8 and no client-side
	// check can see it. The server's own summed byte length of its
	// text-storage cells is the independent expected value: a mangled cell
	// is DELIVERED longer than it is STORED (three bytes for one), so the
	// two sums disagree by exactly the inflation.
	//
	// Only when the table was quiescent: the row-count bracket has already
	// spoken for a table being written under the read, and a legitimate
	// concurrent write moves both numbers.
	// srcTextBytes < 0 is the "no byte evidence" sentinel: a transport that
	// cannot weigh its own text (the canned test doubles) reports it, and the
	// comparison is skipped rather than fabricated. A real D1 response always
	// carries the sum, so this cannot silently disarm the check in production
	// — the count bracket would have refused a response missing columns.
	if quiescent && srcTextBytes >= 0 && gotTextBytes != srcTextBytes {
		r.setErr(sluicecode.Wrap(
			sluicecode.CodeD1TextMangled,
			"read the affected columns as hex(col) and repair the values at the source",
			fmt.Errorf(
				"d1: table %q: the text this read received is %d bytes where the source stores %d, on a table whose "+
					"row count did not move -- D1 replaces every invalid UTF-8 byte with U+FFFD in its query response "+
					"(three bytes for one), so at least one cell was silently rewritten in transit and copying it would "+
					"persist the mangled value. The source is intact: hex(col) still returns the true bytes. Find the "+
					"affected rows by comparing length(CAST(col AS BLOB)) against length(col) per text column, repair "+
					"them, or exclude the table",
				table.Name, gotTextBytes, srcTextBytes,
			),
		))
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
//
// Page SIZE is adaptive (audit LA-2): the invariant it protects is the
// client's response cap, which D1 does nothing to keep a response under (a
// fixed 1,000-row page of 16 KiB BLOBs came back as 32.8 MB, and the 8 MiB cap
// then truncated it into `unexpected end of JSON input` — every table of rows
// wider than ~8 KB was unmigratable). The first page is sized from a
// server-side probe of the widest candidate row ([probeMaxRowWidth]); each
// later page from the previous page's MEASURED bytes per row ([pageRowsFor]
// against [pageByteBudget]); and a page that overflows the cap anyway is
// halved and re-requested at the same bound (idempotent under the keyset)
// until it fits — or, at one row, is refused by name with
// [sluicecode.CodeBulkCopyRowTooLarge] ([rowTooLarge]). Rows narrower than
// the budget over [d1PageSize] still get the full 1,000-row page.
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

	before, beforeBytes, err := r.countRows(ctx, table)
	if err != nil {
		deliver(d1Page{err: fmt.Errorf("count rows before the read: %w", err), final: true})
		return
	}

	projection := buildD1Projection(table, plan)
	maxRows := r.effectivePageSize()
	budget := pageByteBudget(r.client.responseCap())

	width, err := r.probeMaxRowWidth(ctx, table, plan, maxRows)
	if err != nil {
		deliver(d1Page{err: fmt.Errorf("probe row width before the read: %w", err), final: true})
		return
	}
	pageSize := pageRowsFor(width, budget, maxRows)

	var (
		lastKey []string // exact-text bound from the previous page
		ordinal int64    // rows fetched so far (error context for extractKey; the delivered total)
	)
	for {
		sql, params := buildD1PageQuery(table, plan, projection, lastKey, pageSize)
		rows, bodyBytes, err := r.client.queryRowsSized(ctx, sql, params...)
		var tooLarge *d1ResponseTooLargeError
		if errors.As(err, &tooLarge) {
			// The rows grew wider than the previous page predicted (or JSON
			// escaping inflated them past the estimate): halve and re-request
			// the SAME page — the keyset bound makes the retry idempotent. At
			// one row the page IS the row, and a row the cap cannot hold is
			// refused by name rather than retried forever.
			if pageSize > 1 {
				slog.InfoContext(ctx, "d1: page exceeded the response cap; re-requesting it smaller",
					slog.String("table", table.Name),
					slog.Int("rows_requested", pageSize),
					slog.Int("rows_retrying", pageSize/2),
					slog.Int("response_cap_bytes", tooLarge.limit))
				pageSize /= 2
				continue
			}
			deliver(d1Page{err: r.rowTooLarge(ctx, table, plan, lastKey, tooLarge.limit), final: true})
			return
		}
		if err != nil {
			// A failed page ends the stream when the consumer reaches it.
			deliver(d1Page{err: fmt.Errorf("read page: %w", err), final: true})
			return
		}
		ordinal += int64(len(rows))
		// A short (or empty) page is the last page: close the bracket and
		// let its verdict ride the page.
		if len(rows) < pageSize {
			after, afterBytes, err := r.countRows(ctx, table)
			quiescent := false
			if err != nil {
				err = fmt.Errorf("count rows after the read: %w", err)
			} else {
				err = checkRowCount(ctx, table.Name, before, after, ordinal)
				// Quiescent means BOTH readings held still, not just the row
				// count. A COUNT(*) is blind to an UPDATE, and an UPDATE that
				// changes a text cell's length moves the byte sum on its own —
				// so counting rows alone would have made the byte bracket
				// refuse a healthy live database whenever a write landed in an
				// already-delivered page. The count bracket 200 lines above
				// deliberately WARNs rather than refuses for exactly that
				// reason ("a live database would otherwise be unmigratable");
				// this must abstain on the same evidence. A mangle moves only
				// the DELIVERED side, so it still refuses.
				quiescent = before == after && beforeBytes == afterBytes
			}
			deliver(d1Page{rows: rows, err: err, final: true, srcTextBytes: afterBytes, quiescent: quiescent})
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
		// Size the next page from THIS page's measured bytes per row.
		pageSize = pageRowsFor(int64(bodyBytes)/int64(len(rows)), budget, maxRows)
	}
}

// decodeRow turns one D1 result row into an [ir.Row], and returns the exact-text
// keyset bound for the next page. Every decode error is wrapped with
// table/column/row so the operator can find the offending cell (the loud-failure
// tenet).
// textBytes (audit LA-4) is the DELIVERED byte length of every cell whose
// SQLite storage class is text, summed across the row. The projection
// already carries typeof() per column, so this is exact and free: it counts
// precisely the cells the server-side sum counts, and nothing else. A blob
// arrives as a JSON array and an integer as a number (measured on live D1),
// so neither can drift into either total.
func (r *D1RowReader) decodeRow(table *ir.Table, plan pagePlan, raw d1Row, enc dateEncoding, ordinal int64) (row ir.Row, key []string, textBytes int64, err error) {
	row = make(ir.Row, len(table.Columns))
	for i, col := range table.Columns {
		typeofText, ok, err := jsonString(raw[plan.typeofAliases[i]])
		if err != nil {
			return nil, nil, 0, fmt.Errorf("d1: table %q column %q row %d: decode typeof: %w",
				table.Name, col.Name, ordinal, err)
		}
		if !ok {
			typeofText = "null"
		}
		storage, err := d1StorageValue(typeofText, raw[col.Name])
		if err != nil {
			return nil, nil, 0, fmt.Errorf("d1: table %q column %q row %d: %w",
				table.Name, col.Name, ordinal, err)
		}
		// Generated columns are excluded on BOTH sides (see textBytesExpr): a
		// derived value is never written, so a mangle there cannot persist,
		// and staging already skips them on insert. Counting them here but
		// not there is what made the server read double the client on a
		// VIRTUAL column duplicating another (TestStageD1Table_RowidShadowMatrix).
		if typeofText == "text" && !col.IsGenerated() {
			if str, isStr := storage.(string); isStr {
				textBytes += int64(len(str))
			}
		}
		v, err := decodeCell(storage, col.Type, enc)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("d1: table %q column %q row %d: %w",
				table.Name, col.Name, ordinal, err)
		}
		row[col.Name] = v
	}

	key, err = r.extractKey(table, plan, raw, ordinal)
	if err != nil {
		return nil, nil, 0, err
	}
	return row, key, textBytes, nil
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
