// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/rowpredicate"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ADR-0173 Phase 2 — continuous *filtered* sync (the CDC leg).
//
// Phase 1 pushes a `--where` predicate down into the source read for
// migrate. The sync SNAPSHOT (cold-start) leg reuses that push-down
// verbatim ([migcore.ApplyRowFilters] on the snapshot RowReader). The CDC
// leg is the hard part: there is no source-side stream filter, so the
// predicate is evaluated CLIENT-SIDE per [ir.Change] over the decoded
// before/after images, and translated to the correct TARGET op — the
// ADR-0173 "row-move" table.
//
// Both legs are driven by the SAME [Streamer.RowFilters] map (single
// source of truth), and the client-side evaluator ([rowpredicate])
// deliberately REFUSES any predicate it cannot evaluate faithfully — so
// the source-SQL snapshot evaluation and the client-side CDC evaluation
// agree by construction for every predicate that compiles.

// whereCDCFilter holds the compiled client-side predicates for a filtered
// continuous sync, keyed by lower-cased source table name. A nil
// *whereCDCFilter means "no --where" (the byte-identical default).
type whereCDCFilter struct {
	preds map[string]*rowpredicate.Predicate
	// pkCols is the lower-cased primary-key column set per filtered table.
	// The CDC reader emits the FULL before-image for filtered tables (so the
	// predicate can read every OLD column); after evaluating, the intercept
	// re-narrows the before-image to these key columns before forwarding, so
	// the applier's WHERE stays key-only (preserving the Bug-8/88/92 fix).
	// Empty for a PK-less table (the before-image is then left full — the
	// same PK-less fallback the readers use).
	pkCols map[string]map[string]bool
	// refCols is the lower-cased set of columns each table's predicate
	// REFERENCES (from [rowpredicate.Predicate.Columns]). route() checks the
	// decoded before-image (UPDATE/DELETE) AND after-image (UPDATE) carry
	// every one before evaluating a filtered change: a missing column reads
	// as UNKNOWN in the evaluator and silently mis-classifies the row-move —
	// a partial BEFORE turns a move-OUT into a drop (leaking the row on the
	// target), a partial AFTER turns an in-scope UPDATE into a move-OUT (a
	// spurious DELETE — audit 2026-07-23 D0-1). This is the belt-and-
	// suspenders floor for a partial image that slips past the reader's own
	// guards (a self-hosted Vitess on binlog_row_image != FULL; an
	// unchanged-TOAST After the PG reader failed to backfill) — the
	// SLUICE-E-WHERE-CDC-BEFORE-IMAGE / -AFTER-IMAGE refusals at the exact
	// point the columns that matter are known. Keyed by lower-cased source
	// table name.
	refCols map[string]map[string]bool
	// padSpaceCols records, for each filtered table whose predicate references a
	// PAD-SPACE-collation string column, one such column (lower-cased). On a
	// VStream flavor the server-side filter push-down evaluates the pushed WHERE
	// with NO-PAD semantics regardless of the column's PAD_ATTRIBUTE, so such a
	// predicate silently drops trailing-space rows the (PAD-faithful) client
	// route keeps (audit 2026-07-19 A0). On a VStream source the pipeline routes
	// those tables through the client-side COPY fallback (#66): it OMITS them from
	// the server-side push (streams them unfiltered) and filters them client-side
	// with the PAD-faithful predicate ([clientCopyFilter]). Other flavors (vanilla
	// MySQL binlog, Postgres) push through the source's own PAD-faithful `=` and
	// need no fallback. Keyed by lower-cased source table name.
	padSpaceCols map[string]string
	// temporalCoercionCols records, for each filtered table whose predicate
	// ENGAGES the source engine's temporal-literal coercion — a literal
	// Compile normalized (sub-µs fraction, or a date-truncated literal on a
	// PG source), or a time-bearing literal on a DATE column (the
	// MySQL-family promote comparison) — one such column (lower-cased). On
	// a VStream flavor the server-side filter push-down is evaluated by
	// vtgate's evalengine, whose coercion of a finer-than-µs fraction or a
	// date-vs-datetime-literal comparison is an UNVERIFIED surface
	// (ADR-0174 residuals; cluster ground-truth deferred to the
	// vitess-cluster-validator) — so rather than push a possibly-divergent
	// filter, such tables take the same A0 client-side fallback as
	// padSpaceCols: streamed unfiltered server-side, filtered client-side
	// with the engine-faithful predicate (review F3). Other flavors
	// (vanilla MySQL binlog, Postgres) evaluate the pushed predicate with
	// the source's OWN semantics — the exact semantics Compile normalized
	// to — and need no fallback. Keyed by lower-cased source table name.
	temporalCoercionCols map[string]string
	// padSpaceFloatSingleCols records, for each client-copy-FORCED table
	// (padSpaceCols or temporalCoercionCols, above)
	// whose predicate ALSO compares a single-precision FLOAT column BY VALUE, one
	// such column (lower-cased). It is the SL1 hazard (audit 2026-07-19): a
	// client-copy-forced table takes the client-side COPY fallback, and the cold-start
	// COPY carries a single-precision FLOAT column's value DISPLAY-ROUNDED by
	// mysqld's float->text formatter (the exact re-read repair runs AFTER copy, so
	// the keep predicate sees the lossy pre-repair value). A FLOAT stored 0.1
	// promotes to 0.10000000149 so `v > 0.1` keeps the row at the source, but the
	// rounded carrier renders "0.1" -> 0.1 > 0.1 = false -> the keep DROPS a
	// source-in-scope row that is then never written and never repaired: silent
	// loss. DOUBLE is full-precision in the carrier (only single-precision FLOAT
	// rounds) and is NOT recorded. Recorded only for a VALUE comparison
	// ([rowpredicate.Predicate.ValueComparedColumns]): FLOAT equality is refused at
	// compile, so a value comparison is an ordering term or an IN membership —
	// both sensitive to the rounded bits; a `FLOAT IS [NOT] NULL` presence test is
	// display-round-INSENSITIVE and is NOT recorded (else it would be a
	// wrong-refusal, audit 2026-07-19c F-WR-1). preflight REFUSES loudly on a
	// VStream source when this is non-empty rather than mis-filter. Keyed by
	// lower-cased source table name.
	padSpaceFloatSingleCols map[string]string
	// serverFiltered marks the tables whose predicate is ALSO pushed into a
	// server-side filter (today: the ADR-0176 PG publication row filter),
	// keyed by lower-cased source table name. Post-push-down the client
	// evaluator should be a no-op for these tables — the server already
	// excluded everything it would — so a client-side DROP on one of them is
	// the observable, BENIGN divergence direction (server laxer than client)
	// and route() logs it at DEBUG (ADR-0176 §2). The dangerous direction
	// (server stricter) is unobservable client-side by construction, which is
	// why the real-server oracle exists. Set by [preflightRowFilters] via
	// [whereCDCFilter.markServerFiltered].
	serverFiltered map[string]bool
}

// markServerFiltered records the tables whose predicate is pushed into a
// server-side filter, enabling route()'s ADR-0176 §2 belt DEBUG logging.
// Keys are canonicalized to lower case to match route()'s lookups.
func (f *whereCDCFilter) markServerFiltered(tables []string) {
	if f == nil || len(tables) == 0 {
		return
	}
	f.serverFiltered = make(map[string]bool, len(tables))
	for _, t := range tables {
		f.serverFiltered[strings.ToLower(t)] = true
	}
}

// debugServerDeliveredDrop emits the ADR-0176 §2 belt observation: the
// client evaluator dropped a change the server-side publication row filter
// delivered. Benign by direction (server laxer than client — merely
// wasteful), routine below PG 15 (where the engine drops the push-down and
// the client is the only filter), and DEBUG-level so steady state stays
// quiet. No-op for tables that aren't server-filtered.
func (f *whereCDCFilter) debugServerDeliveredDrop(op, schema, table string) {
	if !f.serverFiltered[strings.ToLower(table)] {
		return
	}
	slog.Debug(
		"continuous filtered sync: client evaluator dropped a change on a table whose predicate is pushed down server-side (benign direction; on a PG 15+ source the publication filter should already have excluded it — ADR-0176 §2 belt)",
		slog.String("op", op),
		slog.String("table", qualifiedTableName(schema, table)),
	)
}

// buildWhereCDCFilter compiles each `--where TABLE=<predicate>` string into
// a client-side-evaluable predicate, resolving each referenced column
// against the source schema (for type/collation fidelity). Every predicate
// that cannot be faithfully evaluated client-side is refused loudly with a
// [sluicecode.CodeWhereCDCUnsupportedPredicate] coded error; a `--where`
// key that names no source table is refused too (it can never take effect).
func buildWhereCDCFilter(resolver ir.CollationResolver, rowFilters map[string]string, schema *ir.Schema, strictCollation bool) (*whereCDCFilter, error) {
	if len(rowFilters) == 0 {
		return nil, nil
	}
	byName := make(map[string]*ir.Table, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil {
			byName[strings.ToLower(t.Name)] = t
		}
	}
	preds := make(map[string]*rowpredicate.Predicate, len(rowFilters))
	pkCols := make(map[string]map[string]bool, len(rowFilters))
	refCols := make(map[string]map[string]bool, len(rowFilters))
	var (
		padSpaceCols            map[string]string
		temporalCoercionCols    map[string]string
		padSpaceFloatSingleCols map[string]string
	)
	for table, predicate := range rowFilters {
		tbl, ok := byName[strings.ToLower(table)]
		if !ok {
			return nil, sluicecode.Wrap(
				sluicecode.CodeWhereCDCUnsupportedPredicate,
				"remove the --where entry or correct the table name",
				fmt.Errorf(
					"continuous filtered sync: --where names table %q, which is not in the synced source schema "+
						"(it may be misspelled or excluded by --include/--exclude-table); a filter that matches no "+
						"table would silently do nothing",
					table,
				),
			)
		}
		infos := rowpredicate.ColumnInfosFromIR(resolver, tbl.Columns, strictCollation)
		p, err := rowpredicate.Compile(table, predicate, infos)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(table)
		preds[key] = p
		pkCols[key] = primaryKeyColumnSet(tbl)
		cols := make(map[string]bool)
		for _, c := range p.Columns() {
			cols[c] = true // already lower-cased by the compiler
		}
		refCols[key] = cols
		// A0: record the first PAD-SPACE string column this predicate references,
		// so a VStream source routes it through the client-side COPY fallback
		// (its server-side filter is NO-PAD). infos is keyed by lower-cased
		// column name, matching p.Columns().
		for _, c := range p.Columns() {
			if ci := infos[c]; ci.Family == rowpredicate.FamilyString && ci.PadSpace {
				if padSpaceCols == nil {
					padSpaceCols = make(map[string]string)
				}
				padSpaceCols[key] = c
				break
			}
		}
		// F3: record the first column whose comparison engages the engine's
		// temporal-literal coercion, so a VStream source routes the table
		// through the A0 client-side fallback (see the temporalCoercionCols
		// doc — the vtgate evalengine's coercion is unverified).
		if col := temporalCoercionColumn(tbl, p); col != "" {
			if temporalCoercionCols == nil {
				temporalCoercionCols = make(map[string]string)
			}
			temporalCoercionCols[key] = col
		}
		// SL1: a table that will take the client-side COPY fallback
		// (pad-forced or temporal-coercion-forced above) AND compares a
		// single-precision FLOAT column by VALUE would
		// evaluate that comparison on the DISPLAY-ROUNDED cold-start carrier value
		// (see the padSpaceFloatSingleCols doc). Record the first such column so
		// preflight can REFUSE on a VStream source. Two narrowings keep this from
		// over-refusing: (1) only when the table actually takes the fallback — a
		// server-filtered table is filtered on the EXACT value, so its FLOAT
		// comparison is faithful; (2) only a VALUE comparison (ValueComparedColumns,
		// not the full Columns) — a `FLOAT IS [NOT] NULL` is a presence test whose
		// result cannot depend on the rounded bits, so refusing it would be a
		// wrong-refusal (audit 2026-07-19c F-WR-1).
		if padSpaceCols[key] != "" || temporalCoercionCols[key] != "" {
			singleFloats := singlePrecisionFloatColumnSet(tbl)
			for _, c := range p.ValueComparedColumns() {
				if singleFloats[c] {
					if padSpaceFloatSingleCols == nil {
						padSpaceFloatSingleCols = make(map[string]string)
					}
					padSpaceFloatSingleCols[key] = c
					break
				}
			}
		}
	}
	return &whereCDCFilter{
		preds:                   preds,
		pkCols:                  pkCols,
		refCols:                 refCols,
		padSpaceCols:            padSpaceCols,
		temporalCoercionCols:    temporalCoercionCols,
		padSpaceFloatSingleCols: padSpaceFloatSingleCols,
	}, nil
}

// temporalCoercionColumn returns one lower-cased column of tbl whose
// comparison ENGAGES the source engine's temporal-literal coercion — a
// literal Compile rewrote to the engine's granularity, or a time-bearing
// literal against a DATE column (the promote-family full-instant compare) —
// or "" when the predicate has no such term. These are exactly the terms
// whose RAW text a VStream server-side push would hand to vtgate's
// evalengine, whose own coercion is unverified (review F3; ADR-0174
// residuals), so the caller records them for the A0 client-side fallback.
func temporalCoercionColumn(tbl *ir.Table, p *rowpredicate.Predicate) string {
	var types map[string]ir.Type
	for _, term := range p.PushdownTerms() {
		if term.TemporalLiteralNormalized {
			return term.Column
		}
		if !term.TemporalLiteralTimeBearing {
			continue
		}
		if types == nil {
			types = make(map[string]ir.Type, len(tbl.Columns))
			for _, c := range tbl.Columns {
				if c != nil {
					types[strings.ToLower(c.Name)] = c.Type
				}
			}
		}
		if _, isDate := types[term.Column].(ir.Date); isDate {
			return term.Column
		}
	}
	return ""
}

// singlePrecisionFloatColumnSet returns the lower-cased names of tbl's
// single-precision (32-bit) FLOAT columns — the ones mysqld's float->text COPY
// carrier display-rounds (DOUBLE transits exactly). It builds on
// [migcore.SinglePrecisionFloatColumns] (the same source-of-truth the float-repair
// planner keys off) so the SL1 guard and the repair stay in lock-step on what the
// carrier rounds (audit 2026-07-19c J-ARCH-2).
func singlePrecisionFloatColumnSet(tbl *ir.Table) map[string]bool {
	cols := migcore.SinglePrecisionFloatColumns(tbl)
	if len(cols) == 0 {
		return nil
	}
	out := make(map[string]bool, len(cols))
	for _, col := range cols {
		out[strings.ToLower(col.Name)] = true
	}
	return out
}

// clientCopyFloatSingleColumns returns the map of PAD-SPACE-forced table
// (lower-cased) -> a single-precision FLOAT column its predicate compares BY
// VALUE (an ordering term or IN membership, not IS NULL — the SL1 hazard).
// Non-empty only when a client-copy-fallback table would evaluate a
// single-precision FLOAT value comparison on the lossy cold-start COPY carrier.
// Nil-safe.
func (f *whereCDCFilter) clientCopyFloatSingleColumns() map[string]string {
	if f == nil {
		return nil
	}
	return f.padSpaceFloatSingleCols
}

// serverSidePadSpaceColumns returns the map of filtered table (lower-cased) →
// one PAD-SPACE-collation string column its predicate references. Non-empty
// only when a predicate would diverge under a NO-PAD server-side filter
// (audit 2026-07-19 A0). Nil-safe.
func (f *whereCDCFilter) serverSidePadSpaceColumns() map[string]string {
	if f == nil {
		return nil
	}
	return f.padSpaceCols
}

// serverSideTemporalCoercionColumns returns the map of filtered table
// (lower-cased) → one column whose comparison engages the source engine's
// temporal-literal coercion (review F3 — see temporalCoercionCols). Nil-safe.
func (f *whereCDCFilter) serverSideTemporalCoercionColumns() map[string]string {
	if f == nil {
		return nil
	}
	return f.temporalCoercionCols
}

// clientForcedTables returns the lower-cased set of filtered tables the
// VStream A0 fallback must stream UNFILTERED server-side and filter
// client-side: the PAD-SPACE tables (audit 2026-07-19 A0) plus the
// temporal-coercion tables (review F3). Nil when no table is forced.
// Nil-safe.
func (f *whereCDCFilter) clientForcedTables() map[string]bool {
	if f == nil || (len(f.padSpaceCols) == 0 && len(f.temporalCoercionCols) == 0) {
		return nil
	}
	out := make(map[string]bool, len(f.padSpaceCols)+len(f.temporalCoercionCols))
	for t := range f.padSpaceCols {
		out[t] = true
	}
	for t := range f.temporalCoercionCols {
		out[t] = true
	}
	return out
}

// clientCopyFilter returns the engine-faithful cold-start COPY keep-predicate
// for the A0 fallback (audit 2026-07-19 + review F3): on a VStream source the
// CLIENT-FORCED tables — PAD-SPACE columns (the NO-PAD server filter can't
// reproduce their `=`) and temporal-coercion columns (the evalengine's
// literal coercion is unverified) — stream UNFILTERED server-side, so this
// narrows them client-side with the SAME compiled predicate the CDC route()
// uses. A table that is NOT forced (still server-filtered) is kept
// unconditionally, so the filter only touches the unfiltered-server-side
// tables. Returns nil when no table is forced (no fallback needed). The row
// is fed to Eval exactly as route() feeds a CDC row, so the casing/collation
// contract is identical.
func (f *whereCDCFilter) clientCopyFilter() func(table string, row ir.Row) bool {
	forced := f.clientForcedTables()
	if len(forced) == 0 {
		return nil
	}
	return func(table string, row ir.Row) bool {
		key := strings.ToLower(table)
		if !forced[key] {
			return true // server-filtered → keep
		}
		p := f.preds[key]
		if p == nil {
			return true
		}
		return p.Eval(row)
	}
}

// primaryKeyColumnSet returns the lower-cased primary-key column names of
// tbl as a set, or nil when the table has no primary key (the before-image
// is then left un-narrowed, matching the readers' PK-less fallback).
func primaryKeyColumnSet(tbl *ir.Table) map[string]bool {
	if tbl.PrimaryKey == nil || len(tbl.PrimaryKey.Columns) == 0 {
		return nil
	}
	set := make(map[string]bool, len(tbl.PrimaryKey.Columns))
	for _, c := range tbl.PrimaryKey.Columns {
		set[strings.ToLower(c.Column)] = true
	}
	return set
}

// narrowBefore returns a copy of before restricted to table's primary-key
// columns — the applier's key-only WHERE source (the reader emitted the
// full before-image so the predicate could be evaluated; the WHERE must
// still be key-only per Bug-8/88/92). Returns before unchanged when the
// table has no known primary key.
func (f *whereCDCFilter) narrowBefore(table string, before ir.Row) ir.Row {
	pk := f.pkCols[strings.ToLower(table)]
	if len(pk) == 0 || before == nil {
		return before
	}
	out := make(ir.Row, len(pk))
	for name, v := range before {
		if pk[strings.ToLower(name)] {
			out[name] = v
		}
	}
	return out
}

// imageMissingColumn reports whether a decoded row image (before OR after)
// carries every column the table's predicate references. It returns
// (missingColumn, false) naming the first absent referenced column, or
// ("", true) when the image is complete (or the table has no filter /
// references no columns). Matching is case-insensitive: the row is keyed by
// the source's column-name casing while the predicate's referenced columns
// are lower-cased.
//
// This is the load-bearing guard against a partial row image on a filtered
// table: the evaluator treats an ABSENT column the same as SQL NULL
// (UNKNOWN), so a move evaluated over an incomplete image mis-classifies
// silently — a partial BEFORE turns a move-OUT into "was never in scope"
// (leaking the row on the target), and a partial AFTER turns an in-scope
// UPDATE into a move-OUT (emitting a spurious DELETE — the audit 2026-07-23
// D0-1 unchanged-TOAST class, normally prevented by the PG reader's
// backfill and checked here as the belt). Refusing keeps the loud-failure
// floor at the exact point the relevant columns are known (the reader
// can't know which columns a predicate reads).
func (f *whereCDCFilter) imageMissingColumn(table string, img ir.Row) (string, bool) {
	need := f.refCols[strings.ToLower(table)]
	if len(need) == 0 || img == nil {
		return "", true
	}
	present := make(map[string]bool, len(img))
	for name := range img {
		present[strings.ToLower(name)] = true
	}
	// Deterministic report order so the refusal message is stable.
	missing := make([]string, 0)
	for col := range need {
		if !present[col] {
			missing = append(missing, col)
		}
	}
	if len(missing) == 0 {
		return "", true
	}
	sort.Strings(missing)
	return missing[0], false
}

// partialBeforeImage builds the coded refusal for a filtered UPDATE/DELETE
// whose before-image is present but omits a column the predicate references —
// the source is not delivering full row before-images for the filtered table
// (a self-hosted Vitess / MySQL on binlog_row_image != FULL that slipped past
// the reader's own guards). Evaluating the row-move on a partial before-image
// would silently mis-classify it, so the stream stops loudly instead.
func partialBeforeImage(op, schema, table, column string) error {
	return sluicecode.Wrap(
		sluicecode.CodeWhereCDCBeforeImage,
		"ensure MySQL binlog_row_image=FULL / PG REPLICA IDENTITY FULL on the filtered table, then restart the sync",
		fmt.Errorf(
			"continuous filtered sync: a %s on filtered table %s arrived with a before-image that omits column %q, "+
				"which the --where predicate references — so --where cannot decide whether the row moved out of the "+
				"filter's scope. The source must deliver FULL row before-images for a filtered table (MySQL "+
				"binlog_row_image=FULL, PG REPLICA IDENTITY FULL); a partial image reached the reader, and evaluating "+
				"the row-move over the missing column would read it as NULL and could silently leak an out-of-scope "+
				"row. The stream stops here rather than mis-classify the row",
			op, qualifiedTableName(schema, table), column,
		),
	)
}

// partialAfterImage builds the coded refusal for a filtered UPDATE whose
// AFTER-image omits a column the predicate references — the After-side
// sibling of [partialBeforeImage] (audit 2026-07-23 D0-1). The known
// producer of an incomplete After is pgoutput's unchanged-TOAST omission,
// which the PG reader backfills from the RI-FULL Before before the change
// reaches this filter; anything that still arrives incomplete would be
// mis-classified as a move-OUT and DELETE an in-scope row on the target,
// so the stream stops loudly instead.
func partialAfterImage(schema, table, column string) error {
	return sluicecode.Wrap(
		sluicecode.CodeWhereCDCAfterImage,
		"ensure MySQL binlog_row_image=FULL / PG REPLICA IDENTITY FULL on the filtered table, then restart the sync; if the source is a Postgres 15+ logical stream this shape should be impossible (the reader backfills unchanged-TOAST columns) — report it as a bug",
		fmt.Errorf(
			"continuous filtered sync: an UPDATE on filtered table %s arrived with an AFTER-image that omits column %q, "+
				"which the --where predicate references — so --where cannot decide whether the row moved out of the "+
				"filter's scope. Evaluating the row-move over the missing column would read it as NULL and could emit a "+
				"spurious DELETE for a row still in scope at the source. The stream stops here rather than mis-classify "+
				"the row",
			qualifiedTableName(schema, table), column,
		),
	)
}

// fullBeforeImageSetter is the optional CDC-reader surface a filtered
// continuous sync uses to request UN-narrowed before-images for the
// filtered tables (ADR-0173 Phase 2). The reader normally narrows the
// before-image to identity-key columns (Bug-8/88/92); for the named tables
// it emits the full decoded old tuple so the client-side row-move eval can
// read every OLD column, and [whereCDCFilter.narrowBefore] re-narrows to
// the key columns before the applier builds its WHERE. MySQL and Postgres
// CDC readers implement it.
//
// It is an ALIAS of [ir.FullBeforeImageSetter] so the concrete engine
// readers can compile-assert conformance in their own packages
// (capabilities_assert.go); see the ir declaration for why the surface is
// pinned there (audit 2026-07-18 M-A2).
type fullBeforeImageSetter = ir.FullBeforeImageSetter

// applyFullBeforeImageTables wires the filtered-table set onto the CDC
// reader so it emits full before-images for those tables. It is a no-op
// when no `--where` is configured. When a filter IS configured but the
// reader does NOT implement [fullBeforeImageSetter], it refuses loudly:
// silently accepting the reader's PK-narrowed before-image would make the
// row-move evaluation mis-classify a move-OUT (before-image key-only ⇒ the
// predicate reads NULL for the filtered column ⇒ UNKNOWN ⇒ the now-out-of-
// scope row leaks on the target) — the exact silent-loss class this
// refusal exists to prevent.
func (s *Streamer) applyFullBeforeImageTables(reader any) error {
	if s.whereFilter == nil {
		return nil
	}
	setter, ok := reader.(fullBeforeImageSetter)
	if !ok {
		return sluicecode.Wrap(
			sluicecode.CodeWhereCDCBeforeImage,
			"use `sluice migrate --where` for a one-shot filtered copy instead",
			fmt.Errorf(
				"continuous filtered sync: source engine %q's CDC reader cannot emit full row before-images, "+
					"which the --where row-move evaluation requires (it narrows the before-image to primary-key "+
					"columns, so the filtered column would read NULL and a move-OUT could silently leak an "+
					"out-of-scope row)",
				s.Source.Name(),
			),
		)
	}
	tables := make(map[string]bool, len(s.RowFilters))
	for t := range s.RowFilters {
		tables[t] = true
	}
	setter.SetFullBeforeImageTables(tables)
	return nil
}

// predicateFor returns the compiled predicate for a change's table, or nil
// when the table carries no `--where` (its changes flow through unfiltered).
func (f *whereCDCFilter) predicateFor(table string) *rowpredicate.Predicate {
	if f == nil {
		return nil
	}
	return f.preds[strings.ToLower(table)]
}

// filteredTables returns the sorted list of source table names carrying a
// `--where` predicate — the set the before-image preflight must check.
func (s *Streamer) filteredTableNames() []string {
	names := make([]string, 0, len(s.RowFilters))
	for t := range s.RowFilters {
		names = append(names, t)
	}
	return names
}

// interceptWhereFilter wraps the CDC change channel with the ADR-0173
// row-move dispatch. For each row-bearing change on a filtered table it
// evaluates the predicate on the before/after images and translates to the
// correct TARGET op (see the table below); every other change (non-filtered
// table, Truncate, SchemaSnapshot, Tx boundary) flows through verbatim.
//
//	source op | before | after | target op
//	----------|--------|-------|-----------------------------
//	INSERT    |   -    |  yes  | INSERT
//	INSERT    |   -    |  no   | drop
//	DELETE    |  yes   |   -   | DELETE
//	DELETE    |  no    |   -   | drop
//	UPDATE    |  no    |  no   | drop
//	UPDATE    |  yes   |  yes  | UPDATE (as-is)
//	UPDATE    |  no    |  yes  | INSERT the after-image   (move-IN)
//	UPDATE    |  yes   |  no   | DELETE by key            (move-OUT)
//
// The move-IN → INSERT and move-OUT → DELETE cells are the load-bearing
// correctness: a naive per-event filter would drop a move-OUT (leaking a
// now-out-of-scope row on the target) and drop a move-IN (the target has no
// base row for the UPDATE, so the newly-in-scope row would never appear).
//
// A filtered UPDATE/DELETE whose before-image is absent is a
// [sluicecode.CodeWhereCDCBeforeImage] refusal: the preflight guarantees
// full before-images, but a mid-stream partial image (a slipped-past global,
// a resume replaying an old segment) must fail loudly rather than
// mis-classify the move — the same discipline as the MySQL Bug-193 belt.
//
// nil filter is a verbatim pass-through (no goroutine). On a refusal the
// intercept stores the error in errStore and closes out, mirroring
// [interceptAddColumnForward].
func interceptWhereFilter(
	ctx context.Context,
	in <-chan ir.Change,
	filter *whereCDCFilter,
	errStore *atomic.Pointer[error],
) <-chan ir.Change {
	if filter == nil {
		return in
	}
	out := make(chan ir.Change, migcore.RowChanBuffer)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case c, ok := <-in:
				if !ok {
					return
				}
				emit, err := filter.route(c)
				if err != nil {
					errStore.Store(&err)
					return
				}
				for _, e := range emit {
					if !forwardChange(ctx, out, e) {
						return
					}
				}
			}
		}
	}()
	return out
}

// route applies the row-move table to one change, returning the change(s)
// to forward (nil = drop) or a refusal. It is pure (no I/O), so the
// row-move semantics are unit-testable without a channel harness.
func (f *whereCDCFilter) route(c ir.Change) ([]ir.Change, error) {
	switch e := c.(type) {
	case ir.Insert:
		p := f.predicateFor(e.Table)
		if p == nil || p.Eval(e.Row) {
			return []ir.Change{e}, nil
		}
		f.debugServerDeliveredDrop("INSERT", e.Schema, e.Table)
		return nil, nil
	case ir.Delete:
		p := f.predicateFor(e.Table)
		if p == nil {
			return []ir.Change{e}, nil
		}
		if e.Before == nil {
			return nil, missingBeforeImage("DELETE", e.Schema, e.Table)
		}
		if miss, ok := f.imageMissingColumn(e.Table, e.Before); !ok {
			return nil, partialBeforeImage("DELETE", e.Schema, e.Table, miss)
		}
		if p.Eval(e.Before) {
			// Re-narrow the (full) before-image to the key columns for the
			// applier's WHERE.
			e.Before = f.narrowBefore(e.Table, e.Before)
			return []ir.Change{e}, nil
		}
		f.debugServerDeliveredDrop("DELETE", e.Schema, e.Table)
		return nil, nil
	case ir.Update:
		p := f.predicateFor(e.Table)
		if p == nil {
			return []ir.Change{e}, nil
		}
		if e.Before == nil {
			return nil, missingBeforeImage("UPDATE", e.Schema, e.Table)
		}
		if miss, ok := f.imageMissingColumn(e.Table, e.Before); !ok {
			return nil, partialBeforeImage("UPDATE", e.Schema, e.Table, miss)
		}
		// After-side completeness belt (audit 2026-07-23 D0-1): an AFTER
		// image missing a predicate column would evaluate UNKNOWN→false and
		// turn an in-scope UPDATE into a move-OUT — a spurious DELETE on the
		// target. The PG reader backfills unchanged-TOAST omissions from the
		// RI-FULL Before, so this refusal fires only for a shape that
		// slipped past every reader-side guarantee; refusing beats deleting
		// a row the source still holds.
		if miss, ok := f.imageMissingColumn(e.Table, e.After); !ok {
			return nil, partialAfterImage(e.Schema, e.Table, miss)
		}
		before := p.Eval(e.Before)
		after := p.Eval(e.After)
		switch {
		case before && after:
			// Stays in scope. Re-narrow Before to the key columns for the
			// applier's WHERE (After keeps every column for the SET clause).
			e.Before = f.narrowBefore(e.Table, e.Before)
			return []ir.Change{e}, nil
		case !before && !after:
			f.debugServerDeliveredDrop("UPDATE", e.Schema, e.Table)
			return nil, nil // never in scope
		case !before && after:
			// move-IN: the target never had this row → INSERT the after-image.
			return []ir.Change{ir.Insert{
				Position:   e.Position,
				Schema:     e.Schema,
				Table:      e.Table,
				Row:        e.After,
				CommitTime: e.CommitTime,
			}}, nil
		default:
			// move-OUT (before && !after): DELETE by key so the now-out-of-
			// scope row doesn't leak on the target. Narrow to the key columns.
			//
			// audit 2026-07-18 F0-8 (inherent subset-CDC hazard, NOT a bug):
			// this DELETE has no source analog — the source still holds the row;
			// only its filter scope changed. With ENFORCED target FKs a child of
			// a moved-out parent surfaces loudly (23503 → SLUICE-E-WHERE-FK-
			// ORPHAN). But under `--allow-degraded-fks` (FK left NOT VALID) the
			// parent DELETE succeeds and the target diverges from the source: the
			// source retains parent+child, the target now has neither. This is
			// intrinsic to filtering a parent table in continuous sync; sluice
			// documents it (docs/operator/filtered-subset-migration.md) rather
			// than silently reshaping the move-OUT — the operator who opts into
			// degraded FKs owns the referential-integrity reconciliation.
			return []ir.Change{ir.Delete{
				Position:   e.Position,
				Schema:     e.Schema,
				Table:      e.Table,
				Before:     f.narrowBefore(e.Table, e.Before),
				CommitTime: e.CommitTime,
			}}, nil
		}
	default:
		// Truncate, SchemaSnapshot, TxBegin, TxCommit: not row-scoped filter
		// targets — forward verbatim.
		return []ir.Change{c}, nil
	}
}

// missingBeforeImage builds the coded refusal for a filtered UPDATE/DELETE
// that arrived without a before-image — the row-move evaluation cannot run.
func missingBeforeImage(op, schema, table string) error {
	return sluicecode.Wrap(
		sluicecode.CodeWhereCDCBeforeImage,
		"ensure MySQL binlog_row_image=FULL / PG REPLICA IDENTITY FULL on the filtered table, then restart the sync",
		fmt.Errorf(
			"continuous filtered sync: a %s on filtered table %s arrived without a before-image, so --where cannot "+
				"decide whether the row moved into or out of the filter's scope. The source must deliver full row "+
				"before-images for a filtered table (MySQL binlog_row_image=FULL, PG REPLICA IDENTITY FULL); a "+
				"partial image reached the reader anyway (a session-level override, or a resume replaying a segment "+
				"written before the setting was corrected). The stream stops here rather than mis-classify the row",
			op, qualifiedTableName(schema, table),
		),
	)
}

func qualifiedTableName(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// preflightRowFilters runs the ADR-0173 Phase 2 sync-start checks when a
// `--where` filter is set: it reads the source schema, compiles each
// predicate against it (the unsupported-predicate refusal), verifies the
// source delivers full before-images for the filtered tables (the
// before-image refusal), and stores the compiled filter on the Streamer for
// the CDC-leg intercept. A no-op when RowFilters is empty.
//
// It runs BEFORE any streaming (cold-start snapshot or warm resume), so an
// unfixable predicate / mis-configured source is refused up front — never
// after data has moved. Both refusals are coded and name the table +
// remedy.
func (s *Streamer) preflightRowFilters(ctx context.Context) error {
	if len(s.RowFilters) == 0 {
		return nil
	}
	// The client-side row-move eval requires full before-images. Refuse
	// loudly on a source engine that can't guarantee them (v1 scope:
	// mysql + postgres) or whose tables aren't configured for them.
	pf, ok := s.Source.(ir.FilteredCDCPreflighter)
	if !ok {
		return sluicecode.Wrap(
			sluicecode.CodeWhereCDCBeforeImage,
			"use `sluice migrate --where` for a one-shot filtered copy instead",
			fmt.Errorf(
				"continuous filtered sync (--where on `sync`) is not supported for source engine %q: it cannot "+
					"guarantee the full row before-images the client-side row-move evaluation requires. "+
					"MySQL and Postgres sources are supported",
				s.Source.Name(),
			),
		)
	}
	// The before-image probe itself runs AFTER ValidateRowFilterKeys below
	// (audit 2026-07-23 D0-10): the engines look tables up by EXACT name
	// (PG: pg_class.relname; unfound == not-full), so probing the raw
	// `--where` keys turned a mere case-mismatch into a spurious
	// SLUICE-E-WHERE-CDC-BEFORE-IMAGE naming a REPLICA IDENTITY remedy that
	// cannot fix it. Canonicalizing first routes a bad key to the correct
	// unknown-table refusal and hands the probe schema-cased names.

	// Read the source schema so each predicate can be compiled against its
	// table's real column types + collations.
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: open source schema reader for --where preflight: %w", err))
	}
	defer migcore.CloseIf(sr)
	migcore.ApplyTableScope(sr, s.Filter)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: read source schema for --where preflight: %w", err))
	}
	// audit F0-4: canonicalize the --where keys to the schema's table casing
	// (and refuse an unmatched key) BEFORE the cold-start snapshot's
	// exact-case ApplyRowFilters lookup runs — the CDC leg keys by lower-case
	// so it was already case-insensitive, but the snapshot reader looks up by
	// exact table name and a mis-cased key would leave the initial snapshot
	// UNFILTERED (a whole-table over-copy) while the CDC leg filtered. Pure
	// pre-stream map reassignment; no concurrency. buildWhereCDCFilter's own
	// unknown-table refusal stays as defense in depth.
	canonFilters, err := migcore.ValidateRowFilterKeys(schema, s.RowFilters)
	if err != nil {
		return err
	}
	s.RowFilters = canonFilters
	// The client-side row-move eval requires full before-images on every
	// filtered table — probed on the CANONICAL (schema-cased) names, post-
	// ValidateRowFilterKeys (audit 2026-07-23 D0-10; see the ordering note
	// above the schema read).
	if err := pf.PreflightFilteredCDCBeforeImage(ctx, s.SourceDSN, s.filteredTableNames()); err != nil {
		return err
	}
	// The client-side string comparator is resolved through the SOURCE engine's
	// collation lens (audit 2026-07-18 M2.1): MySQL supplies a Vitess-backed
	// resolver, Postgres a byte-exact-or-refuse one — so the engine-neutral
	// evaluator carries no collation-library dependency. Filtered CDC sync is
	// v1-scoped to engines that provide one (the FilteredCDCPreflighter gate
	// above already restricts to MySQL + Postgres); refuse loudly rather than
	// compare under a guessed collation if a source somehow reaches here without.
	rp, ok := s.Source.(ir.CollationResolverProvider)
	if !ok {
		return sluicecode.Wrap(
			sluicecode.CodeWhereCDCUnsupportedPredicate,
			"use `sluice migrate --where` for a one-shot source-evaluated filter instead",
			fmt.Errorf(
				"continuous filtered sync (--where on `sync`): source engine %q does not supply a collation resolver, "+
					"so a string predicate's `=` cannot be reproduced client-side faithfully; MySQL and Postgres are supported",
				s.Source.Name(),
			),
		)
	}
	filter, err := buildWhereCDCFilter(rp.CollationResolver(), s.RowFilters, schema, s.WhereStrictCollation)
	if err != nil {
		return err
	}

	s.whereFilter = filter

	// A0 fallback (audit 2026-07-19): a VStream (PlanetScale/Vitess) source
	// evaluates the pushed `--where` NO-PAD regardless of the column's real
	// PAD_ATTRIBUTE, so a PAD-SPACE-collation string column (e.g. the legacy
	// utf8mb4_general_ci) can't be filtered faithfully SERVER-side. Rather than
	// refuse it (the pre-fallback interim), stream those tables UNFILTERED
	// server-side (reduce the pushed map) and filter them CLIENT-side with the
	// PAD-faithful predicate: the cold-start COPY via [ir.ClientCopyFilterSetter]
	// (streamer_coldstart), the CDC tail via the route() the reduced stream still
	// flows through. Other flavors (vanilla MySQL binlog, Postgres) push through
	// the source's OWN PAD-faithful `=` and never diverge — no reduction.
	s.serverSideRowFilters = s.RowFilters
	if s.Source.Capabilities().CDC == ir.CDCVStream {
		// SL1 (audit 2026-07-19): a pad-forced table takes the client-side COPY
		// fallback below, whose keep predicate runs on the cold-start COPY row —
		// where a single-precision FLOAT column's value is DISPLAY-ROUNDED by
		// mysqld's float->text carrier (the exact re-read repair runs AFTER copy,
		// so keep sees the lossy value). An ordering term on such a column would
		// then compare on the rounded value and could silently drop a
		// source-in-scope boundary row (never written, so never repaired). Refuse
		// loudly rather than mis-filter. DOUBLE is full-precision in the carrier
		// and unaffected; a non-pad-forced table is server-filtered on the EXACT
		// value and is not recorded here.
		if fs := filter.clientCopyFloatSingleColumns(); len(fs) > 0 {
			tables := make([]string, 0, len(fs))
			for t := range fs {
				tables = append(tables, t)
			}
			sort.Strings(tables)
			t0 := tables[0]
			return sluicecode.Wrap(
				sluicecode.CodeWhereCDCUnsupportedPredicate,
				"use a DOUBLE column, filter on a non-FLOAT column, or run `sluice migrate --where` for a one-shot source-evaluated copy",
				fmt.Errorf(
					"continuous filtered sync (--where on `sync`): table %q takes the client-side COPY fallback (it filters a "+
						"PAD-SPACE-collation or engine-coerced temporal-literal column on a PlanetScale/Vitess source), and its "+
						"predicate also compares the "+
						"single-precision FLOAT column %q — but the cold-start COPY carries FLOAT values display-rounded by "+
						"mysqld's float->text formatter, so a boundary comparison (e.g. `> 0.1`) could silently drop a "+
						"source-in-scope row. The stream refuses rather than mis-filter (DOUBLE is unaffected)",
					t0, fs[t0],
				),
			)
		}
		// The forced set unions the A0 PAD-SPACE tables (the NO-PAD server
		// filter can't reproduce their `=`) with the F3 temporal-coercion
		// tables (vtgate evalengine's coercion of a finer-than-column
		// temporal literal is unverified — ADR-0174 residuals): both stream
		// unfiltered server-side and filter client-side with the
		// engine-faithful compiled predicate.
		if forced := filter.clientForcedTables(); len(forced) > 0 {
			serverSide := make(map[string]string, len(s.RowFilters))
			for t, p := range s.RowFilters {
				if !forced[strings.ToLower(t)] {
					serverSide[t] = p
				}
			}
			s.serverSideRowFilters = serverSide
			s.clientCopyFilter = filter.clientCopyFilter()
			slog.InfoContext(
				ctx,
				"continuous filtered sync: --where column(s) the VStream server-side filter cannot evaluate faithfully stream unfiltered server-side and are filtered client-side (audit 2026-07-19 A0 fallback + review F3 temporal coercion)",
				slog.Int("pad_space_tables", len(filter.serverSidePadSpaceColumns())),
				slog.Int("temporal_coercion_tables", len(filter.serverSideTemporalCoercionColumns())),
			)
		}
	}

	// ADR-0176: Postgres publication row-filter push-down. On a source whose
	// engine can carry per-table row filters on its publication (only the PG
	// logical-replication engine implements [ir.PublicationRowFilterer]),
	// classify each compiled predicate against the PROVEN equivalence
	// envelope ([pgPushdownEligible]) and stage the eligible subset for the
	// engine; [phaseResolvePublicationScope] threads it in alongside the
	// publication scope, and cold start's EnsurePublication emits it (PG 15+
	// only — the engine gates the version). An ineligible predicate takes
	// the A0-style fallback: its table streams UNFILTERED server-side and the
	// client-side evaluator — which stays ON for every table as the ADR-0176
	// §2 belt — remains its filter, logged loudly at INFO like A0's
	// client-copy fallback. Keys stay canonical (s.RowFilters was re-keyed to
	// the schema's casing above), matching the EnsurePublication table list.
	if _, ok := s.Source.(ir.PublicationRowFilterer); ok {
		tableByName := make(map[string]*ir.Table, len(schema.Tables))
		for _, t := range schema.Tables {
			if t != nil {
				tableByName[strings.ToLower(t.Name)] = t
			}
		}
		eligible := make(map[string]string, len(s.RowFilters))
		pushed := make([]string, 0, len(s.RowFilters))
		for table, predicate := range s.RowFilters {
			tbl := tableByName[strings.ToLower(table)]
			ok, reason := pgPushdownEligible(tbl, filter.predicateFor(table))
			if !ok {
				slog.InfoContext(
					ctx, "continuous filtered sync: publication row-filter push-down fallback engaged — the table streams unfiltered server-side and is filtered client-side (ADR-0176 A0-style fallback)",
					slog.String("table", table),
					slog.String("reason", reason),
				)
				continue
			}
			eligible[table] = predicate
			pushed = append(pushed, table)
		}
		if len(eligible) > 0 {
			s.publicationRowFilters = eligible
			filter.markServerFiltered(pushed)
			slog.InfoContext(
				ctx, "continuous filtered sync: --where predicates will be pushed into the publication row filter on a PG 15+ source (ADR-0176); client-side evaluation stays on as the equivalence belt",
				slog.Int("pushed_tables", len(eligible)),
			)
		}
	}

	slog.InfoContext(
		ctx, "continuous filtered sync: --where predicates compiled for the CDC leg (ADR-0173 Phase 2)",
		slog.Int("filtered_tables", len(s.RowFilters)),
	)
	return nil
}
