// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// The applier backstop for KEY-COLLATION-FIRST-BOUNDARY (option 3).
//
// # The harm
//
// An `INSERT … ON DUPLICATE KEY UPDATE` matches an existing row through any
// unique key, under the TARGET column's collation. Where the target key
// equates two values the source keeps apart (`'A'` and `'a'` under `_ci`
// when the source says `_bin`; `'a '` and `'a'` under a PAD SPACE collation
// when the source says NO PAD), the upsert silently replaces or overwrites
// the existing row. The schema-boundary doors (pipeline
// key_collation_boundary.go) stop that when they can see the change; this
// guard is the door for what they cannot see: a source whose change stream
// carries no collation at a boundary, a target collation set by hand, a
// boundary the stream never compared.
//
// # The rule (operator's choice (b): armed by the SOURCE's collation)
//
// A key column is ARMED when the source's current declaration of it — the
// applier's active schema version, the latest boundary it committed (or
// the retained history it primed from) — names a collation that is not the
// target column's. The upsert then refuses when the row it matched holds a
// key that is equal under the target's collation but DIFFERENT under the
// source's: the source has two rows there, and the target is about to
// make them one. It passes when:
//
//   - the matched row's primary key is equal under the source's collation —
//     the same source row (a re-applied INSERT, an UPDATE coalesced into an
//     upsert, a case-only change of a non-PK unique column);
//   - the key values are equal under the source's collation too — a
//     re-delivery the idempotent apply exists to absorb, on a source whose
//     own collation equates them (a `_ci` source changing a key's case
//     inside a re-delivery window converges, it does not wedge);
//   - no key column is armed: the source and target collations agree, and
//     the statement carries no guard at all (zero cost on the common case).
//
// # Mechanism, and its cost
//
// No round trip is added: MySQL has no RETURNING with ON DUPLICATE KEY
// UPDATE and the affected-rows count cannot say which key matched, so the
// guard is evaluated by the server inside the statement. The FIRST
// assignment of the update list (it must come before any assignment that
// changes a key column — MySQL evaluates them left to right on the updated
// row; measured) becomes `col = IF(<ok>, <new>, <raise>)`, where <raise> is
// an unsigned overflow carrying the marker:
// `18446744073709551615 + OCTET_LENGTH(CONCAT('KEY-COLLATION-MERGE', …))`.
// Measured on MySQL 8.0.46, 8.4.11, MariaDB 11.4.13 and vttestserver (a
// sharded keyspace through vtgate): error 1690 in strict and non-strict
// sql_mode alike, the whole multi-row statement rolled back, an
// identical-bytes re-apply untouched, and the server's text keeps the marker.
// The other spellings measured do NOT work everywhere: `CAST(… AS JSON)`
// does not exist on MariaDB, and an invalid REGEXP returns NULL with a
// warning there — the row merged with its column set to NULL. Whatever the
// server's text keeps, the exec sites attribute a 1690 from a guarded
// statement to [ir.KeyCollationMergeError] by provenance.
//
// One metadata read per armed table (its unique keys) and one per new
// collation (whether this server knows it), both cached; a collation the
// target does not know refuses loudly before any row of the table is
// applied (KEY-COLLATION-GUARD-UNSUPPORTED) rather than run unguarded.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// keyCollationMergeMarker is the literal the guard's raise carries.
const keyCollationMergeMarker = "KEY-COLLATION-MERGE"

// keyCollationGuardUnsupportedMarker names the loud refusal when the guard
// cannot be expressed on this target.
const keyCollationGuardUnsupportedMarker = "KEY-COLLATION-GUARD-UNSUPPORTED"

// armedCollation is how one armed key column is compared under the
// source's collation.
type armedCollation struct {
	// bytes: the source compares byte for byte (a deterministic Postgres
	// collation), so the comparison is on the binary strings.
	bytes bool
	// charset / collation: a MySQL-family source collation, applied to both
	// values converted to its character set.
	charset, collation string
}

// keyCollationPlan is the guard for one table, rendered per statement.
type keyCollationPlan struct {
	pk    []string
	keys  [][]string // every unique key (the primary key included) with an armed column
	armed map[string]armedCollation
	// marker is the armed column whose incoming value the raise names.
	marker string
}

// planKeyCollationGuard decides which key columns are armed. source is the
// source's current shape of the table (nil: unknown, nothing armed);
// colTypes the target's columns; pk and uniques the target's keys. nil when
// nothing is armed.
func planKeyCollationGuard(source *ir.Table, colTypes map[string]*ir.Column, pk []string, uniques [][]string, remap map[string]string) *keyCollationPlan {
	armed := armedColumns(source, colTypes, remap)
	if len(armed) == 0 {
		return nil
	}
	p := &keyCollationPlan{pk: pk, armed: armed}
	for _, key := range append([][]string{pk}, uniques...) {
		for _, col := range key {
			if _, ok := armed[col]; ok {
				p.keys = append(p.keys, key)
				if p.marker == "" {
					p.marker = col
				}
				break
			}
		}
	}
	if len(p.keys) == 0 {
		return nil
	}
	return p
}

// armedColumns is every column (key or not) whose source collation the
// target does not share, keyed by name; the caller narrows to keys.
func armedColumns(source *ir.Table, colTypes map[string]*ir.Column, remap map[string]string) map[string]armedCollation {
	if source == nil {
		return nil
	}
	armed := map[string]armedCollation{}
	for _, c := range source.Columns {
		if c == nil {
			continue
		}
		tc := colTypes[c.Name]
		if tc == nil {
			continue
		}
		if ac, ok := armedFor(c.Type, tc.Type, remap); ok {
			armed[c.Name] = ac
		}
	}
	return armed
}

// armedFor reports how a key column whose source type is src and target type
// is tgt must be compared under the source's collation, and whether it must
// be at all. remap is the target flavor's cross-flavor collation remap
// ([Flavor.crossFlavorCollationRemap]): a MySQL source's `utf8mb4_0900_*`
// collation lands on a MariaDB target as its `uca1400` equivalent (and the
// reverse), so the source collation is judged by its remapped name — equal
// to the target's is not a change, and a different one is compared under
// the name the target server has.
func armedFor(src, tgt ir.Type, remap map[string]string) (armedCollation, bool) {
	scs, srcColl := translate.ColumnCollation(src)
	if srcColl == "" {
		return armedCollation{}, false // the source's collation is not known
	}
	tcs, tcoll := translate.ColumnCollation(tgt)
	if tcoll == "" {
		return armedCollation{}, false // not a character column on the target
	}
	switch translate.CollationDialect(scs, srcColl) {
	case "mysql":
		srcColl = strings.ToLower(srcColl)
		if mapped, ok := remap[srcColl]; ok {
			srcColl = mapped
		}
		if strings.EqualFold(scs, tcs) && strings.EqualFold(srcColl, tcoll) {
			return armedCollation{}, false
		}
		return armedCollation{charset: strings.ToLower(scs), collation: srcColl}, true
	case "postgres":
		if collationDeterminismOf(src) == ir.CollationDeterministic {
			return armedCollation{bytes: true}, true
		}
	}
	return armedCollation{}, false
}

// collationDeterminismOf is a character type's declared determinism.
func collationDeterminismOf(t ir.Type) ir.CollationDeterminism {
	switch v := t.(type) {
	case ir.Varchar:
		return v.Determinism
	case ir.Char:
		return v.Determinism
	case ir.Text:
		return v.Determinism
	}
	return ir.CollationDeterminismUnknown
}

// render returns the guarded first assignment of the update list and the
// column it assigns, for a statement inserting cols into schema.table. ok
// is false when the guard cannot reference a key column's incoming value
// (it is not in the insert's column list — a generated column).
func (p *keyCollationPlan) render(schema, table string, cols []string, upsert upsertSpelling) (assign, hang string, ok bool) {
	inCols := make(map[string]bool, len(cols))
	for _, c := range cols {
		inCols[c] = true
	}
	for _, key := range p.keys {
		for _, c := range key {
			if !inCols[c] {
				return "", "", false
			}
		}
	}
	tbl := quoteIdent(schema) + "." + quoteIdent(table) + "."
	existing := func(c string) string { return tbl + quoteIdent(c) }
	incoming := func(c string) string { return upsert.newRowRef(quoteIdent(c)) }
	tgtEq := func(c string) string { return existing(c) + " <=> " + incoming(c) }
	srcEq := func(c string) string {
		ac, armed := p.armed[c]
		switch {
		case !armed:
			return tgtEq(c)
		case ac.bytes:
			return "CAST(" + existing(c) + " AS BINARY) <=> CAST(" + incoming(c) + " AS BINARY)"
		}
		conv := func(x string) string {
			return "CONVERT(" + x + " USING " + ac.charset + ") COLLATE " + ac.collation
		}
		return conv(existing(c)) + " <=> " + conv(incoming(c))
	}
	all := func(cols []string, eq func(string) string) string {
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = eq(c)
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	}
	merges := make([]string, len(p.keys))
	for i, key := range p.keys {
		merges[i] = "(" + all(key, tgtEq) + " AND NOT " + all(key, srcEq) + ")"
	}
	okExpr := "NOT (" + strings.Join(merges, " OR ") + ")"
	if len(p.pk) > 0 {
		okExpr = "(" + all(p.pk, srcEq) + " OR " + okExpr + ")"
	}
	hang = cols[0]
	pkSet := make(map[string]bool, len(p.pk))
	for _, c := range p.pk {
		pkSet[c] = true
	}
	if len(p.pk) > 0 {
		hang = p.pk[0]
		for _, c := range cols {
			if !pkSet[c] {
				hang = c
				break
			}
		}
	}
	raise := "18446744073709551615 + OCTET_LENGTH(CONCAT('" + keyCollationMergeMarker + "', " + incoming(p.marker) + "))"
	return quoteIdent(hang) + " = IF(" + okExpr + ", " + incoming(hang) + ", " + raise + ")", hang, true
}

// keyCollationGuardEntry caches one table's plan against the source shape it
// was computed from.
type keyCollationGuardEntry struct {
	source *ir.Table
	plan   *keyCollationPlan
}

// keyCollationPlanFor returns the guard plan for a write into schema.table
// (routed) of a change from srcSchema.table, or nil when nothing is armed.
func (a *ChangeApplier) keyCollationPlanFor(ctx context.Context, q queryRower, srcSchema, schema, table string, pk []string, colTypes map[string]*ir.Column) (*keyCollationPlan, error) {
	source, _ := a.ActiveSchema(srcSchema, table)
	qn := qualifiedName(schema, table)
	a.cacheMu.RLock()
	e, hit := a.keyCollationGuardCache[qn]
	a.cacheMu.RUnlock()
	if hit && e.source == source {
		return e.plan, nil
	}
	var plan *keyCollationPlan
	remap := a.flavor.crossFlavorCollationRemap()
	if len(armedColumns(source, colTypes, remap)) > 0 {
		// A column differs: read the unique keys beyond the primary key (the
		// only extra read, once per table and source shape), and the target
		// must know every collation the guard names.
		uniques, err := nonPKUniqueKeyColumns(ctx, q, schema, table)
		if err != nil {
			return nil, fmt.Errorf("mysql: applier: read the unique keys of %s for the key-collation guard: %w", qn, err)
		}
		plan = planKeyCollationGuard(source, colTypes, pk, uniques, remap)
		if plan != nil {
			if err := a.checkKeyCollationsExpressible(ctx, q, qn, plan); err != nil {
				return nil, err
			}
		}
	}
	a.cacheMu.Lock()
	if a.keyCollationGuardCache == nil {
		a.keyCollationGuardCache = map[string]keyCollationGuardEntry{}
	}
	a.keyCollationGuardCache[qn] = keyCollationGuardEntry{source: source, plan: plan}
	a.cacheMu.Unlock()
	return plan, nil
}

// queryRower is the read surface the guard's metadata lookups need (a
// transaction or the pool).
type queryRower interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// nonPKUniqueKeyColumns reads the target's unique keys other than the
// primary key, each as its columns in index order.
func nonPKUniqueKeyColumns(ctx context.Context, q queryRower, schema, table string) ([][]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT index_name, column_name FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND non_unique = 0 AND index_name <> 'PRIMARY'
		ORDER BY index_name, seq_in_index`, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out [][]string
	last := ""
	for rows.Next() {
		var idx string
		var col sql.NullString
		if err := rows.Scan(&idx, &col); err != nil {
			return nil, err
		}
		if !col.Valid {
			continue // a functional key part: not a column the guard can compare
		}
		if idx != last || len(out) == 0 {
			out = append(out, nil)
			last = idx
		}
		out[len(out)-1] = append(out[len(out)-1], col.String)
	}
	return out, rows.Err()
}

// checkKeyCollationsExpressible refuses loudly when the target server does
// not know a collation the guard must compare under (a MySQL 8.0
// `utf8mb4_0900_*` collation on a MariaDB target, say): running unguarded
// would be the merge this guard exists for.
func (a *ChangeApplier) checkKeyCollationsExpressible(ctx context.Context, q queryRower, qn string, plan *keyCollationPlan) error {
	for col, ac := range plan.armed {
		if ac.bytes {
			continue
		}
		var probe sql.NullString
		err := q.QueryRowContext(ctx, "SELECT CONVERT('' USING "+ac.charset+") COLLATE "+ac.collation).Scan(&probe)
		if err != nil {
			return &terminalMySQLError{err: fmt.Errorf("%w: %s: mysql: applier: %s: the source declares key column %q "+
				"with CHARACTER SET %s COLLATE %s, which this target server does not support (%v), so sluice cannot "+
				"check that an upsert does not merge two source rows the target's collation equates. Refusing before "+
				"any row of the table is applied. Give the target column a collation that compares as the source's "+
				"does, or re-copy the table",
				ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, col, ac.charset, ac.collation, err)}
		}
	}
	return nil
}

// keyCollationMergeRefusal attributes a guarded upsert's failure: a 1690
// from a statement carrying the guard is the guard's raise, whatever text
// the server kept. Anything else is returned unchanged.
func keyCollationMergeRefusal(err error, plan *keyCollationPlan, schema, table string) error {
	if err == nil || plan == nil {
		return err
	}
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1690 {
		if !strings.Contains(err.Error(), keyCollationMergeMarker) {
			return err
		}
	}
	return &ir.KeyCollationMergeError{
		Schema: schema, Table: table, Cause: err,
		Remedy: fmt.Sprintf("Give the target's key column(s) the source's collation (for example `ALTER TABLE %s.%s "+
			"MODIFY <column> … CHARACTER SET … COLLATE …` as the source declares it) and restart, or re-copy the table",
			quoteIdent(schema), quoteIdent(table)),
	}
}
