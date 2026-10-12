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
// row; measured) becomes
// `col = IF(<ok>, <new>, IF((<raise>) = 0, <new>, <new>))`, where <raise> is
// an unsigned overflow carrying the marker:
// `18446744073709551615 + OCTET_LENGTH(CONCAT('KEY-COLLATION-MERGE', …))`.
// Measured on MySQL 8.0.46, 8.4.11, MariaDB 11.4.13 and vttestserver (a
// sharded keyspace through vtgate): error 1690 in strict and non-strict
// sql_mode alike, the whole multi-row statement rolled back, an
// identical-bytes re-apply untouched, and the server's text keeps the marker.
//
// The raise is in a CONDITION, never a value arm, because a value arm's type
// is aggregated with the column's. Measured 2026-10-12 on MySQL 8.0, 8.4 and
// MariaDB 10.11 / 11.4 over UUID, INET6, GEOMETRY, JSON, TIMESTAMP(6),
// DATETIME(6), DECIMAL(65,30), BIT(64), FLOAT, DOUBLE, VARBINARY, BLOB, SET,
// ENUM, TIME(6), YEAR, BIGINT UNSIGNED and a latin1 VARCHAR (a non-conflicting
// insert, a same-row conflict, a merge): `IF(ok, new, <raise>)` fails EVERY
// statement on MariaDB at its type check when the column is UUID, INET6 or
// GEOMETRY (error 4078, a wedge on the first insert); `IF(ok, new,
// CAST(<raise> AS CHAR))` passes the type check but routes the value through
// a string and SILENTLY changed FLOAT 1.17549435e-38 and 16777217 on all four
// servers; the condition form is byte-exact for every cell and raises 1690
// with the marker. Pinned by TestKeyCollationGuard_HangTypeFamilies. The
// other spellings measured earlier do NOT work everywhere either: `CAST(… AS
// JSON)` does not exist on MariaDB, and an invalid REGEXP returns NULL with a
// warning there — the row merged with its column set to NULL. The exec
// sites attribute a 1690 to [ir.KeyCollationMergeError] only when the
// statement carried the guard AND the server's text keeps the marker.
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
	// values converted to its character set. collation is the name the
	// comparison runs under on THIS server (the key-guard map's, for a
	// cross-flavor source); declared is the source's own name for it, which
	// the GUARD-UNSUPPORTED refusal reports so it never attributes the
	// comparison name to the source.
	charset, collation, declared string
	// uniqueOnly: the source column is on a Postgres database's DEFAULT
	// collation (no name, stamped deterministic) or on a NONDETERMINISTIC
	// Postgres collation (see [armedFor]), which the MySQL target created
	// under its own default — usually case-insensitive. Armed only
	// in a UNIQUE key beside a primary key the column is not part of
	// (operator's choice (b), KEY-COLLATION-PG-DEFAULT-TO-MYSQL): there the
	// primary key recognises a re-delivered change of the same row, so the
	// guard refuses a second source row without wedging a re-delivery. In
	// the primary key itself (or with no primary key) a re-delivered
	// case-only change converges today and the guard would wedge it, so
	// that case stays unarmed — documented as not covered.
	uniqueOnly bool
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
func planKeyCollationGuard(source *ir.Table, colTypes map[string]*ir.Column, pk []string, uniques [][]string, compareAs map[string]string) *keyCollationPlan {
	armed := armedColumns(source, colTypes, compareAs)
	inPK := make(map[string]bool, len(pk))
	for _, c := range pk {
		inPK[c] = true
	}
	for col, ac := range armed {
		if ac.uniqueOnly && (len(pk) == 0 || inPK[col]) {
			delete(armed, col) // the identity key itself: not covered, see uniqueOnly
		}
	}
	return planFromArmed(armed, pk, uniques)
}

// planCopyKeyCollationGuard is [planKeyCollationGuard] for the idempotent
// copy writer (KEY-COLLATION-ROWWRITER). It differs in one rule: a Postgres
// default-collation column is armed in the primary key too. The applier
// leaves that one unarmed because a re-delivered case-only change of the
// identity key would wedge it; a copy re-sends only rows of a snapshot,
// never a change of a key, so the case cannot arise, and without the guard
// two source keys 'A' and 'a' on a `_ci` target key are one row at exit 0.
func planCopyKeyCollationGuard(source *ir.Table, colTypes map[string]*ir.Column, pk []string, uniques [][]string, compareAs map[string]string) *keyCollationPlan {
	return planFromArmed(armedColumns(source, colTypes, compareAs), pk, uniques)
}

// planFromArmed is the guard over the armed columns: every unique key (the
// primary key first) holding one, and the marker. nil when no key does.
func planFromArmed(armed map[string]armedCollation, pk []string, uniques [][]string) *keyCollationPlan {
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
func armedColumns(source *ir.Table, colTypes map[string]*ir.Column, compareAs map[string]string) map[string]armedCollation {
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
		if ac, ok := armedFor(c.Type, tc.Type, compareAs); ok {
			armed[c.Name] = ac
		}
	}
	return armed
}

// armedFor reports how a key column whose source type is src and target type
// is tgt must be compared under the source's collation, and whether it must
// be at all. compareAs is the target flavor's key-guard comparison map
// ([Flavor.keyGuardCollations]): the collation on this target server that
// compares as a cross-flavor source collation does. It is NOT the emitter's
// remap ([Flavor.crossFlavorCollationRemap]), which moves a MySQL 8
// `utf8mb4_0900_ai_ci` (NO PAD) to MariaDB's PAD SPACE `utf8mb4_uca1400_ai_ci`;
// judged by that map the column looked unchanged and an `'a '` row silently
// overwrote `'a'` (review F-A). A source collation equal to the target's
// under compareAs arms nothing; a different one is compared under the
// compareAs name.
func armedFor(src, tgt ir.Type, compareAs map[string]string) (armedCollation, bool) {
	scs, srcColl := translate.ColumnCollation(src)
	tcs, tcoll := translate.ColumnCollation(tgt)
	if tcoll == "" {
		return armedCollation{}, false // not a character column on the target
	}
	if srcColl == "" {
		if collationDeterminismOf(src) == ir.CollationDeterministic {
			// A Postgres database-default collation: byte equality, armed
			// only beside a primary key (planKeyCollationGuard).
			return armedCollation{bytes: true, uniqueOnly: true}, true
		}
		return armedCollation{}, false // the source's collation is not known
	}
	switch translate.CollationDialect(scs, srcColl) {
	case "mysql":
		declared := strings.ToLower(srcColl)
		srcColl = declared
		if mapped, ok := compareAs[srcColl]; ok {
			srcColl = mapped
		}
		if strings.EqualFold(scs, tcs) && strings.EqualFold(srcColl, tcoll) {
			return armedCollation{}, false
		}
		return armedCollation{charset: strings.ToLower(scs), collation: srcColl, declared: declared}, true
	case "postgres":
		switch collationDeterminismOf(src) {
		case ir.CollationDeterministic:
			return armedCollation{bytes: true}, true
		case ir.CollationNonDeterministic:
			// A Postgres NONDETERMINISTIC collation (an ICU `ks-level2`
			// case-insensitive key, a level3 one): the MySQL emitter cannot
			// declare it and creates the key under the target's default, which
			// equates values the source keeps apart (`'e'`/`'é'` at level 2,
			// `'a'`/`'A'` at level 3, both one key under `utf8mb4_0900_ai_ci`).
			// Left unarmed, those merged through every guarded upsert at exit 0
			// (review 2026-10-12, finding 1). Armed as the database default is
			// (bytes, uniqueOnly) — the operator's KEY-COLLATION-PG-DEFAULT-TO-
			// MYSQL policy, mirrored exactly.
			//
			// Why a BYTE comparison is right for a collation that is not byte
			// equality. A nondeterministic collation equates MORE byte strings
			// than byte equality does, never fewer: two values it keeps apart
			// are byte-different, and two byte-different values it equates
			// cannot both be in a source unique key at once. So on a SNAPSHOT
			// (the copy path, where uniqueOnly is ignored and the primary key
			// is armed too) every pair of rows is byte-distinct and
			// source-distinct, a re-sent row is byte-identical to itself, and a
			// target conflict between byte-different keys is exactly a merge.
			// On the APPLY path the byte comparison is STRICTER than the
			// source's: a re-delivered change of a key the source equates
			// (`'A'` → `'a'` on a case-insensitive key) is byte-different, so in
			// the primary key — the identity — it would refuse what converges
			// today; uniqueOnly leaves the identity unarmed for that reason,
			// and the primary-key-equal arm of the guard recognises the
			// re-delivered change of a UNIQUE key beside it. What remains
			// stricter is loud, never silent: a conflict between two DIFFERENT
			// source rows whose keys the source equates, which a source unique
			// key admits only transiently (a DEFERRABLE constraint mid-
			// transaction), refuses rather than passing.
			return armedCollation{bytes: true, uniqueOnly: true}, true
		}
	}
	return armedCollation{}, false
}

// collationDeterminismOf is a character type's declared determinism; a
// domain over a character type has its base type's (as
// [translate.ColumnCollation] reads its collation through the domain).
func collationDeterminismOf(t ir.Type) ir.CollationDeterminism {
	switch v := t.(type) {
	case ir.Varchar:
		return v.Determinism
	case ir.Char:
		return v.Determinism
	case ir.Text:
		return v.Determinism
	case ir.Domain:
		return collationDeterminismOf(v.BaseType)
	}
	return ir.CollationDeterminismUnknown
}

// render returns the guarded first assignment of the update list and the
// column it assigns, for a statement inserting cols into schema.table. ok
// is false when the guard cannot reference a key column's incoming value
// (it is not in the insert's column list — a generated column).
func (p *keyCollationPlan) render(schema, table string, cols []string, upsert upsertSpelling) (assign, hang string, ok bool) {
	return p.renderFor(quoteIdent(schema)+"."+quoteIdent(table), cols, upsert)
}

// renderFor is [keyCollationPlan.render] for a statement whose INSERT names
// the table as tableRef (already quoted): the existing row's columns are
// referenced through it. The copy writer inserts into an unqualified table
// name on its own database, so it passes just the table.
func (p *keyCollationPlan) renderFor(tableRef string, cols []string, upsert upsertSpelling) (assign, hang string, ok bool) {
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
	tbl := tableRef + "."
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
	// The raise sits in the CONDITION of an inner IF whose two arms are both
	// the hang column's incoming value, so the assignment's type is the
	// column's own and no value is converted (see the package comment's
	// measured table for the spellings that fail).
	raise := "18446744073709551615 + OCTET_LENGTH(CONCAT('" + keyCollationMergeMarker + "', " + incoming(p.marker) + "))"
	newHang := incoming(hang)
	return quoteIdent(hang) + " = IF(" + okExpr + ", " + newHang + ", IF((" + raise + ") = 0, " + newHang + ", " + newHang + "))", hang, true
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
	compareAs := a.flavor.keyGuardCollations()
	if len(armedColumns(source, colTypes, compareAs)) > 0 {
		// A column differs: read the unique keys beyond the primary key (the
		// only extra read, once per table and source shape), and the target
		// must know every collation the guard names.
		uniques, err := nonPKUniqueKeyColumns(ctx, q, schema, table)
		if err != nil {
			return nil, fmt.Errorf("mysql: applier: read the unique keys of %s for the key-collation guard: %w", qn, err)
		}
		plan = planKeyCollationGuard(source, colTypes, pk, uniques, compareAs)
		if plan != nil {
			if err := checkKeyCollationsExpressible(ctx, q, qn, plan); err != nil {
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
func checkKeyCollationsExpressible(ctx context.Context, q queryRower, qn string, plan *keyCollationPlan) error {
	for col, ac := range plan.armed {
		if ac.bytes {
			continue
		}
		var probe sql.NullString
		err := q.QueryRowContext(ctx, "SELECT CONVERT('' USING "+ac.charset+") COLLATE "+ac.collation).Scan(&probe)
		if err != nil {
			return keyCollationGuardUnsupported(qn, col, ac, err)
		}
	}
	return nil
}

// keyCollationGuardUnsupported is the GUARD-UNSUPPORTED refusal for one armed
// column. It names the source's own collation and, separately, the one the
// comparison needs on this server: for a MySQL 8 source on a MariaDB target
// they differ (`utf8mb4_0900_ai_ci` is compared as
// `utf8mb4_uca1400_nopad_ai_ci`, which MariaDB has only from 10.10 on, so
// below the supported 10.11 floor this is what the first write to an armed
// table meets — measured on 10.5 and 10.6, nothing of the table applied).
func keyCollationGuardUnsupported(qn, col string, ac armedCollation, cause error) error {
	declared, compared := ac.declared, ""
	if declared == "" {
		declared = ac.collation
	}
	if declared != ac.collation {
		compared = " (compared on this target as " + ac.collation + ")"
	}
	return &terminalMySQLError{err: fmt.Errorf("%w: %s: mysql: %s: the source declares key column %q "+
		"with CHARACTER SET %s COLLATE %s%s, which this target server does not support (%v), so sluice cannot "+
		"check that an upsert does not merge two source rows the target's collation equates. Refusing before "+
		"any row of the table is applied. Give the target column a collation that compares as the source's "+
		"does, move the target to a server version that has %s, or re-copy the table",
		ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, col, ac.charset, declared, compared,
		cause, ac.collation)}
}

// keyCollationMergeRefusal attributes a guarded upsert's failure: a 1690
// from a statement carrying the guard, whose server text names the marker,
// is the guard's raise. Both are required: a 1690 from an ordinary value
// overflow in the same statement must surface as itself, not as a merge.
// The marker survives every server measured, vtgate included (the sharded
// pin, TestVStream_KeyCollationGuard_ShardedTarget, asserts the refusal
// through vtgate). Anything else is returned unchanged.
func keyCollationMergeRefusal(err error, plan *keyCollationPlan, schema, table string) error {
	if plan == nil || !isKeyCollationMergeRaise(err) {
		return err
	}
	return &ir.KeyCollationMergeError{
		Schema: schema, Table: table, Cause: err,
		Remedy: keyCollationMergeRemedy(plan, schema, table),
	}
}

// isKeyCollationMergeRaise reports whether err is the guard's raise: a 1690
// whose server text keeps the marker. The caller also requires that its
// statement carried the guard.
func isKeyCollationMergeRaise(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1690 && strings.Contains(me.Message, keyCollationMergeMarker)
}

// keyCollationMergeRemedy is the refusal's remedy. When the key that
// identifies a row (the primary key, or for a table without one the key the
// applier upserts by) has no armed column, the guard tells a second source
// row from the same one, so aligning the target key's collation converges.
// When it does — no primary key at all, or an armed column inside it — a
// re-delivered change of that key on a source whose collation keeps the old
// and new values apart (`_bin`, a case-only UPDATE) looks exactly like a
// second row: the refusal repeats on every start, and after a collation
// change the re-delivered INSERT no longer conflicts and lands a second
// time. A re-copy converges in both cases, so it leads — when sluice
// creates the key under a collation that compares as the source's.
//
// It names that collation per armed column: the guard's comparison
// collation, which for a MySQL 8 source on MariaDB is the NO PAD uca1400
// form, not the source's own name (Bug 305: "the source's collation" sent
// operators to `utf8mb4_0900_ai_ci`, which MariaDB 10.11 lacks and whose
// remapped PAD SPACE form is the one that refused). For a column armed to
// compare bytes (a Postgres source collation, which a MySQL target cannot
// declare), sluice creates the key under the target's default collation —
// the one that equates the values — so a re-copy does not converge there
// and is not offered.
func keyCollationMergeRemedy(plan *keyCollationPlan, schema, table string) string {
	identityArmed := len(plan.pk) == 0
	for _, c := range plan.pk {
		if _, ok := plan.armed[c]; ok {
			identityArmed = true
		}
	}
	// need lists, in key order, what each armed column must be declared with;
	// recopyAligns is false when one of them is a collation sluice cannot
	// create from the source's declaration.
	var need []string
	recopyAligns := true
	named := map[string]bool{}
	for _, key := range append([][]string{plan.pk}, plan.keys...) {
		for _, c := range key {
			ac, ok := plan.armed[c]
			if !ok || named[c] {
				continue
			}
			named[c] = true
			if ac.bytes {
				recopyAligns = false
				need = append(need, quoteIdent(c)+" a binary collation (for example COLLATE utf8mb4_bin)")
				continue
			}
			need = append(need, quoteIdent(c)+" CHARACTER SET "+ac.charset+" COLLATE "+ac.collation)
		}
	}
	collations := strings.Join(need, ", ")
	if collations == "" {
		collations = "the source's collation" // a plan names its armed keys; kept for a hand-built one
	}
	if identityArmed {
		// No ALTER spelled out here: the collation change converges only for a
		// second source row, so the text names the collation without handing
		// over a statement to run unconditionally (pinned by the "no primary
		// key" cell of TestKeyCollationGuard_MySQLFamily).
		lead := fmt.Sprintf("The key that identifies a row of %s.%s is itself compared differently on the two "+
			"sides (or the table has no primary key), so sluice cannot tell a re-delivered change of that key "+
			"from a second source row, and this refusal repeats on every start. ", quoteIdent(schema), quoteIdent(table))
		if recopyAligns {
			return lead + "Re-copy the table, which converges either way: restart the sync with " +
				"--restart-from-scratch, which recreates it with " + collations + ". Giving the target key that " +
				"collation yourself converges only if this is a second source row, not a re-delivered change of one"
		}
		return lead + "A re-copy does not converge here: sluice recreates the key under the target's default " +
			"collation, which equates the values. Giving the target key " + collations + " converges only if " +
			"this is a second source row, not a re-delivered change of one"
	}
	alter := fmt.Sprintf("`ALTER TABLE %s.%s MODIFY <column> …` with %s", quoteIdent(schema), quoteIdent(table), collations)
	if recopyAligns {
		return "Give the target's key column(s) " + collations + " (" + alter + ") and restart, or re-copy the " +
			"table with --restart-from-scratch, which recreates it with that collation"
	}
	return "Give the target's key column(s) " + collations + " (" + alter + ") and restart"
}
