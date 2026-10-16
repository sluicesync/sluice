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
	"strconv"
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
	// nondet: the source collation is (or, for an unstamped named Postgres
	// collation, may be) NONDETERMINISTIC, so the byte comparison is
	// stricter than the source's: a copy-time refusal on such a key can be a
	// stale target row the source equates with the new one, not two source
	// rows. Read by the copy's remedy ([copyKeyCollationMergeRemedy]).
	nondet bool
}

// keyCollationPlan is the guard for one table, rendered per statement.
type keyCollationPlan struct {
	pk   []string
	keys [][]string // every unique key (the primary key included) with an armed column
	// subs parallels keys: each key part's index prefix length (SUB_PART,
	// in characters for a character column), 0 for the whole column. The
	// server matches an upsert through a prefix key on the prefix alone, so
	// the guard compares exactly that (review 2026-10-12 #2, finding 1).
	subs [][]int
	// fns parallels keys: the functional part a key entry stands for (its
	// column is the one the part reads), nil for a plain column
	// (key_collation_functional.go).
	fns   [][]*functionalKeyPart
	armed map[string]armedCollation
	// marker is the armed column whose incoming value the raise names.
	marker string
}

// planKeyCollationGuard decides which key columns are armed. source is the
// source's current shape of the table (nil: unknown, nothing armed);
// colTypes the target's columns; pk and uniques the target's keys. nil when
// nothing is armed.
func planKeyCollationGuard(source *ir.Table, colTypes map[string]*ir.Column, keys targetUniqueKeys, compareAs map[string]string) *keyCollationPlan {
	armed := applierArmedColumns(source, colTypes, keys, compareAs)
	keys.markFunctionalArming(source, colTypes, armed, compareAs)
	return planFromArmed(armed, keys)
}

// applierArmedColumns is the applier's armed set: every armed column but a
// uniqueOnly one in (or with no) primary key.
func applierArmedColumns(source *ir.Table, colTypes map[string]*ir.Column, keys targetUniqueKeys, compareAs map[string]string) map[string]armedCollation {
	armed := armedColumns(source, colTypes, compareAs)
	pk := keys.pk
	inPK := make(map[string]bool, len(pk))
	for _, c := range pk {
		inPK[c] = true
	}
	for col, ac := range armed {
		if ac.uniqueOnly && (len(pk) == 0 || inPK[col]) {
			delete(armed, col) // the identity key itself: not covered, see uniqueOnly
		}
	}
	return armed
}

// planCopyKeyCollationGuard is [planKeyCollationGuard] for the idempotent
// copy writer (KEY-COLLATION-ROWWRITER). It differs in one rule: a Postgres
// default-collation or nondeterministic column is armed in the primary key
// too. The applier leaves those unarmed because a re-delivered case-only
// change of the identity key would wedge it. A copy delivers rows, not
// changes, but it is NOT confined to one snapshot (review 2026-10-12 #2,
// finding 4): a resumed migrate over a live source, add-table or a restore
// over rows already there can re-send a row whose key the source has since
// changed, and on a nondeterministic key that stale pair is byte-different
// though the source equates it. The guard refuses it, loudly — the cost of
// arming here, and the copy's remedy says that a re-copy from scratch
// converges then ([copyKeyCollationMergeRemedy]). Unarmed, two source keys
// 'A' and 'a' on a `_ci` target key are one row at exit 0, silently.
func planCopyKeyCollationGuard(source *ir.Table, colTypes map[string]*ir.Column, keys targetUniqueKeys, compareAs map[string]string) *keyCollationPlan {
	armed := armedColumns(source, colTypes, compareAs)
	keys.markFunctionalArming(source, colTypes, armed, compareAs)
	return planFromArmed(armed, keys)
}

// planFromArmed is the guard over the armed columns: every unique key (the
// primary key first) holding one, and the marker. nil when no key does. A
// functional part arms its key when its value may be a collated string
// ([functionalKeyPart.collated]), whatever its column's own collation pair
// (review 2026-10-15 F2), and never when it is a numeric or binary value,
// which no collation can merge.
func planFromArmed(armed map[string]armedCollation, keys targetUniqueKeys) *keyCollationPlan {
	p := &keyCollationPlan{pk: keys.pk, armed: armed}
	all := append([][]string{keys.pk}, keys.uniques...)
	allSubs := append([][]int{keys.pkSub}, keys.uniqueSubs...)
	allFns := append([][]*functionalKeyPart{nil}, keys.uniqueFns...)
	for i, key := range all {
		var subs []int
		if i < len(allSubs) {
			subs = allSubs[i]
		}
		var fns []*functionalKeyPart
		if i < len(allFns) {
			fns = allFns[i]
		}
		for j, col := range key {
			var fn *functionalKeyPart
			if j < len(fns) {
				fn = fns[j]
			}
			if _, ok := armed[col]; (fn == nil && !ok) || (fn != nil && !fn.arms) {
				continue
			}
			p.keys = append(p.keys, key)
			p.subs = append(p.subs, subs)
			p.fns = append(p.fns, fns)
			if p.marker == "" {
				p.marker = col // "" for a functional part reading no column: renderFor falls back
			}
			break
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
// remap ([Flavor.crossFlavorCollationRemap]), which through v0.159.0 moved a
// MySQL 8 `utf8mb4_0900_ai_ci` (NO PAD) to MariaDB's PAD SPACE
// `utf8mb4_uca1400_ai_ci`; judged by that map the column looked unchanged and
// an `'a '` row silently overwrote `'a'` (review F-A). Since Bug 305 the
// emitter creates the NO PAD form and the two maps agree on those names, but
// a target an older sluice created still holds the PAD SPACE column, which
// is why the guard keeps its own map. A source collation equal to the target's
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
		case ir.CollationDeterminismUnknown:
			// UNKNOWN determinism on a NAMED Postgres-dialect collation: a
			// schema no reader stamped. One producer today: a backup manifest
			// written before v0.159.0, whose type envelope had no
			// collation_determinism key (review 2026-10-12, finding 5). There
			// was a second until the mydumper reader derived a `COLLATE`-only
			// column's charset from its collation (MYDUMPER-COLLATION-
			// NO-CHARSET): carried with no charset, CollationDialect read its
			// MySQL collation as a Postgres one. A backup manifest written from
			// such an import still carries that shape. Left unarmed, such a key
			// went unguarded on a cross-engine restore and a resumed mydumper
			// import alike.
			//
			// Armed as bytes in EVERY key, the primary key included, at apply
			// too (review 2026-10-12 #2, finding 3; loud over silent). A named
			// DETERMINISTIC primary key ("C") of an old chain left unarmed at
			// apply let 'a' inserted in an incremental merge onto the full's
			// 'A' at exit 0. The cost, stated: if the collation was in fact
			// nondeterministic (case-insensitive), a re-applied case-only change
			// of that primary key is byte-different and refuses
			// KEY-COLLATION-MERGE loudly where it would have converged. nondet
			// is set because it may have been, for the remedy's sake.
			return armedCollation{bytes: true, nondet: true}, true
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
			// cannot both be in a source unique key at once. So WITHIN ONE
			// SNAPSHOT every pair of rows is byte-distinct and source-distinct, a
			// re-sent row is byte-identical to itself, and a target conflict
			// between byte-different keys is exactly a merge. ACROSS source
			// states — a copy resumed over a source that kept changing — a
			// byte-different conflict can also be a stale target row whose key
			// the source now spells differently but still equates (`'a'` copied,
			// then changed to `'A'`): the copy refuses that too, loudly, and its
			// remedy says which case is which ([copyKeyCollationMergeRemedy]).
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
			return armedCollation{bytes: true, uniqueOnly: true, nondet: true}, true
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
	rhs, hang, ok := p.renderValue(tableRef, cols, upsert, nil)
	if !ok {
		return "", "", false
	}
	return quoteIdent(hang) + " = " + rhs, hang, true
}

// renderValue is [keyCollationPlan.renderFor]'s right-hand side alone — the
// guarded value of the hang column. pass, when non-nil, renders the value
// the assignment takes when the keys are NOT a merge (nil: the hang
// column's incoming value), so the conflict-identity guard
// (unique_key_conflict_guard.go) runs only for a row this guard passed: a
// key the target's collation equates is reported as the collation merge it
// is, not as a dropped unique key.
func (p *keyCollationPlan) renderValue(tableRef string, cols []string, upsert upsertSpelling, pass func(hang string) string) (rhs, hang string, ok bool) {
	// Case-insensitively, as MySQL names columns: the catalog spells a key's
	// columns as the target does, the insert as the source does.
	inCols := make(map[string]bool, len(cols))
	for _, c := range cols {
		inCols[strings.ToLower(c)] = true
	}
	for i, key := range p.keys {
		for j, c := range key {
			read := []string{c}
			if i < len(p.fns) && j < len(p.fns[i]) && p.fns[i][j] != nil {
				read = p.fns[i][j].columns
			}
			for _, c := range read {
				if !inCols[strings.ToLower(c)] {
					return "", "", false
				}
			}
		}
	}
	tbl := tableRef + "."
	existing := func(c string) string { return tbl + quoteIdent(c) }
	incoming := func(c string) string { return upsert.newRowRef(quoteIdent(c)) }
	// part is a key part's compared value: the whole column, or — for a
	// prefix key part (SUB_PART n) — LEFT(x, n), because the server matches
	// an upsert through a prefix key on the prefix alone. Comparing whole
	// values there let 'Apple-1' overwrite 'apple-2' on a `k(5)` UNIQUE key
	// (review 2026-10-12 #2, finding 1). LEFT counts characters for a
	// character string and bytes for a binary one, which is how MySQL and
	// MariaDB define SUB_PART; it is applied before any CONVERT or CAST, in
	// the column's own character set, so the source comparison sees the same
	// characters the index holds.
	part := func(x string, n int) string {
		if n <= 0 {
			return x
		}
		return "LEFT(" + x + ", " + strconv.Itoa(n) + ")"
	}
	tgtEq := func(c string, n int) string { return part(existing(c), n) + " <=> " + part(incoming(c), n) }
	bytesEq := func(c string, n int) string {
		return "CAST(" + part(existing(c), n) + " AS BINARY) <=> CAST(" + part(incoming(c), n) + " AS BINARY)"
	}
	srcEq := func(c string, n int) string {
		ac, armed := p.armed[c]
		switch {
		case !armed:
			return tgtEq(c, n)
		case ac.bytes:
			return bytesEq(c, n)
		}
		conv := func(x string) string {
			return "CONVERT(" + part(x, n) + " USING " + ac.charset + ") COLLATE " + ac.collation
		}
		return conv(existing(c)) + " <=> " + conv(incoming(c))
	}
	// A functional part compares as its index does on the target side and
	// as its column's bytes on the source side (key_collation_functional.go
	// has why each is sound). It has no prefix length.
	tgtPart := func(c string, n int, fn *functionalKeyPart) string {
		if fn != nil {
			return fn.targetEq(existing(c), incoming(c))
		}
		return tgtEq(c, n)
	}
	srcPart := func(c string, n int, fn *functionalKeyPart) string {
		if fn != nil {
			return fn.sourceEq(func(c string) string { return bytesEq(c, 0) })
		}
		return srcEq(c, n)
	}
	all := func(cols []string, subs []int, fns []*functionalKeyPart, eq func(string, int, *functionalKeyPart) string) string {
		parts := make([]string, len(cols))
		for i, c := range cols {
			n := 0
			if i < len(subs) {
				n = subs[i]
			}
			var fn *functionalKeyPart
			if i < len(fns) {
				fn = fns[i]
			}
			parts[i] = eq(c, n, fn)
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	}
	merges := make([]string, len(p.keys))
	for i, key := range p.keys {
		var subs []int
		if i < len(p.subs) {
			subs = p.subs[i]
		}
		var fns []*functionalKeyPart
		if i < len(p.fns) {
			fns = p.fns[i]
		}
		merges[i] = "(" + all(key, subs, fns, tgtPart) + " AND NOT " + all(key, subs, fns, srcPart) + ")"
	}
	okExpr := "NOT (" + strings.Join(merges, " OR ") + ")"
	if len(p.pk) > 0 {
		// The same source row: its whole primary key equal under the source's
		// collation (stricter than a prefix match, so never a false "same").
		okExpr = "(" + all(p.pk, nil, nil, srcPart) + " OR " + okExpr + ")"
	}
	hang = guardHangColumn(cols, p.pk)
	// The raise sits in the CONDITION of an inner IF whose two arms are both
	// the hang column's incoming value, so the assignment's type is the
	// column's own and no value is converted (see the package comment's
	// measured table for the spellings that fail).
	marker := p.marker
	if marker == "" { // the only armed part is a functional one reading no column
		marker = cols[0]
	}
	raise := "18446744073709551615 + OCTET_LENGTH(CONCAT('" + keyCollationMergeMarker + "', " + incoming(marker) + "))"
	newHang := incoming(hang)
	passValue := newHang
	if pass != nil {
		passValue = pass(hang)
	}
	return "IF(" + okExpr + ", " + passValue + ", IF((" + raise + ") = 0, " + newHang + ", " + newHang + "))", hang, true
}

// keyCollationGuardEntry caches one table's upsert guards against the source
// shape they were computed from.
type keyCollationGuardEntry struct {
	source *ir.Table
	guard  *applyUpsertGuard
}

// upsertGuardFor returns the upsert guards for a write into schema.table
// (routed) of a change from srcSchema.table — the key-collation guard and
// the conflict-identity guard (unique_key_conflict_guard.go), both read off
// the one unique-key catalog read — or nil when neither is armed.
func (a *ChangeApplier) upsertGuardFor(ctx context.Context, q queryRower, srcSchema, schema, table string, pk []string, colTypes map[string]*ir.Column) (*applyUpsertGuard, error) {
	source := a.guardSourceShape(srcSchema, table)
	qn := qualifiedName(schema, table)
	a.cacheMu.RLock()
	e, hit := a.keyCollationGuardCache[qn]
	a.cacheMu.RUnlock()
	if hit && e.source == source {
		return e.guard, nil
	}
	compareAs := a.flavor.keyGuardCollations()
	// The unique keys beyond the primary key (one read per table and source
	// shape), whether or not a column differs: a functional key part arms
	// its key by its own collation (key_collation_functional.go). The
	// target must know every collation the guard names.
	keys, err := uniqueKeyColumns(ctx, q, a.flavor, schema, table)
	if err != nil {
		return nil, fmt.Errorf("mysql: applier: read the unique keys of %s for the key-collation guard: %w", qn, err)
	}
	if len(keys.unresolved) > 0 {
		return nil, keyCollationFunctionalUnsupported(qn, keys.unresolved)
	}
	// The applier upserts by its own key; the catalog supplies each part's
	// prefix length where that key is the catalog's primary key.
	keys.pk, keys.pkSub = pk, subPartsFor(pk, keys.pk, keys.pkSub)
	plan := planKeyCollationGuard(source, colTypes, keys, compareAs)
	if plan != nil {
		if err := checkKeyCollationsExpressible(ctx, q, qn, plan); err != nil {
			return nil, err
		}
	}
	a.cacheMu.Lock()
	if a.keyCollationGuardCache == nil {
		a.keyCollationGuardCache = map[string]keyCollationGuardEntry{}
	}
	guard := newApplyUpsertGuard(plan, keys)
	a.keyCollationGuardCache[qn] = keyCollationGuardEntry{source: source, guard: guard}
	a.cacheMu.Unlock()
	return guard, nil
}

// queryRower is the read surface the guard's metadata lookups need (a
// transaction or the pool).
type queryRower interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// subPartsFor returns, for each column of key, its prefix length in the
// catalog's primary key catPK/catSub when key is that same key (same
// columns, same order), else all zeros (whole columns).
func subPartsFor(key, catPK []string, catSub []int) []int {
	if len(key) != len(catPK) || len(catSub) != len(catPK) {
		return nil
	}
	for i := range key {
		if key[i] != catPK[i] {
			return nil
		}
	}
	return catSub
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
	needs, recopyAligns := keyCollationNeeds(plan)
	return &ir.KeyCollationMergeError{
		Schema: schema, Table: table, Cause: err,
		Remedy:         keyCollationMergeRemedy(plan, schema, table),
		Needs:          needs,
		RecopyDiverges: !recopyAligns,
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
	collations, recopyAligns := keyCollationNeeds(plan)
	if identityArmed {
		// No ALTER spelled out here: the collation change converges only for a
		// second source row, so the text names the collation without handing
		// over a statement to run unconditionally (pinned by the "no primary
		// key" cell of TestKeyCollationGuard_MySQLFamily).
		lead := fmt.Sprintf("The key that identifies a row of %s.%s is itself compared differently on the two "+
			"sides (or the table has no primary key), so sluice cannot tell a re-delivered change of that key "+
			"from a second source row, and this refusal repeats on every start. ", quoteIdent(schema), quoteIdent(table))
		if recopyAligns {
			return lead + "Re-copy the table, which converges either way: " + keyCollationRecopy + " recreates it with " +
				collations + ". Giving the target key that " +
				"collation yourself converges only if this is a second source row, not a re-delivered change of one"
		}
		return lead + "A re-copy does not converge here: sluice recreates the key under the target's default " +
			"collation, which equates the values. Giving the target key " + collations + " converges only if " +
			"this is a second source row, not a re-delivered change of one"
	}
	if plan.armsFunctional() {
		return "Give the target " + collations + ", and restart"
	}
	alter := fmt.Sprintf("`ALTER TABLE %s.%s MODIFY <column> …` with %s", quoteIdent(schema), quoteIdent(table), collations)
	if recopyAligns {
		return "Give the target's key column(s) " + collations + " (" + alter + ") and restart, or re-copy the " +
			"table: " + keyCollationRecopy + " recreates it with that collation"
	}
	return "Give the target's key column(s) " + collations + " (" + alter + ") and restart"
}

// keyCollationRecopy names the re-copy for each command that reaches the
// applier's remedy, since the flag differs between them (Bug 308: the
// remedy named --restart-from-scratch alone, which only `sync start`
// accepts). `restore` replaces this remedy with its own — it neither
// resumes nor re-copies a table (backup.restoreKeyCollationRemedy).
const keyCollationRecopy = "`sluice sync start --restart-from-scratch` (or, replaying a backup chain, " +
	"`sluice sync from-backup run --reset-target-data --yes`)"

// keyCollationNeeds is what each armed key column of plan must be declared
// with on this target for the guard's comparison to be the key's own, in
// key order: the guard's comparison collation (for a MySQL 8 source on
// MariaDB the NO PAD uca1400 form, not the source's 0900 name — Bug 305),
// or for a column armed to compare bytes (a Postgres source collation,
// which a MySQL target cannot declare) a binary collation. recopyAligns is
// false when any column is byte-armed: sluice creates such a key under the
// target's default collation, which equates the values, so a re-copy that
// recreates the table does not converge there. An armed functional part
// names its key recreated over its expression instead
// ([functionalKeyNeed]), and a re-copy does not converge there either: sluice
// recreates the key from the source's expression. Shared by the applier's
// remedy ([keyCollationMergeRemedy]) and the copy's
// ([copyKeyCollationMergeRefusal]) so the two never name different
// collations for one key.
func keyCollationNeeds(plan *keyCollationPlan) (collations string, recopyAligns bool) {
	var need []string
	recopyAligns = true
	named := map[string]bool{}
	allFns := append([][]*functionalKeyPart{nil}, plan.fns...)
	for i, key := range append([][]string{plan.pk}, plan.keys...) {
		for j, c := range key {
			ac, ok := plan.armed[c]
			if i < len(allFns) && j < len(allFns[i]) && allFns[i][j] != nil {
				// A functional part needs its key recreated, not its column
				// altered (KEY-COLLATION-FUNCTIONAL-KEY-PART).
				if f := allFns[i][j]; f.arms && !named["\x00"+f.index+"!"+f.expr] {
					named["\x00"+f.index+"!"+f.expr] = true
					recopyAligns = false
					need = append(need, functionalKeyNeed(f))
				}
				continue
			}
			if !ok || named[c] {
				continue
			}
			named[c] = true
			if ac.bytes {
				recopyAligns = false
				need = append(need, quoteIdent(c)+" a binary collation that does not pad (for example COLLATE utf8mb4_0900_bin; on MariaDB utf8mb4_nopad_bin)")
				continue
			}
			need = append(need, quoteIdent(c)+" CHARACTER SET "+ac.charset+" COLLATE "+ac.collation)
		}
	}
	collations = strings.Join(need, ", ")
	if collations == "" {
		collations = "the source's collation" // a plan names its armed keys; kept for a hand-built one
	}
	return collations, recopyAligns
}

// guardSourceShape is the source shape the key-collation guard arms from:
// the active schema version for srcSchema.table, or, when none is active, the
// shape a replay seeded through [ChangeApplier.SeedSourceShapes].
func (a *ChangeApplier) guardSourceShape(srcSchema, table string) *ir.Table {
	if source, ok := a.ActiveSchema(srcSchema, table); ok && source != nil {
		return source
	}
	return a.seededShapes.Lookup(srcSchema, table)
}

// SeedSourceShapes implements [ir.GuardSourceShapeSeeder].
func (a *ChangeApplier) SeedSourceShapes(tables []*ir.Table) {
	a.seededShapes.Seed(tables)
}
