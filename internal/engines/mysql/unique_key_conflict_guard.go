// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// The conflict-identity guard (SOURCE-DROPPED-UNIQUE-KEY-MERGE).
//
// # The harm
//
// `INSERT … ON DUPLICATE KEY UPDATE` resolves a conflict through ANY unique
// key the target has, then overwrites the row it matched. The update list
// never names a primary-key column (onDuplicateKeyUpdateClause), so on a
// table with a primary key a conflict through a secondary unique key
// rewrites another row's non-key columns with the incoming row's values;
// on a table without one every column is assigned, so the matched row's
// identity itself is rewritten. Either way the incoming row is gone and the
// matched one corrupted, at exit 0.
//
// It happens when the target still has a unique key the source no longer
// has. Measured on v0.161.0 with real servers: source `kd(id PK, k, v)`
// with `CREATE UNIQUE INDEX kd_k ON kd(k)` and row (1,'a','one'); the
// source runs `DROP INDEX kd_k`, then inserts (99,'a','ninetynine'). The
// MySQL target ended `1 a ninetynine` with no row 99. Silent for a pgoutput
// or postgres-trigger source, and for a MySQL binlog source whose DROP
// landed while the stream was stopped; the source-side doors
// (cdc_unforwarded_classes.go, both engines) catch only what they can see.
// This guard is the door at the write, where every source and every
// stopped window converges.
//
// # The rule
//
// A conflict is the incoming row's own — a re-delivery the idempotent apply
// exists to absorb, an UPDATE coalesced into an upsert — exactly when the
// row it matched carries the incoming row's IDENTITY:
//
//   - with a primary key the row supplies: the primary key, compared as the
//     target compares it (`<=>`). A conflict through the primary key passes
//     by construction; a conflict through any other unique key whose matched
//     row has a different primary key is never a re-delivery, because a
//     re-delivered change of one source row carries that row's primary key.
//     (Whether two primary-key values the target equates are the same SOURCE
//     row is the key-collation guard's question, and it still answers it.)
//   - without one (or with one the row does not supply — a target-only
//     surrogate): the columns of EVERY unique key the target has. A
//     conflict through one key whose matched row disagrees on another key
//     is a second row; a matched row that agrees on all of them is the same
//     row as far as anything the target declares can tell, and its other
//     columns are overwritten — the documented newer-image upsert
//     (ChangeApplier "Identity-key behaviour"; pinned by
//     TestChangeApplier_NoPKWithUniqueKey_Idempotent). That test's comment
//     attributes the need to the ADR-0072 resumable cold-start COPY; since
//     v0.99.8 an interrupted cold start resumes through the bulk-COPY path
//     (streamer_run_phases.go, resumeCopyFrom), i.e. the copy writer, so
//     the applier's re-deliveries are the overlap windows listed below.
//
//     The consequence, stated so it cannot be read as broader: a table with
//     no primary key and exactly ONE unique key is not guarded. There the
//     merge this guard exists for (the source dropped that key and wrote a
//     duplicate) and the newer-image re-send it must absorb (the same source
//     row, changed) are the same statement against the same target row, and
//     nothing the applier holds tells them apart: the CDC boundary shapes
//     carry columns and the primary key, never unique indexes, and are stale
//     across exactly the DROP (postgres projectRelation, mysql
//     projectTableIR). Comparing every column was considered and rejected —
//     it refuses every newer-image re-send. That shape is left to the
//     source-side doors (both engines' cdc_unforwarded_classes.go, live) and
//     the filed stream-open comparison for a DROP made while the stream was
//     stopped (UNIQUE-KEY-STOPPED-WINDOW-DOOR).
//
//     The same holds, accepted and pinned
//     (TestUniqueKeyConflictGuard_AcceptedSilentResidual_NullKeyColumnsMerge),
//     for two different source rows that agree on every unique key because
//     the columns they differ in are NULL there (`<=>` calls NULL equal and a
//     unique key admits many NULLs): apply cannot tell them from a newer image
//     either, and a plain `=` would only move the miss.
//
// The guard is ARMED only where a conflict can come through a key other
// than the identity: a target table with a unique key besides its primary
// key (and, without a primary key, at least two unique keys). Any other
// table renders the statement byte-identical to before.
//
// The cost, stated: on a primary-key table, a unique VALUE that moved
// between rows inside a re-delivery window (a copy-handoff overlap: a
// lock-free MySQL capture, the VStream auto-shard stitch window; a broker
// re-run without apply marks) now refuses where it used to converge (row 1
// inserted with k='a' and deleted, row 99 inserted with k='a', all replayed
// onto a copy that already holds row 99). A Postgres target with a primary
// key has always refused that replay loudly (a unique_violation on the
// non-arbiter key), so this is parity, not a new class of wedge.
//
// The union arm has the same cost, wider (review MEDIUM, pinned as a stated
// trade): on a table without a primary key (or with a surrogate one the rows
// do not supply) and two or more unique keys, the SAME source row re-applied
// in an image whose OTHER unique column differs from the target's —
// kd(id UNIQUE, k UNIQUE): the source inserts (3,'c') and then moves it to id
// 4, the target already holds (4,'c'), and the INSERT (3,'c') is replayed —
// is the same statement as a merge and refuses. It needs a re-delivery of an
// applied change onto a target that is already AHEAD of it; by code reading
// those paths are the lock-free MySQL capture's overlap (the CDC tail starts
// at the pre-snapshot position, resolveLockFreeCapture), the VStream
// auto-shard stitch window (P_start = the per-shard minimum,
// stitchSnapshotMin), and a broker re-run without apply marks. Not measured
// on those paths; provoked directly by
// TestStreamer_UniqueKeyConflict_TradeRemedy, which also measures the printed
// remedy (--restart-from-scratch, same stream id) converging on a binlog and
// a pgoutput source. The ADR-0072
// resume is NOT such a path: it runs through the copy writer, which absorbs a
// conflict through a key the source declares. No narrowing is sound: the
// merge and the re-applied row differ only in source history the applier does
// not hold.
// UNIQUE-KEY-GUARD-OVERLAP-REFUSAL in the audit backlog.
//
// # Mechanism
//
// The key-collation guard's (key_collation_guard.go, whose comment has the
// measured table): the FIRST assignment of the update list becomes
// `h = IF(<identity equal>, new.h, IF((<raise>) = 0, new.h, new.h))`, on the
// row as it was before any assignment. When the key-collation guard is
// armed too, that whole expression is ITS passing value: the key-collation
// check runs first, so a key the target's collation equates reports as the
// collation merge it is, and the identity is checked for every row the
// key-collation guard passed (which is the shape of this bug: equal key
// bytes are no collation merge). <raise> is an
// unsigned overflow carrying UNIQUE-KEY-CONFLICT-OTHER-ROW; its operand is
// `ISNULL(new.h)`, which references the row (so the server cannot fold it)
// and is never NULL (so the raise cannot be swallowed by a NULL hang value,
// which a CONCAT of the value itself would be).

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// uniqueKeyConflictMarker is the literal the guard's raise carries; the
// refusal's sentinel ([ir.ErrUniqueKeyConflictOtherRow]) is the same text.
const uniqueKeyConflictMarker = "UNIQUE-KEY-CONFLICT-OTHER-ROW"

// guardHangColumn is the column whose assignment carries an upsert guard:
// the first non-primary-key column the insert names, else the first
// primary-key column (the all-key table's no-op assignment), else — with
// no primary key — the first column. Each is a column the update list
// assigns ([onDuplicateKeyUpdateColumns], [buildBatchUpsert]), so the
// guard replaces an assignment rather than adding one.
func guardHangColumn(cols, pk []string) string {
	if len(pk) == 0 {
		return cols[0]
	}
	pkSet := make(map[string]bool, len(pk))
	for _, c := range pk {
		pkSet[c] = true
	}
	for _, c := range cols {
		if !pkSet[c] {
			return c
		}
	}
	return pk[0]
}

// conflictIdentityColumns is the identity an upsert's matched row must
// carry to be the incoming row's own (see the file comment): the primary
// key when the insert names every column of it, else the union of the
// columns of every target unique key in uniqueCols when there are at least
// two of them and the insert names every such column. Columns are matched
// case-insensitively, as MySQL names them, and returned in the insert's
// spelling. nil means unarmed.
func conflictIdentityColumns(pk, cols []string, uniqueCols [][]string) []string {
	byLower := make(map[string]string, len(cols))
	for _, c := range cols {
		byLower[strings.ToLower(c)] = c
	}
	pick := func(want []string) []string {
		seen := map[string]bool{}
		out := make([]string, 0, len(want))
		for _, w := range want {
			c, ok := byLower[strings.ToLower(w)]
			if !ok {
				return nil
			}
			if !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
		return out
	}
	if len(pk) > 0 {
		if out := pick(pk); out != nil {
			return out
		}
	}
	if len(uniqueCols) < 2 {
		return nil
	}
	var union []string
	for _, key := range uniqueCols {
		union = append(union, key...)
	}
	return pick(union)
}

// uniqueKeyColumnSets is each target unique key's columns, a functional
// part contributing every column it reads (and a part reading none,
// nothing).
func uniqueKeyColumnSets(keys targetUniqueKeys) [][]string {
	out := make([][]string, 0, len(keys.uniques))
	for i, cols := range keys.uniques {
		var set []string
		for j, c := range cols {
			if i < len(keys.uniqueFns) && j < len(keys.uniqueFns[i]) && keys.uniqueFns[i][j] != nil {
				set = append(set, keys.uniqueFns[i][j].columns...)
				continue
			}
			if c != "" {
				set = append(set, c)
			}
		}
		out = append(out, set)
	}
	return out
}

// renderUpsertGuard composes the update list's guarded first assignment for
// an insert of cols into tableRef. coll (nil: unarmed) is the key-collation
// guard, checked FIRST — a key the target's collation equates is that
// guard's merge, and reporting it as a dropped unique key would send the
// operator after the wrong cause; identity (nil: unarmed) is the
// conflict-identity guard, checked for every row coll passed. pk picks the
// hang column when only the identity guard is armed. assign is "" when
// neither is; ok is false when coll cannot reference a key column's
// incoming value (the caller refuses).
func renderUpsertGuard(tableRef string, cols, pk []string, upsert upsertSpelling, identity []string, scope [][]scopePart, coll *keyCollationPlan) (assign, hang string, ok bool) {
	check := func(h string) string { return identityCheckValue(tableRef, upsert, identity, scope, h) }
	switch {
	case coll != nil:
		var value string
		if len(identity) == 0 {
			check = nil
		}
		if value, hang, ok = coll.renderValue(tableRef, cols, upsert, check); !ok {
			return "", "", false
		}
		return quoteIdent(hang) + " = " + value, hang, true
	case len(identity) > 0:
		hang = guardHangColumn(cols, pk)
		return quoteIdent(hang) + " = " + check(hang), hang, true
	}
	return "", "", true
}

// identityCheckValue is the conflict-identity guard's value of the hang
// column: its incoming value when the existing row (read through tableRef)
// equals the incoming row on every identity column, the marker's raise
// otherwise. The raise sits in the CONDITION of an IF whose arms are both
// the incoming value, as the key-collation guard's does.
//
// scope, when non-empty, limits the check to conflicts that MAY have come
// through one of those keys: the matched row must agree with the incoming
// row on every column of at least one of them (`=`, so a NULL never agrees —
// a unique key never matches through a NULL), else the conflict came through
// another key and is absorbed. nil applies the check to every conflict.
func identityCheckValue(tableRef string, upsert upsertSpelling, identity []string, scope [][]scopePart, hang string) string {
	eq := make([]string, len(identity))
	for i, c := range identity {
		eq[i] = tableRef + "." + quoteIdent(c) + " <=> " + upsert.newRowRef(quoteIdent(c))
	}
	newHang := upsert.newRowRef(quoteIdent(hang))
	raise := "18446744073709551615 + OCTET_LENGTH(CONCAT('" + uniqueKeyConflictMarker + "', ISNULL(" + newHang + ")))"
	ok := "(" + strings.Join(eq, " AND ") + ")"
	if len(scope) > 0 {
		agree := make([]string, len(scope))
		for i, key := range scope {
			parts := make([]string, len(key))
			for j, c := range key {
				parts[j] = c.of(tableRef+"."+quoteIdent(c.col)) + " = " + c.of(upsert.newRowRef(quoteIdent(c.col)))
			}
			agree[i] = "COALESCE((" + strings.Join(parts, " AND ") + "), FALSE)"
		}
		ok = "(" + ok + " OR NOT (" + strings.Join(agree, " OR ") + "))"
	}
	return "IF(" + ok + ", " + newHang + ", IF((" + raise + ") = 0, " + newHang + ", " + newHang + "))"
}

// isUniqueKeyConflictRaise reports whether err is the guard's raise: a 1690
// whose server text keeps the marker. The caller also requires that its
// statement carried the guard.
func isUniqueKeyConflictRaise(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1690 && strings.Contains(me.Message, uniqueKeyConflictMarker)
}

// describeUniqueKeys renders the target's unique keys other than the
// primary key as "name (a, b)", in catalog order. A functional key part
// renders as the first column it reads, or "expr" for one reading none.
func describeUniqueKeys(keys targetUniqueKeys) []string {
	out := make([]string, 0, len(keys.uniques))
	for i, cols := range keys.uniques {
		name := "?"
		if i < len(keys.uniqueNames) {
			name = keys.uniqueNames[i]
		}
		parts := make([]string, len(cols))
		for j, c := range cols {
			if c == "" {
				c = "expr"
			}
			parts[j] = c
		}
		out = append(out, name+" ("+strings.Join(parts, ", ")+")")
	}
	return out
}

// applyUpsertGuard is the CDC applier's per-table guard set for an upsert:
// the key-collation guard (nil when no column is armed) and the target's
// unique keys besides its primary key (empty when a conflict can only come
// through the primary key, which leaves the conflict-identity guard
// unarmed). A nil *applyUpsertGuard is an unguarded table and renders
// byte-identical SQL.
type applyUpsertGuard struct {
	coll   *keyCollationPlan
	others []string
	// uniqueCols parallels others: each key's columns ([uniqueKeyColumnSets]),
	// the identity of a table without a primary key.
	uniqueCols [][]string
	// keyNames parallels others: each key's bare index name, for the
	// remedy's DROP INDEX.
	keyNames []string
}

// newApplyUpsertGuard returns the guard set, or nil when neither guard is
// armed.
func newApplyUpsertGuard(coll *keyCollationPlan, keys targetUniqueKeys) *applyUpsertGuard {
	others := describeUniqueKeys(keys)
	if coll == nil && len(others) == 0 {
		return nil
	}
	return &applyUpsertGuard{coll: coll, others: others, uniqueCols: uniqueKeyColumnSets(keys), keyNames: append([]string(nil), keys.uniqueNames...)}
}

// render returns the update list's guarded first assignment and the column
// it assigns for an insert of cols into tableRef, keyed by pk (the
// applier's primary key). ok is false when the key-collation guard cannot
// reference a key column's incoming value (the caller refuses).
func (g *applyUpsertGuard) render(tableRef string, cols, pk []string, upsert upsertSpelling) (assign, hang string, ok bool) {
	if g == nil {
		return "", "", true
	}
	var identity []string
	if len(g.others) > 0 {
		identity = conflictIdentityColumns(pk, cols, g.uniqueCols)
	}
	return renderUpsertGuard(tableRef, cols, pk, upsert, identity, nil, g.coll)
}

// upsertGuardRefusal attributes a guarded upsert's failure to the guard
// that raised it: KEY-COLLATION-MERGE ([keyCollationMergeRefusal]) or
// UNIQUE-KEY-CONFLICT-OTHER-ROW. Anything else is returned unchanged.
func upsertGuardRefusal(err error, g *applyUpsertGuard, schema, table string) error {
	if g == nil {
		return err
	}
	if g.coll != nil && isKeyCollationMergeRaise(err) {
		return keyCollationMergeRefusal(err, g.coll, schema, table)
	}
	if len(g.others) == 0 || !isUniqueKeyConflictRaise(err) {
		return err
	}
	return &ir.UniqueKeyConflictError{
		Schema: schema, Table: table, Cause: err,
		Keys:   strings.Join(g.others, ", "),
		Remedy: applyUniqueKeyConflictRemedy(g.keyNames, schema, table),
	}
}

// applyUniqueKeyConflictRemedy is the CDC apply path's remedy: drop the
// key the source no longer has, or re-copy — which also converges when the
// source DOES still declare it and the matched target row is stale (a
// unique value that moved between rows inside a re-delivery window).
func applyUniqueKeyConflictRemedy(keyNames []string, schema, table string) string {
	drop := "`ALTER TABLE " + quoteIdent(schema) + "." + quoteIdent(table) + " DROP INDEX <key>`"
	if len(keyNames) == 1 {
		drop = "`ALTER TABLE " + quoteIdent(schema) + "." + quoteIdent(table) + " DROP INDEX " + quoteIdent(keyNames[0]) + "`"
	}
	check := "Check whether the source still declares that key."
	if len(keyNames) > 1 {
		// The statement cannot say which key matched, let alone which one the
		// source dropped: name the work instead of guessing.
		check = "The target has more than one unique key besides the primary key, and sluice cannot tell which " +
			"one the conflict came through or which one the source no longer declares: check EACH against the source."
	}
	return fmt.Sprintf("%s If the source no longer declares one, drop it on the target (%s) "+
		"and restart with the same --stream-id: the row then lands beside the one it matched, as on the source. If "+
		"the source still declares every one, the matched target row is stale (a newer image of a row, or a unique "+
		"value that moved between rows, inside a re-delivery window) and "+ir.UniqueKeyConflictRecopyRemedy, check, drop)
}

// undeclaredUniqueKeys returns the target's unique keys besides its primary
// key that the copy's source table does not declare — rendered as
// [describeUniqueKeys] does, by bare index name, and as the plain columns a
// conflict through it compares (nil for a key with a functional part). A
// plain target key is declared when the source has a unique index (or the
// primary key) over the same columns with the same prefix lengths (column
// names compared case-insensitively, as MySQL names them); the index NAME
// does not count, so a key redefined under its old name is undeclared. A
// key with a functional part cannot be matched by columns and is declared
// when the source has a unique index of the same name with an expression
// entry. The copy writer arms the conflict-identity guard only for an
// undeclared key: a conflict through a key the source DOES declare is a
// stale target row the copy is meant to refresh (a VStream catchup
// re-emission, a resumed copy over rows the source changed since), and the
// source's own key guarantees the copied rows cannot collide on it.
func undeclaredUniqueKeys(table *ir.Table, keys targetUniqueKeys) (rendered, names []string, cols [][]scopePart) {
	declaredFunctional := map[string]bool{}
	declaredCols := map[string]bool{}
	add := func(idx *ir.Index) {
		if idx == nil {
			return
		}
		parts := make([]string, 0, len(idx.Columns))
		for _, c := range idx.Columns {
			if c.Column == "" {
				if idx.Name != "" {
					declaredFunctional[strings.ToLower(idx.Name)] = true
				}
				return
			}
			parts = append(parts, strings.ToLower(c.Column)+"/"+strconv.Itoa(c.Length))
		}
		declaredCols[strings.Join(parts, "\x00")] = true
	}
	add(table.PrimaryKey)
	for _, idx := range table.Indexes {
		if idx != nil && idx.Unique {
			add(idx)
		}
	}
	all := describeUniqueKeys(keys)
	for i, keyCols := range keys.uniques {
		name := ""
		if i < len(keys.uniqueNames) {
			name = keys.uniqueNames[i]
		}
		plain := true
		parts := make([]string, len(keyCols))
		for j, c := range keyCols {
			if c == "" || (i < len(keys.uniqueFns) && j < len(keys.uniqueFns[i]) && keys.uniqueFns[i][j] != nil) {
				plain = false
				break
			}
			sub := 0
			if i < len(keys.uniqueSubs) && j < len(keys.uniqueSubs[i]) {
				sub = keys.uniqueSubs[i][j]
			}
			parts[j] = strings.ToLower(c) + "/" + strconv.Itoa(sub)
		}
		switch {
		case plain && declaredCols[strings.Join(parts, "\x00")]:
			continue
		case !plain && declaredFunctional[strings.ToLower(name)]:
			continue
		}
		rendered = append(rendered, all[i])
		names = append(names, name)
		if plain {
			cols = append(cols, scopeParts(keyCols, keys, i))
		} else {
			cols = append(cols, nil)
		}
	}
	return rendered, names, cols
}

// scopePart is one key part of an undeclared key in the copy writer's
// scope: its column and its index prefix length (SUB_PART; 0 for the whole
// column). The server matches through a prefix key on the prefix alone, so
// the scope compares LEFT(x, sub) — comparing whole values let (99,'abcde2')
// be absorbed into (1,'abcde1') through an undeclared k(5) (review round 3,
// measured on mysql:8.4), as the key-collation guard learned for its own
// prefix parts.
type scopePart struct {
	col string
	sub int
}

// of renders x as this part's compared value.
func (p scopePart) of(x string) string {
	if p.sub <= 0 {
		return x
	}
	return "LEFT(" + x + ", " + strconv.Itoa(p.sub) + ")"
}

// scopeParts is key i's columns with their prefix lengths.
func scopeParts(cols []string, keys targetUniqueKeys, i int) []scopePart {
	out := make([]scopePart, len(cols))
	for j, c := range cols {
		out[j] = scopePart{col: c}
		if i < len(keys.uniqueSubs) && j < len(keys.uniqueSubs[i]) {
			out[j].sub = keys.uniqueSubs[i][j]
		}
	}
	return out
}

// copyGuardScope is the copy writer's per-key scope for the identity check
// ([identityCheckValue]): the undeclared keys' columns, so a conflict that
// cannot have come through an undeclared key (the matched row disagrees with
// the incoming one on every undeclared key) is the stale-row refresh through a
// declared key and is absorbed. nil — the check applies to every conflict —
// when an undeclared key has a functional part, whose match cannot be told
// from its columns.
func copyGuardScope(undeclaredCols [][]scopePart) [][]scopePart {
	for _, c := range undeclaredCols {
		if len(c) == 0 {
			return nil
		}
	}
	return undeclaredCols
}

// copyUpsertGuardRefusal attributes a guarded copy upsert's failure to the
// guard that raised it: KEY-COLLATION-MERGE ([copyKeyCollationMergeRefusal])
// or UNIQUE-KEY-CONFLICT-OTHER-ROW. Anything else is returned unchanged.
func copyUpsertGuardRefusal(err error, guard copyKeyCollationGuard, table string) error {
	if guard.plan != nil && isKeyCollationMergeRaise(err) {
		return copyKeyCollationMergeRefusal(err, guard, table)
	}
	if len(guard.undeclared) == 0 || !isUniqueKeyConflictRaise(err) {
		return err
	}
	ref := quoteIdent(guard.schema) + "." + quoteIdent(table)
	drop := "`ALTER TABLE " + ref + " DROP INDEX <key>`"
	if len(guard.undeclaredNames) == 1 {
		drop = "`ALTER TABLE " + ref + " DROP INDEX " + quoteIdent(guard.undeclaredNames[0]) + "`"
	}
	return &ir.UniqueKeyConflictError{
		Schema: guard.schema, Table: table, Cause: err,
		Keys: strings.Join(guard.undeclared, ", "),
		Remedy: "The source table does not declare that key, so its rows may legitimately share a value the target " +
			"key forbids. Drop the key on the target (" + drop + ") and run the copy again, or exclude the table.",
	}
}
