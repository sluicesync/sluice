// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// ENUM/SET labels the catalog lost (GC-37 (i)).
//
// MySQL and MariaDB keep an ENUM/SET column's labels intact in the table
// definition the server executes against — a row holding the label
// `😀b` reads back from SELECT as the bytes F09F988062 — but every
// catalog surface sluice can read writes a character outside the Basic
// Multilingual Plane as '?': information_schema.COLUMNS.COLUMN_TYPE,
// SHOW CREATE TABLE, and SHOW COLUMNS alike, under any results charset
// including binary. MEASURED on MySQL 8.0.46 and MariaDB 11.4 with
// `ENUM('😀b','x','é') CHARACTER SET utf8mb4`: the catalog reads
// `enum('?b','x','é')` (the BMP 'é' survives).
//
// The schema sluice builds therefore carries '?b'. Where that matters:
//
//   - The bulk copy reads each row's LABEL from SELECT (`😀b`), which the
//     target's '?b' enum rejects: loud on Postgres (22P02) and on MySQL
//     (Error 1265). MEASURED with migrate MySQL → PG and MySQL → MySQL.
//   - The binlog carries an ENUM as its 1-based INDEX and a SET as a
//     bitmask, and [decodeEnum]/[decodeSet] map those to labels through
//     the catalog's list — so a row inserted after cutover landed as
//     '?b' at exit 0, on every target, and was accepted by a target
//     whose own enum was built from the same lossy list. MEASURED:
//     MySQL → PG `?b` / `{?,y}` and MySQL → MySQL 3F62 / 3F2C79 where the
//     source holds F09F988062 / F09F98802C79. This file closes that.
//
// The one faithful source that needs no row is the binlog's own
// TABLE_MAP event under `binlog_row_metadata=FULL`: its optional metadata
// carries each ENUM/SET column's labels from the server's in-memory
// definition. When it is present the decode takes the label from it
// (after checking it agrees with the catalog everywhere the catalog did
// not write '?'); when it is absent the decode refuses loudly, naming the
// label, rather than guess. Refusing is also what happens to a GENUINE
// '?' label on such a column without FULL metadata: the catalog cannot
// tell `?b` from `😀b`, and neither can sluice — FULL metadata is how an
// operator with a real '?' label streams it.
//
// VStream is the second path that maps an index to a label: vttablet's
// vstreamer does it before sluice sees the row, through the same lossy
// catalog, and hands over `?b` as TEXT ([refuseVStreamLostLabel]).
//
// Scope, stated — every MySQL-family path that carries an ENUM/SET value:
//
//   - binlog CDC (sync, backup stream): index/bitmask → guarded here.
//   - VStream CDC (PlanetScale, Vitess): label text rendered by vttablet
//     through the lossy catalog → guarded here (refuse only; no TABLE_MAP).
//   - bulk copy, VStream copy phase, `backup` fulls, mydumper row data,
//     flat files: carry the true label TEXT, which the target's '?' enum
//     rejects loudly (MEASURED for the bulk copy on PG and MySQL targets).
//     Not guarded here; loud already.
//
// A label is refused only when a row actually uses it, so a table no row
// of which uses a lost label streams normally, and an excluded table is
// never examined (the MySQL schema read is database-wide, so a refusal at
// schema-read time would block streams that never touch the table).

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/replication"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// enumLabelNotRecoverableMarker is the grep-stable marker the refusal
// carries.
const enumLabelNotRecoverableMarker = "ENUM-LABEL-NOT-RECOVERABLE"

// supplementaryCapableCharset reports whether a column charset can hold a
// character outside the BMP — the only charsets whose catalog '?' may be a
// lost character rather than a real one.
func supplementaryCapableCharset(charset string) bool {
	switch strings.ToLower(charset) {
	case "utf8mb4", "utf16", "utf16le", "utf32":
		return true
	}
	return false
}

// lostEnumSetLabels returns, for an ENUM/SET column, which of its catalog
// labels may have lost a character (one bool per label), or nil when none
// can have: the column is not ENUM/SET, its charset cannot hold such a
// character, or no label contains '?'.
func lostEnumSetLabels(charset string, t ir.Type) []bool {
	if !supplementaryCapableCharset(charset) {
		return nil
	}
	var values []string
	switch v := t.(type) {
	case ir.Enum:
		values = v.Values
	case ir.Set:
		values = v.Values
	default:
		return nil
	}
	var out []bool
	for i, l := range values {
		if !strings.Contains(l, "?") {
			continue
		}
		if out == nil {
			out = make([]bool, len(values))
		}
		out[i] = true
	}
	return out
}

// binlogLabelGuard is what [decodeBinlogRow] needs to map an ENUM index or
// SET bitmask through a column whose catalog labels may have lost a
// character. Both slices are parallel to the table's columns; the zero
// value guards nothing.
type binlogLabelGuard struct {
	// lost is [tableSchema.LostLabels].
	lost [][]bool
	// tableMap holds, per ENUM/SET column, the labels the row event's
	// TABLE_MAP carried (binlog_row_metadata=FULL); nil entries otherwise.
	tableMap [][]string
}

// binlogLabelGuardFor builds the guard for one rows event. tm may be nil.
func binlogLabelGuardFor(tbl *tableSchema, tm *replication.TableMapEvent) binlogLabelGuard {
	g := binlogLabelGuard{lost: tbl.LostLabels}
	if len(g.lost) == 0 || tm == nil {
		return g
	}
	enums, sets := tm.EnumStrValueString(), tm.SetStrValueString()
	if enums == nil && sets == nil {
		return g
	}
	g.tableMap = make([][]string, len(tbl.Columns))
	ei, si := 0, 0
	for i := 0; i < int(tm.ColumnCount) && i < len(tbl.Columns); i++ {
		switch {
		case tm.IsEnumColumn(i):
			if ei < len(enums) {
				g.tableMap[i] = enums[ei]
			}
			ei++
		case tm.IsSetColumn(i):
			if si < len(sets) {
				g.tableMap[i] = sets[si]
			}
			si++
		}
	}
	return g
}

// recoverLostLabels is applied to a decoded ENUM/SET value when the raw
// binlog value was an index or bitmask. It returns the value with every
// lost label replaced by the TABLE_MAP's label, or refuses.
func (g binlogLabelGuard) recoverLostLabels(table string, colIdx int, col *ir.Column, raw, decoded any) (any, error) {
	if colIdx >= len(g.lost) || g.lost[colIdx] == nil {
		return decoded, nil
	}
	lost := g.lost[colIdx]
	var catalog []string
	typ := ir.UnwrapDomain(col.Type)
	switch t := typ.(type) {
	case ir.Enum:
		catalog = t.Values
	case ir.Set:
		catalog = t.Values
	default:
		return decoded, nil
	}
	var used []int
	switch typ.(type) {
	case ir.Enum:
		idx, ok := enumIndex(raw)
		if !ok || idx <= 0 {
			return decoded, nil // a label the text path carried, or the '' member
		}
		used = []int{int(idx - 1)}
	case ir.Set:
		mask, ok := setMask(raw)
		if !ok {
			return decoded, nil
		}
		for i := range catalog {
			if mask&(uint64(1)<<uint(i)) != 0 {
				used = append(used, i)
			}
		}
	}
	var tm []string
	if colIdx < len(g.tableMap) {
		tm = g.tableMap[colIdx]
	}
	labels := catalog
	replaced := false
	for _, i := range used {
		if i >= len(lost) || !lost[i] {
			continue
		}
		if len(tm) != len(catalog) {
			return nil, enumLabelNotRecoverable(table, col.Name, catalog[i], "the row event's TABLE_MAP carries no labels for this column (binlog_row_metadata is not FULL)", enumLabelRemedyBinlog)
		}
		if !labelAgreesWithCatalog(tm[i], catalog[i]) {
			return nil, enumLabelNotRecoverable(table, col.Name, catalog[i], fmt.Sprintf("the row event's TABLE_MAP label %q does not match the catalog's %q", tm[i], catalog[i]), enumLabelRemedyBinlog)
		}
		if !replaced {
			labels = append([]string(nil), catalog...)
			replaced = true
		}
		labels[i] = tm[i]
	}
	if !replaced {
		return decoded, nil
	}
	switch typ.(type) {
	case ir.Enum:
		return labels[used[0]], nil
	default:
		out := make([]string, 0, len(used))
		for _, i := range used {
			out = append(out, labels[i])
		}
		return out, nil
	}
}

// labelAgreesWithCatalog reports whether a TABLE_MAP label is the one the
// catalog printed: identical once every character outside the BMP is
// written as '?', which is exactly the catalog's substitution. A genuine
// '?' label agrees with itself.
func labelAgreesWithCatalog(tableMap, catalog string) bool {
	if !utf8.ValidString(tableMap) {
		return false // a utf16/utf32 column's raw bytes; not decoded here
	}
	var b strings.Builder
	for _, r := range tableMap {
		if r > 0xFFFF {
			b.WriteByte('?')
			continue
		}
		b.WriteRune(r)
	}
	return b.String() == catalog
}

// refuseVStreamLostLabel is the VStream arm. vttablet's vstreamer renders an
// ENUM/SET cell as label TEXT, mapping the binlog's index through ITS OWN
// catalog view — which is just as lossy — so a row holding `😀b` arrives
// as `?b`. MEASURED through a real vtgate (vttestserver):
// TestVStream_EnumLabelLoss_ThroughVTGate saw e="?b" s=["?" "y"] for
// ('😀b', '😀,y') before this guard. There is no TABLE_MAP to recover from
// on this path, so a cell whose text IS a label the catalog may have lost
// refuses — including a genuine '?' label, which the stream cannot tell
// apart. The copy phase (rowstreamer's SELECT) carries the true text, which
// never equals a lost label and so passes here and fails loudly at the
// target instead. Cheap on every other cell: it returns before parsing
// anything unless the field is ENUM/SET and the text contains '?'.
func refuseVStreamLostLabel(table string, f *query.Field, v any) error {
	var members []string
	switch f.GetType() {
	case query.Type_ENUM:
		s, _ := v.(string)
		if !strings.Contains(s, "?") {
			return nil
		}
		members = []string{s}
	case query.Type_SET:
		ms, _ := v.([]string)
		for _, m := range ms {
			if strings.Contains(m, "?") {
				members = ms
				break
			}
		}
		if members == nil {
			return nil
		}
	default:
		return nil
	}
	charset := ""
	if name := collationEnv().LookupName(collations.ID(f.GetCharset())); name != "" {
		charset, _, _ = strings.Cut(name, "_")
	}
	kind := "enum"
	var typ ir.Type
	if f.GetType() == query.Type_SET {
		kind = "set"
	}
	labels, err := parseEnumOrSet(strings.TrimSpace(f.GetColumnType()), kind)
	if err != nil || len(labels) == 0 {
		// No label list to consult: the '?' could be a lost character on
		// a utf8mb4 column, so refuse unless the charset rules that out.
		if !supplementaryCapableCharset(charset) && charset != "" {
			return nil
		}
		return enumLabelNotRecoverable(table, f.GetName(), members[0],
			"the VStream field carries no label list to check it against", enumLabelRemedyVStream)
	}
	if kind == "set" {
		typ = ir.Set{Values: labels}
	} else {
		typ = ir.Enum{Values: labels}
	}
	lost := lostEnumSetLabels(charset, typ)
	if lost == nil {
		return nil
	}
	for _, m := range members {
		for i, l := range labels {
			if lost[i] && m == l {
				return enumLabelNotRecoverable(table, f.GetName(), l,
					"VStream renders an ENUM/SET value through the same catalog, so the label arrives already rewritten", enumLabelRemedyVStream)
			}
		}
	}
	return nil
}

// anyLostLabel reports whether any column of a [tableSchema.LostLabels]
// slice has a label that may have lost a character.
func anyLostLabel(lost [][]bool) bool {
	for _, l := range lost {
		if l != nil {
			return true
		}
	}
	return false
}

// enumLabelRemedyBinlog and enumLabelRemedyVStream are the refusal's hint
// on the two paths: only the binlog has a TABLE_MAP to recover from.
const (
	enumLabelRemedyBinlog = "either set binlog_row_metadata=FULL on the source (SET PERSIST binlog_row_metadata = 'FULL'; the next row " +
		"event carries the true labels, and a genuine '?' label then streams too), or rename the label on the source to characters " +
		"inside the Basic Multilingual Plane, or exclude the table"
	enumLabelRemedyVStream = "rename the label on the source to characters inside the Basic Multilingual Plane, or exclude the table " +
		"(a VStream source offers no faithful label to recover from)"
)

func enumLabelNotRecoverable(table, column, label, why, remedy string) error {
	return sluicecode.Wrap(
		sluicecode.CodeValueUnrepresentable,
		remedy,
		fmt.Errorf("mysql: cdc: %s: column %s.%s holds the ENUM/SET label the catalog prints as %q, and %s. MySQL's catalog "+
			"(information_schema, SHOW CREATE TABLE) writes a label character outside the Basic Multilingual Plane (an emoji) "+
			"as '?', and the change stream identifies the label only through that catalog, so sluice cannot tell which label "+
			"this row holds — carrying the catalog's text would land %q on the target where the source may hold something "+
			"else. A label that really is '?' is refused too: the catalog cannot distinguish them",
			enumLabelNotRecoverableMarker, table, column, label, why, label),
	)
}
