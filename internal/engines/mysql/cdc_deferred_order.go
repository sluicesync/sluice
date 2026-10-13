// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Deferred shapes — a schema change no boundary will carry
// (BUG301-FULL-DEFERRED-SWAP, BUG301-DEFERRED-RETYPE).
//
// Under binlog_row_metadata=FULL a reader behind the binlog head decodes
// each event by its own column names (cdc_recorded_shape.go). When one of
// those columns has since been dropped or retyped, the shape is typed in
// part from the TABLE_MAP alone, and such a shape is never a schema
// boundary: the change it records reaches the pipeline only folded into
// the next boundary built from read definitions — AFTER its rows. Two
// harms were measured (MySQL 8.0, 8.4, MariaDB 11.4, warm resume):
//
//   - a RENAME swap vanishes: `RENAME b TO c, c TO b` then `DROP COLUMN b`
//     arrives as `(id,b,c) → (id,c)`, a drop with the surviving name in
//     place, and the target keeps its pre-swap values under `c` at exit 0;
//   - a type change lands its rows under the target's OLD type before the
//     boundary that would have changed it: `MODIFY b DATETIME(6)`, a row,
//     then `DROP COLUMN c` — the target rounded the row's fractional
//     seconds away, and the folded boundary's refusal came after it.
//
// So the shape is held here, before any row of it is decoded: a shape that
// will not be a boundary must keep every pair of the names it shares with
// the table's basis in the same relative order (ir.ErrColumnOrderChanged)
// and give each of them the same IR type (ir.ErrDeferredSchemaChange). The
// type equivalence is [ir.SameColumnType], the one that decides whether a
// change is a schema boundary at all — deliberately not
// recordedColumnAccepts, which asks only whether a definition decodes the
// row image (it ignores DATETIME precision and DECIMAL scale, which the
// decoder takes from the image) and so cannot see the second harm. A
// column the shape typed from the TABLE_MAP alone — the column dropped or
// retyped later — has an approximate IR type, so it is held only on what
// the TABLE_MAP records exactly ([tableMapRecorded], and a character
// column's byte length through its character set, [holdRecordedLength]);
// comparing less let a column retyped twice hide its first retype (review
// H1) or its first widen (review of 3e9de74a).
//
// The basis is the shape of the table's previous decoded event, never the
// decode cache — behind the head the cache holds the catalog as of the
// read, the future (priorSig is refreshed from it, so it cannot serve).
// For a table's first event of the run it is the baseline the pipeline
// hands over ([CDCReader.SetDecodedShapeBaseline]), typed in every case:
// the schema the cold start copied from, the retained schema history at
// the warm-resume position, a backup chain's parent manifest. Once a
// baseline is given, a table missing from it has no basis, and its first
// deferred shape refuses with the re-copy remedy rather than pass
// unchecked. Every pipeline open site that streams hands one over (the
// single-stream and multi-database sync paths, both backup lanes); a
// reader given none compares from its first decoded event on.
//
// What else a deferred shape can fold away, measured on MySQL 8.4 (FULL,
// warm resume), and caught downstream: a rename (then a DROP of it or of
// another column) and an ADD (then a DROP of another column) put a column
// the target lacks into the rows, and the apply refuses; a swap followed
// by an ADD is not deferred (every recorded column has a read definition),
// so the swap is its own boundary.

// decodedShape is a table's basis: its column order, and the IR type of
// each column whose type came from a read definition. seeded names the
// columns whose type is still the one [CDCReader.SetDecodedShapeBaseline]
// handed over — a catalog read, which for a backup chain's parent manifest
// can be taken past the parent's end position — rather than one an event of
// this stream was decoded under.
type decodedShape struct {
	names  []string
	types  map[string]ir.Type
	seeded map[string]bool
}

// holdDecodedOrder holds tbl to the table's basis when tbl will not be a
// schema boundary, then makes tbl the basis.
func (r *CDCReader) holdDecodedOrder(qn string, tbl *tableSchema) error {
	cur := projectTableIR(tbl)
	approximate := make(map[string]bool, len(tbl.tableMapTyped))
	for _, n := range tbl.tableMapTyped {
		approximate[n] = true
	}
	basis, hasBasis := r.decodedShapes[qn]
	if tbl.synthesized {
		if !hasBasis && r.decodedShapeBaselineSet {
			return deferredNoBasisRefusal(qn)
		}
		if hasBasis {
			if err := basis.hold(qn, cur, approximate, tbl.unnamedCollations, r.serverCollationIDNames != nil); err != nil {
				return err
			}
		}
	}
	next := decodedShape{names: ir.ColumnNames(cur), types: make(map[string]ir.Type, len(cur.Columns))}
	for _, c := range cur.Columns {
		switch {
		case !approximate[c.Name]:
			next.types[c.Name] = c.Type
		case basis.types[c.Name] != nil:
			// Carried: an approximate type says nothing new about the column.
			next.types[c.Name] = basis.types[c.Name]
			if basis.seeded[c.Name] {
				if next.seeded == nil {
					next.seeded = map[string]bool{}
				}
				next.seeded[c.Name] = true
			}
		}
	}
	if r.decodedShapes == nil {
		r.decodedShapes = map[string]decodedShape{}
	}
	r.decodedShapes[qn] = next
	r.noteBasisAt(qn)
	return nil
}

// hold refuses a shape (cur) that reorders or retypes any column it
// shares with the basis b. unnamed are cur's TABLE_MAP-typed character
// columns whose recorded collation ID was not named, and namesLoaded says
// whether the source's collation table was read at all.
func (b decodedShape) hold(qn string, cur *ir.Table, approximate map[string]bool, unnamed map[string]uint64, namesLoaded bool) error {
	names := ir.ColumnNames(cur)
	if a, c, flipped := firstOrderFlip(b.names, names); flipped {
		return deferredOrderRefusal(qn, a, c)
	}
	for _, col := range cur.Columns {
		was, known := b.types[col.Name]
		if !known {
			continue
		}
		if approximate[col.Name] {
			if !ir.SameColumnType(tableMapRecorded(was), tableMapRecorded(col.Type)) {
				return deferredTypeRefusal(qn, col.Name, was, col.Type)
			}
			if err := holdRecordedLength(qn, col.Name, was, col.Type); err != nil {
				return err
			}
			if id, ok := unnamed[col.Name]; ok && characterCollation(was) != "" {
				return deferredUnnamedCollationRefusal(qn, col.Name, id, characterCollation(was), namesLoaded)
			}
			if !recordedCollationAgrees(was, col.Type) {
				return deferredCollationRefusal(qn, col.Name, characterCollation(was), characterCollation(col.Type))
			}
			continue
		} else if ir.SameColumnType(was, col.Type) {
			continue
		}
		return deferredTypeRefusal(qn, col.Name, was, col.Type)
	}
	return nil
}

// tableMapRecorded is t reduced to what a FULL-metadata TABLE_MAP records
// exactly, so a column typed from the TABLE_MAP alone (whose IR type is
// otherwise approximate) can still be held to a known type: the family,
// temporal precision, decimal precision and scale, integer width and
// signedness, float single/double, BIT width, ENUM/SET labels, string
// size class and character set all survive. Cleared on both sides: what
// the TABLE_MAP does not carry — an ENUM's collation and an integer's
// AUTO_INCREMENT — and two properties it DOES record but that compare
// separately: a character column's collation, recorded as an ID and held
// by its name where the source names it ([recordedCollationAgrees]), and
// its length, recorded in bytes (VARCHAR(40) utf8mb4 reads as 160) and
// held through the character set ([holdRecordedLength]).
// Without this, a column retyped twice hid its first
// retype: `DATETIME` → `DATETIME(6)`, a row, → `VARCHAR(40)` left the
// row's shape typed from the TABLE_MAP, and the row landed rounded in the
// target's DATETIME (measured, review H1). Every family is pinned to hold
// to its own catalog type by TestCDCRecordedColumnTypes_EveryFamily.
func tableMapRecorded(t ir.Type) ir.Type {
	switch v := t.(type) {
	case ir.Varchar:
		return ir.Varchar{Charset: v.Charset}
	case ir.Char:
		return ir.Char{Charset: v.Charset}
	case ir.Text:
		return ir.Text{Size: v.Size, Charset: v.Charset}
	case ir.Integer:
		return ir.Integer{Width: v.Width, Unsigned: v.Unsigned}
	case ir.Enum:
		return ir.Enum{Values: v.Values}
	}
	return t
}

// recordedCollationAgrees is the collation half of holding an approximate
// character column: the collation the TABLE_MAP recorded for it
// ([withRecordedCollation]) against the basis's. A collation change is not
// storage-neutral — `_ci` → `_bin` (or PAD SPACE → NO PAD) on a key column
// changes which values are one key — so rows written under it must not
// reach a target whose column still compares under the old one before any
// boundary changes it: measured on MySQL 8.4 FULL, `MODIFY k … COLLATE
// utf8mb4_bin`, a row 'A' beside the existing 'a', then `MODIFY k
// VARBINARY(80)` — the warm resume merged 'A' into the row of 'a' on the
// target's `_ci` key and only then refused at the folded boundary
// (DEFERRED-KEY-COLLATION). Compared only where both sides name one: a
// collation ID the source's table did not name is unstamped.
func recordedCollationAgrees(was, now ir.Type) bool {
	w, n := characterCollation(was), characterCollation(now)
	return w == "" || n == "" || strings.EqualFold(w, n)
}

// characterCollation is a character type's declared collation, "" for any
// other type.
func characterCollation(t ir.Type) string {
	switch v := t.(type) {
	case ir.Varchar:
		return v.Collation
	case ir.Char:
		return v.Collation
	case ir.Text:
		return v.Collation
	}
	return ""
}

// holdRecordedLength is the length half of holding an approximate
// character column. The TABLE_MAP records a VARCHAR/CHAR's maximum length
// in BYTES (beside its character set), the basis in characters, so the two
// compare only through the character set's maximum bytes per character.
// Only a WIDEN refuses: a column that may now hold more than the basis
// could means rows written under it can exceed the target's column, and
// MySQL (with a Note) and Postgres (silently) cut excess trailing spaces
// rather than refuse — measured: `VARCHAR(20)` → `VARCHAR(100)`, a 31-byte
// value ending in spaces, → `VARBINARY(200)`, and the row landed with 20
// bytes on both targets before the folded boundary refused (review of
// 3e9de74a). A shrink keeps the target wider than any value written and
// is safe, which is also why CHAR(8) recorded as 32 utf8mb4 bytes holds.
// A character set sluice cannot size refuses: nothing proves the bound.
func holdRecordedLength(qn, col string, was, now ir.Type) error {
	var basisChars, recordedBytes int
	var charset string
	switch n := now.(type) {
	case ir.Varchar:
		basisChars, recordedBytes, charset = was.(ir.Varchar).Length, n.Length, n.Charset
	case ir.Char:
		basisChars, recordedBytes, charset = was.(ir.Char).Length, n.Length, n.Charset
	default:
		return nil
	}
	perChar, known := charsetMaxBytes[canonicalCharsetName(charset)]
	if known && recordedBytes <= basisChars*perChar {
		return nil
	}
	return fmt.Errorf("mysql: cdc: %s: %w: column %q holds up to %d bytes of %s at this binlog event, and the last shape "+
		"this stream knew held %d characters (%s). A value longer than the target column would be cut — MySQL and "+
		"Postgres drop excess trailing spaces with no error — before any boundary widens the target; %s",
		qn, ir.ErrDeferredSchemaChange, col, recordedBytes, charset, basisChars, charsetBound(charset, basisChars, perChar, known), deferredWhy)
}

// charsetBound renders what the basis length allows, in bytes.
func charsetBound(charset string, chars, perChar int, known bool) string {
	if !known {
		return "a character set whose bytes per character sluice does not know"
	}
	return fmt.Sprintf("at most %d bytes of %s", chars*perChar, charset)
}

// charsetMaxBytes is each MySQL/MariaDB character set's maximum bytes per
// character (information_schema.CHARACTER_SETS.MAXLEN), by the canonical
// name canonicalCharsetName yields. Static because it is a property of the
// character set, not of a server.
var charsetMaxBytes = map[string]int{
	"armscii8": 1, "ascii": 1, "big5": 2, "binary": 1, "cp1250": 1, "cp1251": 1, "cp1256": 1, "cp1257": 1,
	"cp850": 1, "cp852": 1, "cp866": 1, "cp932": 2, "dec8": 1, "eucjpms": 3, "euckr": 2, "gb18030": 4,
	"gb2312": 2, "gbk": 2, "geostd8": 1, "greek": 1, "hebrew": 1, "hp8": 1, "keybcs2": 1, "koi8r": 1,
	"koi8u": 1, "latin1": 1, "latin2": 1, "latin5": 1, "latin7": 1, "macce": 1, "macroman": 1, "sjis": 2,
	"swe7": 1, "tis620": 1, "ucs2": 2, "ujis": 3, "utf16": 4, "utf16le": 4, "utf32": 4, "utf8mb3": 3,
	"utf8mb4": 4,
}

// firstOrderFlip returns a pair of names both lists hold whose relative
// order differs: a before b in prev, b before a in next.
func firstOrderFlip(prev, next []string) (a, b string, flipped bool) {
	at := make(map[string]int, len(next))
	for i, n := range next {
		at[n] = i
	}
	var shared []string
	for _, n := range prev {
		if _, ok := at[n]; ok {
			shared = append(shared, n)
		}
	}
	for i := 1; i < len(shared); i++ {
		if at[shared[i]] < at[shared[i-1]] {
			return shared[i-1], shared[i], true
		}
	}
	return "", "", false
}

// deferredWhy is what every deferred-shape refusal says about why the
// operator cannot simply apply the change on the target and resume: the
// boundary that would be judged against the target is never emitted.
const deferredWhy = "this event's shape cannot be checked against the target, because a column of it was dropped or " +
	"retyped later in the binlog and its definition is not readable, so no schema boundary will describe it before its " +
	"rows are applied. Refusing before any row of it is decoded. Re-copy the stream: `sluice sync start " +
	"--reset-target-data --yes` with the stream's other flags unchanged for a sync, or a fresh `backup full` for a " +
	"backup chain"

// deferredOrderRefusal is the reader-side COLUMN-ORDER-CHANGED.
func deferredOrderRefusal(qn, a, b string) error {
	return fmt.Errorf("mysql: cdc: %s: %w: the source declared %q before %q at this binlog event and %q before %q "+
		"at the last shape this stream knew. A RENAME COLUMN swap or rotation (the names move, the values stay) and a "+
		"column reorder (the values move with the names) look identical, and only the swap leaves the target's earlier "+
		"rows under the wrong names; %s",
		qn, ir.ErrColumnOrderChanged, b, a, a, b, deferredWhy)
}

// deferredTypeRefusal refuses a column whose type changed under a deferred
// shape: its rows would reach the target under the target's old type,
// which may hold them only by changing them.
func deferredTypeRefusal(qn, col string, was, now ir.Type) error {
	return fmt.Errorf("mysql: cdc: %s: %w: column %q is %v at this binlog event and was %v at the last shape this "+
		"stream knew. Its rows would be applied before the target's column changes type, and a target type that "+
		"cannot hold them exactly may round or truncate them without an error; %s",
		qn, ir.ErrDeferredSchemaChange, col, now, was, deferredWhy)
}

// deferredCollationRefusal refuses a character column whose collation
// changed under a deferred shape.
func deferredCollationRefusal(qn, col, was, now string) error {
	return fmt.Errorf("mysql: cdc: %s: %w: column %q has collation %s at this binlog event and had %s at the last "+
		"shape this stream knew. Its rows would be applied before the target's column changes collation, and on a "+
		"key a target still comparing under %s can merge rows the source keeps apart; %s",
		qn, ir.ErrDeferredSchemaChange, col, now, was, was, deferredWhy)
}

// deferredUnnamedCollationRefusal refuses a character column whose recorded
// collation ID could not be named, where the basis declares a collation:
// whether it changed cannot be told, and passing it would be the
// fail-open review F4 found (the charset half of the same lookup already
// refuses an unnamed ID).
func deferredUnnamedCollationRefusal(qn, col string, id uint64, was string, namesLoaded bool) error {
	why := "the source's collation table does not name it"
	if !namesLoaded {
		why = "the source's collation table (information_schema.COLLATIONS) could not be read, so it could not be " +
			"named; starting the stream again retries the read"
	}
	return fmt.Errorf("mysql: cdc: %s: %w: column %q was written under collation ID %d, and %s. The last shape this "+
		"stream knew declared %s, and whether this event's collation differs cannot be told; %s",
		qn, ir.ErrDeferredSchemaChange, col, id, why, was, deferredWhy)
}

// deferredNoBasisRefusal refuses a deferred shape of a table the stream
// has no typed shape for (on a warm resume: no retained schema history at
// or before the resume position, typically a table that had no row or
// schema change between its copy and the stop).
func deferredNoBasisRefusal(qn string) error {
	return fmt.Errorf("mysql: cdc: %s: %w: the stream holds no typed shape of this table from before this event to "+
		"compare it with; %s", qn, ir.ErrDeferredSchemaChange, deferredWhy)
}

// SetDecodedShapeBaseline hands the reader each table's typed source shape
// at the position it is about to stream from — the schema the cold start
// copied from, the retained schema history at the warm-resume position, a
// backup chain's parent manifest — as the basis [CDCReader.holdDecodedOrder]
// holds the table's first deferred shape to. Once called (even with no
// tables) the reader is strict: a table absent from the baseline refuses
// its first deferred shape. It arms nothing else. A table with no Schema
// takes the reader's bound database. Must be called before
// [StreamChanges]; replaces any earlier baseline.
func (r *CDCReader) SetDecodedShapeBaseline(tables []*ir.Table) {
	r.decodedShapeBaselineSet = true
	r.decodedShapes = make(map[string]decodedShape, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		schema := t.Schema
		if schema == "" {
			schema = r.schema
		}
		s := decodedShape{names: ir.ColumnNames(t), types: make(map[string]ir.Type, len(t.Columns)), seeded: map[string]bool{}}
		for _, c := range t.Columns {
			if c != nil && c.Type != nil {
				s.types[c.Name] = c.Type
				s.seeded[c.Name] = true
			}
		}
		r.decodedShapes[qualifiedName(schema, t.Name)] = s
		r.noteBasisAt(qualifiedName(schema, t.Name))
	}
}
