// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// The charset-DDL replay guard (GC-37 (j) re-review item 1).
//
// # The window it closes
//
// A change-stream value is decoded by the charset its column is declared in.
// On the binlog with TABLE_MAP charsets (MySQL 8 default, MariaDB MINIMAL/
// FULL), that is the charset each event says it was WRITTEN in, so a replay
// across a charset DDL is exact ([binlogColumnCharsets]). Two sources carry
// no such statement:
//
//   - MariaDB under its default binlog_row_metadata=NO_LOG — the TABLE_MAP
//     names no charset, and the reader decodes by the catalog;
//   - PlanetScale/Vitess — vttablet tags a replayed row's field with the
//     column's CURRENT collation (MEASURED on vttestserver).
//
// On both, a stream replaying history recorded before an `ALTER … CHARACTER
// SET` decodes the old bytes by the new charset. MEASURED against a v0.156.2
// build on the same probes: utf8mb4 'é' replayed after a change to latin1 —
// v0.156.2 emitted 'é' (its raw passthrough happened to be right), the first
// cut of this fix 'Ã©' at exit 0; latin1 '€' after a change to cp1251 —
// v0.156.2 emitted raw 0x80 (loud at the target), the first cut 'Ђ' silently.
// Without a guard, this fix made those two sources WORSE.
//
// # How the guard sees it, without a clock
//
// The DDL itself arrives on the stream, after the rows it postdates. When a
// DDL event names a charset explicitly for table T (`CONVERT TO CHARACTER
// SET x`, or `MODIFY`/`CHANGE` of a column `… CHARACTER SET x` or
// `… COLLATE <a collation of x>`), the reader compares that charset with the
// shape it has been decoding T's rows by. On a live stream that shape
// predates the DDL and shows the OLD charset. If it already shows the NEW
// one for every column the DDL names, the shape was loaded AFTER the DDL had
// run — the stream is replaying — and every row of T decoded since the
// shape was loaded was decoded by the post-DDL charset. That refuses with
// the CDC-4 replay-mismatch class and the re-snapshot remedy.
//
// Classification is by the Vitess SQL parser, not a pattern over the text
// (the IR-first rule): a statement it cannot parse is not classified.
//
// # What it cannot see — the windows that remain, stated
//
//   - It fires AFTER the fact: the rows before the DDL event have already
//     been emitted, and may have been applied. The refusal says so, and its
//     remedy (re-snapshot) re-copies them.
//   - A `MODIFY` with no explicit charset still changes a column's charset
//     to the table default; the guard cannot see that change.
//   - A DDL the parser cannot parse, or naming a collation sluice cannot
//     name (e.g. MariaDB-only uca1400 names), is not classified.
//   - An online schema change (gh-ost, pt-online-schema-change, PlanetScale
//     deploy requests / Vitess online DDL) alters a shadow table and swaps
//     it in with a RENAME: no charset ALTER on T ever reaches the stream.
//   - A charset DDL outside the window the stream replays (run while no rows
//     of T were decoded since the shape load) needs no guard and gets none.
//   - A NO-OP charset DDL — setting a column to the charset it already has —
//     looks exactly like a replayed one to this comparison and refuses. That
//     is a loud false positive, stated in the refusal.
//   - The TABLE_MAP path (MySQL default) needs none of this and skips it.
//
// # Why not a clock (the rejected option)
//
// Comparing information_schema.TABLES.CREATE_TIME with the resume position
// would fire BEFORE the first row, but it is not evidence of a charset
// change: every table rebuild (OPTIMIZE, a COPY/INPLACE ALTER of any kind)
// moves it while an INSTANT ALTER does not, MariaDB and MySQL disagree on
// when it moves, a binlog position carries no wall-clock to compare it to
// without reading event timestamps, and vtgate answers information_schema
// per keyspace, not per shard. It would refuse on every unrelated rebuild,
// so the DDL comparison is the guard.

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// charsetHistoryUnrecordedMarker is the grep-stable marker on the WARN a
// reader logs, once per table, when it decodes a table with non-UTF-8 text
// columns and the source states no written charset for it.
const charsetHistoryUnrecordedMarker = "CHARSET-HISTORY-UNRECORDED"

// charsetDDLChange is what one ALTER TABLE says about charsets.
type charsetDDLChange struct {
	schema, table string
	// all is the charset CONVERT TO sets every character column to; "" when
	// the statement has no CONVERT TO.
	all string
	// cols maps a (lower-cased, post-DDL) column name to the charset the
	// statement sets it to explicitly.
	cols map[string]string
}

var (
	ddlParserOnce sync.Once
	ddlParser     *sqlparser.Parser
)

func charsetDDLParser() *sqlparser.Parser {
	ddlParserOnce.Do(func() {
		p, err := sqlparser.New(sqlparser.Options{MySQLServerVersion: collationEnvVersion})
		if err == nil {
			ddlParser = p
		}
	})
	return ddlParser
}

// charsetForCollationName names the charset of a collation, or "".
func charsetForCollationName(name string) string {
	if name == "" {
		return ""
	}
	id := collationEnv().LookupByName(strings.ToLower(name))
	if id == collations.Unknown {
		return ""
	}
	return collationEnv().LookupCharsetName(id)
}

// parseCharsetDDL classifies stmt. ok is false for anything that is not an
// ALTER TABLE naming a charset explicitly, and for a statement the parser
// cannot read.
func parseCharsetDDL(stmt string) (charsetDDLChange, bool) {
	p := charsetDDLParser()
	if p == nil {
		return charsetDDLChange{}, false
	}
	parsed, err := p.Parse(stmt)
	if err != nil {
		return charsetDDLChange{}, false
	}
	alter, ok := parsed.(*sqlparser.AlterTable)
	if !ok {
		return charsetDDLChange{}, false
	}
	c := charsetDDLChange{
		schema: alter.Table.Qualifier.String(),
		table:  alter.Table.Name.String(),
		cols:   map[string]string{},
	}
	colCharset := func(def *sqlparser.ColumnDefinition) {
		if def == nil || def.Type == nil {
			return
		}
		cs := def.Type.Charset.Name
		if cs == "" && def.Type.Options != nil {
			cs = charsetForCollationName(def.Type.Options.Collate)
		}
		if cs != "" {
			c.cols[strings.ToLower(def.Name.String())] = canonicalCharsetName(cs)
		}
	}
	for _, opt := range alter.AlterOptions {
		switch o := opt.(type) {
		case *sqlparser.AlterCharset:
			cs := o.CharacterSet
			if cs == "" {
				cs = charsetForCollationName(o.Collate)
			}
			if cs != "" && !strings.EqualFold(cs, "default") {
				c.all = canonicalCharsetName(cs)
			}
		case *sqlparser.ModifyColumn:
			colCharset(o.NewColDefinition)
		case *sqlparser.ChangeColumn:
			colCharset(o.NewColDefinition)
		}
	}
	if c.all == "" && len(c.cols) == 0 {
		return charsetDDLChange{}, false
	}
	return c, true
}

// alreadyApplied reports whether shape — a table's character columns and
// the charset the reader has been decoding each by (lower-cased names) —
// already carries every change c names, and returns the columns it compared.
// A named column absent from shape (a live CHANGE renames it) means the
// shape predates the DDL.
func (c charsetDDLChange) alreadyApplied(shape map[string]string) (applied bool, compared []string) {
	if c.all != "" {
		if len(shape) == 0 {
			return false, nil
		}
		for col, cs := range shape {
			if cs != c.all {
				return false, nil
			}
			compared = append(compared, col)
		}
	}
	for col, want := range c.cols {
		got, ok := shape[col]
		if !ok || got != want {
			return false, nil
		}
		compared = append(compared, col)
	}
	sort.Strings(compared)
	return len(compared) > 0, compared
}

// errCharsetDDLAlreadyApplied is the guard's refusal.
func errCharsetDDLAlreadyApplied(schema, table string, c charsetDDLChange, cols []string) error {
	to := c.all
	if to == "" {
		parts := make([]string, 0, len(cols))
		for _, col := range cols {
			parts = append(parts, col+"→"+c.cols[col])
		}
		to = strings.Join(parts, ", ")
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCSchemaReplayMismatch,
		"re-snapshot the table (sync --restart-from-scratch); rows of it already applied since the stream loaded its shape were decoded by the post-DDL charset",
		fmt.Errorf("mysql: cdc: %s.%s: the stream reached a DDL that sets the charset of %s (%s), but the shape it has been decoding "+
			"this table's rows by ALREADY carries that charset — it was loaded after the DDL ran, so this stream is replaying "+
			"history written in the OLD charset, and this source records no per-event charset to decode it by. Every row of "+
			"this table the stream emitted since it loaded that shape was decoded by the new charset, and may already be on the "+
			"target. Refusing. Re-snapshot the table (sync --restart-from-scratch), which re-copies those rows. (If this DDL was a no-op — it set a column to the charset it already had — "+
			"this is a false positive, but the stream cannot tell the two apart.)",
			schema, table, strings.Join(cols, ", "), to),
	)
}

// binlogCharsetDDLGuard runs on a binlog DDL QueryEvent, BEFORE the schema
// cache is cleared: it compares a charset-naming ALTER against the cached
// shape of its table, for a table this reader has been decoding without a
// written charset.
func (r *CDCReader) binlogCharsetDDLGuard(stmt, defaultSchema string) error {
	c, ok := parseCharsetDDL(stmt)
	if !ok {
		return nil
	}
	schema := c.schema
	if schema == "" {
		schema = defaultSchema
	}
	for _, tbl := range r.schemaCache {
		if !tbl.decodedWithoutWrittenCharset || !strings.EqualFold(tbl.Name, c.table) || !strings.EqualFold(tbl.Schema, schema) {
			continue
		}
		shape := map[string]string{}
		for _, col := range tbl.Columns {
			if cs, isString := stringTypeCharset(col.Type); isString && cs != "" {
				shape[strings.ToLower(col.Name)] = canonicalCharsetName(cs)
			}
		}
		if applied, cols := c.alreadyApplied(shape); applied {
			return errCharsetDDLAlreadyApplied(tbl.Schema, tbl.Name, c, cols)
		}
	}
	return nil
}

// noteDecodedWithoutWrittenCharset records, for the guard, that a table's
// rows were decoded with no written-charset evidence, and WARNs once per
// table when it has non-UTF-8 text columns (the ones a replay across a
// charset DDL can misdecode).
func (r *CDCReader) noteDecodedWithoutWrittenCharset(tbl *tableSchema) {
	if tbl.decodedWithoutWrittenCharset {
		return
	}
	tbl.decodedWithoutWrittenCharset = true
	var nonUTF8 []string
	for _, col := range tbl.Columns {
		if cs, isString := stringTypeCharset(col.Type); isString && lookupColumnCharset(cs) != nil {
			nonUTF8 = append(nonUTF8, col.Name+" ("+cs+")")
		}
	}
	if len(nonUTF8) == 0 || r.charsetUnrecordedWarned[tbl.Schema+"."+tbl.Name] {
		return
	}
	if r.charsetUnrecordedWarned == nil {
		r.charsetUnrecordedWarned = map[string]bool{}
	}
	r.charsetUnrecordedWarned[tbl.Schema+"."+tbl.Name] = true
	slog.Warn(charsetHistoryUnrecordedMarker+": this source records no written charset in its binlog TABLE_MAP events "+
		"(MariaDB binlog_row_metadata=NO_LOG), so these non-UTF-8 columns are decoded by their CURRENT charset. A charset "+
		"DDL replayed by this stream is caught only when it names the charset explicitly; set binlog_row_metadata=MINIMAL "+
		"on the source to record it for history written after the change",
		"table", tbl.Schema+"."+tbl.Name, "columns", strings.Join(nonUTF8, ", "))
}

// vstreamCharsetDDLGuard is the VStream twin: fields is the reader's FIELD
// cache (keys `shard/keyspace.table`), consulted BEFORE the DDL invalidates
// it. VStream records no written charset at all, so every table is guarded.
func vstreamCharsetDDLGuard(stmt, keyspace string, fields map[string][]*query.Field) error {
	c, ok := parseCharsetDDL(stmt)
	if !ok {
		return nil
	}
	for key, fs := range fields {
		name := key
		if _, after, found := strings.Cut(name, "/"); found {
			name = after
		}
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		if !strings.EqualFold(name, c.table) {
			continue
		}
		shape := map[string]string{}
		for _, f := range fs {
			if f.GetCharset() == 0 || !isVStreamTextField(f) {
				continue
			}
			if cs := collationEnv().LookupCharsetName(collations.ID(f.GetCharset())); cs != "" {
				shape[strings.ToLower(f.GetName())] = canonicalCharsetName(cs)
			}
		}
		if applied, cols := c.alreadyApplied(shape); applied {
			return errCharsetDDLAlreadyApplied(keyspace, name, c, cols)
		}
	}
	return nil
}

// isVStreamTextField reports whether a VStream field is character data.
func isVStreamTextField(f *query.Field) bool {
	switch f.GetType() {
	case query.Type_VARCHAR, query.Type_CHAR, query.Type_TEXT, query.Type_ENUM, query.Type_SET:
		return true
	}
	return false
}

// warnVStreamCharsetUnrecorded logs CHARSET-HISTORY-UNRECORDED once per
// table whose FIELD event carries a non-UTF-8 text column.
func warnVStreamCharsetUnrecorded(warned map[string]bool, table string, fs []*query.Field) {
	if warned[table] {
		return
	}
	var nonUTF8 []string
	for _, f := range fs {
		if !isVStreamTextField(f) {
			continue
		}
		if cc := lookupCollationCharset(f.GetCharset()); cc != nil {
			nonUTF8 = append(nonUTF8, f.GetName()+" ("+cc.name+")")
		}
	}
	if len(nonUTF8) == 0 {
		return
	}
	warned[table] = true
	slog.Warn(charsetHistoryUnrecordedMarker+": PlanetScale/Vitess reports a replayed row's column collation as it is NOW, "+
		"so these non-UTF-8 columns are decoded by their CURRENT charset. A charset DDL this stream replays is caught only "+
		"when it names the charset explicitly; re-snapshot a table after changing its charset while a stream is behind it",
		"table", table, "columns", strings.Join(nonUTF8, ", "))
}
