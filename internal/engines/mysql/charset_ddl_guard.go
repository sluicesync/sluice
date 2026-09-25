// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// The charset-DDL replay guard (GC-37 (j) re-reviews).
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
// v0.156.2 emitted 'é' (its raw passthrough happened to be right), an
// unguarded decode 'Ã©' at exit 0; latin1 '€' after a change to cp1251 —
// v0.156.2 emitted raw 0x80 (loud at the target), an unguarded decode 'Ђ'
// silently.
//
// # Scope: only a change INTO a non-UTF-8 charset
//
// A replay across a change into utf8/utf8mb3/utf8mb4 (or ascii/binary)
// decodes the old bytes by passthrough — exactly what v0.156.2 did for every
// charset: invalid UTF-8 stays loud, bytes that happen to be valid UTF-8 stay
// silent. That is never worse than v0.156.2, and guarding it cost live
// streams a refusal on the most routine DDL there is (a third review
// MEASURED `CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci` on
// a pure-utf8mb4 table refusing a LIVE stream on MariaDB NO_LOG and VStream,
// where v0.156.2 refused nothing). So those targets are not classified.
//
// # How the guard sees a replay, without a clock
//
// The DDL arrives on the stream after the rows it postdates. When an ALTER
// sets a non-UTF-8 charset — `CONVERT TO CHARACTER SET x [COLLATE y]`, or
// `MODIFY`/`CHANGE` of a column `… CHARACTER SET x` and/or `… COLLATE y` —
// the reader compares the charset AND collation it sets (an explicit
// COLLATE, else the charset's default — from the source server's own
// collation table on the binlog) with the shape it has been decoding the
// table by. A live stream's shape predates the DDL; a replaying stream's
// shape was loaded after it and already carries the DDL's charset and
// collation for every column named. That refuses with the CDC-4 replay-
// mismatch class. Comparing the collation too is what lets a live
// collation-only change (`MODIFY v … COLLATE latin1_bin` on a latin1
// column) be recognised as live.
//
// Classification is by the Vitess SQL parser in STRICT mode (a partial parse
// is a failure, not an empty ALTER), after [normalizeAlterPrefix] removes
// the MariaDB-only prefix syntax the parser does not know. A statement that
// still does not parse, or names a collation nothing can name, but mentions
// a charset or collation and targets a guarded table whose shape has
// non-UTF-8 columns, REFUSES ([errCharsetDDLUnclassified]) — a fail-safe
// chosen over a WARN because the guard exists for exactly that lane, and a
// silent miss there is data loss while a false refusal costs a re-snapshot.
//
// # What it cannot see — the windows that remain, stated
//
//   - It fires AFTER the fact: the rows before the DDL event have already
//     been emitted, and may have been applied or committed to a backup
//     window (MEASURED for `backup stream` rollovers). The refusal says so,
//     and its remedy re-copies them; the backup lanes re-hint it to a fresh
//     full backup (pipeline.backupCaptureReaderErr).
//   - It sees only what THIS reader session decoded. A session that ends
//     between the replayed rows and the DDL — a sync restart, every
//     `backup incremental` run boundary — hands the next session a table
//     with no decoded rows, which reaches the DDL with nothing to compare
//     (MEASURED with two incremental runs on MariaDB NO_LOG). The
//     principled fix compares against the schema persisted for the resume
//     position; it is filed (audit backlog GC-37 (p)).
//   - A replay into a UTF-8 charset is carried as v0.156.2 carried it (see
//     Scope).
//   - A `MODIFY` with no charset or collation clause still resets a column
//     to the table default; the guard cannot see that change.
//   - An online schema change (gh-ost, pt-online-schema-change, PlanetScale
//     deploy requests / Vitess online DDL) alters a shadow table and swaps
//     it in with a RENAME: no charset ALTER on T ever reaches the stream.
//   - A statement that mentions no charset keyword the fail-safe looks for.
//   - A replayed ALTER that set a column to the charset AND collation it
//     already had (a no-op, or a restated ORM migration) looks exactly like
//     a replay, and refuses — live or replayed. A loud false positive, stated
//     in the refusal; restricted to non-UTF-8 targets.
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
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// charsetHistoryUnrecordedMarker is the grep-stable marker on the WARN a
// reader logs, once per table, when it decodes a table with non-UTF-8 text
// columns and the source states no written charset for it.
const charsetHistoryUnrecordedMarker = "CHARSET-HISTORY-UNRECORDED"

// charsetSpec is a character set and collation, lower-cased. In a DDL an
// empty collation means "the charset's default"; in a shape it means the
// catalog did not say.
type charsetSpec struct{ charset, collation string }

func (s charsetSpec) String() string {
	if s.collation == "" {
		return s.charset
	}
	return s.charset + "/" + s.collation
}

// charsetDDLChange is what one ALTER TABLE sets, for non-UTF-8 targets.
type charsetDDLChange struct {
	schema, table string
	// all is what CONVERT TO sets every character column to; nil without one.
	all *charsetSpec
	// cols maps a (lower-cased, post-DDL) column name to what the statement
	// sets it to explicitly.
	cols map[string]charsetSpec
}

// charsetDDLVerdict is what [classifyCharsetDDL] made of a statement.
type charsetDDLVerdict int

const (
	// ddlNotCharset: not an ALTER setting a non-UTF-8 charset (including
	// one that sets only UTF-8 targets, or only a table default).
	ddlNotCharset charsetDDLVerdict = iota
	// ddlCharset: classified; the change is in the result.
	ddlCharset
	// ddlUnclassified: an ALTER on a known table that mentions a charset or
	// collation but could not be parsed, or names a collation nothing can
	// name. The fail-safe decides.
	ddlUnclassified
)

// collationNamer names collations for the guard: which charset a collation
// belongs to, and a charset's default collation. Both return "" for a name
// they do not know.
type collationNamer struct {
	charsetOf func(collation string) string
	defaultOf func(charset string) string
}

// vitessCollationNamer names by Vitess's MySQL 8 environment — what vttablet
// runs, so the VStream guard's namer.
func vitessCollationNamer() collationNamer {
	return collationNamer{charsetOf: charsetForCollationName, defaultOf: defaultCollationForCharset}
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

// defaultCollationForCharset names a charset's default collation in Vitess's
// MySQL 8 environment, or "".
func defaultCollationForCharset(charset string) string {
	id := collationEnv().DefaultCollationForCharset(charset)
	if id == collations.Unknown {
		return ""
	}
	return collationEnv().LookupName(id)
}

// unquoteDDLName strips one layer of quotes the parser keeps on a charset or
// collation name (`CHARACTER SET 'latin1'` parses as "'latin1'") and lower-
// cases it.
func unquoteDDLName(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if q := s[0]; (q == '\'' || q == '"' || q == '`') && s[len(s)-1] == q {
			s = s[1 : len(s)-1]
		}
	}
	return strings.ToLower(s)
}

// mentionsCharsetKeyword reports whether a statement the parser could not
// read names a charset or collation anywhere — the fail-safe's trigger. It
// is a keyword presence test that decides only whether to refuse, never what
// a statement means.
func mentionsCharsetKeyword(stmt string) bool {
	u := strings.ToUpper(stmt)
	for _, k := range []string{"CHARACTER SET", "CHARSET", "COLLATE", "CONVERT TO"} {
		if strings.Contains(u, k) {
			return true
		}
	}
	return false
}

// classifyCharsetDDL classifies stmt. The change carries the table (and
// schema, when qualified) for both ddlCharset and ddlUnclassified.
func classifyCharsetDDL(stmt string, n collationNamer) (charsetDDLChange, charsetDDLVerdict) {
	norm, schema, table, isAlter := normalizeAlterPrefix(stmt)
	if !isAlter {
		return charsetDDLChange{}, ddlNotCharset
	}
	unclassified := charsetDDLChange{schema: schema, table: table}
	p := charsetDDLParser()
	if p == nil {
		return unclassified, ddlUnclassified
	}
	parsed, err := p.ParseStrictDDL(norm)
	if err != nil {
		if mentionsCharsetKeyword(stmt) {
			return unclassified, ddlUnclassified
		}
		return charsetDDLChange{}, ddlNotCharset
	}
	alter, ok := parsed.(*sqlparser.AlterTable)
	if !ok {
		return charsetDDLChange{}, ddlNotCharset
	}
	c := charsetDDLChange{
		schema: alter.Table.Qualifier.String(),
		table:  alter.Table.Name.String(),
		cols:   map[string]charsetSpec{},
	}
	unnamed := false
	// spec resolves a charset/collation clause pair; ok is false for a
	// clause that sets nothing sluice guards (none, "default", a UTF-8
	// target) or names a collation nothing can name (unnamed is set).
	spec := func(cs, coll string) (charsetSpec, bool) {
		cs, coll = unquoteDDLName(cs), unquoteDDLName(coll)
		if cs == "" && coll != "" {
			if cs = n.charsetOf(coll); cs == "" {
				unnamed = true
				return charsetSpec{}, false
			}
		}
		cs = canonicalCharsetName(cs)
		if cs == "" || cs == "default" || passthroughCharset(cs) {
			return charsetSpec{}, false
		}
		return charsetSpec{charset: cs, collation: coll}, true
	}
	colSpec := func(def *sqlparser.ColumnDefinition) {
		if def == nil || def.Type == nil {
			return
		}
		coll := ""
		if def.Type.Options != nil {
			coll = def.Type.Options.Collate
		}
		if s, ok := spec(def.Type.Charset.Name, coll); ok {
			c.cols[strings.ToLower(def.Name.String())] = s
		}
	}
	for _, opt := range alter.AlterOptions {
		switch o := opt.(type) {
		case *sqlparser.AlterCharset:
			if s, ok := spec(o.CharacterSet, o.Collate); ok {
				c.all = &s
			}
		case *sqlparser.ModifyColumn:
			colSpec(o.NewColDefinition)
		case *sqlparser.ChangeColumn:
			colSpec(o.NewColDefinition)
		}
	}
	if unnamed {
		return charsetDDLChange{schema: c.schema, table: c.table}, ddlUnclassified
	}
	if c.all == nil && len(c.cols) == 0 {
		return charsetDDLChange{}, ddlNotCharset
	}
	return c, ddlCharset
}

// normalizeAlterPrefix rewrites `ALTER [ONLINE] [IGNORE] TABLE [IF EXISTS]
// name [WAIT n | NOWAIT] rest` — MariaDB syntax the Vitess parser refuses
// (ONLINE, IGNORE, IF EXISTS) or silently truncates the statement at (WAIT,
// NOWAIT; MEASURED) — to `ALTER TABLE name rest`, and returns the table name
// it read. A leading /* … */ comment is skipped. isAlter is false for any
// statement that is not an ALTER TABLE by this reading.
//
// This is a word-level walk of the statement's fixed PREFIX only, to reach a
// form the parser can classify; everything after the table name is handed to
// the parser untouched.
func normalizeAlterPrefix(stmt string) (norm, schema, table string, isAlter bool) {
	s := stmt
	i := 0
	skip := func() {
		for {
			for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
				i++
			}
			if strings.HasPrefix(s[i:], "/*") {
				if end := strings.Index(s[i+2:], "*/"); end >= 0 {
					i += 2 + end + 2
					continue
				}
			}
			return
		}
	}
	word := func() string {
		skip()
		start := i
		for i < len(s) && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' {
			i++
		}
		return s[start:i]
	}
	peek := func() string {
		save := i
		w := word()
		i = save
		return w
	}
	if !strings.EqualFold(word(), "alter") {
		return stmt, "", "", false
	}
	for {
		w := peek()
		if strings.EqualFold(w, "online") || strings.EqualFold(w, "ignore") {
			word()
			continue
		}
		break
	}
	if !strings.EqualFold(word(), "table") {
		return stmt, "", "", false
	}
	if strings.EqualFold(peek(), "if") {
		save := i
		word()
		if !strings.EqualFold(word(), "exists") {
			i = save
		}
	}
	skip()
	nameStart := i
	for i < len(s) {
		if s[i] == '`' {
			i++
			for i < len(s) {
				if s[i] == '`' {
					if i+1 < len(s) && s[i+1] == '`' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' {
			break
		}
		i++
	}
	name := s[nameStart:i]
	if name == "" {
		return stmt, "", "", false
	}
	switch w := peek(); {
	case strings.EqualFold(w, "nowait"):
		word()
	case strings.EqualFold(w, "wait"):
		word()
		word()
	}
	skip()
	schema, table = splitQualifiedName(name)
	return "ALTER TABLE " + name + " " + s[i:], schema, table, true
}

// splitQualifiedName splits `a`.`b`, a.b or b into schema and table,
// unquoting backticks.
func splitQualifiedName(name string) (schema, table string) {
	var parts []string
	var cur strings.Builder
	inQuote := false
	for j := 0; j < len(name); j++ {
		ch := name[j]
		switch {
		case ch == '`' && inQuote && j+1 < len(name) && name[j+1] == '`':
			cur.WriteByte('`')
			j++
		case ch == '`':
			inQuote = !inQuote
		case ch == '.' && !inQuote:
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(ch)
		}
	}
	parts = append(parts, cur.String())
	if len(parts) >= 2 {
		return parts[len(parts)-2], parts[len(parts)-1]
	}
	return "", parts[0]
}

// alreadyApplied reports whether shape — a table's character columns and
// the charset/collation the reader has been decoding each by (lower-cased
// names) — already carries every change c names, and returns the columns it
// compared. A named column absent from shape (a live CHANGE renames it)
// means the shape predates the DDL. The collation compared is the DDL's
// explicit one, else the charset's default by n; where either side's
// collation is unknown, the charset alone decides.
func (c charsetDDLChange) alreadyApplied(shape map[string]charsetSpec, n collationNamer) (applied bool, compared []string) {
	matches := func(got, want charsetSpec) bool {
		if got.charset != want.charset {
			return false
		}
		wantColl := want.collation
		if wantColl == "" && n.defaultOf != nil {
			wantColl = n.defaultOf(want.charset)
		}
		return wantColl == "" || got.collation == "" || got.collation == wantColl
	}
	if c.all != nil {
		if len(shape) == 0 {
			return false, nil
		}
		for col, got := range shape {
			if !matches(got, *c.all) {
				return false, nil
			}
			compared = append(compared, col)
		}
	}
	for col, want := range c.cols {
		got, ok := shape[col]
		if !ok || !matches(got, want) {
			return false, nil
		}
		compared = append(compared, col)
	}
	sort.Strings(compared)
	return len(compared) > 0, compared
}

// shapeHasNonUTF8 reports whether any column of shape is decoded by a
// charset that is not passthrough — the only columns a replay can
// misdecode by this guard's scope.
func shapeHasNonUTF8(shape map[string]charsetSpec) bool {
	for _, s := range shape {
		if !passthroughCharset(s.charset) {
			return true
		}
	}
	return false
}

// charsetReplayRemedy names the re-copy for each surface that reads a MySQL
// change stream. The backup lanes re-hint it (take a fresh full backup);
// the prose names both, since the reader does not know which is running.
const charsetReplayRemedy = "Re-copy the table: under `sync`, re-snapshot it (sync --restart-from-scratch); for a backup chain " +
	"(`backup stream` / `backup incremental`), take a fresh full backup — a window committed before this refusal can hold " +
	"those rows, and no restore of it recovers them."

// errCharsetDDLAlreadyApplied is the guard's refusal.
func errCharsetDDLAlreadyApplied(schema, table string, c charsetDDLChange, cols []string) error {
	to := ""
	if c.all != nil {
		to = c.all.String()
	} else {
		parts := make([]string, 0, len(cols))
		for _, col := range cols {
			parts = append(parts, col+"→"+c.cols[col].String())
		}
		to = strings.Join(parts, ", ")
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCSchemaReplayMismatch,
		"re-snapshot the table (sync --restart-from-scratch); rows of it emitted since the stream loaded its shape were decoded by the post-DDL charset",
		fmt.Errorf("mysql: cdc: %s.%s: the stream reached a DDL that sets the charset of %s (%s), but the shape it has been decoding "+
			"this table's rows by ALREADY carries that charset and collation — it was loaded after the DDL ran, so this stream is "+
			"replaying history written in the OLD charset, and this source records no per-event charset to decode it by. Every "+
			"row of this table the stream emitted since it loaded that shape was decoded by the new charset, and may already be on "+
			"the target. Refusing. %s (If this DDL was a no-op — it set a column to the charset and collation it already had — "+
			"this is a false positive, but the stream cannot tell the two apart.)",
			schema, table, strings.Join(cols, ", "), to, charsetReplayRemedy),
	)
}

// errCharsetDDLUnclassified is the fail-safe's refusal: an ALTER on a guarded
// table with non-UTF-8 columns that mentions a charset or collation, which
// sluice could not classify.
func errCharsetDDLUnclassified(schema, table, stmt string) error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCSchemaReplayMismatch,
		"re-snapshot the table (sync --restart-from-scratch) if the stream was behind this ALTER; otherwise this is a false positive",
		fmt.Errorf("mysql: cdc: %s.%s: the stream reached an ALTER on this table that mentions a character set or collation, "+
			"but sluice could not parse it or could not name its collation, so it cannot tell a live change from a replayed "+
			"one. This table has non-UTF-8 columns decoded by their CURRENT charset, and this source records no per-event "+
			"charset: if the stream was replaying history from before this ALTER, the rows it emitted were decoded by the new "+
			"charset. Refusing rather than risk that silently. %s If the stream was live when the ALTER ran, this is a false "+
			"positive. Statement: %.200q",
			schema, table, charsetReplayRemedy, stmt),
	)
}

// binlogShape is the charset/collation shape of a cached table.
func binlogShape(tbl *tableSchema) map[string]charsetSpec {
	shape := map[string]charsetSpec{}
	for _, col := range tbl.Columns {
		if s, ok := irColumnCharsetSpec(col.Type); ok {
			shape[strings.ToLower(col.Name)] = s
		}
	}
	return shape
}

// irColumnCharsetSpec is an IR string column's declared charset/collation.
func irColumnCharsetSpec(t ir.Type) (charsetSpec, bool) {
	var cs, coll string
	switch v := t.(type) {
	case ir.Char:
		cs, coll = v.Charset, v.Collation
	case ir.Varchar:
		cs, coll = v.Charset, v.Collation
	case ir.Text:
		cs, coll = v.Charset, v.Collation
	default:
		return charsetSpec{}, false
	}
	if cs == "" {
		return charsetSpec{}, false
	}
	return charsetSpec{charset: canonicalCharsetName(cs), collation: strings.ToLower(coll)}, true
}

// binlogCharsetDDLGuard runs on a binlog DDL QueryEvent, BEFORE the schema
// cache is cleared: it compares a charset-setting ALTER against the cached
// shape of its table, for a table this reader has been decoding without a
// written charset.
func (r *CDCReader) binlogCharsetDDLGuard(ctx context.Context, stmt, defaultSchema string) error {
	var guarded []*tableSchema
	for _, tbl := range r.schemaCache {
		if tbl.decodedWithoutWrittenCharset {
			guarded = append(guarded, tbl)
		}
	}
	if len(guarded) == 0 {
		return nil
	}
	n := r.guardCollationNamer(ctx)
	c, verdict := classifyCharsetDDL(stmt, n)
	if verdict == ddlNotCharset {
		return nil
	}
	schema := c.schema
	if schema == "" {
		schema = defaultSchema
	}
	for _, tbl := range guarded {
		if !strings.EqualFold(tbl.Name, c.table) || !strings.EqualFold(tbl.Schema, schema) {
			continue
		}
		shape := binlogShape(tbl)
		if verdict == ddlUnclassified {
			if shapeHasNonUTF8(shape) {
				return errCharsetDDLUnclassified(tbl.Schema, tbl.Name, stmt)
			}
			continue
		}
		if applied, cols := c.alreadyApplied(shape, n); applied {
			return errCharsetDDLAlreadyApplied(tbl.Schema, tbl.Name, c, cols)
		}
	}
	return nil
}

// guardCollationNamer names collations by the source server's own table
// first ([loadServerCollationNames] — it lists MariaDB-only names such as
// latin1_swedish_nopad_ci, and the server's per-charset defaults, which an
// operator can override with character_set_collations), then
// by Vitess's MySQL 8 environment. The table is loaded on first use and a
// failed load is retried per [serverCollationsRetry].
func (r *CDCReader) guardCollationNamer(ctx context.Context) collationNamer {
	if r.serverCollationNames == nil && time.Since(r.serverCollationNamesTried) >= serverCollationsRetry {
		r.serverCollationNamesTried = time.Now()
		lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		r.serverCollationNames, r.serverCollationDefaults = loadServerCollationNames(lctx, r.db)
		cancel()
	}
	names, defaults := r.serverCollationNames, r.serverCollationDefaults
	return collationNamer{
		charsetOf: func(coll string) string {
			if cs := names[strings.ToLower(coll)]; cs != "" {
				return cs
			}
			return charsetForCollationName(coll)
		},
		defaultOf: func(cs string) string {
			if d := defaults[cs]; d != "" {
				return d
			}
			return defaultCollationForCharset(cs)
		},
	}
}

// loadServerCollationNames reads the source server's collation name →
// charset table and each charset's default collation:
// information_schema.COLLATIONS (both engines), MariaDB's
// COLLATION_CHARACTER_SET_APPLICABILITY.FULL_COLLATION_NAME (the only place
// its per-charset uca1400 names are listed), and MariaDB 11.2+'s
// @@global.character_set_collations, which overrides a charset's default
// (MEASURED on the mariadb:11.4 image: "utf8mb4=utf8mb4_uca1400_ai_ci", with
// utf16/ucs2/latin1 left at their COLLATIONS defaults). The two MariaDB-only
// queries fail on MySQL and are skipped. A nil names map means the load
// failed.
//
// UNVERIFIED PREMISE: a replayed ALTER is compared against the GLOBAL
// character_set_collations now; a session that ran the ALTER under a
// different setting is not modelled.
func loadServerCollationNames(ctx context.Context, db *sql.DB) (names, defaults map[string]string) {
	if db == nil {
		return nil, nil
	}
	names, defaults = map[string]string{}, map[string]string{}
	// MariaDB 11.4 lists its charset-independent uca1400 collations with a
	// NULL CHARACTER_SET_NAME; the load failed on 11.4 until those rows were
	// excluded (MEASURED: the MariaDB-only-collation replay pin fell to the
	// fail-safe before, and classified after). They are named per charset by
	// the second query.
	err := scanCollationRows(ctx, db, `SELECT COLLATION_NAME, CHARACTER_SET_NAME, IFNULL(IS_DEFAULT, '')
		FROM information_schema.COLLATIONS WHERE CHARACTER_SET_NAME IS NOT NULL`, func(coll, cs, isDefault string) {
		coll, cs = strings.ToLower(coll), canonicalCharsetName(cs)
		names[coll] = cs
		if strings.EqualFold(isDefault, "yes") && defaults[cs] == "" {
			defaults[cs] = coll
		}
	})
	if err != nil {
		return nil, nil
	}
	// MariaDB only; fails (and is skipped) on MySQL.
	_ = scanCollationRows(ctx, db, `SELECT FULL_COLLATION_NAME, CHARACTER_SET_NAME, ''
		FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY
		WHERE FULL_COLLATION_NAME IS NOT NULL AND CHARACTER_SET_NAME IS NOT NULL`, func(coll, cs, _ string) {
		names[strings.ToLower(coll)] = canonicalCharsetName(cs)
	})
	var overrides sql.NullString
	if db.QueryRowContext(ctx, `SELECT @@global.character_set_collations`).Scan(&overrides) == nil {
		for _, pair := range strings.Split(overrides.String, ",") {
			if cs, coll, ok := strings.Cut(strings.TrimSpace(pair), "="); ok {
				defaults[canonicalCharsetName(cs)] = strings.ToLower(strings.TrimSpace(coll))
			}
		}
	}
	return names, defaults
}

// scanCollationRows runs a three-string-column query and hands each row to
// fn.
func scanCollationRows(ctx context.Context, db *sql.DB, q string, fn func(a, b, c string)) error {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var a, b, c string
		if err := rows.Scan(&a, &b, &c); err != nil {
			return err
		}
		fn(a, b, c)
	}
	return rows.Err()
}

// noteDecodedWithoutWrittenCharset records, for the guard, that a table's
// rows were decoded with no written-charset evidence, and WARNs once per
// table when it has non-UTF-8 text columns — exactly the tables whose
// replays the guard can refuse (a replay into a UTF-8 charset is carried as
// stored bytes, as v0.156.2 did, and is not refused).
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
		"(MariaDB binlog_row_metadata=NO_LOG), so these non-UTF-8 columns are decoded by their CURRENT charset. A replay "+
		"across a change INTO their charset is refused when the stream reaches an ALTER that names it; one done any other "+
		"way (an implicit MODIFY, an online schema change) is not seen. Setting binlog_row_metadata=MINIMAL records the "+
		"charset only for binlog events written after the change",
		"table", tbl.Schema+"."+tbl.Name, "columns", strings.Join(nonUTF8, ", "))
}

// vstreamShape is the charset/collation shape of a VStream FIELD list.
func vstreamShape(fs []*query.Field) map[string]charsetSpec {
	shape := map[string]charsetSpec{}
	for _, f := range fs {
		if f.GetCharset() == 0 || !isVStreamTextField(f) {
			continue
		}
		id := collations.ID(f.GetCharset())
		if cs := collationEnv().LookupCharsetName(id); cs != "" {
			shape[strings.ToLower(f.GetName())] = charsetSpec{
				charset:   canonicalCharsetName(cs),
				collation: strings.ToLower(collationEnv().LookupName(id)),
			}
		}
	}
	return shape
}

// vstreamCharsetDDLGuard is the VStream twin: fields is the reader's FIELD
// cache (keys `shard/keyspace.table`), consulted BEFORE the DDL invalidates
// it. VStream records no written charset at all, so every table is guarded.
func vstreamCharsetDDLGuard(stmt, keyspace string, fields map[string][]*query.Field) error {
	if len(fields) == 0 {
		return nil
	}
	n := vitessCollationNamer()
	c, verdict := classifyCharsetDDL(stmt, n)
	if verdict == ddlNotCharset {
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
		shape := vstreamShape(fs)
		if verdict == ddlUnclassified {
			if shapeHasNonUTF8(shape) {
				return errCharsetDDLUnclassified(keyspace, name, stmt)
			}
			continue
		}
		if applied, cols := c.alreadyApplied(shape, n); applied {
			return errCharsetDDLAlreadyApplied(keyspace, name, c, cols)
		}
	}
	return nil
}

// isVStreamTextField reports whether a VStream field is character data.
// That includes a `_bin`-collated character column, which vttablet types as
// BINARY/VARBINARY/BLOB ([isVStreamBinaryCollatedText]).
func isVStreamTextField(f *query.Field) bool {
	switch f.GetType() {
	case query.Type_VARCHAR, query.Type_CHAR, query.Type_TEXT, query.Type_ENUM, query.Type_SET:
		return true
	}
	return isVStreamBinaryCollatedText(f)
}

// warnVStreamCharsetUnrecorded logs CHARSET-HISTORY-UNRECORDED once per
// table whose FIELD event carries a non-UTF-8 text column — the tables whose
// replays the guard can refuse.
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
		"so these non-UTF-8 columns are decoded by their CURRENT charset. A replay across a change INTO their charset is "+
		"refused when the stream reaches an ALTER that names it; one done any other way (an implicit MODIFY, a deploy "+
		"request or other online schema change) is not seen — re-snapshot a table after such a change while a stream is behind it",
		"table", table, "columns", strings.Join(nonUTF8, ", "))
}
