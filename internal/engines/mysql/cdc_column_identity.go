// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"sluicesync.dev/sluice/internal/ir"
)

// # A column name that now holds another column (RENAME-SWAP-INVISIBLE)
//
// Every schema comparison sluice makes downstream pairs columns BY NAME,
// and a MySQL column has no stable id. So a change that ends with a name
// the table already had, holding a different column, is invisible to all of
// them whenever the declared names, order and types come out the same — or
// come out looking like some other change. Measured on MySQL 8.4 (both
// binlog_row_metadata settings, live at the head and on a warm resume), each
// at exit 0 with the target's existing rows under the wrong names or
// holding values the source no longer has:
//
//   - `RENAME COLUMN b TO c, RENAME COLUMN c TO b` then `MODIFY b … AFTER id`
//     before the table's next row: no boundary at all;
//   - `DROP COLUMN c, ADD COLUMN c` of the same type: no boundary;
//   - `RENAME COLUMN c TO tmp, ADD COLUMN c`: an ADD of tmp;
//   - `DROP COLUMN b, RENAME COLUMN c TO b`: a DROP of c.
//
// No shape sluice can read tells these apart from the harmless changes they
// resemble; FULL metadata's per-event names do not either (they name the
// same columns before and after). The DDL text does, and the reader has it:
// every QUERY event reaches the generic-DDL arm in binlog order, at the
// reader's own position, whatever the metadata and however far behind the
// head the reader is. So the reader keeps, per table, a ledger of what the
// ALTERs since the table's last decoded shape did to its columns' identity
// ([columnIdentityLedger]) and refuses when a name that existed before now
// holds something else. It refuses AT THE DDL EVENT ([CDCReader.noteColumnIdentityDDL]),
// not at the table's next row: the target's existing rows are wrong from
// the moment the source executes the statement, and a restart between the
// two would otherwise start past the DDL with an empty ledger — any commit
// of another table (or the heartbeat) persists a position there, so a
// routine restart is enough (review F1, measured on every shape). A refused
// DDL is never crossed, so a restart re-reads it and refuses again. The
// table's next shape — its next row, or the rotation boundary's settle — is
// judged once more ([CDCReader.judgeColumnIdentity]), for the pure
// permutation below, which only becomes refusable once a column is moved.
//
// One shape is left to the downstream doors: a pure permutation (a rename
// swap or rotation among names that all survive) with no column moved by
// FIRST/AFTER. Its names move over fixed values, so the declared order
// flips and the boundary refuses COLUMN-ORDER-CHANGED unless the operator
// has applied it on the target (pipeline/column_order_boundary.go) —
// refusing it here would take that remedy away, because a restart replays
// the same DDL into the same ledger. Anything else that changed a name's
// column refuses here, with the re-copy remedy: nothing on the target can
// prove the operator mirrored it. The cost of refusing at the DDL: a swap
// spelled as three renames through a temporary name (`b TO tmp; c TO b;
// tmp TO c`) refuses at its second statement, where it is
// indistinguishable from `DROP b; RENAME c TO b`, with the re-copy remedy
// instead of the order doors' apply-the-renames one.
//
// MariaDB's `IF [NOT] EXISTS` on a column clause is outside the parser's
// grammar; it is removed and the statement re-parsed
// ([stripColumnIfExists]), reading each clause as having taken effect — a
// no-op `DROP COLUMN IF EXISTS c` then `ADD COLUMN c` refuses too (loud).
// A statement that still cannot be parsed ([CDCReader.unreadIdentityDDL])
// refuses when it names an in-scope table and either spells DROP, CHANGE
// or RENAME outside quotes, or follows a statement that already vacated a
// name of that table; otherwise it is logged at WARN as
// COLUMN-IDENTITY-UNREAD, so a routine `ADD COLUMN u UUID` keeps flowing.
//
// The expected value is independent of the thing checked: the server's own
// record of the statements it executed, not a comparison of shapes.
//
// # What it does not see
//
//   - A pure permutation, a persisted position past it, a restart, then a
//     `MODIFY … AFTER` restoring the old order: the permutation is not
//     refusable at its own DDL (the order doors judge it), and the fresh
//     ledger after the restart sees only a reorder. The ledger is
//     in-process, like the owed boundary it sits beside
//     (cdc_owed_schema_boundary.go; GC-44's class).
//   - An unparsed statement that names no table, or names one the prefix
//     reader cannot bound, is logged, not refused.
//   - Table identity: `RENAME TABLE a TO tmp, b TO a, tmp TO b` and a DROP
//     plus CREATE of the same name are the same defect one level up; the
//     ledger follows a table across a rename but does not judge it.

// columnOrigin is what a column name holds now, relative to the table's
// last decoded shape: the column that was called name then, or a column
// added since (fresh).
type columnOrigin struct {
	name  string
	fresh bool
}

// columnIdentityLedger is one table's column-identity changes since its last
// decoded shape. Names are lower-cased: MySQL column names are
// case-insensitive.
type columnIdentityLedger struct {
	// now maps each name a statement assigned to what it holds. A name
	// absent from it, and not vacated, still holds its own column.
	now map[string]columnOrigin
	// vacated are names whose own column was dropped or renamed away; a
	// name re-assigned after that is in now as well.
	vacated map[string]bool
	// existed are the names vacated while they still held their own
	// column — so they were names of the last decoded shape.
	existed map[string]bool
	// moved: a FIRST/AFTER clause repositioned a column.
	moved bool
	// first is the first statement recorded, for the refusal.
	first string
}

func (l *columnIdentityLedger) origin(name string) columnOrigin {
	if o, ok := l.now[name]; ok {
		return o
	}
	return columnOrigin{name: name}
}

// columnIdentityEdit is what one ALTER TABLE does to column identities.
// MySQL resolves every clause's old name against the table as it was
// before the statement, so the renames and drops apply simultaneously and
// the adds after them.
type columnIdentityEdit struct {
	renames [][2]string
	drops   []string
	adds    []string
	moved   bool
}

func (e columnIdentityEdit) empty() bool {
	return len(e.renames) == 0 && len(e.drops) == 0 && len(e.adds) == 0 && !e.moved
}

// apply records e on the ledger.
func (l *columnIdentityLedger) apply(e columnIdentityEdit, stmt string) {
	if e.empty() {
		return
	}
	if l.now == nil {
		l.now, l.vacated, l.existed = map[string]columnOrigin{}, map[string]bool{}, map[string]bool{}
	}
	if l.first == "" {
		l.first = stmt
	}
	l.moved = l.moved || e.moved
	before := make(map[string]columnOrigin, len(e.renames))
	for _, r := range e.renames {
		before[r[0]] = l.origin(r[0])
	}
	vacate := func(name string) {
		if _, assigned := l.now[name]; !assigned && !l.vacated[name] {
			l.existed[name] = true
		}
		delete(l.now, name)
		l.vacated[name] = true
	}
	for _, r := range e.renames {
		vacate(r[0])
	}
	for _, d := range e.drops {
		vacate(d)
	}
	for _, r := range e.renames {
		l.now[r[1]] = before[r[0]]
	}
	for _, a := range e.adds {
		l.now[a] = columnOrigin{name: a, fresh: true}
	}
}

// changed returns the names of the last decoded shape that now hold another
// column, sorted, and whether the ledger is a pure permutation the
// downstream column-order doors judge: every changed name holds another
// changed name's column, and no column was moved.
func (l *columnIdentityLedger) changed() (names []string, permutation bool) {
	for n := range l.existed {
		o, ok := l.now[n]
		if !ok || o == (columnOrigin{name: n}) {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, false
	}
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	permutation = !l.moved
	for _, n := range names {
		if o := l.now[n]; o.fresh || !set[o.name] {
			permutation = false
		}
	}
	return names, permutation
}

// describe renders what each changed name holds now.
func (l *columnIdentityLedger) describe(names []string) string {
	parts := make([]string, 0, len(names))
	for _, n := range names {
		o := l.now[n]
		if o.fresh {
			parts = append(parts, fmt.Sprintf("%q is a new column (its old column was dropped or renamed away)", n))
		} else {
			parts = append(parts, fmt.Sprintf("%q now holds the column that was %q", n, o.name))
		}
	}
	return strings.Join(parts, "; ")
}

// columnIdentityTables are the per-table ledgers, keyed by lower-cased
// qualified name (a DDL may spell a table differently from its TABLE_MAP
// under lower_case_table_names; over-matching two tables differing only in
// case costs a refusal, never a miss). Pump goroutine only.
type columnIdentityTables map[string]*columnIdentityLedger

func identityKey(schema, table string) string {
	return strings.ToLower(qualifiedName(schema, table))
}

// noteColumnIdentityDDL records what a DDL statement does to column
// identities, and refuses when the statement leaves an in-scope table with
// a name holding another column (other than a pure permutation the order
// doors judge). Called by the generic-DDL arm for every in-scope DDL; a
// refusal stops the stream before the DDL is crossed.
func (r *CDCReader) noteColumnIdentityDDL(ctx context.Context, stmt, defaultSchema string) error {
	parsed, ok := parseIdentityDDL(stmt)
	if !ok {
		return r.unreadIdentityDDL(ctx, stmt, defaultSchema)
	}
	if r.columnIdentity == nil {
		r.columnIdentity = columnIdentityTables{}
	}
	ref := func(t sqlparser.TableName) ddlTableRef {
		schema := t.Qualifier.String()
		if schema == "" {
			schema = defaultSchema
		}
		return ddlTableRef{schema: schema, table: t.Name.String()}
	}
	key := func(t ddlTableRef) string { return identityKey(t.schema, t.table) }
	move := func(from, to ddlTableRef) {
		l := r.columnIdentity[key(from)]
		delete(r.columnIdentity, key(from))
		if l != nil {
			r.columnIdentity[key(to)] = l
		} else {
			delete(r.columnIdentity, key(to))
		}
	}
	var touched []ddlTableRef
	switch s := parsed.(type) {
	case *sqlparser.AlterTable:
		t := ref(s.Table)
		l := r.columnIdentity[key(t)]
		if l == nil {
			l = &columnIdentityLedger{}
		}
		l.apply(columnIdentityEditOf(s), stmt)
		if l.now != nil {
			r.columnIdentity[key(t)] = l
		}
		touched = append(touched, t)
		for _, opt := range s.AlterOptions {
			if rn, ok := opt.(*sqlparser.RenameTableName); ok {
				move(t, ref(rn.Table))
				t = ref(rn.Table)
				touched = append(touched, t)
			}
		}
	case *sqlparser.RenameTable:
		// Pairs apply in order (`a TO tmp, b TO a, tmp TO b`).
		for _, p := range s.TablePairs {
			move(ref(p.FromTable), ref(p.ToTable))
			touched = append(touched, ref(p.ToTable))
		}
	case *sqlparser.DropTable:
		// A temporary table shadows the base table without touching it.
		if !s.Temp {
			for _, t := range s.FromTables {
				delete(r.columnIdentity, key(ref(t)))
			}
		}
	case *sqlparser.CreateTable:
		// CREATE TABLE IF NOT EXISTS of an existing table is a no-op the
		// server still binlogs (ORMs issue it on every start), and a
		// temporary table leaves the base table alone: neither is a new
		// table, so neither resets its ledger (review F2).
		if !s.IfNotExists && !s.Temp {
			delete(r.columnIdentity, key(ref(s.Table)))
		}
	}
	for _, t := range touched {
		if err := r.refuseIdentityChangeAt(t); err != nil {
			return err
		}
	}
	return nil
}

// identityInScope reports whether a table a DDL names is one this stream
// replicates: its database is in scope and, when the pipeline wired a
// table filter, the table passes it. The names are the statement's own
// spelling, which may differ in case from the stored one the filter was
// given: a mismatch makes a table look out of scope here, and its next row
// is still judged by the row path, which uses the stored name.
func (r *CDCReader) identityInScope(t ddlTableRef) bool {
	if !r.databaseInScope(t.schema) {
		return false
	}
	return r.scopeAllowed == nil || r.scopeAllowed(t.schema, t.table)
}

// refuseIdentityChangeAt refuses when t's ledger now holds a change that is
// not a pure permutation. The ledger is kept, not settled: the refused DDL
// is not crossed, so a restart replays it into a fresh ledger and refuses
// again.
func (r *CDCReader) refuseIdentityChangeAt(t ddlTableRef) error {
	if !r.identityInScope(t) {
		return nil
	}
	l := r.columnIdentity[identityKey(t.schema, t.table)]
	if l == nil {
		return nil
	}
	names, permutation := l.changed()
	if len(names) == 0 || permutation {
		return nil
	}
	return columnIdentityRefusal(qualifiedName(t.schema, t.table), l.describe(names), l.first)
}

// unreadIdentityDDL handles a statement [parseIdentityDDL] cannot read
// (review F3). It refuses when the statement's prefix names an in-scope
// table and either spells DROP, CHANGE or RENAME outside quotes — it may
// have vacated a name — or the table's ledger already holds a vacated name
// the statement may have re-occupied. Anything else is logged and flows: a
// routine MariaDB `ADD COLUMN u UUID` changes no column's identity.
func (r *CDCReader) unreadIdentityDDL(ctx context.Context, stmt, defaultSchema string) error {
	if reach, ok := classifyUnparsedDDL(stripLeadingSQLComments(stripLeadingSessionAssignments(stripLeadingSQLComments(stmt))), defaultSchema); ok && !reach.neutral {
		vacating := sqlWordsInclude(stmt, "DROP", "CHANGE", "RENAME")
		for _, t := range reach.tables {
			if !r.identityInScope(t) {
				continue
			}
			l := r.columnIdentity[identityKey(t.schema, t.table)]
			if vacating || (l != nil && len(l.vacated) > 0) {
				return fmt.Errorf("mysql: cdc: %s: %w (COLUMN-IDENTITY-UNREAD): sluice could not parse the schema change "+
					"%q, which may drop, rename or re-add one of the table's columns under a name the table already had. "+
					"Refusing before it is crossed. Re-copy the stream: `sluice sync start --reset-target-data --yes` "+
					"with the stream's other flags unchanged for a sync, or a fresh `backup full` for a backup chain",
					qualifiedName(t.schema, t.table), ir.ErrColumnIdentityChanged, abbreviateStatement(stmt))
			}
		}
	}
	if reach := classifyDDLReach(stmt, defaultSchema); !reach.neutral {
		slog.WarnContext(ctx, "mysql: cdc: COLUMN-IDENTITY-UNREAD: a schema change could not be parsed; it neither "+
			"spells DROP, CHANGE or RENAME nor follows one on its table, so it is taken as changing no column's identity",
			slog.String("statement", abbreviateStatement(stmt)))
	}
	return nil
}

// parseIdentityDDL parses a DDL statement the way [classifyDDLReach] does,
// retrying once with MariaDB's column-clause `IF [NOT] EXISTS` removed
// ([stripColumnIfExists]). ok is false for a statement it cannot fully read.
func parseIdentityDDL(stmt string) (sqlparser.Statement, bool) {
	if parsed, ok := parseIdentityDDLOnce(stmt); ok {
		return parsed, true
	}
	if stripped, changed := stripColumnIfExists(stmt); changed {
		return parseIdentityDDLOnce(stripped)
	}
	return nil, false
}

func parseIdentityDDLOnce(stmt string) (sqlparser.Statement, bool) {
	stmt = stripLeadingSQLComments(stmt)
	p := charsetDDLParser()
	if stmt == "" || p == nil {
		return nil, false
	}
	executable := strings.Contains(stmt, "/*!") || strings.Contains(stmt, "/*+")
	if norm, _, _, isAlter := normalizeAlterPrefix(stmt); isAlter && !executable {
		stmt = norm
	}
	parsed, err := p.ParseStrictDDL(stmt)
	if err != nil {
		return nil, false
	}
	if a, isAlter := parsed.(*sqlparser.AlterTable); isAlter && !a.FullyParsed {
		return nil, false
	}
	return parsed, true
}

// sqlWord is one token of a statement: an unquoted word (a keyword or a
// bare identifier) with its byte span, or a marker that something else —
// punctuation, a quoted identifier or a string literal — stood there.
type sqlWord struct {
	text       string
	start, end int
	word       bool
}

// sqlWords tokenises a whole statement. String literals and backtick
// identifiers are opaque (never words); plain comments are skipped; the
// content of a comment the server may EXECUTE (`/*!…*/`, `/*+…*/`) is
// tokenised as code, and executable reports that one was seen.
func sqlWords(stmt string) (out []sqlWord, executable bool) {
	for i := 0; i < len(stmt); {
		c := stmt[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			i++
		case strings.HasPrefix(stmt[i:], "/*!") || strings.HasPrefix(stmt[i:], "/*+"):
			executable = true
			i += 3
			for i < len(stmt) && stmt[i] >= '0' && stmt[i] <= '9' {
				i++
			}
		case strings.HasPrefix(stmt[i:], "*/"):
			i += 2
		case strings.HasPrefix(stmt[i:], "/*"):
			end := strings.Index(stmt[i+2:], "*/")
			if end < 0 {
				return out, executable
			}
			i += end + 4
		case c == '#' || strings.HasPrefix(stmt[i:], "-- "):
			end := strings.IndexByte(stmt[i:], '\n')
			if end < 0 {
				return out, executable
			}
			i += end + 1
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < len(stmt) {
				if stmt[j] == '\\' && c != '`' {
					j += 2
					continue
				}
				if stmt[j] == c {
					if j+1 < len(stmt) && stmt[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			out = append(out, sqlWord{start: i, end: min(j+1, len(stmt))})
			i = j + 1
		case isSQLWordByte(c):
			j := i
			for j < len(stmt) && isSQLWordByte(stmt[j]) {
				j++
			}
			out = append(out, sqlWord{text: stmt[i:j], start: i, end: j, word: true})
			i = j
		default:
			out = append(out, sqlWord{start: i, end: i + 1})
			i++
		}
	}
	return out, executable
}

func isSQLWordByte(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// is reports whether w is the unquoted keyword kw.
func (w sqlWord) is(kw string) bool { return w.word && strings.EqualFold(w.text, kw) }

// sqlWordsInclude reports whether stmt spells any of kws as an unquoted word.
func sqlWordsInclude(stmt string, kws ...string) bool {
	words, _ := sqlWords(stmt)
	for _, w := range words {
		for _, kw := range kws {
			if w.is(kw) {
				return true
			}
		}
	}
	return false
}

// stripColumnIfExists removes MariaDB's `IF [NOT] EXISTS` from the column
// clauses of an ALTER TABLE — after `ADD [COLUMN]`, `DROP [COLUMN]`,
// `CHANGE [COLUMN]`, `MODIFY [COLUMN]` and `RENAME COLUMN` — which the
// parser's grammar lacks. It never edits inside a quoted identifier or a
// string, and leaves a statement carrying an executable comment unchanged
// (its clauses cannot be located by position). changed is false when it
// removed nothing.
func stripColumnIfExists(stmt string) (out string, changed bool) {
	words, executable := sqlWords(stmt)
	if executable || len(words) == 0 || !words[0].is("ALTER") {
		return stmt, false
	}
	var cuts [][2]int
	for i := 0; i < len(words); i++ {
		w := words[i]
		renaming := w.is("RENAME")
		if !w.is("ADD") && !w.is("DROP") && !w.is("CHANGE") && !w.is("MODIFY") && !renaming {
			continue
		}
		j := i + 1
		if j < len(words) && words[j].is("COLUMN") {
			j++
		} else if renaming {
			continue
		}
		k := j
		if k < len(words) && words[k].is("IF") {
			k++
			if k < len(words) && words[k].is("NOT") {
				k++
			}
			if k < len(words) && words[k].is("EXISTS") {
				// Through the whitespace after EXISTS, up to the next token.
				end := words[k].end
				if k+1 < len(words) {
					end = words[k+1].start
				}
				cuts = append(cuts, [2]int{words[j].start, end})
				i = k
			}
		}
	}
	if len(cuts) == 0 {
		return stmt, false
	}
	var b strings.Builder
	prev := 0
	for _, c := range cuts {
		b.WriteString(stmt[prev:c[0]])
		prev = c[1]
	}
	b.WriteString(stmt[prev:])
	return b.String(), true
}

// columnIdentityEditOf extracts an ALTER TABLE's column-identity clauses.
func columnIdentityEditOf(s *sqlparser.AlterTable) columnIdentityEdit {
	var e columnIdentityEdit
	for _, opt := range s.AlterOptions {
		switch o := opt.(type) {
		case *sqlparser.RenameColumn:
			if o.OldName != nil && o.NewName != nil && !o.OldName.Name.Equal(o.NewName.Name) {
				e.renames = append(e.renames, [2]string{o.OldName.Name.Lowered(), o.NewName.Name.Lowered()})
			}
		case *sqlparser.ChangeColumn:
			if o.OldColumn != nil && o.NewColDefinition != nil && !o.OldColumn.Name.Equal(o.NewColDefinition.Name) {
				e.renames = append(e.renames, [2]string{o.OldColumn.Name.Lowered(), o.NewColDefinition.Name.Lowered()})
			}
			e.moved = e.moved || o.First || o.After != nil
		case *sqlparser.ModifyColumn:
			e.moved = e.moved || o.First || o.After != nil
		case *sqlparser.DropColumn:
			if o.Name != nil {
				e.drops = append(e.drops, o.Name.Name.Lowered())
			}
		case *sqlparser.AddColumns:
			for _, c := range o.Columns {
				if c != nil {
					e.adds = append(e.adds, c.Name.Lowered())
				}
			}
		}
	}
	return e
}

// judgeColumnIdentity refuses when the ALTERs since qn's last decoded shape
// left a name of that shape holding another column, unless the change is a
// pure permutation the column-order doors downstream judge. Either way the
// ledger is then settled: the shape about to be used is the new reference.
// Called before a table's shape is used — its next row, or the rotation
// boundary's settle — and only for in-scope tables.
func (r *CDCReader) judgeColumnIdentity(qn string) error {
	key := strings.ToLower(qn)
	l := r.columnIdentity[key]
	if l == nil {
		return nil
	}
	names, permutation := l.changed()
	if len(names) == 0 || permutation {
		delete(r.columnIdentity, key)
		return nil
	}
	return columnIdentityRefusal(qn, l.describe(names), l.first)
}

// columnIdentityRefusal is the COLUMN-IDENTITY-CHANGED refusal.
func columnIdentityRefusal(qn, what, first string) error {
	return fmt.Errorf("mysql: cdc: %s: %w: since the last shape this stream decoded the table under, %s (from %q). "+
		"A name that kept its place and type but now holds another column cannot be told apart, by any comparison "+
		"of shapes, from one that did not, and the target's existing rows under it would keep the old column's "+
		"values. Refusing before any row of the new shape is applied. Re-copy the stream: `sluice sync start "+
		"--reset-target-data --yes` with the stream's other flags unchanged for a sync, or a fresh `backup full` "+
		"for a backup chain",
		qn, ir.ErrColumnIdentityChanged, what, abbreviateStatement(first))
}

// abbreviateStatement collapses whitespace and caps a statement's length
// for a log line or a refusal.
func abbreviateStatement(stmt string) string {
	s := strings.Join(strings.Fields(stmt), " ")
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
