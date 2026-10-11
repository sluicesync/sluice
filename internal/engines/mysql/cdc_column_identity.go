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
// ([columnIdentityLedger]), and before the table's next shape is used —
// its next row, or the rotation boundary's settle — refuses when a name
// that existed before now holds something else
// ([CDCReader.judgeColumnIdentity]).
//
// One shape is left to the downstream doors: a pure permutation (a rename
// swap or rotation among names that all survive) with no column moved by
// FIRST/AFTER. Its names move over fixed values, so the declared order
// flips and the boundary refuses COLUMN-ORDER-CHANGED unless the operator
// has applied it on the target (pipeline/column_order_boundary.go) —
// refusing it here would take that remedy away, because a restart replays
// the same DDL into the same ledger. Anything else that changed a name's
// column refuses here, with the re-copy remedy: nothing on the target can
// prove the operator mirrored it.
//
// The expected value is independent of the thing checked: the server's own
// record of the statements it executed, not a comparison of shapes.
//
// # What it does not see
//
//   - A statement the parser cannot read (MariaDB's UUID type, `ADD COLUMN
//     IF NOT EXISTS`, ...) is not tracked: refusing every such DDL at the
//     head would stop working MariaDB streams on routine changes. It is
//     logged at WARN under COLUMN-IDENTITY-UNREAD, naming the statement.
//   - The ledger is in-process, like the owed boundary it sits beside
//     (cdc_owed_schema_boundary.go): a position persisted between two
//     statements of one of these shapes, then a restart before the table's
//     next row, starts a fresh ledger past the first statement (GC-44's
//     class).
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
// identities. Called by the generic-DDL arm for every in-scope DDL.
func (r *CDCReader) noteColumnIdentityDDL(ctx context.Context, stmt, defaultSchema string) {
	parsed, ok := parseIdentityDDL(stmt)
	if !ok {
		if reach := classifyDDLReach(stmt, defaultSchema); !reach.neutral {
			slog.WarnContext(ctx, "mysql: cdc: COLUMN-IDENTITY-UNREAD: a schema change could not be parsed, so a "+
				"column it renamed, dropped or re-added under an existing name would not be seen",
				slog.String("statement", abbreviateStatement(stmt)))
		}
		return
	}
	if r.columnIdentity == nil {
		r.columnIdentity = columnIdentityTables{}
	}
	ref := func(t sqlparser.TableName) string {
		schema := t.Qualifier.String()
		if schema == "" {
			schema = defaultSchema
		}
		return identityKey(schema, t.Name.String())
	}
	move := func(from, to string) {
		l := r.columnIdentity[from]
		delete(r.columnIdentity, from)
		if l != nil {
			r.columnIdentity[to] = l
		} else {
			delete(r.columnIdentity, to)
		}
	}
	switch s := parsed.(type) {
	case *sqlparser.AlterTable:
		key := ref(s.Table)
		l := r.columnIdentity[key]
		if l == nil {
			l = &columnIdentityLedger{}
		}
		l.apply(columnIdentityEditOf(s), stmt)
		if l.now != nil {
			r.columnIdentity[key] = l
		}
		for _, opt := range s.AlterOptions {
			if rn, ok := opt.(*sqlparser.RenameTableName); ok {
				move(key, ref(rn.Table))
			}
		}
	case *sqlparser.RenameTable:
		// Pairs apply in order (`a TO tmp, b TO a, tmp TO b`).
		for _, p := range s.TablePairs {
			move(ref(p.FromTable), ref(p.ToTable))
		}
	case *sqlparser.DropTable:
		for _, t := range s.FromTables {
			delete(r.columnIdentity, ref(t))
		}
	case *sqlparser.CreateTable:
		delete(r.columnIdentity, ref(s.Table))
	}
}

// parseIdentityDDL parses a DDL statement the way [classifyDDLReach] does.
// ok is false for a statement it cannot fully read.
func parseIdentityDDL(stmt string) (sqlparser.Statement, bool) {
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
