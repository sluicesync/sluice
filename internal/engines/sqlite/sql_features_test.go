// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// warnRecorder is a slog.Handler that captures WARN-level records so a test
// can assert the per-table / per-index verbatim-carry WARNs fired (ADR-0133).
type warnRecorder struct {
	mu   sync.Mutex
	msgs []string
}

func (h *warnRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *warnRecorder) Handle(_ context.Context, r slog.Record) error {
	if r.Level < slog.LevelWarn {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		sb.WriteString(" ")
		sb.WriteString(a.Key)
		sb.WriteString("=")
		sb.WriteString(a.Value.String())
		return true
	})
	h.mu.Lock()
	h.msgs = append(h.msgs, sb.String())
	h.mu.Unlock()
	return nil
}

func (h *warnRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnRecorder) WithGroup(string) slog.Handler      { return h }

func (h *warnRecorder) verbatimCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.msgs {
		if strings.Contains(m, "carried from SQLite") {
			n++
		}
	}
	return n
}

// captureWarns installs a warn-capturing default slog logger for the test's
// duration and returns the recorder.
func captureWarns(t *testing.T) *warnRecorder {
	t.Helper()
	rec := &warnRecorder{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return rec
}

// readSchemaWithWarns seeds a temp DB, captures WARNs, and returns the schema.
func readSchemaWithWarns(t *testing.T, stmts ...string) (*ir.Schema, *warnRecorder) {
	t.Helper()
	rec := captureWarns(t)
	path := seedDB(t, stmts...)
	eng := Engine{}
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	return schema, rec
}

// TestReadSchema_SchemaFeatures pins the full ADR-0133 carry over a real temp
// file: a VIRTUAL generated column, a STORED generated column, a named
// table-level CHECK, an inline column CHECK, a partial index, and an expression
// index all land in the IR's existing fields tagged "sqlite", the capability
// flags flip true, and the per-table / per-index verbatim WARNs fire.
func TestReadSchema_SchemaFeatures(t *testing.T) {
	schema, rec := readSchemaWithWarns(
		t,
		`CREATE TABLE calc (
			base   INTEGER PRIMARY KEY,
			qty    INTEGER CHECK (qty >= 0),
			dbl    INTEGER AS (base * 2) VIRTUAL,
			inc    INTEGER GENERATED ALWAYS AS (base + 1) STORED,
			name   TEXT,
			active INTEGER NOT NULL DEFAULT 0,
			CONSTRAINT positive_base CHECK (base > 0)
		)`,
		`CREATE INDEX calc_active_idx ON calc(name) WHERE active = 1`,
		`CREATE INDEX calc_lname_idx ON calc(lower(name))`,
	)

	// Capability flags carry honestly.
	if c := (Engine{}).Capabilities(); !c.SupportsCheckConstraint || !c.SupportsGeneratedColumns {
		t.Errorf("capabilities = %+v; want SupportsCheckConstraint && SupportsGeneratedColumns true", c)
	}

	calc := tableByName(schema, "calc")
	if calc == nil {
		t.Fatal("calc table missing")
	}

	// Generated columns: VIRTUAL dbl and STORED inc, dialect "sqlite".
	dbl := columnByName(calc, "dbl")
	if dbl == nil || !dbl.IsGenerated() {
		t.Fatalf("dbl = %#v; want a generated column", dbl)
	}
	if dbl.GeneratedExpr != "base * 2" || dbl.GeneratedStored || dbl.GeneratedExprDialect != "sqlite" {
		t.Errorf("dbl generated = {%q, stored=%v, dialect=%q}; want {base * 2, false, sqlite}",
			dbl.GeneratedExpr, dbl.GeneratedStored, dbl.GeneratedExprDialect)
	}
	inc := columnByName(calc, "inc")
	if inc == nil || !inc.IsGenerated() {
		t.Fatalf("inc = %#v; want a generated column", inc)
	}
	if inc.GeneratedExpr != "base + 1" || !inc.GeneratedStored || inc.GeneratedExprDialect != "sqlite" {
		t.Errorf("inc generated = {%q, stored=%v, dialect=%q}; want {base + 1, true, sqlite}",
			inc.GeneratedExpr, inc.GeneratedStored, inc.GeneratedExprDialect)
	}

	// CHECK constraints: inline qty CHECK (unnamed) + named table CHECK, in
	// declaration order.
	if len(calc.CheckConstraints) != 2 {
		t.Fatalf("CheckConstraints = %d (%+v); want 2", len(calc.CheckConstraints), calc.CheckConstraints)
	}
	if c0 := calc.CheckConstraints[0]; c0.Name != "" || c0.Expr != "qty >= 0" || c0.ExprDialect != "sqlite" {
		t.Errorf("CheckConstraints[0] = %+v; want {Name:\"\", Expr:\"qty >= 0\", sqlite}", c0)
	}
	if c1 := calc.CheckConstraints[1]; c1.Name != "positive_base" || c1.Expr != "base > 0" || c1.ExprDialect != "sqlite" {
		t.Errorf("CheckConstraints[1] = %+v; want {positive_base, base > 0, sqlite}", c1)
	}

	// Partial index predicate + expression index column.
	active := indexByName(calc, "calc_active_idx")
	if active == nil {
		t.Fatal("calc_active_idx missing")
	}
	if active.Predicate != "active = 1" || active.PredicateDialect != "sqlite" {
		t.Errorf("calc_active_idx predicate = {%q, %q}; want {active = 1, sqlite}", active.Predicate, active.PredicateDialect)
	}
	if len(active.Columns) != 1 || active.Columns[0].Column != "name" {
		t.Errorf("calc_active_idx columns = %+v; want [name]", active.Columns)
	}
	lname := indexByName(calc, "calc_lname_idx")
	if lname == nil {
		t.Fatal("calc_lname_idx missing")
	}
	if len(lname.Columns) != 1 || lname.Columns[0].Expression != "lower(name)" ||
		lname.Columns[0].ExpressionDialect != "sqlite" || lname.Columns[0].Column != "" {
		t.Errorf("calc_lname_idx columns = %+v; want [{Expression:lower(name), sqlite}]", lname.Columns)
	}

	// WARNs: one per table (generated/check) + one per carrying index = 3.
	if got := rec.verbatimCount(); got != 3 {
		t.Errorf("verbatim WARNs = %d; want 3 (1 table + 2 index)\nmsgs: %v", got, rec.msgs)
	}
	if !warnsMention(rec, "calc", "calc_active_idx", "calc_lname_idx", "dbl", "inc") {
		t.Errorf("WARNs should name the table/columns/indexes; got: %v", rec.msgs)
	}
}

// TestExtractCheckConstraints_NoFalsePositive pins the CHECK false-positive
// control: a column named `checkout` / `checked` and a string default
// containing the word CHECK must yield ZERO CHECK constraints — the scanner
// only matches CHECK as a standalone keyword token.
func TestExtractCheckConstraints_NoFalsePositive(t *testing.T) {
	schema, rec := readSchemaWithWarns(
		t,
		`CREATE TABLE acct (
			id       INTEGER PRIMARY KEY,
			checkout TEXT,
			checked  INTEGER NOT NULL DEFAULT 0,
			note     TEXT DEFAULT 'please CHECK this'
		)`,
	)
	acct := tableByName(schema, "acct")
	if acct == nil {
		t.Fatal("acct table missing")
	}
	if len(acct.CheckConstraints) != 0 {
		t.Errorf("CheckConstraints = %+v; want 0 (checkout/checked/'CHECK' must not false-positive)", acct.CheckConstraints)
	}
	if got := rec.verbatimCount(); got != 0 {
		t.Errorf("verbatim WARNs = %d; want 0 for a table with no carried features\nmsgs: %v", got, rec.msgs)
	}
}

// TestReadSchema_PlainTableUnchanged pins that an ordinary table (no generated
// columns, no CHECK, no partial/expression index) carries none of the new
// fields, fires zero WARNs, and produces the same column set the pre-table_xinfo
// reader did.
func TestReadSchema_PlainTableUnchanged(t *testing.T) {
	schema, rec := readSchemaWithWarns(
		t,
		`CREATE TABLE plain (
			id   INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)`,
		`CREATE INDEX plain_name_idx ON plain(name)`,
	)
	plain := tableByName(schema, "plain")
	if plain == nil {
		t.Fatal("plain table missing")
	}
	if len(plain.Columns) != 2 || plain.Columns[0].Name != "id" || plain.Columns[1].Name != "name" {
		t.Fatalf("columns = %+v; want [id name] (byte-identical to the table_info parse)", plain.Columns)
	}
	for _, c := range plain.Columns {
		if c.IsGenerated() || c.GeneratedExprDialect != "" {
			t.Errorf("column %q carries generated fields unexpectedly: %+v", c.Name, c)
		}
	}
	if plain.Columns[0].Type != (ir.Integer{Width: 64, AutoIncrement: true}) {
		t.Errorf("id type = %#v; want INTEGER rowid-alias autoincrement", plain.Columns[0].Type)
	}
	if _, ok := plain.Columns[1].Type.(ir.Text); !ok {
		t.Errorf("name type = %#v; want ir.Text", plain.Columns[1].Type)
	}
	if len(plain.CheckConstraints) != 0 {
		t.Errorf("CheckConstraints = %+v; want none", plain.CheckConstraints)
	}
	idx := indexByName(plain, "plain_name_idx")
	if idx == nil {
		t.Fatal("plain_name_idx missing")
	}
	if idx.Predicate != "" || idx.PredicateDialect != "" {
		t.Errorf("plain_name_idx predicate = {%q, %q}; want empty", idx.Predicate, idx.PredicateDialect)
	}
	if len(idx.Columns) != 1 || idx.Columns[0].Column != "name" || idx.Columns[0].Expression != "" {
		t.Errorf("plain_name_idx columns = %+v; want [name] plain column", idx.Columns)
	}
	if got := rec.verbatimCount(); got != 0 {
		t.Errorf("verbatim WARNs = %d; want 0 for a plain table\nmsgs: %v", got, rec.msgs)
	}
}

// indexByName returns the named index on a table, or nil.
func indexByName(t *ir.Table, name string) *ir.Index {
	for _, ix := range t.Indexes {
		if ix.Name == name {
			return ix
		}
	}
	return nil
}

// warnsMention reports whether the captured WARN text mentions every needle.
func warnsMention(rec *warnRecorder, needles ...string) bool {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	joined := strings.Join(rec.msgs, "\n")
	for _, n := range needles {
		if !strings.Contains(joined, n) {
			return false
		}
	}
	return true
}
