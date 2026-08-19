// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// bprScript drives the preflight fake: which (table -> cols) are TINYINT(1)
// (the information_schema enumeration answer), which "table.col" hold an
// out-of-range value (and its example), and which probes should error (the
// best-effort path). tinyErr makes the enumeration query itself error.
type bprScript struct {
	tiny    map[string][]string
	bad     map[string]int64
	errFor  map[string]bool
	tinyErr bool
}

type bprDriver struct{ s *bprScript }

func (d bprDriver) Open(string) (driver.Conn, error) { return bprConn(d), nil }

type bprConn struct{ s *bprScript }

func (bprConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (bprConn) Close() error                        { return nil }
func (bprConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }

// tableInQuery finds which scripted table's backtick-quoted name appears in q.
func (c bprConn) tableInQuery(q string) string {
	for tbl := range c.s.tiny {
		if strings.Contains(q, "`"+tbl+"`") {
			return tbl
		}
	}
	return ""
}

func (c bprConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(q, "COLUMN_TYPE"): // the TINYINT(1) enumeration
		if c.s.tinyErr {
			return nil, errors.New("enumeration boom")
		}
		var pairs [][2]string
		for tbl, cols := range c.s.tiny {
			for _, col := range cols {
				pairs = append(pairs, [2]string{tbl, col})
			}
		}
		return &strPairRows{pairs: pairs}, nil

	case strings.HasPrefix(strings.TrimSpace(q), "SELECT 1 FROM"): // combined probe
		tbl := c.tableInQuery(q)
		if c.s.errFor[tbl] {
			return nil, errors.New("probe did not complete")
		}
		for key := range c.s.bad {
			if strings.HasPrefix(key, tbl+".") {
				return &boolRow{val: 1}, nil // a row matched
			}
		}
		return &boolRow{done: true}, nil // no rows

	default: // pinpoint: SELECT `col` FROM ... WHERE `col` NOT IN (0,1) LIMIT 1
		tbl := c.tableInQuery(q)
		for key, v := range c.s.bad {
			if !strings.HasPrefix(key, tbl+".") {
				continue
			}
			col := strings.TrimPrefix(key, tbl+".")
			if strings.Contains(q, "`"+col+"`") {
				return &boolRow{val: v}, nil
			}
		}
		return &boolRow{done: true}, nil
	}
}

// strPairRows is a two-string-column driver.Rows for the (TABLE_NAME,
// COLUMN_NAME) enumeration.
type strPairRows struct {
	pairs [][2]string
	i     int
}

func (*strPairRows) Columns() []string { return []string{"TABLE_NAME", "COLUMN_NAME"} }
func (*strPairRows) Close() error      { return nil }
func (r *strPairRows) Next(dest []driver.Value) error {
	if r.i >= len(r.pairs) {
		return io.EOF
	}
	dest[0], dest[1] = r.pairs[r.i][0], r.pairs[r.i][1]
	r.i++
	return nil
}

func newBPRReader(t *testing.T, s *bprScript) *SchemaReader {
	t.Helper()
	name := fmt.Sprintf("sluice-bpr-fake-%s-%d", t.Name(), indexFakeSeq.Add(1))
	sql.Register(name, bprDriver{s: s})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open bpr fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &SchemaReader{db: db, schema: "app"}
}

func bprSchema(tables ...string) *ir.Schema {
	s := &ir.Schema{}
	for _, name := range tables {
		s.Tables = append(s.Tables, &ir.Table{
			Name: name,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "is_active", Type: ir.Boolean{}},
			},
		})
	}
	return s
}

func TestPreflightBoolRanges(t *testing.T) {
	ctx := context.Background()

	t.Run("out-of-range value refuses, coded, names the column + value", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{
			tiny: map[string][]string{"packs": {"is_active"}},
			bad:  map[string]int64{"packs.is_active": 6},
		})
		err := r.PreflightBoolRanges(ctx, bprSchema("packs"))
		if err == nil {
			t.Fatal("want a refusal for an out-of-range TINYINT(1) value, got nil")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeValueTinyint1Range {
			t.Fatalf("want CodeValueTinyint1Range; got ok=%v err=%v", ok, err)
		}
		if !strings.Contains(err.Error(), `"packs.is_active"`) || !strings.Contains(err.Error(), "6") {
			t.Errorf("refusal should name the column and the example value 6; got %q", err.Error())
		}
	})

	t.Run("all in-range returns nil", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{tiny: map[string][]string{"packs": {"is_active"}}})
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs")); err != nil {
			t.Fatalf("clean table: want nil, got %v", err)
		}
	})

	t.Run("best-effort: a probe that errors WARNs and proceeds (no refusal)", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{
			tiny:   map[string][]string{"packs": {"is_active"}},
			bad:    map[string]int64{"packs.is_active": 6}, // bad data exists...
			errFor: map[string]bool{"packs": true},         // ...but the probe can't complete
		})
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs")); err != nil {
			t.Fatalf("probe error must degrade to nil (decode-time guard is the floor), got %v", err)
		}
	})

	t.Run("best-effort: enumeration error degrades to nil", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{tinyErr: true})
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs")); err != nil {
			t.Fatalf("enumeration error must degrade to nil, got %v", err)
		}
	})

	t.Run("a table NOT in the in-scope schema is never probed", func(t *testing.T) {
		// `secret` has bad data in the fake, but it is not in the passed schema,
		// so the preflight must not probe (nor refuse on) it.
		r := newBPRReader(t, &bprScript{
			tiny: map[string][]string{"secret": {"is_active"}},
			bad:  map[string]int64{"secret.is_active": 6},
		})
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs")); err != nil {
			t.Fatalf("out-of-scope table must not be probed; got %v", err)
		}
	})

	t.Run("no ir.Boolean columns in scope is a no-op", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{tiny: map[string][]string{"packs": {"is_active"}}})
		plain := &ir.Schema{Tables: []*ir.Table{{
			Name:    "packs",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}}}
		if err := r.PreflightBoolRanges(ctx, plain); err != nil {
			t.Fatalf("no-bool-column schema: want nil, got %v", err)
		}
	})
}
