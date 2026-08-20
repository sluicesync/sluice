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

	// filterHidesBad models the DATABASE applying the operator's `--where`: when
	// true, any probe whose SQL carries a filter (an ` AND (` after a leading
	// `(`) returns NO rows even though bad data exists — the F-1 case where the
	// filter excludes every out-of-range row. capturedProbes records the combined
	// probe SQL so a test can assert the filter was actually ANDed in.
	filterHidesBad bool
	capturedProbes []string
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

	case strings.Contains(q, "SELECT /*+ MAX_EXECUTION_TIME") && strings.Contains(q, " 1 FROM "): // combined probe
		tbl := c.tableInQuery(q)
		c.s.capturedProbes = append(c.s.capturedProbes, q)
		if c.s.errFor[tbl] {
			return nil, errors.New("probe did not complete")
		}
		// A `--where` filter (rendered by andBoolRangeFilter as `(filter) AND
		// (...)`) that excludes the bad rows: the DATABASE returns no rows even
		// though bad data exists (F-1). Detected by the ") AND (" the wrapper
		// introduces — the unfiltered predicate never contains it.
		if c.s.filterHidesBad && strings.Contains(q, ") AND (") {
			return &boolRow{done: true}, nil
		}
		// Match only if a BAD column is actually IN this probe's WHERE — so a
		// bad column the preflight correctly excluded (overridden away) does not
		// spuriously trip the combined probe (GAP #1 regression guard).
		for key := range c.s.bad {
			if !strings.HasPrefix(key, tbl+".") {
				continue
			}
			col := strings.TrimPrefix(key, tbl+".")
			if strings.Contains(q, "`"+col+"`") {
				return &boolRow{val: 1}, nil // a row matched on a probed column
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
		err := r.PreflightBoolRanges(ctx, bprSchema("packs"), nil)
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
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs"), nil); err != nil {
			t.Fatalf("clean table: want nil, got %v", err)
		}
	})

	t.Run("best-effort: a probe that errors WARNs and proceeds (no refusal)", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{
			tiny:   map[string][]string{"packs": {"is_active"}},
			bad:    map[string]int64{"packs.is_active": 6}, // bad data exists...
			errFor: map[string]bool{"packs": true},         // ...but the probe can't complete
		})
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs"), nil); err != nil {
			t.Fatalf("probe error must degrade to nil (decode-time guard is the floor), got %v", err)
		}
	})

	t.Run("best-effort: enumeration error degrades to nil", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{tinyErr: true})
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs"), nil); err != nil {
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
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs"), nil); err != nil {
			t.Fatalf("out-of-scope table must not be probed; got %v", err)
		}
	})

	t.Run("an overridden TINYINT(1) column (now ir.Integer) is NOT probed (GAP #1)", func(t *testing.T) {
		// A table with a real bool (is_active, clean 0/1) + a column used as a
		// small int (status, holds 6) that the operator overrode to smallint.
		// The passed (post-ApplyMappings) schema has status as ir.Integer; the
		// raw catalog (fake `tiny`) still reports both as tinyint(1). The
		// preflight must probe ONLY is_active and NOT refuse on status —
		// otherwise it defeats the exact remedy the error recommends.
		r := newBPRReader(t, &bprScript{
			tiny: map[string][]string{"packs": {"is_active", "status"}},
			bad:  map[string]int64{"packs.status": 6},
		})
		schema := &ir.Schema{Tables: []*ir.Table{{
			Name: "packs",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "is_active", Type: ir.Boolean{}},
				{Name: "status", Type: ir.Integer{Width: 16}}, // overridden away from bool
			},
		}}}
		if err := r.PreflightBoolRanges(ctx, schema, nil); err != nil {
			t.Fatalf("overridden column must not be probed/refused; got %v", err)
		}
	})

	t.Run("no ir.Boolean columns in scope is a no-op", func(t *testing.T) {
		r := newBPRReader(t, &bprScript{tiny: map[string][]string{"packs": {"is_active"}}})
		plain := &ir.Schema{Tables: []*ir.Table{{
			Name:    "packs",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}}}
		if err := r.PreflightBoolRanges(ctx, plain, nil); err != nil {
			t.Fatalf("no-bool-column schema: want nil, got %v", err)
		}
	})

	t.Run("a --where that excludes the bad rows is ANDed in and does NOT refuse (F-1)", func(t *testing.T) {
		// Bad data exists, but the operator's --where excludes it (modelled by
		// filterHidesBad). The preflight must NOT refuse on rows the copy would
		// never move — the Bug 246 filter-blind-refusal shape.
		s := &bprScript{
			tiny:           map[string][]string{"packs": {"is_active"}},
			bad:            map[string]int64{"packs.is_active": 6},
			filterHidesBad: true,
		}
		r := newBPRReader(t, s)
		filters := map[string]string{"packs": "region = 'us-east'"}
		if err := r.PreflightBoolRanges(ctx, bprSchema("packs"), filters); err != nil {
			t.Fatalf("a --where excluding the bad rows must not refuse; got %v", err)
		}
		// And prove the filter was actually threaded into the probe SQL (not
		// silently dropped, which would re-open the refusal).
		if len(s.capturedProbes) == 0 {
			t.Fatal("no combined probe was captured")
		}
		if !strings.Contains(s.capturedProbes[0], "region = 'us-east'") ||
			!strings.Contains(s.capturedProbes[0], ") AND (") {
			t.Errorf("the --where predicate was not ANDed into the probe; got %q", s.capturedProbes[0])
		}
	})

	t.Run("a --where that does NOT exclude the bad rows still refuses", func(t *testing.T) {
		// The filter is threaded, but the bad rows remain in scope
		// (filterHidesBad false): a real in-scope violation must still refuse —
		// the filter must not suppress a genuine loss.
		s := &bprScript{
			tiny: map[string][]string{"packs": {"is_active"}},
			bad:  map[string]int64{"packs.is_active": 6},
		}
		r := newBPRReader(t, s)
		filters := map[string]string{"packs": "region = 'us-east'"}
		err := r.PreflightBoolRanges(ctx, bprSchema("packs"), filters)
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeValueTinyint1Range {
			t.Fatalf("an in-scope out-of-range value must still refuse; got ok=%v err=%v", ok, err)
		}
	})
}

// TestAndBoolRangeFilter pins the exact SQL andBoolRangeFilter builds — most
// importantly that an OR-group range predicate is wrapped in its own parens when
// a filter is present, so operator precedence (AND over OR) cannot silently
// apply the --where to only the first column, and that an empty filter leaves
// the predicate byte-for-byte unchanged (the unfiltered path is untouched).
func TestAndBoolRangeFilter(t *testing.T) {
	const orGroup = "`a` NOT IN (0,1) OR `b` NOT IN (0,1)"
	const single = "`a` NOT IN (0,1)"
	cases := []struct {
		name   string
		filter string
		pred   string
		want   string
	}{
		{"empty filter leaves an OR-group unchanged", "", orGroup, orGroup},
		{"empty filter leaves a single predicate unchanged", "  ", single, single},
		{"a filter parenthesizes the whole OR-group", "region = 'x'", orGroup, "(region = 'x') AND (`a` NOT IN (0,1) OR `b` NOT IN (0,1))"},
		{"a filter ANDs a single predicate", "region = 'x'", single, "(region = 'x') AND (`a` NOT IN (0,1))"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := andBoolRangeFilter(tc.filter, tc.pred); got != tc.want {
				t.Errorf("andBoolRangeFilter(%q, %q) = %q; want %q", tc.filter, tc.pred, got, tc.want)
			}
		})
	}
}
