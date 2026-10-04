// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowWriter_ProbeReplayKey_ShapeMatrix pins the SQLite half of the F-E1
// target judgment against the real engine, over every key shape SQLite
// distinguishes — not one representative (the Bug 74 rule).
//
// The independent expected value per shape is the WRITER'S OWN behaviour,
// measured: the case's row is written twice through [RowWriter.WriteRows] —
// the path a restore re-run takes — and the outcome recorded. SQLite's
// writer is a plain INSERT, so a row that collides FAILS ("loud") and one
// that does not lands twice ("duplicates"). keyed=true must coincide with
// "loud"; every "duplicates" shape must be keyed=false. A conservative case
// is keyed=false although it collides; refusing it costs a loud refusal of
// a working restore, never a silent duplicate.
//
// recorded is the backup's definition of the table: the probe's "supplied"
// columns and recorded-NOT-NULL facts come from it, exactly as they do on a
// real restore.
func TestRowWriter_ProbeReplayKey_ShapeMatrix(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "probe.db")

	const (
		loud       = "loud"
		duplicates = "duplicates"
	)
	i64 := ir.Integer{Width: 64}
	txt := ir.Text{Size: ir.TextLong}
	col := func(name string, typ ir.Type, nullable bool) *ir.Column {
		return &ir.Column{Name: name, Type: typ, Nullable: nullable}
	}
	v := col("v", txt, true)
	cases := []struct {
		table, ddl   string
		recorded     []*ir.Column
		row          ir.Row
		wantExists   bool
		wantKeyed    bool
		outcome      string
		conservative bool
	}{
		{table: "absent", recorded: []*ir.Column{col("id", i64, false)}},
		{
			"no_key", `CREATE TABLE no_key (id INTEGER NOT NULL, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, false, duplicates, false,
		},
		{
			"ipk", `CREATE TABLE ipk (id INTEGER PRIMARY KEY, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, true, loud, false,
		},
		// The rowid alias reads notnull=0 in table_xinfo; with a recorded
		// column that is nullable too, nothing proves the replayed value is
		// non-NULL (NULL there draws a fresh rowid). Refused, conservatively.
		{
			"ipk_recorded_nullable", `CREATE TABLE ipk_recorded_nullable (id INTEGER PRIMARY KEY, v TEXT)`,
			[]*ir.Column{col("id", i64, true), v},
			ir.Row{"id": int64(1)},
			true, false, loud, true,
		},
		{
			"text_pk_nullable_null", `CREATE TABLE text_pk_nullable_null (id TEXT PRIMARY KEY, v TEXT)`,
			[]*ir.Column{col("id", txt, true), v},
			ir.Row{"id": nil},
			true, false, duplicates, false,
		},
		{
			"text_pk_not_null", `CREATE TABLE text_pk_not_null (id TEXT NOT NULL PRIMARY KEY, v TEXT)`,
			[]*ir.Column{col("id", txt, false), v},
			ir.Row{"id": "a"},
			true, true, loud, false,
		},
		{
			"text_pk_recorded_not_null", `CREATE TABLE text_pk_recorded_not_null (id TEXT PRIMARY KEY, v TEXT)`,
			[]*ir.Column{col("id", txt, false), v},
			ir.Row{"id": "a"},
			true, true, loud, false,
		},
		{
			"nn_unique", `CREATE TABLE nn_unique (id INTEGER NOT NULL UNIQUE, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, true, loud, false,
		},
		{
			"nn_unique_composite", `CREATE TABLE nn_unique_composite (a INTEGER NOT NULL, b INTEGER NOT NULL, v TEXT, UNIQUE (a, b))`,
			[]*ir.Column{col("a", i64, false), col("b", i64, false), v},
			ir.Row{"a": int64(1), "b": int64(1)},
			true, true, loud, false,
		},
		{
			"nullable_unique", `CREATE TABLE nullable_unique (id INTEGER UNIQUE, v TEXT)`,
			[]*ir.Column{col("id", i64, true), v},
			ir.Row{"id": nil},
			true, false, duplicates, false,
		},
		{
			"partial_unique", `CREATE TABLE partial_unique (id INTEGER NOT NULL, v TEXT); CREATE UNIQUE INDEX partial_unique_ix ON partial_unique (id) WHERE id > 0`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(-1)},
			true, false, duplicates, false,
		},
		{
			"expression_unique", `CREATE TABLE expression_unique (id INTEGER NOT NULL, v TEXT); CREATE UNIQUE INDEX expression_unique_ix ON expression_unique ((id * 2))`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, false, loud, true,
		},
		// The review's HIGH 1 on SQLite: a surrogate the rows never carry.
		{
			"surrogate_ipk", `CREATE TABLE surrogate_ipk (sid INTEGER PRIMARY KEY, id INTEGER NOT NULL, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, false, duplicates, false,
		},
		{
			"surrogate_autoincrement", `CREATE TABLE surrogate_autoincrement (sid INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER NOT NULL, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, false, duplicates, false,
		},
		// The same class with the surrogate declared NOT NULL, so the
		// nullability arm cannot refuse it and only the SUPPLY check does
		// (a mutation run found the two rows above refused by nullability
		// alone, leaving the supply check ungraded).
		{
			"surrogate_not_null_autoincrement", `CREATE TABLE surrogate_not_null_autoincrement (sid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, id INTEGER NOT NULL, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, false, duplicates, false,
		},
		{
			"surrogate_not_null_default_expr", `CREATE TABLE surrogate_not_null_default_expr (sid TEXT NOT NULL PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))), id INTEGER NOT NULL, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, false, duplicates, false,
		},
		// Two keys, one supplied: a plain INSERT collides on every unique
		// key, so the supplied one is enough.
		{
			"two_keys_one_supplied", `CREATE TABLE two_keys_one_supplied (sid INTEGER PRIMARY KEY, id INTEGER NOT NULL UNIQUE, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, true, loud, false,
		},
		{
			"generated_unique", `CREATE TABLE generated_unique (id INTEGER NOT NULL, k INTEGER GENERATED ALWAYS AS (id * 2) STORED UNIQUE, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, false, loud, true,
		},
		// SQLite resolves column names case-insensitively; so does the probe.
		{
			"case_folded_pk", `CREATE TABLE case_folded_pk (ID INTEGER NOT NULL PRIMARY KEY, v TEXT)`,
			[]*ir.Column{col("id", i64, false), v},
			ir.Row{"id": int64(1)},
			true, true, loud, false,
		},
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, c := range cases {
		if c.ddl != "" {
			if _, err := db.ExecContext(ctx, c.ddl); err != nil {
				t.Fatalf("%s: %v", c.table, err)
			}
		}
	}

	rwAny, err := (Engine{}).OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	rw := rwAny.(*RowWriter)
	defer func() { _ = rw.Close() }()
	var prober ir.ReplayKeyProber = rw
	var checker ir.TableEmptyChecker = rw

	sawKeyed, sawSilent := false, false
	for _, c := range cases {
		t.Run(c.table, func(t *testing.T) {
			recorded := &ir.Table{Name: c.table, Columns: c.recorded}
			exists, keyed, err := prober.ProbeReplayKey(ctx, recorded)
			if err != nil {
				t.Fatalf("ProbeReplayKey: %v", err)
			}
			if exists != c.wantExists || keyed != c.wantKeyed {
				t.Fatalf("ProbeReplayKey = (exists=%v, keyed=%v); want (%v, %v)", exists, keyed, c.wantExists, c.wantKeyed)
			}
			empty, err := checker.IsTableEmpty(ctx, recorded)
			if err != nil || !empty {
				t.Fatalf("IsTableEmpty before any write = (%v, %v); want (true, nil)", empty, err)
			}
			if !exists {
				return
			}
			row := ir.Row{"v": "x"}
			for k, val := range c.row {
				row[k] = val
			}
			write := func() error {
				ch := make(chan ir.Row, 1)
				ch <- row
				close(ch)
				return rw.WriteRows(ctx, recorded, ch)
			}
			if err := write(); err != nil {
				t.Fatalf("first write: %v", err)
			}
			if empty, err := checker.IsTableEmpty(ctx, recorded); err != nil || empty {
				t.Fatalf("IsTableEmpty after a write = (%v, %v); want (false, nil)", empty, err)
			}
			got := loud
			if err := write(); err == nil {
				var n int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+quoteIdent(c.table)).Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n != 2 {
					t.Fatalf("second write succeeded and left %d rows; the outcome is neither loud nor a duplicate", n)
				}
				got = duplicates
			}
			if got != c.outcome {
				t.Fatalf("re-writing one row %s; the case declares %s — the ground truth moved", got, c.outcome)
			}
			if got == duplicates && keyed {
				t.Error("a silently duplicating table was judged keyed")
			}
			if !c.conservative && keyed != (got == loud) {
				t.Errorf("probe says keyed=%v but a re-write %s", keyed, got)
			}
			sawKeyed = sawKeyed || keyed
			sawSilent = sawSilent || got == duplicates
		})
	}
	if !sawKeyed || !sawSilent {
		t.Fatalf("anti-vacuity: the matrix must reach a keyed table and a silently duplicating one (keyed=%v, silent=%v)", sawKeyed, sawSilent)
	}
}

// TestRowWriter_ProbeReplayKey_RefusesAnEmptyRecordedTable pins the
// fail-closed edge: with no recorded columns there is nothing to judge
// supply against, and guessing "keyed" would re-open the door.
func TestRowWriter_ProbeReplayKey_RefusesAnEmptyRecordedTable(t *testing.T) {
	ctx := context.Background()
	rwAny, err := (Engine{}).OpenRowWriter(ctx, filepath.Join(t.TempDir(), "e.db"))
	if err != nil {
		t.Fatal(err)
	}
	rw := rwAny.(*RowWriter)
	defer func() { _ = rw.Close() }()
	if _, _, err := rw.ProbeReplayKey(ctx, &ir.Table{Name: "t"}); err == nil {
		t.Fatal("a recorded table with no columns was judged instead of refused")
	}
}
