// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestBuildBatchUpsert_SinglePK(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert(table, 2, pk, upsertRowAlias)
	want := "INSERT INTO `users` (`id`, `email`, `name`) VALUES (?, ?, ?), (?, ?, ?) AS new ON DUPLICATE KEY UPDATE `email` = new.`email`, `name` = new.`name`"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestBuildBatchUpsert_CompositePK(t *testing.T) {
	table := &ir.Table{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "sku", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "tenant"}, {Column: "sku"},
		}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert(table, 1, pk, upsertRowAlias)
	want := "INSERT INTO `products` (`tenant`, `sku`, `name`) VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE `name` = new.`name`"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestBuildBatchUpsert_AllPKColumns(t *testing.T) {
	table := &ir.Table{
		Name: "tags",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "tag", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "tenant"}, {Column: "tag"},
		}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert(table, 1, pk, upsertRowAlias)
	// No-op: re-assign first PK to itself so the statement is legal.
	if !strings.Contains(got, "`tenant` = new.`tenant`") {
		t.Errorf("got %q; want self-reassign of first PK column", got)
	}
}

func TestBuildBatchUpsert_NoKeyColsFallsBackToPlainInsert(t *testing.T) {
	table := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}},
			{Name: "data", Type: ir.Text{}},
		},
	}
	// Empty keyCols → plain INSERT (defensive path; the idempotent
	// writer refuses keyless tables before reaching here, Bug 125).
	got := buildBatchUpsert(table, 1, nil, upsertRowAlias)
	if strings.Contains(got, "ON DUPLICATE KEY") {
		t.Errorf("expected plain INSERT for empty keyCols; got %q", got)
	}
}

// TestBuildBatchUpsert_NonNullUniqueKey is the Bug 125 pin: a PK-less
// table keyed on a non-null UNIQUE index produces an ON DUPLICATE KEY
// UPDATE that excludes the key columns from the SET-list, so VStream
// COPY catchup re-emissions upsert instead of colliding.
func TestBuildBatchUpsert_NonNullUniqueKey(t *testing.T) {
	table := &ir.Table{
		Name: "connections",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "tiny", Type: ir.Integer{Width: 8}, Nullable: false},
			{Name: "payload", Type: ir.Text{}},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			{Name: "uk_tiny", Unique: true, Columns: []ir.IndexColumn{{Column: "tiny"}}},
		},
	}
	keyCols, ok := effectiveUpsertKeyColumns(table)
	if !ok {
		t.Fatal("effectiveUpsertKeyColumns: ok=false; want a non-null unique key")
	}
	got := buildBatchUpsert(table, 1, keyCols, upsertRowAlias)
	if !strings.Contains(got, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("expected upsert for non-null unique key; got %q", got)
	}
	// The chosen key is deterministic: both uq_id and uk_tiny are
	// single-column non-null UNIQUE, so the lexicographically smaller
	// index name (uk_tiny) wins.
	if keyCols[0] != "tiny" {
		t.Errorf("keyCols = %v; want [tiny] (deterministic: fewest cols, then name)", keyCols)
	}
	// payload (the only non-key column) is refreshed; the key column is not.
	if !strings.Contains(got, "`payload` = new.`payload`") {
		t.Errorf("got %q; want payload in the SET-list", got)
	}
	if strings.Contains(got, "`tiny` = new.`tiny`") {
		t.Errorf("got %q; key column tiny must NOT be in the SET-list", got)
	}
}

func TestEffectiveUpsertKeyColumns(t *testing.T) {
	cases := []struct {
		name    string
		table   *ir.Table
		wantOK  bool
		wantKey []string
	}{
		{
			name: "pk wins over unique",
			table: &ir.Table{
				Columns:    []*ir.Column{{Name: "id", Nullable: false}, {Name: "u", Nullable: false}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
				Indexes:    []*ir.Index{{Name: "uq_u", Unique: true, Columns: []ir.IndexColumn{{Column: "u"}}}},
			},
			wantOK:  true,
			wantKey: []string{"id"},
		},
		{
			name: "non-null unique when no pk",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "id", Nullable: false}},
				Indexes: []*ir.Index{{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			wantOK:  true,
			wantKey: []string{"id"},
		},
		{
			name: "nullable unique does NOT qualify",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "id", Nullable: true}},
				Indexes: []*ir.Index{{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			wantOK: false,
		},
		{
			name: "non-unique index does NOT qualify",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "id", Nullable: false}},
				Indexes: []*ir.Index{{Name: "idx_id", Unique: false, Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			wantOK: false,
		},
		{
			name: "truly keyless",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "a", Nullable: false}, {Name: "b", Nullable: true}},
			},
			wantOK: false,
		},
		{
			name: "fewest columns wins",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "a", Nullable: false}, {Name: "b", Nullable: false}, {Name: "c", Nullable: false}},
				Indexes: []*ir.Index{
					{Name: "uq_ab", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
					{Name: "uq_c", Unique: true, Columns: []ir.IndexColumn{{Column: "c"}}},
				},
			},
			wantOK:  true,
			wantKey: []string{"c"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := effectiveUpsertKeyColumns(c.table)
			if ok != c.wantOK {
				t.Fatalf("ok = %v; want %v (key=%v)", ok, c.wantOK, got)
			}
			if !ok {
				return
			}
			if len(got) != len(c.wantKey) {
				t.Fatalf("key = %v; want %v", got, c.wantKey)
			}
			for i := range c.wantKey {
				if got[i] != c.wantKey[i] {
					t.Errorf("key[%d] = %q; want %q", i, got[i], c.wantKey[i])
				}
			}
		})
	}
}

// TestWriteRowsIdempotent_RefusesKeylessTable pins the loud refusal:
// a table with no PK and no non-null UNIQUE index cannot be copied
// idempotently (nothing for ON DUPLICATE KEY UPDATE to collide on), so
// the writer refuses rather than silently duplicating catchup
// re-emissions (Bug 125).
func TestWriteRowsIdempotent_RefusesKeylessTable(t *testing.T) {
	w := &RowWriter{bulkLoad: ir.BulkLoadBatchedInsert}
	table := &ir.Table{
		Name: "log_lines",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}, Nullable: false},
			{Name: "msg", Type: ir.Text{}, Nullable: true},
		},
	}
	rows := make(chan ir.Row)
	close(rows)
	err := w.WriteRowsIdempotent(context.Background(), table, rows)
	if err == nil {
		t.Fatal("WriteRowsIdempotent on keyless table: err=nil; want loud refusal")
	}
	if !strings.Contains(err.Error(), "log_lines") || !strings.Contains(err.Error(), "Bug 125") {
		t.Errorf("error %q; want it to name the table and Bug 125", err.Error())
	}
}

// TestWriteRowsIdempotentParallel_RefusesKeylessTable: the conflict key
// is resolved ONCE before any worker spawns, so a keyless table is
// refused loudly (Bug 125) on the fan-out path too — no worker ever
// runs (ADR-0097 §2 no-PK contract preserved). No DB needed: the
// refusal fires before w.db.Conn.
func TestWriteRowsIdempotentParallel_RefusesKeylessTable(t *testing.T) {
	w := &RowWriter{bulkLoad: ir.BulkLoadBatchedInsert}
	table := &ir.Table{
		Name: "log_lines",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}, Nullable: false},
			{Name: "msg", Type: ir.Text{}, Nullable: true},
		},
	}
	ch := make(chan ir.Row)
	close(ch)
	err := w.WriteRowsIdempotentParallel(context.Background(), table, []<-chan ir.Row{ch, ch})
	if err == nil {
		t.Fatal("WriteRowsIdempotentParallel on keyless table: err=nil; want loud refusal")
	}
	if !strings.Contains(err.Error(), "log_lines") || !strings.Contains(err.Error(), "Bug 125") {
		t.Errorf("error %q; want it to name the table and Bug 125", err.Error())
	}
}

// TestWriteRowsIdempotentParallel_Guards: the shape guards fire before
// any connection is pinned (no DB needed).
func TestWriteRowsIdempotentParallel_Guards(t *testing.T) {
	w := &RowWriter{bulkLoad: ir.BulkLoadBatchedInsert}
	pkTable := &ir.Table{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	ch := make(chan ir.Row)
	close(ch)

	if err := w.WriteRowsIdempotentParallel(context.Background(), nil, []<-chan ir.Row{ch}); err == nil {
		t.Error("nil table: want error")
	}
	if err := w.WriteRowsIdempotentParallel(context.Background(), &ir.Table{Name: "x"}, []<-chan ir.Row{ch}); err == nil {
		t.Error("no columns: want error")
	}
	if err := w.WriteRowsIdempotentParallel(context.Background(), pkTable, nil); err == nil {
		t.Error("no worker channels: want error")
	}
	if err := w.WriteRowsIdempotentParallel(context.Background(), pkTable, []<-chan ir.Row{nil}); err == nil {
		t.Error("nil worker channel: want error")
	}
}

// TestWriteRowsParallel_Guards: the ADR-0102 plain fan-out writer's shape
// guards fire before any connection is pinned (no DB needed). Unlike the
// idempotent variant there is NO keyless refusal — the plain path copies a
// no-PK table fine (it just doesn't fan it out; the pipeline routes a no-PK
// table to the single-writer path before reaching here).
func TestWriteRowsParallel_Guards(t *testing.T) {
	w := &RowWriter{bulkLoad: ir.BulkLoadBatchedInsert}
	pkTable := &ir.Table{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	ch := make(chan ir.Row)
	close(ch)

	if err := w.WriteRowsParallel(context.Background(), nil, []<-chan ir.Row{ch}); err == nil {
		t.Error("nil table: want error")
	}
	if err := w.WriteRowsParallel(context.Background(), &ir.Table{Name: "x"}, []<-chan ir.Row{ch}); err == nil {
		t.Error("no columns: want error")
	}
	if err := w.WriteRowsParallel(context.Background(), pkTable, nil); err == nil {
		t.Error("no worker channels: want error")
	}
	if err := w.WriteRowsParallel(context.Background(), pkTable, []<-chan ir.Row{nil}); err == nil {
		t.Error("nil worker channel: want error")
	}
}

// Capability surface pin (ADR-0102): *RowWriter satisfies
// ir.ParallelCopyWriter so the pipeline's type-assert engages the plain
// fan-out. A compile-time assertion — if the method signature drifts, the
// build breaks here.
var _ ir.ParallelCopyWriter = (*RowWriter)(nil)

func TestPrimaryKeyColumns(t *testing.T) {
	table := &ir.Table{
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "a"}, {Column: "b"},
		}},
	}
	got := primaryKeyColumns(table)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("primaryKeyColumns: got %v; want [a b]", got)
	}
	if primaryKeyColumns(&ir.Table{}) != nil {
		t.Error("primaryKeyColumns: expected nil for table without PK")
	}
}

// TestWarningsCheckDue pins the batched-INSERT warning-sampling
// schedule (repo-audit M3.5): the first warningsExhaustiveFlushes
// flushes are ALL checked — the property that a systematic clamp
// (wrong type mapping, coercing column) is caught on the very first
// flush of a table — then 1-in-warningsSampleEvery. The final flush
// is forced by the caller and isn't part of this function.
func TestWarningsCheckDue(t *testing.T) {
	for n := 1; n <= warningsExhaustiveFlushes; n++ {
		if !warningsCheckDue(n) {
			t.Errorf("flush %d: want exhaustive-phase check, got skip", n)
		}
	}
	checked := 0
	for n := warningsExhaustiveFlushes + 1; n <= warningsExhaustiveFlushes+10*warningsSampleEvery; n++ {
		if warningsCheckDue(n) {
			checked++
			if n%warningsSampleEvery != 0 {
				t.Errorf("flush %d: checked off-schedule (every %d)", n, warningsSampleEvery)
			}
		}
	}
	// 10 sampling windows after the exhaustive phase => ~10 checks
	// (±1 for phase alignment); the point is sparse, not zero and
	// not every-flush.
	if checked < 9 || checked > 11 {
		t.Errorf("sampling phase: %d checks across 10 windows; want ~10", checked)
	}
}
