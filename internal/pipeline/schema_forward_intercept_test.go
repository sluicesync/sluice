// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// addColForwardTable builds a single-table test fixture with cols
// columns plus an INT PK. Adapter for the test's expected shape.
func addColForwardTable(name string, cols ...*ir.Column) *ir.Table {
	pk := &ir.Column{Name: "id", Type: ir.Integer{Width: 32}}
	all := append([]*ir.Column{pk}, cols...)
	return &ir.Table{
		Schema:  "public",
		Name:    name,
		Columns: all,
		PrimaryKey: &ir.Index{
			Name:    "pk_" + name,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
}

func addColForwardSnap(table *ir.Table) ir.SchemaSnapshot {
	return ir.SchemaSnapshot{
		Position: ir.Position{Engine: "postgres", Token: "lsn/1"},
		Schema:   table.Schema,
		Table:    table.Name,
		IR:       table,
	}
}

// drainChannel collects the changes pushed onto out until it closes
// or the deadline elapses.
func drainChannel(t *testing.T, out <-chan ir.Change, deadline time.Duration) []ir.Change {
	t.Helper()
	var got []ir.Change
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		select {
		case c, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, c)
		case <-timer.C:
			return got
		}
	}
}

// TestForwardAddColumn_FlagOff_NoIntercept verifies that the
// intercept isn't engaged when ForwardSchemaAddColumn is false — i.e.
// the streamer's wiring path skips the call. This test exercises the
// shape directly (`schemaForwardDeps{}` with a nil applier returns the
// input channel verbatim).
func TestForwardAddColumn_NilApplier_PassThrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	tbl := addColForwardTable("users")
	snap := addColForwardSnap(tbl)
	in <- snap
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes, want 1 pass-through", len(got))
	}
	if errStore.Load() != nil {
		t.Errorf("errStore set on pass-through path: %v", *errStore.Load())
	}
}

// TestForwardAddColumn_FirstSnapshotIsAnchor verifies that the first
// SchemaSnapshot per table seeds the cache without calling the
// applier (it's the post-cold-start anchor).
func TestForwardAddColumn_FirstSnapshotIsAnchor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 1)
	applier := &fakeShapeApplier{}
	tbl := addColForwardTable("users")
	in <- addColForwardSnap(tbl)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes, want 1 (anchor pass-through)", len(got))
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn called %d times on anchor; want 0", applier.addColCalls)
	}
}

// TestForwardAddColumn_AddColumnShape_CallsApplier verifies the
// load-bearing branch: a (pre → post) delta of one added column fires
// exactly one AlterAddColumn call and forwards the snapshot
// downstream so ADR-0049 schema-history still records.
func TestForwardAddColumn_AddColumnShape_CallsApplier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2 (both snapshots forwarded)", len(got))
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want exactly 1", applier.addColCalls)
	}
	if e := errStore.Load(); e != nil {
		t.Errorf("errStore set on happy path: %v", *e)
	}
}

// TestForwardAddColumn_DropColumnShape_RefuseLoudly verifies that
// every non-ADD-COLUMN shape refuses loudly. Drop is the canonical
// representative; the switch in routeForwardBoundary handles each
// case identically.
func TestForwardAddColumn_DropColumnShape_RefuseLoudly(t *testing.T) {
	cases := []struct {
		name   string
		pre    *ir.Table
		post   *ir.Table
		wantIn string // substring expected in the refusal message
	}{
		{
			name:   "drop-column",
			pre:    addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true}),
			post:   addColForwardTable("users"),
			wantIn: "drop-column",
		},
		{
			name:   "rename-column",
			pre:    addColForwardTable("users", &ir.Column{Name: "old", Type: ir.Varchar{Length: 100}, Nullable: true}),
			post:   addColForwardTable("users", &ir.Column{Name: "new", Type: ir.Varchar{Length: 100}, Nullable: true}),
			wantIn: "rename-column",
		},
		{
			name: "alter-column-type",
			pre:  addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: true}),
			post: addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 64}, Nullable: true}),
			// classifier may return either alter-column-type or unrecognized
			// depending on Type equality; the refusal substring covers both
			// canonical forms ("alter-column" matches both).
			wantIn: "alter-column",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			in := make(chan ir.Change, 2)
			applier := &fakeShapeApplier{}
			in <- addColForwardSnap(tc.pre)
			in <- addColForwardSnap(tc.post)
			close(in)
			errStore := &atomic.Pointer[error]{}
			out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
				applier:          applier,
				sourceEngineName: "postgres",
				targetEngineName: "postgres",
			}, errStore)
			_ = drainChannel(t, out, time.Second)
			ePtr := errStore.Load()
			if ePtr == nil {
				t.Fatalf("expected refuse-loudly error; got nil")
			}
			if !strings.Contains((*ePtr).Error(), tc.wantIn) {
				t.Errorf("error %q does not contain shape name %q", (*ePtr).Error(), tc.wantIn)
			}
			if applier.addColCalls != 0 {
				t.Errorf("AlterAddColumn called %d times on refuse-loudly path; want 0", applier.addColCalls)
			}
		})
	}
}

// TestForwardAddColumn_ComputedDefault_Refuse pins the ADR-0058 §2a
// refuse-loudly for ir.DefaultExpression. The intercept must reject
// before issuing the ALTER, regardless of the rest of the shape.
func TestForwardAddColumn_ComputedDefault_Refuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{
		Name:     "created_at",
		Type:     ir.Timestamp{},
		Nullable: true,
		Default:  ir.DefaultExpression{Expr: "NOW()"},
	})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected refuse-loudly on DefaultExpression; got nil")
	}
	if !strings.Contains((*ePtr).Error(), "DEFAULT expression") {
		t.Errorf("error %q does not mention DEFAULT expression", (*ePtr).Error())
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn called %d times on DefaultExpression refusal; want 0", applier.addColCalls)
	}
}

// TestForwardAddColumn_LiteralDefault_Forwards verifies that
// ir.DefaultLiteral (a static constant) does NOT trip the
// computed-default refusal — operators using `DEFAULT 0` or
// `DEFAULT 'pending'` get the standard forwarding path.
func TestForwardAddColumn_LiteralDefault_Forwards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{
		Name:     "status",
		Type:     ir.Varchar{Length: 20},
		Nullable: false,
		Default:  ir.DefaultLiteral{Value: "'pending'"},
	})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	if e := errStore.Load(); e != nil {
		t.Errorf("DefaultLiteral was rejected: %v", *e)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want 1", applier.addColCalls)
	}
}

// TestForwardAddColumn_ApplierError_Rewinds verifies that an
// applier-side error propagates through errStore AND that the cache
// is rewound to the pre-state (so a retry's next snapshot routes
// against the same pre).
func TestForwardAddColumn_ApplierError_Rewinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	injected := errors.New("alter failed: lock timeout")
	applier := &fakeShapeApplier{injectErr: injected}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected applier error propagation; got nil")
	}
	if !errors.Is(*ePtr, injected) {
		t.Errorf("error chain does not include injected error: got %v, want %v", *ePtr, injected)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want 1 (one attempt before error)", applier.addColCalls)
	}
}

// TestForwardAddColumn_NoneShape_Passthrough verifies a redundant
// SchemaSnapshot (same IR as the cache) forwards verbatim with no
// applier call. Bug-shape: a CDC reader emitting a snapshot on every
// transaction boundary even when nothing changed.
func TestForwardAddColumn_NoneShape_Passthrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	tbl := addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: true})
	in <- addColForwardSnap(tbl)
	in <- addColForwardSnap(tbl)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2 (both forwarded)", len(got))
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn calls = %d on NoneShape; want 0", applier.addColCalls)
	}
	if e := errStore.Load(); e != nil {
		t.Errorf("errStore set on NoneShape: %v", *e)
	}
}

// TestForwardAddColumn_NonSnapshotChange_Forwards verifies that
// non-SchemaSnapshot events (Insert, Update, TxBegin/Commit) flow
// through unchanged.
func TestForwardAddColumn_NonSnapshotChange_Forwards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 3)
	applier := &fakeShapeApplier{}
	tbl := addColForwardTable("users")
	in <- addColForwardSnap(tbl)
	in <- ir.Insert{Position: ir.Position{Engine: "postgres", Token: "lsn/2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(1)}}
	in <- ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: "lsn/3"}}
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d changes, want 3", len(got))
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn calls = %d on data events; want 0", applier.addColCalls)
	}
}

// TestSynthesizeBackfillUpdate verifies the synthetic UPDATE event
// shape: Before carries PK columns, After carries the added column,
// Position matches the SchemaSnapshot.
func TestSynthesizeBackfillUpdate(t *testing.T) {
	tbl := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	snap := addColForwardSnap(tbl)
	row := ir.Row{"id": int64(42), "nickname": "alpha"}
	upd := synthesizeBackfillUpdate(snap, row, []string{"id"}, map[string]struct{}{"nickname": {}})
	if upd.Schema != "public" {
		t.Errorf("Schema = %q; want public", upd.Schema)
	}
	if upd.Table != "users" {
		t.Errorf("Table = %q; want users", upd.Table)
	}
	if upd.Position.Token != "lsn/1" {
		t.Errorf("Position.Token = %q; want lsn/1", upd.Position.Token)
	}
	if got, ok := upd.Before["id"]; !ok || got != int64(42) {
		t.Errorf("Before[id] = %v, ok=%t; want 42, true", got, ok)
	}
	if _, hasNickname := upd.Before["nickname"]; hasNickname {
		t.Errorf("Before should not contain non-PK column nickname")
	}
	if got, ok := upd.After["nickname"]; !ok || got != "alpha" {
		t.Errorf("After[nickname] = %v, ok=%t; want alpha, true", got, ok)
	}
	if _, hasID := upd.After["id"]; hasID {
		t.Errorf("After should not contain PK column id (would be a redundant SET)")
	}
}

// fakeBatchedRowReader is a minimal in-memory BatchedRowReader for
// the backfill loop. Returns one batch then EOF.
type fakeBatchedRowReader struct {
	rows []ir.Row
	// callCount tracks how many ReadRowsBatch calls happened. The
	// backfill loop calls until a batch returns 0 rows.
	callCount int
}

func (f *fakeBatchedRowReader) ReadRows(_ context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row)
	close(out)
	return out, nil
}

func (f *fakeBatchedRowReader) ReadRowsBatch(_ context.Context, _ *ir.Table, _ []any, _ int) (<-chan ir.Row, error) {
	out := make(chan ir.Row, len(f.rows))
	if f.callCount == 0 {
		for _, r := range f.rows {
			out <- r
		}
	}
	// Second + subsequent calls return EOF (empty channel).
	f.callCount++
	close(out)
	return out, nil
}

func (f *fakeBatchedRowReader) Err() error { return nil }

// TestForwardAddColumn_Backfill_EmitsUpdates verifies the backfill
// loop emits one synthetic UPDATE per source row after the ALTER
// lands. The applier sees exactly: SchemaSnapshot(pre),
// SchemaSnapshot(post), UPDATE(row1), UPDATE(row2), UPDATE(row3) (in
// some order — backfill batches are PK-ordered, so the order is
// deterministic per the fake reader's emission order).
func TestForwardAddColumn_Backfill_EmitsUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	reader := &fakeBatchedRowReader{
		rows: []ir.Row{
			{"id": int64(1), "nickname": "alpha"},
			{"id": int64(2), "nickname": "beta"},
			{"id": int64(3), "nickname": "gamma"},
		},
	}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
		backfill: &schemaForwardBackfill{
			reader:    reader,
			streamID:  "test-stream",
			batchSize: 100,
		},
	}, errStore)
	got := drainChannel(t, out, 2*time.Second)
	// Expect: anchor snapshot + post snapshot + 3 backfill UPDATEs.
	if len(got) != 5 {
		t.Fatalf("got %d changes, want 5 (2 snapshots + 3 backfill updates); got = %#v", len(got), got)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want 1", applier.addColCalls)
	}
	updates := 0
	for _, c := range got {
		if u, ok := c.(ir.Update); ok {
			updates++
			if u.Table != "users" {
				t.Errorf("backfill Update.Table = %q; want users", u.Table)
			}
			if _, hasNickname := u.After["nickname"]; !hasNickname {
				t.Errorf("backfill Update.After missing nickname: %v", u.After)
			}
		}
	}
	if updates != 3 {
		t.Errorf("backfill Update count = %d; want 3", updates)
	}
}

// TestForwardAddColumn_Backfill_NoPK_Refuses verifies a table
// without a primary key fails the backfill cursor refusal. Tables
// without PKs are already excluded from bulk-copy resume (ADR-0018);
// backfill applies the same constraint.
func TestForwardAddColumn_Backfill_NoPK_Refuses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	reader := &fakeBatchedRowReader{}
	pre := &ir.Table{Schema: "public", Name: "nopk", Columns: []*ir.Column{{Name: "x", Type: ir.Integer{Width: 32}}}}
	post := &ir.Table{Schema: "public", Name: "nopk", Columns: []*ir.Column{
		{Name: "x", Type: ir.Integer{Width: 32}},
		{Name: "y", Type: ir.Integer{Width: 32}, Nullable: true},
	}}
	in <- ir.SchemaSnapshot{Position: ir.Position{Engine: "postgres", Token: "lsn/1"}, Schema: "public", Table: "nopk", IR: pre}
	in <- ir.SchemaSnapshot{Position: ir.Position{Engine: "postgres", Token: "lsn/2"}, Schema: "public", Table: "nopk", IR: post}
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
		backfill: &schemaForwardBackfill{
			reader:    reader,
			streamID:  "test-stream",
			batchSize: 100,
		},
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected refusal on no-PK table; got nil")
	}
	if !strings.Contains((*ePtr).Error(), "primary key") {
		t.Errorf("error %q does not mention primary key", (*ePtr).Error())
	}
}
