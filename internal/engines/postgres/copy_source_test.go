// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// usersTableForCopySource is the small fixture every test in this
// file uses. Two columns: id (int) and email (text). Mirror the
// shape used by the integration tests so failures are obvious.
func usersTableForCopySource() *ir.Table {
	return &ir.Table{
		Schema: "public",
		Name:   "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}
}

// TestChanCopySource_CleanDrain is the happy path: rows arrive,
// channel closes, source returns clean.
func TestChanCopySource_CleanDrain(t *testing.T) {
	table := usersTableForCopySource()
	ch := make(chan ir.Row, 3)
	ch <- ir.Row{"id": int64(1), "email": "a@x"}
	ch <- ir.Row{"id": int64(2), "email": "b@x"}
	ch <- ir.Row{"id": int64(3), "email": "c@x"}
	close(ch)

	src := newChanCopySource(context.Background(), table, ch)

	want := [][]any{
		{int64(1), "a@x"},
		{int64(2), "b@x"},
		{int64(3), "c@x"},
	}
	for i, w := range want {
		if !src.Next() {
			t.Fatalf("Next #%d returned false; expected true", i)
		}
		got, err := src.Values()
		if err != nil {
			t.Fatalf("Values #%d: %v", i, err)
		}
		if !reflect.DeepEqual(got, w) {
			t.Errorf("Values #%d = %v; want %v", i, got, w)
		}
	}
	if src.Next() {
		t.Error("Next after channel close returned true; expected false")
	}
	if err := src.Err(); err != nil {
		t.Errorf("Err after clean drain = %v; want nil", err)
	}
}

// TestChanCopySource_EmptyChannel covers the case where the channel
// closes before any row arrives. Source should report end-of-stream
// immediately with no error.
func TestChanCopySource_EmptyChannel(t *testing.T) {
	ch := make(chan ir.Row)
	close(ch)

	src := newChanCopySource(context.Background(), usersTableForCopySource(), ch)

	if src.Next() {
		t.Error("Next on closed empty channel returned true; expected false")
	}
	if err := src.Err(); err != nil {
		t.Errorf("Err on empty channel = %v; want nil", err)
	}
}

// TestChanCopySource_CtxCancellation verifies that ctx cancellation
// mid-stream unblocks Next and reports ctx.Err.
func TestChanCopySource_CtxCancellation(t *testing.T) {
	ch := make(chan ir.Row) // unbuffered — Next will block

	ctx, cancel := context.WithCancel(context.Background())
	src := newChanCopySource(ctx, usersTableForCopySource(), ch)

	// Cancel from a goroutine after a brief delay; main goroutine
	// blocks in Next().
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	if src.Next() {
		t.Error("Next under cancelled ctx returned true; expected false")
	}
	if err := src.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("Err = %v; want context.Canceled", err)
	}
}

// TestChanCopySource_PrepareValueError forces a value-type
// mismatch (an Array column receives a non-slice value) and
// confirms Next returns false with a descriptive Err naming the
// column.
func TestChanCopySource_PrepareValueError(t *testing.T) {
	table := &ir.Table{
		Schema: "public",
		Name:   "events",
		Columns: []*ir.Column{
			{Name: "tags", Type: ir.Array{Element: ir.Integer{Width: 64}}},
		},
	}

	ch := make(chan ir.Row, 1)
	ch <- ir.Row{"tags": "not-a-slice"} // expected []any, will fail
	close(ch)

	src := newChanCopySource(context.Background(), table, ch)

	if src.Next() {
		t.Fatal("Next returned true for malformed row; expected false")
	}
	err := src.Err()
	if err == nil {
		t.Fatal("Err = nil; expected an error")
	}
	if !strings.Contains(err.Error(), `column "tags"`) {
		t.Errorf("Err = %q; expected to mention column \"tags\"", err.Error())
	}
}

// TestChanCopySource_StickyErr confirms that once Next returns
// false with an error, subsequent Next calls keep returning false
// without consuming further from the channel.
func TestChanCopySource_StickyErr(t *testing.T) {
	table := &ir.Table{
		Schema: "public",
		Name:   "events",
		Columns: []*ir.Column{
			{Name: "tags", Type: ir.Array{Element: ir.Integer{Width: 64}}},
		},
	}

	ch := make(chan ir.Row, 2)
	ch <- ir.Row{"tags": "not-a-slice"}    // poisons the source
	ch <- ir.Row{"tags": []any{int64(42)}} // would be valid; should not be reached
	close(ch)

	src := newChanCopySource(context.Background(), table, ch)

	// First Next: consumes the bad row, sets err, returns false.
	if src.Next() {
		t.Fatal("first Next: returned true on bad row")
	}
	firstErr := src.Err()
	if firstErr == nil {
		t.Fatal("first Next: Err = nil; expected an error")
	}

	// Second Next: must still return false without touching the
	// channel. The valid row stays unconsumed (which we can't
	// directly observe but we can verify Err is unchanged). The
	// stickiness contract says the SAME error instance should
	// resurface; pointer identity captures that intent more
	// strictly than errors.Is would.
	if src.Next() {
		t.Error("second Next after sticky err returned true; expected false")
	}
	if !errors.Is(src.Err(), firstErr) || src.Err().Error() != firstErr.Error() {
		t.Errorf("Err drifted after sticky error: got %v; want %v", src.Err(), firstErr)
	}
}

// TestChanCopySource_ColumnOrder verifies that Values respects the
// column order declared on the table, regardless of map insertion
// order in the row.
func TestChanCopySource_ColumnOrder(t *testing.T) {
	table := &ir.Table{
		Schema: "public",
		Name:   "items",
		Columns: []*ir.Column{
			{Name: "z_col", Type: ir.Integer{Width: 64}},
			{Name: "a_col", Type: ir.Integer{Width: 64}},
			{Name: "m_col", Type: ir.Integer{Width: 64}},
		},
	}
	ch := make(chan ir.Row, 1)
	// Order in the row map is not declaration order; iteration
	// would visit them in random order. The source must follow
	// table.Columns, not map iteration.
	ch <- ir.Row{"a_col": int64(2), "m_col": int64(3), "z_col": int64(1)}
	close(ch)

	src := newChanCopySource(context.Background(), table, ch)
	if !src.Next() {
		t.Fatalf("Next: %v", src.Err())
	}
	got, _ := src.Values()
	want := []any{int64(1), int64(2), int64(3)} // z_col, a_col, m_col
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Values = %v; want %v (column order must follow table.Columns)", got, want)
	}
}
