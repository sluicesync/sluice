// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ukrHarness drives interceptUniqueKeyChecks with a controlled clock: each
// change is sent after advancing the clock by its paired duration.
type ukrHarness struct {
	clock atomic.Int64 // nanoseconds; read by the intercept's goroutine
}

func (h *ukrHarness) now() time.Time { return time.Unix(0, h.clock.Load()) }

func (h *ukrHarness) run(t *testing.T, ctx context.Context, cfg uniqueKeyChecks, changes []ir.Change, advance []time.Duration) ([]ir.Change, error) {
	t.Helper()
	in := make(chan ir.Change)
	var store atomic.Pointer[error]
	out := interceptUniqueKeyChecks(ctx, in, cfg, &store, h.now)
	var got []ir.Change
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range out {
			got = append(got, c)
		}
	}()
	for i, c := range changes {
		h.clock.Add(int64(advance[i]))
		select {
		case in <- c:
		case <-done:
		}
	}
	close(in)
	<-done
	var err error
	if p := store.Load(); p != nil {
		err = *p
	}
	return got, err
}

func ukrSnap(table string, cols ...string) ir.SchemaSnapshot {
	t := &ir.Table{Name: table}
	for _, c := range cols {
		t.Columns = append(t.Columns, &ir.Column{Name: c})
	}
	return ir.SchemaSnapshot{Table: table, IR: t}
}

// TestInterceptUniqueKeyChecks pins the mid-stream checks' placement: the
// periodic re-check runs once a period has passed, before the first change
// of a transaction that touches a watched table (held until then), never
// for a transaction that touches none or for an idle stream; a refusal
// stops the stream before that transaction; read failures WARN until
// maxFailures in a row, then stop it; a cancelled context stops quietly;
// the boundary check runs on a moved column list, not on a table's first
// snapshot.
func TestInterceptUniqueKeyChecks(t *testing.T) {
	begin, commit := ir.TxBegin{}, ir.TxCommit{}
	watchedRow, otherRow := ir.Insert{Table: "kd"}, ir.Insert{Table: "other"}
	watches := func(_, table string) bool { return table == "kd" }
	refuse := func(n *int) func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
		return func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
			*n++
			return &ir.SourceUniqueKeyAbsentError{When: ir.UniqueKeyDoorMidStream}, nil
		}
	}
	pass := func(n *int) func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
		return func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) { *n++; return nil, nil }
	}
	periodic := func(check func(context.Context) (*ir.SourceUniqueKeyAbsentError, error)) uniqueKeyChecks {
		return uniqueKeyChecks{period: 10 * time.Second, periodic: check, watches: watches, maxFailures: 3}
	}
	ctx := context.Background()

	t.Run("a due re-check refuses before the watched transaction's first change", func(t *testing.T) {
		var h ukrHarness
		checks := 0
		got, err := h.run(t, ctx, periodic(refuse(&checks)),
			[]ir.Change{begin, watchedRow, commit, begin, otherRow, watchedRow, commit},
			// Due from the second TxBegin on.
			[]time.Duration{0, 0, 0, 11 * time.Second, 0, 0, 0})
		if checks != 1 || len(got) != 3 || !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("checks=%d forwarded=%d err=%v; want one check, only the first transaction forwarded, the refusal",
				checks, len(got), err)
		}
	})
	t.Run("a transaction touching no watched table is not checked, and is forwarded whole", func(t *testing.T) {
		var h ukrHarness
		checks := 0
		got, err := h.run(t, ctx, periodic(refuse(&checks)),
			[]ir.Change{begin, otherRow, commit, begin, commit},
			[]time.Duration{11 * time.Second, 0, 0, 0, 0})
		if checks != 0 || len(got) != 5 || err != nil {
			t.Fatalf("checks=%d forwarded=%d err=%v; a heartbeat or another table's write must cost nothing", checks, len(got), err)
		}
	})
	t.Run("no check before a period has passed", func(t *testing.T) {
		var h ukrHarness
		checks := 0
		got, err := h.run(t, ctx, periodic(refuse(&checks)), []ir.Change{begin, watchedRow, commit},
			[]time.Duration{time.Second, 0, 0})
		if checks != 0 || len(got) != 3 || err != nil {
			t.Fatalf("checks=%d forwarded=%d err=%v", checks, len(got), err)
		}
	})
	t.Run("a watched row outside a transaction is checked before it is forwarded", func(t *testing.T) {
		var h ukrHarness
		checks := 0
		got, _ := h.run(t, ctx, periodic(refuse(&checks)), []ir.Change{watchedRow}, []time.Duration{11 * time.Second})
		if checks != 1 || len(got) != 0 {
			t.Fatalf("checks=%d forwarded=%d", checks, len(got))
		}
	})
	t.Run("a passing check releases the held transaction in order", func(t *testing.T) {
		var h ukrHarness
		checks := 0
		changes := []ir.Change{begin, otherRow, watchedRow, commit}
		got, err := h.run(t, ctx, periodic(pass(&checks)), changes, []time.Duration{11 * time.Second, 0, 0, 0})
		if checks != 1 || err != nil || len(got) != 4 {
			t.Fatalf("checks=%d forwarded=%d err=%v", checks, len(got), err)
		}
		for i := range changes {
			if !reflect.DeepEqual(got[i], changes[i]) {
				t.Fatalf("change %d out of order: %#v", i, got[i])
			}
		}
	})
	t.Run("the hold is bounded", func(t *testing.T) {
		var h ukrHarness
		checks := 0
		changes := []ir.Change{begin}
		for i := 0; i < uniqueKeyHoldCap; i++ {
			changes = append(changes, otherRow)
		}
		changes = append(changes, commit)
		advance := make([]time.Duration, len(changes))
		advance[0] = 11 * time.Second
		got, _ := h.run(t, ctx, periodic(pass(&checks)), changes, advance)
		if checks != 1 || len(got) != len(changes) {
			t.Fatalf("checks=%d forwarded=%d of %d", checks, len(got), len(changes))
		}
	})
	t.Run("read failures WARN, then stop the stream after maxFailures in a row", func(t *testing.T) {
		var h ukrHarness
		failing := func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) { return nil, errors.New("catalog down") }
		var changes []ir.Change
		var advance []time.Duration
		for i := 0; i < 4; i++ {
			changes = append(changes, begin, watchedRow, commit)
			advance = append(advance, 11*time.Second, 0, 0)
		}
		got, err := h.run(t, ctx, periodic(failing), changes, advance)
		if err == nil || !strings.Contains(err.Error(), SourceUniqueKeyRecheckFailedMarker) || errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("err=%v; want the fail-closed %s, not the refusal marker", err, SourceUniqueKeyRecheckFailedMarker)
		}
		if len(got) != 6 {
			t.Fatalf("forwarded=%d; want the two transactions whose check failed before the limit", len(got))
		}
	})
	t.Run("a success resets the failure count", func(t *testing.T) {
		var h ukrHarness
		n := 0
		flaky := func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
			n++
			if n%2 == 0 {
				return nil, nil
			}
			return nil, errors.New("blip")
		}
		var changes []ir.Change
		var advance []time.Duration
		for i := 0; i < 8; i++ {
			changes = append(changes, begin, watchedRow, commit)
			advance = append(advance, 11*time.Second, 0, 0)
		}
		_, err := h.run(t, ctx, periodic(flaky), changes, advance)
		if err != nil {
			t.Fatalf("alternating blips stopped the stream: %v", err)
		}
	})
	t.Run("a cancelled context stops quietly", func(t *testing.T) {
		var h ukrHarness
		cctx, cancel := context.WithCancel(ctx)
		check := func(context.Context) (*ir.SourceUniqueKeyAbsentError, error) {
			cancel()
			return nil, context.Canceled
		}
		_, err := h.run(t, cctx, periodic(check), []ir.Change{begin, watchedRow, commit}, []time.Duration{11 * time.Second, 0, 0})
		if err != nil {
			t.Fatalf("a cancellation surfaced %v", err)
		}
	})
	refusingBoundary := func(tables *[]string) func(context.Context, string, string) (*ir.SourceUniqueKeyAbsentError, error) {
		return func(_ context.Context, _, table string) (*ir.SourceUniqueKeyAbsentError, error) {
			*tables = append(*tables, table)
			return &ir.SourceUniqueKeyAbsentError{When: ir.UniqueKeyDoorAtSchemaChange}, nil
		}
	}
	keyByTable := func(_, table string) string { return uniqueKeyColsKey("", table) }
	t.Run("a FIRST snapshot that differs from the open door's read is checked (round-2 HIGH-1)", func(t *testing.T) {
		var h ukrHarness
		var tables []string
		cfg := uniqueKeyChecks{
			maxFailures: 3, boundary: refusingBoundary(&tables), colsKey: keyByTable,
			seedCols: map[string]string{uniqueKeyColsKey("", "kd"): columnList(ukrSnap("kd", "id", "k", "b").IR)},
		}
		// The table's first snapshot of the run already lacks b: the DROP
		// COLUMN came before the table's first change in the run.
		got, err := h.run(t, ctx, cfg, []ir.Change{ukrSnap("kd", "id", "k"), watchedRow}, make([]time.Duration, 2))
		if strings.Join(tables, ",") != "kd" || !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) || len(got) != 0 {
			t.Fatalf("checked %v, forwarded %d, err=%v; want the first snapshot checked and withheld", tables, len(got), err)
		}
	})
	t.Run("a first snapshot matching the open door's read is not checked", func(t *testing.T) {
		var h ukrHarness
		var tables []string
		cfg := uniqueKeyChecks{
			maxFailures: 3, boundary: refusingBoundary(&tables), colsKey: keyByTable,
			seedCols: map[string]string{uniqueKeyColsKey("", "kd"): columnList(ukrSnap("kd", "id", "k").IR)},
		}
		got, err := h.run(t, ctx, cfg, []ir.Change{ukrSnap("kd", "k", "id"), watchedRow}, make([]time.Duration, 2))
		if len(tables) != 0 || err != nil || len(got) != 2 {
			t.Fatalf("checked %v, forwarded %d, err=%v", tables, len(got), err)
		}
	})
	t.Run("a boundary read failure stops the stream at once, outside the periodic budget (round-2 MEDIUM-1)", func(t *testing.T) {
		var h ukrHarness
		cfg := uniqueKeyChecks{
			maxFailures: 3, colsKey: keyByTable,
			seedCols: map[string]string{uniqueKeyColsKey("", "kd"): columnList(ukrSnap("kd", "id", "k", "b").IR)},
			boundary: func(context.Context, string, string) (*ir.SourceUniqueKeyAbsentError, error) {
				return nil, errors.New("catalog down")
			},
		}
		got, err := h.run(t, ctx, cfg, []ir.Change{ukrSnap("kd", "id", "k"), watchedRow}, make([]time.Duration, 2))
		if err == nil || !strings.Contains(err.Error(), SourceUniqueKeyRecheckFailedMarker) || errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("err=%v; want the fail-closed %s", err, SourceUniqueKeyRecheckFailedMarker)
		}
		if len(got) != 0 {
			t.Fatalf("forwarded=%d; the unjudged snapshot and what follows must be withheld", len(got))
		}
	})
	t.Run("a hold released by the cap on its commit leaves the transaction closed (round-2 L-b)", func(t *testing.T) {
		var h ukrHarness
		checks := 0
		changes := []ir.Change{begin}
		for i := 0; i < uniqueKeyHoldCap-2; i++ {
			changes = append(changes, otherRow)
		}
		// The cap lands on the commit; then a row outside any transaction. The
		// unwatched row between them is received only once the commit has
		// been judged, so the clock moves for the watched row alone.
		changes = append(changes, commit, otherRow, watchedRow)
		advance := make([]time.Duration, len(changes))
		advance[0] = 11 * time.Second
		advance[len(advance)-1] = 11 * time.Second
		_, _ = h.run(t, ctx, periodic(pass(&checks)), changes, advance)
		if checks != 2 {
			t.Fatalf("checks=%d; want 2 — the cap's check, then the out-of-transaction watched row's", checks)
		}
	})
	t.Run("the boundary check runs on a moved column list, not on an unseeded first snapshot", func(t *testing.T) {
		var h ukrHarness
		var tables []string
		cfg := uniqueKeyChecks{maxFailures: 3, boundary: func(_ context.Context, _, table string) (*ir.SourceUniqueKeyAbsentError, error) {
			tables = append(tables, table)
			return &ir.SourceUniqueKeyAbsentError{When: ir.UniqueKeyDoorAtSchemaChange}, nil
		}}
		got, err := h.run(t, ctx, cfg,
			[]ir.Change{ukrSnap("kd", "a", "b", "id"), watchedRow, ukrSnap("kd", "b", "a", "id"), ukrSnap("kd", "a", "id"), watchedRow},
			make([]time.Duration, 5))
		if strings.Join(tables, ",") != "kd" || !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("checked %v, err=%v; want one check, at the dropped column", tables, err)
		}
		// first snapshot, the row, the reordered (same set) snapshot.
		if len(got) != 3 {
			t.Fatalf("forwarded=%d; want 3, the snapshot that moved the columns withheld", len(got))
		}
	})
	t.Run("nothing armed is a pass-through", func(t *testing.T) {
		in := make(chan ir.Change)
		var store atomic.Pointer[error]
		var h ukrHarness
		if out := interceptUniqueKeyChecks(ctx, in, uniqueKeyChecks{}, &store, h.now); out != (<-chan ir.Change)(in) { //nolint:gocritic // typeUnparen: the conversion is what makes the comparison typecheck
			t.Fatal("a stream with nothing to check grew a stage")
		}
	})
}

// TestInterceptErrorPending_EverySink pins that the reshard auto-follow
// guard sees every intercept error sink phaseSettleDispatch reads, so a
// reopen cannot run past a stored refusal.
func TestInterceptErrorPending_EverySink(t *testing.T) {
	refusal := errors.New("refused")
	for name, sink := range map[string]func(s *Streamer) *atomic.Pointer[error]{
		"schemaSnapshotErr":   func(s *Streamer) *atomic.Pointer[error] { return &s.schemaSnapshotErr },
		"whereFilterErr":      func(s *Streamer) *atomic.Pointer[error] { return &s.whereFilterErr },
		"uniqueKeyRecheckErr": func(s *Streamer) *atomic.Pointer[error] { return &s.uniqueKeyRecheckErr },
	} {
		s := &Streamer{}
		if s.interceptErrorPending() {
			t.Fatalf("%s: pending with nothing stored", name)
		}
		sink(s).Store(&refusal)
		if !s.interceptErrorPending() {
			t.Errorf("%s: a stored refusal is not pending, so a reshard reopen would run past it", name)
		}
	}
}
