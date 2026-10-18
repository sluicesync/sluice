// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// liveAddedApplier is a fakeApplier carrying a live-added set.
type liveAddedApplier struct {
	*fakeApplier
	*fakeLiveAddedReader
}

// TestLiveAddedExcluded is R6-S1's predicate: an explicit --exclude-table
// naming a live-added table is the contradiction; an include-mode or
// unfiltered stream has none, and neither has a live-added table no
// exclusion names.
func TestLiveAddedExcluded(t *testing.T) {
	excl := migcore.TableFilter{Exclude: []string{"tmp_*"}}
	for _, c := range []struct {
		name   string
		filter migcore.TableFilter
		live   []string
		want   []string
	}{
		{"exclude glob names it", excl, []string{"orders", "tmp_keep", "tmp_a"}, []string{"tmp_a", "tmp_keep"}},
		{"exclude names none", excl, []string{"orders"}, nil},
		{"include mode", migcore.TableFilter{Include: []string{"users"}}, []string{"tmp_keep"}, nil},
		{"unfiltered", migcore.TableFilter{}, []string{"tmp_keep"}, nil},
		{"control table never", migcore.TableFilter{Exclude: []string{"sluice_*"}}, []string{"sluice_cdc_state"}, nil},
	} {
		if got := liveAddedExcluded(c.filter, c.live); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// TestRefuseLiveAddedTableExcluded_AtStart is R6-S1 at a start: the
// contradiction refuses terminally (INCLUDE-TABLE-SCOPE-DRIFT, the
// supervisor's do-not-restart roster) with the marker and both remedies;
// --reset-target-data, which deletes the set, does not refuse.
func TestRefuseLiveAddedTableExcluded_AtStart(t *testing.T) {
	ctx := context.Background()
	app := &liveAddedApplier{fakeApplier: &fakeApplier{}, fakeLiveAddedReader: &fakeLiveAddedReader{}}
	app.Set([]string{"tmp_keep", "orders"})
	pg := &resumeDispatchEngine{name: "postgres"}
	s := &Streamer{Source: pg, Target: pg, Filter: migcore.TableFilter{Exclude: []string{"tmp_*"}}}

	err := s.phaseReadLiveAddedScope(ctx, app, "st")
	if err == nil {
		t.Fatal("a live-added table an --exclude-table names started")
	}
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || refusalARestartRepeats(err) == nil {
		t.Fatalf("not terminal: %v", err)
	}
	for _, want := range []string{LiveAddedTableExcludedMarker, "tmp_keep", "narrow --exclude-table", "--abandon --source-driver postgres --source <SOURCE-DSN>"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "orders") {
		t.Errorf("refusal names a table no exclusion matches: %v", err)
	}

	s.ResetTargetData = true
	if err := s.phaseReadLiveAddedScope(ctx, app, "st"); err != nil {
		t.Fatalf("--reset-target-data deletes the live-added set; refused anyway: %v", err)
	}
	if s.startLiveAdded != nil {
		t.Fatalf("--reset-target-data kept the set %v for the copy scope; it deletes it", s.startLiveAdded)
	}
	s.ResetTargetData = false
	app.Set([]string{"orders"})
	if err := s.phaseReadLiveAddedScope(ctx, app, "st"); err != nil {
		t.Fatalf("no contradiction, refused: %v", err)
	}

	// R7b: the set becomes part of the cold start's copy scope.
	s.Filter = migcore.TableFilter{Include: []string{"kd"}}
	if err := s.phaseReadLiveAddedScope(ctx, app, "st"); err != nil {
		t.Fatal(err)
	}
	if cf := s.copyFilter(); !cf.Allows("orders") || !cf.Allows("kd") || cf.Allows("other") {
		t.Fatalf("copy scope: orders %v kd %v other %v; want the include list plus the live-added set",
			cf.Allows("orders"), cf.Allows("kd"), cf.Allows("other"))
	}
	if s.Filter.Allows("orders") {
		t.Fatal("the stream's own filter was widened; only the copy scope is")
	}
}

// TestLiveAddedExcluded_RunningStreamStops is R6-S1 in a running stream: the
// poll that first reads the contradiction raises it, the dispatch filter
// forwards nothing it receives from then on and closes, and the refusal says
// what the poll window means for keeping the table.
func TestLiveAddedExcluded_RunningStreamStops(t *testing.T) {
	setPollIntervalForTest(t, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := migcore.TableFilter{Exclude: []string{"tmp_*"}}
	live := &liveAddedFilter{}
	reader := &fakeLiveAddedReader{}
	retired := make(chan []string, 1)
	check := liveAddedCheck{
		streamID: "st", base: base, source: "postgres", target: "postgres",
		retire: func(_ context.Context, tables []string) error { retired <- tables; return nil },
	}
	go pollLiveAddedTables(ctx, reader, check, live)

	in := make(chan ir.Change, 8)
	out := filterChangesWithLiveAdd(ctx, in, base, live)
	in <- ir.Insert{Schema: "s", Table: "users", Row: ir.Row{"id": int64(1)}}
	if c := <-out; c.(ir.Insert).Table != "users" {
		t.Fatalf("before the add: got %v", c)
	}

	reader.Set([]string{"tmp_keep"})
	select {
	case <-live.refusedChan():
	case <-time.After(2 * time.Second):
		t.Fatal("the poll never raised the contradiction")
	}
	in <- ir.Insert{Schema: "s", Table: "users", Row: ir.Row{"id": int64(2)}}
	select {
	case c, ok := <-out:
		if ok {
			t.Fatalf("the filter forwarded %v after the refusal", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the filter did not close after the refusal")
	}
	err := live.refusalErr()
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
		t.Fatalf("refusal = %v", err)
	}
	for _, want := range []string{
		LiveAddedTableExcludedMarker, "tmp_keep", "stopped when it saw the add", "recorded the table as dropped",
		"only narrows the exclusion refuses", "--source-driver postgres --source <SOURCE-DSN>",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q: %v", want, err)
		}
	}
	// R8: the refusal was written to the control row, before it was raised.
	select {
	case got := <-retired:
		if !reflect.DeepEqual(got, []string{"tmp_keep"}) {
			t.Errorf("retired %v, want [tmp_keep]", got)
		}
	default:
		t.Error("the running refusal did not record the excluded table as dropped (RetireLiveAddedTables)")
	}
	if live.Contains("tmp_keep") {
		t.Error("the contradicting set was installed")
	}
	s := &Streamer{}
	s.liveFilterRef.Store(live)
	if !s.interceptErrorPending() {
		t.Error("interceptErrorPending misses the refusal, so a reshard reopen could run past it")
	}
}

// TestLiveAddedExcluded_RetireFailureStillStops is R8's failure arm: a
// control-row write that fails does not keep the stream running; the
// refusal says the narrowed-exclusion restart is unprotected.
func TestLiveAddedExcluded_RetireFailureStillStops(t *testing.T) {
	live := &liveAddedFilter{}
	check := liveAddedCheck{
		streamID: "st", base: migcore.TableFilter{Exclude: []string{"tmp_*"}}, source: "mysql", target: "mysql",
		retire: func(context.Context, []string) error { return errors.New("control row unwritable") },
	}
	if observeLiveAdded(context.Background(), check, live, []string{"tmp_keep"}) {
		t.Fatal("the contradiction was installed")
	}
	err := live.refusalErr()
	if err == nil {
		t.Fatal("a failed control-row write kept the stream running")
	}
	for _, want := range []string{"FAILED", "control row unwritable", "narrows --exclude-table"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q: %v", want, err)
		}
	}
}
