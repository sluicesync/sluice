// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

type fakeReplayProbe struct {
	exists, keyed bool
	err           error
	calls         int
}

func (p *fakeReplayProbe) ProbeReplayKey(context.Context, *ir.Table) (exists, keyed bool, err error) {
	p.calls++
	return p.exists, p.keyed, p.err
}

func judgeKeyedTable() *ir.Table {
	return &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}}, {Name: "v", Type: ir.Text{}, Nullable: true}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// TestJudgeReplayKey pins the one F-E1 predicate's truth table: the
// recorded half decides first and never probes when it fails; the target
// half maps (exists, keyed) to a verdict; an error is never a verdict.
func TestJudgeReplayKey(t *testing.T) {
	ctx := context.Background()
	keyless := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}}}}

	p := &fakeReplayProbe{exists: true, keyed: true}
	if v, err := JudgeReplayKey(ctx, p, keyless); err != nil || v != ReplayKeylessRecorded || p.calls != 0 {
		t.Errorf("recorded-keyless: (%v, %v, probes=%d); want (ReplayKeylessRecorded, nil, 0)", v, err, p.calls)
	}
	if v, err := JudgeReplayKey(ctx, nil, judgeKeyedTable()); err != nil || v != ReplayKeyCollides {
		t.Errorf("nil prober, keyed recorded: (%v, %v); want ReplayKeyCollides", v, err)
	}
	for _, c := range []struct {
		exists, keyed bool
		want          ReplayKeyVerdict
	}{
		{true, true, ReplayKeyCollides},
		{true, false, ReplayKeylessTarget},
		{false, false, ReplayTargetAbsent},
	} {
		v, err := JudgeReplayKey(ctx, &fakeReplayProbe{exists: c.exists, keyed: c.keyed}, judgeKeyedTable())
		if err != nil || v != c.want {
			t.Errorf("probe (exists=%v, keyed=%v): (%v, %v); want %v", c.exists, c.keyed, v, err, c.want)
		}
	}
	boom := errors.New("boom")
	if v, err := JudgeReplayKey(ctx, &fakeReplayProbe{err: boom}, judgeKeyedTable()); !errors.Is(err, boom) || v == ReplayKeyCollides {
		t.Errorf("probe error: (%v, %v); want the error and no Collides verdict", v, err)
	}
	var zero ReplayKeyVerdict
	if zero == ReplayKeyCollides {
		t.Error("the zero ReplayKeyVerdict reads as ReplayKeyCollides; a forgotten judgment would license a replay")
	}
}

// TestReplayKeyCache_MemoisesAnswersNotErrors pins the cache: one probe per
// (table, supplied columns), errors re-probed, a different supplied set
// re-probed, Reset forgets.
func TestReplayKeyCache_MemoisesAnswersNotErrors(t *testing.T) {
	ctx := context.Background()
	var c ReplayKeyCache
	p := &fakeReplayProbe{err: errors.New("transient")}
	if _, err := c.Judge(ctx, p, judgeKeyedTable()); err == nil {
		t.Fatal("want the probe error")
	}
	p.err, p.exists, p.keyed = nil, true, false
	for i := 0; i < 3; i++ {
		if v, err := c.Judge(ctx, p, judgeKeyedTable()); err != nil || v != ReplayKeylessTarget {
			t.Fatalf("call %d: (%v, %v)", i, v, err)
		}
	}
	if p.calls != 2 {
		t.Errorf("probes = %d; want 2 (the failed one is not memoised, the answer is)", p.calls)
	}
	wider := judgeKeyedTable()
	wider.Columns = append(wider.Columns, &ir.Column{Name: "sid", Type: ir.Integer{Width: 8}})
	if _, err := c.Judge(ctx, p, wider); err != nil || p.calls != 3 {
		t.Errorf("a different supplied-column set reused the memo (probes=%d, err=%v)", p.calls, err)
	}
	c.Reset()
	if _, err := c.Judge(ctx, p, judgeKeyedTable()); err != nil || p.calls != 4 {
		t.Errorf("Reset did not forget (probes=%d, err=%v)", p.calls, err)
	}
}
