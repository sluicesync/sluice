// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/vtgate"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// VSTREAM-TAIL-SKEW-HOLD (Bug 334): every VStream request that carries CDC
// follows ADR-0120's skew policy, the COPY requests keep MinimizeSkew=true,
// and the snapshot stream's tail has the per-shard stall WARN. See
// cdc_vstream_relax_skew_override.go for the mechanism.

// skewPolicyCases is the two-row policy matrix every CDC builder is pinned
// against: the ADR-0120 default and the operator's opt-out.
var skewPolicyCases = []struct {
	name         string
	preserveSkew bool
	wantMinimize bool
}{
	{name: "relaxed (the ADR-0120 default) => MinimizeSkew off", preserveSkew: false, wantMinimize: false},
	{name: "--vstream-preserve-skew => MinimizeSkew on", preserveSkew: true, wantMinimize: true},
}

// tailSkewStream builds a snapshot stream whose client records every
// request it is opened with.
func tailSkewStream(ctx context.Context, preserveSkew bool) (*vstreamSnapshotStream, *reconnectFakeClient) {
	client := &reconnectFakeClient{ctx: ctx, streams: [][]scriptStep{nil}}
	s := newTestSnapshotStream()
	s.client = client
	s.shards = []string{"-80", "80-"}
	s.preserveSkew = preserveSkew
	s.currentVgtid = []shardGtid{
		{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/aaa:1-40"},
		{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/bbb:1-90"},
	}
	return s, client
}

func onlyRequest(t *testing.T, client *reconnectFakeClient) *vtgate.VStreamRequest {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != 1 {
		t.Fatalf("VStream opened %d times; want exactly 1", len(client.requests))
	}
	return client.requests[0]
}

// TestVStreamSnapshot_ReopenForCDC_FollowsSkewPolicy pins the copy→CDC
// handoff's request: the path Bug 334 reproduced on a real cluster, which
// asked for MinimizeSkew=true unconditionally.
func TestVStreamSnapshot_ReopenForCDC_FollowsSkewPolicy(t *testing.T) {
	for _, c := range skewPolicyCases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, client := tailSkewStream(ctx, c.preserveSkew)
			s.streamSkewRelaxed = !vstreamCopyMinimizeSkew
			if err := s.reopenForCDC(ctx, "shard_finished_copy_early"); err != nil {
				t.Fatalf("reopenForCDC: %v", err)
			}
			req := onlyRequest(t, client)
			if got := req.GetFlags().GetMinimizeSkew(); got != c.wantMinimize {
				t.Errorf("tail MinimizeSkew = %v; want %v", got, c.wantMinimize)
			}
			if !req.GetFlags().GetStopOnReshard() {
				t.Error("tail StopOnReshard must stay true")
			}
			if s.streamSkewRelaxed == c.wantMinimize {
				t.Errorf("streamSkewRelaxed = %v after the reopen; want %v (the flag the tail was opened with)", s.streamSkewRelaxed, !c.wantMinimize)
			}
		})
	}
}

// TestVStreamSnapshot_ReopenForCDC_RefusesCopyCursor pins
// VSTREAM-TAIL-COPY-CURSOR: a snapshot position that still carries a TablePKs
// copy cursor on any shard refuses loudly, naming the marker and the shard,
// and opens no stream (a stream opened there would be a copy resume).
func TestVStreamSnapshot_ReopenForCDC_RefusesCopyCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, client := tailSkewStream(ctx, false)
	s.currentVgtid[1].TablePKs = []encodedTablePK{{}}
	err := s.reopenForCDC(ctx, "skew_policy")
	if err == nil || !strings.Contains(err.Error(), vstreamTailCopyCursorMarker) || !strings.Contains(err.Error(), "main/80-") {
		t.Fatalf("reopenForCDC with a copy cursor on 80-: err %v; want a %s refusal naming main/80-", err, vstreamTailCopyCursorMarker)
	}
	client.mu.Lock()
	n := len(client.requests)
	client.mu.Unlock()
	if n != 0 {
		t.Errorf("the refusal opened %d stream(s); want 0", n)
	}
}

// TestVStreamSnapshot_SkewPolicyReopenKeepsCopyFilter pins that a
// single-stream tail reopened for the skew policy streams exactly what the
// copy stream would have streamed had it continued in place: the reopen's
// Filter equals the copy request's, for an explicit table with a --where
// predicate and for the keyspace-wide scope.
func TestVStreamSnapshot_SkewPolicyReopenKeepsCopyFilter(t *testing.T) {
	cases := []struct {
		name    string
		tables  []string
		filters map[string]string
	}{
		{name: "one table with a predicate", tables: []string{"t"}, filters: map[string]string{"t": "id > 10"}},
		{name: "keyspace-wide", tables: nil, filters: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, client := tailSkewStream(ctx, false)
			s.copyTables = c.tables
			s.rowFilters = c.filters
			if err := s.reopenForCDC(ctx, "skew_policy"); err != nil {
				t.Fatalf("reopenForCDC: %v", err)
			}
			// The copy request's filter, as openVStreamSnapshotStreamFrom
			// builds it for a single-stream scope.
			want := &binlogdata.Filter{Rules: vstreamCopyFilterRules(c.tables, c.filters)}
			got := onlyRequest(t, client).GetFilter()
			if !proto.Equal(got, want) {
				t.Errorf("tail Filter = %v; want the copy request's %v", got, want)
			}
		})
	}
}

// TestVStreamSnapshot_OpenReshardCDCStream_FollowsSkewPolicy pins the
// reshard tail's request (reopenAfterReshard and reopenReshardWindow both
// open through it).
func TestVStreamSnapshot_OpenReshardCDCStream_FollowsSkewPolicy(t *testing.T) {
	for _, c := range skewPolicyCases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, client := tailSkewStream(ctx, c.preserveSkew)
			_, streamCancel, err := s.openReshardCDCStream(s.currentVgtid)
			if err != nil {
				t.Fatalf("openReshardCDCStream: %v", err)
			}
			defer streamCancel()
			req := onlyRequest(t, client)
			if got := req.GetFlags().GetMinimizeSkew(); got != c.wantMinimize {
				t.Errorf("reshard tail MinimizeSkew = %v; want %v", got, c.wantMinimize)
			}
			if req.GetTabletType().String() != "PRIMARY" {
				t.Errorf("reshard tail tablet type = %v; want PRIMARY", req.GetTabletType())
			}
		})
	}
}

// TestVStreamSnapshot_CopyRequestsKeepMinimizeSkew pins ADR-0120's COPY
// exemption against BOTH policy values: the per-table reopen and the
// concurrent COPY streams ask for MinimizeSkew=true whatever the CDC policy
// says.
func TestVStreamSnapshot_CopyRequestsKeepMinimizeSkew(t *testing.T) {
	for _, c := range skewPolicyCases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, client := tailSkewStream(ctx, c.preserveSkew)
			if err := s.reopenForTableSeeded(ctx, "t", nil); err != nil {
				t.Fatalf("reopenForTableSeeded: %v", err)
			}
			if got := onlyRequest(t, client).GetFlags().GetMinimizeSkew(); !got {
				t.Error("per-table COPY request MinimizeSkew = false; want true (ADR-0120 COPY exemption)")
			}
			if s.streamSkewRelaxed {
				t.Error("streamSkewRelaxed = true after a COPY open; want false")
			}

			s2, client2 := tailSkewStream(ctx, c.preserveSkew)
			cs := &copyStream{parent: s2}
			if err := cs.openStreamForTable(ctx, "t", nil); err != nil {
				t.Fatalf("openStreamForTable: %v", err)
			}
			if got := onlyRequest(t, client2).GetFlags().GetMinimizeSkew(); !got {
				t.Error("concurrent COPY request MinimizeSkew = false; want true (ADR-0120 COPY exemption)")
			}
		})
	}
}

// TestVStreamSnapshot_CDCHandoffReopenReason pins the handoff's decision
// table: every reason the copy stream may not be continued in place, and
// the one case it may.
func TestVStreamSnapshot_CDCHandoffReopenReason(t *testing.T) {
	cases := []struct {
		name         string
		seq          []string
		rewound      bool
		streamSkew   bool
		preserveSkew bool
		want         string
	}{
		{name: "auto-shard per-table copy", seq: []string{"a", "b"}, streamSkew: true, want: "per_table_copy"},
		{name: "rewound single stream", rewound: true, streamSkew: true, preserveSkew: true, want: "shard_finished_copy_early"},
		{name: "single stream, relaxed policy (the default)", streamSkew: true, want: "skew_policy"},
		{name: "single stream, preserve-skew: continue in place", streamSkew: true, preserveSkew: true, want: ""},
		{name: "single stream already relaxed, relaxed policy: continue in place", streamSkew: false, want: ""},
	}
	// The zero value is the safe one: a stream whose open never recorded its
	// flag must reopen under the default policy, never continue in place.
	if got := newTestSnapshotStream().cdcHandoffReopenReason(); got != "skew_policy" {
		t.Errorf("zero-value stream: cdcHandoffReopenReason() = %q; want %q (an unrecorded flag must reopen)", got, "skew_policy")
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newTestSnapshotStream()
			s.copyTablesSeq = c.seq
			s.cdcResumeRewound = c.rewound
			s.streamSkewRelaxed = !c.streamSkew
			s.preserveSkew = c.preserveSkew
			if got := s.cdcHandoffReopenReason(); got != c.want {
				t.Errorf("cdcHandoffReopenReason() = %q; want %q", got, c.want)
			}
		})
	}
}

// TestVStreamSnapshot_StartPump_SingleStreamTailSkew drives the
// single-stream handoff through startPump: with the default policy the
// copy stream (MinimizeSkew=true) is replaced by a relaxed tail opened at
// the snapshot position; under --vstream-preserve-skew it continues in
// place and nothing is reopened.
func TestVStreamSnapshot_StartPump_SingleStreamTailSkew(t *testing.T) {
	for _, c := range skewPolicyCases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, client := tailSkewStream(ctx, c.preserveSkew)
			copyStreamCtx, copyStreamCancel := context.WithCancel(ctx)
			copyStream := &scriptedStream{ctx: copyStreamCtx}
			s.grpcStream = copyStream
			s.grpcCancel = copyStreamCancel
			s.streamSkewRelaxed = !vstreamCopyMinimizeSkew
			s.copyDone = make(chan struct{})
			close(s.copyDone)
			want := append([]shardGtid(nil), s.currentVgtid...)

			if _, err := s.startPump(ctx); err != nil {
				t.Fatalf("startPump: %v", err)
			}
			defer func() {
				s.cancelGRPCStream()
				s.joinCDCPump()
			}()

			client.mu.Lock()
			reqs := append([]*vtgate.VStreamRequest(nil), client.requests...)
			client.mu.Unlock()
			if c.preserveSkew {
				if len(reqs) != 0 {
					t.Fatalf("preserve-skew handoff opened %d streams; want 0 (the copy stream continues in place)", len(reqs))
				}
				s.mu.Lock()
				same := s.grpcStream == copyStream
				s.mu.Unlock()
				if !same {
					t.Error("preserve-skew handoff replaced the copy stream; want it continued in place")
				}
				return
			}
			if len(reqs) != 1 {
				t.Fatalf("default handoff opened %d streams; want 1 relaxed tail", len(reqs))
			}
			if reqs[0].GetFlags().GetMinimizeSkew() {
				t.Error("default handoff's tail MinimizeSkew = true; want false (VSTREAM-TAIL-SKEW-HOLD)")
			}
			got := make([]shardGtid, 0, len(reqs[0].GetVgtid().GetShardGtids()))
			for _, sg := range reqs[0].GetVgtid().GetShardGtids() {
				got = append(got, shardGtid{Keyspace: sg.GetKeyspace(), Shard: sg.GetShard(), Gtid: sg.GetGtid()})
			}
			if !sameVgtid(got, want) {
				t.Errorf("tail opened at %v; want the snapshot position %v", got, want)
			}
			select {
			case <-copyStreamCtx.Done():
			default:
				t.Error("the copy stream's context was not freed when the tail replaced it")
			}
		})
	}
}

// TestVStreamSnapshot_DispatchCDCVgtidFeedsShardWatchdog pins the tail's
// watchdog feed: a VGTID reports exactly the shards whose GTID moved.
func TestVStreamSnapshot_DispatchCDCVgtidFeedsShardWatchdog(t *testing.T) {
	s, _ := tailSkewStream(context.Background(), false)
	w := &shardProgressWatchdog{advances: make(chan []string, 1), done: make(chan struct{})}
	s.shardProgress = w
	ev := &binlogdata.VEvent{Type: binlogdata.VEventType_VGTID, Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{
		{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/aaa:1-41"},
		{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/bbb:1-90"},
	}}}
	if err := s.dispatchCDCEvent(context.Background(), ev, make(chan ir.Change, 1)); err != nil {
		t.Fatalf("dispatchCDCEvent: %v", err)
	}
	select {
	case got := <-w.advances:
		if len(got) != 1 || got[0] != "-80" {
			t.Errorf("watchdog told %v advanced; want [-80]", got)
		}
	default:
		t.Fatal("the VGTID did not reach the per-shard watchdog")
	}
}

// TestResolveVStreamSnapshotTuning_ShardStallWarnWindow pins the tail's
// WARN window parse: the snapshot honours vstream_shard_stall_warn_timeout
// exactly as the steady-state reader does (default, override, loud on
// malformed).
func TestResolveVStreamSnapshotTuning_ShardStallWarnWindow(t *testing.T) {
	cfg, _ := minimalConfig("host:3306", nil)
	got, err := Engine{}.resolveVStreamSnapshotTuning(cfg)
	if err != nil || got.shardStallWarnWindow != defaultVStreamShardStallWarnWindow {
		t.Errorf("default: window %v err %v; want %v", got.shardStallWarnWindow, err, defaultVStreamShardStallWarnWindow)
	}
	cfg, _ = minimalConfig("host:3306", map[string]string{"vstream_shard_stall_warn_timeout": "90s"})
	if got, err = (Engine{}).resolveVStreamSnapshotTuning(cfg); err != nil || got.shardStallWarnWindow != 90*time.Second {
		t.Errorf("override: window %v err %v; want 90s", got.shardStallWarnWindow, err)
	}
	cfg, _ = minimalConfig("host:3306", map[string]string{"vstream_shard_stall_warn_timeout": "soonish"})
	if _, err = (Engine{}).resolveVStreamSnapshotTuning(cfg); err == nil || !strings.Contains(err.Error(), "vstream_shard_stall_warn_timeout") {
		t.Errorf("malformed: err %v; want a loud error naming the param", err)
	}
}

// pacedVgtidStream emits, every interval until its context ends, a VGTID
// on which only shard "-80" advances: shard "80-" is held, as vtgate's skew
// hold holds it.
type pacedVgtidStream struct {
	scriptedStream
	interval time.Duration
	n        int
}

func (p *pacedVgtidStream) Recv() (*vtgate.VStreamResponse, error) {
	select {
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case <-time.After(p.interval):
	}
	p.n++
	return oneEvent(&binlogdata.VEvent{Type: binlogdata.VEventType_VGTID, Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{
		{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/aaa:1-" + strconv.Itoa(40+p.n)},
		{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/bbb:1-90"},
	}}}), nil
}

// TestVStreamSnapshot_TailWarnsOnHeldShard is the end-to-end liveness pin:
// the snapshot tail's pump WARNs, naming the shard, when one shard stops
// advancing while its peer advances. Through v0.163.10 this fired only on
// the steady-state reader, and a held tail was silent.
func TestVStreamSnapshot_TailWarnsOnHeldShard(t *testing.T) {
	logs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := tailSkewStream(ctx, false)
	s.shardStallWarnWindow = 200 * time.Millisecond
	streamCtx, streamCancel := context.WithCancel(ctx)
	s.grpcStream = &pacedVgtidStream{scriptedStream: scriptedStream{ctx: streamCtx}, interval: 20 * time.Millisecond}
	s.grpcCancel = streamCancel
	out := make(chan ir.Change, 64)
	s.launchCDCPump(ctx, out)
	defer func() {
		s.cancelGRPCStream()
		s.joinCDCPump()
	}()

	// The text handler quotes msg, so the message's quotes arrive escaped.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(logs.String(), `shard \"80-\"`) {
		time.Sleep(20 * time.Millisecond)
	}
	got := logs.String()
	if !strings.Contains(got, `shard \"80-\" of keyspace \"main\" has not advanced`) {
		t.Fatalf("no per-shard stall WARN for the held shard 80- on the snapshot tail; logs:\n%s", got)
	}
	if strings.Contains(got, `shard \"-80\" of keyspace`) {
		t.Errorf("the advancing shard -80 was named as stalled; logs:\n%s", got)
	}
}
