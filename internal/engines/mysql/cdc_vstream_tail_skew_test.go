// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

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
			s.streamMinimizeSkew = vstreamCopyMinimizeSkew
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
			if s.streamMinimizeSkew != c.wantMinimize {
				t.Errorf("streamMinimizeSkew = %v after the reopen; want %v (the flag the tail was opened with)", s.streamMinimizeSkew, c.wantMinimize)
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
			if !s.streamMinimizeSkew {
				t.Error("streamMinimizeSkew = false after a COPY open; want true")
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newTestSnapshotStream()
			s.copyTablesSeq = c.seq
			s.cdcResumeRewound = c.rewound
			s.streamMinimizeSkew = c.streamSkew
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
			s.streamMinimizeSkew = vstreamCopyMinimizeSkew
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

// vstreamSkewRoster classifies every function in the tree that builds a
// vtgate.VStreamFlags literal: COPY builders must set
// MinimizeSkew: vstreamCopyMinimizeSkew, CDC builders
// MinimizeSkew: vstreamCDCMinimizeSkew(...). A new builder fails the roster
// until it is classified here.
var vstreamSkewRoster = map[string]string{
	// The snapshot stream's first request: COPY. On the single-stream path
	// it continues as the tail only when the CDC policy is also
	// MinimizeSkew=true; otherwise startPump reopens (cdcHandoffReopenReason).
	"openVStreamSnapshotStreamFrom": "COPY",
	"reopenForTableSeeded":          "COPY", // auto-shard per-table COPY
	"openStreamForTable":            "COPY", // ADR-0099 concurrent COPY
	"reopenForCDC":                  "CDC",  // the copy→CDC handoff tail
	"openReshardCDCStream":          "CDC",  // the snapshot stream's reshard tail
	"buildVStreamRequest":           "CDC",  // the steady-state reader (and its reshard reopen)
}

// TestVStreamRequestSkewRoster_EveryBuilderClassified walks every non-test
// Go file under internal/ and cmd/ for vtgate.VStreamFlags composite
// literals and checks each against vstreamSkewRoster. It reaches every
// VStream request the code builds, not only this package's; an unlisted
// builder, a missing MinimizeSkew key, a literal true/false, or a builder
// whose class does not match its value all fail. Floor: every roster entry
// must be found, so a rename cannot leave the roster vacuous.
func TestVStreamRequestSkewRoster_EveryBuilderClassified(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	found := map[string]bool{}
	var problems []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return perr
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					lit, ok := n.(*ast.CompositeLit)
					if !ok || !isVStreamFlagsType(lit.Type) {
						return true
					}
					where := fset.Position(lit.Pos()).String()
					class, listed := vstreamSkewRoster[fn.Name.Name]
					if !listed {
						problems = append(problems, where+": "+fn.Name.Name+" builds VStreamFlags but is not classified COPY or CDC in vstreamSkewRoster")
						return true
					}
					found[fn.Name.Name] = true
					if p := checkMinimizeSkew(lit, class); p != "" {
						problems = append(problems, where+": "+fn.Name.Name+" ("+class+"): "+p)
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	for name := range vstreamSkewRoster {
		if !found[name] {
			problems = append(problems, "roster entry "+name+" builds no VStreamFlags any more (renamed or removed?): the roster would pass vacuously for it")
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}

func isVStreamFlagsType(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "VStreamFlags"
}

// checkMinimizeSkew returns "" when lit's MinimizeSkew is the value its
// class requires, else what is wrong.
func checkMinimizeSkew(lit *ast.CompositeLit, class string) string {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != "MinimizeSkew" {
			continue
		}
		switch class {
		case "COPY":
			if id, ok := kv.Value.(*ast.Ident); ok && id.Name == "vstreamCopyMinimizeSkew" {
				return ""
			}
			return "MinimizeSkew must be vstreamCopyMinimizeSkew"
		case "CDC":
			if call, ok := kv.Value.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "vstreamCDCMinimizeSkew" {
					return ""
				}
			}
			return "MinimizeSkew must be vstreamCDCMinimizeSkew(...) (ADR-0120's CDC policy)"
		default:
			return "unknown class " + class
		}
	}
	return "no MinimizeSkew key (it would default to false silently)"
}
