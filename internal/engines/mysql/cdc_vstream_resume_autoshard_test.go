// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/vtgate"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0098 auto-shard-aware VStream resume. These unit tests pin (a) the
// resolver that places the persisted cursor in the in-scope table sequence
// and refuses loudly on a mismatch, and (b) the end-to-end routing — a
// multi-table resume drives the per-table auto-shard pump (NOT the legacy
// keyspace-wide interleave that crash-looped on the ADR-0071 cap), seeding
// the in-progress table from its cursor while re-copying the tables before
// it and copying the tables after it fresh.

// resumeCursorPos builds a persisted []shardGtid carrying a per-table
// TablePKs cursor for exactly one in-progress table (the auto-shard resume
// shape: each per-table COPY is single-table, so vtgate emits a cursor for
// only the in-flight table).
func resumeCursorPos(t *testing.T, table string, pk int64) []shardGtid {
	t.Helper()
	cursor, err := encodeTablePKs([]*binlogdata.TableLastPK{makeTableLastPK(t, table, "id", pk)})
	if err != nil {
		t.Fatalf("encodeTablePKs: %v", err)
	}
	return []shardGtid{{
		Keyspace: "main",
		Shard:    "-",
		Gtid:     "MySQL56/" + uuidA + ":1-50",
		TablePKs: cursor,
	}}
}

// TestResolveResumeAutoShard pins the cursor-placement decision: a
// single-table cursor in scope returns that table; multi-table, out-of-scope,
// and empty cursors refuse loudly (never silently re-copy or skip).
func TestResolveResumeAutoShard(t *testing.T) {
	tables := []string{"users", "orders", "audit_trail", "binary_blobs"}

	t.Run("single in-scope cursor returns the in-progress table", func(t *testing.T) {
		seed := resumeCursorPos(t, "audit_trail", 9000)
		got, err := resolveResumeAutoShard(seed, tables)
		if err != nil {
			t.Fatalf("resolveResumeAutoShard: %v", err)
		}
		if got != "audit_trail" {
			t.Fatalf("in-progress table = %q; want %q", got, "audit_trail")
		}
	})

	t.Run("in-progress table at index 0 resolves (constructor reopen case)", func(t *testing.T) {
		seed := resumeCursorPos(t, "users", 10)
		got, err := resolveResumeAutoShard(seed, tables)
		if err != nil || got != "users" {
			t.Fatalf("resolveResumeAutoShard = (%q, %v); want (users, nil)", got, err)
		}
	})

	t.Run("multi-table cursor (legacy interleaved token) refuses loudly", func(t *testing.T) {
		cursor, err := encodeTablePKs([]*binlogdata.TableLastPK{
			makeTableLastPK(t, "users", "id", 5),
			makeTableLastPK(t, "orders", "id", 7),
		})
		if err != nil {
			t.Fatalf("encodeTablePKs: %v", err)
		}
		seed := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-50", TablePKs: cursor}}
		_, err = resolveResumeAutoShard(seed, tables)
		if err == nil {
			t.Fatal("resolveResumeAutoShard accepted a multi-table cursor; want a loud refusal (legacy/corrupt token)")
		}
		if !strings.Contains(err.Error(), "names 2 tables") {
			t.Errorf("error did not name the table count: %v", err)
		}
	})

	t.Run("out-of-scope cursor refuses loudly", func(t *testing.T) {
		seed := resumeCursorPos(t, "ghost_table", 42)
		_, err := resolveResumeAutoShard(seed, tables)
		if err == nil {
			t.Fatal("resolveResumeAutoShard accepted an out-of-scope cursor; want a loud refusal (stale token / scope change)")
		}
		if !strings.Contains(err.Error(), "ghost_table") {
			t.Errorf("error did not name the offending table: %v", err)
		}
	})

	t.Run("empty cursor refuses loudly", func(t *testing.T) {
		seed := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-50"}}
		_, err := resolveResumeAutoShard(seed, tables)
		if err == nil {
			t.Fatal("resolveResumeAutoShard accepted a cursor-less position; want a loud refusal")
		}
	})
}

// requestSeedsTable reports whether the recorded VStream request at index i
// carries a TablePKs cursor naming the given table (the seeded-resume shape).
func requestSeedsTable(t *testing.T, c *fakeVitessClient, i int, table string) bool {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if i >= len(c.requests) {
		t.Fatalf("request index %d out of range (%d requests)", i, len(c.requests))
	}
	for _, sg := range c.requests[i].GetVgtid().GetShardGtids() {
		for _, pk := range sg.GetTablePKs() {
			if pk.GetTableName() == table {
				return true
			}
		}
	}
	return false
}

// requestHasAnyTablePKs reports whether the recorded VStream request at index
// i carries ANY TablePKs cursor (i.e. it was opened seeded, not
// from-beginning).
func requestHasAnyTablePKs(t *testing.T, c *fakeVitessClient, i int) bool {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if i >= len(c.requests) {
		t.Fatalf("request index %d out of range (%d requests)", i, len(c.requests))
	}
	for _, sg := range c.requests[i].GetVgtid().GetShardGtids() {
		if len(sg.GetTablePKs()) > 0 {
			return true
		}
	}
	return false
}

// newAutoShardResumeHarness mirrors newAutoShardHarness but seeds the stream
// for an ADR-0098 resume: resumeSeedTable + resumeSeed are set so the pump
// opens the in-progress table SEEDED from the cursor. The fake client
// records every VStream request so the test can assert which per-table
// streams were opened from-beginning vs. from the cursor.
func newAutoShardResumeHarness(t *testing.T, tables []string, scripts [][]*vtgate.VStreamResponse, resumeTable string, resumeSeed []shardGtid) (*vstreamSnapshotStream, *ir.SnapshotStream, *fakeVitessClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	client := &fakeVitessClient{ctx: ctx, scripts: scripts}
	// The constructor opens table[0] from-beginning (the fresh shape); mirror
	// that here so the pump's per-table reopens pop the subsequent scripts.
	first, _ := client.VStream(ctx, &vtgate.VStreamRequest{
		Filter: &binlogdata.Filter{Rules: vstreamCopyFilterRules([]string{tables[0]}, nil)},
	})

	s := newTestSnapshotStream()
	s.client = client
	s.shards = []string{"-"}
	s.copyTablesSeq = tables
	s.copyTables = []string{tables[0]}
	s.resumeSeedTable = resumeTable
	s.resumeSeed = resumeSeed
	s.tableCopyComplete = make(map[string]bool)
	s.copyDone = make(chan struct{})
	s.grpcStream = first

	stream := &ir.SnapshotStream{
		Rows:    &vstreamSnapshotRows{snap: s},
		Changes: &vstreamSnapshotChanges{snap: s},
	}
	go s.copyPumpAutoShard(ctx, cancel, stream)
	return s, stream, client, cancel
}

// TestVStreamSnapshot_AutoShardResume_SeedsInProgressTableOnly is the
// ADR-0098 routing pin: a 3-table resume whose cursor names the MIDDLE table
// drives the per-table auto-shard pump (no interleave), opening table[0]
// from-beginning (re-copied idempotently), the in-progress table[1] SEEDED
// from the cursor, and table[2] from-beginning (fresh). The handoff position
// is the per-shard GTID-set MIN over all three captured per-table snapshots.
func TestVStreamSnapshot_AutoShardResume_SeedsInProgressTableOnly(t *testing.T) {
	tables := []string{"users", "orders", "audit_trail"}
	seed := resumeCursorPos(t, "orders", 4242)

	scripts := [][]*vtgate.VStreamResponse{
		perTableCopyScript("users", "MySQL56/"+uuidA+":1-100", 2),
		perTableCopyScript("orders", "MySQL56/"+uuidA+":1-300", 3),
		perTableCopyScript("audit_trail", "MySQL56/"+uuidA+":1-400", 1),
		{}, // CDC handoff stream.
	}

	s, stream, client, cancel := newAutoShardResumeHarness(t, tables, scripts, "orders", seed)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()

	for _, name := range tables {
		tbl := &ir.Table{Name: name, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, tbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", name, err)
		}
		for range ch { //nolint:revive // draining
		}
		if err := stream.Rows.Err(); err != nil {
			t.Fatalf("Err after %s drain (auto-shard resume must NOT loud-refuse): %v", name, err)
		}
	}

	select {
	case <-s.copyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("auto-shard resume pump did not finish")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	// The captured handoff Position must be the per-shard MIN (users, :1-100).
	wantPos, err := encodeVStreamPos([]shardGtid{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-100"}})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	if stream.Position.Token != wantPos.Token {
		t.Fatalf("stitched Position token = %q; want the MIN %q", stream.Position.Token, wantPos.Token)
	}

	// CDC handoff: opens a fresh KEYSPACE-WIDE stream from the stitched
	// position (the 4th VStream call).
	if _, err := stream.Changes.StreamChanges(ctx, stream.Position); err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Request audit: stream 0 = users from-beginning (Match users, empty
	// Gtid), stream 1 = orders SEEDED (Match orders, the cursor's Gtid +
	// TablePKs), stream 2 = audit_trail from-beginning, stream 3 = CDC handoff.
	matches := client.requestMatches()
	if len(matches) != 4 {
		t.Fatalf("VStream call count = %d (%v); want 4 (3 per-table COPY + CDC handoff)", len(matches), matches)
	}
	if matches[0] != "users" || matches[1] != "orders" || matches[2] != "audit_trail" || matches[3] != "/.*/" {
		t.Fatalf("per-table Match sequence = %v; want [users orders audit_trail /.*/]", matches)
	}

	// The in-progress table (orders, stream index 1) MUST have been opened
	// SEEDED from the cursor: its request Gtid carries TablePKs naming
	// "orders". The re-copied table (users, index 0) and the fresh table
	// (audit_trail, index 2) MUST be opened from-beginning (no TablePKs).
	if !requestSeedsTable(t, client, 1, "orders") {
		t.Error("in-progress table 'orders' (stream 1) was NOT seeded from the cursor — vtgate would restart its COPY from row 0")
	}
	if requestHasAnyTablePKs(t, client, 0) {
		t.Error("re-copied table 'users' (stream 0) was seeded with a cursor; want from-beginning (idempotent re-copy)")
	}
	if requestHasAnyTablePKs(t, client, 2) {
		t.Error("fresh table 'audit_trail' (stream 2) was seeded with a cursor; want from-beginning")
	}
}

// TestVStreamSnapshot_AutoShardResume_InProgressAtIndexZero pins the edge the
// constructor's pre-opened table[0] complicates: when the in-progress table
// IS table[0], the pump must STILL reopen it seeded from the cursor (the
// constructor opened it from-beginning), so vtgate resumes its scan rather
// than restarting from row 0.
func TestVStreamSnapshot_AutoShardResume_InProgressAtIndexZero(t *testing.T) {
	tables := []string{"users", "orders"}
	seed := resumeCursorPos(t, "users", 77)

	// When the in-progress table is table[0], the constructor's pre-opened
	// from-beginning stream for users is DISCARDED and reopened seeded by the
	// pump, so the fake needs a (drained-and-discarded) script for the
	// constructor's open PLUS the pump's seeded reopen. Sequence of
	// client.VStream calls: [0] constructor users (discarded), [1] pump
	// seeded users, [2] orders, [3] CDC handoff.
	scripts := [][]*vtgate.VStreamResponse{
		perTableCopyScript("users", "MySQL56/"+uuidA+":1-1", 0),   // constructor's discarded stream
		perTableCopyScript("users", "MySQL56/"+uuidA+":1-100", 2), // pump's seeded reopen
		perTableCopyScript("orders", "MySQL56/"+uuidA+":1-300", 2),
		{},
	}

	s, stream, client, cancel := newAutoShardResumeHarness(t, tables, scripts, "users", seed)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()

	for _, name := range tables {
		tbl := &ir.Table{Name: name, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, tbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", name, err)
		}
		for range ch { //nolint:revive // draining
		}
		if err := stream.Rows.Err(); err != nil {
			t.Fatalf("Err after %s drain: %v", name, err)
		}
	}
	select {
	case <-s.copyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not finish")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	// CDC handoff (the 4th VStream call) so the request audit sees the
	// keyspace-wide tail.
	if _, err := stream.Changes.StreamChanges(ctx, stream.Position); err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// The constructor opened stream 0 (users) from-beginning; the pump must
	// have REOPENED users seeded. The fake records the constructor's open as
	// request 0 and the pump's seeded reopen as request 1; orders is request
	// 2; CDC handoff is request 3.
	matches := client.requestMatches()
	if len(matches) != 4 {
		t.Fatalf("VStream call count = %d (%v); want 4 (constructor users + pump reopen users-seeded + orders + CDC)", len(matches), matches)
	}
	if matches[0] != "users" || matches[1] != "users" || matches[2] != "orders" || matches[3] != "/.*/" {
		t.Fatalf("Match sequence = %v; want [users users orders /.*/] (table[0] reopened seeded)", matches)
	}
	if requestHasAnyTablePKs(t, client, 0) {
		t.Error("constructor's table[0] open carried a cursor; want from-beginning")
	}
	if !requestSeedsTable(t, client, 1, "users") {
		t.Error("pump did NOT reopen table[0] 'users' seeded from the cursor — vtgate would restart from row 0")
	}
}
