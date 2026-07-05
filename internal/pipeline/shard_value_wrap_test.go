// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestShardStampRow_StampsAndEmptyNoop pins the load-bearing
// fast path: empty name is a no-op (row unchanged), non-empty
// name overwrites row[name] unconditionally.
func TestShardStampRow_StampsAndEmptyNoop(t *testing.T) {
	r := ir.Row{"customer_id": int64(42)}
	shardStampRow(r, "", "us-east-1")
	if _, ok := r[""]; ok {
		t.Errorf("empty name should not stamp; got %v", r)
	}
	if got := r["customer_id"]; got != int64(42) {
		t.Errorf("unrelated keys must not change; got %v", got)
	}
	shardStampRow(r, "source_shard_id", "us-east-1")
	if got := r["source_shard_id"]; got != "us-east-1" {
		t.Errorf("stamp expected; got %v", got)
	}
}

func TestShardStampRow_OverwritesExistingKey(t *testing.T) {
	r := ir.Row{"source_shard_id": "stale"}
	shardStampRow(r, "source_shard_id", "us-east-1")
	if got := r["source_shard_id"]; got != "us-east-1" {
		t.Errorf("expected overwrite; got %v", got)
	}
}

// TestShardStampRows_FastPathPassthrough: empty shardName returns
// the src channel verbatim, no goroutine spawned.
func TestShardStampRows_FastPathPassthrough(t *testing.T) {
	ctx := context.Background()
	src := make(chan ir.Row, 1)
	src <- ir.Row{"x": 1}
	close(src)

	out, errFn := shardStampRows(ctx, src, "", nil)
	// out should literally be src — no new goroutine, no new channel.
	row, ok := <-out
	if !ok {
		t.Fatal("expected a row from the pass-through src")
	}
	if got := row["x"]; got != 1 {
		t.Errorf("pass-through row corrupted; got %v", got)
	}
	if _, ok := <-out; ok {
		t.Errorf("expected closed channel after src drained")
	}
	if err := errFn(); err != nil {
		t.Errorf("errFn should be nil on pass-through; got %v", err)
	}
}

// TestShardStampRows_StampsEveryRow pins the every-row stamping
// contract: each row that comes out of the wrap carries shardName
// = shardValue.
func TestShardStampRows_StampsEveryRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan ir.Row, 4)
	src <- ir.Row{"customer_id": int64(1)}
	src <- ir.Row{"customer_id": int64(2)}
	src <- ir.Row{"customer_id": int64(3)}
	close(src)

	out, errFn := shardStampRows(ctx, src, "source_shard_id", "us-east-1")
	got := 0
	for row := range out {
		got++
		if row["source_shard_id"] != "us-east-1" {
			t.Errorf("row %d: stamped value = %v; want us-east-1", got, row["source_shard_id"])
		}
	}
	if got != 3 {
		t.Errorf("processed %d rows; want 3", got)
	}
	if err := errFn(); err != nil {
		t.Errorf("errFn = %v; want nil", err)
	}
}

// TestShardStampRows_RespectsContextCancel: cancellation while
// the wrap goroutine is mid-stream unwinds cleanly without
// blocking the test.
func TestShardStampRows_RespectsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := make(chan ir.Row) // unbuffered — wrap goroutine blocks until we send
	out, _ := shardStampRows(ctx, src, "source_shard_id", "x")
	cancel()
	// Out should close shortly after cancel — give it a generous
	// budget to avoid flakiness on heavily-loaded CI runners.
	deadline := time.After(2 * time.Second)
	select {
	case _, ok := <-out:
		if ok {
			t.Errorf("expected closed channel after cancel; got a row")
		}
	case <-deadline:
		t.Fatal("wrap goroutine did not unwind on ctx cancel within budget")
	}
}

// TestShardStampRows_StampsNonStringValue: the discriminator
// value is `any`, not just `string` — pin that integers / UUIDs /
// other shapes are stamped verbatim (no type coercion at the
// orchestrator-side wrap; the writer's prepareValue handles
// engine-specific marshalling).
func TestShardStampRows_StampsNonStringValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan ir.Row, 1)
	src <- ir.Row{"id": 1}
	close(src)
	out, _ := shardStampRows(ctx, src, "shard_no", int64(42))
	row, ok := <-out
	if !ok {
		t.Fatal("expected one row")
	}
	if row["shard_no"] != int64(42) {
		t.Errorf("non-string stamp value not preserved; got %v (%T)", row["shard_no"], row["shard_no"])
	}
}

// TestShardStampRows_BufferedRelay pins the perf-parity matrix row-6
// fix (gap 6): the shard-stamp tee's relay channel carries the
// standard migcore.RowChanBuffer so an engaged discriminator stamp never
// re-introduces an unbuffered rendezvous hop into the bulk-copy hot
// path.
func TestShardStampRows_BufferedRelay(t *testing.T) {
	src := make(chan ir.Row)
	close(src)
	out, _ := shardStampRows(context.Background(), src, "shard_id", "s1")
	if got := cap(out); got != migcore.RowChanBuffer {
		t.Errorf("shard-stamp relay channel cap = %d; want migcore.RowChanBuffer (%d)", got, migcore.RowChanBuffer)
	}
}
