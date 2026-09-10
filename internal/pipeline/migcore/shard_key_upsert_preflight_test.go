// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

type shardKeyProbe struct {
	detail    string
	err       error
	called    int
	sawTables int
}

func (p *shardKeyProbe) ShardKeyUpsertMismatch(_ context.Context, tables []*ir.Table) (string, error) {
	p.called++
	p.sawTables = len(tables)
	return p.detail, p.err
}

func shardKeySchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{Name: "orders"}, {Name: "users"}}}
}

// The harm is a duplicated primary key that no read a sharded application
// makes will show. It must be refused before anything is written.
func TestPreflightShardKeyUpsertRefusesAMismatch(t *testing.T) {
	t.Parallel()
	p := &shardKeyProbe{detail: `table "orders" is sharded on (tenant_id) but sluice's idempotent upsert keys on (id); tenant_id is not in that key`}
	err := PreflightShardKeyUpsert(context.Background(), shardKeySchema(), p)
	if err == nil {
		t.Fatal("a target whose shard key sits outside the upsert key was accepted; a CDC replay of a shard-key " +
			"change would insert a second row with the same primary key, at exit 0")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeTargetShardKeyNotInUpsertKey {
		t.Errorf("refusal carried code %v (coded=%v), want %q", ce, ok, sluicecode.CodeTargetShardKeyNotInUpsertKey)
	}
	// The operator cannot act on "some table is wrong". Both the table and
	// the offending column have to survive into the message.
	for _, want := range []string{"orders", "tenant_id", "id"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not name %q, so the operator cannot act on it: %v", want, err)
		}
	}
	if !strings.Contains(ce.Hint, "PRIMARY KEY") {
		t.Errorf("the hint does not name the fix: %q", ce.Hint)
	}
	if p.sawTables != 2 {
		t.Errorf("probe saw %d tables, want 2 — the preflight must offer the whole in-scope set", p.sawTables)
	}
}

func TestPreflightShardKeyUpsertPassesWhenTheKeyContainsTheShardKey(t *testing.T) {
	t.Parallel()
	if err := PreflightShardKeyUpsert(context.Background(), shardKeySchema(), &shardKeyProbe{}); err != nil {
		t.Fatalf("a safe target was refused: %v", err)
	}
}

// A probe that could not RUN is not a verdict. Reporting a read failure as
// this refusal would tell the operator their schema is wrong on the strength
// of a network error.
func TestPreflightShardKeyUpsertDoesNotRefuseOnProbeFailure(t *testing.T) {
	t.Parallel()
	p := &shardKeyProbe{err: errors.New("read Neki data topology: connection reset")}
	err := PreflightShardKeyUpsert(context.Background(), shardKeySchema(), p)
	if err == nil {
		t.Fatal("a failed probe was swallowed; the caller learns nothing")
	}
	if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeTargetShardKeyNotInUpsertKey {
		t.Error("a probe FAILURE was reported as a shard-key MISMATCH — that accuses the operator's schema " +
			"of being wrong on the strength of a transient error")
	}
	if !strings.Contains(err.Error(), "connection reset") {
		t.Errorf("the underlying cause was dropped: %v", err)
	}
}

// Every target that is not a sharded Neki must pay nothing.
func TestPreflightShardKeyUpsertIsANoOpWithoutTheSurface(t *testing.T) {
	t.Parallel()
	type bareWriter struct{}
	if err := PreflightShardKeyUpsert(context.Background(), shardKeySchema(), bareWriter{}); err != nil {
		t.Fatalf("a target without the probe surface was refused: %v", err)
	}
	p := &shardKeyProbe{detail: "orders"}
	if err := PreflightShardKeyUpsert(context.Background(), &ir.Schema{}, p); err != nil {
		t.Fatalf("an empty schema was refused: %v", err)
	}
	if p.called != 0 {
		t.Errorf("probe ran %d times on an empty schema; it should not run at all", p.called)
	}
}
