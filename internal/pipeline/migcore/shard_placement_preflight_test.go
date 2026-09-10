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

type placementProbe struct {
	mismatched string
	err        error
	called     int
	sawTables  int
}

func (p *placementProbe) ShardPlacementMismatch(_ context.Context, tables []*ir.Table) (string, error) {
	p.called++
	p.sawTables = len(tables)
	return p.mismatched, p.err
}

func placementSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{Name: "orders"}, {Name: "users"}}}
}

// A target whose routing and placement disagree must be REFUSED, coded, before
// anything is written — the harm is a silently duplicated primary key, which
// no run should be able to opt into.
func TestPreflightShardPlacementRefusesAMismatch(t *testing.T) {
	t.Parallel()
	p := &placementProbe{mismatched: "orders"}
	err := PreflightShardPlacement(context.Background(), placementSchema(), p)
	if err == nil {
		t.Fatal("a mis-placed target was accepted; it would silently accumulate duplicate primary keys")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeTargetShardPlacementMismatch {
		t.Errorf("refusal carried code %v (coded=%v), want %q", ce, ok, sluicecode.CodeTargetShardPlacementMismatch)
	}
	if !strings.Contains(err.Error(), "orders") {
		t.Errorf("refusal does not name the offending table; operator cannot act on it: %v", err)
	}
	if p.sawTables != 2 {
		t.Errorf("probe saw %d tables, want 2 — the preflight must offer the whole in-scope set", p.sawTables)
	}
}

// The healthy path must be silent and must not disturb the run.
func TestPreflightShardPlacementPassesWhenPlacementAgrees(t *testing.T) {
	t.Parallel()
	if err := PreflightShardPlacement(context.Background(), placementSchema(), &placementProbe{}); err != nil {
		t.Fatalf("a correctly-placed target was refused: %v", err)
	}
}

// A probe that could not RUN is not a verdict. It must surface as an error
// that is NOT the mismatch refusal, so a transient read failure can never be
// mistaken for — or reported as — a corrupt target.
func TestPreflightShardPlacementDoesNotRefuseOnProbeFailure(t *testing.T) {
	t.Parallel()
	p := &placementProbe{err: errors.New("connection reset")}
	err := PreflightShardPlacement(context.Background(), placementSchema(), p)
	if err == nil {
		t.Fatal("a failed probe was swallowed; the caller learns nothing")
	}
	if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeTargetShardPlacementMismatch {
		t.Error("a probe FAILURE was reported as a placement MISMATCH — that accuses the operator's data " +
			"of being corrupt on the strength of a network error")
	}
	if !strings.Contains(err.Error(), "connection reset") {
		t.Errorf("the underlying cause was dropped: %v", err)
	}
}

// Every target that is not a sharded Neki must pay nothing: a handle that does
// not implement the surface is skipped without a probe.
func TestPreflightShardPlacementIsANoOpWithoutTheSurface(t *testing.T) {
	t.Parallel()
	type bareWriter struct{}
	if err := PreflightShardPlacement(context.Background(), placementSchema(), bareWriter{}); err != nil {
		t.Fatalf("a target without the probe surface was refused: %v", err)
	}
	// And an empty schema must not probe at all.
	p := &placementProbe{mismatched: "orders"}
	if err := PreflightShardPlacement(context.Background(), &ir.Schema{}, p); err != nil {
		t.Fatalf("an empty schema was refused: %v", err)
	}
	if p.called != 0 {
		t.Errorf("probe ran %d times on an empty schema; it should not run at all", p.called)
	}
}
