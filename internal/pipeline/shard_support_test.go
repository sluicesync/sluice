// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"
)

// stubNoShardColumnSetter is a target type that intentionally does
// NOT implement ir.ShardColumnSetter. Used to pin the Shape-A
// cross-engine refusal — a future engine that ships without the
// surface must surface the refusal at openApplier-time before any
// CDC apply runs.
type stubNoShardColumnSetter struct{}

func TestCheckShardColumnSupport_DisengagedSkips(t *testing.T) {
	// Shape A not engaged → nil regardless of target shape.
	if err := checkShardColumnSupport(stubNoShardColumnSetter{}, ShardColumnSpec{}, "sync"); err != nil {
		t.Errorf("expected nil when not engaged; got %v", err)
	}
}

// stubShardColumnSetter implements ir.ShardColumnSetter — the
// engaged-but-supported happy path.
type stubShardColumnSetter struct {
	gotName string
	gotVal  any
}

func (s *stubShardColumnSetter) SetShardColumn(name string, value any) {
	s.gotName = name
	s.gotVal = value
}

func TestCheckShardColumnSupport_EngagedSupportedOK(t *testing.T) {
	target := &stubShardColumnSetter{}
	err := checkShardColumnSupport(target, ShardColumnSpec{Name: "shard", Value: "v1"}, "sync")
	if err != nil {
		t.Errorf("expected nil when target implements setter; got %v", err)
	}
}

func TestCheckShardColumnSupport_EngagedUnsupportedRefuses(t *testing.T) {
	err := checkShardColumnSupport(stubNoShardColumnSetter{}, ShardColumnSpec{Name: "shard", Value: "v1"}, "sync")
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "ir.ShardColumnSetter") {
		t.Errorf("error %q missing interface name", msg)
	}
	if !strings.Contains(msg, "shard=v1") {
		t.Errorf("error %q missing shard/value", msg)
	}
	if !strings.Contains(msg, "ADR-0048") {
		t.Errorf("error %q missing ADR reference", msg)
	}
}
