// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCollectFleetStreams_DedupAndMerge pins that a target shared by two
// syncs is queried ONCE and the streams across distinct targets are
// merged into one set.
func TestCollectFleetStreams_DedupAndMerge(t *testing.T) {
	fleet := &SyncFleetConfig{Syncs: []SyncSpec{
		{StreamID: "a", TargetDriver: "mysql", Target: "mysql://shared/db"},
		{StreamID: "b", TargetDriver: "mysql", Target: "mysql://shared/db"}, // same target
		{StreamID: "c", TargetDriver: "postgres", Target: "postgres://other/db"},
	}}

	var calls []string
	list := func(_ context.Context, driver, dsn string) ([]ir.StreamStatus, error) {
		calls = append(calls, driver+"|"+dsn)
		return []ir.StreamStatus{{StreamID: "stream-on-" + dsn, UpdatedAt: time.Now()}}, nil
	}

	streams, err := collectFleetStreams(context.Background(), fleet, &strings.Builder{}, list)
	if err != nil {
		t.Fatalf("collectFleetStreams: %v", err)
	}
	if len(calls) != 2 {
		t.Errorf("target listed %d times; want 2 (shared target deduped): %v", len(calls), calls)
	}
	if len(streams) != 2 {
		t.Errorf("merged streams = %d; want 2", len(streams))
	}
}

// TestCollectFleetStreams_FailureIsolated pins that an unreachable
// target is reported inline and skipped — it must NOT blank the rest of
// the fleet view (the supervisor's isolation discipline applied to
// status).
func TestCollectFleetStreams_FailureIsolated(t *testing.T) {
	fleet := &SyncFleetConfig{Syncs: []SyncSpec{
		{StreamID: "dead", TargetDriver: "mysql", Target: "mysql://down/db"},
		{StreamID: "live", TargetDriver: "postgres", Target: "postgres://up/db"},
	}}

	list := func(_ context.Context, _, dsn string) ([]ir.StreamStatus, error) {
		if strings.Contains(dsn, "down") {
			return nil, errors.New("connection refused")
		}
		return []ir.StreamStatus{{StreamID: "live-stream", UpdatedAt: time.Now()}}, nil
	}

	var out strings.Builder
	streams, err := collectFleetStreams(context.Background(), fleet, &out, list)
	if err != nil {
		t.Fatalf("collectFleetStreams returned err %v; want nil (dead target isolated)", err)
	}
	if len(streams) != 1 || streams[0].StreamID != "live-stream" {
		t.Errorf("merged streams = %+v; want just the live target's stream", streams)
	}
	if !strings.Contains(out.String(), "unreachable") {
		t.Errorf("expected an inline unreachable report, got %q", out.String())
	}
}
