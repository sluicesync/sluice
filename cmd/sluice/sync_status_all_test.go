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
	list := func(_ context.Context, driver, dsn, _ string) ([]ir.StreamStatus, error) {
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

// TestCollectFleetStreams_ControlKeyspaceKeyed pins that the dedup key
// includes the per-sync control-keyspace (task 1): two syncs sharing one
// target server but reading control tables from DIFFERENT sidecar keyspaces
// are queried SEPARATELY (their stream rows live in different keyspaces), and
// the control-keyspace is passed through to the lister.
func TestCollectFleetStreams_ControlKeyspaceKeyed(t *testing.T) {
	fleet := &SyncFleetConfig{Syncs: []SyncSpec{
		{StreamID: "a", TargetDriver: "planetscale", Target: "mysql://shared/db", ControlKeyspace: "ks_a"},
		{StreamID: "b", TargetDriver: "planetscale", Target: "mysql://shared/db", ControlKeyspace: "ks_b"},
		{StreamID: "c", TargetDriver: "planetscale", Target: "mysql://shared/db", ControlKeyspace: "ks_a"}, // dup of a
	}}

	var gotKeyspaces []string
	list := func(_ context.Context, _, _, controlKeyspace string) ([]ir.StreamStatus, error) {
		gotKeyspaces = append(gotKeyspaces, controlKeyspace)
		return []ir.StreamStatus{{StreamID: "s-" + controlKeyspace, UpdatedAt: time.Now()}}, nil
	}

	streams, err := collectFleetStreams(context.Background(), fleet, &strings.Builder{}, list)
	if err != nil {
		t.Fatalf("collectFleetStreams: %v", err)
	}
	// a and b query separately (distinct keyspaces); c dedupes against a.
	if len(gotKeyspaces) != 2 {
		t.Errorf("listed %d times; want 2 (distinct control keyspaces, c deduped): %v", len(gotKeyspaces), gotKeyspaces)
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

	list := func(_ context.Context, _, dsn, _ string) ([]ir.StreamStatus, error) {
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

// TestCollectFleetStreams_AllUnreachableIsError pins the boundary of
// the failure-isolation discipline: PARTIAL unreachability keeps exit
// 0 with per-target lines (the test above), but when EVERY configured
// target errored the roll-up must return an error — an all-dead fleet
// rendering an empty-but-successful table would tell a script "no
// streams" when the truth is "no visibility". The error is a plain
// (uncoded) one: kong's generic exit 1.
func TestCollectFleetStreams_AllUnreachableIsError(t *testing.T) {
	fleet := &SyncFleetConfig{Syncs: []SyncSpec{
		{StreamID: "a", TargetDriver: "mysql", Target: "mysql://down1/db"},
		{StreamID: "b", TargetDriver: "postgres", Target: "postgres://down2/db"},
	}}

	list := func(_ context.Context, _, _, _ string) ([]ir.StreamStatus, error) {
		return nil, errors.New("connection refused")
	}

	var out strings.Builder
	streams, err := collectFleetStreams(context.Background(), fleet, &out, list)
	if err == nil {
		t.Fatalf("want an error when every target is unreachable; got nil with %d streams", len(streams))
	}
	if !strings.Contains(err.Error(), "all 2 configured target(s) unreachable") {
		t.Errorf("error should count the dead targets; got %q", err.Error())
	}
	if got := exitCodeLikeKong(err); got != 1 {
		t.Errorf("exit code = %d; want the generic 1", got)
	}
	// The per-target diagnosis lines still precede the terminal error.
	if got := strings.Count(out.String(), "unreachable:"); got != 2 {
		t.Errorf("want 2 per-target unreachable lines, got %d:\n%s", got, out.String())
	}
}
