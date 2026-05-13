// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// stubEmptyChecker is a fake [ir.RowWriter] + [ir.TableEmptyChecker]
// that returns canned IsTableEmpty results per table name.
type stubEmptyChecker struct {
	empty    map[string]bool
	probeErr error
	calls    []string
}

func (s *stubEmptyChecker) WriteRows(_ context.Context, _ *ir.Table, _ <-chan ir.Row) error {
	return errors.New("stubEmptyChecker.WriteRows should not be called by pre-flight")
}

func (s *stubEmptyChecker) IsTableEmpty(_ context.Context, table *ir.Table) (bool, error) {
	s.calls = append(s.calls, table.Name)
	if s.probeErr != nil {
		return false, s.probeErr
	}
	if v, ok := s.empty[table.Name]; ok {
		return v, nil
	}
	return true, nil
}

// TestPreflightColdStart_AllEmpty verifies the pre-flight returns nil
// when every table is empty.
func TestPreflightColdStart_AllEmpty(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users"}, {Name: "orders"}, {Name: "comments"},
		},
	}
	rw := &stubEmptyChecker{empty: map[string]bool{"users": true, "orders": true, "comments": true}}
	if err := preflightColdStart(context.Background(), schema, rw, false, preflightModeMigrate); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
	if len(rw.calls) != 3 {
		t.Errorf("expected 3 IsTableEmpty calls; got %d (%v)", len(rw.calls), rw.calls)
	}
}

// TestPreflightColdStart_PopulatedTableRefuses verifies the pre-flight
// returns errColdStartRefused when any table has data, and that the
// error wraps in a way operators can read.
func TestPreflightColdStart_PopulatedTableRefuses(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users"},
			{Name: "comments"},
		},
	}
	rw := &stubEmptyChecker{empty: map[string]bool{"users": true, "comments": false}}
	err := preflightColdStart(context.Background(), schema, rw, false, preflightModeMigrate)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !errors.Is(err, errColdStartRefused) {
		t.Errorf("expected errColdStartRefused via errors.Is; got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, `"comments"`) {
		t.Errorf("expected the populated table name in the error; got %q", msg)
	}
	if !strings.Contains(msg, "--force-cold-start") {
		t.Errorf("expected recovery hint mentioning --force-cold-start; got %q", msg)
	}
	if !strings.Contains(msg, "slot drop") {
		t.Errorf("expected recovery hint mentioning slot drop; got %q", msg)
	}
}

// TestPreflightColdStart_SyncModeHint verifies the streamer-mode
// recovery message names the GitHub #15 wedge shape and recommends
// `--reset-target-data` over the migrate-mode `--resume` path. The
// migrate-mode `--resume` hint would be misleading for a sync wedge
// because `sluice migrate --resume` is a different code path and
// doesn't apply to continuous-sync flows.
func TestPreflightColdStart_SyncModeHint(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "events"}}}
	rw := &stubEmptyChecker{empty: map[string]bool{"events": false}}
	err := preflightColdStart(context.Background(), schema, rw, false, preflightModeSync)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "--reset-target-data") {
		t.Errorf("sync-mode hint should recommend --reset-target-data; got %q", msg)
	}
	if !strings.Contains(msg, "#15") {
		t.Errorf("sync-mode hint should reference GitHub #15 to give operators a search anchor; got %q", msg)
	}
	if strings.Contains(msg, "sluice migrate") {
		t.Errorf("sync-mode hint should NOT point at `sluice migrate --resume`; that's the migrate-mode hint and confuses operators in sync flows; got %q", msg)
	}
}

// TestPreflightColdStart_ForceSkips verifies --force-cold-start
// bypasses the check entirely (no probes, no error).
func TestPreflightColdStart_ForceSkips(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "anything"}},
	}
	rw := &stubEmptyChecker{empty: map[string]bool{"anything": false}}
	if err := preflightColdStart(context.Background(), schema, rw, true, preflightModeMigrate); err != nil {
		t.Errorf("force=true should skip probe; got %v", err)
	}
	if len(rw.calls) != 0 {
		t.Errorf("force=true should issue no probes; got %d (%v)", len(rw.calls), rw.calls)
	}
}

// TestPreflightColdStart_ProbeErrorPropagates verifies a transient
// failure during the probe (network blip, missing privilege) surfaces
// to the operator rather than getting silently treated as "table is
// empty".
func TestPreflightColdStart_ProbeErrorPropagates(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	probe := errors.New("connection reset")
	rw := &stubEmptyChecker{probeErr: probe}
	err := preflightColdStart(context.Background(), schema, rw, false, preflightModeMigrate)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !errors.Is(err, probe) {
		t.Errorf("expected probe error to be wrapped; got %v", err)
	}
}

// stubWriterNoChecker is a RowWriter that intentionally does NOT
// implement TableEmptyChecker. The pre-flight should silently skip
// (preserve v0.3.0 behaviour for engines without the surface).
type stubWriterNoChecker struct{}

func (stubWriterNoChecker) WriteRows(_ context.Context, _ *ir.Table, _ <-chan ir.Row) error {
	return nil
}

// TestPreflightColdStart_NoCheckerSurfaceSkips verifies engines that
// don't implement TableEmptyChecker fall back to the v0.3.0 behaviour
// of running the bulk-copy without a probe. The check is opportunistic;
// missing surface != error.
func TestPreflightColdStart_NoCheckerSurfaceSkips(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	if err := preflightColdStart(context.Background(), schema, stubWriterNoChecker{}, false, preflightModeMigrate); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}
