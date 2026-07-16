// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// stageTempDirs lists the sluice-d1-stage-* temp dirs currently on
// disk, so the test can assert the staging path leaves none behind.
func stageTempDirs(t *testing.T) map[string]bool {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "sluice-d1-stage-*"))
	if err != nil {
		t.Fatalf("glob temp dirs: %v", err)
	}
	set := make(map[string]bool, len(matches))
	for _, m := range matches {
		set[m] = true
	}
	return set
}

// TestStageD1Source_CanceledCtxCleansUp pins the --stage-local
// cancellation fix: staging used to run on context.Background()
// (Ctrl-C-deaf — the interrupt neither stopped the replica nor
// released the temp dir), and now receives the signal-aware command
// context. A canceled ctx must (a) abort the staging with the
// context's error before any network I/O, and (b) leave no
// sluice-d1-stage-* temp dir behind — the error path removes it.
func TestStageD1Source_CanceledCtxCleansUp(t *testing.T) {
	// A syntactically valid DSN + token so openD1Client succeeds and
	// the flow reaches the first ctx-aware call (the ping's HTTP
	// request, which fails on the canceled ctx before dialing).
	t.Setenv("CLOUDFLARE_API_TOKEN", "test-token")

	before := stageTempDirs(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	path, cleanup, err := stageD1Source(ctx, "d1://acct-id/db-id", "")
	if err == nil {
		if cleanup != nil {
			cleanup()
		}
		t.Fatal("expected the canceled ctx to abort staging; got nil error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("staging error must carry the context cancellation; got %v", err)
	}
	if path != "" || cleanup != nil {
		t.Errorf("error path must return no staged path/cleanup; got path=%q cleanup=%v", path, cleanup != nil)
	}

	after := stageTempDirs(t)
	for dir := range after {
		if !before[dir] {
			t.Errorf("staging leaked temp dir %s on the cancel path", dir)
		}
	}
}
