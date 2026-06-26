// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package main

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/pipeline"
)

// installReloadHandler is a no-op on Windows: SIGHUP does not exist there,
// so fleet hot-reload (ADR-0122 §3) is POSIX-only. The supervisor's
// Reconcile is portable and unit-tested cross-platform; only the SIGHUP
// trigger is gated out here. Operators on Windows change the fleet by
// restarting the process (a clean Ctrl-C drains every sync, then re-run).
func installReloadHandler(ctx context.Context, configPath string, _ *pipeline.Supervisor) {
	slog.InfoContext(ctx, "sync run: config hot-reload (SIGHUP) is not available on Windows; restart the process to change the fleet", slog.String("config", configPath))
}
