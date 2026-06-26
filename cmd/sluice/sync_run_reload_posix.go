// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"sluicesync.dev/sluice/internal/pipeline"
)

// installReloadHandler wires SIGHUP to a fleet hot-reload (ADR-0122 §3):
// each SIGHUP re-reads + re-validates configPath and reconciles the live
// supervisor without a process restart. A bad reload (parse / validation
// failure) is logged LOUDLY and the running fleet keeps going on the old
// config — reloadFleet returns the error before any sync is touched, so
// the live fleet survives a malformed or colliding reload untouched.
//
// The handler goroutine stops when ctx is cancelled (fleet shutdown), so
// no SIGHUP is left registered past the fleet's lifetime.
func installReloadHandler(ctx context.Context, configPath string, sup *pipeline.Supervisor) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		defer signal.Stop(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				slog.InfoContext(ctx, "sync run: SIGHUP received; reloading fleet config", slog.String("config", configPath))
				if err := reloadFleet(ctx, configPath, sup); err != nil {
					slog.ErrorContext(
						ctx,
						"sync run: config reload REFUSED; live fleet unchanged (still running on the previous config)",
						slog.String("config", configPath),
						slog.String("err", err.Error()),
					)
				}
			}
		}
	}()
}
