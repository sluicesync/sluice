// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// WarnSourceHostAdvisories consults the source engine's optional
// [ir.SourceHostAdvisor] surface and logs each returned advisory at
// WARN. The single chokepoint every entry point that opens a source
// calls — migrate, sync, and the backup CDC paths — so a managed-host
// hazard the engine can read off the DSN alone (a pooler endpoint, a
// DigitalOcean binlog-retention trap) is surfaced up front, before any
// connection. Engines without the surface are a silent no-op; the run
// always proceeds (these are advisories, not refusals — the refusal
// sibling is [ir.DSNValidator]).
//
// cdc reports whether the run anchors or consumes a CDC position (see
// [ir.SourceHostAdvisor]).
func WarnSourceHostAdvisories(ctx context.Context, source ir.Engine, dsn string, cdc bool) {
	advisor, ok := source.(ir.SourceHostAdvisor)
	if !ok {
		return
	}
	for _, a := range advisor.SourceHostAdvisories(dsn, cdc) {
		slog.WarnContext(ctx, a.Message, slog.String("hint", a.Hint))
	}
}
