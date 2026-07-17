// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// WarnSourceHostAdvisories consults the source engine's optional
// [ir.SourceHostAdvisor] and [ir.SourceProbedAdvisor] surfaces and logs
// each returned advisory at WARN. The single chokepoint every entry
// point that opens a source calls — migrate, sync, and the backup CDC
// paths — so a managed-host hazard the engine can read off the DSN
// alone (a pooler endpoint, a DigitalOcean binlog-retention trap) or
// off one gated probe query (the AWS RDS MySQL retention setting, the
// Google Cloud SQL in-band fingerprint) is surfaced up front. Engines
// without the surfaces are a silent no-op; the run always proceeds
// (these are advisories, not refusals — the refusal sibling is
// [ir.DSNValidator]).
//
// cdc reports whether the run anchors or consumes a CDC position (see
// [ir.SourceHostAdvisor]).
func WarnSourceHostAdvisories(ctx context.Context, source ir.Engine, dsn string, cdc bool) {
	if advisor, ok := source.(ir.SourceHostAdvisor); ok {
		for _, a := range advisor.SourceHostAdvisories(dsn, cdc) {
			slog.WarnContext(ctx, a.Message, slog.String("hint", a.Hint))
		}
	}
	// The connection-probing sibling (detect-first advisories — e.g.
	// the AWS RDS MySQL binlog-retention probe). Engines gate the
	// probe on a host pattern themselves, so non-matching hosts never
	// pay a connection.
	if advisor, ok := source.(ir.SourceProbedAdvisor); ok {
		for _, a := range advisor.SourceProbedAdvisories(ctx, dsn, cdc) {
			slog.WarnContext(ctx, a.Message, slog.String("hint", a.Hint))
		}
	}
}
