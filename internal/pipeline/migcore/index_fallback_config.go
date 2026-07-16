// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import "sluicesync.dev/sluice/internal/ir"

// ApplyIndexBuildFallback threads the CLI-composed deploy-request
// index-build fallback (ADR-0148) to a freshly-opened target
// [ir.SchemaWriter] via the optional [ir.IndexBuildFallbackSetter]
// surface, before any index phase runs. nil fallback and engines without
// the setter (today: PG, SQLite) both skip cleanly — the orchestrators
// never learn what the fallback does, keeping it engine-neutral.
//
// Lives in migcore because every mode that runs the walled deferred
// CreateIndexes against a PlanetScale writer threads it: migrate, the
// sync cold-start (single- and multi-database), and `backup restore`
// (audit 2026-07-15 MED-A1 — the fallback originally reached only the
// migrate path, the exact "landed in one mode, silently missed
// siblings" class).
func ApplyIndexBuildFallback(target any, f ir.IndexBuildFallback) {
	if f == nil {
		return
	}
	if setter, ok := target.(ir.IndexBuildFallbackSetter); ok {
		setter.SetIndexBuildFallback(f)
	}
}
