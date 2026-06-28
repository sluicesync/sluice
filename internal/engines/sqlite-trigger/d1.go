// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// EngineNameD1 is the short identifier the sibling `d1-trigger` engine
// (ADR-0136) self-registers under. It lives here — alongside the shared trigger
// logic the D1 engine reuses — so the backend's operator recovery hints and the
// engine registration agree on one spelling. The d1-trigger package references
// this constant for [d1trigger.Engine.Name].
const EngineNameD1 = "d1-trigger"

// The D1 entry points below are the shared trigger logic (setup/teardown/CDC/
// snapshot) bound to the D1 backend (ADR-0136). The `d1-trigger` engine is a
// thin wrapper that delegates cold-start reads to the `d1` engine and the CDC/
// setup surfaces to these functions — so Phase 2 is transport substitution, not
// new CDC logic: every one of these just builds the D1 [backend] and calls the
// SAME function the local file engine uses.

// SetupD1 installs the trigger engine's source-side state on a live Cloudflare
// D1 database over the HTTP query API (the `d1-trigger` analogue of [Setup]).
// The DSN is the `d1://` form; the API token is env-only (CLOUDFLARE_API_TOKEN).
func SetupD1(ctx context.Context, dsn string, opts SetupOptions) (*Plan, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	return setup(ctx, b, opts)
}

// TeardownD1 removes the trigger engine's source-side state from a live D1
// database (the `d1-trigger` analogue of [Teardown]).
func TeardownD1(ctx context.Context, dsn string, opts TeardownOptions) (*Plan, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	return teardown(ctx, b, opts)
}

// OpenD1CDCReader opens the trigger CDC reader against a live D1 database (the
// `d1-trigger` analogue of [Engine.OpenCDCReader]).
func OpenD1CDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	return openCDCReaderBackend(ctx, b)
}

// OpenD1SnapshotStream opens the trigger-native snapshot→CDC handoff against a
// live D1 database (the `d1-trigger` analogue of [Engine.OpenSnapshotStream]).
// The cold-start bulk copy uses the lossless `d1` reader (ADR-0132); the CDC tail
// polls the change-log over the same HTTP transport.
func OpenD1SnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	return openSnapshotStream(ctx, b)
}
