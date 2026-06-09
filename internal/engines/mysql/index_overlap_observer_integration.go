//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// SetIndexBuildStartObserverForTest installs (or clears, with nil) the
// test-only per-table index-build-start observability seam used by the
// ADR-0080 overlap integration test in internal/pipeline. It lives under
// the integration build tag so the hook plumbing is reachable from a test
// in a sibling package without exporting a production setter. Returns a
// restore func the caller defers to clear the hook. Mirrors the PG helper of
// the same name.
func SetIndexBuildStartObserverForTest(fn func(tableName string)) func() {
	prev := onIndexBuildStartObserver
	onIndexBuildStartObserver = fn
	return func() { onIndexBuildStartObserver = prev }
}
