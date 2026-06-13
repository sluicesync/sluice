//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// SetBackupReadersOpenedHookForTest installs (or clears, with nil) the
// ADR-0088 consistency-oracle test seam: the supplied function fires
// the instant all N coordinated reader transactions have pinned their
// consistent snapshot AND the FLUSH TABLES WITH READ LOCK has been
// released. It is exported under the `integration` build tag only so
// the cross-engine consistency oracle in internal/pipeline can drive
// it (the oracle INSERTs into the source while the hook runs and
// asserts the backup artifact contains none of those writes). Not
// present in production builds.
func SetBackupReadersOpenedHookForTest(fn func()) {
	backupReadersOpenedHook = fn
}
