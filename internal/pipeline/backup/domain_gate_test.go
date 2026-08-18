// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_BackupDispatchRoster is this package's instantiation of
// the Bug 233 gate (audit A-3): every column-type dispatch either reads the
// STORAGE type through ir.UnwrapDomain or carries a written, code-verified
// reason below.
//
// See the domaingate package doc for what a pass proves and what it does not.
func TestDomainTransparency_BackupDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "pipeline/backup",
		// 1 dispatch site; the floor holds the shape.
		MinSites: 1,
		Allowed:  backupDomainDispatchExemptions,
	})
}

var backupDomainDispatchExemptions = map[string]string{
	"chain_restore.go:hasIdentityColumn:c.Type": "IMPOSSIBLE-SHAPE: the pre-scan fires only for an " +
		"AutoIncrement ir.Integer (`intT.AutoIncrement`), and PostgreSQL — the only DOMAIN producer — does not " +
		"allow GENERATED … AS IDENTITY (or SERIAL) on a DOMAIN-typed column, so ir.Domain never wraps an " +
		"AutoIncrement integer and the arm is unreachable for a domain. Same argument as postgres " +
		"schema_writer.go:SyncIdentitySequences / cutover_sequence.",
}
