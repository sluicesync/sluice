// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import "sluicesync.dev/sluice/internal/ir"

// Capability fixtures for the restore-verbatim gate unit tests. Each mirrors
// the semantically-relevant slice of the real engine's [ir.Capabilities]
// declaration so the gate tests pin the CAPABILITY dispatch rather than
// engine-name strings. Mirrors of the pipeline-root test copies (caps_test.go /
// streamer_warn_test.go), duplicated so the carved-out backup test tree does
// not import root's.
var (
	// capsSlotPG mirrors the slot-based `postgres` engine hosting the PG
	// extension catalog and the ADR-0047 verbatim tier.
	capsSlotPG = ir.Capabilities{
		CDC:                    ir.CDCLogicalReplication,
		PostgresBackend:        true,
		PGExtensionCatalog:     true,
		VerbatimExtensionTypes: true,
	}

	// capsMySQL mirrors the vanilla `mysql` engine: binlog CDC, MySQL DDL
	// dialect, not a PG server.
	capsMySQL = ir.Capabilities{
		CDC:        ir.CDCBinlog,
		DDLDialect: ir.DDLDialectMySQL,
	}

	// capsTxKiller mirrors the Vitess-backed flavors that kill long
	// transactions (planetscale / vitess).
	capsTxKiller = ir.Capabilities{TransactionKiller: true}
)
