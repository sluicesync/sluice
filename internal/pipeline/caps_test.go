// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "sluicesync.dev/sluice/internal/ir"

// Capability fixtures for the preflight / gate unit tests. Each
// mirrors the semantically-relevant slice of the real engine's
// [ir.Capabilities] declaration, so the tests pin the CAPABILITY
// dispatch (what the orchestrator actually branches on) rather than
// engine-name strings. Only the fields the gates read are set; the
// rest stay zero.
var (
	// capsSlotPG mirrors the slot-based `postgres` engine: a genuine
	// PG server whose CDC creates a logical replication slot, hosting
	// the PG extension catalog and the ADR-0047 verbatim tier.
	capsSlotPG = ir.Capabilities{
		CDC:                    ir.CDCLogicalReplication,
		PostgresBackend:        true,
		PGExtensionCatalog:     true,
		VerbatimExtensionTypes: true,
	}

	// capsTriggerPG mirrors `postgres-trigger`: a genuine PG server,
	// but slot-LESS trigger-based CDC — the replication-capability
	// preflight must skip it while the PG-server preflights still
	// fire — and (conservatively, like the real engine) neither the
	// extension catalog nor the verbatim tier.
	capsTriggerPG = ir.Capabilities{
		CDC:             ir.CDCTriggers,
		PostgresBackend: true,
	}

	// capsMySQL mirrors the vanilla `mysql` engine: binlog CDC,
	// MySQL DDL dialect, not a PG server.
	capsMySQL = ir.Capabilities{
		CDC:        ir.CDCBinlog,
		DDLDialect: ir.DDLDialectMySQL,
	}
)
