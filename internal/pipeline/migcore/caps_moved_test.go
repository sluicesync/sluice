// Copied from internal/pipeline/caps_test.go when the RLS / partition /
// inheritance preflights moved into this package (audit W4 H3): backup cannot
// import pipeline, so the preflights had to live where both can reach them,
// and their tests need the same capability fixtures. Deliberately a COPY --
// the two packages test different things and a shared fixture would couple
// them; if they drift, that is information, not a defect.
// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

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

// stubWriterNoChecker is a RowWriter that deliberately implements nothing
// beyond WriteRows — the "engine without the optional surface" case the RLS
// target-side preflight has to skip rather than refuse.
//
// Copied alongside the capability fixtures for the same reason: the
// preflights moved here so `backup` could reach them, and their tests moved
// with them.
type stubWriterNoChecker struct{}

func (stubWriterNoChecker) WriteRows(_ context.Context, _ *ir.Table, _ <-chan ir.Row) error {
	return nil
}
