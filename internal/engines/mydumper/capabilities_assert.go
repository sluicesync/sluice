// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import "sluicesync.dev/sluice/internal/ir"

// Compile-time declarations of the ir interfaces this package's concrete
// types intentionally implement (the sqlite/d1 convention): the
// orchestrator discovers optional surfaces by runtime type-assertion, so a
// method-set break wouldn't fail the build — it would make the assertion
// quietly stop matching and the pipeline silently downgrade. Each line
// names what that downgrade would cost.
//
// The surfaces are intentionally NARROW. [RowReader] implements NONE of
// the batched/counter/bounds surfaces: dump chunks have no PK addressing,
// so every table must route through the single-reader whole-table copy —
// do NOT "fix" a missing-interface report by widening it (a cursor over a
// file re-scan would be both slow and, for non-round-trippable decoded
// values, the exact hazard [ir.BatchedReadDisqualifier] exists to veto on
// live engines).
var (
	// Engine-level: the registry holds Engine values; a break here is a
	// compile error (loud), pinned for auditability next to the optional
	// surfaces.
	_ ir.Engine = Engine{}

	// SchemaReader carries verify's count depth. Losing ir.Verifier turns
	// `sluice verify --depth count` against a dump source into an
	// "engine not supported" refusal.
	_ ir.SchemaReader = (*SchemaReader)(nil)
	_ ir.Verifier     = (*SchemaReader)(nil)

	// RowReader is the whole bulk-copy read surface for this engine.
	_ ir.RowReader = (*RowReader)(nil)
)
