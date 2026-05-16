// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// ADR-0047 — the verbatim passthrough tier, *below* the ADR-0032
// catalog. This file is the single home of the three-level
// determination so the decision reads as one named concept rather
// than scattered conditionals (a tenet point).
//
// For a column whose information_schema.data_type is USER-DEFINED and
// which is NOT one of the catalogued seven (ADR-0032), the question
// "what does sluice do with it?" has exactly three answers:
//
//   (a) RICH      — catalogued + operator-enabled via
//                    --enable-pg-extension → the rich ADR-0032 path
//                    (typmod decode, modifier synthesis, cross-engine
//                    translators). Unchanged by this ADR; decided by
//                    the catalog lookup BEFORE the tier check runs, so
//                    it never reaches verbatimTierFor.
//   (b) VERBATIM  — uncatalogued USER-DEFINED AND the run is provably
//                    same-engine-PG (live) or backup-marked-PG-only →
//                    capture pg_catalog.format_type(...) and the
//                    uncatalogued index AM / opclass verbatim
//                    (ir.VerbatimType). This ADR.
//   (c) REFUSE    — otherwise (cross-engine, or uncatalogued with no
//                    same-engine guarantee) → today's LOUD refusal.
//                    Unchanged; the cross-engine refusal default is
//                    NOT weakened.
//
// The reader only ever observes the (b)-vs-(c) fork: tier (a) is
// resolved earlier by the catalog dispatch in translateType. The
// boolean that distinguishes (b) from (c) is the orchestrator-set
// SchemaReader.verbatimPassthrough — true ONLY for a provably-
// same-engine PG → PG run or a PG backup (see
// SetVerbatimExtensionPassthrough + the pipeline wiring). The
// orchestrator stays engine-neutral: it toggles this via the optional
// [ir.VerbatimExtensionAware] surface using engine *names* only, never
// importing this package.

// verbatimTier is the named outcome of the ADR-0047 determination for
// an uncatalogued USER-DEFINED column.
type verbatimTier uint8

const (
	// verbatimTierRefuse is tier (c): no same-engine guarantee →
	// preserve today's loud refusal. The zero value, so a reader that
	// was never told otherwise refuses by default (loud-fail tenet).
	verbatimTierRefuse verbatimTier = iota
	// verbatimTierVerbatim is tier (b): capture + re-emit verbatim.
	verbatimTierVerbatim
)

// verbatimTierFor decides between tier (b) and tier (c) for an
// uncatalogued USER-DEFINED column. Tier (a) is NOT decided here — a
// catalogued+enabled column is dispatched by the catalog earlier in
// translateType and never reaches this function.
//
// The decision is intentionally a single predicate: the run either
// carries a same-engine-PG guarantee (live PG → PG, or a PG backup
// whose PG-restore-only constraint is enforced by the recorded
// lineage marker + the loud restore-time engine gate) or it does not.
func verbatimTierFor(verbatimPassthrough bool) verbatimTier {
	if verbatimPassthrough {
		return verbatimTierVerbatim
	}
	return verbatimTierRefuse
}
