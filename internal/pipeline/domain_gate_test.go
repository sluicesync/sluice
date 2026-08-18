// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_PipelineDispatchRoster is the orchestrator package's
// instantiation of the Bug 233 gate (audit A-3): every column-type dispatch
// either reads the STORAGE type through ir.UnwrapDomain or carries a written,
// code-verified reason below.
//
// One real fix landed with this gate: redact_preflight's mask:uuid preflight now
// unwraps, so a DOMAIN-over-uuid column is warned like a bare uuid column
// (pinned in domain_gate_pin_test.go). The remaining raw sites are exempt for
// the reasons below — an inference REPLACE that must not strip an explicit
// domain, and two perf/conservative lanes a domain column takes the correct
// (slower) side of.
//
// See the domaingate package doc for what a pass proves and what it does not.
func TestDomainTransparency_PipelineDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "pipeline",
		// 5 dispatch sites (1 now reads storage via UnwrapDomain); floor below.
		MinSites: 3,
		Allowed:  pipelineDomainDispatchExemptions,
	})
}

var pipelineDomainDispatchExemptions = map[string]string{
	"infer_types.go:inferTypeCandidate:col.Type": "UNWRAP-WOULD-BE-ACTIVELY-WRONG: infer-types REPLACES a " +
		"conservative source type (Integer/Text) with a richer inferred one (Boolean/Timestamp/JSON/UUID) via " +
		"ApplyInferredOverrides. An ir.Domain is an EXPLICIT user declaration carrying its own CHECK " +
		"constraints — not a conservative fallback — so a domain column is correctly NOT an inference " +
		"candidate. Unwrapping would make a domain-over-int/text a candidate and then silently rewrite the " +
		"column's type, discarding the CREATE DOMAIN and its CHECKs.",
	"migrate_parallel.go:isIntegerSinglePK:col.Type": "PERF-NOT-CORRECTNESS: this gates the raw-copy lane " +
		"(which inlines bare integer literals in its chunk predicate — safe only for a single integer PK). A " +
		"DOMAIN-over-integer PK is orderable (migcore.IsOrderablePKType unwraps it) and correctly falls to the " +
		"IR keyset loader, which pushes the chunk bound in the column's native semantics — no data loss, only " +
		"the raw-copy fast lane declined. Admitting a domain PK to the raw-copy lane is a perf-parity widening " +
		"that belongs in a perf chunk with a matrix cell + pin, not a silent edit in a transparency gate.",
	"where_pushdown_pg.go:pgPushdownEligibleTerms:c.Type": "CONSERVATIVE-REFUSAL: the ir.Date arm is a " +
		"fail-closed BELT on a time-bearing-literal term; a domain column is refused for server-side push-down " +
		"one call later at pgPushdownEligibleColumn's default arm regardless, so whether this belt fires for a " +
		"domain-over-date is moot — the term is not pushed. The predicate is still evaluated client-side " +
		"(rowpredicate, which DOES unwrap), so no data is lost.",
	"where_pushdown_pg.go:pgPushdownEligibleColumn:c.Type": "CONSERVATIVE-REFUSAL: a DOMAIN column matches no " +
		"arm and falls to the default, which REFUSES server-side publication push-down — the filter is then " +
		"evaluated client-side (correct, never silent-loss). The eligible arms are ground-truthed by the " +
		"real-PG oracle (TestPGPushdownEligible_EnvelopePin) for BARE base types only; unwrapping a domain " +
		"into them would widen the pushed surface past what the oracle validates. Admitting domains is a " +
		"push-down-envelope widening gated on extending that oracle, not a transparency fix.",
}
