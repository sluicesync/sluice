// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_MigcoreDispatchRoster is this package's instantiation
// of the Bug 233 gate (audit A-3): every column-type dispatch either reads the
// STORAGE type through ir.UnwrapDomain or carries a written, code-verified
// reason below.
//
// Both raw sites are exempt: one is a perf lane a DOMAIN-over-integer PK takes
// the correct slower side of (its orderability check, IsOrderablePKType, already
// unwraps), and one is a MySQL/Vitess-VStream-only lossy-float concern a PG
// domain can neither reach nor need. Neither is a correctness hazard.
//
// See the domaingate package doc for what a pass proves and what it does not.
func TestDomainTransparency_MigcoreDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "pipeline/migcore",
		// 2 dispatch sites; floor a touch under.
		MinSites: 1,
		Allowed:  migcoreDomainDispatchExemptions,
	})
}

var migcoreDomainDispatchExemptions = map[string]string{
	"chunk.go:CanParallelChunkTable:col.Type": "PERF-NOT-CORRECTNESS: this arm routes a single INTEGER PK to " +
		"the cheap MIN/MAX/divide chunk strategy. The orderability gate just above (IsOrderablePKType) already " +
		"UNWRAPS the domain, so a DOMAIN-over-integer PK is chunk-eligible and correctly falls to the " +
		"sampled-keyset strategy (StrategyKeysetSample) — a correct chunking, only not the integer fast path. " +
		"Admitting a domain-over-int PK to MIN/MAX/divide is a perf-parity widening for a perf chunk with a " +
		"matrix cell + pin, not a silent edit here.",
	"float_repair.go:SinglePrecisionFloatColumns:c.Type": "CROSS-ENGINE / VSTREAM-ONLY: this detector serves " +
		"the single-precision-FLOAT display-rounding loss that vttablet's rowstreamer inflicts on the VStream " +
		"COPY (ir.LossyFloatCopyReader). Every executing caller gates on that reader — applyVStreamFloatPolicy " +
		"returns early unless snap.Rows is a LossyFloatCopyReader, and the cold-start repair on " +
		"stream.Rows.(LossyFloatCopyReader) — a MySQL/Vitess property. Those sources have no CREATE DOMAIN, so " +
		"ir.Domain cannot occur in that population; Postgres — the only DOMAIN producer — transmits real/float4 " +
		"EXACTLY (its copy is not lossy), so a domain-over-real is correctly not flagged and unwrapping would " +
		"be inert for every executing caller while risking a spurious precision claim on a lossless PG column.",
}
