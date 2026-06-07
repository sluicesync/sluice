package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestOpenRowReader_WorkloadGate pins the gate that decides whether
// OpenRowReader injects `SET workload='olap'` (engine.go): it fires only
// for VStream (vtgate) flavors. Vanilla MySQL has no `workload` session
// variable, so it must NOT match — a regression that widened the gate
// would break every vanilla-MySQL read with an "unknown system variable".
func TestOpenRowReader_WorkloadGate(t *testing.T) {
	if got := (Engine{Flavor: FlavorVanilla}).Capabilities().CDC; got == ir.CDCVStream {
		t.Errorf("vanilla MySQL CDC = %v; must NOT be CDCVStream (else it gets workload=olap and breaks)", got)
	}
	if got := (Engine{Flavor: FlavorPlanetScale}).Capabilities().CDC; got != ir.CDCVStream {
		t.Errorf("planetscale CDC = %v; want CDCVStream (the workload=olap gate)", got)
	}
}
