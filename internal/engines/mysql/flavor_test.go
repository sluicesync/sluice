package mysql

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestFlavorString(t *testing.T) {
	cases := []struct {
		f    Flavor
		want string
	}{
		{FlavorVanilla, "mysql"},
		{FlavorPlanetScale, "planetscale"},
	}
	for _, c := range cases {
		if got := c.f.String(); got != c.want {
			t.Errorf("Flavor(%d).String() = %q; want %q", c.f, got, c.want)
		}
	}
}

// TestEngineZeroValueIsVanilla guards a behavioural promise: callers
// that have been using Engine{} (zero value) since before the Flavor
// field existed should continue to get vanilla MySQL behaviour.
func TestEngineZeroValueIsVanilla(t *testing.T) {
	e := Engine{}
	if e.Flavor != FlavorVanilla {
		t.Errorf("Engine{}.Flavor = %v; want FlavorVanilla", e.Flavor)
	}
	if e.Name() != "mysql" {
		t.Errorf("Engine{}.Name() = %q; want %q", e.Name(), "mysql")
	}
}

func TestEachFlavorHasCapabilities(t *testing.T) {
	flavors := []Flavor{FlavorVanilla, FlavorPlanetScale}
	for _, f := range flavors {
		caps := f.capabilities()
		// A flavor with no SchemaScope and BulkLoadNone is almost
		// certainly the zero-value fallback for an unknown flavor.
		// Every registered flavor should declare a real BulkLoad.
		if caps.BulkLoad == ir.BulkLoadNone {
			t.Errorf("%s: BulkLoad = None; flavors should declare a real bulk-load method", f)
		}
		// Every registered MySQL flavor uses a flat schema scope
		// (MySQL has no nested schemas the way Postgres does).
		if caps.SchemaScope != ir.SchemaScopeFlat {
			t.Errorf("%s: SchemaScope = %v; want Flat for MySQL family", f, caps.SchemaScope)
		}
	}
}

// TestVanillaCapabilities asserts the load-bearing pieces of the
// vanilla declaration. Other capability fields can drift over time
// without test churn; the ones below are the ones that downstream
// strategy depends on.
func TestVanillaCapabilities(t *testing.T) {
	caps := FlavorVanilla.capabilities()
	if caps.BulkLoad != ir.BulkLoadLoadDataInfile {
		t.Errorf("vanilla BulkLoad = %v; want LoadDataInfile", caps.BulkLoad)
	}
	if caps.CDC != ir.CDCBinlog {
		t.Errorf("vanilla CDC = %v; want Binlog", caps.CDC)
	}
	if !caps.SupportsPartitioning {
		t.Error("vanilla SupportsPartitioning = false; want true")
	}
	if !caps.SupportedTypes.Has(ir.ExtGeometry) {
		t.Error("vanilla should declare native Geometry support")
	}
}

// TestPlanetScaleCapabilities asserts the differences from vanilla
// that motivated introducing the Flavor concept. These are the load-
// bearing differences for downstream strategy.
func TestPlanetScaleCapabilities(t *testing.T) {
	caps := FlavorPlanetScale.capabilities()
	if caps.BulkLoad != ir.BulkLoadBatchedInsert {
		t.Errorf("planetscale BulkLoad = %v; want BatchedInsert (LOAD DATA INFILE not supported)", caps.BulkLoad)
	}
	if caps.CDC != ir.CDCVStream {
		t.Errorf("planetscale CDC = %v; want VStream (binlog not exposed; Vitess gRPC instead)", caps.CDC)
	}
	if caps.SupportsPartitioning {
		t.Error("planetscale SupportsPartitioning = true; want false (Vitess handles sharding)")
	}
	if caps.SupportedTypes.Has(ir.ExtGeometry) {
		t.Error("planetscale should not declare Geometry support (excluded for conservatism)")
	}
}

// TestVanillaPlanetScaleDifference makes the diff between flavors
// explicit. If a future change accidentally aligns them, this test
// will alert that the Flavor concept is no longer load-bearing — at
// which point we'd want to either revisit the modelling or remove
// the unnecessary distinction.
func TestVanillaPlanetScaleDifference(t *testing.T) {
	v := FlavorVanilla.capabilities()
	p := FlavorPlanetScale.capabilities()
	if v.BulkLoad == p.BulkLoad {
		t.Error("vanilla and planetscale BulkLoad are equal; one of them is wrong")
	}
}
