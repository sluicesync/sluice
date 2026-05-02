package mysql

import (
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// Flavor identifies which MySQL-compatible service the engine is
// targeting. The schema reader, row reader, DDL emitter, and value
// decoder are flavor-independent; the differences live in the
// [Capabilities] declaration each flavor returns.
//
// Adding a new flavor:
//
//  1. Add a Flavor constant to the iota block below.
//  2. Add a case to Flavor.String() with the engine name to register under.
//  3. Add an entry to the flavorCapabilities map with that flavor's
//     Capabilities. Be honest — declared capabilities drive runtime
//     strategy, and over-declaring causes silent failures at apply time.
//  4. Add an engines.Register call to init() in engine.go.
//  5. Add a Flavor case to Flavor.engineSpecificNotes() if there's
//     anything users should know that isn't already obvious from the
//     Capabilities table.
//  6. Update flavor_test.go to cover the new flavor.
type Flavor uint8

// Recognised flavors. The zero value is FlavorVanilla so that
// `Engine{}` continues to behave as a vanilla MySQL engine.
const (
	FlavorVanilla Flavor = iota
	FlavorPlanetScale
)

// String returns the engine-registry name used to look this flavor
// up at runtime (the value users put in their config under `driver:`).
func (f Flavor) String() string {
	switch f {
	case FlavorVanilla:
		return "mysql"
	case FlavorPlanetScale:
		return "planetscale"
	default:
		return fmt.Sprintf("flavor(%d)", uint8(f))
	}
}

// capabilities returns the capability declaration for this flavor.
func (f Flavor) capabilities() ir.Capabilities {
	if c, ok := flavorCapabilities[f]; ok {
		return c
	}
	// Unknown flavor: return an empty Capabilities. This is a
	// programming error (any registered Flavor should have an entry)
	// so the orchestrator will surface it loudly when it tries to
	// pick a strategy.
	return ir.Capabilities{}
}

// flavorCapabilities maps each Flavor to its declared capabilities.
// Kept as a package-level map so adding a new flavor is a one-line
// addition rather than a switch-statement edit.
var flavorCapabilities = map[Flavor]ir.Capabilities{
	// ---------------------------------------------------------------
	// FlavorVanilla — MySQL 8.0+ (the reference implementation).
	//
	// Includes Oracle MySQL, Percona Server, AWS RDS for MySQL, GCP
	// CloudSQL for MySQL, Azure Database for MySQL — anything that
	// behaves as upstream MySQL with the standard binary protocol.
	// ---------------------------------------------------------------
	FlavorVanilla: {
		BulkLoad:    ir.BulkLoadLoadDataInfile,
		CDC:         ir.CDCBinlog,
		SchemaScope: ir.SchemaScopeFlat,
		SupportedTypes: ir.NewTypeSet(
			ir.ExtEnum,     // column-level ENUM
			ir.ExtSet,      // column-level SET
			ir.ExtGeometry, // built-in spatial types
		),
		SupportsCheckConstraint:  true, // 8.0.16+
		SupportsGeneratedColumns: true,
		SupportsPartitioning:     true,
		EnumSupport:              ir.EnumColumnLevel,
		JSONSupport:              ir.JSONBinary,
		UnsignedIntegers:         true,
	},

	// ---------------------------------------------------------------
	// FlavorPlanetScale — PlanetScale's Vitess-backed MySQL service.
	//
	// PlanetScale is wire-compatible with MySQL but has documented
	// limitations relative to upstream MySQL:
	//
	//   - LOAD DATA INFILE is not supported. Use BatchedInsert.
	//   - Direct binlog access is not exposed. CDC is reported as
	//     None for now; PlanetScale's own change-feed mechanisms
	//     could be added as a separate option later.
	//   - Table partitioning is handled by Vitess sharding rather
	//     than user-defined PARTITION BY clauses.
	//   - Spatial types are excluded from SupportedTypes here for
	//     conservatism; flip the flag if a user reports they work.
	//
	// References:
	//   - Compatibility:    https://planetscale.com/docs/vitess/troubleshooting/mysql-compatibility
	//   - Reference dumper: https://github.com/planetscale/cli
	//                       (internal/dumper/sql_writer.go is the
	//                       battle-tested implementation of batched
	//                       INSERTs against PlanetScale; ~1 MB per
	//                       INSERT statement, plus `set workload=olap;`
	//                       on the session for OLAP-mode timeouts.)
	// ---------------------------------------------------------------
	FlavorPlanetScale: {
		BulkLoad:    ir.BulkLoadBatchedInsert,
		CDC:         ir.CDCNone,
		SchemaScope: ir.SchemaScopeFlat,
		SupportedTypes: ir.NewTypeSet(
			ir.ExtEnum,
			ir.ExtSet,
			// ExtGeometry intentionally excluded — see comment above.
		),
		SupportsCheckConstraint:  true,
		SupportsGeneratedColumns: true,
		SupportsPartitioning:     false, // sharding is Vitess's concern
		EnumSupport:              ir.EnumColumnLevel,
		JSONSupport:              ir.JSONBinary,
		UnsignedIntegers:         true,
	},
}
