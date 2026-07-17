// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
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
	// FlavorVitess is self-hosted Vitess (etcd + vtctld + vtgate +
	// vttablets you run yourself), as opposed to PlanetScale's hosted
	// Vitess. It shares PlanetScale's VStream engine code and
	// Capabilities verbatim (ADR-0073(a): start identical, diverge only
	// on evidence); the only differences are the registered name and the
	// self-hosted connection defaults applied at OpenCDCReader
	// (vstream_transport=plaintext, vstream_auth=none) so
	// `--source-driver=vitess` works against a typical self-hosted vtgate
	// without hand-set vstream_* params.
	FlavorVitess
	// FlavorMariaDB is MariaDB ≥ 10.11 LTS (roadmap item 73 Phase 1).
	// Wire-compatible with the MySQL driver but divergent in exactly the
	// places flavor_mariadb.go names: two MySQL-8-only information_schema
	// columns (srs_id, statistics.expression), the COLUMN_DEFAULT
	// reporting convention (quoted literals, the bare NULL keyword,
	// current_timestamp() with empty extra), the row-alias upsert
	// (`AS new` was never implemented — MariaDB keeps the legacy
	// VALUES() spelling), and the utf8mb4_0900_* ↔ utf8mb4_uca1400_*
	// collation families. CDC is CDCNone for Phase 1: MariaDB replicates
	// with domain-based GTIDs the MySQL binlog reader cannot parse; the
	// refusal is loud and coded (SLUICE-E-CDC-MARIADB-UNSUPPORTED),
	// Phase 3 threads the vendored go-mysql MariaDB-GTID support.
	FlavorMariaDB
)

// String returns the engine-registry name used to look this flavor
// up at runtime (the value users put in their config under `driver:`).
func (f Flavor) String() string {
	switch f {
	case FlavorVanilla:
		return "mysql"
	case FlavorPlanetScale:
		return "planetscale"
	case FlavorVitess:
		return "vitess"
	case FlavorMariaDB:
		return "mariadb"
	default:
		return fmt.Sprintf("flavor(%d)", uint8(f))
	}
}

// usesVStream reports whether this flavor's snapshot + CDC path is Vitess
// VStream (vtgate gRPC) rather than the MySQL binlog. Both the hosted
// PlanetScale flavor and the self-hosted vitess flavor are VStream-backed.
// Every VStream-vs-binlog branch gates on this rather than
// `== FlavorPlanetScale` so a new VStream flavor is correct everywhere by
// construction (the per-path dispatch, the resumable-COPY cursor, the
// `_vt_*` exclusion, the backup-snapshot path).
func (f Flavor) usesVStream() bool {
	return f == FlavorPlanetScale || f == FlavorVitess
}

// capabilities returns the capability declaration for this flavor.
func (f Flavor) capabilities() ir.Capabilities {
	// The self-hosted vitess flavor shares PlanetScale's capabilities
	// verbatim (ADR-0073(a): start identical, diverge only on evidence).
	// Keeping it out of the map and aliasing here guarantees zero drift;
	// when a real capability difference surfaces, give vitess its own map
	// entry and drop this alias.
	if f == FlavorVitess {
		f = FlavorPlanetScale
	}
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
		DDLDialect:               ir.DDLDialectMySQL,
	},

	// ---------------------------------------------------------------
	// FlavorPlanetScale — PlanetScale's Vitess-backed MySQL service.
	//
	// PlanetScale is wire-compatible with MySQL but has documented
	// limitations relative to upstream MySQL:
	//
	//   - LOAD DATA INFILE is not supported. Use BatchedInsert.
	//   - Direct binlog access is not exposed. CDC goes through Vitess's
	//     VStream gRPC protocol against the vtgate endpoint; sluice's
	//     [vstreamCDCReader] handles the GTID-keyed position tokens
	//     and snapshot-anchored COPY-mode handoff. See
	//     internal/engines/mysql/cdc_vstream.go and Bug 27 / ADR-0035
	//     for the spatial-types CDC bytes-parsing detail.
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
		BulkLoad: ir.BulkLoadBatchedInsert,
		CDC:      ir.CDCVStream,
		// VStream stamps positions per-transaction-commit AFTER the rows
		// (the VGTID follows its rows), so a schema snapshot and the rows
		// in the same tx share one position — restore must not trust a
		// schema anchor at EndPosition as proof of data (Bug 184).
		CDCPositionCommitsAfterRows: true,
		SchemaScope:                 ir.SchemaScopeFlat,
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
		DDLDialect:               ir.DDLDialectMySQL,

		// vtgate kills transactions at ~20s by default; the streamer's
		// AIMD controller and apply-batch-size warning both gate on
		// this (GitHub #18 / ADR-0052 DP-2). The self-hosted vitess
		// flavor inherits it via the capabilities alias — same vtgate,
		// same killer.
		TransactionKiller: true,
	},

	// ---------------------------------------------------------------
	// FlavorMariaDB — MariaDB ≥ 10.11 LTS (roadmap item 73 Phase 1).
	//
	// Honest Phase-1 declaration: bulk migrate source+target plus
	// backup/restore/verify. Deliberate differences from vanilla:
	//
	//   - CDC: CDCNone. MariaDB replicates with domain-based GTIDs
	//     (`0-100-38`) that the MySQL binlog reader's position codec
	//     cannot parse; OpenCDCReader and the sync/backup-stream
	//     preflights refuse loudly with SLUICE-E-CDC-MARIADB-UNSUPPORTED
	//     (roadmap item 73 Phase 3 threads the vendored go-mysql
	//     MariaDB-GTID support).
	//   - JSONSupport: JSONText, not JSONBinary. MariaDB JSON is a
	//     LONGTEXT alias — information_schema reports data_type
	//     'longtext' (plus an auto json_valid CHECK); there is no
	//     binary JSON storage to declare. JSON-identity recovery via
	//     the json_valid CHECK is item 73 Phase 2.
	//   - ExtGeometry excluded. MariaDB spells the column SRID
	//     attribute REF_SYSTEM_ID=n (MySQL 8's `SRID n` is a syntax
	//     error there), and it has no information_schema srs_id column
	//     to read one back from — carrying geometry through this flavor
	//     today would silently drop declared SRIDs on read and emit
	//     unparseable DDL on write. Refused loudly instead; the
	//     REF_SYSTEM_ID spelling + SHOW CREATE read-back is Phase 2.
	//   - BulkLoadLoadDataInfile: verified live — the scoping probe's
	//     restore-into-11.4 leg landed the full corpus byte-identically
	//     through the LOAD DATA LOCAL path, and MariaDB ships with
	//     local_infile=ON (both 11.4 and 10.11 defaults). The per-call
	//     BatchedInsert fallback (local_infile=OFF servers) carries the
	//     flavor's VALUES() upsert spelling.
	//
	// Version floor: MariaDB 10.11 LTS. The 11.4-only utf8mb4_0900_*
	// alias set is NOT relied on — the emitter maps 0900 collations to
	// their uca1400 equivalents, which exist on both supported LTS
	// lines (see mariadbTargetCollation). Older MariaDB may work but is
	// unpinned; the schema reader WARNs below the floor.
	//
	// Scoping probe (2026-07-16): sluice-testing
	// workspace/mariadb/scoping-probe.md; roadmap item 73.
	// ---------------------------------------------------------------
	FlavorMariaDB: {
		BulkLoad:    ir.BulkLoadLoadDataInfile,
		CDC:         ir.CDCNone, // Phase 3: MariaDB domain GTIDs
		SchemaScope: ir.SchemaScopeFlat,
		SupportedTypes: ir.NewTypeSet(
			ir.ExtEnum, // column-level ENUM
			ir.ExtSet,  // column-level SET
			// ExtGeometry intentionally excluded — see comment above.
		),
		SupportsCheckConstraint:  true, // MariaDB 10.2+ enforces CHECK
		SupportsGeneratedColumns: true,
		SupportsPartitioning:     true,
		EnumSupport:              ir.EnumColumnLevel,
		JSONSupport:              ir.JSONText, // LONGTEXT alias, see above
		UnsignedIntegers:         true,
		DDLDialect:               ir.DDLDialectMySQL,
	},
}
