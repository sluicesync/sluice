// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"sort"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"

	// Blank-imported for their init() self-registration — the same set the
	// index and view namespace rosters link, and the same reason.
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// WHICH TARGETS REFUSE TWO SOURCE TABLES THAT LAND ON ONE TABLE NAME, kept
// honest against the registry (roadmap item 148).
//
// # Why this gate, and why it is the one that matters most of the three
//
// The index roster (item 134) and the view roster (item 147) each cover a
// member of the `IF NOT EXISTS` silent-no-op class where the casualty is an
// OBJECT. This one covers the member where the casualty is ROWS: the second
// CREATE no-ops, the copy INSERTs under a name that resolves to the first
// table, both tables' rows end up in one, the surviving name's count is right,
// and nothing during the run says so. (A later `verify --depth count` WOULD
// catch it; the MIGRATION is what reports success.) A gate is
// the only thing that can catch this class, because nothing at runtime can.
//
// # What it reaches, stated so the name cannot be read as broader
//
//   - It asks each registered engine, through the connection-free
//     [ir.TableEmitPreflighter] surface, to refuse ONE real collision: two
//     tables in a schema whose names differ only in ASCII case. It does not
//     prove an engine's notion of "collides" is complete; the engine's own
//     tests do that.
//   - IT IS BLIND TO ANY COLLISION THAT NEEDS A CONNECTION TO SEE, and exactly
//     one engine's real collision is of that kind. See
//     tableNamespaceConnectionBearing below, which is deliberately NOT the
//     exemption map: an exemption is a safety argument, and those engines have
//     none — what they have is a DIFFERENT door (ir.TableNameFoldPreflighter,
//     roadmap item 149) with its own gates, named there. Merging the two maps
//     would let this roster's existence imply coverage it does not have.
var tableNamespaceExempt = map[string]string{
	// Source-only engines: no table DDL is ever emitted for them as a target,
	// so there is no target namespace to collide in. Same set and same reasons
	// as indexNamespaceExempt / viewNamespaceExempt.
	"csv":            "source only — OpenSchemaWriter returns ErrNotImplemented",
	"tsv":            "source only — the same flatfile engine as csv",
	"ndjson":         "source only — the same flatfile engine as csv",
	"mydumper":       "source only — a dump directory is read, never written",
	"d1":             "migrate/sync SOURCE only — OpenSchemaWriter returns ErrD1NotImplemented. There is no D1 target engine at all: a D1-bound migration writes a SQLite FILE via the `sqlite` engine, which is checked",
	"sqlite-trigger": "CDC source only (ADR-0134) — a SQLite target uses the `sqlite` engine",
	"d1-trigger":     "CDC source only — OpenSchemaWriter returns ErrNotImplemented",

	// The engines whose table emit cannot silently adopt another table's rows.
	// Both emit `CREATE TABLE IF NOT EXISTS`, so the no-op branch EXISTS — what
	// makes them safe is that two distinct source tables cannot reach one
	// target identifier: the name is schema-QUALIFIED and PostgreSQL compares
	// quoted identifiers byte-exactly. The one route that would collide,
	// NAMEDATALEN truncation at 63 bytes, is refused by validatePGIdentifier
	// ("table") inside emitTableDef under SLUICE-E-SCHEMA-IDENTIFIER-TOO-LONG,
	// and that refusal fires in the create-tables phase — before any row moves.
	"postgres":         "emits schema-qualified `CREATE TABLE IF NOT EXISTS` and compares identifiers byte-exactly; the only collision route (>63-byte truncation) is refused at emit under SLUICE-E-SCHEMA-IDENTIFIER-TOO-LONG, before the copy",
	"postgres-trigger": "delegates its whole schema write to postgres.Engine — same emit, same identifier-length refusal",
}

// tableNamespaceConnectionBearing is the third bucket, and it is NOT an
// exemption either. Each entry is an engine whose table emit CAN silently adopt
// another table's rows, whose collision this connection-free roster is
// STRUCTURALLY unable to see, and which answers the same question through
// [ir.TableNameFoldPreflighter] instead (roadmap item 149).
//
// It replaced a `tableNamespaceKnownGaps` map that said, correctly, that
// nothing asked these engines at all. What is asserted here is weaker than what
// is asserted above and the difference is the point: this roster proves only
// that the SURFACE is implemented. Whether the answer is right is proved by
// TestMySQLTableNameFold* (unit, both lct values) and, end to end against real
// servers with the variable actually set both ways, by
// TestMigrate_TableNameFold_PGToMySQL_* in internal/pipeline. Naming those here
// is the whole reason this bucket is allowed to exist.
var tableNamespaceConnectionBearing = map[string]string{
	"mysql": "emits a BARE `CREATE TABLE IF NOT EXISTS` and folds table names when the server runs " +
		"`lower_case_table_names != 0`, so a PostgreSQL source holding `orders` and `Orders` merges " +
		"there exactly as it would on SQLite — measured, Note 1050 and two rows in one table. The fold " +
		"is a property of the SERVER, not of the schema, and a connection-free check would have to " +
		"either stay silent or refuse that pair on every stock Linux MySQL (lct=0), so the question " +
		"lives on the connection-bearing surface.",
	"planetscale": "same engine as mysql, same statement, same server-side fold",
	"vitess":      "same engine as mysql, same statement, same server-side fold",
	"mariadb": "same engine as mysql, same statement, same server-side fold — measured identical on " +
		"mariadb:11.4 for lct=1, lct=2 and the non-ASCII pair",
}

// tableNamespaceCollision holds each covered target's OWN collision shape.
func sqliteTableNamespaceCollision() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{Schema: "public", Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Schema: "public", Name: "Orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
}

var tableNamespaceCollision = map[string]func() *ir.Schema{
	"sqlite": sqliteTableNamespaceCollision,
}

func TestEveryTargetRefusesACollidingTableNamespace(t *testing.T) {
	names := engines.Names()

	// Anti-vacuity floor, same shape as the index and view rosters'.
	if len(names) < 8 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list has drifted from cmd/sluice and "+
			"this gate is checking a subset of the fleet", len(names), names)
	}

	var checked, exempted, gapped []string
	for _, name := range names {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() reported %q but engines.Get did not return it", name)
		}
		if reason, isConnBearing := tableNamespaceConnectionBearing[name]; isConnBearing {
			if reason == "" {
				t.Errorf("%q is listed as connection-bearing with an EMPTY reason", name)
			}
			// The one thing this roster CAN assert for them: the surface
			// exists on the registered engine value. Without it, "answered
			// elsewhere" would be a promise rather than a check — which is
			// exactly what the gap map this replaced was recording.
			if _, ok := e.(ir.TableNameFoldPreflighter); !ok {
				t.Errorf("engine %q is listed as answering the table-name question through a connection, "+
					"but does not implement ir.TableNameFoldPreflighter — so NOTHING asks it, and this "+
					"roster is now stating coverage that does not exist (roadmap item 149)", name)
			}
			gapped = append(gapped, name)
			continue
		}
		if reason, isExempt := tableNamespaceExempt[name]; isExempt {
			if reason == "" {
				t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is "+
					"indistinguishable from an oversight", name)
			}
			exempted = append(exempted, name)
			continue
		}
		pf, isPreflighter := e.(ir.TableEmitPreflighter)
		if !isPreflighter {
			t.Errorf("engine %q is neither exempt nor a filed gap and does not implement "+
				"ir.TableEmitPreflighter, so nothing asks it whether two source tables would land on one "+
				"name before the copy writes their rows into one table", name)
			continue
		}
		fixture, hasFixture := tableNamespaceCollision[name]
		if !hasFixture {
			t.Errorf("engine %q is a covered target with no entry in tableNamespaceCollision, so this "+
				"roster is not actually checking it. State its collision shape, exempt it with the "+
				"argument that makes it safe, or file it as a gap.", name)
			continue
		}

		err := pf.PreflightTables(fixture())
		if err == nil {
			t.Errorf("engine %q ACCEPTED its own table-collision fixture. Its table emit carries IF NOT "+
				"EXISTS into a folded namespace, so the second CREATE returns OK and creates NOTHING — and "+
				"the rows destined for it are then INSERTed into the table that won the name, at exit 0 "+
				"(item 148).", name)
			continue
		}
		ce, coded := sluicecode.FromError(err)
		if !coded || ce.Code != sluicecode.CodeSchemaTableNameCollision {
			t.Errorf("engine %q refused the colliding table, but not with %s: %v\n"+
				"Operators route on the code. In particular this must NOT arrive as %s or %s — the thing "+
				"lost here is rows, not an object, and the remedy renames a different thing.",
				name, sluicecode.CodeSchemaTableNameCollision, err,
				sluicecode.CodeSchemaIndexNameCollision, sluicecode.CodeSchemaViewNameCollision)
			continue
		}
		checked = append(checked, name)

		// The MUTATION control, in the other direction: the SAME fixture with
		// the second table renamed out of the way must pass. Without it, an
		// engine whose preflight refused every schema would satisfy the
		// assertion above.
		clean := fixture()
		clean.Tables[len(clean.Tables)-1].Name = "sluice_roster_distinct_table_name"
		if err := pf.PreflightTables(clean); err != nil {
			t.Errorf("engine %q refused the same schema with the colliding table RENAMED (%v); the refusal "+
				"above is then not evidence of a namespace check", name, err)
		}
	}

	for name := range tableNamespaceExempt {
		if _, ok := engines.Get(name); !ok {
			t.Errorf("tableNamespaceExempt names %q, which is not a registered engine — the exemption is "+
				"stale; drop it", name)
		}
	}
	for name := range tableNamespaceConnectionBearing {
		if _, ok := engines.Get(name); !ok {
			t.Errorf("tableNamespaceConnectionBearing names %q, which is not a registered engine — the "+
				"entry is stale; drop it", name)
		}
		if _, alsoExempt := tableNamespaceExempt[name]; alsoExempt {
			t.Errorf("%q is listed BOTH as exempt and as connection-bearing; one of the two is a lie "+
				"about how that engine is covered", name)
		}
	}
	if len(exempted)+len(gapped) >= len(names) {
		t.Fatalf("every registered engine (%d exempt + %d connection-bearing of %d) is outside the "+
			"connection-free table-namespace check; the roster has stopped requiring anything",
			len(exempted), len(gapped), len(names))
	}
	sort.Strings(checked)
	if len(checked) < 1 {
		t.Errorf("no engine refuses a colliding table namespace (%v). sqlite is the one target whose "+
			"table emit can send a table's rows into another table, and it must.", checked)
	}
	sort.Strings(gapped)
	t.Logf("table-namespace roster: checked=%v exempt=%d answered-with-a-connection=%v (item 149)",
		checked, len(exempted), gapped)
}
