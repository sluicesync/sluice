// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// The pgoutput lane's prior-shape seed (SLM-1c) at unit level: the
// name→OID bridge, the family matrix of every prior a seed can carry
// against every wire type the pair produces, the binding between the
// seeded predicate and the lane's wire declaration, and the cold-start
// projection agreement that keeps the raw source IR from reading as a
// phantom swap. The end-to-end pins run on real containers in
// pipeline.TestStreamer_PGSource_StoppedStreamZoneSwap.

// seedRel builds a projected relation the way a RelationMessage would
// arrive: (schema, name) plus typed columns.
func seedRel(t *testing.T, schema, name string, cols ...relationColumn) *relationCacheEntry {
	t.Helper()
	return &relationCacheEntry{Schema: schema, Name: name, Columns: cols}
}

func seedTable(schema, name string, cols ...*ir.Column) *ir.Table {
	return &ir.Table{Schema: schema, Name: name, Columns: cols}
}

func requireSeededRefusal(t *testing.T, err error, table, col, pair string) {
	t.Helper()
	if err == nil {
		t.Fatalf("seeded swap on %s.%s passed; want the session-TimeZone refusal", table, col)
	}
	for _, want := range []string{"cannot be forwarded", "while the stream was stopped", table, `column "` + col + `"`, pair, "TimeZone", "drained model", "sync stop --wait"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q; got: %v", want, err)
		}
	}
}

// TestCheckSeededSchemaRace_NameToOIDBridge pins how the (schema, name)
// seed reaches an OID-keyed relation: a bare-named seed binds to the
// reader's schema; a qualified seed keys under its own schema; a cached
// prior for the OID hands the comparison to checkSchemaRace and the
// seed is silent; a relation the seed does not know, a column the seed
// does not carry (ADD COLUMN while stopped), and a reader never seeded
// all prime as before.
func TestCheckSeededSchemaRace_NameToOIDBridge(t *testing.T) {
	naive := &ir.Column{Name: "c", Type: ir.DateTime{PrecisionUnspecified: true}}
	zonedWire := seedRel(t, "public", "events", typedCol(t, "id", pgtype.Int8OID, -1), typedCol(t, "c", pgtype.TimestamptzOID, -1))

	t.Run("bare seed binds to the reader's schema", func(t *testing.T) {
		r := &CDCReader{schema: "public"}
		r.SetSchemaSeed([]*ir.Table{seedTable("", "events", naive)})
		requireSeededRefusal(t, r.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16400, zonedWire), "public.events", "c", "timestamp and timestamptz")
	})
	t.Run("qualified seed keys under its own schema", func(t *testing.T) {
		r := &CDCReader{schema: "public"}
		r.SetSchemaSeed([]*ir.Table{seedTable("other", "events", naive)})
		if err := r.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16400, zonedWire); err != nil {
			t.Fatalf("a seed for other.events spoke for public.events: %v", err)
		}
		otherWire := seedRel(t, "other", "events", typedCol(t, "c", pgtype.TimestamptzOID, -1))
		requireSeededRefusal(t, r.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16401, otherWire), "other.events", "c", "timestamp and timestamptz")
	})
	t.Run("a cached prior owns the comparison", func(t *testing.T) {
		r := &CDCReader{schema: "public"}
		r.SetSchemaSeed([]*ir.Table{seedTable("", "events", naive)})
		cached := map[uint32]*relationCacheEntry{16400: zonedWire}
		if err := r.checkSeededSchemaRace(cached, 16400, zonedWire); err != nil {
			t.Fatalf("seed consulted despite a process-local prior: %v", err)
		}
	})
	t.Run("unknown relation, absent column, never seeded", func(t *testing.T) {
		r := &CDCReader{schema: "public"}
		r.SetSchemaSeed([]*ir.Table{seedTable("", "events", naive)})
		if err := r.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16402, seedRel(t, "public", "orders", typedCol(t, "c", pgtype.TimestamptzOID, -1))); err != nil {
			t.Fatalf("unknown relation refused: %v", err)
		}
		added := seedRel(t, "public", "events", typedCol(t, "id", pgtype.Int8OID, -1), typedCol(t, "d", pgtype.TimestamptzOID, -1))
		if err := r.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16400, added); err != nil {
			t.Fatalf("a column absent from the seed refused: %v", err)
		}
		unseeded := &CDCReader{schema: "public"}
		if err := unseeded.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16400, zonedWire); err != nil {
			t.Fatalf("an unseeded reader refused: %v", err)
		}
	})
	t.Run("a later seed replaces the earlier one", func(t *testing.T) {
		r := &CDCReader{schema: "public"}
		r.SetSchemaSeed([]*ir.Table{seedTable("", "events", naive)})
		r.SetSchemaSeed([]*ir.Table{seedTable("", "events", &ir.Column{Name: "c", Type: ir.Timestamp{WithTimeZone: true}})})
		if err := r.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16400, zonedWire); err != nil {
			t.Fatalf("the replaced seed still spoke: %v", err)
		}
	})
}

// TestCheckSeededSchemaRace_PriorFamilyMatrix is the Bug-74-shaped pin
// on the seed's INPUT families: every prior type a seed can carry for
// the timestamp pair — the Postgres schema reader's projection (cold
// start), the Postgres CDC projection persisted as history, the Postgres
// target's read-back and the MySQL target's read-back (the SLM-1b
// witness) — × every wire type the pair produces, scalar and array, with
// the verdict derived from the zone flags alone. Precision differs
// across the priors on purpose.
func TestCheckSeededSchemaRace_PriorFamilyMatrix(t *testing.T) {
	priors := []struct {
		name  string
		typ   ir.Type
		zoned bool
		array bool
	}{
		{"schema-reader timestamptz", ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true}, true, false},
		{"schema-reader timestamp", ir.DateTime{PrecisionUnspecified: true}, false, false},
		{"history timestamptz(6)", ir.Timestamp{WithTimeZone: true, Precision: 6}, true, false},
		{"history timestamp(3)", ir.DateTime{Precision: 3}, false, false},
		{"pg target timestamptz", ir.Timestamp{WithTimeZone: true, Precision: 6}, true, false},
		{"pg target timestamp", ir.DateTime{Precision: 6}, false, false},
		{"mysql target TIMESTAMP(6)", ir.Timestamp{WithTimeZone: true, Precision: 6}, true, false},
		{"mysql target DATETIME(6)", ir.DateTime{Precision: 6}, false, false},
		{"history timestamptz[]", ir.Array{Element: ir.Timestamp{WithTimeZone: true}}, true, true},
		{"history timestamp[]", ir.Array{Element: ir.DateTime{}}, false, true},
	}
	wires := []struct {
		name  string
		oid   uint32
		zoned bool
		array bool
	}{
		{"timestamptz", pgtype.TimestamptzOID, true, false},
		{"timestamp", pgtype.TimestampOID, false, false},
		{"timestamptz(3)", pgtype.TimestamptzOID, true, false},
		{"timestamptz[]", pgtype.TimestamptzArrayOID, true, true},
		{"timestamp[]", pgtype.TimestampArrayOID, false, true},
	}
	refusals := 0
	for _, p := range priors {
		for _, w := range wires {
			t.Run(p.name+"/"+w.name, func(t *testing.T) {
				r := &CDCReader{schema: "public"}
				r.SetSchemaSeed([]*ir.Table{seedTable("", "events", &ir.Column{Name: "c", Type: p.typ})})
				typmod := int32(-1)
				if strings.HasSuffix(w.name, "(3)") {
					typmod = 3
				}
				wire := seedRel(t, "public", "events", typedCol(t, "c", w.oid, typmod))
				err := r.checkSeededSchemaRace(map[uint32]*relationCacheEntry{}, 16400, wire)
				want := p.array == w.array && p.zoned != w.zoned
				switch {
				case want && err == nil:
					t.Fatalf("prior %s against wire %s primed; want the refusal", p.name, w.name)
				case !want && err != nil:
					t.Fatalf("prior %s against wire %s refused a non-swap: %v", p.name, w.name, err)
				case want:
					refusals++
					pair := "timestamp and timestamptz"
					if w.array {
						pair = "timestamp[] and timestamptz[]"
					}
					requireSeededRefusal(t, err, "public.events", "c", pair)
				}
			})
		}
	}
	// Anti-vacuity, derived: 4 zoned scalar priors × 1 naive scalar wire +
	// 4 naive scalar priors × 2 zoned scalar wires + 2 array cells = 14.
	if refusals != 14 {
		t.Fatalf("%d refusing cells; want exactly 14 — the matrix lost or grew a family without this derivation moving", refusals)
	}
}

// TestSeededSessionTZSwapPair_AgreesWithTheWireDeclaration binds the
// lane's two arms: for every ordered pair of wire OIDs the pgoutput
// projection can produce for the temporal families (the four zone-pair
// members, their arrays, and the non-member temporals), the seeded
// IR-typed predicate refuses exactly the pairs [sessionTZSwapPair]
// refuses, with the same pair label. Two facts pinned separately (the
// wire declaration by the docsync roster, ir.ZoneFamily by its own
// matrix) would leave the ARGUMENT — that the seed refuses what the wire
// refuses — unpinned; this is that binding.
func TestSeededSessionTZSwapPair_AgreesWithTheWireDeclaration(t *testing.T) {
	oids := []uint32{
		pgtype.TimeOID, pgtype.TimetzOID, pgtype.TimestampOID, pgtype.TimestamptzOID,
		pgtype.TimeArrayOID, pgtype.TimetzArrayOID, pgtype.TimestampArrayOID, pgtype.TimestamptzArrayOID,
		pgtype.DateOID, pgtype.IntervalOID, pgtype.DateArrayOID, pgtype.TextOID,
	}
	agreeingSwaps := 0
	for _, a := range oids {
		for _, b := range oids {
			pc, cc := typedCol(t, "c", a, -1), typedCol(t, "c", b, -1)
			wirePair, wireSwap := sessionTZSwapPair(pc, cc)
			seedPair, seedSwap := seededSessionTZSwapPair(pc.Type, cc.Type)
			if wireSwap != seedSwap || wirePair != seedPair {
				t.Errorf("OID %d → %d (%v → %v): wire says (%q, %v), seed says (%q, %v)", a, b, pc.Type, cc.Type, wirePair, wireSwap, seedPair, seedSwap)
			}
			if wireSwap && seedSwap {
				agreeingSwaps++
			}
		}
	}
	// Anti-vacuity: four pairs × two directions, scalar and array.
	if agreeingSwaps != 8 {
		t.Fatalf("%d agreeing swap cells; want exactly 8 (time⇄timetz, timestamp⇄timestamptz, each scalar and array, both directions)", agreeingSwaps)
	}
}

// TestSchemaSeed_ColdStartProjectionsAgree pins the premise the
// cold-start seed rests on: the schema reader's projection of every
// temporal spelling (the raw source IR the streamer seeds) and the CDC
// wire projection of the same column classify to the same zone family,
// so the first RelationMessage after a cold start can never read as a
// phantom swap against its own source's shape. Arrays included — the
// schema reader wraps through ArrayElement, the wire through
// pgArrayElementOID, and they must land on the same IR shape.
func TestSchemaSeed_ColdStartProjectionsAgree(t *testing.T) {
	cells := []struct {
		dataType, udt string
		oid           uint32
	}{
		{"timestamp without time zone", "timestamp", pgtype.TimestampOID},
		{"timestamp with time zone", "timestamptz", pgtype.TimestamptzOID},
		{"time without time zone", "time", pgtype.TimeOID},
		{"time with time zone", "timetz", pgtype.TimetzOID},
	}
	arrayOID := map[uint32]uint32{
		pgtype.TimestampOID:   pgtype.TimestampArrayOID,
		pgtype.TimestamptzOID: pgtype.TimestamptzArrayOID,
		pgtype.TimeOID:        pgtype.TimeArrayOID,
		pgtype.TimetzOID:      pgtype.TimetzArrayOID,
	}
	checked := 0
	for _, c := range cells {
		scalarMeta := columnMeta{DataType: c.dataType, UDTName: c.udt}
		for _, shape := range []struct {
			name string
			meta columnMeta
			oid  uint32
		}{
			{"scalar", scalarMeta, c.oid},
			{"array", columnMeta{DataType: "ARRAY", UDTName: "_" + c.udt, ArrayElement: &scalarMeta}, arrayOID[c.oid]},
		} {
			schemaType, err := translateType(shape.meta)
			if err != nil {
				t.Fatalf("%s %s: schema reader: %v", c.dataType, shape.name, err)
			}
			wireType, err := oidToType(shape.oid, -1)
			if err != nil {
				t.Fatalf("%s %s: wire: %v", c.dataType, shape.name, err)
			}
			sf, sz, sok := ir.ZoneFamily(schemaType)
			wf, wz, wok := ir.ZoneFamily(wireType)
			if !sok || !wok || sf != wf || sz != wz {
				t.Errorf("%s %s: schema reader projects %v (%q, zoned=%v, member=%v); wire projects %v (%q, zoned=%v, member=%v) — the cold-start seed would read as a phantom swap", c.dataType, shape.name, schemaType, sf, sz, sok, wireType, wf, wz, wok)
			}
			if _, swap := seededSessionTZSwapPair(schemaType, wireType); swap {
				t.Errorf("%s %s: the cold-start seed refuses its own source's shape", c.dataType, shape.name)
			}
			checked++
		}
	}
	if checked != 8 {
		t.Fatalf("%d cells checked; want 8", checked)
	}
}
