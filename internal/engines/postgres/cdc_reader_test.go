// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

func TestEncodeDecodePGPos(t *testing.T) {
	cases := []struct {
		name string
		pos  pgPos
	}{
		{
			"canonical",
			pgPos{Slot: "sluice_slot", LSN: "0/16B7350"},
		},
		{
			"large lsn",
			pgPos{Slot: "custom_slot", LSN: "FFFFFFFF/FFFFFFFF"},
		},
		{
			"zero lsn",
			pgPos{Slot: "sluice_slot", LSN: "0/0"},
		},
		{
			// ADR-0051 / research finding F5: position carrying the
			// source-identity pin (systemid + timeline). Both fields
			// must round-trip cleanly so reconnect-time divergence
			// detection has the persisted values to compare against.
			"with source identity pin",
			pgPos{Slot: "sluice_slot", LSN: "0/16B7350", SystemID: "7351234567890123456", Timeline: 3},
		},
		{
			"with source identity pin, timeline=1 (fresh primary)",
			pgPos{Slot: "sluice_slot", LSN: "1/0", SystemID: "7351234567890123456", Timeline: 1},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			encoded, err := encodePGPos(c.pos)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if encoded.Engine != engineNamePostgres {
				t.Errorf("Engine = %q; want %q", encoded.Engine, engineNamePostgres)
			}
			got, ok, err := decodePGPos(encoded)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !ok {
				t.Fatalf("decode: ok=false; expected a valid position")
			}
			if !reflect.DeepEqual(got, c.pos) {
				t.Errorf("round-trip\n got = %#v\nwant = %#v", got, c.pos)
			}
		})
	}
}

// TestDecodePGPosPreADR0051CompatibleToken pins the additive-field
// promise: an older sluice persisted a position whose token has only
// {slot, lsn} (no systemid/timeline). decodePGPos must accept it
// cleanly, populating SystemID="" and Timeline=0 — the sentinel
// values the resume path uses to engage lazy pin-install
// (ADR-0051). Without this pin, a JSON-schema tightening could
// silently break in-flight upgrades.
func TestDecodePGPosPreADR0051CompatibleToken(t *testing.T) {
	// The exact JSON shape pre-ADR-0051 sluice wrote: just slot + lsn,
	// no systemid / timeline keys at all.
	legacy := ir.Position{
		Engine: engineNamePostgres,
		Token:  `{"slot":"sluice_slot","lsn":"0/16B7350"}`,
	}
	got, ok, err := decodePGPos(legacy)
	if err != nil {
		t.Fatalf("decode legacy token: %v", err)
	}
	if !ok {
		t.Fatal("decode legacy token: ok=false; want true")
	}
	if got.Slot != "sluice_slot" || got.LSN != "0/16B7350" {
		t.Errorf("slot/lsn round-trip\n got = %+v\nwant = slot=sluice_slot, lsn=0/16B7350", got)
	}
	if got.SystemID != "" {
		t.Errorf("legacy token must decode with SystemID=\"\" (sentinel for lazy-install); got %q", got.SystemID)
	}
	if got.Timeline != 0 {
		t.Errorf("legacy token must decode with Timeline=0 (sentinel for lazy-install); got %d", got.Timeline)
	}
}

// TestEncodePGPosOmitsZeroIdentityFields documents the wire-format
// contract: when SystemID is empty and Timeline is zero, the encoded
// JSON MUST NOT include those keys (omitempty). This keeps positions
// emitted by code paths that don't have a live IDENTIFY_SYSTEM (e.g.
// the cdc-snapshot path, the change-applier schema-cache path) byte-
// identical to pre-ADR-0051 sluice — a load-bearing invariant for
// "older positions are accepted unchanged" working in both
// directions.
func TestEncodePGPosOmitsZeroIdentityFields(t *testing.T) {
	got, err := encodePGPos(pgPos{Slot: "s1", LSN: "0/1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if strings.Contains(got.Token, "systemid") {
		t.Errorf("token contains \"systemid\" key with zero value: %q", got.Token)
	}
	if strings.Contains(got.Token, "timeline") {
		t.Errorf("token contains \"timeline\" key with zero value: %q", got.Token)
	}
}

// TestCheckSourceIdentity exercises the ADR-0051 divergence detector:
// match (silent), pre-ADR-0051 sentinel (lazy install — silent),
// timeline mismatch (refuse), sysid mismatch (refuse). The refusal
// MUST wrap ir.ErrPositionInvalid so the streamer's existing
// ADR-0022 fall-through path can re-route to cold-start, and MUST
// name both old and new (sysid, timeline) so operators can confirm
// the divergence matches their intended PITR/promotion event.
func TestCheckSourceIdentity(t *testing.T) {
	cases := []struct {
		name           string
		persistedSysID string
		persistedTL    int32
		liveSysID      string
		liveTL         int32
		wantErr        bool
		wantContains   []string
	}{
		{
			name:           "exact match — silent pass",
			persistedSysID: "7351234567890123456",
			persistedTL:    1,
			liveSysID:      "7351234567890123456",
			liveTL:         1,
			wantErr:        false,
		},
		{
			name:           "pre-ADR-0051 sentinel — lazy-install silent pass",
			persistedSysID: "",
			persistedTL:    0,
			liveSysID:      "7351234567890123456",
			liveTL:         1,
			wantErr:        false,
		},
		{
			name:           "timeline diverges (promotion / PITR same-cluster) — refuse",
			persistedSysID: "7351234567890123456",
			persistedTL:    1,
			liveSysID:      "7351234567890123456",
			liveTL:         2,
			wantErr:        true,
			wantContains:   []string{"systemid=\"7351234567890123456\"", "timeline=1", "timeline=2", "PITR"},
		},
		{
			name:           "sysid diverges (pointed at wrong instance) — refuse",
			persistedSysID: "7351234567890123456",
			persistedTL:    1,
			liveSysID:      "9999999999999999999",
			liveTL:         1,
			wantErr:        true,
			wantContains:   []string{"systemid=\"7351234567890123456\"", "systemid=\"9999999999999999999\"", "different instance"},
		},
		{
			name:           "both diverge — refuse (e.g. failover to a fresh primary)",
			persistedSysID: "7351234567890123456",
			persistedTL:    1,
			liveSysID:      "9999999999999999999",
			liveTL:         2,
			wantErr:        true,
			wantContains:   []string{"systemid=\"7351234567890123456\"", "timeline=1", "systemid=\"9999999999999999999\"", "timeline=2"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := checkSourceIdentity(context.Background(), "sluice_slot", c.persistedSysID, c.persistedTL, c.liveSysID, c.liveTL)
			if c.wantErr {
				if err == nil {
					t.Fatal("expected error; got nil")
				}
				if !errors.Is(err, ir.ErrPositionInvalid) {
					t.Errorf("error must wrap ir.ErrPositionInvalid so the streamer's ADR-0022 fall-through engages; got: %v", err)
				}
				for _, sub := range c.wantContains {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error message missing substring %q\nfull message: %s", sub, err.Error())
					}
				}
				// Operator recovery hint must name the slot.
				if !strings.Contains(err.Error(), "sluice_slot") {
					t.Errorf("error must name the slot for the operator recovery command; got: %s", err.Error())
				}
				if !strings.Contains(err.Error(), "sluice slot drop") {
					t.Errorf("error must point at the slot-drop recovery command; got: %s", err.Error())
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestEncodePGPosRejectsEmptyFields(t *testing.T) {
	if _, err := encodePGPos(pgPos{Slot: "", LSN: "0/1"}); err == nil {
		t.Error("expected error for empty slot")
	}
	if _, err := encodePGPos(pgPos{Slot: "x", LSN: ""}); err == nil {
		t.Error("expected error for empty lsn")
	}
}

func TestDecodePGPosFromNowSentinel(t *testing.T) {
	_, ok, err := decodePGPos(ir.Position{})
	if err != nil {
		t.Fatalf("zero position should not error: %v", err)
	}
	if ok {
		t.Errorf("zero position should report ok=false (from-now sentinel)")
	}
}

func TestDecodePGPosErrors(t *testing.T) {
	cases := []struct {
		name string
		in   ir.Position
	}{
		{"wrong engine", ir.Position{Engine: "mysql", Token: `{"slot":"x","lsn":"0/1"}`}},
		{"empty token with non-empty engine", ir.Position{Engine: "postgres", Token: ""}},
		{"malformed json", ir.Position{Engine: "postgres", Token: "not json"}},
		{"missing slot", ir.Position{Engine: "postgres", Token: `{"lsn":"0/1"}`}},
		{"missing lsn", ir.Position{Engine: "postgres", Token: `{"slot":"x"}`}},
		{"unparseable lsn", ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"nope"}`}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, _, err := decodePGPos(c.in)
			if err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

// TestOIDToType walks the OID-to-IR mapping. Coverage focuses on the
// types the conservative integration test will actually see, plus a
// couple of typmod-decoding cases.
func TestOIDToType(t *testing.T) {
	cases := []struct {
		name   string
		oid    uint32
		typmod int32
		want   ir.Type
	}{
		{"bool", pgtype.BoolOID, -1, ir.Boolean{}},
		{"int8", pgtype.Int8OID, -1, ir.Integer{Width: 64}},
		{"int4", pgtype.Int4OID, -1, ir.Integer{Width: 32}},
		{"int2", pgtype.Int2OID, -1, ir.Integer{Width: 16}},
		{"float4", pgtype.Float4OID, -1, ir.Float{Precision: ir.FloatSingle}},
		{"float8", pgtype.Float8OID, -1, ir.Float{Precision: ir.FloatDouble}},
		{"text", pgtype.TextOID, -1, ir.Text{Size: ir.TextLong}},
		{"varchar(255)", pgtype.VarcharOID, 259, ir.Varchar{Length: 255}},
		{"varchar(unbounded)", pgtype.VarcharOID, -1, ir.Text{Size: ir.TextLong}},
		{"bpchar(10)", pgtype.BPCharOID, 14, ir.Char{Length: 10}},
		{"bytea", pgtype.ByteaOID, -1, ir.Blob{Size: ir.BlobLong}},
		{"date", pgtype.DateOID, -1, ir.Date{}},
		{"timestamp(0)", pgtype.TimestampOID, 0, ir.DateTime{Precision: 0}},
		{"timestamp(6)", pgtype.TimestampOID, 6, ir.DateTime{Precision: 6}},
		{"timestamptz(3)", pgtype.TimestamptzOID, 3, ir.Timestamp{Precision: 3, WithTimeZone: true}},
		{"json", pgtype.JSONOID, -1, ir.JSON{Binary: false}},
		{"jsonb", pgtype.JSONBOID, -1, ir.JSON{Binary: true}},
		{"uuid", pgtype.UUIDOID, -1, ir.UUID{}},
		{"inet", pgtype.InetOID, -1, ir.Inet{}},
		{"cidr", pgtype.CIDROID, -1, ir.Cidr{}},
		{"macaddr", pgtype.MacaddrOID, -1, ir.Macaddr{}},
		// numeric(8,2) typmod = ((8<<16)|2) + 4 = 524294
		{"numeric(8,2)", pgtype.NumericOID, 524294, ir.Decimal{Precision: 8, Scale: 2}},

		// Bug 97 (v0.92.0) Stage 1 + Stage 2 verbatim-carry OID
		// coverage. The schema reader's text-keyed allowlist drifted
		// from the CDC reader's OID switch; pre-fix every entry below
		// fell through to "unsupported column type OID N" and crashed
		// the sync stream on first DML. Each entry's expected
		// ir.VerbatimType.Definition is the pg_catalog typname (which
		// is what coreVerbatimCDCOIDs maps each OID to).
		{"tsvector", pgtype.TSVectorOID, -1, ir.VerbatimType{Definition: "tsvector"}},
		{"tsquery", 3615, -1, ir.VerbatimType{Definition: "tsquery"}},
		{"int4range", pgtype.Int4rangeOID, -1, ir.VerbatimType{Definition: "int4range"}},
		{"int8range", pgtype.Int8rangeOID, -1, ir.VerbatimType{Definition: "int8range"}},
		{"numrange", pgtype.NumrangeOID, -1, ir.VerbatimType{Definition: "numrange"}},
		{"tsrange", pgtype.TsrangeOID, -1, ir.VerbatimType{Definition: "tsrange"}},
		{"tstzrange", pgtype.TstzrangeOID, -1, ir.VerbatimType{Definition: "tstzrange"}},
		{"daterange", pgtype.DaterangeOID, -1, ir.VerbatimType{Definition: "daterange"}},
		{"int4multirange", pgtype.Int4multirangeOID, -1, ir.VerbatimType{Definition: "int4multirange"}},
		{"int8multirange", pgtype.Int8multirangeOID, -1, ir.VerbatimType{Definition: "int8multirange"}},
		{"nummultirange", pgtype.NummultirangeOID, -1, ir.VerbatimType{Definition: "nummultirange"}},
		{"tsmultirange", pgtype.TsmultirangeOID, -1, ir.VerbatimType{Definition: "tsmultirange"}},
		{"tstzmultirange", pgtype.TstzmultirangeOID, -1, ir.VerbatimType{Definition: "tstzmultirange"}},
		{"datemultirange", pgtype.DatemultirangeOID, -1, ir.VerbatimType{Definition: "datemultirange"}},
		{"xml", pgtype.XMLOID, -1, ir.VerbatimType{Definition: "xml"}},
		{"money", 790, -1, ir.VerbatimType{Definition: "money"}},
		{"pg_lsn", 3220, -1, ir.VerbatimType{Definition: "pg_lsn"}},
		{"txid_snapshot", 2970, -1, ir.VerbatimType{Definition: "txid_snapshot"}},
		{"pg_snapshot", 5038, -1, ir.VerbatimType{Definition: "pg_snapshot"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := oidToType(c.oid, c.typmod)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %#v; want %#v", got, c.want)
			}
		})
	}
}

func TestOIDToTypeUnknownErrors(t *testing.T) {
	// 99999 is not a real Postgres OID; stand-in for "custom enum
	// type not in the static table".
	_, err := oidToType(99999, -1)
	if err == nil {
		t.Fatal("expected error for unknown OID")
	}
	if !strings.Contains(err.Error(), "99999") {
		t.Errorf("error should name the OID; got %q", err.Error())
	}
}

func TestBuildRelationCacheEntry(t *testing.T) {
	// A minimal RelationMessage covering one key column + one
	// data column. The pglogrepl shape we're projecting from.
	rel := pglogrepl.RelationMessage{
		RelationID:      16384,
		Namespace:       "public",
		RelationName:    "users",
		ReplicaIdentity: 'd',
		ColumnNum:       2,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: pgtype.Int8OID, TypeModifier: -1},
			{Flags: 0, Name: "email", DataType: pgtype.VarcharOID, TypeModifier: 259},
		},
	}
	got, err := buildRelationCacheEntry(rel)
	if err != nil {
		t.Fatalf("buildRelationCacheEntry: %v", err)
	}
	if got.Schema != "public" || got.Name != "users" {
		t.Errorf("schema/name = %q.%q; want public.users", got.Schema, got.Name)
	}
	if got.ReplicaIdentity != 'd' {
		t.Errorf("replica identity = %q; want 'd'", got.ReplicaIdentity)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("columns = %d; want 2", len(got.Columns))
	}
	if got.Columns[0].Name != "id" || !got.Columns[0].KeyColumn {
		t.Errorf("col[0] = %+v; want id + key", got.Columns[0])
	}
	if _, ok := got.Columns[0].Type.(ir.Integer); !ok {
		t.Errorf("col[0].Type = %#v; want ir.Integer", got.Columns[0].Type)
	}
	if v, ok := got.Columns[1].Type.(ir.Varchar); !ok || v.Length != 255 {
		t.Errorf("col[1].Type = %#v; want ir.Varchar{Length:255}", got.Columns[1].Type)
	}
}

func TestBuildRelationCacheEntryUnknownColumnType(t *testing.T) {
	rel := pglogrepl.RelationMessage{
		Namespace:    "public",
		RelationName: "weird",
		ColumnNum:    1,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "x", DataType: 99999, TypeModifier: -1},
		},
	}
	if _, err := buildRelationCacheEntry(rel); err == nil {
		t.Fatal("expected error for unknown column type OID")
	}
}

func TestDecodeTuple(t *testing.T) {
	cols := []relationColumn{
		{Name: "id", OID: pgtype.Int8OID, Type: ir.Integer{Width: 64}},
		{Name: "email", OID: pgtype.VarcharOID, Type: ir.Varchar{Length: 255}},
		{Name: "active", OID: pgtype.BoolOID, Type: ir.Boolean{}},
		{Name: "extra", OID: pgtype.TextOID, Type: ir.Text{Size: ir.TextLong}},
	}
	tuple := &pglogrepl.TupleData{
		ColumnNum: 4,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Length: 2, Data: []byte("42")},
			{DataType: 't', Length: 17, Data: []byte("alice@example.com")},
			{DataType: 't', Length: 1, Data: []byte("t")},
			{DataType: 'u'}, // unchanged toast — should be omitted
		},
	}
	row, err := decodeTuple(tuple, cols)
	if err != nil {
		t.Fatalf("decodeTuple: %v", err)
	}
	if got := row["id"]; got != int64(42) {
		t.Errorf("id = %#v; want int64(42)", got)
	}
	if got := row["email"]; got != "alice@example.com" {
		t.Errorf("email = %#v; want alice@example.com", got)
	}
	if got := row["active"]; got != true {
		t.Errorf("active = %#v; want true", got)
	}
	if _, present := row["extra"]; present {
		t.Errorf("extra should be omitted (unchanged toast); got %#v", row["extra"])
	}
}

func TestDecodeTupleNullColumn(t *testing.T) {
	cols := []relationColumn{
		{Name: "name", OID: pgtype.TextOID, Type: ir.Text{Size: ir.TextLong}},
	}
	tuple := &pglogrepl.TupleData{
		ColumnNum: 1,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 'n'},
		},
	}
	row, err := decodeTuple(tuple, cols)
	if err != nil {
		t.Fatalf("decodeTuple: %v", err)
	}
	if got, present := row["name"]; !present {
		t.Error("name should be present with nil value, not omitted")
	} else if got != nil {
		t.Errorf("name = %#v; want nil", got)
	}
}

func TestDecodeTupleColumnCountMismatch(t *testing.T) {
	cols := []relationColumn{
		{Name: "id", OID: pgtype.Int8OID, Type: ir.Integer{Width: 64}},
	}
	tuple := &pglogrepl.TupleData{
		ColumnNum: 2,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},
			{DataType: 't', Data: []byte("2")},
		},
	}
	if _, err := decodeTuple(tuple, cols); err == nil {
		t.Error("expected error for column count mismatch")
	}
}

func TestCheckSlotUsable(t *testing.T) {
	cases := []struct {
		name      string
		walStatus string
		wantErr   bool
		wantSub   string
	}{
		{"empty (PG <13)", "", false, ""},
		{"reserved", "reserved", false, ""},
		{"extended", "extended", false, ""},
		{"unreserved warns + recovery hint", "unreserved", true, "wal_status=\"unreserved\""},
		{"unreserved names sluice slot drop", "unreserved", true, "sluice slot drop"},
		{"unreserved recovery hint targets --source", "unreserved", true, "--source"},
		{"lost names recovery + max_slot_wal_keep_size", "lost", true, "max_slot_wal_keep_size"},
		{"lost recovery hint targets --source", "lost", true, "--source"},
		{"unrecognised future status", "exotic_future_state", true, "unrecognised wal_status"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := checkSlotUsable(&slotState{SlotName: "sluice_slot", WALStatus: c.walStatus})
			if c.wantErr {
				if got == nil {
					t.Fatalf("expected error for wal_status=%q", c.walStatus)
				}
				if !strings.Contains(got.Error(), c.wantSub) {
					t.Errorf("error %q missing substring %q", got.Error(), c.wantSub)
				}
			} else if got != nil {
				t.Errorf("unexpected error: %v", got)
			}
		})
	}
}

// TestSynthesizeKeyOnlyBefore covers the REPLICA IDENTITY DEFAULT
// path where pgoutput omits OldTuple on UPDATEs that don't modify
// identity columns. Without this synthesis the applier would emit
// "UPDATE t SET ... WHERE " (empty predicate) and Postgres rejects
// with "syntax error at end of input" — see Bug 3 in the v0.1.0
// findings.
func TestSynthesizeKeyOnlyBefore(t *testing.T) {
	rel := &relationCacheEntry{
		Schema:          "public",
		Name:            "users",
		ReplicaIdentity: 'd',
		Columns: []relationColumn{
			{Name: "id", Type: ir.Integer{Width: 64}, KeyColumn: true},
			{Name: "email", Type: ir.Varchar{Length: 255}, KeyColumn: false},
			{Name: "active", Type: ir.Boolean{}, KeyColumn: false},
		},
		IdentityKeyCols: []string{"id"},
	}
	after := ir.Row{
		"id":     int64(42),
		"email":  "alice@example.com",
		"active": true,
	}
	before, err := synthesizeKeyOnlyBefore(rel, after)
	if err != nil {
		t.Fatalf("synthesizeKeyOnlyBefore: %v", err)
	}
	want := ir.Row{"id": int64(42)}
	if !reflect.DeepEqual(before, want) {
		t.Errorf("\n got = %#v\nwant = %#v", before, want)
	}
}

// TestSynthesizeKeyOnlyBeforeCompositeKey covers tables whose
// identity is a multi-column key. All key columns must end up in
// the synthesized Before, in the relation's column order (which
// matches the table's PK ordering).
func TestSynthesizeKeyOnlyBeforeCompositeKey(t *testing.T) {
	rel := &relationCacheEntry{
		Schema:          "public",
		Name:            "memberships",
		ReplicaIdentity: 'd',
		Columns: []relationColumn{
			{Name: "user_id", Type: ir.Integer{Width: 64}, KeyColumn: true},
			{Name: "group_id", Type: ir.Integer{Width: 64}, KeyColumn: true},
			{Name: "role", Type: ir.Text{Size: ir.TextLong}, KeyColumn: false},
		},
		IdentityKeyCols: []string{"user_id", "group_id"},
	}
	after := ir.Row{
		"user_id":  int64(7),
		"group_id": int64(11),
		"role":     "admin",
	}
	before, err := synthesizeKeyOnlyBefore(rel, after)
	if err != nil {
		t.Fatalf("synthesizeKeyOnlyBefore: %v", err)
	}
	want := ir.Row{"user_id": int64(7), "group_id": int64(11)}
	if !reflect.DeepEqual(before, want) {
		t.Errorf("\n got = %#v\nwant = %#v", before, want)
	}
}

func TestSynthesizeKeyOnlyBeforeRejectsReplicaIdentityNothing(t *testing.T) {
	rel := &relationCacheEntry{
		Schema:          "public",
		Name:            "logs",
		ReplicaIdentity: 'n',
		Columns: []relationColumn{
			{Name: "id", Type: ir.Integer{Width: 64}, KeyColumn: true},
		},
	}
	_, err := synthesizeKeyOnlyBefore(rel, ir.Row{"id": int64(1)})
	if err == nil {
		t.Fatal("expected error for REPLICA IDENTITY NOTHING")
	}
	if !strings.Contains(err.Error(), "REPLICA IDENTITY NOTHING") {
		t.Errorf("error should name the misconfiguration; got %q", err.Error())
	}
}

func TestSynthesizeKeyOnlyBeforeRejectsNoKeyColumns(t *testing.T) {
	rel := &relationCacheEntry{
		Schema:          "public",
		Name:            "events",
		ReplicaIdentity: 'd',
		Columns: []relationColumn{
			{Name: "id", Type: ir.Integer{Width: 64}, KeyColumn: false},
			{Name: "kind", Type: ir.Text{Size: ir.TextLong}, KeyColumn: false},
		},
	}
	_, err := synthesizeKeyOnlyBefore(rel, ir.Row{"id": int64(1), "kind": "x"})
	if err == nil {
		t.Fatal("expected error when relation has no identity columns")
	}
	if !strings.Contains(err.Error(), "no identity-key columns") {
		t.Errorf("error should name the missing identity; got %q", err.Error())
	}
}

// TestDecodeTupleDeleteOldTupleHasNullMarkersForNonKey documents the
// pgoutput protocol detail that motivates [filterBeforeToKeyCols]: a
// DELETE message under REPLICA IDENTITY DEFAULT carries an OldTuple
// whose ColumnNum equals the relation's full column count, but with
// 'n' (null) markers for non-key columns. decodeTuple — correctly,
// for the protocol's own semantics — translates those into nil
// entries on the row map. Without filtering, the applier's WHERE
// then emits "col IS NULL" for non-key columns, the DELETE matches
// zero rows, and ADR-0010's resume-idempotent zero-rows-ok behaviour
// silently swallows the miss. This test pins the underlying shape
// down so a future refactor of decodeTuple can't unknowingly redirect
// the bug-fix surface.
func TestDecodeTupleDeleteOldTupleHasNullMarkersForNonKey(t *testing.T) {
	cols := []relationColumn{
		{Name: "order_id", OID: pgtype.Int8OID, Type: ir.Integer{Width: 64}, KeyColumn: true},
		{Name: "line_no", OID: pgtype.Int2OID, Type: ir.Integer{Width: 16}, KeyColumn: true},
		{Name: "qty", OID: pgtype.Int4OID, Type: ir.Integer{Width: 32}, KeyColumn: false},
		{Name: "unit_price", OID: pgtype.NumericOID, Type: ir.Decimal{Precision: 12, Scale: 4}, KeyColumn: false},
	}
	// What pgoutput sends for DELETE under REPLICA IDENTITY DEFAULT:
	// the ColumnNum is the relation's full column count, key columns
	// hold actual data ('t'), non-key columns are null markers ('n').
	tuple := &pglogrepl.TupleData{
		ColumnNum: 4,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Length: 3, Data: []byte("100")},
			{DataType: 't', Length: 1, Data: []byte("1")},
			{DataType: 'n'},
			{DataType: 'n'},
		},
	}
	row, err := decodeTuple(tuple, cols)
	if err != nil {
		t.Fatalf("decodeTuple: %v", err)
	}
	if row["order_id"] != int64(100) {
		t.Errorf("order_id = %#v; want int64(100)", row["order_id"])
	}
	// decodeInteger widens every signed-integer width to int64, so
	// even a SMALLINT (Int2) value comes back as int64 here.
	if row["line_no"] != int64(1) {
		t.Errorf("line_no = %#v; want int64(1)", row["line_no"])
	}
	// The bug-prone shape: non-key columns are present-but-nil.
	if v, present := row["qty"]; !present || v != nil {
		t.Errorf("qty: present=%v value=%#v; want present=true value=nil", present, v)
	}
	if v, present := row["unit_price"]; !present || v != nil {
		t.Errorf("unit_price: present=%v value=%#v; want present=true value=nil", present, v)
	}
}

// TestFilterBeforeToKeyCols exercises every REPLICA IDENTITY shape the
// helper has to handle. The narrowing keys off rel.IdentityKeyCols (the
// resolved replica-identity set produced by resolveIdentityKeyCols),
// NOT the per-column wire flag — so each fixture sets IdentityKeyCols to
// whatever resolveIdentityKeyCols would have produced for that identity.
//
// The canonical Bug 8 surface is the composite-PK + DEFAULT case; the
// FULL-with-PK case (Bug 92) is the UPDATE-path surface where EVERY
// column is wire-flagged KeyColumn=true (pgoutput's real FULL shape) yet
// only the resolved PK must survive into the WHERE — the regression the
// first fix attempt missed by trusting the wire flag. The others are
// correctness invariants the same code path needs to preserve.
func TestFilterBeforeToKeyCols(t *testing.T) {
	cases := []struct {
		name    string
		rel     *relationCacheEntry
		decoded ir.Row
		want    ir.Row
	}{
		{
			name: "single-PK + DEFAULT (non-key cols arrive as nil)",
			rel: &relationCacheEntry{
				Schema:          "public",
				Name:            "users",
				ReplicaIdentity: 'd',
				Columns: []relationColumn{
					{Name: "id", KeyColumn: true},
					{Name: "email", KeyColumn: false},
					{Name: "active", KeyColumn: false},
				},
				IdentityKeyCols: []string{"id"},
			},
			decoded: ir.Row{"id": int64(42), "email": nil, "active": nil},
			want:    ir.Row{"id": int64(42)},
		},
		{
			name: "composite-PK + DEFAULT (non-key cols arrive as nil)",
			rel: &relationCacheEntry{
				Schema:          "public",
				Name:            "order_items",
				ReplicaIdentity: 'd',
				Columns: []relationColumn{
					{Name: "order_id", KeyColumn: true},
					{Name: "line_no", KeyColumn: true},
					{Name: "qty", KeyColumn: false},
					{Name: "unit_price", KeyColumn: false},
				},
				IdentityKeyCols: []string{"order_id", "line_no"},
			},
			decoded: ir.Row{
				"order_id":   int64(100),
				"line_no":    int64(1),
				"qty":        nil,
				"unit_price": nil,
			},
			want: ir.Row{"order_id": int64(100), "line_no": int64(1)},
		},
		{
			// Bug 92: under FULL pgoutput flags EVERY column KeyColumn=true,
			// but resolveIdentityKeyCols resolved the real PK to {id}, so
			// the rich non-key values must be dropped from the WHERE even
			// though they're wire-flagged.
			name: "FULL with PK (all cols wire-flagged; narrowed to resolved PK)",
			rel: &relationCacheEntry{
				Schema:          "public",
				Name:            "users",
				ReplicaIdentity: 'f',
				Columns: []relationColumn{
					{Name: "id", KeyColumn: true},
					{Name: "email", KeyColumn: true},
					{Name: "active", KeyColumn: true},
				},
				IdentityKeyCols: []string{"id"},
			},
			decoded: ir.Row{"id": int64(42), "email": "alice@example.com", "active": true},
			want:    ir.Row{"id": int64(42)},
		},
		{
			name: "USING INDEX (only the indexed columns are the identity)",
			rel: &relationCacheEntry{
				Schema:          "public",
				Name:            "events",
				ReplicaIdentity: 'i',
				Columns: []relationColumn{
					{Name: "id", KeyColumn: false},
					{Name: "event_uuid", KeyColumn: true},
					{Name: "payload", KeyColumn: false},
				},
				IdentityKeyCols: []string{"event_uuid"},
			},
			decoded: ir.Row{"id": nil, "event_uuid": "abc123", "payload": nil},
			want:    ir.Row{"event_uuid": "abc123"},
		},
		{
			// FULL with no PK: resolveIdentityKeyCols leaves IdentityKeyCols
			// empty, and the helper falls back to the full row.
			name: "FULL on a PK-less relation falls back to the full row",
			rel: &relationCacheEntry{
				Schema:          "public",
				Name:            "audit",
				ReplicaIdentity: 'f',
				Columns: []relationColumn{
					{Name: "actor", KeyColumn: true},
					{Name: "happened_at", KeyColumn: true},
				},
				IdentityKeyCols: nil,
			},
			decoded: ir.Row{"actor": "alice", "happened_at": "2024-01-01"},
			want:    ir.Row{"actor": "alice", "happened_at": "2024-01-01"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := filterBeforeToKeyCols(c.rel, c.decoded)
			if err != nil {
				t.Fatalf("filterBeforeToKeyCols: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("\n got = %#v\nwant = %#v", got, c.want)
			}
		})
	}
}

func TestFilterBeforeToKeyColsRejectsMissingKeyValue(t *testing.T) {
	// A key column declared on the relation but absent from the
	// decoded tuple: should refuse to build a partial WHERE, on the
	// same principle as synthesizeKeyOnlyBefore.
	rel := &relationCacheEntry{
		Schema: "public",
		Name:   "users",
		Columns: []relationColumn{
			{Name: "id", KeyColumn: true},
			{Name: "email", KeyColumn: false},
		},
		IdentityKeyCols: []string{"id"},
	}
	_, err := filterBeforeToKeyCols(rel, ir.Row{"email": "alice@example.com"})
	if err == nil {
		t.Fatal("expected error when key column missing from decoded tuple")
	}
	if !strings.Contains(err.Error(), "id") {
		t.Errorf("error should name the missing column; got %q", err.Error())
	}
}

func TestSynthesizeKeyOnlyBeforeRejectsMissingKeyValue(t *testing.T) {
	// A key column declared on the relation but absent from the
	// after-tuple should fail loudly — pgoutput shouldn't produce
	// this shape, but if it ever does we'd rather surface the
	// inconsistency than emit a WHERE that targets the wrong row.
	rel := &relationCacheEntry{
		Schema:          "public",
		Name:            "users",
		ReplicaIdentity: 'd',
		Columns: []relationColumn{
			{Name: "id", Type: ir.Integer{Width: 64}, KeyColumn: true},
			{Name: "email", Type: ir.Varchar{Length: 255}, KeyColumn: false},
		},
		IdentityKeyCols: []string{"id"},
	}
	_, err := synthesizeKeyOnlyBefore(rel, ir.Row{"email": "alice@example.com"})
	if err == nil {
		t.Fatal("expected error when key column missing from after-tuple")
	}
	if !strings.Contains(err.Error(), "id") {
		t.Errorf("error should name the missing column; got %q", err.Error())
	}
}

func TestWithReplicationParam(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"uri without query",
			"postgres://u:p@h:5432/db",
			"postgres://u:p@h:5432/db?replication=database",
		},
		{
			"uri strips schema, adds replication",
			"postgres://u:p@h:5432/db?schema=public&sslmode=disable",
			"postgres://u:p@h:5432/db?replication=database&sslmode=disable",
		},
		{
			"kv form",
			"host=localhost user=u dbname=db",
			"host=localhost user=u dbname=db replication=database",
		},
		{
			"kv form strips schema, replaces existing replication",
			"host=h dbname=d schema=public replication=physical",
			"host=h dbname=d replication=database",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := withReplicationParam(c.in)
			if err != nil {
				t.Fatalf("withReplicationParam: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got = %q\nwant = %q", got, c.want)
			}
		})
	}
}
