package postgres

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/orware/sluice/internal/ir"
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
