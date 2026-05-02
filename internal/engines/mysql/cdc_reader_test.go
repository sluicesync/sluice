package mysql

import (
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestEncodeDecodeBinlogPos(t *testing.T) {
	cases := []struct {
		name string
		pos  binlogPos
	}{
		{
			"file_pos",
			binlogPos{Mode: positionModeFilePos, File: "mysql-bin.000123", Pos: 4567},
		},
		{
			"gtid",
			binlogPos{Mode: positionModeGTID, GTIDSet: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000"},
		},
		{
			"file_pos zero offset",
			binlogPos{Mode: positionModeFilePos, File: "binlog.000001", Pos: 0},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			encoded, err := encodeBinlogPos(c.pos)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if encoded.Engine != engineNameMySQL {
				t.Errorf("Engine = %q; want %q", encoded.Engine, engineNameMySQL)
			}
			got, ok, err := decodeBinlogPos(encoded)
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

func TestEncodeBinlogPosRejectsInvalidMode(t *testing.T) {
	_, err := encodeBinlogPos(binlogPos{Mode: "lsn", File: "x", Pos: 1})
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestDecodeBinlogPosFromNowSentinel(t *testing.T) {
	_, ok, err := decodeBinlogPos(ir.Position{})
	if err != nil {
		t.Fatalf("zero position should not error: %v", err)
	}
	if ok {
		t.Errorf("zero position should report ok=false (from-now sentinel)")
	}
}

func TestDecodeBinlogPosErrors(t *testing.T) {
	cases := []struct {
		name string
		in   ir.Position
	}{
		{"wrong engine", ir.Position{Engine: "postgres", Token: `{"mode":"file_pos"}`}},
		{"empty token with non-empty engine", ir.Position{Engine: "mysql", Token: ""}},
		{"malformed json", ir.Position{Engine: "mysql", Token: "not json"}},
		{"unknown mode", ir.Position{Engine: "mysql", Token: `{"mode":"lsn"}`}},
		{"gtid mode missing set", ir.Position{Engine: "mysql", Token: `{"mode":"gtid"}`}},
		{"file_pos mode missing file", ir.Position{Engine: "mysql", Token: `{"mode":"file_pos","pos":42}`}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, _, err := decodeBinlogPos(c.in)
			if err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

func TestFormatSIDAsUUID(t *testing.T) {
	cases := []struct {
		name string
		sid  []byte
		want string
		err  bool
	}{
		{
			"canonical sid",
			[]byte{
				0x3e, 0x11, 0xfa, 0x47,
				0x71, 0xca,
				0x11, 0xe1,
				0x9e, 0x33,
				0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62,
			},
			"3e11fa47-71ca-11e1-9e33-c80aa9429562",
			false,
		},
		{
			"wrong length",
			[]byte{0x01, 0x02, 0x03},
			"",
			true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := formatSIDAsUUID(c.sid)
			if c.err {
				if err == nil {
					t.Errorf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}

func TestHostPortFromAddr(t *testing.T) {
	cases := []struct {
		in      string
		host    string
		port    uint16
		wantErr bool
	}{
		{"127.0.0.1:3306", "127.0.0.1", 3306, false},
		{"[::1]:33060", "::1", 33060, false},
		{"db.example.com:3307", "db.example.com", 3307, false},
		{"no-port", "", 0, true},
		{"127.0.0.1:notnum", "", 0, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			host, port, err := hostPortFromAddr(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", c.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != c.host || port != c.port {
				t.Errorf("got (%q, %d); want (%q, %d)", host, port, c.host, c.port)
			}
		})
	}
}

func TestSplitQualified(t *testing.T) {
	cases := []struct {
		in           string
		schema, name string
	}{
		{"db.users", "db", "users"},
		{"public.posts", "public", "posts"},
		{"users", "", "users"},
		{"", "", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			s, n := splitQualified(c.in)
			if s != c.schema || n != c.name {
				t.Errorf("got (%q, %q); want (%q, %q)", s, n, c.schema, c.name)
			}
		})
	}
}

func TestDecodeBinlogRow(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}},
		{Name: "active", Type: ir.Boolean{}},
	}
	raw := []any{int64(7), []byte("alice@example.com"), int64(1)}

	row, err := decodeBinlogRow(raw, cols)
	if err != nil {
		t.Fatalf("decodeBinlogRow: %v", err)
	}
	if got := row["id"]; got != int64(7) {
		t.Errorf("id = %#v; want int64(7)", got)
	}
	if got := row["email"]; got != "alice@example.com" {
		t.Errorf("email = %#v; want alice@example.com", got)
	}
	if got := row["active"]; got != true {
		t.Errorf("active = %#v; want true", got)
	}
}

func TestDecodeBinlogRowColumnCountMismatch(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}
	if _, err := decodeBinlogRow([]any{int64(1), int64(2)}, cols); err == nil {
		t.Error("expected error for column count mismatch")
	}
}

func TestGenerateServerIDIsNonZero(t *testing.T) {
	// Cheap sanity: the binlog protocol rejects ID=0, so the helper
	// must never return it.
	for i := 0; i < 50; i++ {
		if id := generateServerID(); id == 0 {
			t.Fatal("generateServerID returned 0")
		}
	}
}
