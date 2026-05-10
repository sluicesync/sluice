// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/orware/sluice/internal/ir"
)

// TestVStreamReader_BasicEventDispatch drives the dispatcher with
// hand-built VEvent values to confirm the FIELD-then-ROW pattern
// produces the expected ir.Change shape. Hits Insert (After only),
// Update (Before+After), Delete (Before only) on a single fixture.
//
// No gRPC, no network — just the in-process dispatch path.
func TestVStreamReader_BasicEventDispatch(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		shards:   []string{"-"},
		fields:   make(map[string][]*query.Field),
		// currentVgtid stays nil; positionFor returns ir.Position{}
		// in that case (no resume token yet).
	}

	out := make(chan ir.Change, 8)

	// Build the FIELD event: column metadata for users(id, email).
	fieldsEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			Fields: []*query.Field{
				{Name: "id", Type: query.Type_INT64},
				{Name: "email", Type: query.Type_VARCHAR},
			},
		},
	}

	// Insert: After only.
	insertEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			RowChanges: []*binlogdata.RowChange{
				{
					After: makeRow([]string{"7", "alice@example.com"}),
				},
			},
		},
	}

	// Update: Before + After (active flag flips).
	updateEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			RowChanges: []*binlogdata.RowChange{
				{
					Before: makeRow([]string{"7", "alice@example.com"}),
					After:  makeRow([]string{"7", "alice@new.example.com"}),
				},
			},
		},
	}

	// Delete: Before only.
	deleteEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			RowChanges: []*binlogdata.RowChange{
				{
					Before: makeRow([]string{"7", "alice@new.example.com"}),
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, ev := range []*binlogdata.VEvent{fieldsEv, insertEv, updateEv, deleteEv} {
		if err := r.dispatch(ctx, ev, out); err != nil {
			t.Fatalf("dispatch %v: %v", ev.GetType(), err)
		}
	}
	close(out)

	got := drainChannel(out)
	if len(got) != 3 {
		t.Fatalf("got %d changes; want 3 (insert, update, delete)", len(got))
	}

	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("got[0] = %T; want ir.Insert", got[0])
	}
	if ins.Schema != "main" || ins.Table != "users" {
		t.Errorf("insert table = %s.%s; want main.users", ins.Schema, ins.Table)
	}
	if id, _ := ins.Row["id"].(int64); id != 7 {
		t.Errorf("insert.Row[id] = %#v; want int64(7)", ins.Row["id"])
	}
	if email, _ := ins.Row["email"].(string); email != "alice@example.com" {
		t.Errorf("insert.Row[email] = %#v; want alice@example.com", ins.Row["email"])
	}

	upd, ok := got[1].(ir.Update)
	if !ok {
		t.Fatalf("got[1] = %T; want ir.Update", got[1])
	}
	if before, _ := upd.Before["email"].(string); before != "alice@example.com" {
		t.Errorf("update.Before[email] = %#v; want alice@example.com", upd.Before["email"])
	}
	if after, _ := upd.After["email"].(string); after != "alice@new.example.com" {
		t.Errorf("update.After[email] = %#v; want alice@new.example.com", upd.After["email"])
	}

	del, ok := got[2].(ir.Delete)
	if !ok {
		t.Fatalf("got[2] = %T; want ir.Delete", got[2])
	}
	if email, _ := del.Before["email"].(string); email != "alice@new.example.com" {
		t.Errorf("delete.Before[email] = %#v; want alice@new.example.com", del.Before["email"])
	}
}

// TestVStreamReader_RowEventBeforeField asserts the dispatcher
// rejects a ROW event for a table the reader hasn't seen a FIELD
// event for. Without the field cache, columns can't be decoded;
// silently emitting the event with empty values would mask the
// real protocol violation upstream.
func TestVStreamReader_RowEventBeforeField(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		fields:   make(map[string][]*query.Field),
	}
	out := make(chan ir.Change, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	rowEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "users",
			Shard:     "-",
			RowChanges: []*binlogdata.RowChange{
				{After: makeRow([]string{"1"})},
			},
		},
	}
	if err := r.dispatch(ctx, rowEv, out); err == nil {
		t.Fatal("expected error for ROW event without preceding FIELD")
	}
}

// TestVStreamReader_VgtidUpdates confirms the VGTID branch of
// dispatch refreshes the reader's currentVgtid in place. The
// next ir.Change emitted should carry that position.
func TestVStreamReader_VgtidUpdates(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		fields:   make(map[string][]*query.Field),
	}
	out := make(chan ir.Change, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	vgtidEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_VGTID,
		Vgtid: &binlogdata.VGtid{
			ShardGtids: []*binlogdata.ShardGtid{
				{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abcd:1-100"},
			},
		},
	}
	if err := r.dispatch(ctx, vgtidEv, out); err != nil {
		t.Fatalf("dispatch VGTID: %v", err)
	}
	if len(r.currentVgtid) != 1 {
		t.Fatalf("currentVgtid = %v; want one entry", r.currentVgtid)
	}
	if r.currentVgtid[0].Gtid != "MySQL56/abcd:1-100" {
		t.Errorf("currentVgtid[0].Gtid = %q; want MySQL56/abcd:1-100", r.currentVgtid[0].Gtid)
	}
}

// TestVStreamReader_JournalErrors confirms a JOURNAL event
// terminates the dispatch with the typed [ShardLayoutChangedError]
// so the caller can detect the reshard via [errors.Is] and
// inspect the new layout to call Reopen. With StopOnReshard:true
// the stream itself terminates after this.
func TestVStreamReader_JournalErrors(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		fields:   make(map[string][]*query.Field),
	}
	out := make(chan ir.Change, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ev := &binlogdata.VEvent{
		Type: binlogdata.VEventType_JOURNAL,
		Journal: &binlogdata.Journal{
			Participants: []*binlogdata.KeyspaceShard{
				{Keyspace: "main", Shard: "-"},
			},
			ShardGtids: []*binlogdata.ShardGtid{
				{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/abcd:1-100"},
				{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/abcd:1-100"},
			},
		},
	}
	err := r.dispatch(ctx, ev, out)
	if err == nil {
		t.Fatal("expected error for JOURNAL event")
	}
	if !errors.Is(err, ErrShardLayoutChanged) {
		t.Errorf("err = %v; want errors.Is(err, ErrShardLayoutChanged)", err)
	}
	var resh *ShardLayoutChangedError
	if !errors.As(err, &resh) {
		t.Fatalf("err = %v; want errors.As(err, *ShardLayoutChangedError)", err)
	}
	if resh.Keyspace != "main" {
		t.Errorf("resh.Keyspace = %q; want main", resh.Keyspace)
	}
	if len(resh.Participants) != 1 || resh.Participants[0].Shard != "-" {
		t.Errorf("resh.Participants = %v; want one entry with shard=-", resh.Participants)
	}
	if len(resh.NewShards) != 2 {
		t.Errorf("resh.NewShards has %d entries; want 2", len(resh.NewShards))
	}
}

// TestVStreamReader_DDLClearsFieldCache confirms a non-TRUNCATE
// DDL event invalidates the cached field metadata. A schema change
// on the source means the next ROW event might have a different
// column shape; clearing the cache forces a fresh FIELD event
// before any ROW decode happens.
func TestVStreamReader_DDLClearsFieldCache(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		fields: map[string][]*query.Field{
			"-/users": {{Name: "id", Type: query.Type_INT64}},
		},
	}
	out := make(chan ir.Change, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ev := &binlogdata.VEvent{
		Type:      binlogdata.VEventType_DDL,
		Statement: "ALTER TABLE users ADD COLUMN active TINYINT(1)",
	}
	if err := r.dispatch(ctx, ev, out); err != nil {
		t.Fatalf("dispatch DDL: %v", err)
	}
	if len(r.fields) != 0 {
		t.Errorf("fields cache size = %d; want 0 after DDL", len(r.fields))
	}
	// No event should have been emitted for a non-TRUNCATE DDL.
	close(out)
	if got := drainChannel(out); len(got) != 0 {
		t.Errorf("got %d events; want 0 for ALTER TABLE", len(got))
	}
}

// TestVStreamReader_DDLTruncateEmitsTruncate confirms a TRUNCATE
// TABLE statement surfaces as ir.Truncate AND clears the field
// cache (TRUNCATE resets auto-increment, so the next FIELD event
// might carry refreshed metadata).
func TestVStreamReader_DDLTruncateEmitsTruncate(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		fields: map[string][]*query.Field{
			"-/users": {{Name: "id", Type: query.Type_INT64}},
		},
	}
	out := make(chan ir.Change, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ev := &binlogdata.VEvent{
		Type:      binlogdata.VEventType_DDL,
		Statement: "TRUNCATE TABLE users",
		Keyspace:  "main",
	}
	if err := r.dispatch(ctx, ev, out); err != nil {
		t.Fatalf("dispatch DDL: %v", err)
	}
	close(out)

	got := drainChannel(out)
	if len(got) != 1 {
		t.Fatalf("got %d events; want 1 (ir.Truncate)", len(got))
	}
	tr, ok := got[0].(ir.Truncate)
	if !ok {
		t.Fatalf("got[0] = %T; want ir.Truncate", got[0])
	}
	if tr.Schema != "main" {
		t.Errorf("truncate.Schema = %q; want main", tr.Schema)
	}
	if tr.Table != "users" {
		t.Errorf("truncate.Table = %q; want users", tr.Table)
	}
	if len(r.fields) != 0 {
		t.Errorf("fields cache size = %d; want 0 after TRUNCATE", len(r.fields))
	}
}

// TestVStreamReader_DDLTruncateOtherKeyspace confirms a TRUNCATE
// against a different keyspace is silently dropped (the reader is
// bound to a single keyspace; a stray cross-keyspace event isn't
// ours to apply). The field cache still gets invalidated though,
// because the conservative-blanket-clear behaviour mirrors the
// binlog path.
func TestVStreamReader_DDLTruncateOtherKeyspace(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		fields: map[string][]*query.Field{
			"-/users": {{Name: "id", Type: query.Type_INT64}},
		},
	}
	out := make(chan ir.Change, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ev := &binlogdata.VEvent{
		Type:      binlogdata.VEventType_DDL,
		Statement: "TRUNCATE TABLE other_ks.something",
		Keyspace:  "main",
	}
	if err := r.dispatch(ctx, ev, out); err != nil {
		t.Fatalf("dispatch DDL: %v", err)
	}
	close(out)

	if got := drainChannel(out); len(got) != 0 {
		t.Errorf("got %d events; want 0 for cross-keyspace TRUNCATE", len(got))
	}
	if len(r.fields) != 0 {
		t.Errorf("fields cache size = %d; want 0 after DDL", len(r.fields))
	}
}

// TestVStreamReader_HeartbeatBeginCommitNoOp confirms transaction-
// boundary and heartbeat events are dropped silently. They're
// important for the wire protocol but produce no IR events.
func TestVStreamReader_HeartbeatBeginCommitNoOp(t *testing.T) {
	r := &vstreamCDCReader{fields: make(map[string][]*query.Field)}
	out := make(chan ir.Change, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for _, typ := range []binlogdata.VEventType{
		binlogdata.VEventType_BEGIN,
		binlogdata.VEventType_COMMIT,
		binlogdata.VEventType_HEARTBEAT,
		binlogdata.VEventType_GTID,
		binlogdata.VEventType_OTHER,
	} {
		ev := &binlogdata.VEvent{Type: typ}
		if err := r.dispatch(ctx, ev, out); err != nil {
			t.Errorf("dispatch %v: %v", typ, err)
		}
	}
	close(out)
	if got := drainChannel(out); len(got) != 0 {
		t.Errorf("got %d changes; want 0 (bookkeeping events)", len(got))
	}
}

// TestVStreamEndpointFromDSN covers the DSN→endpoint derivation.
// The default rule (host + :443) matches PlanetScale's connect
// convention; the explicit `vstream_endpoint` parameter wins.
func TestVStreamEndpointFromDSN(t *testing.T) {
	cases := []struct {
		name string
		addr string
		args map[string]string
		want string
	}{
		{
			"planetscale default port",
			"aws.connect.psdb.cloud:3306",
			nil,
			"aws.connect.psdb.cloud:443",
		},
		{
			"plain hostname (no port)",
			"db.example.com",
			nil,
			"db.example.com:443",
		},
		{
			"vstream_endpoint override",
			"aws.connect.psdb.cloud:3306",
			map[string]string{"vstream_endpoint": "vtgate.local:15991"},
			"vtgate.local:15991",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cfg, err := minimalConfig(c.addr, c.args)
			if err != nil {
				t.Fatalf("build cfg: %v", err)
			}
			got, err := vstreamEndpointFromDSN(cfg)
			if err != nil {
				t.Fatalf("vstreamEndpointFromDSN: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}

// TestVStreamDialOptions covers the transport/auth flag matrix.
// The four valid combinations all succeed; invalid values and the
// "basic auth over plaintext" footgun produce clear errors.
func TestVStreamDialOptions(t *testing.T) {
	cases := []struct {
		name      string
		user      string
		passwd    string
		params    map[string]string
		wantErr   bool
		wantAuth  bool // expect non-empty authHeader
		errSubstr string
	}{
		{
			name: "default (tls + basic) with creds",
			user: "u", passwd: "p",
			wantAuth: true,
		},
		{
			name: "tls + basic explicit",
			user: "u", passwd: "p",
			params:   map[string]string{"vstream_transport": "tls", "vstream_auth": "basic"},
			wantAuth: true,
		},
		{
			name:   "plaintext + none (vttestserver)",
			params: map[string]string{"vstream_transport": "plaintext", "vstream_auth": "none"},
		},
		{
			name:     "tls + none (vanilla Vitess with TLS but no auth)",
			params:   map[string]string{"vstream_auth": "none"},
			wantAuth: false,
		},
		{
			name: "basic without user",
			user: "", passwd: "",
			wantErr:   true,
			errSubstr: "no user",
		},
		{
			name: "basic over plaintext refused",
			user: "u", passwd: "p",
			params:    map[string]string{"vstream_transport": "plaintext"},
			wantErr:   true,
			errSubstr: "refuses to ride plaintext",
		},
		{
			name: "unknown transport",
			user: "u", passwd: "p",
			params:    map[string]string{"vstream_transport": "quic"},
			wantErr:   true,
			errSubstr: "unknown vstream_transport",
		},
		{
			name: "unknown auth",
			user: "u", passwd: "p",
			params:    map[string]string{"vstream_auth": "oauth"},
			wantErr:   true,
			errSubstr: "unknown vstream_auth",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cfg, _ := minimalConfig("host:3306", c.params)
			cfg.User = c.user
			cfg.Passwd = c.passwd
			opts, authHeader, err := vstreamDialOptions(cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q; got nil", c.errSubstr)
				}
				if !strings.Contains(err.Error(), c.errSubstr) {
					t.Errorf("err = %q; want substring %q", err.Error(), c.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("vstreamDialOptions: %v", err)
			}
			if len(opts) == 0 {
				t.Errorf("got 0 dial options; want at least transport credentials")
			}
			if c.wantAuth && authHeader == "" {
				t.Error("authHeader is empty; want non-empty for basic auth")
			}
			if !c.wantAuth && authHeader != "" {
				t.Errorf("authHeader = %q; want empty for non-basic auth", authHeader)
			}
		})
	}
}

// TestDecodeVStreamCell covers the type-specific decode paths the
// reader uses to produce IR-canonical Go values from Vitess wire
// bytes. The matrix mirrors docs/value-types.md so cross-engine
// MySQL→PG behaviour is identical whether changes flow via the
// binlog reader or via VStream.
func TestDecodeVStreamCell(t *testing.T) {
	cases := []struct {
		name       string
		fieldType  query.Type
		columnType string
		raw        string
		want       any
	}{
		// ---- Booleans (TINYINT(1)) ----
		{"tinyint(1) → bool true", query.Type_INT8, "tinyint(1)", "1", true},
		{"tinyint(1) → bool false", query.Type_INT8, "tinyint(1)", "0", false},
		{"tinyint(1) unsigned → bool", query.Type_UINT8, "tinyint(1) unsigned", "1", true},
		// Plain TINYINT (no display width) stays int64.
		{"tinyint plain → int64", query.Type_INT8, "tinyint", "127", int64(127)},

		// ---- Integers ----
		{"int → int64", query.Type_INT32, "int", "12345", int64(12345)},
		{"bigint → int64", query.Type_INT64, "bigint", "9223372036854775807", int64(9223372036854775807)},
		{"uint64", query.Type_UINT64, "bigint unsigned", "18446744073709551615", uint64(18446744073709551615)},

		// ---- Floats ----
		{"double → float64", query.Type_FLOAT64, "double", "3.14159", 3.14159},

		// ---- Decimal (string per IR contract) ----
		{"decimal stays string", query.Type_DECIMAL, "decimal(10,2)", "1234.56", "1234.56"},

		// ---- Strings / enums ----
		{"varchar → string", query.Type_VARCHAR, "varchar(255)", "hello", "hello"},
		{"text → string", query.Type_TEXT, "text", "long form", "long form"},
		{"char → string", query.Type_CHAR, "char(10)", "fixed", "fixed"},
		{"enum → string", query.Type_ENUM, "enum('a','b','c')", "b", "b"},

		// ---- SET ----
		{"set with members", query.Type_SET, "set('a','b','c')", "a,c", []string{"a", "c"}},
		{"empty set", query.Type_SET, "set('a','b')", "", []string{}},

		// ---- Time ----
		{"time stays string", query.Type_TIME, "time", "12:34:56", "12:34:56"},

		// ---- Bytes ----
		{"varbinary → bytes", query.Type_VARBINARY, "varbinary(64)", "abc", []byte("abc")},
		{"blob → bytes", query.Type_BLOB, "blob", "blobdata", []byte("blobdata")},
		{"json → bytes", query.Type_JSON, "json", `{"k":"v"}`, []byte(`{"k":"v"}`)},

		// ---- NULL_TYPE (rare; fields normally use lengths=-1 for NULL) ----
		{"null type", query.Type_NULL_TYPE, "", "", nil},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := &query.Field{
				Type:       c.fieldType,
				ColumnType: c.columnType,
			}
			got := decodeVStreamCell(f, []byte(c.raw))
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("\n got: %#v (%T)\nwant: %#v (%T)", got, got, c.want, c.want)
			}
		})
	}
}

// TestDecodeVStreamCellDateTime covers the time-typed decode path
// separately because comparing time.Time values via DeepEqual is
// fragile (location, monotonic clock). We assert on parsed fields
// instead.
func TestDecodeVStreamCellDateTime(t *testing.T) {
	cases := []struct {
		name     string
		typ      query.Type
		raw      string
		wantYear int
		wantHour int
	}{
		{"date", query.Type_DATE, "2026-05-03", 2026, 0},
		{"datetime no fraction", query.Type_DATETIME, "2026-05-03 14:23:45", 2026, 14},
		{"datetime with fraction", query.Type_DATETIME, "2026-05-03 14:23:45.123456", 2026, 14},
		{"timestamp", query.Type_TIMESTAMP, "2026-05-03 14:23:45", 2026, 14},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := &query.Field{Type: c.typ}
			got := decodeVStreamCell(f, []byte(c.raw))
			tm, ok := got.(time.Time)
			if !ok {
				t.Fatalf("got %#v (%T); want time.Time", got, got)
			}
			if tm.Year() != c.wantYear {
				t.Errorf("year = %d; want %d", tm.Year(), c.wantYear)
			}
			if tm.Hour() != c.wantHour {
				t.Errorf("hour = %d; want %d", tm.Hour(), c.wantHour)
			}
		})
	}

	t.Run("malformed datetime falls back to bytes", func(t *testing.T) {
		f := &query.Field{Type: query.Type_DATETIME}
		got := decodeVStreamCell(f, []byte("not a date"))
		if _, ok := got.([]byte); !ok {
			t.Errorf("got %#v (%T); want []byte fallback for malformed input", got, got)
		}
	})
}

// TestDecodeVStreamCellGeometry covers Bug 27: VStream POINT bytes
// are delivered with a 4-byte little-endian SRID prefix that pre-fix
// tripped the WKB byte-order-flag check (SRID 4326 = E6 10 00 00 →
// byte 0 of 0xE6 isn't a valid byte-order flag). The decoder strips
// the prefix so downstream consumers see standard WKB matching the
// IR contract for ir.Geometry values.
func TestDecodeVStreamCellGeometry(t *testing.T) {
	// MySQL's on-wire geometry layout: <srid uint32 LE><wkb>.
	// SRID 4326 little-endian is E6 10 00 00. The trailing 21 bytes
	// are a canonical POINT(2.0, 3.0) WKB.
	wkb := []byte{
		0x01,                   // byte order = little endian
		0x01, 0x00, 0x00, 0x00, // type = POINT (uint32 LE)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0 (f64 LE)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // y = 3.0 (f64 LE)
	}
	srid4326 := []byte{0xE6, 0x10, 0x00, 0x00}
	mysqlBytes := append(append([]byte{}, srid4326...), wkb...)

	t.Run("strips SRID 4326 prefix yielding raw WKB", func(t *testing.T) {
		f := &query.Field{Type: query.Type_GEOMETRY, ColumnType: "point"}
		got := decodeVStreamCell(f, mysqlBytes)
		gotBytes, ok := got.([]byte)
		if !ok {
			t.Fatalf("got %T; want []byte", got)
		}
		if !reflect.DeepEqual(gotBytes, wkb) {
			t.Errorf("got %x; want %x (raw WKB without SRID prefix)", gotBytes, wkb)
		}
	})

	t.Run("malformed too-short value passes through", func(t *testing.T) {
		// 3-byte input is shorter than the 4-byte SRID prefix; pass
		// through so the downstream WKB validator surfaces the
		// problem rather than silently re-shaping garbage.
		f := &query.Field{Type: query.Type_GEOMETRY, ColumnType: "point"}
		got := decodeVStreamCell(f, []byte{0x01, 0x02, 0x03})
		if _, ok := got.([]byte); !ok {
			t.Errorf("got %T; want []byte fallback", got)
		}
	})

	t.Run("zero-srid prefix still strips cleanly", func(t *testing.T) {
		// SRID 0 (no spatial reference declared) is the most common
		// MySQL default; the prefix is 00 00 00 00 and stripping
		// behaves identically.
		zeroSRID := append([]byte{0, 0, 0, 0}, wkb...)
		f := &query.Field{Type: query.Type_GEOMETRY, ColumnType: "point"}
		got := decodeVStreamCell(f, zeroSRID)
		gotBytes, ok := got.([]byte)
		if !ok {
			t.Fatalf("got %T; want []byte", got)
		}
		if !reflect.DeepEqual(gotBytes, wkb) {
			t.Errorf("got %x; want %x", gotBytes, wkb)
		}
	})
}

// TestIsMySQLBoolColumnType covers the small parser used to detect
// TINYINT(1) from a Vitess FieldEvent's column_type string.
func TestIsMySQLBoolColumnType(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"tinyint(1)", true},
		{"tinyint(1) unsigned", true},
		{"TINYINT(1)", true},          // case-insensitive
		{"TINYINT(1) UNSIGNED", true}, // ditto
		{"tinyint", false},
		{"tinyint(2)", false},
		{"tinyint(4)", false},
		{"int", false},
		{"varchar(255)", false},
		{"", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			if got := isMySQLBoolColumnType(c.in); got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestStripKeyspaceFromTable covers the small helper that drops
// the "keyspace." prefix VStream sometimes prepends on table names
// (depending on flags).
func TestStripKeyspaceFromTable(t *testing.T) {
	cases := []struct {
		in       string
		keyspace string
		want     string
	}{
		{"users", "main", "users"},
		{"main.users", "main", "users"},
		{"main.users", "", "main.users"},       // no keyspace context, leave as-is
		{"other.users", "main", "other.users"}, // mismatched prefix
		{"", "main", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in+"/"+c.keyspace, func(t *testing.T) {
			if got := stripKeyspaceFromTable(c.in, c.keyspace); got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}

// TestResolveVStreamShards covers the open-time shard-resolution
// policy: explicit list wins, auto-discover requires the explicit
// list to be empty, and the contradictory-flags case errors loudly.
//
// Auto-discovery's network branch is exercised by the integration
// suite; here we only assert the validation logic.
func TestResolveVStreamShards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cases := []struct {
		name      string
		params    map[string]string
		want      []string
		wantErr   bool
		errSubstr string
	}{
		{
			name: "default — single unsharded shard",
			want: []string{"-"},
		},
		{
			name:   "explicit single shard",
			params: map[string]string{"vstream_shards": "0"},
			want:   []string{"0"},
		},
		{
			name:   "explicit multi-shard",
			params: map[string]string{"vstream_shards": "-80,80-"},
			want:   []string{"-80", "80-"},
		},
		{
			name: "both shards and auto-discover → error",
			params: map[string]string{
				"vstream_shards":               "-80,80-",
				"vstream_auto_discover_shards": "true",
			},
			wantErr:   true,
			errSubstr: "mutually exclusive",
		},
		// Auto-discovery without explicit shards isn't tested here:
		// it would require a live MySQL endpoint. The integration
		// suite covers it; this layer just validates the policy.
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cfg, _ := minimalConfig("host:3306", c.params)
			got, err := resolveVStreamShards(ctx, cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error containing %q; got nil", c.errSubstr)
				}
				if !strings.Contains(err.Error(), c.errSubstr) {
					t.Errorf("err = %q; want substring %q", err.Error(), c.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveVStreamShards: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestVStreamReader_ApplyReshardState asserts that the internal
// state-mutation half of Reopen swaps the reader's shard set,
// currentVgtid, and clears the field cache when handed a
// [ShardLayoutChangedError]. The gRPC stream rebuild itself is
// exercised by the multi-shard integration test.
func TestVStreamReader_ApplyReshardState(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace:     "main",
		shards:       []string{"-"},
		fields:       map[string][]*query.Field{"-/users": {{Name: "id"}}},
		currentVgtid: []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abcd:1-100"}},
	}
	r.setErr(errors.New("prior shard layout changed"))

	resh := &ShardLayoutChangedError{
		Keyspace: "main",
		Participants: []shardGtid{
			{Keyspace: "main", Shard: "-"},
		},
		NewShards: []shardGtid{
			{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/abcd:1-200"},
			{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/abcd:1-200"},
		},
	}
	if err := r.applyReshardState(resh); err != nil {
		t.Fatalf("applyReshardState: %v", err)
	}

	wantShards := []string{"-80", "80-"}
	if !reflect.DeepEqual(r.shards, wantShards) {
		t.Errorf("r.shards = %v; want %v", r.shards, wantShards)
	}
	if len(r.currentVgtid) != 2 {
		t.Errorf("r.currentVgtid has %d entries; want 2", len(r.currentVgtid))
	}
	if r.currentVgtid[0].Gtid != "MySQL56/abcd:1-200" {
		t.Errorf("r.currentVgtid[0].Gtid = %q; want refreshed GTID", r.currentVgtid[0].Gtid)
	}
	if len(r.fields) != 0 {
		t.Errorf("r.fields has %d entries; want 0 (cache cleared on reshard)", len(r.fields))
	}
	if r.Err() != nil {
		t.Errorf("r.Err() = %v; want nil after applyReshardState", r.Err())
	}
}

// TestVStreamReader_ApplyReshardStateGuards asserts the input
// validation: nil error and empty NewShards both refuse loudly so
// a downstream Reopen never attempts to build a stream against an
// empty vgtid.
func TestVStreamReader_ApplyReshardStateGuards(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		shards:   []string{"-"},
		fields:   make(map[string][]*query.Field),
	}
	if err := r.applyReshardState(nil); err == nil {
		t.Error("expected error for nil ShardLayoutChangedError")
	}
	if err := r.applyReshardState(&ShardLayoutChangedError{Keyspace: "main"}); err == nil {
		t.Error("expected error for empty NewShards")
	}
}

// TestShardLayoutChangedError_Is verifies the typed error matches
// the sentinel under errors.Is, which is the documented contract
// for callers.
func TestShardLayoutChangedError_Is(t *testing.T) {
	e := &ShardLayoutChangedError{Keyspace: "main"}
	if !errors.Is(e, ErrShardLayoutChanged) {
		t.Errorf("errors.Is(typed, sentinel) = false; want true")
	}
	// Unrelated errors must not match.
	if errors.Is(errors.New("other"), ErrShardLayoutChanged) {
		t.Errorf("errors.Is unrelated = true; want false")
	}
}

// makeRow builds a query.Row from a slice of string values. Each
// value's bytes go into Values; lengths array tracks per-cell
// length. Negative length signals NULL.
func makeRow(values []string) *query.Row {
	row := &query.Row{
		Lengths: make([]int64, len(values)),
	}
	for i, v := range values {
		row.Lengths[i] = int64(len(v))
		row.Values = append(row.Values, []byte(v)...)
	}
	return row
}

// drainChannel reads all currently-available changes off out
// (which the test has closed). Returns the slice in arrival order.
func drainChannel(out <-chan ir.Change) []ir.Change {
	got := make([]ir.Change, 0, cap(out))
	for c := range out {
		got = append(got, c)
	}
	return got
}

// minimalConfig wraps go-sql-driver/mysql.NewConfig with just what
// the endpoint test needs.
func minimalConfig(addr string, params map[string]string) (*gomysql.Config, error) {
	cfg := gomysql.NewConfig()
	cfg.Addr = addr
	cfg.User = "u"
	cfg.Passwd = "p"
	cfg.DBName = "main"
	if params != nil {
		cfg.Params = params
	}
	return cfg, nil
}
