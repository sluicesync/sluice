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
	"vitess.io/vitess/go/vt/proto/topodata"

	"sluicesync.dev/sluice/internal/ir"
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

// TestVStreamTabletTypeFromDSN pins the vstream_tablet_type DSN parse
// (ADR-0073 (b2) usability): the three valid values, the default, and a
// loud error for anything else.
func TestVStreamTabletTypeFromDSN(t *testing.T) {
	cases := []struct {
		name    string
		val     string // empty ⇒ param absent
		want    topodata.TabletType
		wantErr bool
	}{
		{name: "default (absent) ⇒ replica", val: "", want: topodata.TabletType_REPLICA},
		{name: "explicit replica", val: "replica", want: topodata.TabletType_REPLICA},
		{name: "primary", val: "primary", want: topodata.TabletType_PRIMARY},
		{name: "rdonly", val: "rdonly", want: topodata.TabletType_RDONLY},
		{name: "unknown ⇒ loud error", val: "secondary", wantErr: true},
		{name: "case-sensitive ⇒ loud error", val: "PRIMARY", wantErr: true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			params := map[string]string{}
			if c.val != "" {
				params["vstream_tablet_type"] = c.val
			}
			cfg, _ := minimalConfig("host:3306", params)
			got, err := vstreamTabletTypeFromDSN(cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want loud error for %q; got nil (type %v)", c.val, got)
				}
				if !strings.Contains(err.Error(), "vstream_tablet_type") {
					t.Errorf("error does not name the param: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestVStreamLivenessWindowFromDSN pins the vstream_liveness_timeout DSN
// parse: default when absent, a parsed duration, 0-disables, and a loud
// error on a malformed value.
func TestVStreamLivenessWindowFromDSN(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    time.Duration
		wantErr bool
	}{
		{name: "default (absent)", val: "", want: defaultVStreamLivenessWindow},
		{name: "explicit 45s", val: "45s", want: 45 * time.Second},
		{name: "zero disables", val: "0s", want: 0},
		{name: "malformed ⇒ loud error", val: "soon", wantErr: true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			params := map[string]string{}
			if c.val != "" {
				params["vstream_liveness_timeout"] = c.val
			}
			cfg, _ := minimalConfig("host:3306", params)
			got, err := vstreamLivenessWindowFromDSN(cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want loud error for %q; got nil (%v)", c.val, got)
				}
				if !strings.Contains(err.Error(), "vstream_liveness_timeout") {
					t.Errorf("error does not name the param: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestVStreamProgressWindowFromDSN pins the vstream_progress_timeout
// (Phase-2 CDC-tail) DSN parse: default when absent, parsed duration,
// 0-disables, malformed ⇒ loud error naming the param.
func TestVStreamProgressWindowFromDSN(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    time.Duration
		wantErr bool
	}{
		{name: "default (absent)", val: "", want: defaultVStreamProgressWindow},
		{name: "explicit 90s", val: "90s", want: 90 * time.Second},
		{name: "zero disables phase 2", val: "0s", want: 0},
		{name: "malformed ⇒ loud error", val: "later", wantErr: true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			params := map[string]string{}
			if c.val != "" {
				params["vstream_progress_timeout"] = c.val
			}
			cfg, _ := minimalConfig("host:3306", params)
			got, err := vstreamProgressWindowFromDSN(cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want loud error for %q; got nil (%v)", c.val, got)
				}
				if !strings.Contains(err.Error(), "vstream_progress_timeout") {
					t.Errorf("error does not name the param: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestVStreamCopyProgressWindowFromDSN pins the
// vstream_copy_progress_timeout (Phase-2 COPY pump) DSN parse, including
// that the default is the generous COPY window (slow-start tolerance).
func TestVStreamCopyProgressWindowFromDSN(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    time.Duration
		wantErr bool
	}{
		{name: "default (absent) is generous", val: "", want: defaultVStreamCopyProgressWindow},
		{name: "explicit 15m", val: "15m", want: 15 * time.Minute},
		{name: "zero disables phase 2", val: "0s", want: 0},
		{name: "malformed ⇒ loud error", val: "soon-ish", wantErr: true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			params := map[string]string{}
			if c.val != "" {
				params["vstream_copy_progress_timeout"] = c.val
			}
			cfg, _ := minimalConfig("host:3306", params)
			got, err := vstreamCopyProgressWindowFromDSN(cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want loud error for %q; got nil (%v)", c.val, got)
				}
				if !strings.Contains(err.Error(), "vstream_copy_progress_timeout") {
					t.Errorf("error does not name the param: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestVStreamCopyProgressWindow_GenerousByDefault sanity-pins that the
// COPY Phase-2 default is materially larger than the CDC-tail Phase-2
// default — the slow-start-tolerance invariant from the F3 design (the
// COPY warmup can take minutes; the CDC tail's seconds-scale window must
// never be applied to it).
func TestVStreamCopyProgressWindow_GenerousByDefault(t *testing.T) {
	if defaultVStreamCopyProgressWindow <= defaultVStreamProgressWindow {
		t.Fatalf("COPY progress window (%v) must be far more generous than the CDC-tail one (%v)",
			defaultVStreamCopyProgressWindow, defaultVStreamProgressWindow)
	}
}

// TestBuildVStreamRequest_TabletTypeSelection pins the cursor-dependent
// tablet selection (ADR-0072 + ADR-0073 (b2)):
//
//   - No TablePKs cursor ⇒ the reader's CONFIGURED tablet type (the
//     vstream_tablet_type default/override) is used for the pure CDC tail.
//   - A TablePKs cursor present ⇒ PRIMARY regardless of the configured
//     type (the COPY-resume override always wins).
func TestBuildVStreamRequest_TabletTypeSelection(t *testing.T) {
	// A valid encoded cursor so toProtoShardGtids decodes cleanly.
	cursor, err := encodeTablePKs([]*binlogdata.TableLastPK{{
		TableName: "widgets",
		Lastpk:    &query.QueryResult{},
	}})
	if err != nil {
		t.Fatalf("encodeTablePKs: %v", err)
	}

	noCursor := []shardGtid{{Keyspace: "main", Shard: "0", Gtid: "current"}}
	withCursor := []shardGtid{{Keyspace: "main", Shard: "0", Gtid: "MySQL56/abcd:1-5", TablePKs: cursor}}

	cases := []struct {
		name       string
		configured topodata.TabletType
		start      []shardGtid
		want       topodata.TabletType
	}{
		{name: "no cursor + default replica ⇒ replica", configured: topodata.TabletType_REPLICA, start: noCursor, want: topodata.TabletType_REPLICA},
		{name: "no cursor + primary override ⇒ primary", configured: topodata.TabletType_PRIMARY, start: noCursor, want: topodata.TabletType_PRIMARY},
		{name: "no cursor + rdonly override ⇒ rdonly", configured: topodata.TabletType_RDONLY, start: noCursor, want: topodata.TabletType_RDONLY},
		{name: "zero-value tablet type ⇒ replica default", configured: topodata.TabletType_UNKNOWN, start: noCursor, want: topodata.TabletType_REPLICA},
		{name: "cursor present + replica configured ⇒ PRIMARY wins", configured: topodata.TabletType_REPLICA, start: withCursor, want: topodata.TabletType_PRIMARY},
		{name: "cursor present + rdonly configured ⇒ PRIMARY wins", configured: topodata.TabletType_RDONLY, start: withCursor, want: topodata.TabletType_PRIMARY},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			r := &vstreamCDCReader{keyspace: "main", shards: []string{"0"}, tabletType: c.configured}
			req, err := r.buildVStreamRequest(c.start)
			if err != nil {
				t.Fatalf("buildVStreamRequest: %v", err)
			}
			if req.GetTabletType() != c.want {
				t.Errorf("TabletType = %v; want %v", req.GetTabletType(), c.want)
			}
		})
	}
}

// TestEventsProveLiveness pins the heartbeat-vs-serving discriminator
// (ADR-0073 (b2) Phase-A ground truth): a HEARTBEAT-only batch does NOT
// prove a serving tablet (vtgate heartbeats even on the no-tablet wedge),
// but any non-heartbeat event does. This is the load-bearing distinction
// that lets the watchdog fire on the dead stream without false-firing on
// a healthy idle one.
func TestEventsProveLiveness(t *testing.T) {
	hb := func() *binlogdata.VEvent { return &binlogdata.VEvent{Type: binlogdata.VEventType_HEARTBEAT} }
	cases := []struct {
		name string
		evs  []*binlogdata.VEvent
		want bool
	}{
		{name: "empty batch ⇒ no proof", evs: nil, want: false},
		{name: "single heartbeat ⇒ no proof", evs: []*binlogdata.VEvent{hb()}, want: false},
		{name: "multiple heartbeats ⇒ no proof", evs: []*binlogdata.VEvent{hb(), hb()}, want: false},
		{
			name: "VGTID present ⇒ proof (the healthy first event)",
			evs:  []*binlogdata.VEvent{{Type: binlogdata.VEventType_VGTID}},
			want: true,
		},
		{
			name: "heartbeat + VGTID ⇒ proof",
			evs:  []*binlogdata.VEvent{hb(), {Type: binlogdata.VEventType_VGTID}},
			want: true,
		},
		{
			name: "ROW present ⇒ proof",
			evs:  []*binlogdata.VEvent{{Type: binlogdata.VEventType_ROW}},
			want: true,
		},
		{
			name: "FIELD present ⇒ proof",
			evs:  []*binlogdata.VEvent{{Type: binlogdata.VEventType_FIELD}},
			want: true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := eventsProveLiveness(c.evs); got != c.want {
				t.Errorf("eventsProveLiveness = %v; want %v", got, c.want)
			}
		})
	}
}

// noopTimeout is a never-expected timeout callback for the phase the test
// is NOT exercising; a fire on it is a test failure.
func failingTimeout(t *testing.T, phase string) func() {
	t.Helper()
	return func() { t.Errorf("unexpected %s timeout fired", phase) }
}

// fakeLivenessTimer is a hand-fired [livenessTimer]: the test owns the
// fire channel and observes every Stop/Reset the watchdog performs, so
// the "must NOT fire while re-armed" property is pinned as a
// DETERMINISTIC re-arm count instead of a real-clock race. The old
// real-clock versions of these tests (sleep < window, observe, repeat)
// flaked on slow runners — a 15ms sleep stretching past the 50ms window
// fired the watchdog spuriously (the v0.99.31 windows-latest flake in
// TestVStreamLiveness_Phase2_ReArmsAcrossManyEvents).
type fakeLivenessTimer struct {
	fire   chan time.Time
	resets chan time.Duration
	stops  chan struct{}
}

func newFakeLivenessTimer() *fakeLivenessTimer {
	return &fakeLivenessTimer{
		fire:   make(chan time.Time, 1),
		resets: make(chan time.Duration, 64),
		stops:  make(chan struct{}, 64),
	}
}

func (f *fakeLivenessTimer) C() <-chan time.Time { return f.fire }

// Stop records the call and reports true (timer "was armed") so the
// watchdog's reset path skips its drain — the fake's channel only ever
// holds test-injected fires.
func (f *fakeLivenessTimer) Stop() bool { f.stops <- struct{}{}; return true }

func (f *fakeLivenessTimer) Reset(d time.Duration) { f.resets <- d }

// factory adapts the fake to the startVStreamLivenessWithTimer seam.
func (f *fakeLivenessTimer) factory() func(time.Duration) livenessTimer {
	return func(time.Duration) livenessTimer { return f }
}

// awaitReset blocks until the watchdog re-arms the fake timer, failing
// the test if no re-arm lands (generous bound — it only gates failure)
// or if the re-arm window differs from want.
func (f *fakeLivenessTimer) awaitReset(t *testing.T, want time.Duration) {
	t.Helper()
	select {
	case got := <-f.resets:
		if got != want {
			t.Fatalf("watchdog re-armed with %v; want %v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog never re-armed the timer")
	}
}

// awaitStop blocks until the watchdog calls Stop on the fake timer.
func (f *fakeLivenessTimer) awaitStop(t *testing.T) {
	t.Helper()
	select {
	case <-f.stops:
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog never stopped the timer")
	}
}

// TestVStreamLiveness_Phase1_FiresOnHeartbeatsOnly pins the v0.99.7
// primary-only guard (Phase 1): with only heartbeat-only observations —
// the no-serving-tablet wedge — the Phase-1 callback fires. Heartbeats do
// NOT clear Phase 1, and they do NOT re-arm it, so the absolute deadline
// from stream-open elapses and fires. THIS MUST NOT REGRESS.
func TestVStreamLiveness_Phase1_FiresOnHeartbeatsOnly(t *testing.T) {
	p1 := make(chan struct{}, 1)
	live := startVStreamLiveness(context.Background(), 60*time.Millisecond, 500*time.Millisecond, 0,
		func() { p1 <- struct{}{} },
		failingTimeout(t, "phase-2"),
		nil)
	defer live.stop()

	// Drip heartbeat-only observations faster than the Phase-1 window from
	// a goroutine bounded by stop. A regression where heartbeats re-armed
	// Phase 1 would keep it alive forever (the primary-only wedge would
	// never surface) — this pins that they do not. The drip goroutine
	// stops itself on stopDrip so it can't outlive the test.
	stopDrip := make(chan struct{})
	defer close(stopDrip)
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				live.observe(false) // heartbeat-only
			case <-stopDrip:
				return
			}
		}
	}()

	select {
	case <-p1:
		// good — heartbeats did not keep Phase 1 alive
	case <-time.After(2 * time.Second):
		t.Fatal("Phase-1 watchdog did not fire on heartbeats-only — the primary-only guard regressed")
	}
}

// TestVStreamLiveness_Phase1_FiresOnNoEvent pins that with NO observations
// at all the Phase-1 callback still fires (the dead-from-open stream).
func TestVStreamLiveness_Phase1_FiresOnNoEvent(t *testing.T) {
	p1 := make(chan struct{}, 1)
	live := startVStreamLiveness(context.Background(), 30*time.Millisecond, time.Second, 0,
		func() { p1 <- struct{}{} },
		failingTimeout(t, "phase-2"),
		nil)
	defer live.stop()
	select {
	case <-p1:
	case <-time.After(2 * time.Second):
		t.Fatal("Phase-1 watchdog did not fire — the silent-hang guard is broken")
	}
}

// TestVStreamLiveness_ServingProofTransitionsToPhase2 pins that a single
// serving-proof observation clears Phase 1: the watchdog re-arms with the
// Phase-2 window, and a subsequent timer fire routes to the PHASE-2
// callback, never Phase 1. Fake-timer test: the fire is hand-injected
// after the re-arm is observed, so there is no real-clock race between
// observe and the Phase-1 deadline.
func TestVStreamLiveness_ServingProofTransitionsToPhase2(t *testing.T) {
	ft := newFakeLivenessTimer()
	p2 := make(chan struct{}, 1)
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 0,
		failingTimeout(t, "phase-1"),
		func() { p2 <- struct{}{} },
		nil,
		ft.factory())
	defer live.stop()

	live.observe(true)            // serving proof
	ft.awaitReset(t, time.Second) // Phase 1 cleared: re-armed with phase2Window

	ft.fire <- time.Now() // a fire AFTER the transition must be Phase 2
	select {
	case <-p2:
	case <-time.After(10 * time.Second):
		t.Fatal("timer fire after serving proof did not reach the Phase-2 callback")
	}
}

// TestVStreamLiveness_Phase2_FiresOnTotalSilence pins the NEW mid-stream
// guard: after data has flowed (a serving-proof observation), TOTAL
// silence past the Phase-2 window fires the Phase-2 callback — the
// post-failover dead-Recv wedge becomes a LOUD failure.
func TestVStreamLiveness_Phase2_FiresOnTotalSilence(t *testing.T) {
	p2 := make(chan struct{}, 1)
	// Real-clock fire test (kept deliberately: it exercises the real
	// time.Timer integration). The Phase-1 window is an hour so a starved
	// runner can't fire Phase 1 before the observe(true) below lands.
	live := startVStreamLiveness(context.Background(), time.Hour, 40*time.Millisecond, 0,
		failingTimeout(t, "phase-1"),
		func() { p2 <- struct{}{} },
		nil)
	defer live.stop()
	live.observe(true) // serving proven; now go silent
	select {
	case <-p2:
	case <-time.After(2 * time.Second):
		t.Fatal("Phase-2 watchdog did not fire on total silence — the mid-stream wedge guard is broken")
	}
}

// TestVStreamLiveness_Phase2_HeartbeatsKeepAlive pins that once in Phase 2
// a healthy idle stream — whose ~5s heartbeats keep arriving — never
// false-times-out: each heartbeat-only observation re-arms the Phase-2
// progress deadline. Fake-timer test: "never times out" is pinned as a
// deterministic re-arm count (every heartbeat-only observation produces a
// Reset(phase2Window)); the timer never fires because the test never
// fires it, so the failing callbacks guard against any spurious path.
func TestVStreamLiveness_Phase2_HeartbeatsKeepAlive(t *testing.T) {
	ft := newFakeLivenessTimer()
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 0,
		failingTimeout(t, "phase-1"),
		func() {
			t.Error("Phase-2 fired despite heartbeats re-arming it — would false-time-out a healthy idle stream")
		},
		nil,
		ft.factory())
	defer live.stop()

	live.observe(true)            // enter Phase 2
	ft.awaitReset(t, time.Second) // armed with phase2Window
	for i := 0; i < 15; i++ {
		live.observe(false)           // heartbeat-only — must re-arm Phase 2
		ft.awaitReset(t, time.Second) // …and here is the re-arm, counted
	}
}

// TestVStreamLiveness_Phase2_ReArmsAcrossManyEvents pins the re-arm path
// across a mix of data and heartbeat observations: as long as SOMETHING
// keeps arriving, Phase 2 re-arms every time and never fires. Fake-timer
// test (this was the v0.99.31 windows-latest flake: the real-clock
// version's 15ms sleeps stretched past the 50ms window under a slow
// runner and fired the watchdog spuriously); each observation's re-arm is
// now awaited explicitly — a deterministic count of 20.
func TestVStreamLiveness_Phase2_ReArmsAcrossManyEvents(t *testing.T) {
	ft := newFakeLivenessTimer()
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 0,
		failingTimeout(t, "phase-1"),
		func() { t.Error("Phase-2 fired despite continuous events re-arming it") },
		nil,
		ft.factory())
	defer live.stop()

	live.observe(true)
	ft.awaitReset(t, time.Second)
	for i := 0; i < 20; i++ {
		live.observe(i%2 == 0) // alternate data / heartbeat
		ft.awaitReset(t, time.Second)
	}
}

// TestVStreamLiveness_Phase2Disabled pins that progressWindow<=0 keeps
// Phase 1 but disables Phase 2: clearing Phase 1 DISARMS the timer (a
// bare Stop, no re-arm), and a stale fire while disarmed is ignored —
// the watchdog stays quiescent but keeps servicing observations.
func TestVStreamLiveness_Phase2Disabled(t *testing.T) {
	ft := newFakeLivenessTimer()
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, 0, 0,
		failingTimeout(t, "phase-1"),
		func() { t.Error("Phase-2 fired despite being disabled (progressWindow=0)") },
		nil,
		ft.factory())
	defer live.stop()

	live.observe(true) // clears Phase 1; Phase 2 disabled ⇒ disarm
	ft.awaitStop(t)    // reset(0) stops the timer WITHOUT re-arming

	ft.fire <- time.Now() // a stale fire while disarmed must be ignored
	live.observe(false)   // quiescent watchdog still services observations…
	ft.awaitStop(t)       // …(its reset(0) lands as another bare Stop)

	// No re-arm may ever have happened with Phase 2 disabled. (The stale
	// fire can never reach a callback: armed stays false for good, so the
	// failing phase-2 callback above guards the whole window.)
	select {
	case d := <-ft.resets:
		t.Fatalf("watchdog re-armed to %v with Phase 2 disabled; want no re-arm", d)
	default:
	}
}

// TestVStreamLiveness_DisabledWindow pins that a 0/negative Phase-1 window
// disables the whole watchdog: no goroutine, no callback ever fires,
// observe/stop are safe no-ops.
func TestVStreamLiveness_DisabledWindow(t *testing.T) {
	live := startVStreamLiveness(context.Background(), 0, time.Second, 0,
		failingTimeout(t, "phase-1"),
		failingTimeout(t, "phase-2"),
		nil)
	live.observe(true)
	live.observe(false)
	live.stop()
	time.Sleep(100 * time.Millisecond)
}

// TestVStreamLiveness_StopBeforeFire pins that stop() tears the watchdog
// down so neither callback fires after teardown. Fake-timer test: the
// watchdog goroutine's exit is observed via its deferred timer.Stop (the
// only Stop in this sequence — no reset ever ran), after which a fire has
// no listener and observe is a safe no-op. The old real-clock version
// raced stop() against a 40ms deadline — on a starved runner both could
// be ready in the same select and the random pick fired the callback.
func TestVStreamLiveness_StopBeforeFire(t *testing.T) {
	ft := newFakeLivenessTimer()
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Minute, 0,
		failingTimeout(t, "phase-1"),
		failingTimeout(t, "phase-2"),
		nil,
		ft.factory())
	live.stop()
	ft.awaitStop(t) // the deferred timer.Stop ⇒ the goroutine has exited

	ft.fire <- time.Now() // no listener left; must not reach a callback
	live.observe(true)    // observe after stop is a safe no-op
}

// TestVStreamLivenessTimeoutError_Actionable pins that the Phase-1 loud
// error names the tablet type, the keyspace, and the vstream_tablet_type
// remediation — the operator-facing contract for the primary-only wedge.
func TestVStreamLivenessTimeoutError_Actionable(t *testing.T) {
	err := vstreamLivenessTimeoutError(30*time.Second, topodata.TabletType_REPLICA, "main", []string{"0"})
	msg := err.Error()
	// Both candidate causes must be named: the primary-only topology wedge
	// AND the source-tablet throttler (item 19(a) change 1) — a Phase-1
	// timeout can be either, and naming only topology mis-diagnoses a
	// throttle as a missing tablet.
	for _, want := range []string{
		"no events within", "REPLICA", `"main"`,
		"vstream_tablet_type=primary",
		"throttler", "SHOW VITESS_THROTTLED_APPS",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q: %v", want, msg)
		}
	}
}

// TestVStreamProgressTimeoutError_Actionable pins that the Phase-2 loud
// error names the mid-stream/failover cause so an operator (or a log
// scraper) can tell it apart from the primary-only Phase-1 error.
func TestVStreamProgressTimeoutError_Actionable(t *testing.T) {
	err := vstreamProgressTimeoutError(45*time.Second, topodata.TabletType_PRIMARY, "main", []string{"0"})
	msg := err.Error()
	for _, want := range []string{"no events for", "PRIMARY", `"main"`, "failover", "reparent"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q: %v", want, msg)
		}
	}
}

// fakeTimerPair hands the watchdog two distinct fake timers via [factory]:
// the FIRST newTimer call (the hard phase timer) gets hard, the SECOND (the
// soft idle-WARN timer) gets soft. The existing single-fake [fakeLivenessTimer]
// can't drive the soft-window tests because [vstreamLiveness.run] owns BOTH
// timers and would otherwise alias them to one instance. The factory is
// call-order-deterministic: run() always creates the hard timer first, then
// (only if softEnabled) the soft timer.
type fakeTimerPair struct {
	hard *fakeLivenessTimer
	soft *fakeLivenessTimer
	n    int
}

func newFakeTimerPair() *fakeTimerPair {
	return &fakeTimerPair{hard: newFakeLivenessTimer(), soft: newFakeLivenessTimer()}
}

func (p *fakeTimerPair) factory() func(time.Duration) livenessTimer {
	return func(time.Duration) livenessTimer {
		p.n++
		if p.n == 1 {
			return p.hard
		}
		return p.soft
	}
}

// TestVStreamLiveness_SoftIdle_FiresOnceOnHeartbeatSpell pins item 19(a)
// change 2: in Phase 2, a spell of heartbeat-only observations lets the
// SOFT timer elapse and fires the idle WARN — and a heartbeat re-arms the
// HARD timer (the stream stays alive) but does NOT re-arm the soft timer,
// which is exactly why the spell is detectable. The WARN fires AT MOST ONCE
// per spell (the latch): a second soft fire without an intervening real
// event is swallowed.
func TestVStreamLiveness_SoftIdle_FiresOnceOnHeartbeatSpell(t *testing.T) {
	ft := newFakeTimerPair()
	warns := make(chan struct{}, 8)
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 100*time.Millisecond,
		failingTimeout(t, "phase-1"),
		failingTimeout(t, "phase-2"), // hard Phase-2 must NOT fire — soft is observability only
		func() { warns <- struct{}{} },
		ft.factory())
	defer live.stop()

	live.observe(true)                          // serving proof → Phase 2
	ft.hard.awaitReset(t, time.Second)          // hard re-armed with phase2Window
	ft.soft.awaitReset(t, 100*time.Millisecond) // soft armed on the transition

	// Heartbeat-only: hard re-arms (stream alive), soft does NOT — this is
	// the throttle/idle signature.
	live.observe(false)
	ft.hard.awaitReset(t, time.Second)
	select {
	case d := <-ft.soft.resets:
		t.Fatalf("heartbeat-only re-armed the SOFT timer to %v; it must not — that would hide the idle spell", d)
	case <-time.After(50 * time.Millisecond):
	}

	// Fire the soft timer: the WARN fires once.
	ft.soft.fire <- time.Now()
	select {
	case <-warns:
	case <-time.After(2 * time.Second):
		t.Fatal("soft idle WARN did not fire on a heartbeat-only spell")
	}

	// A SECOND soft fire without an intervening real event is latched off.
	ft.soft.fire <- time.Now()
	select {
	case <-warns:
		t.Fatal("soft idle WARN fired twice in one quiet spell — the once-per-spell latch is broken")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestVStreamLiveness_SoftIdle_RealEventReArmsAndReLatches pins that a REAL
// (non-heartbeat) event re-arms the soft timer AND clears the warn latch,
// so a stream that resumes progress can WARN again on the NEXT idle spell.
func TestVStreamLiveness_SoftIdle_RealEventReArmsAndReLatches(t *testing.T) {
	ft := newFakeTimerPair()
	warns := make(chan struct{}, 8)
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 100*time.Millisecond,
		failingTimeout(t, "phase-1"),
		failingTimeout(t, "phase-2"),
		func() { warns <- struct{}{} },
		ft.factory())
	defer live.stop()

	live.observe(true) // Phase 2
	ft.hard.awaitReset(t, time.Second)
	ft.soft.awaitReset(t, 100*time.Millisecond)

	// First spell → WARN.
	ft.soft.fire <- time.Now()
	<-warns

	// A real event re-arms the soft timer and clears the latch.
	live.observe(true)
	ft.hard.awaitReset(t, time.Second)
	ft.soft.awaitReset(t, 100*time.Millisecond)

	// Second spell → WARN fires AGAIN (latch was cleared by the real event).
	ft.soft.fire <- time.Now()
	select {
	case <-warns:
	case <-time.After(2 * time.Second):
		t.Fatal("soft idle WARN did not re-fire after a real event cleared the latch")
	}
}

// TestVStreamLiveness_SoftIdle_NeverDuringPhase1 pins that the soft WARN is
// strictly a Phase-2 concept: while Phase 1 is un-cleared the soft timer is
// disarmed, and even a stale soft fire before serving is proven must not
// warn (the phase2 guard in the soft-fire branch).
func TestVStreamLiveness_SoftIdle_NeverDuringPhase1(t *testing.T) {
	ft := newFakeTimerPair()
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 100*time.Millisecond,
		failingTimeout(t, "phase-1"),
		failingTimeout(t, "phase-2"),
		func() { t.Error("soft idle WARN fired during Phase 1 — it must be Phase-2 only") },
		ft.factory())
	defer live.stop()

	// The soft timer is created then immediately Stop()ped (disarmed) at
	// construction; pin that bare Stop landed and no arm/reset happened.
	ft.soft.awaitStop(t)
	select {
	case d := <-ft.soft.resets:
		t.Fatalf("soft timer was armed (%v) during Phase 1; it must stay disarmed until serving is proven", d)
	default:
	}

	// A stale soft fire while still in Phase 1 must not reach the WARN.
	ft.soft.fire <- time.Now()
	live.observe(false) // heartbeat-only; still Phase 1, soft stays disarmed
	time.Sleep(100 * time.Millisecond)
}

// TestVStreamLiveness_SoftIdle_DisabledByZeroWindow pins that softWindow<=0
// disables the soft WARN entirely: no soft timer is ever created, and the
// hard phases behave exactly as before. (Pinned via the fake: only ONE
// timer — the hard one — is ever requested from the factory.)
func TestVStreamLiveness_SoftIdle_DisabledByZeroWindow(t *testing.T) {
	ft := newFakeTimerPair()
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 0,
		failingTimeout(t, "phase-1"),
		failingTimeout(t, "phase-2"),
		func() { t.Error("soft idle WARN fired with softWindow=0 — it must be disabled") },
		ft.factory())
	defer live.stop()

	live.observe(true) // Phase 2
	ft.hard.awaitReset(t, time.Second)

	// The soft timer was never requested (factory call count stays 1).
	if ft.n != 1 {
		t.Fatalf("factory called %d times with softWindow=0; want exactly 1 (hard timer only)", ft.n)
	}
	// And the soft fake never saw any activity.
	select {
	case d := <-ft.soft.resets:
		t.Fatalf("soft timer was armed (%v) with softWindow=0", d)
	default:
	}
}

// TestVStreamIdleWarnMessage_Actionable pins that the heads-up names the
// keyspace, names BOTH causes (throttled or idle), and points at the
// out-of-band primary check — the operator-facing contract for item 19(a).
func TestVStreamIdleWarnMessage_Actionable(t *testing.T) {
	msg := vstreamIdleWarnMessage(30*time.Second, "main", []string{"0"})
	for _, want := range []string{
		"heartbeats flowing", "NO change events", `"main"`,
		"throttled", "idle", "SHOW VITESS_THROTTLED_APPS",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("idle-warn message missing %q: %v", want, msg)
		}
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

// TestDecodeVStreamRow_TinyInt1OutOfRangeWarns pins the Vector D CDC-tail
// wiring on the VStream path: a TINYINT(1) cell outside {0,1} is decoded to
// a bool (per convention) but emits the one-time-per-column WARN naming the
// column + the --type-override remedy. VStream cells are text-encoded, so the
// detection re-parses the literal to recover the real integer.
func TestDecodeVStreamRow_TinyInt1OutOfRangeWarns(t *testing.T) {
	buf := captureSlog(t)
	fields := []*query.Field{
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "active", Type: query.Type_INT8, ColumnType: "tinyint(1)"},
	}
	warner := newBoolRangeWarner()

	// id=1, active=2 (out of range) -> active decodes true, warns once.
	out, ok, err := decodeVStreamRow(&query.Row{Lengths: []int64{1, 1}, Values: []byte("12")}, fields, "users", warner)
	if err != nil {
		t.Fatalf("decodeVStreamRow: %v", err)
	}
	if !ok {
		t.Fatal("decodeVStreamRow ok=false")
	}
	if out["active"] != true {
		t.Errorf("active = %#v; want true (non-zero -> true)", out["active"])
	}
	// id=3, active=127 -> still out of range, must NOT warn again.
	if _, _, err := decodeVStreamRow(&query.Row{Lengths: []int64{1, 3}, Values: []byte("3127")}, fields, "users", warner); err != nil {
		t.Fatalf("decodeVStreamRow: %v", err)
	}

	o := buf.String()
	if c := strings.Count(o, "column=users.active"); c != 1 {
		t.Errorf("users.active warned %d times; want exactly 1\n%s", c, o)
	}
	if !strings.Contains(o, "--type-override users.active=smallint") {
		t.Errorf("WARN missing the --type-override hint:\n%s", o)
	}

	// In-range bool (0/1) never warns.
	buf.Reset()
	if _, _, err := decodeVStreamRow(&query.Row{Lengths: []int64{1, 1}, Values: []byte("10")}, fields, "users", newBoolRangeWarner()); err != nil {
		t.Fatalf("decodeVStreamRow: %v", err)
	}
	if strings.Contains(buf.String(), "users.active") {
		t.Errorf("in-range value warned:\n%s", buf.String())
	}
}

// TestDecodeVStreamCell_ZeroDateSentinel pins Vector A VStream parity at the
// cell level: a zero/partial date decodes to the shared *zeroDateValueError
// sentinel (resolved by decodeVStreamRow per --zero-date), a valid date to
// time.Time, and a genuinely malformed non-zero date to raw bytes.
func TestDecodeVStreamCell_ZeroDateSentinel(t *testing.T) {
	mk := func(ct string, qt query.Type) *query.Field {
		return &query.Field{Type: qt, ColumnType: ct}
	}
	for _, c := range []struct {
		name string
		f    *query.Field
		raw  string
	}{
		{"date all-zero", mk("date", query.Type_DATE), "0000-00-00"},
		{"date zero-month", mk("date", query.Type_DATE), "2026-00-15"},
		{"date zero-day", mk("date", query.Type_DATE), "2026-06-00"},
		{"datetime all-zero", mk("datetime", query.Type_DATETIME), "0000-00-00 00:00:00"},
		{"timestamp zero-day", mk("timestamp", query.Type_TIMESTAMP), "2026-06-00 01:02:03"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := decodeVStreamCell(c.f, []byte(c.raw))
			if _, ok := got.(*zeroDateValueError); !ok {
				t.Fatalf("decodeVStreamCell(%q) = %T; want *zeroDateValueError", c.raw, got)
			}
		})
	}
	// Valid date still decodes to time.Time.
	if got := decodeVStreamCell(mk("date", query.Type_DATE), []byte("2026-06-07")); func() bool { _, ok := got.(*zeroDateValueError); return ok }() {
		t.Errorf("valid date returned the zero-date sentinel: %#v", got)
	}
	// Genuinely malformed non-zero date stays raw bytes (loud downstream).
	if got := decodeVStreamCell(mk("date", query.Type_DATE), []byte("2026-13-40")); func() bool { _, ok := got.([]byte); return !ok }() {
		t.Errorf("malformed date = %T; want []byte passthrough", got)
	}
}

// TestDecodeVStreamRow_ZeroDatePolicy pins that decodeVStreamRow resolves the
// zero-date sentinel per the configured --zero-date policy: error refuses
// loudly (naming the column), null carries nil (refused on a NOT NULL field),
// epoch substitutes the representable floor.
func TestDecodeVStreamRow_ZeroDatePolicy(t *testing.T) {
	// Nullable DATE field "d" (NOT_NULL flag unset) + a non-null id.
	fields := []*query.Field{
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint", Flags: mysqlNotNullFlag},
		{Name: "d", Type: query.Type_DATE, ColumnType: "date"},
	}
	rowZero := func() *query.Row {
		return &query.Row{Lengths: []int64{1, 10}, Values: []byte("10000-00-00")} // id=1, d=0000-00-00
	}

	t.Run("error refuses loudly", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateRefuse)
		_, _, err := decodeVStreamRow(rowZero(), fields, "events", newBoolRangeWarner())
		if err == nil {
			t.Fatal("err = nil; want a zero-date refusal")
		}
		if !strings.Contains(err.Error(), `"d"`) || !strings.Contains(err.Error(), "zero/partial date") {
			t.Errorf("err = %q; want it to name column d + the zero/partial date", err)
		}
	})

	t.Run("null carries nil", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsNull)
		out, _, err := decodeVStreamRow(rowZero(), fields, "events", newBoolRangeWarner())
		if err != nil {
			t.Fatalf("err = %v; want nil under null policy", err)
		}
		if out["d"] != nil {
			t.Errorf("d = %#v; want nil (NULL)", out["d"])
		}
	})

	t.Run("epoch substitutes floor", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsEpoch)
		out, _, err := decodeVStreamRow(rowZero(), fields, "events", newBoolRangeWarner())
		if err != nil {
			t.Fatalf("err = %v; want nil under epoch policy", err)
		}
		got, ok := out["d"].(time.Time)
		if !ok {
			t.Fatalf("d = %T; want time.Time", out["d"])
		}
		if !got.Equal(zeroDateEpochValue) {
			t.Errorf("d = %v; want epoch sentinel %v", got, zeroDateEpochValue)
		}
	})

	t.Run("null on NOT NULL field refuses", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsNull)
		nnFields := []*query.Field{
			{Name: "d", Type: query.Type_DATE, ColumnType: "date", Flags: mysqlNotNullFlag},
		}
		nnRow := &query.Row{Lengths: []int64{10}, Values: []byte("0000-00-00")}
		_, _, err := decodeVStreamRow(nnRow, nnFields, "events", newBoolRangeWarner())
		if err == nil {
			t.Fatal("err = nil; want a NOT NULL refusal under --zero-date=null")
		}
		if !strings.Contains(err.Error(), "NOT NULL") {
			t.Errorf("err = %q; want it to name the NOT NULL conflict", err)
		}
	})
}
