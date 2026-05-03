package mysql

import (
	"context"
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
// terminates the dispatch with a clear error so the caller knows
// to rediscover the shard layout. With StopOnReshard:true the
// stream itself terminates after this; surfacing the error first
// gives the operator something actionable to log.
func TestVStreamReader_JournalErrors(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "main",
		fields:   make(map[string][]*query.Field),
	}
	out := make(chan ir.Change, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ev := &binlogdata.VEvent{Type: binlogdata.VEventType_JOURNAL}
	if err := r.dispatch(ctx, ev, out); err == nil {
		t.Fatal("expected error for JOURNAL event")
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
