package mysql

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"

	gomysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"github.com/orware/sluice/internal/ir"
)

// vstreamCDCReader streams MySQL row changes from a Vitess VStream
// gRPC endpoint as [ir.Change] events. It is the FlavorPlanetScale
// counterpart to the binlog-based [CDCReader] used by FlavorVanilla.
//
// One reader → one [StreamChanges] call. Concurrent calls are not
// supported. Close cancels the streaming goroutine and releases the
// underlying gRPC connection.
//
// VStream's protocol differs from MySQL's binlog in three ways that
// shape the design:
//
//  1. Per-table column metadata arrives in FIELD events; subsequent
//     ROW events reference columns positionally and rely on the
//     reader having cached the field list. (cf. PG pgoutput's
//     RelationMessage.)
//  2. The position primitive is a slice of (keyspace, shard, gtid)
//     tuples — one per shard the operator's stream covers. For
//     unsharded keyspaces (sluice's v1 target) the slice has
//     exactly one entry.
//  3. VStream has built-in COPY mode: an empty-string Gtid asks
//     vtgate to run an internal table-copy phase before tailing
//     CDC, with COPY_COMPLETED events signalling the handoff.
//     Sluice's coldStart path can either let VStream do the
//     snapshot itself (start from "") or pre-snapshot via SQL and
//     resume from "current"; this reader emits ir.Change events
//     either way and leaves the policy choice to the orchestrator.
type vstreamCDCReader struct {
	// host:port of the vtgate gRPC endpoint. PlanetScale uses 443
	// by default; vttestserver and self-hosted Vitess can use any
	// port — the value comes from the DSN's vstream_endpoint
	// override or is derived from the SQL host with :443 appended.
	endpoint string

	// authHeader is the pre-computed `Basic <b64(user:pass)>` value
	// attached to every gRPC call as the "authorization" metadata
	// header. Encoded once at construction so the per-call
	// PerRPCCredentials path stays O(1).
	authHeader string

	// keyspace is the database name from the DSN. PlanetScale's
	// keyspace concept maps onto sluice's per-engine schema.
	keyspace string

	// shards is the shard layout the stream subscribes to. Empty
	// means "discover via SHOW VITESS_SHARDS at open time"; a
	// concrete slice (e.g. ["-"] for unsharded) skips the lookup.
	// The reader populates this from cfg-side config or by
	// querying the keyspace metadata in resolveStartPosition.
	shards []string

	// tlsConfig is the TLS configuration applied to the gRPC dial.
	// nil means "default TLS" (PlanetScale's edge); tests can
	// inject an insecure config for vttestserver-on-localhost.
	tlsConfig *tls.Config

	// conn is the underlying gRPC client connection. Held for the
	// reader's lifetime so multiple StreamChanges calls (currently
	// disallowed; reserved for a future API change) would share it.
	conn *grpc.ClientConn

	// client is the typed Vitess gRPC client wrapping conn.
	client vtgateservice.VitessClient

	// streamerCancel cancels the goroutine pumping events into the
	// out channel. Stored on the reader so Close can stop a stream
	// even when the caller's context isn't readily available.
	streamerCancel context.CancelFunc

	// fields caches column metadata keyed by qualified table name
	// ("keyspace.table" or just "table" when keyspace is empty in
	// the FieldEvent). VStream sends a FIELD event the first time
	// it sees a table; later events for the same table reference
	// columns positionally, so the cache is mandatory for ROW
	// decoding.
	fields map[string][]*query.Field

	// currentVgtid is the latest position the reader has observed.
	// VStream emits a VGTID after each transaction; we update this
	// then promote it to the candidate position emitted alongside
	// every ir.Change.
	currentVgtid []shardGtid

	// mu guards err. The streaming goroutine writes; callers read
	// via Err after the channel closes.
	mu  sync.Mutex
	err error
}

// vstreamChannelBuffer matches the binlog reader's
// cdcChannelBuffer — backpressure knob; small enough to stall the
// gRPC reader on a slow consumer within a few hundred events,
// large enough to absorb burst rates.
const vstreamChannelBuffer = 256

// openVStreamReader is the FlavorPlanetScale path of the engine's
// OpenCDCReader. It parses the standard MySQL DSN to extract the
// service-token user/password, derives the gRPC endpoint, dials,
// and returns a reader ready for StreamChanges.
//
// DSN sources:
//   - User / Passwd → service-token name / value (HTTP Basic auth)
//   - Addr (the host:port from the tcp(...) wrapper) → gRPC
//     endpoint host. The default port is 443; override via the
//     `vstream_endpoint` DSN parameter for self-hosted Vitess /
//     vttestserver.
//   - DBName → keyspace.
//
// Sharded keyspaces are out of scope for v1: this reader assumes
// one shard ("-"). The check for that lands in resolveStartPosition
// when StreamChanges runs (we want errors at usage time, not at
// Open time, so the engine can be opened in dry-run flows that
// don't actually stream).
func openVStreamReader(_ context.Context, dsn string) (ir.CDCReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.User == "" {
		return nil, errors.New("mysql/vstream: DSN has no user (service-token name expected)")
	}
	if cfg.DBName == "" {
		return nil, errors.New("mysql/vstream: DSN has no database name (vitess keyspace expected)")
	}

	endpoint, err := vstreamEndpointFromDSN(cfg)
	if err != nil {
		return nil, err
	}

	tlsCfg := vstreamTLSConfigFromDSN(cfg)

	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(cfg.User+":"+cfg.Passwd))

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithPerRPCCredentials(&vstreamBasicAuth{header: authHeader}),
	)
	if err != nil {
		return nil, fmt.Errorf("mysql/vstream: dial %s: %w", endpoint, err)
	}

	return &vstreamCDCReader{
		endpoint:   endpoint,
		authHeader: authHeader,
		keyspace:   cfg.DBName,
		shards:     []string{"-"}, // v1: unsharded only
		tlsConfig:  tlsCfg,
		conn:       conn,
		client:     vtgateservice.NewVitessClient(conn),
		fields:     make(map[string][]*query.Field),
	}, nil
}

// vstreamEndpointFromDSN derives the vtgate gRPC endpoint from the
// MySQL DSN. The default rule — host (no port) + ":443" — matches
// PlanetScale's connect-host convention. The `vstream_endpoint`
// DSN parameter overrides for self-hosted Vitess and tests.
func vstreamEndpointFromDSN(cfg *gomysql.Config) (string, error) {
	if v := cfg.Params["vstream_endpoint"]; v != "" {
		return v, nil
	}
	host := cfg.Addr
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	if host == "" {
		return "", errors.New("mysql/vstream: empty host in DSN; cannot derive vtgate endpoint")
	}
	return host + ":443", nil
}

// vstreamTLSConfigFromDSN returns the TLS config for the gRPC dial.
// v1 honours one DSN flag — `vstream_insecure_tls=true` — for
// vttestserver-on-localhost; everything else uses default TLS
// (system roots + ServerName from the endpoint host).
func vstreamTLSConfigFromDSN(cfg *gomysql.Config) *tls.Config {
	if cfg.Params["vstream_insecure_tls"] == "true" {
		return &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in for tests
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

// vstreamBasicAuth is a gRPC credentials.PerRPCCredentials that
// attaches a pre-encoded HTTP Basic auth header to every call.
// PlanetScale's edge gateway authenticates service tokens via
// Authorization: Basic <b64(name:value)>.
type vstreamBasicAuth struct {
	header string
}

// GetRequestMetadata implements credentials.PerRPCCredentials.
func (b *vstreamBasicAuth) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": b.header}, nil
}

// RequireTransportSecurity returns true so basic auth never rides
// over plaintext gRPC. The vstream_insecure_tls=true override
// applies to certificate verification, not to whether TLS is used
// at all — testcontainers vttestserver still terminates TLS.
func (*vstreamBasicAuth) RequireTransportSecurity() bool { return true }

// setErr stores the first error the streaming goroutine sees.
// Subsequent errors are dropped — the original cause is the
// useful one. Mirrors the binlog reader's helper.
func (r *vstreamCDCReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// Close cancels the streaming goroutine (if any) and closes the
// gRPC connection. Safe to call multiple times.
func (r *vstreamCDCReader) Close() error {
	if r.streamerCancel != nil {
		r.streamerCancel()
		r.streamerCancel = nil
	}
	if r.conn != nil {
		err := r.conn.Close()
		r.conn = nil
		return err
	}
	return nil
}

// Err returns the error that terminated the streaming goroutine, if
// any. nil after a clean ctx-cancellation shutdown.
func (r *vstreamCDCReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// StreamChanges opens the VStream and pumps decoded events into the
// returned channel. The channel is closed when the stream ends
// (clean ctx cancellation) or errors (Err returns the cause).
//
// from controls the start position:
//   - Empty position (zero ir.Position): start from "current" — the
//     head of the binlog, no COPY phase. Suitable for a CDC-only
//     consumer or for an orchestrator that ran its own snapshot
//     beforehand.
//   - Decoded shardGtid slice: resume from the persisted position.
func (r *vstreamCDCReader) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	startPos, err := r.resolveStartPosition(from)
	if err != nil {
		return nil, err
	}
	r.currentVgtid = startPos

	req := r.buildVStreamRequest(startPos)
	loopCtx, cancel := context.WithCancel(ctx)
	r.streamerCancel = cancel

	stream, err := r.client.VStream(loopCtx, req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("mysql/vstream: open stream: %w", err)
	}

	out := make(chan ir.Change, vstreamChannelBuffer)
	go r.pump(loopCtx, stream, out)
	return out, nil
}

// resolveStartPosition turns the caller-supplied [ir.Position] into
// a concrete []shardGtid. Empty position becomes "current" for the
// configured shard layout; non-empty decodes via decodeVStreamPos.
func (r *vstreamCDCReader) resolveStartPosition(from ir.Position) ([]shardGtid, error) {
	decoded, ok, err := decodeVStreamPos(from)
	if err != nil {
		return nil, err
	}
	if ok {
		return decoded, nil
	}
	return fromNowVStreamPos(r.keyspace, r.shards), nil
}

// buildVStreamRequest assembles the gRPC request from the resolved
// position, the fixed flags v1 uses, and a wildcard table filter.
// The flags are conservative: REPLICA tablet (off the primary's
// hot path), MinimizeSkew for cleaner per-shard ordering,
// StopOnReshard so the reader sees a clean termination instead of
// a silently-rewritten shard layout, and a 5s heartbeat for
// liveness.
func (r *vstreamCDCReader) buildVStreamRequest(start []shardGtid) *vtgate.VStreamRequest {
	shardGtids := make([]*binlogdata.ShardGtid, len(start))
	for i, s := range start {
		shardGtids[i] = &binlogdata.ShardGtid{
			Keyspace: s.Keyspace,
			Shard:    s.Shard,
			Gtid:     s.Gtid,
		}
	}
	return &vtgate.VStreamRequest{
		TabletType: topodata.TabletType_REPLICA,
		Vgtid:      &binlogdata.VGtid{ShardGtids: shardGtids},
		Filter: &binlogdata.Filter{Rules: []*binlogdata.Rule{
			// "/.*/" matches every table in the keyspace. Refining
			// to specific tables is a future enhancement once the
			// IR carries a per-stream table allowlist.
			{Match: "/.*/"},
		}},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew:      true,
			StopOnReshard:     true,
			HeartbeatInterval: 5,
		},
	}
}

// pump owns out and closes it before returning. Errors from the
// gRPC stream get stored via setErr; clean ctx-cancellation just
// closes the channel with no error.
func (r *vstreamCDCReader) pump(ctx context.Context, stream vtgateservice.Vitess_VStreamClient, out chan<- ir.Change) {
	defer close(out)

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			r.setErr(fmt.Errorf("mysql/vstream: recv: %w", err))
			return
		}
		for _, ev := range resp.GetEvents() {
			if err := r.dispatch(ctx, ev, out); err != nil {
				r.setErr(err)
				return
			}
		}
	}
}

// dispatch routes a single VEvent. Most types either update reader
// state (FIELD, VGTID) or are surfaced as ir.Change events (ROW).
// Bookkeeping events (BEGIN, COMMIT, HEARTBEAT, etc.) flow through
// quietly. The DDL branch is intentionally minimal in Phase B —
// Phase C wires in TRUNCATE detection and cache invalidation.
func (r *vstreamCDCReader) dispatch(ctx context.Context, ev *binlogdata.VEvent, out chan<- ir.Change) error {
	switch ev.GetType() {
	case binlogdata.VEventType_FIELD:
		fe := ev.GetFieldEvent()
		if fe == nil {
			return nil
		}
		key := fieldCacheKey(fe.GetShard(), fe.GetTableName())
		r.fields[key] = fe.GetFields()
		return nil

	case binlogdata.VEventType_ROW:
		return r.dispatchRow(ctx, ev, out)

	case binlogdata.VEventType_VGTID:
		vg := ev.GetVgtid()
		if vg == nil {
			return nil
		}
		r.currentVgtid = vgtidToShardGtidSlice(vg)
		return nil

	case binlogdata.VEventType_DDL:
		// Phase C: TRUNCATE detection + per-table cache invalidation.
		// Conservative blanket invalidation here so the cache stays
		// honest while Phase C is in flight.
		clear(r.fields)
		return nil

	case binlogdata.VEventType_JOURNAL:
		// With StopOnReshard the stream terminates after this; the
		// outer Recv loop will see EOF and exit. Reshard handling
		// is its own follow-up chunk; surface as an error so the
		// caller knows to rediscover the shard layout and re-open.
		return errors.New("mysql/vstream: shard layout changed (journal event); caller must reopen with new shards")

	case binlogdata.VEventType_BEGIN,
		binlogdata.VEventType_COMMIT,
		binlogdata.VEventType_HEARTBEAT,
		binlogdata.VEventType_GTID,
		binlogdata.VEventType_OTHER,
		binlogdata.VEventType_VERSION,
		binlogdata.VEventType_LASTPK,
		binlogdata.VEventType_SAVEPOINT,
		binlogdata.VEventType_ROLLBACK:
		return nil

	default:
		// COPY_COMPLETED, statement-level INSERT/UPDATE/DELETE
		// (which we don't get because Filter rules above ask for
		// row-format), SET, and any future event types fall here
		// silently. Logging is the caller's responsibility.
		return nil
	}
}

// dispatchRow turns a ROW event's RowChanges into per-row
// [ir.Insert] / [ir.Update] / [ir.Delete] events. The After/Before
// pair distinguishes the three: After && !Before == Insert,
// Before && After == Update, Before && !After == Delete.
func (r *vstreamCDCReader) dispatchRow(ctx context.Context, ev *binlogdata.VEvent, out chan<- ir.Change) error {
	rev := ev.GetRowEvent()
	if rev == nil {
		return nil
	}
	key := fieldCacheKey(rev.GetShard(), rev.GetTableName())
	fields, ok := r.fields[key]
	if !ok {
		return fmt.Errorf("mysql/vstream: row event for %q without preceding FIELD event", key)
	}

	pos, err := r.positionFor()
	if err != nil {
		return err
	}

	tableName := stripKeyspaceFromTable(rev.GetTableName(), rev.GetKeyspace())

	for _, rc := range rev.GetRowChanges() {
		before, beforeOK := decodeVStreamRow(rc.GetBefore(), fields)
		after, afterOK := decodeVStreamRow(rc.GetAfter(), fields)
		switch {
		case afterOK && !beforeOK:
			if err := send(ctx, out, ir.Insert{
				Position: pos,
				Schema:   rev.GetKeyspace(),
				Table:    tableName,
				Row:      after,
			}); err != nil {
				return err
			}
		case beforeOK && afterOK:
			if err := send(ctx, out, ir.Update{
				Position: pos,
				Schema:   rev.GetKeyspace(),
				Table:    tableName,
				Before:   before,
				After:    after,
			}); err != nil {
				return err
			}
		case beforeOK && !afterOK:
			if err := send(ctx, out, ir.Delete{
				Position: pos,
				Schema:   rev.GetKeyspace(),
				Table:    tableName,
				Before:   before,
			}); err != nil {
				return err
			}
		default:
			// Neither before nor after — malformed event, skip.
			continue
		}
	}
	return nil
}

// fieldCacheKey is the key shape used in r.fields. shard might be
// empty for unsharded keyspaces; tableName might be qualified
// ("keyspace.table") or unqualified depending on
// VStreamFlags.ExcludeKeyspaceFromTableName. Both shapes are
// addressable by this key — we just store under whatever the FIELD
// event used and look up the same way for ROW events.
func fieldCacheKey(shard, tableName string) string {
	if shard == "" {
		return tableName
	}
	return shard + "/" + tableName
}

// stripKeyspaceFromTable removes a leading "keyspace." prefix from
// table names when VStream included it. Sluice's IR contract for
// Insert.Table etc. uses the unqualified table name; the schema is
// carried separately in the Schema field.
func stripKeyspaceFromTable(tableName, keyspace string) string {
	if keyspace != "" && strings.HasPrefix(tableName, keyspace+".") {
		return strings.TrimPrefix(tableName, keyspace+".")
	}
	return tableName
}

// positionFor returns the IR position for the next emitted change.
// The reader's currentVgtid is updated on every VGTID event; this
// just encodes the current state.
func (r *vstreamCDCReader) positionFor() (ir.Position, error) {
	if len(r.currentVgtid) == 0 {
		return ir.Position{}, nil
	}
	return encodeVStreamPos(r.currentVgtid)
}

// vgtidToShardGtidSlice converts the proto VGtid to our domain
// type. The conversion is field-by-field; keeping the two types
// separate prevents the proto from leaking into the position
// encoding format that's persisted to the control table.
func vgtidToShardGtidSlice(vg *binlogdata.VGtid) []shardGtid {
	out := make([]shardGtid, 0, len(vg.GetShardGtids()))
	for _, sg := range vg.GetShardGtids() {
		out = append(out, shardGtid{
			Keyspace: sg.GetKeyspace(),
			Shard:    sg.GetShard(),
			Gtid:     sg.GetGtid(),
		})
	}
	return out
}

// decodeVStreamRow turns a query.Row + cached []*query.Field into
// an ir.Row. Returns ok=false when row is nil (the absent half of
// an Insert's Before or a Delete's After). Decoding goes through
// sqltypes.MakeTrusted for type-aware conversion (NULL detection,
// numeric vs textual representation), then maps the canonical Go
// values to sluice's IR contract:
//
//   - Signed integer-family types → int64
//   - Floating-point → float64
//   - NULL → nil
//   - Everything else → []byte (the raw Vitess wire form)
//
// The decoder is deliberately conservative in v1; type fidelity
// improvements (TINYINT(1)→bool for cross-engine MySQL→PG, JSON
// shape preservation, etc.) layer in as separate small chunks.
func decodeVStreamRow(row *query.Row, fields []*query.Field) (ir.Row, bool) {
	if row == nil {
		return nil, false
	}
	out := make(ir.Row, len(fields))
	values := row.GetValues()
	lengths := row.GetLengths()
	if len(lengths) != len(fields) {
		// Malformed event (length count != field count) — produce
		// an empty row so the caller surfaces the issue rather
		// than silently misaligning columns.
		return out, true
	}
	offset := 0
	for i, f := range fields {
		l := lengths[i]
		if l < 0 {
			out[f.GetName()] = nil
			continue
		}
		raw := values[offset : offset+int(l)]
		offset += int(l)
		out[f.GetName()] = decodeVStreamCell(f.GetType(), raw)
	}
	return out, true
}

// decodeVStreamCell maps a single Vitess-wire cell to its IR-Row
// canonical Go value. The switch covers the cases that matter for
// the cross-engine path; falls through to []byte for everything
// else so the consumer can at least see the bytes.
func decodeVStreamCell(t query.Type, raw []byte) any {
	v := sqltypes.MakeTrusted(t, raw)
	switch t {
	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64:
		n, err := v.ToInt64()
		if err != nil {
			return raw
		}
		return n
	case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
		n, err := v.ToUint64()
		if err != nil {
			return raw
		}
		return n
	case query.Type_FLOAT32, query.Type_FLOAT64:
		n, err := v.ToFloat64()
		if err != nil {
			return raw
		}
		return n
	case query.Type_VARCHAR, query.Type_TEXT, query.Type_CHAR:
		return v.ToString()
	case query.Type_NULL_TYPE:
		return nil
	}
	// VARBINARY, BLOB, JSON, GEOMETRY, BIT, ENUM, SET, dates, etc.:
	// hand back the raw bytes. Cross-engine paths rely on the
	// existing MySQL value-decoder shape (VARCHAR-mapped extension
	// types are already strings via the case above), and JSON is
	// passed-through as bytes by docs/value-types.md anyway.
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}
