// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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

	// snapshotSig is the per-table structural fingerprint of the
	// schema-history version last emitted as an [ir.SchemaSnapshot]
	// (ADR-0049 Chunk B2). Keyed by the same fieldCacheKey as fields.
	// It implements DP-1 sign-off point ii (true-delta only): VStream
	// re-emits a FIELD event on (re)start / per-table first-touch
	// *without* any DDL; a new schema-history version is emitted ONLY
	// when the projected (column-name, ordered-type) signature differs
	// from the one already snapshotted for that key. Absent key = no
	// version snapshotted yet for that table → the first real FIELD is
	// always a true delta.
	snapshotSig map[string]ir.SchemaSignature

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
// OpenCDCReader. It parses the standard MySQL DSN, builds a gRPC
// dial against the vtgate endpoint, and returns a reader ready for
// StreamChanges.
//
// DSN inputs:
//   - User / Passwd → basic-auth credentials (PlanetScale's service-
//     token name / value). Skipped when vstream_auth=none.
//   - Addr (host:port from the tcp(...) wrapper) → vtgate endpoint
//     host. Port defaults to 443; the full endpoint is overridable
//     via the `vstream_endpoint` DSN parameter.
//   - DBName → Vitess keyspace.
//
// DSN flags (all optional, all default to PlanetScale-friendly
// behaviour):
//   - vstream_endpoint=<host:port> — vtgate gRPC endpoint
//     override. Useful for self-hosted Vitess and vttestserver.
//   - vstream_transport={tls|plaintext} — default tls. Plaintext
//     opts out of TLS entirely; only useful for localhost
//     vttestserver / development setups.
//   - vstream_insecure_tls=true — keeps TLS but skips certificate
//     verification. Useful for self-signed certs in tests.
//   - vstream_auth={basic|none} — default basic. None skips the
//     Authorization header entirely; matches vanilla Vitess
//     deployments that don't authenticate VStream calls.
//   - vstream_shards=<comma-separated> — default "-". The
//     PlanetScale convention is "-" for an unsharded keyspace;
//     vttestserver uses "0" for the same case. Multi-shard
//     keyspaces list every shard ("-80,80-").
//   - vstream_auto_discover_shards=true — discover the keyspace's
//     shard layout at Open time via `SHOW VITESS_SHARDS LIKE
//     '<keyspace>/%'` against the standard MySQL endpoint.
//     Mutually exclusive with `vstream_shards`. Default false to
//     keep existing single-shard deployments working without
//     changes.
//
// Multi-shard sharded keyspaces are supported: enable
// auto-discovery (or list shards explicitly) and the receive path
// fans out per-shard cursor tracking through the `[]shardGtid`
// position. Reshard handling is detected via the typed
// [ErrShardLayoutChanged] error — see [vstreamCDCReader.dispatch]
// for the contract.
func openVStreamReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.DBName == "" {
		return nil, errors.New("mysql/vstream: DSN has no database name (vitess keyspace expected)")
	}

	endpoint, err := vstreamEndpointFromDSN(cfg)
	if err != nil {
		return nil, err
	}

	dialOpts, authHeader, err := vstreamDialOptions(cfg)
	if err != nil {
		return nil, err
	}

	shards, err := resolveVStreamShards(ctx, cfg)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(endpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("mysql/vstream: dial %s: %w", endpoint, err)
	}

	return &vstreamCDCReader{
		endpoint:    endpoint,
		authHeader:  authHeader,
		keyspace:    cfg.DBName,
		shards:      shards,
		conn:        conn,
		client:      vtgateservice.NewVitessClient(conn),
		fields:      make(map[string][]*query.Field),
		snapshotSig: make(map[string]ir.SchemaSignature),
	}, nil
}

// vstreamShardsFromDSN returns the shard layout the reader should
// stream. The DSN's `vstream_shards` parameter is a
// comma-separated list of shard names; missing or empty defaults
// to "-" (PlanetScale's convention for an unsharded keyspace).
// vttestserver uses "0" instead and needs the override.
//
// This is the explicit-only path; for the full open-time resolution
// (which includes the auto-discovery branch), see
// [resolveVStreamShards].
func vstreamShardsFromDSN(cfg *gomysql.Config) []string {
	v := cfg.Params["vstream_shards"]
	if v == "" {
		return []string{"-"}
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"-"}
	}
	return out
}

// resolveVStreamShards picks the shard layout for the reader at
// Open time. It applies the DSN policy:
//
//   - If `vstream_shards` is set: parse it (delegates to
//     [vstreamShardsFromDSN]).
//   - If `vstream_auto_discover_shards=true` AND `vstream_shards`
//     is unset: query the vtgate via [discoverShards] and use that
//     list.
//   - If both flags are set: error. They're contradictory and the
//     operator's intent is ambiguous.
//   - Default (neither set): the explicit-default path —
//     ["-"] (PlanetScale's unsharded convention). Backwards-
//     compatible with every existing caller.
func resolveVStreamShards(ctx context.Context, cfg *gomysql.Config) ([]string, error) {
	explicit := strings.TrimSpace(cfg.Params["vstream_shards"])
	autoDiscover := cfg.Params["vstream_auto_discover_shards"] == "true"

	if explicit != "" && autoDiscover {
		return nil, errors.New(
			"mysql/vstream: vstream_shards and vstream_auto_discover_shards=true are mutually exclusive; pick one",
		)
	}
	if !autoDiscover {
		return vstreamShardsFromDSN(cfg), nil
	}

	shards, err := discoverShards(ctx, cfg, cfg.DBName)
	if err != nil {
		return nil, fmt.Errorf("mysql/vstream: shard auto-discovery: %w", err)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("mysql/vstream: shard auto-discovery for keyspace %q returned no shards", cfg.DBName)
	}
	return shards, nil
}

// discoverShards queries the vtgate's MySQL frontend with
// `SHOW VITESS_SHARDS LIKE '<keyspace>/%'` and returns the shard
// names belonging to keyspace. The command is a Vitess-specific
// extension to standard MySQL: vtgate parses it, returns one row
// per shard with a single column whose value is `keyspace/shard`.
//
// keyspace is matched as a literal prefix; the `/%` LIKE pattern
// scopes the result to that keyspace alone (vtgate also serves
// system keyspaces, e.g. `_vt`, that we don't want to stream
// from).
//
// The returned slice is sorted by shard name to make the layout
// deterministic across calls — the position-encoder already
// canonicalises before persisting, but doing it here as well
// keeps log output and test assertions stable.
//
// Connectivity uses a short-lived [database/sql] handle bound to
// the same DSN-derived [gomysql.Config] the reader was opened
// with. The handle is closed before return; the function does
// not retain it.
func discoverShards(ctx context.Context, cfg *gomysql.Config, keyspace string) ([]string, error) {
	if keyspace == "" {
		return nil, errors.New("mysql/vstream: discoverShards: empty keyspace")
	}
	// Clone the Config and drop sluice's vstream_* DSN flags before
	// opening the connection: the go-sql-driver's session-init
	// emits the DSN's Params map as `SET <key>=<value>, ...`, and
	// vtgate's MySQL parser rejects unknown variable names with a
	// syntax error. (The flags are sluice-internal — they belong
	// in cfg.Params for the parser's eyes only, not in a SET
	// statement on the wire.)
	conn := cfg.Clone()
	if conn.Params != nil {
		for k := range conn.Params {
			if strings.HasPrefix(k, "vstream_") {
				delete(conn.Params, k)
			}
		}
	}
	db, err := openDB(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("open mysql for shard discovery: %w", err)
	}
	defer func() { _ = db.Close() }()

	pattern := keyspace + "/%"
	rows, err := db.QueryContext(ctx, "SHOW VITESS_SHARDS LIKE ?", pattern)
	if err != nil {
		return nil, fmt.Errorf("SHOW VITESS_SHARDS LIKE %q: %w", pattern, err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]string, 0, 4)
	for rows.Next() {
		var ksShard string
		if err := rows.Scan(&ksShard); err != nil {
			return nil, fmt.Errorf("scan shard row: %w", err)
		}
		// SHOW VITESS_SHARDS rows have the shape "keyspace/shard"; we
		// strip the keyspace prefix and validate the suffix is the
		// shard name we expected.
		idx := strings.IndexByte(ksShard, '/')
		if idx < 0 {
			return nil, fmt.Errorf("unexpected SHOW VITESS_SHARDS row %q (no '/' separator)", ksShard)
		}
		ks, shard := ksShard[:idx], ksShard[idx+1:]
		if ks != keyspace {
			// Defensive: the LIKE filter should have constrained
			// keyspace, but a vtgate quirk could surface a foreign
			// row; skip rather than fold it into our shard list.
			continue
		}
		if shard != "" {
			out = append(out, shard)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate shard rows: %w", err)
	}

	// Sort for deterministic output; vtgate doesn't guarantee any
	// particular order from SHOW VITESS_SHARDS.
	sort.Strings(out)
	return out, nil
}

// vstreamDialOptions builds the gRPC dial options from the DSN's
// transport/auth flags. Returned authHeader is the encoded value
// stashed on the reader for diagnostics; the gRPC layer attaches
// it via PerRPCCredentials when vstream_auth=basic.
func vstreamDialOptions(cfg *gomysql.Config) ([]grpc.DialOption, string, error) {
	transport := cfg.Params["vstream_transport"]
	authMode := cfg.Params["vstream_auth"]

	dialOpts := make([]grpc.DialOption, 0, 2)

	switch transport {
	case "", "tls":
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(credentials.NewTLS(vstreamTLSConfigFromDSN(cfg))))
	case "plaintext":
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	default:
		return nil, "", fmt.Errorf("mysql/vstream: unknown vstream_transport %q (want tls or plaintext)", transport)
	}

	var authHeader string
	switch authMode {
	case "", "basic":
		if cfg.User == "" {
			return nil, "", errors.New("mysql/vstream: DSN has no user (service-token name expected); set vstream_auth=none for unauthenticated Vitess setups")
		}
		if transport == "plaintext" {
			return nil, "", errors.New("mysql/vstream: vstream_auth=basic refuses to ride plaintext (gRPC RequireTransportSecurity); use vstream_auth=none if intentional")
		}
		authHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte(cfg.User+":"+cfg.Passwd))
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&vstreamBasicAuth{header: authHeader}))
	case "none":
		// Vanilla Vitess / vttestserver: no auth header.
	default:
		return nil, "", fmt.Errorf("mysql/vstream: unknown vstream_auth %q (want basic or none)", authMode)
	}

	return dialOpts, authHeader, nil
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
			r.setErr(classifyReaderError(fmt.Errorf("mysql/vstream: recv: %w", err)))
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
		return r.maybeSnapshotSchema(ctx, fe, out)

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
		return r.dispatchDDL(ctx, ev, out)

	case binlogdata.VEventType_JOURNAL:
		// With StopOnReshard the stream terminates after this; the
		// outer Recv loop will see EOF and exit. Surface a typed
		// error carrying the journal payload so the caller can
		// inspect the new shard layout and call Reopen to resume.
		return journalToShardLayoutErr(ev.GetJournal())

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

// dispatchDDL handles a DDL event. Mirrors the binlog reader's
// QueryEvent handling: try to parse the SQL as a TRUNCATE TABLE
// and, if it matches, emit an ir.Truncate. Either way, clear the
// field cache so the next ROW event triggers a fresh FIELD lookup
// — DDL might have changed the column shape.
//
// The parser (parseTruncateTable, package-level, shared with the
// binlog path) recognises the canonical
// `TRUNCATE [TABLE] [<schema>.]<table>` shapes and returns
// ok=false for anything else. Multi-table truncates, parenthesised
// arguments, etc. fall through to generic DDL handling — the
// operator loses the typed ir.Truncate event but keeps correctness
// (the cache invalidation still fires).
func (r *vstreamCDCReader) dispatchDDL(ctx context.Context, ev *binlogdata.VEvent, out chan<- ir.Change) error {
	stmt := ev.GetStatement()
	if stmt == "" {
		clear(r.fields)
		return nil
	}

	if truncSchema, truncTable, ok := parseTruncateTable(stmt); ok {
		// VStream's DDL events carry the keyspace on the parent
		// event; an unqualified TRUNCATE inherits that as its
		// implicit schema (matches MySQL's USE-context semantics
		// from the binlog path).
		if truncSchema == "" {
			truncSchema = ev.GetKeyspace()
		}
		// VStream events come with the source's keyspace prefix
		// already (e.g., "test.users") in some configurations —
		// strip it before emitting if it duplicates the keyspace
		// we resolved above.
		truncTable = stripKeyspaceFromTable(truncTable, truncSchema)

		// The reader is bound to a single keyspace via the DSN's
		// DBName. Emit only when the truncate falls inside it; a
		// truncate of an unrelated keyspace is silently dropped
		// (the change-applier on the target side wouldn't know
		// what to do with it anyway).
		if truncSchema == r.keyspace {
			pos, err := r.positionFor()
			if err != nil {
				return err
			}
			if err := send(ctx, out, ir.Truncate{
				Position: pos,
				Schema:   truncSchema,
				Table:    truncTable,
			}); err != nil {
				return err
			}
		}
		// TRUNCATE resets the auto-increment counter; fall
		// through to the generic invalidation below so the field
		// cache stays honest with whatever the next FIELD event
		// reports.
	}

	clear(r.fields)
	return nil
}

// maybeSnapshotSchema is the ADR-0049 Chunk B2 boundary path. On
// every FIELD event it projects the just-cached field metadata into an
// [ir.Table] (the in-stream position-anchored snapshot — NEVER a fresh
// information_schema re-introspection, locked decision #2) and emits an
// [ir.SchemaSnapshot] iff the projected (column-name, ordered-type)
// signature differs from the one already snapshotted for this table
// (locked DP-1 sign-off point ii: true-delta only — VStream re-emits
// FIELD on (re)start / first-touch and a naive "an event arrived"
// trigger would bloat the history with no-op versions and break DP-2's
// retention ∝ DDL-count assumption).
//
// The anchor is the FIELD event's OWN position — r.currentVgtid as of
// FIELD detection, BEFORE any post-DDL ROW advances it (locked
// decision #4c: the boundary event's own position captured at
// detection, never the first subsequent row's; else a replayed event
// between the DDL and the first post-DDL row resolves to the pre-DDL
// schema — the exact silent-mis-decode this ADR kills).
//
// Ordering vs the loud floor: the SchemaSnapshot is emitted on the
// FIELD event itself, strictly before the first post-FIELD ROW for
// the table, so the existing "row event for %q without preceding
// FIELD event" hard error (dispatchRow) is untouched — this path adds
// a durable version write ahead of the rows, it never swallows or
// reorders the floor.
//
// A projection failure is fatal to the stream (returned as an error
// → the pump's setErr → stream stops loudly): a column whose
// ColumnType can't be mapped must not be silently dropped from a
// persisted schema-history version (locked decision #4b; the
// loud-failure tenet).
func (r *vstreamCDCReader) maybeSnapshotSchema(ctx context.Context, fe *binlogdata.FieldEvent, out chan<- ir.Change) error {
	keyspace := fe.GetKeyspace()
	if keyspace == "" {
		keyspace = r.keyspace
	}
	// The reader is bound to a single keyspace via the DSN; a FIELD
	// event for an unrelated keyspace carries no table the applier
	// could host a schema-history row for. Skip — symmetric with
	// dispatchDDL's keyspace gate.
	if keyspace != r.keyspace {
		return nil
	}
	table := stripKeyspaceFromTable(fe.GetTableName(), keyspace)

	tbl, err := projectVStreamFields(keyspace, table, fe.GetFields())
	if err != nil {
		if errors.Is(err, errFieldMetadataUnavailable) {
			// Position-anchored metadata absent on this FIELD event —
			// degrade to the pre-ADR-0049 safe floor (no version
			// written → a later resume across a real DDL on this table
			// falls back to ir.ErrPositionInvalid → ADR-0022
			// cold-start). NOT fatal: the loud ROW-without-FIELD floor
			// (dispatchRow) is untouched, and halting an otherwise-
			// healthy stream over a minimal FIELD shape the decode
			// path already tolerates would be an availability
			// regression with no correctness gain. See
			// errFieldMetadataUnavailable's LEAD-REVIEW note.
			return nil
		}
		// A present-but-unmappable ColumnType is a genuine unknown
		// type — fatal/loud (#4b; the loud-failure tenet).
		return err
	}

	cacheKey := fieldCacheKey(fe.GetShard(), fe.GetTableName())
	sig := ir.SchemaSignatureOf(tbl)
	if prev, ok := r.snapshotSig[cacheKey]; ok && prev.Equal(sig) {
		// No-op FIELD re-emit (restart / first-touch / reconnect with
		// no DDL): not a true delta — do NOT write a new version.
		return nil
	}

	pos, err := r.positionFor()
	if err != nil {
		return err
	}

	if err := send(ctx, out, ir.SchemaSnapshot{
		Position: pos,
		Schema:   keyspace,
		Table:    table,
		IR:       tbl,
	}); err != nil {
		return err
	}
	r.snapshotSig[cacheKey] = sig
	return nil
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
// an Insert's Before or a Delete's After). Decoding follows the
// same IR-canonical Go-value contract documented in
// docs/value-types.md, so cross-engine MySQL→PG paths behave
// identically whether changes flow through the binlog reader or
// the VStream reader.
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
		out[f.GetName()] = decodeVStreamCell(f, raw)
	}
	return out, true
}

// decodeVStreamCell maps a single Vitess-wire cell to its IR-Row
// canonical Go value. The mapping mirrors the binlog reader's
// value contract (docs/value-types.md) so cross-engine work can
// rely on identical Go shapes from either CDC path:
//
//   - INT8 with column_type "tinyint(1)" → bool (the cross-engine
//     MySQL→PG bool path; TINYINT(1) is the canonical MySQL bool).
//   - Other signed integer families → int64.
//   - Unsigned integer families → uint64.
//   - FLOAT32 / FLOAT64 → float64.
//   - DECIMAL → string (NUMERIC stays textual to preserve precision
//     past float64's range; matches the binlog reader's contract).
//   - VARCHAR / TEXT / CHAR / ENUM → string.
//   - SET → []string (split on comma; empty SET gives an empty
//     slice, not nil, matching docs/value-types.md).
//   - DATE / DATETIME / TIMESTAMP → time.Time, parsed from
//     Vitess's textual wire format. parseTime=true on the
//     standalone driver path produces time.Time directly; VStream
//     hands us bytes, so we parse here.
//   - TIME → string (HH:MM:SS textual; matches the binlog reader).
//   - JSON → []byte (binary; the IR contract for JSON is "bytes
//     of the JSON document", consumers parse as needed).
//   - VARBINARY / BINARY / BLOB / BIT / GEOMETRY → []byte.
//   - NULL_TYPE → nil.
//   - Everything else → []byte fallback so the consumer at least
//     sees the bytes when a future Vitess release adds a type the
//     IR doesn't yet model.
func decodeVStreamCell(field *query.Field, raw []byte) any {
	t := field.GetType()
	v := sqltypes.MakeTrusted(t, raw)
	switch t {
	case query.Type_INT8:
		n, err := v.ToInt64()
		if err != nil {
			return copyBytes(raw)
		}
		// TINYINT(1) is MySQL's canonical bool. Detect via the
		// column_type string ("tinyint(1)" or "tinyint(1) unsigned");
		// the proto Type alone collapses TINYINT and TINYINT(1) into
		// INT8 / UINT8, so we'd lose the distinction without this.
		if isMySQLBoolColumnType(field.GetColumnType()) {
			return n != 0
		}
		return n
	case query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64:
		n, err := v.ToInt64()
		if err != nil {
			return copyBytes(raw)
		}
		return n
	case query.Type_UINT8:
		n, err := v.ToUint64()
		if err != nil {
			return copyBytes(raw)
		}
		// TINYINT(1) UNSIGNED is also a bool by MySQL convention.
		if isMySQLBoolColumnType(field.GetColumnType()) {
			return n != 0
		}
		return n
	case query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
		n, err := v.ToUint64()
		if err != nil {
			return copyBytes(raw)
		}
		return n
	case query.Type_FLOAT32, query.Type_FLOAT64:
		n, err := v.ToFloat64()
		if err != nil {
			return copyBytes(raw)
		}
		return n
	case query.Type_DECIMAL:
		// NUMERIC stays textual — float64 round-trips lose precision
		// past 15 digits, and the IR contract says string.
		return v.ToString()
	case query.Type_VARCHAR, query.Type_TEXT, query.Type_CHAR, query.Type_ENUM:
		return v.ToString()
	case query.Type_SET:
		s := v.ToString()
		if s == "" {
			return []string{}
		}
		return strings.Split(s, ",")
	case query.Type_DATE, query.Type_DATETIME, query.Type_TIMESTAMP:
		t, err := parseVStreamDateTime(t, raw)
		if err != nil {
			// Malformed date — surface bytes so the consumer
			// notices rather than silently misinterpreting.
			return copyBytes(raw)
		}
		return t
	case query.Type_TIME:
		// HH:MM:SS[.fffff] textual. The binlog reader returns the
		// same string shape; matching here keeps cross-engine
		// time-only columns consistent.
		return v.ToString()
	case query.Type_JSON, query.Type_BLOB, query.Type_VARBINARY,
		query.Type_BINARY, query.Type_BIT:
		return copyBytes(raw)
	case query.Type_GEOMETRY:
		// Bug 27: VStream delivers spatial values in MySQL's on-wire
		// geometry format — `<srid uint32 LE><wkb>`. The IR contract
		// for ir.Geometry values is "raw WKB" (per docs/value-types.md),
		// matching the standard MySQL row-decoder path
		// (decodeMySQLGeometry). Strip the 4-byte SRID prefix so
		// downstream consumers (most importantly the PG row writer's
		// EWKB framing) see the same WKB bytes regardless of which
		// MySQL CDC source delivered them. SRID is intentionally
		// dropped here — per-column SRID lives on the IR's
		// ir.Geometry.SRID and is set at schema-translation time.
		// Malformed-prefix payloads (under 5 bytes) fall through with
		// raw bytes copied; the downstream WKB validator surfaces a
		// clearer error than a silent re-shape would.
		if len(raw) >= 5 {
			return copyBytes(raw[4:])
		}
		return copyBytes(raw)
	case query.Type_NULL_TYPE:
		return nil
	}
	// Unknown type — pass bytes so the consumer at least sees
	// something. Future Vitess releases may add types the IR
	// doesn't yet model.
	return copyBytes(raw)
}

// isMySQLBoolColumnType returns true when the field's MySQL
// column_type string identifies TINYINT(1) (the canonical MySQL
// bool). Both signed and unsigned variants are accepted.
//
// VStream's FieldEvent populates ColumnType with the source's DDL
// string ("tinyint(1)", "tinyint(1) unsigned", "tinyint", etc.).
// Vitess's proto Type alone collapses TINYINT and TINYINT(1) into
// INT8/UINT8, so the column_type string is the only place the
// display-width-1 distinction survives over the wire.
func isMySQLBoolColumnType(columnType string) bool {
	s := strings.ToLower(columnType)
	return strings.HasPrefix(s, "tinyint(1)")
}

// parseVStreamDateTime parses a DATE / DATETIME / TIMESTAMP cell
// from its Vitess wire form (textual) into a time.Time. Vitess's
// canonical formats:
//
//	DATE      "YYYY-MM-DD"
//	DATETIME  "YYYY-MM-DD HH:MM:SS[.fffffffff]"
//	TIMESTAMP "YYYY-MM-DD HH:MM:SS[.fffffffff]"
//
// All three are parsed in UTC unless the source carries a zone (it
// doesn't — MySQL DATETIME/TIMESTAMP wire values are zone-agnostic).
func parseVStreamDateTime(t query.Type, raw []byte) (time.Time, error) {
	s := string(raw)
	if t == query.Type_DATE {
		return time.Parse("2006-01-02", s)
	}
	// DATETIME / TIMESTAMP — try the precision-bearing format first,
	// fall back to the no-fraction form. time.Parse is strict on
	// the literal layout so probing both shapes is necessary.
	if v, err := time.Parse("2006-01-02 15:04:05.999999999", s); err == nil {
		return v, nil
	}
	return time.Parse("2006-01-02 15:04:05", s)
}

// copyBytes returns a fresh []byte with the same contents as raw.
// VStream's underlying gRPC buffer may be reused across messages,
// so cells that we hand to consumers as []byte must be copied —
// otherwise the consumer's value silently mutates on the next
// stream Recv.
func copyBytes(raw []byte) []byte {
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// ShardLayoutChangedError is returned by [vstreamCDCReader] when
// vtgate emits a JOURNAL VEvent — Vitess's signal that the source
// keyspace's shard layout is being replaced (a reshard split,
// merge, or move). With StopOnReshard:true (the flag the reader
// sets), the gRPC stream terminates immediately after the journal
// commits, so no further events arrive on the original shards.
//
// The error carries the journal's payload so the caller can:
//
//   - Log which shards were participating ([Participants]).
//   - Resume from the new layout via [vstreamCDCReader.Reopen],
//     which seeds a fresh stream from [NewShards] using the GTIDs
//     vtgate stamped on each entry.
//
// The reader does NOT auto-reopen: that's a policy decision
// (whether to retry, how long to wait, whether to alert). The
// caller — typically [pipeline.Streamer]'s outer loop — owns the
// retry semantics. Detect via [errors.Is] (or a type assertion
// for the payload).
//
// The position-token format on persistence is unchanged; the
// reader simply emits a new vgtid covering the new shard set
// once Reopen succeeds, and subsequent ir.Change events carry
// positions that decode to the new []shardGtid shape.
type ShardLayoutChangedError struct {
	// Keyspace is the keyspace the journal applies to. Sluice
	// readers are bound to one keyspace via the DSN, so this is
	// always the reader's own keyspace; surfacing it makes the
	// error self-describing without forcing the caller to remember
	// context.
	Keyspace string

	// Participants is the list of (keyspace, shard) tuples that
	// were streaming when the journal committed — the shards being
	// retired. Useful for logging and for sanity-checking that the
	// reader was streaming the shards the journal expected to
	// replace.
	Participants []shardGtid

	// NewShards is the list of (keyspace, shard, gtid) tuples for
	// the post-reshard layout. The Gtid on each entry is the
	// position vtgate stamped at the journal commit, so a stream
	// opened against this slice resumes exactly at the seam — no
	// gap, no overlap.
	NewShards []shardGtid
}

// Error implements error. The message is intentionally terse;
// detailed shard listings are available on the typed fields.
func (e *ShardLayoutChangedError) Error() string {
	return fmt.Sprintf(
		"mysql/vstream: shard layout changed for keyspace %q (was %d shard(s), now %d); reopen required",
		e.Keyspace, len(e.Participants), len(e.NewShards),
	)
}

// ErrShardLayoutChanged is the sentinel paired with
// [ShardLayoutChangedError] for [errors.Is] checks. The concrete
// error type carries the new layout; the sentinel is the contract
// boundary (so callers don't depend on the struct shape just to
// detect the case).
var ErrShardLayoutChanged = errors.New("mysql/vstream: shard layout changed")

// Is implements the [errors.Is] hook so the typed error matches
// the sentinel.
func (e *ShardLayoutChangedError) Is(target error) bool {
	return target == ErrShardLayoutChanged
}

// journalToShardLayoutErr converts a Vitess Journal proto into the
// typed sluice error. Returns the sentinel on a nil journal so the
// dispatcher's contract — "JOURNAL → error" — stays unconditional;
// in practice vtgate always populates the field, but defending
// against nil keeps the dispatch loop bullet-proof.
func journalToShardLayoutErr(j *binlogdata.Journal) error {
	if j == nil {
		return ErrShardLayoutChanged
	}
	out := &ShardLayoutChangedError{}

	for _, p := range j.GetParticipants() {
		out.Keyspace = p.GetKeyspace()
		out.Participants = append(out.Participants, shardGtid{
			Keyspace: p.GetKeyspace(),
			Shard:    p.GetShard(),
		})
	}
	for _, sg := range j.GetShardGtids() {
		out.NewShards = append(out.NewShards, shardGtid{
			Keyspace: sg.GetKeyspace(),
			Shard:    sg.GetShard(),
			Gtid:     sg.GetGtid(),
		})
		// Backfill keyspace from the new-shards list when the
		// journal carries no participants (rare; defensive).
		if out.Keyspace == "" {
			out.Keyspace = sg.GetKeyspace()
		}
	}
	return out
}

// Reopen builds a fresh stream against the post-reshard shard
// layout described by [ShardLayoutChangedError]. The underlying
// gRPC connection is reused — only the stream is replaced — so
// the typical use pattern is:
//
//	changes, err := rdr.StreamChanges(ctx, pos)
//	for {
//	    select {
//	    case ev, ok := <-changes:
//	        if !ok {
//	            var resh *mysql.ShardLayoutChangedError
//	            if errors.As(rdr.Err(), &resh) {
//	                changes, err = rdr.Reopen(ctx, resh)
//	                if err != nil { ... }
//	                continue
//	            }
//	            return rdr.Err()
//	        }
//	        // handle ev
//	    }
//	}
//
// On success the reader's currentVgtid is replaced with the new
// layout, the field cache is cleared (post-reshard tablets emit
// fresh FIELD events), and a new ir.Change channel is returned.
// The previous channel (already closed when the caller saw the
// error) is no longer used.
//
// Reopen is intentionally a separate method from StreamChanges:
// the latter is the public CDCReader entry point that decodes a
// caller-supplied [ir.Position]; Reopen takes the typed error
// directly so the GTIDs don't have to round-trip through the
// position layer (which canonicalises and could lose information
// in a future format revision).
func (r *vstreamCDCReader) Reopen(ctx context.Context, resh *ShardLayoutChangedError) (<-chan ir.Change, error) {
	if err := r.applyReshardState(resh); err != nil {
		return nil, err
	}

	req := r.buildVStreamRequest(r.currentVgtid)
	loopCtx, cancel := context.WithCancel(ctx)
	r.streamerCancel = cancel

	stream, err := r.client.VStream(loopCtx, req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("mysql/vstream: reopen: open stream: %w", err)
	}

	out := make(chan ir.Change, vstreamChannelBuffer)
	go r.pump(loopCtx, stream, out)
	return out, nil
}

// applyReshardState mutates the reader to match the post-reshard
// layout: cancels any in-flight stream, clears the field cache,
// and replaces the shard list and currentVgtid. Lifted out of
// Reopen so the state transition is independently unit-testable
// without needing a live gRPC client.
func (r *vstreamCDCReader) applyReshardState(resh *ShardLayoutChangedError) error {
	if resh == nil {
		return errors.New("mysql/vstream: Reopen: nil ShardLayoutChangedError")
	}
	if len(resh.NewShards) == 0 {
		return errors.New("mysql/vstream: Reopen: no new shards in journal")
	}

	// Cancel the previous streamer goroutine — Reopen is the
	// transition point between the old layout and the new, and
	// holding two streams open against the same keyspace would
	// confuse the position bookkeeping.
	if r.streamerCancel != nil {
		r.streamerCancel()
		r.streamerCancel = nil
	}

	// Reset error state so the caller sees a clean slate after a
	// successful reopen. The reshard error itself was already
	// observed via Err(); leaving it cached would mask any future
	// genuine failure on the new stream.
	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	// New layout becomes the reader's authoritative shard set.
	r.shards = make([]string, 0, len(resh.NewShards))
	for _, s := range resh.NewShards {
		r.shards = append(r.shards, s.Shard)
	}
	r.currentVgtid = make([]shardGtid, len(resh.NewShards))
	copy(r.currentVgtid, resh.NewShards)

	// Field cache is keyed by (shard, table); the new tablets emit
	// fresh FIELD events so the old cache entries would be stale
	// at best, mis-aligned at worst.
	clear(r.fields)

	return nil
}
