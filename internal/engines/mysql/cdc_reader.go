package mysql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/orware/sluice/internal/ir"
)

// cdcChannelBuffer is the number of [ir.Change] events the CDC channel
// holds before producers block. Small enough that backpressure reaches
// the binlog syncer (which then stalls on the network read) within a
// few hundred events; large enough to absorb burst rates without
// hammering the consumer's loop body. Tunable later if real workloads
// reveal a sweet spot.
const cdcChannelBuffer = 256

// CDCReader streams MySQL row changes from the binlog as [ir.Change]
// events. It implements [ir.CDCReader] for the vanilla MySQL flavor;
// PlanetScale (which doesn't expose binlog) gets [ErrNotImplemented]
// from the engine.
//
// One reader → one [StreamChanges] call. Concurrent calls are not
// supported. The reader owns two distinct connections under the hood:
// a regular *sql.DB pool used for schema-cache refresh from
// information_schema, and a binlog client (replication.BinlogSyncer)
// used for the streaming protocol. Close releases both.
type CDCReader struct {
	// db is the standard database/sql handle used only for the schema
	// cache (information_schema queries) and for the initial mode
	// detection. The binlog connection is separate — it speaks a
	// different protocol and needs the REPLICATION SLAVE privilege.
	db *sql.DB

	// schema is the database name the reader is bound to. Events from
	// other databases are dropped during dispatch — this engine
	// presents a single-schema view, same as RowReader and SchemaReader.
	schema string

	// host and port are extracted from the DSN at construction time
	// and used to configure the binlog syncer's connection. Stored
	// separately because the syncer takes them as discrete fields
	// rather than a DSN string.
	host string
	port uint16

	// user and password are likewise extracted from the DSN. The
	// account needs REPLICATION SLAVE (and REPLICATION CLIENT, for
	// the SHOW MASTER STATUS / @@gtid_executed queries) at minimum.
	user, password string

	// serverID is the replica identifier the syncer registers with
	// the source. Must be unique across all replicas of the source —
	// a collision causes silent event loss. Generated from a hash of
	// host+pid+startNanos at construction time and surfaced in logs.
	serverID uint32

	// syncer is the binlog client. nil until StreamChanges starts the
	// stream; non-nil after that until Close.
	syncer *replication.BinlogSyncer

	// streamerCancel cancels the goroutine pumping events into the
	// out channel. Stored on the reader so Close can stop a stream
	// even when the caller's context isn't readily available.
	streamerCancel context.CancelFunc

	// tableMap is the transient binlog table_id → qualified-name map.
	// Repopulated on every TABLE_MAP_EVENT; entries are not durable
	// across server restarts and are not cached on disk. Empty string
	// is a sentinel meaning "this table_id refers to a table outside
	// our schema; drop its row events without further lookup".
	tableMap map[uint64]string

	// schemaCache holds the column lists the row decoder needs.
	// Populated lazily on first row event for a table and invalidated
	// wholesale on any DDL event. Re-reading on DDL is cheaper than
	// parsing the DDL string and avoids the regex-over-DDL antipattern
	// the project tenets call out.
	schemaCache map[string]*tableSchema

	// posMode and gtidSet track the current resume position. In GTID
	// mode, gtidSet accumulates committed GTIDs and is encoded into
	// each emitted Change. In file/pos mode, currentFile and the
	// per-event LogPos are encoded instead.
	posMode     positionMode
	gtidSet     mysql.GTIDSet
	currentFile string

	// mu guards err. The streaming goroutine writes; callers read via
	// [Err] after the channel closes.
	mu  sync.Mutex
	err error
}

// tableSchema is the slice of [ir.Table] the CDC dispatcher actually
// needs: the column list, in declaration order, with each column's IR
// type. Indexes and foreign keys are not consulted on a row event so
// we don't pay to load them on every cache miss.
type tableSchema struct {
	Schema  string
	Name    string
	Columns []*ir.Column
}

// Close releases the schema-DB pool and stops the binlog syncer if one
// is open. Safe to call multiple times.
func (r *CDCReader) Close() error {
	if r.streamerCancel != nil {
		r.streamerCancel()
		r.streamerCancel = nil
	}
	if r.syncer != nil {
		r.syncer.Close()
		r.syncer = nil
	}
	if r.db != nil {
		err := r.db.Close()
		r.db = nil
		return err
	}
	return nil
}

// Err returns the error, if any, that terminated the most recent
// streaming session. Only valid after the change channel has closed.
func (r *CDCReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// StreamChanges starts streaming binlog events from the given position,
// translating them to [ir.Change] events on the returned channel. Pass
// the zero value [ir.Position{}] to stream from the source's current
// position; pass a previously-emitted Position to resume.
//
// The channel is closed when ctx is cancelled, when the syncer
// terminates, or when a fatal error occurs (visible via [Err]). The
// caller should drain the channel or cancel ctx to avoid leaking the
// streaming goroutine.
func (r *CDCReader) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	if r.syncer != nil {
		return nil, errors.New("mysql: StreamChanges already in progress; construct a new reader for a second stream")
	}

	startPos, err := r.resolveStartPosition(ctx, from)
	if err != nil {
		return nil, err
	}
	r.posMode = startPos.Mode

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: r.serverID,
		Flavor:   mysql.MySQLFlavor,
		Host:     r.host,
		Port:     r.port,
		User:     r.user,
		Password: r.password,
		// The default ParseTime=false returns timestamps as a
		// replication.Time string; we'd rather decode lazily ourselves
		// inside decodeValue, so leave ParseTime alone.
	}
	r.syncer = replication.NewBinlogSyncer(syncerCfg)

	streamer, err := r.startStreamer(startPos)
	if err != nil {
		r.syncer.Close()
		r.syncer = nil
		return nil, fmt.Errorf("mysql: start binlog stream: %w", err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	r.streamerCancel = cancel
	out := make(chan ir.Change, cdcChannelBuffer)
	go r.pump(loopCtx, streamer, out)
	return out, nil
}

// resolveStartPosition turns the caller-supplied [ir.Position] into a
// concrete start position. An empty position triggers auto-detection
// based on the source's gtid_mode.
func (r *CDCReader) resolveStartPosition(ctx context.Context, from ir.Position) (binlogPos, error) {
	decoded, ok, err := decodeBinlogPos(from)
	if err != nil {
		return binlogPos{}, err
	}
	if ok {
		return decoded, nil
	}

	// Empty position: auto-detect mode and ask the source where it is.
	useGTID, err := gtidModeOn(ctx, r.db)
	if err != nil {
		return binlogPos{}, fmt.Errorf("mysql: detect gtid mode: %w", err)
	}
	if useGTID {
		set, err := executedGTIDSet(ctx, r.db)
		if err != nil {
			return binlogPos{}, fmt.Errorf("mysql: read @@gtid_executed: %w", err)
		}
		return binlogPos{Mode: positionModeGTID, GTIDSet: set}, nil
	}
	file, pos, err := masterStatus(ctx, r.db)
	if err != nil {
		return binlogPos{}, fmt.Errorf("mysql: SHOW MASTER STATUS: %w", err)
	}
	return binlogPos{Mode: positionModeFilePos, File: file, Pos: pos}, nil
}

// startStreamer hands the resolved position to the syncer. Initialises
// r.gtidSet (in GTID mode) or r.currentFile (in file/pos mode) so the
// per-event position emitter has something to anchor on.
func (r *CDCReader) startStreamer(p binlogPos) (*replication.BinlogStreamer, error) {
	switch p.Mode {
	case positionModeGTID:
		set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, p.GTIDSet)
		if err != nil {
			return nil, fmt.Errorf("parse gtid set %q: %w", p.GTIDSet, err)
		}
		r.gtidSet = set
		return r.syncer.StartSyncGTID(set)
	case positionModeFilePos:
		r.currentFile = p.File
		return r.syncer.StartSync(mysql.Position{Name: p.File, Pos: p.Pos})
	default:
		return nil, fmt.Errorf("unknown position mode %q", p.Mode)
	}
}

// pump is the event loop. It owns out and is responsible for closing
// it before returning. Errors are stored on the reader via setErr;
// callers see them via Err after the channel closes.
func (r *CDCReader) pump(ctx context.Context, streamer *replication.BinlogStreamer, out chan<- ir.Change) {
	defer close(out)

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			// Context cancellation is the orderly-shutdown path; not an
			// error worth recording.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			r.setErr(fmt.Errorf("mysql: cdc: get event: %w", err))
			return
		}
		if err := r.dispatch(ctx, ev, out); err != nil {
			r.setErr(err)
			return
		}
	}
}

// dispatch routes a single binlog event according to the table in
// docs/dev/notes/prep-mysql-cdc.md. Returns an error only for
// unrecoverable conditions; transient or unknown event types are
// quietly ignored.
func (r *CDCReader) dispatch(ctx context.Context, ev *replication.BinlogEvent, out chan<- ir.Change) error {
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		r.currentFile = string(e.NextLogName)
		return nil

	case *replication.GTIDEvent:
		// Append the new GTID to the running executed set. In file/pos
		// mode we don't maintain a set, so this is a no-op by structure.
		if r.posMode != positionModeGTID || r.gtidSet == nil {
			return nil
		}
		uuidStr, err := formatSIDAsUUID(e.SID)
		if err != nil {
			return fmt.Errorf("mysql: cdc: gtid sid: %w", err)
		}
		if err := r.gtidSet.Update(fmt.Sprintf("%s:%d", uuidStr, e.GNO)); err != nil {
			return fmt.Errorf("mysql: cdc: gtid update: %w", err)
		}
		return nil

	case *replication.TableMapEvent:
		schema := string(e.Schema)
		if schema != r.schema {
			// Mark the table_id as out-of-scope so subsequent row
			// events for it are dropped without a cache lookup.
			r.tableMap[e.TableID] = ""
			return nil
		}
		r.tableMap[e.TableID] = qualifiedName(schema, string(e.Table))
		return nil

	case *replication.RowsEvent:
		return r.dispatchRows(ctx, ev.Header, e, out)

	case *replication.QueryEvent:
		q := string(e.Query)
		if q == "BEGIN" || q == "COMMIT" {
			return nil
		}
		// Anything else is treated as DDL. Conservative blanket
		// invalidation: we'd rather over-invalidate than risk the
		// row decoder using a stale column list. The cost is one
		// information_schema query per affected table on the next
		// row event for that table.
		stmtSchema := string(e.Schema)
		if stmtSchema == r.schema || stmtSchema == "" {
			clear(r.schemaCache)
		}
		return nil

	case *replication.XIDEvent:
		// Transaction commit boundary in InnoDB. We don't emit
		// anything on commit today; future work (a "commit" hook for
		// position persistence) will live here.
		return nil

	default:
		// FORMAT_DESCRIPTION_EVENT, ROWS_QUERY_EVENT, and the various
		// MariaDB / payload-compressed events fall through silently.
		return nil
	}
}

// dispatchRows fans a row event out into per-row [ir.Change] values.
// WriteRows produces Inserts; UpdateRows produces Updates with the
// rows-event Before/After convention; DeleteRows produces Deletes.
func (r *CDCReader) dispatchRows(
	ctx context.Context,
	hdr *replication.EventHeader,
	ev *replication.RowsEvent,
	out chan<- ir.Change,
) error {
	qn, ok := r.tableMap[ev.TableID]
	if !ok {
		// Row event arrived before its TableMapEvent — should not
		// happen in a well-formed binlog, but bail out clearly if so.
		return fmt.Errorf("mysql: cdc: rows event for unknown table_id %d (no preceding TABLE_MAP_EVENT)", ev.TableID)
	}
	if qn == "" {
		// Out-of-scope schema; silently drop.
		return nil
	}

	tbl, err := r.tableFor(ctx, qn)
	if err != nil {
		return fmt.Errorf("mysql: cdc: load schema for %s: %w", qn, err)
	}

	pos, err := r.positionFor(hdr)
	if err != nil {
		return err
	}

	switch hdr.EventType {
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:
		for _, raw := range ev.Rows {
			row, err := decodeBinlogRow(raw, tbl.Columns)
			if err != nil {
				return fmt.Errorf("mysql: cdc: decode insert: %w", err)
			}
			if err := send(ctx, out, ir.Insert{
				Position: pos,
				Schema:   tbl.Schema,
				Table:    tbl.Name,
				Row:      row,
			}); err != nil {
				return err
			}
		}
	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		// MySQL emits update rows as alternating before/after pairs.
		// Defensive: if the count is odd, surface the malformed event
		// rather than silently dropping the trailing image.
		if len(ev.Rows)%2 != 0 {
			return fmt.Errorf("mysql: cdc: update rows event has odd row count %d", len(ev.Rows))
		}
		for i := 0; i < len(ev.Rows); i += 2 {
			before, err := decodeBinlogRow(ev.Rows[i], tbl.Columns)
			if err != nil {
				return fmt.Errorf("mysql: cdc: decode update before: %w", err)
			}
			after, err := decodeBinlogRow(ev.Rows[i+1], tbl.Columns)
			if err != nil {
				return fmt.Errorf("mysql: cdc: decode update after: %w", err)
			}
			if err := send(ctx, out, ir.Update{
				Position: pos,
				Schema:   tbl.Schema,
				Table:    tbl.Name,
				Before:   before,
				After:    after,
			}); err != nil {
				return err
			}
		}
	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		for _, raw := range ev.Rows {
			before, err := decodeBinlogRow(raw, tbl.Columns)
			if err != nil {
				return fmt.Errorf("mysql: cdc: decode delete: %w", err)
			}
			if err := send(ctx, out, ir.Delete{
				Position: pos,
				Schema:   tbl.Schema,
				Table:    tbl.Name,
				Before:   before,
			}); err != nil {
				return err
			}
		}
	default:
		// Other rows-flavoured events (PARTIAL_UPDATE_ROWS_EVENT,
		// MariaDB compressed variants) aren't in v1 scope. Surface as
		// debug-only by virtue of falling through with no emission.
		return nil
	}
	return nil
}

// tableFor returns the cached table schema for qn, loading it from
// information_schema on a cache miss. The IR contract for row events
// promises that the column list is the post-DDL view: MySQL serialises
// DDL relative to DML on the same table in the binlog, so the
// information_schema lookup at this point sees the correct shape.
func (r *CDCReader) tableFor(ctx context.Context, qn string) (*tableSchema, error) {
	if cached, ok := r.schemaCache[qn]; ok {
		return cached, nil
	}
	schema, table := splitQualified(qn)
	tbl, err := loadTableSchema(ctx, r.db, schema, table)
	if err != nil {
		return nil, err
	}
	r.schemaCache[qn] = tbl
	return tbl, nil
}

// positionFor builds the [ir.Position] to attach to events emitted from
// the given binlog event header. In GTID mode the bookmark is the
// running executed set; in file/pos mode it's (currentFile, LogPos).
func (r *CDCReader) positionFor(hdr *replication.EventHeader) (ir.Position, error) {
	switch r.posMode {
	case positionModeGTID:
		set := ""
		if r.gtidSet != nil {
			set = r.gtidSet.String()
		}
		return encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: set})
	case positionModeFilePos:
		return encodeBinlogPos(binlogPos{
			Mode: positionModeFilePos,
			File: r.currentFile,
			Pos:  hdr.LogPos,
		})
	default:
		return ir.Position{}, fmt.Errorf("mysql: cdc: position mode %q unset", r.posMode)
	}
}

// setErr stores the first error from the streaming goroutine.
// Subsequent calls are no-ops so the originating cause isn't masked.
func (r *CDCReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// send pushes c onto out, honouring ctx cancellation.
func send(ctx context.Context, out chan<- ir.Change, c ir.Change) error {
	select {
	case out <- c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// gtidModeOn queries the source's gtid_mode variable. ON, ON_PERMISSIVE,
// and (rarely) ON_PERMISSIVE_NEW_REPLICA all count as "GTID is in
// effect"; OFF and OFF_PERMISSIVE return false.
func gtidModeOn(ctx context.Context, db *sql.DB) (bool, error) {
	var name, value string
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'gtid_mode'").Scan(&name, &value)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "ON" || value == "ON_PERMISSIVE", nil
}

// executedGTIDSet returns the source's current @@gtid_executed.
func executedGTIDSet(ctx context.Context, db *sql.DB) (string, error) {
	var set string
	err := db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&set)
	if err != nil {
		return "", err
	}
	return set, nil
}

// masterStatus returns the source's current binlog file + position.
// Uses SHOW BINARY LOG STATUS first (MySQL 8.4+), falling back to
// SHOW MASTER STATUS for older versions where the new spelling
// doesn't exist yet.
func masterStatus(ctx context.Context, db *sql.DB) (file string, pos uint32, err error) {
	for _, q := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		file, pos, err = scanMasterStatus(ctx, db, q)
		if err == nil {
			return file, pos, nil
		}
	}
	return "", 0, err
}

// scanMasterStatus runs one of the master-status queries and pulls the
// first two columns (file, position) from the result. The query may
// return additional columns (Binlog_Do_DB, etc.) which we discard via
// a generic-scan trick: scan into a slice of *any sized to the column
// count, ignore everything past the first two.
func scanMasterStatus(ctx context.Context, db *sql.DB, q string) (file string, pos uint32, err error) {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", 0, err
		}
		return "", 0, errors.New("master status returned no rows (binlog disabled?)")
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	dest := make([]any, len(cols))
	holders := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}
	if err := rows.Scan(holders...); err != nil {
		return "", 0, err
	}
	f, ok := scanString(dest[0])
	if !ok {
		return "", 0, fmt.Errorf("master status: unexpected file type %T", dest[0])
	}
	p, ok := scanUint32(dest[1])
	if !ok {
		return "", 0, fmt.Errorf("master status: unexpected position type %T", dest[1])
	}
	return f, p, nil
}

func scanString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

func scanUint32(v any) (uint32, bool) {
	switch n := v.(type) {
	case int64:
		return uint32(n), true
	case uint64:
		return uint32(n), true
	case []byte:
		parsed, err := strconv.ParseUint(string(n), 10, 32)
		if err != nil {
			return 0, false
		}
		return uint32(parsed), true
	case string:
		parsed, err := strconv.ParseUint(n, 10, 32)
		if err != nil {
			return 0, false
		}
		return uint32(parsed), true
	}
	return 0, false
}

// formatSIDAsUUID renders a 16-byte SID from a GTID event into the
// canonical 8-4-4-4-12 hex form the GTIDSet parser accepts.
func formatSIDAsUUID(sid []byte) (string, error) {
	if len(sid) != 16 {
		return "", fmt.Errorf("gtid sid: want 16 bytes, got %d", len(sid))
	}
	const hex = "0123456789abcdef"
	out := make([]byte, 36)
	j := 0
	for i, b := range sid {
		switch i {
		case 4, 6, 8, 10:
			out[j] = '-'
			j++
		}
		out[j] = hex[b>>4]
		out[j+1] = hex[b&0x0f]
		j += 2
	}
	return string(out), nil
}

// generateServerID returns a uint32 server ID derived from the host,
// PID, and current time. The hash isn't cryptographically strong — its
// job is to keep concurrent sluice processes from picking the same ID
// against the same source, which is enough for the well-behaved-cluster
// case the project targets.
func generateServerID() uint32 {
	h := fnv.New32a()
	if hostname, err := os.Hostname(); err == nil {
		_, _ = h.Write([]byte(hostname))
	}
	var buf [16]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(os.Getpid()))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(time.Now().UnixNano()))
	_, _ = h.Write(buf[:12])
	id := h.Sum32()
	// Avoid the documented "reserved" zero ID; the binlog protocol
	// rejects it.
	if id == 0 {
		return 1
	}
	return id
}

// hostPortFromAddr splits an "addr" of the form "host:port" into its
// components. The MySQL driver normalises DSNs to that form via
// mysql.Config.Addr, so we can rely on net.SplitHostPort.
func hostPortFromAddr(addr string) (host string, port uint16, err error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("split host/port: %w", err)
	}
	parsed, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("parse port %q: %w", portStr, err)
	}
	return host, uint16(parsed), nil
}

// qualifiedName joins schema and table with a dot, mirroring the
// convention used for cache keys throughout this engine.
func qualifiedName(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// splitQualified is the inverse of qualifiedName.
func splitQualified(qn string) (schema, table string) {
	for i := 0; i < len(qn); i++ {
		if qn[i] == '.' {
			return qn[:i], qn[i+1:]
		}
	}
	return "", qn
}

// decodeBinlogRow maps a positional row from a RowsEvent (one entry
// per column, in column-declaration order) onto an [ir.Row] keyed by
// column name with values run through decodeValue.
func decodeBinlogRow(raw []any, cols []*ir.Column) (ir.Row, error) {
	if len(raw) != len(cols) {
		return nil, fmt.Errorf("row has %d values; schema has %d columns", len(raw), len(cols))
	}
	row := make(ir.Row, len(cols))
	for i, col := range cols {
		v, err := decodeValue(raw[i], col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		row[col.Name] = v
	}
	return row, nil
}
