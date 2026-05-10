// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
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

// defaultBinlogHeartbeatPeriod is the default cadence at which the
// source MySQL server is asked to send heartbeat events on an idle
// binlog connection. Without it (HeartbeatPeriod=0), the underlying
// TCP connection can silently stall on Docker / NAT setups when no
// row events flow for a few seconds — sluice cold-starts cleanly
// but never advances past the initial rotate event. Bug 12 in v0.4.0
// soak testing.
//
// 10 seconds matches go-mysql's documented "conventional" value and
// is well below MySQL's default replica-net-timeout (60s), so a
// stalled connection surfaces as a clean read error rather than a
// silent hang.
const defaultBinlogHeartbeatPeriod = 10 * time.Second

// noEventsGracePeriod is the wall-clock window the streaming
// goroutine waits for the first row event after CDC starts before
// emitting a "no events received" warning. The warning is purely
// diagnostic — long-idle source workloads are legitimate — but it
// surfaces the Bug 12 silent-stall surface promptly so an operator
// can investigate (binary logging disabled? REPLICATION SLAVE
// missing? heartbeat misconfigured?). 30 seconds gives enough
// breathing room that a normal startup never hits it.
const noEventsGracePeriod = 30 * time.Second

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

	// noEventsSuppress / noEventsSuppressOnce coordinate the Bug 12
	// startup watchdog. The watchdog goroutine listens on the
	// channel; the pump closes it (via the Once) the first time a
	// row-relevant event arrives. A timer expiry without the close
	// is the "stalled connection" surface and emits a diagnostic
	// WARN line. Allocated lazily in StreamChanges so the zero-
	// valued reader has nothing to leak.
	noEventsSuppress     chan struct{}
	noEventsSuppressOnce sync.Once
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
		// Non-zero HeartbeatPeriod is load-bearing on Docker /
		// NAT-y networks: with HeartbeatPeriod=0, the source never
		// sends keepalive events on an idle binlog connection and
		// the read can stall indefinitely. Bug 12 — observed on
		// localhost mysql:8.0 containers under Rancher Desktop on
		// Windows. The value matches go-mysql's documented
		// convention and is well under MySQL's default
		// replica-net-timeout, so a stalled connection now surfaces
		// as a clean read error rather than a silent hang.
		HeartbeatPeriod: defaultBinlogHeartbeatPeriod,
		// The default ParseTime=false returns timestamps as their
		// raw string form; decodeTime in value_decode.go parses
		// MySQL temporal strings into time.Time at row-decode time
		// (Bug 12, ADR follow-up).
		//
		// TimestampStringLocation is load-bearing for Bug 19. The
		// binlog wire format encodes TIMESTAMP as a UTC seconds-
		// since-epoch integer. go-mysql's decodeTimestamp2 builds
		// a fracTime via time.Unix(sec, ...) — the underlying
		// time.Time instant is correct, but its Location defaults
		// to time.Local. When fracTime.String() formats the value
		// without a TimestampStringLocation, it formats in the
		// process's local TZ. The string then flows into
		// decodeTime, which parses naked datetime strings as UTC,
		// silently shifting the value by the host's offset.
		// Pinning to time.UTC formats the value in UTC, matching
		// the decoder's contract. DATETIME isn't affected (its
		// binlog encoding is the broken-down date/time directly).
		TimestampStringLocation: time.UTC,
	}
	r.syncer = replication.NewBinlogSyncer(syncerCfg)

	streamer, err := r.startStreamer(startPos)
	if err != nil {
		r.syncer.Close()
		r.syncer = nil
		return nil, fmt.Errorf("mysql: start binlog stream: %w", err)
	}

	r.noEventsSuppress = make(chan struct{})
	r.noEventsSuppressOnce = sync.Once{}

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
		// Pre-flight: confirm the source still has the WAL/binlog
		// referenced by the persisted position (file/pos: file
		// present in SHOW BINARY LOGS; GTID: gtid_purged ⊆ resume
		// set). When the check fails, wrap with [ir.ErrPositionInvalid]
		// so the pipeline orchestrator falls through to cold-start
		// (ADR-0022). The wrap message stays engine-specific.
		if err := r.verifyPositionResumable(ctx, decoded); err != nil {
			return binlogPos{}, err
		}
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
//
// A startup grace-period watchdog fires once if no row events arrive
// within [noEventsGracePeriod] (Bug 12 diagnostic). The watchdog
// does NOT cancel the stream — long-idle source workloads are
// legitimate; it only surfaces a one-shot WARN line so the operator
// has a footprint to investigate ("is binary logging on? does the
// connecting role have REPLICATION SLAVE?"). Heartbeat events
// (from the source's HeartbeatPeriod ack) and rotate events do NOT
// count — only row-level events satisfy the watchdog.
func (r *CDCReader) pump(ctx context.Context, streamer *replication.BinlogStreamer, out chan<- ir.Change) {
	defer close(out)
	// Tear down the BinlogSyncer's internal goroutines (in particular
	// the retrySync loop in onStream) on pump exit. Without this, a
	// ctx cancellation only stops GetEvent — the BinlogSyncer's
	// reconnect loop keeps trying to dial the source until the
	// process exits. Under integration-job container pressure the
	// leaked goroutines pollute subsequent tests (TestMigrate_*
	// flakes during the v0.27.0 cycle were the symptom). Idempotent;
	// safe to call alongside any explicit Close() in the engine's
	// outer Close path. Unconditionally fires on pump exit (whether
	// via ctx cancellation or read error) so the leak class is
	// closed for both shutdown shapes.
	defer func() {
		if r.syncer != nil {
			r.syncer.Close()
		}
	}()

	pumpCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.startNoEventsWatchdog(pumpCtx)

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
		// Suppress the watchdog as soon as anything row-relevant
		// shows up. Heartbeat / format-description / rotate events
		// are book-keeping and don't count — the watchdog wants to
		// catch the "no rows ever delivered" Bug 12 surface
		// specifically.
		if isRowRelevantEvent(ev) {
			r.suppressNoEventsWatchdog()
		}
		if err := r.dispatch(ctx, ev, out); err != nil {
			r.setErr(err)
			return
		}
	}
}

// startNoEventsWatchdog runs a one-shot timer; if it expires before
// suppressNoEventsWatchdog is called, it emits a Bug-12-style WARN
// line. Cancellation via ctx is the clean-shutdown path; the
// suppress channel is the "row event seen, all good" path.
func (r *CDCReader) startNoEventsWatchdog(ctx context.Context) {
	t := time.NewTimer(noEventsGracePeriod)
	defer t.Stop()
	select {
	case <-r.noEventsSuppress:
		return
	case <-ctx.Done():
		return
	case <-t.C:
		slog.WarnContext(ctx, "mysql: cdc: no binlog row events received during startup grace period",
			slog.String("hint", "verify binary logging is enabled (log_bin=ON), the connecting role has REPLICATION SLAVE, and the source is producing changes; if running against a localhost docker container, see Bug 12 in the project changelog"),
		)
	}
}

// suppressNoEventsWatchdog signals the watchdog goroutine to exit
// without warning. Idempotent (closing a closed channel would panic;
// we use a once-style guarded close).
func (r *CDCReader) suppressNoEventsWatchdog() {
	r.noEventsSuppressOnce.Do(func() { close(r.noEventsSuppress) })
}

// isRowRelevantEvent reports whether a binlog event represents
// real DML or DDL traffic (the events the watchdog is watching for).
// Heartbeat, format-description, and rotate events are bookkeeping
// and don't count as "the source is producing changes".
func isRowRelevantEvent(ev *replication.BinlogEvent) bool {
	if ev == nil {
		return false
	}
	switch ev.Event.(type) {
	case *replication.RowsEvent, *replication.QueryEvent, *replication.GTIDEvent, *replication.XIDEvent:
		return true
	}
	return false
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
		if q == "BEGIN" {
			// Source-tx start: surface the boundary so the batched
			// applier can flush a previous in-flight batch and open
			// a target tx aligned to this source transaction.
			// Per-change appliers treat this as a no-op. See
			// ADR-0027. (MySQL's binlog does not emit a separate
			// COMMIT QueryEvent when GTIDs / XIDs are in use; the
			// XIDEvent below is the canonical commit boundary for
			// InnoDB transactions. The COMMIT QueryEvent only
			// appears on non-transactional storage engines, which
			// sluice doesn't target.)
			pos, err := r.positionFor(ev.Header)
			if err != nil {
				return err
			}
			return send(ctx, out, ir.TxBegin{Position: pos})
		}
		if q == "COMMIT" {
			// Defensive: emit TxCommit on the rare COMMIT-as-
			// QueryEvent surface (non-InnoDB storage engines).
			// Sluice doesn't formally support those, but if one
			// shows up we'd rather flush than silently drop the
			// boundary signal.
			pos, err := r.positionFor(ev.Header)
			if err != nil {
				return err
			}
			return send(ctx, out, ir.TxCommit{Position: pos})
		}
		// TRUNCATE TABLE arrives as a QUERY_EVENT carrying the SQL
		// text. The IR has ir.Truncate; PG's pgoutput emits typed
		// truncate messages directly, but on MySQL the only way to
		// recognise truncates is parsing the query string. The
		// parser is narrow (TRUNCATE [TABLE] [<schema>.]<table>);
		// out-of-shape forms (multi-table truncate, etc.) fall
		// through to generic DDL handling.
		if truncSchema, truncTable, ok := parseTruncateTable(q); ok {
			// The parsed schema is empty when the source DDL didn't
			// qualify it; default to the QueryEvent's schema (the
			// session's USE-context).
			if truncSchema == "" {
				truncSchema = string(e.Schema)
			}
			if truncSchema == r.schema {
				pos, err := r.positionFor(ev.Header)
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
			// Truncate also resets the auto-increment counter; fall
			// through to the generic invalidation below so the
			// schema cache stays honest.
		}
		// Generic DDL: conservative blanket cache invalidation.
		// We'd rather over-invalidate than risk the row decoder
		// using a stale column list.
		stmtSchema := string(e.Schema)
		if stmtSchema == r.schema || stmtSchema == "" {
			clear(r.schemaCache)
		}
		return nil

	case *replication.XIDEvent:
		// InnoDB transaction commit boundary. Surface as TxCommit so
		// the batched applier can flush the in-flight target tx in
		// one shot. The empty-source-tx case (BEGIN → XID with no
		// row events between, e.g. an aborted-but-still-recorded
		// transaction) is handled by the applier's flush path, which
		// skips when no rows have accumulated. See ADR-0027.
		pos, err := r.positionFor(ev.Header)
		if err != nil {
			return err
		}
		return send(ctx, out, ir.TxCommit{Position: pos})

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

// verifyPositionResumable confirms that the source still has the
// WAL/binlog data referenced by p. Returns an error wrapping
// [ir.ErrPositionInvalid] when the persisted position can't be
// resumed from — typically because the binlog file has been purged
// (file/pos mode) or the source's gtid_purged advanced past GTIDs
// the resume set hasn't consumed (GTID mode).
//
// The pipeline orchestrator detects the wrap via [errors.Is] and
// falls through to cold-start (ADR-0022). Empty/auto-detect
// positions don't reach this helper — the resolveStartPosition
// caller short-circuits before calling it.
func (r *CDCReader) verifyPositionResumable(ctx context.Context, p binlogPos) error {
	switch p.Mode {
	case positionModeFilePos:
		return verifyBinlogFilePresent(ctx, r.db, p.File)
	case positionModeGTID:
		return verifyGTIDSetReachable(ctx, r.db, p.GTIDSet)
	default:
		return fmt.Errorf("mysql: cannot verify position with mode %q", p.Mode)
	}
}

// verifyBinlogFilePresent checks that the named binlog file is in
// the source's SHOW BINARY LOGS output. Returns ErrPositionInvalid-
// wrapped when the file is missing — the typical "binlog purged"
// surface. SHOW BINARY LOGS' column count varies across versions
// (2 columns pre-8.0.14; 3 with Encrypted in 8.0.14+); we only read
// the first column (Log_name).
func verifyBinlogFilePresent(ctx context.Context, db *sql.DB, file string) error {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return fmt.Errorf("mysql: SHOW BINARY LOGS: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("mysql: SHOW BINARY LOGS columns: %w", err)
	}

	holders := make([]any, len(cols))
	dest := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}

	for rows.Next() {
		if err := rows.Scan(holders...); err != nil {
			return fmt.Errorf("mysql: SHOW BINARY LOGS scan: %w", err)
		}
		name, ok := scanString(dest[0])
		if !ok {
			continue
		}
		if name == file {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mysql: SHOW BINARY LOGS rows: %w", err)
	}
	return fmt.Errorf("mysql: binlog file %q is no longer available on the source (purged); cannot resume: %w",
		file, ir.ErrPositionInvalid)
}

// verifyGTIDSetReachable checks that the source's @@gtid_purged is
// a subset of the resume set. Equivalent to: every purged GTID has
// already been consumed by us. If a purged GTID is NOT in the resume
// set, it sits in (E - R) ∩ P — needed-but-purged — and the stream
// would skip data on resume.
//
// `GTID_SUBSET(@@gtid_purged, ?)` returns 1 when every purged GTID
// is within the supplied set, 0 otherwise. The empty-string resume
// set short-circuits to "all of gtid_purged is unknown to us" —
// callers that hit this helper always passed a non-empty decoded set.
func verifyGTIDSetReachable(ctx context.Context, db *sql.DB, resumeSet string) error {
	if strings.TrimSpace(resumeSet) == "" {
		// Defensive: the caller's decoded position should always
		// carry a set in GTID mode, but be explicit about not
		// silently passing an empty check.
		return fmt.Errorf("mysql: cannot verify empty GTID resume set: %w", ir.ErrPositionInvalid)
	}
	var subset int
	err := db.QueryRowContext(ctx,
		"SELECT GTID_SUBSET(@@global.gtid_purged, ?)",
		resumeSet,
	).Scan(&subset)
	if err != nil {
		return fmt.Errorf("mysql: GTID_SUBSET(@@gtid_purged, resume): %w", err)
	}
	if subset == 1 {
		return nil
	}
	return fmt.Errorf("mysql: source has purged GTIDs not present in resume set; cannot resume: %w",
		ir.ErrPositionInvalid)
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

// parseTruncateTable detects whether a binlog QUERY_EVENT body is a
// TRUNCATE statement and, if so, returns the schema-qualified table
// reference. Returns ok=false for any non-TRUNCATE input — the
// caller treats that as "this is some other DDL, fall through to
// schema-cache invalidation".
//
// Recognised forms:
//
//	TRUNCATE TABLE foo
//	TRUNCATE foo                  (the optional-TABLE form)
//	TRUNCATE TABLE `foo`
//	TRUNCATE TABLE schema.foo
//	TRUNCATE TABLE `schema`.`foo`
//
// Multi-table TRUNCATEs (TRUNCATE foo, bar) and anything outside
// the recognised shapes return ok=false. The bound is deliberate:
// this is not a SQL parser. Out-of-shape TRUNCATEs fall through to
// generic DDL handling, which still invalidates the schema cache —
// the only thing the operator loses is a typed ir.Truncate event,
// not correctness.
func parseTruncateTable(query string) (schema, table string, ok bool) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	const truncateKW = "TRUNCATE"
	if !strings.HasPrefix(upper, truncateKW) {
		return "", "", false
	}
	// Strip "TRUNCATE" plus exactly one whitespace separator.
	rest := strings.TrimSpace(q[len(truncateKW):])
	if rest == "" {
		return "", "", false
	}

	// Optional "TABLE" keyword. Two cases to reject explicitly:
	//
	//   - rest is exactly "TABLE" (bare keyword, no name follows)
	//     — the source DDL "TRUNCATE TABLE" would have errored at
	//     the server anyway, but the parser should not pretend the
	//     keyword is a table name.
	//   - rest is "TABLEFOO" (no whitespace separator) — that's a
	//     legal table name beginning with the letters T-A-B-L-E,
	//     not a TABLE-keyword + name. Don't strip the prefix.
	const tableKW = "TABLE"
	if strings.EqualFold(rest, tableKW) {
		return "", "", false
	}
	if len(rest) > len(tableKW) && strings.EqualFold(rest[:len(tableKW)], tableKW) {
		next := rest[len(tableKW)]
		if next == ' ' || next == '\t' {
			rest = strings.TrimSpace(rest[len(tableKW):])
		}
	}

	if rest == "" {
		return "", "", false
	}

	// rest is now "foo" / "`foo`" / "schema.foo" / "`schema`.`foo`"
	// (possibly with trailing punctuation we reject below). No commas
	// allowed — multi-table TRUNCATE falls through to generic DDL.
	if strings.ContainsAny(rest, ",;()") {
		return "", "", false
	}

	// Split into at most two parts on the first non-quoted dot.
	left, right, hasDot := splitTruncateRef(rest)
	if hasDot {
		schema = stripBackticks(left)
		table = stripBackticks(right)
	} else {
		schema = ""
		table = stripBackticks(left)
	}

	if table == "" {
		return "", "", false
	}
	return schema, table, true
}

// splitTruncateRef splits a table reference on the first dot that
// is *not* inside backticks. Returns (left, right, true) when a dot
// was found, (whole, "", false) otherwise.
func splitTruncateRef(s string) (left, right string, hasDot bool) {
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '`':
			inQuote = !inQuote
		case '.':
			if !inQuote {
				return s[:i], s[i+1:], true
			}
		}
	}
	return s, "", false
}

// stripBackticks removes a single matching pair of backticks
// surrounding s. Internal backticks (uncommon in real identifiers)
// are left alone.
func stripBackticks(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}

// decodeBinlogRow maps a positional row from a RowsEvent (one entry
// per column, in column-declaration order) onto an [ir.Row] keyed by
// column name with values run through decodeValue.
//
// Generated columns are decoded into the local positional walk (the
// binlog row image carries them) but dropped from the emitted row
// map. The applier's INSERT/UPDATE column list is derived from the
// row map's keys, so dropping the generated entry here naturally
// excludes the column from the target SQL — the target's GENERATED
// clause then recomputes the value rather than freezing the source-
// side result.
func decodeBinlogRow(raw []any, cols []*ir.Column) (ir.Row, error) {
	if len(raw) != len(cols) {
		return nil, fmt.Errorf("row has %d values; schema has %d columns", len(raw), len(cols))
	}
	row := make(ir.Row, len(cols))
	for i, col := range cols {
		if col.IsGenerated() {
			continue
		}
		v, err := decodeValue(raw[i], col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		row[col.Name] = v
	}
	return row, nil
}
