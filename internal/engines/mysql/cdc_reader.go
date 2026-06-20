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

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/netkeepalive"
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

	// posVerifyTimeout bounds the warm-resume position-verify queries
	// (see [positionVerifyTimeout]). Zero — the production default for a
	// reader built without setting it — falls back to the const; only
	// unit tests set a small value to exercise the deadline path
	// deterministically (the zero-value-safe default pattern).
	posVerifyTimeout time.Duration

	// posVerifyProbeTimeout bounds the SELECT 1 liveness probe that
	// diagnoses WHY a verify query timed out (see
	// [sourceLivenessProbeTimeout] / [sourceUnresponsiveDiagnosis]). Zero
	// falls back to the const; only tests set a small value. The probe is
	// itself bounded so a wedged source can't turn the diagnosis into a
	// second hang.
	posVerifyProbeTimeout time.Duration

	// schema is the database name the reader is bound to. In the
	// default single-database mode, events from other databases are
	// dropped during dispatch — this engine presents a single-schema
	// view, same as RowReader and SchemaReader.
	schema string

	// boolWarn carries the one-time-per-column TINYINT(1)-out-of-range
	// WARN (Vector D) for the binlog decode path, mirroring the
	// bulk-copy reader. Lazily created on the (single-goroutine) pump in
	// dispatchRows; nil until then. A non-{0,1} value in a TINYINT(1)
	// column is still carried as a bool per MySQL convention, but the
	// operator is told (and pointed at --type-override) rather than it
	// being silent on the steady-state CDC tail.
	boolWarn *boolRangeWarner

	// cdcDBInScope is the ADR-0074 Phase 1b multi-database event-allow
	// predicate, set by [SetCDCDatabaseScope]. When non-nil the reader
	// streams the server-wide binlog scoped to the SELECTED database set
	// instead of the single bound `schema`: an event is emitted iff its
	// source database satisfies this predicate, and each emitted
	// [ir.Change.Schema] carries that source database (read from the
	// event's own TABLE_MAP_EVENT / QUERY-event metadata) so the applier
	// can route it to the matching target namespace. A nil predicate
	// (the default) keeps the single-database drop EXACTLY as before:
	// only `schema` is in scope. Consulted only on the pump goroutine.
	cdcDBInScope func(database string) bool

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

	// pendingDDLAnchor is the ADR-0049 Chunk B1 deferred-snapshot
	// anchor. clear(r.schemaCache) on a DDL QueryEvent is eager, but
	// the *tableSchema (→ ir.Table) rebuilds LAZILY on the next row
	// event for each table. Locked decision #4c requires the
	// schema-history version be anchored at the DDL event's OWN
	// position, NOT the first post-DDL row's. So at clear time we
	// capture the QUERY-event's GTID/file-pos here; when tableFor next
	// rebuilds a table we emit an ir.SchemaSnapshot keyed with THIS
	// anchor (gated by the true-delta check). pendingDDLActive
	// distinguishes "anchor captured, awaiting rebuilds" from the
	// zero value (no DDL seen yet / all post-DDL tables already
	// re-snapshotted). Set/cleared only on the single pump goroutine.
	pendingDDLAnchor ir.Position
	pendingDDLActive bool

	// snapshotSig is the per-qualified-table structural fingerprint of
	// the schema-history version last emitted as an
	// [ir.SchemaSnapshot] (ADR-0049 Chunk B1). Implements DP-1
	// sign-off point ii (true-delta only): a DDL that does not change
	// a given table's (column-name, ordered-type) decode contract
	// (e.g. an ALTER on a *different* table, an index-only change)
	// must NOT write a new version for that table — the blanket
	// schemaCache clear is conservative, the snapshot is precise.
	snapshotSig map[string]ir.SchemaSignature

	// schemaForward enables ADR-0091 F7a single-stream schema-change
	// forwarding. When true, maybeSnapshotSchemaB1 ALSO emits a
	// SchemaSnapshot on a per-column NULLABILITY-only change (GAP #2),
	// even though ir.SchemaSignatureOf (name + ordered type, the ADR-0049
	// decode contract) is unchanged. The zero value (false) preserves the
	// pre-ADR-0091 behavior: only a signature delta emits a boundary. Set
	// by [CDCReader.SetSchemaForward] before StreamChanges; read only on
	// the pump goroutine.
	schemaForward bool

	// forwardNullSig tracks, per qualified table, the last-emitted
	// per-column nullability vector — the SEPARATE forward-delta signal
	// for GAP #2. It is intentionally NOT folded into snapshotSig (which
	// must stay the pure ADR-0049 decode/history signature); a
	// nullability change moves forwardNullSig without touching snapshotSig.
	forwardNullSig map[string]string

	// posMode and gtidSet track the current resume position. In GTID
	// mode, gtidSet accumulates committed GTIDs and is encoded into
	// each emitted Change. In file/pos mode, currentFile and the
	// per-event LogPos are encoded instead.
	posMode     positionMode
	gtidSet     mysql.GTIDSet
	currentFile string

	// serverUUID is the source instance's @@server_uuid, read once at
	// stream start. Stamped into every file/pos position so a resume
	// against a replaced/restored instance (Track 1c node-replace
	// class) is detected loudly rather than silently starting the
	// syncer at an offset in an unrelated binlog lineage. Empty if
	// the lookup failed (the verify path then falls back to the
	// name-only check — no regression, just no extra protection).
	serverUUID string

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
// type, plus the primary-key column-name list. Indexes (other than the
// PK) and foreign keys are not consulted on a row event so we don't
// pay to load them on every cache miss.
//
// PrimaryKey was added for Bug 88: under `binlog_row_image=MINIMAL`
// (and `NOBLOB` when BLOB/TEXT non-PK columns exist), the binlog
// DELETE rows-event carries `nil` for non-PK columns; the CDC
// reader's DELETE emit path narrows the Before-image to PK columns
// only via [filterDeleteBefore] so the applier's `buildWhereClause`
// doesn't emit "non_pk_col IS NULL" predicates that fail to match
// real target rows. Same shape as the PG-side helper of the same
// name. Empty slice on a PK-less table — [filterDeleteBefore] falls
// back to the full Before-image in that case (the only usable
// identity on a PK-less table is "every column", same fallback as
// PG's REPLICA IDENTITY FULL on a PK-less relation).
type tableSchema struct {
	Schema     string
	Name       string
	Columns    []*ir.Column
	PrimaryKey []string
}

// SetCDCDatabaseScope implements [ir.CDCDatabaseScoper]. It switches
// the reader from its default single-database view to a multi-database
// fan-out (ADR-0074 Phase 1b): the server-wide binlog is streamed
// scoped to the database set `inScope` admits, and each emitted change
// carries its source database in [ir.Change.Schema]. A nil predicate is
// a no-op (single-database mode preserved). Must be called before
// [StreamChanges]; the predicate is read on the pump goroutine.
func (r *CDCReader) SetCDCDatabaseScope(inScope func(database string) bool) {
	if inScope == nil {
		return
	}
	r.cdcDBInScope = inScope
}

// SetSchemaForward enables ADR-0091 F7a single-stream schema-change
// forwarding for this reader. GAP #2 (this method): when true,
// maybeSnapshotSchemaB1 ALSO emits an ir.SchemaSnapshot on a per-column
// NULLABILITY-only change — a MODIFY … NULL / NOT NULL that does not move
// ir.SchemaSignatureOf (name + ordered type) and so would otherwise be
// swallowed by the ADR-0049 true-delta gate. That boundary lets the
// pipeline forward intercept classify ShapeKindAlterColumnNullability and
// carry the operator's DDL to the target. ADD / DROP / TYPE changes move
// the signature and already emit; this only widens the nullability case.
//
// The zero value (false) — non-streamer callers, or --schema-changes=
// refuse — preserves the exact pre-ADR-0091 behavior: only a signature
// delta emits a boundary, so a nullability-only ALTER produces no extra
// boundary. Must be called before [StreamChanges]; read on the pump
// goroutine. Implements pipeline.schemaForwardModeSetter.
func (r *CDCReader) SetSchemaForward(enabled bool) {
	r.schemaForward = enabled
}

// databaseInScope reports whether events from the given source database
// should be emitted. In multi-database mode (cdcDBInScope non-nil) it
// delegates to the selected-set predicate; otherwise it preserves the
// single-database rule — only the bound `schema` is in scope. This is
// the single decision point the dispatch paths consult, so the
// single-database behaviour stays byte-identical: a nil predicate makes
// this exactly `database == r.schema`.
func (r *CDCReader) databaseInScope(database string) bool {
	if r.cdcDBInScope != nil {
		return r.cdcDBInScope(database)
	}
	return database == r.schema
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

	// Read the source instance identity once, before resolving the
	// start position — resolveStartPosition's verify step needs it to
	// reject a resume against a replaced/restored instance (Track 1c
	// node-replace class). A failed lookup is non-fatal: serverUUID
	// stays empty and the verify path falls back to the name-only
	// check (pre-existing behaviour, no regression).
	if uuid, err := sourceServerUUID(ctx, r.db); err == nil {
		r.serverUUID = uuid
	} else {
		slog.WarnContext(
			ctx, "mysql: cdc: could not read @@server_uuid; "+
				"node-replace position-loss detection degraded to binlog-filename-only",
			slog.String("err", err.Error()),
		)
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
		// MaxConnAttempts bounds the BinlogSyncer's internal retry
		// loop (onStream → retrySync). Default 0 = infinite retries.
		// In long-lived production streams that's the right default
		// (transient source restarts shouldn't kill the streamer).
		// But in CI's integration-job container-pressure environment,
		// torn-down testcontainers leave the retry loop in a "dial
		// tcp [::1]:32xxx: connection refused" loop forever; under
		// pressure this leaks goroutines into subsequent tests
		// (TestMigrate_MySQLToPostgres flake on v0.27.0+). 30 attempts
		// at the default 1s interval = ~30s of retries before the
		// goroutine gives up — long enough for legitimate transient
		// outages, short enough that test-side ctx cancellation
		// doesn't leave goroutines piling up.
		MaxReconnectAttempts: 30,
		// Dialer is the transport-level complement to HeartbeatPeriod
		// above: the heartbeat keeps MySQL from timing the replica out,
		// while sluice's shared TCP keep-alive policy keeps a cloud-NAT
		// mapping warm on an idle binlog stream and bounds dead-peer
		// detection to seconds rather than the kernel's multi-minute
		// default. #77.
		Dialer: netkeepalive.Dialer().DialContext,
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
			r.setErr(classifyReaderError(fmt.Errorf("mysql: cdc: get event: %w", err)))
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
		slog.WarnContext(
			ctx, "mysql: cdc: no binlog row events received during startup grace period",
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
		// The TABLE_MAP_EVENT's own schema field is the authoritative
		// per-event source database (the binlog is server-wide). In
		// single-database mode databaseInScope reduces to
		// `schema == r.schema`; in multi-database mode it admits every
		// selected database — the SAME drop mechanism, a wider allow set.
		schema := string(e.Schema)
		if !r.databaseInScope(schema) {
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
			if r.databaseInScope(truncSchema) {
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
		if stmtSchema == "" || r.databaseInScope(stmtSchema) {
			clear(r.schemaCache)
			// ADR-0049 Chunk B1, locked decision #4c: capture THIS
			// QUERY (DDL) event's own position now. The schemaCache
			// clear is eager but the *tableSchema rebuilds lazily on
			// the next row per table; the schema-history version must
			// be anchored at the DDL boundary, not the first post-DDL
			// row, else a replayed event between the two resolves to
			// the pre-DDL schema (the silent-mis-decode this ADR
			// kills). tableFor consumes pendingDDLAnchor on the next
			// rebuild. positionFor here reflects the GTID set
			// accumulated through this DDL's own GTIDEvent.
			ddlPos, err := r.positionFor(ev.Header)
			if err != nil {
				return err
			}
			r.pendingDDLAnchor = ddlPos
			r.pendingDDLActive = true
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

	if r.boolWarn == nil { // single-goroutine pump — no lock needed
		r.boolWarn = newBoolRangeWarner()
	}

	// ADR-0049 Chunk B1: after a DDL invalidated the schema cache,
	// the first row event per table forces tableFor to rebuild the
	// *tableSchema. Snapshot that rebuilt schema HERE — strictly
	// before this row is decoded/sent — anchored at the DDL event's
	// own captured position (#4c), gated by the true-delta check
	// (DP-1 ii). This preserves the loud floor: the snapshot is a
	// durable version write that lands ahead of the row, it never
	// swallows or reorders the existing decode-error path below.
	if err := r.maybeSnapshotSchemaB1(ctx, qn, tbl, out); err != nil {
		return err
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
			row, err := decodeBinlogRow(raw, tbl.Columns, tbl.Name, r.boolWarn)
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
			before, err := decodeBinlogRow(ev.Rows[i], tbl.Columns, tbl.Name, r.boolWarn)
			if err != nil {
				return fmt.Errorf("mysql: cdc: decode update before: %w", err)
			}
			after, err := decodeBinlogRow(ev.Rows[i+1], tbl.Columns, tbl.Name, r.boolWarn)
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
			before, err := decodeBinlogRow(raw, tbl.Columns, tbl.Name, r.boolWarn)
			if err != nil {
				return fmt.Errorf("mysql: cdc: decode delete: %w", err)
			}
			// Bug 88: narrow the Before-image to PK columns before
			// emit. Under `binlog_row_image=MINIMAL` (and `NOBLOB`
			// when BLOB/TEXT non-PK columns exist), the binlog DELETE
			// rows-event carries `nil` for non-PK columns; without
			// this filter the applier's buildWhereClause would emit
			// `non_pk_col IS NULL` predicates that fail to match real
			// target rows whose non-PK columns hold non-null values
			// — Bug-8-equivalent silent data loss. Mirrors the
			// PG-side helper of the same name. See
			// docs/adr/adr-0057-hard-delete-semantics-across-engines.md.
			before = filterDeleteBefore(tbl, before)
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

// maybeSnapshotSchemaB1 is the ADR-0049 Chunk B1 deferred-snapshot
// emitter. It runs on every row event but does work only while a DDL
// boundary is pending (pendingDDLActive — set when the generic-DDL
// branch cleared the schema cache and captured the DDL's own
// position). For the rebuilt *tableSchema of the table this row
// targets it:
//
//   - projects *tableSchema → ir.Table (the binlog reader's existing
//     post-DDL view; locked decision #2 — B1 reuses the lazily-
//     rebuilt schema, the readiness brief's "cheapest" boundary path,
//     position-anchored by capturing the DDL GTID at clear time
//     rather than re-introspecting "schema now" at an arbitrary later
//     position);
//   - true-delta gates it (DP-1 ii): the blanket schemaCache clear is
//     conservative — a DDL on a *different* table, or an index-only
//     ALTER, leaves THIS table's (column-name, ordered-type) decode
//     contract unchanged, so no version is written for it;
//   - emits ir.SchemaSnapshot anchored at pendingDDLAnchor — the DDL
//     event's OWN position (#4c), NOT this row's — so a replay
//     between the DDL and the first post-DDL row resolves to the
//     post-DDL schema.
//
// A column whose type can't be reconstructed already failed loudly in
// loadTableSchema/translateType before reaching here (tableFor
// returned that error), so this path sees only well-formed tables;
// its only failure surface is a blocked channel send (propagated —
// fatal/loud, #4b).
func (r *CDCReader) maybeSnapshotSchemaB1(ctx context.Context, qn string, tbl *tableSchema, out chan<- ir.Change) error {
	if !r.pendingDDLActive || tbl == nil {
		return nil
	}
	irTbl := &ir.Table{Schema: tbl.Schema, Name: tbl.Name, Columns: tbl.Columns}
	// Bug 89: surface the PK so downstream consumers (ADR-0058 backfill,
	// other future per-PK paths) can resolve a cursor-paginated iteration
	// against the table. The CDC reader's tableSchema already carries the
	// PK column name list (Bug 88); project it into the IR Index shape.
	if len(tbl.PrimaryKey) > 0 {
		pkCols := make([]ir.IndexColumn, len(tbl.PrimaryKey))
		for i, name := range tbl.PrimaryKey {
			pkCols[i] = ir.IndexColumn{Column: name}
		}
		irTbl.PrimaryKey = &ir.Index{Columns: pkCols}
	}
	sig := ir.SchemaSignatureOf(irTbl)
	sigPrev, hadSig := r.snapshotSig[qn]
	sigDelta := !hadSig || !sigPrev.Equal(sig)

	// ADR-0091 F7a GAP #2: a per-column NULLABILITY change does NOT move
	// ir.SchemaSignatureOf (name + ordered type — the ADR-0049 decode
	// contract deliberately excludes nullability). So a MODIFY … NULL /
	// NOT NULL on its own would skip emission below and never reach the
	// pipeline forward intercept. When schema-change forwarding is on we
	// add a SEPARATE forward signal: emit a boundary when the nullability
	// vector changed even if the decode signature did not. forwardNullSig
	// is tracked independently of snapshotSig precisely so this does NOT
	// perturb the value-fidelity-critical decode/history contract.
	nullSig := nullabilitySignature(tbl)
	if r.forwardNullSig == nil {
		// Lazy-init: the production constructor seeds this map, but unit
		// readers built as a struct literal may omit it.
		r.forwardNullSig = make(map[string]string)
	}
	nullDelta := false
	if r.schemaForward {
		nsPrev, hadNS := r.forwardNullSig[qn]
		nullDelta = hadNS && nsPrev != nullSig
	}

	if !sigDelta && !nullDelta {
		// This table's decode contract didn't change across the DDL
		// (blanket-clear was conservative) — not a true delta — and (in
		// forward mode) its nullability is unchanged too. Not a boundary.
		// Keep the nullability tracker warm so the FIRST nullability change
		// after this point is detected.
		r.forwardNullSig[qn] = nullSig
		return nil
	}
	if err := send(ctx, out, ir.SchemaSnapshot{
		Position: r.pendingDDLAnchor,
		Schema:   tbl.Schema,
		Table:    tbl.Name,
		IR:       irTbl,
	}); err != nil {
		return err
	}
	// Advance the decode signature only on a real signature delta — the
	// ADR-0049 history true-delta gate stays exactly as before. Always
	// advance the separate nullability tracker.
	if sigDelta {
		r.snapshotSig[qn] = sig
	}
	r.forwardNullSig[qn] = nullSig
	return nil
}

// nullabilitySignature renders a stable per-column NULLABLE vector for a
// table — the ADR-0091 F7a GAP #2 forward-delta signal. It is kept
// SEPARATE from ir.SchemaSignatureOf (the ADR-0049 decode contract, which
// must remain name + ordered type only); a change here drives a forward
// boundary without perturbing the decode/history signature. Column order
// is the post-DDL information_schema order, which both compared snapshots
// share, so a pure nullability flip changes exactly one entry.
func nullabilitySignature(tbl *tableSchema) string {
	var b strings.Builder
	for _, c := range tbl.Columns {
		b.WriteString(c.Name)
		if c.Nullable {
			b.WriteString("=1;")
		} else {
			b.WriteString("=0;")
		}
	}
	return b.String()
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
			Mode:       positionModeFilePos,
			File:       r.currentFile,
			Pos:        hdr.LogPos,
			ServerUUID: r.serverUUID,
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

// positionVerifyTimeout bounds the warm-resume position-verify source
// queries (SHOW BINARY LOGS / GTID_SUBSET). Without a deadline, a
// half-dead source connection — e.g. one left in the pool by a prior
// broken pipe after a transaction-killer-induced stream restart — makes
// the verify QueryContext block on the TCP read FOREVER, hanging the
// whole stream startup (goroutine 1) with the apply position frozen: a
// "looks alive but wedged" stall, found live on Track D (2026-06-20,
// goroutine 1 stuck 302min in verifyBinlogFilePresent [IO wait]). 30s is
// generous for these metadata queries; on expiry the verify returns a
// RETRIABLE error (so the stream reconnects + retries) and NEVER
// [ir.ErrPositionInvalid] (which would trigger a destructive cold-start
// re-snapshot on a transient source blip).
const positionVerifyTimeout = 30 * time.Second

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
//
// The verify queries run under a bounded [positionVerifyTimeout] so a
// wedged source connection fails LOUDLY + retriably (reconnect) instead
// of hanging the stream forever; see the const's comment for the live
// Track-D hang this closes.
func (r *CDCReader) verifyPositionResumable(ctx context.Context, p binlogPos) error {
	timeout := r.posVerifyTimeout
	if timeout <= 0 {
		timeout = positionVerifyTimeout
	}
	vctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := r.verifyPositionResumableInner(vctx, p)
	if err != nil && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		// OUR bounded deadline fired (the parent ctx is still live, so this
		// is not a shutdown cancel): the source did not answer the resume
		// preflight in time — typically a half-dead connection or a wedged
		// source. Run a bounded differential liveness probe to NARROW the
		// cause (server globally unresponsive vs binlog-subsystem-slow vs
		// out-of-disk) and fold it into the message. Surface RETRIABLE
		// (classifyApplierError maps context.DeadlineExceeded →
		// ir.RetriableError) so the streamer's ADR-0038 loop reconnects +
		// retries, rather than hanging forever OR being mistaken for a
		// purged position (which would cold-start). The retry draws a fresh
		// connection from the pool, leaving the wedged one behind.
		diag := r.sourceUnresponsiveDiagnosis(ctx, p)
		return classifyApplierError(fmt.Errorf(
			"mysql: resume-position verify exceeded %s (%s); reconnecting: %w",
			timeout, diag, err,
		))
	}
	return err
}

// verifyPositionResumableInner runs the actual mode-specific verify
// queries under the caller-supplied (already deadline-bounded) context.
func (r *CDCReader) verifyPositionResumableInner(ctx context.Context, p binlogPos) error {
	switch p.Mode {
	case positionModeFilePos:
		// Track 1c node-replace floor: a file/pos position is only
		// meaningful on the exact instance it was captured on, because
		// binlog file NAMES are instance-local and a replaced /
		// restored-from-backup instance reuses the same names for an
		// unrelated lineage. Reject loudly when the persisted instance
		// identity doesn't match the source's current one — BEFORE the
		// name check, which would false-positive on the name reuse.
		if err := verifySourceInstanceIdentity(ctx, p.ServerUUID, r.serverUUID); err != nil {
			return err
		}
		return verifyBinlogFilePresent(ctx, r.db, p.File)
	case positionModeGTID:
		// GTID UUIDs are themselves instance-bound, so a fresh
		// instance's gtid_purged/gtid_executed carry a different
		// source UUID and verifyGTIDSetReachable already catches the
		// node-replace case without a separate identity check.
		return verifyGTIDSetReachable(ctx, r.db, p.GTIDSet)
	default:
		return fmt.Errorf("mysql: cannot verify position with mode %q", p.Mode)
	}
}

// sourceLivenessProbeTimeout bounds the SELECT 1 liveness probe in
// [sourceUnresponsiveDiagnosis]. Short on purpose: a wedged source must not
// turn the diagnosis itself into a second long stall, and a healthy server
// answers SELECT 1 in milliseconds.
const sourceLivenessProbeTimeout = 5 * time.Second

// sourceUnresponsiveDiagnosis runs a bounded `SELECT 1` liveness probe to
// NARROW why a resume-position verify query timed out, returning an
// operator-facing hint folded into the timeout error. The verify query
// (SHOW BINARY LOGS / GTID_SUBSET) touches the binlog subsystem; a plain
// SELECT 1 does not — so the differential tells the operator which layer is
// stuck:
//
//   - SELECT 1 returns a disk-full signal ([isDiskFullSignal]) → the source
//     host is OUT OF DISK; name it explicitly (the highest-value case).
//   - SELECT 1 ALSO times out → the server is GLOBALLY unresponsive (a full
//     datadir blocks MySQL writes server-wide, severe overload, or down).
//   - SELECT 1 succeeds → the server is up but the BINLOG SUBSYSTEM
//     specifically is slow — commonly an over-large binlog file count or a
//     slow/full binlog volume (this is the Track-D shape: 2585 binlog files
//     on a full disk made SHOW BINARY LOGS block while the server otherwise
//     answered).
//   - SELECT 1 fails some other way → the connection is unhealthy.
//
// Why this is best-effort, not authoritative: the MySQL wire protocol exposes
// no datadir free-space surface, and a full disk frequently makes MySQL BLOCK
// ("waiting for someone to free some space") rather than return an error — so
// the probe NARROWS the cause, it does not definitively assert "disk full".
// It is itself bounded ([sourceLivenessProbeTimeout]) so it can never hang.
// Pure diagnosis: it changes only the error TEXT, never the retry/position
// decision.
//
// For the binlog/disk-pressure causes it also appends an EXACT, safe remediation
// derived from the resume position p (see [safePurgeHint]) — e.g. the precise
// `PURGE BINARY LOGS TO '<resume-file>'` that frees space without losing this
// stream's resume point — so the operator gets the command, not just "consider
// PURGE BINARY LOGS". sluice surfaces the command; it never runs it (purging
// source binlogs is destructive, affects shared infra the tool can't see, and
// needs an elevated source privilege sluice deliberately does not require).
func (r *CDCReader) sourceUnresponsiveDiagnosis(ctx context.Context, p binlogPos) string {
	probeTimeout := r.posVerifyProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = sourceLivenessProbeTimeout
	}
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	var one int
	err := r.db.QueryRowContext(pctx, "SELECT 1").Scan(&one)
	switch {
	case err == nil:
		return "a SELECT 1 liveness probe succeeded, so the source server is up but its BINLOG subsystem " +
			"specifically is slow — commonly an over-large binlog file count or a slow/full binlog volume; " +
			"check the source's binlog disk and binlog_expire_logs_seconds." + safePurgeHint(p)
	case isDiskFullSignal(err):
		return "a SELECT 1 liveness probe returned a disk-full signal — the source host appears to be OUT OF " +
			"DISK SPACE; free space on the source before resuming." + safePurgeHint(p)
	case errors.Is(err, context.DeadlineExceeded):
		return "a SELECT 1 liveness probe ALSO timed out, so the source server is globally unresponsive — " +
			"disk exhaustion (a full datadir blocks MySQL writes), severe overload, or the server is down; " +
			"check the source host's disk and load." + safePurgeHint(p)
	default:
		return fmt.Sprintf("a SELECT 1 liveness probe failed (%v) — the source connection is unhealthy", err)
	}
}

// safePurgeHint returns an EXACT, safe source-side binlog-purge recommendation
// derived from the resume position, for the diagnosis branches that point at
// binlog/disk pressure. In file/pos mode it names the precise
// `PURGE BINARY LOGS TO '<resume-file>'` — MySQL deletes only logs OLDER than
// the named file and KEEPS that file, so the resume (which reads from it) is
// preserved; sluice already knows the resume file, so it can hand the operator
// the exact command instead of a generic "consider PURGE BINARY LOGS". In GTID
// mode the safe boundary is a GTID set, not a single file, so it states the
// constraint rather than a command. ALWAYS carries the shared-infra caveat:
// sluice only knows ITS OWN resume needs — other replicas / PITR backups may
// still need the older logs, so the operator must confirm before purging. An
// empty resume file (defensive) yields no hint.
func safePurgeHint(p binlogPos) string {
	switch p.Mode {
	case positionModeFilePos:
		if p.File == "" {
			return ""
		}
		return fmt.Sprintf(" To free space WITHOUT losing this stream's resume point, run on the source: "+
			"PURGE BINARY LOGS TO '%s'; — this deletes only logs OLDER than the resume file and keeps it; "+
			"first confirm no other replica or backup still needs those older logs (sluice cannot see them).", p.File)
	case positionModeGTID:
		return " To free space safely, purge so that NO GTID in this stream's resume set is removed " +
			"(e.g. PURGE BINARY LOGS BEFORE a timestamp older than the resume), and confirm no other " +
			"replica or backup needs those logs (sluice cannot see them)."
	default:
		return ""
	}
}

// verifySourceInstanceIdentity refuses a file/pos resume whose
// persisted @@server_uuid differs from the source instance the
// resume is now connecting to. This is the loud-failure floor for
// the PlanetScale "node replaced / restored from backup / failed
// over" position-loss class: the new instance carries the same
// logical data but an independent binlog lineage that frequently
// reuses the old filenames, so a filename-only check (the historical
// behaviour) silently starts the syncer at a byte offset in an
// unrelated file. Returning ir.ErrPositionInvalid here routes the
// streamer's existing ADR-0022 fall-through to a clean cold-start
// re-snapshot instead.
//
// persistedUUID is empty for positions written before the
// ServerUUID field existed (transitional, zero-users) OR when the
// reader couldn't read @@server_uuid at stream start; in either
// degraded case we skip the identity check and let the filename
// check stand — no false refusal, no regression, just no extra
// protection for that one position. currentUUID empty (lookup
// failed now) likewise degrades rather than refusing — a transient
// information_schema hiccup shouldn't force a full re-snapshot.
func verifySourceInstanceIdentity(ctx context.Context, persistedUUID, currentUUID string) error {
	if persistedUUID == "" || currentUUID == "" {
		return nil
	}
	if persistedUUID == currentUUID {
		return nil
	}
	slog.WarnContext(
		ctx, "mysql: cdc: source instance identity changed since the persisted "+
			"position was captured (node replaced / restored from backup / failed over); the binlog "+
			"lineage does not carry over — refusing to resume to avoid a silent data gap",
		slog.String("persisted_server_uuid", persistedUUID),
		slog.String("current_server_uuid", currentUUID),
	)
	return fmt.Errorf("mysql: source server_uuid %q does not match the persisted position's "+
		"server_uuid %q (source instance was replaced/restored; binlog lineage does not carry "+
		"over); cannot resume: %w", currentUUID, persistedUUID, ir.ErrPositionInvalid)
}

// sourceServerUUID reads the source instance's @@server_uuid — a
// value MySQL generates once per data directory and keeps stable
// across restarts, but which is necessarily different on a fresh
// instance / restored-from-backup node. Used to bind a file/pos
// position to the instance that produced it (see
// verifySourceInstanceIdentity).
func sourceServerUUID(ctx context.Context, db *sql.DB) (string, error) {
	var uuid string
	if err := db.QueryRowContext(ctx, "SELECT @@global.server_uuid").Scan(&uuid); err != nil {
		return "", fmt.Errorf("mysql: read @@server_uuid: %w", err)
	}
	return uuid, nil
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
	err := db.QueryRowContext(
		ctx,
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
	q := strings.TrimSpace(stripLeadingSQLComments(query))
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

// stripLeadingSQLComments removes leading whitespace and SQL comments
// from a statement so the keyword scan in [parseTruncateTable] sees the
// actual command verb. MySQL preserves leading comments verbatim in the
// binlog QUERY_EVENT body (only the trailing statement delimiter is
// stripped), so a commented TRUNCATE arrives here with the comment
// still attached — e.g. a hand-written migration `-- clear staging\n
// TRUNCATE TABLE staging`, or an APM/ORM query tag `/* trace=… */
// TRUNCATE …`. Without this strip the HasPrefix("TRUNCATE") check
// fails, the statement falls through to generic DDL handling, and the
// typed [ir.Truncate] is never emitted — the target silently retains
// the rows the source truncated (Bug 140, a HIGH silent-divergence
// regression the sync-convergence property surfaced on MySQL→MySQL).
//
// Recognised leading-comment forms (MySQL comment syntax):
//
//	-- to end of line   (the "--" must be followed by whitespace/EOL)
//	#  to end of line
//	/* … */             block comment
//
// Executable comments (`/*! …` version-gated, `/*+ …` optimizer hints)
// are deliberately NOT stripped: removing them could discard
// conditionally-executed SQL, and a statement led by one simply falls
// through to generic DDL handling exactly as before — no typed event,
// but no incorrectness. Trailing comments are likewise out of scope:
// a TRUNCATE with a trailing comment fails the table-name parse and
// falls through to a loud apply-side error, not the silent loss this
// fixes.
func stripLeadingSQLComments(q string) string {
	for {
		q = strings.TrimLeft(q, " \t\r\n\f\v")
		switch {
		case strings.HasPrefix(q, "--") && (len(q) == 2 || q[2] == ' ' || q[2] == '\t' || q[2] == '\r' || q[2] == '\n'):
			i := strings.IndexAny(q, "\r\n")
			if i < 0 {
				return ""
			}
			q = q[i+1:]
		case strings.HasPrefix(q, "#"):
			i := strings.IndexAny(q, "\r\n")
			if i < 0 {
				return ""
			}
			q = q[i+1:]
		case strings.HasPrefix(q, "/*") && !strings.HasPrefix(q, "/*!") && !strings.HasPrefix(q, "/*+"):
			i := strings.Index(q[2:], "*/")
			if i < 0 {
				return "" // unterminated block comment — nothing usable follows
			}
			q = q[2+i+2:]
		default:
			return q
		}
	}
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
func decodeBinlogRow(raw []any, cols []*ir.Column, tableName string, warner *boolRangeWarner) (ir.Row, error) {
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
			var zd *zeroDateValueError
			if errors.As(err, &zd) {
				v, err = applyZeroDatePolicy(zd, col)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		if _, isBool := col.Type.(ir.Boolean); isBool {
			// Vector D: a TINYINT(1) value outside {0,1} is collapsed to
			// a bool here too — warn once per column on the CDC tail.
			warner.observe(tableName, col, raw[i])
		}
		row[col.Name] = v
	}
	return row, nil
}

// filterDeleteBefore narrows a binlog DELETE event's Before-image down
// to its primary-key columns. The narrowing is load-bearing for
// silent-data-loss prevention (Bug 88), so the protocol detail driving
// it is worth spelling out:
//
// Under `binlog_row_image=MINIMAL` the BEFORE-image of a DELETE
// rows-event carries only the PK column(s); MySQL emits every non-PK
// column as nil. Under `binlog_row_image=NOBLOB`, BLOB/TEXT/JSON
// non-PK columns are also stripped (sent as nil), but every other
// column carries its actual value. [decodeBinlogRow] faithfully
// translates the binlog's nil markers into present-but-nil entries
// in the row map. The applier's [buildWhereClause]
// (`change_applier.go:1240-1248`) then emits "non_pk_col IS NULL"
// for those entries, predicates that fail to match real rows whose
// non-PK columns hold non-null values. The DELETE matches zero
// rows, ADR-0010 absorbs the miss for resume idempotency, and the
// position advances — silent data divergence.
//
// Filtering to PK columns produces a WHERE that uses only the
// identity-key predicates, which is exactly what an idempotent
// DELETE against the primary key needs. The same filter is correct
// under FULL (every column is in the Before-image, but only the PK
// columns are required to identify the row; the WHERE is still
// right, just shorter).
//
// PK-less tables: MySQL allows tables without a PRIMARY KEY (and
// historically the source-readability check at ADR-0036 refuses to
// stream such a table, but a defensive fallback here keeps the
// helper total). With no PK there is no shorter identity than the
// full row image, so we hand back `before` verbatim. Same shape as
// PG's filterDeleteBefore on a PK-less REPLICA IDENTITY FULL
// relation.
//
// Same name and shape as the PG-side helper at
// `internal/engines/postgres/cdc_reader.go`; the family-dispatched
// fix locus matches between engines (per ADR-0057's Bug-74
// family-pin discipline).
func filterDeleteBefore(tbl *tableSchema, before ir.Row) ir.Row {
	if len(tbl.PrimaryKey) == 0 {
		// PK-less table: no shorter identity exists. Hand back the
		// full image — anything else would silently lose DELETEs on
		// PK-less tables, the very class of bug this helper exists
		// to prevent.
		return before
	}
	out := make(ir.Row, len(tbl.PrimaryKey))
	for _, col := range tbl.PrimaryKey {
		// A PK column missing from the Before-image is structurally
		// impossible under any binlog_row_image setting (the PK is
		// always carried; that's the whole point of MINIMAL). If it
		// somehow happens, copying the (possibly nil) value through
		// produces a `pk IS NULL` predicate — the same behaviour
		// the un-narrowed code path would have produced. We don't
		// refuse loudly here because the binlog wire format makes
		// the "PK missing" case unreachable in practice and the
		// alternative (returning an error from the rows-loop) is
		// noisier than the structural impossibility warrants.
		out[col] = before[col]
	}
	return out
}
