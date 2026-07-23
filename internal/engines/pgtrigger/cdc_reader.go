// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

// Defaults for the polling loop. ADR-0066 §6 — operator-tunable in a
// follow-up koanf section; Phase 1 hardcodes the safe defaults.
const (
	defaultPollInterval = 1 * time.Second
	defaultBatchSize    = 10000
	cdcChannelBuffer    = 256
)

// CDCReader is the trigger-engine CDC reader. It polls
// `sluice_change_log` at a configurable cadence (default 1s) and emits
// `ir.Change` events via the channel returned from [StreamChanges].
//
// One reader → one [StreamChanges] call. Concurrent calls are not
// supported; the polling loop owns the underlying *sql.DB pool for
// the lifetime of the stream.
type CDCReader struct {
	db     *sql.DB
	schema string
	dsn    string

	pollInterval time.Duration
	batchSize    int

	// pumpCancel cancels the polling goroutine when Close is called.
	pumpCancel context.CancelFunc

	// pruneMu guards pruneDB — the lazily-opened pool the ADR-0137 Phase-B
	// auto-prune ([PruneConsumedChangeLog]) reuses across ticks instead of
	// dialing+pinging per tick (P-1). The sidecar goroutine opens and uses it;
	// Close (another goroutine) releases it.
	pruneMu sync.Mutex
	pruneDB *sql.DB

	// pruneBook tracks the auto-prune remaining-rows estimate (P-1). Owned by
	// the single auto-prune sidecar goroutine; no locking.
	pruneBook triggercdc.Bookkeeper

	// mu guards err. The pump writes; the caller reads via Err.
	mu  sync.Mutex
	err error
}

// openCDCReader constructs a [CDCReader] bound to dsn. The reader's
// own *sql.DB pool is opened here so Close can release it cleanly;
// the embedded postgres.Engine's connection lifecycle is not shared.
// appID is the engine's connection-label id, stamped on the pool's
// application_name (empty → the "-" fallback).
//
// Refuses with a clear error when `sluice_change_log` is absent —
// the operator forgot to run `sluice trigger setup`. The refusal
// fires at open time so the streamer surfaces it before any data
// would move.
func openCDCReader(ctx context.Context, dsn, appID string) (ir.CDCReader, error) {
	cfg, err := parseDSNCompat(dsn)
	if err != nil {
		return nil, err
	}
	db, err := postgres.OpenPgxDB(cfg.dsn, appID)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: cdc open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: cdc ping: %w", err)
	}
	// Refuse loudly when the change-log table is missing. The
	// operator forgot to run `sluice trigger setup`; the error
	// message names the recovery action.
	if exists, err := changeLogTableExists(ctx, db, cfg.schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: cdc preflight: %w", err)
	} else if !exists {
		_ = db.Close()
		return nil, fmt.Errorf(
			"pgtrigger: %s.%s does not exist on the source — run `sluice trigger setup --dsn=...` before starting the stream",
			cfg.schema, ChangeLogTable,
		)
	}
	return &CDCReader{
		db:           db,
		schema:       cfg.schema,
		dsn:          cfg.dsn,
		pollInterval: defaultPollInterval,
		batchSize:    defaultBatchSize,
	}, nil
}

// SetPollInterval overrides the default 1 s poll cadence for this
// reader. Called by the orchestrator after [openCDCReader] when the
// operator passes `--poll-interval=DUR` on `sync start`. Idempotent;
// must be called before [StreamChanges] (the polling loop captures
// the interval at start). A zero or negative duration is rejected to
// keep the loop from spinning.
//
// Surfaced via a setter rather than the engine's [ir.Engine.OpenCDCReader]
// signature to preserve the existing interface contract — the
// streamer type-asserts on [pollIntervalSetter] and silently skips
// the call against engines that don't implement it. ADR-0066 §6 / roadmap
// item 18(c).
func (r *CDCReader) SetPollInterval(d time.Duration) {
	if d > 0 {
		r.pollInterval = d
	}
}

// Close releases the underlying connection pool (and the auto-prune
// pool, if a prune tick ever opened one) and stops any in-flight
// polling goroutine.
func (r *CDCReader) Close() error {
	if r.pumpCancel != nil {
		r.pumpCancel()
	}
	r.pruneMu.Lock()
	if r.pruneDB != nil {
		_ = r.pruneDB.Close()
		r.pruneDB = nil
	}
	r.pruneMu.Unlock()
	if r.db != nil {
		err := r.db.Close()
		r.db = nil
		return err
	}
	return nil
}

// Err returns the most recent error the polling goroutine recorded.
// Callers MUST consult Err after the channel returned by
// [StreamChanges] closes — a poll-time decode failure is the engine's
// loud-failure surface (mirroring the postgres engine's
// [postgres.CDCReader.Err]).
func (r *CDCReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// setErr records a terminal error from the pump goroutine. Idempotent
// on repeat calls (only the first error wins so the operator sees the
// root cause rather than a downstream effect).
func (r *CDCReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// StreamChanges opens the polling loop. The returned channel emits
// [ir.Change] events in change-log id order (filtered through the §2
// safety-lag predicate so a row whose allocating txid is still
// in-flight is held back until commit).
//
// from carries the durable resume bookmark — the last successfully-
// applied change-log id. The zero-value [ir.Position] means "from
// now": the reader starts at MAX(id) on the source and emits only
// changes captured AFTER the stream opens (mirror of the postgres
// engine's "from now" semantics).
//
// On context cancel the goroutine drains in-flight rows, closes the
// channel, and stops. On a poll failure the channel closes and Err
// returns the failure.
func (r *CDCReader) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	pos, ok, err := decodePos(from)
	if err != nil {
		return nil, err
	}
	startID := int64(0)
	if ok {
		startID = pos.LastID
	} else {
		// "From now" — anchor to the current MAX(id) so the stream
		// emits only changes captured AFTER this call.
		startID, err = readChangeLogMaxID(ctx, r.db, r.schema)
		if err != nil {
			return nil, fmt.Errorf("pgtrigger: stream: read MAX(id) start anchor: %w", err)
		}
	}

	out := make(chan ir.Change, cdcChannelBuffer)
	pumpCtx, cancel := context.WithCancel(ctx)
	r.pumpCancel = cancel

	go func() {
		defer close(out)
		r.pump(pumpCtx, startID, out)
	}()
	return out, nil
}

// pump is the polling-loop body. ADR-0066 §2 / §6: each iteration
// runs the safety-lag query (`txid < pg_snapshot_xmin(...)`) plus
// `id > $lastSeen` to fetch the next batch in commit-safe id order.
// When a poll returns exactly batchSize rows the next poll fires
// immediately so the back-pressure pull saturates against bursty
// sources without batch-cap throttling.
func (r *CDCReader) pump(ctx context.Context, startID int64, out chan<- ir.Change) {
	lastSeen := startID
	timer := time.NewTimer(0) // fire immediately on first iteration
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		fetched, newLast, ddl, err := r.poll(ctx, lastSeen)
		if err != nil {
			// Classify transients so the ADR-0038 pipeline retry loop reopens
			// the pump instead of terminating a long-running poll on a routine
			// blip. Two legs: PG SQLSTATE connection-availability transients
			// (57P01/57P02/57P03 + class 08 — a server restart / standby
			// promotion; the v0.99.286 tracked follow-up, closed via the
			// exported postgres.IsReadTransientSQLState predicate +
			// AsTransient, which exists for exactly this
			// caller-holds-the-structured-signal case) and the shared
			// network/transport shapes (connection reset, EOF, i/o timeout).
			// Anything else stays TERMINAL — notably a missing change-log
			// table (42P01), which is an operator/setup fault.
			r.setErr(classifyPollError(err))
			return
		}
		if ddl != "" {
			// §7 — refuse-loudly on observed DDL. The polling loop
			// terminates; the operator runs the drained-model
			// recovery (ADR-0054 hint).
			r.setErr(fmt.Errorf(
				"pgtrigger: observed source-side DDL (%s); the trigger engine refuses to forward DDL — drain the stream (`sluice sync stop --wait`), run `sluice migrate` on the target to land the schema change, then re-run `sluice sync start --reset-position`",
				ddl,
			))
			return
		}
		for _, ev := range fetched {
			select {
			case <-ctx.Done():
				return
			case out <- ev:
			}
		}
		if newLast > lastSeen {
			lastSeen = newLast
		}
		// Adaptive cadence: a full batch means the source is busy;
		// fire the next poll immediately so back-pressure has the
		// shortest possible feedback window. Otherwise wait the
		// configured interval.
		if len(fetched) == r.batchSize {
			timer.Reset(0)
		} else {
			timer.Reset(r.pollInterval)
		}
	}
}

// classifyPollError wraps a change-log poll failure for the pipeline retry
// loop: retriable when it carries a PG connection-availability SQLSTATE
// (57P01/57P02/57P03, class 08 — the structured leg the caller judges via
// the postgres predicate) or a shared transient transport shape; terminal
// otherwise. Pulled out of pump so both legs are pinned without a live
// poll loop.
func classifyPollError(err error) error {
	wrapped := fmt.Errorf("pgtrigger: poll: %w", err)
	if postgres.IsReadTransientSQLState(err) {
		return triggercdc.AsTransient(wrapped)
	}
	return triggercdc.ClassifyTransient(wrapped)
}

// pollQuery renders the one-poll fetch with the §2 safety-lag
// hold-back: `txid < pg_snapshot_xmin(pg_current_snapshot())` holds
// back any row whose allocating transaction is still in-flight.
// Without it an overlapping txn that allocated id=5 but committed
// AFTER id=6 would be skipped forever by a reader that observed id=6
// first (the watermark advances past 5 before 5 becomes visible).
//
// BOTH sides of the hold-back comparison live in the 64-bit
// epoch-carrying xid8 domain: `txid` is the allocating transaction's
// `pg_current_xact_id()::text::bigint`, recorded by the capture
// trigger at insert time (ADR-0066 §2 — the column exists precisely so
// this query can read it, and it has been NOT NULL since the engine's
// first release, so there are no legacy rows to special-case), and the
// right side is the poll snapshot's xmin through the same cast. The
// text → bigint cast keeps both values on the driver's int64 path (a
// JSON-number float64 would lose precision above 2^53).
//
// The row's system `xmin` column MUST NOT be used on the left side:
// xmin is the 32-bit epoch-LESS xid, while pg_snapshot_xmin() is
// epoch-carrying, so once the cluster's lifetime txid count crosses
// 2^32 (routine on long-lived busy PG) the cross-domain comparison is
// ALWAYS true and the hold-back silently stops — reopening the
// overlap-commit gap this predicate exists to prevent. That regression
// shipped (the original implementation compared `xmin::text::bigint`;
// live-confirmed on a pg_resetwal-epoch-bumped PG 16, 2026-07-08).
// Pinned by TestPollQuery_ComparesInXID8Domain and
// TestCDCReader_XIDEpochBump.
func pollQuery(tableRef string) string {
	return "SELECT id, txid, EXTRACT(epoch FROM committed_at)::bigint, schema_name, table_name, op, " +
		"pk_jsonb::text, before_jsonb::text, after_jsonb::text " +
		"FROM " + tableRef + " " +
		"WHERE id > $1 " +
		"  AND txid < pg_snapshot_xmin(pg_current_snapshot())::text::bigint " +
		"ORDER BY id ASC LIMIT $2"
}

// poll runs one safety-lag-bounded fetch (see [pollQuery] for the
// hold-back predicate and its xid8-domain rationale). Returns the
// events, the new high-water id, and (when non-empty) the
// human-readable DDL command tag that fired a refuse-loudly path. A
// zero-row, zero-DDL return is the steady-state "nothing new" shape.
func (r *CDCReader) poll(ctx context.Context, lastSeen int64) (events []ir.Change, newLast int64, ddl string, err error) {
	tableRef := quoteIdent(r.schema) + "." + quoteIdent(ChangeLogTable)
	q := pollQuery(tableRef)
	//nolint:rowserrcheck,sqlclosecheck // closed via defer below; linter can't track the early-return path
	rows, qErr := r.db.QueryContext(ctx, q, lastSeen, r.batchSize)
	if qErr != nil {
		return nil, lastSeen, "", qErr
	}
	defer func() { _ = rows.Close() }()
	newLast = lastSeen
	for rows.Next() {
		var (
			id         int64
			txid       int64
			committed  int64
			schema     string
			table      string
			op         string
			pkJSON     sql.NullString
			beforeJSON sql.NullString
			afterJSON  sql.NullString
		)
		if err := rows.Scan(&id, &txid, &committed, &schema, &table, &op, &pkJSON, &beforeJSON, &afterJSON); err != nil {
			return nil, lastSeen, "", fmt.Errorf("scan row: %w", err)
		}
		if id > newLast {
			newLast = id
		}

		// §7 DDL marker handling — short-circuit the loop and
		// surface the refusal to the pump.
		// Source commit timestamp for the engine-neutral sync-lag metric
		// (roadmap item 45): the change-log row's committed_at (projected
		// as epoch seconds) is the instant the captured transaction
		// committed on the source. Carry it onto every emitted change.
		commitTime := pgTriggerCommitTime(committed)

		if op == "X" {
			tag := decodeDDLTag(pkJSON.String)
			return nil, newLast, tag, nil
		}
		// Truncate handling.
		if op == "T" {
			pos, err := encodePos(pgTriggerPos{LastID: id})
			if err != nil {
				return nil, lastSeen, "", fmt.Errorf("encode position: %w", err)
			}
			events = append(events, ir.Truncate{Position: pos, Schema: schema, Table: table, CommitTime: commitTime})
			continue
		}

		pos, err := encodePos(pgTriggerPos{LastID: id})
		if err != nil {
			return nil, lastSeen, "", fmt.Errorf("encode position: %w", err)
		}
		pkRow, err := decodeJSONBRow(pkJSON.String)
		if err != nil {
			return nil, lastSeen, "", fmt.Errorf("decode pk_jsonb (id=%d): %w", id, err)
		}
		_ = pkRow // §2: pk_jsonb is part of before_jsonb / after_jsonb already

		var beforeRow, afterRow ir.Row
		if beforeJSON.Valid {
			beforeRow, err = decodeJSONBRow(beforeJSON.String)
			if err != nil {
				return nil, lastSeen, "", fmt.Errorf("decode before_jsonb (id=%d): %w", id, err)
			}
		}
		if afterJSON.Valid {
			afterRow, err = decodeJSONBRow(afterJSON.String)
			if err != nil {
				return nil, lastSeen, "", fmt.Errorf("decode after_jsonb (id=%d): %w", id, err)
			}
		}

		switch op {
		case "I":
			events = append(events, ir.Insert{Position: pos, Schema: schema, Table: table, Row: afterRow, CommitTime: commitTime})
		case "U":
			// `before`/`after` completeness is a deliberate
			// capture-payload mode choice (ADR-0068), NOT a REPLICA
			// IDENTITY artifact: a plpgsql trigger's OLD/NEW are ALWAYS
			// the full row regardless of REPLICA IDENTITY (that setting
			// governs only the WAL old-tuple for logical decoding — the
			// slot/pgoutput path, not trigger variables). So `before`
			// carries the full old row in `full`/`changed` modes and
			// PK-only in `minimal`; `after` carries the full new row in
			// `full` and PK ∪ changed-cols in `changed`/`minimal`. The
			// reader decodes whatever the change-log holds verbatim and
			// the applier builds its WHERE from `before` and SET from
			// `after` — both correct and idempotent for any of the
			// modes, with no reader/applier code change.
			events = append(events, ir.Update{Position: pos, Schema: schema, Table: table, Before: beforeRow, After: afterRow, CommitTime: commitTime})
		case "D":
			// Delete events carry only OLD; the applier's PK-only
			// path uses Before to identify the row.
			if beforeRow == nil {
				// Defensive — the row trigger always emits a Before
				// for DELETE. If we ever see a NULL it indicates a
				// driver-side mis-decode and the loud-failure path
				// is correct.
				return nil, lastSeen, "", fmt.Errorf("delete event id=%d has NULL before_jsonb", id)
			}
			events = append(events, ir.Delete{Position: pos, Schema: schema, Table: table, Before: beforeRow, CommitTime: commitTime})
		default:
			return nil, lastSeen, "", fmt.Errorf("unknown op %q at id=%d", op, id)
		}
		// txid is scanned (the SELECT projects it for schema-shape
		// stability with the trigger's audit table) but not consumed in
		// Go: ordering uses the bigserial id alone, and the safety-lag
		// hold-back consumes txid inside the SQL WHERE (see [pollQuery]).
		// It becomes load-bearing in Go if/when transactional batching
		// lands. committed is consumed above as the sync-lag commit
		// timestamp (roadmap item 45).
		_ = txid
	}
	if err := rows.Err(); err != nil {
		return nil, lastSeen, "", fmt.Errorf("iter rows: %w", err)
	}
	if len(events) > 0 {
		slog.DebugContext(
			ctx, "pgtrigger: poll batch",
			slog.Int("events", len(events)),
			slog.Int64("last_id", newLast),
		)
	}
	return events, newLast, "", nil
}

// decodeJSONBRow decodes a JSONB column value (as a TEXT-cast string)
// into an [ir.Row]. ADR-0066 §4 — `Decoder.UseNumber()` is set so PG's
// unbounded `numeric` round-trips through Go without losing precision
// (the Bug-74-class silent-loss this engine must not have).
//
// Returns nil for "" or "null" (PG's `NULL::jsonb::text` returns
// "null" while an actual SQL NULL surfaces via the caller's
// sql.NullString check).
// pgTriggerCommitTime maps the change-log row's committed_at epoch-seconds
// projection to the [ir.Change] source-commit-time the sync-lag metric
// consumes (roadmap item 45). A non-positive value — a row whose
// committed_at was somehow NULL/0 — maps to the zero time, which the metric
// treats as "unknown" and omits, never as "committed at the epoch".
func pgTriggerCommitTime(epochSeconds int64) time.Time {
	if epochSeconds <= 0 {
		return time.Time{}
	}
	return time.Unix(epochSeconds, 0).UTC()
}

func decodeJSONBRow(s string) (ir.Row, error) {
	if s == "" || s == "null" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewBufferString(s))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	// Convert json.Number leaves into typed values where the loss-
	// free conversion is unambiguous — RECURSIVELY through arrays, so
	// array ELEMENTS follow the same rule as scalars (RDS validation
	// F3, 2026-07-16: array leaves were left raw, so int[] elements
	// reached the applier as json.Number and crash-looped the stream).
	// Integers become int64; non-integer numerics stay json.Number so
	// the applier's prepareValue path sees the exact source
	// representation and re-parses against the target column type (§4).
	for k, v := range m {
		m[k] = normalizePayloadValue(v)
	}
	return ir.Row(m), nil
}

// normalizePayloadValue applies decodeJSONBRow's loss-free-only leaf
// rule to one decoded value, recursing into JSON arrays (which carry
// array-column elements — including nested levels of a multi-dim
// array). Three deliberate boundaries:
//
//   - Non-integer json.Number stays json.Number: parsing to float64
//     here would silently truncate numeric(p,s) precision (the
//     Bug-74-class loss ADR-0066 §4 exists to prevent). The applier
//     re-parses type-aware (scalars via pgx's text binding; array
//     elements via the postgres writer's convertArray leaf funcs).
//   - The literal "-0" stays json.Number even though Int64 succeeds:
//     int64(0) would silently drop a float sign bit. Defensive-only
//     today — PG's to_jsonb stores numbers as numeric (no signed
//     zero), so a live capture can never actually emit -0 (the
//     engine.go "negative zero" wart) — but the decode rule must not
//     be the layer that destroys a sign if the capture format ever
//     becomes sign-faithful.
//   - Objects (jsonb documents) are NOT descended: their leaves are
//     re-marshaled verbatim on apply, and encoding/json emits a
//     json.Number byte-identically, so rewriting them buys nothing. A
//     jsonb column whose top-level value is a JSON array is
//     indistinguishable from an array column here and IS normalized —
//     harmless for the same reason (int64 and integral json.Number
//     marshal identically).
func normalizePayloadValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		if x.String() == "-0" {
			return x
		}
		if i, err := x.Int64(); err == nil && !strings.ContainsAny(x.String(), ".eE") {
			return i
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = normalizePayloadValue(e)
		}
		return x
	}
	return v
}

// decodeDDLTag pulls the command_tag from the §7 DDL-marker row's
// pk_jsonb payload. Returns the empty string when the payload is
// missing or malformed (defensive — the operator should still see
// the refusal; we synthesise "DDL" if no tag is recoverable).
func decodeDDLTag(s string) string {
	if s == "" {
		return "DDL"
	}
	dec := json.NewDecoder(bytes.NewBufferString(s))
	var m map[string]string
	if err := dec.Decode(&m); err != nil {
		return "DDL"
	}
	if tag := m["command_tag"]; tag != "" {
		return tag
	}
	return "DDL"
}

// changeLogTableExists probes for the §2 table on the source. A
// missing relation surfaces as "relation does not exist" (PG SQLSTATE
// 42P01); the helper returns ok=false rather than the error so the
// caller can surface a polished refusal.
func changeLogTableExists(ctx context.Context, db *sql.DB, schema string) (bool, error) {
	const q = `
SELECT EXISTS (
    SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = $1
       AND n.nspname = $2
       AND c.relkind = 'r'
)`
	var exists bool
	if err := db.QueryRowContext(ctx, q, ChangeLogTable, schema).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// readChangeLogMaxID returns COALESCE(MAX(id), 0) from the change-log
// table. Used as the "from now" anchor when [StreamChanges] is called
// with the zero-value position.
func readChangeLogMaxID(ctx context.Context, db *sql.DB, schema string) (int64, error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(ChangeLogTable)
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT MAX(id) FROM "+tableRef).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

// anchorQuerier is the slice of database/sql readChangeLogAnchor needs.
// Both *sql.DB and a snapshot-pinned *sql.Conn satisfy it — the anchor
// MUST be read on the SAME connection/transaction the snapshot Rows are
// read on (see [readChangeLogAnchor]).
type anchorQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// anchorQuery renders the CDC-handoff anchor computation (semantics in
// [readChangeLogAnchor]). The `txid >=` arm must compare in the 64-bit
// epoch-carrying xid8 domain for the same reason as [pollQuery]: with
// the row's 32-bit epoch-less `xmin` on the left, the comparison
// against pg_snapshot_xmin() is false for EVERY row once the cluster's
// lifetime txid count crosses 2^32, the arm never matches, and
// COALESCE silently falls through to MAX(id) — the exact too-high
// anchor Bug 94 exists to prevent. Pinned by
// TestAnchorQuery_ComparesInXID8Domain and TestCDCReader_XIDEpochBump.
func anchorQuery(tableRef string) string {
	return `
SELECT COALESCE(
  (SELECT MIN(id) - 1 FROM ` + tableRef + `
     WHERE txid >= pg_snapshot_xmin(pg_current_snapshot())::text::bigint),
  (SELECT COALESCE(MAX(id), 0) FROM ` + tableRef + `)
)`
}

// readChangeLogAnchor computes the CDC handoff anchor: the highest
// change-log id we can prove is committed AND inside the reading
// transaction's snapshot. This is the load-bearing correctness point
// of the snapshot→CDC handoff (Bug 94).
//
// Why not MAX(id): the BIGSERIAL `id` is allocated at INSERT time but
// is NOT commit-ordered. A transaction can allocate a LOW id and commit
// AFTER a transaction that allocated a HIGHER id; rolled-back txns leave
// permanent id gaps. So a naive MAX(id) anchor risks a SILENT GAP — an
// in-flight txn's low id is masked by an already-committed higher id, so
// CDC (which replays `id > anchor`) skips the low id forever once it
// commits. Silent data loss is FORBIDDEN under the loud-failure tenet.
//
// The anchor is instead "(first not-provably-settled id) − 1, else
// MAX(id)" — see [anchorQuery] for the SQL. `txid >=
// pg_snapshot_xmin(current)` selects visible rows whose allocating
// transaction is NOT definitely-finished-before-our-snapshot — the
// rows the [pollQuery] steady-state hold-back would currently keep
// back. Anchoring below the FIRST such id means CDC replays all of
// them. Over-replay (anchor too LOW) is SAFE: the applier is
// idempotent (ADR-0010), so an event whose row is ALSO in the
// bulk-copy snapshot just re-applies to the same value. A GAP (anchor
// too HIGH) is silent loss and is forbidden.
//
// Worked example: committed id=6 whose txid is ≥ the snapshot's xmin
// (an older transaction was still running when it committed). The arm
// selects id=6 ⇒ anchor = 5 ⇒ CDC replays id=6 — a harmless
// idempotent re-apply — plus everything later, instead of trusting a
// MAX(id) that a not-yet-visible lower id may be hiding behind.
//
// MVCC BLIND SPOT — the invisible in-flight low-id window (epoch-
// independent; live-confirmed on PG 16, 2026-07-08; CLOSED at the
// handoff, see below): a change-log row INSERTed by a transaction
// still uncommitted when this query runs is INVISIBLE to it (MVCC),
// so the MIN arm cannot see — and therefore cannot anchor below — an
// in-flight txn's already-allocated id when that id is LOWER than
// every visible not-yet-settled row. Concretely: A inserts change-log
// id=1 and stays open; B inserts id=2 and commits; this query
// computes MIN(2)−1 = 1, but the gap-free anchor is 0 (A's id=1 is in
// neither the bulk-copy snapshot nor `id > 1`). This query therefore
// returns an anchor that is correct ONLY relative to rows visible in
// its snapshot; [Engine.OpenSnapshotStream] — the sole handoff caller
// — closes the blind spot by exporting the same snapshot's visibility
// horizon ([captureSnapshotText]), assigning a txid upper bound,
// waiting for every pre-bound transaction to settle (bounded, loud on
// timeout — [waitForPreSnapshotTxnsToSettle], which also states the
// full gap-freedom invariant and why the bound must be a freshly
// ASSIGNED xid rather than the snapshot's xmax or xip set), and
// clamping the anchor below the now-visible change-log ids the
// snapshot couldn't see ([minChangeLogIDForInvisibleTxns]). Any new
// caller anchoring a snapshot handoff MUST pair this query with that
// settle+clamp step. Pinned by
// TestSnapshotStream_InFlightTxnAnchor_NoGap.
//
// q MUST be the same connection/transaction the snapshot Rows read on,
// so `pg_current_snapshot()` reflects the snapshot the bulk copy sees.
func readChangeLogAnchor(ctx context.Context, q anchorQuerier, schema string) (int64, error) {
	query := anchorQuery(quoteIdent(schema) + "." + quoteIdent(ChangeLogTable))
	var anchor int64
	if err := q.QueryRowContext(ctx, query).Scan(&anchor); err != nil {
		return 0, err
	}
	if anchor < 0 {
		// MIN(id) - 1 can only go negative if the lowest in-flight id is
		// 0, which BIGSERIAL never allocates (it starts at 1). Clamp
		// defensively so the position decoder's last_id >= 0 invariant
		// holds; anchoring at 0 replays everything (safe over-replay).
		anchor = 0
	}
	return anchor, nil
}

// Compile-time check that [CDCReader] implements [ir.CDCReader] with
// the addition of an Err method (the load-bearing loud-failure
// surface for streaming readers — see [ir.RowReader] Err doc).
var _ ir.CDCReader = (*CDCReader)(nil)
