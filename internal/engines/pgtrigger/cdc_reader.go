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
	// The gap-free ordering rests on the change-log id sequence being an
	// untouched BIGSERIAL — CACHE 1, MINVALUE 1, INCREMENT 1, NO CYCLE
	// (see cdc_gapfree.go's premises). Preflight it here so a source
	// someone ALTERed refuses at open time rather than dropping changes
	// mid-stream.
	if err := verifyChangeLogSequence(ctx, db, cfg.schema); err != nil {
		_ = db.Close()
		return nil, err
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

// pump is the polling-loop body. Each iteration fetches the next batch
// window in id order and consumes the CONTIGUOUS committed run out of
// it (cdc_gapfree.go — the invariant, and why a per-row hold-back is
// not enough). A hole in that run stops the watermark until the hole
// either fills (the allocating transaction committed) or is PROVEN
// permanent by [holeGuard] (it aborted). When a poll returns exactly
// batchSize events the next poll fires immediately so the
// back-pressure pull saturates against bursty sources without batch-cap
// throttling.
func (r *CDCReader) pump(ctx context.Context, startID int64, out chan<- ir.Change) {
	lastSeen := startID
	var holes holeGuard
	timer := time.NewTimer(0) // fire immediately on first iteration
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		// Step 3 of the abort proof runs BEFORE this poll's snapshot: a
		// settle probe taken AFTER the observation it licenses would
		// admit a transaction that committed in between, and skipping
		// there is the silent loss the guard exists to prevent.
		if err := holes.refresh(ctx, r.db); err != nil {
			slog.DebugContext(ctx, "pgtrigger: change-log gap settle probe failed; the stream keeps waiting",
				slog.Any("err", err))
		}
		b, err := r.poll(ctx, lastSeen)
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
		if b.ddl != "" {
			// §7 — refuse-loudly on observed DDL. The polling loop
			// terminates; the operator runs the drained-model
			// recovery (ADR-0054 hint).
			r.setErr(fmt.Errorf(
				"pgtrigger: observed source-side DDL (%s); the trigger engine refuses to forward DDL — drain the stream (`sluice sync stop --wait`), run `sluice migrate` on the target to land the schema change, then re-run `sluice sync start --restart-from-scratch` (there is no --reset-position flag; --restart-from-scratch is what discards the persisted position and re-copies from the beginning)",
				b.ddl,
			))
			return
		}
		for _, ev := range b.events {
			select {
			case <-ctx.Done():
				return
			case out <- ev:
			}
		}
		if b.lastID > lastSeen {
			lastSeen = b.lastID
		}
		if b.holeAt > 0 {
			if next, ok := r.resolveHole(ctx, &holes, b); ok {
				lastSeen = next
				timer.Reset(0)
				continue
			}
		} else {
			holes.clear()
		}
		// Adaptive cadence: a full batch means the source is busy;
		// fire the next poll immediately so back-pressure has the
		// shortest possible feedback window. Otherwise wait the
		// configured interval.
		if len(b.events) == r.batchSize {
			timer.Reset(0)
		} else {
			timer.Reset(r.pollInterval)
		}
	}
}

// resolveHole runs one round of the gap-freedom hole protocol for a
// poll that stopped short of the window's end, and reports the
// watermark the stream may jump to when the hole is PROVEN permanent
// (its allocating transaction aborted). Otherwise the stream simply
// waits: over-holding is safe, skipping an unproven hole is silent
// loss. See [holeGuard] for the ordering that makes the proof sound.
func (r *CDCReader) resolveHole(ctx context.Context, holes *holeGuard, b pollBatch) (int64, bool) {
	now := time.Now()
	holes.observe(b.holeAt, now)
	if to, ok := holes.skipTo(b.holeAt, b.holeEnd); ok {
		slog.InfoContext(
			ctx, "pgtrigger: skipping a permanently-absent change-log id range (the transactions that allocated it rolled back; every transaction that could still have committed one has settled)",
			slog.Int64("from_id", b.holeAt),
			slog.Int64("through_id", to),
		)
		return to, true
	}
	if holes.needsBound() {
		// Assign the xid bound AFTER the observation above (step 2).
		bound, err := captureTxidUpperBound(ctx, r.db)
		if err != nil {
			slog.WarnContext(
				ctx, "pgtrigger: cannot assign the txid bound that proves a change-log gap permanent; the stream will keep waiting at the gap rather than risk skipping a change",
				slog.Int64("change_log_id", b.holeAt),
				slog.Any("err", err),
			)
		} else {
			holes.arm(bound, b.seenTo)
		}
	}
	holes.warnStuck(ctx, now)
	return 0, false
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

// pollQuery renders the one-poll fetch: the next batch WINDOW of the
// change log in id order, truncated at the shared settled ceiling
// ([settledCeilingSQL] — the same expression the cold-start anchor
// computes, over the window instead of over the whole table).
//
// That ceiling is the conservative arm only. The load-bearing rule for
// gap-freedom is the CONTIGUOUS run [CDCReader.poll] walks out of this
// window: an id missing from the window belongs to a transaction that
// has not committed, and consuming past it is the silent loss
// cdc_gapfree.go documents. Rows beyond the first hole are still
// fetched — an id above a hole is what proves the hole's row was
// already allocated — but not consumed.
//
// The window is MATERIALIZED so both ceiling arms aggregate over the
// LIMIT-ed batch instead of re-scanning the change-log tail; without it
// a multi-million-row catch-up backlog makes every poll O(backlog).
// MATERIALIZED is PG 12+; the engine's floor is PG 13.
func pollQuery(tableRef string) string {
	return "WITH " + changeLogWindow + " AS MATERIALIZED (\n" +
		"  SELECT id, txid, EXTRACT(epoch FROM committed_at)::bigint AS committed_epoch,\n" +
		"         schema_name, table_name, op,\n" +
		"         pk_jsonb::text AS pk_text, before_jsonb::text AS before_text,\n" +
		"         after_jsonb::text AS after_text\n" +
		"    FROM " + tableRef + "\n" +
		"   WHERE id > $1 ORDER BY id ASC LIMIT $2\n" +
		")\n" +
		"SELECT id, txid, committed_epoch, schema_name, table_name, op, pk_text, before_text, after_text\n" +
		"  FROM " + changeLogWindow + "\n" +
		" WHERE id <= " + settledCeilingSQL(changeLogWindow) + "\n" +
		" ORDER BY id ASC"
}

// pollBatch is one poll's outcome. lastID is the watermark the stream
// may durably advance to — the end of the contiguous committed run, NOT
// the highest id fetched — and holeAt/holeEnd/seenTo carry what this
// poll learned about the first gap so [holeGuard] can choose between
// waiting and skipping.
type pollBatch struct {
	events  []ir.Change
	lastID  int64  // end of the contiguous run this poll consumed
	holeAt  int64  // lowest id > lastID missing from the window (0 = the run reached the window's end)
	holeEnd int64  // lowest VISIBLE id above holeAt — the row that proves holeAt was allocated
	seenTo  int64  // highest id observed in this poll's window
	ddl     string // non-empty → §7 refuse-loudly DDL marker
}

// poll runs one bounded fetch and consumes the contiguous committed run
// out of it (see [pollQuery] and cdc_gapfree.go for why contiguity, not
// a per-row predicate, is what makes the watermark gap-free). A
// zero-event, zero-hole, zero-DDL return is the steady-state "nothing
// new" shape.
func (r *CDCReader) poll(ctx context.Context, lastSeen int64) (pollBatch, error) {
	tableRef := quoteIdent(r.schema) + "." + quoteIdent(ChangeLogTable)
	q := pollQuery(tableRef)
	b := pollBatch{lastID: lastSeen, seenTo: lastSeen}
	//nolint:rowserrcheck,sqlclosecheck // closed via defer below; linter can't track the early-return path
	rows, qErr := r.db.QueryContext(ctx, q, lastSeen, r.batchSize)
	if qErr != nil {
		return pollBatch{}, qErr
	}
	defer func() { _ = rows.Close() }()
	// want is the next id the contiguous run needs. The first row that
	// is not it opens the hole; from there the loop keeps scanning to
	// extend seenTo (the abort proof's upper bound) and decodes nothing.
	want := lastSeen + 1
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
			return pollBatch{}, fmt.Errorf("scan row: %w", err)
		}
		if id > b.seenTo {
			b.seenTo = id
		}
		if b.holeAt > 0 {
			// Past the first hole: this row is only evidence that the
			// hole's id was already allocated. Do not decode or consume it.
			continue
		}
		if id != want {
			b.holeAt, b.holeEnd = want, id
			continue
		}
		want = id + 1

		// §7 DDL marker handling — short-circuit the loop and
		// surface the refusal to the pump.
		// Source commit timestamp for the engine-neutral sync-lag metric
		// (roadmap item 45): the change-log row's committed_at (projected
		// as epoch seconds) is the instant the captured transaction
		// committed on the source. Carry it onto every emitted change.
		commitTime := pgTriggerCommitTime(committed)

		if op == "X" {
			b.ddl = decodeDDLTag(pkJSON.String)
			return b, nil
		}
		// Truncate handling.
		if op == "T" {
			pos, err := encodePos(pgTriggerPos{LastID: id})
			if err != nil {
				return pollBatch{}, fmt.Errorf("encode position: %w", err)
			}
			b.events = append(b.events, ir.Truncate{Position: pos, Schema: schema, Table: table, CommitTime: commitTime})
			b.lastID = id
			continue
		}

		pos, err := encodePos(pgTriggerPos{LastID: id})
		if err != nil {
			return pollBatch{}, fmt.Errorf("encode position: %w", err)
		}
		pkRow, err := decodeJSONBRow(pkJSON.String)
		if err != nil {
			return pollBatch{}, fmt.Errorf("decode pk_jsonb (id=%d): %w", id, err)
		}
		_ = pkRow // §2: pk_jsonb is part of before_jsonb / after_jsonb already

		var beforeRow, afterRow ir.Row
		if beforeJSON.Valid {
			beforeRow, err = decodeJSONBRow(beforeJSON.String)
			if err != nil {
				return pollBatch{}, fmt.Errorf("decode before_jsonb (id=%d): %w", id, err)
			}
		}
		if afterJSON.Valid {
			afterRow, err = decodeJSONBRow(afterJSON.String)
			if err != nil {
				return pollBatch{}, fmt.Errorf("decode after_jsonb (id=%d): %w", id, err)
			}
		}

		switch op {
		case "I":
			b.events = append(b.events, ir.Insert{Position: pos, Schema: schema, Table: table, Row: afterRow, CommitTime: commitTime})
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
			b.events = append(b.events, ir.Update{Position: pos, Schema: schema, Table: table, Before: beforeRow, After: afterRow, CommitTime: commitTime})
		case "D":
			// Delete events carry only OLD; the applier's PK-only
			// path uses Before to identify the row.
			if beforeRow == nil {
				// Defensive — the row trigger always emits a Before
				// for DELETE. If we ever see a NULL it indicates a
				// driver-side mis-decode and the loud-failure path
				// is correct.
				return pollBatch{}, fmt.Errorf("delete event id=%d has NULL before_jsonb", id)
			}
			b.events = append(b.events, ir.Delete{Position: pos, Schema: schema, Table: table, Before: beforeRow, CommitTime: commitTime})
		default:
			return pollBatch{}, fmt.Errorf("unknown op %q at id=%d", op, id)
		}
		b.lastID = id
		// txid is scanned (the SELECT projects it for schema-shape
		// stability with the trigger's audit table) but not consumed in
		// Go: the watermark rides the bigserial id alone, and the
		// settled-ceiling arm consumes txid inside the SQL (see
		// [pollQuery] / [settledCeilingSQL]). It becomes load-bearing in
		// Go if/when transactional batching lands. committed is consumed
		// above as the sync-lag commit timestamp (roadmap item 45).
		_ = txid
	}
	if err := rows.Err(); err != nil {
		return pollBatch{}, fmt.Errorf("iter rows: %w", err)
	}
	if len(b.events) > 0 || b.holeAt > 0 {
		slog.DebugContext(
			ctx, "pgtrigger: poll batch",
			slog.Int("events", len(b.events)),
			slog.Int64("last_id", b.lastID),
			slog.Int64("hole_at", b.holeAt),
			slog.Int64("seen_to", b.seenTo),
		)
	}
	return b, nil
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
// [readChangeLogAnchor]) as the shared settled ceiling over the WHOLE
// change log. It is the same [settledCeilingSQL] expression the
// steady-state [pollQuery] applies to its batch window — deliberately,
// so the two cannot drift back into encoding two different rules (see
// cdc_gapfree.go's header for the drift that shipped, and
// TestPollAndAnchorShareTheSettledCeiling for the pin).
func anchorQuery(tableRef string) string {
	return "SELECT " + settledCeilingSQL(tableRef)
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
// rows the steady-state poll's shared ceiling ([settledCeilingSQL])
// would keep back too. Anchoring below the FIRST such id means CDC replays all of
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
		// Defensive floor for the position codec's last_id >= 0
		// invariant; anchoring at 0 replays everything (safe
		// over-replay).
		//
		// CORRECTION (2026-08-07): this used to read "MIN(id) - 1 can
		// only go negative if the lowest in-flight id is 0, which
		// BIGSERIAL never allocates (it starts at 1)" — asserted, never
		// checked, and FALSE of a tampered sequence. `ALTER SEQUENCE …
		// MINVALUE 0 RESTART WITH 0` really does allocate id 0, and
		// `MINVALUE -100 RESTART WITH -100` really does allocate
		// negative ids (both ground-truthed on PG 16), at which point
		// this clamp silently declares them already-covered. The
		// premise is now PREFLIGHTED rather than asserted —
		// [verifyChangeLogSequence] refuses a MINVALUE below 1 at every
		// CDC open — and pinned by
		// TestCDCReader_RefusesSequenceConfigThatCanReissueIDs plus the
		// defect proof TestChangeLogAnchor_MinValueZeroStrandsTheChange.
		anchor = 0
	}
	return anchor, nil
}

// Compile-time check that [CDCReader] implements [ir.CDCReader] with
// the addition of an Err method (the load-bearing loud-failure
// surface for streaming readers — see [ir.RowReader] Err doc).
var _ ir.CDCReader = (*CDCReader)(nil)
