// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// Defaults for the polling loop (ADR-0135 §3 — the pgtrigger defaults). Phase 1
// hardcodes them; --poll-interval flows through [CDCReader.SetPollInterval].
const (
	defaultPollInterval = 1 * time.Second
	defaultBatchSize    = 10000
	cdcChannelBuffer    = 256
)

// committedAtLayout is the change-log captured_at format produced by the trigger
// (`strftime('%Y-%m-%d %H:%M:%f', 'now')`, UTC). Parsed back for the sync-lag
// metric; a parse failure maps to the zero time ("unknown"), never a guess.
const committedAtLayout = "2006-01-02 15:04:05.999"

// CDCReader is the trigger-engine CDC reader. It polls `sluice_change_log` at a
// configurable cadence (default 1s) and emits [ir.Change] events via the channel
// returned from [StreamChanges].
//
// One reader → one [StreamChanges] call. Concurrent calls are not supported; the
// polling goroutine owns the underlying *sql.DB pool for the lifetime of the
// stream. The reader emits NO [ir.TxBegin]/[ir.TxCommit] markers (a change-log
// row carries no source-transaction grouping), so it is a marker-less stream —
// exactly like pgtrigger; the Streamer's checkpoint cadence persists the
// watermark (the pgtrigger Bug-159 contract applies identically here).
type CDCReader struct {
	db  *sql.DB
	dec *sqlite.CapturedCellDecoder

	// colTypes maps table → column-name → resolved IR type, read once at open
	// from the validated SQLite schema reader so each captured cell decodes
	// through the SAME storage-class-faithful path as a cold-start row.
	colTypes map[string]map[string]ir.Type

	pollInterval time.Duration
	batchSize    int

	// pumpCancel cancels the polling goroutine when Close is called.
	pumpCancel context.CancelFunc

	mu  sync.Mutex
	err error
}

// openCDCReader constructs a [CDCReader] bound to dsn (a SQLite file path/URI).
// It resolves the per-source date/bool policy, reads the schema once to build
// the column-type lookup, opens a read-only poll connection, and refuses loudly
// when the change-log table is absent — the operator forgot to run
// `sluice trigger setup`. The refusal fires at open time so the streamer
// surfaces it before any data moves.
func openCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	dec, err := sqlite.NewCapturedCellDecoderForDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: cdc: resolve date encoding: %w", err)
	}
	colTypes, err := loadColumnTypes(ctx, dsn)
	if err != nil {
		return nil, err
	}

	db, _, err := sqlite.OpenFile(ctx, dsn, true)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: cdc open: %w", err)
	}
	if exists, err := changeLogTableExists(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite-trigger: cdc preflight: %w", err)
	} else if !exists {
		_ = db.Close()
		return nil, fmt.Errorf(
			"sqlite-trigger: %s does not exist on the source — run `sluice trigger setup --source-driver sqlite-trigger --dsn=... --tables=...` before starting the stream",
			ChangeLogTable,
		)
	}
	return &CDCReader{
		db:           db,
		dec:          dec,
		colTypes:     colTypes,
		pollInterval: defaultPollInterval,
		batchSize:    defaultBatchSize,
	}, nil
}

// loadColumnTypes reads the SQLite schema once and builds table → column → IR
// type, reusing the validated [sqlite.Engine] schema reader so the CDC reader's
// per-column typing matches the cold-start reader exactly. The change-log/meta
// tables are already skipped by that reader (ADR-0135).
func loadColumnTypes(ctx context.Context, dsn string) (map[string]map[string]ir.Type, error) {
	sr, err := (sqlite.Engine{}).OpenSchemaReader(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: cdc: open schema reader: %w", err)
	}
	defer func() { _ = closeReader(sr) }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: cdc: read schema: %w", err)
	}
	out := make(map[string]map[string]ir.Type, len(schema.Tables))
	for _, t := range schema.Tables {
		cols := make(map[string]ir.Type, len(t.Columns))
		for _, c := range t.Columns {
			cols[c.Name] = c.Type
		}
		out[t.Name] = cols
	}
	return out, nil
}

// SetPollInterval overrides the default 1s poll cadence. Idempotent; must be
// called before [StreamChanges]. A zero/negative duration is rejected so the
// loop never busy-spins. Surfaced via a setter (rather than the Engine signature)
// so the streamer's pollIntervalSetter type-assertion drives it — same contract
// as pgtrigger (ADR-0066 §6).
func (r *CDCReader) SetPollInterval(d time.Duration) {
	if d > 0 {
		r.pollInterval = d
	}
}

// Close releases the underlying connection pool and stops any in-flight polling
// goroutine.
func (r *CDCReader) Close() error {
	if r.pumpCancel != nil {
		r.pumpCancel()
	}
	if r.db != nil {
		err := r.db.Close()
		r.db = nil
		return err
	}
	return nil
}

// Err returns the most recent error the polling goroutine recorded. Callers MUST
// consult Err after the channel returned by [StreamChanges] closes — a poll-time
// decode failure is the engine's loud-failure surface.
func (r *CDCReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// setErr records the first terminal error from the pump goroutine (so the
// operator sees the root cause, not a downstream effect).
func (r *CDCReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// StreamChanges opens the polling loop. The returned channel emits [ir.Change]
// events in change-log id order.
//
// from carries the durable resume bookmark — the last successfully-applied
// change-log id. The zero-value [ir.Position] means "from now": the reader
// anchors at the current MAX(id) and emits only changes captured AFTER this call
// (mirror of the other engines' "from now" semantics).
//
// On context cancel the goroutine drains in-flight rows, closes the channel, and
// stops. On a poll failure the channel closes and Err returns the failure.
func (r *CDCReader) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	pos, ok, err := decodePos(from)
	if err != nil {
		return nil, err
	}
	startID := int64(0)
	if ok {
		startID = pos.LastID
	} else {
		startID, err = readChangeLogMaxID(ctx, r.db)
		if err != nil {
			return nil, fmt.Errorf("sqlite-trigger: stream: read MAX(id) start anchor: %w", err)
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

// pump is the polling-loop body. Each iteration fetches the next id-ordered
// batch. SQLite serialises writers, so the change-log id is allocated in COMMIT
// order and a plain `id > lastSeen` scan is gap-free — NO safety-lag predicate
// is needed (the load-bearing simplification over pgtrigger, whose PG bigserial
// can commit out of allocation order; see ADR-0135 §3 / readChangeLogAnchor).
// When a poll returns a full batch the next poll fires immediately so a bursty
// source isn't throttled by the cadence.
func (r *CDCReader) pump(ctx context.Context, startID int64, out chan<- ir.Change) {
	lastSeen := startID
	timer := time.NewTimer(0) // fire immediately on the first iteration
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		events, newLast, err := r.poll(ctx, lastSeen)
		if err != nil {
			r.setErr(fmt.Errorf("sqlite-trigger: poll: %w", err))
			return
		}
		for _, ev := range events {
			select {
			case <-ctx.Done():
				return
			case out <- ev:
			}
		}
		if newLast > lastSeen {
			lastSeen = newLast
		}
		if len(events) == r.batchSize {
			timer.Reset(0)
		} else {
			timer.Reset(r.pollInterval)
		}
	}
}

// poll runs one id-ordered fetch and decodes each change-log row into an
// [ir.Change]. A zero-row return is the steady-state "nothing new" shape.
func (r *CDCReader) poll(ctx context.Context, lastSeen int64) (events []ir.Change, newLast int64, err error) {
	const q = "SELECT id, op, tbl, before, after, captured_at FROM " +
		`"` + ChangeLogTable + `" WHERE id > ? ORDER BY id ASC LIMIT ?`
	//nolint:rowserrcheck,sqlclosecheck // closed via defer below; linter can't track the early-return path
	rows, qErr := r.db.QueryContext(ctx, q, lastSeen, r.batchSize)
	if qErr != nil {
		return nil, lastSeen, qErr
	}
	defer func() { _ = rows.Close() }()
	newLast = lastSeen
	for rows.Next() {
		var (
			id         int64
			op, tbl    string
			beforeJSON sql.NullString
			afterJSON  sql.NullString
			capturedAt sql.NullString
		)
		if err := rows.Scan(&id, &op, &tbl, &beforeJSON, &afterJSON, &capturedAt); err != nil {
			return nil, lastSeen, fmt.Errorf("scan row: %w", err)
		}
		if id > newLast {
			newLast = id
		}
		ev, err := r.buildChange(id, op, tbl, beforeJSON, afterJSON, commitTime(capturedAt))
		if err != nil {
			return nil, lastSeen, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, lastSeen, fmt.Errorf("iter rows: %w", err)
	}
	if len(events) > 0 {
		slog.DebugContext(ctx, "sqlite-trigger: poll batch",
			slog.Int("events", len(events)), slog.Int64("last_id", newLast))
	}
	return events, newLast, nil
}

// buildChange decodes one change-log row into the appropriate [ir.Change],
// reconstructing the faithful before/after images via the shared decoder.
func (r *CDCReader) buildChange(id int64, op, tbl string, beforeJSON, afterJSON sql.NullString, ct time.Time) (ir.Change, error) {
	pos, err := encodePos(sqliteTriggerPos{LastID: id})
	if err != nil {
		return nil, fmt.Errorf("encode position (id=%d): %w", id, err)
	}
	var before, after ir.Row
	if beforeJSON.Valid {
		if before, err = r.decodeImage(tbl, beforeJSON.String, id); err != nil {
			return nil, err
		}
	}
	if afterJSON.Valid {
		if after, err = r.decodeImage(tbl, afterJSON.String, id); err != nil {
			return nil, err
		}
	}

	switch op {
	case "I":
		return ir.Insert{Position: pos, Table: tbl, Row: after, CommitTime: ct}, nil
	case "U":
		return ir.Update{Position: pos, Table: tbl, Before: before, After: after, CommitTime: ct}, nil
	case "D":
		if before == nil {
			// Defensive — the DELETE trigger always records OLD. A NULL here is
			// a corrupted change-log row; refuse loudly rather than apply a
			// delete with no WHERE.
			return nil, fmt.Errorf("delete event id=%d has NULL before image", id)
		}
		return ir.Delete{Position: pos, Table: tbl, Before: before, CommitTime: ct}, nil
	default:
		return nil, fmt.Errorf("unknown op %q at id=%d", op, id)
	}
}

// capturedCell is one column's captured (typeof, value) pair, as written by the
// trigger's `json_object('t', typeof(c), 'v', <value-expr>)`. V is the EXACT
// text/hex (a JSON string) for a non-NULL cell, or JSON null for a NULL cell.
type capturedCell struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

// decodeImage parses a captured before/after JSON image and reconstructs each
// column's faithful IR value via the shared decoder + the per-column IR type.
// An empty/`null` image yields a nil Row. A column absent from the schema (a
// source schema change since `trigger setup`) is refused loudly — Phase 1 does
// not forward schema changes (ADR-0135); the operator must re-run setup.
func (r *CDCReader) decodeImage(table, jsonText string, id int64) (ir.Row, error) {
	if jsonText == "" || jsonText == "null" {
		return nil, nil
	}
	var outer map[string]capturedCell
	dec := json.NewDecoder(strings.NewReader(jsonText))
	if err := dec.Decode(&outer); err != nil {
		return nil, fmt.Errorf("sqlite-trigger: table %q id=%d: decode captured image: %w", table, id, err)
	}
	if len(outer) == 0 {
		return nil, nil
	}
	types, ok := r.colTypes[table]
	if !ok {
		return nil, fmt.Errorf(
			"sqlite-trigger: captured table %q has no schema (it was dropped or renamed since `trigger setup`); "+
				"re-run setup after a schema change (Phase 1 does not forward DDL)", table,
		)
	}
	row := make(ir.Row, len(outer))
	for col, cell := range outer {
		t, ok := types[col]
		if !ok {
			return nil, fmt.Errorf(
				"sqlite-trigger: table %q column %q is not in the current schema (a column was added/removed since "+
					"`trigger setup`); re-run setup after a schema change", table, col,
			)
		}
		v, err := r.dec.Decode(cell.T, cell.V, t)
		if err != nil {
			return nil, fmt.Errorf("sqlite-trigger: table %q column %q id=%d: %w", table, col, id, err)
		}
		row[col] = v
	}
	return row, nil
}

// commitTime parses the change-log captured_at into the [ir.Change] source-commit
// time the sync-lag metric consumes. A NULL/unparseable value maps to the zero
// time, which the metric treats as "unknown" and omits — never a misleading
// epoch (the *Known honesty contract).
func commitTime(capturedAt sql.NullString) time.Time {
	if !capturedAt.Valid {
		return time.Time{}
	}
	tm, err := time.Parse(committedAtLayout, strings.TrimSpace(capturedAt.String))
	if err != nil {
		return time.Time{}
	}
	return tm.UTC()
}

// changeLogTableExists reports whether the change-log table is present.
func changeLogTableExists(ctx context.Context, db *sql.DB) (bool, error) {
	var name string
	err := db.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", ChangeLogTable).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// readChangeLogMaxID returns COALESCE(MAX(id), 0) — the "from now" / snapshot
// anchor.
func readChangeLogMaxID(ctx context.Context, db *sql.DB) (int64, error) {
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(id) FROM "`+ChangeLogTable+`"`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

// Compile-time check that [CDCReader] implements [ir.CDCReader].
var _ ir.CDCReader = (*CDCReader)(nil)
