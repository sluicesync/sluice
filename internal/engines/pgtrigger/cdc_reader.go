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

	"github.com/orware/sluice/internal/engines/postgres"
	"github.com/orware/sluice/internal/ir"
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

	// mu guards err. The pump writes; the caller reads via Err.
	mu  sync.Mutex
	err error
}

// openCDCReader constructs a [CDCReader] bound to dsn. The reader's
// own *sql.DB pool is opened here so Close can release it cleanly;
// the embedded postgres.Engine's connection lifecycle is not shared.
//
// Refuses with a clear error when `sluice_change_log` is absent —
// the operator forgot to run `sluice trigger setup`. The refusal
// fires at open time so the streamer surfaces it before any data
// would move.
func openCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	cfg, err := parseDSNCompat(dsn)
	if err != nil {
		return nil, err
	}
	db, err := postgres.OpenPgxDB(cfg.dsn)
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

// Close releases the underlying connection pool and stops any
// in-flight polling goroutine.
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
// runs the safety-lag query (`xmin < pg_snapshot_xmin(...)`) plus
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
			r.setErr(fmt.Errorf("pgtrigger: poll: %w", err))
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

// poll runs one safety-lag-bounded fetch. Returns the events, the
// new high-water id, and (when non-empty) the human-readable DDL
// command tag that fired a refuse-loudly path. A zero-row, zero-DDL
// return is the steady-state "nothing new" shape.
func (r *CDCReader) poll(ctx context.Context, lastSeen int64) (events []ir.Change, newLast int64, ddl string, err error) {
	// §2 safety-lag query: `xmin < pg_snapshot_xmin(pg_current_snapshot())`
	// holds back any row whose allocating txid is still in-flight.
	// Without this an overlapping txn that allocated id=5 but
	// committed AFTER id=6 would be skipped by a reader that
	// observed id=6 first.
	//
	// `xmin::text::bigint` is the standard portable shape: PG 12's
	// xmin is xid (32-bit, wraparound); the JSON Number we'd surface
	// later would lose precision on the 32-bit edge. Cast to text →
	// bigint to keep the comparison stable across PG versions.
	tableRef := quoteIdent(r.schema) + "." + quoteIdent(ChangeLogTable)
	q := "SELECT id, txid, EXTRACT(epoch FROM committed_at)::bigint, schema_name, table_name, op, " +
		"pk_jsonb::text, before_jsonb::text, after_jsonb::text " +
		"FROM " + tableRef + " " +
		"WHERE id > $1 " +
		"  AND xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())::text::bigint " +
		"ORDER BY id ASC LIMIT $2"
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
			events = append(events, ir.Truncate{Position: pos, Schema: schema, Table: table})
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
			events = append(events, ir.Insert{Position: pos, Schema: schema, Table: table, Row: afterRow})
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
			events = append(events, ir.Update{Position: pos, Schema: schema, Table: table, Before: beforeRow, After: afterRow})
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
			events = append(events, ir.Delete{Position: pos, Schema: schema, Table: table, Before: beforeRow})
		default:
			return nil, lastSeen, "", fmt.Errorf("unknown op %q at id=%d", op, id)
		}
		_ = txid
		_ = committed
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
	// free conversion is unambiguous. Integers stay int64; non-
	// integer numerics stay json.Number so the applier's prepareValue
	// path sees the exact source representation.
	for k, v := range m {
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil && !strings.ContainsAny(n.String(), ".eE") {
				m[k] = i
				continue
			}
			// Leave as json.Number — preserves precision for
			// numeric(p,s) round-trip. The applier consults the
			// target column type's prepareValue and re-parses if
			// needed (§4).
			m[k] = n
		}
	}
	return ir.Row(m), nil
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

// readChangeLogAnchor computes the CDC handoff anchor as the CONTIGUOUS
// COMMITTED-PREFIX high-water of the capture log. This is the
// load-bearing correctness point of the snapshot→CDC handoff (Bug 94).
//
// Why not MAX(id): the BIGSERIAL `id` is allocated at INSERT time but
// is NOT commit-ordered. A transaction can allocate a LOW id and commit
// AFTER a transaction that allocated a HIGHER id; rolled-back txns leave
// permanent id gaps. So a naive MAX(id) anchor risks a SILENT GAP — an
// in-flight txn's low id is masked by an already-committed higher id, so
// CDC (which replays `id > anchor`) skips the low id forever once it
// commits. Silent data loss is FORBIDDEN under the loud-failure tenet.
//
// The anchor is instead "(first not-yet-safe id) − 1, else MAX(id) when
// nothing is in-flight":
//
//	SELECT COALESCE(
//	  (SELECT MIN(id) - 1 FROM <schema>.sluice_change_log
//	     WHERE xmin::text::bigint >= pg_snapshot_xmin(pg_current_snapshot())::text::bigint),
//	  (SELECT COALESCE(MAX(id), 0) FROM <schema>.sluice_change_log)
//	)
//
// `xmin >= pg_snapshot_xmin(current)` selects rows whose allocating txid
// is NOT yet definitely-committed-old relative to the reading
// transaction's snapshot — i.e. the rows the §2 steady-state safety-lag
// predicate (`xmin < pg_snapshot_xmin`) would currently hold back. The
// FIRST such id minus one is the highest id we can prove is committed
// AND in our REPEATABLE READ snapshot. Everything ≤ anchor is
// committed-old (in the snapshot, copied by Rows); everything > anchor
// is replayed by CDC.
//
// Over-replay (anchor too LOW) is SAFE: the applier is idempotent
// (ADR-0010), so an event whose row is ALSO in the bulk-copy snapshot
// just re-applies to the same value. A GAP (anchor too HIGH) is silent
// loss and is forbidden. When in doubt the formula anchors LOWER (it
// subtracts one from the first in-flight id rather than trusting MAX).
//
// Worked example (the one the task asks be pinned in a comment):
// in-flight id=5 (its txid still ≥ snapshot xmin) + committed id=6.
// MIN(id) WHERE in-flight = 5 ⇒ anchor = 5 − 1 = 4. CDC replays id > 4,
// i.e. BOTH 5 and 6. id=6 is also in the REPEATABLE READ snapshot and
// thus already bulk-copied, but the idempotent applier makes the
// re-apply a no-op. id=5 commits and is replayed by CDC. No gap. A
// MAX(id)=6 anchor would have skipped id=5 forever once it committed —
// the silent-loss bug this formula exists to prevent.
//
// q MUST be the same connection/transaction the snapshot Rows read on,
// so `pg_current_snapshot()` reflects the snapshot the bulk copy sees.
func readChangeLogAnchor(ctx context.Context, q anchorQuerier, schema string) (int64, error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(ChangeLogTable)
	query := `
SELECT COALESCE(
  (SELECT MIN(id) - 1 FROM ` + tableRef + `
     WHERE xmin::text::bigint >= pg_snapshot_xmin(pg_current_snapshot())::text::bigint),
  (SELECT COALESCE(MAX(id), 0) FROM ` + tableRef + `)
)`
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
