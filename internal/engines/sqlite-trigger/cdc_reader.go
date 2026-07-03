// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"encoding/json"
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
	// defaultCheckpointInterval is how often the LOCAL SQLite poller issues
	// PRAGMA wal_checkpoint(TRUNCATE) to bound the source WAL (Bug 167). The
	// primary fix is releasing the poller's idle connection (so the app writer's
	// own auto-checkpoint can reset the WAL); this cadence is the backstop that
	// keeps the WAL bounded even when the operator's app has disabled
	// auto-checkpoint (PRAGMA wal_autocheckpoint=0). 30 s is low-overhead (a
	// checkpoint on a quiescent WAL is near-instant) while capping a runaway.
	// The D1 path's checkpointWAL is a no-op, so this is inert there.
	defaultCheckpointInterval = 30 * time.Second
)

// committedAtLayout is the change-log captured_at format produced by the trigger
// (`strftime('%Y-%m-%d %H:%M:%f', 'now')`, UTC). Parsed back for the sync-lag
// metric; a parse failure maps to the zero time ("unknown"), never a guess.
const committedAtLayout = "2006-01-02 15:04:05.999"

// CDCReader is the trigger-engine CDC reader. It polls `sluice_change_log` at a
// configurable cadence (default 1s) and emits [ir.Change] events via the channel
// returned from [StreamChanges]. It is transport-neutral: the underlying
// [executor] is either a local *sql.DB (Phase 1) or the D1 `/query` HTTP API
// (Phase 2, ADR-0136); the poll/decode logic above it is identical.
//
// One reader → one [StreamChanges] call. Concurrent calls are not supported; the
// polling goroutine owns the executor for the lifetime of the stream. The reader
// emits NO [ir.TxBegin]/[ir.TxCommit] markers (a change-log row carries no
// source-transaction grouping), so it is a marker-less stream — exactly like
// pgtrigger; the Streamer's checkpoint cadence persists the watermark (the
// pgtrigger Bug-159 contract applies identically here).
type CDCReader struct {
	exec executor
	dec  *sqlite.CapturedCellDecoder

	// b is the transport backend (dsn + executor factory) the reader was opened
	// against. Retained so the ADR-0137 Phase-B auto-prune ([PruneConsumedChangeLog])
	// can open its OWN writable executor for the DELETE — independent of the
	// read-only poll executor above, so the prune never contends with (or races
	// the Close of) the polling connection.
	b backend

	// colTypes maps table → column-name → resolved IR type, read once at open
	// from the validated cold-start schema reader so each captured cell decodes
	// through the SAME storage-class-faithful path as a cold-start row.
	colTypes map[string]map[string]ir.Type

	pollInterval time.Duration
	batchSize    int

	// checkpointInterval is the cadence at which the pump issues
	// exec.checkpointWAL to bound the source WAL (Bug 167). Zero disables it.
	checkpointInterval time.Duration

	// pumpCancel cancels the polling goroutine when Close is called.
	pumpCancel context.CancelFunc

	mu  sync.Mutex
	err error
}

// openCDCReader constructs a [CDCReader] for a local SQLite FILE (Phase 1,
// ADR-0135). It is the byte-identical entry point the file engine + the
// unit/integration tests use; it delegates to [openCDCReaderBackend] with the
// local backend.
func openCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return openCDCReaderBackend(ctx, localBackend(dsn))
}

// openCDCReaderBackend constructs a [CDCReader] against any [backend]. It
// resolves the captured-cell decoder, reads the schema once to build the
// column-type lookup + the live fingerprints, opens a read-only executor, and
// refuses loudly when the change-log table is absent (the operator forgot to run
// `sluice trigger setup`) or when the source schema has drifted since setup. The
// refusals fire at open time so the streamer surfaces them before any data moves.
func openCDCReaderBackend(ctx context.Context, b backend) (ir.CDCReader, error) {
	dec, err := b.newDecoder()
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: cdc: resolve date encoding: %w", err)
	}
	colTypes, liveFingerprints, err := loadColumnTypes(ctx, b.coldStart, b.dsn)
	if err != nil {
		return nil, err
	}

	exec, err := b.openExec(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: cdc open: %w", err)
	}
	if exists, err := exec.changeLogExists(ctx); err != nil {
		_ = exec.close()
		return nil, fmt.Errorf("sqlite-trigger: cdc preflight: %w", err)
	} else if !exists {
		_ = exec.close()
		return nil, changeLogAbsentErr(b.driver)
	}
	// Refuse loudly on un-re-setup source schema drift (ADR-0135). SQLite/D1 have
	// no DDL triggers, so an ADD/DROP/RENAME COLUMN after setup leaves the
	// triggers capturing a stale column set — and an ADD COLUMN would SILENTLY
	// drop the new column from the stream. The startup fingerprint check catches
	// BOTH directions before any data moves.
	if err := verifyNoSchemaDrift(ctx, exec, liveFingerprints); err != nil {
		_ = exec.close()
		return nil, err
	}
	return &CDCReader{
		exec:               exec,
		dec:                dec,
		b:                  b,
		colTypes:           colTypes,
		pollInterval:       defaultPollInterval,
		batchSize:          defaultBatchSize,
		checkpointInterval: defaultCheckpointInterval,
	}, nil
}

// changeLogAbsentErr is the shared "you forgot `trigger setup`" refusal, naming
// the actual source-driver so the recovery command is copy-pasteable.
func changeLogAbsentErr(driver string) error {
	return fmt.Errorf(
		"sqlite-trigger: %s does not exist on the source — run "+
			"`sluice trigger setup --source-driver %s --dsn=... --tables=...` before starting the stream",
		ChangeLogTable, driver,
	)
}

// loadColumnTypes reads the schema once (via the cold-start engine's schema
// reader, so types + the captured column set match the cold-start reader exactly)
// and builds (1) table → column → IR type for per-cell decode and (2) table →
// captured-column FINGERPRINT for the startup drift check. The change-log/meta/
// columns tables are already skipped by that reader (ADR-0135).
func loadColumnTypes(ctx context.Context, coldStart ir.Engine, dsn string) (colTypes map[string]map[string]ir.Type, fingerprints map[string]string, err error) {
	sr, err := coldStart.OpenSchemaReader(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite-trigger: cdc: open schema reader: %w", err)
	}
	defer func() { _ = closeReader(sr) }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite-trigger: cdc: read schema: %w", err)
	}
	colTypes = make(map[string]map[string]ir.Type, len(schema.Tables))
	fingerprints = make(map[string]string, len(schema.Tables))
	for _, t := range schema.Tables {
		cols := make(map[string]ir.Type, len(t.Columns))
		for _, c := range t.Columns {
			cols[c.Name] = c.Type
		}
		colTypes[t.Name] = cols
		fingerprints[t.Name] = columnFingerprint(nonGeneratedColumnNames(t))
	}
	return colTypes, fingerprints, nil
}

// verifyNoSchemaDrift compares the captured-column fingerprint recorded at
// `trigger setup` (in [ChangeLogColumnsTable]) against the LIVE schema's
// non-generated column set for every replicated table, and refuses loudly on the
// first difference. This is the load-bearing guard for the silent ADD-COLUMN
// direction: the stale trigger still captures every OLD column (so the per-row
// decode check never fires), but the new column's values would vanish from the
// stream — caught here at stream start instead. A DROP/RENAME (live set differs)
// and a dropped table (live fingerprint absent) are caught the same way.
func verifyNoSchemaDrift(ctx context.Context, exec executor, liveFingerprints map[string]string) error {
	fps, err := exec.readFingerprints(ctx)
	if err != nil {
		// The columns table is created alongside the change-log (and dropped with
		// it), so "change-log exists ⟹ columns exists". A missing/erroring table
		// here is an inconsistent half-install — refuse rather than skip the guard.
		return fmt.Errorf(
			"sqlite-trigger: cannot read the captured-column fingerprint table %s (%w); "+
				"the trigger install looks inconsistent — re-run `sluice trigger setup`",
			ChangeLogColumnsTable, err,
		)
	}
	for _, fp := range fps {
		live, ok := liveFingerprints[fp.tbl]
		if !ok {
			return fmt.Errorf(
				"sqlite-trigger: table %q has capture triggers installed but no longer exists in the source schema; "+
					"re-run `sluice trigger setup` after a schema change (Phase 1 does not forward DDL)", fp.tbl,
			)
		}
		if live != fp.columns {
			return fmt.Errorf(
				"sqlite-trigger: table %q schema has drifted since `trigger setup` (captured columns %s, live columns %s); "+
					"the stale triggers would mis-capture (an ADD COLUMN would be SILENTLY dropped) — "+
					"re-run `sluice trigger setup` (Phase 1 does not forward DDL)", fp.tbl, fp.columns, live,
			)
		}
	}
	return nil
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

// SetCheckpointInterval overrides the WAL-checkpoint cadence (Bug 167). Must be
// called before [StreamChanges]. A zero or negative duration DISABLES the
// periodic checkpoint (the idle-connection release alone still lets the app
// writer's auto-checkpoint bound the WAL). Used by tests to force a tight
// cadence; not wired to a CLI flag — the default is sane for operators.
func (r *CDCReader) SetCheckpointInterval(d time.Duration) {
	r.checkpointInterval = d
}

// Close releases the underlying executor and stops any in-flight polling
// goroutine.
func (r *CDCReader) Close() error {
	if r.pumpCancel != nil {
		r.pumpCancel()
	}
	if r.exec != nil {
		err := r.exec.close()
		r.exec = nil
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
		startID, err = r.exec.maxChangeLogID(ctx)
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
// can commit out of allocation order; see ADR-0135 §3). On D1 the same holds:
// the poll uses the PRIMARY (strongly-consistent) query path, where writes are
// serialised per database, so id-order = commit-order (ADR-0136 §4) — NOT a
// lagging read replica. When a poll returns a full batch the next poll fires
// immediately so a bursty source isn't throttled by the cadence.
func (r *CDCReader) pump(ctx context.Context, startID int64, out chan<- ir.Change) {
	lastSeen := startID
	timer := time.NewTimer(0) // fire immediately on the first iteration
	defer timer.Stop()
	// lastCheckpoint paces the WAL-checkpoint cadence (Bug 167). Initialised to
	// now so the first checkpoint fires one interval into the stream.
	lastCheckpoint := time.Now()
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
		// Bound the source WAL on a cadence (Bug 167). Runs in this same
		// goroutine, between polls, so it never races the poll read on the
		// executor. A checkpoint failure must NOT break the stream — WAL
		// management is orthogonal to read/apply correctness — so it is logged
		// and the loop continues.
		if r.checkpointInterval > 0 && time.Since(lastCheckpoint) >= r.checkpointInterval {
			if cerr := r.exec.checkpointWAL(ctx); cerr != nil {
				slog.DebugContext(ctx, "sqlite-trigger: WAL checkpoint failed (non-fatal)",
					slog.Any("err", cerr))
			}
			lastCheckpoint = time.Now()
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
	raws, qErr := r.exec.pollChangeLog(ctx, lastSeen, r.batchSize)
	if qErr != nil {
		return nil, lastSeen, qErr
	}
	newLast = lastSeen
	for _, rc := range raws {
		if rc.id > newLast {
			newLast = rc.id
		}
		ev, err := r.buildChange(rc.id, rc.op, rc.tbl, rc.before, rc.after, commitTime(rc.capturedAt))
		if err != nil {
			return nil, lastSeen, err
		}
		events = append(events, ev)
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

// Compile-time check that [CDCReader] implements [ir.CDCReader].
var _ ir.CDCReader = (*CDCReader)(nil)
