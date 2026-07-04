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
	"strconv"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// This file is the TRANSPORT SEAM (ADR-0136). The trigger engine's setup +
// CDC-reader logic is identical for a local SQLite FILE and a live Cloudflare
// D1 database — the setup DDL, the trigger bodies, the change-log/meta/
// fingerprint schema, the poll SQL, the watermark, and the MAX(id) snapshot
// anchor are all the same (ADR-0135). ONLY the executor differs: a local
// *sql.DB vs the D1 `/query` HTTP API. So all that logic runs against the small
// [executor] interface below, with two implementations:
//
//   - [localExecutor] — the Phase-1 local *sql.DB path, byte-identical to the
//     shipped v0.99.148 engine (its SQL + scan code moved here verbatim).
//   - [d1Executor] — the Phase-2 D1-over-HTTP path, backed by [sqlite.D1Conn]
//     (the same transport the cold-start `d1` reader uses).
//
// [backend] bundles the three transport-specific pieces the shared logic needs
// (the cold-start engine to delegate schema/row reads to, the captured-cell
// decoder, and an executor factory) so Setup / OpenCDCReader / OpenSnapshotStream
// are written ONCE over it.

// executor is the transport the shared trigger logic runs against. execDDL runs
// one non-SELECT setup/teardown statement; the query methods return
// transport-neutral results (the local path scans *sql.Rows, the D1 path
// reconstructs from JSON).
type executor interface {
	execDDL(ctx context.Context, stmt string) error
	pollChangeLog(ctx context.Context, sinceID int64, batch int) ([]rawChangeRow, error)
	readFingerprints(ctx context.Context) ([]fingerprintRow, error)
	changeLogExists(ctx context.Context) (bool, error)
	maxChangeLogID(ctx context.Context) (int64, error)
	discoverTriggers(ctx context.Context) ([]string, error)
	// pruneChangeLogBatch DELETEs one bounded keyset step of change-log rows —
	// floor < id <= upper — and returns the number deleted (ADR-0137, batched
	// per P-1). The `<=` upper bound is load-bearing exactly as the old
	// single-shot form's `id <= cut` was: the caller guarantees upper never
	// exceeds the durably-applied cut, so id == upper is itself durably
	// applied and safe to remove.
	pruneChangeLogBatch(ctx context.Context, floor, upper int64) (deleted int64, err error)
	// minChangeLogID returns MIN(id) of the change-log (0 when empty) — the
	// prune loop's keyset floor. Indexed (id is the PK) and cheap.
	minChangeLogID(ctx context.Context) (int64, error)
	// pruneBatchSize is the transport's DELETE batch bound; see the
	// per-transport constants for the rationale (P-1).
	pruneBatchSize() int64
	// maxPollBatch is the transport's poll-batch ceiling; 0 means no ceiling.
	// The reader clamps its poll batch to it at open (P-3).
	maxPollBatch() int
	// checkpointWAL bounds the source WAL under sustained CDC (Bug 167). On the
	// local SQLite path it issues `PRAGMA wal_checkpoint(TRUNCATE)` so the
	// continuously-churned change-log WAL is reset on a cadence instead of
	// growing without bound; a BUSY result (a reader/writer momentarily held the
	// WAL) is not an error — the next cadence retries. The D1 path is a no-op:
	// D1 polls over HTTP against Cloudflare-managed storage with no local pager
	// or WAL file. It is pure WAL-file management — it must never change the
	// read/apply path or the watermark.
	checkpointWAL(ctx context.Context) error
	// vacuum reclaims file space after a prune (SQLite/D1 only; PG uses
	// autovacuum). Behind the operator's --vacuum opt-in — VACUUM rewrites the
	// whole database.
	vacuum(ctx context.Context) error
	// changeLogStats returns the post-prune MIN(id) (0 on an empty change-log)
	// and total row count, for the operator-facing prune report.
	changeLogStats(ctx context.Context) (minID, count int64, err error)
	close() error
}

// rawChangeRow is one decoded `sluice_change_log` row, transport-neutral. before
// / after carry the captured (typeof, text/hex) JSON image text (a NULL image is
// !Valid); the reader reconstructs the faithful ir.Row from it.
type rawChangeRow struct {
	id         int64
	op         string
	tbl        string
	before     sql.NullString
	after      sql.NullString
	capturedAt sql.NullString
}

// fingerprintRow is one (tbl, columns) row from `sluice_change_log_columns` —
// the captured-column fingerprint the startup drift check compares to the live
// schema.
type fingerprintRow struct {
	tbl     string
	columns string
}

// backend supplies the transport-specific pieces the shared trigger logic needs.
// The local-file backend ([localBackend]) and the D1 backend ([d1Backend]) each
// construct one; everything above it (setup/poll/drift/anchor) is shared.
type backend struct {
	// driver is the user-facing source-driver name woven into the operator
	// recovery hints (e.g. "run sluice trigger setup --source-driver <driver>").
	driver string
	// dsn is the source DSN (the SQLite file path, or the d1:// form), passed to
	// the cold-start engine's schema/row readers.
	dsn string
	// coldStart is the engine the schema + row reads delegate to (the validated
	// `sqlite` file reader or the `d1` HTTP reader), so the captured column set,
	// types, and snapshot rows match the cold-start exactly.
	coldStart ir.Engine
	// newDecoder builds the captured-cell decoder (date/bool policy resolved from
	// the source DSN), shared with the cold-start reader so a captured change
	// decodes byte-identically to a snapshot row.
	newDecoder func() (*sqlite.CapturedCellDecoder, error)
	// openExec opens an executor. readOnly is honoured by the local path (the
	// poller opens a read-only connection, setup/teardown a writable one); the
	// D1 `/query` endpoint executes both, so it ignores the flag.
	openExec func(ctx context.Context, readOnly bool) (executor, error)
}

// localBackend is the Phase-1 local SQLite-FILE backend (ADR-0135). It preserves
// the shipped engine's behaviour exactly: the `sqlite` cold-start engine, the
// DSN-resolved decoder, and a *sql.DB executor opened via [sqlite.OpenFile].
func localBackend(dsn string) backend {
	return backend{
		driver:    EngineName,
		dsn:       dsn,
		coldStart: sqlite.Engine{},
		newDecoder: func() (*sqlite.CapturedCellDecoder, error) {
			return sqlite.NewCapturedCellDecoderForDSN(dsn)
		},
		openExec: func(ctx context.Context, readOnly bool) (executor, error) {
			db, _, err := sqlite.OpenFile(ctx, dsn, readOnly)
			if err != nil {
				return nil, err
			}
			if readOnly {
				// Bug 167: the CDC poller's read connection must NOT linger idle
				// in the pool. An idle pooled connection retains a stale WAL
				// read-mark, which pins SQLite's checkpoint (the app writer's
				// auto-checkpoint AND our own wal_checkpoint(TRUNCATE)) from ever
				// resetting the WAL — so under sustained change-log churn the
				// source WAL (and modernc's mmap of it, hence process RSS) grows
				// without bound. Closing the connection after each poll releases
				// the mark, so the checkpoint can reset the WAL. Ground-truthed:
				// default idle pool grew 69→158 MB in 12 s; SetMaxIdleConns(0)
				// held flat at ~8 MB. Read-only autocommit reads remain correct
				// (each opens its own consistent snapshot); exactly-once and the
				// watermark are unaffected.
				db.SetMaxIdleConns(0)
			}
			return &localExecutor{db: db}, nil
		},
	}
}

// d1Backend is the Phase-2 live-Cloudflare-D1 backend (ADR-0136): the `d1`
// cold-start engine, the D1-DSN-resolved decoder, and a [d1Executor] over the
// shared `/query` transport. Credentials are resolved (and refused loudly if
// absent) at [sqlite.OpenD1Conn]; reachability is verified on the first
// openExec via Ping.
func d1Backend(dsn string) (backend, error) {
	conn, err := sqlite.OpenD1Conn(dsn)
	if err != nil {
		return backend{}, err
	}
	return backend{
		driver:    EngineNameD1,
		dsn:       dsn,
		coldStart: sqlite.NewD1Engine(),
		newDecoder: func() (*sqlite.CapturedCellDecoder, error) {
			return conn.CellDecoder(), nil
		},
		openExec: func(ctx context.Context, _ bool) (executor, error) {
			// Verify the token/account/database at open so a credential or
			// reachability problem fails before any DDL or poll (ADR-0136).
			if err := conn.Ping(ctx); err != nil {
				return nil, err
			}
			return &d1Executor{conn: conn}, nil
		},
	}, nil
}

// --- shared catalog SQL (byte-identical across both transports) -------------

const (
	// changeLogExistsSQL probes for the change-log table by exact name.
	changeLogExistsSQL = `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`
	// discoverTriggersSQL lists every sluice-installed capture trigger.
	discoverTriggersSQL = `SELECT name FROM sqlite_master WHERE type = 'trigger' ` +
		`AND name LIKE 'sluice\_capture\_%' ESCAPE '\' ORDER BY name`
	// readFingerprintsSQL reads the per-table captured-column fingerprints.
	readFingerprintsSQL = `SELECT tbl, columns FROM "` + ChangeLogColumnsTable + `" ORDER BY tbl`
)

// --- localExecutor: the Phase-1 *sql.DB transport ---------------------------

// localExecutor runs the trigger SQL against a local SQLite file's *sql.DB. Its
// SQL and scan code are the shipped v0.99.148 engine's verbatim, so the local
// path is byte-identical after the executor refactor.
type localExecutor struct {
	db *sql.DB
}

func (e *localExecutor) execDDL(ctx context.Context, stmt string) error {
	_, err := e.db.ExecContext(ctx, stmt)
	return err
}

func (e *localExecutor) pollChangeLog(ctx context.Context, sinceID int64, batch int) ([]rawChangeRow, error) {
	const q = "SELECT id, op, tbl, before, after, captured_at FROM " +
		`"` + ChangeLogTable + `" WHERE id > ? ORDER BY id ASC LIMIT ?`
	rows, err := e.db.QueryContext(ctx, q, sinceID, batch)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []rawChangeRow
	for rows.Next() {
		var rc rawChangeRow
		if err := rows.Scan(&rc.id, &rc.op, &rc.tbl, &rc.before, &rc.after, &rc.capturedAt); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		out = append(out, rc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter rows: %w", err)
	}
	return out, nil
}

func (e *localExecutor) readFingerprints(ctx context.Context) ([]fingerprintRow, error) {
	rows, err := e.db.QueryContext(ctx, readFingerprintsSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []fingerprintRow
	for rows.Next() {
		var fr fingerprintRow
		if err := rows.Scan(&fr.tbl, &fr.columns); err != nil {
			return nil, fmt.Errorf("sqlite-trigger: scan captured-column fingerprint: %w", err)
		}
		out = append(out, fr)
	}
	return out, rows.Err()
}

func (e *localExecutor) changeLogExists(ctx context.Context) (bool, error) {
	var name string
	err := e.db.QueryRowContext(ctx, changeLogExistsSQL, ChangeLogTable).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *localExecutor) maxChangeLogID(ctx context.Context) (int64, error) {
	var id sql.NullInt64
	if err := e.db.QueryRowContext(ctx, `SELECT MAX(id) FROM "`+ChangeLogTable+`"`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func (e *localExecutor) discoverTriggers(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, discoverTriggersSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// localPruneBatchSize bounds one local-file prune DELETE (P-1). SQLite is
// single-writer: the SOURCE APPLICATION's writes stall for the duration of any
// DELETE we run, so a monolithic backlog DELETE would hold its writer hostage.
// ~20k id-ordered PK deletes is milliseconds on a local file — larger than
// D1's bound (no per-query CPU ceiling here) but short enough that the app's
// writer interleaves between batches.
const localPruneBatchSize = 20_000

func (e *localExecutor) pruneChangeLogBatch(ctx context.Context, floor, upper int64) (int64, error) {
	res, err := e.db.ExecContext(ctx,
		`DELETE FROM "`+ChangeLogTable+`" WHERE id > ? AND id <= ?`, floor, upper)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (e *localExecutor) minChangeLogID(ctx context.Context) (int64, error) {
	var id sql.NullInt64
	if err := e.db.QueryRowContext(ctx, `SELECT MIN(id) FROM "`+ChangeLogTable+`"`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func (e *localExecutor) pruneBatchSize() int64 { return localPruneBatchSize }

// maxPollBatch: no transport ceiling — the local *sql.DB path streams rows
// with no response-size cap, so the reader keeps the shared default batch.
func (e *localExecutor) maxPollBatch() int { return 0 }

func (e *localExecutor) checkpointWAL(ctx context.Context) error {
	// PRAGMA wal_checkpoint returns one row: (busy, log_frames, checkpointed).
	// busy=1 means a reader/writer held the WAL so it could not fully truncate
	// this round — NOT an error; the next cadence retries. On a non-WAL database
	// it returns (0,-1,-1) with no error, so this is harmless if the operator's
	// source is not in WAL mode. With the poller pool's idle connection released
	// (SetMaxIdleConns(0)) there is no stale read-mark of our own to pin it.
	var busy, logFrames, checkpointed int
	if err := e.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").
		Scan(&busy, &logFrames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 {
		slog.DebugContext(ctx, "sqlite-trigger: WAL checkpoint busy; retrying next cadence",
			slog.Int("log_frames", logFrames), slog.Int("checkpointed", checkpointed))
	} else {
		slog.DebugContext(ctx, "sqlite-trigger: WAL checkpoint(TRUNCATE) ok",
			slog.Int("log_frames", logFrames), slog.Int("checkpointed", checkpointed))
	}
	return nil
}

func (e *localExecutor) vacuum(ctx context.Context) error {
	_, err := e.db.ExecContext(ctx, "VACUUM")
	return err
}

func (e *localExecutor) changeLogStats(ctx context.Context) (minID, count int64, err error) {
	var m sql.NullInt64
	if scanErr := e.db.QueryRowContext(ctx, `SELECT MIN(id), COUNT(*) FROM "`+ChangeLogTable+`"`).Scan(&m, &count); scanErr != nil {
		return 0, 0, scanErr
	}
	if m.Valid {
		minID = m.Int64
	}
	return minID, count, nil
}

func (e *localExecutor) close() error {
	if e.db == nil {
		return nil
	}
	err := e.db.Close()
	e.db = nil
	return err
}

// --- d1Executor: the Phase-2 D1 /query HTTP transport -----------------------

// d1Executor runs the SAME trigger SQL against a live Cloudflare D1 database
// over the `/query` HTTP API (ADR-0136). The catalog queries are byte-identical
// to the local path; the poll and MAX(id) project the `id` as exact TEXT
// (CAST AS TEXT) and bind the watermark as a STRING param so the watermark is
// never rounded through a JSON number — the ADR-0132 no-JSON-number-for-an-
// integer-we-key-on discipline, kept uniform even though the change-log
// AUTOINCREMENT id is bounded well under 2^53.
type d1Executor struct {
	conn *sqlite.D1Conn
}

func (e *d1Executor) execDDL(ctx context.Context, stmt string) error {
	return e.conn.Exec(ctx, stmt)
}

func (e *d1Executor) pollChangeLog(ctx context.Context, sinceID int64, batch int) ([]rawChangeRow, error) {
	// LIMIT is a trusted in-process int (embedded, like the d1 row reader's
	// pagination); only the watermark crosses as a bound param, sent as a string.
	q := `SELECT CAST(id AS TEXT) AS id, op, tbl, before, after, captured_at FROM "` +
		ChangeLogTable + `" WHERE id > ? ORDER BY id ASC LIMIT ` + strconv.Itoa(batch)
	rows, err := e.conn.Query(ctx, q, strconv.FormatInt(sinceID, 10))
	if err != nil {
		return nil, err
	}
	out := make([]rawChangeRow, 0, len(rows))
	for _, row := range rows {
		idText, ok, err := d1CellString(row["id"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: decode change-log id: %w", err)
		}
		if !ok {
			return nil, errors.New("d1-trigger: change-log row has a NULL/absent id")
		}
		id, perr := strconv.ParseInt(idText, 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id %q is not a valid int64: %w", idText, perr)
		}
		op, _, err := d1CellString(row["op"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode op: %w", id, err)
		}
		tbl, _, err := d1CellString(row["tbl"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode tbl: %w", id, err)
		}
		before, err := d1NullString(row["before"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode before: %w", id, err)
		}
		after, err := d1NullString(row["after"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode after: %w", id, err)
		}
		capturedAt, err := d1NullString(row["captured_at"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode captured_at: %w", id, err)
		}
		out = append(out, rawChangeRow{
			id: id, op: op, tbl: tbl,
			before: before, after: after, capturedAt: capturedAt,
		})
	}
	return out, nil
}

func (e *d1Executor) readFingerprints(ctx context.Context) ([]fingerprintRow, error) {
	rows, err := e.conn.Query(ctx, readFingerprintsSQL)
	if err != nil {
		return nil, err
	}
	out := make([]fingerprintRow, 0, len(rows))
	for _, row := range rows {
		tbl, _, err := d1CellString(row["tbl"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: scan captured-column fingerprint tbl: %w", err)
		}
		columns, _, err := d1CellString(row["columns"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: scan captured-column fingerprint columns: %w", err)
		}
		out = append(out, fingerprintRow{tbl: tbl, columns: columns})
	}
	return out, nil
}

func (e *d1Executor) changeLogExists(ctx context.Context) (bool, error) {
	rows, err := e.conn.Query(ctx, changeLogExistsSQL, ChangeLogTable)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func (e *d1Executor) maxChangeLogID(ctx context.Context) (int64, error) {
	rows, err := e.conn.Query(ctx, `SELECT CAST(MAX(id) AS TEXT) AS m FROM "`+ChangeLogTable+`"`)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	text, ok, err := d1CellString(rows[0]["m"])
	if err != nil {
		return 0, err
	}
	if !ok { // MAX(id) over an empty change-log is NULL.
		return 0, nil
	}
	id, perr := strconv.ParseInt(text, 10, 64)
	if perr != nil {
		return 0, fmt.Errorf("d1-trigger: MAX(id) text %q is not a valid int64: %w", text, perr)
	}
	return id, nil
}

func (e *d1Executor) discoverTriggers(ctx context.Context) ([]string, error) {
	rows, err := e.conn.Query(ctx, discoverTriggersSQL)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		name, ok, err := d1CellString(row["name"])
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, name)
		}
	}
	return out, nil
}

// d1PruneBatchSize bounds one D1 prune DELETE (P-1). D1 enforces a per-query
// CPU ceiling (a too-big statement fails with the HTTP 400 code-7500 class —
// the same limit that broke `--infer-types` GLOBs, ADR-0145), and a failed
// tick retries an even BIGGER delete next interval — prune would fall behind
// permanently, the exact failure ADR-0137 Phase B exists to prevent. 2k
// id-ordered PK deletes stays comfortably under the ceiling; the batching loop
// makes throughput a function of the tick budget, not statement size.
const d1PruneBatchSize = 2_000

func (e *d1Executor) pruneChangeLogBatch(ctx context.Context, floor, upper int64) (int64, error) {
	// The D1 `/query` transport doesn't surface rows-affected, so count the
	// to-be-deleted range first. This is exact and concurrency-safe: only rows
	// with id <= upper (<= the durably-applied cut) are deleted, the
	// AUTOINCREMENT id only ever grows, and the loop steps disjoint
	// (floor, upper] ranges, so no row can enter the range between this count
	// and the DELETE (the same property that makes pruning safe while a sync
	// is live). COUNT is projected as TEXT so a JSON-number cell never reaches
	// d1CellString.
	floorStr := strconv.FormatInt(floor, 10)
	upperStr := strconv.FormatInt(upper, 10)
	rows, err := e.conn.Query(ctx,
		`SELECT CAST(COUNT(*) AS TEXT) AS n FROM "`+ChangeLogTable+`" WHERE id > ? AND id <= ?`,
		floorStr, upperStr)
	if err != nil {
		return 0, err
	}
	var deleted int64
	if len(rows) > 0 {
		text, ok, cerr := d1CellString(rows[0]["n"])
		if cerr != nil {
			return 0, fmt.Errorf("d1-trigger: decode prune count: %w", cerr)
		}
		if ok {
			if deleted, err = strconv.ParseInt(text, 10, 64); err != nil {
				return 0, fmt.Errorf("d1-trigger: prune count %q is not a valid int64: %w", text, err)
			}
		}
	}
	if err := e.conn.Exec(ctx,
		`DELETE FROM "`+ChangeLogTable+`" WHERE id > ? AND id <= ?`, floorStr, upperStr); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (e *d1Executor) minChangeLogID(ctx context.Context) (int64, error) {
	rows, err := e.conn.Query(ctx,
		`SELECT CAST(COALESCE(MIN(id), 0) AS TEXT) AS mn FROM "`+ChangeLogTable+`"`)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	id, err := d1IntCell(rows[0]["mn"])
	if err != nil {
		return 0, fmt.Errorf("d1-trigger: decode change-log MIN(id): %w", err)
	}
	return id, nil
}

func (e *d1Executor) pruneBatchSize() int64 { return d1PruneBatchSize }

// d1PollBatchSize clamps the change-log poll batch on the D1 transport (P-3).
// Cloudflare caps a /query response at ~1 MB — the same limit that sizes the
// cold-copy row reader's page at 1000 (see sqlite.d1PageSize) — and change
// rows are HEAVIER than data rows (each carries full before/after JSON
// images), so the shared defaultBatchSize (10000) can overflow the cap on a
// catch-up poll. 1000 matches d1PageSize's reasoning; the pump's full-batch
// fast-repoll keeps catch-up throughput unthrottled.
const d1PollBatchSize = 1000

func (e *d1Executor) maxPollBatch() int { return d1PollBatchSize }

// checkpointWAL is a no-op on the D1 transport: D1 polls over the `/query` HTTP
// API against Cloudflare-managed storage — there is no local pager or WAL file
// for sluice to checkpoint (Bug 167 is local-SQLite-only).
func (e *d1Executor) checkpointWAL(context.Context) error { return nil }

func (e *d1Executor) vacuum(ctx context.Context) error {
	return e.conn.Exec(ctx, "VACUUM")
}

func (e *d1Executor) changeLogStats(ctx context.Context) (minID, count int64, err error) {
	rows, err := e.conn.Query(ctx,
		`SELECT CAST(COALESCE(MIN(id), 0) AS TEXT) AS mn, CAST(COUNT(*) AS TEXT) AS cnt FROM "`+ChangeLogTable+`"`)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}
	minID, err = d1IntCell(rows[0]["mn"])
	if err != nil {
		return 0, 0, fmt.Errorf("d1-trigger: decode change-log MIN(id): %w", err)
	}
	count, err = d1IntCell(rows[0]["cnt"])
	if err != nil {
		return 0, 0, fmt.Errorf("d1-trigger: decode change-log count: %w", err)
	}
	return minID, count, nil
}

// d1IntCell decodes a CAST(... AS TEXT) integer cell to int64. A NULL/absent
// cell decodes to 0 (the COALESCE in the stats query already guards MIN(id);
// this is a defensive fallback).
func d1IntCell(raw json.RawMessage) (int64, error) {
	text, ok, err := d1CellString(raw)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return strconv.ParseInt(text, 10, 64)
}

// close is a no-op: the D1 HTTP transport has no pool/file to release.
func (e *d1Executor) close() error { return nil }

// d1CellString extracts a Go string from a JSON string cell. ok is false for a
// JSON null/absent value (so the caller can distinguish absent from ""); a
// non-string, non-null JSON value is an error.
func d1CellString(raw json.RawMessage) (s string, ok bool, err error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false, nil
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false, err
	}
	return s, true, nil
}

// d1NullString maps a JSON string/null cell to a sql.NullString, matching the
// local path's nullable change-log columns (before / after / captured_at).
func d1NullString(raw json.RawMessage) (sql.NullString, error) {
	s, ok, err := d1CellString(raw)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: s, Valid: ok}, nil
}
